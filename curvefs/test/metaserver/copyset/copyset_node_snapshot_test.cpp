/*
 *  Copyright (c) 2021 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * Project: curve
 * Date: Sun Sep  5 16:31:26 CST 2021
 * Author: wuhanqing
 */

#include <braft/node.h>
#include <braft/storage.h>
#include <brpc/server.h>
#include <gtest/gtest.h>

#include "curvefs/src/metaserver/copyset/copyset_node.h"
#include "curvefs/src/metaserver/copyset/copyset_node_manager.h"
#include "curvefs/test/metaserver/mock/mock_metastore.h"
#include "src/common/uuid.h"
#include "test/fs/mock_local_filesystem.h"

namespace curvefs {
namespace metaserver {
namespace copyset {

using ::curve::common::UUIDGenerator;
using ::curve::fs::MockLocalFileSystem;
using ::testing::_;
using ::testing::DoAll;
using ::testing::Invoke;
using ::testing::Matcher;
using ::testing::Return;
using ::testing::SetArgPointee;

class FakeSnapshotSaveClosure : public braft::Closure {
 public:
    FakeSnapshotSaveClosure() : mtx_(), cond_(), runned_(false) {}

    void Run() override {
        std::lock_guard<std::mutex> lock(mtx_);
        runned_ = true;
        cond_.notify_one();
    }

    void WaitRunned() {
        std::unique_lock<std::mutex> lock(mtx_);
        cond_.wait(lock, [this]() { return runned_; });
    }

 private:
    std::mutex mtx_;
    std::condition_variable cond_;
    bool runned_;
};

class MockSnapshotWriter : public braft::SnapshotWriter {
 public:
    MOCK_METHOD1(save_meta, int(const braft::SnapshotMeta&));
    MOCK_METHOD1(add_file, int(const std::string&));
    MOCK_METHOD2(add_file,
                 int(const std::string&, const google::protobuf::Message*));
    MOCK_METHOD1(remove_file, int(const std::string&));
    MOCK_METHOD0(get_path, std::string());
    MOCK_METHOD1(list_files, void(std::vector<std::string>*));
    MOCK_METHOD2(get_file_meta,
                 int(const std::string&, google::protobuf::Message*));
};

class MockSnapshotReader : public braft::SnapshotReader {
 public:
    MOCK_METHOD1(load_meta, int(braft::SnapshotMeta*));
    MOCK_METHOD0(generate_uri_for_copy, std::string());
    MOCK_METHOD0(get_path, std::string());
    MOCK_METHOD1(list_files, void(std::vector<std::string>*));
    MOCK_METHOD2(get_file_meta,
                 int(const std::string&, google::protobuf::Message*));
};

const int kTestPort = 29930;
const char* kCopysetInitConf =
    "127.0.0.1:29930:0,127.0.0.1:29931:0,127.0.0.1:29932:0";

class CopysetNodeRaftSnapshotTest : public testing::Test {
 protected:
    void SetUp() override {
        dataPath_ = "./runlog/" + UUIDGenerator{}.GenerateUUID();
        trashPath_ = dataPath_ + "_trash";
        mockfs_ = absl::make_unique<MockLocalFileSystem>();
        nodeManager_ = &CopysetNodeManager::GetInstance();
        mockMetaStore_ = absl::make_unique<mock::MockMetaStore>();

        poolId_ = time(nullptr) % 12345;
        copysetId_ = time(nullptr) % reinterpret_cast<uint64_t>(this);

        options_.ip = "127.0.0.1";
        options_.port = kTestPort;

        options_.dataUri = "local://" + dataPath_;
        options_.raftNodeOptions.log_uri  = "local://" + dataPath_;
        options_.raftNodeOptions.raft_meta_uri = "local://" + dataPath_;
        options_.raftNodeOptions.snapshot_uri = "local://" + dataPath_;

        // disable raft snapshot
        options_.raftNodeOptions.snapshot_interval_s = -1;

        options_.localFileSystem = mockfs_.get();

        options_.trashOptions.trashUri = "local://" + trashPath_;
        options_.storageOptions.type = "memory";

        butil::ip_t ip;
        ASSERT_EQ(0, butil::str2ip(options_.ip.c_str(), &ip));
        butil::EndPoint listenAddr(ip, kTestPort);

        EXPECT_CALL(*mockfs_, DirExists(_)).WillOnce(Return(false));

        ASSERT_TRUE(nodeManager_->Init(options_));
        ASSERT_TRUE(nodeManager_->Start());
        nodeManager_->AddService(&server_, listenAddr);

        ASSERT_EQ(0, server_.Start(listenAddr, nullptr));

        EXPECT_CALL(*mockfs_, Mkdir(_))
            .WillRepeatedly(Return(0));
    }

    void TearDown() {
        nodeManager_->Stop();

        server_.Stop(0);
        server_.Join();

        system(std::string("rm -rf " + dataPath_).c_str());
    }

 protected:
    bool CreateOneCopyset() {
        braft::Configuration conf;
        if (0 != conf.parse_from(kCopysetInitConf)) {
            LOG(ERROR) << "Parse conf failed";
            return false;
        }

        if (!nodeManager_->CreateCopysetNode(poolId_, copysetId_, conf)) {
            LOG(ERROR) << "CreateCopysetNode failed";
            return false;
        }

        return true;
    }

 protected:
    PoolId poolId_;
    CopysetId copysetId_;
    std::string dataPath_;
    std::string trashPath_;

    CopysetNodeOptions options_;
    CopysetNodeManager* nodeManager_;

    std::unique_ptr<MockLocalFileSystem> mockfs_;
    std::unique_ptr<mock::MockMetaStore> mockMetaStore_;

    brpc::Server server_;
};

TEST_F(CopysetNodeRaftSnapshotTest, SnapshotSaveTest_SaveConfEpochFailed) {
    ASSERT_TRUE(CreateOneCopyset());

    auto* node = nodeManager_->GetCopysetNode(poolId_, copysetId_);
    ASSERT_NE(nullptr, node);

    MockSnapshotWriter writer;
    FakeSnapshotSaveClosure done;

    EXPECT_CALL(writer, get_path())
        .WillRepeatedly(Return(dataPath_));
    EXPECT_CALL(*mockfs_, Open(_, _))
        .WillOnce(Invoke([](const std::string&, int) {
            errno = EINVAL;
            return -1;
        }));
    EXPECT_CALL(*mockMetaStore_, Save(_, _)).Times(0);

    node->on_snapshot_save(&writer, &done);

    done.WaitRunned();

    EXPECT_FALSE(done.status().ok());
    EXPECT_EQ(EINVAL, done.status().error_code());
}

TEST_F(CopysetNodeRaftSnapshotTest, SnapshotSaveTest_MetaStoreSaveFailed) {
    ASSERT_TRUE(CreateOneCopyset());

    auto* node = nodeManager_->GetCopysetNode(poolId_, copysetId_);
    ASSERT_NE(nullptr, node);

    auto mockMetaStore = mockMetaStore_.get();
    node->SetMetaStore(mockMetaStore_.release());

    MockSnapshotWriter writer;
    FakeSnapshotSaveClosure done;

    EXPECT_CALL(writer, get_path())
        .WillRepeatedly(Return(dataPath_));
    EXPECT_CALL(writer, add_file(_))
        .Times(1);
    EXPECT_CALL(*mockfs_, Open(_, _))
        .WillOnce(Return(0));
    EXPECT_CALL(*mockfs_, Write(_, Matcher<const char*>(_), _, _))
        .WillOnce(Invoke(
            [](int fd, const char*, uint64_t, int length) { return length; }));
    EXPECT_CALL(*mockfs_, Fsync(_))
        .WillOnce(Return(0));
    EXPECT_CALL(*mockfs_, Close(_))
        .Times(1);
    EXPECT_CALL(*mockMetaStore, Save(_, _))
        .WillOnce(Invoke([](std::string path, OnSnapshotSaveDoneClosure* done) {
            done->SetError(MetaStatusCode::UNKNOWN_ERROR);
            done->Run();
            return false;
        }));

    node->on_snapshot_save(&writer, &done);
    done.WaitRunned();

    EXPECT_FALSE(done.status().ok());
    EXPECT_EQ(MetaStatusCode::UNKNOWN_ERROR, done.status().error_code());

    // TODO(wuhanqing): check metric
}

TEST_F(CopysetNodeRaftSnapshotTest, SnapshotSaveTest_Success) {
    ASSERT_TRUE(CreateOneCopyset());

    auto* node = nodeManager_->GetCopysetNode(poolId_, copysetId_);
    ASSERT_NE(nullptr, node);

    auto mockMetaStore = mockMetaStore_.get();
    node->SetMetaStore(mockMetaStore_.release());

    MockSnapshotWriter writer;
    FakeSnapshotSaveClosure done;

    EXPECT_CALL(writer, get_path())
        .WillRepeatedly(Return(dataPath_));
    EXPECT_CALL(writer, add_file(_))
        .Times(1);
    EXPECT_CALL(*mockfs_, Open(_, _))
        .WillOnce(Return(0));
    EXPECT_CALL(*mockfs_, Write(_, Matcher<const char*>(_), _, _))
        .WillOnce(Invoke(
            [](int fd, const char*, uint64_t, int length) { return length; }));
    EXPECT_CALL(*mockfs_, Fsync(_))
        .WillOnce(Return(0));
    EXPECT_CALL(*mockfs_, Close(_))
        .Times(1);
    EXPECT_CALL(*mockMetaStore, Save(_, _))
        .WillOnce(Invoke([](std::string path, OnSnapshotSaveDoneClosure* done) {
            done->SetSuccess();
            done->Run();
            return true;
        }));

    node->on_snapshot_save(&writer, &done);
    done.WaitRunned();

    EXPECT_TRUE(done.status().ok());
    EXPECT_EQ(MetaStatusCode::OK, done.status().error_code());

    // TODO(wuhanqing): check metric
}

TEST_F(CopysetNodeRaftSnapshotTest, SnapshotLoadTest_LoadConfFileFailed) {
    ASSERT_TRUE(CreateOneCopyset());

    auto* node = nodeManager_->GetCopysetNode(poolId_, copysetId_);
    ASSERT_NE(nullptr, node);

    MockSnapshotReader reader;
    EXPECT_CALL(reader, get_path())
        .WillRepeatedly(Return(dataPath_));
    EXPECT_CALL(*mockfs_, FileExists(_))
        .WillOnce(Return(true));
    EXPECT_CALL(*mockfs_, Open(_, _))
        .WillOnce(Return(-1));

    ASSERT_FALSE(node->IsLoading());
    EXPECT_NE(0, node->on_snapshot_load(&reader));
    ASSERT_FALSE(node->IsLoading());
}

TEST_F(CopysetNodeRaftSnapshotTest,
       SnapshotLoadTest_ConfFileAndMetaFileNotExists) {
    ASSERT_TRUE(CreateOneCopyset());

    auto* node = nodeManager_->GetCopysetNode(poolId_, copysetId_);
    ASSERT_NE(nullptr, node);

    auto mockMetaStore = mockMetaStore_.get();
    node->SetMetaStore(mockMetaStore_.release());

    MockSnapshotReader reader;
    EXPECT_CALL(reader, get_path())
        .WillRepeatedly(Return(dataPath_));
    EXPECT_CALL(*mockfs_, FileExists(_))
        .WillOnce(Return(false));
    EXPECT_CALL(*mockfs_, Open(_, _))
        .Times(0);
    EXPECT_CALL(*mockMetaStore, Clear())
        .Times(1);
    EXPECT_CALL(*mockMetaStore, Load(_))
        .WillOnce(Return(true));

    braft::SnapshotMeta meta;
    meta.set_last_included_index(100);
    meta.set_last_included_term(100);
    meta.add_peers("127.0.0.1:123:0");

    EXPECT_CALL(reader, load_meta(_))
        .WillOnce(DoAll(SetArgPointee<0>(meta), Return(0)));

    ASSERT_FALSE(node->IsLoading());
    EXPECT_EQ(0, node->on_snapshot_load(&reader));
    ASSERT_FALSE(node->IsLoading());
    EXPECT_EQ(100, node->LatestLoadSnapshotIndex());

    // load snapshot doesn't change epoch
    std::vector<Peer> peers;
    node->ListPeers(&peers);
    EXPECT_EQ(3, peers.size());
    auto epochBefore = node->GetConfEpoch();

    braft::Configuration conf;
    for (int i = 0; i < meta.peers_size(); ++i) {
        conf.add_peer(meta.peers(i));
    }

    node->on_configuration_committed(conf, meta.last_included_index());
    peers.clear();
    node->ListPeers(&peers);
    EXPECT_EQ(1, peers.size());
    EXPECT_EQ(epochBefore, node->GetConfEpoch());
    node->SetMetaStore(nullptr);
}

TEST_F(CopysetNodeRaftSnapshotTest, SnapshotLoadTest_MetaStoreLoadFailed) {
    ASSERT_TRUE(CreateOneCopyset());

    auto* node = nodeManager_->GetCopysetNode(poolId_, copysetId_);
    ASSERT_NE(nullptr, node);

    auto mockMetaStore = mockMetaStore_.get();
    node->SetMetaStore(mockMetaStore_.release());

    MockSnapshotReader reader;
    EXPECT_CALL(reader, get_path())
        .WillRepeatedly(Return(dataPath_));
    EXPECT_CALL(*mockfs_, FileExists(_))
        .WillOnce(Return(false));
    EXPECT_CALL(*mockfs_, Open(_, _))
        .Times(0);
    EXPECT_CALL(*mockMetaStore, Clear())
        .Times(1);
    EXPECT_CALL(*mockMetaStore, Load(_))
        .WillOnce(Return(false));
    EXPECT_CALL(reader, load_meta(_))
        .Times(0);

    ASSERT_FALSE(node->IsLoading());
    EXPECT_NE(0, node->on_snapshot_load(&reader));
    ASSERT_FALSE(node->IsLoading());

    node->SetMetaStore(nullptr);
}

void RunOnSnapshotLoad(CopysetNode* node, MockSnapshotReader *reader,
                       uint32_t sleepSec) {
    sleep(sleepSec);
    ASSERT_FALSE(node->IsLoading());
    EXPECT_EQ(0, node->on_snapshot_load(reader));
    ASSERT_FALSE(node->IsLoading());
}

void RunGetPartitionInfoList(CopysetNode* node, uint32_t sleepSec,
                             bool expectedValue) {
    sleep(sleepSec);
    std::list<PartitionInfo> partitionInfoList;
    ASSERT_EQ(node->GetPartitionInfoList(&partitionInfoList), expectedValue);
}

// get partition list while copyset is loading
TEST_F(CopysetNodeRaftSnapshotTest, SnapshotLoadTest_MetaStoreLoadSuccess1) {
    ASSERT_TRUE(CreateOneCopyset());

    auto* node = nodeManager_->GetCopysetNode(poolId_, copysetId_);
    ASSERT_NE(nullptr, node);

    auto mockMetaStore = mockMetaStore_.get();
    node->SetMetaStore(mockMetaStore_.release());

    MockSnapshotReader reader;
    EXPECT_CALL(reader, get_path())
        .WillRepeatedly(Return(dataPath_));
    EXPECT_CALL(*mockfs_, FileExists(_))
        .WillOnce(Return(false));
    EXPECT_CALL(*mockfs_, Open(_, _))
        .Times(0);
    EXPECT_CALL(*mockMetaStore, Clear())
        .Times(1);
    EXPECT_CALL(*mockMetaStore, Load(_))
        .WillOnce(Invoke([](const std::string& pathname){
            sleep(3);
            return true;
        }));
    EXPECT_CALL(reader, load_meta(_))
        .Times(1);

    std::thread thread1(RunOnSnapshotLoad, node, &reader, 0);
    std::thread thread2(RunGetPartitionInfoList, node, 3, false);
    thread1.join();
    thread2.join();

    node->SetMetaStore(nullptr);
}

// get partition list after copyset is loading
TEST_F(CopysetNodeRaftSnapshotTest, SnapshotLoadTest_MetaStoreLoadSuccess2) {
    ASSERT_TRUE(CreateOneCopyset());

    auto* node = nodeManager_->GetCopysetNode(poolId_, copysetId_);
    ASSERT_NE(nullptr, node);

    auto mockMetaStore = mockMetaStore_.get();
    node->SetMetaStore(mockMetaStore_.release());

    MockSnapshotReader reader;
    EXPECT_CALL(reader, get_path())
        .WillRepeatedly(Return(dataPath_));
    EXPECT_CALL(*mockfs_, FileExists(_))
        .WillOnce(Return(false));
    EXPECT_CALL(*mockfs_, Open(_, _))
        .Times(0);
    EXPECT_CALL(*mockMetaStore, Clear())
        .Times(1);
    EXPECT_CALL(*mockMetaStore, Load(_))
        .WillOnce(Invoke([](const std::string& pathname){
            sleep(1);
            return true;
        }));
    EXPECT_CALL(reader, load_meta(_))
        .Times(1);
    EXPECT_CALL(*mockMetaStore, GetPartitionInfoList(_))
        .WillOnce(Return(true));

    std::thread thread1(RunOnSnapshotLoad, node, &reader, 0);
    std::thread thread2(RunGetPartitionInfoList, node, 3, true);
    thread1.join();
    thread2.join();

    node->SetMetaStore(nullptr);
}

// get partition list before copyset is loading
TEST_F(CopysetNodeRaftSnapshotTest, SnapshotLoadTest_MetaStoreLoadSuccess3) {
    ASSERT_TRUE(CreateOneCopyset());

    auto* node = nodeManager_->GetCopysetNode(poolId_, copysetId_);
    ASSERT_NE(nullptr, node);

    auto mockMetaStore = mockMetaStore_.get();
    node->SetMetaStore(mockMetaStore_.release());

    MockSnapshotReader reader;
    EXPECT_CALL(reader, get_path())
        .WillRepeatedly(Return(dataPath_));
    EXPECT_CALL(*mockfs_, FileExists(_))
        .WillOnce(Return(false));
    EXPECT_CALL(*mockfs_, Open(_, _))
        .Times(0);
    EXPECT_CALL(*mockMetaStore, Clear())
        .Times(1);
    EXPECT_CALL(*mockMetaStore, Load(_))
        .WillOnce(Invoke([](const std::string& pathname){
            sleep(1);
            return true;
        }));
    EXPECT_CALL(reader, load_meta(_))
        .Times(1);
    EXPECT_CALL(*mockMetaStore, GetPartitionInfoList(_))
        .WillOnce(Return(true));

    std::thread thread1(RunOnSnapshotLoad, node, &reader, 2);
    std::thread thread2(RunGetPartitionInfoList, node, 1, true);
    thread1.join();
    thread2.join();

    node->SetMetaStore(nullptr);
}
}  // namespace copyset
}  // namespace metaserver
}  // namespace curvefs
