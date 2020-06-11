/*
 *  Copyright (c) 2020 NetEase Inc.
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
 * Created Date: 18-11-14
 * Author: wudemiao
 */

#include <gtest/gtest.h>
#include <unistd.h>
#include <brpc/server.h>
#include <gmock/gmock-more-actions.h>
#include <gmock/gmock-generated-function-mockers.h>

#include <memory>
#include <cstdio>
#include <vector>
#include <string>
#include <cstdlib>

#include "test/fs/mock_local_filesystem.h"
#include "src/chunkserver/copyset_node_manager.h"
#include "src/chunkserver/copyset_node.h"
#include "test/chunkserver/fake_datastore.h"
#include "test/chunkserver/mock_node.h"
#include "src/chunkserver/conf_epoch_file.h"
#include "proto/heartbeat.pb.h"
#include "src/chunkserver/raftsnapshot/curve_snapshot_attachment.h"
#include "test/chunkserver/mock_curve_filesystem_adaptor.h"

namespace curve {
namespace chunkserver {

using ::testing::_;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::AnyNumber;
using ::testing::DoAll;
using ::testing::SetArgPointee;
using ::testing::SetArgReferee;
using ::testing::InSequence;
using ::testing::AtLeast;
using ::testing::SaveArgPointee;

using curve::fs::MockLocalFileSystem;
using curve::fs::FileSystemType;
using curve::fs::MockLocalFileSystem;

const char copysetUri[] = "local://./copyset_node_test";
const int port = 9044;

class FakeSnapshotReader : public braft::SnapshotReader {
 public:
    std::string get_path() {
        /* 返回一个不存在的 path */
        return std::string("/1002093939/temp/238408034");
    }
    void list_files(std::vector<std::string> *files) {
        return;
    }
    int load_meta(braft::SnapshotMeta *meta) {
        return 1;
    }
    std::string generate_uri_for_copy() {
        return std::string("");
    }
};

class FakeSnapshotWriter : public braft::SnapshotWriter {
 public:
    std::string get_path() {
        /* 返回一个不存在的 path */
        return std::string(".");
    }
    void list_files(std::vector<std::string> *files) {
        return;
    }
    virtual int save_meta(const braft::SnapshotMeta &meta) {
        return 0;
    }

    virtual int add_file(const std::string &filename) {
        return 0;
    }

    virtual int add_file(const std::string &filename,
                         const ::google::protobuf::Message *file_meta) {
        return 0;
    }

    virtual int remove_file(const std::string &filename) {
        return 0;
    }
};

class FakeClosure : public braft::Closure {
 public:
    void Run() {
        std::cerr << "FakeClosure run" << std::endl;
    }
};

class CopysetNodeTest : public ::testing::Test {
 protected:
    void SetUp() {
        defaultOptions_.ip = "127.0.0.1";
        defaultOptions_.port = port;
        defaultOptions_.electionTimeoutMs = 1000;
        defaultOptions_.snapshotIntervalS = 30;
        defaultOptions_.catchupMargin = 50;
        defaultOptions_.chunkDataUri = copysetUri;
        defaultOptions_.chunkSnapshotUri = copysetUri;
        defaultOptions_.logUri = copysetUri;
        defaultOptions_.raftMetaUri = copysetUri;
        defaultOptions_.raftSnapshotUri = copysetUri;
        defaultOptions_.loadConcurrency = 5;
        defaultOptions_.checkRetryTimes = 3;
        defaultOptions_.finishLoadMargin = 1000;

        defaultOptions_.concurrentapply = &concurrentModule_;
        defaultOptions_.concurrentapply->Init(2, 1);
        std::shared_ptr<LocalFileSystem> fs =
            LocalFsFactory::CreateFs(FileSystemType::EXT4, "");
        ASSERT_TRUE(nullptr != fs);
        defaultOptions_.localFileSystem = fs;
        defaultOptions_.chunkfilePool =
            std::make_shared<ChunkfilePool>(fs);
        defaultOptions_.trash = std::make_shared<Trash>();
    }

    void TearDown() {
        ::system("rm -rf copyset_node_test");
    }

 protected:
    CopysetNodeOptions  defaultOptions_;
    ConcurrentApplyModule concurrentModule_;
};

TEST_F(CopysetNodeTest, error_test) {
    std::shared_ptr<LocalFileSystem>
        fs(LocalFsFactory::CreateFs(FileSystemType::EXT4, ""));    //NOLINT
    const uint32_t kMaxChunkSize = 16 * 1024 * 1024;
    std::string rmCmd("rm -f ");
    rmCmd += kCurveConfEpochFilename;

    // on_snapshot_save: List failed
    {
        LogicPoolID logicPoolID = 123;
        CopysetID copysetID = 1345;
        Configuration conf;
        std::vector<std::string> files;
        files.push_back("test-1.txt");
        files.push_back("test-2.txt");

        char *json = "{\"logicPoolId\":123,\"copysetId\":1345,\"epoch\":0,\"checksum\":774340440}";  // NOLINT
        std::string jsonStr(json);

        CopysetNode copysetNode(logicPoolID, copysetID, conf);
        ASSERT_EQ(0, copysetNode.Init(defaultOptions_));
        FakeClosure closure;
        FakeSnapshotWriter writer;
        std::shared_ptr<MockLocalFileSystem>
            mockfs = std::make_shared<MockLocalFileSystem>();
        std::unique_ptr<ConfEpochFile>
            epochFile = std::make_unique<ConfEpochFile>(mockfs);

        copysetNode.SetLocalFileSystem(mockfs);
        copysetNode.SetConfEpochFile(std::move(epochFile));
        EXPECT_CALL(*mockfs, Open(_, _)).Times(1).WillOnce(Return(10));
        EXPECT_CALL(*mockfs, Write(_, _, _, _)).Times(1)
            .WillOnce(Return(jsonStr.size()));
        EXPECT_CALL(*mockfs, Fsync(_)).Times(1).WillOnce(Return(0));
        EXPECT_CALL(*mockfs, Close(_)).Times(1).WillOnce(Return(0));
        EXPECT_CALL(*mockfs, List(_, _)).Times(1).WillOnce(Return(-1));

        copysetNode.on_snapshot_save(&writer, &closure);
        LOG(INFO) << closure.status().error_cstr();
    }

    // on_snapshot_save: save conf open failed
    {
        LogicPoolID logicPoolID = 123;
        CopysetID copysetID = 1345;
        Configuration conf;
        std::vector<std::string> files;
        files.push_back("test-1.txt");
        files.push_back("test-2.txt");

        CopysetNode copysetNode(logicPoolID, copysetID, conf);
        ASSERT_EQ(0, copysetNode.Init(defaultOptions_));
        FakeClosure closure;
        FakeSnapshotWriter writer;
        std::shared_ptr<MockLocalFileSystem>
            mockfs = std::make_shared<MockLocalFileSystem>();
        std::unique_ptr<ConfEpochFile>
            epochFile = std::make_unique<ConfEpochFile>(mockfs);

        copysetNode.SetLocalFileSystem(mockfs);
        copysetNode.SetConfEpochFile(std::move(epochFile));
        EXPECT_CALL(*mockfs, Open(_, _)).Times(1).WillOnce(Return(-1));

        copysetNode.on_snapshot_save(&writer, &closure);
        LOG(INFO) << closure.status().error_cstr();
    }
    // on_snapshot_save: success
    {
        LogicPoolID logicPoolID = 123;
        CopysetID copysetID = 1345;
        Configuration conf;
        std::vector<std::string> files;
        files.push_back("test-1.txt");
        files.push_back("test-2.txt");

        char *json = "{\"logicPoolId\":123,\"copysetId\":1345,\"epoch\":0,\"checksum\":774340440}";  // NOLINT
        std::string jsonStr(json);

        CopysetNode copysetNode(logicPoolID, copysetID, conf);
        ASSERT_EQ(0, copysetNode.Init(defaultOptions_));
        FakeClosure closure;
        FakeSnapshotWriter writer;
        std::shared_ptr<MockLocalFileSystem>
            mockfs = std::make_shared<MockLocalFileSystem>();
        std::unique_ptr<ConfEpochFile>
            epochFile = std::make_unique<ConfEpochFile>(mockfs);

        copysetNode.SetLocalFileSystem(mockfs);
        copysetNode.SetConfEpochFile(std::move(epochFile));
        EXPECT_CALL(*mockfs, Open(_, _)).Times(1).WillOnce(Return(10));
        EXPECT_CALL(*mockfs, Write(_, _, _, _)).Times(1)
            .WillOnce(Return(jsonStr.size()));
        EXPECT_CALL(*mockfs, Fsync(_)).Times(1).WillOnce(Return(0));
        EXPECT_CALL(*mockfs, Close(_)).Times(1).WillOnce(Return(0));
        EXPECT_CALL(*mockfs, List(_, _)).Times(1)
            .WillOnce(DoAll(SetArgPointee<1>(files), Return(0)));

        copysetNode.on_snapshot_save(&writer, &closure);
    }

    // on_snapshot_load: Dir not exist, File not exist, data init success
    {
        LogicPoolID logicPoolID = 123;
        CopysetID copysetID = 1345;
        Configuration conf;
        CopysetNode copysetNode(logicPoolID, copysetID, conf);
        FakeClosure closure;
        FakeSnapshotReader reader;
        std::shared_ptr<MockLocalFileSystem>
            mockfs = std::make_shared<MockLocalFileSystem>();
        std::unique_ptr<ConfEpochFile>
            epochFile = std::make_unique<ConfEpochFile>(mockfs);
        copysetNode.SetLocalFileSystem(mockfs);
        copysetNode.SetConfEpochFile(std::move(epochFile));
        DataStoreOptions options;
        options.baseDir = "./test-temp";
        options.chunkSize = 16 * 1024 * 1024;
        options.pageSize = 4 * 1024;
        std::shared_ptr<FakeCSDataStore> dataStore =
            std::make_shared<FakeCSDataStore>(options, fs);
        copysetNode.SetCSDateStore(dataStore);

        EXPECT_CALL(*mockfs, DirExists(_)).Times(1).WillOnce(Return(false));
        EXPECT_CALL(*mockfs, FileExists(_)).Times(1).WillOnce(Return(false));

        ASSERT_EQ(0, copysetNode.on_snapshot_load(&reader));
        LOG(INFO) << "OK";
    }
    // on_snapshot_load: Dir not exist, File not exist, data init failed
    {
        LogicPoolID logicPoolID = 123;
        CopysetID copysetID = 1345;
        Configuration conf;
        CopysetNode copysetNode(logicPoolID, copysetID, conf);
        std::shared_ptr<MockLocalFileSystem>
            mockfs = std::make_shared<MockLocalFileSystem>();
        std::unique_ptr<ConfEpochFile>
            epochFile = std::make_unique<ConfEpochFile>(mockfs);
        FakeClosure closure;
        FakeSnapshotReader reader;
        copysetNode.SetLocalFileSystem(mockfs);
        copysetNode.SetConfEpochFile(std::move(epochFile));
        DataStoreOptions options;
        options.baseDir = "./test-temp";
        options.chunkSize = 16 * 1024 * 1024;
        options.pageSize = 4 * 1024;
        std::shared_ptr<FakeCSDataStore> dataStore =
            std::make_shared<FakeCSDataStore>(options, fs);
        copysetNode.SetCSDateStore(dataStore);
        dataStore->InjectError();

        EXPECT_CALL(*mockfs, DirExists(_)).Times(1).WillOnce(Return(false));
        EXPECT_CALL(*mockfs, FileExists(_)).Times(1).WillOnce(Return(false));

        ASSERT_EQ(-1, copysetNode.on_snapshot_load(&reader));
        LOG(INFO) << "OK";
    }
    // on_snapshot_load: Dir not exist, File exist, load conf.epoch failed
    {
        LogicPoolID logicPoolID = 123;
        CopysetID copysetID = 1345;
        Configuration conf;
        CopysetNode copysetNode(logicPoolID, copysetID, conf);
        FakeClosure closure;
        FakeSnapshotReader reader;
        std::shared_ptr<MockLocalFileSystem>
            mockfs = std::make_shared<MockLocalFileSystem>();
        std::unique_ptr<ConfEpochFile>
            epochFile = std::make_unique<ConfEpochFile>(mockfs);
        copysetNode.SetLocalFileSystem(mockfs);
        copysetNode.SetConfEpochFile(std::move(epochFile));

        EXPECT_CALL(*mockfs, DirExists(_)).Times(1).WillOnce(Return(false));
        EXPECT_CALL(*mockfs, FileExists(_)).Times(1).WillOnce(Return(true));
        EXPECT_CALL(*mockfs, Open(_, _)).Times(1).WillOnce(Return(-1));

        ASSERT_EQ(-1, copysetNode.on_snapshot_load(&reader));
    }

    // on_snapshot_load: Dir exist, delete failed
    {
        LogicPoolID logicPoolID = 123;
        CopysetID copysetID = 1345;
        Configuration conf;
        CopysetNode copysetNode(logicPoolID, copysetID, conf);
        FakeClosure closure;
        FakeSnapshotReader reader;
        std::shared_ptr<MockLocalFileSystem>
            mockfs = std::make_shared<MockLocalFileSystem>();
        std::unique_ptr<ConfEpochFile>
            epochFile = std::make_unique<ConfEpochFile>(mockfs);
        MockCurveFilesystemAdaptor* cfa =
            new MockCurveFilesystemAdaptor();
        auto sfs = new scoped_refptr<braft::FileSystemAdaptor>(cfa);
        copysetNode.SetSnapshotFileSystem(sfs);
        copysetNode.SetLocalFileSystem(mockfs);
        copysetNode.SetConfEpochFile(std::move(epochFile));
        EXPECT_CALL(*mockfs, DirExists(_)).Times(1).WillOnce(Return(true));
        EXPECT_CALL(*cfa,
                    delete_file(_, _)).Times(1).WillOnce(Return(false));

        ASSERT_EQ(-1, copysetNode.on_snapshot_load(&reader));
    }

    // on_snapshot_load: Dir exist, delete success, rename failed
    {
        LogicPoolID logicPoolID = 123;
        CopysetID copysetID = 1345;
        Configuration conf;
        CopysetNode copysetNode(logicPoolID, copysetID, conf);
        FakeClosure closure;
        FakeSnapshotReader reader;
        std::shared_ptr<MockLocalFileSystem>
            mockfs = std::make_shared<MockLocalFileSystem>();
        std::unique_ptr<ConfEpochFile>
            epochFile = std::make_unique<ConfEpochFile>(mockfs);
        defaultOptions_.localFileSystem = mockfs;
        MockCurveFilesystemAdaptor* cfa =
            new MockCurveFilesystemAdaptor();
        auto sfs = new scoped_refptr<braft::FileSystemAdaptor>(cfa);
        copysetNode.SetSnapshotFileSystem(sfs);
        copysetNode.SetLocalFileSystem(mockfs);
        copysetNode.SetConfEpochFile(std::move(epochFile));
        EXPECT_CALL(*mockfs, DirExists(_)).Times(1).WillOnce(Return(true));
        EXPECT_CALL(*cfa,
                    delete_file(_, _)).Times(1).WillOnce(Return(true));
        EXPECT_CALL(*cfa,
                    rename(_, _)).Times(1).WillOnce(Return(false));

        ASSERT_EQ(-1, copysetNode.on_snapshot_load(&reader));
    }

    // on_snapshot_load: Dir exist, rename success
    // file exist, open failed
    {
        LogicPoolID logicPoolID = 1;
        CopysetID copysetID = 1;
        Configuration conf;
        std::vector<std::string> files;
        files.push_back("test-1.txt");

        CopysetNode copysetNode(logicPoolID, copysetID, conf);
        FakeClosure closure;
        FakeSnapshotReader reader;
        std::shared_ptr<MockLocalFileSystem>
            mockfs = std::make_shared<MockLocalFileSystem>();
        std::unique_ptr<ConfEpochFile>
            epochFile = std::make_unique<ConfEpochFile>(mockfs);
        defaultOptions_.localFileSystem = mockfs;
        MockCurveFilesystemAdaptor* cfa =
            new MockCurveFilesystemAdaptor();
        auto sfs = new scoped_refptr<braft::FileSystemAdaptor>(cfa);
        copysetNode.SetSnapshotFileSystem(sfs);
        copysetNode.SetLocalFileSystem(mockfs);
        copysetNode.SetConfEpochFile(std::move(epochFile));
        EXPECT_CALL(*mockfs, DirExists(_)).Times(1)
            .WillOnce(Return(true));
        EXPECT_CALL(*cfa,
                    delete_file(_, _)).Times(1).WillOnce(Return(true));
        EXPECT_CALL(*cfa,
                    rename(_, _)).Times(1).WillOnce(Return(true));
        EXPECT_CALL(*mockfs, FileExists(_)).Times(1)
            .WillOnce(Return(true));
        EXPECT_CALL(*mockfs, Open(_, _)).Times(1)
            .WillOnce(Return(-1));

        ASSERT_EQ(-1, copysetNode.on_snapshot_load(&reader));
        LOG(INFO) << "OK";
    }
    /* on_error */
    {
        LogicPoolID logicPoolID = 123;
        CopysetID copysetID = 1345;
        Configuration conf;
        CopysetNode copysetNode(logicPoolID, copysetID, conf);
        braft::Error error;
        copysetNode.on_error(error);
        ASSERT_EQ(1, 1);
    }
    /* Fini, raftNode is null */
    {
        LogicPoolID logicPoolID = 123;
        CopysetID copysetID = 1345;
        Configuration conf;
        CopysetNode copysetNode(logicPoolID, copysetID, conf);
        copysetNode.Fini();
    }
    /* Fini, raftNode is not null */
    {
        LogicPoolID logicPoolID = 123;
        CopysetID copysetID = 1345;
        Configuration conf;
        std::vector<std::string> files;
        CopysetNode copysetNode(logicPoolID, copysetID, conf);
        defaultOptions_.localFileSystem = fs;
        ASSERT_EQ(0, copysetNode.Init(defaultOptions_));
        copysetNode.Fini();
    }
    /* Load/SaveConfEpoch */
    {
        LogicPoolID logicPoolID = 123;
        CopysetID copysetID = 1345;
        Configuration conf;
        CopysetNode copysetNode(logicPoolID, copysetID, conf);
        defaultOptions_.localFileSystem = fs;
        ASSERT_EQ(0, copysetNode.Init(defaultOptions_));
        ASSERT_EQ(0, copysetNode.SaveConfEpoch(kCurveConfEpochFilename));
        ASSERT_EQ(0, copysetNode.LoadConfEpoch(kCurveConfEpochFilename));
        ASSERT_EQ(0, copysetNode.GetConfEpoch());
        copysetNode.Fini();
        ::system(rmCmd.c_str());
    }
    /* load: ConfEpochFile load failed*/
    {
        LogicPoolID logicPoolID = 123;
        CopysetID copysetID = 1345;
        Configuration conf;
        CopysetNode copysetNode(logicPoolID, copysetID, conf);
        defaultOptions_.localFileSystem = fs;
        ASSERT_EQ(0, copysetNode.Init(defaultOptions_));
        ASSERT_NE(0, copysetNode.LoadConfEpoch(kCurveConfEpochFilename));
        copysetNode.Fini();
        ::system(rmCmd.c_str());
    }
    /* load: logic pool id 错误 */
    {
        LogicPoolID logicPoolID = 123;
        CopysetID copysetID = 1345;
        uint64_t epoch = 12;
        Configuration conf;
        CopysetNode copysetNode(logicPoolID, copysetID, conf);
        auto fs = LocalFsFactory::CreateFs(FileSystemType::EXT4, "");
        ConfEpochFile confEpochFile(fs);
        ASSERT_EQ(0,
                  confEpochFile.Save(kCurveConfEpochFilename,
                                     logicPoolID + 1,
                                     copysetID,
                                     epoch));
        defaultOptions_.localFileSystem = fs;
        ASSERT_EQ(0, copysetNode.Init(defaultOptions_));
        ASSERT_NE(0, copysetNode.LoadConfEpoch(kCurveConfEpochFilename));
        copysetNode.Fini();
        ::system(rmCmd.c_str());
    }
    /* load: copyset id 错误 */
    {
        LogicPoolID logicPoolID = 123;
        CopysetID copysetID = 1345;
        uint64_t epoch = 12;
        Configuration conf;
        CopysetNode copysetNode(logicPoolID, copysetID, conf);
        ASSERT_EQ(0, copysetNode.Init(defaultOptions_));
        auto fs = LocalFsFactory::CreateFs(FileSystemType::EXT4, "");
        ConfEpochFile confEpochFile(fs);
        ASSERT_EQ(0,
                  confEpochFile.Save(kCurveConfEpochFilename,
                                     logicPoolID,
                                     copysetID + 1,
                                     epoch));
        defaultOptions_.localFileSystem = fs;
        ASSERT_EQ(0, copysetNode.Init(defaultOptions_));
        ASSERT_NE(0, copysetNode.LoadConfEpoch(kCurveConfEpochFilename));
        copysetNode.Fini();
        ::system(rmCmd.c_str());
    }
}

TEST_F(CopysetNodeTest, get_conf_change) {
    std::shared_ptr<LocalFileSystem>
        fs(LocalFsFactory::CreateFs(FileSystemType::EXT4, ""));    //NOLINT
    const uint32_t kMaxChunkSize = 16 * 1024 * 1024;
    std::string rmCmd("rm -f ");
    rmCmd += kCurveConfEpochFilename;

    LogicPoolID logicPoolID = 1;
    CopysetID copysetID = 1;
    Configuration conf;
    Configuration conf1;
    Configuration conf2;
    PeerId peer("127.0.0.1:3200:0");
    PeerId peer1("127.0.0.1:3201:0");
    PeerId emptyPeer;
    conf.add_peer(peer);
    conf1.add_peer(peer);
    conf1.add_peer(peer1);
    conf2.add_peer(peer1);

    // 当前没有在做配置变更
    {
        CopysetNode copysetNode(logicPoolID, copysetID, conf);
        std::shared_ptr<MockNode> mockNode
            = std::make_shared<MockNode>(logicPoolID,
                                         copysetID);
        copysetNode.SetCopysetNode(mockNode);

        ConfigChangeType type;
        Configuration oldConf;
        Peer alterPeer;

        copysetNode.on_leader_start(8);

        EXPECT_CALL(*mockNode, conf_changes(_, _, _, _)).Times(1)
            .WillOnce(Return(false));
        EXPECT_EQ(0, copysetNode.GetConfChange(&type, &oldConf, &alterPeer));
        EXPECT_EQ(ConfigChangeType::NONE, type);
    }
    // 当前正在Add Peer
    {
        CopysetNode copysetNode(logicPoolID, copysetID, conf);
        std::shared_ptr<MockNode> mockNode
            = std::make_shared<MockNode>(logicPoolID,
                                         copysetID);
        copysetNode.SetCopysetNode(mockNode);

        ConfigChangeType type;
        Configuration oldConf;
        Peer alterPeer;

        copysetNode.on_leader_start(8);

        EXPECT_CALL(*mockNode, conf_changes(_, _, _, _)).Times(1)
            .WillOnce(DoAll(SetArgPointee<1>(conf),
                            Return(true)));
        EXPECT_EQ(0, copysetNode.GetConfChange(&type, &oldConf, &alterPeer));
        EXPECT_EQ(ConfigChangeType::ADD_PEER, type);
    }
    // 当前正在Remove Peer
    {
        CopysetNode copysetNode(logicPoolID, copysetID, conf);
        std::shared_ptr<MockNode> mockNode
            = std::make_shared<MockNode>(logicPoolID,
                                         copysetID);
        copysetNode.SetCopysetNode(mockNode);

        ConfigChangeType type;
        Configuration oldConf;
        Peer alterPeer;

        copysetNode.on_leader_start(8);

        EXPECT_CALL(*mockNode, conf_changes(_, _, _, _)).Times(1)
            .WillOnce(DoAll(SetArgPointee<2>(conf),
                            Return(true)));
        EXPECT_EQ(0, copysetNode.GetConfChange(&type, &oldConf, &alterPeer));
        EXPECT_EQ(ConfigChangeType::REMOVE_PEER, type);
    }
    // 当前正在Transfer leader
    {
        CopysetNode copysetNode(logicPoolID, copysetID, conf);
        std::shared_ptr<MockNode> mockNode
            = std::make_shared<MockNode>(logicPoolID,
                                         copysetID);
        copysetNode.SetCopysetNode(mockNode);

        ConfigChangeType type;
        Configuration oldConf;
        Peer alterPeer;

        copysetNode.on_leader_start(8);

        EXPECT_CALL(*mockNode, conf_changes(_, _, _, _)).Times(1)
            .WillOnce(DoAll(SetArgPointee<3>(peer),
                            Return(true)));
        EXPECT_EQ(0, copysetNode.GetConfChange(&type, &oldConf, &alterPeer));
        EXPECT_EQ(ConfigChangeType::TRANSFER_LEADER, type);
    }
    // 当前正在Change Peer
    {
        CopysetNode copysetNode(logicPoolID, copysetID, conf);
        std::shared_ptr<MockNode> mockNode
            = std::make_shared<MockNode>(logicPoolID,
                                         copysetID);
        copysetNode.SetCopysetNode(mockNode);

        ConfigChangeType type;
        Configuration oldConf;
        Peer alterPeer;

        copysetNode.on_leader_start(8);

        EXPECT_CALL(*mockNode, conf_changes(_, _, _, _)).Times(1)
            .WillOnce(DoAll(SetArgPointee<1>(conf),
                            SetArgPointee<2>(conf2),
                            Return(true)));
        EXPECT_EQ(0, copysetNode.GetConfChange(&type, &oldConf, &alterPeer));
        EXPECT_EQ(ConfigChangeType::CHANGE_PEER, type);
        EXPECT_EQ(alterPeer.address(), peer.to_string());
    }
    // 异常，braft::node配置变更返回true，但是没有正在进行配置变更的成员
    {
        CopysetNode copysetNode(logicPoolID, copysetID, conf);
        std::shared_ptr<MockNode> mockNode
            = std::make_shared<MockNode>(logicPoolID,
                                         copysetID);
        copysetNode.SetCopysetNode(mockNode);

        ConfigChangeType type;
        Configuration oldConf;
        Peer alterPeer;

        copysetNode.on_leader_start(8);

        EXPECT_CALL(*mockNode, conf_changes(_, _, _, _)).Times(1)
            .WillOnce(Return(true));
        EXPECT_EQ(-1, copysetNode.GetConfChange(&type, &oldConf, &alterPeer));
    }
    // 异常，正在add peer，但是是add多个成员
    {
        CopysetNode copysetNode(logicPoolID, copysetID, conf);
        std::shared_ptr<MockNode> mockNode
            = std::make_shared<MockNode>(logicPoolID,
                                         copysetID);
        copysetNode.SetCopysetNode(mockNode);

        ConfigChangeType type;
        Configuration oldConf;
        Peer alterPeer;

        copysetNode.on_leader_start(8);

        EXPECT_CALL(*mockNode, conf_changes(_, _, _, _)).Times(1)
            .WillOnce(DoAll(SetArgPointee<1>(conf1),
                            Return(true)));
        EXPECT_EQ(-1, copysetNode.GetConfChange(&type, &oldConf, &alterPeer));
    }
    // 异常，正在remove peer，但是是remove多个成员
    {
        CopysetNode copysetNode(logicPoolID, copysetID, conf);
        std::shared_ptr<MockNode> mockNode
            = std::make_shared<MockNode>(logicPoolID,
                                         copysetID);
        copysetNode.SetCopysetNode(mockNode);

        ConfigChangeType type;
        Configuration oldConf;
        Peer alterPeer;

        copysetNode.on_leader_start(8);

        EXPECT_CALL(*mockNode, conf_changes(_, _, _, _)).Times(1)
            .WillOnce(DoAll(SetArgPointee<2>(conf1),
                            Return(true)));
        EXPECT_EQ(-1, copysetNode.GetConfChange(&type, &oldConf, &alterPeer));
    }
    // 异常，正在transfer leader，但是transferee是空
    {
        CopysetNode copysetNode(logicPoolID, copysetID, conf);
        std::shared_ptr<MockNode> mockNode
            = std::make_shared<MockNode>(logicPoolID,
                                         copysetID);
        copysetNode.SetCopysetNode(mockNode);

        ConfigChangeType type;
        Configuration oldConf;
        Peer alterPeer;

        copysetNode.on_leader_start(8);

        EXPECT_CALL(*mockNode, conf_changes(_, _, _, _)).Times(1)
            .WillOnce(DoAll(SetArgPointee<3>(emptyPeer),
                            Return(true)));
        EXPECT_EQ(-1, copysetNode.GetConfChange(&type, &oldConf, &alterPeer));
    }
    // 当前正在Change Peer，但是change peer有多个成员
    {
        CopysetNode copysetNode(logicPoolID, copysetID, conf);
        std::shared_ptr<MockNode> mockNode
            = std::make_shared<MockNode>(logicPoolID,
                                         copysetID);
        copysetNode.SetCopysetNode(mockNode);

        ConfigChangeType type;
        Configuration oldConf;
        Peer alterPeer;

        copysetNode.on_leader_start(8);

        EXPECT_CALL(*mockNode, conf_changes(_, _, _, _)).Times(1)
            .WillOnce(DoAll(SetArgPointee<1>(conf),
                            SetArgPointee<2>(conf1),
                            Return(true)));
        EXPECT_EQ(-1, copysetNode.GetConfChange(&type, &oldConf, &alterPeer));

        EXPECT_CALL(*mockNode, conf_changes(_, _, _, _)).Times(1)
            .WillOnce(DoAll(SetArgPointee<1>(conf1),
                            SetArgPointee<2>(conf),
                            Return(true)));
        EXPECT_EQ(-1, copysetNode.GetConfChange(&type, &oldConf, &alterPeer));
    }
}

TEST_F(CopysetNodeTest, get_hash) {
    std::shared_ptr<LocalFileSystem>
        fs(LocalFsFactory::CreateFs(FileSystemType::EXT4, ""));    //NOLINT
    const uint32_t kMaxChunkSize = 16 * 1024 * 1024;
    std::string rmCmd("rm -f ");
    rmCmd += kCurveConfEpochFilename;

    LogicPoolID logicPoolID = 1 + 1;
    CopysetID copysetID = 1 + 1;
    Configuration conf;
    Configuration conf1;
    PeerId peer("127.0.0.1:3200:0");
    PeerId peer1("127.0.0.1:3201:0");
    PeerId emptyPeer;
    conf.add_peer(peer);
    conf1.add_peer(peer);
    conf1.add_peer(peer1);

    std::string hashValue = std::to_string(1355371765);
    // get hash
    {
        std::string hash;
        CopysetNode copysetNode(logicPoolID, copysetID, conf);

        ASSERT_EQ(0, copysetNode.Init(defaultOptions_));

        // 生成多个有数据的文件
        ::system("echo \"abcddddddddd333\" >"
                 "copyset_node_test/8589934594/data/test-2.txt");
        ::system("echo \"mmmmmmmm\" >"
                 "copyset_node_test/8589934594/data/test-4.txt");
        ::system("dd if=/dev/zero of="
                 "copyset_node_test/8589934594/data/test-3.txt bs=512 count=15");  // NOLINT
        ::system("echo \"eeeeeeeeeee\" > "
                 "copyset_node_test/8589934594/data/test-5.txt");

        ::system("touch copyset_node_test/8589934594/data/test-1.txt");
        ::system("echo \"wwwww\" > "
                 "copyset_node_test/8589934594/data/test-1.txt");

        // 获取hash
        ASSERT_EQ(0, copysetNode.GetHash(&hash));
        ASSERT_STREQ(hashValue.c_str(), hash.c_str());
        ::system("rm -fr copyset_node_test/8589934594");
    }

    {
        std::string hash;
        // 使用不同的copyset id，让目录不一样
        CopysetNode copysetNode(logicPoolID, copysetID + 1, conf);

        ASSERT_EQ(0, copysetNode.Init(defaultOptions_));

        // 生成多个有数据的文件，并且交换生成文件的顺序
        ::system("touch copyset_node_test/8589934595/data/test-1.txt");
        ::system("echo \"wwwww\" > "
                 "copyset_node_test/8589934595/data/test-1.txt");

        ::system("echo \"mmmmmmmm\" > "
                 "copyset_node_test/8589934595/data/test-4.txt");
        ::system("echo \"eeeeeeeeeee\" > "
                 "copyset_node_test/8589934595/data/test-5.txt");
        ::system("dd if=/dev/zero of="
                 "copyset_node_test/8589934595/data/test-3.txt bs=512 count=15");  // NOLINT
        ::system("echo \"abcddddddddd333\" > "
                 "copyset_node_test/8589934595/data/test-2.txt");

        // 获取hash
        ASSERT_EQ(0, copysetNode.GetHash(&hash));
        ASSERT_STREQ(hashValue.c_str(), hash.c_str());
        ::system("rm -fr copyset_node_test/8589934595");
    }

    // List failed
    {
        std::string hash;
        CopysetNode copysetNode(logicPoolID, copysetID, conf);

        std::shared_ptr<MockLocalFileSystem>
            mockfs = std::make_shared<MockLocalFileSystem>();
        copysetNode.SetLocalFileSystem(mockfs);

        std::vector<std::string> files;
        files.push_back("test-1.txt");


        EXPECT_CALL(*mockfs, List(_, _)).Times(1)
            .WillOnce(DoAll(SetArgPointee<1>(files), Return(0)));
        EXPECT_CALL(*mockfs, Open(_, _)).Times(1)
            .WillOnce(Return(-1));

        ASSERT_EQ(-1, copysetNode.GetHash(&hash));
    }

    // List success
    {
        std::string hash;
        CopysetNode copysetNode(logicPoolID, copysetID, conf);
        std::shared_ptr<MockLocalFileSystem>
            mockfs = std::make_shared<MockLocalFileSystem>();
        copysetNode.SetLocalFileSystem(mockfs);

        std::vector<std::string> files;

        EXPECT_CALL(*mockfs, List(_, _)).Times(1)
            .WillOnce(DoAll(SetArgPointee<1>(files), Return(0)));

        ASSERT_EQ(0, copysetNode.GetHash(&hash));
        ASSERT_EQ(hash, "0");
    }

    // List success,  open failed
    {
        std::string hash;
        CopysetNode copysetNode(logicPoolID, copysetID, conf);
        std::shared_ptr<MockLocalFileSystem>
            mockfs = std::make_shared<MockLocalFileSystem>();
        copysetNode.SetLocalFileSystem(mockfs);

        std::vector<std::string> files;
        files.push_back("test-1.txt");


        EXPECT_CALL(*mockfs, List(_, _)).Times(1)
            .WillOnce(DoAll(SetArgPointee<1>(files), Return(0)));
        EXPECT_CALL(*mockfs, Open(_, _)).Times(1)
            .WillOnce(Return(-1));

        ASSERT_EQ(-1, copysetNode.GetHash(&hash));
    }

    // List success,  open success，fstat failed
    {
        std::string hash;
        CopysetNode copysetNode(logicPoolID, copysetID, conf);
        std::shared_ptr<MockLocalFileSystem>
            mockfs = std::make_shared<MockLocalFileSystem>();
        copysetNode.SetLocalFileSystem(mockfs);

        std::vector<std::string> files;
        files.push_back("test-1.txt");


        EXPECT_CALL(*mockfs, List(_, _)).Times(1)
            .WillOnce(DoAll(SetArgPointee<1>(files), Return(0)));
        EXPECT_CALL(*mockfs, Open(_, _)).Times(1)
            .WillOnce(Return(3));
        EXPECT_CALL(*mockfs, Fstat(_, _)).Times(1)
            .WillOnce(Return(-1));

        ASSERT_EQ(-1, copysetNode.GetHash(&hash));
    }

    // List success,  open success, fstat success, read failed
    {
        std::string hash;
        struct stat fileInfo;
        fileInfo.st_size = 1024;
        CopysetNode copysetNode(logicPoolID, copysetID, conf);
        std::shared_ptr<MockLocalFileSystem>
            mockfs = std::make_shared<MockLocalFileSystem>();
        copysetNode.SetLocalFileSystem(mockfs);

        std::vector<std::string> files;
        files.push_back("test-1.txt");


        EXPECT_CALL(*mockfs, List(_, _)).Times(1)
            .WillOnce(DoAll(SetArgPointee<1>(files), Return(0)));
        EXPECT_CALL(*mockfs, Open(_, _)).Times(1)
            .WillOnce(Return(3));
        EXPECT_CALL(*mockfs, Fstat(_, _)).Times(1)
            .WillOnce(DoAll(SetArgPointee<1>(fileInfo), Return(0)));
        EXPECT_CALL(*mockfs, Read(_, _, _, _)).Times(1)
            .WillOnce(Return(-1));

        ASSERT_EQ(-1, copysetNode.GetHash(&hash));
    }

    // List success,  open success, fstat success, read success
    {
        char *buff = new (std::nothrow) char[1024];
        ::memset(buff, 'a', 1024);
        std::string hash;
        struct stat fileInfo;
        fileInfo.st_size = 1024;
        CopysetNode copysetNode(logicPoolID, copysetID, conf);
        std::shared_ptr<MockLocalFileSystem>
            mockfs = std::make_shared<MockLocalFileSystem>();
        copysetNode.SetLocalFileSystem(mockfs);

        std::vector<std::string> files;
        files.push_back("test-1.txt");


        EXPECT_CALL(*mockfs, List(_, _)).Times(1)
            .WillOnce(DoAll(SetArgPointee<1>(files), Return(0)));
        EXPECT_CALL(*mockfs, Open(_, _)).Times(1)
            .WillOnce(Return(3));
        EXPECT_CALL(*mockfs, Fstat(_, _)).Times(1)
            .WillOnce(DoAll(SetArgPointee<1>(fileInfo), Return(0)));
        EXPECT_CALL(*mockfs, Read(_, _, _, _)).Times(1)
            .WillOnce(DoAll(SetArgPointee<1>(*buff), Return(1024)));

        ASSERT_EQ(0, copysetNode.GetHash(&hash));
    }
}

TEST_F(CopysetNodeTest, get_leader_status) {
    LogicPoolID logicPoolID = 1;
    CopysetID copysetID = 1;
    Configuration conf;
    std::shared_ptr<MockNode> mockNode
            = std::make_shared<MockNode>(logicPoolID,
                                         copysetID);
    CopysetNode copysetNode(logicPoolID, copysetID, conf);
    copysetNode.SetCopysetNode(mockNode);

    // 当前peer不是leader，且当前无leader
    {
        NodeStatus status;
        EXPECT_CALL(*mockNode, get_status(_))
        .WillOnce(SetArgPointee<0>(status));
        NodeStatus leaderStatus;
        ASSERT_FALSE(copysetNode.GetLeaderStatus(&leaderStatus));
    }

    // 当前peer为leader
    {
        NodeStatus status;
        status.leader_id.parse("127.0.0.1:3200:0");
        status.peer_id = status.leader_id;
        status.committed_index = 6666;
        EXPECT_CALL(*mockNode, get_status(_))
        .WillOnce(SetArgPointee<0>(status));
        NodeStatus leaderStatus;
        ASSERT_TRUE(copysetNode.GetLeaderStatus(&leaderStatus));
        ASSERT_EQ(status.committed_index,
                  leaderStatus.committed_index);
    }

    // 存在leader，但不是当前peer
    {
        // 模拟启动chunkserver
        CopysetNodeManager* copysetNodeManager
            = &CopysetNodeManager::GetInstance();
        ASSERT_EQ(0, copysetNodeManager->Init(defaultOptions_));
        ASSERT_EQ(0, copysetNodeManager->Run());
        PeerId leader_peer("127.0.0.1:9044:0");
        brpc::Server server;
        ASSERT_EQ(0, copysetNodeManager->AddService(&server, leader_peer.addr));
        if (server.Start(port, NULL) != 0) {
            LOG(FATAL) << "Fail to start Server";
        }
        // 构造leader copyset
        ASSERT_TRUE(copysetNodeManager->CreateCopysetNode(logicPoolID,
                                                          copysetID,
                                                          conf));
        auto leaderNode = copysetNodeManager->GetCopysetNode(logicPoolID,
                                                             copysetID);
        ASSERT_TRUE(nullptr != leaderNode);
        // 设置预期值
        std::shared_ptr<MockNode> mockLeader
            = std::make_shared<MockNode>(logicPoolID,
                                         copysetID);
        leaderNode->SetCopysetNode(mockLeader);
        NodeStatus mockLeaderStatus;
        mockLeaderStatus.leader_id = leader_peer;
        mockLeaderStatus.peer_id = leader_peer;
        mockLeaderStatus.committed_index = 10000;
        mockLeaderStatus.known_applied_index = 6789;
        EXPECT_CALL(*mockLeader, get_status(_))
        .WillRepeatedly(SetArgPointee<0>(mockLeaderStatus));

        // 测试通过follower的node获取leader的committed index
        NodeStatus followerStatus;
        followerStatus.leader_id = leader_peer;
        followerStatus.peer_id.parse("127.0.0.1:3201:0");
        followerStatus.committed_index = 3456;
        followerStatus.known_applied_index = 3456;
        EXPECT_CALL(*mockNode, get_status(_))
        .WillOnce(SetArgPointee<0>(followerStatus));

        NodeStatus leaderStatus;
        ASSERT_TRUE(copysetNode.GetLeaderStatus(&leaderStatus));
        ASSERT_EQ(mockLeaderStatus.committed_index,
                  leaderStatus.committed_index);
        ASSERT_EQ(mockLeaderStatus.known_applied_index,
                  leaderStatus.known_applied_index);
    }
}

}  // namespace chunkserver
}  // namespace curve
