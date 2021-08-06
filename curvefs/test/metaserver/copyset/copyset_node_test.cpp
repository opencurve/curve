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
 * Date: Tue Aug 17 15:38:00 CST 2021
 * Author: wuhanqing
 */

#include "curvefs/src/metaserver/copyset/copyset_node.h"

#include <brpc/server.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <thread>

#include "curvefs/test/metaserver/copyset/mock/mock_copyset_service.h"
#include "curvefs/test/metaserver/copyset/mock/mock_raft_node.h"
#include "test/fs/mock_local_filesystem.h"

namespace curvefs {
namespace metaserver {
namespace copyset {

using ::testing::_;
using ::testing::DoAll;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::SetArgPointee;
using ::testing::SetArrayArgument;

using ::curve::fs::MockLocalFileSystem;

class CopysetNodeTest : public testing::Test {
 protected:
    void SetUp() override {
        unsigned int seed = reinterpret_cast<uint64_t>(this);
        poolId_ = rand_r(&seed) % 12345;
        copysetId_ = time(nullptr) % 54321;

        conf_.parse_from(
            "127.0.0.1:29940:0,127.0.0.1:29941:0,127.0.0.1:29942:0");

        options_.dataUri = "local:///data/";
        options_.ip = "127.0.0.1";
        options_.port = 29940;
    }

    void TearDown() override {
        if (server_) {
            server_->Stop(0);
            server_->Join();
        }
    }

    void StartMockCopysetService(butil::EndPoint listenAddr) {
        server_ = absl::make_unique<brpc::Server>();
        mockCopysetService_ = absl::make_unique<MockCopysetService>();

        ASSERT_EQ(0, server_->AddService(mockCopysetService_.get(),
                                         brpc::SERVER_DOESNT_OWN_SERVICE));

        ASSERT_EQ(0, server_->Start(listenAddr, nullptr));
    }

 protected:
    PoolId poolId_;
    CopysetId copysetId_;
    braft::Configuration conf_;
    CopysetNodeOptions options_;

    std::unique_ptr<brpc::Server> server_;
    std::unique_ptr<MockCopysetService> mockCopysetService_;
};

TEST_F(CopysetNodeTest, TestInit) {
    // parse data uri protocol failed
    {
        CopysetNode node(poolId_, copysetId_, conf_);
        CopysetNodeOptions options;

        EXPECT_EQ(false, node.Init(options));
    }

    // apply queue init failed
    {
        CopysetNodeOptions options;
        options.applyQueueOption.queueDepth = 0;
        options.applyQueueOption.workerCount = 0;
        options.dataUri = "local:///mnt/data";

        CopysetNode node(poolId_, copysetId_, conf_);
        EXPECT_EQ(false, node.Init(options));
    }
}

TEST_F(CopysetNodeTest, TestLeaderTerm) {
    CopysetNode node(poolId_, copysetId_, conf_);
    CopysetNodeOptions options;
    options.dataUri = "local:///data/";
    options.ip = "127.0.0.1";
    options.port = 29940;

    EXPECT_TRUE(node.Init(options));

    EXPECT_FALSE(node.IsLeaderTerm());
    EXPECT_EQ(-1, node.LeaderTerm());

    const int64_t term = 128;
    node.on_leader_start(term);
    EXPECT_TRUE(node.IsLeaderTerm());
    EXPECT_EQ(term, node.LeaderTerm());

    butil::Status status;
    status.set_error(EINVAL, "invalid");

    node.on_leader_stop(status);
    EXPECT_FALSE(node.IsLeaderTerm());
    EXPECT_EQ(-1, node.LeaderTerm());
}

TEST_F(CopysetNodeTest, OnErrorTest) {
    CopysetNode node(poolId_, copysetId_, conf_);
    braft::Error e;

    ASSERT_DEATH(node.on_error(e), "");
}

TEST_F(CopysetNodeTest, LoadConEpochFailed_EpochLoadFailed) {
    poolId_ = 1;
    copysetId_ = 1;
    CopysetNode node(poolId_, copysetId_, conf_);
    CopysetNodeOptions options;
    options.dataUri = "local:///mnt/data";

    std::unique_ptr<MockLocalFileSystem> mockfs(
        absl::make_unique<MockLocalFileSystem>());
    options.localFileSystem = mockfs.get();

    EXPECT_CALL(*mockfs, Open(_, _))
        .WillOnce(Return(-1));

    EXPECT_TRUE(node.Init(options));
    EXPECT_NE(0, node.LoadConfEpoch("test"));
}

TEST_F(CopysetNodeTest, LoadConEpochFailed_PoolIdIsNotIdentical) {
    poolId_ = 1;
    copysetId_ = 1;
    CopysetNode node(poolId_, copysetId_, conf_);
    CopysetNodeOptions options;
    options.dataUri = "local:///mnt/data";

    std::unique_ptr<MockLocalFileSystem> mockfs(
        absl::make_unique<MockLocalFileSystem>());
    options.localFileSystem = mockfs.get();

    EXPECT_CALL(*mockfs, Open(_, _)).
        WillOnce(Return(0));
    EXPECT_CALL(*mockfs, Close(_)).Times(1);
    const char* data =
        "{\"poolId\": 123, \"copysetId\": 1, \"epoch\": 3, \"checksum\": 3480649501}";  // NOLINT

    size_t datasize = strlen(data);
    EXPECT_CALL(*mockfs, Read(_, _, _, _))
        .WillOnce(DoAll(SetArrayArgument<1>(data, data + datasize),
                        Return(datasize)));

    EXPECT_TRUE(node.Init(options));
    EXPECT_NE(0, node.LoadConfEpoch("test"));
}

TEST_F(CopysetNodeTest, LoadConEpochFailed_CopysetIdIsNotIdentical) {
    poolId_ = 1;
    copysetId_ = 1;
    CopysetNode node(poolId_, copysetId_, conf_);
    CopysetNodeOptions options;
    options.dataUri = "local:///mnt/data";

    std::unique_ptr<MockLocalFileSystem> mockfs(
        absl::make_unique<MockLocalFileSystem>());
    options.localFileSystem = mockfs.get();

    EXPECT_CALL(*mockfs, Open(_, _))
        .WillOnce(Return(0));
    EXPECT_CALL(*mockfs, Close(_))
        .Times(1);
    const char* data =
        "{\"poolId\": 1, \"copysetId\": 431, \"epoch\": 3, \"checksum\": 2004834618}";  // NOLINT

    size_t datasize = strlen(data);
    EXPECT_CALL(*mockfs, Read(_, _, _, _))
        .WillOnce(DoAll(SetArrayArgument<1>(data, data + datasize),
                        Return(datasize)));

    EXPECT_TRUE(node.Init(options));
    EXPECT_NE(0, node.LoadConfEpoch("test"));
}

TEST_F(CopysetNodeTest, LoadConEpoch_Success) {
    poolId_ = 1;
    copysetId_ = 1;
    CopysetNode node(poolId_, copysetId_, conf_);
    CopysetNodeOptions options;
    options.dataUri = "local:///mnt/data";

    std::unique_ptr<MockLocalFileSystem> mockfs(
        absl::make_unique<MockLocalFileSystem>());
    options.localFileSystem = mockfs.get();

    EXPECT_CALL(*mockfs, Open(_, _))
        .WillOnce(Return(0));
    EXPECT_CALL(*mockfs, Close(_))
        .Times(1);
    const char* data =
        "{\"poolId\": 1, \"copysetId\": 1, \"epoch\": 3, \"checksum\": 3896751047}";  // NOLINT

    size_t datasize = strlen(data);
    EXPECT_CALL(*mockfs, Read(_, _, _, _))
        .WillOnce(DoAll(SetArrayArgument<1>(data, data + datasize),
                        Return(datasize)));

    EXPECT_TRUE(node.Init(options));
    EXPECT_EQ(0, node.LoadConfEpoch("test"));
    EXPECT_EQ(3, node.GetConfEpoch());
}

TEST_F(CopysetNodeTest, UpdateAppliedIndexTest) {
    CopysetNode node(poolId_, copysetId_, conf_);
    CopysetNodeOptions options;
    options.dataUri = "local:///data/";
    options.ip = "127.0.0.1";
    options.port = 29940;

    EXPECT_TRUE(node.Init(options));

    const uint64_t minAppliedIndex = 0;
    const uint64_t maxAppliedIndex = 1000;
    std::atomic<bool> running_(true);

    std::thread setMaxThread([&running_, &node, maxAppliedIndex]() {
        while (running_.load(std::memory_order_relaxed)) {
            node.UpdateAppliedIndex(maxAppliedIndex);
            std::this_thread::sleep_for(std::chrono::microseconds(100));
        }
    });

    std::thread setMinThread([&running_, &node, minAppliedIndex]() {
        while (running_.load(std::memory_order_relaxed)) {
            node.UpdateAppliedIndex(maxAppliedIndex);
            std::this_thread::sleep_for(std::chrono::microseconds(50));
        }
    });

    std::thread setRandomThread([&running_, &node, maxAppliedIndex]() {
        unsigned int seed = reinterpret_cast<uint64_t>(&node);
        while (running_.load(std::memory_order_relaxed)) {
            node.UpdateAppliedIndex(rand_r(&seed) % maxAppliedIndex);
            std::this_thread::sleep_for(std::chrono::microseconds(20));
        }
    });

    std::this_thread::sleep_for(std::chrono::seconds(10));
    running_.store(false, std::memory_order_relaxed);

    setMaxThread.join();
    setMinThread.join();
    setRandomThread.join();

    EXPECT_EQ(maxAppliedIndex, node.GetAppliedIndex());
}

TEST_F(CopysetNodeTest, ListPeersTest) {
    CopysetNode node(poolId_, copysetId_, conf_);
    CopysetNodeOptions options;
    options.dataUri = "local:///data/";
    options.ip = "127.0.0.1";
    options.port = 29940;

    EXPECT_TRUE(node.Init(options));

    std::vector<Peer> peers;
    node.ListPeers(&peers);
    EXPECT_EQ(peers[0].address(), "127.0.0.1:29940:0");
    EXPECT_EQ(peers[1].address(), "127.0.0.1:29941:0");
    EXPECT_EQ(peers[2].address(), "127.0.0.1:29942:0");
}

TEST_F(CopysetNodeTest, FetchLeaderStatusTest_LeaderIdIsEmpty) {
    CopysetNode node(poolId_, copysetId_, conf_);
    CopysetNodeOptions options;
    options.dataUri = "local:///data/";
    options.ip = "127.0.0.1";
    options.port = 29940;

    EXPECT_TRUE(node.Init(options));
    auto* mockRaftNode = new MockRaftNode();
    node.SetRaftNode(mockRaftNode);

    braft::NodeStatus status;
    status.leader_id.reset();
    EXPECT_CALL(*mockRaftNode, get_status(_))
        .WillOnce(SetArgPointee<0>(status));
    EXPECT_CALL(*mockRaftNode, shutdown(_))
        .Times(1);
    EXPECT_CALL(*mockRaftNode, join())
        .Times(1);

    braft::NodeStatus leaderStatus;
    EXPECT_FALSE(node.GetLeaderStatus(&leaderStatus));
}

TEST_F(CopysetNodeTest, FetchLeaderStatusTest_CurrentNodeIsLeader) {
    CopysetNode node(poolId_, copysetId_, conf_);
    CopysetNodeOptions options;
    options.dataUri = "local:///data/";
    options.ip = "127.0.0.1";
    options.port = 29940;

    EXPECT_TRUE(node.Init(options));
    auto* mockRaftNode = new MockRaftNode();
    node.SetRaftNode(mockRaftNode);

    braft::NodeStatus status;
    status.leader_id.parse("127.0.0.1:29940:0");
    status.peer_id.parse("127.0.0.1:29940:0");
    EXPECT_CALL(*mockRaftNode, get_status(_))
        .WillOnce(SetArgPointee<0>(status));
    EXPECT_CALL(*mockRaftNode, shutdown(_))
        .Times(1);
    EXPECT_CALL(*mockRaftNode, join())
        .Times(1);

    braft::NodeStatus leaderStatus;
    EXPECT_TRUE(node.GetLeaderStatus(&leaderStatus));
    EXPECT_THAT(status.peer_id.to_string(), leaderStatus.peer_id.to_string());
    EXPECT_THAT(status.leader_id.to_string(),
                leaderStatus.leader_id.to_string());
}

TEST_F(CopysetNodeTest,
       FetchLeaderStatusTest_CurrentNodeIsNotLeader_InitChannelToLeaderFailed) {
    CopysetNode node(poolId_, copysetId_, conf_);

    EXPECT_TRUE(node.Init(options_));
    auto* mockRaftNode = new MockRaftNode();
    node.SetRaftNode(mockRaftNode);

    braft::NodeStatus status;
    status.leader_id.parse("127.0.0.1:299400:0");
    status.peer_id.parse("127.0.0.1:29940:0");
    EXPECT_CALL(*mockRaftNode, get_status(_))
        .WillOnce(SetArgPointee<0>(status));
    EXPECT_CALL(*mockRaftNode, shutdown(_))
        .Times(1);
    EXPECT_CALL(*mockRaftNode, join())
        .Times(1);

    braft::NodeStatus leaderStatus;
    EXPECT_FALSE(node.GetLeaderStatus(&leaderStatus));
}

TEST_F(CopysetNodeTest,
       FetchLeaderStatusTest_CurrentNodeIsNotLeader_RpcToLeaderFailed) {
    CopysetNode node(poolId_, copysetId_, conf_);

    EXPECT_TRUE(node.Init(options_));
    auto* mockRaftNode = new MockRaftNode();
    node.SetRaftNode(mockRaftNode);

    braft::NodeStatus status;
    status.leader_id.parse("127.0.0.1:29941:0");  // leader don't start service
    status.peer_id.parse("127.0.0.1:29940:0");
    EXPECT_CALL(*mockRaftNode, get_status(_))
        .WillOnce(SetArgPointee<0>(status));
    EXPECT_CALL(*mockRaftNode, shutdown(_))
        .Times(1);
    EXPECT_CALL(*mockRaftNode, join())
        .Times(1);

    braft::NodeStatus leaderStatus;
    EXPECT_FALSE(node.GetLeaderStatus(&leaderStatus));
}

TEST_F(CopysetNodeTest,
       FetchLeaderStatusTest_CurrentNodeIsNotLeader_RpcResponseToLeaderFailed) {
    CopysetNode node(poolId_, copysetId_, conf_);

    EXPECT_TRUE(node.Init(options_));
    auto* mockRaftNode = new MockRaftNode();
    node.SetRaftNode(mockRaftNode);

    braft::NodeStatus status;
    status.leader_id.parse("127.0.0.1:29941:0");
    status.peer_id.parse("127.0.0.1:29940:0");
    EXPECT_CALL(*mockRaftNode, get_status(_))
        .WillOnce(SetArgPointee<0>(status));
    EXPECT_CALL(*mockRaftNode, shutdown(_))
        .Times(1);
    EXPECT_CALL(*mockRaftNode, join())
        .Times(1);

    StartMockCopysetService(status.leader_id.addr);
    EXPECT_CALL(*mockCopysetService_, GetCopysetStatus(_, _, _, _))
        .WillOnce(Invoke(
            [](::google::protobuf::RpcController* controller,
               const ::curvefs::metaserver::copyset::CopysetStatusRequest*
                   request,
               ::curvefs::metaserver::copyset::CopysetStatusResponse* response,
               ::google::protobuf::Closure* done) {
                response->set_status(
                    COPYSET_OP_STATUS::COPYSET_OP_STATUS_COPYSET_NOTEXIST);
                done->Run();
            }));

    braft::NodeStatus leaderStatus;
    EXPECT_FALSE(node.GetLeaderStatus(&leaderStatus));
}

TEST_F(
    CopysetNodeTest,
    FetchLeaderStatusTest_CurrentNodeIsNotLeader_RpcResponseHasNoCopystatus) {
    CopysetNode node(poolId_, copysetId_, conf_);

    EXPECT_TRUE(node.Init(options_));
    auto* mockRaftNode = new MockRaftNode();
    node.SetRaftNode(mockRaftNode);

    braft::NodeStatus status;
    status.leader_id.parse("127.0.0.1:29941:0");
    status.peer_id.parse("127.0.0.1:29940:0");
    EXPECT_CALL(*mockRaftNode, get_status(_))
        .WillOnce(SetArgPointee<0>(status));
    EXPECT_CALL(*mockRaftNode, shutdown(_))
        .Times(1);
    EXPECT_CALL(*mockRaftNode, join())
        .Times(1);

    StartMockCopysetService(status.leader_id.addr);
    EXPECT_CALL(*mockCopysetService_, GetCopysetStatus(_, _, _, _))
        .WillOnce(Invoke(
            [](::google::protobuf::RpcController* controller,
               const ::curvefs::metaserver::copyset::CopysetStatusRequest*
                   request,
               ::curvefs::metaserver::copyset::CopysetStatusResponse* response,
               ::google::protobuf::Closure* done) {
                response->set_status(
                    COPYSET_OP_STATUS::COPYSET_OP_STATUS_SUCCESS);
                done->Run();
            }));

    braft::NodeStatus leaderStatus;
    EXPECT_FALSE(node.GetLeaderStatus(&leaderStatus));
}

TEST_F(CopysetNodeTest,
       FetchLeaderStatusTest_CurrentNodeIsNotLeader_RpcResponseSuccess) {
    CopysetNode node(poolId_, copysetId_, conf_);

    EXPECT_TRUE(node.Init(options_));
    auto* mockRaftNode = new MockRaftNode();
    node.SetRaftNode(mockRaftNode);

    braft::NodeStatus status;
    status.leader_id.parse("127.0.0.1:29941:0");
    status.peer_id.parse("127.0.0.1:29940:0");
    EXPECT_CALL(*mockRaftNode, get_status(_))
        .WillOnce(SetArgPointee<0>(status));
    EXPECT_CALL(*mockRaftNode, shutdown(_))
        .Times(1);
    EXPECT_CALL(*mockRaftNode, join())
        .Times(1);

    StartMockCopysetService(status.leader_id.addr);
    EXPECT_CALL(*mockCopysetService_, GetCopysetStatus(_, _, _, _))
        .WillOnce(Invoke(
            [](::google::protobuf::RpcController* controller,
               const ::curvefs::metaserver::copyset::CopysetStatusRequest*
                   request,
               ::curvefs::metaserver::copyset::CopysetStatusResponse* response,
               ::google::protobuf::Closure* done) {
                response->set_status(
                    COPYSET_OP_STATUS::COPYSET_OP_STATUS_SUCCESS);

                auto* status = response->mutable_copysetstatus();
                status->set_state(braft::State::STATE_LEADER);
                status->mutable_peer()->set_address("127.0.0.1:29941:0");
                status->mutable_leader()->set_address("127.0.0.1:29941:0");
                status->set_readonly(false);
                status->set_term(1);
                status->set_committedindex(2);
                status->set_knownappliedindex(3);
                status->set_pendingindex(4);
                status->set_pendingqueuesize(5);
                status->set_applyingindex(6);
                status->set_firstindex(7);
                status->set_lastindex(8);
                status->set_diskindex(9);
                status->set_epoch(10);

                done->Run();
            }));

    braft::NodeStatus leaderStatus;
    EXPECT_TRUE(node.GetLeaderStatus(&leaderStatus));
    EXPECT_EQ(leaderStatus.state, braft::State::STATE_LEADER);
    EXPECT_EQ(leaderStatus.peer_id.to_string(), "127.0.0.1:29941:0");
    EXPECT_EQ(leaderStatus.leader_id.to_string(), "127.0.0.1:29941:0");
    EXPECT_EQ(leaderStatus.readonly, false);
    EXPECT_EQ(leaderStatus.term, 1);
    EXPECT_EQ(leaderStatus.committed_index, 2);
    EXPECT_EQ(leaderStatus.known_applied_index, 3);
    EXPECT_EQ(leaderStatus.pending_index, 4);
    EXPECT_EQ(leaderStatus.pending_queue_size, 5);
    EXPECT_EQ(leaderStatus.applying_index, 6);
    EXPECT_EQ(leaderStatus.first_index, 7);
    EXPECT_EQ(leaderStatus.last_index, 8);
    EXPECT_EQ(leaderStatus.disk_index, 9);
}

TEST_F(CopysetNodeTest, StartWithoutInitReturnFailed) {
    CopysetNode node(poolId_, copysetId_, conf_);

    EXPECT_FALSE(node.Start());
}

}  // namespace copyset
}  // namespace metaserver
}  // namespace curvefs
