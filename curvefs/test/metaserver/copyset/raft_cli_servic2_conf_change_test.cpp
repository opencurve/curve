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
 * Date: Wed Dec 15 14:11:10 CST 2021
 * Author: wuhanqing
 */

#include <brpc/server.h>
#include <butil/endpoint.h>
#include <butil/status.h>
#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include <utility>

#include "curvefs/src/metaserver/copyset/raft_cli_service2.h"
#include "curvefs/test/metaserver/copyset/mock/mock_copyset_node.h"
#include "curvefs/test/metaserver/copyset/mock/mock_copyset_node_manager.h"

namespace curvefs {
namespace metaserver {
namespace copyset {

using ::testing::_;
using ::testing::DoAll;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::SaveArg;
using ::testing::SetArgumentPointee;

const char* kServerAddress = "127.0.0.1:29919";

class RaftCliService2ConfChangeTest : public testing::Test {
 protected:
    void SetUp() override {
        mockNodeManager_ = absl::make_unique<MockCopysetNodeManager>();
        service_ = absl::make_unique<RaftCliService2>(mockNodeManager_.get());
        server_ = absl::make_unique<brpc::Server>();

        ASSERT_EQ(0, server_->AddService(service_.get(),
                                         brpc::SERVER_DOESNT_OWN_SERVICE));
        ASSERT_EQ(0, server_->Start(kServerAddress, nullptr));

        brpc::ChannelOptions opts;
        opts.timeout_ms = 10 * 1000;
        opts.max_retry = 0;
        ASSERT_EQ(0, channel_.Init(kServerAddress, &opts));
    }

    void TearDown() override {
        if (server_) {
            server_->Stop(0);
            server_->Join();
        }
    }

 protected:
    std::unique_ptr<MockCopysetNodeManager> mockNodeManager_;
    std::unique_ptr<RaftCliService2> service_;
    std::unique_ptr<brpc::Server> server_;
    brpc::Channel channel_;
};

TEST_F(RaftCliService2ConfChangeTest, TransferLeaderTest) {
    std::string targetPeer = kServerAddress + std::string(":0");

    TransferLeaderRequest2 request;
    TransferLeaderResponse2 response;
    request.set_poolid(1);
    request.set_copysetid(1);
    auto* peer = request.mutable_transferee();
    peer->set_address(targetPeer);

    // pool or copyset is not exists
    {
        EXPECT_CALL(*mockNodeManager_, GetCopysetNode(_, _))
            .WillOnce(Return(nullptr));

        brpc::Controller cntl;

        CliService2_Stub stub(&channel_);
        stub.TransferLeader(&cntl, &request, &response, nullptr);
        ASSERT_TRUE(cntl.Failed());
        ASSERT_EQ(ENOENT, cntl.ErrorCode());
    }

    // copyset node return error
    {
        auto mockNode = absl::make_unique<MockCopysetNode>();
        EXPECT_CALL(*mockNodeManager_, GetCopysetNode(_, _))
            .WillOnce(Return(mockNode.get()));
        EXPECT_CALL(*mockNode, TransferLeader(_))
            .WillOnce(Return(butil::Status(EPERM, "%s", "dummy")));

        brpc::Controller cntl;

        CliService2_Stub stub(&channel_);
        stub.TransferLeader(&cntl, &request, &response, nullptr);
        ASSERT_TRUE(cntl.Failed());
        ASSERT_EQ(EPERM, cntl.ErrorCode());
    }
}

TEST_F(RaftCliService2ConfChangeTest, AddPeerTest) {
    std::string targetPeer = kServerAddress + std::string(":0");

    std::vector<Peer> peers;
    Peer peer;
    peer.set_address("127.0.0.1:29910:0");
    peers.push_back(peer);
    peer.set_address("127.0.0.1:29911:0");
    peers.push_back(peer);
    peer.set_address("127.0.0.1:29912:0");
    peers.push_back(peer);

    AddPeerRequest2 request;
    AddPeerResponse2 response;
    request.set_poolid(1);
    request.set_copysetid(1);
    auto* p = request.mutable_addpeer();
    p->set_address(targetPeer);

    // pool or copyset not found
    {
        EXPECT_CALL(*mockNodeManager_, GetCopysetNode(_, _))
            .WillOnce(Return(nullptr));

        brpc::Controller cntl;

        CliService2_Stub stub(&channel_);
        stub.AddPeer(&cntl, &request, &response, nullptr);
        ASSERT_TRUE(cntl.Failed());
        ASSERT_EQ(ENOENT, cntl.ErrorCode());
    }

    // add peer fail
    {
        auto mockNode = absl::make_unique<MockCopysetNode>();
        EXPECT_CALL(*mockNodeManager_, GetCopysetNode(_, _))
            .WillOnce(Return(mockNode.get()));

        EXPECT_CALL(*mockNode, ListPeers(_))
            .WillOnce(SetArgumentPointee<0>(peers));

        EXPECT_CALL(*mockNode, AddPeer(_, _))
            .WillOnce(Invoke([](const Peer& peer, braft::Closure* done) {
                done->status() = butil::Status(EPERM, "%s", "dummy");
                done->Run();
            }));

        brpc::Controller cntl;

        CliService2_Stub stub(&channel_);
        stub.AddPeer(&cntl, &request, &response, nullptr);
        ASSERT_TRUE(cntl.Failed());
        ASSERT_EQ(EPERM, cntl.ErrorCode());
    }

    // add peer success
    {
        auto mockNode = absl::make_unique<MockCopysetNode>();
        EXPECT_CALL(*mockNodeManager_, GetCopysetNode(_, _))
            .WillOnce(Return(mockNode.get()));

        EXPECT_CALL(*mockNode, ListPeers(_))
            .WillOnce(SetArgumentPointee<0>(peers));

        EXPECT_CALL(*mockNode, AddPeer(_, _))
            .WillOnce(Invoke([](const Peer& peer, braft::Closure* done) {
                done->status() = butil::Status::OK();
                done->Run();
            }));

        brpc::Controller cntl;

        CliService2_Stub stub(&channel_);
        stub.AddPeer(&cntl, &request, &response, nullptr);
        ASSERT_FALSE(cntl.Failed());
        ASSERT_EQ(3, response.oldpeers_size());
        ASSERT_EQ(4, response.newpeers_size());
    }

    // add peer already exists
    {
        AddPeerRequest2 request;
        AddPeerResponse2 response;
        request.set_poolid(1);
        request.set_copysetid(1);
        auto* peer = request.mutable_addpeer();
        peer->set_address(peers[0].address());

        auto mockNode = absl::make_unique<MockCopysetNode>();
        EXPECT_CALL(*mockNodeManager_, GetCopysetNode(_, _))
            .WillOnce(Return(mockNode.get()));

        EXPECT_CALL(*mockNode, ListPeers(_))
            .WillOnce(SetArgumentPointee<0>(peers));

        EXPECT_CALL(*mockNode, AddPeer(_, _))
            .WillOnce(Invoke([](const Peer& peer, braft::Closure* done) {
                done->status() = butil::Status::OK();
                done->Run();
            }));

        brpc::Controller cntl;

        CliService2_Stub stub(&channel_);
        stub.AddPeer(&cntl, &request, &response, nullptr);
        ASSERT_FALSE(cntl.Failed());
        ASSERT_EQ(3, response.oldpeers_size());
        ASSERT_EQ(3, response.newpeers_size());
    }
}

TEST_F(RaftCliService2ConfChangeTest, RemovePeerTest) {
    std::vector<Peer> peers;
    Peer peer;
    peer.set_address("127.0.0.1:29910:0");
    peers.push_back(peer);
    peer.set_address("127.0.0.1:29911:0");
    peers.push_back(peer);
    peer.set_address("127.0.0.1:29912:0");
    peers.push_back(peer);

    std::string targetPeer{peers[0].address()};

    RemovePeerRequest2 request;
    RemovePeerResponse2 response;
    request.set_poolid(1);
    request.set_copysetid(1);
    auto* p = request.mutable_removepeer();
    p->set_address(targetPeer);

    // pool or copyset not found
    {
        EXPECT_CALL(*mockNodeManager_, GetCopysetNode(_, _))
            .WillOnce(Return(nullptr));

        brpc::Controller cntl;

        CliService2_Stub stub(&channel_);
        stub.RemovePeer(&cntl, &request, &response, nullptr);
        ASSERT_TRUE(cntl.Failed());
        ASSERT_EQ(ENOENT, cntl.ErrorCode());
    }

    // remove peer fail
    {
        auto mockNode = absl::make_unique<MockCopysetNode>();
        EXPECT_CALL(*mockNodeManager_, GetCopysetNode(_, _))
            .WillOnce(Return(mockNode.get()));

        EXPECT_CALL(*mockNode, ListPeers(_))
            .WillOnce(SetArgumentPointee<0>(peers));

        EXPECT_CALL(*mockNode, RemovePeer(_, _))
            .WillOnce(Invoke([](const Peer& peer, braft::Closure* done) {
                done->status() = butil::Status(EPERM, "%s", "dummy");
                done->Run();
            }));

        brpc::Controller cntl;

        CliService2_Stub stub(&channel_);
        stub.RemovePeer(&cntl, &request, &response, nullptr);
        ASSERT_TRUE(cntl.Failed());
        ASSERT_EQ(EPERM, cntl.ErrorCode());
    }

    // remove peer success
    {
        auto mockNode = absl::make_unique<MockCopysetNode>();
        EXPECT_CALL(*mockNodeManager_, GetCopysetNode(_, _))
            .WillOnce(Return(mockNode.get()));

        EXPECT_CALL(*mockNode, ListPeers(_))
            .WillOnce(SetArgumentPointee<0>(peers));

        EXPECT_CALL(*mockNode, RemovePeer(_, _))
            .WillOnce(Invoke([](const Peer& peer, braft::Closure* done) {
                done->status() = butil::Status::OK();
                done->Run();
            }));

        brpc::Controller cntl;

        CliService2_Stub stub(&channel_);
        stub.RemovePeer(&cntl, &request, &response, nullptr);
        ASSERT_FALSE(cntl.Failed());
        ASSERT_EQ(3, response.oldpeers_size());
        ASSERT_EQ(2, response.newpeers_size());
    }

    // remove peer not exists
    {
        RemovePeerRequest2 request;
        RemovePeerResponse2 response;
        request.set_poolid(1);
        request.set_copysetid(1);
        auto* peer = request.mutable_removepeer();
        peer->set_address(kServerAddress + std::string(":0"));

        auto mockNode = absl::make_unique<MockCopysetNode>();
        EXPECT_CALL(*mockNodeManager_, GetCopysetNode(_, _))
            .WillOnce(Return(mockNode.get()));

        EXPECT_CALL(*mockNode, ListPeers(_))
            .WillOnce(SetArgumentPointee<0>(peers));

        EXPECT_CALL(*mockNode, RemovePeer(_, _))
            .WillOnce(Invoke([](const Peer& peer, braft::Closure* done) {
                done->status() = butil::Status::OK();
                done->Run();
            }));

        brpc::Controller cntl;

        CliService2_Stub stub(&channel_);
        stub.RemovePeer(&cntl, &request, &response, nullptr);
        ASSERT_FALSE(cntl.Failed());
        ASSERT_EQ(3, response.oldpeers_size());
        ASSERT_EQ(3, response.newpeers_size());
    }
}

TEST_F(RaftCliService2ConfChangeTest, ChangePeersTest) {
    std::vector<Peer> peers;
    Peer peer;
    peer.set_address("127.0.0.1:29910:0");
    peers.push_back(peer);
    peer.set_address("127.0.0.1:29911:0");
    peers.push_back(peer);
    peer.set_address("127.0.0.1:29912:0");
    peers.push_back(peer);

    std::vector<Peer> newPeers;
    peer.set_address("127.0.0.1:29911:0");
    newPeers.push_back(peer);
    peer.set_address("127.0.0.1:29912:0");
    newPeers.push_back(peer);
    peer.set_address("127.0.0.1:29913:0");
    newPeers.push_back(peer);

    ChangePeersRequest2 request;
    ChangePeersResponse2 response;
    request.set_poolid(1);
    request.set_copysetid(1);
    for (auto& peer : newPeers) {
        auto* p = request.add_newpeers();
        p->CopyFrom(peer);
    }

    // pool or copyset not found
    {
        EXPECT_CALL(*mockNodeManager_, GetCopysetNode(_, _))
            .WillOnce(Return(nullptr));

        brpc::Controller cntl;

        CliService2_Stub stub(&channel_);
        stub.ChangePeers(&cntl, &request, &response, nullptr);
        ASSERT_TRUE(cntl.Failed());
        ASSERT_EQ(ENOENT, cntl.ErrorCode());
    }

    // change peers fail
    {
        auto mockNode = absl::make_unique<MockCopysetNode>();
        EXPECT_CALL(*mockNodeManager_, GetCopysetNode(_, _))
            .WillOnce(Return(mockNode.get()));

        EXPECT_CALL(*mockNode, ListPeers(_))
            .WillOnce(SetArgumentPointee<0>(peers));

        EXPECT_CALL(*mockNode, ChangePeers(_, _))
            .WillOnce(Invoke(
                [](const std::vector<Peer>& peers, braft::Closure* done) {
                    done->status() = butil::Status(EPERM, "%s", "dummy");
                    done->Run();
                }));

        brpc::Controller cntl;

        CliService2_Stub stub(&channel_);
        stub.ChangePeers(&cntl, &request, &response, nullptr);
        ASSERT_TRUE(cntl.Failed());
        ASSERT_EQ(EPERM, cntl.ErrorCode());
    }

    // change peers success
    {
        auto mockNode = absl::make_unique<MockCopysetNode>();
        EXPECT_CALL(*mockNodeManager_, GetCopysetNode(_, _))
            .WillOnce(Return(mockNode.get()));

        EXPECT_CALL(*mockNode, ListPeers(_))
            .WillOnce(SetArgumentPointee<0>(peers));

        EXPECT_CALL(*mockNode, ChangePeers(_, _))
            .WillOnce(Invoke(
                [](const std::vector<Peer>& peers, braft::Closure* done) {
                    done->status() = butil::Status::OK();
                    done->Run();
                }));

        brpc::Controller cntl;

        CliService2_Stub stub(&channel_);
        stub.ChangePeers(&cntl, &request, &response, nullptr);
        ASSERT_FALSE(cntl.Failed());
        ASSERT_EQ(3, response.oldpeers_size());
        ASSERT_EQ(3, response.newpeers_size());
    }
}

}  // namespace copyset
}  // namespace metaserver
}  // namespace curvefs
