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
 * Date: Friday Nov 26 16:55:39 CST 2021
 * Author: wuhanqing
 */

#include <brpc/server.h>
#include <gmock/gmock.h>
#include <google/protobuf/util/message_differencer.h>
#include <gtest/gtest.h>

#include "curvefs/src/metaserver/copyset/copyset_node.h"
#include "curvefs/test/metaserver/copyset/mock/mock_copyset_node_manager.h"
#include "curvefs/test/metaserver/copyset/mock/mock_copyset_service.h"
#include "curvefs/test/metaserver/copyset/mock/mock_raft_node.h"
#include "src/fs/ext4_filesystem_impl.h"

namespace curvefs {
namespace metaserver {
namespace copyset {

using ::google::protobuf::util::MessageDifferencer;
using ::testing::_;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::SaveArgPointee;

static std::shared_ptr<curve::fs::Ext4FileSystemImpl> localfs =
    curve::fs::Ext4FileSystemImpl::getInstance();
static const char* const kTestDataDir = "./CopysetNodeConfChangeTest";

class CopysetNodeConfChangeTest : public testing::Test {
 protected:
    void SetUp() override {
        unsigned int seed = reinterpret_cast<uint64_t>(this);
        poolId_ = rand_r(&seed) % 12345;
        copysetId_ = time(nullptr) % 54321;

        conf_.parse_from(
            "127.0.0.1:29960:0,127.0.0.1:29961:0,127.0.0.1:29962:0");

        options_.dataUri = "local://" + std::string(kTestDataDir);
        options_.ip = "127.0.0.1";
        options_.port = 29960;
        options_.localFileSystem = localfs.get();
        options_.storageOptions.type = "memory";

        ON_CALL(mockNodeManager_, IsLoadFinished())
            .WillByDefault(Return(true));
    }

    void TearDown() override {
        if (server_) {
            server_->Stop(0);
            server_->Join();
        }

        ASSERT_EQ(0, localfs->Delete(kTestDataDir));
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
    MockCopysetNodeManager mockNodeManager_;

    std::unique_ptr<brpc::Server> server_;
    std::unique_ptr<MockCopysetService> mockCopysetService_;
};

namespace {

struct FakeClosure : public braft::Closure {
    std::mutex mtx;
    std::condition_variable cond;
    bool runned{false};
    std::atomic<uint32_t> count{0};

    void Run() override {
        std::unique_lock<std::mutex> lk(mtx);
        runned = true;
        cond.notify_one();
    }

    void Wait() {
        std::unique_lock<std::mutex> lk(mtx);
        cond.wait(lk, [this]() { return runned; });
    }
};

}  // namespace

TEST_F(CopysetNodeConfChangeTest, GetConfTest_NoConfChange) {
    CopysetNode node(poolId_, copysetId_, conf_, &mockNodeManager_);
    EXPECT_TRUE(node.Init(options_));

    ConfigChangeType type;
    Configuration oldConf;
    Peer alterPeer;

    node.GetConfChange(&type, &alterPeer);
    EXPECT_EQ(ConfigChangeType::NONE, type);
    EXPECT_FALSE(alterPeer.has_address());
    EXPECT_FALSE(alterPeer.has_id());
}

TEST_F(CopysetNodeConfChangeTest, TestTransferLeader) {
    // target peer is invalid
    {
        CopysetNode node(poolId_, copysetId_, conf_, &mockNodeManager_);
        EXPECT_TRUE(node.Init(options_));

        Peer peer;
        peer.set_address("127.0.0.1:xxxx:0");

        auto st = node.TransferLeader(peer);
        EXPECT_EQ(EINVAL, st.error_code());
    }

    // no a leader
    {
        CopysetNode node(poolId_, copysetId_, conf_, &mockNodeManager_);
        EXPECT_TRUE(node.Init(options_));

        Peer peer;
        peer.set_address("127.0.0.1:29960:0");

        auto st = node.TransferLeader(peer);
        EXPECT_EQ(EPERM, st.error_code());
    }

    // transfer to self
    {
        CopysetNode node(poolId_, copysetId_, conf_, &mockNodeManager_);
        EXPECT_TRUE(node.Init(options_));

        auto* raftNode = new MockRaftNode();
        node.SetRaftNode(raftNode);
        node.on_leader_start(100);

        Peer peer;
        peer.set_address("127.0.0.1:29960:0");

        EXPECT_CALL(*raftNode, leader_id()).WillOnce(Invoke([peer]() {
            braft::PeerId peerId(peer.address());
            return peerId;
        }));

        auto st = node.TransferLeader(peer);
        EXPECT_EQ(0, st.error_code());
    }

    // raft node return ENOENT
    {
        CopysetNode node(poolId_, copysetId_, conf_, &mockNodeManager_);
        EXPECT_TRUE(node.Init(options_));

        auto* raftNode = new MockRaftNode();
        node.SetRaftNode(raftNode);
        node.on_leader_start(100);

        Peer peer;
        peer.set_address("127.0.0.1:29961:0");

        EXPECT_CALL(*raftNode, leader_id()).WillOnce(Invoke([peer]() {
            braft::PeerId peerId("127.0.0.1:29960:0");
            return peerId;
        }));

        EXPECT_CALL(*raftNode, transfer_leadership_to(_))
            .WillOnce(Return(ENOENT));

        auto st = node.TransferLeader(peer);
        EXPECT_EQ(ENOENT, st.error_code());
    }

    // raft node return OK
    {
        CopysetNode node(poolId_, copysetId_, conf_, &mockNodeManager_);
        EXPECT_TRUE(node.Init(options_));

        auto* raftNode = new MockRaftNode();
        node.SetRaftNode(raftNode);
        node.on_leader_start(100);

        Peer peer;
        peer.set_address("127.0.0.1:29961:0");

        EXPECT_CALL(*raftNode, leader_id()).WillOnce(Invoke([peer]() {
            braft::PeerId peerId("127.0.0.1:29960:0");
            return peerId;
        }));

        EXPECT_CALL(*raftNode, transfer_leadership_to(_)).WillOnce(Return(OK));

        auto st = node.TransferLeader(peer);
        EXPECT_EQ(OK, st.error_code());

        // get conf change
        ConfigChangeType type;
        Peer alterPeer;

        EXPECT_CALL(*raftNode, get_status(_))
            .WillOnce(Invoke([](braft::NodeStatus* status) {
                status->state = braft::State::STATE_TRANSFERRING;
                return;
            }));

        node.GetConfChange(&type, &alterPeer);
        EXPECT_EQ(ConfigChangeType::TRANSFER_LEADER, type);
        EXPECT_TRUE(MessageDifferencer::Equals(peer, alterPeer));
    }
}

TEST_F(CopysetNodeConfChangeTest, TestTransferLeader_CopysetsStillLoading) {
    CopysetNode node(poolId_, copysetId_, conf_, &mockNodeManager_);
    EXPECT_TRUE(node.Init(options_));

    EXPECT_CALL(mockNodeManager_, IsLoadFinished())
        .WillOnce(Return(false));

    Peer peer;
    peer.set_address("127.0.0.1:29960:0");

    auto st = node.TransferLeader(peer);
    EXPECT_EQ(EBUSY, st.error_code());
}

TEST_F(CopysetNodeConfChangeTest, TestAddPeer) {
    // add peer already exists
    {
        CopysetNode node(poolId_, copysetId_, conf_, &mockNodeManager_);
        EXPECT_TRUE(node.Init(options_));

        node.on_leader_start(100);

        Peer peer;
        peer.set_address("127.0.0.1:29960:0");

        FakeClosure done;
        node.AddPeer(peer, &done);
        done.Wait();
        EXPECT_EQ(EEXIST, done.status().error_code());
    }

    // add peer is invalid
    {
        CopysetNode node(poolId_, copysetId_, conf_, &mockNodeManager_);
        EXPECT_TRUE(node.Init(options_));

        node.on_leader_start(100);

        Peer peer;
        peer.set_address("127.0.0.1:xxxx:0");

        FakeClosure done;
        node.AddPeer(peer, &done);
        done.Wait();
        EXPECT_EQ(EINVAL, done.status().error_code());
    }

    // not a leader
    {
        CopysetNode node(poolId_, copysetId_, conf_, &mockNodeManager_);
        EXPECT_TRUE(node.Init(options_));

        Peer peer;
        peer.set_address("127.0.0.1:29963:0");

        FakeClosure done;
        node.AddPeer(peer, &done);
        done.Wait();
        EXPECT_EQ(EPERM, done.status().error_code());
    }

    // raft node return EBUSY
    {
        CopysetNode node(poolId_, copysetId_, conf_, &mockNodeManager_);
        EXPECT_TRUE(node.Init(options_));

        node.on_leader_start(100);

        auto* raftNode = new MockRaftNode();
        node.SetRaftNode(raftNode);

        Peer peer;
        peer.set_address("127.0.0.1:29963:0");

        EXPECT_CALL(*raftNode, add_peer(_, _))
            .WillOnce(
                Invoke([](const braft::PeerId& peer, braft::Closure* done) {
                    braft::AsyncClosureGuard guard(done);
                    done->status().set_error(EBUSY, "busy");
                }));

        FakeClosure done;
        node.AddPeer(peer, &done);
        done.Wait();
        EXPECT_EQ(EBUSY, done.status().error_code());
    }

    // raft node succeeded
    {
        CopysetNode node(poolId_, copysetId_, conf_, &mockNodeManager_);
        EXPECT_TRUE(node.Init(options_));

        node.on_leader_start(100);

        auto* raftNode = new MockRaftNode();
        node.SetRaftNode(raftNode);

        Peer peer;
        peer.set_address("127.0.0.1:29963:0");

        EXPECT_CALL(*raftNode, add_peer(_, _))
            .WillOnce(Invoke([](const braft::PeerId&, braft::Closure* done) {
                braft::AsyncClosureGuard guard(done);
                done->status() = butil::Status::OK();
            }));

        FakeClosure done;
        node.AddPeer(peer, &done);
        done.Wait();
        EXPECT_TRUE(done.status().ok());

        // get conf change
        ConfigChangeType type;
        Peer alterPeer;

        node.GetConfChange(&type, &alterPeer);
        EXPECT_EQ(ConfigChangeType::NONE, type);
    }
}

TEST_F(CopysetNodeConfChangeTest, TestRemovePeer) {
    // remove peer don't exists
    {
        CopysetNode node(poolId_, copysetId_, conf_, &mockNodeManager_);
        EXPECT_TRUE(node.Init(options_));

        node.on_leader_start(100);

        Peer peer;
        peer.set_address("127.0.0.1:29970:0");

        FakeClosure done;
        node.RemovePeer(peer, &done);
        done.Wait();
        EXPECT_EQ(ENOENT, done.status().error_code());
    }

    // remove peer is invalid
    {
        CopysetNode node(poolId_, copysetId_, conf_, &mockNodeManager_);
        EXPECT_TRUE(node.Init(options_));

        node.on_leader_start(100);

        Peer peer;
        peer.set_address("127.0.0.1:xxx:0");

        FakeClosure done;
        node.RemovePeer(peer, &done);
        done.Wait();
        EXPECT_EQ(EINVAL, done.status().error_code());
    }

    // not a leader
    {
        CopysetNode node(poolId_, copysetId_, conf_, &mockNodeManager_);
        EXPECT_TRUE(node.Init(options_));

        Peer peer;
        peer.set_address("127.0.0.1:29962:0");

        FakeClosure done;
        node.RemovePeer(peer, &done);
        done.Wait();
        EXPECT_EQ(EPERM, done.status().error_code());
    }

    // raft node return EBUSY
    {
        CopysetNode node(poolId_, copysetId_, conf_, &mockNodeManager_);
        EXPECT_TRUE(node.Init(options_));

        node.on_leader_start(100);

        auto* raftNode = new MockRaftNode();
        node.SetRaftNode(raftNode);

        Peer peer;
        peer.set_address("127.0.0.1:29962:0");

        EXPECT_CALL(*raftNode, remove_peer(_, _))
            .WillOnce(
                Invoke([](const braft::PeerId& peer, braft::Closure* done) {
                    braft::AsyncClosureGuard guard(done);
                    done->status().set_error(EBUSY, "busy");
                }));

        FakeClosure done;
        node.RemovePeer(peer, &done);
        done.Wait();
        LOG(INFO) << "done: " << &done
                  << ", status: " << done.status().error_str();
        EXPECT_EQ(EBUSY, done.status().error_code());
    }

    // raft node succeeded
    {
        CopysetNode node(poolId_, copysetId_, conf_, &mockNodeManager_);
        EXPECT_TRUE(node.Init(options_));

        node.on_leader_start(100);

        auto* raftNode = new MockRaftNode();
        node.SetRaftNode(raftNode);

        Peer peer;
        peer.set_address("127.0.0.1:29962:0");

        EXPECT_CALL(*raftNode, remove_peer(_, _))
            .WillOnce(
                Invoke([](const braft::PeerId& peer, braft::Closure* done) {
                    braft::AsyncClosureGuard guard(done);
                    done->status() = butil::Status::OK();
                }));

        FakeClosure done;
        node.RemovePeer(peer, &done);
        done.Wait();
        EXPECT_TRUE(done.status().ok());

        // get conf change
        ConfigChangeType type;
        Peer alterPeer;

        node.GetConfChange(&type, &alterPeer);
        EXPECT_EQ(ConfigChangeType::NONE, type);
    }
}

TEST_F(CopysetNodeConfChangeTest, TestChangePeer) {
    // not a leader
    {
        CopysetNode node(poolId_, copysetId_, conf_, &mockNodeManager_);
        EXPECT_TRUE(node.Init(options_));

        Peer peer;
        std::vector<Peer> newPeers;

        peer.set_address("127.0.0.1:29961:0");
        newPeers.emplace_back(peer);
        peer.set_address("127.0.0.1:29962:0");
        newPeers.emplace_back(peer);
        peer.set_address("127.0.0.1:29963:0");
        newPeers.emplace_back(peer);

        FakeClosure done;
        node.ChangePeers(newPeers, &done);
        done.Wait();
        EXPECT_EQ(EPERM, done.status().error_code());
    }

    // adding or removing more than one peers
    {
        CopysetNode node(poolId_, copysetId_, conf_, &mockNodeManager_);
        EXPECT_TRUE(node.Init(options_));

        node.on_leader_start(100);

        Peer peer;
        std::vector<Peer> newPeers;

        peer.set_address("127.0.0.1:29962:0");
        newPeers.emplace_back(peer);
        peer.set_address("127.0.0.1:29963:0");
        newPeers.emplace_back(peer);
        peer.set_address("127.0.0.1:29964:0");
        newPeers.emplace_back(peer);

        FakeClosure done;
        node.ChangePeers(newPeers, &done);
        done.Wait();
        EXPECT_EQ(EPERM, done.status().error_code());
    }

    // raft node return EBUSY
    {
        CopysetNode node(poolId_, copysetId_, conf_, &mockNodeManager_);
        EXPECT_TRUE(node.Init(options_));

        node.on_leader_start(100);

        auto* raftNode = new MockRaftNode();
        node.SetRaftNode(raftNode);

        Peer peer;
        std::vector<Peer> newPeers;

        peer.set_address("127.0.0.1:29961:0");
        newPeers.emplace_back(peer);
        peer.set_address("127.0.0.1:29962:0");
        newPeers.emplace_back(peer);
        peer.set_address("127.0.0.1:29963:0");
        newPeers.emplace_back(peer);

        EXPECT_CALL(*raftNode, change_peers(_, _))
            .WillOnce(Invoke([](const braft::Configuration& new_peers,
                                braft::Closure* done) {
                braft::AsyncClosureGuard guard(done);
                done->status().set_error(EBUSY, "busy");
            }));

        FakeClosure done;
        node.ChangePeers(newPeers, &done);
        done.Wait();
        EXPECT_EQ(EBUSY, done.status().error_code());
    }

    // raft node succeeded
    {
        CopysetNode node(poolId_, copysetId_, conf_, &mockNodeManager_);
        EXPECT_TRUE(node.Init(options_));

        node.on_leader_start(100);

        auto* raftNode = new MockRaftNode();
        node.SetRaftNode(raftNode);

        Peer peer;
        std::vector<Peer> newPeers;

        peer.set_address("127.0.0.1:29961:0");
        newPeers.emplace_back(peer);
        peer.set_address("127.0.0.1:29962:0");
        newPeers.emplace_back(peer);
        peer.set_address("127.0.0.1:29963:0");
        newPeers.emplace_back(peer);

        EXPECT_CALL(*raftNode, change_peers(_, _))
            .WillOnce(Invoke([](const braft::Configuration& new_peers,
                                braft::Closure* done) {
                braft::AsyncClosureGuard guard(done);
                done->status() = butil::Status::OK();
            }));

        FakeClosure done;
        node.ChangePeers(newPeers, &done);
        done.Wait();
        EXPECT_TRUE(done.status().ok());

        // get conf change
        ConfigChangeType type;
        Peer alterPeer;

        node.GetConfChange(&type, &alterPeer);
        EXPECT_EQ(ConfigChangeType::NONE, type);
    }
}

TEST_F(CopysetNodeConfChangeTest, RejectConfChangeIfPreviousNotComplete) {
    CopysetNode node(poolId_, copysetId_, conf_, &mockNodeManager_);
    EXPECT_TRUE(node.Init(options_));

    node.on_leader_start(100);

    auto* raftNode = new MockRaftNode();
    node.SetRaftNode(raftNode);

    // issue add peer
    Peer peerToAdd;
    peerToAdd.set_address("127.0.0.1:29963:0");

    braft::Closure* addPeerDone;
    EXPECT_CALL(*raftNode, add_peer(_, _))
        .WillOnce(
            Invoke([&addPeerDone](const braft::PeerId&, braft::Closure* done) {
                addPeerDone = done;
            }));

    FakeClosure onAddPeerDone;
    node.AddPeer(peerToAdd, &onAddPeerDone);

    // issue remove peer
    Peer peerToRemove;
    peerToRemove.set_address("127.0.0.1:29960:0");

    EXPECT_CALL(*raftNode, remove_peer(_, _)).Times(0);

    FakeClosure onRemovePeerDone;
    node.RemovePeer(peerToRemove, &onRemovePeerDone);
    onRemovePeerDone.Wait();
    EXPECT_EQ(EBUSY, onRemovePeerDone.status().error_code());

    // let add peer complete
    addPeerDone->status() = butil::Status::OK();
    addPeerDone->Run();
}

TEST_F(CopysetNodeConfChangeTest, UpdateTransferLeaderStateWhenGetConfChange) {
    // issue a transfer leader request
    CopysetNode node(poolId_, copysetId_, conf_, &mockNodeManager_);
    EXPECT_TRUE(node.Init(options_));

    auto* raftNode = new MockRaftNode();
    node.SetRaftNode(raftNode);
    node.on_leader_start(100);

    Peer peer;
    peer.set_address("127.0.0.1:29961:0");

    EXPECT_CALL(*raftNode, leader_id()).WillOnce(Invoke([]() {
        braft::PeerId peerId("127.0.0.1:29960:0");
        return peerId;
    }));

    EXPECT_CALL(*raftNode, transfer_leadership_to(_)).WillOnce(Return(OK));

    auto st = node.TransferLeader(peer);
    EXPECT_EQ(OK, st.error_code());

    // get conf change
    ConfigChangeType type;
    Peer alterPeer;

    EXPECT_CALL(*raftNode, get_status(_))
        .WillOnce(Invoke([](braft::NodeStatus* status) {
            status->state = braft::State::STATE_FOLLOWER;
            return;
        }));

    node.GetConfChange(&type, &alterPeer);
    EXPECT_EQ(ConfigChangeType::NONE, type);

    // later get conf change won't call raft node get_status
    node.GetConfChange(&type, &alterPeer);
    EXPECT_EQ(ConfigChangeType::NONE, type);
}

}  // namespace copyset
}  // namespace metaserver
}  // namespace curvefs
