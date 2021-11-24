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
 * Date: Wednesday Dec 01 19:13:53 CST 2021
 * Author: wuhanqing
 */

#include <brpc/server.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "curvefs/src/metaserver/heartbeat.h"
#include "curvefs/test/metaserver/copyset/mock/mock_copyset_node.h"
#include "curvefs/test/metaserver/copyset/mock/mock_copyset_node_manager.h"
#include "curvefs/test/metaserver/copyset/mock/mock_copyset_service.h"

namespace curvefs {
namespace metaserver {

using ::curvefs::metaserver::copyset::MockCopysetNode;
using ::curvefs::metaserver::copyset::MockCopysetNodeManager;
using ::curvefs::metaserver::copyset::MockCopysetService;
using ::testing::_;
using ::testing::DoAll;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::Test;

class HeartbeatTaskExecutorTest : public testing::Test {
 protected:
    void SetUp() override {
        butil::EndPoint ep;
        ASSERT_EQ(0, butil::str2endpoint(current_.c_str(), &ep));

        executor_.reset(new HeartbeatTaskExecutor(&mockCopysetMgr_, ep));
    }

    void TearDown() override {
        if (server_) {
            server_->Stop(0);
            server_->Join();
        }
    }

    void StartFakeCopysetService(const std::string& ipport) {
        service_.reset(new MockCopysetService());
        server_.reset(new brpc::Server());
        ASSERT_EQ(0, server_->AddService(service_.get(),
                                         brpc::SERVER_DOESNT_OWN_SERVICE));
        ASSERT_EQ(0, server_->Start(ipport.c_str(), nullptr));
    }

 protected:
    const std::string current_{"127.0.0.1:6701"};
    const std::string confChangeTarget_{"127.0.0.1:6702"};

    MockCopysetNodeManager mockCopysetMgr_;
    MockCopysetNode mockCopysetNode_;

    std::unique_ptr<HeartbeatTaskExecutor> executor_;
    std::unique_ptr<brpc::Server> server_;
    std::unique_ptr<MockCopysetService> service_;
};

TEST_F(HeartbeatTaskExecutorTest, CopysetNotFound) {
    EXPECT_CALL(mockCopysetMgr_, GetCopysetNode(_, _))
        .WillOnce(Return(nullptr));

    HeartbeatResponse response;
    auto* conf = response.add_needupdatecopysets();
    conf->set_poolid(1);
    conf->set_copysetid(1);

    ASSERT_NO_FATAL_FAILURE(executor_->ExecTasks(response));
}

TEST_F(HeartbeatTaskExecutorTest, NeedPurge_EpochIsZeroAndConfIsEmpty) {
    EXPECT_CALL(mockCopysetMgr_, GetCopysetNode(_, _))
        .WillOnce(Return(&mockCopysetNode_));

    EXPECT_CALL(mockCopysetMgr_, PurgeCopysetNode(_, _))
        .Times(1);

    HeartbeatResponse response;
    auto* conf = response.add_needupdatecopysets();
    conf->set_poolid(1);
    conf->set_copysetid(1);
    conf->set_epoch(0);
    conf->mutable_peers()->Clear();

    executor_->ExecTasks(response);
}

TEST_F(HeartbeatTaskExecutorTest, NeedPurge_ConfDoesnotContainCurrentServer) {
    EXPECT_CALL(mockCopysetMgr_, GetCopysetNode(_, _))
        .WillOnce(Return(&mockCopysetNode_));

    EXPECT_CALL(mockCopysetMgr_, PurgeCopysetNode(_, _)).Times(1);

    HeartbeatResponse response;
    auto* conf = response.add_needupdatecopysets();
    conf->set_poolid(1);
    conf->set_copysetid(1);
    conf->set_epoch(1);
    auto* peer = conf->add_peers();
    peer->set_address("1.2.3.4:1234:0");

    executor_->ExecTasks(response);
}

TEST_F(HeartbeatTaskExecutorTest, EpochMismatch) {
    EXPECT_CALL(mockCopysetMgr_, GetCopysetNode(_, _))
        .WillOnce(Return(&mockCopysetNode_));

    EXPECT_CALL(mockCopysetMgr_, PurgeCopysetNode(_, _))
        .Times(0);

    EXPECT_CALL(mockCopysetNode_, GetConfEpoch())
        .WillOnce(Return(100));

    EXPECT_CALL(mockCopysetNode_, TransferLeader(_))
        .Times(0);
    EXPECT_CALL(mockCopysetNode_, AddPeer(_, _))
        .Times(0);
    EXPECT_CALL(mockCopysetNode_, RemovePeer(_, _))
        .Times(0);
    EXPECT_CALL(mockCopysetNode_, ChangePeers(_, _))
        .Times(0);

    HeartbeatResponse response;
    auto* conf = response.add_needupdatecopysets();
    conf->set_poolid(1);
    conf->set_copysetid(1);
    conf->set_epoch(1);
    auto* peer = conf->add_peers();
    peer->set_address(current_ + ":0");

    executor_->ExecTasks(response);
}

TEST_F(HeartbeatTaskExecutorTest, HasNoConfChangeType) {
    EXPECT_CALL(mockCopysetMgr_, GetCopysetNode(_, _))
        .WillOnce(Return(&mockCopysetNode_));

    EXPECT_CALL(mockCopysetMgr_, PurgeCopysetNode(_, _))
        .Times(0);

    EXPECT_CALL(mockCopysetNode_, GetConfEpoch())
        .WillOnce(Return(100));

    EXPECT_CALL(mockCopysetNode_, TransferLeader(_))
        .Times(0);
    EXPECT_CALL(mockCopysetNode_, AddPeer(_, _))
        .Times(0);
    EXPECT_CALL(mockCopysetNode_, RemovePeer(_, _))
        .Times(0);
    EXPECT_CALL(mockCopysetNode_, ChangePeers(_, _))
        .Times(0);

    HeartbeatResponse response;
    auto* conf = response.add_needupdatecopysets();
    conf->set_poolid(1);
    conf->set_copysetid(1);
    conf->set_epoch(100);
    auto* peer = conf->add_peers();
    peer->set_address(current_ + ":0");

    executor_->ExecTasks(response);
}

TEST_F(HeartbeatTaskExecutorTest, TransferLeader_Success) {
    StartFakeCopysetService(confChangeTarget_);

    EXPECT_CALL(mockCopysetMgr_, GetCopysetNode(_, _))
        .WillOnce(Return(&mockCopysetNode_));

    EXPECT_CALL(mockCopysetMgr_, PurgeCopysetNode(_, _))
        .Times(0);

    EXPECT_CALL(mockCopysetNode_, GetConfEpoch())
        .WillOnce(Return(1));

    EXPECT_CALL(mockCopysetNode_, TransferLeader(_))
        .WillOnce(Return(butil::Status::OK()));

    HeartbeatResponse response;
    auto* conf = response.add_needupdatecopysets();
    conf->set_poolid(1);
    conf->set_copysetid(1);
    conf->set_epoch(1);
    conf->set_type(curve::mds::heartbeat::ConfigChangeType::TRANSFER_LEADER);
    auto* peer = conf->add_peers();
    peer->set_address(current_ + ":0");

    conf->mutable_configchangeitem()->set_address(confChangeTarget_ + ":0");

    executor_->ExecTasks(response);
}

TEST_F(HeartbeatTaskExecutorTest, AddPeer_Success) {
    EXPECT_CALL(mockCopysetMgr_, GetCopysetNode(_, _))
        .WillOnce(Return(&mockCopysetNode_));

    EXPECT_CALL(mockCopysetMgr_, PurgeCopysetNode(_, _))
        .Times(0);

    EXPECT_CALL(mockCopysetNode_, GetConfEpoch())
        .WillOnce(Return(1));

    EXPECT_CALL(mockCopysetNode_, AddPeer(_, _))
        .Times(1);

    HeartbeatResponse response;
    auto* conf = response.add_needupdatecopysets();
    conf->set_poolid(1);
    conf->set_copysetid(1);
    conf->set_epoch(1);
    conf->set_type(curve::mds::heartbeat::ConfigChangeType::ADD_PEER);
    auto* peer = conf->add_peers();
    peer->set_address(current_ + ":0");

    conf->mutable_configchangeitem()->set_address(confChangeTarget_ + ":0");

    executor_->ExecTasks(response);
}

TEST_F(HeartbeatTaskExecutorTest, RemovePeer_Success) {
    EXPECT_CALL(mockCopysetMgr_, GetCopysetNode(_, _))
        .WillOnce(Return(&mockCopysetNode_));

    EXPECT_CALL(mockCopysetMgr_, PurgeCopysetNode(_, _))
        .Times(0);

    EXPECT_CALL(mockCopysetNode_, GetConfEpoch())
        .WillOnce(Return(1));

    EXPECT_CALL(mockCopysetNode_, RemovePeer(_, _))
        .Times(1);

    HeartbeatResponse response;
    auto* conf = response.add_needupdatecopysets();
    conf->set_poolid(1);
    conf->set_copysetid(1);
    conf->set_epoch(1);
    conf->set_type(curve::mds::heartbeat::ConfigChangeType::REMOVE_PEER);
    auto* peer = conf->add_peers();
    peer->set_address(current_ + ":0");

    conf->mutable_configchangeitem()->set_address(confChangeTarget_ + ":0");

    executor_->ExecTasks(response);
}

TEST_F(HeartbeatTaskExecutorTest, ChangePeer_Success) {
    EXPECT_CALL(mockCopysetMgr_, GetCopysetNode(_, _))
        .WillOnce(Return(&mockCopysetNode_));

    EXPECT_CALL(mockCopysetMgr_, PurgeCopysetNode(_, _))
        .Times(0);

    EXPECT_CALL(mockCopysetNode_, GetConfEpoch())
        .WillOnce(Return(1));

    EXPECT_CALL(mockCopysetNode_, ChangePeers(_, _))
        .Times(1);

    HeartbeatResponse response;
    auto* conf = response.add_needupdatecopysets();
    conf->set_poolid(1);
    conf->set_copysetid(1);
    conf->set_epoch(1);
    conf->set_type(curve::mds::heartbeat::ConfigChangeType::CHANGE_PEER);
    auto* peer = conf->add_peers();
    peer->set_address(current_ + ":0");

    conf->mutable_oldpeer()->set_address("127.0.0.1:6700:0");
    conf->mutable_configchangeitem()->set_address(confChangeTarget_ + ":0");

    executor_->ExecTasks(response);
}

}  // namespace metaserver
}  // namespace curvefs
