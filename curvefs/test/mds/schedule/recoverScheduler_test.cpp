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
 * @Project: curve
 * @Date: 2021-11-15 11:01:48
 * @Author: chenwei
 */

#include <glog/logging.h>
#include "curvefs/src/mds/common/mds_define.h"
#include "curvefs/src/mds/schedule/operatorController.h"
#include "curvefs/src/mds/schedule/scheduleMetrics.h"
#include "curvefs/src/mds/schedule/scheduler.h"
#include "curvefs/src/mds/topology/topology_id_generator.h"
#include "curvefs/test/mds/mock/mock_topology.h"
#include "curvefs/test/mds/schedule/common.h"
#include "curvefs/test/mds/mock/mock_topoAdapter.h"

using ::testing::_;
using ::testing::Return;
using ::testing::AtLeast;
using ::testing::SetArgPointee;
using ::testing::DoAll;

using ::curvefs::mds::topology::TopologyIdGenerator;
using ::curvefs::mds::topology::MockTopology;
using ::curvefs::mds::topology::MockIdGenerator;
using ::curvefs::mds::topology::MockTokenGenerator;
using ::curvefs::mds::topology::MockStorage;
using ::std::chrono::steady_clock;
namespace curvefs {
namespace mds {
namespace schedule {
class TestRecoverSheduler : public ::testing::Test {
 protected:
    TestRecoverSheduler() {}
    ~TestRecoverSheduler() {}

    void SetUp() override {
        topo_ = std::make_shared<MockTopology>(idGenerator_, tokenGenerator_,
                                               storage_);
        metric_ = std::make_shared<ScheduleMetrics>(topo_);
        opController_ = std::make_shared<OperatorController>(2, metric_);
        topoAdapter_ = std::make_shared<MockTopoAdapter>();

        ScheduleOption opt;
        opt.transferLeaderTimeLimitSec = 10;
        opt.removePeerTimeLimitSec = 100;
        opt.addPeerTimeLimitSec = 1000;
        opt.changePeerTimeLimitSec = 1000;
        opt.recoverSchedulerIntervalSec = 1;
        recoverScheduler_ = std::make_shared<RecoverScheduler>(
            opt, topoAdapter_, opController_);
    }
    void TearDown() override {
        opController_ = nullptr;
        topoAdapter_ = nullptr;
        recoverScheduler_ = nullptr;
    }

 protected:
    std::shared_ptr<MockTopoAdapter> topoAdapter_;
    std::shared_ptr<OperatorController> opController_;
    std::shared_ptr<RecoverScheduler> recoverScheduler_;
    std::shared_ptr<MockIdGenerator> idGenerator_;
    std::shared_ptr<MockTokenGenerator> tokenGenerator_;
    std::shared_ptr<MockStorage> storage_;
    std::shared_ptr<MockTopology> topo_;
    std::shared_ptr<ScheduleMetrics> metric_;
};

TEST_F(TestRecoverSheduler, test_copySet_already_has_operator) {
    CopySetKey copySetKey;
    copySetKey.first = 1;
    copySetKey.second = 1;
    EXPECT_CALL(*topoAdapter_, GetCopySetInfos())
        .WillOnce(Return(std::vector<CopySetInfo>({GetCopySetInfoForTest()})));
    EXPECT_CALL(*topo_, GetCopySet(copySetKey, _)).WillOnce(Return(true));
    EXPECT_CALL(*topo_, GetMetaServer(1, _)).WillOnce(Return(false));
    Operator testOperator(1, copySetKey, OperatorPriority::NormalPriority,
                          steady_clock::now(), std::make_shared<AddPeer>(1));
    ASSERT_TRUE(opController_->AddOperator(testOperator));
    recoverScheduler_->Schedule();
    ASSERT_EQ(1, opController_->GetOperators().size());
}

TEST_F(TestRecoverSheduler, test_copySet_has_configChangeInfo) {
    auto testCopySetInfo = GetCopySetInfoForTest();
    testCopySetInfo.candidatePeerInfo = PeerInfo(1, 1, 1, "", 9000);
    EXPECT_CALL(*topoAdapter_, GetCopySetInfos())
        .WillOnce(Return(std::vector<CopySetInfo>({testCopySetInfo})));
    recoverScheduler_->Schedule();
    ASSERT_EQ(0, opController_->GetOperators().size());
}

TEST_F(TestRecoverSheduler, test_metaServer_cannot_get) {
    EXPECT_CALL(*topoAdapter_, GetCopySetInfos())
        .WillOnce(Return(std::vector<CopySetInfo>({GetCopySetInfoForTest()})));
    EXPECT_CALL(*topoAdapter_, GetMetaServerInfo(_, _))
        .Times(3)
        .WillRepeatedly(Return(false));
    recoverScheduler_->Schedule();
    ASSERT_EQ(0, opController_->GetOperators().size());
}

TEST_F(TestRecoverSheduler, test_server_has_more_offline_metaserver) {
    auto testCopySetInfo = GetCopySetInfoForTest();
    EXPECT_CALL(*topoAdapter_, GetCopySetInfos())
        .WillRepeatedly(Return(std::vector<CopySetInfo>({testCopySetInfo})));
    MetaServerSpace space(100, 100);
    MetaServerInfo csInfo1(testCopySetInfo.peers[0], OnlineState::OFFLINE,
                           space);
    MetaServerInfo csInfo2(testCopySetInfo.peers[1], OnlineState::ONLINE,
                           space);
    MetaServerInfo csInfo3(testCopySetInfo.peers[2], OnlineState::ONLINE,
                           space);
    PeerInfo peer4(4, 1, 1, "192.168.10.1", 9001);
    PeerInfo peer5(5, 1, 1, "192.168.10.1", 9002);
    MetaServerInfo csInfo4(peer4, OnlineState::OFFLINE, space);
    MetaServerInfo csInfo5(peer5, OnlineState::UNSTABLE, space);

    EXPECT_CALL(*topoAdapter_, GetMetaServerInfo(csInfo1.info.id, _))
        .WillOnce(DoAll(SetArgPointee<1>(csInfo1), Return(true)));
    EXPECT_CALL(*topoAdapter_, GetMetaServerInfo(csInfo2.info.id, _))
        .WillOnce(DoAll(SetArgPointee<1>(csInfo2), Return(true)));
    EXPECT_CALL(*topoAdapter_, GetMetaServerInfo(csInfo3.info.id, _))
        .WillOnce(DoAll(SetArgPointee<1>(csInfo3), Return(true)));
    EXPECT_CALL(*topoAdapter_, GetStandardReplicaNumInPool(1))
        .WillOnce(Return(0));
    recoverScheduler_->Schedule();
    ASSERT_EQ(0, opController_->GetOperators().size());
}

TEST_F(TestRecoverSheduler,
       test_server_has_more_offline_and_retired_metaserver) {
    auto testCopySetInfo = GetCopySetInfoForTest();
    EXPECT_CALL(*topoAdapter_, GetCopySetInfos())
        .WillRepeatedly(Return(std::vector<CopySetInfo>({testCopySetInfo})));
    MetaServerSpace space(100, 100);
    MetaServerInfo csInfo1(testCopySetInfo.peers[0], OnlineState::OFFLINE,
                           space);
    MetaServerInfo csInfo2(testCopySetInfo.peers[1], OnlineState::ONLINE,
                           space);
    MetaServerInfo csInfo3(testCopySetInfo.peers[2], OnlineState::ONLINE,
                           space);
    PeerInfo peer4(4, 1, 1, "192.168.10.1", 9001);
    PeerInfo peer5(5, 1, 1, "192.168.10.1", 9002);
    MetaServerInfo csInfo4(peer4, OnlineState::OFFLINE, space);
    MetaServerInfo csInfo5(peer5, OnlineState::OFFLINE, space);
    EXPECT_CALL(*topoAdapter_, GetMetaServerInfo(csInfo1.info.id, _))
        .WillOnce(DoAll(SetArgPointee<1>(csInfo1), Return(true)));
    EXPECT_CALL(*topoAdapter_, GetMetaServerInfo(csInfo2.info.id, _))
        .WillOnce(DoAll(SetArgPointee<1>(csInfo2), Return(true)));
    EXPECT_CALL(*topoAdapter_, GetMetaServerInfo(csInfo3.info.id, _))
        .WillOnce(DoAll(SetArgPointee<1>(csInfo3), Return(true)));
    EXPECT_CALL(*topoAdapter_, GetStandardReplicaNumInPool(_))
        .WillOnce(Return(2));
    recoverScheduler_->Schedule();
    Operator op;
    ASSERT_TRUE(opController_->GetOperatorById(testCopySetInfo.id, &op));
    ASSERT_TRUE(dynamic_cast<RemovePeer *>(op.step.get()) != nullptr);
    ASSERT_EQ(std::chrono::seconds(100), op.timeLimit);
}

TEST_F(TestRecoverSheduler, test_all_metaServer_online_offline) {
    auto testCopySetInfo = GetCopySetInfoForTest();
    EXPECT_CALL(*topoAdapter_, GetCopySetInfos())
        .WillRepeatedly(Return(std::vector<CopySetInfo>({testCopySetInfo})));
    MetaServerSpace space(100, 100);
    MetaServerInfo csInfo1(testCopySetInfo.peers[0], OnlineState::ONLINE,
                           space);
    MetaServerInfo csInfo2(testCopySetInfo.peers[1], OnlineState::ONLINE,
                           space);
    MetaServerInfo csInfo3(testCopySetInfo.peers[2], OnlineState::ONLINE,
                           space);
    PeerInfo peer4(4, 4, 4, "192.168.10.4", 9000);
    MetaServerInfo csInfo4(peer4, OnlineState::ONLINE, space);
    MetaServerIdType id1 = 1;
    MetaServerIdType id2 = 2;
    MetaServerIdType id3 = 3;
    Operator op;
    EXPECT_CALL(*topoAdapter_, GetAvgScatterWidthInPool(_))
        .WillRepeatedly(Return(90));
    {
        //1. All metaserveronline
        EXPECT_CALL(*topoAdapter_, GetMetaServerInfo(id1, _))
            .WillOnce(DoAll(SetArgPointee<1>(csInfo1), Return(true)));
        EXPECT_CALL(*topoAdapter_, GetMetaServerInfo(id2, _))
            .WillRepeatedly(DoAll(SetArgPointee<1>(csInfo2), Return(true)));
        EXPECT_CALL(*topoAdapter_, GetMetaServerInfo(id3, _))
            .WillRepeatedly(DoAll(SetArgPointee<1>(csInfo3), Return(true)));
        recoverScheduler_->Schedule();
        ASSERT_EQ(0, opController_->GetOperators().size());
    }

    {
        //2. The number of copies exceeds the standard, and the leader is suspended
        csInfo1.state = OnlineState::OFFLINE;
        EXPECT_CALL(*topoAdapter_, GetMetaServerInfo(id1, _))
            .WillOnce(DoAll(SetArgPointee<1>(csInfo1), Return(true)));
        EXPECT_CALL(*topoAdapter_, GetStandardReplicaNumInPool(_))
            .Times(2)
            .WillRepeatedly(Return(2));
        recoverScheduler_->Schedule();
        ASSERT_TRUE(opController_->GetOperatorById(testCopySetInfo.id, &op));
        ASSERT_TRUE(dynamic_cast<RemovePeer *>(op.step.get()) != nullptr);
        ASSERT_EQ(std::chrono::seconds(100), op.timeLimit);
    }

    {
        //3. The number of copies exceeds the standard, the follower will be suspended
        opController_->RemoveOperator(op.copysetID);
        csInfo1.state = OnlineState::ONLINE;
        csInfo2.state = OnlineState::OFFLINE;
        EXPECT_CALL(*topoAdapter_, GetMetaServerInfo(id1, _))
            .WillRepeatedly(DoAll(SetArgPointee<1>(csInfo1), Return(true)));
        EXPECT_CALL(*topoAdapter_, GetMetaServerInfo(id2, _))
            .WillRepeatedly(DoAll(SetArgPointee<1>(csInfo2), Return(true)));
        recoverScheduler_->Schedule();
        ASSERT_TRUE(opController_->GetOperatorById(testCopySetInfo.id, &op));
        ASSERT_TRUE(dynamic_cast<RemovePeer *>(op.step.get()) != nullptr);
        ASSERT_EQ(std::chrono::seconds(100), op.timeLimit);
    }

    {
        //4. The number of copies equals the standard, and the follower will be dropped
        opController_->RemoveOperator(op.copysetID);
        EXPECT_CALL(*topoAdapter_, GetStandardReplicaNumInPool(_))
            .WillRepeatedly(Return(3));
        std::vector<MetaServerInfo> metaserverList(
            {csInfo1, csInfo2, csInfo3, csInfo4});
        EXPECT_CALL(*topoAdapter_, GetMetaServersInPool(_))
            .WillOnce(Return(metaserverList));
        EXPECT_CALL(*topoAdapter_, GetStandardZoneNumInPool(_))
            .WillOnce(Return(3));
        std::map<MetaServerIdType, int> map1{{3, 1}};
        EXPECT_CALL(*topoAdapter_, ChooseNewMetaServerForCopyset(_, _, _, _))
            .WillOnce(DoAll(SetArgPointee<3>(4), Return(true)));
        EXPECT_CALL(*topoAdapter_, CreateCopySetAtMetaServer(_, _))
            .WillOnce(Return(true));
        recoverScheduler_->Schedule();
        ASSERT_TRUE(opController_->GetOperatorById(testCopySetInfo.id, &op));
        ASSERT_TRUE(dynamic_cast<ChangePeer *>(op.step.get()) != nullptr);
        ASSERT_EQ(std::chrono::seconds(1000), op.timeLimit);
    }

    {
        //5. Unable to select a replacement metaserver
        opController_->RemoveOperator(op.copysetID);
        EXPECT_CALL(*topoAdapter_, GetMetaServersInPool(_))
            .WillOnce(Return(std::vector<MetaServerInfo>{}));
        recoverScheduler_->Schedule();
        ASSERT_EQ(0, opController_->GetOperators().size());
    }

    {
        //6. Failed to create copyset on metaserver
        EXPECT_CALL(*topoAdapter_, GetStandardReplicaNumInPool(_))
            .WillRepeatedly(Return(3));
        std::vector<MetaServerInfo> metaserverList(
            {csInfo1, csInfo2, csInfo3, csInfo4});
        EXPECT_CALL(*topoAdapter_, GetMetaServersInPool(_))
            .WillOnce(Return(metaserverList));
        EXPECT_CALL(*topoAdapter_, GetStandardZoneNumInPool(_))
            .WillOnce(Return(3));
        std::map<MetaServerIdType, int> map1{{3, 1}};
        EXPECT_CALL(*topoAdapter_, ChooseNewMetaServerForCopyset(_, _, _, _))
            .WillOnce(DoAll(SetArgPointee<3>(4), Return(true)));
        EXPECT_CALL(*topoAdapter_, CreateCopySetAtMetaServer(_, _))
            .WillOnce(Return(false));
        recoverScheduler_->Schedule();
        ASSERT_EQ(0, opController_->GetOperators().size());
    }
}

}  // namespace schedule
}  // namespace mds
}  // namespace curvefs
