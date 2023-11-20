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

#include "curvefs/src/mds/schedule/coordinator.h"

#include <glog/logging.h>

#include "curvefs/src/mds/common/mds_define.h"
#include "curvefs/test/mds/mock/mock_topoAdapter.h"
#include "curvefs/test/mds/mock/mock_topology.h"
#include "curvefs/test/mds/schedule/common.h"

using ::curvefs::mds::schedule::ScheduleOption;
using ::curvefs::mds::topology::MockTopology;
using ::curvefs::mds::topology::TopologyIdGenerator;
using ::curvefs::mds::topology::TopologyStorage;
using ::curvefs::mds::topology::TopologyTokenGenerator;
using ::std::chrono::steady_clock;
using ::testing::_;
using ::testing::DoAll;
using ::testing::Return;
using ::testing::SetArgPointee;

using ::curvefs::mds::topology::UNINITIALIZE_ID;

namespace curvefs {
namespace mds {
namespace schedule {

class CoordinatorTest : public ::testing::Test {
 protected:
    CoordinatorTest() {}
    ~CoordinatorTest() {}

    void SetUp() override {
        topo_ = std::make_shared<MockTopology>(idGenerator_, tokenGenerator_,
                                               storage_);
        metric_ = std::make_shared<ScheduleMetrics>(topo_);
        topoAdapter_ = std::make_shared<MockTopoAdapter>();
        coordinator_ = std::make_shared<Coordinator>(topoAdapter_);
    }
    void TearDown() override {
        coordinator_ = nullptr;
        topoAdapter_ = nullptr;
        metric_ = nullptr;
        topo_ = nullptr;
    }

 public:
    std::shared_ptr<TopologyIdGenerator> idGenerator_;
    std::shared_ptr<TopologyTokenGenerator> tokenGenerator_;
    std::shared_ptr<TopologyStorage> storage_;
    std::shared_ptr<MockTopology> topo_;
    std::shared_ptr<ScheduleMetrics> metric_;
    std::shared_ptr<MockTopoAdapter> topoAdapter_;
    std::shared_ptr<Coordinator> coordinator_;
};

ScheduleOption GetFalseScheduleOption() {
    ScheduleOption scheduleOption;
    scheduleOption.enableCopysetScheduler = false;
    scheduleOption.enableLeaderScheduler = false;
    scheduleOption.enableRecoverScheduler = false;
    scheduleOption.copysetSchedulerIntervalSec = 0;
    scheduleOption.leaderSchedulerIntervalSec = 0;
    scheduleOption.recoverSchedulerIntervalSec = 0;
    scheduleOption.operatorConcurrent = 2;
    scheduleOption.transferLeaderTimeLimitSec = 1;
    scheduleOption.addPeerTimeLimitSec = 1;
    scheduleOption.removePeerTimeLimitSec = 1;

    return scheduleOption;
}

ScheduleOption GetTrueScheduleOption() {
    ScheduleOption scheduleOption;
    scheduleOption.enableCopysetScheduler = true;
    scheduleOption.enableLeaderScheduler = true;
    scheduleOption.enableRecoverScheduler = true;
    scheduleOption.copysetSchedulerIntervalSec = 10;
    scheduleOption.leaderSchedulerIntervalSec = 10;
    scheduleOption.recoverSchedulerIntervalSec = 10;
    scheduleOption.operatorConcurrent = 2;
    scheduleOption.transferLeaderTimeLimitSec = 1;
    scheduleOption.addPeerTimeLimitSec = 1;
    scheduleOption.removePeerTimeLimitSec = 1;

    return scheduleOption;
}

TEST_F(CoordinatorTest, test_AddPeer_CopySetHeartbeat) {
    ScheduleOption scheduleOption = GetTrueScheduleOption();
    coordinator_->InitScheduler(scheduleOption, metric_);

    ::curvefs::mds::topology::CopySetInfo testCopySetInfo(1, 1);
    testCopySetInfo.SetEpoch(1);
    EpochType startEpoch = 1;
    CopySetKey copySetKey;
    copySetKey.first = 1;
    copySetKey.second = 1;
    Operator testOperator(startEpoch, copySetKey,
                          OperatorPriority::NormalPriority, steady_clock::now(),
                          std::make_shared<AddPeer>(4));
    testOperator.timeLimit = std::chrono::seconds(100);

    auto info = GetCopySetInfoForTest();
    PeerInfo peer(4, 1, 1, "127.0.0.1", 9000);
    MetaServerSpace space(10, 1);
    MetaServerInfo csInfo(peer, OnlineState::ONLINE, space);

    ::curvefs::mds::heartbeat::CopySetConf res;
    {
        // 1. test copySet do not have operator
        EXPECT_CALL(*topoAdapter_, CopySetFromTopoToSchedule(_, _))
            .WillOnce(DoAll(SetArgPointee<1>(info), Return(true)));
        ASSERT_EQ(UNINITIALIZE_ID,
                  coordinator_->CopySetHeartbeat(testCopySetInfo,
                                                 ConfigChangeInfo{}, &res));
    }
    {
        // 2. test copySet has operator and not execute
        EXPECT_CALL(*topoAdapter_, CopySetFromTopoToSchedule(_, _))
            .Times(2)
            .WillOnce(DoAll(SetArgPointee<1>(info), Return(true)))
            .WillOnce(DoAll(SetArgPointee<1>(info), Return(true)));
        EXPECT_CALL(*topoAdapter_, GetMetaServerInfo(_, _))
            .Times(3)
            .WillOnce(DoAll(SetArgPointee<1>(csInfo), Return(true)))
            .WillOnce(DoAll(SetArgPointee<1>(csInfo), Return(true)))
            .WillOnce(Return(false));
        coordinator_->GetOpController()->AddOperator(testOperator);
        Operator opRes;
        ASSERT_TRUE(
            coordinator_->GetOpController()->GetOperatorById(info.id, &opRes));
        // First configuration distribution
        ASSERT_EQ(4, coordinator_->CopySetHeartbeat(testCopySetInfo,
                                                    ConfigChangeInfo{}, &res));
        ASSERT_EQ("127.0.0.1:9000:0", res.configchangeitem().address());
        ASSERT_EQ(ConfigChangeType::ADD_PEER, res.type());

        // Failed to obtain metaserver for the second time
        ASSERT_EQ(UNINITIALIZE_ID,
                  coordinator_->CopySetHeartbeat(testCopySetInfo,
                                                 ConfigChangeInfo{}, &res));
    }

    {
        // 3. Distribute configuration, but candidate is in offline status
        EXPECT_CALL(*topoAdapter_, CopySetFromTopoToSchedule(_, _))
            .Times(2)
            .WillRepeatedly(DoAll(SetArgPointee<1>(info), Return(true)));
        coordinator_->GetOpController()->RemoveOperator(info.id);
        ASSERT_TRUE(coordinator_->GetOpController()->AddOperator(testOperator));
        csInfo.state = OnlineState::OFFLINE;
        EXPECT_CALL(*topoAdapter_, GetMetaServerInfo(_, _))
            .WillOnce(DoAll(SetArgPointee<1>(csInfo), Return(true)));

        ASSERT_EQ(UNINITIALIZE_ID,
                  coordinator_->CopySetHeartbeat(testCopySetInfo,
                                                 ConfigChangeInfo{}, &res));
        Operator opRes;
        ASSERT_FALSE(
            coordinator_->GetOpController()->GetOperatorById(info.id, &opRes));
        csInfo.state = OnlineState::ONLINE;

        // Unable to obtain information on metaserver
        ASSERT_TRUE(coordinator_->GetOpController()->AddOperator(testOperator));
        EXPECT_CALL(*topoAdapter_, GetMetaServerInfo(_, _))
            .WillOnce(Return(false));
        ASSERT_EQ(UNINITIALIZE_ID,
                  coordinator_->CopySetHeartbeat(testCopySetInfo,
                                                 ConfigChangeInfo{}, &res));
        ASSERT_FALSE(
            coordinator_->GetOpController()->GetOperatorById(info.id, &opRes));
    }

    {
        // 4. test op executing and not finish
        info.candidatePeerInfo = PeerInfo(4, 1, 1, "", 9000);
        info.configChangeInfo.set_finished(false);
        info.configChangeInfo.set_type(ConfigChangeType::ADD_PEER);
        auto replica = new ::curvefs::common::Peer();
        replica->set_id(4);
        replica->set_address("192.168.10.4:9000:0");
        info.configChangeInfo.set_allocated_peer(replica);
        EXPECT_CALL(*topoAdapter_, CopySetFromTopoToSchedule(_, _))
            .WillOnce(DoAll(SetArgPointee<1>(info), Return(true)));
        ASSERT_EQ(UNINITIALIZE_ID,
                  coordinator_->CopySetHeartbeat(testCopySetInfo,
                                                 info.configChangeInfo, &res));
    }

    {
        // 5. test operator with staled startEpoch
        info.configChangeInfo.Clear();
        info.epoch = startEpoch + 1;
        EXPECT_CALL(*topoAdapter_, CopySetFromTopoToSchedule(_, _))
            .WillOnce(DoAll(SetArgPointee<1>(info), Return(true)));
        coordinator_->GetOpController()->RemoveOperator(info.id);
        ASSERT_TRUE(coordinator_->GetOpController()->AddOperator(testOperator));
        ASSERT_EQ(UNINITIALIZE_ID,
                  coordinator_->CopySetHeartbeat(testCopySetInfo,
                                                 ConfigChangeInfo{}, &res));
    }

    {
        // 6. test op success
        info.configChangeInfo.set_finished(true);
        info.peers.emplace_back(PeerInfo(4, 4, 4, "192.10.123.1", 9000));
        EXPECT_CALL(*topoAdapter_, CopySetFromTopoToSchedule(_, _))
            .WillOnce(DoAll(SetArgPointee<1>(info), Return(true)));
        ASSERT_EQ(UNINITIALIZE_ID,
                  coordinator_->CopySetHeartbeat(testCopySetInfo,
                                                 ConfigChangeInfo{}, &res));
    }

    {
        // 7. test transfer copysetInfo err
        EXPECT_CALL(*topoAdapter_, CopySetFromTopoToSchedule(_, _))
            .WillOnce(Return(false));
        ASSERT_EQ(UNINITIALIZE_ID,
                  coordinator_->CopySetHeartbeat(testCopySetInfo,
                                                 ConfigChangeInfo{}, &res));
    }
}

TEST_F(CoordinatorTest, test_ChangePeer_CopySetHeartbeat) {
    ScheduleOption scheduleOption = GetTrueScheduleOption();
    coordinator_->InitScheduler(scheduleOption, metric_);

    ::curvefs::mds::topology::CopySetInfo testCopySetInfo(1, 1);
    testCopySetInfo.SetEpoch(1);
    EpochType startEpoch = 1;
    CopySetKey copySetKey;
    copySetKey.first = 1;
    copySetKey.second = 1;
    Operator testOperator(startEpoch, copySetKey,
                          OperatorPriority::NormalPriority, steady_clock::now(),
                          std::make_shared<ChangePeer>(1, 4));
    testOperator.timeLimit = std::chrono::seconds(100);

    auto info = GetCopySetInfoForTest();
    PeerInfo peer(4, 1, 1, "127.0.0.1", 9000);
    MetaServerSpace space(10, 1);
    MetaServerInfo csInfo(peer, OnlineState::ONLINE, space);
    PeerInfo peer1(1, 1, 1, "127.0.0.1", 9001);
    MetaServerInfo csInfo1(peer1, OnlineState::ONLINE, space);

    ::curvefs::mds::heartbeat::CopySetConf res;
    {
        // 1. test copySet do not have operator
        EXPECT_CALL(*topoAdapter_, CopySetFromTopoToSchedule(_, _))
            .WillOnce(DoAll(SetArgPointee<1>(info), Return(true)));
        ASSERT_EQ(UNINITIALIZE_ID,
                  coordinator_->CopySetHeartbeat(testCopySetInfo,
                                                 ConfigChangeInfo{}, &res));
    }
    {
        // 2. test copySet has operator and not execute
        EXPECT_CALL(*topoAdapter_, CopySetFromTopoToSchedule(_, _))
            .Times(2)
            .WillOnce(DoAll(SetArgPointee<1>(info), Return(true)))
            .WillOnce(DoAll(SetArgPointee<1>(info), Return(true)));
        EXPECT_CALL(*topoAdapter_, GetMetaServerInfo(4, _))
            .Times(3)
            .WillOnce(DoAll(SetArgPointee<1>(csInfo), Return(true)))
            .WillOnce(DoAll(SetArgPointee<1>(csInfo), Return(true)))
            .WillOnce(Return(false));
        EXPECT_CALL(*topoAdapter_, GetMetaServerInfo(1, _))
            .WillOnce(DoAll(SetArgPointee<1>(csInfo1), Return(true)));
        coordinator_->GetOpController()->AddOperator(testOperator);
        Operator opRes;
        ASSERT_TRUE(
            coordinator_->GetOpController()->GetOperatorById(info.id, &opRes));
        // First configuration distribution
        ASSERT_EQ(4, coordinator_->CopySetHeartbeat(testCopySetInfo,
                                                    ConfigChangeInfo{}, &res));
        ASSERT_EQ("127.0.0.1:9000:0", res.configchangeitem().address());
        ASSERT_EQ("127.0.0.1:9001:0", res.oldpeer().address());
        ASSERT_EQ(ConfigChangeType::CHANGE_PEER, res.type());

        // Failed to obtain metaserver for the second time
        ASSERT_EQ(UNINITIALIZE_ID,
                  coordinator_->CopySetHeartbeat(testCopySetInfo,
                                                 ConfigChangeInfo{}, &res));
    }

    {
        // 3. Distribute configuration, but candidate is in offline status
        EXPECT_CALL(*topoAdapter_, CopySetFromTopoToSchedule(_, _))
            .Times(2)
            .WillRepeatedly(DoAll(SetArgPointee<1>(info), Return(true)));
        coordinator_->GetOpController()->RemoveOperator(info.id);
        ASSERT_TRUE(coordinator_->GetOpController()->AddOperator(testOperator));
        csInfo.state = OnlineState::OFFLINE;
        EXPECT_CALL(*topoAdapter_, GetMetaServerInfo(4, _))
            .WillOnce(DoAll(SetArgPointee<1>(csInfo), Return(true)));

        ASSERT_EQ(UNINITIALIZE_ID,
                  coordinator_->CopySetHeartbeat(testCopySetInfo,
                                                 ConfigChangeInfo{}, &res));
        Operator opRes;
        ASSERT_FALSE(
            coordinator_->GetOpController()->GetOperatorById(info.id, &opRes));
        csInfo.state = OnlineState::ONLINE;

        // Unable to obtain information on metaserver
        ASSERT_TRUE(coordinator_->GetOpController()->AddOperator(testOperator));
        EXPECT_CALL(*topoAdapter_, GetMetaServerInfo(_, _))
            .WillOnce(Return(false));
        ASSERT_EQ(UNINITIALIZE_ID,
                  coordinator_->CopySetHeartbeat(testCopySetInfo,
                                                 ConfigChangeInfo{}, &res));
        ASSERT_FALSE(
            coordinator_->GetOpController()->GetOperatorById(info.id, &opRes));
    }

    {
        // 4. test op executing and not finish
        info.candidatePeerInfo = PeerInfo(4, 1, 1, "", 9000);
        info.configChangeInfo.set_finished(false);
        info.configChangeInfo.set_type(ConfigChangeType::ADD_PEER);
        auto replica = new ::curvefs::common::Peer();
        replica->set_id(4);
        replica->set_address("192.168.10.4:9000:0");
        info.configChangeInfo.set_allocated_peer(replica);
        EXPECT_CALL(*topoAdapter_, CopySetFromTopoToSchedule(_, _))
            .WillOnce(DoAll(SetArgPointee<1>(info), Return(true)));
        ASSERT_EQ(UNINITIALIZE_ID,
                  coordinator_->CopySetHeartbeat(testCopySetInfo,
                                                 info.configChangeInfo, &res));
    }

    {
        // 5. test operator with staled startEpoch
        info.configChangeInfo.Clear();
        info.epoch = startEpoch + 1;
        EXPECT_CALL(*topoAdapter_, CopySetFromTopoToSchedule(_, _))
            .WillOnce(DoAll(SetArgPointee<1>(info), Return(true)));
        coordinator_->GetOpController()->RemoveOperator(info.id);
        ASSERT_TRUE(coordinator_->GetOpController()->AddOperator(testOperator));
        ASSERT_EQ(UNINITIALIZE_ID,
                  coordinator_->CopySetHeartbeat(testCopySetInfo,
                                                 ConfigChangeInfo{}, &res));
    }

    {
        // 6. test op success
        info.configChangeInfo.set_finished(true);
        info.peers.emplace_back(PeerInfo(4, 4, 4, "192.10.123.1", 9000));
        EXPECT_CALL(*topoAdapter_, CopySetFromTopoToSchedule(_, _))
            .WillOnce(DoAll(SetArgPointee<1>(info), Return(true)));
        ASSERT_EQ(UNINITIALIZE_ID,
                  coordinator_->CopySetHeartbeat(testCopySetInfo,
                                                 ConfigChangeInfo{}, &res));
    }

    {
        // 7. test transfer copysetInfo err
        EXPECT_CALL(*topoAdapter_, CopySetFromTopoToSchedule(_, _))
            .WillOnce(Return(false));
        ASSERT_EQ(UNINITIALIZE_ID,
                  coordinator_->CopySetHeartbeat(testCopySetInfo,
                                                 ConfigChangeInfo{}, &res));
    }
}

TEST_F(CoordinatorTest, test_MetaserverGoingToAdd) {
    ScheduleOption scheduleOption;
    scheduleOption.operatorConcurrent = 4;
    coordinator_->InitScheduler(scheduleOption,
                                std::make_shared<ScheduleMetrics>(topo_));

    {
        // 1. There are no operators to change on the copyset
        ASSERT_FALSE(coordinator_->MetaserverGoingToAdd(1, CopySetKey{1, 1}));
    }

    {
        // 2. There is a leader change on the copyset and the target leader is
        // metaserver-1
        Operator testOperator(
            1, CopySetKey{1, 1}, OperatorPriority::NormalPriority,
            steady_clock::now(), std::make_shared<TransferLeader>(2, 1));
        ASSERT_TRUE(coordinator_->GetOpController()->AddOperator(testOperator));
        ASSERT_FALSE(coordinator_->MetaserverGoingToAdd(1, CopySetKey{1, 1}));
    }

    {
        // 3. There is a remove peer operation on the copyset
        Operator testOperator(
            1, CopySetKey{1, 2}, OperatorPriority::NormalPriority,
            steady_clock::now(), std::make_shared<RemovePeer>(1));
        ASSERT_TRUE(coordinator_->GetOpController()->AddOperator(testOperator));
        ASSERT_FALSE(coordinator_->MetaserverGoingToAdd(1, CopySetKey{1, 2}));
    }

    {
        // 4. There is an add peer operation on the copyset, but the target is
        // not 1
        Operator testOperator(
            1, CopySetKey{1, 3}, OperatorPriority::NormalPriority,
            steady_clock::now(), std::make_shared<AddPeer>(2));
        ASSERT_TRUE(coordinator_->GetOpController()->AddOperator(testOperator));
        ASSERT_FALSE(coordinator_->MetaserverGoingToAdd(1, CopySetKey{1, 3}));
    }

    {
        // 5. There is an add peer operation on the copyset, with a target of 1
        Operator testOperator(
            1, CopySetKey{1, 4}, OperatorPriority::NormalPriority,
            steady_clock::now(), std::make_shared<AddPeer>(1));
        ASSERT_TRUE(coordinator_->GetOpController()->AddOperator(testOperator));
        ASSERT_TRUE(coordinator_->MetaserverGoingToAdd(1, CopySetKey{1, 4}));
    }

    {
        // 6. There is a change peer operation on the copyset, but the target is
        // not 1
        Operator testOperator(
            1, CopySetKey{1, 5}, OperatorPriority::NormalPriority,
            steady_clock::now(), std::make_shared<ChangePeer>(4, 2));
        ASSERT_TRUE(coordinator_->GetOpController()->AddOperator(testOperator));
        ASSERT_FALSE(coordinator_->MetaserverGoingToAdd(1, CopySetKey{1, 5}));
    }

    {
        // 7. There is a change peer operation on the copyset, with a target of
        // 1
        Operator testOperator(
            1, CopySetKey{1, 6}, OperatorPriority::NormalPriority,
            steady_clock::now(), std::make_shared<ChangePeer>(4, 1));
        ASSERT_TRUE(coordinator_->GetOpController()->AddOperator(testOperator));
        ASSERT_TRUE(coordinator_->MetaserverGoingToAdd(1, CopySetKey{1, 6}));
    }
}

TEST_F(CoordinatorTest, test_SchedulerSwitch) {
    ScheduleOption scheduleOption = GetTrueScheduleOption();
    scheduleOption.copysetSchedulerIntervalSec = 0;
    scheduleOption.leaderSchedulerIntervalSec = 0;
    scheduleOption.recoverSchedulerIntervalSec = 0;
    coordinator_->InitScheduler(scheduleOption, metric_);

    EXPECT_CALL(*topoAdapter_, GetCopySetInfos()).Times(0);
    EXPECT_CALL(*topoAdapter_, Getpools()).Times(0);
    EXPECT_CALL(*topoAdapter_, GetMetaServerInfos()).Times(0);

    // Set flags to false
    gflags::SetCommandLineOption("enableCopySetScheduler", "false");
    gflags::SetCommandLineOption("enableRecoverScheduler", "false");
    gflags::SetCommandLineOption("enableLeaderScheduler", "false");

    coordinator_->Run();
    ::sleep(1);
    coordinator_->Stop();
}

TEST_F(CoordinatorTest, test_QueryMetaServerRecoverStatus) {
    /*
    Scenario:
    metaserver1: offline has recovery op
    metaserver2: offline has no recovery op, no candidate, and other op
    metaserver3: offline has a candidate
    metaserver4: online
    metaserver4: online
    */
    // Get option
    ScheduleOption scheduleOption = GetFalseScheduleOption();
    coordinator_->InitScheduler(scheduleOption, metric_);

    // Construct metaserver
    std::vector<MetaServerInfo> metaserverInfos;
    std::vector<PeerInfo> peerInfos;
    for (int i = 1; i <= 6; i++) {
        PeerInfo peer(i, i % 3 + 1, i, "192.168.0." + std::to_string(i), 9000);
        MetaServerSpace space(10, 1);
        MetaServerInfo csInfo(peer, OnlineState::ONLINE, space);
        if (i <= 3) {
            csInfo.state = OnlineState::OFFLINE;
        }

        metaserverInfos.emplace_back(csInfo);
        peerInfos.emplace_back(peer);
    }

    // Construct op
    Operator opForCopySet1(1, CopySetKey{1, 1}, OperatorPriority::HighPriority,
                           steady_clock::now(),
                           std::make_shared<ChangePeer>(1, 4));
    ASSERT_TRUE(coordinator_->GetOpController()->AddOperator(opForCopySet1));

    Operator opForCopySet2(
        2, CopySetKey{1, 2}, OperatorPriority::NormalPriority,
        steady_clock::now(), std::make_shared<TransferLeader>(2, 4));
    ASSERT_TRUE(coordinator_->GetOpController()->AddOperator(opForCopySet2));

    // Construct a copyset
    std::vector<PeerInfo> peersFor2({peerInfos[1], peerInfos[3], peerInfos[4]});
    CopySetInfo copyset2(CopySetKey{1, 2}, 1, 4, peersFor2, ConfigChangeInfo{});

    std::vector<PeerInfo> peersFor3({peerInfos[2], peerInfos[3], peerInfos[4]});
    ConfigChangeInfo configChangeInfoForCS3;
    auto replica = new ::curvefs::common::Peer();
    replica->set_id(6);
    replica->set_address("192.168.0.6:9000:0");
    configChangeInfoForCS3.set_allocated_peer(replica);
    configChangeInfoForCS3.set_type(ConfigChangeType::CHANGE_PEER);
    configChangeInfoForCS3.set_finished(true);
    CopySetInfo copyset3(CopySetKey{1, 3}, 1, 4, peersFor3,
                         configChangeInfoForCS3);

    // 1. Query all metaservers
    {
        EXPECT_CALL(*topoAdapter_, GetMetaServerInfos())
            .WillOnce(Return(metaserverInfos));
        EXPECT_CALL(*topoAdapter_, GetCopySetInfosInMetaServer(2))
            .WillOnce(Return(std::vector<CopySetInfo>{copyset2}));
        EXPECT_CALL(*topoAdapter_, GetCopySetInfosInMetaServer(3))
            .WillOnce(Return(std::vector<CopySetInfo>{copyset3}));

        std::map<MetaServerIdType, bool> statusMap;
        ASSERT_EQ(ScheduleStatusCode::Success,
                  coordinator_->QueryMetaServerRecoverStatus(
                      std::vector<MetaServerIdType>{}, &statusMap));
        ASSERT_EQ(6, statusMap.size());
        ASSERT_TRUE(statusMap[1]);
        ASSERT_FALSE(statusMap[2]);
        ASSERT_TRUE(statusMap[3]);
        ASSERT_FALSE(statusMap[4]);
        ASSERT_FALSE(statusMap[5]);
        ASSERT_FALSE(statusMap[6]);
    }

    // 2. Query specified metaserver, but metaserver does not exist
    {
        EXPECT_CALL(*topoAdapter_, GetMetaServerInfo(7, _))
            .WillOnce(Return(false));

        std::map<MetaServerIdType, bool> statusMap;
        ASSERT_EQ(ScheduleStatusCode::InvalidQueryMetaserverID,
                  coordinator_->QueryMetaServerRecoverStatus(
                      std::vector<MetaServerIdType>{7}, &statusMap));
    }

    // 3. Query specified metaserver, not in recovery
    {
        EXPECT_CALL(*topoAdapter_, GetMetaServerInfo(6, _))
            .WillOnce(
                DoAll(SetArgPointee<1>(metaserverInfos[5]), Return(true)));
        std::map<MetaServerIdType, bool> statusMap;
        ASSERT_EQ(ScheduleStatusCode::Success,
                  coordinator_->QueryMetaServerRecoverStatus(
                      std::vector<MetaServerIdType>{6}, &statusMap));
        ASSERT_EQ(1, statusMap.size());
        ASSERT_FALSE(statusMap[6]);
    }
}

}  // namespace schedule
}  // namespace mds
}  // namespace curvefs
