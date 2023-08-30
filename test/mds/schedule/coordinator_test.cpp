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
 * Created Date: Tue Dec 25 2018
 * Author: lixiaocui
 */

#include <glog/logging.h>
#include "src/mds/schedule/coordinator.h"
#include "src/mds/common/mds_define.h"
#include "test/mds/schedule/mock_topoAdapter.h"
#include "test/mds/mock/mock_topology.h"
#include "test/mds/schedule/common.h"

using ::curve::mds::topology::MockTopology;
using ::curve::mds::schedule::ScheduleOption;
using ::testing::Return;
using ::testing::SetArgPointee;
using ::testing::DoAll;
using ::testing::_;

using ::curve::mds::topology::UNINTIALIZE_ID;

namespace curve {
namespace mds {
namespace schedule {

ScheduleOption GetScheduleOption() {
    ScheduleOption scheduleOption;
    scheduleOption.enableCopysetScheduler = false;
    scheduleOption.enableLeaderScheduler = false;
    scheduleOption.enableRecoverScheduler = false;
    scheduleOption.enableReplicaScheduler = false;
    scheduleOption.copysetSchedulerIntervalSec = 0;
    scheduleOption.leaderSchedulerIntervalSec = 0;
    scheduleOption.recoverSchedulerIntervalSec = 0;
    scheduleOption.replicaSchedulerIntervalSec = 0;
    scheduleOption.operatorConcurrent = 2;
    scheduleOption.transferLeaderTimeLimitSec = 1;
    scheduleOption.addPeerTimeLimitSec = 1;
    scheduleOption.removePeerTimeLimitSec = 1;

    return scheduleOption;
}

TEST(CoordinatorTest, test_AddPeer_CopySetHeartbeat) {
    auto topo = std::make_shared<MockTopology>();
    auto metric = std::make_shared<ScheduleMetrics>(topo);
    auto topoAdapter = std::make_shared<MockTopoAdapter>();
    auto coordinator = std::make_shared<Coordinator>(topoAdapter);
    ScheduleOption scheduleOption;
    scheduleOption.enableCopysetScheduler = true;
    scheduleOption.enableLeaderScheduler = true;
    scheduleOption.enableRecoverScheduler = true;
    scheduleOption.enableReplicaScheduler = true;
    scheduleOption.copysetSchedulerIntervalSec = 10;
    scheduleOption.leaderSchedulerIntervalSec = 10;
    scheduleOption.recoverSchedulerIntervalSec = 10;
    scheduleOption.replicaSchedulerIntervalSec = 10;
    scheduleOption.operatorConcurrent = 2;
    scheduleOption.transferLeaderTimeLimitSec = 1;
    scheduleOption.addPeerTimeLimitSec = 1;
    scheduleOption.removePeerTimeLimitSec = 1;
    coordinator->InitScheduler(scheduleOption, metric);

    ::curve::mds::topology::CopySetInfo testCopySetInfo(1, 1);
    testCopySetInfo.SetEpoch(1);
    EpochType startEpoch = 1;
    CopySetKey copySetKey;
    copySetKey.first = 1;
    copySetKey.second = 1;
    Operator testOperator(startEpoch, copySetKey,
                          OperatorPriority::NormalPriority,
                          steady_clock::now(), std::make_shared<AddPeer>(4));
    testOperator.timeLimit = std::chrono::seconds(100);

    auto info = GetCopySetInfoForTest();
    PeerInfo peer(4, 1, 1, "127.0.0.1", 9000);
    ChunkServerInfo csInfo(peer, OnlineState::ONLINE, DiskState::DISKNORMAL,
                           ChunkServerStatus::READWRITE,
                           1, 10, 1, ChunkServerStatisticInfo{});

    ::curve::mds::heartbeat::CopySetConf res;
    {
        // 1. test copySet do not have operator
        EXPECT_CALL(*topoAdapter, CopySetFromTopoToSchedule(_, _))
            .WillOnce(DoAll(SetArgPointee<1>(info), Return(true)));
        ASSERT_EQ(UNINTIALIZE_ID, coordinator->CopySetHeartbeat(
                testCopySetInfo, ConfigChangeInfo{}, &res));
    }
    {
        // 2. test copySet has operator and not execute
        EXPECT_CALL(*topoAdapter, CopySetFromTopoToSchedule(_, _))
            .Times(2).WillOnce(DoAll(SetArgPointee<1>(info), Return(true)))
                    .WillOnce(DoAll(SetArgPointee<1>(info), Return(true)));
        EXPECT_CALL(*topoAdapter, GetChunkServerInfo(_, _))
            .Times(3)
            .WillOnce(DoAll(SetArgPointee<1>(csInfo), Return(true)))
            .WillOnce(DoAll(SetArgPointee<1>(csInfo), Return(true)))
            .WillOnce(Return(false));
        coordinator->GetOpController()->AddOperator(testOperator);
        Operator opRes;
        ASSERT_TRUE(coordinator->GetOpController()->GetOperatorById(
            info.id, &opRes));
        //First configuration distribution
        ASSERT_EQ(4, coordinator->CopySetHeartbeat(
            testCopySetInfo, ConfigChangeInfo{}, &res));
        ASSERT_EQ("127.0.0.1:9000:0", res.configchangeitem().address());
        ASSERT_EQ(ConfigChangeType::ADD_PEER, res.type());

        //Failed to obtain chunkserver for the second time
        ASSERT_EQ(UNINTIALIZE_ID, coordinator->CopySetHeartbeat(
            testCopySetInfo, ConfigChangeInfo{}, &res));
    }

    {
        //3 Distribute configuration, but candidate is in offline status
        EXPECT_CALL(*topoAdapter, CopySetFromTopoToSchedule(_, _))
            .Times(2)
            .WillRepeatedly(DoAll(SetArgPointee<1>(info), Return(true)));
        coordinator->GetOpController()->RemoveOperator(info.id);
        ASSERT_TRUE(coordinator->GetOpController()->AddOperator(testOperator));
        csInfo.state = OnlineState::OFFLINE;
        EXPECT_CALL(*topoAdapter, GetChunkServerInfo(_, _))
            .WillOnce(DoAll(SetArgPointee<1>(csInfo), Return(true)));

        ASSERT_EQ(UNINTIALIZE_ID, coordinator->CopySetHeartbeat(
            testCopySetInfo, ConfigChangeInfo{}, &res));
        Operator opRes;
        ASSERT_FALSE(coordinator->GetOpController()->GetOperatorById(
            info.id, &opRes));
        csInfo.state = OnlineState::ONLINE;

        //Unable to obtain chunkserver information
        ASSERT_TRUE(coordinator->GetOpController()->AddOperator(testOperator));
        EXPECT_CALL(*topoAdapter, GetChunkServerInfo(_, _))
            .WillOnce(Return(false));
        ASSERT_EQ(UNINTIALIZE_ID, coordinator->CopySetHeartbeat(
            testCopySetInfo, ConfigChangeInfo{}, &res));
        ASSERT_FALSE(coordinator->GetOpController()->GetOperatorById(
            info.id, &opRes));
    }

    {
        // 4. test op executing and not finish
        info.candidatePeerInfo = PeerInfo(4, 1, 1, "", 9000);
        info.configChangeInfo.set_finished(false);
        info.configChangeInfo.set_type(ConfigChangeType::ADD_PEER);
        auto replica = new ::curve::common::Peer();
        replica->set_id(4);
        replica->set_address("192.168.10.4:9000:0");
        info.configChangeInfo.set_allocated_peer(replica);
        EXPECT_CALL(*topoAdapter, CopySetFromTopoToSchedule(_, _))
            .WillOnce(DoAll(SetArgPointee<1>(info), Return(true)));
        ASSERT_EQ(UNINTIALIZE_ID, coordinator->CopySetHeartbeat(
            testCopySetInfo, info.configChangeInfo, &res));
    }

    {
        // 5. test operator with staled startEpoch
        info.configChangeInfo.Clear();
        info.epoch = startEpoch + 1;
        EXPECT_CALL(*topoAdapter, CopySetFromTopoToSchedule(_, _))
            .WillOnce(DoAll(SetArgPointee<1>(info), Return(true)));
        coordinator->GetOpController()->RemoveOperator(info.id);
        ASSERT_TRUE(coordinator->GetOpController()->AddOperator(testOperator));
        ASSERT_EQ(UNINTIALIZE_ID, coordinator->CopySetHeartbeat(
            testCopySetInfo, ConfigChangeInfo{}, &res));
    }

    {
        // 6. test op success
        info.configChangeInfo.set_finished(true);
        info.peers.emplace_back(PeerInfo(4, 4, 4, "192.10.123.1", 9000));
        EXPECT_CALL(*topoAdapter, CopySetFromTopoToSchedule(_, _))
            .WillOnce(DoAll(SetArgPointee<1>(info), Return(true)));
        ASSERT_EQ(UNINTIALIZE_ID, coordinator->CopySetHeartbeat(
            testCopySetInfo, ConfigChangeInfo{}, &res));
    }

    {
        // 7. test transfer copysetInfo err
        EXPECT_CALL(*topoAdapter, CopySetFromTopoToSchedule(_, _))
            .WillOnce(Return(false));
        ASSERT_EQ(UNINTIALIZE_ID, coordinator->CopySetHeartbeat(
            testCopySetInfo, ConfigChangeInfo{}, &res));
    }
}

TEST(CoordinatorTest, test_ChangePeer_CopySetHeartbeat) {
    auto topo = std::make_shared<MockTopology>();
    auto metric = std::make_shared<ScheduleMetrics>(topo);
    auto topoAdapter = std::make_shared<MockTopoAdapter>();
    auto coordinator = std::make_shared<Coordinator>(topoAdapter);
    ScheduleOption scheduleOption;
    scheduleOption.enableCopysetScheduler = true;
    scheduleOption.enableLeaderScheduler = true;
    scheduleOption.enableRecoverScheduler = true;
    scheduleOption.enableReplicaScheduler = true;
    scheduleOption.copysetSchedulerIntervalSec = 10;
    scheduleOption.leaderSchedulerIntervalSec = 10;
    scheduleOption.recoverSchedulerIntervalSec = 10;
    scheduleOption.replicaSchedulerIntervalSec = 10;
    scheduleOption.operatorConcurrent = 2;
    scheduleOption.transferLeaderTimeLimitSec = 1;
    scheduleOption.addPeerTimeLimitSec = 1;
    scheduleOption.removePeerTimeLimitSec = 1;
    coordinator->InitScheduler(scheduleOption, metric);

    ::curve::mds::topology::CopySetInfo testCopySetInfo(1, 1);
    testCopySetInfo.SetEpoch(1);
    EpochType startEpoch = 1;
    CopySetKey copySetKey;
    copySetKey.first = 1;
    copySetKey.second = 1;
    Operator testOperator(
        startEpoch, copySetKey, OperatorPriority::NormalPriority,
        steady_clock::now(), std::make_shared<ChangePeer>(1, 4));
    testOperator.timeLimit = std::chrono::seconds(100);

    auto info = GetCopySetInfoForTest();
    PeerInfo peer(4, 1, 1, "127.0.0.1", 9000);
    ChunkServerInfo csInfo(peer, OnlineState::ONLINE, DiskState::DISKNORMAL,
                           ChunkServerStatus::READWRITE,
                           1, 10, 1, ChunkServerStatisticInfo{});
    PeerInfo peer1(1, 1, 1, "127.0.0.1", 9001);
    ChunkServerInfo csInfo1(peer1, OnlineState::ONLINE, DiskState::DISKNORMAL,
                           ChunkServerStatus::READWRITE,
                           1, 10, 1, ChunkServerStatisticInfo{});

    ::curve::mds::heartbeat::CopySetConf res;
    {
        // 1. test copySet do not have operator
        EXPECT_CALL(*topoAdapter, CopySetFromTopoToSchedule(_, _))
            .WillOnce(DoAll(SetArgPointee<1>(info), Return(true)));
        ASSERT_EQ(UNINTIALIZE_ID, coordinator->CopySetHeartbeat(
                testCopySetInfo, ConfigChangeInfo{}, &res));
    }
    {
        // 2. test copySet has operator and not execute
        EXPECT_CALL(*topoAdapter, CopySetFromTopoToSchedule(_, _))
            .Times(2).WillOnce(DoAll(SetArgPointee<1>(info), Return(true)))
                    .WillOnce(DoAll(SetArgPointee<1>(info), Return(true)));
        EXPECT_CALL(*topoAdapter, GetChunkServerInfo(4, _))
            .Times(3)
            .WillOnce(DoAll(SetArgPointee<1>(csInfo), Return(true)))
            .WillOnce(DoAll(SetArgPointee<1>(csInfo), Return(true)))
            .WillOnce(Return(false));
        EXPECT_CALL(*topoAdapter, GetChunkServerInfo(1, _))
            .WillOnce(DoAll(SetArgPointee<1>(csInfo1), Return(true)));
        coordinator->GetOpController()->AddOperator(testOperator);
        Operator opRes;
        ASSERT_TRUE(coordinator->GetOpController()->GetOperatorById(
            info.id, &opRes));
        //First configuration distribution
        ASSERT_EQ(4, coordinator->CopySetHeartbeat(
            testCopySetInfo, ConfigChangeInfo{}, &res));
        ASSERT_EQ("127.0.0.1:9000:0", res.configchangeitem().address());
        ASSERT_EQ("127.0.0.1:9001:0", res.oldpeer().address());
        ASSERT_EQ(ConfigChangeType::CHANGE_PEER, res.type());

        //Failed to obtain chunkserver for the second time
        ASSERT_EQ(UNINTIALIZE_ID, coordinator->CopySetHeartbeat(
            testCopySetInfo, ConfigChangeInfo{}, &res));
    }

    {
        //3 Distribute configuration, but candidate is in offline status
        EXPECT_CALL(*topoAdapter, CopySetFromTopoToSchedule(_, _))
            .Times(2)
            .WillRepeatedly(DoAll(SetArgPointee<1>(info), Return(true)));
        coordinator->GetOpController()->RemoveOperator(info.id);
        ASSERT_TRUE(coordinator->GetOpController()->AddOperator(testOperator));
        csInfo.state = OnlineState::OFFLINE;
        EXPECT_CALL(*topoAdapter, GetChunkServerInfo(4, _))
            .WillOnce(DoAll(SetArgPointee<1>(csInfo), Return(true)));

        ASSERT_EQ(UNINTIALIZE_ID, coordinator->CopySetHeartbeat(
            testCopySetInfo, ConfigChangeInfo{}, &res));
        Operator opRes;
        ASSERT_FALSE(coordinator->GetOpController()->GetOperatorById(
            info.id, &opRes));
        csInfo.state = OnlineState::ONLINE;

        //Unable to obtain chunkserver information
        ASSERT_TRUE(coordinator->GetOpController()->AddOperator(testOperator));
        EXPECT_CALL(*topoAdapter, GetChunkServerInfo(_, _))
            .WillOnce(Return(false));
        ASSERT_EQ(UNINTIALIZE_ID, coordinator->CopySetHeartbeat(
            testCopySetInfo, ConfigChangeInfo{}, &res));
        ASSERT_FALSE(coordinator->GetOpController()->GetOperatorById(
            info.id, &opRes));
    }

    {
        // 4. test op executing and not finish
        info.candidatePeerInfo = PeerInfo(4, 1, 1, "", 9000);
        info.configChangeInfo.set_finished(false);
        info.configChangeInfo.set_type(ConfigChangeType::ADD_PEER);
        auto replica = new ::curve::common::Peer();
        replica->set_id(4);
        replica->set_address("192.168.10.4:9000:0");
        info.configChangeInfo.set_allocated_peer(replica);
        EXPECT_CALL(*topoAdapter, CopySetFromTopoToSchedule(_, _))
            .WillOnce(DoAll(SetArgPointee<1>(info), Return(true)));
        ASSERT_EQ(UNINTIALIZE_ID, coordinator->CopySetHeartbeat(
            testCopySetInfo, info.configChangeInfo, &res));
    }

    {
        // 5. test operator with staled startEpoch
        info.configChangeInfo.Clear();
        info.epoch = startEpoch + 1;
        EXPECT_CALL(*topoAdapter, CopySetFromTopoToSchedule(_, _))
            .WillOnce(DoAll(SetArgPointee<1>(info), Return(true)));
        coordinator->GetOpController()->RemoveOperator(info.id);
        ASSERT_TRUE(coordinator->GetOpController()->AddOperator(testOperator));
        ASSERT_EQ(UNINTIALIZE_ID, coordinator->CopySetHeartbeat(
            testCopySetInfo, ConfigChangeInfo{}, &res));
    }

    {
        // 6. test op success
        info.configChangeInfo.set_finished(true);
        info.peers.emplace_back(PeerInfo(4, 4, 4, "192.10.123.1", 9000));
        EXPECT_CALL(*topoAdapter, CopySetFromTopoToSchedule(_, _))
            .WillOnce(DoAll(SetArgPointee<1>(info), Return(true)));
        ASSERT_EQ(UNINTIALIZE_ID, coordinator->CopySetHeartbeat(
            testCopySetInfo, ConfigChangeInfo{}, &res));
    }

    {
        // 7. test transfer copysetInfo err
        EXPECT_CALL(*topoAdapter, CopySetFromTopoToSchedule(_, _))
            .WillOnce(Return(false));
        ASSERT_EQ(UNINTIALIZE_ID, coordinator->CopySetHeartbeat(
            testCopySetInfo, ConfigChangeInfo{}, &res));
    }
}

TEST(CoordinatorTest, test_ChunkserverGoingToAdd) {
    auto topo = std::make_shared<MockTopology>();
    auto topoAdapter = std::make_shared<MockTopoAdapter>();
    auto coordinator = std::make_shared<Coordinator>(topoAdapter);
    ScheduleOption scheduleOption;
    scheduleOption.operatorConcurrent = 4;
    coordinator->InitScheduler(
        scheduleOption, std::make_shared<ScheduleMetrics>(topo));

    {
        //1 There are no operators to change on the copyset
        ASSERT_FALSE(coordinator->ChunkserverGoingToAdd(1, CopySetKey{1, 1}));
    }

    {
        //2 There is a leader change on the copyset and the target leader is chunkserver-1
        Operator testOperator(1, CopySetKey{1, 1},
                          OperatorPriority::NormalPriority,
                          steady_clock::now(),
                          std::make_shared<TransferLeader>(2, 1));
        ASSERT_TRUE(coordinator->GetOpController()->AddOperator(testOperator));
        ASSERT_FALSE(coordinator->ChunkserverGoingToAdd(1, CopySetKey{1, 1}));
    }

    {
        //3 There is a remove peer operation on the copyset
        Operator testOperator(1, CopySetKey{1, 2},
                          OperatorPriority::NormalPriority,
                          steady_clock::now(),
                          std::make_shared<RemovePeer>(1));
        ASSERT_TRUE(coordinator->GetOpController()->AddOperator(testOperator));
        ASSERT_FALSE(coordinator->ChunkserverGoingToAdd(1, CopySetKey{1, 2}));
    }

    {
        //4 There is an add peer operation on the copyset, but the target is not 1
        Operator testOperator(1, CopySetKey{1, 3},
                          OperatorPriority::NormalPriority,
                          steady_clock::now(),
                          std::make_shared<AddPeer>(2));
        ASSERT_TRUE(coordinator->GetOpController()->AddOperator(testOperator));
        ASSERT_FALSE(coordinator->ChunkserverGoingToAdd(1, CopySetKey{1, 3}));
    }

    {
        //5 There is an add peer operation on the copyset, with a target of 1
        Operator testOperator(1, CopySetKey{1, 4},
                          OperatorPriority::NormalPriority,
                          steady_clock::now(),
                          std::make_shared<AddPeer>(1));
        ASSERT_TRUE(coordinator->GetOpController()->AddOperator(testOperator));
        ASSERT_TRUE(coordinator->ChunkserverGoingToAdd(1, CopySetKey{1, 4}));
    }

    {
        //6 There is a change peer operation on the copyset, but the target is not 1
        Operator testOperator(1, CopySetKey{1, 5},
                          OperatorPriority::NormalPriority,
                          steady_clock::now(),
                          std::make_shared<ChangePeer>(4, 2));
        ASSERT_TRUE(coordinator->GetOpController()->AddOperator(testOperator));
        ASSERT_FALSE(coordinator->ChunkserverGoingToAdd(1, CopySetKey{1, 5}));
    }

    {
        //7 There is a change peer operation on the copyset, with a target of 1
        Operator testOperator(1, CopySetKey{1, 6},
                          OperatorPriority::NormalPriority,
                          steady_clock::now(),
                          std::make_shared<ChangePeer>(4, 1));
        ASSERT_TRUE(coordinator->GetOpController()->AddOperator(testOperator));
        ASSERT_TRUE(coordinator->ChunkserverGoingToAdd(1, CopySetKey{1, 6}));
    }
}

TEST(CoordinatorTest, test_SchedulerSwitch) {
    auto topo = std::make_shared<MockTopology>();
    auto metric = std::make_shared<ScheduleMetrics>(topo);
    auto topoAdapter = std::make_shared<MockTopoAdapter>();
    auto coordinator = std::make_shared<Coordinator>(topoAdapter);
    ScheduleOption scheduleOption;
    scheduleOption.enableCopysetScheduler = true;
    scheduleOption.enableLeaderScheduler = true;
    scheduleOption.enableRecoverScheduler = true;
    scheduleOption.enableReplicaScheduler = true;
    scheduleOption.enableScanScheduler = true;
    scheduleOption.copysetSchedulerIntervalSec = 0;
    scheduleOption.leaderSchedulerIntervalSec = 0;
    scheduleOption.recoverSchedulerIntervalSec = 0;
    scheduleOption.replicaSchedulerIntervalSec = 0;
    scheduleOption.scanSchedulerIntervalSec = 0;
    scheduleOption.operatorConcurrent = 2;
    scheduleOption.transferLeaderTimeLimitSec = 1;
    scheduleOption.addPeerTimeLimitSec = 1;
    scheduleOption.removePeerTimeLimitSec = 1;
    scheduleOption.scanPeerTimeLimitSec = 1;
    coordinator->InitScheduler(scheduleOption, metric);

    EXPECT_CALL(*topoAdapter, GetCopySetInfos()).Times(0);
    EXPECT_CALL(*topoAdapter, GetLogicalpools()).Times(0);
    EXPECT_CALL(*topoAdapter, GetChunkServerInfos()).Times(0);

    // Set all switch flag to false
    gflags::SetCommandLineOption("enableCopySetScheduler", "false");
    gflags::SetCommandLineOption("enableLeaderScheduler", "false");
    gflags::SetCommandLineOption("enableReplicaScheduler", "false");
    gflags::SetCommandLineOption("enableRecoverScheduler", "false");
    gflags::SetCommandLineOption("enableScanScheduler", "false");

    coordinator->Run();
    ::sleep(1);
    coordinator->Stop();
}

TEST(CoordinatorTest, test_RapidLeaderSchedule) {
    auto topo = std::make_shared<MockTopology>();
    auto metric = std::make_shared<ScheduleMetrics>(topo);
    auto topoAdapter = std::make_shared<MockTopoAdapter>();
    auto coordinator = std::make_shared<Coordinator>(topoAdapter);

    ScheduleOption scheduleOption = GetScheduleOption();
    coordinator->InitScheduler(scheduleOption, metric);

    EXPECT_CALL(*topoAdapter, GetLogicalpools())
        .WillOnce(Return(std::vector<PoolIdType>{}));
    ASSERT_EQ(kScheduleErrCodeInvalidLogicalPool,
        coordinator->RapidLeaderSchedule(2));
}

TEST(CoordinatorTest, test_QueryChunkServerRecoverStatus) {
    /*
    Scenario:
    Chunkserver1: Offline has recovery op
    Chunkserver2: Offline has no recovery op, no candidate, and other ops
    Chunkserver3: Offline has candidate
    Chunkserver4: online
    Chunkserver4: online
    */
    auto topo = std::make_shared<MockTopology>();
    auto metric = std::make_shared<ScheduleMetrics>(topo);
    auto topoAdapter = std::make_shared<MockTopoAdapter>();
    auto coordinator = std::make_shared<Coordinator>(topoAdapter);

    //Get option
    ScheduleOption scheduleOption = GetScheduleOption();
    coordinator->InitScheduler(scheduleOption, metric);

    //Construct chunkserver
    std::vector<ChunkServerInfo> chunkserverInfos;
    std::vector<PeerInfo> peerInfos;
    for (int i = 1; i <= 6; i++) {
        PeerInfo peer(i, i % 3 + 1, i, "192.168.0." + std::to_string(i), 9000);
        ChunkServerInfo csInfo(
            peer,
            OnlineState::ONLINE,
            DiskState::DISKNORMAL,
            ChunkServerStatus::READWRITE,
            1, 10, 1, ChunkServerStatisticInfo{});
        if (i <= 3) {
            csInfo.state = OnlineState::OFFLINE;
        }

        chunkserverInfos.emplace_back(csInfo);
        peerInfos.emplace_back(peer);
    }

    //Construct op
    Operator opForCopySet1(
        1, CopySetKey{1, 1},
        OperatorPriority::HighPriority,
        steady_clock::now(),
        std::make_shared<ChangePeer>(1, 4));
    ASSERT_TRUE(coordinator->GetOpController()->AddOperator(opForCopySet1));

    Operator opForCopySet2(
        2, CopySetKey{1, 2},
        OperatorPriority::NormalPriority,
        steady_clock::now(),
        std::make_shared<TransferLeader>(2, 4));
    ASSERT_TRUE(coordinator->GetOpController()->AddOperator(opForCopySet2));

    //Construct a copyset
    std::vector<PeerInfo> peersFor2({peerInfos[1], peerInfos[3], peerInfos[4]});
    CopySetInfo copyset2(
        CopySetKey{1, 2}, 1, 4,
        peersFor2,
        ConfigChangeInfo{},
        CopysetStatistics{});

    std::vector<PeerInfo> peersFor3({peerInfos[2], peerInfos[3], peerInfos[4]});
    ConfigChangeInfo configChangeInfoForCS3;
    auto replica = new ::curve::common::Peer();
    replica->set_id(6);
    replica->set_address("192.168.0.6:9000:0");
    configChangeInfoForCS3.set_allocated_peer(replica);
    configChangeInfoForCS3.set_type(ConfigChangeType::CHANGE_PEER);
    configChangeInfoForCS3.set_finished(true);
    CopySetInfo copyset3(
        CopySetKey{1, 3}, 1, 4,
        peersFor3,
        configChangeInfoForCS3,
        CopysetStatistics{});

    //1 Query all chunkservers
    {
        EXPECT_CALL(*topoAdapter, GetChunkServerInfos())
            .WillOnce(Return(chunkserverInfos));
        EXPECT_CALL(*topoAdapter, GetCopySetInfosInChunkServer(2))
            .WillOnce(Return(std::vector<CopySetInfo>{copyset2}));
        EXPECT_CALL(*topoAdapter, GetCopySetInfosInChunkServer(3))
            .WillOnce(Return(std::vector<CopySetInfo>{copyset3}));

        std::map<ChunkServerIdType, bool> statusMap;
        ASSERT_EQ(kScheduleErrCodeSuccess,
            coordinator->QueryChunkServerRecoverStatus(
            std::vector<ChunkServerIdType>{}, &statusMap));
        ASSERT_EQ(6, statusMap.size());
        ASSERT_TRUE(statusMap[1]);
        ASSERT_FALSE(statusMap[2]);
        ASSERT_TRUE(statusMap[3]);
        ASSERT_FALSE(statusMap[4]);
        ASSERT_FALSE(statusMap[5]);
        ASSERT_FALSE(statusMap[6]);
    }

    //2 Query for specified chunkserver, but chunkserver does not exist
    {
        EXPECT_CALL(*topoAdapter, GetChunkServerInfo(7, _))
            .WillOnce(Return(false));

        std::map<ChunkServerIdType, bool> statusMap;
        ASSERT_EQ(kScheduleErrInvalidQueryChunkserverID,
            coordinator->QueryChunkServerRecoverStatus(
            std::vector<ChunkServerIdType>{7}, &statusMap));
    }

    //3 Query the specified chunkserver, not in recovery
    {
        EXPECT_CALL(*topoAdapter, GetChunkServerInfo(6, _))
            .WillOnce(DoAll(SetArgPointee<1>(chunkserverInfos[5]),
                            Return(true)));
        std::map<ChunkServerIdType, bool> statusMap;
        ASSERT_EQ(kScheduleErrCodeSuccess,
            coordinator->QueryChunkServerRecoverStatus(
            std::vector<ChunkServerIdType>{6}, &statusMap));
        ASSERT_EQ(1, statusMap.size());
        ASSERT_FALSE(statusMap[6]);
    }
}

}  // namespace schedule
}  // namespace mds
}  // namespace curve

