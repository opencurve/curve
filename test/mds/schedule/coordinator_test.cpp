/*
 * Project: curve
 * Created Date: Tue Dec 25 2018
 * Author: lixiaocui
 * Copyright (c) 2018 netease
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
        // 第一次下发配置
        ASSERT_EQ(4, coordinator->CopySetHeartbeat(
            testCopySetInfo, ConfigChangeInfo{}, &res));
        ASSERT_EQ("127.0.0.1:9000:0", res.configchangeitem().address());
        ASSERT_EQ(ConfigChangeType::ADD_PEER, res.type());

        // 第二次获取chunkserver失败
        ASSERT_EQ(UNINTIALIZE_ID, coordinator->CopySetHeartbeat(
            testCopySetInfo, ConfigChangeInfo{}, &res));
    }

    {
        // 3. 下发配置，但candidate是offline状态
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

        // 获取不到chunkserver的信息
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
        // 第一次下发配置
        ASSERT_EQ(4, coordinator->CopySetHeartbeat(
            testCopySetInfo, ConfigChangeInfo{}, &res));
        ASSERT_EQ("127.0.0.1:9000:0", res.configchangeitem().address());
        ASSERT_EQ("127.0.0.1:9001:0", res.oldpeer().address());
        ASSERT_EQ(ConfigChangeType::CHANGE_PEER, res.type());

        // 第二次获取chunkserver失败
        ASSERT_EQ(UNINTIALIZE_ID, coordinator->CopySetHeartbeat(
            testCopySetInfo, ConfigChangeInfo{}, &res));
    }

    {
        // 3. 下发配置，但candidate是offline状态
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

        // 获取不到chunkserver的信息
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
        // 1. copyset上没有要变更的operator
        ASSERT_FALSE(coordinator->ChunkserverGoingToAdd(1, CopySetKey{1, 1}));
    }

    {
        // 2. copyset上有leader变更，并且目的leader为chunkserver-1
        Operator testOperator(1, CopySetKey{1, 1},
                          OperatorPriority::NormalPriority,
                          steady_clock::now(),
                          std::make_shared<TransferLeader>(2, 1));
        ASSERT_TRUE(coordinator->GetOpController()->AddOperator(testOperator));
        ASSERT_FALSE(coordinator->ChunkserverGoingToAdd(1, CopySetKey{1, 1}));
    }

    {
        // 3. copyset上有remove peer操作
        Operator testOperator(1, CopySetKey{1, 2},
                          OperatorPriority::NormalPriority,
                          steady_clock::now(),
                          std::make_shared<RemovePeer>(1));
        ASSERT_TRUE(coordinator->GetOpController()->AddOperator(testOperator));
        ASSERT_FALSE(coordinator->ChunkserverGoingToAdd(1, CopySetKey{1, 2}));
    }

    {
        // 4. copyset上有add peer操作, target不是1
        Operator testOperator(1, CopySetKey{1, 3},
                          OperatorPriority::NormalPriority,
                          steady_clock::now(),
                          std::make_shared<AddPeer>(2));
        ASSERT_TRUE(coordinator->GetOpController()->AddOperator(testOperator));
        ASSERT_FALSE(coordinator->ChunkserverGoingToAdd(1, CopySetKey{1, 3}));
    }

    {
        // 5. copyset上有add peer操作, target是1
        Operator testOperator(1, CopySetKey{1, 4},
                          OperatorPriority::NormalPriority,
                          steady_clock::now(),
                          std::make_shared<AddPeer>(1));
        ASSERT_TRUE(coordinator->GetOpController()->AddOperator(testOperator));
        ASSERT_TRUE(coordinator->ChunkserverGoingToAdd(1, CopySetKey{1, 4}));
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
    scheduleOption.copysetSchedulerIntervalSec = 0;
    scheduleOption.leaderSchedulerIntervalSec = 0;
    scheduleOption.recoverSchedulerIntervalSec = 0;
    scheduleOption.replicaSchedulerIntervalSec = 0;
    scheduleOption.operatorConcurrent = 2;
    scheduleOption.transferLeaderTimeLimitSec = 1;
    scheduleOption.addPeerTimeLimitSec = 1;
    scheduleOption.removePeerTimeLimitSec = 1;
    coordinator->InitScheduler(scheduleOption, metric);

    EXPECT_CALL(*topoAdapter, GetCopySetInfos()).Times(0);
    EXPECT_CALL(*topoAdapter, GetLogicalpools()).Times(0);
    EXPECT_CALL(*topoAdapter, GetChunkServerInfos()).Times(0);

    // 设置flag都为false
    gflags::SetCommandLineOption("enableCopySetScheduler", "false");
    gflags::SetCommandLineOption("enableLeaderScheduler", "false");
    gflags::SetCommandLineOption("enableReplicaScheduler", "false");
    gflags::SetCommandLineOption("enableRecoverScheduler", "false");

    coordinator->Run();
    ::sleep(1);
    coordinator->Stop();
}
}  // namespace schedule
}  // namespace mds
}  // namespace curve

