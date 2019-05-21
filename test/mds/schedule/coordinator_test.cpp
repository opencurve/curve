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
#include "test/mds/schedule/common.h"

using ::curve::mds::schedule::ScheduleOption;
using ::testing::Return;
using ::testing::SetArgPointee;
using ::testing::DoAll;
using ::testing::_;

using ::curve::mds::topology::UNINTIALIZE_ID;

namespace curve {
namespace mds {
namespace schedule {
TEST(CoordinatorTest, test_copySet_heartbeat) {
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
    coordinator->InitScheduler(scheduleOption);

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
    PeerInfo peer(4, 1, 1, 1, "127.0.0.1", 9000);
    ChunkServerInfo csInfo(peer, OnlineState::ONLINE, DiskState::DISKNORMAL,
                           1, 10, 1, 0, ChunkServerStatisticInfo{});

    // 1. test copySet do not have operator
    EXPECT_CALL(*topoAdapter, CopySetFromTopoToSchedule(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(info), Return(true)));
    ::curve::mds::heartbeat::CopySetConf res;
    ASSERT_TRUE(UNINTIALIZE_ID ==
        coordinator->CopySetHeartbeat(testCopySetInfo, &res));

    // 2. test copySet has operator and not execute
    EXPECT_CALL(*topoAdapter, CopySetFromTopoToSchedule(_, _))
        .Times(2).WillOnce(DoAll(SetArgPointee<1>(info), Return(true)))
                 .WillOnce(DoAll(SetArgPointee<1>(info), Return(true)));
    EXPECT_CALL(*topoAdapter, GetChunkServerInfo(_, _))
        .Times(2).WillOnce(DoAll(SetArgPointee<1>(csInfo), Return(true)))
                 .WillOnce(Return(false));
    coordinator->GetOpController()->AddOperator(testOperator);
    Operator opRes;
    ASSERT_TRUE(coordinator->GetOpController()->GetOperatorById(
        info.id, &opRes));
    ASSERT_FALSE(UNINTIALIZE_ID ==
        coordinator->CopySetHeartbeat(testCopySetInfo, &res));
    ASSERT_EQ("127.0.0.1:9000:0", res.configchangeitem().address());
    ASSERT_EQ(ConfigChangeType::ADD_PEER, res.type());

    ASSERT_FALSE(coordinator->CopySetHeartbeat(testCopySetInfo, &res));

    // 3. test op executing and not finish
    info.candidatePeerInfo = PeerInfo(4, 1, 1, 1, "", 9000);
    info.configChangeInfo.set_finished(false);
    info.configChangeInfo.set_type(ConfigChangeType::ADD_PEER);
    auto replica = new ::curve::common::Peer();
    replica->set_id(4);
    replica->set_address("192.168.10.4:9000:0");
    info.configChangeInfo.set_allocated_peer(replica);
    EXPECT_CALL(*topoAdapter, CopySetFromTopoToSchedule(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(info), Return(true)));
    ASSERT_FALSE(coordinator->CopySetHeartbeat(testCopySetInfo, &res));

    // 4. test op success
    info.configChangeInfo.set_finished(true);
    info.peers.emplace_back(PeerInfo(4, 4, 4, 1, "192.10.123.1", 9000));
    EXPECT_CALL(*topoAdapter, CopySetFromTopoToSchedule(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(info), Return(true)));
    ASSERT_FALSE(coordinator->CopySetHeartbeat(testCopySetInfo, &res));

    // 5. test transfer copysetInfo err
    EXPECT_CALL(*topoAdapter, CopySetFromTopoToSchedule(_, _))
        .WillOnce(Return(false));
    ASSERT_FALSE(coordinator->CopySetHeartbeat(testCopySetInfo, &res));
}
}  // namespace schedule
}  // namespace mds
}  // namespace curve

