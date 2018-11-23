/*
 * Project: curve
 * Created Date: Tue Dec 25 2018
 * Author: lixiaocui
 * Copyright (c) 2018 netease
 */

#include <glog/logging.h>
#include "src/mds/schedule/coordinator.h"
#include "test/mds/schedule/mock_topoAdapter.h"
#include "test/mds/schedule/common.h"

using ::curve::mds::schedule::ScheduleConfig;

namespace curve {
namespace mds {
namespace schedule {
TEST(CoordinatorTest, test_copySet_heartbeat) {
    auto topoAdapter = std::make_shared<MockTopoAdapter>();
    auto coordinator = std::make_shared<Coordinator>(topoAdapter);
    ScheduleConfig scheduleConfig(true, true, true, true, 10, 10, 10, 10, 2);
    coordinator->InitScheduler(scheduleConfig);
    auto testCopySetInfo = GetCopySetInfoForTest();
    EpochType startEpoch = 1;
    CopySetKey copySetKey;
    copySetKey.first = 1;
    copySetKey.second = 1;
    Operator testOperator(startEpoch, copySetKey,
                          OperatorPriority::NormalPriority,
                          steady_clock::now(), std::make_shared<AddPeer>(4));
    testOperator.timeLimit = std::chrono::seconds(100);

    // 1. test copySet do not have operator
    CopySetConf res;
    ASSERT_FALSE(coordinator->CopySetHeartbeat(testCopySetInfo, &res));

    // 2. test copySet has operator and not execute
    coordinator->GetOpController()->AddOperator(testOperator);
    Operator opRes;
    ASSERT_TRUE(coordinator->GetOpController()->GetOperatorById(
        testCopySetInfo.id, &opRes));
    ASSERT_TRUE(coordinator->CopySetHeartbeat(testCopySetInfo, &res));
    ASSERT_EQ(4, res.configChangeItem);
    ASSERT_EQ(ConfigChangeType::ADD_PEER, res.type);

    // 3. test op executing and not finish
    testCopySetInfo.candidatePeerInfo = PeerInfo(4, 1, 1, "", 9000);
    testCopySetInfo.configChangeInfo.set_finished(false);
    testCopySetInfo.configChangeInfo.set_peer("192.168.10.4:9000");
    ASSERT_FALSE(coordinator->CopySetHeartbeat(testCopySetInfo, &res));

    // 4. test op success success
    testCopySetInfo.configChangeInfo.set_finished(true);
    testCopySetInfo.peers.emplace_back(PeerInfo(4, 4, 4, "192.10.123.1", 9000));
    ASSERT_FALSE(coordinator->CopySetHeartbeat(testCopySetInfo, &res));
}
}  // namespace schedule
}  // namespace mds
}  // namespace curve

