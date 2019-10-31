/*
 * Project: curve
 * Created Date: Sun Dec 23 2018
 * Author: lixiaocui
 * Copyright (c) 2018 netease
 */

#include <glog/logging.h>
#include <gtest/gtest.h>
#include "test/mds/schedule/common.h"
#include "src/mds/schedule/operatorController.h"
#include "src/mds/schedule/operator.h"
#include "src/mds/schedule/scheduleMetrics.h"
#include "test/mds/mock/mock_topology.h"

using ::std::chrono::steady_clock;
using ::curve::mds::schedule::OperatorStep;
using ::curve::mds::schedule::Operator;
using ::curve::mds::schedule::OperatorPriority;
using ::curve::mds::schedule::OperatorController;
using ::curve::mds::topology::MockTopology;

namespace curve {
namespace mds {
namespace schedule {
TEST(OperatorControllerTest, test_AddGetRemove) {
    auto topo = std::make_shared<MockTopology>();
    auto opController = std::make_shared<OperatorController>(
        2, std::make_shared<ScheduleMetrics>(topo));

    CopySetKey copySetKey;
    copySetKey.first = 1;
    copySetKey.second = 2;
    Operator testOperator(1, copySetKey, OperatorPriority::NormalPriority,
                          steady_clock::now(), std::make_shared<AddPeer>(1));
    // 1. test add operator
    // add operator success
    ASSERT_TRUE(opController->AddOperator(testOperator));
    // add duplicate operator fail
    ASSERT_FALSE(opController->AddOperator(testOperator));
    // add higher operator success
    testOperator.priority = OperatorPriority::HighPriority;
    ASSERT_TRUE(opController->AddOperator(testOperator));
    // add lower operator fail
    testOperator.priority = OperatorPriority::LowPriority;
    ASSERT_FALSE(opController->AddOperator(testOperator));
    // add operator exceed concurrent
    testOperator.priority = OperatorPriority::LowPriority;
    testOperator.copysetID.first = 2;
    ASSERT_TRUE(opController->AddOperator(testOperator));
    testOperator.copysetID.first = 3;
    ASSERT_FALSE(opController->AddOperator(testOperator));
    // add higher operator with critical-concurrent
    testOperator.priority = OperatorPriority::HighPriority;
    testOperator.copysetID.first = 2;
    ASSERT_TRUE(opController->AddOperator(testOperator));

    // 2. test get operator
    // get all operators
    auto opList = opController->GetOperators();
    ASSERT_EQ(2, opList.size());
    // get operator by id
    copySetKey.first = 1;
    Operator op;
    ASSERT_TRUE(opController->GetOperatorById(copySetKey, &op));
    ASSERT_EQ(OperatorPriority::HighPriority, op.priority);
    ASSERT_TRUE(static_cast<AddPeer *>(op.step.get()));
    ASSERT_EQ(1, op.startEpoch);
    copySetKey.first = 2;
    ASSERT_TRUE(opController->GetOperatorById(copySetKey, &op));
    ASSERT_EQ(OperatorPriority::HighPriority, op.priority);
    ASSERT_TRUE(static_cast<AddPeer *>(op.step.get()));
    ASSERT_EQ(1, op.startEpoch);
    copySetKey.first = 3;
    ASSERT_FALSE(opController->GetOperatorById(copySetKey, &op));

    // 3. test remove operator
    opController->RemoveOperator(copySetKey);
    ASSERT_FALSE(opController->GetOperatorById(copySetKey, &op));
}

TEST(OperatorControllerTest, test_ApplyOp) {
    auto topo = std::make_shared<MockTopology>();
    auto opController = std::make_shared<OperatorController>(
        2, std::make_shared<ScheduleMetrics>(topo));

    CopySetKey copySetKey;
    copySetKey.first = 1;
    copySetKey.second = 2;
    Operator testOperator(1, copySetKey, OperatorPriority::NormalPriority,
                          steady_clock::now(), std::make_shared<AddPeer>(5));
    testOperator.timeLimit = std::chrono::seconds(10);
    testOperator.createTime -= std::chrono::seconds(15);
    ASSERT_TRUE(opController->AddOperator(testOperator));

    // 1. test apply operator not exist
    auto originCopySetinfo = GetCopySetInfoForTest();
    CopySetConf copySetConf;
    ASSERT_FALSE(opController->ApplyOperator(originCopySetinfo, &copySetConf));

    // 2. test apply operator timeout
    originCopySetinfo.id.second = 2;
    ASSERT_FALSE(opController->ApplyOperator(originCopySetinfo, &copySetConf));

    // 3. test apply operator finished
    testOperator.createTime = steady_clock::now();
    originCopySetinfo.peers.emplace_back(PeerInfo(5, 1, 1, "", 9000));
    ASSERT_FALSE(opController->ApplyOperator(originCopySetinfo, &copySetConf));

    // 4. test apply operator failed
    originCopySetinfo.candidatePeerInfo = PeerInfo(5, 1, 1, "", 9000);
    originCopySetinfo.configChangeInfo.set_finished(false);
    auto replica = new ::curve::common::Peer();
    replica->set_id(5);
    replica->set_address("192.168.10.5:9000:0");
    originCopySetinfo.configChangeInfo.set_allocated_peer(replica);
    std::string *errMsg = new std::string("execute operator err");
    CandidateError *candidateError = new CandidateError();
    candidateError->set_allocated_errmsg(errMsg);
    originCopySetinfo.configChangeInfo.set_allocated_err(candidateError);
    ASSERT_FALSE(opController->ApplyOperator(originCopySetinfo, &copySetConf));
    Operator op;
    ASSERT_FALSE(opController->GetOperatorById(originCopySetinfo.id, &op));

    // 5. test apply operator finished
    ASSERT_TRUE(opController->AddOperator(testOperator));
    originCopySetinfo.configChangeInfo.release_err();
    originCopySetinfo.configChangeInfo.set_finished(true);
    originCopySetinfo.configChangeInfo.set_type(ConfigChangeType::ADD_PEER);
    originCopySetinfo.peers.emplace_back(
        PeerInfo(5, 4, 4, "192.168.10.1", 9000));
    ASSERT_FALSE(opController->ApplyOperator(originCopySetinfo, &copySetConf));
    ASSERT_FALSE(opController->GetOperatorById(originCopySetinfo.id, &op));

    // 6. test apply operator start
    ASSERT_TRUE(opController->AddOperator(testOperator));
    originCopySetinfo = GetCopySetInfoForTest();
    originCopySetinfo.id.second = 2;
    ASSERT_TRUE(opController->ApplyOperator(originCopySetinfo, &copySetConf));
    ASSERT_EQ(5, copySetConf.configChangeItem);

    // 7. test apply operator not finished
    originCopySetinfo.candidatePeerInfo = PeerInfo(5, 1, 1, "", 9000);
    originCopySetinfo.configChangeInfo.set_finished(false);
    originCopySetinfo.configChangeInfo.set_type(ConfigChangeType::ADD_PEER);
    auto replica1 = new ::curve::common::Peer();
    replica1->set_id(5);
    replica1->set_address("192.168.10.5:9000:0");
    originCopySetinfo.configChangeInfo.set_allocated_peer(replica1);
    ASSERT_FALSE(opController->ApplyOperator(originCopySetinfo, &copySetConf));
    originCopySetinfo.configChangeInfo.Clear();
}
}  // namespace schedule
}  // namespace mds
}  // namespace curve
