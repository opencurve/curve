/*
 * Project: curve
 * Created Date: Tue Dec 25 2018
 * Author: lixiaocui
 * Copyright (c) 2018 netease
 */

#include <gtest/gtest.h>
#include "src/mds/schedule/operatorFactory.h"
#include "test/mds/schedule/common.h"

namespace curve {
namespace mds {
namespace schedule {
TEST(OperatorFactoryTest, test_transfer_leader_operator) {
    auto copySetInfo = GetCopySetInfoForTest();
    auto resOp = operatorFactory.CreateTransferLeaderOperator(
        copySetInfo, 2, OperatorPriority::NormalPriority);
    ASSERT_TRUE(dynamic_cast<TransferLeader *>(resOp.step.get()) != nullptr);
}

TEST(OperatorFactoryTest, test_remove_peer) {
    auto copySetInfo = GetCopySetInfoForTest();
    auto resOp = operatorFactory.CreateRemovePeerOperator(
        copySetInfo, 1, OperatorPriority::NormalPriority);
    ASSERT_TRUE(dynamic_cast<RemovePeer *>(resOp.step.get()) != nullptr);
}

TEST(OperatorFactoryTest, test_add_peer) {
    auto copySetInfo = GetCopySetInfoForTest();
    auto resOp = operatorFactory.CreateAddPeerOperator(
        copySetInfo, 4, OperatorPriority::NormalPriority);
    ASSERT_TRUE(dynamic_cast<AddPeer *>(resOp.step.get()) != nullptr);
}

TEST(OperatorFactoryTest, test_change_peer) {
    auto copySetInfo = GetCopySetInfoForTest();
    auto resOp = operatorFactory.CreateChangePeerOperator(
        copySetInfo, 3, 4, OperatorPriority::NormalPriority);
    ASSERT_TRUE(dynamic_cast<ChangePeer *>(resOp.step.get()) != nullptr);
}
}  // namespace schedule
}  // namespace mds
}  // namespace curve

