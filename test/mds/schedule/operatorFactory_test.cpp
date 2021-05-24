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

TEST(OperatorFactoryTest, TestScanPeer) {
    auto copysetInfo = GetCopySetInfoForTest();
    auto op = operatorFactory.CreateScanPeerOperator(
        copysetInfo,
        1,
        OperatorPriority::LowPriority,
        ConfigChangeType::START_SCAN_PEER);
    ASSERT_TRUE(dynamic_cast<ScanPeer*>(op.step.get()) != nullptr);
}
}  // namespace schedule
}  // namespace mds
}  // namespace curve

