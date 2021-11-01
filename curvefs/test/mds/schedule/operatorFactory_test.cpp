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

#include <gtest/gtest.h>
#include "curvefs/src/mds/schedule/operatorFactory.h"
#include "curvefs/test/mds/schedule/common.h"

namespace curvefs {
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
}  // namespace curvefs

