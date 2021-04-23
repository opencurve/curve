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
 * File Created: 2021-02-04
 * Author: charisu
 */

#include <gtest/gtest.h>
#include "src/tools/copyset_tool.h"
#include "test/tools/mock/mock_copyset_check_core.h"
#include "test/tools/mock/mock_mds_client.h"

using ::testing::_;
using ::testing::Return;
using ::testing::ReturnRef;
using ::testing::DoAll;
using ::testing::SetArgPointee;

DECLARE_bool(dryrun);
DECLARE_bool(availFlag);

namespace curve {
namespace tool {

class CopysetToolTest : public ::testing::Test {
 protected:
    void SetUp() {
        copysetCheck_ = std::make_shared<MockCopysetCheckCore>();
        mdsClient_ = std::make_shared<MockMDSClient>();
        FLAGS_dryrun = true;
    }
    void TearDown() {
        copysetCheck_ = nullptr;
        mdsClient_ = nullptr;
    }
    std::shared_ptr<MockCopysetCheckCore> copysetCheck_;
    std::shared_ptr<MockMDSClient> mdsClient_;
};

TEST_F(CopysetToolTest, SetCopysetsUnAvailable) {
    FLAGS_availFlag = false;
    CopysetTool copysetTool(copysetCheck_, mdsClient_);
    ASSERT_EQ(-1, copysetTool.RunCommand("test"));
    copysetTool.PrintHelp(kSetCopysetAvailFlag);
    // check copysets fail
    EXPECT_CALL(*copysetCheck_, CheckCopysetsOnOfflineChunkServer())
        .Times(5)
        .WillOnce(Return(-1))
        .WillRepeatedly(Return(0));
    ASSERT_EQ(-1, copysetTool.RunCommand(kSetCopysetAvailFlag));
    // copysets empty
    std::vector<CopysetInfo> copysets;
    CopysetInfo copyset;
    copyset.set_logicalpoolid(1);
    copyset.set_copysetid(10);
    std::vector<CopysetInfo> copysets2 = {copyset};
    EXPECT_CALL(*copysetCheck_, GetCopysetInfos(_, _))
        .Times(4)
        .WillOnce(SetArgPointee<1>(copysets))
        .WillRepeatedly(SetArgPointee<1>(copysets2));
    EXPECT_CALL(*mdsClient_, SetCopysetsAvailFlag(_, _))
        .Times(2)
        .WillOnce(Return(-1))
        .WillOnce(Return(0));
    ASSERT_EQ(0, copysetTool.RunCommand(kSetCopysetAvailFlag));
    ASSERT_EQ(0, copysetTool.RunCommand(kSetCopysetAvailFlag));
    FLAGS_dryrun = false;
    // set copysets availFlag fail
    ASSERT_EQ(-1, copysetTool.RunCommand(kSetCopysetAvailFlag));
    // success
    ASSERT_EQ(0, copysetTool.RunCommand(kSetCopysetAvailFlag));
}

TEST_F(CopysetToolTest, SetCopysetsAvailable) {
    FLAGS_availFlag = true;
    CopysetTool copysetTool(copysetCheck_, mdsClient_);
    EXPECT_CALL(*copysetCheck_, Init(_))
        .Times(1)
        .WillOnce(Return(0));
    std::vector<CopysetInfo> copysets;
    CopysetInfo copyset;
    copyset.set_logicalpoolid(1);
    copyset.set_copysetid(10);
    std::vector<CopysetInfo> copysets2 = {copyset};
    // list unavail copysets fail
    EXPECT_CALL(*mdsClient_, ListUnAvailCopySets(_))
        .Times(6)
        .WillOnce(Return(-1))
        .WillOnce(DoAll(SetArgPointee<0>(copysets), Return(0)))
        .WillRepeatedly(DoAll(SetArgPointee<0>(copysets2), Return(0)));
    ASSERT_EQ(-1, copysetTool.RunCommand(kSetCopysetAvailFlag));
    // copysets empty
    ASSERT_EQ(0, copysetTool.RunCommand(kSetCopysetAvailFlag));
    EXPECT_CALL(*copysetCheck_, CheckOneCopyset(_, _))
        .Times(4)
        .WillOnce(Return(CheckResult::kMajorityPeerNotOnline))
        .WillRepeatedly(Return(CheckResult::kHealthy));
    ASSERT_EQ(0, copysetTool.RunCommand(kSetCopysetAvailFlag));
    // dryrun normal
    ASSERT_EQ(0, copysetTool.RunCommand(kSetCopysetAvailFlag));
    FLAGS_dryrun = false;
    // set copysets availFlag fail
    EXPECT_CALL(*mdsClient_, SetCopysetsAvailFlag(_, _))
        .WillOnce(Return(-1))
        .WillOnce(Return(0));
    ASSERT_EQ(-1, copysetTool.RunCommand(kSetCopysetAvailFlag));
    // success
    ASSERT_EQ(0, copysetTool.RunCommand(kSetCopysetAvailFlag));
}

}  // namespace tool
}  // namespace curve
