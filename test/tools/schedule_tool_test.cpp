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
 * File Created: 2021-01-04
 * Author: charisu
 */

#include <gtest/gtest.h>
#include "src/tools/schedule_tool.h"
#include "test/tools/mock/mock_mds_client.h"

using ::testing::_;
using ::testing::Return;
using ::testing::DoAll;
using ::testing::SetArgPointee;
using curve::mds::topology::LogicalPoolType;
using curve::mds::topology::AllocateStatus;

DECLARE_int32(logical_pool_id);
DECLARE_bool(scheduleAll);

namespace curve {
namespace tool {

class ScheduleToolTest : public ::testing::Test {
 protected:
    void SetUp() {
        client_ = std::make_shared<curve::tool::MockMDSClient>();
    }
    void TearDown() {
        client_ = nullptr;
    }

    void GetLogicalPoolForTest(PoolIdType id,
                        curve::mds::topology::LogicalPoolInfo *lpInfo) {
        lpInfo->set_logicalpoolid(id);
        lpInfo->set_logicalpoolname("defaultLogicalPool");
        lpInfo->set_physicalpoolid(1);
        lpInfo->set_type(LogicalPoolType::PAGEFILE);
        lpInfo->set_createtime(1574218021);
        lpInfo->set_redundanceandplacementpolicy(
            "{\"zoneNum\": 3, \"copysetNum\": 4000, \"replicaNum\": 3}");
        lpInfo->set_userpolicy("{\"policy\": 1}");
        lpInfo->set_allocatestatus(AllocateStatus::ALLOW);
    }

    std::shared_ptr<curve::tool::MockMDSClient> client_;
};

TEST_F(ScheduleToolTest, ScheduleAll) {
    ScheduleTool scheduleTool(client_);
    std::vector<LogicalPoolInfo> pools;
    for (int i = 1; i <= 3; ++i) {
        LogicalPoolInfo pool;
        GetLogicalPoolForTest(i, &pool);
        pools.emplace_back(pool);
    }
    EXPECT_CALL(*client_, Init(_))
        .Times(1)
        .WillOnce(Return(0));
    EXPECT_CALL(*client_, ListLogicalPoolsInCluster(_))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<0>(pools),
                        Return(0)));
    EXPECT_CALL(*client_, RapidLeaderSchedule(_))
        .Times(3)
        .WillOnce(Return(-1))
        .WillRepeatedly(Return(0));
    // common
    ASSERT_EQ(-1, scheduleTool.RunCommand(kRapidLeaderSchedule));
}

TEST_F(ScheduleToolTest, ScheduleOne) {
    ScheduleTool scheduleTool(client_);
    ASSERT_TRUE(scheduleTool.SupportCommand(kRapidLeaderSchedule));
    ASSERT_FALSE(scheduleTool.SupportCommand("wrong-command"));
    scheduleTool.PrintHelp(kRapidLeaderSchedule);
    scheduleTool.PrintHelp("wrong-command");
    EXPECT_CALL(*client_, Init(_))
        .Times(2)
        .WillRepeatedly(Return(0));
    EXPECT_CALL(*client_, RapidLeaderSchedule(_))
        .Times(2)
        .WillOnce(Return(0))
        .WillOnce(Return(-1));
    FLAGS_scheduleAll = false;
    // common
    ASSERT_EQ(0, scheduleTool.RunCommand(kRapidLeaderSchedule));
    // rapid leader schedule fail
    ASSERT_EQ(-1, scheduleTool.RunCommand(kRapidLeaderSchedule));
}

}  // namespace tool
}  // namespace curve
