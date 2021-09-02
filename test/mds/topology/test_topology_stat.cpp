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
 * Created Date: Fri Jun 28 2019
 * Author: xuchaojie
 */

#include <gtest/gtest.h>
#include <gmock/gmock.h>


#include "test/mds/mock/mock_topology.h"

#include "src/mds/topology/topology_stat.h"


namespace curve {
namespace mds {
namespace topology {


using ::testing::Return;
using ::testing::_;
using ::testing::AnyOf;
using ::testing::SetArgPointee;
using ::testing::Invoke;

class TestTopologyStat : public ::testing::Test {
 public:
     TestTopologyStat() {}

     virtual void SetUp() {
         topology_ = std::make_shared<MockTopology>();
         testObj_ = std::make_shared<TopologyStatImpl>(topology_);
     }

     virtual void TearDown() {
         topology_ = nullptr;
         testObj_  = nullptr;
     }

 protected:
    std::shared_ptr<MockTopology> topology_;
    std::shared_ptr<TopologyStatImpl> testObj_;
};

TEST_F(TestTopologyStat, TestUpdateAndGetChunkServerStat) {
    ChunkServerStat stat1, stat2, stat3;
    CopysetStat cstat1, cstat2, cstat3;
    stat1.leaderCount = 1;
    stat1.copysetCount = 1;
    stat1.readRate = 1;
    stat1.writeRate = 1;
    stat1.readIOPS = 1;
    stat1.writeIOPS = 1;
    cstat1.logicalPoolId = 1;
    cstat1.copysetId = 1;
    cstat1.readRate = 1;
    cstat1.writeRate = 1;
    cstat1.readIOPS = 1;
    cstat1.writeIOPS = 1;
    stat1.copysetStats.push_back(cstat1);
    stat2.leaderCount = 2;
    stat2.copysetCount = 2;
    stat2.readRate = 2;
    stat2.writeRate = 2;
    stat2.readIOPS = 2;
    stat2.writeIOPS = 2;
    cstat2.logicalPoolId = 2;
    cstat2.copysetId = 2;
    cstat2.readRate = 2;
    cstat2.writeRate = 2;
    cstat2.readIOPS = 2;
    cstat2.writeIOPS = 2;
    stat2.copysetStats.push_back(cstat2);
    stat3.leaderCount = 3;
    stat3.copysetCount = 3;
    stat3.readRate = 3;
    stat3.writeRate = 3;
    stat3.readIOPS = 3;
    stat3.writeIOPS = 3;
    cstat3.logicalPoolId = 3;
    cstat3.copysetId = 3;
    cstat3.readRate = 3;
    cstat3.writeRate = 3;
    cstat3.readIOPS = 3;
    cstat3.writeIOPS = 3;
    stat3.copysetStats.push_back(cstat3);

    testObj_->UpdateChunkServerStat(1, stat1);
    testObj_->UpdateChunkServerStat(1, stat2);

    bool ret = testObj_->GetChunkServerStat(1, &stat3);
    ASSERT_TRUE(ret);
    ASSERT_EQ(2, stat3.leaderCount);
    ASSERT_EQ(2, stat3.copysetCount);
    ASSERT_EQ(2, stat3.readRate);
    ASSERT_EQ(2, stat3.writeRate);
    ASSERT_EQ(2, stat3.readIOPS);
    ASSERT_EQ(2, stat3.writeIOPS);
    ASSERT_EQ(1, stat3.copysetStats.size());
    ASSERT_EQ(2, stat3.copysetStats[0].logicalPoolId);
    ASSERT_EQ(2, stat3.copysetStats[0].copysetId);
    ASSERT_EQ(2, stat3.copysetStats[0].readRate);
    ASSERT_EQ(2, stat3.copysetStats[0].writeRate);
    ASSERT_EQ(2, stat3.copysetStats[0].readIOPS);
    ASSERT_EQ(2, stat3.copysetStats[0].writeIOPS);
}

}  // namespace topology
}  // namespace mds
}  // namespace curve
