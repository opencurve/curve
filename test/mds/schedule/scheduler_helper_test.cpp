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
 * Created Date: Thur July 06th 2019
 * Author: lixiaocui
 */

#include <map>
#include "test/mds/schedule/common.h"
#include "test/mds/schedule/mock_topoAdapter.h"
#include "src/mds/schedule/scheduler_helper.h"

using ::testing::_;
using ::testing::Return;
using ::testing::SetArgPointee;
using ::testing::DoAll;

namespace curve {
namespace mds {
namespace schedule {
class TestSchedulerHelper : public ::testing::Test {
 protected:
    TestSchedulerHelper() {}
    ~TestSchedulerHelper() {}

    void SetUp() override {
        topoAdapter_ = std::make_shared<MockTopoAdapter>();
    }

    void TearDown() override {
        topoAdapter_ = nullptr;
    }

 protected:
    std::shared_ptr<MockTopoAdapter> topoAdapter_;
};

TEST_F(TestSchedulerHelper, test_SatisfyScatterWidth_target) {
    int minScatterWidth = 90;
    float scatterWidthRangePerent = 0.2;
    int maxScatterWidth = minScatterWidth * (1 + scatterWidthRangePerent);
    bool target = true;
    {
        //1 After the change, the minimum value was not reached, but it increased the scattter-width
        int oldValue = 10;
        int newValue = 13;
        ASSERT_TRUE(SchedulerHelper::SatisfyScatterWidth(target, oldValue,
            newValue, minScatterWidth, scatterWidthRangePerent));
    }
    {
        //2 After the change, the minimum value is not reached, and the scattter-width remains unchanged
        int oldValue = 10;
        int newValue = 10;
        ASSERT_FALSE(SchedulerHelper::SatisfyScatterWidth(target, oldValue,
            newValue, minScatterWidth, scatterWidthRangePerent));
    }
    {
        //3 After the change, the minimum value was not reached and the scattter-width decreased
        int oldValue = 10;
        int newValue = 8;
        ASSERT_FALSE(SchedulerHelper::SatisfyScatterWidth(target, oldValue,
            newValue, minScatterWidth, scatterWidthRangePerent));
    }
    {
        //4 Equal to minimum value after change
        int oldValue = minScatterWidth + 2;
        int newValue = minScatterWidth;
        ASSERT_TRUE(SchedulerHelper::SatisfyScatterWidth(target, oldValue,
            newValue, minScatterWidth, scatterWidthRangePerent));
    }
    {
        //5 After the change, it is greater than the minimum value and less than the maximum value
        int oldValue = minScatterWidth;
        int newValue = minScatterWidth + 2;
        ASSERT_TRUE(SchedulerHelper::SatisfyScatterWidth(target, oldValue,
            newValue, minScatterWidth, scatterWidthRangePerent));
    }
    {
        //6 Equal to maximum value after change
        int oldValue = maxScatterWidth - 2;
        int newValue = maxScatterWidth;
        ASSERT_TRUE(SchedulerHelper::SatisfyScatterWidth(target, oldValue,
            newValue, minScatterWidth, scatterWidthRangePerent));
    }
    {
        //7 After the change, it is greater than the maximum value and the scattter-width increases
        int oldValue = maxScatterWidth + 1;
        int newValue = maxScatterWidth + 2;
        ASSERT_FALSE(SchedulerHelper::SatisfyScatterWidth(target, oldValue,
            newValue, minScatterWidth, scatterWidthRangePerent));
    }
    {
        //8 After the change, it is greater than the maximum value, and the scattter-width remains unchanged
        int oldValue = maxScatterWidth + 2;
        int newValue = maxScatterWidth + 2;
        ASSERT_FALSE(SchedulerHelper::SatisfyScatterWidth(target, oldValue,
            newValue, minScatterWidth, scatterWidthRangePerent));
    }
    {
        //9 After the change is greater than the maximum value, the scattter-width decreases
        int oldValue = maxScatterWidth + 3;
        int newValue = maxScatterWidth + 2;
        ASSERT_TRUE(SchedulerHelper::SatisfyScatterWidth(target, oldValue,
            newValue, minScatterWidth, scatterWidthRangePerent));
    }
}

TEST_F(TestSchedulerHelper, test_SatisfyScatterWidth_not_target) {
    int minScatterWidth = 90;
    float scatterWidthRangePerent = 0.2;
    int maxScatterWidth = minScatterWidth * (1 + scatterWidthRangePerent);
    bool target = false;
    {
        //1 After the change, the minimum value was not reached, but it increased the scattter-width
        int oldValue = 10;
        int newValue = 13;
        ASSERT_TRUE(SchedulerHelper::SatisfyScatterWidth(target, oldValue,
            newValue, minScatterWidth, scatterWidthRangePerent));
    }
    {
        //2 After the change, the minimum value is not reached, and the scattter-width remains unchanged
        int oldValue = 10;
        int newValue = 10;
        ASSERT_TRUE(SchedulerHelper::SatisfyScatterWidth(target, oldValue,
            newValue, minScatterWidth, scatterWidthRangePerent));
    }
    {
        //3 After the change, the minimum value was not reached and the scattter-width decreased
        int oldValue = 10;
        int newValue = 8;
        ASSERT_FALSE(SchedulerHelper::SatisfyScatterWidth(target, oldValue,
            newValue, minScatterWidth, scatterWidthRangePerent));
    }
    {
        //4 Equal to minimum value after change
        int oldValue = minScatterWidth + 2;
        int newValue = minScatterWidth;
        ASSERT_TRUE(SchedulerHelper::SatisfyScatterWidth(target, oldValue,
            newValue, minScatterWidth, scatterWidthRangePerent));
    }
    {
        //5 After the change, it is greater than the minimum value and less than the maximum value
        int oldValue = minScatterWidth;
        int newValue = minScatterWidth + 2;
        ASSERT_TRUE(SchedulerHelper::SatisfyScatterWidth(target, oldValue,
            newValue, minScatterWidth, scatterWidthRangePerent));
    }
    {
        //6 Equal to maximum value after change
        int oldValue = maxScatterWidth - 2;
        int newValue = maxScatterWidth;
        ASSERT_TRUE(SchedulerHelper::SatisfyScatterWidth(target, oldValue,
            newValue, minScatterWidth, scatterWidthRangePerent));
    }
    {
        //7 After the change, it is greater than the maximum value and the scattter-width increases
        int oldValue = maxScatterWidth + 1;
        int newValue = maxScatterWidth + 2;
        ASSERT_FALSE(SchedulerHelper::SatisfyScatterWidth(target, oldValue,
            newValue, minScatterWidth, scatterWidthRangePerent));
    }
    {
        //8 After the change, it is greater than the maximum value, and the scattter-width remains unchanged
        int oldValue = maxScatterWidth + 2;
        int newValue = maxScatterWidth + 2;
        ASSERT_TRUE(SchedulerHelper::SatisfyScatterWidth(target, oldValue,
            newValue, minScatterWidth, scatterWidthRangePerent));
    }
    {
        //9 After the change is greater than the maximum value, the scattter-width decreases
        int oldValue = maxScatterWidth + 3;
        int newValue = maxScatterWidth + 2;
        ASSERT_TRUE(SchedulerHelper::SatisfyScatterWidth(target, oldValue,
            newValue, minScatterWidth, scatterWidthRangePerent));
    }
}

TEST_F(TestSchedulerHelper, test_SatisfyZoneAndScatterWidthLimit) {
    CopySetInfo copyset = GetCopySetInfoForTest();
    ChunkServerIdType source = 1;
    ChunkServerIdType target = 4;
    {
        //1 Failed to obtain information for target
        EXPECT_CALL(*topoAdapter_, GetChunkServerInfo(4, _))
            .WillOnce(Return(false));
        ASSERT_FALSE(SchedulerHelper::SatisfyZoneAndScatterWidthLimit(
            topoAdapter_, target, source, copyset, 1, 0.01));
    }

    PeerInfo peer4(4, 1, 1, "192.168.10.1", 9001);
    ChunkServerInfo info4(peer4, OnlineState::ONLINE, DiskState::DISKERROR,
        ChunkServerStatus::READWRITE, 1, 1, 1, ChunkServerStatisticInfo{});
    {
        //2 Obtained standard zoneNum=0
        EXPECT_CALL(*topoAdapter_, GetChunkServerInfo(4, _))
            .WillOnce(DoAll(SetArgPointee<1>(info4), Return(true)));
        EXPECT_CALL(*topoAdapter_, GetStandardZoneNumInLogicalPool(1))
            .WillOnce(Return(0));
        ASSERT_FALSE(SchedulerHelper::SatisfyZoneAndScatterWidthLimit(
            topoAdapter_, target, source, copyset, 1, 0.01));
    }

    {
        //3 Does not meet zone conditions after migration
        EXPECT_CALL(*topoAdapter_, GetChunkServerInfo(4, _))
            .WillOnce(DoAll(SetArgPointee<1>(info4), Return(true)));
        EXPECT_CALL(*topoAdapter_, GetStandardZoneNumInLogicalPool(1))
            .WillOnce(Return(4));
         ASSERT_FALSE(SchedulerHelper::SatisfyZoneAndScatterWidthLimit(
            topoAdapter_, target, source, copyset, 1, 0.01));
    }
}

TEST_F(TestSchedulerHelper, test_SortDistribute) {
    std::map<ChunkServerIdType, std::vector<CopySetInfo>> distribute;
    GetCopySetInChunkServersForTest(&distribute);
    std::vector<std::pair<ChunkServerIdType, std::vector<CopySetInfo>>> desc;
    SchedulerHelper::SortDistribute(distribute, &desc);
    bool initial = false;
    int before;
    for (auto item : desc) {
        if (!initial) {
            before = item.second.size();
            continue;
        }

        ASSERT_TRUE(before >= item.second.size());
        before = item.second.size();
    }
}

TEST_F(TestSchedulerHelper, test_SortScatterWitAffected) {
    std::vector<std::pair<ChunkServerIdType, int>> candidates;
    candidates.emplace_back(std::pair<ChunkServerIdType, int>{1, -1});
    candidates.emplace_back(std::pair<ChunkServerIdType, int>{2, 3});
    candidates.emplace_back(std::pair<ChunkServerIdType, int>{3, 1});
    candidates.emplace_back(std::pair<ChunkServerIdType, int>{4, 2});

    SchedulerHelper::SortScatterWitAffected(&candidates);
    ASSERT_EQ(1, candidates[0].first);
    ASSERT_EQ(3, candidates[1].first);
    ASSERT_EQ(4, candidates[2].first);
    ASSERT_EQ(2, candidates[3].first);
}

TEST_F(TestSchedulerHelper, test_CalculateAffectOfMigration) {
    CopySetInfo copyset = GetCopySetInfoForTest();
    ChunkServerIdType source = 1;
    ChunkServerIdType target = 4;
    std::map<ChunkServerIdType, int> targetMap;
    targetMap[2] = 1;
    std::map<ChunkServerIdType, int> sourceMap;
    sourceMap[2] = 1;
    sourceMap[3] = 2;
    std::map<ChunkServerIdType, int> replica2Map;
    replica2Map[1] = 1;
    replica2Map[3] = 2;
    replica2Map[4] = 1;
    std::map<ChunkServerIdType, int> replica3Map;
    replica3Map[1] = 2;
    replica3Map[2] = 2;
    std::map<ChunkServerIdType, std::pair<int, int>> scatterWidth;
    EXPECT_CALL(*topoAdapter_, GetChunkServerScatterMap(target, _))
        .WillOnce(SetArgPointee<1>(targetMap));
    EXPECT_CALL(*topoAdapter_, GetChunkServerScatterMap(source, _))
        .WillOnce(SetArgPointee<1>(sourceMap));
    EXPECT_CALL(*topoAdapter_, GetChunkServerScatterMap(2, _))
        .WillOnce(SetArgPointee<1>(replica2Map));
    EXPECT_CALL(*topoAdapter_, GetChunkServerScatterMap(3, _))
        .WillOnce(SetArgPointee<1>(replica3Map));
    SchedulerHelper::CalculateAffectOfMigration(
        copyset, source, target, topoAdapter_, &scatterWidth);
    //For source, old=2, new=1
    ASSERT_EQ(2, scatterWidth[source].first);
    ASSERT_EQ(1, scatterWidth[source].second);
    //For target, old=1, new=2
    ASSERT_EQ(1, scatterWidth[target].first);
    ASSERT_EQ(2, scatterWidth[target].second);
    //For replica2, old=3, new=2
    ASSERT_EQ(3, scatterWidth[2].first);
    ASSERT_EQ(2, scatterWidth[2].second);
    //For replica3, old=2, new=3
    ASSERT_EQ(2, scatterWidth[3].first);
    ASSERT_EQ(3, scatterWidth[3].second);
}

TEST_F(TestSchedulerHelper, test_CalculateAffectOfMigration_no_source) {
    CopySetInfo copyset = GetCopySetInfoForTest();
    ChunkServerIdType source = UNINTIALIZE_ID;
    ChunkServerIdType target = 4;
    std::map<ChunkServerIdType, int> targetMap;
    targetMap[2] = 1;
    std::map<ChunkServerIdType, int> replica1Map;
    replica1Map[2] = 1;
    replica1Map[3] = 2;
    std::map<ChunkServerIdType, int> replica2Map;
    replica2Map[1] = 1;
    replica2Map[3] = 2;
    replica2Map[4] = 1;
    std::map<ChunkServerIdType, int> replica3Map;
    replica3Map[1] = 2;
    replica3Map[2] = 2;
    std::map<ChunkServerIdType, std::pair<int, int>> scatterWidth;
    EXPECT_CALL(*topoAdapter_, GetChunkServerScatterMap(target, _))
        .WillOnce(SetArgPointee<1>(targetMap));
    EXPECT_CALL(*topoAdapter_, GetChunkServerScatterMap(1, _))
        .WillOnce(SetArgPointee<1>(replica1Map));
    EXPECT_CALL(*topoAdapter_, GetChunkServerScatterMap(2, _))
        .WillOnce(SetArgPointee<1>(replica2Map));
    EXPECT_CALL(*topoAdapter_, GetChunkServerScatterMap(3, _))
        .WillOnce(SetArgPointee<1>(replica3Map));
    SchedulerHelper::CalculateAffectOfMigration(
        copyset, source, target, topoAdapter_, &scatterWidth);

    //For target, old=1, new=3
    ASSERT_EQ(1, scatterWidth[target].first);
    ASSERT_EQ(3, scatterWidth[target].second);
    //For replica1, old=2, new=3
    ASSERT_EQ(2, scatterWidth[1].first);
    ASSERT_EQ(3, scatterWidth[1].second);
    //For replica2, old=3, new=3
    ASSERT_EQ(3, scatterWidth[2].first);
    ASSERT_EQ(3, scatterWidth[2].second);
    //For replica3, old=2, new=3
    ASSERT_EQ(2, scatterWidth[3].first);
    ASSERT_EQ(3, scatterWidth[3].second);
}

TEST_F(TestSchedulerHelper, test_CalculateAffectOfMigration_no_target) {
    CopySetInfo copyset = GetCopySetInfoForTest();
    ChunkServerIdType source = 1;
    ChunkServerIdType target = UNINTIALIZE_ID;
    std::map<ChunkServerIdType, int> sourceMap;
    sourceMap[2] = 1;
    sourceMap[3] = 2;
    std::map<ChunkServerIdType, int> replica2Map;
    replica2Map[1] = 1;
    replica2Map[3] = 2;
    replica2Map[4] = 1;
    std::map<ChunkServerIdType, int> replica3Map;
    replica3Map[1] = 2;
    replica3Map[2] = 2;
    std::map<ChunkServerIdType, std::pair<int, int>> scatterWidth;
    EXPECT_CALL(*topoAdapter_, GetChunkServerScatterMap(source, _))
        .WillOnce(SetArgPointee<1>(sourceMap));
    EXPECT_CALL(*topoAdapter_, GetChunkServerScatterMap(2, _))
        .WillOnce(SetArgPointee<1>(replica2Map));
    EXPECT_CALL(*topoAdapter_, GetChunkServerScatterMap(3, _))
        .WillOnce(SetArgPointee<1>(replica3Map));
    SchedulerHelper::CalculateAffectOfMigration(
        copyset, source, target, topoAdapter_, &scatterWidth);

    //For source, old=2, new=1
    ASSERT_EQ(2, scatterWidth[source].first);
    ASSERT_EQ(1, scatterWidth[source].second);
    //For replica2, old=3, new=2
    ASSERT_EQ(3, scatterWidth[2].first);
    ASSERT_EQ(2, scatterWidth[2].second);
    //For replica3, old=2, new=2
    ASSERT_EQ(2, scatterWidth[3].first);
    ASSERT_EQ(2, scatterWidth[3].second);
}

TEST_F(TestSchedulerHelper,
    test_InvovledReplicasSatisfyScatterWidthAfterMigration_not_satisfy) {
    CopySetInfo copyset = GetCopySetInfoForTest();
    ChunkServerIdType source = 1;
    ChunkServerIdType target = 4;
    std::map<ChunkServerIdType, int> targetMap;
    targetMap[2] = 1;
    std::map<ChunkServerIdType, int> sourceMap;
    sourceMap[2] = 1;
    sourceMap[3] = 2;
    std::map<ChunkServerIdType, int> replica2Map;
    replica2Map[1] = 1;
    replica2Map[3] = 2;
    replica2Map[4] = 1;
    std::map<ChunkServerIdType, int> replica3Map;
    replica3Map[1] = 2;
    replica3Map[2] = 2;
    std::map<ChunkServerIdType, std::pair<int, int>> scatterWidth;
    EXPECT_CALL(*topoAdapter_, GetChunkServerScatterMap(target, _))
        .WillOnce(SetArgPointee<1>(targetMap));
    EXPECT_CALL(*topoAdapter_, GetChunkServerScatterMap(source, _))
        .WillOnce(SetArgPointee<1>(sourceMap));
    EXPECT_CALL(*topoAdapter_, GetChunkServerScatterMap(2, _))
        .WillOnce(SetArgPointee<1>(replica2Map));
    EXPECT_CALL(*topoAdapter_, GetChunkServerScatterMap(3, _))
        .WillOnce(SetArgPointee<1>(replica3Map));
    int affected = 0;
    bool res =
        SchedulerHelper::InvovledReplicasSatisfyScatterWidthAfterMigration(
        copyset, source, target, UNINTIALIZE_ID, topoAdapter_,
        10, 0.1, &affected);
    ASSERT_FALSE(res);
    ASSERT_EQ(0, affected);
}

TEST_F(TestSchedulerHelper,
    test_InvovledReplicasSatisfyScatterWidthAfterMigration_satisfy) {
    CopySetInfo copyset = GetCopySetInfoForTest();
    ChunkServerIdType source = 1;
    ChunkServerIdType target = 4;
    std::map<ChunkServerIdType, int> targetMap;
    targetMap[2] = 1;
    std::map<ChunkServerIdType, int> sourceMap;
    sourceMap[2] = 1;
    sourceMap[3] = 2;
    std::map<ChunkServerIdType, int> replica2Map;
    replica2Map[1] = 1;
    replica2Map[3] = 2;
    replica2Map[4] = 1;
    std::map<ChunkServerIdType, int> replica3Map;
    replica3Map[1] = 2;
    replica3Map[2] = 2;
    std::map<ChunkServerIdType, std::pair<int, int>> scatterWidth;
    EXPECT_CALL(*topoAdapter_, GetChunkServerScatterMap(target, _))
        .WillOnce(SetArgPointee<1>(targetMap));
    EXPECT_CALL(*topoAdapter_, GetChunkServerScatterMap(source, _))
        .WillOnce(SetArgPointee<1>(sourceMap));
    EXPECT_CALL(*topoAdapter_, GetChunkServerScatterMap(2, _))
        .WillOnce(SetArgPointee<1>(replica2Map));
    EXPECT_CALL(*topoAdapter_, GetChunkServerScatterMap(3, _))
        .WillOnce(SetArgPointee<1>(replica3Map));
    int affected = 0;
    bool res =
        SchedulerHelper::InvovledReplicasSatisfyScatterWidthAfterMigration(
        copyset, source, target, UNINTIALIZE_ID, topoAdapter_, 1, 2, &affected);
    ASSERT_TRUE(res);
    ASSERT_EQ(0, affected);
}


TEST_F(TestSchedulerHelper, test_SortChunkServerByCopySetNumAsc) {
    PeerInfo peer1(1, 1, 1, "192.168.10.1", 9000);
    PeerInfo peer2(2, 2, 2, "192.168.10.2", 9000);
    PeerInfo peer3(3, 3, 3, "192.168.10.3", 9000);
    PeerInfo peer4(4, 4, 4, "192.168.10.4", 9000);
    ChunkServerInfo info1(peer1, OnlineState::ONLINE, DiskState::DISKNORMAL,
        ChunkServerStatus::READWRITE, 10, 10, 10, ChunkServerStatisticInfo{});
    ChunkServerInfo info2(peer2, OnlineState::ONLINE, DiskState::DISKNORMAL,
        ChunkServerStatus::READWRITE, 10, 10, 10, ChunkServerStatisticInfo{});
    ChunkServerInfo info3(peer3, OnlineState::ONLINE, DiskState::DISKNORMAL,
        ChunkServerStatus::READWRITE, 10, 10, 10, ChunkServerStatisticInfo{});
    std::vector<ChunkServerInfo> chunkserverList{info1, info2, info3};

    // {1,2,3}
    CopySetInfo copyset1(CopySetKey{1, 1}, 1, 1,
        std::vector<PeerInfo>{peer1, peer2, peer3},
        ConfigChangeInfo{}, CopysetStatistics{});
    // {1,3,4}
    CopySetInfo copyset2(CopySetKey{1, 2}, 1, 1,
        std::vector<PeerInfo>{peer1, peer3, peer4},
        ConfigChangeInfo{}, CopysetStatistics{});
    // {1,2,3}
    CopySetInfo copyset3(CopySetKey{1, 3}, 1, 1,
        std::vector<PeerInfo>{peer1, peer2, peer3},
        ConfigChangeInfo{}, CopysetStatistics{});
    // {1,2,4}
    CopySetInfo copyset4(CopySetKey{1, 4}, 1, 1,
        std::vector<PeerInfo>{peer1, peer2, peer4},
        ConfigChangeInfo{}, CopysetStatistics{});
    // {1,3,4}
    CopySetInfo copyset5(CopySetKey{1, 5}, 1, 1,
        std::vector<PeerInfo>{peer1, peer3, peer4},
        ConfigChangeInfo{}, CopysetStatistics{});
    std::vector<CopySetInfo> copysetList{
        copyset1, copyset2, copyset3, copyset4, copyset5};

    // chunkserver-1: 5, chunkserver-2: 3 chunkserver-3: 4
    EXPECT_CALL(*topoAdapter_, GetCopySetInfos())
        .WillOnce(Return(copysetList));
    SchedulerHelper::SortChunkServerByCopySetNumAsc(
        &chunkserverList, topoAdapter_);

    ASSERT_EQ(info2.info.id, chunkserverList[0].info.id);
    ASSERT_EQ(info3.info.id, chunkserverList[1].info.id);
    ASSERT_EQ(info1.info.id, chunkserverList[2].info.id);
}

}  // namespace schedule
}  // namespace mds
}  // namespace curve

