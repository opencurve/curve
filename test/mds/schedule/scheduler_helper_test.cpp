/*
 * Project: curve
 * Created Date: Thur July 06th 2019
 * Author: lixiaocui
 * Copyright (c) 2018 netease
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
        // 1. 变更之后未达到最小值，但使得scatter-width增大
        int oldValue = 10;
        int newValue = 13;
        ASSERT_TRUE(SchedulerHelper::SatisfyScatterWidth(target, oldValue,
            newValue, minScatterWidth, scatterWidthRangePerent));
    }
    {
        // 2. 变更之后未达到最小值，scattter-width不变
        int oldValue = 10;
        int newValue = 10;
        ASSERT_FALSE(SchedulerHelper::SatisfyScatterWidth(target, oldValue,
            newValue, minScatterWidth, scatterWidthRangePerent));
    }
    {
        // 3. 变更之后未达到最小值，scatter-width减小
        int oldValue = 10;
        int newValue = 8;
        ASSERT_FALSE(SchedulerHelper::SatisfyScatterWidth(target, oldValue,
            newValue, minScatterWidth, scatterWidthRangePerent));
    }
    {
        // 4. 变更之后等于最小值
        int oldValue = minScatterWidth + 2;
        int newValue = minScatterWidth;
        ASSERT_TRUE(SchedulerHelper::SatisfyScatterWidth(target, oldValue,
            newValue, minScatterWidth, scatterWidthRangePerent));
    }
    {
        // 5. 变更之后大于最小值，小于最大值
        int oldValue = minScatterWidth;
        int newValue = minScatterWidth + 2;
        ASSERT_TRUE(SchedulerHelper::SatisfyScatterWidth(target, oldValue,
            newValue, minScatterWidth, scatterWidthRangePerent));
    }
    {
        // 6. 变更之后等于最大值
        int oldValue = maxScatterWidth - 2;
        int newValue = maxScatterWidth;
        ASSERT_TRUE(SchedulerHelper::SatisfyScatterWidth(target, oldValue,
            newValue, minScatterWidth, scatterWidthRangePerent));
    }
    {
        // 7. 变更之后大于最大值，scatter-width增大
        int oldValue = maxScatterWidth + 1;
        int newValue = maxScatterWidth + 2;
        ASSERT_FALSE(SchedulerHelper::SatisfyScatterWidth(target, oldValue,
            newValue, minScatterWidth, scatterWidthRangePerent));
    }
    {
        // 8. 变更之后大于最大值，scatter-width不变
        int oldValue = maxScatterWidth + 2;
        int newValue = maxScatterWidth + 2;
        ASSERT_FALSE(SchedulerHelper::SatisfyScatterWidth(target, oldValue,
            newValue, minScatterWidth, scatterWidthRangePerent));
    }
    {
        // 9. 变更之后大于最大值，scatter-width减小
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
        // 1. 变更之后未达到最小值，但使得scatter-width增大
        int oldValue = 10;
        int newValue = 13;
        ASSERT_TRUE(SchedulerHelper::SatisfyScatterWidth(target, oldValue,
            newValue, minScatterWidth, scatterWidthRangePerent));
    }
    {
        // 2. 变更之后未达到最小值，scattter-width不变
        int oldValue = 10;
        int newValue = 10;
        ASSERT_TRUE(SchedulerHelper::SatisfyScatterWidth(target, oldValue,
            newValue, minScatterWidth, scatterWidthRangePerent));
    }
    {
        // 3. 变更之后未达到最小值，scatter-width减小
        int oldValue = 10;
        int newValue = 8;
        ASSERT_FALSE(SchedulerHelper::SatisfyScatterWidth(target, oldValue,
            newValue, minScatterWidth, scatterWidthRangePerent));
    }
    {
        // 4. 变更之后等于最小值
        int oldValue = minScatterWidth + 2;
        int newValue = minScatterWidth;
        ASSERT_TRUE(SchedulerHelper::SatisfyScatterWidth(target, oldValue,
            newValue, minScatterWidth, scatterWidthRangePerent));
    }
    {
        // 5. 变更之后大于最小值，小于最大值
        int oldValue = minScatterWidth;
        int newValue = minScatterWidth + 2;
        ASSERT_TRUE(SchedulerHelper::SatisfyScatterWidth(target, oldValue,
            newValue, minScatterWidth, scatterWidthRangePerent));
    }
    {
        // 6. 变更之后等于最大值
        int oldValue = maxScatterWidth - 2;
        int newValue = maxScatterWidth;
        ASSERT_TRUE(SchedulerHelper::SatisfyScatterWidth(target, oldValue,
            newValue, minScatterWidth, scatterWidthRangePerent));
    }
    {
        // 7. 变更之后大于最大值，scatter-width增大
        int oldValue = maxScatterWidth + 1;
        int newValue = maxScatterWidth + 2;
        ASSERT_FALSE(SchedulerHelper::SatisfyScatterWidth(target, oldValue,
            newValue, minScatterWidth, scatterWidthRangePerent));
    }
    {
        // 8. 变更之后大于最大值，scatter-width不变
        int oldValue = maxScatterWidth + 2;
        int newValue = maxScatterWidth + 2;
        ASSERT_TRUE(SchedulerHelper::SatisfyScatterWidth(target, oldValue,
            newValue, minScatterWidth, scatterWidthRangePerent));
    }
    {
        // 9. 变更之后大于最大值，scatter-width减小
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
        // 1. 获取target的信息失败
        EXPECT_CALL(*topoAdapter_, GetChunkServerInfo(4, _))
            .WillOnce(Return(false));
        ASSERT_FALSE(SchedulerHelper::SatisfyZoneAndScatterWidthLimit(
            topoAdapter_, target, source, copyset, 1, 0.01));
    }

    PeerInfo peer4(4, 1, 1, 1, "192.168.10.1", 9001);
    ChunkServerInfo info4(peer4, OnlineState::ONLINE, DiskState::DISKERROR,
        1, 1, 1, 1, ChunkServerStatisticInfo{});
    {
        // 2. 获取到的标准zoneNum = 0
        EXPECT_CALL(*topoAdapter_, GetChunkServerInfo(4, _))
            .WillOnce(DoAll(SetArgPointee<1>(info4), Return(true)));
        EXPECT_CALL(*topoAdapter_, GetStandardZoneNumInLogicalPool(1))
            .WillOnce(Return(0));
        ASSERT_FALSE(SchedulerHelper::SatisfyZoneAndScatterWidthLimit(
            topoAdapter_, target, source, copyset, 1, 0.01));
    }

    {
        // 3. 迁移之后不符合zone条件
        EXPECT_CALL(*topoAdapter_, GetChunkServerInfo(4, _))
            .WillOnce(DoAll(SetArgPointee<1>(info4), Return(true)));
        EXPECT_CALL(*topoAdapter_, GetStandardZoneNumInLogicalPool(1))
            .WillOnce(Return(4));
         ASSERT_FALSE(SchedulerHelper::SatisfyZoneAndScatterWidthLimit(
            topoAdapter_, target, source, copyset, 1, 0.01));
    }
}

TEST_F(TestSchedulerHelper, test_SortDistribute) {
    std::map<ChunkServerIDType, std::vector<CopySetInfo>> distribute;
    GetCopySetInChunkServersForTest(&distribute);
    std::vector<std::pair<ChunkServerIDType, std::vector<CopySetInfo>>> desc;
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
    // 对于source, old=2, new=1
    ASSERT_EQ(2, scatterWidth[source].first);
    ASSERT_EQ(1, scatterWidth[source].second);
    // 对于target, old=1, new=2
    ASSERT_EQ(1, scatterWidth[target].first);
    ASSERT_EQ(2, scatterWidth[target].second);
    // 对于replica2, old=3, new=2
    ASSERT_EQ(3, scatterWidth[2].first);
    ASSERT_EQ(2, scatterWidth[2].second);
    // 对于replica3, old=2, new=3
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

    // 对于target, old=1, new=3
    ASSERT_EQ(1, scatterWidth[target].first);
    ASSERT_EQ(3, scatterWidth[target].second);
    // 对于replica1, old=2, new=3
    ASSERT_EQ(2, scatterWidth[1].first);
    ASSERT_EQ(3, scatterWidth[1].second);
    // 对于replica2, old=3, new=3
    ASSERT_EQ(3, scatterWidth[2].first);
    ASSERT_EQ(3, scatterWidth[2].second);
    // 对于replica3, old=2, new=3
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

    // 对于source, old=2, new=1
    ASSERT_EQ(2, scatterWidth[source].first);
    ASSERT_EQ(1, scatterWidth[source].second);
    // 对于replica2, old=3, new=2
    ASSERT_EQ(3, scatterWidth[2].first);
    ASSERT_EQ(2, scatterWidth[2].second);
    // 对于replica3, old=2, new=2
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

}  // namespace schedule
}  // namespace mds
}  // namespace curve

