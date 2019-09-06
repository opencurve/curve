/*
 * Project: curve
 * Created Date: 20190902
 * Author: lixiaocui
 * Copyright (c) 2018 netease
 */

#include <gtest/gtest.h>
#include "src/mds/nameserver2/helper/namespace_helper.h"
#include "src/mds/nameserver2/allocstatistic/alloc_statistic_helper.h"
#include "test/mds/mock/mock_etcdclient.h"
#include "src/mds/nameserver2/allocstatistic/alloc_statistic.h"

using ::testing::_;
using ::testing::Return;
using ::testing::SetArgPointee;
using ::testing::DoAll;

namespace curve {
namespace mds {

class AllocStatisticTest : public ::testing::Test {
 protected:
    void SetUp() override {
        periodicPersistInterMs_ = 2;
        retryInterMs_ = 2;
        mockEtcdClient_ = std::make_shared<MockEtcdClient>();
        allocStatistc_ = std::make_shared<AllocStatistic>(
            periodicPersistInterMs_, retryInterMs_, mockEtcdClient_);
    }

 protected:
    uint64_t periodicPersistInterMs_;
    uint64_t retryInterMs_;
    std::shared_ptr<AllocStatistic> allocStatistc_;
    std::shared_ptr<MockEtcdClient> mockEtcdClient_;
};

TEST_F(AllocStatisticTest, test_Init) {
    {
        // 1. 从etcd中获取当前revision失败
        LOG(INFO) << "test1......";
        EXPECT_CALL(*mockEtcdClient_, GetCurrentRevision(_)).
            WillOnce(Return(EtcdErrCode::Canceled));
        ASSERT_EQ(-1, allocStatistc_->Init());
    }
    {
        // 2. 获取已经存在的logicalPool对应的alloc大小失败
        LOG(INFO) << "test2......";
        EXPECT_CALL(*mockEtcdClient_, GetCurrentRevision(_)).
            WillOnce(Return(EtcdErrCode::OK));
        EXPECT_CALL(*mockEtcdClient_, List(
            SEGMENTALLOCSIZEKEY, SEGMENTALLOCSIZEKEYEND, _))
            .WillOnce(Return(EtcdErrCode::Canceled));
        ASSERT_EQ(-1, allocStatistc_->Init());
        uint64_t alloc;
        ASSERT_FALSE(allocStatistc_->GetAllocByLogicalPool(1, &alloc));
    }
    {
        // 3. init成功
        LOG(INFO) << "test3......";
        std::vector<std::string> values{
            NameSpaceStorageCodec::EncodeSegmentAllocValue(1, 1024)};
        EXPECT_CALL(*mockEtcdClient_, GetCurrentRevision(_)).
            WillOnce(DoAll(SetArgPointee<0>(2), Return(EtcdErrCode::OK)));
        EXPECT_CALL(*mockEtcdClient_, List(
            SEGMENTALLOCSIZEKEY, SEGMENTALLOCSIZEKEYEND, _))
            .WillOnce(DoAll(SetArgPointee<2>(values), Return(EtcdErrCode::OK)));
        ASSERT_EQ(0, allocStatistc_->Init());
        uint64_t alloc;
        ASSERT_TRUE(allocStatistc_->GetAllocByLogicalPool(1, &alloc));
        ASSERT_EQ(1024, alloc);
    }
}

TEST_F(AllocStatisticTest, test_PeriodicPersist_CalculateSegmentAlloc) {
    // 初始化 allocStatistic
    // 旧值: logicalPooId(1):1024
    std::vector<std::string> values{
            NameSpaceStorageCodec::EncodeSegmentAllocValue(1, 1024)};
    EXPECT_CALL(*mockEtcdClient_, GetCurrentRevision(_))
        .Times(3)
        .WillOnce(DoAll(SetArgPointee<0>(2), Return(EtcdErrCode::OK)))
        .WillOnce(Return(EtcdErrCode::Canceled))
        .WillOnce(DoAll(SetArgPointee<0>(2), Return(EtcdErrCode::OK)));
    EXPECT_CALL(*mockEtcdClient_, List(
        SEGMENTALLOCSIZEKEY, SEGMENTALLOCSIZEKEYEND, _))
        .WillOnce(DoAll(SetArgPointee<2>(values), Return(EtcdErrCode::OK)));
    ASSERT_EQ(0, allocStatistc_->Init());

    PageFileSegment segment;
    segment.set_segmentsize(1 << 30);
    segment.set_logicalpoolid(1);
    std::string encodeSegment;
    values.clear();
    ASSERT_TRUE(
        NameSpaceStorageCodec::EncodeSegment(segment, &encodeSegment));
    for (int i = 1; i <= 500; i++) {
        values.emplace_back(encodeSegment);
    }

    // 设置mock的etcd中segment的值
    // logicalPoolId(1):500 * (1<<30)
    // logicalPoolId(2):501 * (1<<30)
    segment.set_logicalpoolid(2);
    ASSERT_TRUE(
        NameSpaceStorageCodec::EncodeSegment(segment, &encodeSegment));
    for (int i = 501; i <= 1000; i++) {
        values.emplace_back(encodeSegment);
    }
    std::string lastKey1 =
        NameSpaceStorageCodec::EncodeSegmentStoreKey(1, 500);
    std::string lastKey2 =
        NameSpaceStorageCodec::EncodeSegmentStoreKey(501, 1000);
    EXPECT_CALL(*mockEtcdClient_, ListWithLimitAndRevision(
        SEGMENTINFOKEYPREFIX, SEGMENTINFOKEYEND, GETBUNDLE, 2, _, _))
        .Times(2)
        .WillOnce(Return(EtcdErrCode::Canceled))
        .WillOnce(DoAll(SetArgPointee<4>(values),
                        SetArgPointee<5>(lastKey1),
                        Return(EtcdErrCode::OK)));
    EXPECT_CALL(*mockEtcdClient_, ListWithLimitAndRevision(
        lastKey1, SEGMENTINFOKEYEND, GETBUNDLE, 2, _, _))
        .WillOnce(DoAll(SetArgPointee<4>(
            std::vector<std::string>{encodeSegment, encodeSegment}),
                        SetArgPointee<5>(lastKey2),
                        Return(EtcdErrCode::OK)));
    // 设置mock的Put结果
    EXPECT_CALL(*mockEtcdClient_, Put(
        NameSpaceStorageCodec::EncodeSegmentAllocKey(1),
        NameSpaceStorageCodec::EncodeSegmentAllocValue(1, 500L *(1 << 30))))
        .Times(2)
        .WillOnce(Return(EtcdErrCode::Canceled))
        .WillOnce(Return(EtcdErrCode::OK));
    EXPECT_CALL(*mockEtcdClient_, Put(
        NameSpaceStorageCodec::EncodeSegmentAllocKey(2),
        NameSpaceStorageCodec::EncodeSegmentAllocValue(2, 501L *(1 << 30))))
        .WillOnce(Return(EtcdErrCode::OK));
    EXPECT_CALL(*mockEtcdClient_, Put(
        NameSpaceStorageCodec::EncodeSegmentAllocKey(1),
        NameSpaceStorageCodec::EncodeSegmentAllocValue(1, 499L *(1 << 30))))
        .WillOnce(Return(EtcdErrCode::OK));
    EXPECT_CALL(*mockEtcdClient_, Put(
        NameSpaceStorageCodec::EncodeSegmentAllocKey(2),
        NameSpaceStorageCodec::EncodeSegmentAllocValue(2, 500L *(1 << 30))))
        .WillOnce(Return(EtcdErrCode::OK));

    // 在定期持久化线程和统计线程启动前，只能获取旧值
    uint64_t alloc;
    ASSERT_TRUE(allocStatistc_->GetAllocByLogicalPool(1, &alloc));
    ASSERT_EQ(1024, alloc);
    ASSERT_FALSE(allocStatistc_->GetAllocByLogicalPool(2, &alloc));


    // 启动定期持久化线程和统计线程
    allocStatistc_->Run();
    std::this_thread::sleep_for(std::chrono::milliseconds(30));

    // 通过AllocChange进行更新
    for (int i = 1; i <= 2; i++) {
        allocStatistc_->UpdateChangeLock();
        allocStatistc_->AllocChange(i, 0L - (1 << 30));
        allocStatistc_->UpdateChangeUnlock();
    }

    // 等待统计和持久化结束
    std::this_thread::sleep_for(std::chrono::milliseconds(30));

    ASSERT_TRUE(allocStatistc_->GetAllocByLogicalPool(1, &alloc));
    ASSERT_EQ(499L *(1 << 30), alloc);
    ASSERT_TRUE(allocStatistc_->GetAllocByLogicalPool(2, &alloc));
    ASSERT_EQ(500L *(1 << 30), alloc);

    allocStatistc_->Stop();
}

}  // namespace mds
}  // namespace curve
