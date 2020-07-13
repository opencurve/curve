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
 * Created Date: 20190902
 * Author: lixiaocui
 */

#include <gtest/gtest.h>
#include "src/mds/nameserver2/helper/namespace_helper.h"
#include "src/mds/nameserver2/allocstatistic/alloc_statistic_helper.h"
#include "test/mds/mock/mock_etcdclient.h"
#include "src/mds/nameserver2/allocstatistic/alloc_statistic.h"
#include "src/common/namespace_define.h"

using ::testing::_;
using ::testing::Return;
using ::testing::SetArgPointee;
using ::testing::DoAll;

using ::curve::common::SEGMENTALLOCSIZEKEYEND;
using ::curve::common::SEGMENTALLOCSIZEKEY;
using ::curve::common::SEGMENTINFOKEYEND;
using ::curve::common::SEGMENTINFOKEYPREFIX;

namespace curve {
namespace mds {

class AllocStatisticTest : public ::testing::Test {
 protected:
    void SetUp() override {
        periodicPersistInterMs_ = 2;
        retryInterMs_ = 2;
        mockEtcdClient_ = std::make_shared<MockEtcdClient>();
        allocStatistic_ = std::make_shared<AllocStatistic>(
            periodicPersistInterMs_, retryInterMs_, mockEtcdClient_);
    }

 protected:
    int64_t periodicPersistInterMs_;
    int64_t retryInterMs_;
    std::shared_ptr<AllocStatistic> allocStatistic_;
    std::shared_ptr<MockEtcdClient> mockEtcdClient_;
};

TEST_F(AllocStatisticTest, test_Init) {
    {
        // 1. 从etcd中获取当前revision失败
        LOG(INFO) << "test1......";
        EXPECT_CALL(*mockEtcdClient_, GetCurrentRevision(_)).
            WillOnce(Return(EtcdErrCode::EtcdCanceled));
        ASSERT_EQ(-1, allocStatistic_->Init());
    }
    {
        // 2. 获取已经存在的logicalPool对应的alloc大小失败
        LOG(INFO) << "test2......";
        EXPECT_CALL(*mockEtcdClient_, GetCurrentRevision(_)).
            WillOnce(Return(EtcdErrCode::EtcdOK));
        EXPECT_CALL(*mockEtcdClient_, List(
            SEGMENTALLOCSIZEKEY, SEGMENTALLOCSIZEKEYEND, _))
            .WillOnce(Return(EtcdErrCode::EtcdCanceled));
        ASSERT_EQ(-1, allocStatistic_->Init());
        int64_t alloc;
        ASSERT_FALSE(allocStatistic_->GetAllocByLogicalPool(1, &alloc));
    }
    {
        // 3. init成功
        LOG(INFO) << "test3......";
        std::vector<std::string> values{
            NameSpaceStorageCodec::EncodeSegmentAllocValue(1, 1024)};
        EXPECT_CALL(*mockEtcdClient_, GetCurrentRevision(_)).
            WillOnce(DoAll(SetArgPointee<0>(2), Return(EtcdErrCode::EtcdOK)));
        EXPECT_CALL(*mockEtcdClient_, List(
            SEGMENTALLOCSIZEKEY, SEGMENTALLOCSIZEKEYEND, _))
            .WillOnce(
                DoAll(SetArgPointee<2>(values), Return(EtcdErrCode::EtcdOK)));
        ASSERT_EQ(0, allocStatistic_->Init());
        int64_t alloc;
        ASSERT_TRUE(allocStatistic_->GetAllocByLogicalPool(1, &alloc));
        ASSERT_EQ(1024, alloc);
    }
}

TEST_F(AllocStatisticTest, test_PeriodicPersist_CalculateSegmentAlloc) {
    // 初始化 allocStatistic
    // 旧值: logicalPooId(1):1024
    std::vector<std::string> values{
            NameSpaceStorageCodec::EncodeSegmentAllocValue(1, 1024)};
    EXPECT_CALL(*mockEtcdClient_, GetCurrentRevision(_))
        .WillOnce(DoAll(SetArgPointee<0>(2), Return(EtcdErrCode::EtcdOK)));
    EXPECT_CALL(*mockEtcdClient_, List(
        SEGMENTALLOCSIZEKEY, SEGMENTALLOCSIZEKEYEND, _))
        .WillOnce(DoAll(SetArgPointee<2>(values), Return(EtcdErrCode::EtcdOK)));
    ASSERT_EQ(0, allocStatistic_->Init());

    PageFileSegment segment;
    segment.set_segmentsize(1 << 30);
    segment.set_logicalpoolid(1);
    segment.set_chunksize(16*1024*1024);
    segment.set_startoffset(0);
    std::string encodeSegment;
    values.clear();
    ASSERT_TRUE(
        NameSpaceStorageCodec::EncodeSegment(segment, &encodeSegment));
    for (int i = 1; i <= 500; i++) {
        values.emplace_back(encodeSegment);
    }

    // 1. 在定期持久化线程和统计线程启动前，只能获取旧值
    int64_t alloc;
    ASSERT_TRUE(allocStatistic_->GetAllocByLogicalPool(1, &alloc));
    ASSERT_EQ(1024, alloc);
    ASSERT_FALSE(allocStatistic_->GetAllocByLogicalPool(2, &alloc));

    // 2. 更新segment的值
    allocStatistic_->DeAllocSpace(1, 64, 1);
    allocStatistic_->AllocSpace(1, 32, 1);
    ASSERT_TRUE(allocStatistic_->GetAllocByLogicalPool(1, &alloc));
    ASSERT_EQ(1024 - 32, alloc);

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
        .WillOnce(Return(EtcdErrCode::EtcdCanceled))
        .WillOnce(DoAll(SetArgPointee<4>(values),
                        SetArgPointee<5>(lastKey1),
                        Return(EtcdErrCode::EtcdOK)));
    EXPECT_CALL(*mockEtcdClient_, ListWithLimitAndRevision(
        lastKey1, SEGMENTINFOKEYEND, GETBUNDLE, 2, _, _))
        .WillOnce(DoAll(SetArgPointee<4>(
            std::vector<std::string>{encodeSegment, encodeSegment}),
                        SetArgPointee<5>(lastKey2),
                        Return(EtcdErrCode::EtcdOK)));
     EXPECT_CALL(*mockEtcdClient_, GetCurrentRevision(_))
        .Times(2)
        .WillOnce(Return(EtcdErrCode::EtcdCanceled))
        .WillOnce(DoAll(SetArgPointee<0>(2), Return(EtcdErrCode::EtcdOK)));

    // 设置mock的Put结果
    EXPECT_CALL(*mockEtcdClient_, Put(
        NameSpaceStorageCodec::EncodeSegmentAllocKey(1),
        NameSpaceStorageCodec::EncodeSegmentAllocValue(
            1, 1024 - 32 + (1L << 30))))
        .WillOnce(Return(EtcdErrCode::EtcdOK));
    EXPECT_CALL(*mockEtcdClient_, Put(
        NameSpaceStorageCodec::EncodeSegmentAllocKey(2),
        NameSpaceStorageCodec::EncodeSegmentAllocValue(2, 1L << 30)))
        .WillOnce(Return(EtcdErrCode::EtcdOK));
    EXPECT_CALL(*mockEtcdClient_, Put(
        NameSpaceStorageCodec::EncodeSegmentAllocKey(1),
        NameSpaceStorageCodec::EncodeSegmentAllocValue(1, 501L *(1 << 30))))
        .WillOnce(Return(EtcdErrCode::EtcdOK));
    EXPECT_CALL(*mockEtcdClient_, Put(
        NameSpaceStorageCodec::EncodeSegmentAllocKey(2),
        NameSpaceStorageCodec::EncodeSegmentAllocValue(2, 502L *(1 << 30))))
        .WillOnce(Return(EtcdErrCode::EtcdOK));
    EXPECT_CALL(*mockEtcdClient_, Put(
        NameSpaceStorageCodec::EncodeSegmentAllocKey(1),
        NameSpaceStorageCodec::EncodeSegmentAllocValue(1, 500L *(1 << 30))))
        .WillOnce(Return(EtcdErrCode::EtcdOK));
    EXPECT_CALL(*mockEtcdClient_, Put(
        NameSpaceStorageCodec::EncodeSegmentAllocKey(2),
        NameSpaceStorageCodec::EncodeSegmentAllocValue(2, 501L *(1 << 30))))
        .WillOnce(Return(EtcdErrCode::EtcdOK));
    EXPECT_CALL(*mockEtcdClient_, Put(
        NameSpaceStorageCodec::EncodeSegmentAllocKey(3),
        NameSpaceStorageCodec::EncodeSegmentAllocValue(3, 1L << 30)))
        .WillOnce(Return(EtcdErrCode::EtcdOK));

    // 2. 启动定期持久化线程和统计线程
    for (int i = 1; i <= 2; i++) {
        allocStatistic_->AllocSpace(i, 1L << 30, i + 3);
    }
    allocStatistic_->Run();
    std::this_thread::sleep_for(std::chrono::seconds(6));

    ASSERT_TRUE(allocStatistic_->GetAllocByLogicalPool(1, &alloc));
    ASSERT_EQ(501L *(1 << 30), alloc);
    ASSERT_TRUE(allocStatistic_->GetAllocByLogicalPool(2, &alloc));
    ASSERT_EQ(502L *(1 << 30), alloc);
    std::this_thread::sleep_for(std::chrono::milliseconds(30));

    // 再通过alloc进行更新
    for (int i = 1; i <= 2; i++) {
        allocStatistic_->DeAllocSpace(i, 1L << 30, i + 4);
    }
    allocStatistic_->AllocSpace(3, 1L << 30, 10);

    ASSERT_TRUE(allocStatistic_->GetAllocByLogicalPool(1, &alloc));
    ASSERT_EQ(500L *(1 << 30), alloc);
    ASSERT_TRUE(allocStatistic_->GetAllocByLogicalPool(2, &alloc));
    ASSERT_EQ(501L *(1 << 30), alloc);
    ASSERT_TRUE(allocStatistic_->GetAllocByLogicalPool(3, &alloc));
    ASSERT_EQ(1L << 30, alloc);
    std::this_thread::sleep_for(std::chrono::milliseconds(30));

    allocStatistic_->Stop();
}

}  // namespace mds
}  // namespace curve
