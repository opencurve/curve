/*
 * Project: curve
 * Created Date: 20190831
 * Author: lixiaocui
 * Copyright (c) 2018 netease
 */

#include <gtest/gtest.h>
#include <memory>
#include "src/mds/nameserver2/helper/namespace_helper.h"
#include "src/mds/nameserver2/allocstatistic/alloc_statistic_helper.h"
#include "src/common/namespace_define.h"
#include "test/mds/mock/mock_etcdclient.h"

using ::testing::_;
using ::testing::Return;
using ::testing::SetArgPointee;
using ::testing::DoAll;

using ::curve::common::SEGMENTALLOCSIZEKEYEND;
using ::curve::common::SEGMENTALLOCSIZEKEY;
using ::curve::common::SEGMENTINFOKEYPREFIX;
using ::curve::common::SEGMENTINFOKEYEND;

namespace curve {
namespace mds {
TEST(TestAllocStatisticHelper, test_GetExistSegmentAllocValues) {
    auto mockEtcdClient = std::make_shared<MockEtcdClient>();

    {
        // 1. list失败
        EXPECT_CALL(*mockEtcdClient, List(
            SEGMENTALLOCSIZEKEY, SEGMENTALLOCSIZEKEYEND, _))
            .WillOnce(Return(EtcdErrCode::EtcdCanceled));
        std::map<PoolIdType, int64_t> out;
        ASSERT_EQ(-1, AllocStatisticHelper::GetExistSegmentAllocValues(
            &out, mockEtcdClient));
    }

    {
        // 2. list成功，解析失败
        std::vector<std::string> values{"hello"};
        EXPECT_CALL(*mockEtcdClient, List(
            SEGMENTALLOCSIZEKEY, SEGMENTALLOCSIZEKEYEND, _))
            .WillOnce(
                DoAll(SetArgPointee<2>(values), Return(EtcdErrCode::EtcdOK)));
        std::map<PoolIdType, int64_t> out;
        ASSERT_EQ(0, AllocStatisticHelper::GetExistSegmentAllocValues(
            &out, mockEtcdClient));
    }
    {
        // 3. 获取已有的segment alloc value成功
        std::vector<std::string> values{
            NameSpaceStorageCodec::EncodeSegmentAllocValue(1, 1024)};
        EXPECT_CALL(*mockEtcdClient, List(
            SEGMENTALLOCSIZEKEY, SEGMENTALLOCSIZEKEYEND, _))
            .WillOnce(
                DoAll(SetArgPointee<2>(values), Return(EtcdErrCode::EtcdOK)));
        std::map<PoolIdType, int64_t> out;
        ASSERT_EQ(0, AllocStatisticHelper::GetExistSegmentAllocValues(
            &out, mockEtcdClient));
        ASSERT_EQ(1, out.size());
        ASSERT_EQ(1024, out[1]);
    }
}

TEST(TestAllocStatisticHelper, test_CalculateSegmentAlloc) {
    auto mockEtcdClient = std::make_shared<MockEtcdClient>();
    {
        // 1. CalculateSegmentAlloc ok
        LOG(INFO) << "start test1......";
        EXPECT_CALL(*mockEtcdClient, ListWithLimitAndRevision(
            SEGMENTINFOKEYPREFIX, SEGMENTINFOKEYEND, GETBUNDLE, 2, _, _))
            .WillOnce(Return(EtcdErrCode::EtcdUnknown));
        std::map<PoolIdType, int64_t> out;
        ASSERT_EQ(-1, AllocStatisticHelper::CalculateSegmentAlloc(
            2, mockEtcdClient, &out));
    }
    {
        // 2. ListWithLimitAndRevision成功，但是解析失败
        LOG(INFO) << "start test2......";
        std::vector<std::string> values{"hello"};
        std::string lastKey = "021";
        EXPECT_CALL(*mockEtcdClient, ListWithLimitAndRevision(
            SEGMENTINFOKEYPREFIX, SEGMENTINFOKEYEND, GETBUNDLE, 2, _, _))
            .WillOnce(
                DoAll(SetArgPointee<4>(values), Return(EtcdErrCode::EtcdOK)));
        std::map<PoolIdType, int64_t> out;
        ASSERT_EQ(-1, AllocStatisticHelper::CalculateSegmentAlloc(
            2, mockEtcdClient, &out));
    }
    {
        // 3. ListWithLimitAndRevision成功, 解析成功, bundle=1000, 获取个数为1
        LOG(INFO) << "start test3......";
        PageFileSegment segment;
        segment.set_segmentsize(1 << 30);
        segment.set_logicalpoolid(1);
        segment.set_chunksize(16*1024*1024);
        segment.set_startoffset(0);
        std::string encodeSegment;
        ASSERT_TRUE(
            NameSpaceStorageCodec::EncodeSegment(segment, &encodeSegment));
        std::vector<std::string> values{encodeSegment};
        std::string lastKey =
            NameSpaceStorageCodec::EncodeSegmentStoreKey(1, 0);
        EXPECT_CALL(*mockEtcdClient, ListWithLimitAndRevision(
            SEGMENTINFOKEYPREFIX, SEGMENTINFOKEYEND, GETBUNDLE, 2, _, _))
            .WillOnce(DoAll(SetArgPointee<4>(values), SetArgPointee<5>(lastKey),
                Return(EtcdErrCode::EtcdOK)));
        std::map<PoolIdType, int64_t> out;
        ASSERT_EQ(0, AllocStatisticHelper::CalculateSegmentAlloc(
            2, mockEtcdClient, &out));
        ASSERT_EQ(1, out.size());
        ASSERT_EQ(1 << 30, out[1]);
    }
    {
        // 4. ListWithLimitAndRevision成功, 解析成功
        // bundle=1000, 获取个数为1001
        LOG(INFO) << "start test4......";
        PageFileSegment segment;
        segment.set_segmentsize(1 << 30);
        segment.set_logicalpoolid(1);
        segment.set_chunksize(16*1024*1024);
        segment.set_startoffset(0);
        std::string encodeSegment;
        std::vector<std::string> values;
        ASSERT_TRUE(
            NameSpaceStorageCodec::EncodeSegment(segment, &encodeSegment));
        for (int i = 1; i <= 500; i++) {
            values.emplace_back(encodeSegment);
        }

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
        EXPECT_CALL(*mockEtcdClient, ListWithLimitAndRevision(
            SEGMENTINFOKEYPREFIX, SEGMENTINFOKEYEND, GETBUNDLE, 2, _, _))
            .WillOnce(DoAll(SetArgPointee<4>(values),
                            SetArgPointee<5>(lastKey1),
                            Return(EtcdErrCode::EtcdOK)));
        EXPECT_CALL(*mockEtcdClient, ListWithLimitAndRevision(
            lastKey1, SEGMENTINFOKEYEND, GETBUNDLE, 2, _, _))
            .WillOnce(DoAll(SetArgPointee<4>(
                std::vector<std::string>{encodeSegment, encodeSegment}),
                            SetArgPointee<5>(lastKey2),
                            Return(EtcdErrCode::EtcdOK)));

        std::map<PoolIdType, int64_t> out;
        ASSERT_EQ(0, AllocStatisticHelper::CalculateSegmentAlloc(
            2, mockEtcdClient, &out));
        ASSERT_EQ(2, out.size());
        ASSERT_EQ(500L * (1 << 30), out[1]);
        ASSERT_EQ(501L * (1 << 30), out[2]);
    }
}
}  // namespace mds
}  // namespace curve


