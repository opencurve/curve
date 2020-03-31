/*
 * Project: curve
 * File Created: Friday, 28th June 2019 2:29:14 pm
 * Author: tongguangxun
 * Copyright (c)￼ 2018 netease
 */
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <gflags/gflags.h>
#include <fiu-control.h>

#include "src/tools/consistency_check.h"
#include "test/tools/mock_namespace_tool_core.h"
#include "test/tools/mock_chunkserver_client.h"

DECLARE_bool(check_hash);

using ::testing::_;
using ::testing::Return;
using ::testing::DoAll;
using ::testing::SetArgPointee;

extern uint32_t segment_size;
extern uint32_t chunk_size;

class ConsistencyCheckTest : public ::testing::Test {
 public:
    void SetUp() {
        nameSpaceTool_ =
                std::make_shared<curve::tool::MockNameSpaceToolCore>();
        csClient_ = std::make_shared<curve::tool::MockChunkServerClient>();
    }

    void TearDown() {
        nameSpaceTool_ = nullptr;
        csClient_ = nullptr;
    }

    void GetSegmentForTest(PageFileSegment* segment) {
        segment->set_logicalpoolid(1);
        segment->set_segmentsize(segment_size);
        segment->set_chunksize(chunk_size);
        segment->set_startoffset(0);
        for (int i = 0; i < 10; ++i) {
            auto chunk = segment->add_chunks();
            chunk->set_copysetid(1000 + i);
            chunk->set_chunkid(2000 + i);
        }
    }

    void GetCsLocForTest(ChunkServerLocation* csLoc, uint64_t csId) {
        csLoc->set_chunkserverid(csId);
        csLoc->set_hostip("127.0.0.1");
        csLoc->set_port(8200 + csId);
    }

    void GetCopysetStatusForTest(CopysetStatusResponse* response,
                        int64_t applyingIndex = 1111,
                        bool ok = true) {
        if (ok) {
            response->set_status(COPYSET_OP_STATUS::COPYSET_OP_STATUS_SUCCESS);
        } else {
            response->set_status(
                COPYSET_OP_STATUS::COPYSET_OP_STATUS_COPYSET_NOTEXIST);
        }
        if (ok) {
            response->set_knownappliedindex(applyingIndex);
        }
    }

 public:
    std::shared_ptr<curve::tool::MockNameSpaceToolCore> nameSpaceTool_;
    std::shared_ptr<curve::tool::MockChunkServerClient> csClient_;
};

TEST_F(ConsistencyCheckTest, SupportCommand) {
    curve::tool::ConsistencyCheck cfc(nameSpaceTool_, csClient_);
    ASSERT_TRUE(cfc.SupportCommand("check-consistency"));
    ASSERT_FALSE(cfc.SupportCommand("check-chunkserver"));
}

TEST_F(ConsistencyCheckTest, Consistency) {
    std::vector<PageFileSegment> segments;
    for (int i = 0; i < 3; ++i) {
        PageFileSegment segment;
        GetSegmentForTest(&segment);
        segments.emplace_back(segment);
    }
    std::vector<ChunkServerLocation> csLocs;
    for (uint64_t i = 1; i <= 3; ++i) {
        ChunkServerLocation csLoc;
        GetCsLocForTest(&csLoc, i);
        csLocs.emplace_back(csLoc);
    }
    CopysetStatusResponse response;
    GetCopysetStatusForTest(&response);

    // 设置期望
    EXPECT_CALL(*nameSpaceTool_, Init(_))
        .Times(2)
        .WillRepeatedly(Return(0));
    EXPECT_CALL(*nameSpaceTool_, GetFileSegments(_, _))
        .Times(2)
        .WillRepeatedly(DoAll(SetArgPointee<1>(segments),
                        Return(0)));
    EXPECT_CALL(*nameSpaceTool_, GetChunkServerListInCopySet(_, _, _))
        .Times(20)
        .WillRepeatedly(DoAll(SetArgPointee<2>(csLocs),
                        Return(0)));
    EXPECT_CALL(*csClient_, Init(_))
        .Times(90)
        .WillRepeatedly(Return(0));
    EXPECT_CALL(*csClient_, GetCopysetStatus(_, _))
        .Times(60)
        .WillRepeatedly(DoAll(SetArgPointee<1>(response),
                        Return(0)));
    EXPECT_CALL(*csClient_, GetChunkHash(_, _))
        .Times(30)
        .WillRepeatedly(DoAll(SetArgPointee<1>("1111"),
                        Return(0)));
    // 1、检查hash
    FLAGS_check_hash = true;
    curve::tool::ConsistencyCheck cfc1(nameSpaceTool_, csClient_);
    cfc1.PrintHelp("check-consistency");
    cfc1.PrintHelp("check-nothing");
    ASSERT_EQ(0, cfc1.RunCommand("check-consistency"));
    // 2、检查applyIndex
    FLAGS_check_hash = false;
    curve::tool::ConsistencyCheck cfc2(nameSpaceTool_, csClient_);
    ASSERT_EQ(0, cfc2.RunCommand("check-consistency"));
    ASSERT_EQ(-1, cfc2.RunCommand("check-nothing"));

    // mds返回副本为空的情况
    EXPECT_CALL(*nameSpaceTool_, GetFileSegments(_, _))
        .Times(2)
        .WillRepeatedly(DoAll(SetArgPointee<1>(segments),
                        Return(0)));
    EXPECT_CALL(*nameSpaceTool_, GetChunkServerListInCopySet(_, _, _))
        .Times(20)
        .WillRepeatedly(DoAll(SetArgPointee<2>(
                        std::vector<ChunkServerLocation>()),
                        Return(0)));
    ASSERT_EQ(0, cfc2.RunCommand("check-consistency"));
    FLAGS_check_hash = true;
    ASSERT_EQ(0, cfc2.RunCommand("check-consistency"));
}

TEST_F(ConsistencyCheckTest, NotConsistency) {
    std::vector<PageFileSegment> segments;
    for (int i = 0; i < 3; ++i) {
        PageFileSegment segment;
        GetSegmentForTest(&segment);
        segments.emplace_back(segment);
    }
    std::vector<ChunkServerLocation> csLocs;
    for (uint64_t i = 1; i <= 3; ++i) {
        ChunkServerLocation csLoc;
        GetCsLocForTest(&csLoc, i);
        csLocs.emplace_back(csLoc);
    }
    CopysetStatusResponse response1;
    GetCopysetStatusForTest(&response1);
    CopysetStatusResponse response2;
    GetCopysetStatusForTest(&response2, 1111);
    CopysetStatusResponse response3;
    GetCopysetStatusForTest(&response3, 2222);

    // 设置期望
    EXPECT_CALL(*nameSpaceTool_, Init(_))
        .Times(3)
        .WillRepeatedly(Return(0));
    EXPECT_CALL(*nameSpaceTool_, GetFileSegments(_, _))
        .Times(3)
        .WillRepeatedly(DoAll(SetArgPointee<1>(segments),
                        Return(0)));
    EXPECT_CALL(*nameSpaceTool_, GetChunkServerListInCopySet(_, _, _))
        .Times(3)
        .WillRepeatedly(DoAll(SetArgPointee<2>(csLocs),
                        Return(0)));

    // 1、检查hash，apply index一致，hash不一致
    FLAGS_check_hash = true;
    EXPECT_CALL(*csClient_, Init(_))
        .Times(5)
        .WillRepeatedly(Return(0));
    EXPECT_CALL(*csClient_, GetCopysetStatus(_, _))
        .Times(3)
        .WillRepeatedly(DoAll(SetArgPointee<1>(response1),
                        Return(0)));
    EXPECT_CALL(*csClient_, GetChunkHash(_, _))
        .Times(2)
        .WillOnce(DoAll(SetArgPointee<1>("2222"),
                        Return(0)))
        .WillOnce(DoAll(SetArgPointee<1>("1111"),
                        Return(0)));
    curve::tool::ConsistencyCheck cfc1(nameSpaceTool_, csClient_);
    ASSERT_EQ(-1, cfc1.RunCommand("check-consistency"));

    // 2、检查hash的时候apply index不一致
    EXPECT_CALL(*csClient_, Init(_))
        .Times(2)
        .WillRepeatedly(Return(0));
    EXPECT_CALL(*csClient_, GetCopysetStatus(_, _))
        .Times(2)
        .WillOnce(DoAll(SetArgPointee<1>(response1),
                        Return(0)))
        .WillOnce(DoAll(SetArgPointee<1>(response3),
                        Return(0)));
    curve::tool::ConsistencyCheck cfc2(nameSpaceTool_, csClient_);
    ASSERT_EQ(-1, cfc2.RunCommand("check-consistency"));

    // 3、检查applyIndex
    FLAGS_check_hash = false;
    EXPECT_CALL(*csClient_, Init(_))
        .Times(2)
        .WillRepeatedly(Return(0));
    EXPECT_CALL(*csClient_, GetCopysetStatus(_, _))
        .Times(2)
        .WillOnce(DoAll(SetArgPointee<1>(response1),
                        Return(0)))
        .WillOnce(DoAll(SetArgPointee<1>(response3),
                        Return(0)));
    curve::tool::ConsistencyCheck cfc3(nameSpaceTool_, csClient_);
    ASSERT_EQ(-1, cfc3.RunCommand("check-consistency"));
}

TEST_F(ConsistencyCheckTest, CheckError) {
    std::vector<PageFileSegment> segments;
    for (int i = 0; i < 3; ++i) {
        PageFileSegment segment;
        GetSegmentForTest(&segment);
        segments.emplace_back(segment);
    }
    std::vector<ChunkServerLocation> csLocs;
    for (uint64_t i = 1; i <= 3; ++i) {
        ChunkServerLocation csLoc;
        GetCsLocForTest(&csLoc, i);
        csLocs.emplace_back(csLoc);
    }
    FLAGS_check_hash = false;
    curve::tool::ConsistencyCheck cfc(nameSpaceTool_, csClient_);
    // 0、Init失败
    EXPECT_CALL(*nameSpaceTool_, Init(_))
        .Times(1)
        .WillOnce(Return(-1));
    ASSERT_EQ(-1, cfc.RunCommand("check-consistency"));
    // 1、获取segment失败
    EXPECT_CALL(*nameSpaceTool_, Init(_))
        .Times(1)
        .WillOnce(Return(0));
    EXPECT_CALL(*nameSpaceTool_, GetFileSegments(_, _))
        .Times(1)
        .WillOnce(Return(-1));
    ASSERT_EQ(-1, cfc.RunCommand("check-consistency"));

    // 2、获取chunkserver list失败
    EXPECT_CALL(*nameSpaceTool_, GetFileSegments(_, _))
        .Times(4)
        .WillRepeatedly(DoAll(SetArgPointee<1>(segments),
                        Return(0)));
    EXPECT_CALL(*nameSpaceTool_, GetChunkServerListInCopySet(_, _, _))
        .Times(1)
        .WillOnce(Return(-1));
    ASSERT_EQ(-1, cfc.RunCommand("check-consistency"));

    // 3、init 向chunkserverclient init失败
    EXPECT_CALL(*nameSpaceTool_, GetChunkServerListInCopySet(_, _, _))
        .Times(3)
        .WillRepeatedly(DoAll(SetArgPointee<2>(csLocs),
                        Return(0)));
    EXPECT_CALL(*csClient_, Init(_))
        .Times(1)
        .WillOnce(Return(-1));
    ASSERT_EQ(-1, cfc.RunCommand("check-consistency"));

    // 4、从chunkserver获取copyset status失败
    EXPECT_CALL(*csClient_, Init(_))
        .Times(1)
        .WillOnce(Return(0));
    EXPECT_CALL(*csClient_, GetCopysetStatus(_, _))
        .Times(1)
        .WillOnce(Return(-1));
    ASSERT_EQ(-1, cfc.RunCommand("check-consistency"));

    // 5、从chunkserver获取chunk hash失败
    FLAGS_check_hash = true;
    CopysetStatusResponse response1;
    GetCopysetStatusForTest(&response1);
    EXPECT_CALL(*csClient_, Init(_))
        .Times(4)
        .WillRepeatedly(Return(0));
    EXPECT_CALL(*csClient_, GetCopysetStatus(_, _))
        .Times(3)
        .WillRepeatedly(DoAll(SetArgPointee<1>(response1),
                        Return(0)));
    EXPECT_CALL(*csClient_, GetChunkHash(_, _))
        .Times(1)
        .WillOnce(Return(-1));
    ASSERT_EQ(-1, cfc.RunCommand("check-consistency"));
}
