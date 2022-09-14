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
 * File Created: 2019-12-03
 * Author: charisu
 */

#include <gtest/gtest.h>
#include "src/tools/namespace_tool_core.h"
#include "test/tools/mock/mock_mds_client.h"

using ::testing::_;
using ::testing::Return;
using ::testing::DoAll;
using ::testing::SetArgPointee;
using curve::tool::GetSegmentRes;
using curve::tool::CreateFileContext;

DECLARE_bool(isTest);
DECLARE_string(fileName);
DECLARE_uint64(offset);

class NameSpaceToolCoreTest : public ::testing::Test {
 protected:
    void SetUp() {
        client_ = std::make_shared<curve::tool::MockMDSClient>();
    }
    void TearDown() {
        client_ = nullptr;
    }

    void GetFileInfoForTest(FileInfo* fileInfo) {
        fileInfo->set_id(1);
        fileInfo->set_filename("test");
        fileInfo->set_parentid(0);
        fileInfo->set_filetype(curve::mds::FileType::INODE_PAGEFILE);
        fileInfo->set_segmentsize(segmentSize);
        fileInfo->set_length(5 * segmentSize);
        fileInfo->set_originalfullpathname("/cinder/test");
        fileInfo->set_ctime(1573546993000000);
    }

    void GetCsLocForTest(ChunkServerLocation* csLoc, uint64_t csId) {
        csLoc->set_chunkserverid(csId);
        csLoc->set_hostip("127.0.0.1");
        csLoc->set_port(9191 + csId);
    }

    void GetSegmentForTest(PageFileSegment* segment) {
        segment->set_logicalpoolid(1);
        segment->set_segmentsize(segmentSize);
        segment->set_chunksize(chunkSize);
        segment->set_startoffset(0);
        for (int i = 0; i < 10; ++i) {
            auto chunk = segment->add_chunks();
            chunk->set_copysetid(1000 + i);
            chunk->set_chunkid(2000 + i);
        }
    }
    uint64_t segmentSize = 1 * 1024 * 1024 * 1024ul;
    uint64_t chunkSize = 16 * 1024 * 1024;
    std::shared_ptr<curve::tool::MockMDSClient> client_;
};

TEST_F(NameSpaceToolCoreTest, Init) {
    EXPECT_CALL(*client_, Init(_))
        .Times(2)
        .WillOnce(Return(0))
        .WillOnce(Return(-1));
    curve::tool::NameSpaceToolCore namespaceTool(client_);
    ASSERT_EQ(0, namespaceTool.Init("127.0.0.1:6666"));
    ASSERT_EQ(-1, namespaceTool.Init("127.0.0.1:6666"));
}

TEST_F(NameSpaceToolCoreTest, GetFileInfo) {
    curve::tool::NameSpaceToolCore namespaceTool(client_);
    std::string fileName = "/test";
    FileInfo fileInfo;
    FileInfo expected;
    GetFileInfoForTest(&expected);

    EXPECT_CALL(*client_, GetFileInfo(_, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<1>(expected),
                        Return(0)));
    ASSERT_EQ(0, namespaceTool.GetFileInfo(fileName, &fileInfo));
    ASSERT_EQ(expected.DebugString(), fileInfo.DebugString());

    EXPECT_CALL(*client_, GetFileInfo(_, _))
        .Times(1)
        .WillOnce(Return(-1));
    ASSERT_EQ(-1, namespaceTool.GetFileInfo(fileName, &fileInfo));
}

TEST_F(NameSpaceToolCoreTest, ListDir) {
    curve::tool::NameSpaceToolCore namespaceTool(client_);
    std::string fileName = "/test";
    std::vector<FileInfo> files;
    std::vector<FileInfo> expected;
    for (int i = 0; i < 3; i++) {
        FileInfo tmp;
        GetFileInfoForTest(&tmp);
        expected.emplace_back(tmp);
    }

    EXPECT_CALL(*client_, ListDir(_, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<1>(expected),
                        Return(0)));
    ASSERT_EQ(0, namespaceTool.ListDir(fileName, &files));
    ASSERT_EQ(expected.size(), files.size());
    for (uint64_t i = 0; i < expected.size(); ++i) {
        ASSERT_EQ(expected[i].DebugString(), files[i].DebugString());
    }

    EXPECT_CALL(*client_, ListDir(_, _))
        .Times(1)
        .WillOnce(Return(-1));
    ASSERT_EQ(-1, namespaceTool.ListDir(fileName, &files));
}

TEST_F(NameSpaceToolCoreTest, CreateFile) {
    curve::tool::NameSpaceToolCore namespaceTool(client_);
    std::string fileName = "/test";
    uint64_t length = 5 * segmentSize;
    uint64_t stripeUnit = 32 * 1024 *1024;
    uint64_t stripeCount = 32;
    std::string pstName = "";

    // 1、正常情况
    EXPECT_CALL(*client_, CreateFile(_))
        .Times(1)
        .WillOnce(Return(0));

    CreateFileContext context;
    context.type = curve::mds::FileType::INODE_PAGEFILE;
    context.name = fileName;
    context.length = length;
    context.stripeUnit = stripeUnit;
    context.stripeCount = stripeCount;
    context.poolset = pstName;

    ASSERT_EQ(0, namespaceTool.CreateFile(context));

    // 2、创建失败
    EXPECT_CALL(*client_, CreateFile(_))
        .Times(1)
        .WillOnce(Return(-1));
    ASSERT_EQ(-1, namespaceTool.CreateFile(context));
}

TEST_F(NameSpaceToolCoreTest, ExtendVolume) {
    curve::tool::NameSpaceToolCore namespaceTool(client_);
    std::string fileName = "/test";
    uint64_t length = 10 * segmentSize;
    // 1、正常情况
    EXPECT_CALL(*client_, ExtendVolume(_, _))
        .Times(1)
        .WillOnce(Return(0));
    ASSERT_EQ(0, namespaceTool.ExtendVolume(fileName, length));

    // 2、创建失败
    EXPECT_CALL(*client_, ExtendVolume(_, _))
        .Times(1)
        .WillOnce(Return(-1));
    ASSERT_EQ(-1, namespaceTool.ExtendVolume(fileName, length));
}

TEST_F(NameSpaceToolCoreTest, DeleteFile) {
    curve::tool::NameSpaceToolCore namespaceTool(client_);
    std::string fileName = "/test";
    bool forceDelete = false;

    // 1、正常情况
    EXPECT_CALL(*client_, DeleteFile(_, _))
        .Times(1)
        .WillOnce(Return(0));
    ASSERT_EQ(0, namespaceTool.DeleteFile(fileName, forceDelete));

    // 2、创建失败
    EXPECT_CALL(*client_, DeleteFile(_, _))
        .Times(1)
        .WillOnce(Return(-1));
    ASSERT_EQ(-1, namespaceTool.DeleteFile(fileName, forceDelete));
}

TEST_F(NameSpaceToolCoreTest, GetChunkServerListInCopySet) {
    curve::tool::NameSpaceToolCore namespaceTool(client_);
    PoolIdType logicalPoolId = 1;
    CopySetIdType copysetId = 100;
    std::vector<ChunkServerLocation> csLocs;
    std::vector<ChunkServerLocation> expected;
    for (uint64_t i = 0; i < 3; ++i) {
        ChunkServerLocation csLoc;
        GetCsLocForTest(&csLoc, i);
        expected.emplace_back(csLoc);
    }

    // 1、正常情况
    EXPECT_CALL(*client_, GetChunkServerListInCopySet(_, _, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<2>(expected),
                        Return(0)));
    ASSERT_EQ(0, namespaceTool.GetChunkServerListInCopySet(logicalPoolId,
                                                        copysetId, &csLocs));
    ASSERT_EQ(expected.size(), csLocs.size());
    for (uint64_t i = 0; i < expected.size(); ++i) {
        ASSERT_EQ(expected[i].DebugString(), csLocs[i].DebugString());
    }
    // 2、失败
    EXPECT_CALL(*client_, GetChunkServerListInCopySet(_, _, _))
        .Times(1)
        .WillOnce(Return(-1));
    ASSERT_EQ(-1, namespaceTool.GetChunkServerListInCopySet(logicalPoolId,
                                                        copysetId, &csLocs));
}

TEST_F(NameSpaceToolCoreTest, CleanRecycleBin) {
    curve::tool::NameSpaceToolCore namespaceTool(client_);
    FileInfo fileInfo;
    GetFileInfoForTest(&fileInfo);
    std::vector<FileInfo> files;
    for (uint64_t i = 0; i < 3; ++i) {
        files.emplace_back(fileInfo);
    }

    // 1、正常情况
    EXPECT_CALL(*client_, ListDir(_, _))
        .Times(2)
        .WillRepeatedly(DoAll(SetArgPointee<1>(files),
                        Return(0)));
    EXPECT_CALL(*client_, DeleteFile(_, _))
        .Times(6)
        .WillRepeatedly(Return(0));
    FLAGS_fileName = "";
    ASSERT_EQ(0, namespaceTool.CleanRecycleBin());
    // 带fileName清理RecycleBin
    ASSERT_EQ(0, namespaceTool.CleanRecycleBin("/cinder"));

    // 2、list RecycleBin失败
    EXPECT_CALL(*client_, ListDir(_, _))
        .Times(1)
        .WillOnce(Return(-1));
    ASSERT_EQ(-1, namespaceTool.CleanRecycleBin());

    // 3、删除失败
    EXPECT_CALL(*client_, ListDir(_, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<1>(files),
                        Return(0)));
    EXPECT_CALL(*client_, DeleteFile(_, _))
        .Times(3)
        .WillOnce(Return(-1))
        .WillRepeatedly(Return(0));
    ASSERT_EQ(-1, namespaceTool.CleanRecycleBin());
}


TEST_F(NameSpaceToolCoreTest, GetAllocatedSize) {
    curve::tool::NameSpaceToolCore namespaceTool(client_);
    // 1、正常情况
    uint64_t allocSize;
    EXPECT_CALL(*client_, GetAllocatedSize(_, _, _))
        .Times(1)
        .WillOnce(Return(0));
    ASSERT_EQ(0, namespaceTool.GetAllocatedSize("/test", &allocSize));
}

TEST_F(NameSpaceToolCoreTest, QueryChunkCopyset) {
    curve::tool::NameSpaceToolCore namespaceTool(client_);
    FileInfo fileInfo;
    GetFileInfoForTest(&fileInfo);
    PageFileSegment segment;
    GetSegmentForTest(&segment);
    std::string fileName = "/test";
    uint64_t offset = chunkSize + 1;
    uint64_t chunkId;
    std::pair<uint32_t, uint32_t> copyset;

    // 正常情况
    EXPECT_CALL(*client_, GetFileInfo(_, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<1>(fileInfo),
                        Return(0)));
    EXPECT_CALL(*client_, GetSegmentInfo(_, _, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<2>(segment),
                        Return(GetSegmentRes::kOK)));
    ASSERT_EQ(0, namespaceTool.QueryChunkCopyset(fileName, offset,
                                            &chunkId, &copyset));
    ASSERT_EQ(2001, chunkId);
    ASSERT_EQ(1, copyset.first);
    ASSERT_EQ(1001, copyset.second);

    // GetFileInfo失败
    EXPECT_CALL(*client_, GetFileInfo(_, _))
        .Times(1)
        .WillOnce(Return(-1));
    ASSERT_EQ(-1, namespaceTool.QueryChunkCopyset(fileName, offset,
                                            &chunkId, &copyset));

    // GetSegmentInfo失败
    EXPECT_CALL(*client_, GetFileInfo(_, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<1>(fileInfo),
                        Return(0)));
    EXPECT_CALL(*client_, GetSegmentInfo(_, _, _))
        .Times(1)
        .WillOnce(Return(GetSegmentRes::kOtherError));
    ASSERT_EQ(-1, namespaceTool.QueryChunkCopyset(fileName, offset,
                                            &chunkId, &copyset));
}

TEST_F(NameSpaceToolCoreTest, GetFileSegments) {
    curve::tool::NameSpaceToolCore namespaceTool(client_);
    std::string fileName = "/test/";
    std::vector<PageFileSegment> segments;
    FileInfo fileInfo;
    GetFileInfoForTest(&fileInfo);
    PageFileSegment expected;
    GetSegmentForTest(&expected);

    // 1、正常情况
    EXPECT_CALL(*client_, GetFileInfo(_, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<1>(fileInfo),
                        Return(0)));
    EXPECT_CALL(*client_, GetSegmentInfo(_, _, _))
        .Times(5)
        .WillOnce(Return(GetSegmentRes::kSegmentNotAllocated))
        .WillRepeatedly(DoAll(SetArgPointee<2>(expected),
                        Return(GetSegmentRes::kOK)));
    ASSERT_EQ(0, namespaceTool.GetFileSegments(fileName, &segments));
    ASSERT_EQ(4, segments.size());
    for (uint64_t i = 0; i < segments.size(); ++i) {
        ASSERT_EQ(expected.DebugString(), segments[i].DebugString());
    }

    // 2、GetFileInfo失败的情况
    EXPECT_CALL(*client_, GetFileInfo(_, _))
        .Times(1)
        .WillOnce(Return(-1));
    ASSERT_EQ(-1, namespaceTool.GetFileSegments(fileName, &segments));

    // 3、获取segment失败
    EXPECT_CALL(*client_, GetFileInfo(_, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<1>(fileInfo),
                        Return(0)));
    EXPECT_CALL(*client_, GetSegmentInfo(_, _, _))
        .Times(1)
        .WillOnce(Return(GetSegmentRes::kOtherError));
    ASSERT_EQ(-1, namespaceTool.GetFileSegments(fileName, &segments));
}

TEST_F(NameSpaceToolCoreTest, GetFileSize) {
    curve::tool::NameSpaceToolCore namespaceTool(client_);
    // 1、正常情况
    uint64_t size;
    EXPECT_CALL(*client_, GetFileSize(_, _))
        .Times(1)
        .WillOnce(Return(0));
    ASSERT_EQ(0, namespaceTool.GetFileSize("/test", &size));
}

