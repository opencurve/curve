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
#include "src/common/timeutility.h"
#include "src/tools/namespace_tool_core.h"
#include "test/tools/mock/mock_mds_client.h"

using ::testing::_;
using ::testing::Return;
using ::testing::DoAll;
using ::testing::SetArgPointee;
using ::testing::SaveArg;
using curve::tool::GetSegmentRes;

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
<<<<<<< HEAD
=======
    uint64_t stripeUnit = 32 * 1024;
    uint64_t stripeCount = 32;
>>>>>>> 7467df60... curvebs|tools:curve_ops_tool support expand volume

    // 1、正常情况
    EXPECT_CALL(*client_, CreateFile(_, _))
        .Times(1)
        .WillOnce(Return(0));
    ASSERT_EQ(0, namespaceTool.CreateFile(fileName, length));

    // 2、创建失败
    EXPECT_CALL(*client_, CreateFile(_, _))
        .Times(1)
        .WillOnce(Return(-1));
    ASSERT_EQ(-1, namespaceTool.CreateFile(fileName, length));
}

TEST_F(NameSpaceToolCoreTest, ExpandVolume) {
    curve::tool::NameSpaceToolCore namespaceTool(client_);
    std::string fileName = "/test";
    uint64_t length = 10 * segmentSize;
    // 1、正常情况
    EXPECT_CALL(*client_, ExpandVolume(_, _))
        .Times(1)
        .WillOnce(Return(0));
    ASSERT_EQ(0, namespaceTool.ExpandVolume(fileName, length));

    // 2、创建失败
    EXPECT_CALL(*client_, ExpandVolume(_, _))
        .Times(1)
        .WillOnce(Return(-1));
    ASSERT_EQ(-1, namespaceTool.ExpandVolume(fileName, length));
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
    std::vector<FileInfo> files;

    auto parseArg = [&](const std::string& value) -> uint64_t {
        uint64_t expireTime;
        ::curve::common::StringToTime(value, &expireTime);
        return expireTime;
    };

    // STEP 1: add an old format file
    {
        FileInfo fileInfo;
        GetFileInfoForTest(&fileInfo);
        fileInfo.set_filename("test-1");
        files.push_back(fileInfo);
    }

    // STEP 2: add new format files
    {
        uint64_t now = ::curve::common::TimeUtility::GetTimeofDaySec();
        auto expireTimes = std::vector<uint64_t>{
            0,               // now
            3,               // 3 seconds
            3 * 60,          // 3 minutes
            3 * 3600,        // 3 hours
            3 * 24 * 3600,   // 3 days
            90 * 24 * 3600,  // 3 months
        };

        for (auto& expireTime : expireTimes) {
            FileInfo fileInfo;
            GetFileInfoForTest(&fileInfo);
            std::string filename = "test-1-" + std::to_string(now - expireTime);
            fileInfo.set_filename(filename);
            files.push_back(fileInfo);
        }
    }

    // CASE 1: clean recycle bin success
    EXPECT_CALL(*client_, ListDir(_, _))
        .Times(1)
        .WillRepeatedly(DoAll(SetArgPointee<1>(files),
                              Return(0)));
    EXPECT_CALL(*client_, DeleteFile(_, _))
        .Times(7)
        .WillRepeatedly(Return(0));
    ASSERT_EQ(0, namespaceTool.CleanRecycleBin("/", parseArg("0s")));

    // CASE 2: clean recycle bin fail
    EXPECT_CALL(*client_, ListDir(_, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<1>(files),
                        Return(0)));
    EXPECT_CALL(*client_, DeleteFile(_, _))
        .Times(7)
        .WillOnce(Return(-1))
        .WillRepeatedly(Return(0));
    ASSERT_EQ(-1, namespaceTool.CleanRecycleBin("/", parseArg("0s")));

    // CASE 3: list dir fail
    EXPECT_CALL(*client_, ListDir(_, _))
        .Times(1)
        .WillOnce(Return(-1));
    ASSERT_EQ(-1, namespaceTool.CleanRecycleBin("/", parseArg("0s")));

    // CASE 4: clean recycle bin with expireTime is "3s"
    EXPECT_CALL(*client_, ListDir(_, _))
        .Times(1)
        .WillRepeatedly(DoAll(SetArgPointee<1>(files),
                              Return(0)));
    EXPECT_CALL(*client_, DeleteFile(_, _))
        .Times(6)
        .WillRepeatedly(Return(0));
    ASSERT_EQ(0, namespaceTool.CleanRecycleBin("/", parseArg("3s")));

    // CASE 5: clean recycle bin with expireTime is "3m"
    EXPECT_CALL(*client_, ListDir(_, _))
        .Times(1)
        .WillRepeatedly(DoAll(SetArgPointee<1>(files),
                              Return(0)));
    EXPECT_CALL(*client_, DeleteFile(_, _))
        .Times(5)
        .WillRepeatedly(Return(0));
    ASSERT_EQ(0, namespaceTool.CleanRecycleBin("/", parseArg("3m")));

    // CASE 6: clean recycle bin with expireTime is "3d"
    EXPECT_CALL(*client_, ListDir(_, _))
        .Times(1)
        .WillRepeatedly(DoAll(SetArgPointee<1>(files),
                              Return(0)));
    EXPECT_CALL(*client_, DeleteFile(_, _))
        .Times(3)
        .WillRepeatedly(Return(0));
    ASSERT_EQ(0, namespaceTool.CleanRecycleBin("/", parseArg("3d")));

    // CASE 7: clean recycle bin with different dirname
    auto cleanByDir = [&](const std::string& dirname, int deleteTimes) {
        EXPECT_CALL(*client_, ListDir(_, _))
            .Times(1)
            .WillRepeatedly(DoAll(SetArgPointee<1>(files),
                                  Return(0)));

        EXPECT_CALL(*client_, DeleteFile(_, _))
            .Times(deleteTimes)
            .WillRepeatedly(Return(0));
        ASSERT_EQ(0, namespaceTool.CleanRecycleBin(dirname, parseArg("0s")));
    };

    cleanByDir("/dir", 0);
    cleanByDir("/cindera", 0);
    cleanByDir("/cinder/a", 0);
    cleanByDir("/cinder/test/", 0);
    cleanByDir("/cinder/test/a", 0);
    cleanByDir("/cinder", 7);
    cleanByDir("/cinder/", 7);
    cleanByDir("/cinder///", 7);
    cleanByDir("/", 7);
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

TEST_F(NameSpaceToolCoreTest, TestUpdateThrottle) {
    curve::tool::NameSpaceToolCore namespaceTool(client_);

    // 1. throttle type is invalid
    {
        EXPECT_CALL(*client_, UpdateFileThrottleParams(_, _))
            .Times(0);

        ASSERT_EQ(-1, namespaceTool.UpdateFileThrottle("/test", "hello", 10000,
                                                       0, 0));
    }

    // 2. burst and burstLength is not specified
    {
        curve::mds::ThrottleParams params;
        EXPECT_CALL(*client_, UpdateFileThrottleParams(_, _))
            .WillOnce(
                DoAll(SaveArg<1>(&params), Return(0)));

        ASSERT_EQ(0, namespaceTool.UpdateFileThrottle("/test", "BPS_TOTAL",
                                                       10000, -1, -1));
        ASSERT_EQ(10000, params.limit());
        ASSERT_FALSE(params.has_burst());
        ASSERT_FALSE(params.has_burstlength());
    }

    // 3. burst lower than limit
    {
        curve::mds::ThrottleParams params;
        EXPECT_CALL(*client_, UpdateFileThrottleParams(_, _))
            .Times(0);

        ASSERT_EQ(-1, namespaceTool.UpdateFileThrottle("/test", "BPS_TOTAL",
                                                       10000, 5000, -1));
    }

    // 4. burstLength is not specified
    {
        curve::mds::ThrottleParams params;
        EXPECT_CALL(*client_, UpdateFileThrottleParams(_, _))
            .Times(1)
            .WillOnce(DoAll(SaveArg<1>(&params), Return(0)));

        ASSERT_EQ(0, namespaceTool.UpdateFileThrottle("/test", "BPS_TOTAL",
                                                       10000, 50000, -1));
        ASSERT_EQ(10000, params.limit());
        ASSERT_EQ(50000, params.burst());
        ASSERT_EQ(1, params.burstlength());
    }

    // 5. burst and burstLength is specified
    {
        curve::mds::ThrottleParams params;
        EXPECT_CALL(*client_, UpdateFileThrottleParams(_, _))
            .Times(1)
            .WillOnce(DoAll(SaveArg<1>(&params), Return(0)));

        ASSERT_EQ(0, namespaceTool.UpdateFileThrottle("/test", "BPS_TOTAL",
                                                       10000, 50000, 10));
        ASSERT_EQ(10000, params.limit());
        ASSERT_EQ(50000, params.burst());
        ASSERT_EQ(10, params.burstlength());
    }
}
