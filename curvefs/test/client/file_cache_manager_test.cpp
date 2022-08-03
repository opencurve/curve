/*
 *  Copyright (c) 2021 NetEase Inc.
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
 * Created Date: Wed Mar 23 2022
 * Author: huyao
 */

#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include "curvefs/src/client/s3/client_s3_adaptor.h"
#include "curvefs/src/client/cache/client_cache_manager.h"
#include "curvefs/test/client/mock_client_s3_cache_manager.h"
#include "curvefs/test/client/mock_inode_cache_manager.h"
#include "curvefs/test/client/mock_client_s3.h"

namespace curvefs {
namespace client {
using ::testing::_;
using ::testing::DoAll;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::SetArgPointee;
using ::testing::SetArgReferee;
using ::testing::WithArg;

class FileCacheManagerTest : public testing::Test {
 protected:
    FileCacheManagerTest() {}
    ~FileCacheManagerTest() {}
    void SetUp() override {
        uint64_t inodeId = 1;
        uint64_t fsId = 2;
        S3ClientAdaptorOption option;
        option.blockSize = 1 * 1024 * 1024;
        option.chunkSize = 4 * 1024 * 1024;
        option.pageSize = 64 * 1024;
        option.intervalSec = 5000;
        option.flushIntervalSec = 5000;
        option.readCacheMaxByte = 104857600;
        option.writeCacheMaxByte = 10485760000;
        option.diskCacheOpt.diskCacheType = (DiskCacheType)0;
        option.chunkFlushThreads = 5;
        s3ClientAdaptor_ = new S3Adaptor();
        auto fsCacheManager_ = std::make_shared<FsCacheManager>(
            s3ClientAdaptor_, option.readCacheMaxByte,
            option.writeCacheMaxByte);
        mockInodeManager_ = std::make_shared<MockInodeCacheManager>();
        mockS3Client_ = std::make_shared<MockS3Client>();
        s3ClientAdaptor_->Init(option, mockS3Client_, mockInodeManager_,
                               nullptr, fsCacheManager_, nullptr);
        s3ClientAdaptor_->SetFsId(fsId);
        fileCacheManager_ =
            std::make_shared<FileCacheManager>(fsId, inodeId, s3ClientAdaptor_);
        mockChunkCacheManager_ = std::make_shared<MockChunkCacheManager>();
    }

    void TearDown() override {
        // s3ClientAdaptor_->Stop();
        delete s3ClientAdaptor_;
        s3ClientAdaptor_ = nullptr;
    }

 protected:
    S3Adaptor *s3ClientAdaptor_;
    std::shared_ptr<FileCacheManager> fileCacheManager_;
    std::shared_ptr<MockChunkCacheManager> mockChunkCacheManager_;
    std::shared_ptr<MockInodeCacheManager> mockInodeManager_;
    std::shared_ptr<MockS3Client> mockS3Client_;
};

TEST_F(FileCacheManagerTest, test_FindOrCreateChunkCacheManager) {
    uint64_t index = 0;

    auto chunkCaCheManager =
        fileCacheManager_->FindOrCreateChunkCacheManager(index);
    ASSERT_EQ(chunkCaCheManager,
              fileCacheManager_->FindOrCreateChunkCacheManager(index));
}

TEST_F(FileCacheManagerTest, test_release_cache) {
    uint64_t index = 0;

    auto chunkCaCheManager =
        fileCacheManager_->FindOrCreateChunkCacheManager(index);
    fileCacheManager_->ReleaseCache();
    auto chunkCaCheManager1 =
        fileCacheManager_->FindOrCreateChunkCacheManager(index);
    ASSERT_NE(chunkCaCheManager, chunkCaCheManager1);
}

TEST_F(FileCacheManagerTest, test_flush_ok) {
    uint64_t index = 0;

    EXPECT_CALL(*mockChunkCacheManager_, Flush(_, _, _))
        .WillOnce(Return(CURVEFS_ERROR::OK));
    fileCacheManager_->SetChunkCacheManagerForTest(index,
                                                   mockChunkCacheManager_);
    ASSERT_EQ(CURVEFS_ERROR::OK, fileCacheManager_->Flush(true, true));
}

TEST_F(FileCacheManagerTest, test_flush_fail) {
    uint64_t index = 0;

    EXPECT_CALL(*mockChunkCacheManager_, Flush(_, _, _))
        .WillOnce(Return(CURVEFS_ERROR::INTERNAL));
    fileCacheManager_->SetChunkCacheManagerForTest(index,
                                                   mockChunkCacheManager_);
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, fileCacheManager_->Flush(true, true));
}

TEST_F(FileCacheManagerTest, test_new_write) {
    uint64_t offset = 0;
    uint64_t len = 5 * 1024 * 1024;
    char buf[len] = {0};

    memset(buf, 'a', len);
    EXPECT_CALL(*mockChunkCacheManager_, FindWriteableDataCache(_, _, _, _))
        .WillOnce(Return(nullptr))
        .WillOnce(Return(nullptr));
    EXPECT_CALL(*mockChunkCacheManager_, WriteNewDataCache(_, _, _, _))
        .WillOnce(Return())
        .WillOnce(Return());
    fileCacheManager_->SetChunkCacheManagerForTest(0, mockChunkCacheManager_);
    fileCacheManager_->SetChunkCacheManagerForTest(1, mockChunkCacheManager_);
    ASSERT_EQ(len, fileCacheManager_->Write(offset, len, buf));
}

TEST_F(FileCacheManagerTest, test_old_write) {
    uint64_t offset = 0;
    uint64_t len = 1024;
    char buf[len] = {0};

    auto dataCache = std::make_shared<MockDataCache>(s3ClientAdaptor_, nullptr,
                                                     offset, 0, nullptr);
    EXPECT_CALL(*dataCache, Write(_, _, _, _)).WillOnce(Return());
    EXPECT_CALL(*mockChunkCacheManager_, FindWriteableDataCache(_, _, _, _))
        .WillOnce(Return(dataCache));
    EXPECT_CALL(*mockChunkCacheManager_, ReleaseCache()).WillOnce(Return());
    fileCacheManager_->SetChunkCacheManagerForTest(0, mockChunkCacheManager_);
    ASSERT_EQ(len, fileCacheManager_->Write(offset, len, buf));
    fileCacheManager_->ReleaseCache();
}

TEST_F(FileCacheManagerTest, test_read_cache) {
    uint64_t inodeId = 1;
    uint64_t offset = 0;
    uint64_t len = 5 * 1024 * 1024;
    char buf[len] = {0};
    ReadRequest request;
    std::vector<ReadRequest> requests;
    std::vector<ReadRequest> emptyRequests;
    requests.emplace_back(request);
    EXPECT_CALL(*mockChunkCacheManager_, ReadByWriteCache(_, _, _, _, _))
        .WillOnce(DoAll(SetArgPointee<4>(requests), Return()))
        .WillOnce(DoAll(SetArgPointee<4>(requests), Return()));
    EXPECT_CALL(*mockChunkCacheManager_, ReadByReadCache(_, _, _, _, _))
        .WillOnce(DoAll(SetArgPointee<4>(emptyRequests), Return()))
        .WillOnce(DoAll(SetArgPointee<4>(emptyRequests), Return()));
    fileCacheManager_->SetChunkCacheManagerForTest(0, mockChunkCacheManager_);
    fileCacheManager_->SetChunkCacheManagerForTest(1, mockChunkCacheManager_);

    ASSERT_EQ(len, fileCacheManager_->Read(inodeId, offset, len, buf));
}

TEST_F(FileCacheManagerTest, test_read_getinode_fail) {
    uint64_t inodeId = 1;
    uint64_t offset = 0;
    uint64_t len = 1024;
    char buf[len] = {0};

    ReadRequest request;
    std::vector<ReadRequest> requests;
    request.index = 0;
    request.chunkPos = offset;
    request.len = len;
    request.bufOffset = 0;
    requests.emplace_back(request);
    EXPECT_CALL(*mockChunkCacheManager_, ReadByWriteCache(_, _, _, _, _))
        .WillOnce(DoAll(SetArgPointee<4>(requests), Return()));
    EXPECT_CALL(*mockChunkCacheManager_, ReadByReadCache(_, _, _, _, _))
        .WillOnce(DoAll(SetArgPointee<4>(requests), Return()));
    fileCacheManager_->SetChunkCacheManagerForTest(0, mockChunkCacheManager_);
    EXPECT_CALL(*mockInodeManager_, GetInode(_, _))
        .WillOnce(Return(CURVEFS_ERROR::NOTEXIST));
    ASSERT_EQ(-1, fileCacheManager_->Read(inodeId, offset, len, buf));
}

TEST_F(FileCacheManagerTest, test_read_s3) {
    uint64_t inodeId = 1;
    uint64_t offset = 0;
    uint64_t len = 1024;
    int length = len;
    char *buf = new char[len];
    char *tmpbuf = new char[len];

    memset(tmpbuf, 'a', len);
    ReadRequest request;
    std::vector<ReadRequest> requests;
    request.index = 0;
    request.chunkPos = offset;
    request.len = len;
    request.bufOffset = 0;
    requests.emplace_back(request);
    EXPECT_CALL(*mockChunkCacheManager_, ReadByWriteCache(_, _, _, _, _))
        .WillOnce(DoAll(SetArgPointee<4>(requests), Return()))
        .WillOnce(DoAll(SetArgPointee<4>(requests), Return()));
    EXPECT_CALL(*mockChunkCacheManager_, ReadByReadCache(_, _, _, _, _))
        .WillOnce(DoAll(SetArgPointee<4>(requests), Return()))
        .WillOnce(DoAll(SetArgPointee<4>(requests), Return()));
    EXPECT_CALL(*mockChunkCacheManager_, AddReadDataCache(_))
        .WillOnce(Return());
    fileCacheManager_->SetChunkCacheManagerForTest(0, mockChunkCacheManager_);
    Inode inode;
    inode.set_length(len);
    auto s3ChunkInfoMap = inode.mutable_s3chunkinfomap();
    S3ChunkInfoList *s3ChunkInfoList = new S3ChunkInfoList();
    S3ChunkInfo *s3ChunkInfo = s3ChunkInfoList->add_s3chunks();
    s3ChunkInfo->set_chunkid(25);
    s3ChunkInfo->set_compaction(0);
    s3ChunkInfo->set_offset(offset);
    s3ChunkInfo->set_len(len);
    s3ChunkInfo->set_size(len);
    s3ChunkInfo->set_zero(false);
    s3ChunkInfoMap->insert({0, *s3ChunkInfoList});
    auto inodeWrapper = std::make_shared<InodeWrapper>(inode, nullptr);
    EXPECT_CALL(*mockInodeManager_, GetInode(_, _))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)));
    EXPECT_CALL(*mockS3Client_, Download(_, _, _, _))
        .WillOnce(DoAll(SetArgPointee<1>(*tmpbuf), Return(len)))
        .WillOnce(Return(-1));

    ASSERT_EQ(len, fileCacheManager_->Read(inodeId, offset, len, buf));
    ASSERT_EQ(-1, fileCacheManager_->Read(inodeId, offset, len, buf));

    delete buf;
    delete tmpbuf;
}

}  // namespace client
}  // namespace curvefs

