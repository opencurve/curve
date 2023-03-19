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

#include "curvefs/src/client/s3/client_s3_adaptor.h"

#include <brpc/server.h>
#include <google/protobuf/util/message_differencer.h>
#include <gtest/gtest.h>

#include "curvefs/src/client/inode_wrapper.h"
#include "curvefs/test/client/mock_client_s3.h"
#include "curvefs/test/client/mock_client_s3_cache_manager.h"
#include "curvefs/test/client/mock_disk_cache_manager.h"
#include "curvefs/test/client/mock_inode_cache_manager.h"
#include "curvefs/test/client/mock_metaserver_service.h"
#include "curvefs/test/client/rpcclient/mock_mds_client.h"
#include "src/common/curve_define.h"

namespace curvefs {
namespace client {
using ::testing::_;
using ::testing::DoAll;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::SetArgPointee;
using ::testing::SetArgReferee;
using ::testing::WithArg;

using rpcclient::MockMdsClient;

// extern KVClientManager *g_kvClientManager;

class ClientS3AdaptorTest : public testing::Test {
 protected:
    ClientS3AdaptorTest() {}
    ~ClientS3AdaptorTest() {}
    void SetUp() override {
        s3ClientAdaptor_ = std::make_shared<S3ClientAdaptorImpl>();
        mockInodeManager_ = std::make_shared<MockInodeCacheManager>();
        mockDiskcacheManagerImpl_ =
            std::make_shared<MockDiskCacheManagerImpl>();
        mockFsCacheManager_ = std::make_shared<MockFsCacheManager>();
        mockS3Client_ = std::make_shared<MockS3Client>();
        mockMdsClient_ = std::make_shared<MockMdsClient>();
        S3ClientAdaptorOption option;
        option.blockSize = 1 * 1024 * 1024;
        option.chunkSize = 4 * 1024 * 1024;
        option.pageSize = 64 * 1024;
        option.intervalSec = 5000;
        option.flushIntervalSec = 5000;
        option.readCacheMaxByte = 104857600;
        option.writeCacheMaxByte = 10485760000;
        option.fuseMaxSize = 131072;
        option.chunkFlushThreads = 5;
        option.diskCacheOpt.diskCacheType = (DiskCacheType)0;
        kvClientManager_ = nullptr;
        s3ClientAdaptor_->Init(option, mockS3Client_, mockInodeManager_,
                               mockMdsClient_, mockFsCacheManager_,
                               mockDiskcacheManagerImpl_, kvClientManager_);
    }

    void TearDown() override {
    }

 protected:
    std::shared_ptr<S3ClientAdaptorImpl> s3ClientAdaptor_;
    std::shared_ptr<MockInodeCacheManager> mockInodeManager_;
    std::shared_ptr<MockDiskCacheManagerImpl> mockDiskcacheManagerImpl_;
    std::shared_ptr<MockFsCacheManager> mockFsCacheManager_;
    std::shared_ptr<MockS3Client> mockS3Client_;
    std::shared_ptr<MockMdsClient> mockMdsClient_;
    std::shared_ptr<KVClientManager> kvClientManager_;
};

uint64_t gInodeId = 1;
std::unique_ptr<InodeWrapper> InitInode() {
    Inode inode;

    inode.set_inodeid(gInodeId);
    inode.set_fsid(2);
    inode.set_length(0);
    inode.set_ctime(1623835517);
    inode.set_mtime(1623835517);
    inode.set_atime(1623835517);
    inode.set_uid(1);
    inode.set_gid(1);
    inode.set_mode(1);
    inode.set_nlink(1);
    inode.set_type(curvefs::metaserver::FsFileType::TYPE_S3);
    gInodeId++;

    return absl::make_unique<InodeWrapper>(std::move(inode), nullptr);
}

TEST_F(ClientS3AdaptorTest, test_init) {
    ASSERT_EQ(1024 * 1024, s3ClientAdaptor_->GetBlockSize());
    ASSERT_EQ(4 * 1024 * 1024, s3ClientAdaptor_->GetChunkSize());
    ASSERT_EQ(64 * 1024, s3ClientAdaptor_->GetPageSize());
    ASSERT_EQ(true, s3ClientAdaptor_->DisableDiskCache());
}

TEST_F(ClientS3AdaptorTest, write_success) {
    uint64_t inodeId = 1;
    uint64_t offset = 0;
    uint64_t length = 1024;
    char buf[length] = {0};
    memset(buf, 'a', length);
    auto fileCache = std::make_shared<MockFileCacheManager>();
    EXPECT_CALL(*mockFsCacheManager_, FindOrCreateFileCacheManager(_, _))
        .WillOnce(Return(fileCache));
    EXPECT_CALL(*mockFsCacheManager_, GetDataCacheSize())
        .WillOnce(Return(length));
    EXPECT_CALL(*mockFsCacheManager_, GetDataCacheMaxSize())
        .WillOnce(Return(10485760000));
    EXPECT_CALL(*mockFsCacheManager_, MemCacheRatio()).WillOnce(Return(10));
    EXPECT_CALL(*fileCache, Write(_, _, _)).WillOnce(Return(length));
    ASSERT_EQ(length, s3ClientAdaptor_->Write(inodeId, offset, length, buf));
}

TEST_F(ClientS3AdaptorTest, read_success) {
    uint64_t inodeId = 1;
    uint64_t offset = 0;
    uint64_t length = 1024;
    char buf[length] = {0};
    memset(buf, 'a', length);
    auto fileCache = std::make_shared<MockFileCacheManager>();
    EXPECT_CALL(*mockFsCacheManager_, FindOrCreateFileCacheManager(_, _))
        .WillOnce(Return(fileCache));
    EXPECT_CALL(*fileCache, Read(_, _, _, _)).WillOnce(Return(length));
    ASSERT_EQ(length, s3ClientAdaptor_->Read(inodeId, offset, length, buf));
}

TEST_F(ClientS3AdaptorTest, read_fail) {
    uint64_t inodeId = 1;
    uint64_t offset = 0;
    uint64_t length = 1024;
    char buf[length] = {0};
    memset(buf, 'a', length);
    auto fileCache = std::make_shared<MockFileCacheManager>();
    EXPECT_CALL(*mockFsCacheManager_, FindOrCreateFileCacheManager(_, _))
        .WillOnce(Return(fileCache));
    EXPECT_CALL(*fileCache, Read(_, _, _, _)).WillOnce(Return(-1));
    ASSERT_EQ(-1, s3ClientAdaptor_->Read(inodeId, offset, length, buf));
}

TEST_F(ClientS3AdaptorTest, truncate_small) {
    auto inode = InitInode();
    inode->SetLength(1000);

    auto fileCache = std::make_shared<MockFileCacheManager>();
    EXPECT_CALL(*mockFsCacheManager_, FindOrCreateFileCacheManager(_, _))
        .WillOnce(Return(fileCache));
    EXPECT_CALL(*fileCache, TruncateCache(_, _)).WillOnce(Return());
    ASSERT_EQ(CURVEFS_ERROR::OK, s3ClientAdaptor_->Truncate(inode.get(), 100));
}

TEST_F(ClientS3AdaptorTest, truncate_unchange) {
    auto inode = InitInode();
    inode->SetLength(1000);

    ASSERT_EQ(CURVEFS_ERROR::OK, s3ClientAdaptor_->Truncate(inode.get(), 1000));
}

TEST_F(ClientS3AdaptorTest, truncate_big_alloc_chunkId_fail) {
    auto inode = InitInode();

    EXPECT_CALL(*mockMdsClient_, AllocS3ChunkId(_, _, _))
        .WillOnce(Return(FSStatusCode::UNKNOWN_ERROR));
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL,
              s3ClientAdaptor_->Truncate(inode.get(), 1000));
}

TEST_F(ClientS3AdaptorTest, truncate_big_success) {
    auto inode = InitInode();

    uint64_t chunkId = 999;
    EXPECT_CALL(*mockMdsClient_, AllocS3ChunkId(_, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(chunkId), Return(FSStatusCode::OK)));
    ASSERT_EQ(CURVEFS_ERROR::OK, s3ClientAdaptor_->Truncate(inode.get(), 1000));
}

TEST_F(ClientS3AdaptorTest, truncate_big_more_chunkId) {
    auto inode = InitInode();

    uint64_t chunkId = 999;
    EXPECT_CALL(*mockMdsClient_, AllocS3ChunkId(_, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(chunkId), Return(FSStatusCode::OK)));
    ASSERT_EQ(CURVEFS_ERROR::OK,
              s3ClientAdaptor_->Truncate(inode.get(), 8 * 1024 * 1024));
    auto s3ChunkInfoMap = inode->GetChunkInfoMap();
    auto s3chunkInfoListIter = s3ChunkInfoMap->find(0);
    auto s3ChunkInfo = s3chunkInfoListIter->second.s3chunks(0);
    ASSERT_EQ(999, s3ChunkInfo.chunkid());
    auto s3chunkInfoListIter1 = s3ChunkInfoMap->find(1);
    auto s3ChunkInfo1 = s3chunkInfoListIter1->second.s3chunks(0);
    ASSERT_EQ(1000, s3ChunkInfo1.chunkid());
}

TEST_F(ClientS3AdaptorTest, flush_no_file_cache) {
    uint64_t inodeId = 1;

    EXPECT_CALL(*mockFsCacheManager_, FindFileCacheManager(_))
        .WillOnce(Return(nullptr));
    ASSERT_EQ(CURVEFS_ERROR::OK, s3ClientAdaptor_->Flush(inodeId));
}

TEST_F(ClientS3AdaptorTest, flush_fail) {
    uint64_t inodeId = 1;

    auto fileCache = std::make_shared<MockFileCacheManager>();
    EXPECT_CALL(*mockFsCacheManager_, FindFileCacheManager(_))
        .WillOnce(Return(fileCache));
    EXPECT_CALL(*fileCache, Flush(_, _))
        .WillOnce(Return(CURVEFS_ERROR::INTERNAL));
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, s3ClientAdaptor_->Flush(inodeId));
}

TEST_F(ClientS3AdaptorTest, FlushAllCache_no_filecachaeManager) {
    EXPECT_CALL(*mockFsCacheManager_, FindFileCacheManager(_))
        .WillOnce(Return(nullptr));
    ASSERT_EQ(CURVEFS_ERROR::OK, s3ClientAdaptor_->FlushAllCache(1));
}

TEST_F(ClientS3AdaptorTest, FlushAllCache_flush_fail) {
    auto filecache = std::make_shared<MockFileCacheManager>();
    EXPECT_CALL(*mockFsCacheManager_, FindFileCacheManager(_))
        .WillOnce(Return(filecache));
    EXPECT_CALL(*filecache, Flush(_, _))
        .WillOnce(Return(CURVEFS_ERROR::INTERNAL));
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, s3ClientAdaptor_->FlushAllCache(1));
}

TEST_F(ClientS3AdaptorTest, FlushAllCache_with_no_cache) {
    s3ClientAdaptor_->SetDiskCache(DiskCacheType::Disable);

    LOG(INFO) << "############ case1: do not find file cache";
    auto filecache = std::make_shared<MockFileCacheManager>();
    EXPECT_CALL(*mockFsCacheManager_, FindFileCacheManager(_))
        .WillOnce(Return(nullptr));
    ASSERT_EQ(CURVEFS_ERROR::OK, s3ClientAdaptor_->FlushAllCache(1));

    LOG(INFO) << "############ case2: find file cache";
    EXPECT_CALL(*mockFsCacheManager_, FindFileCacheManager(_))
        .WillOnce(Return(filecache));
    EXPECT_CALL(*filecache, Flush(_, _)).WillOnce(Return(CURVEFS_ERROR::OK));
    ASSERT_EQ(CURVEFS_ERROR::OK, s3ClientAdaptor_->FlushAllCache(1));
}

TEST_F(ClientS3AdaptorTest, FlushAllCache_with_cache) {
     s3ClientAdaptor_->SetDiskCache(DiskCacheType::ReadWrite);

    LOG(INFO) << "############ case1: clear write cache fail";
    auto filecache = std::make_shared<MockFileCacheManager>();
    EXPECT_CALL(*mockFsCacheManager_, FindFileCacheManager(_))
        .WillOnce(Return(filecache));
    EXPECT_CALL(*filecache, Flush(_, _)).WillOnce(Return(CURVEFS_ERROR::OK));
    EXPECT_CALL(*mockDiskcacheManagerImpl_, UploadWriteCacheByInode(_))
        .WillOnce(Return(-1));
    ASSERT_EQ(CURVEFS_ERROR::INTERNAL, s3ClientAdaptor_->FlushAllCache(1));

    LOG(INFO)
        << "############ case2: clear write cache ok, update write cache ok ";
    EXPECT_CALL(*mockFsCacheManager_, FindFileCacheManager(_))
        .WillOnce(Return(filecache));
    EXPECT_CALL(*filecache, Flush(_, _)).WillOnce(Return(CURVEFS_ERROR::OK));
    EXPECT_CALL(*mockDiskcacheManagerImpl_, UploadWriteCacheByInode(_))
        .WillOnce(Return(0));
    ASSERT_EQ(CURVEFS_ERROR::OK, s3ClientAdaptor_->FlushAllCache(1));
}

}  // namespace client
}  // namespace curvefs

