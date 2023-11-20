/*
 *  Copyright (c) 2023 NetEase Inc.
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
 * Created Date: Wed Mar 23 2023
 * Author: wuhongsong
 */

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "curvefs/src/client/common/common.h"
#include "curvefs/src/client/s3/client_s3_adaptor.h"
#include "curvefs/src/client/s3/client_s3_cache_manager.h"
#include "curvefs/src/client/s3/disk_cache_manager_impl.h"
#include "curvefs/test/client/mock_client_s3.h"
#include "curvefs/test/client/mock_client_s3_cache_manager.h"
#include "curvefs/test/client/mock_disk_cache_base.h"
#include "curvefs/test/client/mock_disk_cache_manager.h"
#include "curvefs/test/client/mock_disk_cache_read.h"
#include "curvefs/test/client/mock_disk_cache_write.h"
#include "curvefs/test/client/mock_inode_manager.h"
#include "curvefs/test/client/mock_kvclient.h"
#include "curvefs/test/client/mock_test_posix_wapper.h"
#include "src/common/concurrent/task_thread_pool.h"

namespace curvefs {
namespace client {
namespace common {
DECLARE_bool(enableCto);
DECLARE_bool(supportKVcache);
}  // namespace common
}  // namespace client
}  // namespace curvefs

namespace curvefs {
namespace client {
using curve::common::TaskThreadPool;
using ::testing::_;
using ::testing::DoAll;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::SetArgPointee;
using ::testing::SetArgReferee;
using ::testing::WithArg;

class FileCacheManagerDiskTest : public testing::Test {
 protected:
    FileCacheManagerDiskTest() {}
    ~FileCacheManagerDiskTest() {}
    void SetUp() override {
        Aws::InitAPI(awsOptions_);
        uint64_t inodeId = 1;
        uint64_t fsId = 2;
        std::string fsName = "test";
        S3ClientAdaptorOption option;
        option.blockSize = 1 * 1024 * 1024;
        option.chunkSize = 4 * 1024 * 1024;
        option.baseSleepUs = 500;
        option.objectPrefix = 0;
        option.pageSize = 64 * 1024;
        option.intervalSec = 5000;
        option.flushIntervalSec = 5000;
        option.readCacheMaxByte = 104857600;
        option.writeCacheMaxByte = 10485760000;
        option.readCacheThreads = 5;
        option.diskCacheOpt.diskCacheType = (DiskCacheType)2;
        option.chunkFlushThreads = 5;
        option.s3ToLocal = true;
        s3ClientAdaptor_ = new S3ClientAdaptorImpl();
        fsCacheManager_ = std::make_shared<FsCacheManager>(
            s3ClientAdaptor_, option.readCacheMaxByte, option.writeCacheMaxByte,
            option.readCacheThreads, nullptr);
        mockInodeManager_ = std::make_shared<MockInodeCacheManager>();
        mockS3Client_ = std::make_shared<MockS3Client>();
        std::shared_ptr<S3Client> client = std::make_shared<MockS3Client>();
        std::shared_ptr<PosixWrapper> wrapper =
            std::make_shared<MockPosixWrapper>();
        std::shared_ptr<DiskCacheWrite> diskCacheWrite =
            std::make_shared<MockDiskCacheWrite>();
        std::shared_ptr<DiskCacheRead> diskCacheRead =
            std::make_shared<MockDiskCacheRead>();
        std::shared_ptr<DiskCacheManager> diskCacheManager =
            std::make_shared<MockDiskCacheManager>(wrapper, diskCacheWrite,
                                                   diskCacheRead);
        mockDiskcacheManagerImpl_ =
            std::make_shared<MockDiskCacheManagerImpl>();
        mockKVClient_ = std::make_shared<MockKVClient>();
        KVClientManagerOpt config;
        kvClientManager_ = std::make_shared<KVClientManager>();
        kvClientManager_->Init(config, mockKVClient_, fsName);
        s3ClientAdaptor_->Init(option, mockS3Client_, mockInodeManager_,
                               nullptr, fsCacheManager_,
                               mockDiskcacheManagerImpl_, kvClientManager_);
        s3ClientAdaptor_->SetFsId(fsId);

        threadPool_->Start(option.readCacheThreads);
        fileCacheManager_ = std::make_shared<FileCacheManager>(
            fsId, inodeId, s3ClientAdaptor_, kvClientManager_, threadPool_);
        mockChunkCacheManager_ = std::make_shared<MockChunkCacheManager>();
        curvefs::client::common::FLAGS_enableCto = false;
    }

    void TearDown() override {
        Aws::ShutdownAPI(awsOptions_);
        delete s3ClientAdaptor_;
        s3ClientAdaptor_ = nullptr;
    }

 protected:
    Aws::SDKOptions awsOptions_;
    S3ClientAdaptorImpl* s3ClientAdaptor_;
    std::shared_ptr<FileCacheManager> fileCacheManager_;
    std::shared_ptr<FsCacheManager> fsCacheManager_;
    std::shared_ptr<MockChunkCacheManager> mockChunkCacheManager_;
    std::shared_ptr<MockInodeCacheManager> mockInodeManager_;
    std::shared_ptr<MockS3Client> mockS3Client_;

    std::shared_ptr<KVClientManager> kvClientManager_;
    std::shared_ptr<MockKVClient> mockKVClient_;
    std::shared_ptr<MockDiskCacheManagerImpl> mockDiskcacheManagerImpl_;

    std::shared_ptr<TaskThreadPool<>> threadPool_ =
        std::make_shared<TaskThreadPool<>>();
};

TEST_F(FileCacheManagerDiskTest, test_read_local_prefetchfail) {
    const uint64_t inodeId = 1;
    const uint64_t offset = 0;
    const uint64_t len = 1024;

    std::vector<char> buf(len);
    std::vector<char> tmpBuf(len, 'a');
    ReadRequest req{.index = 0, .chunkPos = offset, .len = len, .bufOffset = 0};
    std::vector<ReadRequest> requests{req};
    EXPECT_CALL(*mockChunkCacheManager_, ReadByWriteCache(_, _, _, _, _))
        .WillOnce(DoAll(SetArgPointee<4>(requests), Return()));
    EXPECT_CALL(*mockChunkCacheManager_, ReadByReadCache(_, _, _, _, _))
        .WillOnce(DoAll(SetArgPointee<4>(requests), Return()));
    EXPECT_CALL(*mockChunkCacheManager_, AddReadDataCache(_))
        .WillOnce(Return());
    fileCacheManager_->SetChunkCacheManagerForTest(0, mockChunkCacheManager_);
    Inode inode;
    inode.set_length(len);
    auto* s3ChunkInfoMap = inode.mutable_s3chunkinfomap();
    auto* s3ChunkInfoList = new S3ChunkInfoList();
    auto* s3ChunkInfo = s3ChunkInfoList->add_s3chunks();
    s3ChunkInfo->set_chunkid(25);
    s3ChunkInfo->set_compaction(0);
    s3ChunkInfo->set_offset(offset);
    s3ChunkInfo->set_len(len);
    s3ChunkInfo->set_size(len);
    s3ChunkInfo->set_zero(false);
    s3ChunkInfoMap->insert({0, *s3ChunkInfoList});
    fsCacheManager_->SetFileCacheManagerForTest(inodeId, fileCacheManager_);
    auto inodeWrapper = std::make_shared<InodeWrapper>(inode, nullptr);
    EXPECT_CALL(*mockInodeManager_, GetInode(_, _))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)));
    EXPECT_CALL(*mockS3Client_, DownloadAsync(_))
        .WillRepeatedly(
            Invoke([&](const std::shared_ptr<GetObjectAsyncContext>& context) {
                context->retCode = 0;
                context->cb(nullptr, context);
            }));
    EXPECT_CALL(*mockDiskcacheManagerImpl_, IsCached(_))
        .WillOnce(Return(false))
        .WillOnce(Return(false))
        .WillOnce(Return(false))
        .WillOnce(Return(true));
    EXPECT_CALL(*mockKVClient_, Get(_, _, _, _, _, _, _))
        .WillOnce(Return(true));
    EXPECT_CALL(*mockDiskcacheManagerImpl_, Read(_, _, _, _))
        .WillOnce(Return(len));
    EXPECT_CALL(*mockDiskcacheManagerImpl_, WriteReadDirect(_, _, _))
        .WillOnce(Return(-1));
    ASSERT_EQ(len, fileCacheManager_->Read(inodeId, offset, len, buf.data()));
    sleep(3);
}

TEST_F(FileCacheManagerDiskTest, test_read_local_prefetchsuc) {
    const uint64_t inodeId = 1;
    const uint64_t offset = 0;
    const uint64_t len = 1024;

    std::vector<char> buf(len);
    std::vector<char> tmpBuf(len, 'a');

    ReadRequest req{.index = 0, .chunkPos = offset, .len = len, .bufOffset = 0};
    std::vector<ReadRequest> requests{req};
    EXPECT_CALL(*mockChunkCacheManager_, ReadByWriteCache(_, _, _, _, _))
        .WillOnce(DoAll(SetArgPointee<4>(requests), Return()));
    EXPECT_CALL(*mockChunkCacheManager_, ReadByReadCache(_, _, _, _, _))
        .WillOnce(DoAll(SetArgPointee<4>(requests), Return()));
    EXPECT_CALL(*mockChunkCacheManager_, AddReadDataCache(_))
        .WillOnce(Return());
    fileCacheManager_->SetChunkCacheManagerForTest(0, mockChunkCacheManager_);
    Inode inode;
    inode.set_length(len);
    auto* s3ChunkInfoMap = inode.mutable_s3chunkinfomap();
    auto* s3ChunkInfoList = new S3ChunkInfoList();
    auto* s3ChunkInfo = s3ChunkInfoList->add_s3chunks();
    s3ChunkInfo->set_chunkid(25);
    s3ChunkInfo->set_compaction(0);
    s3ChunkInfo->set_offset(offset);
    s3ChunkInfo->set_len(len);
    s3ChunkInfo->set_size(len);
    s3ChunkInfo->set_zero(false);
    s3ChunkInfoMap->insert({0, *s3ChunkInfoList});

    fsCacheManager_->SetFileCacheManagerForTest(inodeId, fileCacheManager_);
    auto inodeWrapper = std::make_shared<InodeWrapper>(inode, nullptr);
    EXPECT_CALL(*mockInodeManager_, GetInode(_, _))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)));
    EXPECT_CALL(*mockS3Client_, DownloadAsync(_))
        .WillRepeatedly(
            Invoke([&](const std::shared_ptr<GetObjectAsyncContext>& context) {
                context->retCode = 0;
                context->cb(nullptr, context);
            }));
    EXPECT_CALL(*mockDiskcacheManagerImpl_, IsCached(_))
        .WillOnce(Return(false))
        .WillOnce(Return(false))
        .WillOnce(Return(false))
        .WillOnce(Return(true));
    EXPECT_CALL(*mockDiskcacheManagerImpl_, Read(_, _, _, _))
        .WillOnce(Return(len));
    EXPECT_CALL(*mockDiskcacheManagerImpl_, WriteReadDirect(_, _, _))
        .WillOnce(Return(0));
    EXPECT_CALL(*mockKVClient_, Get(_, _, _, _, _, _, _))
        .WillOnce(Return(true));
    ASSERT_EQ(len, fileCacheManager_->Read(inodeId, offset, len, buf.data()));
    sleep(3);
}

TEST_F(FileCacheManagerDiskTest, test_read_remote) {
    const uint64_t inodeId = 1;
    const uint64_t offset = 0;
    const uint64_t len = 1024;

    std::vector<char> buf(len);
    std::vector<char> tmpBuf(len, 'a');

    ReadRequest req{.index = 0, .chunkPos = offset, .len = len, .bufOffset = 0};
    std::vector<ReadRequest> requests{req};
    EXPECT_CALL(*mockChunkCacheManager_, ReadByWriteCache(_, _, _, _, _))
        .WillOnce(DoAll(SetArgPointee<4>(requests), Return()));
    EXPECT_CALL(*mockChunkCacheManager_, ReadByReadCache(_, _, _, _, _))
        .WillOnce(DoAll(SetArgPointee<4>(requests), Return()));
    EXPECT_CALL(*mockChunkCacheManager_, AddReadDataCache(_))
        .WillOnce(Return());
    fileCacheManager_->SetChunkCacheManagerForTest(0, mockChunkCacheManager_);
    Inode inode;
    inode.set_length(len);
    auto* s3ChunkInfoMap = inode.mutable_s3chunkinfomap();
    auto* s3ChunkInfoList = new S3ChunkInfoList();
    auto* s3ChunkInfo = s3ChunkInfoList->add_s3chunks();
    s3ChunkInfo->set_chunkid(25);
    s3ChunkInfo->set_compaction(0);
    s3ChunkInfo->set_offset(offset);
    s3ChunkInfo->set_len(len);
    s3ChunkInfo->set_size(len);
    s3ChunkInfo->set_zero(false);
    s3ChunkInfoMap->insert({0, *s3ChunkInfoList});

    fsCacheManager_->SetFileCacheManagerForTest(inodeId, fileCacheManager_);
    auto inodeWrapper = std::make_shared<InodeWrapper>(inode, nullptr);
    EXPECT_CALL(*mockInodeManager_, GetInode(_, _))
        .WillOnce(
            DoAll(SetArgReferee<1>(inodeWrapper), Return(CURVEFS_ERROR::OK)));
    EXPECT_CALL(*mockS3Client_, DownloadAsync(_))
        .WillRepeatedly(
            Invoke([&](const std::shared_ptr<GetObjectAsyncContext>& context) {
                context->retCode = 0;
                context->cb(nullptr, context);
            }));
    EXPECT_CALL(*mockDiskcacheManagerImpl_, IsCached(_))
        .WillOnce(Return(false))
        .WillOnce(Return(false))
        .WillOnce(Return(false))
        .WillOnce(Return(false));

    EXPECT_CALL(*mockDiskcacheManagerImpl_, WriteReadDirect(_, _, _))
        .WillOnce(Return(0));

    EXPECT_CALL(*mockKVClient_, Get(_, _, _, _, _, _, _))
        .WillOnce(Return(true))
        .WillOnce(Return(true));

    ASSERT_EQ(len, fileCacheManager_->Read(inodeId, offset, len, buf.data()));
    sleep(3);
}

}  // namespace client
}  // namespace curvefs
