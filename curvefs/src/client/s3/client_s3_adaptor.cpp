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
 * Created Date: 21-5-31
 * Author: huyao
 */

#include "curvefs/src/client/s3/client_s3_adaptor.h"

#include <brpc/channel.h>
#include <brpc/controller.h>

#include <algorithm>

namespace curvefs {

namespace client {

CURVEFS_ERROR S3ClientAdaptorImpl::Init(
    const S3ClientAdaptorOption& option, S3Client* client,
    std::shared_ptr<InodeCacheManager> inodeManager,
    std::shared_ptr<MdsClient> mdsClient) {
    blockSize_ = option.blockSize;
    chunkSize_ = option.chunkSize;
    if (chunkSize_ % blockSize_ != 0) {
        LOG(ERROR) << "chunkSize:" << chunkSize_
                   << " is not integral multiple for the blockSize:"
                   << blockSize_;
        return CURVEFS_ERROR::INVALIDPARAM;
    }
    enableDiskCache_ = option.diskCacheOpt.enableDiskCache;
    memCacheNearfullRatio_ = option.nearfullRatio;
    throttleBaseSleepUs_ = option.baseSleepUs;
    flushIntervalSec_ = option.flushIntervalSec;
    client_ = client;
    inodeManager_ = inodeManager;
    mdsClient_ = mdsClient;
    fsCacheManager_ = std::make_shared<FsCacheManager>(
        this, option.readCacheMaxByte, option.writeCacheMaxByte);
    waitIntervalSec_.Init(option.intervalSec * 1000);
    LOG(INFO) << "Init(): block size:" << blockSize_
              << ",chunk size:" << chunkSize_
              << ",intervalSec:" << option.intervalSec
              << ",flushIntervalSec:" << option.flushIntervalSec
              << ",writeCacheMaxByte:" << option.writeCacheMaxByte
              << ",readCacheMaxByte:" << option.readCacheMaxByte
              << ", nearfullRatio: " << option.nearfullRatio
              << ", baseSleepUs: " << option.baseSleepUs;
    toStop_.store(false, std::memory_order_release);
    bgFlushThread_ = Thread(&S3ClientAdaptorImpl::BackGroundFlush, this);

    if (enableDiskCache_) {
        std::shared_ptr<PosixWrapper> wrapper =
            std::make_shared<PosixWrapper>();
        std::shared_ptr<DiskCacheRead> diskCacheRead =
            std::make_shared<DiskCacheRead>();
        std::shared_ptr<DiskCacheWrite> diskCacheWrite =
            std::make_shared<DiskCacheWrite>();
        std::shared_ptr<DiskCacheManager> diskCacheManager =
            std::make_shared<DiskCacheManager>(wrapper, diskCacheWrite,
                                               diskCacheRead);
        diskCacheManagerImpl_ =
            std::make_shared<DiskCacheManagerImpl>(diskCacheManager, client);
        diskCacheManagerImpl_->Init(option);
    }
}

int S3ClientAdaptorImpl::Write(uint64_t inodeId, uint64_t offset,
                               uint64_t length, const char* buf) {
    LOG(INFO) << "write start offset:" << offset << ", len:" << length
              << ", fsId:" << fsId_ << ", inodeId:" << inodeId;

    FileCacheManagerPtr fileCacheManager =
        fsCacheManager_->FindOrCreateFileCacheManager(fsId_, inodeId);
    if (fsCacheManager_->WriteCacheIsFull()) {
        LOG(INFO) << "write cache is full, wait flush";
        fsCacheManager_->WaitFlush();
    }
    uint64_t memCacheRatio = fsCacheManager_->MemCacheRatio();
    int64_t exceedRatio = memCacheRatio - memCacheNearfullRatio_;
    if (exceedRatio > 0) {
        // upload to s3 derectly or cache disk full
        if (!enableDiskCache_ || (enableDiskCache_ &&
             diskCacheManagerImpl_->IsDiskCacheFull())) {
            uint32_t exponent = pow(2, (exceedRatio)/10);
            bthread_usleep(throttleBaseSleepUs_*exceedRatio*exponent);
            LOG(INFO) << "write cache is nearfull and use ratio is: "
                      << memCacheRatio
                      << ", exponent is: " << exponent;
        }
    }

    int ret = fileCacheManager->Write(offset, length, buf);
    LOG(INFO) << "write end inodeId:" << inodeId << ",ret:" << ret;
    return ret;
}

int S3ClientAdaptorImpl::Read(Inode* inode, uint64_t offset, uint64_t length,
                              char* buf) {
    uint64_t fsId = inode->fsid();
    uint64_t inodeId = inode->inodeid();

    assert(offset + length <= inode->length());
    LOG(INFO) << "read start offset:" << offset << ", len:" << length
              << ",inode length:" << inode->length() << ", fsId:" << fsId
              << ", inodeId" << inodeId;
    FileCacheManagerPtr fileCacheManager =
        fsCacheManager_->FindOrCreateFileCacheManager(fsId, inodeId);

    return fileCacheManager->Read(inode, offset, length, buf);
}

CURVEFS_ERROR S3ClientAdaptorImpl::Truncate(Inode* inode, uint64_t size) {
    uint64_t fileSize = inode->length();

    if (size <= fileSize) {
        LOG(INFO) << "Truncate size:" << size << " less or equal fileSize"
                  << fileSize;
        return CURVEFS_ERROR::OK;
    }

    LOG(INFO) << "Truncate size:" << size << " more than fileSize" << fileSize;

    uint64_t offset = fileSize;
    uint64_t len = size - fileSize;
    uint64_t index = offset / chunkSize_;
    uint64_t chunkPos = offset % chunkSize_;
    uint64_t n = 0;
    uint64_t chunkId;
    FSStatusCode ret;
    uint64_t fsId = inode->fsid();
    while (len > 0) {
        if (chunkPos + len > chunkSize_) {
            n = chunkSize_ - chunkPos;
        } else {
            n = len;
        }
        ret = AllocS3ChunkId(fsId, &chunkId);
        if (ret != FSStatusCode::OK) {
            LOG(ERROR) << "Truncate alloc s3 chunkid fail. ret:" << ret;
            return CURVEFS_ERROR::INTERNAL;
        }
        S3ChunkInfo* tmp;
        auto s3ChunkInfoMap = inode->mutable_s3chunkinfomap();
        auto s3chunkInfoListIter = s3ChunkInfoMap->find(index);
        if (s3chunkInfoListIter == s3ChunkInfoMap->end()) {
            S3ChunkInfoList s3chunkInfoList;
            tmp = s3chunkInfoList.add_s3chunks();
            tmp->set_chunkid(chunkId);
            tmp->set_offset(offset);
            tmp->set_len(n);
            tmp->set_size(n);
            tmp->set_zero(true);
            s3ChunkInfoMap->insert({index, s3chunkInfoList});
        } else {
            S3ChunkInfoList& s3chunkInfoList = s3chunkInfoListIter->second;
            tmp = s3chunkInfoList.add_s3chunks();
            tmp->set_chunkid(chunkId);
            tmp->set_offset(offset);
            tmp->set_len(n);
            tmp->set_size(n);
            tmp->set_zero(true);
        }
        len -= n;
        index++;
        chunkPos = (chunkPos + n) % chunkSize_;
        offset += n;
    }
    return CURVEFS_ERROR::OK;
}

void S3ClientAdaptorImpl::ReleaseCache(uint64_t inodeId) {
    FileCacheManagerPtr fileCacheManager =
        fsCacheManager_->FindFileCacheManager(inodeId);
    if (!fileCacheManager) {
        return;
    }
    fileCacheManager->ReleaseCache();
    fsCacheManager_->ReleaseFileCacheManager(inodeId);
    return;
}

CURVEFS_ERROR S3ClientAdaptorImpl::Flush(uint64_t inodeId) {
    FileCacheManagerPtr fileCacheManager =
        fsCacheManager_->FindFileCacheManager(inodeId);
    if (!fileCacheManager) {
        return CURVEFS_ERROR::OK;
    }
    LOG(INFO) << "Flush inodeId:" << inodeId;
    return fileCacheManager->Flush(true);
}

CURVEFS_ERROR S3ClientAdaptorImpl::FsSync() {
    return fsCacheManager_->FsSync(true);
}

FSStatusCode S3ClientAdaptorImpl::AllocS3ChunkId(uint32_t fsId,
                                                 uint64_t* chunkId) {
    return mdsClient_->AllocS3ChunkId(fsId, chunkId);
}

void S3ClientAdaptorImpl::BackGroundFlush() {
    while (!toStop_.load(std::memory_order_acquire)) {
        {
            std::unique_lock<std::mutex> lck(mtx_);
            if (fsCacheManager_->GetDataCacheNum() == 0) {
                LOG(INFO) << "BackGroundFlush has no write cache, so wait";
                cond_.wait(lck);
            }
        }
        if (fsCacheManager_->MemCacheRatio() > memCacheNearfullRatio_) {
            LOG(INFO) << "BackGroundFlush radically, write cache num is: "
                  << fsCacheManager_->GetDataCacheNum()
                  << "cache ratio is: "
                  << fsCacheManager_->MemCacheRatio();
            fsCacheManager_->FsSync(true);

        } else {
            waitIntervalSec_.WaitForNextExcution();
            LOG(INFO) << "BackGroundFlush, write cache num is:"
                      << fsCacheManager_->GetDataCacheNum()
                      << "cache ratio is: "
                      << fsCacheManager_->MemCacheRatio();
            fsCacheManager_->FsSync(false);
            LOG(INFO) << "background fssync end";
        }
    }
    return;
}

int S3ClientAdaptorImpl::Stop() {
    LOG(INFO) << "start Stopping S3ClientAdaptor.";
    if (enableDiskCache_) {
       diskCacheManagerImpl_->UmountDiskCache();
    }
    waitIntervalSec_.StopWait();
    toStop_.store(true, std::memory_order_release);
    FsSyncSignal();
    bgFlushThread_.join();
    LOG(INFO) << "Stopping S3ClientAdaptor success";
    return 0;
}

}  // namespace client
}  // namespace curvefs
