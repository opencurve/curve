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
 * Created Date: Thur March 14 2023
 * Author: wuhongsong
 */

#include "curvefs/src/client/client_storage_adaptor.h"

#include <brpc/channel.h>
#include <brpc/controller.h>
#include <algorithm>
#include <list>

#include "absl/memory/memory.h"


#include "curvefs/src/common/s3util.h"

namespace curvefs {
namespace client {

CURVEFS_ERROR
StorageAdaptor::Init(const FuseClientOption &fuseOption,
    std::shared_ptr<InodeCacheManager> inodeManager,
    std::shared_ptr<MdsClient> mdsClient,
    std::shared_ptr<FsCacheManager> fsCacheManager,
    std::shared_ptr<DiskCacheManagerImpl> diskCacheManagerImpl,
    std::shared_ptr<KVClientManager> kvClientManager,
    std::shared_ptr<FsInfo> fsInfo) {
        const S3ClientAdaptorOption option =
          fuseOption.s3Opt.s3ClientAdaptorOpt;
        LOG(INFO) << "StorageAdaptor Init. block size:" << blockSize_
            << ", chunk size: " << chunkSize_
            << ", intervalSec: " << option.intervalSec
            << ", flushIntervalSec: " << option.flushIntervalSec
            << ", writeCacheMaxByte: " << option.writeCacheMaxByte
            << ", readCacheMaxByte: " << option.readCacheMaxByte
            << ", nearfullRatio: " << option.nearfullRatio
            << ", baseSleepUs: " << option.baseSleepUs;
        pageSize_ = option.pageSize;
        if (chunkSize_ % blockSize_ != 0) {
            LOG(ERROR) << "chunkSize:" << chunkSize_
                    << " is not integral multiple for the blockSize:"
                    << blockSize_;
            return CURVEFS_ERROR::INVALIDPARAM;
        }
        diskCacheType_ = option.diskCacheOpt.diskCacheType;
        memCacheNearfullRatio_ = option.nearfullRatio;
        throttleBaseSleepUs_ = option.baseSleepUs;
        flushIntervalSec_ = option.flushIntervalSec;
        chunkFlushThreads_ = option.chunkFlushThreads;
        inodeManager_ = inodeManager;
        mdsClient_ = mdsClient;
        fsCacheManager_ = fsCacheManager;
        waitInterval_.Init(option.intervalSec * 1000);
        fsInfo_ = fsInfo;

        if (nullptr != diskCacheManagerImpl) {
            diskCacheManagerImpl_ = diskCacheManagerImpl;
            if (diskCacheManagerImpl_->Init(option) < 0) {
                LOG(ERROR) << "Init disk cache failed";
                return CURVEFS_ERROR::INTERNAL;
            }
        }

        if (enableBgFlush_) {        // start background flush thread
            toStop_.store(false, std::memory_order_release);
            bgFlushThread_ = Thread(&StorageAdaptor::BackGroundFlush, this);
        }

        // start chunk flush threads
        taskPool_.Start(chunkFlushThreads_);
        LOG(INFO) << "storage Adaptor init success.";
        return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR StorageAdaptor::FuseOpInit(void *userdata,
  struct fuse_conn_info *conn) {
    LOG(INFO) << " storage adaptor fuse init start.";
    SetFsId(fsInfo_->fsid());
    InitMetrics(fsInfo_->fsname());
    return CURVEFS_ERROR::OK;
}

int StorageAdaptor::Stop() {
    LOG(INFO) << " stop storage adaptor start.";
    waitInterval_.StopWait();
    toStop_.store(true, std::memory_order_release);
    FsSyncSignal();
    if (bgFlushThread_.joinable()) {
        bgFlushThread_.join();
    }
    if (HasDiskCache()) {
        diskCacheManagerImpl_->UmountDiskCache();
    }
    taskPool_.Stop();
    LOG(INFO) << " stop storage adaptor success.";
    return 0;
}

int StorageAdaptor::Write(uint64_t inodeId, uint64_t offset,
  uint64_t length, const char *buf) {
    VLOG(6) << "write start offset:" << offset << ", len:" << length
            << ", fsId:" << fsId_ << ", inodeId:" << inodeId;
    uint64_t start = butil::cpuwide_time_us();
    FileCacheManagerPtr fileCacheManager =
        fsCacheManager_->FindOrCreateFileCacheManager(fsId_, inodeId);
    {
        std::lock_guard<std::mutex> lockguard(ioMtx_);
        fsCacheManager_->DataCacheByteInc(length);
        uint64_t size = fsCacheManager_->GetDataCacheSize();
        uint64_t maxSize = fsCacheManager_->GetDataCacheMaxSize();
        if (size >= maxSize) {
            VLOG(6) << "write cache is full, wait flush. size: " << size
                    << ", maxSize:" << maxSize;
            // offer to do flush
            waitInterval_.StopWait();
            fsCacheManager_->WaitFlush();
        }
    }
    uint64_t memCacheRatio = fsCacheManager_->MemCacheRatio();
    int64_t exceedRatio = memCacheRatio - memCacheNearfullRatio_;
    if (exceedRatio > 0) {
        // offer to do flush
        waitInterval_.StopWait();
        // upload to s3 derectly or cache disk full
        bool needSleep =
            (DisableDiskCache() || IsReadCache()) ||
            (IsReadWriteCache() && diskCacheManagerImpl_->IsDiskCacheFull());
        if (needSleep) {
            uint32_t exponent = pow(2, (exceedRatio) / 10);
            bthread_usleep(throttleBaseSleepUs_ * exceedRatio * exponent);
            VLOG(6) << "write cache nearfull and use ratio is: "
                    << memCacheRatio << ", exponent is: " << exponent;
        }
    }
    int ret = fileCacheManager->Write(offset, length, buf);
    fsCacheManager_->DataCacheByteDec(length);
    if (ioMetric_.get() != nullptr) {
        CollectMetrics(&ioMetric_->adaptorWrite, ret, start);
        ioMetric_->writeSize.set_value(length);
    }
    VLOG(6) << "write end inodeId:" << inodeId << ",ret:" << ret;
    return ret;
}

int StorageAdaptor::Read(uint64_t inodeId, uint64_t offset,
  uint64_t length, char *buf) {
    VLOG(6) << "read start offset:" << offset << ", len:" << length
            << ", fsId:" << fsId_ << ", inodeId:" << inodeId;
    uint64_t start = butil::cpuwide_time_us();
    FileCacheManagerPtr fileCacheManager =
        fsCacheManager_->FindOrCreateFileCacheManager(fsId_, inodeId);

    int ret = fileCacheManager->Read(inodeId, offset, length, buf);
    VLOG(6) << "read end inodeId:" << inodeId << ",ret:" << ret;
    if (ret < 0) {
        return ret;
    }
    if (ioMetric_.get() != nullptr) {
        CollectMetrics(&ioMetric_->adaptorRead, ret, start);
        ioMetric_->readSize.set_value(length);
    }
    VLOG(6) << "read end offset:" << offset << ", len:" << length
            << ", fsId:" << fsId_ << ", inodeId:" << inodeId;
    return ret;
}

void StorageAdaptor::BackGroundFlush() {
    while (!toStop_.load(std::memory_order_acquire)) {
        {
            std::unique_lock<std::mutex> lck(mtx_);
            if (fsCacheManager_->GetDataCacheNum() == 0) {
                VLOG(3) << "BackGroundFlush has no write cache, so wait";
                cond_.wait(lck);
            }
        }
        uint64_t start = butil::cpuwide_time_us();
        if (fsCacheManager_->MemCacheRatio() > memCacheNearfullRatio_) {
            VLOG(3) << "BackGroundFlush radically, write cache num is: "
                    << fsCacheManager_->GetDataCacheNum()
                    << "cache ratio is: " << fsCacheManager_->MemCacheRatio();
            fsCacheManager_->FsSync(true);

        } else {
            waitInterval_.WaitForNextExcution();
            VLOG(6) << "BackGroundFlush, write cache num is:"
                    << fsCacheManager_->GetDataCacheNum()
                    << "cache ratio is: " << fsCacheManager_->MemCacheRatio();
            fsCacheManager_->FsSync(false);
            VLOG(6) << "background fssync end";
        }
        if (ioMetric_.get() != nullptr) {
            CollectMetrics(&ioMetric_->adaptorBgFlush, 1, start);
        }
    }
    return;
}


CURVEFS_ERROR StorageAdaptor::Flush(uint64_t inodeId) {
    FileCacheManagerPtr fileCacheManager =
        fsCacheManager_->FindFileCacheManager(inodeId);
    if (nullptr == fileCacheManager) {
        return CURVEFS_ERROR::OK;
    }
    VLOG(6) << "Flush data of inodeId:" << inodeId;
    return fileCacheManager->Flush(true, false);
}

CURVEFS_ERROR StorageAdaptor::FlushAllCache(uint64_t inodeId) {
    VLOG(6) << "FlushAllCache, inodeId:" << inodeId;
    FileCacheManagerPtr fileCacheManager =
        fsCacheManager_->FindFileCacheManager(inodeId);
    if (!fileCacheManager) {
        return CURVEFS_ERROR::OK;
    }

    // force flush data in memory to s3
    VLOG(6) << "FlushAllCache, flush memory data of inodeId:" << inodeId;
    CURVEFS_ERROR ret = fileCacheManager->Flush(true, false);
    if (ret != CURVEFS_ERROR::OK) {
        return ret;
    }

    // force flush data in diskcache to s3
    if (HasDiskCache()) {
        VLOG(6) << "FlushAllCache, wait inodeId:" << inodeId
                << "related chunk upload to s3";
        if (ClearDiskCache(inodeId) < 0) {
            return CURVEFS_ERROR::INTERNAL;
        }
    }

    return ret;
}

CURVEFS_ERROR StorageAdaptor::FsSync() {
    return fsCacheManager_->FsSync(true);
}

int StorageAdaptor::ClearDiskCache(int64_t inodeId) {
    // flush disk cache. read cache do not need clean
    int ret =
        diskCacheManagerImpl_->UploadWriteCacheByInode(std::to_string(inodeId));
    LOG_IF(ERROR, ret < 0) << "FlushAllCache, inode:" << inodeId
                           << ", upload write cache fail";
    return ret;
}

void StorageAdaptor::ReleaseCache(uint64_t inodeId) {
    FileCacheManagerPtr fileCacheManager =
        fsCacheManager_->FindFileCacheManager(inodeId);
    if (!fileCacheManager) {
        return;
    }
    VLOG(9) << "ReleaseCache inode:" << inodeId;
    fileCacheManager->ReleaseCache();
    fsCacheManager_->ReleaseFileCacheManager(inodeId);
    return;
}

void StorageAdaptor::Enqueue(
  std::shared_ptr<FlushChunkCacheContext> context) {
    auto task = [this, context]() {
        this->FlushChunkClosure(context);
    };
    taskPool_.Enqueue(task);
}

int StorageAdaptor::FlushChunkClosure(
  std::shared_ptr<FlushChunkCacheContext> context) {
    VLOG(9) << "FlushChunkCacheClosure start: " << context->inode;
    CURVEFS_ERROR ret = context->chunkCacheManptr->Flush(
      context->inode, context->force);
    // set the returned value
    // it is need in FlushChunkCacheCallBack
    context->retCode = ret;
    context->cb(context);
    VLOG(9) << "FlushChunkCacheClosure end: " << context->inode;
    return 0;
}

}  // namespace client
}  // namespace curvefs

