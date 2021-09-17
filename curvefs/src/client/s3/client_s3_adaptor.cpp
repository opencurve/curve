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

void S3ClientAdaptorImpl::Init(
    const S3ClientAdaptorOption& option, S3Client* client,
    std::shared_ptr<InodeCacheManager> inodeManager) {
    blockSize_ = option.blockSize;
    chunkSize_ = option.chunkSize;
    metaServerEps_ = option.metaServerEps;
    allocateServerEps_ = option.allocateServerEps;
    flushIntervalSec_ = option.flushInterval;
    enableDiskCache_ = option.diskCacheOpt.enableDiskCache;
    client_ = client;
    inodeManager_ = inodeManager;
    fsCacheManager_ = std::make_shared<FsCacheManager>(this);
    waitIntervalSec_.Init(option.intervalSec * 1000);
    toStop_.store(false, std::memory_order_release);
    bgFlushThread_ = Thread(&S3ClientAdaptorImpl::BackGroundFlush, this);

    if (enableDiskCache_) {
        std::shared_ptr<PosixWrapper> wrapper =
            std::make_shared<PosixWrapper>();
        std::shared_ptr<DiskCacheRead> diskCacheRead =
            std::make_shared<DiskCacheRead>();
        std::shared_ptr<DiskCacheWrite> diskCacheWrite =
            std::make_shared<DiskCacheWrite>();
        std::shared_ptr<DiskCacheManager>  diskCacheManager = std::make_shared<
            DiskCacheManager>(wrapper, diskCacheWrite, diskCacheRead);
        diskCacheManagerImpl_ =
            std::make_shared<DiskCacheManagerImpl>(diskCacheManager, client);
        diskCacheManagerImpl_->Init(option);
    }
}

int S3ClientAdaptorImpl::Write(Inode* inode, uint64_t offset, uint64_t length,
                               const char* buf) {
    uint64_t fsId = inode->fsid();
    uint64_t inodeId = inode->inodeid();

    LOG(INFO) << "write start offset:" << offset << ", len:" << length
              << ",inode length:" << inode->length() << ", fsId:" << fsId
              << ", inodeId" << inodeId;

    FileCacheManagerPtr fileCacheManager =
        fsCacheManager_->FindOrCreateFileCacheManager(fsId, inodeId);

    return fileCacheManager->Write(offset, length, buf);
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
    CURVEFS_ERROR ret;
    uint64_t fsId = inode->fsid();
    while (len > 0) {
        if (chunkPos + len > chunkSize_) {
            n = chunkSize_ - chunkPos;
        } else {
            n = len;
        }
        ret = AllocS3ChunkId(fsId, &chunkId);
        if (ret != CURVEFS_ERROR::OK) {
            LOG(ERROR) << "Truncate alloc s3 chunkid fail. ret:" << ret;
            return ret;
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

CURVEFS_ERROR S3ClientAdaptorImpl::Flush(Inode* inode) {
    FileCacheManagerPtr fileCacheManager =
        fsCacheManager_->FindFileCacheManager(inode->inodeid());
    if (!fileCacheManager) {
        return CURVEFS_ERROR::OK;
    }

    return fileCacheManager->Flush(inode, true);
}

CURVEFS_ERROR S3ClientAdaptorImpl::FsSync() {
    return fsCacheManager_->FsSync(true);
}

CURVEFS_ERROR S3ClientAdaptorImpl::AllocS3ChunkId(uint32_t fsId,
                                                  uint64_t* chunkId) {
    brpc::Channel channel;

    if (channel.Init(allocateServerEps_.c_str(), nullptr) != 0) {
        LOG(ERROR) << "Fail to init channel to allocate Server"
                   << " for alloc chunkId: " << allocateServerEps_;
        return CURVEFS_ERROR::INTERNAL;
    }
    brpc::Controller* cntl = new brpc::Controller();
    AllocateS3ChunkRequest request;
    AllocateS3ChunkResponse response;

    request.set_fsid(fsId);

    curvefs::space::SpaceAllocService_Stub stub(&channel);

    stub.AllocateS3Chunk(cntl, &request, &response, nullptr);

    if (cntl->Failed()) {
        LOG(WARNING) << "Allocate s3 chunkid Failed, errorcode = "
                     << cntl->ErrorCode()
                     << ", error content:" << cntl->ErrorText()
                     << ", log id = " << cntl->log_id();
        CURVEFS_ERROR error = static_cast<CURVEFS_ERROR>(-cntl->ErrorCode());
        delete cntl;
        cntl = nullptr;
        return error;
    }

    ::curvefs::space::SpaceStatusCode ssCode = response.status();
    if (ssCode != ::curvefs::space::SpaceStatusCode::SPACE_OK) {
        LOG(WARNING) << "Allocate s3 chunkid response Failed, retCode = "
                     << ssCode;
        delete cntl;
        cntl = nullptr;
        return CURVEFS_ERROR::INTERNAL;
    }

    *chunkId = response.chunkid();
    delete cntl;
    cntl = nullptr;
    return CURVEFS_ERROR::OK;
}
void S3ClientAdaptorImpl::BackGroundFlush() {
    while (!toStop_.load(std::memory_order_acquire)) {
        if (fsCacheManager_->GetDataCacheNum() == 0) {
            std::unique_lock<std::mutex> lck(mtx_);
            cond_.wait(lck);
        }

        waitIntervalSec_.WaitForNextExcution();
        fsCacheManager_->FsSync(false);
    }
    return;
}

int S3ClientAdaptorImpl::Stop() {
    LOG(INFO) << "Stopping S3ClientAdaptor.";
    diskCacheManagerImpl_->UmountDiskCache();
    waitIntervalSec_.StopWait();
    toStop_.store(true, std::memory_order_release);
    bgFlushThread_.join();

    return 0;
}

}  // namespace client
}  // namespace curvefs
