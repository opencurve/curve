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
 * Created Date: 21-08-13
 * Author: hzwuhongsong
 */
#include <sys/vfs.h>
#include <errno.h>
#include <string>
#include <cstdio>
#include <list>
#include <memory>

#include "curvefs/src/client/s3/client_s3_adaptor.h"
#include "curvefs/src/client/s3/disk_cache_manager_impl.h"

namespace curvefs {

namespace client {

DiskCacheManagerImpl::DiskCacheManagerImpl(
    std::shared_ptr<DiskCacheManager> diskCacheManager, S3Client *client) {
    diskCacheManager_ = diskCacheManager;
    client_ = client;
}

int DiskCacheManagerImpl::Init(const S3ClientAdaptorOption option) {
    LOG(INFO) << "DiskCacheManagerImpl init start.";
    int ret = diskCacheManager_->Init(client_, option);
    if (ret < 0) {
        LOG(ERROR) << "DiskCacheManagerImpl init error.";
        return ret;
    }

    forceFlush_ = option.diskCacheOpt.forceFlush;
    threads_ = option.diskCacheOpt.threads;
    taskPool_.Start(threads_);
    LOG(INFO) << "DiskCacheManagerImpl init end.";
    return 0;
}

void DiskCacheManagerImpl::Enqueue(
  std::shared_ptr<PutObjectAsyncContext> context) {
    auto task = [this, context]() {
        this->WriteClosure(context);
    };
    taskPool_.Enqueue(task);
}

int DiskCacheManagerImpl::WriteClosure(
  std::shared_ptr<PutObjectAsyncContext> context) {
    VLOG(9) << "WriteClosure start, name: " << context->key;
    int ret = Write(context->key, context->buffer, context->bufferSize);
     // set the returned value
    // it is need in CallBack
    context->retCode = ret;
    context->cb(context);
    VLOG(9) << "WriteClosure end, name: " << context->key;
    return 0;
}

int DiskCacheManagerImpl::Write(const std::string name, const char *buf,
                                uint64_t length) {
    VLOG(9) << "write name = " << name << ", length = " << length;
    int ret = 0;
    ret = WriteDiskFile(name, buf, length);
    if (ret < 0) {
        ret = client_->Upload(name, buf, length);
        if (ret < 0) {
            LOG(ERROR) << "upload object fail. object: " << name;
            return -1;
        }
    }
    VLOG(9) << "write success, write name = " << name;
    return 0;
}

int DiskCacheManagerImpl::WriteDiskFile(const std::string name, const char *buf,
                                        uint64_t length) {
    VLOG(9) << "write name = " << name << ", length = " << length;
    // if cache disk is full
    if (diskCacheManager_->IsDiskCacheFull()) {
        VLOG(3) << "write disk file fail, disk full.";
        return -1;
    }
    // write to cache disk
    int writeRet =
        diskCacheManager_->WriteDiskFile(name, buf, length, forceFlush_);
    if (writeRet < 0) {
        LOG(ERROR) << "write disk file error. writeRet = " << writeRet;
        return writeRet;
    }
    // add read cache
    std::string cacheWriteFullDir, cacheReadFullDir;
    cacheWriteFullDir = diskCacheManager_->GetCacheWriteFullDir();
    cacheReadFullDir = diskCacheManager_->GetCacheReadFullDir();
    int linkRet = diskCacheManager_->LinkWriteToRead(name, cacheWriteFullDir,
                                                     cacheReadFullDir);
    if (linkRet < 0) {
        LOG(ERROR) << "link write file to read error. linkRet = " << linkRet;
        return linkRet;
    }
    // add cache.
    diskCacheManager_->AddCache(name);

    // notify async load to s3
    diskCacheManager_->AsyncUploadEnqueue(name);
    return writeRet;
}

int DiskCacheManagerImpl::WriteReadDirect(const std::string fileName,
                                          const char *buf, uint64_t length) {
    if (diskCacheManager_->IsDiskCacheFull()) {
        VLOG(3) << "write disk file fail, disk full.";
        return -1;
    }
    int ret = diskCacheManager_->WriteReadDirect(fileName, buf, length);
    if (ret < 0) {
        LOG(ERROR) << "write file read direct fail, ret = " << ret;
        return ret;
    }
    // add cache.
    diskCacheManager_->AddCache(fileName, false);
    return ret;
}

int DiskCacheManagerImpl::Read(const std::string name, char *buf,
                               uint64_t offset, uint64_t length) {
    VLOG(9) << "read name = " << name << ", offset = " << offset
            << ", length = " << length;
    if (buf == NULL) {
        LOG(ERROR) << "read file error, read buf is null.";
        return -1;
    }
    // read disk file maybe fail because of disk file has been removed.
    int ret = diskCacheManager_->ReadDiskFile(name, buf, offset, length);
    if (ret < 0 || ret < length) {
        LOG(ERROR) << "read disk file error. readRet = " << ret;
        ret = client_->Download(name, buf, offset, length);
        if (ret < 0) {
            LOG(ERROR) << "download object fail. object name = " << name;
            return ret;
        }
    }
    VLOG(9) << "read success, read name = " << name;
    return ret;
}

bool DiskCacheManagerImpl::IsCached(const std::string name) {
    return diskCacheManager_->IsCached(name);
}

bool DiskCacheManagerImpl::IsDiskCacheFull() {
    return diskCacheManager_->IsDiskCacheFull();
}

int DiskCacheManagerImpl::UmountDiskCache() {
    int ret;
    ret = diskCacheManager_->UmountDiskCache();
    if (ret < 0) {
        LOG(ERROR) << "umount disk cache error.";
        return -1;
    }
    taskPool_.Stop();
    return 0;
}

void DiskCacheManagerImpl::InitMetrics(std::string fsName) {
    diskCacheManager_->InitMetrics(fsName);
}

int DiskCacheManagerImpl::UploadWriteCacheByInode(const std::string &inode) {
    return diskCacheManager_->UploadWriteCacheByInode(inode);
}

int DiskCacheManagerImpl::ClearReadCache(const std::list<std::string> &files) {
    return diskCacheManager_->ClearReadCache(files);
}

}  // namespace client
}  // namespace curvefs
