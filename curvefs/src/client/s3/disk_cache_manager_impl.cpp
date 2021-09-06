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

#include "curvefs/src/client/s3/client_s3_adaptor.h"
#include "curvefs/src/client/s3/disk_cache_manager_impl.h"

namespace curvefs {

namespace client {

DiskCacheManagerImpl::DiskCacheManagerImpl(
        std::shared_ptr<DiskCacheManager> diskCacheManager) {
    diskCacheManager_ = diskCacheManager;
}

int DiskCacheManagerImpl::Init(S3Client *client,
                          const S3ClientAdaptorOption option) {
    LOG(INFO) << "DiskCacheManagerImpl init start.";
    int ret;
    ret = diskCacheManager_->Init(client, option);
    if (ret < 0) {
        LOG(ERROR) << "DiskCacheManagerImpl init error.";
        return ret;
    }
    forceFlush_ = option.forceFlush;
    LOG(INFO) << "DiskCacheManagerImpl init end.";
    return 0;
}

int DiskCacheManagerImpl::Write(const std::string name,
                          const char* buf, uint64_t length) {
    LOG(INFO) << "write name = " << name
              << ", length = " << length;
    int ret;
    // if cache disk is full
    if (diskCacheManager_->IsDiskCacheFull()) {
        LOG(ERROR) << "write disk file fail, disk full.";
        return -1;
    }
    // write to cache disk
    int writeRet = diskCacheManager_->WriteDiskFile(
                 name, buf, length, forceFlush_);
    if (writeRet < 0) {
        LOG(ERROR) << "write disk file error. writeRet = " << writeRet;
        return writeRet;
    }
    // add read cache
    std::string cacheWriteFullDir, cacheReadFullDir;
    cacheWriteFullDir = diskCacheManager_->GetCacheWriteFullDir();
    cacheReadFullDir = diskCacheManager_->GetCacheReadFullDir();
    int linkRet = diskCacheManager_->LinkWriteToRead(name,
                                cacheWriteFullDir, cacheReadFullDir);
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

int DiskCacheManagerImpl::Read(const std::string name,
        char* buf, uint64_t offset, uint64_t length) {
    LOG(INFO) << "read name = " << name
              << ", offset = " << offset
              << ", length = " << length;
    if (buf == NULL) {
        LOG(ERROR) << "read file error, read buf is null.";
        return -1;
    }
    // read disk file maybe fail because of disk file has been removed.
    int readRet = diskCacheManager_->ReadDiskFile(
                 name, buf, offset, length);
    if (readRet < length) {
        LOG(ERROR) << "read disk file error. readRet = " << readRet;
        return readRet;
    }
    return readRet;
}

bool DiskCacheManagerImpl::IsCached(const std::string name) {
    return diskCacheManager_->IsCached(name);
}

int DiskCacheManagerImpl::UmountDiskCache() {
    int ret;
    ret = diskCacheManager_->UmountDiskCache();
    if (ret < 0) {
        LOG(ERROR) << "umount disk cache error.";
        return -1;
    }
    return 0;
}

}  // namespace client
}  // namespace curvefs
