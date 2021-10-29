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
#include "curvefs/src/client/s3/disk_cache_manager.h"

namespace curvefs {

namespace client {

DiskCacheManager::DiskCacheManager(std::shared_ptr<PosixWrapper> posixWrapper,
                    std::shared_ptr<DiskCacheWrite> cacheWrite,
                    std::shared_ptr<DiskCacheRead> cacheRead) {
    posixWrapper_ = posixWrapper;
    cacheWrite_ = cacheWrite;
    cacheRead_ = cacheRead;
    isRunning_ = false;
}

int DiskCacheManager::Init(S3Client *client,
                          const S3ClientAdaptorOption option) {
    LOG(INFO) << "DiskCacheManager init start.";
    client_ = client;

    trimCheckIntervalSec_ = option.diskCacheOpt.trimCheckIntervalSec;
    fullRatio_ = option.diskCacheOpt.fullRatio;
    safeRatio_ = option.diskCacheOpt.safeRatio;
    cacheDir_ = option.diskCacheOpt.cacheDir;

    cacheWrite_->Init(client_, posixWrapper_, cacheDir_);
    cacheRead_->Init(posixWrapper_, cacheDir_);
    int ret;
    ret = CreateDir();
    if (ret < 0) {
        LOG(ERROR) << "create cache dir error, ret = " << ret;
        return ret;
    }
    // start aync upload thread
    cacheWrite_->AsyncUploadRun();
    // upload all file in write cache to S3
    ret = cacheWrite_->UploadAllCacheWriteFile();
    if (ret < 0) {
        LOG(ERROR) << "upload cache write file error. ret = " << ret;
        return ret;
    }
    // load all cache read file
    ret = cacheRead_->LoadAllCacheReadFile(&cachedObjName_);
    if (ret < 0) {
        LOG(ERROR) << "load all cache read file error. ret = " << ret;
        return ret;
    }
    // start trim thread
    TrimRun();
    LOG(INFO) << "DiskCacheManager init success.";
    return 0;
}

void DiskCacheManager::AddCache(const std::string name) {
    std::lock_guard<bthread::Mutex>  lk(mtx_);
    cachedObjName_.emplace(name);
}

bool DiskCacheManager::IsCached(const std::string name) {
    std::lock_guard<bthread::Mutex>  lk(mtx_);
    std::set<std::string>::iterator cacheKeyIter;
    cacheKeyIter = cachedObjName_.find(name);
    if (cacheKeyIter == cachedObjName_.end()) {
        LOG(INFO) << "not cached, name = " << name;
        return false;
    }
    LOG(INFO) << "cached, name = " << name;
    return true;
}

int DiskCacheManager::UmountDiskCache() {
    LOG(INFO) << "umount disk cache.";
    int ret;
    ret = cacheWrite_->UploadAllCacheWriteFile();
    if (ret < 0) {
        LOG(ERROR) << "umount disk cache error.";
        return -1;
    }
    TrimStop();
    LOG(INFO) << "umount disk cache end.";
    return 0;
}

int DiskCacheManager::CreateDir() {
    struct stat statFile;
    int ret;
    ret = posixWrapper_->stat(cacheDir_.c_str(), &statFile);
    if (ret < 0) {
        ret = posixWrapper_->mkdir(cacheDir_.c_str(), 0755);
        if (ret < 0) {
            LOG(ERROR) << "create cache dir error. errno = " << errno
                       << ", dir = " << cacheDir_;
            return -1;
        }
        LOG(INFO) << "cache dir is not exist, create it success."
                     << ", dir = " << cacheDir_;
    } else {
        LOG(INFO) << "cache dir is exist."
                     << ", dir = " << cacheDir_;
    }

    ret = cacheWrite_->CreateIoDir(true);
    if (ret < 0) {
        LOG(ERROR) << "create cache write dir error. ret = " << ret;
        return ret;
    }
    ret = cacheRead_->CreateIoDir(false);
    if (ret < 0) {
        LOG(ERROR) << "create cache read dir error. ret = " << ret;
        return ret;
    }
    LOG(INFO) << "create cache dir sucess.";
    return 0;
}

std::string DiskCacheManager::GetCacheReadFullDir() {
    return cacheRead_->GetCacheIoFullDir();
}

std::string DiskCacheManager::GetCacheWriteFullDir() {
    return cacheWrite_->GetCacheIoFullDir();
}

int DiskCacheManager::WriteDiskFile(const std::string fileName,
                 const char* buf, uint64_t length, bool force) {
    return cacheWrite_->WriteDiskFile(fileName, buf, length, force);
}

void DiskCacheManager::AsyncUploadEnqueue(const std::string objName) {
    cacheWrite_->AsyncUploadEnqueue(objName);
}

int DiskCacheManager::ReadDiskFile(const std::string name,
                char* buf, uint64_t offset, uint64_t length) {
    return cacheRead_->ReadDiskFile(name, buf, offset, length);
}

int DiskCacheManager::WriteReadDirect(const std::string fileName,
                 const char* buf, uint64_t length) {
    return cacheRead_->WriteDiskFile(fileName, buf, length);
}

int DiskCacheManager::LinkWriteToRead(const std::string fileName,
                   const std::string fullWriteDir,
                   const std::string fullReadDir) {
    return cacheRead_->LinkWriteToRead(fileName, fullWriteDir, fullReadDir);
}

int64_t DiskCacheManager::CacheDiskUsedRatio() {
    struct statfs stat;
    if (posixWrapper_->statfs(cacheDir_.c_str(), &stat) == -1) {
        LOG(ERROR) << "get cache disk space error.";
        return -1;
    }

    int64_t frsize = stat.f_frsize;
    int64_t totalBytes = stat.f_blocks * frsize;
    int64_t freeBytes = stat.f_bfree * frsize;
    int64_t availableBytes = stat.f_bavail * frsize;
    int64_t usedBytes = totalBytes - freeBytes;
    int64_t usedPercent = 100 * usedBytes / (usedBytes + availableBytes) + 1;

    LOG(INFO) << "cache disk usage = " << usedPercent;
    return usedPercent;
}

bool DiskCacheManager::IsDiskCacheFull() {
    int64_t ratio = CacheDiskUsedRatio();
    if (ratio < 0) {
        LOG(ERROR) << "get disk use ratio error.";
        return false;
    }
    if (ratio >= fullRatio_) {
        LOG(INFO) << "disk cache is full"
                     << ", ratio = " << ratio
                     << ", fullRatio = " << fullRatio_;
        return true;
    }
    return false;
}

bool DiskCacheManager::IsDiskCacheSafe() {
    int64_t ratio = CacheDiskUsedRatio();
    if (ratio < 0) {
        LOG(ERROR) << "get disk use ratio error." << ratio;
        return false;
    }
    if (ratio < safeRatio_) {
        LOG(INFO) << "disk cache is safe"
                     << ", ratio = " << ratio
                     << ", safeRatio = " << safeRatio_;
        return true;
    }
    return false;
}

void DiskCacheManager::TrimCache() {
    const std::chrono::seconds sleepSec(trimCheckIntervalSec_);
    LOG(INFO) << "trim function start.";
    // 1. check cache disk usage every sleepSec seconds.
    // 2. if cache disk is full,
    //    then remove disk file until cache disk is lower than safeRatio_.
    while (sleeper_.wait_for(sleepSec)) {
        if (!isRunning_) {
            LOG(INFO) << "trim thread end.";
            return;
        }
        LOG(INFO) << "trim thread wake up.";
        if (IsDiskCacheFull()) {
            LOG(INFO) << "disk cache full, begin trim.";
            std::string cacheReadFullDir;
            std::string cacheWriteFullDir;
            cacheReadFullDir = GetCacheReadFullDir();
            cacheWriteFullDir = GetCacheWriteFullDir();
            std::set<std::string> cachedObjNameTmp;
            {
                std::lock_guard<bthread::Mutex>  lk(mtx_);
                cachedObjNameTmp = cachedObjName_;
            }
            while (!IsDiskCacheSafe()) {
                std::string cacheReadFile, cacheWriteFile;
                std::set<std::string>::iterator cacheKeyIter;
                cacheKeyIter = cachedObjNameTmp.begin();
                if (cacheKeyIter == cachedObjNameTmp.end()) {
                    LOG(ERROR) << "remove disk file error"
                              << ", cachedObjName is empty.";
                    break;
                }

                cacheReadFile = cacheReadFullDir + "/" + *cacheKeyIter;
                cacheWriteFile = cacheWriteFullDir + "/" + *cacheKeyIter;
                struct stat statFile;
                int ret;
                ret = posixWrapper_->stat(cacheWriteFile.c_str(), &statFile);
                // if file has not been uploaded to S3,
                // but remove the cache read file,
                // then read will fail when do cache read,
                // and then it cannot load the file from S3.
                // so read is fail.
                if (ret == 0) {
                    LOG(INFO) << "do not remove this disk file"
                                 << ", file has not been uploaded to S3."
                                 << ", file = " << *cacheKeyIter;
                    cachedObjNameTmp.erase(cacheKeyIter);
                    continue;
                }
                {
                    std::lock_guard<bthread::Mutex>  lk(mtx_);
                    std::set<std::string>::iterator iter;
                    iter = cachedObjName_.find(*cacheKeyIter);
                    if (iter == cachedObjName_.end()) {
                        LOG(INFO) << "remove disk file error"
                                     << ", file is not exist in cachedObjName"
                                     << ", file = " << *cacheKeyIter;
                        cachedObjNameTmp.erase(cacheKeyIter);
                        continue;
                    }
                    cachedObjName_.erase(iter);
                }
                // if remove disk file before delete cache,
                // then read maybe fail.
                const char* toDelFile;
                toDelFile = cacheReadFile.c_str();
                ret = posixWrapper_->remove(toDelFile);
                if (ret < 0) {
                    LOG(ERROR) << "remove disk file error, file = "
                               << *cacheKeyIter;
                    cachedObjNameTmp.erase(cacheKeyIter);
                    continue;
                }
                LOG(INFO) << "remove disk file success, file = "
                             << *cacheKeyIter;
                cachedObjNameTmp.erase(cacheKeyIter);
            }
            LOG(INFO) << "trim over.";
        }
    }
    LOG(INFO) << "trim function end.";
}

int DiskCacheManager::TrimRun() {
    if (isRunning_.exchange(true)) {
        LOG(INFO) << "DiskCacheManager trim thread is on running.";
        return -1;
    }
    backEndThread_ = std::thread(
        &DiskCacheManager::TrimCache, this);
    LOG(INFO) << "DiskCacheManager trim thread started.";
    return 0;
}

int DiskCacheManager::TrimStop() {
    if (isRunning_.exchange(false)) {
        LOG(INFO) << "stop DiskCacheManager trim thread...";
        isRunning_ = false;
        sleeper_.interrupt();
        backEndThread_.join();
        LOG(INFO) << "stop DiskCacheManager trim thread ok.";
        return -1;
    } else {
        LOG(INFO) << "DiskCacheManager trim thread not running.";
    }
    return 0;
}

}  // namespace client
}  // namespace curvefs
