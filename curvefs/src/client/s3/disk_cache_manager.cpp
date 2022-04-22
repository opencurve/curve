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
#include <memory>
#include <list>

#include "curvefs/src/client/s3/client_s3_adaptor.h"
#include "curvefs/src/client/s3/disk_cache_manager.h"

namespace curvefs {

namespace client {

/**
 * use curl -L mdsIp:port/flags/avgFlushBytes?setvalue=true
 * for dynamic parameter configuration
 */
static bool pass_uint64(const char *, uint64_t) { return true; }
DEFINE_uint64(avgFlushBytes, 83886080, "the write throttle bps of disk cache");
DEFINE_validator(avgFlushBytes, &pass_uint64);
DEFINE_uint64(burstFlushBytes, 104857600, "the write burst bps of disk cache");
DEFINE_validator(burstFlushBytes, &pass_uint64);
DEFINE_uint64(burstSecs, 180, "the times that write burst bps can continue");
DEFINE_validator(burstSecs, &pass_uint64);
DEFINE_uint64(avgFlushIops, 0, "the write throttle iops of disk cache");
DEFINE_validator(avgFlushIops, &pass_uint64);
DEFINE_uint64(avgReadFileBytes, 83886080,
              "the read throttle bps of disk cache");
DEFINE_validator(avgReadFileBytes, &pass_uint64);
DEFINE_uint64(avgReadFileIops, 0, "the read throttle iops of disk cache");
DEFINE_validator(avgReadFileIops, &pass_uint64);

DiskCacheManager::DiskCacheManager(std::shared_ptr<PosixWrapper> posixWrapper,
                                   std::shared_ptr<DiskCacheWrite> cacheWrite,
                                   std::shared_ptr<DiskCacheRead> cacheRead) {
    posixWrapper_ = posixWrapper;
    cacheWrite_ = cacheWrite;
    cacheRead_ = cacheRead;
    isRunning_ = false;
    usedBytes_ = 0;
    diskFsUsedRatio_ = 0;
    fullRatio_ = 0;
    safeRatio_ = 0;
    maxUsableSpaceBytes_ = 0;
    // cannot limit the size,
    // because cache is been delete must after upload to s3
    cachedObjName_ = std::make_shared<
      LRUCache<std::string, bool>>(0,
        std::make_shared<CacheMetrics>("diskcache"));
}

int DiskCacheManager::Init(S3Client *client,
                           const S3ClientAdaptorOption option) {
    LOG(INFO) << "DiskCacheManager init start.";
    client_ = client;

    option_ = option;
    trimCheckIntervalSec_ = option.diskCacheOpt.trimCheckIntervalSec;
    fullRatio_ = option.diskCacheOpt.fullRatio;
    safeRatio_ = option.diskCacheOpt.safeRatio;
    cacheDir_ = option.diskCacheOpt.cacheDir;
    maxUsableSpaceBytes_ = option.diskCacheOpt.maxUsableSpaceBytes;
    cmdTimeoutSec_ = option.diskCacheOpt.cmdTimeoutSec;

    cacheWrite_->Init(client_, posixWrapper_, cacheDir_,
                      option.diskCacheOpt.asyncLoadPeriodMs, cachedObjName_);
    cacheRead_->Init(posixWrapper_, cacheDir_);
    int ret;
    ret = CreateDir();
    if (ret < 0) {
        LOG(ERROR) << "create cache dir error, ret = " << ret;
        return ret;
    }
    // load all cache read file
    // the all value of cachedObjName_ is set false
    ret = cacheRead_->LoadAllCacheReadFile(cachedObjName_);
    if (ret < 0) {
        LOG(ERROR) << "load all cache read file error. ret = " << ret;
        return ret;
    }

    // start aync upload thread
    cacheWrite_->AsyncUploadRun();
    std::thread uploadThread =
        std::thread(&DiskCacheManager::UploadAllCacheWriteFile, this);
    uploadThread.detach();

    // start trim thread
    TrimRun();

    SetDiskInitUsedBytes();
    SetDiskFsUsedRatio();

    FLAGS_avgFlushIops = option_.diskCacheOpt.avgFlushIops;
    FLAGS_avgFlushBytes = option_.diskCacheOpt.avgFlushBytes;
    FLAGS_burstFlushBytes = option_.diskCacheOpt.burstFlushBytes;
    FLAGS_burstSecs = option_.diskCacheOpt.burstSecs;
    FLAGS_avgReadFileIops = option_.diskCacheOpt.avgReadFileIops;
    FLAGS_avgReadFileBytes = option_.diskCacheOpt.avgReadFileBytes;

    InitQosParam();

    LOG(INFO) << "DiskCacheManager init success. "
              << ", cache dir is: " << cacheDir_
              << ", maxUsableSpaceBytes is: " << maxUsableSpaceBytes_
              << ", cmdTimeoutSec is: " << cmdTimeoutSec_
              << ", safeRatio is: " << safeRatio_
              << ", fullRatio is: " << fullRatio_
              << ", disk used bytes: " << GetDiskUsedbytes();
    return 0;
}

void DiskCacheManager::InitQosParam() {
    ReadWriteThrottleParams params;
    params.iopsWrite = ThrottleParams(FLAGS_avgFlushIops, 0, 0);
    params.bpsWrite = ThrottleParams(FLAGS_avgFlushBytes, FLAGS_burstFlushBytes,
                                     FLAGS_burstSecs);
    params.iopsRead = ThrottleParams(FLAGS_avgReadFileIops, 0, 0);
    params.bpsRead = ThrottleParams(FLAGS_avgReadFileBytes, 0, 0);

    diskCacheThrottle_.UpdateThrottleParams(params);
}

int DiskCacheManager::UploadAllCacheWriteFile() {
    return cacheWrite_->UploadAllCacheWriteFile();
}

int DiskCacheManager::UploadWriteCacheByInode(const std::string &inode) {
    return cacheWrite_->UploadFileByInode(inode);
}

int DiskCacheManager::ClearReadCache(const std::list<std::string> &files) {
    return cacheRead_->ClearReadCache(files);
}

void DiskCacheManager::AddCache(const std::string name) {
    cachedObjName_->Put(name, true);
    VLOG(9) << "cache size is: " << cachedObjName_->Size();
}

bool DiskCacheManager::IsCached(const std::string name) {
    bool exist;
    if (!cachedObjName_->Get(name, &exist)) {
        VLOG(9) << "not cached, name = " << name;
        return false;
    }
    VLOG(9) << "cached, name = " << name;
    return true;
}

int DiskCacheManager::UmountDiskCache() {
    LOG(INFO) << "umount disk cache.";
    int ret;
    ret = cacheWrite_->UploadAllCacheWriteFile();
    if (ret < 0) {
        LOG(ERROR) << "umount disk cache error.";
    }
    TrimStop();
    cacheWrite_->AsyncUploadStop();
    LOG(INFO) << "umount disk cache end.";
    return 0;
}

int DiskCacheManager::CreateDir() {
    struct stat statFile;
    int ret;
    ret = posixWrapper_->stat(cacheDir_.c_str(), &statFile);
    if (ret < 0) {
        ret = posixWrapper_->mkdir(cacheDir_.c_str(), 0755);
        if ((ret < 0) && (errno != EEXIST)) {
            LOG(ERROR) << "create cache dir error. errno = " << errno
                       << ", dir = " << cacheDir_;
            return -1;
        }
        VLOG(9) << "cache dir is not exist, create it success."
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
    VLOG(9) << "create cache dir sucess.";
    return 0;
}

std::string DiskCacheManager::GetCacheReadFullDir() {
    return cacheRead_->GetCacheIoFullDir();
}

std::string DiskCacheManager::GetCacheWriteFullDir() {
    return cacheWrite_->GetCacheIoFullDir();
}

int DiskCacheManager::WriteDiskFile(const std::string fileName, const char *buf,
                                    uint64_t length, bool force) {
    // write throttle
    diskCacheThrottle_.Add(false, length);
    int ret = cacheWrite_->WriteDiskFile(fileName, buf, length, force);
    if (ret > 0)
        AddDiskUsedBytes(ret);
    return ret;
}

void DiskCacheManager::AsyncUploadEnqueue(const std::string objName) {
    cacheWrite_->AsyncUploadEnqueue(objName);
}

int DiskCacheManager::ReadDiskFile(const std::string name, char *buf,
                                   uint64_t offset, uint64_t length) {
    // read throttle
    diskCacheThrottle_.Add(true, length);
    return cacheRead_->ReadDiskFile(name, buf, offset, length);
}

int DiskCacheManager::WriteReadDirect(const std::string fileName,
                                      const char *buf, uint64_t length) {
    // write hrottle
    diskCacheThrottle_.Add(false, length);
    int ret = cacheRead_->WriteDiskFile(fileName, buf, length);
    if (ret > 0)
        AddDiskUsedBytes(ret);
    return ret;
}

int DiskCacheManager::LinkWriteToRead(const std::string fileName,
                                      const std::string fullWriteDir,
                                      const std::string fullReadDir) {
    return cacheRead_->LinkWriteToRead(fileName, fullWriteDir, fullReadDir);
}

int64_t DiskCacheManager::SetDiskFsUsedRatio() {
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

    VLOG(9) << "cache disk usage = " << usedPercent;

    diskFsUsedRatio_.store(usedPercent, std::memory_order_seq_cst);
    return usedPercent;
}

void DiskCacheManager::SetDiskInitUsedBytes() {
    std::string cmd = "timeout " + std::to_string(cmdTimeoutSec_) + " du -sb " +
                      cacheDir_ + " | awk '{printf $1}' ";
    SysUtils sysUtils;
    std::string result = sysUtils.RunSysCmd(cmd);
    if (result.empty()) {
        LOG(ERROR) << "get disk used size failed.";
        return;
    }
    uint64_t usedBytes = 0;
    if (!curve::common::StringToUll(result, &usedBytes)) {
        LOG(ERROR) << "get disk used size failed.";
        return;
    }
    usedBytes_.fetch_add(usedBytes, std::memory_order_seq_cst);
    VLOG(9) << "cache disk used size is: " << result;
    return;
}

bool DiskCacheManager::IsDiskCacheFull() {
    int64_t ratio = diskFsUsedRatio_.load(std::memory_order_seq_cst);
    uint64_t usedBytes = GetDiskUsedbytes();
    if (ratio >= fullRatio_ || usedBytes >= maxUsableSpaceBytes_) {
        VLOG(6) << "disk cache is full"
                     << ", ratio is: " << ratio << ", fullRatio is: "
                     << fullRatio_ << ", used bytes is: " << usedBytes;
        waitIntervalSec_.StopWait();
        return true;
    }
    if (!IsDiskCacheSafe()) {
        VLOG(6) << "wake up trim thread.";
        waitIntervalSec_.StopWait();
    }
    return false;
}

bool DiskCacheManager::IsDiskCacheSafe() {
    int64_t ratio = diskFsUsedRatio_.load(std::memory_order_seq_cst);
    uint64_t usedBytes = GetDiskUsedbytes();
    if ((usedBytes < (safeRatio_ * maxUsableSpaceBytes_ / 100))
      && (ratio < safeRatio_)) {
        VLOG(9) << "disk cache is safe"
                << ", usedBytes is: " << usedBytes
                << ", use ratio is: " << ratio;
        return true;
    }
    VLOG(6) << "disk cache is not safe"
                << ", usedBytes is: " << usedBytes
                << ", use ratio is: " << ratio;
    return false;
}

void DiskCacheManager::TrimCache() {
    const std::chrono::seconds sleepSec(trimCheckIntervalSec_);
    LOG(INFO) << "trim function start.";
    waitIntervalSec_.Init(trimCheckIntervalSec_ * 1000);
    // 1. check cache disk usage every sleepSec seconds.
    // 2. if cache disk is full,
    //    then remove disk file until cache disk is lower than safeRatio_.
    std::string cacheReadFullDir, cacheWriteFullDir,
      cacheReadFile, cacheWriteFile, cacheKey;
    cacheReadFullDir = GetCacheReadFullDir();
    cacheWriteFullDir = GetCacheWriteFullDir();
    while (true) {
        waitIntervalSec_.WaitForNextExcution();
        if (!isRunning_) {
            LOG(INFO) << "trim thread end.";
            return;
        }
        VLOG(9) << "trim thread wake up.";
        InitQosParam();
        SetDiskFsUsedRatio();
            while (!IsDiskCacheSafe()) {
                if (!cachedObjName_->GetLast(false, &cacheKey)) {
                    VLOG(3) << "obj is empty";
                    break;
                }

                VLOG(6) << "obj will be removed01: " << cacheKey;
                cacheReadFile = cacheReadFullDir + "/" + cacheKey;
                cacheWriteFile = cacheWriteFullDir + "/" + cacheKey;
                struct stat statFile;
                int ret;
                ret = posixWrapper_->stat(cacheWriteFile.c_str(), &statFile);
                // if file has not been uploaded to S3,
                // but remove the cache read file,
                // then read will fail when do cache read,
                // and then it cannot load the file from S3.
                // so read is fail.
                if (ret == 0) {
                    VLOG(1) << "do not remove this disk file"
                            << ", file has not been uploaded to S3."
                            << ", file is: " << cacheKey;
                    continue;
                }
                cachedObjName_->Remove(cacheKey);
                struct stat statReadFile;
                ret = posixWrapper_->stat(cacheReadFile.c_str(), &statReadFile);
                if (ret != 0) {
                    VLOG(0) << "stat disk file error"
                            << ", file is: " << cacheKey;
                    continue;
                }
                // if remove disk file before delete cache,
                // then read maybe fail.
                const char *toDelFile;
                toDelFile = cacheReadFile.c_str();
                ret = posixWrapper_->remove(toDelFile);
                if (ret < 0) {
                    LOG(ERROR)
                        << "remove disk file error, file is: " << cacheKey;
                    continue;
                }
                DecDiskUsedBytes(statReadFile.st_size);
                VLOG(6) << "remove disk file success, file is: " << cacheKey;
            }
    }
    LOG(INFO) << "trim function end.";
}

int DiskCacheManager::TrimRun() {
    if (isRunning_.exchange(true)) {
        LOG(INFO) << "DiskCacheManager trim thread is on running.";
        return -1;
    }
    backEndThread_ = std::thread(&DiskCacheManager::TrimCache, this);
    LOG(INFO) << "DiskCacheManager trim thread started.";
    return 0;
}

int DiskCacheManager::TrimStop() {
    if (isRunning_.exchange(false)) {
        LOG(INFO) << "stop DiskCacheManager trim thread...";
        isRunning_ = false;
        waitIntervalSec_.StopWait();
        backEndThread_.join();
        LOG(INFO) << "stop DiskCacheManager trim thread ok.";
        return -1;
    } else {
        LOG(INFO) << "DiskCacheManager trim thread not running.";
    }
    return 0;
}

void DiskCacheManager::InitMetrics(const std::string &fsName) {
    metric_ = std::make_shared<DiskCacheMetric>(fsName);
    cacheWrite_->InitMetrics(metric_);
    cacheRead_->InitMetrics(metric_);
}

}  // namespace client
}  // namespace curvefs
