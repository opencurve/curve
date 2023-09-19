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

#include <glog/logging.h>
#include <sys/vfs.h>
#include <errno.h>
#include <string>
#include <cstdio>
#include <memory>
#include <list>
#include <cstdint>

#include "curvefs/src/client/s3/client_s3_adaptor.h"
#include "curvefs/src/client/s3/disk_cache_manager.h"
#include "curvefs/src/common/s3util.h"

namespace curvefs {

namespace client {

/**
 * use curl -L mdsIp:port/flags/avgFlushBytes?setvalue=true
 * for dynamic parameter configuration
 */
static bool pass_uint32(const char *, uint32_t) { return true; }
static bool ValidateRatio(const char *, uint32_t ratio) { return ratio <= 100; }
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

DEFINE_uint32(diskNearFullRatio, 70, "the nearfull ratio of disk cache");
DEFINE_validator(diskNearFullRatio, &ValidateRatio);
DEFINE_uint32(diskFullRatio, 90, "the nearfull ratio of disk cache");
DEFINE_validator(diskFullRatio, &ValidateRatio);
DEFINE_uint32(diskTrimRatio, 50, "the trim ratio when disk nearfull");
DEFINE_validator(diskTrimRatio, &ValidateRatio);
DEFINE_uint32(diskTrimCheckIntervalSec, 5, "trim check interval seconds");
DEFINE_validator(diskTrimCheckIntervalSec, &pass_uint32);
DEFINE_uint64(diskMaxUsableSpaceBytes, 107374182400, "max space bytes can use");
DEFINE_validator(diskMaxUsableSpaceBytes, &pass_uint64);
DEFINE_uint64(diskMaxFileNums, 1000000, "max file nums can owner");
DEFINE_validator(diskMaxFileNums, &pass_uint64);

DiskCacheManager::DiskCacheManager(std::shared_ptr<PosixWrapper> posixWrapper,
                                   std::shared_ptr<DiskCacheWrite> cacheWrite,
                                   std::shared_ptr<DiskCacheRead> cacheRead) {
    posixWrapper_ = posixWrapper;
    cacheWrite_ = cacheWrite;
    cacheRead_ = cacheRead;
    isRunning_ = false;
    usedBytes_ = 0;
    diskFsUsedRatio_ = 0;
    diskUsedInit_ = false;
    objectPrefix_ = 0;
    // cannot limit the size,
    // because cache is been delete must after upload to s3
    cachedObjName_ = std::make_shared<
      SglLRUCache<std::string>>(0,
        std::make_shared<CacheMetrics>("diskcache"));
}

int DiskCacheManager::Init(std::shared_ptr<S3Client> client,
                           const S3ClientAdaptorOption option) {
    LOG(INFO) << "DiskCacheManager init start.";
    client_ = client;

    option_ = option;
    FLAGS_diskTrimCheckIntervalSec = option.diskCacheOpt.trimCheckIntervalSec;
    FLAGS_diskFullRatio = option.diskCacheOpt.fullRatio;
    FLAGS_diskNearFullRatio = option.diskCacheOpt.safeRatio;
    FLAGS_diskTrimRatio = option.diskCacheOpt.trimRatio;
    cacheDir_ = option.diskCacheOpt.cacheDir;
    FLAGS_diskMaxUsableSpaceBytes = option.diskCacheOpt.maxUsableSpaceBytes;
    FLAGS_diskMaxFileNums = option.diskCacheOpt.maxFileNums;
    cmdTimeoutSec_ = option.diskCacheOpt.cmdTimeoutSec;
    objectPrefix_ = option.objectPrefix;
    cacheWrite_->Init(client_, posixWrapper_, cacheDir_, objectPrefix_,
        option.diskCacheOpt.asyncLoadPeriodMs, cachedObjName_);
    cacheRead_->Init(posixWrapper_, cacheDir_, objectPrefix_);
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

    // start async upload thread
    cacheWrite_->AsyncUploadRun();
    std::thread uploadThread =
        std::thread(&DiskCacheManager::UploadAllCacheWriteFile, this);
    uploadThread.detach();

    // start trim thread
    TrimRun();

    UpdateDiskFsUsedRatio();

    FLAGS_avgFlushIops = option_.diskCacheOpt.avgFlushIops;
    FLAGS_avgFlushBytes = option_.diskCacheOpt.avgFlushBytes;
    FLAGS_burstFlushBytes = option_.diskCacheOpt.burstFlushBytes;
    FLAGS_burstSecs = option_.diskCacheOpt.burstSecs;
    FLAGS_avgReadFileIops = option_.diskCacheOpt.avgReadFileIops;
    FLAGS_avgReadFileBytes = option_.diskCacheOpt.avgReadFileBytes;
    InitQosParam();

    LOG(INFO) << "DiskCacheManager init success. "
              << ", cache dir is: " << cacheDir_
              << ", maxUsableSpaceBytes is: " << FLAGS_diskMaxUsableSpaceBytes
              << ", maxFileNums is: " << FLAGS_diskMaxFileNums
              << ", cmdTimeoutSec is: " << cmdTimeoutSec_
              << ", safeRatio is: " << FLAGS_diskNearFullRatio
              << ", fullRatio is: " << FLAGS_diskFullRatio
              << ", trimRatio is: " << FLAGS_diskTrimRatio
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

void DiskCacheManager::AddCache(const std::string &name) {
    cachedObjName_->Put(name);
    VLOG(9) << "cache size is: " << cachedObjName_->Size();
}

bool DiskCacheManager::IsCached(const std::string &name) {
    if (!cachedObjName_->IsCached(name)) {
        VLOG(9) << "not cached, name = " << name;
        return false;
    }
    VLOG(9) << "cached, name = " << name;
    return true;
}

bool DiskCacheManager::IsCacheClean() {
    return cacheWrite_->IsCacheClean();
}

int DiskCacheManager::UmountDiskCache() {
    LOG(INFO) << "umount disk cache.";
    if (diskInitThread_.joinable()) {
        diskInitThread_.join();
    }
    TrimStop();
    cacheWrite_->AsyncUploadStop();
    LOG_IF(ERROR, !IsCacheClean()) << "umount disk cache error.";
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
    VLOG(9) << "create cache dir success.";
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

int64_t DiskCacheManager::UpdateDiskFsUsedRatio() {
    struct statfs stat;
    if (posixWrapper_->statfs(cacheDir_.c_str(), &stat) == -1) {
        LOG_EVERY_N(WARNING, 100)
            << "get cache disk space error, errno is: " << errno;
        return -1;
    }

    int64_t frsize = stat.f_frsize;
    int64_t totalBytes = stat.f_blocks * frsize;
    int64_t freeBytes = stat.f_bfree * frsize;
    int64_t availableBytes = stat.f_bavail * frsize;
    int64_t usedBytes = totalBytes - freeBytes;
    if ((usedBytes == 0) &&
      (availableBytes == 0)) {
        LOG_EVERY_N(WARNING, 100) << "get cache disk space zero.";
        return -1;
    }
    int64_t usedPercent = 100 * usedBytes / (usedBytes + availableBytes) + 1;
    diskFsUsedRatio_.store(usedPercent);
    return usedPercent;
}

void DiskCacheManager::SetDiskInitUsedBytes() {
    std::string cmd = "timeout " + std::to_string(cmdTimeoutSec_) + " du -sb " +
                      cacheDir_ + " | awk '{printf $1}' ";
    SysUtils sysUtils;
    std::string result = sysUtils.RunSysCmd(cmd);
    if (result.empty()) {
        LOG_EVERY_N(WARNING, 100)
            << "get disk used size failed.";
        return;
    }
    uint64_t usedBytes = 0;
    if (!curve::common::StringToUll(result, &usedBytes)) {
        LOG_EVERY_N(WARNING, 100)
            << "get disk used size failed.";
        return;
    }
    usedBytes_.fetch_add(usedBytes);
    if (metric_.get() != nullptr)
        metric_->diskUsedBytes.set_value(usedBytes_);
    diskUsedInit_.store(true);
    VLOG(9) << "cache disk used size is: " << result;
    return;
}

bool DiskCacheManager::IsDiskCacheFull() {
    int64_t ratio = diskFsUsedRatio_.load();
    uint64_t usedBytes = GetDiskUsedbytes();
    if (ratio >= FLAGS_diskFullRatio ||
        usedBytes >= FLAGS_diskMaxUsableSpaceBytes ||
        IsExceedFileNums(kRatioLevel)) {
        VLOG(6) << "disk cache is full"
                << ", ratio is: " << ratio
                << ", fullRatio is: " << FLAGS_diskFullRatio
                << ", used bytes is: " << usedBytes;
        waitIntervalSec_.StopWait();
        return true;
    }
    if (!IsDiskCacheSafe(kRatioLevel)) {
        VLOG(6) << "wake up trim thread.";
        waitIntervalSec_.StopWait();
    }
    return false;
}

bool DiskCacheManager::IsDiskCacheSafe(uint32_t baseRatio) {
    if (IsExceedFileNums(baseRatio)) {
        return false;
    }
    int64_t ratio = diskFsUsedRatio_.load();
    uint64_t usedBytes = GetDiskUsedbytes();
    if ((usedBytes < (FLAGS_diskNearFullRatio * FLAGS_diskMaxUsableSpaceBytes /
                      kRatioLevel * baseRatio / kRatioLevel)) &&
        (ratio < FLAGS_diskNearFullRatio * baseRatio / kRatioLevel)) {
        VLOG(9) << "disk cache is safe"
                << ", usedBytes is: " << usedBytes
                << ", use ratio is: " << ratio
                << ", baseRatio is: " << baseRatio;
        return true;
    }
    VLOG_EVERY_N(6, 1000) << "disk cache is not safe"
            << ", usedBytes is: " << usedBytes << ", limit is "
            << FLAGS_diskNearFullRatio * FLAGS_diskMaxUsableSpaceBytes /
                   kRatioLevel * baseRatio / kRatioLevel
            << ", use ratio is: " << ratio << ", limit is: "
            << FLAGS_diskNearFullRatio * baseRatio / kRatioLevel;
    return false;
}

// TODO(wuhongsong):
// See Also: https://github.com/opencurve/curve/issues/1534
bool DiskCacheManager::IsExceedFileNums(uint32_t baseRatio) {
    uint64_t fileNums = cachedObjName_->Size();
    if (fileNums >= FLAGS_diskMaxFileNums * baseRatio / kRatioLevel) {
        VLOG_EVERY_N(9, 1000) << "disk cache file nums is exceed"
                << ", fileNums is: " << fileNums
                << ", maxFileNums is: " << FLAGS_diskMaxFileNums
                << ", baseRatio is: " << baseRatio;
        return true;
    }
    return false;
}


//
//       ok           nearfull               full
// |------------|-------------------|----------------------|
// 0     trimRatio*safeRatio    safeRatio               fullRatio
//
// 1. 0<=ok<trimRatio*safeRatio;
// 2. trimRatio*safeRatio<=nearfull<safeRatio
// 3. safeRatio<=full<=fullRatio
// If the status is ok or ok->nearfull does not clean up
// If the status is full or
// full->nearfull clean up
void DiskCacheManager::TrimCache() {
    const std::chrono::seconds sleepSec(FLAGS_diskTrimCheckIntervalSec);
    LOG(INFO) << "trim function start.";
    waitIntervalSec_.Init(FLAGS_diskTrimCheckIntervalSec * 1000);
    // trim will start after get the disk size
    while (!IsDiskUsedInited()) {
        if (!isRunning_) {
            return;
        }
        waitIntervalSec_.WaitForNextExcution();
    }
    // 1. check cache disk usage every sleepSec seconds.
    // 2. if cache disk is full,
    //    then remove disk file until cache disk is lower than
    //    FLAGS_diskNearFullRatio*FLAGS_trimRatio/kRatioLevel.
    std::string cacheReadFullDir, cacheWriteFullDir,
      cacheReadFile, cacheWriteFile, cacheKey;
    cacheReadFullDir = GetCacheReadFullDir();
    cacheWriteFullDir = GetCacheWriteFullDir();
    while (true) {
        UpdateDiskFsUsedRatio();
        waitIntervalSec_.WaitForNextExcution();
        if (!isRunning_) {
            LOG(INFO) << "trim thread end.";
            return;
        }
        VLOG(9) << "trim thread wake up.";
        InitQosParam();
        if (!IsDiskCacheSafe(kRatioLevel)) {
            while (!IsDiskCacheSafe(FLAGS_diskTrimRatio)) {
                UpdateDiskFsUsedRatio();
                if (!cachedObjName_->GetBack(&cacheKey)) {
                    VLOG_EVERY_N(9, 1000) << "obj is empty";
                    break;
                }

                VLOG(6) << "obj will be removed01: " << cacheKey;
                cacheReadFile = cacheReadFullDir + "/" +
                                curvefs::common::s3util::GenPathByObjName(
                                    cacheKey, objectPrefix_);
                cacheWriteFile = cacheWriteFullDir + "/" +
                                 curvefs::common::s3util::GenPathByObjName(
                                     cacheKey, objectPrefix_);
                struct stat statFile;
                int ret = 0;
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
                    usleep(1000);
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
                        << "remove disk file error, file is: " << cacheKey
                        << "error is: " << errno;
                    continue;
                }
                DecDiskUsedBytes(statReadFile.st_size);
                VLOG(6) << "remove disk file success, file is: " << cacheKey;
            }
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
    // this function move to here from init，
    // Otherwise, you can't get the original metric.
    // SetDiskInitUsedBytes may takes a long time,
    // so use a separate thread to do this.
    diskInitThread_ = curve::common::Thread(
      &DiskCacheManager::SetDiskInitUsedBytes, this);
}

}  // namespace client
}  // namespace curvefs
