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
 * Author: wuhongsong
 */
#ifndef CURVEFS_SRC_CLIENT_S3_DISK_CACHE_MANAGER_H_
#define CURVEFS_SRC_CLIENT_S3_DISK_CACHE_MANAGER_H_

#include <bthread/mutex.h>

#include <list>
#include <string>
#include <vector>
#include <set>
#include <memory>

#include "absl/strings/string_view.h"
#include "src/common/concurrent/concurrent.h"
#include "src/common/interruptible_sleeper.h"
#include "src/common/lru_cache.h"
#include "src/common/throttle.h"
#include "src/common/wait_interval.h"
#include "curvefs/src/common/wrap_posix.h"
#include "curvefs/src/common/utils.h"
#include "curvefs/src/client/s3/client_s3.h"
#include "curvefs/src/client/s3/disk_cache_write.h"
#include "curvefs/src/client/s3/disk_cache_read.h"
#include "curvefs/src/client/common/config.h"
namespace curvefs {
namespace client {

using ::curve::common::LRUCache;
using curve::common::ReadWriteThrottleParams;
using ::curve::common::SglLRUCache;
using curve::common::Throttle;
using curve::common::ThrottleParams;
using curvefs::client::common::S3ClientAdaptorOption;
using curvefs::common::PosixWrapper;
using curvefs::common::SysUtils;

class DiskCacheManager {
 public:
    DiskCacheManager(std::shared_ptr<PosixWrapper> posixWrapper,
                     std::shared_ptr<DiskCacheWrite> cacheWrite,
                     std::shared_ptr<DiskCacheRead> cacheRead);
    DiskCacheManager() {}
    virtual ~DiskCacheManager() { TrimStop(); }

    virtual int Init(std::shared_ptr<S3Client> client,
        const S3ClientAdaptorOption option);

    virtual int UmountDiskCache();
    virtual bool IsCached(const std::string& name);

    /**
     * @brief add obj to cachedObjName
     * @param[in] name obj name
     * @param[in] cacheWriteExist whether the obj is
     *                            exist in cache write
     */
    void AddCache(absl::string_view name,
      bool cacheWriteExist = true);

    int CreateDir();
    std::string GetCacheReadFullDir();
    std::string GetCacheWriteFullDir();

    int WriteDiskFile(absl::string_view fileName, const char *buf,
                      uint64_t length, bool force = true);
    void AsyncUploadEnqueue(absl::string_view objName);
    virtual int WriteReadDirect(absl::string_view fileName, const char *buf,
                                uint64_t length);
    int ReadDiskFile(absl::string_view name, char *buf, uint64_t offset,
                     uint64_t length);
    int LinkWriteToRead(absl::string_view fileName,
                        absl::string_view fullWriteDir,
                        absl::string_view fullReadDir);
    int UploadAllCacheWriteFile();
    int UploadWriteCacheByInode(const std::string &inode);
    int ClearReadCache(const std::list<std::string> &files);
    /**
     * @brief get use ratio of cache disk
     * @return the use ratio
     */
    int64_t SetDiskFsUsedRatio();
    virtual bool IsDiskCacheFull();
    bool IsDiskCacheSafe();
    /**
     * @brief: start trim thread.
     */
    int TrimRun();
    /**
     * @brief: stop trim thread.
     */
    int TrimStop();

    void InitMetrics(const std::string &fsName);

    /**
     * @brief: has geted the origin used size or not.
     */
    virtual bool IsDiskUsedInited() {
        return diskUsedInit_.load();
    }

 private:
    /**
     * @brief add the used bytes of disk cache.
     */
    void AddDiskUsedBytes(uint64_t length) {
        usedBytes_.fetch_add(length);
        if (metric_.get() != nullptr)
            metric_->diskUsedBytes.set_value(usedBytes_/1024/1024);
        VLOG(9) << "add disk used size is: " << length
                << ", now is: " << usedBytes_.load();
        return;
    }
    /**
     * @brief dec the used bytes of disk cache.
     * can not dec disk used bytes after file have been loaded,
     * because there are link in read cache
     */
    void DecDiskUsedBytes(uint64_t length) {
        usedBytes_.fetch_sub(length);
        assert(usedBytes_ >= 0);
        if (metric_.get() != nullptr)
            metric_->diskUsedBytes.set_value(usedBytes_);
        VLOG(9) << "dec disk used size is: " << length
                << ", now is: " << usedBytes_.load();
        return;
    }
    void SetDiskInitUsedBytes();
    uint64_t GetDiskUsedbytes() {
        return usedBytes_.load();
    }

    void InitQosParam();
    /**
     * @brief trim cache func.
     */
    void TrimCache();

    /**
     * @brief whether the cache file is exceed maxFileNums_.
     */
    bool IsExceedFileNums();

    curve::common::Thread backEndThread_;
    curve::common::Atomic<bool> isRunning_;
    curve::common::InterruptibleSleeper sleeper_;
    curve::common::WaitInterval waitIntervalSec_;
    uint32_t trimCheckIntervalSec_;
    uint32_t fullRatio_;
    uint32_t safeRatio_;
    uint64_t maxUsableSpaceBytes_;
    uint64_t maxFileNums_;
    // used bytes of disk cache
    std::atomic<int64_t> usedBytes_;
    // used ratio of the file system in disk cache
    std::atomic<int32_t> diskFsUsedRatio_;
    uint32_t cmdTimeoutSec_;
    std::string cacheDir_;
    std::shared_ptr<DiskCacheWrite> cacheWrite_;
    std::shared_ptr<DiskCacheRead> cacheRead_;

    std::shared_ptr<SglLRUCache<std::string>> cachedObjName_;

    std::shared_ptr<S3Client> client_;
    std::shared_ptr<PosixWrapper> posixWrapper_;
    std::shared_ptr<DiskCacheMetric> metric_;

    Throttle diskCacheThrottle_;

    S3ClientAdaptorOption option_;

    // has geted the origin used size or not
    std::atomic<bool> diskUsedInit_;
    curve::common::Thread diskInitThread_;
};


}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_S3_DISK_CACHE_MANAGER_H_
