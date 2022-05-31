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

    virtual int Init(S3Client *client, const S3ClientAdaptorOption option);

    virtual int UmountDiskCache();
    virtual bool IsCached(const std::string name);

    /**
     * @brief add obj to cachedObjName
     * @param[in] name obj name
     * @param[in] cacheWriteExist whether the obj is
     *                            exist in cache write
     */
    void AddCache(const std::string name,
      bool cacheWriteExist = true);

    int CreateDir();
    std::string GetCacheReadFullDir();
    std::string GetCacheWriteFullDir();

    int WriteDiskFile(const std::string fileName, const char *buf,
                      uint64_t length, bool force = true);
    void AsyncUploadEnqueue(const std::string objName);
    virtual int WriteReadDirect(const std::string fileName, const char *buf,
                                uint64_t length);
    int ReadDiskFile(const std::string name, char *buf, uint64_t offset,
                     uint64_t length);
    int LinkWriteToRead(const std::string fileName,
                        const std::string fullWriteDir,
                        const std::string fullReadDir);
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

 private:
    /**
     * @brief add the used bytes of disk cache.
     */
    void AddDiskUsedBytes(uint64_t length) {
        usedBytes_.fetch_add(length, std::memory_order_seq_cst);
        VLOG(9) << "add disk used size is: " << length
                << ", now is: " << usedBytes_.load(std::memory_order_seq_cst);
        return;
    }
    /**
     * @brief dec the used bytes of disk cache.
     * can not dec disk used bytes after file have been loaded,
     * because there are link in read cache
     */
    void DecDiskUsedBytes(uint64_t length) {
        int64_t usedBytes;
        usedBytes = usedBytes_.fetch_sub(length, std::memory_order_seq_cst);
        assert(usedBytes >= 0);
        (void)usedBytes;
        VLOG(9) << "dec disk used size is: " << length
                << ", now is: " << usedBytes_.load(std::memory_order_seq_cst);
        return;
    }
    void SetDiskInitUsedBytes();
    uint64_t GetDiskUsedbytes() {
        return usedBytes_.load(std::memory_order_seq_cst);
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

    std::shared_ptr<LRUCache<std::string, bool>> cachedObjName_;

    S3Client *client_;
    std::shared_ptr<PosixWrapper> posixWrapper_;
    std::shared_ptr<DiskCacheMetric> metric_;

    Throttle diskCacheThrottle_;

    S3ClientAdaptorOption option_;
};


}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_S3_DISK_CACHE_MANAGER_H_
