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

#include <string>
#include <vector>
#include <set>

#include "src/common/concurrent/concurrent.h"
#include "src/common/interruptible_sleeper.h"
#include "curvefs/src/common/wrap_posix.h"
#include "curvefs/src/client/s3/client_s3.h"
#include "curvefs/src/client/s3/disk_cache_write.h"
#include "curvefs/src/client/s3/disk_cache_read.h"
#include "curvefs/src/client/common/config.h"
namespace curvefs {
namespace client {

using curvefs::common::PosixWrapper;
using curvefs::client::common::S3ClientAdaptorOption;

class DiskCacheManager {
 public:
    DiskCacheManager(std::shared_ptr<PosixWrapper> posixWrapper,
                    std::shared_ptr<DiskCacheWrite> cacheWrite,
                    std::shared_ptr<DiskCacheRead> cacheRead);
    virtual ~DiskCacheManager() {TrimStop();}

    virtual int Init(S3Client *client, const S3ClientAdaptorOption option);

    virtual int UmountDiskCache();
    virtual bool IsCached(const std::string name);

    /**
     * @brief add obj to cachedObjName
     * @param[in] name obj name
     */
    void AddCache(const std::string name);

    int CreateDir();
    std::string GetCacheReadFullDir();
    std::string GetCacheWriteFullDir();

    int WriteDiskFile(const std::string fileName,
                     const char* buf, uint64_t length,
                     bool force = true);
    void AsyncUploadEnqueue(const std::string objName);
    virtual int WriteReadDirect(const std::string fileName,
                        const char* buf, uint64_t length);
    int ReadDiskFile(const std::string name,
                    char* buf, uint64_t offset, uint64_t length);
    int LinkWriteToRead(const std::string fileName,
                       const std::string fullWriteDir,
                       const std::string fullReadDir);
    /**
     * @brief get use ratio of cache disk
     * @return the use ratio
     */
    int64_t CacheDiskUsedRatio();
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

 private:
    /**
     * @brief trim cache func.
    */
    void TrimCache();

    curve::common::Thread backEndThread_;
    curve::common::Atomic<bool> isRunning_;
    curve::common::InterruptibleSleeper sleeper_;
    uint32_t trimCheckIntervalSec_;
    uint32_t fullRatio_;
    uint32_t safeRatio_;
    std::string cacheDir_;
    std::shared_ptr<DiskCacheWrite> cacheWrite_;
    std::shared_ptr<DiskCacheRead> cacheRead_;
    std::set<std::string> cachedObjName_;

    bthread::Mutex mtx_;

    S3Client *client_;

    std::shared_ptr<PosixWrapper> posixWrapper_;
};


}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_S3_DISK_CACHE_MANAGER_H_
