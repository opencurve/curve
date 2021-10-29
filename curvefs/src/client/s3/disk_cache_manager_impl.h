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
#ifndef CURVEFS_SRC_CLIENT_S3_DISK_CACHE_MANAGER_IMPL_H_
#define CURVEFS_SRC_CLIENT_S3_DISK_CACHE_MANAGER_IMPL_H_

#include <bthread/mutex.h>

#include <string>
#include <vector>
#include <set>

#include "src/common/concurrent/concurrent.h"
#include "src/common/interruptible_sleeper.h"
#include "curvefs/src/common/wrap_posix.h"
#include "curvefs/src/client/common/common.h"
#include "curvefs/src/client/s3/client_s3.h"
#include "curvefs/src/client/s3/disk_cache_write.h"
#include "curvefs/src/client/s3/disk_cache_read.h"
#include "curvefs/src/client/s3/disk_cache_manager.h"

namespace curvefs {
namespace client {

using curvefs::common::PosixWrapper;

class S3ClientAdaptorOption;

struct DiskCacheOption {
    DiskCacheType diskCacheType;
    uint64_t trimCheckIntervalSec;
    uint64_t asyncLoadPeriodMs;
    uint64_t fullRatio;
    uint64_t safeRatio;
    std::string cacheDir;
    bool forceFlush;
};

class DiskCacheManagerImpl {
 public:
    DiskCacheManagerImpl(std::shared_ptr<DiskCacheManager>
      diskCacheManager, S3Client *client);
    virtual ~DiskCacheManagerImpl() {}
    /**
     * @brief init DiskCacheManagerImpl
     * @param[in] client S3Client
     * @param[in] option config option
     * @return success: 0, fail : < 0
     */
    int Init(const S3ClientAdaptorOption option);
    /**
     * @brief Write obj
     * @param[in] name obj name
     * @param[in] buf what to write
     * @param[in] length wtite length
     * @return success: write length, fail : < 0
     */
    int Write(const std::string name, const char* buf, uint64_t length);
    /**
     * @brief whether obj is cached in cached disk
     * @param[in] name obj name
     * @return cached: true, not cached : < 0
     */
    bool IsCached(const std::string name);
    /**
     * @brief read obj
     * @param[in] name obj name
     * @param[in] buf read buf
     * @param[in] offset offset in this object will start read
     * @param[in] length read length
     * @return success: length, fail : < length
     */
    int Read(const std::string name,
             char* buf, uint64_t offset, uint64_t length);
    /**
     * @brief umount disk cache
     * @return success: 0, fail : < 0
     */
    int UmountDiskCache();

    bool IsDiskCacheFull();
    int WriteReadDirect(const std::string fileName,
                        const char* buf, uint64_t length);

 private:
    int WriteDiskFile(const std::string name, const char* buf, uint64_t length);

    std::shared_ptr<DiskCacheManager> diskCacheManager_;
    bool forceFlush_;
    S3Client *client_;
};

}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_S3_DISK_CACHE_MANAGER_IMPL_H_
