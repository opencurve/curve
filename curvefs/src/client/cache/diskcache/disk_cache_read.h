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
#ifndef CURVEFS_SRC_CLIENT_CACHE_DISKCACHE_DISK_CACHE_READ_H_
#define CURVEFS_SRC_CLIENT_CACHE_DISKCACHE_DISK_CACHE_READ_H_

#include <list>
#include <string>
#include <set>
#include <vector>
#include <memory>

#include "src/common/concurrent/concurrent.h"
#include "src/common/interruptible_sleeper.h"
#include "src/common/lru_cache.h"
#include "curvefs/src/common/wrap_posix.h"
#include "curvefs/src/client/cache/diskcache/disk_cache_base.h"

namespace curvefs {
namespace client {

using curve::common::SglLRUCache;
using curvefs::common::PosixWrapper;

class DiskCacheRead : public DiskCacheBase {
 public:
    DiskCacheRead() {}
    virtual ~DiskCacheRead() {}
    virtual void Init(std::shared_ptr<PosixWrapper> posixWrapper,
                      const std::string cacheDir, uint32_t objectPrefix);
    virtual int ReadDiskFile(const std::string name, char *buf, uint64_t offset,
                             uint64_t length);
    virtual int WriteDiskFile(const std::string fileName, const char *buf,
                              uint64_t length);
    virtual int LinkWriteToRead(const std::string fileName,
                                const std::string fullWriteDir,
                                const std::string fullReadDir);

    /**
     * @brief after reboot，load all files that store in read cache.
     */
    virtual int
    LoadAllCacheReadFile(std::shared_ptr<SglLRUCache<
      std::string>> cachedObj);
    virtual int ClearReadCache(const std::list<std::string> &files);
    virtual void InitMetrics(std::shared_ptr<DiskCacheMetric> metric) {
        metric_ = metric;
    }

 private:
    // file system operation encapsulation
    std::shared_ptr<PosixWrapper> posixWrapper_;
    std::shared_ptr<DiskCacheMetric> metric_;
};

}  // namespace client
}  // namespace curvefs
#endif  // CURVEFS_SRC_CLIENT_CACHE_DISKCACHE_DISK_CACHE_READ_H_
