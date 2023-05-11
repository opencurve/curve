/*
 *  Copyright (c) 2023 NetEase Inc.
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
 * Project: Curve
 * Created Date: 2023-03-31
 * Author: Jingli Chen (Wine93)
 */

#ifndef CURVEFS_SRC_CLIENT_FILESYSTEM_LOOKUP_CACHE_H_
#define CURVEFS_SRC_CLIENT_FILESYSTEM_LOOKUP_CACHE_H_

#include "src/common/lru_cache.h"
#include "curvefs/src/client/common/config.h"
#include "curvefs/src/client/filesystem/meta.h"

namespace curvefs {
namespace client {
namespace filesystem {

using ::curve::common::LRUCache;
using ::curve::common::RWLock;
using ::curve::common::ReadLockGuard;
using ::curve::common::WriteLockGuard;
using ::curvefs::client::common::LookupCacheOption;

// memory cache for lookup result, now we only support cache negative result,
// other positive entry will be cached in kernel.
class LookupCache {
 public:
    struct CacheEntry {
        uint32_t uses;
        TimeSpec expireTime;
    };

    using LRUType = LRUCache<std::string, CacheEntry>;

 public:
    explicit LookupCache(LookupCacheOption option);

    bool Get(Ino parent, const std::string& name);

    bool Put(Ino parent, const std::string& name);

    bool Delete(Ino parent, const std::string& name);

 private:
    std::string CacheKey(Ino parent, const std::string& name);

 private:
    bool enable_;
    RWLock rwlock_;
    LookupCacheOption option_;
    std::shared_ptr<LRUType> lru_;
};

}  // namespace filesystem
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_FILESYSTEM_LOOKUP_CACHE_H_
