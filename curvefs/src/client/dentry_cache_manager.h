/*
 *  Copyright (c) 2021 NetEase Inc.
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
 * Created Date: Thur May 27 2021
 * Author: xuchaojie
 */

#ifndef CURVEFS_SRC_CLIENT_DENTRY_CACHE_MANAGER_H_
#define CURVEFS_SRC_CLIENT_DENTRY_CACHE_MANAGER_H_

#include <memory>
#include <string>
#include <list>
#include <unordered_map>

#include "curvefs/src/client/rpcclient/metaserver_client.h"
#include "curvefs/src/client/error_code.h"
#include "src/common/concurrent/concurrent.h"
#include "src/common/lru_cache.h"
#include "src/common/concurrent/name_lock.h"

using ::curvefs::metaserver::Dentry;
using ::curve::common::LRUCache;
using ::curve::common::CacheMetrics;

namespace curvefs {
namespace client {

using rpcclient::MetaServerClient;
using rpcclient::MetaServerClientImpl;

class DentryCacheManager {
 public:
    DentryCacheManager() : fsId_(0) {}
    virtual ~DentryCacheManager() {}

    void SetFsId(uint32_t fsId) {
        fsId_ = fsId;
    }

    virtual CURVEFS_ERROR Init(uint64_t cacheSize, bool enableCacheMetrics) = 0;

    virtual void InsertOrReplaceCache(const Dentry& dentry) = 0;

    virtual void DeleteCache(uint64_t parentId, const std::string& name) = 0;

    virtual CURVEFS_ERROR GetDentry(uint64_t parent,
        const std::string &name, Dentry *out) = 0;

    virtual CURVEFS_ERROR CreateDentry(const Dentry &dentry) = 0;

    virtual CURVEFS_ERROR DeleteDentry(uint64_t parent,
        const std::string &name) = 0;

    virtual CURVEFS_ERROR ListDentry(uint64_t parent,
        std::list<Dentry> *dentryList, uint32_t limit,
        bool onlyDir = false) = 0;

 protected:
    uint32_t fsId_;
};

static const char* kDentryKeyDelimiter = ":";

class DentryCacheManagerImpl : public DentryCacheManager {
 public:
    DentryCacheManagerImpl()
      : metaClient_(std::make_shared<MetaServerClientImpl>()),
        dCache_(nullptr) {}

    explicit DentryCacheManagerImpl(
        const std::shared_ptr<MetaServerClient> &metaClient)
      : metaClient_(metaClient),
        dCache_(nullptr) {}

    CURVEFS_ERROR Init(uint64_t cacheSize, bool enableCacheMetrics) override {
        if (enableCacheMetrics) {
            dCache_ = std::make_shared<
                LRUCache<std::string, Dentry>>(cacheSize,
                    std::make_shared<CacheMetrics>("dcache"));
        } else {
            dCache_ = std::make_shared<
                LRUCache<std::string, Dentry>>(cacheSize);
        }
        return CURVEFS_ERROR::OK;
    }

    void InsertOrReplaceCache(const Dentry& dentry) override;

    void DeleteCache(uint64_t parentId, const std::string& name) override;

    CURVEFS_ERROR GetDentry(uint64_t parent,
        const std::string &name, Dentry *out) override;

    CURVEFS_ERROR CreateDentry(const Dentry &dentry) override;

    CURVEFS_ERROR DeleteDentry(uint64_t parent,
        const std::string &name) override;

    CURVEFS_ERROR ListDentry(uint64_t parent,
        std::list<Dentry> *dentryList, uint32_t limit,
        bool dirOnly = false) override;

    std::string GetDentryCacheKey(uint64_t parent, const std::string &name) {
        return std::to_string(parent) + kDentryKeyDelimiter + name;
    }

 private:
    std::shared_ptr<MetaServerClient> metaClient_;
    // key is parentId + name
    std::shared_ptr<LRUCache<std::string, Dentry>> dCache_;
    curve::common::GenericNameLock<Mutex> nameLock_;
};

}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_DENTRY_CACHE_MANAGER_H_
