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

#ifndef CURVEFS_SRC_CLIENT_INODE_CACHE_MANAGER_H_
#define CURVEFS_SRC_CLIENT_INODE_CACHE_MANAGER_H_

#include <memory>
#include <unordered_map>
#include <map>
#include <set>
#include <list>
#include <vector>

#include "src/common/lru_cache.h"

#include "curvefs/src/client/rpcclient/metaserver_client.h"
#include "curvefs/proto/metaserver.pb.h"
#include "curvefs/src/client/error_code.h"
#include "src/common/concurrent/concurrent.h"
#include "curvefs/src/client/inode_wrapper.h"
#include "src/common/concurrent/name_lock.h"

using ::curve::common::LRUCache;
using ::curve::common::CacheMetrics;
using ::curvefs::metaserver::InodeAttr;
using ::curvefs::metaserver::XAttr;

namespace curvefs {
namespace client {

using rpcclient::MetaServerClient;
using rpcclient::MetaServerClientImpl;
using rpcclient::InodeParam;
using rpcclient::MetaServerClientDone;

class InodeCacheManager {
 public:
    InodeCacheManager()
      : fsId_(0) {}
    virtual ~InodeCacheManager() {}

    void SetFsId(uint32_t fsId) {
        fsId_ = fsId;
    }

    virtual CURVEFS_ERROR Init(uint64_t cacheSize, bool enableCacheMetrics) = 0;

    virtual CURVEFS_ERROR GetInode(uint64_t inodeId,
        std::shared_ptr<InodeWrapper> &out) = 0;   // NOLINT

    virtual CURVEFS_ERROR BatchGetInodeAsync(std::set<uint64_t> *inodeIds) = 0;

    virtual CURVEFS_ERROR BatchGetInodeAttr(
        std::set<uint64_t> *inodeIds,
        std::list<InodeAttr> *attrs) = 0;

    virtual CURVEFS_ERROR BatchGetXAttr(std::set<uint64_t> *inodeIds,
        std::list<XAttr> *xattrs) = 0;

    virtual CURVEFS_ERROR CreateInode(const InodeParam &param,
        std::shared_ptr<InodeWrapper> &out) = 0;   // NOLINT

    virtual CURVEFS_ERROR DeleteInode(uint64_t inodeId) = 0;

    virtual void AddInode(const Inode& inode) = 0;

    virtual void ClearInodeCache(uint64_t inodeId) = 0;

    virtual void ShipToFlush(
        const std::shared_ptr<InodeWrapper> &inodeWrapper) = 0;

    virtual void FlushAll() = 0;

    virtual void FlushInodeOnce() = 0;

 protected:
    uint32_t fsId_;
};

class InodeCacheManagerImpl : public InodeCacheManager,
    public std::enable_shared_from_this<InodeCacheManagerImpl> {
 public:
    InodeCacheManagerImpl()
      : metaClient_(std::make_shared<MetaServerClientImpl>()),
        iCache_(nullptr) {}

    explicit InodeCacheManagerImpl(
        const std::shared_ptr<MetaServerClient> &metaClient)
      : metaClient_(metaClient),
        iCache_(nullptr) {}

    CURVEFS_ERROR Init(uint64_t cacheSize, bool enableCacheMetrics) override {
        if (enableCacheMetrics) {
            iCache_ = std::make_shared<
                LRUCache<uint64_t, std::shared_ptr<InodeWrapper>>>(cacheSize,
                    std::make_shared<CacheMetrics>("icache"));
        } else {
            iCache_ = std::make_shared<
                LRUCache<uint64_t, std::shared_ptr<InodeWrapper>>>(cacheSize);
        }
        return CURVEFS_ERROR::OK;
    }

    CURVEFS_ERROR GetInode(uint64_t inodeId,
        std::shared_ptr<InodeWrapper> &out) override;

    CURVEFS_ERROR BatchGetInodeAsync(std::set<uint64_t> *inodeIds) override;

    CURVEFS_ERROR BatchGetInodeAttr(std::set<uint64_t> *inodeIds,
        std::list<InodeAttr> *attrs) override;

    CURVEFS_ERROR BatchGetXAttr(std::set<uint64_t> *inodeIds,
        std::list<XAttr> *xattrs) override;

    CURVEFS_ERROR CreateInode(const InodeParam &param,
        std::shared_ptr<InodeWrapper> &out) override;

    CURVEFS_ERROR DeleteInode(uint64_t inodeId) override;

    void AddInode(const Inode& inode) override;

    void ClearInodeCache(uint64_t inodeId) override;

    void ShipToFlush(
        const std::shared_ptr<InodeWrapper> &inodeWrapper) override;

    void FlushAll() override;

    void FlushInodeOnce() override;

 private:
    std::shared_ptr<MetaServerClient> metaClient_;
    std::shared_ptr<LRUCache<uint64_t, std::shared_ptr<InodeWrapper>>> iCache_;

    // dirty map, key is inodeid
    std::map<uint64_t, std::shared_ptr<InodeWrapper>> dirtyMap_;
    curve::common::Mutex dirtyMapMutex_;

    curve::common::GenericNameLock<Mutex> nameLock_;
};

class BatchGetInodeAsyncDone : public MetaServerClientDone {
 public:
    BatchGetInodeAsyncDone(
        const std::shared_ptr<InodeCacheManager> &inodeCacheManager):
        inodeCacheManager_(inodeCacheManager) {}
    ~BatchGetInodeAsyncDone() {}

    void Run() override {
        std::unique_ptr<BatchGetInodeAsyncDone> self_guard(this);
        MetaStatusCode ret = GetStatusCode();
        if (ret != MetaStatusCode::OK) {
            LOG(ERROR) << "BatchGetInodeAsync failed, "
                       << "MetaStatusCode: " << ret
                       << ", MetaStatusCode_Name: " << MetaStatusCode_Name(ret);
        } else {
            auto inodes = GetInodes();
            VLOG(6) << "update inodeCache size = " << inodes.size();
            for (const auto &it : inodes) {
                inodeCacheManager_ -> AddInode(it);
            }
        }
    };

 private:
    std::shared_ptr<InodeCacheManager> inodeCacheManager_;
};

}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_INODE_CACHE_MANAGER_H_
