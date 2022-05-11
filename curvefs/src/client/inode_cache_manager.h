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

#include <cstdint>
#include <memory>
#include <unordered_map>
#include <map>
#include <set>
#include <list>
#include <vector>
#include <utility>

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

class InodeAttrCache {
 public:
    InodeAttrCache() {}
    ~InodeAttrCache() {}

    bool Get(uint64_t inodeId, InodeAttr *out, uint64_t parentId = 0) {
        VLOG(1) << "iAttrCache get parentId = " << parentId
                << ", inodeId = " << inodeId;
        curve::common::LockGuard lg(iAttrCacheMutex_);
        if (parentId != 0) {
            auto iter = iAttrCache_.find(parentId);
            if (iter != iAttrCache_.end()) {
                auto it = iter->second.find(inodeId);
                if (it != iter->second.end()) {
                    *out = it->second;
                    return true;
                }
            }
            return false;
        } else {
            for (const auto &it : iAttrCache_) {
                auto iter = it.second.find(inodeId);
                if (iter != it.second.end()) {
                    *out = iter->second;
                    return true;
                }
            }
        }
        return false;
    }

    void Set(uint64_t parentId, const RepeatedPtrField<InodeAttr>& inodeAttrs) {
        curve::common::LockGuard lg(iAttrCacheMutex_);
        VLOG(1) << "parentId = " << parentId
                << ", iAttrCache set size = " << inodeAttrs.size();
        auto iter = iAttrCache_.find(parentId);
        if (iter != iAttrCache_.end()) {
            for (const auto &it : inodeAttrs) {
                iter->second.emplace(it.inodeid(), it);
            }
        } else {
            std::map<uint64_t, InodeAttr> imap;
            for (const auto &it : inodeAttrs) {
                imap.emplace(it.inodeid(), it);
            }
            iAttrCache_.emplace(parentId, std::move(imap));
        }
    }

    void Release(uint64_t parentId) {
        curve::common::LockGuard lg(iAttrCacheMutex_);
        auto size = iAttrCache_.size();
        auto iter = iAttrCache_.find(parentId);
        if (iter != iAttrCache_.end()) {
            iAttrCache_.erase(iter);
        }
        VLOG(1) << "inodeId = " << parentId
                << ", inode attr cache release, before = "
                << size << ", after = " << iAttrCache_.size();
    }

    void ReleaseOne(uint64_t inodeId, uint64_t parentId = 0) {
        curve::common::LockGuard lg(iAttrCacheMutex_);
        if (parentId != 0) {
            auto iter = iAttrCache_.find(parentId);
            if (iter != iAttrCache_.end()) {
                auto it = iter->second.find(inodeId);
                if (it != iter->second.end()) {
                    iter->second.erase(it);
                }
            }
        } else {
            for (auto &it : iAttrCache_) {
                auto iter = it.second.find(inodeId);
                if (iter != it.second.end()) {
                    it.second.erase(iter);
                }
            }
        }
    }

 private:
    // inodeAttr cache; <parentId, <inodeId, inodeAttr>>
    std::map<uint64_t, std::map<uint64_t, InodeAttr>> iAttrCache_;
    curve::common::Mutex iAttrCacheMutex_;
};

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

    virtual CURVEFS_ERROR GetInodeAttr(uint64_t inodeId,
        InodeAttr *out, uint64_t parentId = 0) = 0;

    virtual CURVEFS_ERROR BatchGetInodeAttr(
        std::set<uint64_t> *inodeIds,
        std::list<InodeAttr> *attrs) = 0;

    virtual CURVEFS_ERROR BatchGetInodeAttrAsync(
        uint64_t parentId,
        std::set<uint64_t> *inodeIds) = 0;

    virtual CURVEFS_ERROR BatchGetXAttr(std::set<uint64_t> *inodeIds,
        std::list<XAttr> *xattrs) = 0;

    virtual CURVEFS_ERROR CreateInode(const InodeParam &param,
        std::shared_ptr<InodeWrapper> &out) = 0;   // NOLINT

    virtual CURVEFS_ERROR DeleteInode(uint64_t inodeId) = 0;

    virtual void AddInodeAttrs(uint64_t parentId,
        const RepeatedPtrField<InodeAttr>& inodeAttrs) = 0;

    virtual void ClearInodeCache(uint64_t inodeId) = 0;

    virtual void ShipToFlush(
        const std::shared_ptr<InodeWrapper> &inodeWrapper) = 0;

    virtual void FlushAll() = 0;

    virtual void FlushInodeOnce() = 0;

    virtual void ReleaseCache(uint64_t parentId) = 0;

    virtual void LockAsync(uint64_t parent, uint32_t rpcTimes) = 0;

    virtual void UnlockAsync(uint64_t parent) = 0;

 protected:
    uint32_t fsId_;
};

class InodeCacheManagerImpl : public InodeCacheManager,
    public std::enable_shared_from_this<InodeCacheManagerImpl> {
 public:
    InodeCacheManagerImpl()
      : metaClient_(std::make_shared<MetaServerClientImpl>()),
        iCache_(nullptr),
        iAttrCache_(nullptr) {}

    explicit InodeCacheManagerImpl(
        const std::shared_ptr<MetaServerClient> &metaClient)
      : metaClient_(metaClient),
        iCache_(nullptr),
        iAttrCache_(nullptr) {}

    CURVEFS_ERROR Init(uint64_t cacheSize, bool enableCacheMetrics) override {
        if (enableCacheMetrics) {
            iCache_ = std::make_shared<
                LRUCache<uint64_t, std::shared_ptr<InodeWrapper>>>(cacheSize,
                    std::make_shared<CacheMetrics>("icache"));
        } else {
            iCache_ = std::make_shared<
                LRUCache<uint64_t, std::shared_ptr<InodeWrapper>>>(cacheSize);
        }
        iAttrCache_ = std::make_shared<InodeAttrCache>();
        return CURVEFS_ERROR::OK;
    }

    CURVEFS_ERROR GetInode(uint64_t inodeId,
        std::shared_ptr<InodeWrapper> &out) override;

    CURVEFS_ERROR GetInodeAttr(uint64_t inodeId,
        InodeAttr *out, uint64_t parentId) override;

    CURVEFS_ERROR BatchGetInodeAttr(std::set<uint64_t> *inodeIds,
        std::list<InodeAttr> *attrs) override;

    CURVEFS_ERROR BatchGetInodeAttrAsync(uint64_t parentId,
        std::set<uint64_t> *inodeIds) override;

    CURVEFS_ERROR BatchGetXAttr(std::set<uint64_t> *inodeIds,
        std::list<XAttr> *xattrs) override;

    CURVEFS_ERROR CreateInode(const InodeParam &param,
        std::shared_ptr<InodeWrapper> &out) override;

    CURVEFS_ERROR DeleteInode(uint64_t inodeId) override;

    void AddInodeAttrs(uint64_t parentId,
        const RepeatedPtrField<InodeAttr>& inodeAttrs) override;

    void ClearInodeCache(uint64_t inodeId) override;

    void ShipToFlush(
        const std::shared_ptr<InodeWrapper> &inodeWrapper) override;

    void FlushAll() override;

    void FlushInodeOnce() override;

    void ReleaseCache(uint64_t parentId) override;

    void LockAsync(uint64_t parent, uint32_t rpcTimes) override;

    void UnlockAsync(uint64_t parent) override;

 private:
    std::shared_ptr<MetaServerClient> metaClient_;
    std::shared_ptr<LRUCache<uint64_t, std::shared_ptr<InodeWrapper>>> iCache_;

    std::shared_ptr<InodeAttrCache> iAttrCache_;

    // dirty map, key is inodeid
    std::map<uint64_t, std::shared_ptr<InodeWrapper>> dirtyMap_;
    curve::common::Mutex dirtyMapMutex_;

    curve::common::GenericNameLock<Mutex> nameLock_;

    // used for batchGetInodeAttr
    // asyncLockMap_: <parentId, rpcTimes>
    // rpcTimes is the number of request devided by partition and batchLimit,
    // if all the subRpcRequest back and will release the lock of the parent.
    curve::common::GenericNameLock<Mutex> asyncNameLock_;
    std::map<uint64_t, uint32_t> asyncLockMap_;
    curve::common::Mutex asyncLockMapMutex_;
};

class BatchGetInodeAttrAsyncDone : public MetaServerClientDone {
 public:
    BatchGetInodeAttrAsyncDone(
        const std::shared_ptr<InodeCacheManager> &inodeCacheManager,
        uint64_t parentId):
        inodeCacheManager_(inodeCacheManager), parentId_(parentId) {}
    ~BatchGetInodeAttrAsyncDone() {}

    void Run() override {
        std::unique_ptr<BatchGetInodeAttrAsyncDone> self_guard(this);
        MetaStatusCode ret = GetStatusCode();
        if (ret != MetaStatusCode::OK) {
            LOG(ERROR) << "BatchGetInodeAttrAsync failed, "
                       << "parentId = " << parentId_
                       << ", MetaStatusCode: " << ret
                       << ", MetaStatusCode_Name: " << MetaStatusCode_Name(ret);
        } else {
            auto inodeAttrs = GetInodeAttrs();
            VLOG(3) << "BatchGetInodeAttrAsyncDone update inodeAttrCache"
                    << " size = " << inodeAttrs.size();
            inodeCacheManager_->AddInodeAttrs(parentId_, inodeAttrs);
        }
        inodeCacheManager_->UnlockAsync(parentId_);
    };

 private:
    std::shared_ptr<InodeCacheManager> inodeCacheManager_;
    uint64_t parentId_;
};

}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_INODE_CACHE_MANAGER_H_
