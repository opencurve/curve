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

#include "curvefs/src/client/rpcclient/task_excutor.h"
#include "src/common/lru_cache.h"

#include "curvefs/src/client/rpcclient/metaserver_client.h"
#include "curvefs/proto/metaserver.pb.h"
#include "curvefs/src/client/error_code.h"
#include "src/common/concurrent/concurrent.h"
#include "src/common/interruptible_sleeper.h"
#include "curvefs/src/client/inode_wrapper.h"
#include "src/common/concurrent/name_lock.h"
#include "curvefs/src/client/common/config.h"

using ::curve::common::LRUCache;
using ::curve::common::CacheMetrics;
using ::curvefs::metaserver::InodeAttr;
using ::curvefs::metaserver::XAttr;
using ::curve::common::Atomic;
using ::curve::common::InterruptibleSleeper;
using ::curve::common::Thread;

namespace curvefs {
namespace client {

using rpcclient::MetaServerClient;
using rpcclient::MetaServerClientImpl;
using rpcclient::InodeParam;
using rpcclient::MetaServerClientDone;
using rpcclient::BatchGetInodeAttrDone;
using curve::common::CountDownEvent;
using metric::S3ChunkInfoMetric;
using common::RefreshDataOption;

class InodeAttrCache {
 public:
    InodeAttrCache() {}
    ~InodeAttrCache() {}

    bool Get(uint64_t parentId, std::map<uint64_t, InodeAttr> *imap) {
        curve::common::LockGuard lg(iAttrCacheMutex_);
        auto iter = iAttrCache_.find(parentId);
        if (iter != iAttrCache_.end()) {
            *imap = iter->second;
            return true;
        }
        return false;
    }

    void Set(uint64_t parentId, const RepeatedPtrField<InodeAttr>& inodeAttrs) {
        curve::common::LockGuard lg(iAttrCacheMutex_);
        VLOG(1) << "parentId = " << parentId
                << ", iAttrCache set size = " << inodeAttrs.size();
        auto& inner = iAttrCache_[parentId];
        for (const auto &it : inodeAttrs) {
            inner.emplace(it.inodeid(), it);
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

    virtual CURVEFS_ERROR Init(uint64_t cacheSize, bool enableCacheMetrics,
                               uint32_t flushPeriodSec,
                               RefreshDataOption option) = 0;

    virtual void Run() = 0;

    virtual void Stop() = 0;

    virtual CURVEFS_ERROR GetInode(uint64_t inodeId,
        std::shared_ptr<InodeWrapper> &out) = 0;   // NOLINT

    virtual CURVEFS_ERROR GetInodeAttr(uint64_t inodeId, InodeAttr *out) = 0;

    virtual CURVEFS_ERROR BatchGetInodeAttr(
        std::set<uint64_t> *inodeIds,
        std::list<InodeAttr> *attrs) = 0;

    virtual CURVEFS_ERROR BatchGetInodeAttrAsync(
        uint64_t parentId,
        const std::set<uint64_t> &inodeIds,
        std::map<uint64_t, InodeAttr> *attrs) = 0;

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

 protected:
    uint32_t fsId_;
};

class InodeCacheManagerImpl : public InodeCacheManager,
    public std::enable_shared_from_this<InodeCacheManagerImpl> {
 public:
    InodeCacheManagerImpl()
      : metaClient_(std::make_shared<MetaServerClientImpl>()),
        iCache_(nullptr),
        iAttrCache_(nullptr),
        isStop_(true) {}

    explicit InodeCacheManagerImpl(
        const std::shared_ptr<MetaServerClient> &metaClient)
      : metaClient_(metaClient),
        iCache_(nullptr),
        iAttrCache_(nullptr) {}

    CURVEFS_ERROR Init(uint64_t cacheSize, bool enableCacheMetrics,
                       uint32_t flushPeriodSec,
                       RefreshDataOption option) override {
        if (enableCacheMetrics) {
            iCache_ = std::make_shared<
                LRUCache<uint64_t, std::shared_ptr<InodeWrapper>>>(0,
                    std::make_shared<CacheMetrics>("icache"));
        } else {
            iCache_ = std::make_shared<
                LRUCache<uint64_t, std::shared_ptr<InodeWrapper>>>(0);
        }
        maxCacheSize_ = cacheSize;
        option_ = option;
        flushPeriodSec_ = flushPeriodSec;
        iAttrCache_ = std::make_shared<InodeAttrCache>();
        s3ChunkInfoMetric_ = std::make_shared<S3ChunkInfoMetric>();
        return CURVEFS_ERROR::OK;
    }

    void Run() {
        isStop_.exchange(false);
        flushThread_ =
            Thread(&InodeCacheManagerImpl::FlushInodeBackground, this);
        LOG(INFO) << "Start inodeManager flush thread ok.";
    }

    void Stop() {
        isStop_.exchange(true);
        LOG(INFO) << "stop inodeManager flush thread ...";
        sleeper_.interrupt();
        flushThread_.join();
    }

    bool IsDirtyMapExist(uint64_t inodeId) {
        curve::common::LockGuard lg(dirtyMapMutex_);
        return dirtyMap_.count(inodeId) > 0;
    }

    CURVEFS_ERROR GetInode(uint64_t inodeId,
        std::shared_ptr<InodeWrapper> &out) override;

    CURVEFS_ERROR GetInodeAttr(uint64_t inodeId, InodeAttr *out) override;

    CURVEFS_ERROR BatchGetInodeAttr(std::set<uint64_t> *inodeIds,
        std::list<InodeAttr> *attrs) override;

    CURVEFS_ERROR BatchGetInodeAttrAsync(uint64_t parentId,
        const std::set<uint64_t> &inodeIds,
        std::map<uint64_t, InodeAttr> *attrs = nullptr) override;

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

    void RemoveICache(const std::shared_ptr<InodeWrapper> &inode);

 private:
    virtual void FlushInodeBackground();
    void TrimIcache(uint64_t trimSize);

 private:
    std::shared_ptr<MetaServerClient> metaClient_;
    std::shared_ptr<LRUCache<uint64_t, std::shared_ptr<InodeWrapper>>> iCache_;
    std::shared_ptr<S3ChunkInfoMetric> s3ChunkInfoMetric_;

    std::shared_ptr<InodeAttrCache> iAttrCache_;

    // dirty map, key is inodeid
    std::map<uint64_t, std::shared_ptr<InodeWrapper>> dirtyMap_;
    curve::common::Mutex dirtyMapMutex_;

    curve::common::GenericNameLock<Mutex> nameLock_;

    curve::common::GenericNameLock<Mutex> asyncNameLock_;

    uint64_t maxCacheSize_;
    RefreshDataOption option_;
    uint32_t flushPeriodSec_;
    Thread flushThread_;
    InterruptibleSleeper sleeper_;
    Atomic<bool> isStop_;
};

class BatchGetInodeAttrAsyncDone : public BatchGetInodeAttrDone {
 public:
    BatchGetInodeAttrAsyncDone(
        const std::shared_ptr<InodeCacheManager> &inodeCacheManager,
        std::shared_ptr<CountDownEvent> cond,
        uint64_t parentId):
        inodeCacheManager_(inodeCacheManager),
        cond_(cond),
        parentId_(parentId) {}
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
        cond_->Signal();
    };

 private:
    std::shared_ptr<InodeCacheManager> inodeCacheManager_;
    std::shared_ptr<CountDownEvent> cond_;
    uint64_t parentId_;
};

}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_INODE_CACHE_MANAGER_H_
