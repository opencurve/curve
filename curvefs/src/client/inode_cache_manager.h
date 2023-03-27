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
#include "curvefs/src/client/filesystem/error.h"
#include "src/common/concurrent/concurrent.h"
#include "src/common/interruptible_sleeper.h"
#include "curvefs/src/client/inode_wrapper.h"
#include "src/common/concurrent/name_lock.h"
#include "curvefs/src/client/common/config.h"
#include "curvefs/src/client/filesystem/openfile.h"
#include "curvefs/src/client/filesystem/defer_sync.h"

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
using ::curvefs::client::filesystem::OpenFiles;
using ::curvefs::client::filesystem::DeferSync;

class InodeCacheManager {
 public:
    InodeCacheManager()
      : fsId_(0) {}
    virtual ~InodeCacheManager() {}

    void SetFsId(uint32_t fsId) {
        fsId_ = fsId;
    }

    virtual CURVEFS_ERROR Init(RefreshDataOption option,
                               std::shared_ptr<OpenFiles> openFiles,
                               std::shared_ptr<DeferSync> deferSync) = 0;

    virtual CURVEFS_ERROR
    GetInode(uint64_t inodeId,
             std::shared_ptr<InodeWrapper> &out) = 0;  // NOLINT

    virtual CURVEFS_ERROR GetInodeAttr(uint64_t inodeId, InodeAttr *out) = 0;

    virtual CURVEFS_ERROR BatchGetInodeAttr(
        std::set<uint64_t> *inodeIds,
        std::list<InodeAttr> *attrs) = 0;

    virtual CURVEFS_ERROR BatchGetInodeAttrAsync(
        uint64_t parentId,
        std::set<uint64_t> *inodeIds,
        std::map<uint64_t, InodeAttr> *attrs) = 0;

    virtual CURVEFS_ERROR BatchGetXAttr(std::set<uint64_t> *inodeIds,
        std::list<XAttr> *xattrs) = 0;

    virtual CURVEFS_ERROR CreateInode(const InodeParam &param,
        std::shared_ptr<InodeWrapper> &out) = 0;   // NOLINT

    virtual CURVEFS_ERROR CreateManageInode(const InodeParam &param,
        std::shared_ptr<InodeWrapper> &out) = 0;   // NOLINT

    virtual CURVEFS_ERROR DeleteInode(uint64_t inodeId) = 0;

    virtual void ShipToFlush(
        const std::shared_ptr<InodeWrapper> &inodeWrapper) = 0;

 protected:
    uint32_t fsId_;
};

class InodeCacheManagerImpl : public InodeCacheManager,
    public std::enable_shared_from_this<InodeCacheManagerImpl> {

 public:
    InodeCacheManagerImpl()
      : metaClient_(std::make_shared<MetaServerClientImpl>()) {}

    explicit InodeCacheManagerImpl(
        const std::shared_ptr<MetaServerClient> &metaClient)
      : metaClient_(metaClient) {}

    CURVEFS_ERROR Init(RefreshDataOption option,
                       std::shared_ptr<OpenFiles> openFiles,
                       std::shared_ptr<DeferSync> deferSync) override {
        option_ = option;
        s3ChunkInfoMetric_ = std::make_shared<S3ChunkInfoMetric>();
        openFiles_ =  openFiles;
        deferSync_ = deferSync;
        return CURVEFS_ERROR::OK;
    }

    CURVEFS_ERROR GetInode(uint64_t inodeId,
                           std::shared_ptr<InodeWrapper> &out) override;

    CURVEFS_ERROR GetInodeAttr(uint64_t inodeId, InodeAttr *out) override;

    CURVEFS_ERROR BatchGetInodeAttr(std::set<uint64_t> *inodeIds,
        std::list<InodeAttr> *attrs) override;

    CURVEFS_ERROR BatchGetInodeAttrAsync(uint64_t parentId,
        std::set<uint64_t> *inodeIds,
        std::map<uint64_t, InodeAttr> *attrs = nullptr) override;

    CURVEFS_ERROR BatchGetXAttr(std::set<uint64_t> *inodeIds,
        std::list<XAttr> *xattrs) override;

    CURVEFS_ERROR CreateInode(const InodeParam &param,
        std::shared_ptr<InodeWrapper> &out) override;

    CURVEFS_ERROR CreateManageInode(const InodeParam &param,
        std::shared_ptr<InodeWrapper> &out) override;

    CURVEFS_ERROR DeleteInode(uint64_t inodeId) override;

    void ShipToFlush(
        const std::shared_ptr<InodeWrapper> &inodeWrapper) override;

 private:
    CURVEFS_ERROR RefreshData(std::shared_ptr<InodeWrapper> &inode,  // NOLINT
                              bool streaming = true);

 private:
    std::shared_ptr<MetaServerClient> metaClient_;
    std::shared_ptr<S3ChunkInfoMetric> s3ChunkInfoMetric_;

    std::shared_ptr<OpenFiles> openFiles_;

    std::shared_ptr<DeferSync> deferSync_;

    curve::common::GenericNameLock<Mutex> nameLock_;

    curve::common::GenericNameLock<Mutex> asyncNameLock_;

    RefreshDataOption option_;
};

class BatchGetInodeAttrAsyncDone : public BatchGetInodeAttrDone {
 public:
    BatchGetInodeAttrAsyncDone(std::map<uint64_t, InodeAttr>* attrs,
                               ::curve::common::Mutex* mutex,
                               std::shared_ptr<CountDownEvent> cond):
        attrs_(attrs),
        mutex_(mutex),
        cond_(cond) {}

    ~BatchGetInodeAttrAsyncDone() {}

    void Run() override {
        std::unique_ptr<BatchGetInodeAttrAsyncDone> self_guard(this);
        MetaStatusCode ret = GetStatusCode();
        if (ret != MetaStatusCode::OK) {
            LOG(ERROR) << "BatchGetInodeAttrAsync failed, "
                       << ", MetaStatusCode: " << ret
                       << ", MetaStatusCode_Name: " << MetaStatusCode_Name(ret);
        } else {
            auto inodeAttrs = GetInodeAttrs();
            VLOG(3) << "BatchGetInodeAttrAsyncDone update inodeAttrCache"
                    << " size = " << inodeAttrs.size();

            curve::common::LockGuard lk(*mutex_);
            for (const auto& attr : inodeAttrs) {
                attrs_->emplace(attr.inodeid(), attr);
            }
        }
        cond_->Signal();
    };

 private:
    ::curve::common::Mutex* mutex_;
    std::map<uint64_t, InodeAttr>* attrs_;
    std::shared_ptr<CountDownEvent> cond_;
};

}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_INODE_CACHE_MANAGER_H_
