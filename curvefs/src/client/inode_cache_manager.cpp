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

#include "curvefs/src/client/inode_cache_manager.h"

#include <glog/logging.h>
#include <cstdint>
#include <map>
#include <memory>
#include <utility>
#include "curvefs/proto/metaserver.pb.h"
#include "curvefs/src/client/error_code.h"

using ::curvefs::metaserver::Inode;
using ::curvefs::metaserver::MetaStatusCode_Name;

namespace curvefs {
namespace client {
namespace common {
DECLARE_bool(enableCto);
}  // namespace common
}  // namespace client
}  // namespace curvefs

namespace curvefs {
namespace client {

using NameLockGuard = ::curve::common::GenericNameLockGuard<Mutex>;

bool IsNotDirtyInode(const std::shared_ptr<InodeWrapper> &inode) {
    return !inode->IsDirty() && inode->S3ChunkInfoEmpty();
}

#define GET_INODE_REMOTE(FSID, INODEID, OUT, STREAMING)                        \
    MetaStatusCode ret = metaClient_->GetInode(FSID, INODEID, OUT, STREAMING); \
    if (ret != MetaStatusCode::OK) {                                           \
        LOG_IF(ERROR, ret != MetaStatusCode::NOT_FOUND)                        \
            << "metaClient_ GetInode failed, MetaStatusCode = " << ret         \
            << ", MetaStatusCode_Name = " << MetaStatusCode_Name(ret)          \
            << ", inodeid = " << INODEID;                                      \
        return MetaStatusCodeToCurvefsErrCode(ret);                            \
    }

#define PUT_INODE_CACHE(INODEID, INODEWRAPPER)                                 \
    std::shared_ptr<InodeWrapper> eliminatedOne;                               \
    bool eliminated = iCache_->Put(INODEID, INODEWRAPPER, &eliminatedOne);     \
    if (eliminated) {                                                          \
        VLOG(3) << "GetInode eliminate one inode, ino: "                       \
                << eliminatedOne->GetInodeId()                                 \
                << ", iCache does not evict inodes via put interface ";        \
        assert(0);                                                             \
    }

#define REFRESH_DATA_REMOTE(OUT, STREAMING)                                    \
    CURVEFS_ERROR rc = RefreshData(OUT, STREAMING);                            \
    if (rc != CURVEFS_ERROR::OK) {                                             \
        return rc;                                                             \
    }

CURVEFS_ERROR
InodeCacheManagerImpl::GetInode(uint64_t inodeId,
                                std::shared_ptr<InodeWrapper> &out) {
    NameLockGuard lock(nameLock_, std::to_string(inodeId));
    // get inode from cache
    bool ok = iCache_->Get(inodeId, &out);
    if (ok) {
        return CURVEFS_ERROR::OK;
    }

    // get inode from metaserver
    Inode inode;
    bool streaming = false;
    GET_INODE_REMOTE(fsId_, inodeId, &inode, &streaming);
    out = std::make_shared<InodeWrapper>(std::move(inode), metaClient_);

    // refresh data
    REFRESH_DATA_REMOTE(out, streaming);

    // put to cache
    PUT_INODE_CACHE(inodeId, out);

    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR
InodeCacheManagerImpl::RefreshInode(uint64_t inodeId) {
    NameLockGuard lock(nameLock_, std::to_string(inodeId));

    // get inode from metaserver
    Inode inode;
    bool streaming = false;
    GET_INODE_REMOTE(fsId_, inodeId, &inode, &streaming);

    // get inode from cache
    std::shared_ptr<InodeWrapper> out;
    bool ok = iCache_->Get(inodeId, &out);
    curve::common::UniqueLock lgGuard;
    if (!ok) {
        out = std::make_shared<InodeWrapper>(std::move(inode), metaClient_);
    } else {
        lgGuard = out->GetUniqueLock();
        streaming = true;
    }

    // refresh data
    REFRESH_DATA_REMOTE(out, streaming);

    // put to cache or refresh length
    if (!ok) {
        PUT_INODE_CACHE(inodeId, out);
    } else {
        out->SetLengthLocked(inode.length());
    }

    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR InodeCacheManagerImpl::GetInodeAttr(uint64_t inodeId,
                                                  InodeAttr *out) {
    NameLockGuard lock(nameLock_, std::to_string(inodeId));
    // 1. find in icache
    std::shared_ptr<InodeWrapper> inodeWrapper;
    bool ok = iCache_->Get(inodeId, &inodeWrapper);
    if (ok) {
        inodeWrapper->GetInodeAttr(out);
        return CURVEFS_ERROR::OK;
    }

    // 2. get form metaserver
    std::set<uint64_t> inodeIds;
    std::list<InodeAttr> attrs;
    inodeIds.emplace(inodeId);
    MetaStatusCode ret =
        metaClient_->BatchGetInodeAttr(fsId_, inodeIds, &attrs);
    if (MetaStatusCode::OK != ret) {
        LOG(ERROR) << "metaClient BatchGetInodeAttr failed"
                   << ", inodeId = " << inodeId << ", MetaStatusCode = " << ret
                   << ", MetaStatusCode_Name = " << MetaStatusCode_Name(ret);
        return MetaStatusCodeToCurvefsErrCode(ret);
    }

    if (attrs.size() != 1) {
        LOG(ERROR) << "metaClient BatchGetInodeAttr error,"
                   << " getSize is 1, inodeId = " << inodeId
                   << "but real size = " << attrs.size();
        return CURVEFS_ERROR::INTERNAL;
    }

    *out = *attrs.begin();
    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR InodeCacheManagerImpl::BatchGetInodeAttr(
    std::set<uint64_t> *inodeIds,
    std::list<InodeAttr> *attrs) {
    // get some inode attr in icache
    for (auto iter = inodeIds->begin(); iter != inodeIds->end();) {
        std::shared_ptr<InodeWrapper> inodeWrapper;
        NameLockGuard lock(nameLock_, std::to_string(*iter));
        bool ok = iCache_->Get(*iter, &inodeWrapper);
        if (ok) {
            InodeAttr tmpAttr;
            inodeWrapper->GetInodeAttr(&tmpAttr);
            attrs->emplace_back(tmpAttr);
            iter = inodeIds->erase(iter);
        } else {
            ++iter;
        }
    }

    if (inodeIds->empty()) {
        return CURVEFS_ERROR::OK;
    }

    MetaStatusCode ret = metaClient_->BatchGetInodeAttr(fsId_, *inodeIds,
        attrs);
    if (MetaStatusCode::OK != ret) {
        LOG(ERROR) << "metaClient BatchGetInodeAttr failed, MetaStatusCode = "
                   << ret << ", MetaStatusCode_Name = "
                   << MetaStatusCode_Name(ret);
    }
    return MetaStatusCodeToCurvefsErrCode(ret);
}

CURVEFS_ERROR InodeCacheManagerImpl::BatchGetInodeAttrAsync(
    uint64_t parentId,
    std::set<uint64_t> *inodeIds,
    std::map<uint64_t, InodeAttr> *attrs) {
    NameLockGuard lg(asyncNameLock_, std::to_string(parentId));
    std::map<uint64_t, InodeAttr> cachedAttr;
    bool cache  = iAttrCache_->Get(parentId, &cachedAttr);

    // get some inode attr in icache
    for (auto iter = inodeIds->begin(); iter != inodeIds->end();) {
        std::shared_ptr<InodeWrapper> inodeWrapper;
        NameLockGuard lock(nameLock_, std::to_string(*iter));
        bool ok = iCache_->Get(*iter, &inodeWrapper);
        if (ok) {
            InodeAttr tmpAttr;
            inodeWrapper->GetInodeAttr(&tmpAttr);
            attrs->emplace(*iter, tmpAttr);
            iter = inodeIds->erase(iter);
        } else if (cache && cachedAttr.find(*iter) != cachedAttr.end()) {
            attrs->emplace(*iter, cachedAttr[*iter]);
            iter = inodeIds->erase(iter);
        } else {
            ++iter;
        }
    }

    if (inodeIds->empty()) {
        return CURVEFS_ERROR::OK;
    }

    // split inodeIds by partitionId and batch limit
    std::vector<std::vector<uint64_t>> inodeGroups;
    if (!metaClient_->SplitRequestInodes(fsId_, *inodeIds, &inodeGroups)) {
        return CURVEFS_ERROR::NOTEXIST;
    }

    std::shared_ptr<CountDownEvent> cond =
        std::make_shared<CountDownEvent>(inodeGroups.size());
    for (const auto& it : inodeGroups) {
        VLOG(3) << "BatchGetInodeAttrAsync Send " << it.size();
        auto* done = new BatchGetInodeAttrAsyncDone(shared_from_this(),
                                                    cond, parentId);
        MetaStatusCode ret = metaClient_->BatchGetInodeAttrAsync(fsId_, it,
                                                                 done);
        if (MetaStatusCode::OK != ret) {
            LOG(ERROR) << "metaClient BatchGetInodeAsync failed,"
                       << " MetaStatusCode = " << ret
                       << ", MetaStatusCode_Name = "
                       << MetaStatusCode_Name(ret);
        }
    }

    // wait for all sudrequest finished
    cond->Wait();

    bool ok  = iAttrCache_->Get(parentId, attrs);
    if (!ok) {
        LOG(WARNING) << "get attrs form iAttrCache_ failed.";
    }
    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR InodeCacheManagerImpl::BatchGetXAttr(
    std::set<uint64_t> *inodeIds,
    std::list<XAttr> *xattr) {
    // get some inode in icache
    for (auto iter = inodeIds->begin(); iter != inodeIds->end();) {
        std::shared_ptr<InodeWrapper> inodeWrapper;
        NameLockGuard lock(nameLock_, std::to_string(*iter));
        bool ok = iCache_->Get(*iter, &inodeWrapper);
        if (ok) {
            xattr->emplace_back(inodeWrapper->GetXattr());
            iter = inodeIds->erase(iter);
        } else {
            ++iter;
        }
    }

    if (inodeIds->empty()) {
        return CURVEFS_ERROR::OK;
    }

    MetaStatusCode ret = metaClient_->BatchGetXAttr(fsId_, *inodeIds, xattr);
    if (MetaStatusCode::OK != ret) {
        LOG(ERROR) << "metaClient BatchGetXAttr failed, MetaStatusCode = "
                   << ret << ", MetaStatusCode_Name = "
                   << MetaStatusCode_Name(ret);
    }
    return MetaStatusCodeToCurvefsErrCode(ret);
}

CURVEFS_ERROR InodeCacheManagerImpl::CreateInode(
    const InodeParam &param,
    std::shared_ptr<InodeWrapper> &out) {
    Inode inode;
    MetaStatusCode ret = metaClient_->CreateInode(param, &inode);
    if (ret != MetaStatusCode::OK) {
        LOG(ERROR) << "metaClient_ CreateInode failed, MetaStatusCode = " << ret
                   << ", MetaStatusCode_Name = " << MetaStatusCode_Name(ret);
        return MetaStatusCodeToCurvefsErrCode(ret);
    }
    uint64_t inodeid = inode.inodeid();
    out = std::make_shared<InodeWrapper>(
        std::move(inode), metaClient_);

    std::shared_ptr<InodeWrapper> eliminatedOne;
    bool eliminated = false;
    {
        NameLockGuard lock(nameLock_, std::to_string(inodeid));
        eliminated = iCache_->Put(inodeid, out, &eliminatedOne);
    }
    if (eliminated) {
        eliminatedOne->FlushAsync();
    }
    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR InodeCacheManagerImpl::DeleteInode(uint64_t inodeId) {
    NameLockGuard lock(nameLock_, std::to_string(inodeId));
    iCache_->Remove(inodeId);
    MetaStatusCode ret = metaClient_->DeleteInode(fsId_, inodeId);
    if (ret != MetaStatusCode::OK && ret != MetaStatusCode::NOT_FOUND) {
        LOG(ERROR) << "metaClient_ DeleteInode failed, MetaStatusCode = " << ret
                   << ", MetaStatusCode_Name = " << MetaStatusCode_Name(ret)
                   << ", inodeId = " << inodeId;
        return MetaStatusCodeToCurvefsErrCode(ret);
    }

    curve::common::LockGuard lg2(dirtyMapMutex_);
    dirtyMap_.erase(inodeId);
    return CURVEFS_ERROR::OK;
}

void InodeCacheManagerImpl::AddInodeAttrs(
    uint64_t parentId, const RepeatedPtrField<InodeAttr>& inodeAttrs) {
    iAttrCache_->Set(parentId, inodeAttrs);
}

void InodeCacheManagerImpl::ClearInodeCache(uint64_t inodeId) {
    {
        NameLockGuard lock(nameLock_, std::to_string(inodeId));
        iCache_->Remove(inodeId);
    }
    curve::common::LockGuard lg2(dirtyMapMutex_);
    dirtyMap_.erase(inodeId);
}

void InodeCacheManagerImpl::ShipToFlush(
    const std::shared_ptr<InodeWrapper> &inodeWrapper) {
    curve::common::LockGuard lg(dirtyMapMutex_);
    dirtyMap_.emplace(inodeWrapper->GetInodeId(), inodeWrapper);
}

void InodeCacheManagerImpl::FlushAll() {
    while (!dirtyMap_.empty()) {
        FlushInodeOnce();
    }
}

void InodeCacheManagerImpl::FlushInodeOnce() {
    std::map<uint64_t, std::shared_ptr<InodeWrapper>> temp_;
    {
        curve::common::LockGuard lg(dirtyMapMutex_);
        temp_.swap(dirtyMap_);
    }
    for (auto it = temp_.begin(); it != temp_.end(); it++) {
        curve::common::UniqueLock ulk = it->second->GetUniqueLock();
        it->second->FlushAsync();
    }
}

void InodeCacheManagerImpl::ReleaseCache(uint64_t parentId) {
    NameLockGuard lg(asyncNameLock_, std::to_string(parentId));
    iAttrCache_->Release(parentId);
}

void InodeCacheManagerImpl::FlushInodeBackground() {
    LOG(INFO) << "flush thread is start.";
    while (!isStop_.load()) {
        FlushInodeOnce();
        sleeper_.wait_for(std::chrono::seconds(flushPeriodSec_));
        if (iCache_->Size() > maxCacheSize_) {
            TrimIcache(iCache_->Size() - maxCacheSize_);
        }
    }
    LOG(INFO) << "flush thread is stop.";
}

void InodeCacheManagerImpl::TrimIcache(uint64_t trimSize) {
    std::shared_ptr<InodeWrapper> inodeWrapper;
    uint64_t inodeId;
    while (trimSize > 0) {
        bool ok = iCache_->GetLast(&inodeId, &inodeWrapper, IsNotDirtyInode);
        if (ok) {
            NameLockGuard lock(nameLock_, std::to_string(inodeId));
            ::curve::common::UniqueLock lgGuard = inodeWrapper->GetUniqueLock();
            if (!inodeWrapper->IsDirty() &&
                inodeWrapper->S3ChunkInfoEmptyNolock()) {
                VLOG(9) << "TrimIcache remove inode " << inodeId
                        << " from iCache";
                iCache_->Remove(inodeId);
                trimSize--;
            }
            trimSize--;
            // remove the attr of the inode in iattrcache
            auto parents = inodeWrapper->GetParentLocked();
            for (uint64_t parent : parents) {
                iAttrCache_->Remove(parent, inodeId);
            }
        } else {
            VLOG(9) << "iCache size " << iCache_->Size() << " wait inode flush";
            break;
        }
    }
}

CURVEFS_ERROR
InodeCacheManagerImpl::RefreshData(std::shared_ptr<InodeWrapper> &inode,
                                   bool streaming) {
    auto type = inode->GetType();
    CURVEFS_ERROR rc = CURVEFS_ERROR::OK;

    switch (type) {
    case FsFileType::TYPE_S3:
        if (streaming) {
            // NOTE: if the s3chunkinfo inside inode is too large,
            // we should invoke RefreshS3ChunkInfo() to receive s3chunkinfo
            // by streaming and padding its into inode.
            rc = inode->RefreshS3ChunkInfo();
            LOG_IF(ERROR, rc != CURVEFS_ERROR::OK)
                << "RefreshS3ChunkInfo() failed, retCode = " << rc;
        }
        break;

    case FsFileType::TYPE_FILE:
        rc = inode->RefreshVolumeExtent();
        LOG_IF(ERROR, rc != CURVEFS_ERROR::OK)
            << "RefreshVolumeExtent failed, error: " << rc;
        break;

    default:
        rc = CURVEFS_ERROR::OK;
    }

    return rc;
}

}  // namespace client
}  // namespace curvefs
