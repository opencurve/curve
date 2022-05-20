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

CURVEFS_ERROR InodeCacheManagerImpl::GetInode(uint64_t inodeId,
    std::shared_ptr<InodeWrapper> &out) {
    NameLockGuard lock(nameLock_, std::to_string(inodeId));
    bool ok = iCache_->Get(inodeId, &out);
    if (ok) {
        // if enableCto, we need and is unopen, we need reload from metaserver
        if (curvefs::client::common::FLAGS_enableCto && !out->IsOpen()) {
            VLOG(6) << "InodeCacheManagerImpl, GetInode: enableCto and inode: "
                    << inodeId << " opencount is 0";
            iCache_->Remove(inodeId);
        } else {
            return CURVEFS_ERROR::OK;
        }
    }

    Inode inode;
    bool streaming;

    MetaStatusCode ret2 = metaClient_->GetInode(
        fsId_, inodeId, &inode, &streaming);

    if (ret2 != MetaStatusCode::OK) {
        LOG_IF(ERROR, ret2 != MetaStatusCode::NOT_FOUND)
            << "metaClient_ GetInode failed, MetaStatusCode = " << ret2
            << ", MetaStatusCode_Name = " << MetaStatusCode_Name(ret2)
            << ", inodeid = " << inodeId;
        return MetaStatusCodeToCurvefsErrCode(ret2);
    }

    out = std::make_shared<InodeWrapper>(
        std::move(inode), metaClient_);

    // NOTE: if the s3chunkinfo inside inode is too large,
    // we should invoke RefreshS3ChunkInfo() to receive s3chunkinfo
    // by streaming and padding its into inode.
    if (streaming) {
        CURVEFS_ERROR rc = out->RefreshS3ChunkInfo();
        if (rc != CURVEFS_ERROR::OK) {
            LOG(ERROR) << "RefreshS3ChunkInfo() failed, retCode = " << rc;
            return rc;
        }
    }

    std::shared_ptr<InodeWrapper> eliminatedOne;
    bool eliminated = iCache_->Put(inodeId, out, &eliminatedOne);
    if (eliminated) {
        VLOG(3) << "GetInode eliminate one inode, ino: "
                << eliminatedOne->GetInodeId();
        eliminatedOne->FlushAsync();

        // also delete from iAttrCache to be consistent with iCache
        Inode inode = eliminatedOne->GetInodeLocked();
        for (const auto &it : inode.parent()) {
            NameLockGuard lg(asyncNameLock_, std::to_string(it));
        }
        iAttrCache_->ReleaseOne(eliminatedOne->GetInodeId());
    }
    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR InodeCacheManagerImpl::GetInodeAttr(uint64_t inodeId,
    InodeAttr *out, uint64_t parentId) {
    NameLockGuard lock(nameLock_, std::to_string(inodeId));
    // 1. find in icache
    std::shared_ptr<InodeWrapper> inodeWrapper;
    bool ok = iCache_->Get(inodeId, &inodeWrapper);
    if (ok) {
        if (curvefs::client::common::FLAGS_enableCto &&
            !inodeWrapper->IsOpen()) {
            iCache_->Remove(inodeId);
        } else {
            inodeWrapper->GetInodeAttrLocked(out);
            VLOG(3) << "hit iCache";
            return CURVEFS_ERROR::OK;
        }
    }

    // 2. find in iAttrCache
    NameLockGuard lg(asyncNameLock_, std::to_string(parentId));
    if (iAttrCache_->Get(inodeId, out, parentId)) {
        if (curvefs::client::common::FLAGS_enableCto) {
            iAttrCache_->ReleaseOne(inodeId, parentId);
        } else {
            VLOG(3) << "hit iAttrCache";
            return CURVEFS_ERROR::OK;
        }
    }

    // 3. get form metaserver
    VLOG(3) << "not hit get form metaserver";
    std::set<uint64_t> inodeIds;
    std::list<InodeAttr> attrs;
    inodeIds.emplace(inodeId);
    MetaStatusCode ret = metaClient_->BatchGetInodeAttr(
        fsId_, inodeIds, &attrs);
    if (MetaStatusCode::OK != ret) {
        LOG(ERROR) << "metaClient BatchGetInodeAttr failed"
                   << ", inodeId = " << inodeId
                   << ", MetaStatusCode = " << ret
                   << ", MetaStatusCode_Name = " << MetaStatusCode_Name(ret);
        return MetaStatusCodeToCurvefsErrCode(ret);
    }

    if (attrs.size() !=  1) {
        LOG(ERROR) << "metaClient BatchGetInodeAttr error,"
                   << " getSize is 1, inodeId = " << inodeId
                   << "but real size = " << attrs.size();
        return CURVEFS_ERROR::INTERNAL;
    }
    *out = *attrs.begin();

    // // set back to cache
    // RepeatedPtrField<InodeAttr> tmpAttr;
    // *tmpAttr.Add() = *out;
    // iAttrCache_->Set(parentId, tmpAttr);
    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR InodeCacheManagerImpl::BatchGetInodeAttr(
    std::set<uint64_t> *inodeIds,
    std::list<InodeAttr> *attr) {
    // get some inode in icache
    for (auto iter = inodeIds->begin(); iter != inodeIds->end();) {
        std::shared_ptr<InodeWrapper> inodeWrapper;
        NameLockGuard lock(nameLock_, std::to_string(*iter));
        bool ok = iCache_->Get(*iter, &inodeWrapper);
        if (ok) {
            InodeAttr tmpAttr;
            inodeWrapper->GetInodeAttrLocked(&tmpAttr);
            attr->emplace_back(tmpAttr);
            iter = inodeIds->erase(iter);
        } else {
            ++iter;
        }
    }

    if (inodeIds->empty()) {
        return CURVEFS_ERROR::OK;
    }

    MetaStatusCode ret = metaClient_->BatchGetInodeAttr(fsId_, *inodeIds, attr);
    if (MetaStatusCode::OK != ret) {
        LOG(ERROR) << "metaClient BatchGetInodeAttr failed, MetaStatusCode = "
                   << ret << ", MetaStatusCode_Name = "
                   << MetaStatusCode_Name(ret);
    }
    return MetaStatusCodeToCurvefsErrCode(ret);
}

CURVEFS_ERROR InodeCacheManagerImpl::BatchGetInodeAttrAsync(
    uint64_t parentId,
    std::set<uint64_t> *inodeIds) {
    // check if this inode has already in iCache
    // for (const auto it : *inodeIds) {
    //     NameLockGuard lock(nameLock_, std::to_string(it));
    //     std::shared_ptr<InodeWrapper> out;
    //     bool ok = iCache_->Get(it, &out);
    //     if (ok) {
    //         inodeIds->erase(it);
    //     }
    // }

    if (inodeIds->empty()) {
        return CURVEFS_ERROR::OK;
    }

    // split inodeIds by partitionId and batch limit
    std::vector<std::list<uint64_t>> inodeGroups;
    if (!metaClient_->SplitRequestInodes(fsId_, *inodeIds, &inodeGroups)) {
        return CURVEFS_ERROR::NOTEXIST;
    }

    LockAsync(parentId, inodeGroups.size());
    for (const auto& it : inodeGroups) {
        VLOG(1) << "BatchGetInodeAttrAsync Send >>>>>>";
        auto* done = new BatchGetInodeAttrAsyncDone(shared_from_this(),
                                                    parentId);
        MetaStatusCode ret = metaClient_->BatchGetInodeAttrAsync(fsId_, it,
                                                                 done);
        if (MetaStatusCode::OK != ret) {
            LOG(ERROR) << "metaClient BatchGetInodeAsync failed,"
                       << " MetaStatusCode = " << ret
                       << ", MetaStatusCode_Name = "
                       << MetaStatusCode_Name(ret);
        }
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
            XAttr tmpXattr;
            inodeWrapper->GetXattrLocked(&tmpXattr);
            xattr->emplace_back(tmpXattr);
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

        // also delete from iAttrCache to be consistent with iCache
        Inode inode = eliminatedOne->GetInodeLocked();
        for (const auto &it : inode.parent()) {
            NameLockGuard lg(asyncNameLock_, std::to_string(it));
        }
        iAttrCache_->ReleaseOne(eliminatedOne->GetInodeId());
    }
    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR InodeCacheManagerImpl::DeleteInode(uint64_t inodeId) {
    NameLockGuard lock(nameLock_, std::to_string(inodeId));
    std::shared_ptr<InodeWrapper> out;
    if (iCache_->Get(inodeId, &out)) {
        iCache_->Remove(inodeId);
        // also delete from iAttrCache to be consistent with iCache
        Inode inode = out->GetInodeLocked();
        for (const auto &it : inode.parent()) {
            NameLockGuard lg(asyncNameLock_, std::to_string(it));
        }
        iAttrCache_->ReleaseOne(inodeId);
    }

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
        std::shared_ptr<InodeWrapper> out;
        if (iCache_->Get(inodeId, &out)) {
            iCache_->Remove(inodeId);
            // also delete from iAttrCache to be consistent with iCache
            Inode inode = out->GetInodeLocked();
            for (const auto &it : inode.parent()) {
                NameLockGuard lg(asyncNameLock_, std::to_string(it));
            }
            iAttrCache_->ReleaseOne(inodeId);
        }
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

void InodeCacheManagerImpl::LockAsync(uint64_t parent, uint32_t rpcTimes) {
    asyncNameLock_.Lock(std::to_string(parent));
    curve::common::LockGuard lg(asyncLockMapMutex_);
    if (rpcTimes > 0) {
        asyncLockMap_[parent] = std::atomic<uint32_t>(rpcTimes);
    } else {
        LOG(WARNING) << "LockAsync failed, parent = " << parent
                     << ", rpcTimes = " << rpcTimes;
        asyncNameLock_.Unlock(std::to_string(parent));
    }
}

void InodeCacheManagerImpl::UnlockAsync(uint64_t parent) {
    curve::common::LockGuard lg(asyncLockMapMutex_);
    auto iter = asyncLockMap_.find(parent);
    if (iter != asyncLockMap_.end()) {
        iter->second--;
        if (iter->second == 0) {
            asyncNameLock_.Unlock(std::to_string(parent));
            asyncLockMap_.erase(iter);
        }
    }
}

}  // namespace client
}  // namespace curvefs
