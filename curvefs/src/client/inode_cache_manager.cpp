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

#include <map>
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

    MetaStatusCode ret2 = metaClient_->GetInode(fsId_, inodeId, &inode);
    if (ret2 != MetaStatusCode::OK) {
        LOG_IF(ERROR, ret2 != MetaStatusCode::NOT_FOUND)
            << "metaClient_ GetInode failed, MetaStatusCode = " << ret2
            << ", MetaStatusCode_Name = " << MetaStatusCode_Name(ret2)
            << ", inodeid = " << inodeId;
        return MetaStatusCodeToCurvefsErrCode(ret2);
    }

    out = std::make_shared<InodeWrapper>(
        std::move(inode), metaClient_);

    // NOTE: now the s3chunkinfo in inode is empty for
    // we had store it with alone, so we should invoke
    // RefreshS3ChunkInfo() to padding inode's s3chunkinfo.
    CURVEFS_ERROR rc = out->RefreshS3ChunkInfo();
    if (rc != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "RefreshS3ChunkInfo() failed, retCode = " << rc;
        return rc;
    }

    std::shared_ptr<InodeWrapper> eliminatedOne;
    bool eliminated = iCache_->Put(inodeId, out, &eliminatedOne);
    if (eliminated) {
        eliminatedOne->FlushAsync();
    }
    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR InodeCacheManagerImpl::BatchGetInode(
    std::set<uint64_t> *inodeIds) {
    for (const auto it : *inodeIds) {
        NameLockGuard lock(nameLock_, std::to_string(it));
        std::shared_ptr<InodeWrapper> out;
        bool ok = iCache_->Get(it, &out);
        if (ok) {
            inodeIds->erase(it);
        }
    }

    std::list<Inode> inodes;
    MetaStatusCode ret = metaClient_->BatchGetInode(fsId_, inodeIds, &inodes);
    if (MetaStatusCode::OK != ret) {
        LOG(ERROR) << "metaClient BatchGetInode failed, MetaStatusCode = "
                   << ret << ", MetaStatusCode_Name = "
                   << MetaStatusCode_Name(ret);
    }

    for (const auto &it : inodes) {
        uint64_t inodeId = it.inodeid();
        auto out = std::make_shared<InodeWrapper>(std::move(it),
                                                  metaClient_);
        std::shared_ptr<InodeWrapper> eliminatedOne;
        std::shared_ptr<InodeWrapper> exist;
        bool eliminated = false;
        {
            NameLockGuard lock(nameLock_, std::to_string(inodeId));
            // the dirty inode may be deleted if already exist in cache
            // when put directly
            if (!iCache_->Get(inodeId, &exist)) {
                eliminated = iCache_->Put(inodeId, out, &eliminatedOne);
            }
        }
        if (eliminated) {
            eliminatedOne->FlushAsync();
        }
    }
    return MetaStatusCodeToCurvefsErrCode(ret);
}

CURVEFS_ERROR InodeCacheManagerImpl::BatchGetInodeAsync(
    std::set<uint64_t> *inodeIds) {
    for (const auto it : *inodeIds) {
        NameLockGuard lock(nameLock_, std::to_string(it));
        std::shared_ptr<InodeWrapper> out;
        bool ok = iCache_->Get(it, &out);
        if (ok) {
            inodeIds->erase(it);
        }
    }

    // split inodeIds by partitionId and batch limit
    std::vector<std::list<uint64_t>> inodeGroups;
    if (!metaClient_->SplitRequestInodes(fsId_, inodeIds, &inodeGroups)) {
        return CURVEFS_ERROR::NOTEXIST;
    }

    for (const auto& it : inodeGroups) {
        auto* done = new BatchGetInodeAsyncDone(shared_from_this());
        MetaStatusCode ret = metaClient_->BatchGetInodeAsync(fsId_, it,
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

    MetaStatusCode ret = metaClient_->BatchGetInodeAttr(fsId_, inodeIds, attr);
    if (MetaStatusCode::OK != ret) {
        LOG(ERROR) << "metaClient BatchGetInodeAttr failed, MetaStatusCode = "
                   << ret << ", MetaStatusCode_Name = "
                   << MetaStatusCode_Name(ret);
    }
    return MetaStatusCodeToCurvefsErrCode(ret);
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

    MetaStatusCode ret = metaClient_->BatchGetXAttr(fsId_, inodeIds, xattr);
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

void InodeCacheManagerImpl::AddInode(const Inode& inode) {
    auto inodeId = inode.inodeid();
    NameLockGuard lock(nameLock_, std::to_string(inodeId));
    // the dirty inode may be deleted if already exist in cache
    // when put directly
    std::shared_ptr<InodeWrapper> out;
    if (!iCache_->Get(inodeId, &out)) {
        out = std::make_shared<InodeWrapper>(inode, metaClient_);
        std::shared_ptr<InodeWrapper> eliminatedOne;
        bool eliminated = iCache_->Put(inodeId, out, &eliminatedOne);
        if (eliminated) {
            eliminatedOne->FlushAsync();
        }
    }
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

}  // namespace client
}  // namespace curvefs
