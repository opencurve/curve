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

namespace curvefs {
namespace client {

CURVEFS_ERROR InodeCacheManagerImpl::GetInode(uint64_t inodeid,
    std::shared_ptr<InodeWrapper> &out) {
    {
        curve::common::ReadLockGuard lg(mtx_);
        bool ok = iCache_->Get(inodeid, &out);
        if (ok) {
            return CURVEFS_ERROR::OK;
        }
    }

    curve::common::WriteLockGuard lg(mtx_);
    bool ok = iCache_->Get(inodeid, &out);
    if (ok) {
        return CURVEFS_ERROR::OK;
    }

    Inode inode;
    MetaStatusCode ret2 = metaClient_->GetInode(fsId_, inodeid, &inode);
    if (ret2 != MetaStatusCode::OK) {
        LOG(ERROR) << "metaClient_ GetInode failed, ret = " << ret2
                   << ", inodeid = " << inodeid;
        return MetaStatusCodeToCurvefsErrCode(ret2);
    }

    out = std::make_shared<InodeWrapper>(
        std::move(inode), metaClient_);

    std::shared_ptr<InodeWrapper> eliminatedOne;
    bool eliminated = iCache_->Put(inodeid, out, &eliminatedOne);
    if (eliminated) {
        CURVEFS_ERROR ret = eliminatedOne->Sync();
        if (ret != CURVEFS_ERROR::OK) {
            LOG(ERROR) << "sync inode failed, ret = " << ret;
            return ret;
        }
    }
    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR InodeCacheManagerImpl::CreateInode(
    const InodeParam &param,
    std::shared_ptr<InodeWrapper> &out) {
    curve::common::WriteLockGuard lg(mtx_);
    Inode inode;
    MetaStatusCode ret = metaClient_->CreateInode(param, &inode);
    if (ret != MetaStatusCode::OK) {
        LOG(ERROR) << "metaClient_ CreateInode failed, ret = " << ret;
        return MetaStatusCodeToCurvefsErrCode(ret);
    }
    uint64_t inodeid = inode.inodeid();
    out = std::make_shared<InodeWrapper>(
        std::move(inode), metaClient_);

    std::shared_ptr<InodeWrapper> eliminatedOne;
    bool eliminated = iCache_->Put(inodeid, out, &eliminatedOne);
    if (eliminated) {
        CURVEFS_ERROR ret = eliminatedOne->Sync();
        if (ret != CURVEFS_ERROR::OK) {
            LOG(ERROR) << "sync inode failed, ret = " << ret;
            return ret;
        }
    }
    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR InodeCacheManagerImpl::DeleteInode(uint64_t inodeid) {
    curve::common::WriteLockGuard lg(mtx_);
    iCache_->Remove(inodeid);
    MetaStatusCode ret = metaClient_->DeleteInode(fsId_, inodeid);
    if (ret != MetaStatusCode::OK && ret != MetaStatusCode::NOT_FOUND) {
        LOG(ERROR) << "metaClient_ DeleteInode failed, ret = " << ret
                   << ", inodeid = " << inodeid;
        return MetaStatusCodeToCurvefsErrCode(ret);
    }
    curve::common::LockGuard lg2(dirtyMapMutex_);
    dirtyMap_.erase(inodeid);
    return CURVEFS_ERROR::OK;
}

void InodeCacheManagerImpl::ClearInodeCache(uint64_t inodeid) {
    curve::common::WriteLockGuard lg(mtx_);
    iCache_->Remove(inodeid);
    curve::common::LockGuard lg2(dirtyMapMutex_);
    dirtyMap_.erase(inodeid);
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
    for (auto it = temp_.begin(); it != temp_.end();) {
        curve::common::UniqueLock ulk = it->second->GetUniqueLock();
        CURVEFS_ERROR ret = it->second->Sync();
        if (ret != CURVEFS_ERROR::OK && ret != CURVEFS_ERROR::NOTEXIST) {
            LOG(ERROR) << "Flush inode failed, inodeid = "
                       << it->second->GetInodeId();
            it++;
            continue;
        }
        it = temp_.erase(it);
    }
    LOG_IF(WARNING, temp_.size() > 0) << "FlushInodeOnce, remain inode num = "
        << temp_.size();
    {
        curve::common::LockGuard lg(dirtyMapMutex_);
        for (const auto &v : temp_) {
            dirtyMap_.emplace(v.first, v.second);
        }
    }
}


}  // namespace client
}  // namespace curvefs
