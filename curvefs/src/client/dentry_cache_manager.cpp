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
#include "curvefs/src/client/dentry_cache_manager.h"

#include <string>
#include <list>
#include <vector>
#include <unordered_map>

namespace curvefs {
namespace client {

CURVEFS_ERROR DentryCacheManagerImpl::Init(const DCacheOption &option) {
    maxListDentryCount_ = option.maxListDentryCount;
    return CURVEFS_ERROR::OK;
}

void DentryCacheManagerImpl::InsertOrReplaceCache(const Dentry& dentry,
                                                  bool replace) {
    auto parentId = dentry.parentinodeid();
    auto name = dentry.name();
    auto iter = dCache_.emplace(parentId, Hash()).first;

    auto hash = &(iter->second);
    auto ret = hash->emplace(name, dentry);
    if (!ret.second && replace) {
       ret.first->second = dentry;
    }
}

void DentryCacheManagerImpl::DeleteCache(uint64_t parentId,
                                         const std::string& name) {
    auto iter = dCache_.find(parentId);
    if (iter == dCache_.end()) {
        return;
    }

    iter->second.erase(name);
    if (iter->second.empty()) {
        dCache_.erase(iter);
    }
}

CURVEFS_ERROR DentryCacheManagerImpl::GetTxId(uint32_t fsId,
                                              uint64_t inodeId,
                                              uint32_t* partitionId,
                                              uint64_t* txId) {
    auto rc = metaClient_->GetTxId(fsId, inodeId, partitionId, txId);
    if (rc != MetaStatusCode::OK) {
        LOG(ERROR) << "GetTxId failed, retCode = " << rc
                   << ", fsId = " << fsId << ", inodeId = " << inodeId;
    }
    return MetaStatusCodeToCurvefsErrCode(rc);
}

CURVEFS_ERROR DentryCacheManagerImpl::SetTxId(uint32_t partitionId,
                                              uint64_t txId) {
    auto rc = metaClient_->SetTxId(partitionId, txId);
    if (rc != MetaStatusCode::OK) {
        LOG(ERROR) << "SetTxId failed, retCode = " << rc;
    }
    return MetaStatusCodeToCurvefsErrCode(rc);
}

CURVEFS_ERROR DentryCacheManagerImpl::GetDentry(
    uint64_t parent, const std::string &name, Dentry *out) {
    auto it = dCache_.end();
    {
        curve::common::ReadLockGuard lg(mtx_);
        it = dCache_.find(parent);
        if (it != dCache_.end()) {
            auto ix = it->second.find(name);
            if (ix != it->second.end()) {
                *out = ix->second;
                return CURVEFS_ERROR::OK;
            }
        }
    }
    curve::common::WriteLockGuard lg(mtx_);
    MetaStatusCode ret = metaClient_->GetDentry(fsId_, parent, name, out);
    if (ret != MetaStatusCode::OK) {
        LOG(ERROR) << "metaClient_ GetDentry failed, ret = " << ret
                   << ", parent = " << parent
                   << ", name = " << name;
        return MetaStatusCodeToCurvefsErrCode(ret);
    }
    if (it == dCache_.end()) {
        it = dCache_.emplace(parent,
            std::unordered_map<std::string, Dentry>()).first;
    }
    it->second.emplace(name, *out);
    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR DentryCacheManagerImpl::CreateDentry(const Dentry &dentry) {
    curve::common::WriteLockGuard lg(mtx_);
    uint64_t parent = dentry.parentinodeid();
    std::string name = dentry.name();
    MetaStatusCode ret = metaClient_->CreateDentry(dentry);
    if (ret != MetaStatusCode::OK) {
        LOG(ERROR) << "metaClient_ CreateDentry failed, ret = " << ret
                   << ", parent = " << parent
                   << ", name = " << name;
        return MetaStatusCodeToCurvefsErrCode(ret);
    }
    InsertOrReplaceCache(dentry, false);
    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR DentryCacheManagerImpl::DeleteDentry(
    uint64_t parent, const std::string &name) {
    curve::common::WriteLockGuard lg(mtx_);
    DeleteCache(parent, name);
    MetaStatusCode ret = metaClient_->DeleteDentry(fsId_, parent, name);
    if (ret != MetaStatusCode::OK) {
        LOG(ERROR) << "metaClient_ DeleteInode failed, ret = " << ret
                   << ", parent = " << parent
                   << ", name = " << name;
        return MetaStatusCodeToCurvefsErrCode(ret);
    }
    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR DentryCacheManagerImpl::ListDentry(
    uint64_t parent, std::list<Dentry> *dentryList, uint32_t limit) {
    bool perceed = true;
    MetaStatusCode ret = MetaStatusCode::OK;
    dentryList->clear();
    std::string last = "";
    limit = (limit == 0) ? maxListDentryCount_ : limit;
    do {
        std::list<Dentry> part;
        ret = metaClient_->ListDentry(fsId_, parent, last, limit, &part);
        LOG(INFO) << "ListDentry fsId = " << fsId_
                  << ", parent = " << parent
                  << ", last = " << last
                  << ", count = " << limit
                  << ", ret = " << ret
                  << ", part.size() = " << part.size();
        if (ret != MetaStatusCode::OK) {
            if (MetaStatusCode::NOT_FOUND == ret) {
                return CURVEFS_ERROR::OK;
            }
            LOG(ERROR) << "metaClient_ ListDentry failed, ret = " << ret
                       << ", parent = " << parent
                       << ", last = " << last
                       << ", count = " << limit;
            return MetaStatusCodeToCurvefsErrCode(ret);
        }
        if (part.size() < limit) {
            perceed = false;
        }
        if (!part.empty()) {
            last = part.back().name();
            dentryList->splice(dentryList->end(), part);
        }
    } while (perceed);
    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR DentryCacheManagerImpl::Rename(const Dentry& srcDentry,
                                             const Dentry& dstDentry) {
    std::vector<Dentry> dentrys{ srcDentry, dstDentry };
    auto rc = metaClient_->PrepareRenameTx(dentrys);
    if (rc != MetaStatusCode::OK) {
        LOG(ERROR) << "Rename failed, retCode = " << rc;
    }

    return MetaStatusCodeToCurvefsErrCode(rc);
}

}  // namespace client
}  // namespace curvefs
