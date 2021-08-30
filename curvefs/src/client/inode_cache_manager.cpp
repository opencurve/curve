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

using ::curvefs::metaserver::Inode;

namespace curvefs {
namespace client {

CURVEFS_ERROR InodeCacheManagerImpl::GetInode(uint64_t inodeid,
    std::shared_ptr<InodeWapper> &out) {
    CURVEFS_ERROR ret = CURVEFS_ERROR::OK;
    {
        curve::common::ReadLockGuard lg(mtx_);
        auto it = iCache_.find(inodeid);
        if (it != iCache_.end()) {
            out = it->second;
            return CURVEFS_ERROR::OK;
        }
    }

    curve::common::WriteLockGuard lg(mtx_);
    Inode inode;
    MetaStatusCode ret2 = metaClient_->GetInode(fsId_, inodeid, &inode);
    if (ret2 != MetaStatusCode::OK) {
        LOG(ERROR) << "metaClient_ GetInode failed, ret = " << ret2
                   << ", inodeid = " << inodeid;
        return MetaStatusCodeToCurvefsErrCode(ret2);
    }
    out = std::make_shared<InodeWapper>(
        std::move(inode), metaClient_);
    iCache_.emplace(inodeid, out);
    return ret;
}

CURVEFS_ERROR InodeCacheManagerImpl::CreateInode(
    const InodeParam &param,
    std::shared_ptr<InodeWapper> &out) {
    curve::common::WriteLockGuard lg(mtx_);
    Inode inode;
    MetaStatusCode ret = metaClient_->CreateInode(param, &inode);
    if (ret != MetaStatusCode::OK) {
        LOG(ERROR) << "metaClient_ CreateInode failed, ret = " << ret;
        return MetaStatusCodeToCurvefsErrCode(ret);
    }
    uint64_t inodeid = inode.inodeid();
    out = std::make_shared<InodeWapper>(
        std::move(inode), metaClient_);
    iCache_.emplace(inodeid, out);
    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR InodeCacheManagerImpl::DeleteInode(uint64_t inodeid) {
    curve::common::WriteLockGuard lg(mtx_);
    iCache_.erase(inodeid);
    MetaStatusCode ret = metaClient_->DeleteInode(fsId_, inodeid);
    if (ret != MetaStatusCode::OK) {
        LOG(ERROR) << "metaClient_ DeleteInode failed, ret = " << ret
                   << ", inodeid = " << inodeid;
        return MetaStatusCodeToCurvefsErrCode(ret);
    }
    return CURVEFS_ERROR::OK;
}


}  // namespace client
}  // namespace curvefs
