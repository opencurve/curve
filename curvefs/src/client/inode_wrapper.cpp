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

#include "curvefs/src/client/inode_wrapper.h"
#include "curvefs/src/client/rpcclient/metaserver_client.h"

namespace curvefs {
namespace client {

using rpcclient::MetaServerClient;
using rpcclient::MetaServerClientImpl;

CURVEFS_ERROR InodeWapper::Sync() {
    if (dirty_) {
        MetaStatusCode ret = metaClient_->UpdateInode(inode_);
        if (ret != MetaStatusCode::OK) {
            LOG(ERROR) << "metaClient_ UpdateInode failed, ret = " << ret
                << ", inodeid = " << inode_.inodeid();
            return MetaStatusCodeToCurvefsErrCode(ret);
        }
        dirty_ = false;
    }
    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR InodeWapper::Link() {
    uint32_t old = inode_.nlink();
    inode_.set_nlink(old + 1);
    MetaStatusCode ret = metaClient_->UpdateInode(inode_);
    if (ret != MetaStatusCode::OK) {
        inode_.set_nlink(old);
        LOG(ERROR) << "metaClient_ UpdateInode failed, ret = " << ret
            << ", inodeid = " << inode_.inodeid();
        return MetaStatusCodeToCurvefsErrCode(ret);
    }
    dirty_ = false;
    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR InodeWapper::UnLink() {
    uint32_t old = inode_.nlink();
    if (old > 0) {
        uint32_t newnlink = old - 1;
        if (newnlink == 1 && inode_.type() == FsFileType::TYPE_DIRECTORY) {
            newnlink--;
        }
        inode_.set_nlink(newnlink);
        MetaStatusCode ret = metaClient_->UpdateInode(inode_);
        if (ret != MetaStatusCode::OK) {
            LOG(ERROR) << "metaClient_ UpdateInode failed, ret = " << ret
                << ", inodeid = " << inode_.inodeid();
            return MetaStatusCodeToCurvefsErrCode(ret);
        }
        dirty_ = false;
        return CURVEFS_ERROR::OK;
    }
    LOG(ERROR) << "Unlink find nlink <= 0, nlink = " << old;
    dirty_ = false;
    return CURVEFS_ERROR::INTERNAL;
}



}  // namespace client
}  // namespace curvefs

