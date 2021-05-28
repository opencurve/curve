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

#include "curvefs/src/client/metaserver_client.h"

namespace curvefs {
namespace client {

CURVEFS_ERROR MetaServerClientImpl::GetDentry(uint32_t fsId, uint64_t inodeid,
              const std::string &name, Dentry *out) {
    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR MetaServerClientImpl::ListDentry(uint32_t fsId, uint64_t inodeid,
        const std::string &last, uint32_t count) {
    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR MetaServerClientImpl::UpdateDentry(const Dentry &dentry) {
    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR MetaServerClientImpl::CreateDentry(const Dentry &dentry) {
    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR MetaServerClientImpl::DeleteDentry(
    uint32_t fsId, uint64_t inodeid, const std::string &name) {
    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR MetaServerClientImpl::GetInode(
    uint32_t fsId, uint64_t inodeid, Inode *out) {
    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR MetaServerClientImpl::UpdateInode(const Inode &inode) {
    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR MetaServerClientImpl::CreateInode(
    const InodeParam &param, Inode *out) {
    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR MetaServerClientImpl::DeleteInode(
    uint32_t fsId, uint64_t inodeid) {
    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR MetaServerClientImpl::AllocExtents(uint32_t fsId,
    const std::list<ExtentAllocInfo> &toAllocExtents,
    std::list<Extent> *allocatedExtents) {
    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR MetaServerClientImpl::DeAllocExtents(uint32_t fsId,
    std::list<Extent> allocatedExtents) {
    return CURVEFS_ERROR::OK;
}

}  // namespace client
}  // namespace curvefs
