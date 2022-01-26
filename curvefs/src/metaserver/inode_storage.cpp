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
 * Created Date: 2021-05-19
 * Author: chenwei
 */

#include "curvefs/src/metaserver/inode_storage.h"

#include <algorithm>
#include <vector>

namespace curvefs {
namespace metaserver {
MetaStatusCode MemoryInodeStorage::Insert(const Inode &inode) {
    WriteLockGuard writeLockGuard(rwLock_);
    std::shared_ptr<Inode> newInode = std::make_shared<Inode>(inode);
    auto it = inodeMap_.emplace(InodeKey(inode), newInode);
    if (it.second == false) {
        return MetaStatusCode::INODE_EXIST;
    }
    return MetaStatusCode::OK;
}

MetaStatusCode MemoryInodeStorage::Get(
    const InodeKey &key, std::shared_ptr<Inode> *inode) {
    ReadLockGuard readLockGuard(rwLock_);
    auto it = inodeMap_.find(key);
    if (it == inodeMap_.end()) {
        return MetaStatusCode::NOT_FOUND;
    }
    *inode = it->second;
    return MetaStatusCode::OK;
}

MetaStatusCode MemoryInodeStorage::GetCopy(const InodeKey &key, Inode *inode) {
    ReadLockGuard readLockGuard(rwLock_);
    auto it = inodeMap_.find(key);
    if (it == inodeMap_.end()) {
        return MetaStatusCode::NOT_FOUND;
    }
    *inode = *(it->second);
    return MetaStatusCode::OK;
}

MetaStatusCode MemoryInodeStorage::GetAttr(const InodeKey &key,
    InodeAttr *attr) {
    ReadLockGuard readLockGuard(rwLock_);
    auto it = inodeMap_.find(key);
    if (it == inodeMap_.end()) {
        return MetaStatusCode::NOT_FOUND;
    }

    // get attr from inode
    attr->set_inodeid(it->second->inodeid());
    attr->set_fsid(it->second->fsid());
    attr->set_length(it->second->length());
    attr->set_ctime(it->second->ctime());
    attr->set_ctime_ns(it->second->ctime_ns());
    attr->set_mtime(it->second->mtime());
    attr->set_mtime_ns(it->second->mtime_ns());
    attr->set_atime(it->second->atime());
    attr->set_atime_ns(it->second->atime_ns());
    attr->set_uid(it->second->uid());
    attr->set_gid(it->second->gid());
    attr->set_mode(it->second->mode());
    attr->set_nlink(it->second->nlink());
    attr->set_type(it->second->type());
    if (it->second->has_symlink()) {
        attr->set_symlink(it->second->symlink());
    }
    if (it->second->has_rdev()) {
        attr->set_rdev(it->second->rdev());
    }
    if (it->second->has_dtime()) {
        attr->set_dtime(it->second->dtime());
    }
    if (it->second->has_openmpcount()) {
        attr->set_openmpcount(it->second->openmpcount());
    }
    return MetaStatusCode::OK;
}

MetaStatusCode MemoryInodeStorage::GetXAttr(const InodeKey &key, XAttr *xattr) {
    ReadLockGuard readLockGuard(rwLock_);
    auto it = inodeMap_.find(key);
    if (it == inodeMap_.end()) {
        return MetaStatusCode::NOT_FOUND;
    }

    if (!it->second->xattr().empty()) {
        *(xattr->mutable_xattrinfos()) = it->second->xattr();
    }
    return MetaStatusCode::OK;
}

MetaStatusCode MemoryInodeStorage::Delete(const InodeKey &key) {
    WriteLockGuard writeLockGuard(rwLock_);
    auto it = inodeMap_.find(key);
    if (it != inodeMap_.end()) {
        inodeMap_.erase(it);
        return MetaStatusCode::OK;
    }
    return MetaStatusCode::NOT_FOUND;
}

MetaStatusCode MemoryInodeStorage::Update(const Inode &inode) {
    WriteLockGuard writeLockGuard(rwLock_);
    auto it = inodeMap_.find(InodeKey(inode));
    if (it == inodeMap_.end()) {
        return MetaStatusCode::NOT_FOUND;
    }
    *(it->second) = inode;
    return MetaStatusCode::OK;
}

int MemoryInodeStorage::Count() {
    ReadLockGuard readLockGuard(rwLock_);
    return inodeMap_.size();
}

InodeStorage::ContainerType* MemoryInodeStorage::GetContainer() {
    return &inodeMap_;
}

void MemoryInodeStorage::GetInodeIdList(std::list<uint64_t>* inodeIdList) {
    ReadLockGuard readLockGuard(rwLock_);
    for (auto it = inodeMap_.begin(); it != inodeMap_.end(); ++it) {
        inodeIdList->push_back(it->second->inodeid());
    }
}

}  // namespace metaserver
}  // namespace curvefs
