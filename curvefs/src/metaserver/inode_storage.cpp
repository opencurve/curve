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

#include <string>
#include <memory>
#include <vector>
#include <algorithm>

#include "src/common/string_util.h"
#include "curvefs/src/metaserver/inode_storage.h"

namespace curvefs {
namespace metaserver {

using ::curve::common::SplitString;
using ::curve::common::StringToUl;
using ::curve::common::StringToUll;
using ::curvefs::metaserver::storage::KVStorage;
using ::curvefs::metaserver::storage::Status;

InodeStorage::InodeStorage(std::shared_ptr<KVStorage> kvStorage,
                           const std::string& tablename)
    : kvStorage_(kvStorage),
      tablename_(tablename) {}

inline std::string InodeStorage::Key2Str(const InodeKey& key) {
    std::ostringstream oss;
    oss << key.fsId << ":" << key.inodeId;
    return oss.str();
}

// NOTE: we can gurantee that key is valid
std::pair<uint32_t, uint64_t> InodeStorage::ExtractKey(const std::string& key) {
    uint32_t fsId;
    uint64_t inodeId;
    std::vector<std::string> items;
    SplitString(key, ":", &items);
    StringToUl(items[0], &fsId);
    StringToUll(items[1], &inodeId);
    return std::make_pair(fsId, inodeId);
}

inline bool InodeStorage::Inode2Str(const Inode& inode, std::string* value) {
    if (!inode.IsInitialized()) {
        return false;
    }
    return inode.SerializeToString(value);
}

inline bool InodeStorage::Str2Inode(const std::string& value, Inode* inode) {
    return inode->ParseFromString(value);
}

inline void InodeStorage::AddInodeId(const std::string& key) {
    counter_.insert(key);
}

inline void InodeStorage::DeleteInodeId(const std::string& key) {
    counter_.erase(key);
}

inline bool InodeStorage::InodeIdExist(const std::string& key) {
    return counter_.find(key) != counter_.end();
}

MetaStatusCode InodeStorage::Insert(const Inode& inode) {
    WriteLockGuard writeLockGuard(rwLock_);
    std::string key = Key2Str(InodeKey(inode.fsid(), inode.inodeid()));
    std::string value;
    if (InodeIdExist(key)) {
        return MetaStatusCode::INODE_EXIST;
    } else if (!Inode2Str(inode, &value)) {
        return MetaStatusCode::ENCODE_INODE_FAILED;
    }

    Status s = kvStorage_->HSet(tablename_, key, value);
    if (s.ok()) {
        AddInodeId(key);
        return MetaStatusCode::OK;
    }
    return MetaStatusCode::STORAGE_INTERNAL_ERROR;
}

MetaStatusCode InodeStorage::Get(const InodeKey& inodeKey, Inode* inode) {
    ReadLockGuard readLockGuard(rwLock_);
    std::string key = Key2Str(inodeKey);
    if (!InodeIdExist(key)) {
        return MetaStatusCode::NOT_FOUND;
    }

    std::string value;
    Status s = kvStorage_->HGet(tablename_, key, &value);
    if (!s.ok()) {
        return MetaStatusCode::STORAGE_INTERNAL_ERROR;
    } else if (!Str2Inode(value, inode)) {
        return MetaStatusCode::DECODE_INODE_FAILED;
    }
    return MetaStatusCode::OK;
}

MetaStatusCode InodeStorage::GetAttr(const InodeKey& inodeKey,
    InodeAttr *attr) {
    ReadLockGuard readLockGuard(rwLock_);
    std::string key = Key2Str(inodeKey);
    if (!InodeIdExist(key)) {
        return MetaStatusCode::NOT_FOUND;
    }

    Inode inode;
    std::string value;
    Status s = kvStorage_->HGet(tablename_, key, &value);
    if (!s.ok()) {
        return MetaStatusCode::STORAGE_INTERNAL_ERROR;
    } else if (!Str2Inode(value, &inode)) {
        return MetaStatusCode::DECODE_INODE_FAILED;
    }

    // get attr from inode
    attr->set_inodeid(inode.inodeid());
    attr->set_fsid(inode.fsid());
    attr->set_length(inode.length());
    attr->set_ctime(inode.ctime());
    attr->set_ctime_ns(inode.ctime_ns());
    attr->set_mtime(inode.mtime());
    attr->set_mtime_ns(inode.mtime_ns());
    attr->set_atime(inode.atime());
    attr->set_atime_ns(inode.atime_ns());
    attr->set_uid(inode.uid());
    attr->set_gid(inode.gid());
    attr->set_mode(inode.mode());
    attr->set_nlink(inode.nlink());
    attr->set_type(inode.type());
    if (inode.has_symlink()) {
        attr->set_symlink(inode.symlink());
    }
    if (inode.has_rdev()) {
        attr->set_rdev(inode.rdev());
    }
    if (inode.has_dtime()) {
        attr->set_dtime(inode.dtime());
    }
    if (inode.has_openmpcount()) {
        attr->set_openmpcount(inode.openmpcount());
    }
    return MetaStatusCode::OK;
}

MetaStatusCode InodeStorage::GetXAttr(const InodeKey& inodeKey, XAttr *xattr) {
    ReadLockGuard readLockGuard(rwLock_);
    std::string key = Key2Str(inodeKey);
    if (!InodeIdExist(key)) {
        return MetaStatusCode::NOT_FOUND;
    }

    Inode inode;
    std::string value;
    Status s = kvStorage_->HGet(tablename_, key, &value);
    if (!s.ok()) {
        return MetaStatusCode::STORAGE_INTERNAL_ERROR;
    } else if (!Str2Inode(value, &inode)) {
        return MetaStatusCode::DECODE_INODE_FAILED;
    }

    if (!inode.xattr().empty()) {
        *(xattr->mutable_xattrinfos()) = inode.xattr();
    }
    return MetaStatusCode::OK;
}

MetaStatusCode InodeStorage::Delete(const InodeKey& inodeKey) {
    WriteLockGuard writeLockGuard(rwLock_);
    std::string key = Key2Str(inodeKey);
    if (!InodeIdExist(key)) {
        return MetaStatusCode::NOT_FOUND;
    }

    Status s = kvStorage_->HDel(tablename_, key);
    if (s.ok()) {
        DeleteInodeId(key);
        return MetaStatusCode::OK;
    }
    return MetaStatusCode::STORAGE_INTERNAL_ERROR;
}

MetaStatusCode InodeStorage::Update(const Inode& inode) {
    WriteLockGuard writeLockGuard(rwLock_);
    std::string value;
    std::string key = Key2Str(InodeKey(inode.fsid(), inode.inodeid()));
    if (!InodeIdExist(key)) {
        return MetaStatusCode::NOT_FOUND;
    } else if (!Inode2Str(inode, &value)) {
        return MetaStatusCode::ENCODE_INODE_FAILED;
    }

    Status s = kvStorage_->HSet(tablename_, key, value);
    if (s.ok()) {
        return MetaStatusCode::OK;
    }
    return MetaStatusCode::STORAGE_INTERNAL_ERROR;
}

std::shared_ptr<Iterator> InodeStorage::GetAll() {
    return kvStorage_->HGetAll(tablename_);
}

MetaStatusCode InodeStorage::Clear() {
    ReadLockGuard w(rwLock_);
    Status s = kvStorage_->HClear(tablename_);
    if (s.ok()) {
        return MetaStatusCode::OK;
    }
    return MetaStatusCode::STORAGE_INTERNAL_ERROR;
}

size_t InodeStorage::Size() {
    return kvStorage_->HSize(tablename_);
}

void InodeStorage::GetInodeIdList(std::list<uint64_t>* inodeIdList) {
    ReadLockGuard readLockGuard(rwLock_);
    for (const auto& key : counter_) {
        auto pair = ExtractKey(key);
        inodeIdList->push_back(pair.second);
    }
}

}  // namespace metaserver
}  // namespace curvefs
