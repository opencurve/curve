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

#include <vector>
#include <algorithm>

namespace curvefs {
namespace metaserver {

using ::curvefs::metaserver::storage::KVStorage;
using ::curve::common::SplitString;
using ::curve::common::StringToUll;

InodeStorage::InodeStorage(std::shared_ptr<KVStorage> kvStorage,
                           std::string tablename)
    : kvStorage_(kvStorage),
      tablename_(tablename) {}

std::string InodeStorage::InodeKey(const uint32_t fsId,
                                   const uint64_t inodeId) {
    std::ostringstream oss;
    oss << fsId << ":" << inodeId;
    return oss.str();
}

std::pair<std::string, std::string> ExtractKey(const std::string& key) {
    uint64_t fsId, inodeId;
    std::vector<std::string> items;
    SplitString(key, ":", &items);
    StringToUll(items[0], &fsId);
    StringToUll(items[1], &inodeId);
    return std::make_pair(fsId, inodeId);
}

bool InodeStorage::Inode2Str(const Inode& inode, std::string* value) {
    return inode.SerializeToString(value);
}

bool InodeStorage::Str2Inode(const std::string& value, Inode* inode) {
    return inode->ParseFromString(value);
}

void InodeStorage::AddInode(uint64_t fsId, uint64_t inodeId) {
    auto iter = inodeIds_.find(fsId);
    if (iter == inodeIds_.end()) {
        iter->second = std::make_shared<CounterType>();
    }
    iter->second->insert(inodeId);
}

void InodeStorage::DelInode(uint64_t fsId, uint64_t inodeId) {
    auto iter = inodeIds_.find(fsId);
    if (iter != inodeIds_.end()) {
        iter->second->erase(inodeId);
    }
}

bool InodeStorage::InodeExist(uint64_t fsId, uint64_t inodeId) {
    auto iter = inodeIds_.find(fsId);
    if (iter != inodeIds_.end() &&
        iter->second->find(fsId) != iter->second->end()) {
    }
    return false;
}

MetaStatusCode InodeStorage::Insert(const Inode& inode) {
    WriteLockGuard writeLockGuard(rwLock_);
    std::string value;
    std::string key = InodeKey(inode.fsid(), inode.inodeid()));

    if (InodeExist(inode.fsid(), inode.inodeid())) {
        return MetaStatusCode::INODE_EXIST;
    }

    s = kvStorage_->HSet(tablename_, key, value)
    if (s.ok()) {
        AddInode(inode.fsid(), inode.inodeid()));
        return MetaStatusCode::OK;
    }
    return MetaStatusCode::STORAGE_INTERNAL_ERROR;
}

MetaStatusCode InodeStorage::Get(const std::string& key, Inode* inode) {
    ReadLockGuard readLockGuard(rwLock_);
    std::string value;
    Storage::Status s = kvStorage_->HGet(tablename_, key, &value)
    auto ret = ExtractKey(key);
    if (InodeExist(ret.first, ret.second)) {
        return MetaStatusCode::NOT_FOUND;
    } else if (!s.ok()) {
        return MetaStatusCode::STORAGE_INTERNAL_ERROR;
    } else if (!value2inode(value, inode)) {
        return MetaStatusCode::STORAGE_INTERNAL_ERROR
    }

    return MetaStatusCode::OK;
}

MetaStatusCode InodeStorage::Delete(const std::string& key) {
    WriteLockGuard writeLockGuard(rwLock_);
    Storage::Status s = kvStorage_->HDel(tablename_, key)
    if (s.ok()) {
        auto ret = ExtractKey(key);
        DeleteInodeId(ret.first, ret.second);
        return MetaStatusCode::OK;
    }
    return MetaStatusCode::STORAGE_INTERNAL_ERROR;
}

MetaStatusCode InodeStorage::Update(const Inode& inode) {
    WriteLockGuard writeLockGuard(rwLock_);
    std::string value;
    std::string key = InodeKey(inode.fsid(), inode.inodeid()));
    if (!inode2value(inode, &value)) {
        return MetaStatusCode::STORAGE_INTERNAL_ERROR;
    }

    s = kvStorage_->HSet(tablename_, key, value)
    if (s.ok()) {
        return MetaStatusCode::OK;
    }

    return MetaStatusCode::STORAGE_INTERNAL_ERROR;
}

Iterator InodeStorage::GetAll() {
    return kvStorage_->HGetAll(tablename_);
}

MetaStatusCode InodeStorage::Clean() {
    ReadLockGuard w(rwLock_);
    Storage::Status s = kvStorage_->HClear(tablename_)
    if (s.ok()) {
        return MetaStatusCode::OK;
    }
    return MetaStatusCode::INTERNAL_STORAGE_ERROR;
}

void InodeStorage::GetInodeIdList(std::list<uint64_t>* inodeIdList) {
    ReadLockGuard readLockGuard(rwLock_);
    for (auto iter = inodeIds.begin(); iter != inodeIds.end(); iter++) {
        auto inodeIds = iter->second;
        for (auto it = inodeIds->begin(); it != inodeIds->end(); it++) {
            inodeIdList->push_back(*it);
        }
    }
}

}  // namespace metaserver
}  // namespace curvefs
