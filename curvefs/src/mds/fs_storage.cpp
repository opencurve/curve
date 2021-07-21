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

#include "curvefs/src/mds/fs_storage.h"

#include <glog/logging.h>

#include <string>
#include <utility>
#include <vector>

#include "curvefs/src/mds/codec/codec.h"

namespace curvefs {
namespace mds {

using ::curve::idgenerator::EtcdIdGenerator;
using ::curve::kvstorage::KVStorageClient;

bool MemoryFsStorage::Init() {
    WriteLockGuard writeLockGuard(rwLock_);
    fsInfoMap_.clear();
    return true;
}

void MemoryFsStorage::Uninit() {
    WriteLockGuard writeLockGuard(rwLock_);
    fsInfoMap_.clear();
}

FSStatusCode MemoryFsStorage::Insert(const FsInfoWrapper& fs) {
    WriteLockGuard writeLockGuard(rwLock_);
    auto it = fsInfoMap_.emplace(fs.GetFsName(), fs);
    if (it.second == false) {
        return FSStatusCode::FS_EXIST;
    }
    return FSStatusCode::OK;
}

FSStatusCode MemoryFsStorage::Get(uint64_t fsId, FsInfoWrapper* fs) {
    ReadLockGuard readLockGuard(rwLock_);

    for (const auto& it : fsInfoMap_) {
        if (it.second.GetFsId() == fsId) {
            *fs = it.second;
            return FSStatusCode::OK;
        }
    }
    return FSStatusCode::NOT_FOUND;
}

FSStatusCode MemoryFsStorage::Get(const std::string& fsName,
                                  FsInfoWrapper* fs) {
    ReadLockGuard readLockGuard(rwLock_);
    auto it = fsInfoMap_.find(fsName);
    if (it == fsInfoMap_.end()) {
        return FSStatusCode::NOT_FOUND;
    }
    *fs = it->second;
    return FSStatusCode::OK;
}

FSStatusCode MemoryFsStorage::Delete(const std::string& fsName) {
    WriteLockGuard writeLockGuard(rwLock_);
    auto size = fsInfoMap_.erase(fsName);
    if (size == 0) {
        return FSStatusCode::NOT_FOUND;
    }
    return FSStatusCode::OK;
}

FSStatusCode MemoryFsStorage::Update(const FsInfoWrapper& fs) {
    WriteLockGuard writeLockGuard(rwLock_);
    auto it = fsInfoMap_.find(fs.GetFsName());
    if (it == fsInfoMap_.end()) {
        return FSStatusCode::NOT_FOUND;
    }
    if (it->second.GetFsId() != fs.GetFsId()) {
        return FSStatusCode::FS_ID_MISMATCH;
    }
    // fsInfoMap_[fs->GetFsName()] = fs;
    it->second = fs;
    return FSStatusCode::OK;
}

bool MemoryFsStorage::Exist(uint64_t fsId) {
    ReadLockGuard readLockGuard(rwLock_);
    for (const auto& it : fsInfoMap_) {
        if (it.second.GetFsId() == fsId) {
            return true;
        }
    }
    return false;
}

bool MemoryFsStorage::Exist(const std::string& fsName) {
    ReadLockGuard readLockGuard(rwLock_);
    auto it = fsInfoMap_.find(fsName);
    if (it == fsInfoMap_.end()) {
        return false;
    }

    return true;
}

uint64_t MemoryFsStorage::NextFsId() {
    return id_.fetch_add(1, std::memory_order_relaxed);
}

PersisKVStorage::PersisKVStorage(
    const std::shared_ptr<curve::kvstorage::KVStorageClient>& storage)
    : storage_(storage),
      idGen_(new FsIdGenerator(storage_)),
      fsLock_(),
      fs_(),
      idToNameLock_(),
      idToName_() {}

PersisKVStorage::~PersisKVStorage() = default;

FSStatusCode PersisKVStorage::Get(uint64_t fsId, FsInfoWrapper* fsInfo) {
    std::string name;
    if (!FsIDToName(fsId, &name)) {
        return FSStatusCode::NOT_FOUND;
    }

    return Get(name, fsInfo);
}

bool PersisKVStorage::Init() {
    bool ret = LoadAllFs();
    return ret;
}

void PersisKVStorage::Uninit() {}

FSStatusCode PersisKVStorage::Get(const std::string& fsName,
                                  FsInfoWrapper* fsInfo) {
    ReadLockGuard lock(fsLock_);
    auto iter = fs_.find(fsName);
    if (iter != fs_.end()) {
        *fsInfo = iter->second;
        return FSStatusCode::OK;
    }

    return FSStatusCode::NOT_FOUND;
}

FSStatusCode PersisKVStorage::Insert(const FsInfoWrapper& fs) {
    WriteLockGuard idLock(idToNameLock_);
    WriteLockGuard fsLock(fsLock_);

    // check if fsname already exists
    bool exists = fs_.count(fs.GetFsName()) != 0;
    if (exists) {
        LOG(ERROR) << "fsname already exists, fsname: " << fs.GetFsName();
        return FSStatusCode::FS_EXIST;
    }

    // persist to storage
    if (!PersitToStorage(fs)) {
        return FSStatusCode::STORAGE_ERROR;
    }

    // update cache
    fs_.emplace(fs.GetFsName(), fs);
    idToName_.emplace(fs.GetFsId(), fs.GetFsName());

    return FSStatusCode::OK;
}

FSStatusCode PersisKVStorage::Update(const FsInfoWrapper& fs) {
    WriteLockGuard lock(fsLock_);
    auto iter = fs_.find(fs.GetFsName());
    if (iter == fs_.end()) {
        LOG(ERROR) << "fsname not found, fsName: " << fs.GetFsName();
        return FSStatusCode::NOT_FOUND;
    }

    if (iter->second.GetFsId() != fs.GetFsId()) {
        LOG(ERROR) << "fs id not match, fs id in cache: "
                   << iter->second.GetFsId()
                   << ", current fs id : " << fs.GetFsId()
                   << ", fsName: " << fs.GetFsName();
        return FSStatusCode::FS_ID_MISMATCH;
    }

    // update to storage
    if (!PersitToStorage(fs)) {
        LOG(ERROR) << "Persist to storage failed, fsName: " << fs.GetFsName();
        return FSStatusCode::STORAGE_ERROR;
    }

    iter->second = fs;
    return FSStatusCode::OK;
}

FSStatusCode PersisKVStorage::Delete(const std::string& fsName) {
    WriteLockGuard idLock(idToNameLock_);
    WriteLockGuard fsLock(fsLock_);
    auto iter = fs_.find(fsName);
    if (iter == fs_.end()) {
        LOG(ERROR) << "fs name '" << fsName << "' not found";
        return FSStatusCode::NOT_FOUND;
    }

    if (!RemoveFromStorage(iter->second)) {
        LOG(ERROR) << "Remove fs from storage failed, fsName: " << fsName;
        return FSStatusCode::STORAGE_ERROR;
    }

    idToName_.erase(iter->second.GetFsId());
    fs_.erase(iter);
    return FSStatusCode::OK;
}

bool PersisKVStorage::Exist(uint64_t fsId) {
    std::string name;
    if (!FsIDToName(fsId, &name)) {
        return false;
    }

    return Exist(name);
}

bool PersisKVStorage::Exist(const std::string& fsName) {
    ReadLockGuard lock(fsLock_);
    return fs_.count(fsName) != 0;
}

uint64_t PersisKVStorage::NextFsId() {
    uint64_t id = 0;
    if (idGen_->GenFsId(&id)) {
        return id;
    }

    return INVALID_FS_ID;
}

bool PersisKVStorage::LoadAllFs() {
    std::vector<std::pair<std::string, std::string>> out;

    int err = storage_->List(codec::FsNameStoreKey(),
                             codec::FsNameStoreEndKey(), &out);

    if (err != EtcdErrCode::EtcdOK) {
        LOG(ERROR) << "List all fs from etcd failed, error: " << err;
        return false;
    }

    for (const auto& kv : out) {
        FsInfo fsInfo;
        if (!codec::DecodeProtobufMessage(kv.second, &fsInfo)) {
            LOG(ERROR) << "Decode fs info failed, encoded fsName: " << kv.first;
            return false;
        }

        LOG(INFO) << "Load fs '" << fsInfo.fsname()
                  << "' success, detail: " << fsInfo.ShortDebugString();

        idToName_.emplace(fsInfo.fsid(), fsInfo.fsname());
        fs_.emplace(fsInfo.fsname(), std::move(fsInfo));
    }

    return true;
}

bool PersisKVStorage::FsIDToName(uint64_t fsId, std::string* name) const {
    ReadLockGuard lock(idToNameLock_);
    auto iter = idToName_.find(fsId);
    if (iter != idToName_.end()) {
        *name = iter->second;
        return true;
    }

    LOG(ERROR) << "fsId: " << fsId << " not found";
    return false;
}

bool PersisKVStorage::PersitToStorage(const FsInfoWrapper& fs) {
    std::string key = codec::EncodeFsName(fs.GetFsName());
    std::string value;

    if (!codec::EncodeProtobufMessage(fs.fsInfo_, &value)) {
        LOG(ERROR) << "Encode fs info failed, fsName: " << fs.GetFsName();
        return false;
    }

    int ret = storage_->Put(key, value);
    if (ret != EtcdErrCode::EtcdOK) {
        LOG(ERROR) << "Put key-value to storage failed, fsName: "
                   << fs.GetFsName();
        return false;
    }

    return true;
}

bool PersisKVStorage::RemoveFromStorage(const FsInfoWrapper& fs) {
    std::string key = codec::EncodeFsName(fs.GetFsName());

    int ret = storage_->Delete(key);
    if (ret != EtcdErrCode::EtcdOK) {
        LOG(ERROR) << "Remove fs from storage failed, fsName: "
                   << fs.GetFsName();
        return false;
    }

    return true;
}

}  // namespace mds
}  // namespace curvefs
