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

namespace curvefs {
namespace mds {
bool MemoryFsStorage::Init() {
    WriteLockGuard writeLockGuard(rwLock_);
    fsInfoMap_.clear();
    return true;
}

void MemoryFsStorage::Uninit() {
    WriteLockGuard writeLockGuard(rwLock_);
    fsInfoMap_.clear();
}

FSStatusCode MemoryFsStorage::Insert(std::shared_ptr<MdsFsInfo> fs) {
    WriteLockGuard writeLockGuard(rwLock_);
    auto it = fsInfoMap_.emplace(fs->GetFsName(), fs);
    if (it.second == false) {
        return FSStatusCode::FS_EXIST;
    }
    return FSStatusCode::OK;
}

FSStatusCode MemoryFsStorage::Get(uint64_t fsId,
                                  std::shared_ptr<MdsFsInfo> *fs) {
    ReadLockGuard readLockGuard(rwLock_);

    for (auto it : fsInfoMap_) {
        if (it.second->GetFsId() == fsId) {
            *fs = it.second;
            return FSStatusCode::OK;
        }
    }
    return FSStatusCode::NOT_FOUND;
}

FSStatusCode MemoryFsStorage::Get(const std::string &fsName,
                                  std::shared_ptr<MdsFsInfo> *fs) {
    ReadLockGuard readLockGuard(rwLock_);
    auto it = fsInfoMap_.find(fsName);
    if (it == fsInfoMap_.end()) {
        return FSStatusCode::NOT_FOUND;
    }
    *fs = it->second;
    return FSStatusCode::OK;
}

FSStatusCode MemoryFsStorage::Delete(const std::string &fsName) {
    WriteLockGuard writeLockGuard(rwLock_);
    auto size = fsInfoMap_.erase(fsName);
    if (size == 0) {
        return FSStatusCode::NOT_FOUND;
    }
    return FSStatusCode::OK;
}

FSStatusCode MemoryFsStorage::Update(std::shared_ptr<MdsFsInfo> fs) {
    WriteLockGuard writeLockGuard(rwLock_);
    auto it = fsInfoMap_.find(fs->GetFsName());
    if (it == fsInfoMap_.end()) {
        return FSStatusCode::NOT_FOUND;
    }
    if (it->second->GetFsId() != fs->GetFsId()) {
        return FSStatusCode::FS_ID_MISMATCH;
    }
    // fsInfoMap_[fs->GetFsName()] = fs;
    it->second = fs;
    return FSStatusCode::OK;
}

bool MemoryFsStorage::Exist(uint64_t fsId) {
    ReadLockGuard readLockGuard(rwLock_);
    for (auto it : fsInfoMap_) {
        if (it.second->GetFsId() == fsId) {
            return true;
        }
    }
    return false;
}

bool MemoryFsStorage::Exist(const std::string &fsName) {
    ReadLockGuard readLockGuard(rwLock_);
    auto it = fsInfoMap_.find(fsName);
    if (it == fsInfoMap_.end()) {
        return false;
    }

    return true;
}

}  // namespace mds
}  // namespace curvefs
