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
FSStatusCode MemoryFsStorage::Insert(const MdsFsInfo &fs) {
    WriteLockGuard writeLockGuard(rwLock_);
    auto it = FsInfoMap_.emplace(fs.GetFsName(), fs);
    if (it.second == false) {
        return FSStatusCode::FS_EXIST;
    }
    return FSStatusCode::OK;
}

FSStatusCode MemoryFsStorage::Get(uint64_t fsId, MdsFsInfo *fs) {
    ReadLockGuard readLockGuard(rwLock_);
    for (auto it = FsInfoMap_.begin(); it != FsInfoMap_.end(); it++) {
        if (it->second.GetFsId() == fsId) {
            *fs = it->second;
            return FSStatusCode::OK;
        }
    }
    return FSStatusCode::NOT_FOUND;
}

FSStatusCode MemoryFsStorage::Get(const std::string &fsName, MdsFsInfo *fs) {
    ReadLockGuard readLockGuard(rwLock_);
    auto it = FsInfoMap_.find(fsName);
    if (it == FsInfoMap_.end()) {
        return FSStatusCode::NOT_FOUND;
    }
    *fs = it->second;
    return FSStatusCode::OK;
}

FSStatusCode MemoryFsStorage::Delete(const std::string &fsName) {
    WriteLockGuard writeLockGuard(rwLock_);
    auto it = FsInfoMap_.find(fsName);
    if (it != FsInfoMap_.end()) {
        FsInfoMap_.erase(it);
        return FSStatusCode::OK;
    }
    return FSStatusCode::NOT_FOUND;
}

FSStatusCode MemoryFsStorage::Update(const MdsFsInfo &fs) {
    WriteLockGuard writeLockGuard(rwLock_);
    auto it = FsInfoMap_.find(fs.GetFsName());
    if (it == FsInfoMap_.end()) {
        return FSStatusCode::NOT_FOUND;
    }
    FsInfoMap_[fs.GetFsName()] = fs;
    return FSStatusCode::OK;
}

bool MemoryFsStorage::Exist(uint64_t fsId) {
    ReadLockGuard readLockGuard(rwLock_);
    for (auto it = FsInfoMap_.begin(); it != FsInfoMap_.end(); it++) {
        if (it->second.GetFsId() == fsId) {
            return true;
        }
    }
    return false;
}

bool MemoryFsStorage::Exist(const std::string &fsName) {
    ReadLockGuard readLockGuard(rwLock_);
    auto it = FsInfoMap_.find(fsName);
    if (it == FsInfoMap_.end()) {
        return false;
    }

    return true;
}

}  // namespace mds
}  // namespace curvefs
