/*
 *  Copyright (c) 2020 NetEase Inc.
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

/**
 * Project: nebd
 * Create Date: 2020-01-20
 * Author: wuhanqing
 */

#include "nebd/src/part1/nebd_metacache.h"

namespace nebd {
namespace client {

using nebd::common::WriteLockGuard;
using nebd::common::ReadLockGuard;

void NebdClientMetaCache::AddFileInfo(const NebdClientFileInfo& fileInfo) {
    WriteLockGuard guard(rwLock_);
    fileinfos_.emplace(fileInfo.fd, fileInfo);
}

void NebdClientMetaCache::RemoveFileInfo(int fd) {
    WriteLockGuard guard(rwLock_);
    fileinfos_.erase(fd);
}

int NebdClientMetaCache::GetFileInfo(
    int fd, NebdClientFileInfo* fileInfo) const {
    ReadLockGuard guard(rwLock_);
    auto iter = fileinfos_.find(fd);
    if (iter != fileinfos_.end()) {
        *fileInfo = iter->second;
        return 0;
    }

    return -1;
}

std::vector<NebdClientFileInfo> NebdClientMetaCache::GetAllFileInfo() const {
    ReadLockGuard guard(rwLock_);
    std::vector<NebdClientFileInfo> result;

    result.reserve(fileinfos_.size());
    for (const auto& kv : fileinfos_) {
        result.push_back(kv.second);
    }

    return result;
}

}  // namespace client
}  // namespace nebd
