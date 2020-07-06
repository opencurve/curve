/**
 * Project: nebd
 * Create Date: 2020-01-20
 * Author: wuhanqing
 * Copyright (c) 2020 netease
 */

#include "nebd/src/part1/nebd_metacache.h"

namespace nebd {
namespace client {

using curve::common::WriteLockGuard;
using curve::common::ReadLockGuard;

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
