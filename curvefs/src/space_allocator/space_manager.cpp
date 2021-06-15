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

#include "curvefs/src/space_allocator/space_manager.h"

#include <glog/logging.h>
#include <vector>

#include <utility>

#include "curvefs/proto/space.pb.h"

#include "curvefs/src/space_allocator/reloader.h"

namespace curvefs {
namespace space {

DEFINE_bool(enableReload, false, "enable reload extents");

SpaceStatusCode DefaultSpaceManager::InitSpace(uint32_t fsId, uint64_t volSize,
                                        uint64_t blkSize,
                                        uint64_t rootInodeId) {
    WriteLockGuard lock(rwlock_);
    if (allocators_.count(fsId) != 0) {
        LOG(ERROR) << "allocator already exists, fsId: " << fsId;
        return SPACE_EXISTS;
    }

    BitmapAllocatorOption opt;
    std::unique_ptr<BitmapAllocator> allocator(new BitmapAllocator(opt));

    if (FLAGS_enableReload) {
        DefaultRealoder reloader(allocator.get(), fsId, rootInodeId);
        if (reloader.Reload() == false) {
            LOG(ERROR) << "reload extents failed";
            return SPACE_RELOAD_ERROR;
        }
    }

    allocators_.emplace(fsId, std::move(allocator));
    LOG(INFO) << "Init allocator success, fsId: " << fsId
              << "volume size: " << volSize << ", block size: " << blkSize;
    return SPACE_OK;
}

SpaceStatusCode DefaultSpaceManager::UnInitSpace(uint32_t fsId) {
    WriteLockGuard lock(rwlock_);
    auto iter = allocators_.find(fsId);
    if (iter == allocators_.end()) {
        return SPACE_NOT_FOUND;
    }

    allocators_.erase(iter);
    return SPACE_OK;
}

SpaceStatusCode DefaultSpaceManager::StatSpace(uint32_t fsId, uint64_t* total,
                                        uint64_t* available,
                                        uint64_t* blkSize) {
    ReadLockGuard lock(rwlock_);
    auto iter = allocators_.find(fsId);
    if (iter == allocators_.end()) {
        return SPACE_NOT_FOUND;
    }

    *total = iter->second->Total();
    *available = iter->second->AvailableSize();
    return SPACE_OK;
}

SpaceStatusCode DefaultSpaceManager::AllocateSpace(uint32_t fsId, uint32_t size,
                                            const SpaceAllocateHint& hint,
                                            std::vector<PExtent>* exts) {
    ReadLockGuard lock(rwlock_);
    auto iter = allocators_.find(fsId);
    if (iter == allocators_.end()) {
        return SPACE_NOT_FOUND;
    }

    if (iter->second->AvailableSize() < size) {
        return SPACE_NOSPACE;
    }

    std::vector<PExtent> tmp;
    uint64_t allocated = iter->second->Alloc(size, hint, &tmp);
    if (allocated < size) {
        iter->second->DeAlloc(tmp);
        return SPACE_NOSPACE;
    } else {
        exts->insert(exts->end(), tmp.begin(), tmp.end());
        return SPACE_OK;
    }
}

SpaceStatusCode DefaultSpaceManager::DeallocateSpace(
    uint32_t fsId,
    const ::google::protobuf::RepeatedPtrField<::curvefs::space::Extent>&
        extents) {
    ReadLockGuard lock(rwlock_);
    auto iter = allocators_.find(fsId);
    if (iter == allocators_.end()) {
        return SPACE_NOT_FOUND;
    }

    for (auto& e : extents) {
        iter->second->DeAlloc(e.offset(), e.length());
    }

    return SPACE_OK;
}

DefaultSpaceManager::DefaultSpaceManager(const SpaceManagerOption& opt)
    : opt_(opt) {}

}  // namespace space
}  // namespace curvefs
