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
/**
 * Project: curve
 * File Created: Fri Jul 16 21:22:40 CST 2021
 * Author: wuhanqing
 */

#include "curvefs/src/space/space_manager.h"

#include <glog/logging.h>

#include <utility>

#include "curvefs/proto/mds.pb.h"
#include "curvefs/proto/space.pb.h"
#include "curvefs/src/space/reloader.h"
#include "curvefs/src/space/utils.h"

namespace curvefs {
namespace space {

using curve::common::ReadLockGuard;
using curve::common::WriteLockGuard;

SpaceStatusCode SpaceManagerImpl::InitSpace(const mds::FsInfo& fsInfo) {
    WriteLockGuard lock(blkRwlock_);
    if (allocators_.count(fsInfo.fsid()) != 0) {
        LOG(ERROR) << "allocator already exists, fsId: " << fsInfo.fsid();
        return SPACE_EXISTS;
    }

    AllocatorOption allocOpt(opt_.allocatorOption);
    allocOpt.bitmapAllocatorOption.startOffset = 0;
    allocOpt.bitmapAllocatorOption.length = fsInfo.capacity();

    auto allocator =
        Allocator::Create(opt_.allocatorType, allocOpt);
    if (!allocator) {
        LOG(ERROR) << "unknown allocator type: " << opt_.allocatorType;
        return SPACE_UNKNOWN_ERROR;
    }

    Reloader reloader(allocator.get(), fsInfo.fsid(), fsInfo.rootinodeid(),
                      opt_.reloaderOption);
    if (reloader.Reload() == false) {
        LOG(ERROR) << "reload extents failed";
        return SPACE_RELOAD_ERROR;
    }

    allocators_.emplace(fsInfo.fsid(), std::move(allocator));
    LOG(INFO) << "Init allocator success, fsId: " << fsInfo.fsid()
              << ", volume size: " << fsInfo.capacity()
              << ", block size: " << fsInfo.blocksize();
    return SPACE_OK;
}

SpaceStatusCode SpaceManagerImpl::UnInitSpace(uint32_t fsId) {
    WriteLockGuard lock(blkRwlock_);
    auto iter = allocators_.find(fsId);
    if (iter == allocators_.end()) {
        return SPACE_NOT_FOUND;
    }

    allocators_.erase(iter);
    return SPACE_OK;
}

SpaceStatusCode SpaceManagerImpl::StatSpace(uint32_t fsId, SpaceStat* stat) {
    ReadLockGuard lock(blkRwlock_);
    auto iter = allocators_.find(fsId);
    if (iter == allocators_.end()) {
        return SPACE_NOT_FOUND;
    }

    stat->blockSize = opt_.blockSize;
    stat->total = iter->second->Total();
    stat->available = iter->second->AvailableSize();
    return SPACE_OK;
}

SpaceStatusCode SpaceManagerImpl::AllocateSpace(uint32_t fsId, uint64_t size,
                                                const SpaceAllocateHint& hint,
                                                Extents* exts) {
    ReadLockGuard lock(blkRwlock_);
    auto iter = allocators_.find(fsId);
    if (iter == allocators_.end()) {
        return SPACE_NOT_FOUND;
    }

    if (iter->second->AvailableSize() < size) {
        return SPACE_NO_SPACE;
    }

    Extents tmp;
    uint64_t allocated = iter->second->Alloc(size, hint, &tmp);
    if (allocated < size) {
        iter->second->DeAlloc(tmp);
        return SPACE_NO_SPACE;
    } else {
        std::move(tmp.begin(), tmp.end(), std::back_inserter(*exts));
        return SPACE_OK;
    }
}

SpaceStatusCode SpaceManagerImpl::DeallocateSpace(uint32_t fsId,
                                                  const ProtoExtents& extents) {
    ReadLockGuard lock(blkRwlock_);
    auto iter = allocators_.find(fsId);
    if (iter == allocators_.end()) {
        return SPACE_NOT_FOUND;
    }

    for (auto& e : extents) {
        bool r = iter->second->DeAlloc(e.offset(), e.length());
        if (!r) {
            LOG(ERROR) << "Dealloc " << e << " failed";
            return SPACE_DEALLOC_ERROR;
        }
    }

    return SPACE_OK;
}

SpaceManagerImpl::SpaceManagerImpl(const SpaceManagerOption& opt) : opt_(opt) {}

}  // namespace space
}  // namespace curvefs
