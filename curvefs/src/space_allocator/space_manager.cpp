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

#include <utility>
#include <vector>

#include "curvefs/proto/mds.pb.h"
#include "curvefs/proto/space.pb.h"
#include "curvefs/src/space_allocator/reloader.h"

namespace curvefs {
namespace space {

DEFINE_bool(enableReload, false, "enable reload extents");

SpaceStatusCode DefaultSpaceManager::InitSpace(const mds::FsInfo& fsInfo) {
    if (fsInfo.fstype() == mds::FSType::TYPE_S3) {
        return InitS3Space(fsInfo);
    } else {
        return InitBlockSpace(fsInfo);
    }
}

SpaceStatusCode DefaultSpaceManager::UnInitSpace(uint32_t fsId) {
    WriteLockGuard lock(blkRwlock_);
    auto iter = blkAllocators_.find(fsId);
    if (iter == blkAllocators_.end()) {
        return SPACE_NOT_FOUND;
    }

    blkAllocators_.erase(iter);
    return SPACE_OK;
}

SpaceStatusCode DefaultSpaceManager::StatSpace(uint32_t fsId, uint64_t* total,
                                               uint64_t* available,
                                               uint64_t* blkSize) {
    ReadLockGuard lock(blkRwlock_);
    auto iter = blkAllocators_.find(fsId);
    if (iter == blkAllocators_.end()) {
        return SPACE_NOT_FOUND;
    }

    *total = iter->second->Total();
    *available = iter->second->AvailableSize();
    return SPACE_OK;
}

SpaceStatusCode DefaultSpaceManager::AllocateSpace(
    uint32_t fsId, uint32_t size, const SpaceAllocateHint& hint,
    std::vector<PExtent>* exts) {
    ReadLockGuard lock(blkRwlock_);
    auto iter = blkAllocators_.find(fsId);
    if (iter == blkAllocators_.end()) {
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
    ReadLockGuard lock(blkRwlock_);
    auto iter = blkAllocators_.find(fsId);
    if (iter == blkAllocators_.end()) {
        return SPACE_NOT_FOUND;
    }

    for (auto& e : extents) {
        iter->second->DeAlloc(e.offset(), e.length());
    }

    return SPACE_OK;
}

SpaceStatusCode DefaultSpaceManager::AllocateS3Chunk(uint32_t fsId,
                                                     uint64_t* id) {
    ReadLockGuard lock(s3Rwlock_);
    auto iter = s3Allocator_.find(fsId);
    if (iter == s3Allocator_.end()) {
        return SPACE_NOT_FOUND;
    }

    *id = iter->second->NextChunkId();
    return SPACE_OK;
}

SpaceStatusCode DefaultSpaceManager::InitS3Space(const mds::FsInfo& fsInfo) {
    WriteLockGuard lock(s3Rwlock_);
    if (s3Allocator_.count(fsInfo.fsid()) != 0) {
        LOG(ERROR) << "allocator already exists, fsId: " << fsInfo.fsid();
        return SPACE_EXISTS;
    }

    std::unique_ptr<S3Allocator> allocator(new S3Allocator(0));
    s3Allocator_.emplace(fsInfo.fsid(), std::move(allocator));
    return SPACE_OK;
}

SpaceStatusCode DefaultSpaceManager::InitBlockSpace(const mds::FsInfo& fsInfo) {
    WriteLockGuard lock(blkRwlock_);
    if (blkAllocators_.count(fsInfo.fsid()) != 0) {
        LOG(ERROR) << "allocator already exists, fsId: " << fsInfo.fsid();
        return SPACE_EXISTS;
    }

    BitmapAllocatorOption opt;
    opt.length = fsInfo.capacity();
    opt.sizePerBit = 4 * kMiB;
    opt.startOffset = 0;
    opt.smallAllocProportion = 0.2;

    std::unique_ptr<BitmapAllocator> allocator(new BitmapAllocator(opt));

    if (FLAGS_enableReload) {
        DefaultRealoder reloader(allocator.get(), fsInfo.fsid(),
                                 fsInfo.rootinodeid());
        if (reloader.Reload() == false) {
            LOG(ERROR) << "reload extents failed";
            return SPACE_RELOAD_ERROR;
        }
    }

    blkAllocators_.emplace(fsInfo.fsid(), std::move(allocator));
    LOG(INFO) << "Init allocator success, fsId: " << fsInfo.fsid()
              << "volume size: " << fsInfo.capacity()
              << ", block size: " << fsInfo.blocksize();
    return SPACE_OK;
}

DefaultSpaceManager::DefaultSpaceManager(const SpaceManagerOption& opt)
    : opt_(opt) {}

}  // namespace space
}  // namespace curvefs
