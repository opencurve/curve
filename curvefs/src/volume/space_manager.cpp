/*
 *  Copyright (c) 2022 NetEase Inc.
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
 * Date: Wednesday Mar 02 19:56:31 CST 2022
 * Author: wuhanqing
 */

#include "curvefs/src/volume/space_manager.h"

#include <butil/time.h>
#include <bvar/bvar.h>

#include <atomic>
#include <unordered_set>
#include <utility>

#include "absl/cleanup/cleanup.h"
#include "curvefs/src/volume/common.h"
#include "src/common/fast_align.h"

namespace curvefs {
namespace volume {

using ::curve::common::align_down;
using ::curve::common::ReadLockGuard;
using ::curve::common::WriteLockGuard;

SpaceManagerImpl::SpaceManagerImpl(
    const SpaceManagerOption &option,
    const std::shared_ptr<MdsClient> &mdsClient,
    const std::shared_ptr<BlockDeviceClient> &blockDev)
    : totalBytes_(0), availableBytes_(0),
      blockSize_(option.blockGroupManagerOption.blockSize),
      blockGroupSize_(option.blockGroupManagerOption.blockGroupSize),
      blockGroupManager_(new BlockGroupManagerImpl(
          this, mdsClient, blockDev, option.blockGroupManagerOption,
          option.allocatorOption)),
      allocating_(false), threshold_(option.threshold),
      releaseInterSec_(option.releaseInterSec) {}

bool SpaceManagerImpl::Alloc(uint32_t size,
                             const AllocateHint& hint,
                             std::vector<Extent>* extents) {
    VLOG(9) << "Alloc size: " << size << ", hint: " << hint;

    butil::Timer timer;
    timer.start();

    if (availableBytes_.load(std::memory_order_acquire) < size) {
        auto ret = AllocateBlockGroup(size);
        if (!ret) {
            LOG(ERROR) << "Allocate block group error";
            metric_.errorCount << 1;
            return false;
        }
    }

    int64_t left = size;
    while (left > 0) {
        auto allocated = AllocInternal(left, hint, extents);
        availableBytes_.fetch_sub(allocated, std::memory_order_relaxed);
        if (allocated < left) {
            auto ret = AllocateBlockGroup(left);
            if (!ret) {
                LOG(ERROR) << "Allocate block group error";
                metric_.errorCount << 1;
                return false;
            }
        }
        left -= allocated;
    }

    auto ret = UpdateBitmap(*extents, BlockGroupBitmapUpdater::Set);
    if (!ret) {
        LOG(ERROR) << "Update bitmap failed";
        metric_.errorCount << 1;
        return false;
    }

    timer.stop();
    metric_.allocLatency << timer.u_elapsed();
    metric_.allocSize << size;

    VLOG(9) << "Alloc success, " << *extents;

    return true;
}

bool SpaceManagerImpl::DeAlloc(uint64_t blockGroupOffset,
                               const std::vector<Extent> &extents) {
    butil::Timer timer;
    timer.start();

    if (!AcquireBlockGroup(blockGroupOffset)) {
        LOG(WARNING) << "Acquire block group failed, block group offset: "
                     << blockGroupOffset;
        return false;
    }

    bool ret = UpdateBitmap(extents, BlockGroupBitmapUpdater::Op::Clear);
    if (!ret) {
        LOG(ERROR) << "DeAlloc update bitmap failed";
        metric_.errorCount << 1;
        return false;
    }

    uint64_t size = 0;
    for (const auto &ext : extents) {
        size += ext.len;
    }

    timer.stop();
    metric_.deallocLatency << timer.u_elapsed();
    metric_.allocSize << -size;

    VLOG(9) << "Dealloc success, " << extents;
    return true;
}

std::map<uint64_t, std::unique_ptr<Allocator>>::iterator
SpaceManagerImpl::FindAllocator(const AllocateHint& hint) {
    if (hint.HasRightHint()) {
        auto it = allocators_.lower_bound(
            align_down(hint.rightOffset, blockGroupSize_));
        if (it != allocators_.end()) {
            return it;
        }
    }

    if (hint.HasLeftHint()) {
        auto it = allocators_.lower_bound(
            align_down(hint.leftOffset - 1, blockGroupSize_));
        if (it != allocators_.end()) {
            return it;
        }
    }

    static thread_local unsigned int seed = time(nullptr);
    auto it = allocators_.begin();
    std::advance(it, rand_r(&seed) % allocators_.size());

    return it;
}

int64_t SpaceManagerImpl::AllocInternal(int64_t size,
                                        const AllocateHint& hint,
                                        std::vector<Extent>* exts) {
    ReadLockGuard lk(allocatorsLock_);
    int64_t left = size;
    auto it = FindAllocator(hint);
    const auto beginIt = it;

    do {
        left -= it->second->Alloc(left, hint, exts);
        if (left <= 0) {
            break;
        }

        ++it;
        if (it == allocators_.end()) {
            it = allocators_.begin();
        }

        if (it == beginIt) {
            break;
        }
    } while (true);

    return size - left;
}

bool SpaceManagerImpl::UpdateBitmap(const std::vector<Extent> &exts,
                                    BlockGroupBitmapUpdater::Op op) {
    ReadLockGuard lk(updatersLock_);

    std::unordered_set<BlockGroupBitmapUpdater*> dirty;
    for (const auto& ext : exts) {
        BlockGroupBitmapUpdater* updater = FindBitmapUpdater(ext);
        updater->Update(ext, op);
        dirty.insert(updater);
    }

    for (auto d : dirty) {
        d->Sync();
    }

    return true;
}

BlockGroupBitmapUpdater* SpaceManagerImpl::FindBitmapUpdater(
    const Extent& ext) {
    uint64_t blockGroupOffset = align_down(ext.offset, blockGroupSize_);
    VLOG(9) << "block group offset: " << blockGroupOffset << ", ext: " << ext
            << ", group block size: " << blockGroupSize_;
    auto it = bitmapUpdaters_.find(blockGroupOffset);
    CHECK(it != bitmapUpdaters_.end())
        << "block group offset: " << blockGroupOffset;

    return it->second.get();
}

void SpaceManagerImpl::Run() {
    releaseT_ = std::thread(&SpaceManagerImpl::ReleaseFullBlockGroups, this);
    running_ = true;
}


void SpaceManagerImpl::ReleaseFullBlockGroups() {
    while (sleeper_.wait_for(std::chrono::seconds(releaseInterSec_))) {
        std::vector<uint64_t> selectBlockGroups;

        // find the blockgroup whose space usage ratio is greater than a certain
        // threshold
        {
            ReadLockGuard lk(allocatorsLock_);
            for (auto &alloc : allocators_) {
                if (alloc.second->Total() == 0) {
                    continue;
                }

                double total = alloc.second->Total();
                double available = alloc.second->AvailableSize();
                double usedPer = 1.0 - available / total;

                if (usedPer > threshold_) {
                    selectBlockGroups.push_back(alloc.first);
                }
            }
        }

        if (selectBlockGroups.empty()) {
            LOG(INFO) << "No block group need to release";
            continue;
        }

        // release selected blockgroups
        {
            WriteLockGuard allocLk(allocatorsLock_);
            WriteLockGuard updaterLk(updatersLock_);
            for (auto &id : selectBlockGroups) {
                auto iter = allocators_.find(id);
                assert(iter != allocators_.end());

                allocators_.erase(id);
                bitmapUpdaters_.erase(id);

                availableBytes_.fetch_sub(iter->second->AvailableSize(),
                                          std::memory_order_relaxed);
                totalBytes_.fetch_sub(iter->second->AvailableSize(),
                                      std::memory_order_release);

                bool ret = blockGroupManager_->ReleaseBlockGroup(id);
                if (ret) {
                    LOG(INFO) << "Release block group success, id: " << id;
                } else {
                    LOG(ERROR) << "Release block group failed, id: " << id;
                }
            }
        }
    }
}

bool SpaceManagerImpl::Shutdown() {
    bool ret = false;

    {
        WriteLockGuard allocLk(allocatorsLock_);
        WriteLockGuard updaterLk(updatersLock_);

        // sync all bitmap updater
        for (auto &updater : bitmapUpdaters_) {
            ret = updater.second->Sync();
            if (!ret) {
                LOG(ERROR) << "Sync bitmap updater failed";
                return false;
            }
        }

        // release all block group
        ret = blockGroupManager_->ReleaseAllBlockGroups();
        LOG_IF(ERROR, !ret) << "Release all block groups failed";
    }

    if (running_) {
        sleeper_.interrupt();
        releaseT_.join();
        LOG(INFO) << "SpaceManagerImpl stop thread ok";
    }
    return ret;
}

bool SpaceManagerImpl::AllocateBlockGroup(uint64_t hint) {
    std::unique_lock<std::mutex> lk(mtx_);
    if (availableBytes_.load(std::memory_order_relaxed) >= hint) {
        return true;
    }

    std::vector<AllocatorAndBitmapUpdater> out;
    auto ret = blockGroupManager_->AllocateBlockGroup(&out);
    if (!ret) {
        LOG(ERROR) << "Allocate block group failed";
        return false;
    }

    uint64_t available = 0;
    uint64_t total = 0;
    WriteLockGuard allocLk(allocatorsLock_);
    WriteLockGuard updaterLk(updatersLock_);
    for (auto &d : out) {
        VLOG(9) << "add allocator, offset: " << d.blockGroupOffset
                << ", available: " << d.allocator->AvailableSize()
                << ", total: " << d.allocator->Total();
        available += d.allocator->AvailableSize();
        total += d.allocator->Total();
        allocators_.emplace(d.blockGroupOffset, std::move(d.allocator));
        bitmapUpdaters_.emplace(d.blockGroupOffset, std::move(d.bitmapUpdater));
    }

    availableBytes_.fetch_add(available, std::memory_order_release);
    totalBytes_.fetch_add(total, std::memory_order_release);
    return true;
}

bool SpaceManagerImpl::AcquireBlockGroup(uint64_t blockGroupOffset) {
    std::unique_lock<std::mutex> lk(mtx_);
    if (bitmapUpdaters_.find(blockGroupOffset) != bitmapUpdaters_.end()) {
        return true;
    }

    AllocatorAndBitmapUpdater out;
    bool ret = blockGroupManager_->AcquireBlockGroup(blockGroupOffset, &out);
    if (!ret) {
        LOG(WARNING) << "Acquire block group failed, offset: "
                     << blockGroupOffset;
        return false;
    }

    WriteLockGuard allocLk(allocatorsLock_);
    WriteLockGuard updaterLk(updatersLock_);

    allocators_.emplace(blockGroupOffset, std::move(out.allocator));
    bitmapUpdaters_.emplace(blockGroupOffset, std::move(out.bitmapUpdater));
    return true;
}

}  // namespace volume
}  // namespace curvefs
