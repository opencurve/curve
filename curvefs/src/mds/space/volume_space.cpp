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
 * Date: Friday Feb 25 17:45:50 CST 2022
 * Author: wuhanqing
 */

#include "curvefs/src/mds/space/volume_space.h"

#include <bthread/mutex.h>
#include <glog/logging.h>

#include <algorithm>
#include <cmath>
#include <memory>
#include <mutex>
#include <set>
#include <utility>

#include "absl/memory/memory.h"
#include "curvefs/proto/common.pb.h"
#include "curvefs/proto/mds.pb.h"
#include "curvefs/proto/space.pb.h"
#include "curvefs/src/common/fast_align.h"
#include "curvefs/src/mds/fs_info_wrapper.h"
#include "curvefs/src/mds/space/mds_proxy_manager.h"

namespace curvefs {
namespace mds {
namespace space {

using LockGuard = std::lock_guard<bthread::Mutex>;

namespace {

BlockGroup BuildBlockGroupFromClean(uint64_t offset,
                                    uint64_t size,
                                    BitmapLocation location,
                                    const std::string& owner) {
    BlockGroup group;
    group.set_offset(offset);
    group.set_size(size);
    group.set_available(size);
    group.set_bitmaplocation(location);
    group.set_owner(owner);

    return group;
}

}  // namespace

std::unique_ptr<VolumeSpace> VolumeSpace::Create(uint32_t fsId,
                                                 const Volume &volume,
                                                 BlockGroupStorage *storage,
                                                 FsStorage *fsStorage,
                                                 uint64_t calcIntervalSec) {
    if (!volume.has_volumesize()) {
        LOG(ERROR) << "Volume info doesn't have size";
        return nullptr;
    }

    auto space =
        absl::WrapUnique(new VolumeSpace(fsId, volume, storage, fsStorage));

    // reload from storage
    std::vector<BlockGroup> groups;
    auto err = storage->ListBlockGroups(fsId, &groups);
    if (err != SpaceOk) {
        LOG(WARNING) << "List block groups from storage failed, fsId: " << fsId
                     << ", err: " << SpaceErrCode_Name(err);
        return nullptr;
    }

    const auto blockGroupSize = volume.blockgroupsize();
    const auto location = volume.bitmaplocation();
    const auto volumeSize = volume.volumesize();
    const auto blockSize = volume.blocksize();

    // only record available block groups and clean groups
    // for allocated groups, client will send heartbeat to update usage
    uint64_t availableSize = 0;
    std::set<uint64_t> usedGroupOffsets;
    for (auto &group : groups) {
        usedGroupOffsets.insert(group.offset());
        // availableSize += group.available();

        auto offset = group.offset();
        assert(offset % blockGroupSize == 0);
        assert(group.size() == blockGroupSize);
        assert(group.bitmaplocation() == location);
        assert((group.has_owner() && group.deallocating_size()) == 0);
        if (group.has_owner()) {
            space->allocatedGroups_.emplace(offset, std::move(group));
        } else if (group.deallocating_size()) {
            space->deallocatingGroups_.emplace(offset, std::move(group));
        } else {
            space->availableGroups_.emplace(offset, std::move(group));
            availableSize += group.available();
        }
    }

    std::set<uint64_t> allOffsets;
    for (uint64_t off = 0; off < volumeSize; off += blockGroupSize) {
        allOffsets.insert(off);
    }

    std::unordered_set<uint64_t> cleanGroupOffsets;
    std::set_difference(
        allOffsets.begin(), allOffsets.end(), usedGroupOffsets.begin(),
        usedGroupOffsets.end(),
        std::inserter(cleanGroupOffsets, cleanGroupOffsets.end()));

    space->cleanGroups_ = std::move(cleanGroupOffsets);

    space->calcIntervalSec_ = calcIntervalSec;
    space->metaserverNum_ = 0;

    LOG(INFO) << "Init volume space success, fsId: " << fsId
              << ", size: " << volumeSize << ", available: " << availableSize
              << ", block size: " << blockSize
              << ", block group size: " << blockGroupSize
              << ", total groups: " << volumeSize / blockGroupSize
              << ", allocated groups: " << space->allocatedGroups_.size()
              << ", available groups: " << space->availableGroups_.size()
              << ", deallocating groups: " << space->deallocatingGroups_.size()
              << ", clean groups: " << space->cleanGroups_.size();

    return space;
}

VolumeSpace::VolumeSpace(uint32_t fsId,
                         Volume volume,
                         BlockGroupStorage* storage,
                         FsStorage* fsStorage)
    : fsId_(fsId),
      volume_(std::move(volume)),
      storage_(storage),
      fsStorage_(fsStorage) {}

SpaceErrCode VolumeSpace::AllocateBlockGroups(
    uint32_t count,
    const std::string& owner,
    std::vector<BlockGroup>* blockGroups) {
    LockGuard lk(mtx_);
    auto err = AllocateBlockGroupsInternal(count, owner, blockGroups);
    if (err != SpaceOk) {
        LOG(WARNING) << "Allocate block groups failed, fsId: " << fsId_
                     << ", err: " << SpaceErrCode_Name(err);
        return err;
    }

    for (auto& group : *blockGroups) {
        allocatedGroups_.emplace(group.offset(), group);
    }

    err = PersistBlockGroups(*blockGroups);
    if (err != SpaceOk) {
        LOG(WARNING) << "Mark group allocated failed, fsId: " << fsId_
                     << ", err: " << SpaceErrCode_Name(err);
        return err;
    }

    return SpaceOk;
}

SpaceErrCode VolumeSpace::AllocateBlockGroupsInternal(
    uint32_t count,
    const std::string& owner,
    std::vector<BlockGroup>* blockGroups) {
    bool extend = false;
    uint32_t allocated = 0;

    while (allocated < count) {
        allocated += AllocateFromCleanGroups(count, owner, blockGroups);
        if (allocated >= count) {
            return SpaceOk;
        }

        allocated +=
            AllocateFromAvailableGroups(count - allocated, owner, blockGroups);
        if (allocated >= count) {
            return SpaceOk;
        }

        if (!extend) {
            extend = true;
            auto ret = ExtendVolume();
            if (ret != SpaceOk) {
                LOG(WARNING) << "Fail to extend volume, fsId: " << fsId_
                             << ", err: " << SpaceErrCode_Name(ret);
                // don't expose internal error
                return SpaceErrNoSpace;
            }
        } else {
            break;
        }
    }

    return SpaceErrNoSpace;
}

uint32_t VolumeSpace::AllocateFromCleanGroups(uint32_t count,
                                              const std::string& owner,
                                              std::vector<BlockGroup>* groups) {
    uint32_t allocated = 0;
    auto it = cleanGroups_.begin();
    while (allocated < count && it != cleanGroups_.end()) {
        auto offset = *it;
        it = cleanGroups_.erase(it);

        ++allocated;
        groups->push_back(BuildBlockGroupFromClean(
            offset, volume_.blockgroupsize(), volume_.bitmaplocation(), owner));
    }

    return allocated;
}

uint32_t VolumeSpace::AllocateFromAvailableGroups(
    uint32_t count,
    const std::string& owner,
    std::vector<BlockGroup>* groups) {
    uint32_t allocated = 0;
    auto it = availableGroups_.begin();
    while (allocated < count && it != availableGroups_.end()) {
        assert(!it->second.has_owner());
        ++allocated;
        it->second.set_owner(owner);
        groups->push_back(std::move(it->second));
        it = availableGroups_.erase(it);
    }

    return allocated;
}

SpaceErrCode VolumeSpace::AcquireBlockGroup(uint64_t blockGroupOffset,
                                            const std::string& owner,
                                            BlockGroup* group) {
    LockGuard lk(mtx_);
    auto err = AcquireBlockGroupInternal(blockGroupOffset, owner, group);
    if (err != SpaceOk) {
        LOG(WARNING) << "Acquire block group failed, fsId: " << fsId_
                     << ", block group offset: " << blockGroupOffset
                     << ", err: " << SpaceErrCode_Name(err);
        return err;
    }

    allocatedGroups_.emplace(blockGroupOffset, *group);

    err = PersistBlockGroup(*group);
    if (err != SpaceOk) {
        LOG(WARNING) << "Persist block group failed, fsId: " << fsId_
                     << ", block group offset: " << blockGroupOffset
                     << ", err: " << SpaceErrCode_Name(err);
    }

    return err;
}

SpaceErrCode VolumeSpace::AcquireBlockGroupInternal(uint64_t blockGroupOffset,
                                                    const std::string& owner,
                                                    BlockGroup* group) {
    // find in availables
    {
        auto it = availableGroups_.find(blockGroupOffset);
        if (it != availableGroups_.end()) {
            assert(!it->second.has_owner());
            *group = std::move(it->second);
            group->set_owner(owner);
            availableGroups_.erase(it);
            return SpaceOk;
        }
    }

    // find from allocated
    {
        auto it = allocatedGroups_.find(blockGroupOffset);
        if (it != allocatedGroups_.end()) {
            assert(it->second.has_owner());
            if (it->second.owner() == owner) {
                *group = it->second;
                return SpaceOk;
            } else {
                return SpaceErrConflict;
            }
        }
    }

    // this shouldn't happen, because currently only delete inode needs acquire
    // block group and in this case, block group must not be emtpy
    {
        LOG(WARNING) << "unexpected acquire block group, fsid: " << fsId_
                     << ", block group offset: " << blockGroupOffset
                     << ", owner: " << owner;

        if (cleanGroups_.count(blockGroupOffset) != 0) {
            *group = BuildBlockGroupFromClean(blockGroupOffset,
                                              volume_.blockgroupsize(),
                                              volume_.bitmaplocation(), owner);
            allocatedGroups_.emplace(blockGroupOffset, *group);
            return SpaceOk;
        }
    }

    return SpaceErrNotFound;
}

SpaceErrCode VolumeSpace::ReleaseBlockGroups(
    const std::vector<BlockGroup>& blockGroups) {
    LockGuard lk(mtx_);

    for (auto& group : blockGroups) {
        auto it = allocatedGroups_.find(group.offset());
        if (it != allocatedGroups_.end()) {
            if (it->second.owner() != group.owner()) {
                LOG(WARNING)
                    << "Owner is not identical, block group may "
                       "assign to others, fsId: "
                    << fsId_ << ", block group offset: " << group.offset();
                return SpaceErrConflict;
            }

            // space is total available
            if (group.available() == group.size()) {
                auto err = ClearBlockGroup(group);
                if (err != SpaceOk) {
                    LOG(WARNING) << "Clear block group failed, fsId: " << fsId_
                                 << ", block group offset: " << group.offset();
                    return err;
                }

                cleanGroups_.insert(group.offset());
            } else {
                auto copy = group;
                copy.clear_owner();
                auto err = PersistBlockGroup(copy);
                if (err != SpaceOk) {
                    LOG(WARNING)
                        << "Persist block group failed, fsId: " << fsId_
                        << ", block group offset: " << group.offset()
                        << ", err: " << SpaceErrCode_Name(err);
                    return err;
                }

                availableGroups_.emplace(group.offset(), std::move(copy));
            }

            allocatedGroups_.erase(group.offset());
        }

        // and if it's not allocated, this request must be a retry request
    }

    return SpaceOk;
}

SpaceErrCode VolumeSpace::PersistBlockGroup(const BlockGroup& group) {
    return storage_->PutBlockGroup(fsId_, group.offset(), group);
}

SpaceErrCode VolumeSpace::PersistBlockGroups(
    const std::vector<BlockGroup>& blockGroups) {
    SpaceErrCode err = SpaceOk;
    for (const auto& group : blockGroups) {
        err = PersistBlockGroup(group);

        // TODO(wuhanqing): handle error, and rollback if necessary
        if (err != SpaceOk) {
            LOG(ERROR) << "Put block group failed, fsId: " << fsId_
                       << ", offset: " << group.offset()
                       << ", err: " << SpaceErrCode_Name(err);
            return err;
        }
    }

    return err;
}

SpaceErrCode VolumeSpace::ClearBlockGroup(const BlockGroup& group) {
    auto err = storage_->RemoveBlockGroup(fsId_, group.offset());
    if (err != SpaceOk) {
        LOG(WARNING) << "Remove block group failed, fsId: " << fsId_
                     << ", offset: " << group.offset()
                     << ", err: " << SpaceErrCode_Name(err);
    }

    return err;
}

SpaceErrCode VolumeSpace::RemoveAllBlockGroups() {
    LOG(INFO) << "Going to remove all block groups from backend storage, fsId: "
              << fsId_;

    LockGuard lk(mtx_);
    for (auto& group : allocatedGroups_) {
        // TODO(wuhanqing): clear all block groups once by prefix
        auto err = ClearBlockGroup(group.second);
        if (err != SpaceOk && err != SpaceErrNotFound) {
            return err;
        }
    }

    for (auto& group : availableGroups_) {
        auto err = ClearBlockGroup(group.second);
        if (err != SpaceOk && err != SpaceErrNotFound) {
            return err;
        }
    }

    allocatedGroups_.clear();
    availableGroups_.clear();
    cleanGroups_.clear();

    return SpaceOk;
}

bool VolumeSpace::UpdateFsInfo(uint64_t origin, uint64_t extended) {
    FsInfoWrapper fsInfo;
    auto ret = fsStorage_->Get(fsId_, &fsInfo);
    if (ret != FSStatusCode::OK) {
        LOG(WARNING) << "Fail to get fs info from storage, fsId: " << fsId_;
        return false;
    }

    fsInfo.SetCapacity(fsInfo.GetCapacity() + (extended - origin));
    fsInfo.SetVolumeSize(extended);
    ret = fsStorage_->Update(fsInfo);
    if (ret != FSStatusCode::OK) {
        LOG(WARNING) << "Fail to update fs info, fsId: " << fsId_;
        return false;
    }

    return true;
}

void VolumeSpace::AddCleanGroups(uint64_t origin, uint64_t extended) {
    for (auto offset = origin; offset < extended;
         offset += volume_.blockgroupsize()) {
        cleanGroups_.insert(offset);
    }
}

void VolumeSpace::CalBlockGroupAvailableForDeAllocate() {
    LockGuard lk(mtx_);
    LockGuard statlk(statmtx_);
    // check whether deallocatingGroups_ need move to availableGroups_
    auto iter = deallocatingGroups_.begin();
    while (iter != deallocatingGroups_.end()) {
        uint32_t expectMetaserverNum =
            metaserverNum_.load(std::memory_order_acquire);
        uint32_t actualMetaserverNum = iter->second.deallocated().size();
        if (actualMetaserverNum != expectMetaserverNum) {
            ++iter;
            continue;
        }

        LOG(INFO)
            << "VolumeSpace move deallocatingGroups_ to availableGroups_, "
               "fsId="
            << fsId_ << ", block group offset=" << iter->first;

        iter->second.clear_deallocated();
        auto err = PersistBlockGroup(iter->second);
        if (err != SpaceOk) {
            LOG(ERROR) << "VolumeSpace put block group failed, fsId=" << fsId_
                       << ", block group offset=" << iter->first
                       << ", err=" << SpaceErrCode_Name(err);
            continue;
        }

        availableGroups_.emplace(iter->first, std::move(iter->second));
        iter = deallocatingGroups_.erase(iter);
        metric_.dealloc << 1;
    }

    // check whether the cal conditions are met
    if (!waitDeallocateGroups_.empty() || !deallocatingGroups_.empty() ||
        availableGroups_.empty() || summary_.empty()) {
        LOG_EVERY_N(INFO, 50)
            << "VolumeSpace wait for cal, "
               "waitDeallocateGroups_ size="
            << waitDeallocateGroups_.size()
            << ",deallocatingGroups_ size=" << deallocatingGroups_.size()
            << ",availableGroups_ size=" << availableGroups_.size()
            << ",summary_ size=" << summary_.size();
        return;
    }

    // get the keys shared by availableGroups_ and summary_
    std::vector<std::pair<uint64_t, uint64_t>> commonKeys;
    for (const auto &item : summary_) {
        if (availableGroups_.count(item.first)) {
            commonKeys.push_back(item);
        }
    }

    // sort
    std::sort(commonKeys.begin(), commonKeys.end(),
              [](const std::pair<uint64_t, uint64_t> &a,
                 const std::pair<uint64_t, uint64_t> &b) {
                  return a.second > b.second;
              });
    uint64_t selectKey = commonKeys[0].first;
    LOG(INFO) << "VolumeSpace cal blockgroup=" << selectKey
              << ",fsid=" << fsId_ << " wait for deallocate";

    // move key from availableGroups_ to waitDeallocateGroups_
    BlockGroup selectGroup;
    auto it = availableGroups_.find(selectKey);
    if (it != availableGroups_.end()) {
        assert(!it->second.has_owner());
        selectGroup = std::move(it->second);
        selectGroup.clear_owner();
        availableGroups_.erase(it);
    }
    assert(waitDeallocateGroups_.count(selectKey) == 0);
    waitDeallocateGroups_.emplace(selectKey, std::move(selectGroup));
    metric_.waitingDealloc << 1;
}

SpaceErrCode VolumeSpace::ExtendVolume() {
    if (!volume_.autoextend()) {
        LOG(WARNING) << "Auto extend is not supported, fsId: " << fsId_
                     << ", volume: " << volume_.volumename();
        return SpaceErrNotSupport;
    }

    const auto origin = volume_.volumesize();
    const auto extended =
        ExtendedSize(origin, volume_.extendfactor(), volume_.extendalignment());

    LOG(INFO) << "Going to extend volume size from " << volume_.volumesize()
              << " to " << extended;

    auto *proxy = MdsProxyManager::GetInstance().GetOrCreateProxy(
        {volume_.cluster().begin(), volume_.cluster().end()});
    if (proxy == nullptr) {
        LOG(WARNING) << "Fail to get or create proxy";
        return SpaceErrUnknown;
    }

    auto ret = proxy->ExtendVolume(volume_, extended);
    if (!ret) {
        LOG(WARNING) << "Fail to extend volume";
        return SpaceErrExtendVolumeError;
    }

    if (!UpdateFsInfo(origin, extended)) {
        LOG(WARNING) << "Fail to update fs info";
        return SpaceErrStorage;
    }

    volume_.set_volumesize(extended);
    AddCleanGroups(origin, extended);

    LOG(INFO) << "Extended volume size from " << origin << " to " << extended;

    return SpaceOk;
}

void VolumeSpace::Run() {
    calThread_ = std::thread([&] {
        while (sleeper_.wait_for(std::chrono::seconds(calcIntervalSec_))) {
            CalBlockGroupAvailableForDeAllocate();
        }
    });
}

void VolumeSpace::Stop() {
    LOG(INFO) << "VolumeSpace stopping, fsid=" << fsId_;

    sleeper_.interrupt();
    calThread_.join();

    LOG(INFO) << "VolumeSpace stopped, fsid=" << fsId_;
}

bool VolumeSpace::UpdateDeallocatableBlockGroup(
    uint32_t metaserverId, uint32_t metaserverNum,
    const DeallocatableBlockGroupVec &groups,
    const BlockGroupDeallcateStatusMap &stats, uint64_t *issue) {
    uint32_t current = metaserverNum_.load(std::memory_order_acquire);
    metaserverNum_.compare_exchange_strong(current, metaserverNum,
                                           std::memory_order_acq_rel);

    UpdateBlockGroupDeallocatableSpace(metaserverId, groups);

    UpdateDeallocatingBlockGroup(metaserverId, stats);

    return SelectBlockGroupForDeAllocate(metaserverId, issue);
}

void VolumeSpace::UpdateBlockGroupDeallocatableSpace(
    uint32_t metaserverId, const DeallocatableBlockGroupVec &groups) {
    LockGuard statlk(statmtx_);

    // update summary_ with latest groups
    std::unordered_map<uint64_t, uint64_t> reportGroups;
    for (auto &group : groups) {
        auto offset = group.blockgroupoffset();
        auto deallocatableSize = group.deallocatablesize();
        reportGroups[offset] = deallocatableSize;

        auto iter = summary_.find(offset);
        if (iter == summary_.end()) {
            summary_.emplace(offset, deallocatableSize);
        } else {
            iter->second += group.deallocatablesize();
        }
        VLOG(6) << "VolumeSpace update summary, fsId=" << fsId_
                << ", blockGroupOffset=" << offset
                << ", deallocatableSize=" << group.deallocatablesize();
    }

    // remove groups from last round of reporting and record latest in
    // lastUpdate_
    auto lastUpdateIter = lastUpdate_.find(metaserverId);
    if (lastUpdateIter == lastUpdate_.end()) {
        lastUpdate_[metaserverId] = groups;
    } else {
        for (auto &group : lastUpdateIter->second) {
            auto offset = group.blockgroupoffset();
            auto lastDeallocatableSize = group.deallocatablesize();
            if (reportGroups.count(offset) == 0) {
                continue;
            }

            summary_[offset] -= lastDeallocatableSize;
            if (summary_[offset] == 0) {
                summary_.erase(offset);
                LOG(INFO) << "VolumeSpace remove block group from summary, no "
                             "need deallocatable, "
                             "fsId="
                          << fsId_ << ", blcokGroupOffset=" << offset;
            }

            group.set_deallocatablesize(reportGroups[offset]);
        }
    }
}

void VolumeSpace::UpdateDeallocatingBlockGroup(
    uint32_t metaserverId, const BlockGroupDeallcateStatusMap &stats) {
    LockGuard lk(mtx_);

    // get completed deallocate blockgroup
    std::vector<uint64_t> doneGroups;
    for (auto &stat : stats) {
        auto offset = stat.first;
        auto status = stat.second;

        auto iter = deallocatingGroups_.find(offset);
        if (iter == deallocatingGroups_.end()) {
            LOG(ERROR) << "VolumeSpace block group not found in "
                          "deallocatingGroups_, "
                          "fsId: "
                       << fsId_ << ", blcokGroupOffset: " << offset;
            continue;
        }

        auto arlreadyDeallocated =
            std::find(iter->second.deallocated().begin(),
                      iter->second.deallocated().end(), metaserverId);
        if (status == BlockGroupDeallcateStatusCode::BGDP_DONE &&
            arlreadyDeallocated == iter->second.deallocated().end()) {
            doneGroups.emplace_back(offset);
            LOG(INFO) << "VolumeSpace block group is deallocated done, fsId: "
                      << fsId_ << ", blcokGroupOffset: " << offset
                      << ", metaserverId: " << metaserverId;
        }
    }

    // update the metaserver from the deallocating state of the blockgroup to
    // the deallocated state
    for (auto offset : doneGroups) {
        auto iter = deallocatingGroups_.find(offset);
        assert(iter != deallocatingGroups_.end());

        std::vector<uint32_t> newDeallocating;
        for (auto id : iter->second.deallocating()) {
            if (metaserverId == id) {
                continue;
            }
            newDeallocating.emplace_back(id);
        }

        iter->second.add_deallocated(metaserverId);
        auto mutableDeallocating = iter->second.mutable_deallocating();
        mutableDeallocating->Resize(newDeallocating.size(), 0);
        std::copy(newDeallocating.begin(), newDeallocating.end(),
                  mutableDeallocating->begin());

        auto err = PersistBlockGroup(iter->second);
        // TODO(wuhanqing): handle error, and rollback if necessary
        if (err != SpaceOk) {
            LOG(ERROR) << "VolumeSpace put block group failed, fsId: " << fsId_
                       << ", offset: " << offset
                       << ", err: " << SpaceErrCode_Name(err);
            return;
        }
    }
}

bool VolumeSpace::SelectBlockGroupForDeAllocate(uint32_t metaserverId,
                                                uint64_t *issue) {
    assert(issue != nullptr);
    LockGuard lk(mtx_);

    // TODO(ilixiaocui): support more groups to be issued
    if (!waitDeallocateGroups_.empty()) {
        auto iter = waitDeallocateGroups_.begin();
        auto offset = iter->first;
        iter->second.add_deallocating(metaserverId);
        deallocatingGroups_[offset] = std::move(iter->second);
        waitDeallocateGroups_.erase(iter);
        metric_.waitingDealloc << -1;

        auto err = PersistBlockGroup(deallocatingGroups_[offset]);
        // TODO(wuhanqing): handle error, and rollback if necessary
        if (err != SpaceOk) {
            LOG(ERROR) << "VolumeSpace put block group failed, fsId: " << fsId_
                       << ", offset: " << offset
                       << ", err: " << SpaceErrCode_Name(err);
            return false;
        }
        *issue = offset;
        LOG(INFO) << "VolumeSpace issue block group from "
                     "waitDeallocateGroups_, fsId: "
                  << fsId_ << ", offset: " << offset
                  << ", to metaserverId: " << metaserverId;
        return true;
    }

    auto deallocatingOne = deallocatingGroups_.begin();
    if (deallocatingOne != deallocatingGroups_.end()) {
        *issue = deallocatingOne->first;

        auto alreadyIssued = std::find(
            deallocatingOne->second.deallocating().begin(),
            deallocatingOne->second.deallocating().end(), metaserverId);
        if (alreadyIssued != deallocatingOne->second.deallocating().end()) {
            return true;
        }

        auto alreadyDone = std::find(
            deallocatingOne->second.deallocated().begin(),
            deallocatingOne->second.deallocated().end(), metaserverId);
        if (alreadyDone != deallocatingOne->second.deallocated().end()) {
            return false;
        }

        deallocatingOne->second.add_deallocating(metaserverId);
        auto err = PersistBlockGroup(deallocatingOne->second);
        // TODO(wuhanqing): handle error, and rollback if necessary
        if (err != SpaceOk) {
            LOG(ERROR) << "VolumeSpace put block group failed, fsId: " << fsId_
                       << ", offset: " << *issue
                       << ", err: " << SpaceErrCode_Name(err);
            return false;
        }

        LOG(INFO) << "VolumeSpace issue block group from "
                     "deallocatingGroups_, fsId: "
                  << fsId_ << ", offset: " << *issue
                  << ", to metaserverId: " << metaserverId;
        return true;
    }

    return false;
}

uint64_t ExtendedSize(uint64_t origin, double factor, uint64_t alignment) {
    return common::align_up(
        static_cast<uint64_t>(std::floor(static_cast<double>(origin) * factor)),
        alignment);
}

}  // namespace space
}  // namespace mds
}  // namespace curvefs
