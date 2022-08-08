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
                                                 const Volume& volume,
                                                 BlockGroupStorage* storage,
                                                 FsStorage* fsStorage) {
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
    for (auto& group : groups) {
        usedGroupOffsets.insert(group.offset());
        // availableSize += group.available();

        auto offset = group.offset();
        assert(offset % blockGroupSize == 0);
        assert(group.size() == blockGroupSize);
        assert(group.bitmaplocation() == location);
        if (group.has_owner()) {
            space->allocatedGroups_.emplace(offset, std::move(group));
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

    LOG(INFO) << "Init volume space success, fsId: " << fsId
              << ", size: " << volumeSize << ", available: " << availableSize
              << ", block size: " << blockSize
              << ", block group size: " << blockGroupSize
              << ", total groups: " << volumeSize / blockGroupSize
              << ", allocated groups: " << space->allocatedGroups_.size()
              << ", available groups: " << space->availableGroups_.size()
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

    auto* proxy = MdsProxyManager::GetInstance().GetOrCreateProxy(
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

uint64_t ExtendedSize(uint64_t origin, double factor, uint64_t alignment) {
    return common::align_up(
        static_cast<uint64_t>(std::floor(static_cast<double>(origin) * factor)),
        alignment);
}

}  // namespace space
}  // namespace mds
}  // namespace curvefs
