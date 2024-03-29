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
 * Date: Wednesday Mar 02 20:39:51 CST 2022
 * Author: wuhanqing
 */

#include <glog/logging.h>
#include <utility>
#include "absl/memory/memory.h"
#include "curvefs/proto/space.pb.h"
#include "curvefs/src/volume/block_group_manager.h"
#include "curvefs/src/volume/space_manager.h"

namespace curvefs {
namespace volume {

using ::curve::common::Bitmap;
using ::curve::common::BITMAP_UNIT_SIZE;
using ::curve::common::BitRange;
using ::curve::common::WriteLockGuard;
using ::curvefs::common::BitmapLocation;
using ::curvefs::mds::space::BlockGroup;
using ::curvefs::mds::space::SpaceErrCode_Name;

BlockGroupManagerImpl::BlockGroupManagerImpl(
    SpaceManager* spaceManager,
    const std::shared_ptr<MdsClient>& mdsClient,
    const std::shared_ptr<BlockDeviceClient>& blockDevice,
    const BlockGroupManagerOption& managerOption,
    const AllocatorOption& allocatorOption)
    : spaceManager_(spaceManager),
      mdsClient_(mdsClient),
      blockDeviceClient_(blockDevice),
      option_(managerOption),
      allocatorOption_(allocatorOption) {}

bool BlockGroupManagerImpl::AllocateBlockGroup(
    std::vector<AllocatorAndBitmapUpdater>* out) {
    std::vector<BlockGroup> groups;
    auto err = mdsClient_->AllocateVolumeBlockGroup(
        option_.fsId, option_.blockGroupAllocateOnce, option_.owner, &groups);

    LOG_IF(ERROR, err != SpaceErrCode::SpaceOk)
        << "Allocate volume block group failed, err: "
        << SpaceErrCode_Name(err);

    if (groups.empty()) {
        LOG(ERROR)
            << "Allocate volume block group failed, no block group allocated";
        return false;
    }

    WriteLockGuard lk(groupsLock_);
    for (auto& group : groups) {
        VLOG(9) << "load group: " << group.ShortDebugString();
        AllocatorAndBitmapUpdater res;
        res.blockGroupOffset = group.offset();
        BlockGroupBitmapLoader loader(blockDeviceClient_.get(),
                                      option_.blockSize, allocatorOption_,
                                      group);
        auto ret = loader.Load(&res);
        if (!ret) {
            LOG(ERROR) << "Create allocator for block group failed";
            return false;
        } else {
            out->push_back(std::move(res));
        }

        groups_.emplace_back(std::move(group));
    }

    return true;
}

bool BlockGroupManagerImpl::AcquireBlockGroup(uint64_t blockGroupOffset,
                                              AllocatorAndBitmapUpdater *out) {
    BlockGroup group;
    SpaceErrCode err = mdsClient_->AcquireVolumeBlockGroup(
        option_.fsId, blockGroupOffset, option_.owner, &group);
    if (err != SpaceErrCode::SpaceOk) {
        LOG(WARNING) << "Acquire volume block group failed, err: "
                     << SpaceErrCode_Name(err);
        return false;
    }

    AllocatorAndBitmapUpdater res;
    res.blockGroupOffset = group.offset();
    BlockGroupBitmapLoader loader(blockDeviceClient_.get(), option_.blockSize,
                                  allocatorOption_, group);
    auto ret = loader.Load(&res);
    if (!ret) {
        LOG(ERROR) << "Create allocator for block group failed";
        return false;
    }

    *out = std::move(res);
    return true;
}


bool BlockGroupManagerImpl::ReleaseBlockGroup(uint64_t blockGroupOffset,
                                              uint64_t available) {
    WriteLockGuard lk(groupsLock_);
    auto iter = std::find_if(groups_.begin(), groups_.end(),
                             [blockGroupOffset](const BlockGroup &group) {
                                 return group.offset() == blockGroupOffset;
                             });
    if (iter == groups_.end()) {
        LOG(ERROR) << "BlockGroupManagerImpl find group block: "
                   << blockGroupOffset << " failed";
        return false;
    }

    iter->set_available(available);

    SpaceErrCode err = mdsClient_->ReleaseVolumeBlockGroup(
        option_.fsId, option_.owner, {*iter});
    if (err != SpaceErrCode::SpaceOk) {
        LOG(WARNING) << "BlockGroupManagerImpl release block group: "
                     << blockGroupOffset
                     << "failed, err: " << SpaceErrCode_Name(err)
                     << ", blockGroupOffset: " << blockGroupOffset;
        return false;
    }

    groups_.erase(iter);
    LOG(INFO) << "BlockGroupManagerImpl release one volume block group="
              << blockGroupOffset << " successfully";
    return true;
}

bool BlockGroupManagerImpl::ReleaseAllBlockGroups() {
    WriteLockGuard lk(groupsLock_);
    SpaceErrCode err = mdsClient_->ReleaseVolumeBlockGroup(
        option_.fsId, option_.owner, groups_);
    if (err != SpaceErrCode::SpaceOk) {
        LOG(WARNING) << "BlockGroupManagerImpl release all volume block groups "
                        "failed, err: "
                     << SpaceErrCode_Name(err) << "";
        return false;
    }

    groups_.clear();
    LOG(INFO) << "BlockGroupManagerImpl release all volume block groups "
                 "successfully";
    return true;
}

void BlockGroupManagerImpl::AllocateBlockGroupAsync() {}

}  // namespace volume
}  // namespace curvefs
