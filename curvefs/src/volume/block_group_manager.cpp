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

#include "curvefs/src/volume/block_group_manager.h"

#include <glog/logging.h>

#include <utility>

#include "absl/memory/memory.h"
#include "curvefs/proto/space.pb.h"

namespace curvefs {
namespace volume {

using ::curve::common::Bitmap;
using ::curve::common::BITMAP_UNIT_SIZE;
using ::curve::common::BitRange;
using ::curvefs::common::BitmapLocation;
using ::curvefs::mds::space::SpaceErrCode_Name;

BlockGroupManagerImpl::BlockGroupManagerImpl(
    SpaceManager* spaceManager, const std::shared_ptr<MdsClient>& mdsClient,
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
    if (err != SpaceErrCode::SpaceOk) {
        LOG(ERROR) << "Allocate volume block group failed, err: "
                   << SpaceErrCode_Name(err);
        return false;
    } else if (groups.empty()) {
        LOG(ERROR)
            << "Allocate volume block group failed, no block group allocated";
        return false;
    }

    for (const auto& group : groups) {
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
    }

    return true;
}

bool BlockGroupManagerImpl::ReleaseAllBlockGroups(
    const std::vector<BlockGroup>& blockGroups) {
    SpaceErrCode err = mdsClient_->ReleaseVolumeBlockGroup(
        option_.fsId, option_.owner, blockGroups);
    if (err != SpaceErrCode::SpaceOk) {
        LOG(ERROR) << "Release volume block group failed, err: "
                   << SpaceErrCode_Name(err);
        return false;
    }

    return true;
}

bool BlockGroupManagerImpl::ReleaseBlockGroup(const BlockGroup& blockGroup) {
    SpaceErrCode err = mdsClient_->ReleaseVolumeBlockGroup(
        option_.fsId, option_.owner, {blockGroup});
    if (err != SpaceErrCode::SpaceOk) {
        LOG(ERROR) << "Release volume block group failed, err: "
                   << SpaceErrCode_Name(err);
        return false;
    }

    return true;
}

void BlockGroupManagerImpl::AllocateBlockGroupAsync() {}

bool BlockGroupManagerImpl::AcquireBlockGroup(uint64_t offset,
                                              AllocatorAndBitmapUpdater* out) {
    BlockGroup group;
    SpaceErrCode err = mdsClient_->AcquireVolumeBlockGroup(
        option_.fsId, offset, option_.owner, &group);
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

}  // namespace volume
}  // namespace curvefs
