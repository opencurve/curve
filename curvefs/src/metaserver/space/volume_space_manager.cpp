/*
 *  Copyright (c) 2023 NetEase Inc.
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
 * Date: Tue Mar 28 19:55:27 CST 2023
 * Author: lixiaocui
 */

#include "curvefs/src/metaserver/space/volume_space_manager.h"
#include "curvefs/src/metaserver/mds/fsinfo_manager.h"

namespace curvefs {
namespace metaserver {

using curvefs::volume::SpaceManagerImpl;

void VolumeSpaceManager::Uninit() {
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto &item : spaceManagers_) {
        item.second->Shutdown();
    }
}

void VolumeSpaceManager::Destroy(uint32_t fsId) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = spaceManagers_.find(fsId);
    if (it != spaceManagers_.end()) {
        spaceManagers_.erase(it);
    }
}

uint64_t VolumeSpaceManager::GetBlockGroupSize(uint32_t fsId) {
    auto spaceManager = GetSpaceManager(fsId);
    if (spaceManager == nullptr) {
        LOG(ERROR) << "VolumeSpaceManager get block group size failed, could "
                      "not get space manager, fsId "
                   << fsId << " not exist";
        return 0;
    }
    return spaceManager->GetBlockGroupSize();
}

bool VolumeSpaceManager::DeallocVolumeSpace(
    uint32_t fsId, uint64_t blockGroupOffset,
    const std::vector<Extent> &deallocatableVolumeSpace) {
    auto spaceManager = GetSpaceManager(fsId);
    if (spaceManager == nullptr) {
        LOG(ERROR) << "VolumeSpaceManager dealloc volume space failed, could "
                      "not get space manager, fsId "
                   << fsId << " not exist";
        return false;
    }
    return spaceManager->DeAlloc(blockGroupOffset, deallocatableVolumeSpace);
}

std::shared_ptr<SpaceManager>
VolumeSpaceManager::GetSpaceManager(uint32_t fsId) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = spaceManagers_.find(fsId);
    if (it == spaceManagers_.end()) {
        if (!GenerateSpaceManager(fsId)) {
            return nullptr;
        }
    }
    return spaceManagers_[fsId];
}

bool VolumeSpaceManager::GenerateSpaceManager(uint32_t fsId) {
    FsInfo fsInfo;
    bool ret = FsInfoManager::GetInstance().GetFsInfo(fsId, &fsInfo);
    if (!ret) {
        LOG(ERROR) << "VolumeSpaceManager generate space manager failed, fsId "
                   << fsId << " not exist";
        return false;
    }

    // init block device client
    auto blockDeviceClient = std::make_shared<BlockDeviceClientImpl>();
    ret = blockDeviceClient->Init(options_.deviceOpt);
    if (!ret) {
        LOG(ERROR) << "VolumeSpaceManager init block device failed";
        return false;
    }
    const auto &vol = fsInfo.detail().volume();
    const auto &volName = vol.volumename();
    const auto &user = vol.user();
    ret = blockDeviceClient->Open(volName, user);
    if (!ret) {
        LOG(ERROR) << "VolumeSpaceManager open failed, volName=" << volName
                   << ", user=" << user;
        return false;
    }

    // init space manager option
    SpaceManagerOption spaceManagerOpt;
    spaceManagerOpt.blockGroupManagerOption.fsId = fsInfo.fsid();
    spaceManagerOpt.blockGroupManagerOption.blockGroupSize =
        fsInfo.detail().volume().blockgroupsize();
    spaceManagerOpt.blockGroupManagerOption.blockSize =
        fsInfo.detail().volume().blocksize();
    spaceManagerOpt.allocatorOption.type = "bitmap";

    spaceManagers_[fsId] = std::make_shared<SpaceManagerImpl>(
        spaceManagerOpt, options_.mdsClient, blockDeviceClient);

    return true;
}

}  // namespace metaserver
}  // namespace curvefs
