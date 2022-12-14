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
 * @Project: curve
 * @Date: 2022-12-14 16:52:24
 * @Author: chenwei
 */

#include "curvefs/src/metaserver/volume/volume_space_manager.h"

#include "curvefs/src/metaserver/fsinfo_manager.h"

namespace curvefs {
namespace metaserver {

using curvefs::volume::SpaceManagerImpl;

std::shared_ptr<SpaceManager> VolumeSpaceManager::GenerateSpaceManager(
    uint32_t fsId) {
    FsInfo fsInfo;
    bool ret = FsInfoManager::GetInstance().GetFsInfo(fsId, &fsInfo);
    if (!ret) {
        LOG(ERROR) << "GenerateSpaceManager failed, fsId " << fsId
                   << " not exist";
        return nullptr;
    }

    SpaceManagerOption option;
    option.blockGroupManagerOption.fsId = fsInfo.fsid();
    option.blockGroupManagerOption.owner = options_.host;
    return std::make_shared<SpaceManagerImpl>(option, options_.mdsClient,
                                              options_.blockDeviceClient);
}

std::shared_ptr<SpaceManager> VolumeSpaceManager::GetOrGenerateSpaceManager(
    uint32_t fsId) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = spaceManagers_.find(fsId);
    if (it != spaceManagers_.end()) {
        return it->second;
    }
    auto spaceManager = GenerateSpaceManager(fsId);
    if (spaceManager == nullptr) {
        LOG(ERROR) << "GetOrGenerateSpaceManager failed, fsId " << fsId
                   << " GenerateSpaceManager failed";
        return nullptr;
    }
    spaceManagers_.emplace(fsId, spaceManager);
    return spaceManager;
}

int VolumeSpaceManager::RemoveSpaceManager(uint32_t fsId) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = spaceManagers_.find(fsId);
    if (it == spaceManagers_.end()) {
        LOG(ERROR) << "RemoveSpaceManager failed, fsId " << fsId
                   << " not exist";
        return -1;
    }
    spaceManagers_.erase(it);
    return 0;
}

bool VolumeSpaceManager::DeallocVolumeSpace(uint32_t fsId,
                                           uint64_t blockGroupOffset,
                                           const std::vector<Extent>& extents) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = spaceManagers_.find(fsId);
    if (it == spaceManagers_.end()) {
        LOG(ERROR) << "DeallocVolumeSpace failed, fsId " << fsId
                   << " not exist";
        return false;
    }

    return it->second->DeAlloc(blockGroupOffset, extents);
}

}  // namespace metaserver
}  // namespace curvefs
