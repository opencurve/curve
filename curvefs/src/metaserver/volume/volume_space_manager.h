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
 * @Date: 2022-12-15 10:37:07
 * @Author: chenwei
 */
#ifndef CURVEFS_SRC_METASERVER_VOLUME_VOLUME_SPACE_MANAGER_H_
#define CURVEFS_SRC_METASERVER_VOLUME_VOLUME_SPACE_MANAGER_H_

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "curvefs/src/volume/space_manager.h"

namespace curvefs {
namespace metaserver {
using curvefs::client::rpcclient::MdsClient;
using curvefs::volume::BlockDeviceClient;
using curvefs::volume::Extent;
using curvefs::volume::SpaceManager;
using curvefs::volume::SpaceManagerOption;

struct VolumeSpaceManagerOptions {
    std::string host;  // ip:port
    std::shared_ptr<MdsClient> mdsClient;
    std::shared_ptr<BlockDeviceClient> blockDeviceClient;
};

class VolumeSpaceManager {
 public:
    void Init(VolumeSpaceManagerOptions options) { options_ = options; }
    std::shared_ptr<SpaceManager> GetOrGenerateSpaceManager(uint32_t fsId);
    int RemoveSpaceManager(uint32_t fsId);
    bool DeallocVolumeSpace(uint32_t fsId, uint64_t offset,
                           const std::vector<Extent>& extents);

 private:
    std::shared_ptr<SpaceManager> GenerateSpaceManager(uint32_t fsId);

 private:
    std::mutex mutex_;
    std::map<uint32_t, std::shared_ptr<SpaceManager>> spaceManagers_;
    VolumeSpaceManagerOptions options_;
};

}  // namespace metaserver
}  // namespace curvefs

#endif  // CURVEFS_SRC_METASERVER_VOLUME_VOLUME_SPACE_MANAGER_H_
