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
 * Date: Fri Mar 24 17:09:28 CST 2023
 * Author: lixiaocui
 */

#ifndef CURVEFS_SRC_METASERVER_SPACE_VOLUME_SPACE_MANAGER_H_
#define CURVEFS_SRC_METASERVER_SPACE_VOLUME_SPACE_MANAGER_H_

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "curvefs/src/volume/space_manager.h"

namespace curvefs {
namespace metaserver {
using curvefs::client::rpcclient::MdsClient;
using curvefs::volume::BlockDeviceClientOptions;
using curvefs::volume::BlockDeviceClientImpl;
using curvefs::volume::Extent;
using curvefs::volume::SpaceManager;
using curvefs::volume::SpaceManagerOption;

struct VolumeSpaceManagerOptions {
    std::shared_ptr<MdsClient> mdsClient;
    BlockDeviceClientOptions deviceOpt;
};

class VolumeSpaceManager {
 public:
    void Init(VolumeSpaceManagerOptions options) { options_ = options; }

    void Uninit();

    virtual bool
    DeallocVolumeSpace(uint32_t fsId, uint64_t volumeOffset,
                       const std::vector<Extent> &deallocatableVolumeSpace);

    virtual void Destroy(uint32_t fsId);

    virtual uint64_t GetBlockGroupSize(uint32_t fsId);

 private:
    std::shared_ptr<SpaceManager> GetSpaceManager(uint32_t fsId);

    bool GenerateSpaceManager(uint32_t fsId);

 private:
    std::mutex mutex_;
    std::map<uint32_t, std::shared_ptr<SpaceManager>> spaceManagers_;
    VolumeSpaceManagerOptions options_;
};

}  // namespace metaserver
}  // namespace curvefs

#endif  // CURVEFS_SRC_METASERVER_SPACE_VOLUME_SPACE_MANAGER_H_
