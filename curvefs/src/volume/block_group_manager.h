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
 * Date: Wednesday Mar 02 20:31:16 CST 2022
 * Author: wuhanqing
 */

#ifndef CURVEFS_SRC_VOLUME_BLOCK_GROUP_MANAGER_H_
#define CURVEFS_SRC_VOLUME_BLOCK_GROUP_MANAGER_H_

#include <memory>
#include <vector>

#include "curvefs/proto/common.pb.h"
#include "curvefs/src/client/rpcclient/mds_client.h"
#include "curvefs/src/volume/block_device_client.h"
#include "curvefs/src/volume/block_group_loader.h"
#include "curvefs/src/volume/common.h"
#include "curvefs/src/volume/option.h"
#include "src/common/bitmap.h"

namespace curvefs {
namespace volume {

using ::curvefs::client::rpcclient::MdsClient;

class SpaceManager;

class BlockGroupManager {
 public:
    virtual ~BlockGroupManager() = default;

    virtual bool AllocateBlockGroup(
        std::vector<AllocatorAndBitmapUpdater>* out) = 0;

    virtual bool ReleaseAllBlockGroups() = 0;
};

class BlockGroupManagerImpl final : public BlockGroupManager {
 public:
    BlockGroupManagerImpl(SpaceManager* spaceManager,
                          const std::shared_ptr<MdsClient>& mdsClient,
                          const std::shared_ptr<BlockDeviceClient>& blockDevice,
                          const BlockGroupManagerOption& managerOption,
                          const AllocatorOption& allocatorOption);

    bool AllocateBlockGroup(std::vector<AllocatorAndBitmapUpdater>* out);

    void AcquireBlockGroup();

    void AllocateBlockGroupAsync();

    bool ReleaseAllBlockGroups();

 private:
    SpaceManager* spaceManager_;
    std::shared_ptr<MdsClient> mdsClient_;
    std::shared_ptr<BlockDeviceClient> blockDeviceClient_;

    BlockGroupManagerOption option_;
    AllocatorOption allocatorOption_;
};

}  // namespace volume
}  // namespace curvefs

#endif  // CURVEFS_SRC_VOLUME_BLOCK_GROUP_MANAGER_H_
