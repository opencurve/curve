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
 * Date: Wednesday Mar 16 15:44:00 CST 2022
 * Author: wuhanqing
 */

#ifndef CURVEFS_SRC_CLIENT_VOLUME_DEFAULT_VOLUME_STORAGE_H_
#define CURVEFS_SRC_CLIENT_VOLUME_DEFAULT_VOLUME_STORAGE_H_

#include <bvar/bvar.h>

#include <memory>

#include "curvefs/src/client/volume/metric.h"
#include "curvefs/src/client/volume/volume_storage.h"
#include "curvefs/src/volume/block_device_client.h"
#include "curvefs/src/volume/space_manager.h"
#include "curvefs/src/client/filesystem/error.h"
#include "curvefs/src/client/filesystem/meta.h"

namespace curvefs {
namespace client {

using ::curvefs::volume::BlockDeviceClient;
using ::curvefs::volume::SpaceManager;
using ::curvefs::client::filesystem::CURVEFS_ERROR;
using ::curvefs::client::filesystem::FileOut;

class InodeCacheManager;

// `DefaultVolumeStorage` implements from `VolumeStorage`
// for write operation, data is written directly to the backend storage,
// meta-data is cached
class DefaultVolumeStorage final : public VolumeStorage {
 public:
    DefaultVolumeStorage(SpaceManager* spaceManager,
                         BlockDeviceClient* blockDeviceClient,
                         InodeCacheManager* inodeCacheManager)
        : spaceManager_(spaceManager),
          blockDeviceClient_(blockDeviceClient),
          inodeCacheManager_(inodeCacheManager),
          metric_("default_volume_storage") {}

    DefaultVolumeStorage(const DefaultVolumeStorage&) = delete;
    DefaultVolumeStorage& operator=(const DefaultVolumeStorage&) = delete;

    ~DefaultVolumeStorage() override = default;

    CURVEFS_ERROR Read(uint64_t ino,
                       off_t offset,
                       size_t len,
                       char* data) override;

    CURVEFS_ERROR Write(uint64_t ino,
                        off_t offset,
                        size_t len,
                        const char* data,
                        FileOut* fileOut) override;

    CURVEFS_ERROR Flush(uint64_t ino) override;

    bool Shutdown() override;

 private:
    SpaceManager* spaceManager_;
    BlockDeviceClient* blockDeviceClient_;
    InodeCacheManager* inodeCacheManager_;
    VolumeStorageMetric metric_;
};

}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_VOLUME_DEFAULT_VOLUME_STORAGE_H_
