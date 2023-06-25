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
 * Date: Friday Feb 25 17:21:10 CST 2022
 * Author: wuhanqing
 */

#ifndef CURVEFS_SRC_MDS_SPACE_MANAGER_H_
#define CURVEFS_SRC_MDS_SPACE_MANAGER_H_

#include <cstdint>
#include <memory>
#include <unordered_map>
#include <utility>

#include "curvefs/proto/mds.pb.h"
#include "curvefs/proto/space.pb.h"
#include "curvefs/src/mds/common/types.h"
#include "curvefs/src/mds/fs_storage.h"
#include "curvefs/src/mds/space/block_group_storage.h"
#include "curvefs/src/mds/space/volume_space.h"
#include "src/common/concurrent/name_lock.h"

namespace curvefs {
namespace mds {
namespace space {

class SpaceManager {
 public:
    virtual ~SpaceManager() = default;

    virtual AbstractVolumeSpace* GetVolumeSpace(uint32_t fsId) const = 0;
    virtual SpaceErrCode AddVolume(const FsInfo& fsInfo) = 0;
    virtual SpaceErrCode RemoveVolume(uint32_t fsId) = 0;
    virtual SpaceErrCode DeleteVolume(uint32_t fsId) = 0;
};

class SpaceManagerImpl final : public SpaceManager {
 public:
    SpaceManagerImpl(
        const std::shared_ptr<curve::kvstorage::StorageClient>& kvstore,
        std::shared_ptr<FsStorage> fsStorage)
        : storage_(new BlockGroupStorageImpl(kvstore)),
          fsStorage_(std::move(fsStorage)) {}

    SpaceManagerImpl(const SpaceManagerImpl&) = delete;
    SpaceManagerImpl& operator=(const SpaceManagerImpl&) = delete;

    AbstractVolumeSpace* GetVolumeSpace(uint32_t fsId) const override;

    SpaceErrCode AddVolume(const FsInfo& fsInfo) override;

    SpaceErrCode RemoveVolume(uint32_t fsId) override;

    SpaceErrCode DeleteVolume(uint32_t fsId) override;

 private:
    mutable RWLock rwlock_;

    // key is fs id
    std::unordered_map<uint32_t, std::unique_ptr<VolumeSpace>> volumes_;

    std::unique_ptr<BlockGroupStorage> storage_;

    curve::common::GenericNameLock<Mutex> namelock_;

    std::shared_ptr<FsStorage> fsStorage_;
};

}  // namespace space
}  // namespace mds
}  // namespace curvefs

#endif  // CURVEFS_SRC_MDS_SPACE_MANAGER_H_
