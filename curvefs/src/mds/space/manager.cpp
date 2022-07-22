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

#include "curvefs/src/mds/space/manager.h"
#include <glog/logging.h>

#include <utility>

#include "curvefs/proto/mds.pb.h"
#include "curvefs/src/mds/space/volume_space.h"

namespace curvefs {
namespace mds {
namespace space {

using ::curve::common::ReadLockGuard;
using ::curve::common::WriteLockGuard;

using NameLockGuard = ::curve::common::GenericNameLockGuard<Mutex>;

AbstractVolumeSpace* SpaceManagerImpl::GetVolumeSpace(uint32_t fsId) const {
    ReadLockGuard lk(rwlock_);
    auto it = volumes_.find(fsId);
    if (it == volumes_.end()) {
        return nullptr;
    }

    return it->second.get();
}

SpaceErrCode SpaceManagerImpl::AddVolume(const FsInfo& fsInfo) {
    CHECK(fsInfo.detail().has_volume());

    NameLockGuard lock(namelock_, fsInfo.fsname());

    {
        ReadLockGuard lk(rwlock_);
        if (volumes_.count(fsInfo.fsid()) != 0) {
            return SpaceErrCode::SpaceErrExist;
        }
    }

    auto space = VolumeSpace::Create(fsInfo.fsid(), fsInfo.detail().volume(),
                                     storage_.get(), fsStorage_.get());

    if (!space) {
        LOG(ERROR) << "Create volume space failed, fsId: " << fsInfo.fsid();
        return SpaceErrCreate;
    }

    {
        WriteLockGuard lk(rwlock_);
        volumes_.emplace(fsInfo.fsid(), std::move(space));
    }

    LOG(INFO) << "Added volume space, fsName: " << fsInfo.fsname()
              << ", fsId: " << fsInfo.fsid();

    return SpaceOk;
}

SpaceErrCode SpaceManagerImpl::RemoveVolume(uint32_t fsId) {
    WriteLockGuard lk(rwlock_);
    auto it = volumes_.find(fsId);
    if (it == volumes_.end()) {
        return SpaceErrNotFound;
    }

    // TODO(wuhanqing): move this out of lock
    volumes_.erase(it);
    return SpaceOk;
}

SpaceErrCode SpaceManagerImpl::DeleteVolume(uint32_t fsId) {
    // remove all persist block groups
    WriteLockGuard lk(rwlock_);
    auto it = volumes_.find(fsId);
    if (it == volumes_.end()) {
        return SpaceErrNotFound;
    }

    auto err = it->second->RemoveAllBlockGroups();
    if (err != SpaceOk) {
        return err;
    }

    volumes_.erase(it);
    return SpaceOk;
}

}  // namespace space
}  // namespace mds
}  // namespace curvefs
