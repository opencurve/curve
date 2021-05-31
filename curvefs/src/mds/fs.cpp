/*
 *  Copyright (c) 2021 NetEase Inc.
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
 * @Date: 2021-06-04 14:11:16
 * @Author: chenwei
 */

#include "curvefs/src/mds/fs.h"
namespace curvefs {
namespace mds {
MdsFsInfo::MdsFsInfo(uint32_t fsId, std::string fsName, uint64_t rootInodeId,
        uint64_t capacity, uint64_t blockSize, const common::Volume& volume) {
    fsId_ = fsId;
    fsName_ = fsName;
    rootInodeId_ = rootInodeId;
    capacity_ = capacity;
    blockSize_ = blockSize;
    volume_.CopyFrom(volume);
    mountNum_ = 0;
}

void MdsFsInfo::ConvertToProto(FsInfo *file) {
    // ReadLockGuard readLockGuard(rwLock_);
    file->set_fsid(fsId_);
    file->set_fsname(fsName_);
    file->set_rootinodeid(rootInodeId_);
    file->set_capacity(capacity_);
    file->set_blocksize(blockSize_);
    file->set_mountnum(mountNum_);
    file->mutable_volume()->CopyFrom(volume_);
    *file->mutable_mountpoints() = {mountPointList_.begin(),
                                   mountPointList_.end()};
    return;
}

std::list<MountPoint> MdsFsInfo::GetMountPointList() {
    return mountPointList_;
}

bool MdsFsInfo::MountPointEmpty() {
    return mountNum_ == 0;
}

bool MdsFsInfo::MountPointExist(const MountPoint& mountpoint) {
    // ReadLockGuard readLockGuard(rwLock_);
    for (auto it : mountPointList_) {
        if (it.host() == mountpoint.host()
            && it.mountdir() == mountpoint.mountdir()) {
            return true;
        }
    }
    return false;
}

void MdsFsInfo::AddMountPoint(const MountPoint& mountpoint) {
    // WriteLockGuard writeLockGuard(rwLock_);
    mountPointList_.push_back(mountpoint);
    mountNum_++;
    return;
}

FSStatusCode MdsFsInfo::DeleteMountPoint(const MountPoint& mountpoint) {
    // WriteLockGuard writeLockGuard(rwLock_);
    for (auto it = mountPointList_.begin(); it != mountPointList_.end(); it++) {
        if (it->host() == mountpoint.host()
            && it->mountdir() == mountpoint.mountdir()) {
            mountPointList_.erase(it);
            mountNum_--;
            return FSStatusCode::OK;
        }
    }
    return FSStatusCode::NOT_FOUND;
}

uint32_t MdsFsInfo::GetFsId() const {
    return fsId_;
}

std::string MdsFsInfo::GetFsName() const {
    return fsName_;
}

Volume MdsFsInfo::GetVolumeInfo() {
    return volume_;
}
}  // namespace mds
}  // namespace curvefs
