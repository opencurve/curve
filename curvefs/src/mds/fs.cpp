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
void MdsFsInfo::ConvertToProto(FsInfo* file) {
    file->set_fsid(fsId_);
    file->set_fsname(fsName_);
    file->set_status(status_);
    file->set_rootinodeid(rootInodeId_);
    file->set_capacity(capacity_);
    file->set_blocksize(blockSize_);
    file->set_mountnum(mountPointList_.size());
    *file->mutable_mountpoints() = {mountPointList_.begin(),
                                    mountPointList_.end()};
    return;
}

void MdsS3FsInfo::ConvertToProto(FsInfo* file) {
    ReadLockGuard readLockGuard(rwLock_);
    MdsFsInfo::ConvertToProto(file);
    file->set_fstype(FSType::TYPE_S3);
    file->mutable_s3info()->CopyFrom(s3Info_);

    return;
}

void MdsVolumeFsInfo::ConvertToProto(FsInfo* file) {
    ReadLockGuard readLockGuard(rwLock_);
    MdsFsInfo::ConvertToProto(file);
    file->set_fstype(FSType::TYPE_VOLUME);
    file->mutable_volume()->CopyFrom(volume_);

    return;
}

std::list<std::string> MdsFsInfo::GetMountPointList() {
    ReadLockGuard readLockGuard(rwLock_);
    return mountPointList_;
}

bool MdsFsInfo::MountPointEmpty() {
    ReadLockGuard readLockGuard(rwLock_);
    return mountPointList_.empty();
}

bool MdsFsInfo::MountPointExist(const std::string& mountpoint) {
    ReadLockGuard readLockGuard(rwLock_);
    for (const auto& it : mountPointList_) {
        if (it == mountpoint) {
            return true;
        }
    }
    return false;
}

void MdsFsInfo::AddMountPoint(const std::string& mountpoint) {
    WriteLockGuard writeLockGuard(rwLock_);
    mountPointList_.push_back(mountpoint);
    return;
}

FSStatusCode MdsFsInfo::DeleteMountPoint(const std::string& mountpoint) {
    WriteLockGuard writeLockGuard(rwLock_);
    for (auto it = mountPointList_.begin(); it != mountPointList_.end(); it++) {
        if (*it == mountpoint) {
            mountPointList_.erase(it);
            return FSStatusCode::OK;
        }
    }
    return FSStatusCode::NOT_FOUND;
}

uint32_t MdsFsInfo::GetFsId() {
    ReadLockGuard readLockGuard(rwLock_);
    return fsId_;
}

std::string MdsFsInfo::GetFsName() {
    ReadLockGuard readLockGuard(rwLock_);
    return fsName_;
}

void MdsFsInfo::SetStatus(FsStatus status) {
    WriteLockGuard writeLockGuard(rwLock_);
    status_ = status;
}

FsStatus MdsFsInfo::GetStatus() {
    ReadLockGuard readLockGuard(rwLock_);
    return status_;
}

FSType MdsFsInfo::GetFsType() {
    ReadLockGuard readLockGuard(rwLock_);
    return type_;
}

void MdsFsInfo::SetFsType(FSType type) {
    WriteLockGuard writeLockGuard(rwLock_);
    type_ = type;
}

}  // namespace mds
}  // namespace curvefs
