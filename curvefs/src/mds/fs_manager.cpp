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
 * Project: curve
 * Created Date: 2021-05-19
 * Author: chenwei
 */

#include "curvefs/src/mds/fs_manager.h"
#include <glog/logging.h>
#include <google/protobuf/util/message_differencer.h>
#include <sys/stat.h>  // for S_IFDIR
#include <limits>
#include "curvefs/src/mds/metric/fs_metric.h"
#include "curvefs/src/mds/common/types.h"

namespace curvefs {
namespace mds {

using ::curvefs::common::FSType;
using NameLockGuard = ::curve::common::GenericNameLockGuard<Mutex>;
using curvefs::mds::topology::TopoStatusCode;

bool FsManager::Init() {
    LOG_IF(FATAL, !spaceClient_->Init()) << "spaceClient Init fail";
    LOG_IF(FATAL, !fsStorage_->Init()) << "fsStorage Init fail";

    return true;
}

void FsManager::Uninit() {
    fsStorage_->Uninit();
    spaceClient_->Uninit();
}

FSStatusCode FsManager::CreateFs(const std::string& fsName,
                                 FSType fsType, uint64_t blockSize,
                                 const FsDetail& detail, FsInfo* fsInfo) {
    NameLockGuard lock(nameLock_, fsName);
    FsInfoWrapper wrapper;
    bool skipCreateNewFs = false;

    // 1. query fs
    // TODO(cw123): if fs status is FsStatus::New, here need more consideration
    if (fsStorage_->Exist(fsName)) {
        int existRet = IsExactlySameOrCreateUnComplete(fsName, fsType,
                                                       blockSize, detail);
        if (existRet == 0) {
            LOG(INFO) << "CreateFs success, fs exist, fsName = " << fsName
                         << ", fstype = " << FSType_Name(fsType)
                         << ", blocksize = " << blockSize
                         << ", detail = " << detail.ShortDebugString();
            fsStorage_->Get(fsName, &wrapper);
            *fsInfo = wrapper.ProtoFsInfo();
            return FSStatusCode::OK;
        }

        if (existRet == 1) {
            LOG(INFO) << "CreateFs found previous create operation uncompleted"
                  << ", fsName = " << fsName
                  << ", fstype = " << FSType_Name(fsType)
                  << ", blocksize = " << blockSize
                  << ", detail = " << detail.ShortDebugString();
            skipCreateNewFs = true;
        } else {
            return FSStatusCode::FS_EXIST;
        }
    }

    if (!skipCreateNewFs) {
        uint32_t fsId = fsStorage_->NextFsId();
        if (fsId == INVALID_FS_ID) {
            LOG(ERROR) << "Generator fs id failed, fsName = " << fsName;
            return FSStatusCode::INTERNAL_ERROR;
        }

        wrapper =
            GenerateFsInfoWrapper(fsName, fsId, blockSize, GetRootId(), detail);

        FSStatusCode ret = fsStorage_->Insert(wrapper);
        if (ret != FSStatusCode::OK) {
            LOG(ERROR) << "CreateFs fail, insert fs fail, fsName = " << fsName
                       << ", ret = " << FSStatusCode_Name(ret);
            return ret;
        }
    } else {
        fsStorage_->Get(fsName, &wrapper);
    }

    uint32_t uid = 0;                 // TODO(cw123)
    uint32_t gid = 0;                 // TODO(cw123)
    uint32_t mode = S_IFDIR | 01777;  // TODO(cw123)

    // create partition
    auto ret = FSStatusCode::OK;
    PartitionInfo partition;
    TopoStatusCode topoRet = topoManager_->CreatePartitionsAndGetMinPartition(
        wrapper.GetFsId(), &partition);
    if (TopoStatusCode::TOPO_OK != topoRet) {
        LOG(ERROR) << "CreateFs fail, create partition fail"
                   << ", fsId = " << wrapper.GetFsId();
        ret = FSStatusCode::CREATE_PARTITION_ERROR;
    } else {
        // get copyset members
        std::set<std::string> addrs;
        if (TopoStatusCode::TOPO_OK != topoManager_->GetCopysetMembers(
                partition.poolid(), partition.copysetid(), &addrs)) {
            LOG(ERROR) << "CreateFs fail, get copyset members fail,"
                       << " poolId = " << partition.poolid()
                       << ", copysetId = " << partition.copysetid();
            ret = FSStatusCode::UNKNOWN_ERROR;
        } else {
            ret = metaserverClient_->CreateRootInode(wrapper.GetFsId(),
                    partition.poolid(), partition.copysetid(),
                    partition.partitionid(), uid, gid, mode, addrs);
        }
    }
    if (ret != FSStatusCode::OK && ret != FSStatusCode::INODE_EXIST) {
        LOG(ERROR) << "CreateFs fail, insert root inode fail"
                   << ", fsName = " << fsName
                   << ", ret = " << FSStatusCode_Name(ret);
        // TODO(wanghai): need delete partition if created
        FSStatusCode ret2 = fsStorage_->Delete(fsName);
        if (ret2 != FSStatusCode::OK) {
            LOG(ERROR) << "CreateFs fail, insert root inode fail,"
                       << " then delete fs fail, fsName = " << fsName
                       << ", errCode = " << ret2;
            return ret2;
        }
        return ret;
    }

    wrapper.SetStatus(FsStatus::INITED);

    // for persistence consider
    ret = fsStorage_->Update(wrapper);
    if (ret != FSStatusCode::OK) {
        LOG(ERROR) << "CreateFs fail, update fs to inited fail"
                   << ", fsName = " << fsName
                   << ", ret = " << FSStatusCode_Name(ret);
        // TODO(wanghai): delete partiton and inode
        // ret = metaserverClient_->DeleteInode(wrapper.GetFsId(), GetRootId());
        // if (ret != FSStatusCode::OK) {
        //     LOG(ERROR) << "CreateFs fail, update fs status to inited fail"
        //                << ", then delete root inode fail"
        //                << ", fsName = " << fsName
        //                << ", ret = " << FSStatusCode_Name(ret);
        //     return ret;
        // }

        // ret = fsStorage_->Delete(fsName);
        // if (ret != FSStatusCode::OK) {
        //     LOG(ERROR) << "CreateFs fail, insert root inode fail, "
        //                << "then delete fs fail, fsName = " << fsName
        //                << ", errCode = " << FSStatusCode_Name(ret);
        // }
        return ret;
    }

    *fsInfo = wrapper.ProtoFsInfo();
    return FSStatusCode::OK;
}

FSStatusCode FsManager::DeleteFs(const std::string& fsName) {
    NameLockGuard lock(nameLock_, fsName);

    // 1. query fs
    FsInfoWrapper wrapper;
    FSStatusCode ret = fsStorage_->Get(fsName, &wrapper);
    if (ret != FSStatusCode::OK) {
        LOG(WARNING) << "DeleteFs fail, get fs fail, fsName = " << fsName
                     << ", errCode = " << FSStatusCode_Name(ret);
        return ret;
    }

    // 2. check mount point num
    if (!wrapper.IsMountPointEmpty()) {
        LOG(WARNING) << "DeleteFs fail, mount point exist, fsName = " << fsName;
        for (auto& it : wrapper.MountPoints()) {
            LOG(WARNING) << "mountpoint [" << it << "] exist";
        }
        return FSStatusCode::FS_BUSY;
    }

    // 3. check fs status
    FsStatus status = wrapper.GetStatus();
    switch (status) {
        case FsStatus::NEW:
        case FsStatus::INITED:
            // update fs status to deleting
            wrapper.SetStatus(FsStatus::DELETING);
            // for persistence consider
            ret = fsStorage_->Update(wrapper);
            if (ret != FSStatusCode::OK) {
                LOG(ERROR) << "DeleteFs fail, update fs to deleting fail"
                           << ", fsName = " << fsName
                           << ", ret = " << FSStatusCode_Name(ret);
                return ret;
            }
            break;
        case FsStatus::DELETING:
            LOG(WARNING) << "DeleteFs already in deleting, fsName = " << fsName;
            break;
        default:
            LOG(ERROR) << "DeleteFs fs in wrong status, fsName = " << fsName
                       << ", fs status = " << FsStatus_Name(status);
            return FSStatusCode::UNKNOWN_ERROR;
    }

    // 4. use metaserver interface, delete inode and dentry, todo
    ret = CleanFsInodeAndDentry(wrapper.GetFsId());
    if (ret != FSStatusCode::OK) {
        LOG(ERROR) << "DeleteFs fail, clean inode and dentry fail"
                   << ", fsName = " << fsName
                   << ", ret = " << FSStatusCode_Name(ret);
        return ret;
    }

    // 5. use space interface, destory space, todo

    // 6. delete fs
    ret = fsStorage_->Delete(fsName);
    if (ret != FSStatusCode::OK) {
        LOG(WARNING) << "DeleteFs fail, delete fs fail, fsName = " << fsName
                     << ", errCode = " << ret;
        return ret;
    }

    return FSStatusCode::OK;
}

FSStatusCode FsManager::MountFs(const std::string& fsName,
                                const std::string& mountpoint, FsInfo* fsInfo) {
    NameLockGuard lock(nameLock_, fsName);

    // 1. query fs
    FsInfoWrapper wrapper;
    FSStatusCode ret = fsStorage_->Get(fsName, &wrapper);
    if (ret != FSStatusCode::OK) {
        LOG(WARNING) << "MountFs fail, get fs fail, fsName = " << fsName
                     << ", errCode = " << FSStatusCode_Name(ret);
        return ret;
    }

    // 2. check fs status
    FsStatus status = wrapper.GetStatus();
    switch (status) {
        case FsStatus::NEW:
            LOG(WARNING) << "MountFs fs is not inited, fsName = " << fsName;
            return FSStatusCode::NOT_INITED;
        case FsStatus::INITED:
            // inited status, go on process
            break;
        case FsStatus::DELETING:
            LOG(WARNING) << "MountFs fs is in deleting, fsName = " << fsName;
            return FSStatusCode::UNDER_DELETRING;
        default:
            LOG(ERROR) << "MountFs fs in wrong status, fsName = " << fsName
                       << ", fs status = " << FsStatus_Name(status);
            return FSStatusCode::UNKNOWN_ERROR;
    }

    // 3. if mount point exist, return MOUNT_POINT_EXIST
    if (wrapper.IsMountPointExist(mountpoint)) {
        LOG(WARNING) << "MountFs fail, mount point exist, fsName = " << fsName
                     << ", mountpoint = " << mountpoint;
        return FSStatusCode::MOUNT_POINT_EXIST;
    }

    // 4. If this is the first mountpoint, init space,
    if (wrapper.GetFsType() == FSType::TYPE_VOLUME &&
        wrapper.IsMountPointEmpty()) {
        FsInfo tempFsInfo = wrapper.ProtoFsInfo();
        ret = spaceClient_->InitSpace(tempFsInfo);
        if (ret != FSStatusCode::OK) {
            LOG(ERROR) << "MountFs fail, init space fail, fsName = " << fsName
                       << ", mountpoint = " << mountpoint
                       << ", errCode = " << FSStatusCode_Name(ret);
            return ret;
        }
    }

    // 5. insert mountpoint
    wrapper.AddMountPoint(mountpoint);
    // for persistence consider
    ret = fsStorage_->Update(wrapper);
    if (ret != FSStatusCode::OK) {
        LOG(WARNING) << "MountFs fail, update fs fail, fsName = " << fsName
                     << ", mountpoint = " << mountpoint
                     << ", errCode = " << FSStatusCode_Name(ret);
        return ret;
    }

    // 6. convert fs info
    *fsInfo = wrapper.ProtoFsInfo();

    FsMetric::GetInstance().OnMount(wrapper.GetFsName(), mountpoint);

    return FSStatusCode::OK;
}

FSStatusCode FsManager::UmountFs(const std::string& fsName,
                                 const std::string& mountpoint) {
    NameLockGuard lock(nameLock_, fsName);

    // 1. query fs
    FsInfoWrapper wrapper;
    FSStatusCode ret = fsStorage_->Get(fsName, &wrapper);
    if (ret != FSStatusCode::OK) {
        LOG(WARNING) << "UmountFs fail, get fs fail, fsName = " << fsName
                     << ", errCode = " << FSStatusCode_Name(ret);
        return ret;
    }

    // 2. umount
    if (!wrapper.IsMountPointExist(mountpoint)) {
        ret = FSStatusCode::MOUNT_POINT_NOT_EXIST;
        LOG(WARNING) << "UmountFs fail, mount point not exist, fsName = "
                     << fsName << ", mountpoint = " << mountpoint
                     << ", errCode = " << FSStatusCode_Name(ret);
        return ret;
    }

    ret = wrapper.DeleteMountPoint(mountpoint);
    if (ret != FSStatusCode::OK) {
        LOG(WARNING) << "UmountFs fail, delete mount point fail, fsName = "
                     << fsName << ", mountpoint = " << mountpoint
                     << ", errCode = " << FSStatusCode_Name(ret);
        return ret;
    }

    // 3. if no mount point exist, uninit space
    if (wrapper.GetFsType() == FSType::TYPE_VOLUME &&
        wrapper.IsMountPointEmpty()) {
        ret = spaceClient_->UnInitSpace(wrapper.GetFsId());
        if (ret != FSStatusCode::OK) {
            LOG(ERROR) << "UmountFs fail, uninit space fail, fsName = "
                       << fsName << ", mountpoint = " << mountpoint
                       << ", errCode = " << FSStatusCode_Name(ret);
            return ret;
        }
    }

    // 4. update fs info
    // for persistence consider
    ret = fsStorage_->Update(wrapper);
    if (ret != FSStatusCode::OK) {
        LOG(WARNING) << "UmountFs fail, update fs fail, fsName = " << fsName
                     << ", mountpoint = " << mountpoint
                     << ", errCode = " << FSStatusCode_Name(ret);
        return ret;
    }

    FsMetric::GetInstance().OnUnMount(fsName, mountpoint);

    return FSStatusCode::OK;
}

FSStatusCode FsManager::GetFsInfo(const std::string& fsName, FsInfo* fsInfo) {
    // 1. query fs
    FsInfoWrapper wrapper;
    FSStatusCode ret = fsStorage_->Get(fsName, &wrapper);
    if (ret != FSStatusCode::OK) {
        LOG(WARNING) << "GetFsInfo fail, get fs fail, fsName = " << fsName
                     << ", errCode = " << FSStatusCode_Name(ret);
        return ret;
    }

    *fsInfo = wrapper.ProtoFsInfo();
    return FSStatusCode::OK;
}

FSStatusCode FsManager::GetFsInfo(uint32_t fsId, FsInfo* fsInfo) {
    // 1. query fs
    FsInfoWrapper wrapper;
    FSStatusCode ret = fsStorage_->Get(fsId, &wrapper);
    if (ret != FSStatusCode::OK) {
        LOG(WARNING) << "GetFsInfo fail, get fs fail, fsId = " << fsId
                     << ", errCode = " << FSStatusCode_Name(ret);
        return ret;
    }

    *fsInfo = wrapper.ProtoFsInfo();
    return FSStatusCode::OK;
}

FSStatusCode FsManager::GetFsInfo(const std::string& fsName, uint32_t fsId,
                                  FsInfo* fsInfo) {
    // 1. query fs by fsName
    FsInfoWrapper wrapper;
    FSStatusCode ret = fsStorage_->Get(fsName, &wrapper);
    if (ret != FSStatusCode::OK) {
        LOG(WARNING) << "GetFsInfo fail, get fs fail, fsName = " << fsName
                     << ", errCode = " << FSStatusCode_Name(ret);
        return ret;
    }

    // 2. check fsId
    if (wrapper.GetFsId() != fsId) {
        LOG(WARNING) << "GetFsInfo fail, fsId missmatch, fsName = " << fsName
                     << ", param fsId = " << fsId
                     << ", fsInfo.fsId = " << wrapper.GetFsId();
        return FSStatusCode::PARAM_ERROR;
    }

    *fsInfo = wrapper.ProtoFsInfo();
    return FSStatusCode::OK;
}

int FsManager::IsExactlySameOrCreateUnComplete(const std::string& fsName,
                                           FSType fsType, uint64_t blocksize,
                                           const FsDetail& detail) {
    FsInfoWrapper existFs;

    // assume fsname exists
    fsStorage_->Get(fsName, &existFs);
    if (fsName == existFs.GetFsName() && fsType == existFs.GetFsType() &&
        blocksize == existFs.GetBlockSize() &&
        google::protobuf::util::MessageDifferencer::Equals(detail,
        existFs.GetFsDetail())) {
        if (FsStatus::NEW == existFs.GetStatus()) {
            return 1;
        } else if (FsStatus::INITED == existFs.GetStatus()) {
            return 0;
        }
    }
    return -1;
}

uint64_t FsManager::GetRootId() { return ROOTINODEID; }

FSStatusCode FsManager::CleanFsInodeAndDentry(uint32_t fsId) {
    // TODO(cw123) : to be implemented
    LOG(WARNING) << "CleanFsInodeAndDentry not implemented, return OK!";
    return FSStatusCode::OK;
}

}  // namespace mds
}  // namespace curvefs
