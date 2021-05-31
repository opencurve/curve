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
#include <glog/logging.h>
#include "curvefs/src/mds/fs_manager.h"
namespace curvefs {
namespace mds {
FSStatusCode FsManager::CreateFs(std::string fsName, uint64_t blockSize,
                curvefs::common::Volume volume, FsInfo* fsInfo) {
    // 1、query fs
    if (fsStorage_->Exist(fsName)) {
        LOG(WARNING) << "CreateFs fail, fs exist, fsName = " << fsName
                  << ", blockSize = " << blockSize;
        return FSStatusCode::FS_EXIST;
    }

    // 2、create fs
    MdsFsInfo newFsInfo(GetNextFsId(), fsName, GetRootId(), volume.volumesize(),
                        blockSize, volume);

    // 3、insert fs
    FSStatusCode ret = fsStorage_->Insert(newFsInfo);
    if (ret != FSStatusCode::OK) {
        LOG(WARNING) << "CreateFs fail, insert fs fail, fsName = " << fsName
                   << ", blockSize = " << blockSize
                   << ",ret = " << FSStatusCode_Name(ret);
    }

    newFsInfo.ConvertToProto(fsInfo);
    return FSStatusCode::OK;
}

FSStatusCode FsManager::DeleteFs(std::string fsName) {
    // 1、query fs
    MdsFsInfo mdsFsInfo;
    FSStatusCode ret = fsStorage_->Get(fsName, &mdsFsInfo);
    if (ret != FSStatusCode::OK) {
        LOG(WARNING) << "DeleteFs fail, get fs fail, fsName = " << fsName
                  << ", errCode = " << FSStatusCode_Name(ret);
        return ret;
    }

    // 2、check mount point num
    if (!mdsFsInfo.MountPointEmpty()) {
        LOG(WARNING) << "DeleteFs fail, mount point exist, fsName = " << fsName;
        for (auto it : mdsFsInfo.GetMountPointList()) {
            LOG(WARNING) << "mountPoint [host = " + it.host()
                         << ", mountdir = " + it.mountdir() + "] exist";
        }
        return FSStatusCode::FS_BUSY;
    }

    // 3、use space interface, destory space, todo

    // 4、delete fs
    ret = fsStorage_->Delete(fsName);
    if (ret != FSStatusCode::OK) {
        LOG(WARNING) << "DeleteFs fail, delelte fs faile, fsName = " << fsName
                     << ", errCode = " << ret;
        return ret;
    }

    return FSStatusCode::OK;
}

FSStatusCode FsManager::MountFs(std::string fsName, MountPoint mountpoint,
                        FsInfo* fsInfo) {
    // 1、query fs
    MdsFsInfo mdsFsInfo;
    FSStatusCode ret = fsStorage_->Get(fsName, &mdsFsInfo);
    if (ret != FSStatusCode::OK) {
        LOG(WARNING) << "MountFs fail, get fs fail, fsName = " << fsName
                  << ", errCode = " << FSStatusCode_Name(ret);
        return ret;
    }

    // 2、if mount point exist, return MOUNT_POINT_EXIST
    if (mdsFsInfo.MountPointExist(mountpoint)) {
        LOG(WARNING) << "MountFs fail, mount point exist, fsName = " << fsName
                  << ", host = " << mountpoint.host()
                  << ", mountdir = " << mountpoint.mountdir();
        return FSStatusCode::MOUNT_POINT_EXIST;
    }

    // 3、If this is the first mountpoint, init space,
    if (mdsFsInfo.MountPointEmpty()) {
        FsInfo tempFsInfo;
        mdsFsInfo.ConvertToProto(&tempFsInfo);
        ret = spaceClient_->InitSpace(tempFsInfo);
        if (ret != FSStatusCode::OK) {
            LOG(ERROR) << "MountFs fail, init space fail, fsName = " << fsName
                       << ", errCode = " << FSStatusCode_Name(ret);
            return ret;
        }
    }

    // 4、insert mountpoint
    mdsFsInfo.AddMountPoint(mountpoint);
    ret = fsStorage_->Update(mdsFsInfo);
    if (ret != FSStatusCode::OK) {
        LOG(WARNING) << "MountFs fail, update fs fail, fsName = " << fsName
                       << ", errCode = " << FSStatusCode_Name(ret);
        return ret;
    }

    // 5、返回fs info
    mdsFsInfo.ConvertToProto(fsInfo);
    return FSStatusCode::OK;
}

FSStatusCode FsManager::UmountFs(std::string fsName, MountPoint mountpoint) {
    // 1、query fs
    MdsFsInfo mdsFsInfo;
    FSStatusCode ret = fsStorage_->Get(fsName, &mdsFsInfo);
    if (ret != FSStatusCode::OK) {
        LOG(WARNING) << "UmountFs fail, get fs fail, fsName = " << fsName
                  << ", errCode = " << FSStatusCode_Name(ret);
        return ret;
    }

    // 2、umount
    ret = mdsFsInfo.DeleteMountPoint(mountpoint);
    if (ret != FSStatusCode::OK) {
        LOG(WARNING) << "UmountFs fail, delete mount point fail, fsName = " << fsName  // NOLINT
                  << ", errCode = " << FSStatusCode_Name(ret);
        return ret;
    }

    ret = fsStorage_->Update(mdsFsInfo);
    if (ret != FSStatusCode::OK) {
        LOG(WARNING) << "UmountFs fail, update fs fail, fsName = " << fsName
                  << ", errCode = " << FSStatusCode_Name(ret);
        return ret;
    }

    if (mdsFsInfo.MountPointEmpty()) {
        ret = spaceClient_->UnInitSpace(mdsFsInfo.GetFsId());
        if (ret != FSStatusCode::OK) {
            LOG(ERROR) << "UmountFs fail, uninit space fail, fsName = " << fsName  // NOLINT
                       << ", errCode = " << FSStatusCode_Name(ret);
            return ret;
        }
    }

    return FSStatusCode::OK;
}

FSStatusCode FsManager::GetFsInfo(std::string fsName, FsInfo* fsInfo) {
    // 1、query fs
    MdsFsInfo mdsFsInfo;
    FSStatusCode ret = fsStorage_->Get(fsName, &mdsFsInfo);
    if (ret != FSStatusCode::OK) {
        LOG(WARNING) << "GetFsInfo fail, get fs fail, fsName = " << fsName
                  << ", errCode = " << FSStatusCode_Name(ret);
        return ret;
    }

    mdsFsInfo.ConvertToProto(fsInfo);
    return FSStatusCode::OK;
}

FSStatusCode FsManager::GetFsInfo(uint32_t fsId, FsInfo* fsInfo) {
    // 1、query fs
    MdsFsInfo mdsFsInfo;
    FSStatusCode ret = fsStorage_->Get(fsId, &mdsFsInfo);
    if (ret != FSStatusCode::OK) {
        LOG(WARNING) << "GetFsInfo fail, get fs fail, fsId = " << fsId
                  << ", errCode = " << FSStatusCode_Name(ret);
        return ret;
    }

    mdsFsInfo.ConvertToProto(fsInfo);
    return FSStatusCode::OK;
}

FSStatusCode FsManager::GetFsInfo(std::string fsName, uint32_t fsId,
                                FsInfo* fsInfo) {
    // 1、query fs by fsName
    MdsFsInfo mdsFsInfo;
    FSStatusCode ret = fsStorage_->Get(fsName, &mdsFsInfo);
    if (ret != FSStatusCode::OK) {
        LOG(WARNING) << "GetFsInfo fail, get fs fail, fsName = " << fsName
                  << ", errCode = " << FSStatusCode_Name(ret);
        return ret;
    }

    // 2、check if
    if (mdsFsInfo.GetFsId() != fsId) {
        LOG(WARNING) << "GetFsInfo fail, fsId missmatch, fsName = " << fsName
                     << ", param fsId = " << fsId
                     << ", fsInfo.fsId = " << mdsFsInfo.GetFsId();
        return FSStatusCode::PARAM_ERROR;
    }

    mdsFsInfo.ConvertToProto(fsInfo);
    return FSStatusCode::OK;
}

uint32_t FsManager::GetNextFsId() {
    return nextFsId_.fetch_add(1, std::memory_order_relaxed);
}

uint64_t FsManager::GetRootId() {
    return 0;
}

}  // namespace mds
}  // namespace curvefs
