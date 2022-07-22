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

#include <sys/stat.h>  // for S_IFDIR
#include <glog/logging.h>
#include <google/protobuf/util/message_differencer.h>
#include <string>

#include <limits>
#include <list>
#include <utility>

#include "curvefs/proto/common.pb.h"
#include "curvefs/proto/mds.pb.h"
#include "curvefs/src/mds/common/types.h"
#include "curvefs/src/mds/metric/fs_metric.h"
#include "curvefs/src/mds/space/reloader.h"
#include "curvefs/src/mds/space/mds_proxy_manager.h"

namespace curvefs {
namespace mds {

using ::curvefs::common::FSType;
using ::curvefs::mds::space::Reloader;
using ::curvefs::mds::dlock::LOCK_STATUS;
using ::curvefs::mds::topology::TopoStatusCode;
using ::google::protobuf::util::MessageDifferencer;
using NameLockGuard = ::curve::common::GenericNameLockGuard<Mutex>;

bool FsManager::Init() {
    LOG_IF(FATAL, !fsStorage_->Init()) << "fsStorage Init fail";
    s3Adapter_->Init(option_.s3AdapterOption);
    auto ret = ReloadMountedFsVolumeSpace();
    if (ret != FSStatusCode::OK) {
        LOG(ERROR) << "Reload mounted fs volume space error";
    }
    RebuildTimeRecorder();
    return ret == FSStatusCode::OK;
}

void FsManager::Run() {
    if (isStop_.exchange(false)) {
        backEndThread_ = Thread(&FsManager::BackEndFunc, this);
        checkMountPointThread_ = Thread(&FsManager::BackEndCheckMountPoint,
                                        this);
        LOG(INFO) << "FsManager start running";
    } else {
        LOG(INFO) << "FsManager already is running";
    }
}

void FsManager::Stop() {
    if (!isStop_.exchange(true)) {
        LOG(INFO) << "stop FsManager...";
        sleeper_.interrupt();
        backEndThread_.join();
        checkMountPointSleeper_.interrupt();
        checkMountPointThread_.join();
        LOG(INFO) << "stop FsManager ok.";
    } else {
        LOG(INFO) << "FsManager not running.";
    }
}

void FsManager::Uninit() {
    Stop();
    fsStorage_->Uninit();
    LOG(INFO) << "FsManager Uninit ok.";
}

bool FsManager::DeletePartiton(std::string fsName,
                               const PartitionInfo& partition) {
    LOG(INFO) << "delete fs partition, fsName = " << fsName
              << ", partitionId = " << partition.partitionid();
    // send rpc to metaserver, get copyset members
    std::set<std::string> addrs;
    if (TopoStatusCode::TOPO_OK !=
        topoManager_->GetCopysetMembers(partition.poolid(),
                                        partition.copysetid(), &addrs)) {
        LOG(ERROR) << "delete partition fail, get copyset "
                      "members fail"
                   << ", poolId = " << partition.poolid()
                   << ", copysetId = " << partition.copysetid();
        return false;
    }

    FSStatusCode ret = metaserverClient_->DeletePartition(
        partition.poolid(), partition.copysetid(), partition.partitionid(),
        addrs);
    if (ret != FSStatusCode::OK && ret != FSStatusCode::UNDER_DELETING) {
        LOG(ERROR) << "delete partition fail, fsName = " << fsName
                   << ", partitionId = " << partition.partitionid()
                   << ", errCode = " << FSStatusCode_Name(ret);
        return false;
    }

    return true;
}

bool FsManager::SetPartitionToDeleting(const PartitionInfo& partition) {
    LOG(INFO) << "set partition status to deleting, partitionId = "
              << partition.partitionid();
    TopoStatusCode ret = topoManager_->UpdatePartitionStatus(
        partition.partitionid(), PartitionStatus::DELETING);
    if (ret != TopoStatusCode::TOPO_OK) {
        LOG(ERROR) << "set partition to deleting fail, partitionId = "
                   << partition.partitionid();
        return false;
    }
    return true;
}

void FsManager::ScanFs(const FsInfoWrapper& wrapper) {
    if (wrapper.GetStatus() != FsStatus::DELETING) {
        return;
    }

    std::list<PartitionInfo> partitionList;
    topoManager_->ListPartitionOfFs(wrapper.GetFsId(), &partitionList);
    if (partitionList.empty()) {
        LOG(INFO) << "fs has no partition, delete fs record, fsName = "
                  << wrapper.GetFsName() << ", fsId = " << wrapper.GetFsId();

        if (wrapper.GetFsType() == FSType::TYPE_VOLUME) {
            auto err = spaceManager_->DeleteVolume(wrapper.GetFsId());
            if (err != space::SpaceOk && err != space::SpaceErrNotFound) {
                LOG(ERROR) << "Delete volume space failed, fsId: "
                           << wrapper.GetFsId()
                           << ", err: " << space::SpaceErrCode_Name(err);
                return;
            }
        }

        FSStatusCode ret = fsStorage_->Delete(wrapper.GetFsName());
        if (ret != FSStatusCode::OK) {
            LOG(ERROR) << "delete fs record fail, fsName = "
                       << wrapper.GetFsName()
                       << ", errCode = " << FSStatusCode_Name(ret);
        }

        return;
    }

    for (const PartitionInfo& partition : partitionList) {
        if (partition.status() != PartitionStatus::DELETING) {
            if (!DeletePartiton(wrapper.GetFsName(), partition)) {
                continue;
            }
            SetPartitionToDeleting(partition);
        }
    }
}

void FsManager::BackEndFunc() {
    while (sleeper_.wait_for(
        std::chrono::seconds(option_.backEndThreadRunInterSec))) {
        std::vector<FsInfoWrapper> wrapperVec;
        fsStorage_->GetAll(&wrapperVec);
        for (const FsInfoWrapper& wrapper : wrapperVec) {
            ScanFs(wrapper);
        }
    }
}

void MountPoint2Str(const Mountpoint& in, std::string *out) {
    *out = in.hostname() + ":" + std::to_string(in.port()) + ":" + in.path();
}

bool Str2MountPoint(const std::string& in, Mountpoint* out) {
    std::vector<std::string> vec;
    curve::common::SplitString(in, ":", &vec);
    if (vec.size() != 3) {
        LOG(ERROR) << "split string to mountpoint failed, str = " << in;
        return false;
    }
    out->set_hostname(vec[0]);
    uint32_t port;
    if (!curve::common::StringToUl(vec[1], &port)) {
        LOG(ERROR) << "StringToUl failed, str = " << vec[1];
        return false;
    }
    out->set_port(port);
    out->set_path(vec[2]);
    return true;
}

void FsManager::CheckMountPoint() {
    std::map<std::string, std::pair<std::string, uint64_t>> tmap;
    {
        ReadLockGuard rlock(recorderMutex_);
        tmap = mpTimeRecorder_;
    }
    uint64_t now = ::curve::common::TimeUtility::GetTimeofDaySec();
    for (auto iter = tmap.begin(); iter != tmap.end(); iter++) {
        std::string fsName = iter->second.first;
        std::string mountpath = iter->first;
        if (now - iter->second.second > option_.clientTimeoutSec) {
            Mountpoint mountpoint;
            if (!Str2MountPoint(mountpath, &mountpoint)) {
                LOG(ERROR) << "mountpath to mountpoint failed, mountpath = "
                           << mountpath;
                DeleteClientAliveTime(iter->first);
            } else {
                auto ret = UmountFs(fsName, mountpoint);
                if (ret != FSStatusCode::OK &&
                    ret != FSStatusCode::MOUNT_POINT_NOT_EXIST) {
                    LOG(WARNING) << "umount fs = " << fsName
                                 << " form mountpoint = " << mountpath
                                 << " failed when client timeout";
                } else {
                    LOG(INFO) << "umount fs = " << fsName
                              << " mountpoint = " << mountpath
                              << " success after client timeout.";
                }
            }
        }
    }
}

void FsManager::BackEndCheckMountPoint() {
    while (checkMountPointSleeper_.wait_for(
        std::chrono::seconds(option_.backEndThreadRunInterSec))) {
        CheckMountPoint();
    }
}

FSStatusCode FsManager::CreateFs(const ::curvefs::mds::CreateFsRequest* request,
                                 FsInfo* fsInfo) {
    const auto& fsName = request->fsname();
    const auto& blockSize = request->blocksize();
    const auto& fsType = request->fstype();
    const auto& detail = request->fsdetail();

    NameLockGuard lock(nameLock_, fsName);
    FsInfoWrapper wrapper;
    bool skipCreateNewFs = false;

    // 1. query fs
    // TODO(cw123): if fs status is FsStatus::New, here need more consideration
    if (fsStorage_->Exist(fsName)) {
        int existRet =
            IsExactlySameOrCreateUnComplete(fsName, fsType, blockSize, detail);
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

    // check s3info
    if (detail.has_s3info()) {
        const auto& s3Info = detail.s3info();
        option_.s3AdapterOption.ak = s3Info.ak();
        option_.s3AdapterOption.sk = s3Info.sk();
        option_.s3AdapterOption.s3Address = s3Info.endpoint();
        option_.s3AdapterOption.bucketName = s3Info.bucketname();
        s3Adapter_->Reinit(option_.s3AdapterOption);
        if (!s3Adapter_->BucketExist()) {
            LOG(ERROR) << "CreateFs " << fsName
                       << " error, s3info is not available!";
            return FSStatusCode::S3_INFO_ERROR;
        }
    }

    // fill volume size and segment size
    if (detail.has_volume()) {
        if (!FillVolumeInfo(const_cast<curvefs::mds::CreateFsRequest*>(request)
                                ->mutable_fsdetail()
                                ->mutable_volume())) {
            LOG(WARNING) << "Fail to get volume size";
            return FSStatusCode::VOLUME_INFO_ERROR;
        }

        LOG(INFO) << "Volume info: " << detail.volume().ShortDebugString();
    }

    if (!skipCreateNewFs) {
        uint64_t fsId = fsStorage_->NextFsId();
        if (fsId == INVALID_FS_ID) {
            LOG(ERROR) << "Generator fs id failed, fsName = " << fsName;
            return FSStatusCode::INTERNAL_ERROR;
        }

        wrapper = FsInfoWrapper(request, fsId, GetRootId());

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
    FSStatusCode ret = FSStatusCode::OK;
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
        if (TopoStatusCode::TOPO_OK !=
            topoManager_->GetCopysetMembers(partition.poolid(),
                                            partition.copysetid(), &addrs)) {
            LOG(ERROR) << "CreateFs fail, get copyset members fail,"
                       << " poolId = " << partition.poolid()
                       << ", copysetId = " << partition.copysetid();
            ret = FSStatusCode::UNKNOWN_ERROR;
        } else {
            ret = metaserverClient_->CreateRootInode(
                wrapper.GetFsId(), partition.poolid(), partition.copysetid(),
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

    *fsInfo = std::move(wrapper).ProtoFsInfo();
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
            LOG(WARNING) << "mountpoint [" << it.ShortDebugString()
                         << "] exist";
        }
        return FSStatusCode::FS_BUSY;
    }

    // 3. check fs status
    FsStatus status = wrapper.GetStatus();
    if (status == FsStatus::NEW || status == FsStatus::INITED) {
        FsInfoWrapper newWrapper = wrapper;
        // update fs status to deleting
        newWrapper.SetStatus(FsStatus::DELETING);
        // change fs name to oldname+"_deleting_"+fsid+deletetime
        uint64_t now = ::curve::common::TimeUtility::GetTimeofDaySec();
        newWrapper.SetFsName(fsName + "_deleting_" +
                             std::to_string(wrapper.GetFsId()) + "_" +
                             std::to_string(now));
        // for persistence consider
        ret = fsStorage_->Rename(wrapper, newWrapper);
        if (ret != FSStatusCode::OK) {
            LOG(ERROR) << "DeleteFs fail, update fs to deleting and rename fail"
                       << ", fsName = " << fsName
                       << ", ret = " << FSStatusCode_Name(ret);
            return ret;
        }
        return FSStatusCode::OK;
    } else if (status == FsStatus::DELETING) {
        LOG(WARNING) << "DeleteFs already in deleting, fsName = " << fsName;
        return FSStatusCode::UNDER_DELETING;
    } else {
        LOG(ERROR) << "DeleteFs fs in wrong status, fsName = " << fsName
                   << ", fs status = " << FsStatus_Name(status);
        return FSStatusCode::UNKNOWN_ERROR;
    }

    return FSStatusCode::OK;
}

FSStatusCode FsManager::MountFs(const std::string& fsName,
                                const Mountpoint& mountpoint, FsInfo* fsInfo) {
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
            return FSStatusCode::UNDER_DELETING;
        default:
            LOG(ERROR) << "MountFs fs in wrong status, fsName = " << fsName
                       << ", fs status = " << FsStatus_Name(status);
            return FSStatusCode::UNKNOWN_ERROR;
    }

    // 3. if mount point exist, return MOUNT_POINT_EXIST
    if (wrapper.IsMountPointExist(mountpoint)) {
        LOG(WARNING) << "MountFs fail, mount point exist, fsName = " << fsName
                     << ", mountpoint = " << mountpoint.ShortDebugString();
        return FSStatusCode::MOUNT_POINT_EXIST;
    }

    // 4. If this is the first mountpoint, init space,
    if (wrapper.GetFsType() == FSType::TYPE_VOLUME &&
        wrapper.IsMountPointEmpty()) {
        const auto& tempFsInfo = wrapper.ProtoFsInfo();
        auto ret = spaceManager_->AddVolume(tempFsInfo);
        if (ret != space::SpaceOk) {
            LOG(ERROR) << "MountFs fail, init space fail, fsName = " << fsName
                       << ", mountpoint = " << mountpoint.ShortDebugString()
                       << ", errCode = " << FSStatusCode_Name(INIT_SPACE_ERROR);
            return INIT_SPACE_ERROR;
        }
    }

    // 5. insert mountpoint
    wrapper.AddMountPoint(mountpoint);
    // for persistence consider
    ret = fsStorage_->Update(wrapper);
    if (ret != FSStatusCode::OK) {
        LOG(WARNING) << "MountFs fail, update fs fail, fsName = " << fsName
                     << ", mountpoint = " << mountpoint.ShortDebugString()
                     << ", errCode = " << FSStatusCode_Name(ret);
        return ret;
    }
    // update client alive time
    UpdateClientAliveTime(mountpoint, fsName, false);

    // 6. convert fs info
    FsMetric::GetInstance().OnMount(wrapper.GetFsName(), mountpoint);
    *fsInfo = std::move(wrapper).ProtoFsInfo();

    return FSStatusCode::OK;
}

FSStatusCode FsManager::UmountFs(const std::string& fsName,
                                 const Mountpoint& mountpoint) {
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
                     << fsName
                     << ", mountpoint = " << mountpoint.ShortDebugString()
                     << ", errCode = " << FSStatusCode_Name(ret);
        return ret;
    }

    ret = wrapper.DeleteMountPoint(mountpoint);
    if (ret != FSStatusCode::OK) {
        LOG(WARNING) << "UmountFs fail, delete mount point fail, fsName = "
                     << fsName
                     << ", mountpoint = " << mountpoint.ShortDebugString()
                     << ", errCode = " << FSStatusCode_Name(ret);
        return ret;
    }

    std::string mountpath;
    MountPoint2Str(mountpoint, &mountpath);
    DeleteClientAliveTime(mountpath);

    // 3. if no mount point exist, uninit space
    if (wrapper.GetFsType() == FSType::TYPE_VOLUME &&
        wrapper.IsMountPointEmpty()) {
        auto ret = spaceManager_->RemoveVolume(wrapper.GetFsId());
        if (ret != space::SpaceOk) {
            LOG(ERROR) << "UmountFs fail, uninit space fail, fsName = "
                       << fsName
                       << ", mountpoint = " << mountpoint.ShortDebugString()
                       << ", errCode = "
                       << FSStatusCode_Name(UNINIT_SPACE_ERROR);
            return UNINIT_SPACE_ERROR;
        }
    }

    // 4. update fs info
    // for persistence consider
    ret = fsStorage_->Update(wrapper);
    if (ret != FSStatusCode::OK) {
        LOG(WARNING) << "UmountFs fail, update fs fail, fsName = " << fsName
                     << ", mountpoint = " << mountpoint.ShortDebugString()
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
                                               FSType fsType,
                                               uint64_t blocksize,
                                               const FsDetail& detail) {
    FsInfoWrapper existFs;

    auto volumeInfoComparator = [](common::Volume lhs, common::Volume rhs) {
        // only compare required fields
        // 1. clear `volumeSize` and `extendAlignment`
        // 2. if `autoExtend` is true, `extendFactor` must be equal too
        lhs.clear_volumesize();
        lhs.clear_extendalignment();
        rhs.clear_volumesize();
        rhs.clear_extendalignment();

        return google::protobuf::util::MessageDifferencer::Equals(lhs, rhs);
    };

    auto checkFsInfo = [fsType, volumeInfoComparator](const FsDetail& lhs,
                                                      const FsDetail& rhs) {
        switch (fsType) {
            case curvefs::common::FSType::TYPE_S3:
                return MessageDifferencer::Equals(lhs.s3info(), rhs.s3info());
            case curvefs::common::FSType::TYPE_VOLUME:
                return volumeInfoComparator(lhs.volume(), rhs.volume());
            case curvefs::common::FSType::TYPE_HYBRID:
                return MessageDifferencer::Equals(lhs.s3info(), rhs.s3info()) &&
                       volumeInfoComparator(lhs.volume(), rhs.volume());
        }

        return false;
    };

    // assume fsname exists
    fsStorage_->Get(fsName, &existFs);
    if (fsName == existFs.GetFsName() && fsType == existFs.GetFsType() &&
        blocksize == existFs.GetBlockSize() &&
        checkFsInfo(detail, existFs.GetFsDetail())) {
        if (FsStatus::NEW == existFs.GetStatus()) {
            return 1;
        } else if (FsStatus::INITED == existFs.GetStatus()) {
            return 0;
        }
    }
    return -1;
}

uint64_t FsManager::GetRootId() {
    return ROOTINODEID;
}

void FsManager::GetAllFsInfo(
    ::google::protobuf::RepeatedPtrField<::curvefs::mds::FsInfo>* fsInfoVec) {
    std::vector<FsInfoWrapper> wrapperVec;
    fsStorage_->GetAll(&wrapperVec);
    for (auto const& i : wrapperVec) {
        *fsInfoVec->Add() = i.ProtoFsInfo();
    }
    LOG(INFO) << "get all fsinfo.";
}

void FsManager::RefreshSession(const RefreshSessionRequest* request,
    RefreshSessionResponse* response) {
    if (request->txids_size() != 0) {
        std::vector<PartitionTxId> out;
        std::vector<PartitionTxId> in = {request->txids().begin(),
                                         request->txids().end()};
        topoManager_->GetLatestPartitionsTxId(in, &out);
        *response->mutable_latesttxidlist() = {
            std::make_move_iterator(out.begin()),
            std::make_move_iterator(out.end())};
    }

    // update this client's alive time
    UpdateClientAliveTime(request->mountpoint(), request->fsname());
}

FSStatusCode FsManager::ReloadMountedFsVolumeSpace() {
    std::vector<FsInfoWrapper> allfs;
    fsStorage_->GetAll(&allfs);

    Reloader reloader(spaceManager_.get(), option_.spaceReloadConcurrency);
    for (auto& fs : allfs) {
        if (fs.GetFsType() != FSType::TYPE_VOLUME) {
            continue;
        }

        if (!fs.MountPoints().empty()) {
            reloader.Add(fs.ProtoFsInfo());
        }
    }

    auto err = reloader.Wait();
    if (err != space::SpaceOk) {
        LOG(ERROR) << "Reload volume space failed, err: "
                   << space::SpaceErrCode_Name(err);
        return FSStatusCode::INIT_SPACE_ERROR;
    }

    return FSStatusCode::OK;
}

void FsManager::GetLatestTxId(const uint32_t fsId,
                              std::vector<PartitionTxId>* txIds) {
    std::list<PartitionInfo> list;
    topoManager_->ListPartitionOfFs(fsId, &list);
    for (const auto& item : list) {
        PartitionTxId partitionTxId;
        partitionTxId.set_partitionid(item.partitionid());
        partitionTxId.set_txid(item.txid());
        txIds->push_back(std::move(partitionTxId));
    }
}

FSStatusCode FsManager::IncreaseFsTxSequence(const std::string& fsName,
                                             const std::string& owner,
                                             uint64_t* sequence) {
    FsInfoWrapper wrapper;
    FSStatusCode rc = fsStorage_->Get(fsName, &wrapper);
    if (rc != FSStatusCode::OK) {
        LOG(WARNING) << "Increase fs transaction sequence fail, fsName="
                     << fsName << ", retCode=" << FSStatusCode_Name(rc);
        return rc;
    }

    *sequence = wrapper.IncreaseFsTxSequence(owner);
    rc = fsStorage_->Update(wrapper);
    if (rc != FSStatusCode::OK) {
        LOG(WARNING) << "Increase fs transaction sequence fail, fsName="
                     << fsName << ", retCode=" << FSStatusCode_Name(rc);
        return rc;
    }

    return rc;
}

FSStatusCode FsManager::GetFsTxSequence(const std::string& fsName,
                                        uint64_t* sequence) {
    FsInfoWrapper wrapper;
    FSStatusCode rc = fsStorage_->Get(fsName, &wrapper);
    if (rc != FSStatusCode::OK) {
        LOG(WARNING) << "Get fs transaction sequence fail, fsName="
                     << fsName << ", retCode=" << FSStatusCode_Name(rc);
        return rc;
    }

    *sequence = wrapper.GetFsTxSequence();
    return rc;
}

void FsManager::GetLatestTxId(const GetLatestTxIdRequest* request,
                              GetLatestTxIdResponse* response) {
    std::vector<PartitionTxId> txIds;
    uint32_t fsId = request->fsid();
    if (!request->lock()) {
        GetLatestTxId(fsId, &txIds);
        response->set_statuscode(FSStatusCode::OK);
        *response->mutable_txids() = { txIds.begin(), txIds.end() };
        return;
    }

    // lock for multi-mount rename
    FSStatusCode rc;
    const std::string& fsName = request->fsname();
    const std::string& uuid = request->uuid();
    LOCK_STATUS status = dlock_->Lock(fsName, uuid);
    if (status != LOCK_STATUS::OK) {
        rc = (status == LOCK_STATUS::TIMEOUT) ? FSStatusCode::LOCK_TIMEOUT
                                              : FSStatusCode::LOCK_FAILED;
        response->set_statuscode(rc);
        LOG(WARNING) << "DLock lock failed, fsName=" << fsName
                     << ", uuid=" << uuid << ", retCode="
                     << FSStatusCode_Name(rc);
        return;
    }

    // status = LOCK_STATUS::OK
    NameLockGuard lock(nameLock_, fsName);
    if (!dlock_->CheckOwner(fsName, uuid)) {  // double check
        LOG(WARNING) << "DLock lock failed for owner transfer"
                     << ", fsName=" << fsName << ", owner=" << uuid;
        response->set_statuscode(FSStatusCode::LOCK_FAILED);
        return;
    }

    uint64_t txSequence;
    rc = IncreaseFsTxSequence(fsName, uuid, &txSequence);
    if (rc == FSStatusCode::OK) {
        GetLatestTxId(fsId, &txIds);
        *response->mutable_txids() = { txIds.begin(), txIds.end() };
        response->set_txsequence(txSequence);
        LOG(INFO) << "Acquire dlock success, fsName=" << fsName
                  << ", uuid=" << uuid << ", txSequence=" << txSequence;
    } else {
        LOG(ERROR) << "Increase fs txSequence failed";
    }
    response->set_statuscode(rc);
}

void FsManager::CommitTx(const CommitTxRequest* request,
                         CommitTxResponse* response) {
    std::vector<PartitionTxId> txIds = {
        request->partitiontxids().begin(),
        request->partitiontxids().end(),
    };
    if (!request->lock()) {
        if (topoManager_->CommitTxId(txIds) == TopoStatusCode::TOPO_OK) {
            response->set_statuscode(FSStatusCode::OK);
        } else {
            LOG(ERROR) << "Commit txid failed";
            response->set_statuscode(FSStatusCode::UNKNOWN_ERROR);
        }
        return;
    }

    // lock for multi-mountpoints
    FSStatusCode rc;
    std::string fsName = request->fsname();
    std::string uuid = request->uuid();
    LOCK_STATUS status = dlock_->Lock(fsName, uuid);
    if (status != LOCK_STATUS::OK) {
        rc = (status == LOCK_STATUS::TIMEOUT) ? FSStatusCode::LOCK_TIMEOUT
                                              : FSStatusCode::LOCK_FAILED;
        LOG(WARNING) << "DLock lock failed, fsName=" << fsName
                     << ", uuid=" << uuid << ", retCode="
                     << FSStatusCode_Name(rc);
        response->set_statuscode(rc);
        return;
    }

    // status = LOCK_STATUS::OK
    {
        NameLockGuard lock(nameLock_, fsName);
        if (!dlock_->CheckOwner(fsName, uuid)) {  // double check
            LOG(WARNING) << "DLock lock failed for owner transfer"
                         << ", fsName=" << fsName << ", owner=" << uuid;
            response->set_statuscode(FSStatusCode::LOCK_FAILED);
            return;
        }

        // txSequence mismatch
        uint64_t txSequence;
        rc = GetFsTxSequence(fsName, &txSequence);
        if (rc != FSStatusCode::OK) {
            LOG(ERROR) << "Get fs tx sequence failed";
            response->set_statuscode(rc);
            return;
        } else if (txSequence != request->txsequence()) {
            LOG(ERROR) << "Commit tx with txSequence mismatch, fsName="
                       << fsName << ", uuid=" << uuid << ", current txSequence="
                       << txSequence << ", commit txSequence="
                       << request->txsequence();
            response->set_statuscode(FSStatusCode::COMMIT_TX_SEQUENCE_MISMATCH);
            return;
        }

        // commit txId
        if (topoManager_->CommitTxId(txIds) == TopoStatusCode::TOPO_OK) {
            response->set_statuscode(FSStatusCode::OK);
        } else {
            LOG(ERROR) << "Commit txid failed";
            response->set_statuscode(FSStatusCode::UNKNOWN_ERROR);
        }
    }

    // we can ignore the UnLock result for the
    // lock can releaseed automaticlly by timeout
    dlock_->UnLock(fsName, uuid);
}

// after mds restart need rebuild mountpoint ttl recorder
void FsManager::RebuildTimeRecorder() {
    std::vector<FsInfoWrapper> fsInfos;
    fsStorage_->GetAll(&fsInfos);
    for (auto const& info : fsInfos) {
        for (auto const& mount : info.MountPoints()) {
            UpdateClientAliveTime(mount, info.GetFsName(), false);
        }
    }
    LOG(INFO) << "RebuildTimeRecorder size = " << mpTimeRecorder_.size();
}

FSStatusCode FsManager::AddMountPoint(const Mountpoint& mountpoint,
    const std::string& fsName) {
    LOG(INFO) << "AddMountPoint mountpoint = " << mountpoint.DebugString()
              << ", fsName = " << fsName;
    // 1. query fs
    FsInfoWrapper wrapper;
    FSStatusCode ret = fsStorage_->Get(fsName, &wrapper);
    if (ret != FSStatusCode::OK) {
        LOG(WARNING) << "AddMountPoint fail, get fs fail, fsName = " << fsName
                     << ", errCode = " << FSStatusCode_Name(ret);
        return ret;
    }

    // 2. insert mountpoint
    wrapper.AddMountPoint(mountpoint);
    // for persistence consider
    ret = fsStorage_->Update(wrapper);
    if (ret != FSStatusCode::OK) {
        LOG(WARNING) << "AddMountPoint update fs fail, fsName = " << fsName
                     << ", mountpoint = " << mountpoint.ShortDebugString()
                     << ", errCode = " << FSStatusCode_Name(ret);
        return ret;
    }

    return FSStatusCode::OK;
}

void FsManager::UpdateClientAliveTime(const Mountpoint& mountpoint,
    const std::string& fsName, bool addMountPoint) {
    VLOG(1) << "UpdateClientAliveTime fsName = " << fsName
            << ", mp = " << mountpoint.DebugString()
            << ". addMountPoint = " << addMountPoint;
    std::string mountpath;
    MountPoint2Str(mountpoint, &mountpath);
    WriteLockGuard wlock(recorderMutex_);
    if (addMountPoint) {
        auto iter = mpTimeRecorder_.find(mountpath);
        // client hang timeout and recover later
        // need add mountpoint to fsInfo
        if (iter == mpTimeRecorder_.end()) {
            if (AddMountPoint(mountpoint, fsName) != FSStatusCode::OK) {
                return;
            }
        }
    }
    mpTimeRecorder_[mountpath] = std::make_pair(
        fsName, ::curve::common::TimeUtility::GetTimeofDaySec());
}

void FsManager::DeleteClientAliveTime(const std::string& mountpoint) {
    WriteLockGuard wlock(recorderMutex_);
    auto it = mpTimeRecorder_.find(mountpoint);
    if (it != mpTimeRecorder_.end()) {
        mpTimeRecorder_.erase(it);
    }
}

// for utest
bool FsManager::GetClientAliveTime(const std::string& mountpoint,
    std::pair<std::string, uint64_t>* out) {
    ReadLockGuard rlock(recorderMutex_);
    auto iter = mpTimeRecorder_.find(mountpoint);
    if (iter == mpTimeRecorder_.end()) {
        return false;
    }

    *out = iter->second;
    return true;
}

bool FsManager::FillVolumeInfo(common::Volume* volume) {
    auto* proxy = space::MdsProxyManager::GetInstance().GetOrCreateProxy(
        {volume->cluster().begin(), volume->cluster().end()});
    if (proxy == nullptr) {
        LOG(ERROR) << "Fail to get or create proxy";
        return false;
    }

    uint64_t size = 0;
    uint64_t alignment = 0;
    auto ret = proxy->GetVolumeInfo(*volume, &size, &alignment);
    if (!ret) {
        LOG(WARNING) << "Fail to get volume size, volume name: "
                     << volume->volumename();
        return false;
    }

    volume->set_volumesize(size);
    volume->set_extendalignment(alignment);
    return true;
}

}  // namespace mds
}  // namespace curvefs
