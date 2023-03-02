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
 * Created Date: 2021-08-31
 * Author: xuchaojie
 */
#include "curvefs/src/metaserver/trash.h"

#include <map>

#include "curvefs/proto/mds.pb.h"
#include "curvefs/src/metaserver/fsinfo_manager.h"
#include "src/common/timeutility.h"

using ::curve::common::TimeUtility;

namespace curvefs {
namespace metaserver {

using ::curvefs::mds::FsInfo;
using ::curvefs::mds::FSStatusCode;

void TrashOption::InitTrashOptionFromConf(std::shared_ptr<Configuration> conf) {
    conf->GetValueFatalIfFail("trash.scanPeriodSec", &scanPeriodSec);
    conf->GetValueFatalIfFail("trash.expiredAfterSec", &expiredAfterSec);
}

void TrashImpl::Init(const TrashOption &option) {
    options_ = option;
    s3Adaptor_ = option.s3Adaptor;
    isStop_ = false;
}

void TrashImpl::Add(uint32_t fsId, uint64_t inodeId, uint32_t dtime) {
    TrashItem item;
    item.fsId = fsId;
    item.inodeId = inodeId;
    item.dtime = dtime;

    LockGuard lg(itemsMutex_);
    if (isStop_) {
        return;
    }
    trashItems_.push_back(item);
    VLOG(6) << "Add Trash Item success, item.fsId = " << item.fsId
            << ", item.inodeId = " << item.inodeId
            << ", item.dtime = " << item.dtime;
}

void TrashImpl::ScanTrash() {
    LockGuard lgScan(scanMutex_);
    std::list<TrashItem> temp;
    {
        LockGuard lgItems(itemsMutex_);
        trashItems_.swap(temp);
    }

    for (auto it = temp.begin(); it != temp.end();) {
        if (isStop_) {
            return;
        }
        if (NeedDelete(*it)) {
            MetaStatusCode ret = DeleteInodeAndData(*it);
            if (MetaStatusCode::NOT_FOUND == ret) {
                it = temp.erase(it);
                continue;
            }
            if (ret != MetaStatusCode::OK) {
                LOG(ERROR) << "DeleteInodeAndData fail, fsId = " << it->fsId
                           << ", inodeId = " << it->inodeId
                           << ", ret = " << MetaStatusCode_Name(ret);
                it++;
                continue;
            }
            VLOG(6) << "Trash Delete Inode, fsId = " << it->fsId
                    << ", inodeId = " << it->inodeId;
            it = temp.erase(it);
        } else {
            it++;
        }
    }

    {
        LockGuard lgItems(itemsMutex_);
        trashItems_.splice(trashItems_.end(), temp);
    }
}

void TrashImpl::StopScan() { isStop_ = true; }

bool TrashImpl::IsStop() { return isStop_; }

bool TrashImpl::NeedDelete(const TrashItem &item) {
    uint32_t now = TimeUtility::GetTimeofDaySec();
    Inode inode;
    MetaStatusCode ret =
        inodeStorage_->Get(Key4Inode(item.fsId, item.inodeId), &inode);
    if (MetaStatusCode::NOT_FOUND == ret) {
        LOG(WARNING) << "GetInode find inode not exist, fsId = " << item.fsId
                     << ", inodeId = " << item.inodeId
                     << ", ret = " << MetaStatusCode_Name(ret);
        return true;
    } else if (ret != MetaStatusCode::OK) {
        LOG(WARNING) << "GetInode fail, fsId = " << item.fsId
                     << ", inodeId = " << item.inodeId
                     << ", ret = " << MetaStatusCode_Name(ret);
        return false;
    }

    // for compatibility, if fs recycleTimeHour is 0, use old trash logic
    // if fs recycleTimeHour is 0, use trash wait until expiredAfterSec
    // if fs recycleTimeHour is not 0, return true
    uint64_t recycleTimeHour = GetFsRecycleTimeHour(item.fsId);
    if (recycleTimeHour == 0) {
        return ((now - item.dtime) >= options_.expiredAfterSec);
    } else {
        return true;
    }
}

uint64_t TrashImpl::GetFsRecycleTimeHour(uint32_t fsId) {
    FsInfo fsInfo;
    if (!GetFsInfo(fsId, &fsInfo)) {
        LOG(WARNING) << "GetFsInfo fail.";
        return 0;
    }

    uint64_t recycleTimeHour = 0;
    if (fsInfo.has_recycletimehour()) {
        recycleTimeHour = fsInfo.recycletimehour();
    } else {
        recycleTimeHour = 0;
    }
    return recycleTimeHour;
}

void TrashImpl::GenerateExtentsForBlockGroup(
    const std::map<uint64_t, uint64_t> &VolumeOffsetMap,
    std::vector<Extent> *extents) {
    extents->clear();
    uint64_t offset = VolumeOffsetMap.begin()->first;
    uint64_t len = VolumeOffsetMap.begin()->second;
    for (auto it = VolumeOffsetMap.begin(); it != VolumeOffsetMap.end(); it++) {
        if (it->first == offset + len) {
            len += it->second;
        } else {
            if (len > 0) {
                Extent extent;
                extent.offset = offset;
                extent.len = len;
                extents->emplace_back(extent);
            }
            offset = it->first;
            len = it->second;
        }
    }
    if (len > 0) {
        Extent extent;
        extent.offset = offset;
        extent.len = len;
        extents->emplace_back(extent);
    }
    return;
}

MetaStatusCode TrashImpl::GenerateExtents(
    uint64_t blockGroupSize, const VolumeExtentList &extentList,
    std::map<uint64_t, std::vector<Extent>> *extentsMap) {
    // 遍历slices，把slices转换为offsetMap
    typedef std::map<uint64_t, uint64_t> VolumeOffsetMap;
    std::map<uint64_t, VolumeOffsetMap> offsetMap;

    auto slices = extentList.slices();
    for (auto it = slices.begin(); it != slices.end(); it++) {
        for (auto it2 = it->extents().begin(); it2 != it->extents().end();
             it2++) {
            uint64_t offset = it2->volumeoffset();
            uint64_t len = it2->length();
            uint64_t blockGroupOffset =
                offset / blockGroupSize * blockGroupSize;  // NOLINT
            if (offset + len > blockGroupOffset + blockGroupSize) {
                len = blockGroupOffset + blockGroupSize - offset;
                offsetMap[blockGroupOffset][offset] = len;
                offsetMap[blockGroupOffset + blockGroupSize][offset + len] =
                    it2->length() - len;
            } else {
                offsetMap[blockGroupOffset][offset] = len;
            }
        }
    }

    // 遍历offsetMap，把offsetMap转换为extents，合并相邻的offset
    for (auto it = offsetMap.begin(); it != offsetMap.end(); it++) {
        std::vector<Extent> extents;
        GenerateExtentsForBlockGroup(it->second, &extents);
        extentsMap->emplace(it->first, extents);
    }

    return MetaStatusCode::OK;
}

bool TrashImpl::UpdateExtentSlice(uint64_t fsId, uint64_t inodeId,
                                  const std::vector<Extent> &extents) {
    return true;
}

bool TrashImpl::FreeSpaceForVolume(const Inode &inode, const FsInfo &fsInfo) {
    // get all volume extent
    VolumeExtentList extentList;
    MetaStatusCode ret = inodeStorage_->GetAllVolumeExtent(
        inode.fsid(), inode.inodeid(), &extentList);
    if (ret != MetaStatusCode::OK) {
        LOG(ERROR) << "GetAllVolumeExtent fail, fsId = " << inode.fsid()
                   << ", inodeId = " << inode.inodeid()
                   << ", ret = " << MetaStatusCode_Name(ret);
        return false;
    }

    // convert VolumeExtentList to Extent
    uint64_t blockGroupSize = fsInfo.detail().volume().blockgroupsize();
    std::map<uint64_t, std::vector<Extent>> extentsMap;
    GenerateExtents(blockGroupSize, extentList, &extentsMap);

    // dealloc volume space for every block group
    bool retCode = true;
    for (auto it = extentsMap.begin(); it != extentsMap.end(); it++) {
        if (volumeSpaceManager_->DeallocVolumeSpace(fsInfo.fsid(), it->first,
                                                    it->second)) {
            // update Extent in metaserver
            UpdateExtentSlice(fsInfo.fsid(), inode.inodeid(), it->second);
        } else {
            retCode = false;
            LOG(WARNING) << "DeallocVolumeSpace fail, skip, fsId = "
                         << fsInfo.fsid() << ", inodeId = " << inode.inodeid()
                         << ", blockGroupOffset = " << it->first;
            continue;
        }
    }
    return retCode;
}

MetaStatusCode TrashImpl::DeleteInodeAndData(const TrashItem &item) {
    Inode inode;
    MetaStatusCode ret =
        inodeStorage_->Get(Key4Inode(item.fsId, item.inodeId), &inode);
    if (ret != MetaStatusCode::OK) {
        LOG(WARNING) << "GetInode fail, fsId = " << item.fsId
                     << ", inodeId = " << item.inodeId
                     << ", ret = " << MetaStatusCode_Name(ret);
        return ret;
    }

    FsInfo fsInfo;
    if (!GetFsInfo(item.fsId, &fsInfo)) {
        LOG(ERROR) << "GetFsInfo fail, fsId = " << item.fsId;
        return MetaStatusCode::S3_DELETE_ERR;
    }

    if (FsFileType::TYPE_FILE == inode.type()) {
        if (!FreeSpaceForVolume(inode, fsInfo)) {
            LOG(ERROR) << "FreeSpaceForVolume fail, fsId = " << item.fsId
                       << ", inodeId = " << item.inodeId;
            return MetaStatusCode::VOLUME_FREE_ERR;
        }
    } else if (FsFileType::TYPE_S3 == inode.type()) {
        const auto &s3Info = fsInfo.detail().s3info();
        // reinit s3 adaptor
        S3ClientAdaptorOption clientAdaptorOption;
        s3Adaptor_->GetS3ClientAdaptorOption(&clientAdaptorOption);
        clientAdaptorOption.blockSize = s3Info.blocksize();
        clientAdaptorOption.chunkSize = s3Info.chunksize();
        s3Adaptor_->Reinit(clientAdaptorOption, s3Info.ak(), s3Info.sk(),
                           s3Info.endpoint(), s3Info.bucketname());
        int retVal = s3Adaptor_->Delete(inode);
        if (retVal != 0) {
            LOG(ERROR) << "S3ClientAdaptor delete s3 data failed"
                       << ", ret = " << retVal << ", fsId = " << item.fsId
                       << ", inodeId = " << item.inodeId;
            return MetaStatusCode::S3_DELETE_ERR;
        }
    }

    ret = inodeStorage_->Delete(Key4Inode(item.fsId, item.inodeId));
    if (ret != MetaStatusCode::OK && ret != MetaStatusCode::NOT_FOUND) {
        LOG(ERROR) << "Delete Inode fail, fsId = " << item.fsId
                   << ", inodeId = " << item.inodeId
                   << ", ret = " << MetaStatusCode_Name(ret);
        return ret;
    }
    return MetaStatusCode::OK;
}

void TrashImpl::ListItems(std::list<TrashItem> *items) {
    LockGuard lgScan(scanMutex_);
    LockGuard lgItems(itemsMutex_);
    *items = trashItems_;
}

bool TrashImpl::GetFsInfo(uint32_t fsId, FsInfo *fsInfo) {
    return FsInfoManager::GetInstance().GetFsInfo(fsId, fsInfo);
}

}  // namespace metaserver
}  // namespace curvefs
