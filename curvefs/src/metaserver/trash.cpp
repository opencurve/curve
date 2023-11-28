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

#include "curvefs/proto/mds.pb.h"
#include "curvefs/src/metaserver/inode_storage.h"
#include "src/common/timeutility.h"

using ::curve::common::TimeUtility;

namespace curvefs {
namespace metaserver {

using ::curvefs::mds::FsInfo;
using ::curvefs::mds::FSStatusCode;

bool pass_uint32(const char*, uint32_t value) { return true; }
DEFINE_uint32(trash_expiredAfterSec, 604800,
              "time to delete data in the recycle bin");
DEFINE_uint32(trash_scanPeriodSec, 600, "recycle bin scan interval");
DEFINE_validator(trash_expiredAfterSec, pass_uint32);
DEFINE_validator(trash_scanPeriodSec, pass_uint32);

void TrashOption::InitTrashOptionFromConf(std::shared_ptr<Configuration> conf) {
    conf->GetValueFatalIfFail("trash.scanPeriodSec", &scanPeriodSec);
    FLAGS_trash_scanPeriodSec = scanPeriodSec;
    conf->GetValueFatalIfFail("trash.expiredAfterSec", &expiredAfterSec);
    FLAGS_trash_expiredAfterSec = expiredAfterSec;
}

void TrashImpl::Init(const TrashOption &option) {
    options_ = option;
    s3Adaptor_ = option.s3Adaptor;
    mdsClient_ = option.mdsClient;
    isStop_ = false;
}

void TrashImpl::Add(uint32_t fsId, uint64_t inodeId, uint32_t dtime, bool deleted) {
    TrashItem item;
    item.fsId = fsId;
    item.inodeId = inodeId;
    item.dtime = dtime;
    item.deleted = deleted;

    LockGuard lg(itemsMutex_);
    if (isStop_) {
        return;
    }
    trashItems_.push_back(item);
    VLOG(0) << "whs add trash item success, item.fsId = " << item.fsId
            << ", item.inodeId = " << item.inodeId
            << ", item.dtime = " << item.dtime
            << ", item.deleted = " << item.deleted;
}

void TrashImpl::ScanTrash() {
    VLOG(0) << "whs ScanTrash start.";
    LockGuard lgScan(scanMutex_);
    std::list<TrashItem> temp;
    {
        LockGuard lgItems(itemsMutex_);
        trashItems_.swap(temp);
    }
    for (auto it = temp.begin(); it != temp.end(); it++) {
        VLOG(0) << "11whs ScanTrash , " << "item.fsId = " << it->fsId
                << ", item.inodeId = " << it->inodeId
                << ", item.dtime = " << it->dtime << ", " << temp.size() << ", " << it->deleted;
    }

    for (auto it = temp.begin(); it != temp.end();) {
        if (isStop_) {
            VLOG(0) << "whs ScanTrash stop.";
            return;
        }
        VLOG(0) << "whs  ScanTrash , " << "item.fsId = " << it->fsId
                << ", item.inodeId = " << it->inodeId
                << ", item.dtime = " << it->dtime;
        if (NeedDelete(*it)) {
            MetaStatusCode ret = DeleteInodeAndData(*it);
            if (MetaStatusCode::NOT_FOUND == ret) {
                  VLOG(0) << "whs ScanTrash, inode not exist, fsId = " << it->fsId
                        << ", inodeId = " << it->inodeId;
                ClearDeleted(*it);
                it = temp.erase(it);
                VLOG(0) << "whs ScanTrash, inode not exist, fsId = " << it->fsId
                        << ", inodeId = " << it->inodeId;
                continue;
            }
            if (ret != MetaStatusCode::OK) {
                LOG(ERROR) << "DeleteInodeAndData fail, fsId = " << it->fsId
                           << ", inodeId = " << it->inodeId
                           << ", ret = " << MetaStatusCode_Name(ret);
                it++;
                continue;
            }
            VLOG(0) << "whs Trash Delete Inode, fsId = " << it->fsId
                    << ", inodeId = " << it->inodeId;
            ClearDeleted(*it);
            it = temp.erase(it);
        } else {
            VLOG(0) << "whs ScanTrash, inode not expired, fsId = " << it->fsId
                    << ", inodeId = " << it->inodeId;
            it++;
        }
    }

    {

        LockGuard lgItems(itemsMutex_);

         for (auto it = temp.begin(); it != temp.end(); it++) {
        VLOG(0) << "00whs ScanTrash , " << "item.fsId = " << it->fsId
                << ", item.inodeId = " << it->inodeId
                << ", item.dtime = " << it->dtime;
    }


        trashItems_.splice(trashItems_.end(), temp);
    }
    VLOG(0) << "whs ScanTrash end.";

}

MetaStatusCode TrashImpl::ClearDeleted(const TrashItem &item) {
    if (item.deleted) {
        VLOG(0) << "ClearDeleted: " << item.fsId << ", " << item.inodeId
                << ", " << item.dtime << ", " << item.deleted ;
        inodeStorage_->ClearDelKey(Key4Inode(item.fsId, item.inodeId));
    } else {

           VLOG(0) << "ClearDeleted: " << item.fsId << ", " << item.inodeId
                << ", " << item.dtime << ", " << item.deleted ;
    }
    return MetaStatusCode::OK;
}

void TrashImpl::StopScan() {
    isStop_ = true;
}

bool TrashImpl::IsStop() {
    return isStop_;
}

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
        return ((now - item.dtime) >= FLAGS_trash_expiredAfterSec);
    } else {
        return true;
    }
}

uint64_t TrashImpl::GetFsRecycleTimeHour(uint32_t fsId) {
    FsInfo fsInfo;
    uint64_t recycleTimeHour = 0;
    if (fsInfoMap_.find(fsId) == fsInfoMap_.end()) {
        auto ret = mdsClient_->GetFsInfo(fsId, &fsInfo);
        if (ret != FSStatusCode::OK) {
            if (FSStatusCode::NOT_FOUND == ret) {
                LOG(ERROR) << "The fs not exist, fsId = " << fsId;
                return 0;
            } else {
                LOG(ERROR)
                    << "GetFsInfo failed, FSStatusCode = " << ret
                    << ", FSStatusCode_Name = " << FSStatusCode_Name(ret)
                    << ", fsId = " << fsId;
                return 0;
            }
        }
        fsInfoMap_.insert({fsId, fsInfo});
    } else {
        fsInfo = fsInfoMap_.find(fsId)->second;
    }

    if (fsInfo.has_recycletimehour()) {
        recycleTimeHour = fsInfo.recycletimehour();
    } else {
        recycleTimeHour = 0;
    }
    return recycleTimeHour;
}

MetaStatusCode TrashImpl::
DeleteInodeAndData(const TrashItem &item) {
    Inode inode;
    MetaStatusCode ret =
        inodeStorage_->Get(Key4Inode(item.fsId, item.inodeId), &inode);
    if (ret != MetaStatusCode::OK) {
        LOG(WARNING) << "GetInode fail, fsId = " << item.fsId
                     << ", inodeId = " << item.inodeId
                     << ", ret = " << MetaStatusCode_Name(ret);
        return ret;
    }

    if (FsFileType::TYPE_FILE == inode.type()) {
        // TODO(xuchaojie) : delete on volume
    } else if (FsFileType::TYPE_S3 == inode.type()) {
        // get s3info from mds
        FsInfo fsInfo;
        if (fsInfoMap_.find(item.fsId) == fsInfoMap_.end()) {
            auto ret = mdsClient_->GetFsInfo(item.fsId, &fsInfo);
            if (ret != FSStatusCode::OK) {
                if (FSStatusCode::NOT_FOUND == ret) {
                    LOG(ERROR) << "The fsName not exist, fsId = " << item.fsId;
                    return MetaStatusCode::S3_DELETE_ERR;
                } else {
                    LOG(ERROR)
                        << "GetFsInfo failed, FSStatusCode = " << ret
                        << ", FSStatusCode_Name = " << FSStatusCode_Name(ret)
                        << ", fsId = " << item.fsId;
                    return MetaStatusCode::S3_DELETE_ERR;
                }
            }
            fsInfoMap_.insert({item.fsId, fsInfo});
        } else {
            fsInfo = fsInfoMap_.find(item.fsId)->second;
        }
        const auto& s3Info = fsInfo.detail().s3info();
        // reinit s3 adaptor
        S3ClientAdaptorOption clientAdaptorOption;
        s3Adaptor_->GetS3ClientAdaptorOption(&clientAdaptorOption);
        clientAdaptorOption.blockSize = s3Info.blocksize();
        clientAdaptorOption.chunkSize = s3Info.chunksize();
        clientAdaptorOption.objectPrefix = s3Info.objectprefix();
        s3Adaptor_->Reinit(clientAdaptorOption, s3Info.ak(), s3Info.sk(),
            s3Info.endpoint(), s3Info.bucketname());
        ret = inodeStorage_->PaddingInodeS3ChunkInfo(item.fsId,
          item.inodeId, inode.mutable_s3chunkinfomap());
        if (ret != MetaStatusCode::OK) {
            LOG(ERROR) << "GetInode chunklist fail, fsId = " << item.fsId
                << ", inodeId = " << item.inodeId
                << ", retCode = " << MetaStatusCode_Name(ret);
            return ret;
        }
        if (inode.s3chunkinfomap().empty()) {
            LOG(WARNING) << "GetInode chunklist empty, fsId = " << item.fsId
                << ", inodeId = " << item.inodeId;
            // Todo(hzwuhongsong ：the s3chunkinfomap is empty too as empty file
            // empty file will never be deleted
            return MetaStatusCode::NOT_FOUND;
        }
        VLOG(0) << "whs: DeleteInodeAndData, inode: "
            << inode.ShortDebugString();
        int retVal = s3Adaptor_->Delete(inode);
        if (retVal != 0) {
            LOG(ERROR) << "S3ClientAdaptor delete s3 data failed"
                       << ", ret = " << retVal << ", fsId = " << item.fsId
                       << ", inodeId = " << item.inodeId;
            return MetaStatusCode::S3_DELETE_ERR;
        }
    }
    ret = inodeStorage_->ForceDelete(Key4Inode(item.fsId, item.inodeId));
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

}  // namespace metaserver
}  // namespace curvefs
