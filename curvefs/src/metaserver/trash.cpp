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
#include "curvefs/src/metaserver/copyset/copyset_node_manager.h"
#include "curvefs/src/metaserver/copyset/meta_operator.h"

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
    copysetNode_ = copyset::CopysetNodeManager::GetInstance().
        GetSharedCopysetNode(poolId_, copysetId_);

    isStop_ = false;
}

void TrashImpl::StopScan() {
    isStop_ = true;
}

void TrashImpl::Add(uint64_t inodeId, uint64_t dtime, bool deleted) {
    if (isStop_) {
        return;
    }

    LockGuard lg(itemsMutex_);
    trashItems_.emplace(inodeId, dtime);

    VLOG(6) << "Add trash item success"
            << ", fsId = " << fsId_
            << ", partitionId = " << partitionId_
            << ", inodeId = " << inodeId
            << ", dtime = " << dtime
            << ", deleted = " << deleted;
    if (!deleted) {
        inodeStorage_->AddDeletedInode(Key4Inode(fsId_, inodeId), dtime);
    }
}

bool TrashImpl::IsStop() {
    return isStop_;
}

void TrashImpl::Remove(uint64_t inodeId) {
    if (isStop_) {
        return;
    }
    LockGuard lg(itemsMutex_);
    trashItems_.erase(inodeId);
    RemoveDeletedInode(inodeId);
    VLOG(6) << "Remove Trash Item success, fsId = " << fsId_
            << ", partitionId = " << partitionId_
            << ", inodeId = " << inodeId;
}

void TrashImpl::ScanTrash() {
    LockGuard lgScan(scanMutex_);
    LOG(INFO) << "ScanTrash, fsId = " << fsId_
              << ", partitionId = " << partitionId_
              << ", trashItems size = " << trashItems_.size();
    // only scan on leader
    if (copysetNode_ == nullptr || !copysetNode_->IsLeaderTerm()) {
        return;
    }

    std::unordered_map<uint64_t, uint64_t> temp;
    {
        LockGuard lgItems(itemsMutex_);
        trashItems_.swap(temp);
    }

    for (auto it = temp.begin(); it != temp.end();) {
        if (isStop_ || !copysetNode_->IsLeaderTerm()) {
            return;
        }
        if (NeedDelete(it->second)) {
            MetaStatusCode ret = DeleteInodeAndData(it->first);
            if (ret != MetaStatusCode::OK) {
                LOG(ERROR) << "DeleteInodeAndData fail, fsId = " << fsId_
                           << ", inodeId = " << it->first
                           << ", ret = " << MetaStatusCode_Name(ret);
                it++;
                continue;
            }
            LOG(INFO) << "Trash delete inode, fsId = " << fsId_
                      << ", partitionId = " << partitionId_
                      << ", inodeId = " << it->first;
            it = temp.erase(it);
        } else {
            it++;
        }
    }
    {
        LockGuard lgItems(itemsMutex_);
        trashItems_.insert(temp.begin(), temp.end());
    }
}

void TrashImpl::RemoveDeletedInode(uint64_t inodeId) {
    VLOG(9) << "RemoveDeletedInode: " << fsId_
            << ", " << partitionId_ << ", " << ", " << inodeId;
    if (MetaStatusCode::OK !=
        inodeStorage_->RemoveDeletedInode(Key4Inode(fsId_, inodeId))) {
        LOG(WARNING) << "RemoveDeletedInode failed, " << fsId_
                     << ", " << partitionId_ << ", " << ", " << inodeId;
    }
}

void TrashImpl::RemoveDeleteNode() {
    inodeStorage_->RemoveAllDeletedInode();
}

bool TrashImpl::NeedDelete(uint64_t dtime) {
    // for compatibility, if fs recycleTimeHour is 0, use old trash logic
    // if fs recycleTimeHour is 0, use trash wait until expiredAfterSec
    // if fs recycleTimeHour is not 0, return true
    uint64_t recycleTimeHour = GetFsRecycleTimeHour(fsId_);
    if (recycleTimeHour == 0) {
        return ((TimeUtility::GetTimeofDaySec() - dtime) >=
            FLAGS_trash_expiredAfterSec);
    } else {
        return true;
    }
}

uint64_t TrashImpl::GetFsRecycleTimeHour(uint32_t fsId) {
    uint64_t recycleTimeHour = 0;
    if (fsInfo_.fsid() == 0) {
        auto ret = mdsClient_->GetFsInfo(fsId, &fsInfo_);
        if (ret != FSStatusCode::OK) {
            if (FSStatusCode::NOT_FOUND == ret) {
                LOG(ERROR) << "The fs not exist, fsId = " << fsId;
                return 0;
            } else {
                LOG(ERROR) << "GetFsInfo failed, FSStatusCode = " << ret
                           << ", FSStatusCode_Name = " << FSStatusCode_Name(ret)
                           << ", fsId = " << fsId;
                return 0;
            }
        }
    }

    if (fsInfo_.has_recycletimehour()) {
        recycleTimeHour = fsInfo_.recycletimehour();
    }
    return recycleTimeHour;
}

MetaStatusCode TrashImpl::DeleteInodeAndData(uint64_t inodeId) {
    Inode inode;
    auto ret = inodeStorage_->Get(Key4Inode(fsId_, inodeId), &inode);
    if (ret == MetaStatusCode::NOT_FOUND) {
        LOG(WARNING) << "GetInode fail, fsId = " << fsId_
                     << ", inodeId = " << inodeId
                     << ", ret = " << MetaStatusCode_Name(ret);
        return MetaStatusCode::OK;
    }
    if (ret != MetaStatusCode::OK) {
        LOG(WARNING) << "GetInode fail, fsId = " << fsId_
                     << ", inodeId = " << inodeId
                     << ", ret = " << MetaStatusCode_Name(ret);
        return ret;
    }

    // 1. delete data first
    if (FsFileType::TYPE_FILE == inode.type()) {
        // TODO(xuchaojie) : delete on volume
    } else if (FsFileType::TYPE_S3 == inode.type()) {
        // get s3info from mds
        if (fsInfo_.fsid() == 0) {
            auto ret = mdsClient_->GetFsInfo(fsId_, &fsInfo_);
            if (ret != FSStatusCode::OK) {
                if (FSStatusCode::NOT_FOUND == ret) {
                    LOG(ERROR) << "The fsName not exist, fsId = " << fsId_;
                    return MetaStatusCode::S3_DELETE_ERR;
                } else {
                    LOG(ERROR)
                        << "GetFsInfo failed, FSStatusCode = " << ret
                        << ", FSStatusCode_Name = " << FSStatusCode_Name(ret)
                        << ", fsId = " << fsId_;
                    return MetaStatusCode::S3_DELETE_ERR;
                }
            }
        }
        const auto& s3Info = fsInfo_.detail().s3info();
        // reinit s3 adaptor
        S3ClientAdaptorOption clientAdaptorOption;
        s3Adaptor_->GetS3ClientAdaptorOption(&clientAdaptorOption);
        clientAdaptorOption.blockSize = s3Info.blocksize();
        clientAdaptorOption.chunkSize = s3Info.chunksize();
        clientAdaptorOption.objectPrefix = s3Info.objectprefix();
        s3Adaptor_->Reinit(clientAdaptorOption, s3Info.ak(), s3Info.sk(),
            s3Info.endpoint(), s3Info.bucketname());
        ret = inodeStorage_->PaddingInodeS3ChunkInfo(fsId_, inodeId,
            inode.mutable_s3chunkinfomap());
        if (ret != MetaStatusCode::OK) {
            LOG(ERROR) << "GetInode chunklist fail, fsId = " << fsId_
                       << ", inodeId = " << inodeId
                       << ", retCode = " << MetaStatusCode_Name(ret);
            return ret;
        }
        VLOG(9) << "DeleteInodeAndData, inode: " << inode.ShortDebugString();
        if (!inode.s3chunkinfomap().empty()) {
            int retVal = s3Adaptor_->Delete(inode);
            if (retVal != 0) {
                LOG(ERROR) << "S3ClientAdaptor delete s3 data failed"
                           << ", ret = " << retVal << ", fsId = " << fsId_
                           << ", inodeId = " << inodeId;
                return MetaStatusCode::S3_DELETE_ERR;
            }
        }
    }
    // 2. delete metadata
    if (copysetNode_->IsLeaderTerm()) {
        return DeleteInode(inodeId);
    }
    return MetaStatusCode::OK;
}

MetaStatusCode TrashImpl::DeleteInode(uint64_t inodeId) {
    DeleteInodeRequest request;
    request.set_poolid(poolId_);
    request.set_copysetid(copysetId_);
    request.set_partitionid(partitionId_);
    request.set_fsid(fsId_);
    request.set_inodeid(inodeId);

    DeleteInodeResponse response;
    DeleteInodeClosure done;
    auto DeleteInodeOp = new copyset::DeleteInodeOperator(
        copysetNode_.get(), nullptr, &request, &response, &done);
    DeleteInodeOp->Propose();
    done.WaitRunned();
    return response.statuscode();
}

uint64_t TrashImpl::Size() {
    LockGuard lgItems(itemsMutex_);
    return trashItems_.size();
}

}  // namespace metaserver
}  // namespace curvefs
