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
 * @Date: 2021-08-30 19:48:47
 * @Author: chenwei
 */

#include "curvefs/src/metaserver/partition.h"

#include <assert.h>

#include <algorithm>
#include <memory>

#include "curvefs/proto/metaserver.pb.h"
#include "curvefs/src/metaserver/s3compact_manager.h"
#include "curvefs/src/metaserver/trash_manager.h"

namespace curvefs {
namespace metaserver {

Partition::Partition(const PartitionInfo& paritionInfo,
                     std::shared_ptr<KVStorage> kvStorage) {
    assert(paritionInfo.start() <= paritionInfo.end());
    partitionInfo_ = paritionInfo;

    inodeStorage_ = std::make_shared<InodeStorage>(
        kvStorage, GetInodeTablename());
    dentryStorage_ = std::make_shared<DentryStorage>(
        kvStorage, GetDentryTablename());
    trash_ = std::make_shared<TrashImpl>(inodeStorage_);
    inodeManager_ = std::make_shared<InodeManager>(inodeStorage_, trash_);
    txManager_ = std::make_shared<TxManager>(dentryStorage_);
    dentryManager_ =
        std::make_shared<DentryManager>(dentryStorage_, txManager_);
    if (!paritionInfo.has_nextid()) {
        partitionInfo_.set_nextid(
            std::max(kMinPartitionStartId, partitionInfo_.start()));
    }

    if (paritionInfo.status() != PartitionStatus::DELETING) {
        TrashManager::GetInstance().Add(paritionInfo.partitionid(), trash_);
        s3compact_ = std::make_shared<S3Compact>(inodeManager_, partitionInfo_);
        S3CompactManager::GetInstance().RegisterS3Compact(s3compact_);
    }
}

// dentry
MetaStatusCode Partition::CreateDentry(const Dentry& dentry, bool isLoadding) {
    if (!IsInodeBelongs(dentry.fsid(), dentry.parentinodeid())) {
        return MetaStatusCode::PARTITION_ID_MISSMATCH;
    }

    if (GetStatus() == PartitionStatus::DELETING) {
        return MetaStatusCode::PARTITION_DELETING;
    }
    MetaStatusCode ret = dentryManager_->CreateDentry(dentry, isLoadding);
    if (MetaStatusCode::OK == ret) {
        if (dentry.has_type()) {
            if ((!isLoadding) &&
                (dentry.type() == FsFileType::TYPE_DIRECTORY)) {
                return inodeManager_->UpdateInodeWhenCreateOrRemoveSubNode(
                    dentry.fsid(), dentry.parentinodeid(), true);
            } else {
                return MetaStatusCode::OK;
            }
        } else {
            LOG(ERROR) << "CreateDentry does not have type, "
                       << dentry.ShortDebugString();
            return MetaStatusCode::PARAM_ERROR;
        }
    } else if (MetaStatusCode::IDEMPOTENCE_OK == ret) {
        return MetaStatusCode::OK;
    } else {
        return ret;
    }
}

MetaStatusCode Partition::DeleteDentry(const Dentry& dentry) {
    if (!IsInodeBelongs(dentry.fsid(), dentry.parentinodeid())) {
        return MetaStatusCode::PARTITION_ID_MISSMATCH;
    }

    MetaStatusCode ret = dentryManager_->DeleteDentry(dentry);
    if (MetaStatusCode::OK == ret) {
        if (dentry.has_type()) {
            if (dentry.type() == FsFileType::TYPE_DIRECTORY) {
                return inodeManager_->UpdateInodeWhenCreateOrRemoveSubNode(
                    dentry.fsid(), dentry.parentinodeid(), false);
            }
            return ret;
        } else {
            LOG(ERROR) << "DeleteDentry does not have type, "
                       << dentry.ShortDebugString();
            return MetaStatusCode::PARAM_ERROR;
        }
    } else {
        return ret;
    }
}

MetaStatusCode Partition::GetDentry(Dentry* dentry) {
    if (!IsInodeBelongs(dentry->fsid(), dentry->parentinodeid())) {
        return MetaStatusCode::PARTITION_ID_MISSMATCH;
    }

    if (GetStatus() == PartitionStatus::DELETING) {
        return MetaStatusCode::PARTITION_DELETING;
    }

    return dentryManager_->GetDentry(dentry);
}

MetaStatusCode Partition::ListDentry(const Dentry& dentry,
                                     std::vector<Dentry>* dentrys,
                                     uint32_t limit,
                                     bool onlyDir) {
    if (!IsInodeBelongs(dentry.fsid(), dentry.parentinodeid())) {
        return MetaStatusCode::PARTITION_ID_MISSMATCH;
    }

    if (GetStatus() == PartitionStatus::DELETING) {
        return MetaStatusCode::PARTITION_DELETING;
    }

    return dentryManager_->ListDentry(dentry, dentrys, limit, onlyDir);
}

void Partition::ClearDentry() {
    dentryManager_->ClearDentry();
}

MetaStatusCode Partition::HandleRenameTx(const std::vector<Dentry>& dentrys) {
    for (const auto& it : dentrys) {
        if (!IsInodeBelongs(it.fsid(), it.parentinodeid())) {
            return MetaStatusCode::PARTITION_ID_MISSMATCH;
        }
    }

    if (GetStatus() == PartitionStatus::DELETING) {
        return MetaStatusCode::PARTITION_DELETING;
    }

    return dentryManager_->HandleRenameTx(dentrys);
}

bool Partition::InsertPendingTx(const PrepareRenameTxRequest& pendingTx) {
    std::vector<Dentry> dentrys{pendingTx.dentrys().begin(),
                                pendingTx.dentrys().end()};
    for (const auto& it : dentrys) {
        if (!IsInodeBelongs(it.fsid(), it.parentinodeid())) {
            return false;
        }
    }

    if (GetStatus() == PartitionStatus::DELETING) {
        return false;
    }

    auto renameTx = RenameTx(dentrys, dentryStorage_);
    return txManager_->InsertPendingTx(renameTx);
}

bool Partition::FindPendingTx(PrepareRenameTxRequest* pendingTx) {
    if (GetStatus() == PartitionStatus::DELETING) {
        return false;
    }

    RenameTx renameTx;
    auto succ = txManager_->FindPendingTx(&renameTx);
    if (!succ) {
        return false;
    }

    auto dentrys = renameTx.GetDentrys();
    pendingTx->set_poolid(partitionInfo_.poolid());
    pendingTx->set_copysetid(partitionInfo_.copysetid());
    pendingTx->set_partitionid(partitionInfo_.partitionid());
    *pendingTx->mutable_dentrys() = {dentrys->begin(), dentrys->end()};
    return true;
}

// inode
MetaStatusCode Partition::CreateInode(const InodeParam &param,
                                      Inode* inode) {
    if (GetStatus() == PartitionStatus::READONLY) {
        return MetaStatusCode::PARTITION_ALLOC_ID_FAIL;
    }

    if (GetStatus() == PartitionStatus::DELETING) {
        return MetaStatusCode::PARTITION_DELETING;
    }

    uint64_t inodeId = GetNewInodeId();
    if (inodeId == UINT64_MAX) {
        return MetaStatusCode::PARTITION_ALLOC_ID_FAIL;
    }

    if (!IsInodeBelongs(param.fsId, inodeId)) {
        return MetaStatusCode::PARTITION_ID_MISSMATCH;
    }

    return UpdatePartitionInfoFsType2InodeNum(
        inodeManager_->CreateInode(inodeId, param, inode), param.type, 1);
}

MetaStatusCode Partition::CreateRootInode(const InodeParam &param) {
    if (!IsInodeBelongs(param.fsId)) {
        return MetaStatusCode::PARTITION_ID_MISSMATCH;
    }

    if (GetStatus() == PartitionStatus::DELETING) {
        return MetaStatusCode::PARTITION_DELETING;
    }

    return UpdatePartitionInfoFsType2InodeNum(
        inodeManager_->CreateRootInode(param), param.type, 1);
}

MetaStatusCode Partition::GetInode(uint32_t fsId, uint64_t inodeId,
                                   Inode* inode) {
    if (!IsInodeBelongs(fsId, inodeId)) {
        return MetaStatusCode::PARTITION_ID_MISSMATCH;
    }

    return inodeManager_->GetInode(fsId, inodeId, inode);
}

MetaStatusCode Partition::GetInodeAttr(uint32_t fsId, uint64_t inodeId,
                                       InodeAttr* attr) {
    if (!IsInodeBelongs(fsId, inodeId)) {
        return MetaStatusCode::PARTITION_ID_MISSMATCH;
    }

    return inodeManager_->GetInodeAttr(fsId, inodeId, attr);
}

MetaStatusCode Partition::GetXAttr(uint32_t fsId, uint64_t inodeId,
                                   XAttr* xattr) {
    if (!IsInodeBelongs(fsId, inodeId)) {
        return MetaStatusCode::PARTITION_ID_MISSMATCH;
    }

    return inodeManager_->GetXAttr(fsId, inodeId, xattr);
}

MetaStatusCode Partition::DeleteInode(uint32_t fsId, uint64_t inodeId) {
    if (!IsInodeBelongs(fsId, inodeId)) {
        return MetaStatusCode::PARTITION_ID_MISSMATCH;
    }
    InodeAttr attr;
    GetInodeAttr(fsId, inodeId, &attr);

    return UpdatePartitionInfoFsType2InodeNum(
        inodeManager_->DeleteInode(fsId, inodeId), attr.type(), -1);
}

MetaStatusCode Partition::UpdateInode(const UpdateInodeRequest& request) {
    if (!IsInodeBelongs(request.fsid(), request.inodeid())) {
        return MetaStatusCode::PARTITION_ID_MISSMATCH;
    }

    if (GetStatus() == PartitionStatus::DELETING) {
        return MetaStatusCode::PARTITION_DELETING;
    }
    Inode inode;
    int deletedNum = 0;
    const MetaStatusCode& ret =
        inodeManager_->UpdateInode(request, &inode, &deletedNum);

    return UpdatePartitionInfoFsType2InodeNum(ret, inode.type(),
                                              (-1) * deletedNum);
}

MetaStatusCode Partition::GetOrModifyS3ChunkInfo(
    uint32_t fsId,
    uint64_t inodeId,
    const S3ChunkInfoMap& map2add,
    const S3ChunkInfoMap& map2del,
    bool returnS3ChunkInfoMap,
    std::shared_ptr<Iterator>* iterator) {
    if (!IsInodeBelongs(fsId, inodeId)) {
        return MetaStatusCode::PARTITION_ID_MISSMATCH;
    } else if (GetStatus() == PartitionStatus::DELETING) {
        return MetaStatusCode::PARTITION_DELETING;
    }

    return inodeManager_->GetOrModifyS3ChunkInfo(
        fsId, inodeId, map2add, map2del, returnS3ChunkInfoMap, iterator);
}

MetaStatusCode Partition::PaddingInodeS3ChunkInfo(int32_t fsId,
                                                  uint64_t inodeId,
                                                  S3ChunkInfoMap* m,
                                                  uint64_t limit) {
    if (!IsInodeBelongs(fsId, inodeId)) {
        return MetaStatusCode::PARTITION_ID_MISSMATCH;
    } else if (GetStatus() == PartitionStatus::DELETING) {
        return MetaStatusCode::PARTITION_DELETING;
    }
    return inodeManager_->PaddingInodeS3ChunkInfo(fsId, inodeId, m, limit);
}

MetaStatusCode Partition::InsertInode(const Inode& inode) {
    if (!IsInodeBelongs(inode.fsid(), inode.inodeid())) {
        return MetaStatusCode::PARTITION_ID_MISSMATCH;
    }

    return UpdatePartitionInfoFsType2InodeNum(inodeManager_->InsertInode(inode),
                                              inode.type(), 1);
}

bool Partition::GetInodeIdList(std::list<uint64_t>* InodeIdList) {
    return inodeManager_->GetInodeIdList(InodeIdList);
}

bool Partition::IsDeletable() {
    // if patition has no inode or no dentry, it is deletable
    if (dentryStorage_->Size() != 0) {
        return false;
    }

    if (inodeStorage_->Size() != 0) {
        return false;
    }

    // TODO(@Wine93): add check txManager

    return true;
}

bool Partition::IsInodeBelongs(uint32_t fsId, uint64_t inodeId) {
    if (fsId != partitionInfo_.fsid()) {
        LOG(WARNING) << "partition fsid mismatch, fsId = " << fsId
                     << ", inodeId = " << inodeId
                     << ", partition fsId = " << partitionInfo_.fsid();
        return false;
    }

    if (inodeId < partitionInfo_.start() || inodeId > partitionInfo_.end()) {
        LOG(WARNING) << "partition inode mismatch, fsId = " << fsId
                     << ", inodeId = " << inodeId
                     << ", partition fsId = " << partitionInfo_.fsid()
                     << ", partition starst = " << partitionInfo_.start()
                     << ", partition end = " << partitionInfo_.end();
        return false;
    }

    return true;
}

bool Partition::IsInodeBelongs(uint32_t fsId) {
    if (fsId != partitionInfo_.fsid()) {
        return false;
    }

    return true;
}

uint32_t Partition::GetPartitionId() const {
    return partitionInfo_.partitionid();
}

PartitionInfo Partition::GetPartitionInfo() { return partitionInfo_; }

std::shared_ptr<Iterator> Partition::GetAllInode() {
    return inodeStorage_->GetAllInode();
}

std::shared_ptr<Iterator> Partition::GetAllDentry() {
    return dentryStorage_->GetAll();
}

std::shared_ptr<Iterator> Partition::GetAllS3ChunkInfoList() {
    return inodeStorage_->GetAllS3ChunkInfoList();
}

std::shared_ptr<Iterator> Partition::GetAllVolumeExtentList() {
    return inodeStorage_->GetAllVolumeExtentList();
}

bool Partition::Clear() {
    if (inodeStorage_->Clear() != MetaStatusCode::OK) {
        LOG(ERROR) << "Clear inode storage failed";
        return false;
    } else if (dentryStorage_->Clear() != MetaStatusCode::OK) {
        LOG(ERROR) << "Clear dentry storage failed";
        return false;
    }
    partitionInfo_.set_inodenum(0);
    for (auto& it : *partitionInfo_.mutable_filetype2inodenum()) {
        it.second = 0;
    }
    return true;
}

uint64_t Partition::GetNewInodeId() {
    if (partitionInfo_.nextid() > partitionInfo_.end()) {
        partitionInfo_.set_status(PartitionStatus::READONLY);
        return UINT64_MAX;
    }
    uint64_t newInodeId = partitionInfo_.nextid();
    partitionInfo_.set_nextid(newInodeId + 1);
    return newInodeId;
}

uint32_t Partition::GetInodeNum() {
    return static_cast<uint32_t>(inodeStorage_->Size());
}

uint32_t Partition::GetDentryNum() {
    return static_cast<uint32_t>(dentryStorage_->Size());
}

std::string Partition::GetInodeTablename() {
    std::ostringstream oss;
    oss << "partition:" << GetPartitionId() << ":inode";
    return oss.str();
}

std::string Partition::GetDentryTablename() {
    std::ostringstream oss;
    oss << "partition:" << GetPartitionId() << ":dentry";
    return oss.str();
}

#define PRECHECK(fsId, inodeId)                            \
    do {                                                   \
        if (!IsInodeBelongs((fsId), (inodeId))) {          \
            return MetaStatusCode::PARTITION_ID_MISSMATCH; \
        }                                                  \
        if (GetStatus() == PartitionStatus::DELETING) {    \
            return MetaStatusCode::PARTITION_DELETING;     \
        }                                                  \
    } while (0)

MetaStatusCode Partition::UpdateVolumeExtent(uint32_t fsId,
                                             uint64_t inodeId,
                                             const VolumeExtentList& extents) {
    PRECHECK(fsId, inodeId);
    return inodeManager_->UpdateVolumeExtent(fsId, inodeId, extents);
}

MetaStatusCode Partition::UpdateVolumeExtentSlice(
    uint32_t fsId,
    uint64_t inodeId,
    const VolumeExtentSlice& slice) {
    PRECHECK(fsId, inodeId);
    return inodeManager_->UpdateVolumeExtentSlice(fsId, inodeId, slice);
}

MetaStatusCode Partition::GetVolumeExtent(uint32_t fsId,
                                          uint64_t inodeId,
                                          const std::vector<uint64_t>& slices,
                                          VolumeExtentList* extents) {
    PRECHECK(fsId, inodeId);
    return inodeManager_->GetVolumeExtent(fsId, inodeId, slices, extents);
}

}  // namespace metaserver
}  // namespace curvefs
