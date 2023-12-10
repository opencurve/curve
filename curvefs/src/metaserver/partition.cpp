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
#include <cstdint>
#include <memory>
#include <string>
#include <utility>

#include "curvefs/proto/metaserver.pb.h"
#include "curvefs/src/metaserver/copyset/copyset_node_manager.h"
#include "curvefs/src/metaserver/mds/fsinfo_manager.h"
#include "curvefs/src/metaserver/s3compact.h"
#include "curvefs/src/metaserver/s3compact_manager.h"
#include "curvefs/src/metaserver/space/inode_volume_space_deallocate.h"
#include "curvefs/src/metaserver/space/volume_deallocate_manager.h"
#include "curvefs/src/metaserver/storage/converter.h"
#include "curvefs/src/metaserver/trash_manager.h"

namespace curvefs {
namespace metaserver {

using ::curvefs::metaserver::storage::NameGenerator;

Partition::Partition(PartitionInfo partition,
                     std::shared_ptr<KVStorage> kvStorage, bool startCompact,
                     bool startVolumeDeallocate) {
    assert(partition.start() <= partition.end());
    partitionInfo_ = std::move(partition);
    uint32_t partitionId = partitionInfo_.partitionid();

    kvStorage_ = kvStorage;
    nameGen_ = std::make_shared<NameGenerator>(partitionId);

    uint64_t nInode = 0;
    uint64_t nDentry = 0;
    if (partitionInfo_.has_inodenum()) {
        nInode = partitionInfo_.inodenum();
    }
    if (partitionInfo_.has_dentrynum()) {
        nDentry = partitionInfo_.dentrynum();
    }
    inodeStorage_ =
        std::make_shared<InodeStorage>(kvStorage_, nameGen_, nInode);
    dentryStorage_ =
        std::make_shared<DentryStorage>(kvStorage_, nameGen_, nDentry);

    auto trash = std::make_shared<TrashImpl>(inodeStorage_,
        partitionInfo_.fsid(), partitionInfo_.poolid(),
        partitionInfo_.copysetid(), partitionInfo_.partitionid());
    inodeManager_ = std::make_shared<InodeManager>(
        inodeStorage_, trash, partitionInfo_.mutable_filetype2inodenum());
    txManager_ = std::make_shared<TxManager>(dentryStorage_, partitionInfo_);
    dentryManager_ =
        std::make_shared<DentryManager>(dentryStorage_, txManager_);
    if (!partitionInfo_.has_nextid()) {
        partitionInfo_.set_nextid(
            std::max(kMinPartitionStartId, partitionInfo_.start()));
    }

    if (partitionInfo_.status() != PartitionStatus::DELETING) {
        auto handle = std::async(std::launch::async, [&]() {
            TrashManager::GetInstance().Add(
                partitionInfo_.partitionid(), trash);
        });
        handle.wait();

        if (startCompact) {
            StartS3Compact();
        }
        if (startVolumeDeallocate) {
            StartVolumeDeallocate();
        }
    }
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

#define PRECHECK_FSID(fsId)                                \
    do {                                                   \
        if (!IsInodeBelongs((fsId))) {                     \
            return MetaStatusCode::PARTITION_ID_MISSMATCH; \
        }                                                  \
        if (GetStatus() == PartitionStatus::DELETING) {    \
            return MetaStatusCode::PARTITION_DELETING;     \
        }                                                  \
    } while (0)

MetaStatusCode Partition::CreateDentry(const Dentry& dentry, const Time& tm,
                                       int64_t logIndex) {
    PRECHECK(dentry.fsid(), dentry.parentinodeid());
    MetaStatusCode ret = dentryManager_->CreateDentry(dentry, logIndex);
    if (MetaStatusCode::OK == ret) {
        if (dentry.has_type()) {
            return inodeManager_->UpdateInodeWhenCreateOrRemoveSubNode(
                dentry, tm, true, logIndex);
        } else {
            LOG(ERROR) << "CreateDentry does not have type, "
                       << dentry.ShortDebugString();
            return MetaStatusCode::PARAM_ERROR;
        }
    } else if (MetaStatusCode::IDEMPOTENCE_OK == ret) {
        if (dentry.has_type()) {
            // NOTE: we enter here means that
            // this log maybe is "half apply"
            ret = inodeManager_->UpdateInodeWhenCreateOrRemoveSubNode(
                dentry, tm, true, logIndex);
            if (ret == MetaStatusCode::IDEMPOTENCE_OK) {
                ret = MetaStatusCode::OK;
            }
            if (ret != MetaStatusCode::OK) {
                return ret;
            }
        }
        return MetaStatusCode::OK;
    } else {
        return ret;
    }
}

MetaStatusCode Partition::LoadDentry(const DentryVec& vec, bool merge,
                                     int64_t logIndex) {
    auto dentry = vec.dentrys(0);

    PRECHECK(dentry.fsid(), dentry.parentinodeid());

    MetaStatusCode rc = dentryManager_->CreateDentry(vec, merge, logIndex);
    if (rc == MetaStatusCode::OK || rc == MetaStatusCode::IDEMPOTENCE_OK) {
        return MetaStatusCode::OK;
    }
    return rc;
}

MetaStatusCode Partition::DeleteDentry(
    const Dentry& dentry, const Time& tm, int64_t logIndex) {
    PRECHECK(dentry.fsid(), dentry.parentinodeid());

    MetaStatusCode ret = dentryManager_->DeleteDentry(dentry, logIndex);
    if (MetaStatusCode::OK == ret) {
        if (dentry.has_type()) {
            return inodeManager_->UpdateInodeWhenCreateOrRemoveSubNode(
                dentry, tm, false, logIndex);
        } else {
            LOG(ERROR) << "DeleteDentry does not have type, "
                       << dentry.ShortDebugString();
            return MetaStatusCode::PARAM_ERROR;
        }
    } else if (MetaStatusCode::IDEMPOTENCE_OK == ret) {
        if (dentry.has_type()) {
            // NOTE: we enter here means that
            // this log maybe is "half apply"
            ret = inodeManager_->UpdateInodeWhenCreateOrRemoveSubNode(
                dentry, tm, false, logIndex);
        }
        if (ret == MetaStatusCode::IDEMPOTENCE_OK) {
            ret = MetaStatusCode::OK;
        }
        return ret;
    } else {
        return ret;
    }
}

MetaStatusCode Partition::GetDentry(Dentry* dentry) {
    PRECHECK(dentry->fsid(), dentry->parentinodeid());
    return dentryManager_->GetDentry(dentry);
}

MetaStatusCode Partition::ListDentry(const Dentry& dentry,
                                     std::vector<Dentry>* dentrys,
                                     uint32_t limit, bool onlyDir) {
    PRECHECK(dentry.fsid(), dentry.parentinodeid());
    return dentryManager_->ListDentry(dentry, dentrys, limit, onlyDir);
}

void Partition::ClearDentry() { dentryManager_->ClearDentry(); }

MetaStatusCode Partition::HandleRenameTx(const std::vector<Dentry>& dentrys,
                                         int64_t logIndex) {
    for (const auto& it : dentrys) {
        PRECHECK(it.fsid(), it.parentinodeid());
    }

    return dentryManager_->HandleRenameTx(dentrys, logIndex);
}

bool Partition::InsertPendingTx(const PrepareRenameTxRequest& pendingTx) {
    std::vector<Dentry> dentrys{pendingTx.dentrys().begin(),
                                pendingTx.dentrys().end()};
    for (const auto& it : dentrys) {
        if (!IsInodeBelongs(it.fsid(), it.parentinodeid())) {
            return false;
        }
    }

    auto renameTx = RenameTx(dentrys, dentryStorage_);
    return txManager_->InsertPendingTx(renameTx);
}

bool Partition::Init() {
    // NOTE: invoke `dentryStorage::Init()`
    // and `inodeStorage::Init()` is unnecessary
    // they will be invoked by `manager`
    return dentryManager_->Init() && inodeManager_->Init() &&
           txManager_->Init();
}

void Partition::SerializeRenameTx(const RenameTx& in,
                                  PrepareRenameTxRequest* out) {
    txManager_->SerializeRenameTx(in, out);
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

    SerializeRenameTx(renameTx, pendingTx);
    return true;
}

// inode
MetaStatusCode Partition::CreateInode(const InodeParam& param, Inode* inode,
                                      int64_t logIndex) {
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

    auto ret = inodeManager_->CreateInode(inodeId, param, inode, logIndex);
    if (ret == MetaStatusCode::IDEMPOTENCE_OK) {
        ret = MetaStatusCode::OK;
    }
    return ret;
}

MetaStatusCode Partition::CreateRootInode(const InodeParam& param,
                                          int64_t logIndex) {
    PRECHECK_FSID(param.fsId);
    auto ret = inodeManager_->CreateRootInode(param, logIndex);
    if (ret == MetaStatusCode::IDEMPOTENCE_OK) {
        ret = MetaStatusCode::OK;
    }
    return ret;
}

MetaStatusCode Partition::CreateManageInode(const InodeParam& param,
                                            ManageInodeType manageType,
                                            Inode* inode, int64_t logIndex) {
    PRECHECK_FSID(param.fsId);
    auto ret =
        inodeManager_->CreateManageInode(param, manageType, inode, logIndex);
    if (ret == MetaStatusCode::IDEMPOTENCE_OK) {
        ret = MetaStatusCode::OK;
    }
    return ret;
}

MetaStatusCode Partition::GetInode(uint32_t fsId, uint64_t inodeId,
                                   Inode* inode) {
    PRECHECK(fsId, inodeId);
    return inodeManager_->GetInode(fsId, inodeId, inode);
}

MetaStatusCode Partition::GetInodeAttr(uint32_t fsId, uint64_t inodeId,
                                       InodeAttr* attr) {
    PRECHECK(fsId, inodeId);
    return inodeManager_->GetInodeAttr(fsId, inodeId, attr);
}

MetaStatusCode Partition::GetXAttr(uint32_t fsId, uint64_t inodeId,
                                   XAttr* xattr) {
    PRECHECK(fsId, inodeId);
    return inodeManager_->GetXAttr(fsId, inodeId, xattr);
}

MetaStatusCode Partition::DeleteInode(uint32_t fsId, uint64_t inodeId,
                                      int64_t logIndex) {
    PRECHECK(fsId, inodeId);
    auto ret = inodeManager_->DeleteInode(fsId, inodeId, logIndex);
    if (ret == MetaStatusCode::IDEMPOTENCE_OK) {
        ret = MetaStatusCode::OK;
    }
    return ret;
}

MetaStatusCode Partition::UpdateInode(const UpdateInodeRequest& request,
                                      int64_t logIndex) {
    PRECHECK(request.fsid(), request.inodeid());
    auto ret = inodeManager_->UpdateInode(request, logIndex);
    if (ret == MetaStatusCode::IDEMPOTENCE_OK) {
        ret = MetaStatusCode::OK;
    }
    return ret;
}

MetaStatusCode Partition::GetOrModifyS3ChunkInfo(
    uint32_t fsId, uint64_t inodeId, const S3ChunkInfoMap& map2add,
    const S3ChunkInfoMap& map2del, bool returnS3ChunkInfoMap,
    std::shared_ptr<Iterator>* iterator, int64_t logIndex) {
    PRECHECK(fsId, inodeId);
    auto ret = inodeManager_->GetOrModifyS3ChunkInfo(
        fsId, inodeId, map2add, map2del, returnS3ChunkInfoMap, iterator,
        logIndex);
    if (ret == MetaStatusCode::IDEMPOTENCE_OK) {
        ret = MetaStatusCode::OK;
    }
    return ret;
}

MetaStatusCode Partition::PaddingInodeS3ChunkInfo(int32_t fsId,
                                                  uint64_t inodeId,
                                                  S3ChunkInfoMap* m,
                                                  uint64_t limit) {
    PRECHECK(fsId, inodeId);
    return inodeManager_->PaddingInodeS3ChunkInfo(fsId, inodeId, m, limit);
}

bool Partition::GetInodeIdList(std::list<uint64_t>* InodeIdList) {
    return inodeManager_->GetInodeIdList(InodeIdList);
}

bool Partition::IsDeletable() {
    // if patition has no inode or no dentry, it is deletable
    if (!dentryStorage_->Empty()) {
        return false;
    }

    if (!inodeStorage_->Empty()) {
        return false;
    }

    // TODO(@Wine93): add check txManager

    return true;
}

bool Partition::IsInodeBelongs(uint32_t fsId, uint64_t inodeId) const {
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

bool Partition::IsInodeBelongs(uint32_t fsId) const {
    if (fsId != partitionInfo_.fsid()) {
        return false;
    }

    return true;
}

PartitionInfo Partition::GetPartitionInfo() {
    partitionInfo_.set_inodenum(GetInodeNum());
    partitionInfo_.set_dentrynum(GetDentryNum());
    return partitionInfo_;
}

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
    partitionInfo_.set_dentrynum(0);
    for (auto& it : *partitionInfo_.mutable_filetype2inodenum()) {
        it.second = 0;
    }

    LOG(INFO) << "Clear partition " << partitionInfo_.partitionid()
              << " success";
    return true;
}

// NOTE: store nextid to kvstroage is unnecessary
// we will replay the logs filter the log entries that
// already applied, but keep nextid changes in memory
// so it will grow to corrected value after replay
uint64_t Partition::GetNewInodeId() {
    if (partitionInfo_.nextid() > partitionInfo_.end()) {
        partitionInfo_.set_status(PartitionStatus::READONLY);
        return UINT64_MAX;
    }
    uint64_t newInodeId = partitionInfo_.nextid();
    partitionInfo_.set_nextid(newInodeId + 1);
    return newInodeId;
}

uint64_t Partition::GetInodeNum() {
    return inodeStorage_->Size();
}

uint64_t Partition::GetDentryNum() {
    return dentryStorage_->Size();
}

bool Partition::EmptyInodeStorage() { return inodeStorage_->Empty(); }

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

MetaStatusCode Partition::UpdateVolumeExtent(
    uint32_t fsId, uint64_t inodeId, const VolumeExtentSliceList& extents,
    int64_t logIndex) {
    PRECHECK(fsId, inodeId);
    auto ret =
        inodeManager_->UpdateVolumeExtent(fsId, inodeId, extents, logIndex);
    if (ret == MetaStatusCode::IDEMPOTENCE_OK) {
        ret = MetaStatusCode::OK;
    }
    return ret;
}

MetaStatusCode Partition::UpdateVolumeExtentSlice(
    uint32_t fsId, uint64_t inodeId, const VolumeExtentSlice& slice,
    int64_t logIndex) {
    PRECHECK(fsId, inodeId);
    auto ret =
        inodeManager_->UpdateVolumeExtentSlice(fsId, inodeId, slice, logIndex);
    if (ret == MetaStatusCode::IDEMPOTENCE_OK) {
        ret = MetaStatusCode::OK;
    }
    return ret;
}

MetaStatusCode Partition::GetVolumeExtent(uint32_t fsId, uint64_t inodeId,
                                          const std::vector<uint64_t>& slices,
                                          VolumeExtentSliceList* extents) {
    PRECHECK(fsId, inodeId);
    return inodeManager_->GetVolumeExtent(fsId, inodeId, slices, extents);
}

MetaStatusCode Partition::UpdateDeallocatableBlockGroup(
    const UpdateDeallocatableBlockGroupRequest& request, int64_t logIndex) {
    PRECHECK_FSID(request.fsid());
    auto ret = inodeManager_->UpdateDeallocatableBlockGroup(request, logIndex);
    if (ret == MetaStatusCode::IDEMPOTENCE_OK) {
        ret = MetaStatusCode::OK;
    }
    return ret;
}

MetaStatusCode Partition::GetAllBlockGroup(
    std::vector<DeallocatableBlockGroup>* deallocatableBlockGroupVec) {
    return inodeStorage_->GetAllBlockGroup(deallocatableBlockGroupVec);
}

void Partition::StartS3Compact() {
    S3CompactManager::GetInstance().Register(
        S3Compact{inodeManager_, partitionInfo_});
}

void Partition::CancelS3Compact() {
    S3CompactManager::GetInstance().Cancel(partitionInfo_.partitionid());
}

void Partition::StartVolumeDeallocate() {
    FsInfo fsInfo;
    bool ok =
        FsInfoManager::GetInstance().GetFsInfo(partitionInfo_.fsid(), &fsInfo);
    if (!ok) {
        LOG(ERROR)
            << "Partition start volume deallocate fail, get fsinfo fail. fsid="
            << partitionInfo_.fsid();
        return;
    }

    if (!fsInfo.detail().has_volume()) {
        LOG(INFO) << "Partition not belong to volume, do not need start "
                     "deallocate. partitionInfo="
                  << partitionInfo_.DebugString();
        return;
    }

    VolumeDeallocateCalOption calOpt;
    calOpt.kvStorage = kvStorage_;
    calOpt.inodeStorage = inodeStorage_;
    calOpt.nameGen = nameGen_;
    auto copysetNode =
        copyset::CopysetNodeManager::GetInstance().GetSharedCopysetNode(
            partitionInfo_.poolid(), partitionInfo_.copysetid());
    if (copysetNode == nullptr) {
        LOG(ERROR) << "Partition get copyset node failed. poolid="
                   << partitionInfo_.poolid()
                   << ", copysetid=" << partitionInfo_.copysetid();
        return;
    }

    InodeVolumeSpaceDeallocate task(partitionInfo_.fsid(),
                                    partitionInfo_.partitionid(), copysetNode);
    task.Init(calOpt);

    VolumeDeallocateManager::GetInstance().Register(std::move(task));

    VLOG(3) << "Partition start volume deallocate success. partitionInfo="
            << partitionInfo_.DebugString();
}

void Partition::CancelVolumeDeallocate() {
    VolumeDeallocateManager::GetInstance().Cancel(partitionInfo_.partitionid());
}

void Partition::SetVolumeDeallocate(uint64_t fsId, uint64_t blockGroupOffset) {
    assert(fsId == partitionInfo_.fsid());
    VolumeDeallocateManager::GetInstance().Deallocate(
        partitionInfo_.partitionid(), blockGroupOffset);
}

}  // namespace metaserver
}  // namespace curvefs
