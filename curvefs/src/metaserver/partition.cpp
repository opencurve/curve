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

#include "curvefs/src/metaserver/s3compact_manager.h"
#include "curvefs/src/metaserver/trash_manager.h"

namespace curvefs {
namespace metaserver {

Partition::Partition(const PartitionInfo& paritionInfo) {
    assert(paritionInfo.start() <= paritionInfo.end());

    inodeStorage_ = std::make_shared<MemoryInodeStorage>();
    dentryStorage_ = std::make_shared<MemoryDentryStorage>();
    trash_ = std::make_shared<TrashImpl>(inodeStorage_);
    inodeManager_ = std::make_shared<InodeManager>(inodeStorage_, trash_);
    txManager_ = std::make_shared<TxManager>(dentryStorage_);
    dentryManager_ = std::make_shared<DentryManager>(dentryStorage_,
                                                     txManager_);
    partitionInfo_ = paritionInfo;
    if (!paritionInfo.has_nextid()) {
        partitionInfo_.set_nextid(
            std::max(kMinPartitionStartId, partitionInfo_.start()));
    }
    TrashManager::GetInstance().Add(paritionInfo.partitionid(), trash_);
    s3compact_ = std::make_shared<S3Compact>(inodeStorage_, &partitionInfo_);
    S3CompactManager::GetInstance().RegisterS3Compact(s3compact_);
}

// dentry
MetaStatusCode Partition::CreateDentry(const Dentry& dentry) {
    if (!IsInodeBelongs(dentry.fsid(), dentry.parentinodeid())) {
        return MetaStatusCode::PARTITION_ID_MISSMATCH;
    }
    return dentryManager_->CreateDentry(dentry);
}

MetaStatusCode Partition::DeleteDentry(const Dentry& dentry) {
    if (!IsInodeBelongs(dentry.fsid(), dentry.parentinodeid())) {
        return MetaStatusCode::PARTITION_ID_MISSMATCH;
    }
    return dentryManager_->DeleteDentry(dentry);
}

MetaStatusCode Partition::GetDentry(Dentry* dentry) {
    if (!IsInodeBelongs(dentry->fsid(), dentry->parentinodeid())) {
        return MetaStatusCode::PARTITION_ID_MISSMATCH;
    }
    return dentryManager_->GetDentry(dentry);
}

MetaStatusCode Partition::ListDentry(const Dentry& dentry,
                                    std::vector<Dentry>* dentrys,
                                    uint32_t limit) {
    if (!IsInodeBelongs(dentry.fsid(), dentry.parentinodeid())) {
        return MetaStatusCode::PARTITION_ID_MISSMATCH;
    }
    return dentryManager_->ListDentry(dentry, dentrys, limit);
}

MetaStatusCode Partition::HandleRenameTx(const std::vector<Dentry>& dentrys) {
    for (const auto& it : dentrys) {
        if (!IsInodeBelongs(it.fsid(), it.parentinodeid())) {
            return MetaStatusCode::PARTITION_ID_MISSMATCH;
        }
    }
    return dentryManager_->HandleRenameTx(dentrys);
}

bool Partition::InsertPendingTx(const PrepareRenameTxRequest& pendingTx) {
    std::vector<Dentry> dentrys{
        pendingTx.dentrys().begin(),
        pendingTx.dentrys().end()
    };
    for (const auto& it : dentrys) {
        if (!IsInodeBelongs(it.fsid(), it.parentinodeid())) {
            return false;
        }
    }
    auto renameTx = RenameTx(dentrys, dentryStorage_);
    return txManager_->InsertPendingTx(renameTx);
}

bool Partition::FindPendingTx(PrepareRenameTxRequest* pendingTx) {
    RenameTx renameTx;
    auto succ = txManager_->FindPendingTx(&renameTx);
    if (!succ) {
        return false;
    }

    auto dentrys = renameTx.GetDentrys();
    pendingTx->set_poolid(partitionInfo_.poolid());
    pendingTx->set_copysetid(partitionInfo_.copysetid());
    pendingTx->set_partitionid(partitionInfo_.partitionid());
    *pendingTx->mutable_dentrys() = { dentrys->begin(), dentrys->end() };
    return true;
}

// inode
MetaStatusCode Partition::CreateInode(uint32_t fsId, uint64_t length,
                                      uint32_t uid, uint32_t gid, uint32_t mode,
                                      FsFileType type,
                                      const std::string& symlink,
                                      Inode* inode) {
    uint64_t inodeId = GetNewInodeId();
    if (inodeId == UINT64_MAX) {
        return MetaStatusCode::PARTITION_ALLOC_ID_FAIL;
    }

    if (!IsInodeBelongs(fsId, inodeId)) {
        return MetaStatusCode::PARTITION_ID_MISSMATCH;
    }
    return inodeManager_->CreateInode(fsId, inodeId, length, uid, gid, mode,
                                      type, symlink, inode);
}

MetaStatusCode Partition::CreateRootInode(uint32_t fsId, uint32_t uid,
                                          uint32_t gid, uint32_t mode) {
    if (!IsInodeBelongs(fsId)) {
        return MetaStatusCode::PARTITION_ID_MISSMATCH;
    }
    return inodeManager_->CreateRootInode(fsId, uid, gid, mode);
}

MetaStatusCode Partition::GetInode(uint32_t fsId, uint64_t inodeId,
                                   Inode* inode) {
    if (!IsInodeBelongs(fsId, inodeId)) {
        return MetaStatusCode::PARTITION_ID_MISSMATCH;
    }
    return inodeManager_->GetInode(fsId, inodeId, inode);
}

MetaStatusCode Partition::DeleteInode(uint32_t fsId, uint64_t inodeId) {
    if (!IsInodeBelongs(fsId, inodeId)) {
        return MetaStatusCode::PARTITION_ID_MISSMATCH;
    }
    return inodeManager_->DeleteInode(fsId, inodeId);
}

MetaStatusCode Partition::UpdateInode(const Inode& inode) {
    if (!IsInodeBelongs(inode.fsid(), inode.inodeid())) {
        return MetaStatusCode::PARTITION_ID_MISSMATCH;
    }
    return inodeManager_->UpdateInode(inode);
}

MetaStatusCode Partition::UpdateInodeVersion(uint32_t fsId, uint64_t inodeId,
                                             uint64_t* version) {
    if (!IsInodeBelongs(fsId, inodeId)) {
        return MetaStatusCode::PARTITION_ID_MISSMATCH;
    }
    return inodeManager_->UpdateInodeVersion(fsId, inodeId, version);
}

MetaStatusCode Partition::InsertInode(const Inode& inode) {
    if (!IsInodeBelongs(inode.fsid(), inode.inodeid())) {
        return MetaStatusCode::PARTITION_ID_MISSMATCH;
    }
    return inodeManager_->InsertInode(inode);
}

bool Partition::IsDeletable() {
    // if patition has no inode or no dentry, it is deletable
    if (dentryStorage_->Size() != 0) {
        return false;
    }

    if (inodeStorage_->Count() != 0) {
        return false;
    }

    // TODO(@Wine93): add check txManager

    return true;
}

bool Partition::IsInodeBelongs(uint32_t fsId, uint64_t inodeId) {
    if (fsId != partitionInfo_.fsid()) {
        return false;
    }

    if (inodeId < partitionInfo_.start() || inodeId > partitionInfo_.end()) {
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

uint32_t Partition::GetPartitionId() { return partitionInfo_.partitionid(); }

PartitionInfo Partition::GetPartitionInfo() { return partitionInfo_; }

InodeStorage::ContainerType* Partition::GetInodeContainer() {
    return inodeStorage_->GetContainer();
}

DentryStorage::ContainerType* Partition::GetDentryContainer() {
    return dentryStorage_->GetContainer();
}

uint64_t Partition::GetNewInodeId() {
    if (partitionInfo_.nextid() > partitionInfo_.end()) {
        return UINT64_MAX;
    }
    uint64_t newInodeId = partitionInfo_.nextid();
    partitionInfo_.set_nextid(newInodeId + 1);
    return newInodeId;
}

}  // namespace metaserver
}  // namespace curvefs
