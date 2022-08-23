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
 * Project: Curve
 * Created Date: 2021-09-11
 * Author: Jingli Chen (Wine93)
 */

#include <list>

#include "src/common/uuid.h"
#include "curvefs/src/client/client_operator.h"

namespace curvefs {
namespace client {

using ::curve::common::UUIDGenerator;
using ::curvefs::metaserver::DentryFlag;
using ::curvefs::mds::topology::PartitionTxId;

#define LOG_ERROR(action, rc) \
    LOG(ERROR) << action << " failed, retCode = " << rc \
               << ", DebugString = " << DebugString();

RenameOperator::RenameOperator(uint32_t fsId,
                               const std::string& fsName,
                               uint64_t parentId,
                               std::string name,
                               uint64_t newParentId,
                               std::string newname,
                               std::shared_ptr<DentryCacheManager> dentryManager,  // NOLINT
                               std::shared_ptr<InodeCacheManager> inodeManager,
                               std::shared_ptr<MetaServerClient> metaClient,
                               std::shared_ptr<MdsClient> mdsClient,
                               bool enableParallel)
    : fsId_(fsId),
      fsName_(fsName),
      parentId_(parentId),
      name_(name),
      newParentId_(newParentId),
      newname_(newname),
      srcPartitionId_(0),
      dstPartitionId_(0),
      srcTxId_(0),
      dstTxId_(0),
      oldInodeId_(0),
      oldInodeSize_(-1),
      dentryManager_(dentryManager),
      inodeManager_(inodeManager),
      metaClient_(metaClient),
      mdsClient_(mdsClient),
      enableParallel_(enableParallel),
      uuid_(),
      sequence_(0) {}

std::string RenameOperator::DebugString() {
    std::ostringstream os;
    os << "( fsId = " << fsId_
       << ", fsName = " << fsName_
       << ", parentId = " << parentId_ << ", name = " << name_
       << ", newParentId = " << newParentId_ << ", newname = " << newname_
       << ", srcPartitionId = " << srcPartitionId_
       << ", dstPartitionId = " << dstPartitionId_
       << ", srcTxId = " << srcTxId_ << ", dstTxId_ = " << dstTxId_
       << ", oldInodeId = " << oldInodeId_
       << ", srcDentry = [" << srcDentry_.ShortDebugString() << "]"
       << ", dstDentry = [" << dstDentry_.ShortDebugString() << "]"
       << ", prepare dentry = [" << dentry_.ShortDebugString() << "]"
       << ", prepare new dentry = [" << newDentry_.ShortDebugString() << "]"
       << ", enableParallel = " << enableParallel_
       << ", uuid = " << uuid_
       << ", sequence = " << sequence_ << ")";
    return os.str();
}

CURVEFS_ERROR RenameOperator::GetTxId(uint32_t fsId,
                                      uint64_t inodeId,
                                      uint32_t* partitionId,
                                      uint64_t* txId) {
    auto rc = metaClient_->GetTxId(fsId, inodeId, partitionId, txId);
    if (rc != MetaStatusCode::OK) {
        LOG_ERROR("GetTxId", rc);
    }
    return MetaStatusCodeToCurvefsErrCode(rc);
}

CURVEFS_ERROR RenameOperator::GetLatestTxIdWithLock() {
    std::vector<PartitionTxId> txIds;
    uuid_ = UUIDGenerator().GenerateUUID();
    auto rc = mdsClient_->GetLatestTxIdWithLock(
        fsId_, fsName_, uuid_, &txIds, &sequence_);
    if (rc != FSStatusCode::OK) {
        return CURVEFS_ERROR::INTERNAL;
    }

    for (const auto& item : txIds) {
        SetTxId(item.partitionid(), item.txid());
    }
    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR RenameOperator::GetTxId() {
    CURVEFS_ERROR rc;
    if (enableParallel_) {
        rc = GetLatestTxIdWithLock();
        if (rc != CURVEFS_ERROR::OK) {
            LOG_ERROR("GetLatestTxIdWithLock", rc);
            return rc;
        }
    }

    rc = GetTxId(fsId_, parentId_, &srcPartitionId_, &srcTxId_);
    if (rc != CURVEFS_ERROR::OK) {
        LOG_ERROR("GetTxId", rc);
        return rc;
    }

    rc = GetTxId(fsId_, newParentId_, &dstPartitionId_, &dstTxId_);
    if (rc != CURVEFS_ERROR::OK) {
        LOG_ERROR("GetTxId", rc);
    }

    return rc;
}

void RenameOperator::SetTxId(uint32_t partitionId, uint64_t txId) {
    metaClient_->SetTxId(partitionId, txId);
}

// TODO(Wine93): we should improve the check for whether a directory is empty
CURVEFS_ERROR RenameOperator::CheckOverwrite() {
    if (dstDentry_.flag() & DentryFlag::TYPE_FILE_FLAG) {
        return CURVEFS_ERROR::OK;
    }

    std::list<Dentry> dentrys;
    auto rc = dentryManager_->ListDentry(dstDentry_.inodeid(), &dentrys, 1);
    if (rc == CURVEFS_ERROR::OK && !dentrys.empty()) {
        LOG(ERROR) << "The directory is not empty"
                   << ", dentry = (" << dstDentry_.ShortDebugString() << ")";
        rc = CURVEFS_ERROR::NOTEMPTY;
    }

    return rc;
}

// The rename operate must met the following 2 conditions:
//   (1) the source dentry must exist
//   (2) if the target dentry exist then it must be file or an empty directory
CURVEFS_ERROR RenameOperator::Precheck() {
    auto rc = dentryManager_->GetDentry(parentId_, name_, &srcDentry_);
    if (rc != CURVEFS_ERROR::OK) {
        LOG_ERROR("GetDentry", rc);
        return rc;
    }

    rc = dentryManager_->GetDentry(newParentId_, newname_, &dstDentry_);
    if (rc == CURVEFS_ERROR::NOTEXIST) {
        return CURVEFS_ERROR::OK;
    } else if (rc == CURVEFS_ERROR::OK) {
        oldInodeId_ = dstDentry_.inodeid();
        return CheckOverwrite();
    }

    LOG_ERROR("GetDentry", rc);
    return rc;
}

// record old inode info if overwrite
CURVEFS_ERROR RenameOperator::RecordOldInodeInfo() {
    if (oldInodeId_ != 0) {
        std::shared_ptr<InodeWrapper> inodeWrapper;
        auto rc = inodeManager_->GetInode(oldInodeId_, inodeWrapper);
        if (rc == CURVEFS_ERROR::OK) {
            oldInodeSize_ = inodeWrapper->GetLength();
            oldInodeType_ = inodeWrapper->GetType();
        } else {
            LOG_ERROR("GetInode", rc);
            return CURVEFS_ERROR::NOTEXIST;
        }
    }

    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR RenameOperator::PrepareRenameTx(
    const std::vector<Dentry>& dentrys) {
    auto rc = metaClient_->PrepareRenameTx(dentrys);
    if (rc != MetaStatusCode::OK) {
        LOG_ERROR("PrepareRenameTx", rc);
    }

    return MetaStatusCodeToCurvefsErrCode(rc);
}

CURVEFS_ERROR RenameOperator::PrepareTx() {
    dentry_ = Dentry(srcDentry_);
    dentry_.set_txid(srcTxId_ + 1);
    dentry_.set_txsequence(sequence_);
    dentry_.set_flag(dentry_.flag() |
                     DentryFlag::DELETE_MARK_FLAG |
                     DentryFlag::TRANSACTION_PREPARE_FLAG);
    dentry_.set_type(srcDentry_.type());

    newDentry_ = Dentry(srcDentry_);
    newDentry_.set_parentinodeid(newParentId_);
    newDentry_.set_name(newname_);
    newDentry_.set_txid(dstTxId_ + 1);
    newDentry_.set_txsequence(sequence_);
    newDentry_.set_flag(newDentry_.flag() |
                        DentryFlag::TRANSACTION_PREPARE_FLAG);
    newDentry_.set_type(srcDentry_.type());

    CURVEFS_ERROR rc;
    std::vector<Dentry> dentrys{ dentry_ };
    if (srcPartitionId_ == dstPartitionId_) {
        dentrys.push_back(newDentry_);
        rc = PrepareRenameTx(dentrys);
    } else {
        rc = PrepareRenameTx(dentrys);
        if (rc == CURVEFS_ERROR::OK) {
            dentrys[0] = newDentry_;
            rc = PrepareRenameTx(dentrys);
        }
    }

    if (rc != CURVEFS_ERROR::OK) {
        LOG_ERROR("PrepareTx", rc);
    }
    return rc;
}

CURVEFS_ERROR RenameOperator::CommitTx() {
    PartitionTxId partitionTxId;
    std::vector<PartitionTxId> txIds;

    partitionTxId.set_partitionid(srcPartitionId_);
    partitionTxId.set_txid(srcTxId_ + 1);
    txIds.push_back(partitionTxId);

    if (srcPartitionId_ != dstPartitionId_) {
        partitionTxId.set_partitionid(dstPartitionId_);
        partitionTxId.set_txid(dstTxId_ + 1);
        txIds.push_back(partitionTxId);
    }

    FSStatusCode rc;
    if (enableParallel_) {
        rc = mdsClient_->CommitTxWithLock(txIds, fsName_, uuid_, sequence_);
    } else {
        rc = mdsClient_->CommitTx(txIds);
    }
    if (rc != FSStatusCode::OK) {
        LOG_ERROR("CommitTx", rc);
        return CURVEFS_ERROR::INTERNAL;
    }
    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR RenameOperator::LinkInode(uint64_t inodeId, uint64_t parent) {
    std::shared_ptr<InodeWrapper> inodeWrapper;
    auto rc = inodeManager_->GetInode(inodeId, inodeWrapper);
    if (rc != CURVEFS_ERROR::OK) {
        LOG_ERROR("GetInode", rc);
        return rc;
    }

    rc = inodeWrapper->LinkLocked(parent);
    if (rc != CURVEFS_ERROR::OK) {
        LOG_ERROR("Link", rc);
        return rc;
    }

    // CURVEFS_ERROR::OK
    return rc;
}

CURVEFS_ERROR RenameOperator::UnLinkInode(uint64_t inodeId, uint64_t parent) {
    std::shared_ptr<InodeWrapper> inodeWrapper;
    auto rc = inodeManager_->GetInode(inodeId, inodeWrapper);
    if (rc != CURVEFS_ERROR::OK) {
        LOG_ERROR("GetInode", rc);
        return rc;
    }

    rc = inodeWrapper->UnLinkLocked(parent);
    if (rc != CURVEFS_ERROR::OK) {
        LOG_ERROR("UnLink", rc);
        return rc;
    }

    // CURVEFS_ERROR::OK
    return rc;
}

CURVEFS_ERROR RenameOperator::UpdateMCTime(uint64_t inodeId) {
    std::shared_ptr<InodeWrapper> inodeWrapper;
    auto rc = inodeManager_->GetInode(inodeId, inodeWrapper);
    if (rc != CURVEFS_ERROR::OK) {
        LOG_ERROR("GetInode", rc);
        return rc;
    }

    curve::common::UniqueLock lk = inodeWrapper->GetUniqueLock();
    inodeWrapper->UpdateTimestampLocked(kModifyTime | kChangeTime);

    rc = inodeWrapper->SyncAttr();
    if (rc != CURVEFS_ERROR::OK) {
        LOG_ERROR("SyncAttr", rc);
        return rc;
    }
    // CURVEFS_ERROR::OK
    return rc;
}

CURVEFS_ERROR RenameOperator::LinkDestParentInode() {
    // Link action is unnecessary when met one of the following 2 conditions:
    //   (1) source and destination under same directory
    //   (2) destination already exist
    //   (3) destination is not a directory
    if (!srcDentry_.has_type()) {
        LOG(ERROR) << "srcDentry_ not have type!"
                   << "Dentry: " << srcDentry_.ShortDebugString();
        return CURVEFS_ERROR::INTERNAL;
    }
    if (FsFileType::TYPE_DIRECTORY != srcDentry_.type() ||
        parentId_ == newParentId_ || oldInodeId_ != 0) {
        UpdateMCTime(newParentId_);
        return CURVEFS_ERROR::OK;
    }
    return LinkInode(newParentId_);
}

CURVEFS_ERROR RenameOperator::UnlinkSrcParentInode() {
    // UnLink action is unnecessary when met the following 2 conditions:
    //   (1) source and destination under same directory
    //   (2) destination not exist
    // or
    //    source is not a directory
    if (!srcDentry_.has_type()) {
        LOG(ERROR) << "srcDentry_ not have type!"
                   << "Dentry: " << srcDentry_.ShortDebugString();
        return CURVEFS_ERROR::INTERNAL;
    }
    if (FsFileType::TYPE_DIRECTORY != srcDentry_.type() ||
        (parentId_ == newParentId_ && oldInodeId_ == 0)) {
        if (parentId_ != newParentId_) {
            UpdateMCTime(parentId_);
        }
        return CURVEFS_ERROR::OK;
    }
    auto rc = UnLinkInode(parentId_);
    LOG(INFO) << "Unlink source parent inode, retCode = " << rc;
    return rc;
}

void RenameOperator::UnlinkOldInode() {
    if (oldInodeId_ == 0) {
        return;
    }

    auto rc = UnLinkInode(oldInodeId_, newParentId_);
    LOG(INFO) << "Unlink old inode, retCode = " << rc;
}

CURVEFS_ERROR RenameOperator::UpdateInodeParent() {
    std::shared_ptr<InodeWrapper> inodeWrapper;
    auto rc = inodeManager_->GetInode(srcDentry_.inodeid(), inodeWrapper);
    if (rc != CURVEFS_ERROR::OK) {
        LOG_ERROR("GetInode", rc);
        return rc;
    }

    rc = inodeWrapper->UpdateParentLocked(parentId_, newParentId_);
    if (rc != CURVEFS_ERROR::OK) {
        LOG_ERROR("UpdateInodeParent", rc);
        return rc;
    }

    LOG(INFO) << "UpdateInodeParent oldParent = " << parentId_
              << ", newParent = " << newParentId_;
    return rc;
}

void RenameOperator::UpdateCache() {
    dentryManager_->DeleteCache(parentId_, name_);
    dentryManager_->InsertOrReplaceCache(newDentry_);
    SetTxId(srcPartitionId_, srcTxId_ + 1);
    SetTxId(dstPartitionId_, dstTxId_ + 1);
}

}  // namespace client
}  // namespace curvefs
