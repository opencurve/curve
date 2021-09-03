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
namespace curvefs {
namespace metaserver {
Partition::Partition(const PartitionInfo& paritionInfo) {
    assert(paritionInfo.start() <= paritionInfo.end());

    inodeStorage_ = std::make_shared<MemoryInodeStorage>();
    dentryStorage_ = std::make_shared<MemoryDentryStorage>();
    inodeManager_ = std::make_shared<InodeManager>(inodeStorage_);
    dentryManager_ = std::make_shared<DentryManager>(dentryStorage_);
    partitionInfo_ = paritionInfo;
    if (!paritionInfo.has_nextid()) {
        partitionInfo_.set_nextid(partitionInfo_.start());
    }
}

// dentry
MetaStatusCode Partition::CreateDentry(const Dentry& dentry) {
    return dentryManager_->CreateDentry(dentry);
}

MetaStatusCode Partition::GetDentry(uint32_t fsId, uint64_t parentId,
                                    const std::string& name, Dentry* dentry) {
    return dentryManager_->GetDentry(fsId, parentId, name, dentry);
}

MetaStatusCode Partition::DeleteDentry(uint32_t fsId, uint64_t parentId,
                                       const std::string& name) {
    return dentryManager_->DeleteDentry(fsId, parentId, name);
}

MetaStatusCode Partition::ListDentry(uint32_t fsId, uint64_t dirId,
                                     std::list<Dentry>* dentryList) {
    return dentryManager_->ListDentry(fsId, dirId, dentryList);
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
    return inodeManager_->CreateInode(fsId, inodeId, length, uid, gid, mode,
                                      type, symlink, inode);
}

MetaStatusCode Partition::CreateRootInode(uint32_t fsId, uint32_t uid,
                                          uint32_t gid, uint32_t mode) {
    return inodeManager_->CreateRootInode(fsId, uid, gid, mode);
}

MetaStatusCode Partition::GetInode(uint32_t fsId, uint64_t inodeId,
                                   Inode* inode) {
    return inodeManager_->GetInode(fsId, inodeId, inode);
}

MetaStatusCode Partition::DeleteInode(uint32_t fsId, uint64_t inodeId) {
    return inodeManager_->DeleteInode(fsId, inodeId);
}

MetaStatusCode Partition::UpdateInode(const Inode& inode) {
    return inodeManager_->UpdateInode(inode);
}

MetaStatusCode Partition::UpdateInodeVersion(uint32_t fsId, uint64_t inodeId,
                                             uint64_t* version) {
    return inodeManager_->UpdateInodeVersion(fsId, inodeId, version);
}

MetaStatusCode Partition::InsertInode(const Inode& inode) {
    return inodeManager_->InsertInode(inode);
}

bool Partition::IsDeletable() {
    // if patition has no inode or no dentry, it is deletable
    if (dentryStorage_->Count() != 0) {
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

uint32_t Partition::GetPartitionId() { return partitionInfo_.partitionid(); }

PartitionInfo Partition::GetPartitionInfo() { return partitionInfo_; }

InodeContainerType* Partition::GetInodeContainer() {
    return inodeStorage_->GetInodeContainer();
}

DentryContainerType* Partition::GetDentryContainer() {
    return dentryStorage_->GetDentryContainer();
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
