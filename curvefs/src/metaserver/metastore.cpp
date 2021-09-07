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
 * @Date: 2021-08-30 19:43:26
 * @Author: chenwei
 */
#include "curvefs/src/metaserver/metastore.h"
#include <glog/logging.h>
#include <thread>  // NOLINT
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include "curvefs/src/metaserver/storage.h"
namespace curvefs {
namespace metaserver {
MetaStore::MetaStore() {}

// NOTE: if we use set we need define hash function, it's complicate
using PartitionContainerType = std::unordered_map<uint32_t, PartitionInfo>;
using InodeContainerType = InodeStorage::ContainerType;
using DentryContainerType = DentryStorage::ContainerType;
using PendingTxContainerType = std::unordered_map<int, PrepareRenameTxRequest>;

using PartitionIteratorType = MapContainerIterator<PartitionContainerType>;
using InodeIteratorType = MapContainerIterator<InodeContainerType>;
using DentryIteratorType = SetContainerIterator<DentryContainerType>;
using PendingTxIteratorType = MapContainerIterator<PendingTxContainerType>;

bool MetaStore::LoadPartition(uint32_t partitionId, void* entry) {
    auto partitionInfo = reinterpret_cast<PartitionInfo*>(entry);
    partitionId = partitionInfo->partitionid();
    auto partition = std::make_shared<Partition>(*partitionInfo);
    partitionMap_.emplace(partitionId, partition);
    return true;
}

bool MetaStore::LoadInode(uint32_t partitionId, void* entry) {
    auto partition = GetPartition(partitionId);
    if (nullptr == partition) {
        LOG(ERROR) << "Partition not found, partitionId = " << partitionId;
        return false;
    }

    auto inode = reinterpret_cast<Inode*>(entry);
    auto rc = partition->InsertInode(*inode);
    if (rc != MetaStatusCode::OK) {
        LOG(ERROR) << "InsertInode failed, retCode = " << rc;
        return false;
    }
    return true;
}

bool MetaStore::LoadDentry(uint32_t partitionId, void* entry) {
    auto partition = GetPartition(partitionId);
    if (nullptr == partition) {
        LOG(ERROR) << "Partition not found, partitionId = " << partitionId;
        return false;
    }

    auto dentry = reinterpret_cast<Dentry*>(entry);
    auto rc = partition->CreateDentry(*dentry);
    if (rc != MetaStatusCode::OK) {
        LOG(ERROR) << "CreateDentry failed, retCode = " << rc;
        return false;
    }
    return true;
}

bool MetaStore::LoadPendingTx(uint32_t partitionId, void* entry) {
    auto partition = GetPartition(partitionId);
    if (nullptr == partition) {
        LOG(ERROR) << "Partition not found, partitionId = " << partitionId;
        return false;
    }

    auto pendingTx = reinterpret_cast<PrepareRenameTxRequest*>(entry);
    auto rc = partition->InsertPendingTx(*pendingTx);
    if (rc != MetaStatusCode::OK) {
        LOG(ERROR) << "InsertPendingTx failed, retCode " << rc;
        return false;
    }
    return true;
}

bool MetaStore::Load(const std::string& pathname) {
    auto callback = [&](ENTRY_TYPE entryType,
                        uint32_t paritionId,
                        void* entry) -> bool {
        switch (entryType) {
            case ENTRY_TYPE::PARTITION:
                return LoadPartition(paritionId, entry);
            case ENTRY_TYPE::INODE:
                return LoadInode(paritionId, entry);
            case ENTRY_TYPE::DENTRY:
                return LoadDentry(paritionId, entry);
            case ENTRY_TYPE::PENDING_TX:
                return LoadPendingTx(paritionId, entry);
        }

        LOG(ERROR) << "Load failed, unknown entry type";
        return false;
    };

    // Load from raft snap file to memory
    WriteLockGuard writeLockGuard(rwLock_);
    auto succ = LoadFromFile(pathname, callback);
    if (!succ) {
        partitionMap_.clear();
        LOG(ERROR) << "Load metadata failed.";
    }
    return succ;
}

std::shared_ptr<Iterator> MetaStore::NewPartitionIterator() {
    auto container = std::make_shared<PartitionContainerType>();
    for (const auto& item : partitionMap_) {
        auto partitionId = item.first;
        auto partition = item.second;
        container->emplace(partitionId, partition->GetPartitionInfo());
    }

    auto iterator = std::make_shared<PartitionIteratorType>(
        ENTRY_TYPE::PARTITION, 0, container);
    return iterator;
}

std::shared_ptr<Iterator> MetaStore::NewInodeIterator(
    std::shared_ptr<Partition> partition) {
    auto partitionId = partition->GetPartitionId();
    auto cntr = partition->GetInodeContainer();
    auto container = std::shared_ptr<InodeContainerType>(
        cntr, [](InodeContainerType*){});   // don't release storage
    auto iterator = std::make_shared<InodeIteratorType>(
        ENTRY_TYPE::INODE, partitionId, container);
    return iterator;
}

std::shared_ptr<Iterator> MetaStore::NewDentryIterator(
    std::shared_ptr<Partition> partition) {
    auto partitionId = partition->GetPartitionId();
    auto cntr = partition->GetDentryContainer();
    auto container = std::shared_ptr<DentryContainerType>(
        cntr, [](DentryContainerType*){});  // don't release storage
    auto iterator = std::make_shared<DentryIteratorType>(
        ENTRY_TYPE::DENTRY, partitionId, container);
    return iterator;
}

std::shared_ptr<Iterator> MetaStore::NewPendingTxIterator(
    std::shared_ptr<Partition> partition) {
    PrepareRenameTxRequest pendingTx;
    auto container = std::make_shared<PendingTxContainerType>();
    if (partition->FindPendingTx(&pendingTx)) {
        container->emplace(0, pendingTx);
    }

    auto partitionId = partition->GetPartitionId();
    auto iterator = std::make_shared<PendingTxIteratorType>(
        ENTRY_TYPE::PENDING_TX, partitionId, container);
    return iterator;
}

void MetaStore::SaveBackground(const std::string& path,
                               OnSnapshotSaveDone* done) {
    LOG(INFO) << "Save metadata to file background.";

    std::vector<std::shared_ptr<Iterator>> children;
    auto iterator = NewPartitionIterator();  // partition
    children.push_back(iterator);

    for (const auto& item : partitionMap_) {
        auto partition = item.second;

        iterator = NewInodeIterator(partition);  // inode
        children.push_back(iterator);

        iterator = NewDentryIterator(partition);  // dentry
        children.push_back(iterator);

        iterator = NewPendingTxIterator(partition);  // pending tx
        children.push_back(iterator);
    }

    auto mergeIterator = std::make_shared<MergeIterator>(children);
    bool succ = SaveToFile(path, mergeIterator);
    LOG(INFO) << "Save metadata to file " << (succ ? "success" : "fail");
    if (succ) {
        done->SetSuccess();
    } else {
        done->SetError(MetaStatusCode::SAVE_META_FAIL);
    }

    done->Run();
}

bool MetaStore::Save(const std::string& path, OnSnapshotSaveDone* done) {
    ReadLockGuard readLockGuard(rwLock_);
    std::thread th = std::thread(&MetaStore::SaveBackground, this, path, done);
    th.detach();
    return true;
}

bool MetaStore::Clear() {
    WriteLockGuard writeLockGuard(rwLock_);
    partitionMap_.clear();
    return true;
}

MetaStatusCode MetaStore::CreatePartition(const CreatePartitionRequest* request,
                                          CreatePartitionResponse* response) {
    WriteLockGuard writeLockGuard(rwLock_);
    MetaStatusCode status;
    PartitionInfo partition = request->partition();
    auto it = partitionMap_.find(partition.partitionid());
    if (it != partitionMap_.end()) {
        status = MetaStatusCode::PARTITION_EXIST;
        response->set_statuscode(status);
        return status;
    }

    partitionMap_.emplace(partition.partitionid(),
                          std::make_shared<Partition>(partition));
    response->set_statuscode(MetaStatusCode::OK);
    return MetaStatusCode::OK;
}

MetaStatusCode MetaStore::DeletePartition(const DeletePartitionRequest* request,
                                          DeletePartitionResponse* response) {
    WriteLockGuard writeLockGuard(rwLock_);
    MetaStatusCode status;
    uint32_t partitionId = request->partition().partitionid();
    auto it = partitionMap_.find(partitionId);
    if (it == partitionMap_.end()) {
        status = MetaStatusCode::PARTITION_NOT_FOUND;
        response->set_statuscode(status);
        return status;
    }

    if (it->second->IsDeletable()) {
        partitionMap_.erase(it);
        status = MetaStatusCode::OK;
    } else {
        status = MetaStatusCode::PARTITION_BUSY;
    }

    response->set_statuscode(status);
    return status;
}

// dentry
MetaStatusCode MetaStore::CreateDentry(const CreateDentryRequest* request,
                                       CreateDentryResponse* response) {
    ReadLockGuard readLockGuard(rwLock_);
    std::shared_ptr<Partition> partition = GetPartition(request->partitionid());
    if (partition == nullptr) {
        MetaStatusCode status = MetaStatusCode::PARTITION_NOT_FOUND;
        response->set_statuscode(status);
        return status;
    }
    MetaStatusCode status = partition->CreateDentry(request->dentry());
    response->set_statuscode(status);
    return status;
}

MetaStatusCode MetaStore::GetDentry(const GetDentryRequest* request,
                                    GetDentryResponse* response) {
    uint32_t fsId = request->fsid();
    uint64_t parentInodeId = request->parentinodeid();
    std::string name = request->name();
    auto txId = request->txid();
    ReadLockGuard readLockGuard(rwLock_);
    std::shared_ptr<Partition> partition = GetPartition(request->partitionid());
    if (partition == nullptr) {
        MetaStatusCode status = MetaStatusCode::PARTITION_NOT_FOUND;
        response->set_statuscode(status);
        return status;
    }

    // handle by partition
    Dentry dentry;
    dentry.set_fsid(fsId);
    dentry.set_parentinodeid(parentInodeId);
    dentry.set_name(name);
    dentry.set_txid(txId);

    auto rc = partition->GetDentry(&dentry);
    response->set_statuscode(rc);
    if (rc == MetaStatusCode::OK) {
        *response->mutable_dentry() = dentry;
    }
    return rc;
}

MetaStatusCode MetaStore::DeleteDentry(const DeleteDentryRequest* request,
                                       DeleteDentryResponse* response) {
    uint32_t fsId = request->fsid();
    uint64_t parentInodeId = request->parentinodeid();
    std::string name = request->name();
    auto txId = request->txid();
    ReadLockGuard readLockGuard(rwLock_);
    std::shared_ptr<Partition> partition = GetPartition(request->partitionid());
    if (partition == nullptr) {
        MetaStatusCode status = MetaStatusCode::PARTITION_NOT_FOUND;
        response->set_statuscode(status);
        return status;
    }

    // handle by partition
    Dentry dentry;
    dentry.set_fsid(fsId);
    dentry.set_parentinodeid(parentInodeId);
    dentry.set_name(name);
    dentry.set_txid(txId);

    auto rc = partition->DeleteDentry(dentry);
    response->set_statuscode(rc);
    return rc;
}

MetaStatusCode MetaStore::ListDentry(const ListDentryRequest* request,
                                     ListDentryResponse* response) {
    uint32_t fsId = request->fsid();
    uint64_t parentInodeId = request->dirinodeid();
    auto txId = request->txid();
    ReadLockGuard readLockGuard(rwLock_);
    std::shared_ptr<Partition> partition = GetPartition(request->partitionid());
    if (partition == nullptr) {
        MetaStatusCode status = MetaStatusCode::PARTITION_NOT_FOUND;
        response->set_statuscode(status);
        return status;
    }

    // handle by partition
    Dentry dentry;
    dentry.set_fsid(fsId);
    dentry.set_parentinodeid(parentInodeId);
    dentry.set_txid(txId);
    if (request->has_last()) {
        dentry.set_name(request->last());
    }

    std::vector<Dentry> dentrys;
    auto rc = partition->ListDentry(dentry, &dentrys, request->count());
    response->set_statuscode(rc);
    if (rc == MetaStatusCode::OK && !dentrys.empty()) {
        *response->mutable_dentrys() = { dentrys.begin(), dentrys.end() };
    }
    return rc;
}

MetaStatusCode MetaStore::PrepareRenameTx(const PrepareRenameTxRequest* request,
                                          PrepareRenameTxResponse* response) {
    ReadLockGuard readLockGuard(rwLock_);
    MetaStatusCode rc;
    auto partitionId = request->partitionid();
    auto partition = GetPartition(partitionId);
    if (nullptr == partition) {
        rc = MetaStatusCode::PARTITION_NOT_FOUND;
    } else {
        std::vector<Dentry> dentrys{
            request->dentrys().begin(),
            request->dentrys().end()
        };
        rc = partition->HandleRenameTx(dentrys);
    }

    response->set_statuscode(rc);
    return rc;
}

// inode
MetaStatusCode MetaStore::CreateInode(const CreateInodeRequest* request,
                                      CreateInodeResponse* response) {
    uint32_t fsId = request->fsid();
    uint64_t length = request->length();
    uint32_t uid = request->uid();
    uint32_t gid = request->gid();
    uint32_t mode = request->mode();
    FsFileType type = request->type();
    std::string symlink;
    if (type == FsFileType::TYPE_SYM_LINK) {
        if (!request->has_symlink()) {
            response->set_statuscode(MetaStatusCode::SYM_LINK_EMPTY);
            return MetaStatusCode::SYM_LINK_EMPTY;
        }

        symlink = request->symlink();
        if (symlink.empty()) {
            response->set_statuscode(MetaStatusCode::SYM_LINK_EMPTY);
            return MetaStatusCode::SYM_LINK_EMPTY;
        }
    }

    ReadLockGuard readLockGuard(rwLock_);
    std::shared_ptr<Partition> partition = GetPartition(request->partitionid());
    if (partition == nullptr) {
        MetaStatusCode status = MetaStatusCode::PARTITION_NOT_FOUND;
        response->set_statuscode(status);
        return status;
    }
    MetaStatusCode status = partition->CreateInode(
        fsId, length, uid, gid, mode, type, symlink, response->mutable_inode());
    response->set_statuscode(status);
    if (status != MetaStatusCode::OK) {
        response->clear_inode();
    }
    return status;
}

MetaStatusCode MetaStore::CreateRootInode(const CreateRootInodeRequest* request,
                                          CreateRootInodeResponse* response) {
    uint32_t fsId = request->fsid();
    uint32_t uid = request->uid();
    uint32_t gid = request->gid();
    uint32_t mode = request->mode();

    ReadLockGuard readLockGuard(rwLock_);
    std::shared_ptr<Partition> partition = GetPartition(request->partitionid());
    if (partition == nullptr) {
        MetaStatusCode status = MetaStatusCode::PARTITION_NOT_FOUND;
        response->set_statuscode(status);
        return status;
    }

    MetaStatusCode status = partition->CreateRootInode(fsId, uid, gid, mode);
    response->set_statuscode(status);
    if (status != MetaStatusCode::OK) {
        LOG(ERROR) << "CreateRootInode fail, fsId = " << fsId
                   << ", uid = " << uid << ", gid = " << gid
                   << ", mode = " << mode
                   << ", retCode = " << MetaStatusCode_Name(status);
    }
    return status;
}

MetaStatusCode MetaStore::GetInode(const GetInodeRequest* request,
                                   GetInodeResponse* response) {
    uint32_t fsId = request->fsid();
    uint64_t inodeId = request->inodeid();

    ReadLockGuard readLockGuard(rwLock_);
    std::shared_ptr<Partition> partition = GetPartition(request->partitionid());
    if (partition == nullptr) {
        MetaStatusCode status = MetaStatusCode::PARTITION_NOT_FOUND;
        response->set_statuscode(status);
        return status;
    }

    MetaStatusCode status =
        partition->GetInode(fsId, inodeId, response->mutable_inode());
    if (status != MetaStatusCode::OK) {
        response->clear_inode();
    }
    response->set_statuscode(status);
    return status;
}

MetaStatusCode MetaStore::DeleteInode(const DeleteInodeRequest* request,
                                      DeleteInodeResponse* response) {
    uint32_t fsId = request->fsid();
    uint64_t inodeId = request->inodeid();

    ReadLockGuard readLockGuard(rwLock_);
    std::shared_ptr<Partition> partition = GetPartition(request->partitionid());
    if (partition == nullptr) {
        MetaStatusCode status = MetaStatusCode::PARTITION_NOT_FOUND;
        response->set_statuscode(status);
        return status;
    }

    MetaStatusCode status = partition->DeleteInode(fsId, inodeId);
    response->set_statuscode(status);
    return status;
}

MetaStatusCode MetaStore::UpdateInode(const UpdateInodeRequest* request,
                                      UpdateInodeResponse* response) {
    uint32_t fsId = request->fsid();
    uint64_t inodeId = request->inodeid();
    if (request->has_volumeextentlist() && request->has_s3chunkinfolist()) {
        LOG(ERROR) << "only one of type space info, choose volume or s3";
        response->set_statuscode(MetaStatusCode::PARAM_ERROR);
        return MetaStatusCode::PARAM_ERROR;
    }

    ReadLockGuard readLockGuard(rwLock_);
    std::shared_ptr<Partition> partition = GetPartition(request->partitionid());
    if (partition == nullptr) {
        MetaStatusCode status = MetaStatusCode::PARTITION_NOT_FOUND;
        response->set_statuscode(status);
        return status;
    }

    Inode inode;
    MetaStatusCode status = partition->GetInode(fsId, inodeId, &inode);
    if (status != MetaStatusCode::OK) {
        response->set_statuscode(status);
        return status;
    }

    bool needUpdate = false;

#define UPDATE_INODE(param)                  \
    if (request->has_##param()) {            \
        inode.set_##param(request->param()); \
        needUpdate = true;                   \
    }

    UPDATE_INODE(length)
    UPDATE_INODE(ctime)
    UPDATE_INODE(mtime)
    UPDATE_INODE(atime)
    UPDATE_INODE(uid)
    UPDATE_INODE(gid)
    UPDATE_INODE(mode)

    if (request->has_volumeextentlist()) {
        VLOG(1) << "update inode has extent";
        inode.mutable_volumeextentlist()->CopyFrom(request->volumeextentlist());
        needUpdate = true;
    }

    if (request->has_s3chunkinfolist()) {
        VLOG(1) << "update inode has extent";
        inode.mutable_s3chunkinfolist()->CopyFrom(request->s3chunkinfolist());
        needUpdate = true;
    }

    if (needUpdate) {
        // TODO(cw123) : Update each field individually
        status = partition->UpdateInode(inode);
        response->set_statuscode(status);
    } else {
        LOG(WARNING) << "inode has no param to update";
        response->set_statuscode(MetaStatusCode::OK);
    }

    return status;
}

MetaStatusCode MetaStore::UpdateInodeVersion(
    const UpdateInodeS3VersionRequest* request,
    UpdateInodeS3VersionResponse* response) {
    uint32_t fsId = request->fsid();
    uint64_t inodeId = request->inodeid();

    ReadLockGuard readLockGuard(rwLock_);
    std::shared_ptr<Partition> partition = GetPartition(request->partitionid());
    if (partition == nullptr) {
        MetaStatusCode status = MetaStatusCode::PARTITION_NOT_FOUND;
        response->set_statuscode(status);
        return status;
    }

    uint64_t version;
    MetaStatusCode status =
        partition->UpdateInodeVersion(fsId, inodeId, &version);
    response->set_statuscode(status);
    if (status != MetaStatusCode::OK) {
        LOG(ERROR) << "UpdateInodeS3Version fail, fsId = " << fsId
                   << ", inodeId = " << inodeId
                   << ", ret = " << MetaStatusCode_Name(status);
    } else {
        response->set_version(version);
    }
    return status;
}

std::shared_ptr<Partition> MetaStore::GetPartition(uint32_t partitionId) {
    auto it = partitionMap_.find(partitionId);
    if (it != partitionMap_.end()) {
        return it->second;
    }

    return nullptr;
}

}  // namespace metaserver
}  // namespace curvefs
