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
#include <utility>
#include <vector>
#include "curvefs/src/metaserver/partition_clean_manager.h"
#include "curvefs/src/metaserver/storage.h"

namespace curvefs {
namespace metaserver {
MetaStoreImpl::MetaStoreImpl() {}

// NOTE: if we use set we need define hash function, it's complicate
using PartitionContainerType = std::unordered_map<uint32_t, PartitionInfo>;
using InodeContainerType = InodeStorage::ContainerType;
using DentryContainerType = DentryStorage::ContainerType;
using PendingTxContainerType = std::unordered_map<int, PrepareRenameTxRequest>;

using PartitionIteratorType = MapContainerIterator<PartitionContainerType>;
using InodeIteratorType = MapContainerIterator<InodeContainerType>;
using DentryIteratorType = SetContainerIterator<DentryContainerType>;
using PendingTxIteratorType = MapContainerIterator<PendingTxContainerType>;

bool MetaStoreImpl::LoadPartition(uint32_t partitionId, void* entry) {
    auto partitionInfo = reinterpret_cast<PartitionInfo*>(entry);
    partitionId = partitionInfo->partitionid();
    auto partition = std::make_shared<Partition>(*partitionInfo);
    partitionMap_.emplace(partitionId, partition);
    if (partitionInfo->status() == PartitionStatus::DELETING) {
        std::shared_ptr<PartitionCleaner> partitionCleaner =
            std::make_shared<PartitionCleaner>(GetPartition(partitionId));
        copyset::CopysetNode *copysetNode =
            copyset::CopysetNodeManager::GetInstance().GetCopysetNode(
                partition->GetPoolId(), partition->GetCopySetId());
        PartitionCleanManager::GetInstance().Add(partitionId, partitionCleaner,
                                                 copysetNode);
    }
    return true;
}

bool MetaStoreImpl::LoadInode(uint32_t partitionId, void* entry) {
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

bool MetaStoreImpl::LoadDentry(uint32_t partitionId, void* entry) {
    auto partition = GetPartition(partitionId);
    if (nullptr == partition) {
        LOG(ERROR) << "Partition not found, partitionId = " << partitionId;
        return false;
    }

    auto dentry = reinterpret_cast<Dentry*>(entry);
    auto rc = partition->CreateDentry(*dentry, true);
    if (rc != MetaStatusCode::OK) {
        LOG(ERROR) << "CreateDentry failed, retCode = " << rc;
        return false;
    }
    return true;
}

bool MetaStoreImpl::LoadPendingTx(uint32_t partitionId, void* entry) {
    auto partition = GetPartition(partitionId);
    if (nullptr == partition) {
        LOG(ERROR) << "Partition not found, partitionId = " << partitionId;
        return false;
    }

    auto pendingTx = reinterpret_cast<PrepareRenameTxRequest*>(entry);
    auto rc = partition->InsertPendingTx(*pendingTx);
    if (!rc) {
        LOG(ERROR) << "InsertPendingTx failed, retCode " << rc;
        return false;
    }
    return true;
}

bool MetaStoreImpl::Load(const std::string& pathname) {
    auto callback = [&](ENTRY_TYPE entryType, uint32_t paritionId,
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
            case ENTRY_TYPE::UNKNOWN:
            default:
                break;
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

std::shared_ptr<Iterator> MetaStoreImpl::NewPartitionIterator() {
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

std::shared_ptr<Iterator> MetaStoreImpl::NewInodeIterator(
    std::shared_ptr<Partition> partition) {
    auto partitionId = partition->GetPartitionId();
    auto cntr = partition->GetInodeContainer();
    auto container = std::shared_ptr<InodeContainerType>(
        cntr, [](InodeContainerType*) {});  // don't release storage
    auto iterator = std::make_shared<InodeIteratorType>(ENTRY_TYPE::INODE,
                                                        partitionId, container);
    return iterator;
}

std::shared_ptr<Iterator> MetaStoreImpl::NewDentryIterator(
    std::shared_ptr<Partition> partition) {
    auto partitionId = partition->GetPartitionId();
    auto cntr = partition->GetDentryContainer();
    auto container = std::shared_ptr<DentryContainerType>(
        cntr, [](DentryContainerType*) {});  // don't release storage
    auto iterator = std::make_shared<DentryIteratorType>(
        ENTRY_TYPE::DENTRY, partitionId, container);
    return iterator;
}

std::shared_ptr<Iterator> MetaStoreImpl::NewPendingTxIterator(
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

void MetaStoreImpl::SaveBackground(const std::string& path,
                                   OnSnapshotSaveDoneClosure* done) {
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

bool MetaStoreImpl::Save(const std::string& path,
                         OnSnapshotSaveDoneClosure* done) {
    ReadLockGuard readLockGuard(rwLock_);
    std::thread th =
        std::thread(&MetaStoreImpl::SaveBackground, this, path, done);
    th.detach();
    return true;
}

bool MetaStoreImpl::Clear() {
    WriteLockGuard writeLockGuard(rwLock_);
    for (auto it = partitionMap_.begin(); it != partitionMap_.end(); it++) {
        TrashManager::GetInstance().Remove(it->first);
        it->second->ClearS3Compact();
        PartitionCleanManager::GetInstance().Remove(it->first);
    }
    partitionMap_.clear();
    return true;
}

MetaStatusCode MetaStoreImpl::CreatePartition(
    const CreatePartitionRequest* request, CreatePartitionResponse* response) {
    WriteLockGuard writeLockGuard(rwLock_);
    MetaStatusCode status;
    PartitionInfo partition = request->partition();
    auto it = partitionMap_.find(partition.partitionid());
    if (it != partitionMap_.end()) {
        // keep idempotence
        status = MetaStatusCode::OK;
        response->set_statuscode(status);
        return status;
    }

    partitionMap_.emplace(partition.partitionid(),
                          std::make_shared<Partition>(partition));
    response->set_statuscode(MetaStatusCode::OK);
    return MetaStatusCode::OK;
}

MetaStatusCode MetaStoreImpl::DeletePartition(
    const DeletePartitionRequest* request, DeletePartitionResponse* response) {
    WriteLockGuard writeLockGuard(rwLock_);
    uint32_t partitionId = request->partitionid();
    auto it = partitionMap_.find(partitionId);
    if (it == partitionMap_.end()) {
        LOG(WARNING) << "DeletePartition, partition is not found"
                     << ", partitionId = " <<  partitionId;
        response->set_statuscode(MetaStatusCode::PARTITION_NOT_FOUND);
        return MetaStatusCode::PARTITION_NOT_FOUND;
    }

    if (it->second->IsDeletable()) {
        LOG(INFO) << "DeletePartition, partition is deletable, delete it"
                  << ", partitionId = " <<  partitionId;
        TrashManager::GetInstance().Remove(partitionId);
        it->second->ClearS3Compact();
        PartitionCleanManager::GetInstance().Remove(partitionId);
        partitionMap_.erase(it);
        response->set_statuscode(MetaStatusCode::OK);
        return MetaStatusCode::OK;
    }

    if (it->second->GetStatus() != PartitionStatus::DELETING) {
        LOG(INFO) << "DeletePartition, set partition to deleting"
                  << ", partitionId = " <<  partitionId;
        it->second->ClearDentry();
        std::shared_ptr<PartitionCleaner> partitionCleaner =
            std::make_shared<PartitionCleaner>(GetPartition(partitionId));
        copyset::CopysetNode *copysetNode =
            copyset::CopysetNodeManager::GetInstance().GetCopysetNode(
                it->second->GetPoolId(), it->second->GetCopySetId());
        PartitionCleanManager::GetInstance().Add(partitionId, partitionCleaner,
                                                 copysetNode);
        it->second->SetStatus(PartitionStatus::DELETING);
        TrashManager::GetInstance().Remove(partitionId);
        it->second->ClearS3Compact();
    } else {
        LOG(INFO) << "DeletePartition, partition is already deleting"
                  << ", partitionId = " <<  partitionId;
    }

    response->set_statuscode(MetaStatusCode::PARTITION_DELETING);
    return MetaStatusCode::PARTITION_DELETING;
}

std::list<PartitionInfo> MetaStoreImpl::GetPartitionInfoList() {
    std::list<PartitionInfo> partitionInfoList;
    ReadLockGuard readLockGuard(rwLock_);
    for (const auto& it : partitionMap_) {
        PartitionInfo partitionInfo = it.second->GetPartitionInfo();
        partitionInfo.set_inodenum(it.second->GetInodeNum());
        partitionInfo.set_dentrynum(it.second->GetDentryNum());
        partitionInfoList.push_back(std::move(partitionInfo));
    }
    return partitionInfoList;
}

// dentry
MetaStatusCode MetaStoreImpl::CreateDentry(const CreateDentryRequest* request,
                                           CreateDentryResponse* response) {
    ReadLockGuard readLockGuard(rwLock_);
    std::shared_ptr<Partition> partition = GetPartition(request->partitionid());
    if (partition == nullptr) {
        MetaStatusCode status = MetaStatusCode::PARTITION_NOT_FOUND;
        response->set_statuscode(status);
        return status;
    }
    MetaStatusCode status = partition->CreateDentry(request->dentry(), false);
    response->set_statuscode(status);
    return status;
}

MetaStatusCode MetaStoreImpl::GetDentry(const GetDentryRequest* request,
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

MetaStatusCode MetaStoreImpl::DeleteDentry(const DeleteDentryRequest* request,
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

MetaStatusCode MetaStoreImpl::ListDentry(const ListDentryRequest* request,
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
        *response->mutable_dentrys() = {dentrys.begin(), dentrys.end()};
    }
    return rc;
}

MetaStatusCode MetaStoreImpl::PrepareRenameTx(
    const PrepareRenameTxRequest* request, PrepareRenameTxResponse* response) {
    ReadLockGuard readLockGuard(rwLock_);
    MetaStatusCode rc;
    auto partitionId = request->partitionid();
    auto partition = GetPartition(partitionId);
    if (nullptr == partition) {
        rc = MetaStatusCode::PARTITION_NOT_FOUND;
    } else {
        std::vector<Dentry> dentrys{request->dentrys().begin(),
                                    request->dentrys().end()};
        rc = partition->HandleRenameTx(dentrys);
    }

    response->set_statuscode(rc);
    return rc;
}

// inode
MetaStatusCode MetaStoreImpl::CreateInode(const CreateInodeRequest* request,
                                          CreateInodeResponse* response) {
    uint32_t fsId = request->fsid();
    uint64_t length = request->length();
    uint32_t uid = request->uid();
    uint32_t gid = request->gid();
    uint32_t mode = request->mode();
    FsFileType type = request->type();
    std::string symlink;
    uint32_t rdev = request->rdev();
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
    MetaStatusCode status =
        partition->CreateInode(fsId, length, uid, gid, mode, type, symlink,
                               rdev, response->mutable_inode());
    response->set_statuscode(status);
    if (status != MetaStatusCode::OK) {
        response->clear_inode();
    }
    return status;
}

MetaStatusCode MetaStoreImpl::CreateRootInode(
    const CreateRootInodeRequest* request, CreateRootInodeResponse* response) {
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

MetaStatusCode MetaStoreImpl::GetInode(const GetInodeRequest* request,
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

MetaStatusCode MetaStoreImpl::DeleteInode(const DeleteInodeRequest* request,
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

MetaStatusCode MetaStoreImpl::UpdateInode(const UpdateInodeRequest* request,
                                          UpdateInodeResponse* response) {
    uint32_t fsId = request->fsid();
    uint64_t inodeId = request->inodeid();
    if (request->has_volumeextentlist() && !request->s3chunkinfomap().empty()) {
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

    MetaStatusCode status = partition->UpdateInode(*request);
    response->set_statuscode(status);
    return status;
}

MetaStatusCode MetaStoreImpl::GetOrModifyS3ChunkInfo(
    const GetOrModifyS3ChunkInfoRequest* request,
    GetOrModifyS3ChunkInfoResponse* response) {
    uint32_t fsId = request->fsid();
    uint64_t inodeId = request->inodeid();
    ReadLockGuard readLockGuard(rwLock_);
    std::shared_ptr<Partition> partition = GetPartition(request->partitionid());
    if (partition == nullptr) {
        MetaStatusCode status = MetaStatusCode::PARTITION_NOT_FOUND;
        response->set_statuscode(status);
        return status;
    }
    MetaStatusCode status = partition->GetOrModifyS3ChunkInfo(fsId, inodeId,
        request->s3chunkinfoadd(), request->s3chunkinforemove(),
        request->returns3chunkinfomap(), response->mutable_s3chunkinfomap());
    response->set_statuscode(status);
    return status;
}

std::shared_ptr<Partition> MetaStoreImpl::GetPartition(uint32_t partitionId) {
    auto it = partitionMap_.find(partitionId);
    if (it != partitionMap_.end()) {
        return it->second;
    }

    return nullptr;
}

}  // namespace metaserver
}  // namespace curvefs
