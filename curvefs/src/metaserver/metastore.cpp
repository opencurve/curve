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
#include "curvefs/src/metaserver/copyset/copyset_node.h"
#include "curvefs/src/metaserver/storage/iterator.h"
#include "curvefs/src/metaserver/storage/storage_fstream.h"

namespace curvefs {
namespace metaserver {

using ::curvefs::metaserver::storage::IteratorWrapper;
using ::curvefs::metaserver::storage::ContainerIterator;
using ::curvefs::metaserver::storage::MergeIterator;
using ::curvefs::metaserver::storage::ENTRY_TYPE;
using ::curvefs::metaserver::storage::SaveToFile;
using ::curvefs::metaserver::storage::LoadFromFile;

using ContainerType = std::unordered_map<std::string, std::string>;
using KVStorage = ::curvefs::metaserver::storage::KVStorage;
using STORAGE_TYPE = ::curvefs::metaserver::storage::KVStorage::STORAGE_TYPE;

MetaStoreImpl::MetaStoreImpl(copyset::CopysetNode* node,
                             std::shared_ptr<KVStorage> kvStorage)
    : copysetNode_(node), kvStorage_(kvStorage) {}

bool MetaStoreImpl::LoadPartition(uint32_t partitionId,
                                  const std::string& key,
                                  const std::string& value) {
    PartitionInfo partitionInfo;
    if (!partitionInfo.ParseFromString(value)) {
        LOG(ERROR) << "Decode PartitionInfo failed";
        return false;
    }

    partitionId = partitionInfo.partitionid();
    auto partition = std::make_shared<Partition>(partitionInfo, kvStorage_);
    partitionMap_.emplace(partitionId, partition);

    if (!partition->ClearAllInode() || !partition->ClearAllDentry()) {
        LOG(ERROR) << "Clear inode/dentry failed, partitionId = "
                   << partitionId;
        return false;
    }
    return true;
}

bool MetaStoreImpl::LoadInode(uint32_t partitionId,
                              const std::string& key,
                              const std::string& value) {
    auto partition = GetPartition(partitionId);
    if (nullptr == partition) {
        LOG(ERROR) << "Partition not found, partitionId = " << partitionId;
        return false;
    }

    Inode inode;
    if (!inode.ParseFromString(value)) {
        LOG(ERROR) << "Decode inode failed";
        return false;
    }

    MetaStatusCode rc = partition->InsertInode(inode);
    if (rc != MetaStatusCode::OK) {
        LOG(ERROR) << "InsertInode failed, retCode = "
                   << MetaStatusCode_Name(rc);
        return false;
    }
    return true;
}

bool MetaStoreImpl::LoadDentry(uint32_t partitionId,
                               const std::string& key,
                               const std::string& value) {
    auto partition = GetPartition(partitionId);
    if (nullptr == partition) {
        LOG(ERROR) << "Partition not found, partitionId = " << partitionId;
        return false;
    }

    Dentry dentry;
    if (!::curvefs::metaserver::DentryStorage::ExtractKey(key, &dentry) ||
        !::curvefs::metaserver::DentryStorage::ExtractValue(value, &dentry)) {
        LOG(ERROR) << "Decode dentry failed";
        return false;
    }

    MetaStatusCode rc = partition->CreateDentry(dentry, true);
    if (rc != MetaStatusCode::OK) {
        LOG(ERROR) << "CreateDentry failed, retCode = "
                   << MetaStatusCode_Name(rc);
        return false;
    }
    return true;
}

bool MetaStoreImpl::LoadPendingTx(uint32_t partitionId,
                                  const std::string& key,
                                  const std::string& value) {
    auto partition = GetPartition(partitionId);
    if (nullptr == partition) {
        LOG(ERROR) << "Partition not found, partitionId = " << partitionId;
        return false;
    }

    PrepareRenameTxRequest pendingTx;
    if (!pendingTx.ParseFromString(value)) {
        LOG(ERROR) << "Decode pending tx failed";
        return false;
    }

    auto rc = partition->InsertPendingTx(pendingTx);
    if (!rc) {
        LOG(ERROR) << "InsertPendingTx failed, retCode " << rc;
        return false;
    }
    return true;
}

bool MetaStoreImpl::Load(const std::string& pathname) {
    auto callback = [&](ENTRY_TYPE entryType,
                        uint32_t paritionId,
                        const std::string& key,
                        const std::string& value) -> bool {
        switch (entryType) {
            case ENTRY_TYPE::PARTITION:
                return LoadPartition(paritionId, key, value);
            case ENTRY_TYPE::INODE:
                return LoadInode(paritionId, key, value);
            case ENTRY_TYPE::DENTRY:
                return LoadDentry(paritionId, key, value);
            case ENTRY_TYPE::PENDING_TX:
                return LoadPendingTx(paritionId, key, value);
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

    for (auto it = partitionMap_.begin(); it != partitionMap_.end(); it++) {
        if (it->second->GetStatus() == PartitionStatus::DELETING) {
            uint32_t partitionId = it->second->GetPartitionId();
            std::shared_ptr<PartitionCleaner> partitionCleaner =
                std::make_shared<PartitionCleaner>(GetPartition(partitionId));
            PartitionCleanManager::GetInstance().Add(
                partitionId, partitionCleaner, copysetNode_);
        }
    }

    return succ;
}

std::shared_ptr<Iterator> MetaStoreImpl::NewPartitionIterator() {
    std::string value;
    auto container = std::make_shared<ContainerType>();
    for (const auto& item : partitionMap_) {
        auto partitionId = item.first;
        auto partition = item.second;
        auto partitionInfo = partition->GetPartitionInfo();
        if (!partitionInfo.IsInitialized()) {
            return nullptr;
        } else if (!partitionInfo.SerializeToString(&value)) {
            return nullptr;
        }
        container->emplace(std::to_string(partitionId), value);
    }

    auto iterator = std::make_shared<ContainerIterator<ContainerType>>(
        container);
    return std::make_shared<IteratorWrapper>(
        ENTRY_TYPE::PARTITION, 0, iterator);
}

std::shared_ptr<Iterator> MetaStoreImpl::NewInodeIterator(
    std::shared_ptr<Partition> partition) {
    auto partitionId = partition->GetPartitionId();
    auto iterator = partition->GetAllInode();
    if (iterator->Status() != 0) {
        return nullptr;
    }
    return std::make_shared<IteratorWrapper>(
        ENTRY_TYPE::INODE, partitionId, iterator);
}

std::shared_ptr<Iterator> MetaStoreImpl::NewDentryIterator(
    std::shared_ptr<Partition> partition) {
    auto partitionId = partition->GetPartitionId();
    auto iterator = partition->GetAllDentry();
    if (iterator->Status() != 0) {
        return nullptr;
    }
    return std::make_shared<IteratorWrapper>(
        ENTRY_TYPE::DENTRY, partitionId, iterator);
}

std::shared_ptr<Iterator> MetaStoreImpl::NewPendingTxIterator(
    std::shared_ptr<Partition> partition) {
    std::string value;
    PrepareRenameTxRequest pendingTx;
    auto container = std::make_shared<ContainerType>();
    if (partition->FindPendingTx(&pendingTx)) {
        if (!pendingTx.IsInitialized() ||
            !pendingTx.SerializeToString(&value)) {
            return nullptr;
        }
        container->emplace("", value);
    }

    auto partitionId = partition->GetPartitionId();
    auto iterator = std::make_shared<ContainerIterator<ContainerType>>(
        container);
    return std::make_shared<IteratorWrapper>(
        ENTRY_TYPE::PENDING_TX, partitionId, iterator);
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

    for (const auto& child : children) {
        if (nullptr == child) {
            LOG(INFO) << "Save metadata to file failed"
                      << " for generate iterator error";
            done->SetError(MetaStatusCode::SAVE_META_FAIL);
            done->Run();
            return;
        }
    }

    auto mergeIterator = std::make_shared<MergeIterator>(children);
    bool background = (kvStorage_->Type() == STORAGE_TYPE::MEMORY_STORAGE);
    bool succ = SaveToFile(path, mergeIterator, background);
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
                          std::make_shared<Partition>(partition, kvStorage_));
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
                     << ", partitionId = " << partitionId;
        response->set_statuscode(MetaStatusCode::PARTITION_NOT_FOUND);
        return MetaStatusCode::PARTITION_NOT_FOUND;
    }

    if (it->second->IsDeletable()) {
        LOG(INFO) << "DeletePartition, partition is deletable, delete it"
                  << ", partitionId = " << partitionId;
        TrashManager::GetInstance().Remove(partitionId);
        it->second->ClearS3Compact();
        PartitionCleanManager::GetInstance().Remove(partitionId);
        partitionMap_.erase(it);
        response->set_statuscode(MetaStatusCode::OK);
        return MetaStatusCode::OK;
    }

    if (it->second->GetStatus() != PartitionStatus::DELETING) {
        LOG(INFO) << "DeletePartition, set partition to deleting"
                  << ", partitionId = " << partitionId;
        it->second->ClearDentry();
        std::shared_ptr<PartitionCleaner> partitionCleaner =
            std::make_shared<PartitionCleaner>(GetPartition(partitionId));
        PartitionCleanManager::GetInstance().Add(partitionId, partitionCleaner,
                                                 copysetNode_);
        it->second->SetStatus(PartitionStatus::DELETING);
        TrashManager::GetInstance().Remove(partitionId);
        it->second->ClearS3Compact();
    } else {
        LOG(INFO) << "DeletePartition, partition is already deleting"
                  << ", partitionId = " << partitionId;
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
    MetaStatusCode status = partition->GetOrModifyS3ChunkInfo(
        fsId, inodeId, request->s3chunkinfoadd(), request->s3chunkinforemove(),
        request->returns3chunkinfomap(), response->mutable_s3chunkinfomap(),
        request->has_froms3compaction() && request->froms3compaction());
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
