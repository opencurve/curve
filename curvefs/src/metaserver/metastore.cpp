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

using PendingTxContainerType = std::unordered_map<int, PrepareRenameTxRequest>;
using DentryIteratorType = SetContainerIterator<DentryStorage::ContainerType>;
using PendingTxIteratorType = ContainerIterator<PendingTxContainerType>;

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

std::shared_ptr<Iterator> MetaStore::NewDentryIterator(
    std::shared_ptr<Partition> partition) {
    auto partitionId = partition->GetPartitionId();
    auto container = partition->GetDentryContainer();
    auto iterator = std::make_shared<DentryIteratorType>(
        ENTRY_TYPE::DENTRY, partitionId, container);
    return iterator;
}

std::shared_ptr<Iterator> MetaStore::NewPendingTxIterator(
    std::shared_ptr<Partition> partition) {
    PrepareRenameTxRequest pendingTx;
    if (!partition->FindPendingTx(&pendingTx)) {  // not found
        return nullptr;
    }

    auto partitionId = partition->GetPartitionId();
    auto container = PendingTxContainerType{ { 0, pendingTx } };
    auto iterator = std::make_shared<PendingTxIteratorType>(
        ENTRY_TYPE::PENDING_TX, partitionId, &container);
    return iterator;
}

bool MetaStore::Load(const std::string& path) {
    auto callbackFunc = [&](ENTRY_TYPE type, uint32_t paritionId,
                            void* metadata) -> bool {
        if (type == ENTRY_TYPE::PARTITION) {
            PartitionInfo* partition =
                reinterpret_cast<PartitionInfo*>(metadata);
            partitionMap_.emplace(partition->partitionid(),
                                  std::make_shared<Partition>(*partition));
            return true;
        } else if (type == ENTRY_TYPE::INODE) {
            std::shared_ptr<Partition> partition = GetPartition(paritionId);
            if (partition == nullptr) {
                LOG(ERROR) << "MetaStore Load GetPartition fail, type INODE";
                return false;
            }
            Inode* inode = reinterpret_cast<Inode*>(metadata);
            MetaStatusCode status = partition->InsertInode(*inode);
            return (status == MetaStatusCode::OK) ? true : false;
        } else if (type == ENTRY_TYPE::DENTRY) {
            std::shared_ptr<Partition> partition = GetPartition(paritionId);
            if (partition == nullptr) {
                LOG(ERROR) << "MetaStore Load GetPartition fail, type DENTRY";
                return false;
            }
            Dentry* dentry = reinterpret_cast<Dentry*>(metadata);
            MetaStatusCode status = partition->CreateDentry(*dentry);
            if (status != MetaStatusCode::OK) {
                LOG(ERROR) << "MetaStore Load CreateDentry fail, type DENTRY";
                return false;
            }
            return true;
        } else if (type == ENTRY_TYPE::PENDING_TX) {
            return LoadPendingTx(paritionId, metadata);
        } else {
            LOG(ERROR) << "load meta from file, not supported type";
            return false;
        }
        return true;
    };

    // Load from raft snap file to memory
    WriteLockGuard writeLockGuard(rwLock_);
    bool ret = LoadFromFile(path, callbackFunc);
    if (!ret) {
        partitionMap_.clear();
    }
    return ret;
}

void MetaStore::SaveBack(const std::string& path, OnSnapshotSaveDone* done) {
    // 1. call back dump function
    std::vector<std::shared_ptr<Iterator>> iteratorList;
    LOG(INFO) << "Save meta to file back.";
    // add partition iteractor
    std::map<uint32_t, PartitionInfo> partitionInfoMap;
    for (auto& partitionIter : partitionMap_) {
        partitionInfoMap.emplace(partitionIter.second->GetPartitionId(),
                                 partitionIter.second->GetPartitionInfo());
    }
    // partitionIter no need pass partitionId, here pass 0 instead
    std::shared_ptr<ContainerIterator<std::map<uint32_t, PartitionInfo>>>
        partitionContainerIter = std::make_shared<
            ContainerIterator<std::map<uint32_t, PartitionInfo>>>(
            ENTRY_TYPE::PARTITION, 0, &partitionInfoMap);
    iteratorList.push_back(partitionContainerIter);

    // add inode and dentry iterator
    for (auto& it : partitionMap_) {
        // add inode iterator
        std::shared_ptr<ContainerIterator<InodeContainerType>> inodeIter =
            std::make_shared<ContainerIterator<InodeContainerType>>(
                ENTRY_TYPE::INODE, it.second->GetPartitionId(),
                it.second->GetInodeContainer());
        iteratorList.push_back(inodeIter);

        // add dentry iterator
        auto dentryIterator = NewDentryIterator(it.second);
        iteratorList.push_back(dentryIterator);

        // add pending tx iterator
        auto pendingTxIterator = NewPendingTxIterator(it.second);
        if (nullptr != pendingTxIterator) {
            iteratorList.push_back(pendingTxIterator);
        }
    }

    auto mergeIterator = std::make_shared<MergeIterator>(iteratorList);
    bool ret = SaveToFile(path, mergeIterator);
    if (ret) {
        LOG(INFO) << "Save meta to file back success.";
        done->SetSuccess();
    } else {
        LOG(INFO) << "Save meta to file back fail.";
        done->SetError(MetaStatusCode::SAVE_META_FAIL);
    }

    done->Run();
}

bool MetaStore::Save(const std::string& path, OnSnapshotSaveDone* done) {
    ReadLockGuard readLockGuard(rwLock_);
    std::thread th = std::thread(&MetaStore::SaveBack, this, path, done);
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
