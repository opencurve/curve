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

#include "absl/cleanup/cleanup.h"
#include "curvefs/src/metaserver/partition_clean_manager.h"
#include "curvefs/src/metaserver/copyset/copyset_node.h"
#include "curvefs/src/metaserver/storage_common.h"
#include "curvefs/src/metaserver/metastore_fstream.h"
#include "curvefs/src/metaserver/storage/iterator.h"

namespace curvefs {
namespace metaserver {

using ::curvefs::common::StreamOptions;
using ::curvefs::common::GetStreamEOFString;
using KVStorage = ::curvefs::metaserver::storage::KVStorage;

MetaStoreImpl::MetaStoreImpl(copyset::CopysetNode* node,
                             std::shared_ptr<KVStorage> kvStorage)
    : copysetNode_(node),
      kvStorage_(kvStorage),
      streamSender_(std::make_shared<StreamSender>()) {}

bool MetaStoreImpl::Load(const std::string& pathname) {
    // Load from raft snap file to memory
    WriteLockGuard writeLockGuard(rwLock_);
    MetaStoreFStream fstream(&partitionMap_, kvStorage_);
    auto succ = fstream.Load(pathname);
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

void MetaStoreImpl::SaveBackground(const std::string& path,
                                   OnSnapshotSaveDoneClosure* done) {
    LOG(INFO) << "Save metadata to file background.";
    MetaStoreFStream fstream(&partitionMap_, kvStorage_);
    bool succ = fstream.Save(path);
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


std::shared_ptr<StreamSender> MetaStoreImpl::GetStreamSender() {
    return streamSender_;
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

    bool onlyDir = false;
    if (request->has_onlydir()) {
        onlyDir = request->onlydir();
    }

    std::vector<Dentry> dentrys;
    auto rc = partition->ListDentry(dentry, &dentrys, request->count(),
                                    onlyDir);
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

MetaStatusCode MetaStoreImpl::BatchGetInodeAttr(
    const BatchGetInodeAttrRequest* request,
    BatchGetInodeAttrResponse* response) {
    ReadLockGuard readLockGuard(rwLock_);
    std::shared_ptr<Partition> partition = GetPartition(request->partitionid());
    if (partition == nullptr) {
        MetaStatusCode status = MetaStatusCode::PARTITION_NOT_FOUND;
        response->set_statuscode(status);
        return status;
    }

    uint32_t fsId = request->fsid();
    MetaStatusCode status = MetaStatusCode::OK;
    for (int i = 0; i < request->inodeid_size(); i++) {
        status = partition->GetInodeAttr(fsId, request->inodeid(i),
                                         response->add_attr());
        if (status != MetaStatusCode::OK) {
            response->clear_attr();
            response->set_statuscode(status);
            return status;
        }
    }
    response->set_statuscode(status);
    return status;
}

MetaStatusCode MetaStoreImpl::BatchGetXAttr(
    const BatchGetXAttrRequest* request,
    BatchGetXAttrResponse* response) {
    ReadLockGuard readLockGuard(rwLock_);
    std::shared_ptr<Partition> partition = GetPartition(request->partitionid());
    if (partition == nullptr) {
        MetaStatusCode status = MetaStatusCode::PARTITION_NOT_FOUND;
        response->set_statuscode(status);
        return status;
    }

    uint32_t fsId = request->fsid();
    MetaStatusCode status = MetaStatusCode::OK;
    for (int i = 0; i < request->inodeid_size(); i++) {
        status = partition->GetXAttr(fsId, request->inodeid(i),
                                     response->add_xattr());
        if (status != MetaStatusCode::OK) {
            response->clear_xattr();
            response->set_statuscode(status);
            return status;
        }
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
    GetOrModifyS3ChunkInfoResponse* response,
    std::shared_ptr<Iterator>* iterator) {
    MetaStatusCode rc;
    ReadLockGuard readLockGuard(rwLock_);
    auto partition = GetPartition(request->partitionid());
    if (nullptr == partition) {
        rc = MetaStatusCode::PARTITION_NOT_FOUND;
    } else {
        rc = partition->GetOrModifyS3ChunkInfo(request->fsid(),
                                               request->inodeid(),
                                               request->s3chunkinfoadd(),
                                               iterator,
                                               request->returns3chunkinfomap(),
                                               request->froms3compaction());
    }
    response->set_statuscode(rc);
    return rc;
}

MetaStatusCode MetaStoreImpl::SendS3ChunkInfoByStream(
    std::shared_ptr<StreamConnection> connection,
    std::shared_ptr<Iterator> iterator) {
    butil::IOBuf buffer;
    Key4S3ChunkInfoList key;
    auto conv = std::make_shared<Converter>();
    for (iterator->SeekToFirst(); iterator->Valid(); iterator->Next()) {
        std::string skey = iterator->Key();
        std::string svalue = iterator->Value();
        if (!conv->ParseFromString(skey, &key)) {
            return MetaStatusCode::PARSE_FROM_STRING_FAILED;
        }

        buffer.clear();
        buffer.append(std::to_string(key.chunkIndex));
        buffer.append(":");
        buffer.append(svalue);
        if (!connection->Write(buffer)) {
            LOG(ERROR) << "BRPC stream write failed";
            return MetaStatusCode::RPC_STREAM_ERROR;
        }
    }

    // send eof
    buffer.clear();
    buffer.append(GetStreamEOFString());
    if (!connection->Write(buffer)) {
        LOG(ERROR) << "BRPC stream write failed";
        return MetaStatusCode::RPC_STREAM_ERROR;
    }
    LOG(INFO) << "Sending s3chunkinfo by stream success";
    return MetaStatusCode::OK;
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
