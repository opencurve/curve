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
#include <vector>
#include "curvefs/src/metaserver/storage.h"
namespace curvefs {
namespace metaserver {
MetaStoreImpl::MetaStoreImpl() {}

bool MetaStoreImpl::Load(const std::string& path) {
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

void MetaStoreImpl::SaveBack(const std::string& path,
                             OnSnapshotSaveDoneClosure* done) {
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
        std::shared_ptr<ContainerIterator<DentryContainerType>> dentryIter =
            std::make_shared<ContainerIterator<DentryContainerType>>(
                ENTRY_TYPE::DENTRY, it.second->GetPartitionId(),
                it.second->GetDentryContainer());
        iteratorList.push_back(dentryIter);
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

bool MetaStoreImpl::Save(const std::string& path,
                         OnSnapshotSaveDoneClosure* done) {
    ReadLockGuard readLockGuard(rwLock_);
    std::thread th = std::thread(&MetaStoreImpl::SaveBack, this, path, done);
    th.detach();
    return true;
}

bool MetaStoreImpl::Clear() {
    WriteLockGuard writeLockGuard(rwLock_);
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
        status = MetaStatusCode::PARTITION_EXIST;
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
MetaStatusCode MetaStoreImpl::CreateDentry(const CreateDentryRequest* request,
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

MetaStatusCode MetaStoreImpl::GetDentry(const GetDentryRequest* request,
                                    GetDentryResponse* response) {
    uint32_t fsId = request->fsid();
    uint64_t parentInodeId = request->parentinodeid();
    std::string name = request->name();
    ReadLockGuard readLockGuard(rwLock_);
    std::shared_ptr<Partition> partition = GetPartition(request->partitionid());
    if (partition == nullptr) {
        MetaStatusCode status = MetaStatusCode::PARTITION_NOT_FOUND;
        response->set_statuscode(status);
        return status;
    }
    MetaStatusCode status = partition->GetDentry(fsId, parentInodeId, name,
                                                 response->mutable_dentry());
    response->set_statuscode(status);
    if (status != MetaStatusCode::OK) {
        response->clear_dentry();
    }
    return status;
}

MetaStatusCode MetaStoreImpl::DeleteDentry(const DeleteDentryRequest* request,
                                       DeleteDentryResponse* response) {
    uint32_t fsId = request->fsid();
    uint64_t parentInodeId = request->parentinodeid();
    std::string name = request->name();
    ReadLockGuard readLockGuard(rwLock_);
    std::shared_ptr<Partition> partition = GetPartition(request->partitionid());
    if (partition == nullptr) {
        MetaStatusCode status = MetaStatusCode::PARTITION_NOT_FOUND;
        response->set_statuscode(status);
        return status;
    }
    MetaStatusCode status = partition->DeleteDentry(fsId, parentInodeId, name);
    response->set_statuscode(status);
    return status;
}

MetaStatusCode MetaStoreImpl::ListDentry(const ListDentryRequest* request,
                                     ListDentryResponse* response) {
    uint32_t fsId = request->fsid();
    uint64_t parentInodeId = request->dirinodeid();
    ReadLockGuard readLockGuard(rwLock_);
    std::shared_ptr<Partition> partition = GetPartition(request->partitionid());
    if (partition == nullptr) {
        MetaStatusCode status = MetaStatusCode::PARTITION_NOT_FOUND;
        response->set_statuscode(status);
        return status;
    }

    std::list<Dentry> dentryList;
    MetaStatusCode status =
        partition->ListDentry(fsId, parentInodeId, &dentryList);
    if (status != MetaStatusCode::OK) {
        response->set_statuscode(status);
        return status;
    }

    // find last dentry
    std::string last;
    bool findLast = false;
    auto iter = dentryList.begin();
    if (request->has_last()) {
        last = request->last();
        VLOG(1) << "last = " << last;
        for (; iter != dentryList.end(); ++iter) {
            if (iter->name() == last) {
                iter++;
                findLast = true;
                break;
            }
        }
    }

    if (!findLast) {
        iter = dentryList.begin();
    }

    uint32_t count = UINT32_MAX;
    if (request->has_count()) {
        count = request->count();
        VLOG(1) << "count = " << count;
    }

    uint64_t index = 0;
    while (iter != dentryList.end() && index < count) {
        Dentry* dentry = response->add_dentrys();
        dentry->CopyFrom(*iter);
        VLOG(1) << "return client, index = " << index
                << ", dentry :" << iter->ShortDebugString();
        index++;
        iter++;
    }

    VLOG(1) << "return count = " << index;

    response->set_statuscode(status);
    return status;
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

MetaStatusCode MetaStoreImpl::UpdateInodeS3Version(
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

// TODO(chenjingli): implement
MetaStatusCode MetaStoreImpl::PrepareRenameTx(
    const PrepareRenameTxRequest* request, PrepareRenameTxResponse* response) {
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
