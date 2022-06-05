/*
 *  Copyright (c) 2022 NetEase Inc.
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
 * Created Date: 2022-03-16
 * Author: Jingli Chen (Wine93)
 */

#include <memory>
#include <string>
#include <vector>
#include <unordered_map>
#include <utility>

#include "curvefs/proto/common.pb.h"
#include "curvefs/proto/metaserver.pb.h"
#include "curvefs/src/metaserver/metastore_fstream.h"
#include "curvefs/src/metaserver/storage/converter.h"
#include "curvefs/src/metaserver/storage/storage_fstream.h"
#include "curvefs/src/metaserver/copyset/utils.h"

namespace curvefs {
namespace metaserver {

using ::curvefs::common::PartitionInfo;
using ::curvefs::metaserver::Inode;
using ::curvefs::metaserver::Dentry;
using ::curvefs::metaserver::storage::ENTRY_TYPE;
using ::curvefs::metaserver::storage::SaveToFile;
using ::curvefs::metaserver::storage::LoadFromFile;
using ::curvefs::metaserver::storage::IteratorWrapper;
using ::curvefs::metaserver::storage::ContainerIterator;

using ContainerType = std::unordered_map<std::string, std::string>;
using STORAGE_TYPE = ::curvefs::metaserver::storage::KVStorage::STORAGE_TYPE;
using ChildrenType = ::curvefs::metaserver::storage::MergeIterator::ChildrenType;  // NOLINT
using DumpFileClosure = ::curvefs::metaserver::storage::DumpFileClosure;
using Key4S3ChunkInfoList = ::curvefs::metaserver::storage::Key4S3ChunkInfoList;

using ::curvefs::metaserver::storage::Key4VolumeExtentSlice;

MetaStoreFStream::MetaStoreFStream(PartitionMap* partitionMap,
                                   std::shared_ptr<KVStorage> kvStorage,
                                   PoolId poolId,
                                   CopysetId copysetId)
    : partitionMap_(partitionMap),
      kvStorage_(std::move(kvStorage)),
      conv_(std::make_shared<Converter>()),
      poolId_(poolId),
      copysetId_(copysetId) {}

std::shared_ptr<Partition> MetaStoreFStream::GetPartition(
    uint32_t partitionId) {
    auto iter = partitionMap_->find(partitionId);
    if (iter != partitionMap_->end()) {
        return iter->second;
    }
    return nullptr;
}

bool MetaStoreFStream::LoadPartition(uint32_t partitionId,
                                     const std::string& key,
                                     const std::string& value) {
    PartitionInfo partitionInfo;
    if (!conv_->ParseFromString(value, &partitionInfo)) {
        LOG(ERROR) << "Decode PartitionInfo failed";
        return false;
    }

    partitionId = partitionInfo.partitionid();
    auto partition = std::make_shared<Partition>(partitionInfo, kvStorage_);
    partitionMap_->emplace(partitionId, partition);
    if (!partition->Clear()) {  // it will clear all inodes and dentrys
        LOG(ERROR) << "Clear partition failed, partitionId = " << partitionId;
        return false;
    }
    return true;
}

bool MetaStoreFStream::LoadInode(uint32_t partitionId,
                                 const std::string& key,
                                 const std::string& value) {
    auto partition = GetPartition(partitionId);
    if (nullptr == partition) {
        LOG(ERROR) << "Partition not found, partitionId = " << partitionId;
        return false;
    }

    Inode inode;
    if (!conv_->ParseFromString(value, &inode)) {
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

bool MetaStoreFStream::LoadDentry(uint8_t version,
                                  uint32_t partitionId,
                                  const std::string& key,
                                  const std::string& value) {
    auto partition = GetPartition(partitionId);
    if (nullptr == partition) {
        LOG(ERROR) << "Partition not found, partitionId = " << partitionId;
        return false;
    }

    DentryVec vec;
    if (version == 1) {
        Dentry dentry;
        if (!conv_->ParseFromString(value, &dentry)) {
            LOG(ERROR) << "Decode dentry failed";
            return false;
        }
        *vec.add_dentrys() = dentry;
    } else if (!conv_->ParseFromString(value, &vec)) {
        LOG(ERROR) << "Decode dentry vector failed";
        return false;
    }

    MetaStatusCode rc = partition->LoadDentry(vec, version == 1);
    if (rc != MetaStatusCode::OK) {
        LOG(ERROR) << "LoadDentry failed, retCode = "
                   << MetaStatusCode_Name(rc);
        return false;
    }
    return true;
}

bool MetaStoreFStream::LoadPendingTx(uint32_t partitionId,
                                     const std::string& key,
                                     const std::string& value) {
    auto partition = GetPartition(partitionId);
    if (nullptr == partition) {
        LOG(ERROR) << "Partition not found, partitionId = " << partitionId;
        return false;
    }

    PrepareRenameTxRequest pendingTx;
    if (!conv_->ParseFromString(value, &pendingTx)) {
        LOG(ERROR) << "Decode pending tx failed";
        return false;
    }

    bool succ = partition->InsertPendingTx(pendingTx);
    if (!succ) {
        LOG(ERROR) << "InsertPendingTx failed";
    }
    return succ;
}

bool MetaStoreFStream::LoadInodeS3ChunkInfoList(uint32_t partitionId,
                                                const std::string& key,
                                                const std::string& value) {
    auto partition = GetPartition(partitionId);
    if (nullptr == partition) {
        LOG(ERROR) << "Partition not found, partitionId = " << partitionId;
        return false;
    }

    S3ChunkInfoList list;
    Key4S3ChunkInfoList key4list;
    if (!conv_->ParseFromString(key, &key4list)) {
        LOG(ERROR) << "Decode Key4S3ChunkInfoList failed";
        return false;
    } else if (!conv_->ParseFromString(value, &list)) {
        LOG(ERROR) << "Decode S3ChunkInfoList failed";
        return false;
    }

    S3ChunkInfoMap map2add;
    S3ChunkInfoMap map2del;
    std::shared_ptr<Iterator> iterator;
    map2add.insert({key4list.chunkIndex, list});
    MetaStatusCode rc = partition->GetOrModifyS3ChunkInfo(
        key4list.fsId, key4list.inodeId, map2add, map2del, false, &iterator);
    if (rc != MetaStatusCode::OK) {
        LOG(ERROR) << "GetOrModifyS3ChunkInfo failed, retCode = "
                   << MetaStatusCode_Name(rc);
        return false;
    }
    return true;
}

bool MetaStoreFStream::LoadVolumeExtentList(uint32_t partitionId,
                                            const std::string& key,
                                            const std::string& value) {
    auto partition = GetPartition(partitionId);
    if (!partition) {
        LOG(ERROR) << "Partition not found, partitionId: " << partitionId;
        return false;
    }

    Key4VolumeExtentSlice sliceKey;
    VolumeExtentSlice slice;

    if (!sliceKey.ParseFromString(key)) {
        LOG(ERROR) << "Fail to decode Key4VolumeExtentSlice, key: `" << key
                   << "`";
        return false;
    }

    if (!conv_->ParseFromString(value, &slice)) {
        LOG(ERROR) << "Decode VolumeExtentSlice failed";
        return false;
    }

    auto st = partition->UpdateVolumeExtentSlice(sliceKey.fsId_,
                                                 sliceKey.inodeId_, slice);

    LOG_IF(ERROR, st != MetaStatusCode::OK)
        << "LoadVolumeExtentList update extent failed, error: "
        << MetaStatusCode_Name(st);

    return st == MetaStatusCode::OK;
}

std::shared_ptr<Iterator> MetaStoreFStream::NewPartitionIterator() {
    std::string value;
    auto container = std::make_shared<ContainerType>();
    for (const auto& item : *partitionMap_) {
        auto partitionId = item.first;
        auto partition = item.second;
        auto partitionInfo = partition->GetPartitionInfo();
        partitionInfo.set_inodenum(partition->GetInodeNum());
        partitionInfo.set_dentrynum(partition->GetDentryNum());
        if (!conv_->SerializeToString(partitionInfo, &value)) {
            return nullptr;
        }
        container->emplace(std::to_string(partitionId), value);
    }

    auto iterator = std::make_shared<ContainerIterator<ContainerType>>(
        container);
    return std::make_shared<IteratorWrapper>(
        ENTRY_TYPE::PARTITION, 0, iterator);
}

std::shared_ptr<Iterator> MetaStoreFStream::NewInodeIterator(
    std::shared_ptr<Partition> partition) {
    auto partitionId = partition->GetPartitionId();
    auto iterator = partition->GetAllInode();
    if (iterator->Status() != 0) {
        return nullptr;
    }
    return std::make_shared<IteratorWrapper>(
        ENTRY_TYPE::INODE, partitionId, iterator);
}

std::shared_ptr<Iterator> MetaStoreFStream::NewDentryIterator(
    std::shared_ptr<Partition> partition) {
    auto partitionId = partition->GetPartitionId();
    auto iterator = partition->GetAllDentry();
    if (iterator->Status() != 0) {
        return nullptr;
    }
    return std::make_shared<IteratorWrapper>(
        ENTRY_TYPE::DENTRY, partitionId, iterator);
}

std::shared_ptr<Iterator> MetaStoreFStream::NewPendingTxIterator(
    std::shared_ptr<Partition> partition) {
    std::string value;
    PrepareRenameTxRequest pendingTx;
    auto container = std::make_shared<ContainerType>();
    if (partition->FindPendingTx(&pendingTx)) {
        if (!conv_->SerializeToString(pendingTx, &value)) {
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

std::shared_ptr<Iterator> MetaStoreFStream::NewInodeS3ChunkInfoListIterator(
    std::shared_ptr<Partition> partition) {
    auto partitionId = partition->GetPartitionId();
    auto iterator = partition->GetAllS3ChunkInfoList();
    if (iterator->Status() != 0) {
        return nullptr;
    }
    return std::make_shared<IteratorWrapper>(
        ENTRY_TYPE::S3_CHUNK_INFO_LIST, partitionId, iterator);
}

std::shared_ptr<Iterator> MetaStoreFStream::NewVolumeExtentListIterator(
    Partition* partition) {
    auto partitionId = partition->GetPartitionId();
    auto iterator = partition->GetAllVolumeExtentList();
    if (iterator->Status() != 0) {
        return nullptr;
    }

    return std::make_shared<IteratorWrapper>(ENTRY_TYPE::VOLUME_EXTENT,
                                             partitionId, std::move(iterator));
}

bool MetaStoreFStream::Load(const std::string& pathname) {
    uint64_t totalPartition = 0;
    uint64_t totalInode = 0;
    uint64_t totalDentry = 0;
    uint64_t totalS3ChunkInfoList = 0;
    uint64_t totalVolumeExtent = 0;
    uint64_t totalPendingTx = 0;

    auto callback = [&](uint8_t version,
                        ENTRY_TYPE entryType,
                        uint32_t partitionId,
                        const std::string& key,
                        const std::string& value) -> bool {
        switch (entryType) {
            case ENTRY_TYPE::PARTITION:
                ++totalPartition;
                return LoadPartition(partitionId, key, value);
            case ENTRY_TYPE::INODE:
                ++totalInode;
                return LoadInode(partitionId, key, value);
            case ENTRY_TYPE::DENTRY:
                ++totalDentry;
                return LoadDentry(version, partitionId, key, value);
            case ENTRY_TYPE::PENDING_TX:
                ++totalPendingTx;
                return LoadPendingTx(partitionId, key, value);
            case ENTRY_TYPE::S3_CHUNK_INFO_LIST:
                ++totalS3ChunkInfoList;
                return LoadInodeS3ChunkInfoList(partitionId, key, value);
            case ENTRY_TYPE::VOLUME_EXTENT:
                ++totalVolumeExtent;
                return LoadVolumeExtentList(partitionId, key, value);
            case ENTRY_TYPE::UNKNOWN:
                break;
        }

        LOG(ERROR) << "Load failed, unknown entry type";
        return false;
    };

    auto ret = LoadFromFile(pathname, callback);

    std::ostringstream oss;
    oss << "total partition: " << totalPartition
        << ", total inode: " << totalInode << ", total dentry: " << totalDentry
        << ", total s3chunkinfolist: " << totalS3ChunkInfoList
        << ", total volumeextent: " << totalVolumeExtent
        << ", total pendingtx: " << totalPendingTx;

    if (ret) {
        LOG(INFO) << "Metastore "
                  << copyset::ToGroupIdString(poolId_, copysetId_)
                  << " load from " << pathname << " succeeded, " << oss.str();
    } else {
        LOG(ERROR) << "Metastore "
                   << copyset::ToGroupIdString(poolId_, copysetId_)
                   << " load from " << pathname << " failed, " << oss.str();
    }

    return ret;
}

bool MetaStoreFStream::Save(const std::string& path,
                            DumpFileClosure* done) {
    ChildrenType children;

    auto iterator = NewPartitionIterator();  // partition
    children.push_back(iterator);
    for (const auto& item : *partitionMap_) {
        auto& partition = item.second;

        iterator = NewInodeIterator(partition);  // inode
        children.push_back(iterator);

        iterator = NewDentryIterator(partition);  // dentry
        children.push_back(iterator);

        iterator = NewPendingTxIterator(partition);  // pending tx
        children.push_back(iterator);

        iterator = NewInodeS3ChunkInfoListIterator(partition);  // s3chunkinfo
        children.push_back(iterator);

        children.push_back(NewVolumeExtentListIterator(partition.get()));
    }

    for (const auto& child : children) {
        if (nullptr == child) {
            done->Runned();
            return false;
        }
    }

    auto mergeIterator = std::make_shared<MergeIterator>(children);
    bool background = (kvStorage_->Type() == STORAGE_TYPE::MEMORY_STORAGE);
    bool succ = SaveToFile(path, mergeIterator, background, done);
    if (succ) {
        LOG(INFO) << "MetaStoreFStream save success";
    } else {
        LOG(ERROR) << "MetaStoreFStream save failed";
    }

    return succ;
}

}  // namespace metaserver
}  // namespace curvefs
