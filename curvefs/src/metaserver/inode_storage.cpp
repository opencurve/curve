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
 * Project: curve
 * Created Date: 2021-05-19
 * Author: chenwei
 */

#include <string>
#include <memory>
#include <vector>
#include <algorithm>

#include "src/common/string_util.h"
#include "curvefs/src/metaserver/inode_storage.h"
#include "curvefs/src/metaserver/storage_common.h"

namespace curvefs {
namespace metaserver {

using ::curve::common::StringStartWith;
using ::curvefs::metaserver::storage::Status;
using ::curvefs::metaserver::storage::KVStorage;
using Transaction = std::shared<StorageTransaction>;

InodeStorage::InodeStorage(const std::string& tablename
                           std::shared_ptr<KVStorage> kvStorage)
    : tablename_(tablename),
      kvStorage_(kvStorage),
      converter_(std::make_shared<Converter>()) {}

MetaStatusCode InodeStorage::Insert(const Inode& inode) {
    WriteLockGuard writeLockGuard(rwLock_);
    Key4Inode key4Inode(inode.fsid(), inode.inodeid());
    std::string key = converter_->SerializeToString(key4Inode);
    std::string value;
    if (FindKey(key)) {
        return MetaStatusCode::INODE_EXIST;
    } else if (!converter_->SerializeToString(inode, &value)) {
        return MetaStatusCode::SERIALIZE_TO_STRING_FAILED;
    }

    Status s = kvStorage_->HSet(tablename_, key, value);
    if (s.ok()) {
        InsertKey(key);
        return MetaStatusCode::OK;
    }
    return MetaStatusCode::STORAGE_INTERNAL_ERROR;
}

MetaStatusCode InodeStorage::Get(const Key4Inode& key4Inode, Inode* inode) {
    ReadLockGuard readLockGuard(rwLock_);
    std::string key = converter_->SerializeToString(key4Inode);
    if (!FindKey(key)) {
        return MetaStatusCode::NOT_FOUND;
    }

    std::string value;
    Status s = kvStorage_->HGet(tablename_, key, &value);
    if (!s.ok()) {
        return MetaStatusCode::STORAGE_INTERNAL_ERROR;
    } else if (!converter_->SerializeToString(inode, &value)) {
        return MetaStatusCode::SERIALIZE_TO_STRING_FAILED;
    }
    return MetaStatusCode::OK;
}

MetaStatusCode InodeStorage::GetAttr(const Key4Inode& key4Inode,
    InodeAttr *attr) {
    ReadLockGuard readLockGuard(rwLock_);
    std::string key = converter_->SerializeToString(key4Inode);
    if (!FindKey(key)) {
        return MetaStatusCode::NOT_FOUND;
    }

    Inode inode;
    std::string value;
    Status s = kvStorage_->HGet(tablename_, key, &value);
    if (!s.ok()) {
        return MetaStatusCode::STORAGE_INTERNAL_ERROR;
    } else if (!converter_->SerializeToString(inode, &value)) {
        return MetaStatusCode::DECODE_INODE_FAILED;
    }

    // get attr from inode
    attr->set_inodeid(inode.inodeid());
    attr->set_fsid(inode.fsid());
    attr->set_length(inode.length());
    attr->set_ctime(inode.ctime());
    attr->set_ctime_ns(inode.ctime_ns());
    attr->set_mtime(inode.mtime());
    attr->set_mtime_ns(inode.mtime_ns());
    attr->set_atime(inode.atime());
    attr->set_atime_ns(inode.atime_ns());
    attr->set_uid(inode.uid());
    attr->set_gid(inode.gid());
    attr->set_mode(inode.mode());
    attr->set_nlink(inode.nlink());
    attr->set_type(inode.type());
    if (inode.has_symlink()) {
        attr->set_symlink(inode.symlink());
    }
    if (inode.has_rdev()) {
        attr->set_rdev(inode.rdev());
    }
    if (inode.has_dtime()) {
        attr->set_dtime(inode.dtime());
    }
    if (inode.has_openmpcount()) {
        attr->set_openmpcount(inode.openmpcount());
    }
    return MetaStatusCode::OK;
}

MetaStatusCode InodeStorage::GetXAttr(const Key4Inode& inodeKey, XAttr *xattr) {
    ReadLockGuard readLockGuard(rwLock_);
    std::string key = converter_->SerializeToString(key4Inode);
    if (!FindKey(key)) {
        return MetaStatusCode::NOT_FOUND;
    }

    Inode inode;
    std::string value;
    Status s = kvStorage_->HGet(tablename_, key, &value);
    if (!s.ok()) {
        return MetaStatusCode::STORAGE_INTERNAL_ERROR;
    } else if (!converter_->SerializeToString(inode, &value)) {
        return MetaStatusCode::DECODE_INODE_FAILED;
    }

    if (!inode.xattr().empty()) {
        *(xattr->mutable_xattrinfos()) = inode.xattr();
    }
    return MetaStatusCode::OK;
}

MetaStatusCode InodeStorage::Delete(const Key4Inode& key4Inode) {
    WriteLockGuard writeLockGuard(rwLock_);
    std::string key = converter_->SerializeToString(key4Inode);
    if (!FindKey(key)) {
        return MetaStatusCode::NOT_FOUND;
    }

    Status s = kvStorage_->HDel(tablename_, key);
    if (s.ok()) {
        EraseKey(key);
        return MetaStatusCode::OK;
    }
    return MetaStatusCode::STORAGE_INTERNAL_ERROR;
}

MetaStatusCode InodeStorage::Update(const Inode& inode) {
    WriteLockGuard writeLockGuard(rwLock_);
    std::string value;
    Key4Inode key4Inode(inode.fsid(), inode.inodeid());
    std::string key = converter_->SerializeToString(key4Inode);
    if (!FindKey(key)) {
        return MetaStatusCode::NOT_FOUND;
    } else if (converter_->SerializeToString(inode, &value)) {
        return MetaStatusCode::SERIALIZE_TO_STRING_FAILED;
    }

    Status s = kvStorage_->HSet(tablename_, key, value);
    if (s.ok()) {
        return MetaStatusCode::OK;
    }
    return MetaStatusCode::STORAGE_INTERNAL_ERROR;
}

MetaStatusCode InodeStorage::AddS3ChunkInfoList(Transaction txn,
                                                uint32_t fsId,
                                                uint64_t inodeId,
                                                uint64_t chunkIndex,
                                                S3ChunkInfoList& list2add) {
    // key
    uint64_t firstChunkId = list2add.s3chunks(0).chunkid();
    uint64_t lastChunkId = list2add.s3chunks(size - 1).chunkid();
    Key4S3ChunkInfoList key4List(fsId, inodeId, chunkindex,
                                 firstChunkId, lastChunkId);
    std::string key = converter_->SerializeToString(key4List);

    // value
    std::string value;
    if (!converter_->SerializeToString(list2add, &value)) {
        LOG(ERROR) << "Serialize S3ChunkInfoList failed."
        return MetaStatusCode::SERIALIZE_TO_STRING_FAILED;
    }

    Status s = kvStorage_->SSet(key, value);
    return s.ok() ? MetaStatusCode::OK ?
                    MetaStatusCode::STORAGE_INTERNAL_ERROR;
}

// NOTE: s3chunkinfo which its chunkid less then min chunkid should be removed
MetaStatusCode InodeStorage::RemoveS3ChunkInfoList(Transaction txn,
                                                   uint32_t fsId,
                                                   uint64_t inodeId,
                                                   uint64_t chunkIndex,
                                                   uint64_t minChunkId) {
    Prefix4ChunkIndexS3ChunkInfoList prefixKey(fsId, inodeId, chunkIndex);
    std::string prefix = converter_->SerializeToString(prefixKey);
    auto iterator = kvStorage_->SSeek(prefix);
    if (iterator->Status() != 0) {
        return MetaStatusCode::STORAGE_INTERNAL_ERROR;
    }

    uint64_t lastChunkId;
    Key4S3ChunkInfoList key4List;
    for (iterator->SeekToFirst(); iterator->Valid(); iterator->Next()) {
        std::string key = iterator->Key();
        if (!StringStartWith(key, prefix)) {
            break;
        } else if (converter_->ParseFromString(key, &key4List)) {
            return MetaStatusCode::PARSE_FROM_STRING_FAILED;
        } else if (key4List.lastChunkId > minChunkId) {
            continue;
        }

        // lastChunkId <= minChunkId
        if (!txn->SDel(key).ok()) {
            return MetaStatusCode::STORAGE_INTERNAL_ERROR;
        }
    }
    return MetaStatusCode::STORAGE_INTERNAL_ERROR;
}

MetaStatusCode InodeStorage::AppendS3ChunkInfoList(uint32_t fsId,
                                                   uint64_t inodeId,
                                                   uint64_t chunkIndex,
                                                   S3ChunkInfoList& list2add,
                                                   bool compaction) {
    WriteLockGuard writeLockGuard(rwLock_);
    size_t size = list2add.s3chunks_size()
    if (size == 0) {
        return MetaStatusCode::OK;
    }

    auto txn = kvStorage_->BeginTransaction();
    if (nullptr == txn) {
        return MetaStatusCode::STORAGE_INTERNAL_ERROR;
    }

    MetaStatusCode rc;
    rc = AddS3ChunkInfoList(txn, fsId, inodeId, chunkIndex, list2add);
    if (rc == MetaStatusCode::OK && compaction) {
        uint64_t minChunkId = list2add.s3chunks(0).chunkid();
        rc = RemoveS3ChunkInfoList(txn, fsId, inodeId, chunkIndex, minChunkId);
    }

    if (rc != MetaStatusCode::OK) {
        txn->Rollback();
    } else if (!txn.Commit().ok()) {
        rc = MetaStatusCode::STORAGE_INTERNAL_ERROR
    }
    return rc;
}

std::shared_ptr<Iterator> InodeStorage::GetInodeS3ChunkInfoList(
    uint32_t fsId, uint64_t inodeId) {
    ReadLockGuard readLockGuard(rwLock_);
    std::string key = Key4InodeS3ChunkInfo(fsId, inodeId);
    return kvStorage_->SSeek(tablename_, key);
}

std::shared_ptr<Iterator> InodeStorage::GetAllS3ChunkInfoList() {
    ReadLockGuard readLockGuard(rwLock_);
    std::string prefix = converter_->SerializeToString(
        Prefix4AllInodeS3ChunkInfoList());
    return kvStorage_->SSeek(tablename_, prefix);
}

MetaStatusCode InodeStorage::PaddingInodeS3ChunkInfo(int32_t fsId,
                                                     uint64_t inodeId,
                                                     Inode* inode) {
    ReadLockGuard readLockGuard(rwLock_);
    auto iterator = GetAllS3ChunkInfo(fsId, inodeId);
    if (iterator->Status() != 0) {
        LOG(ERROR) << "Get s3chunkinfo failed"
        return MetaStatusCode::STORAGE_INTERNAL_ERROR;
    }

    auto merge = [](const S3ChunkInfoList& from, S3ChunkInfoList* to) {
        for (const auto& item : from) {
            auto chunkinfo = to->add_s3chunks();
            chunkinfo->CopyFrom(item);
        }
    };

    S3ChunkInfoList list;
    Key4S3ChunkInfoList key4S3ChunkInfoList;
    auto m = inode->mutable_s3chunkinfomap();
    for (iterator->SeekToFirst(); iterator->Valid(); iterator->Next()) {
        std::string key = iterator->Key();
        std::string value = iterator->Value();
        if (!keyGenerator_.ParseFromString(key, &key4S3ChunkInfoList)) {
            return MetaStatusCode::PARSE_KEY_FAILED;
        } else if (!valueGenerator_.ParseFromString(value, &list)) {
            return MetaStatusCode::PARSE_VALUE_FAILED;
        }

        uint64_t chunkIndex = key4S3ChunkInfoList.chunkIndex;
        auto iter = m->find(chunkIndex);
        if (iter == m->end()) {
            m->insert({chunkIndex, list})
        } else {
            merge(list, &iter->second);
        }
    }

    return MetaStatusCode::OK;
}

std::shared_ptr<Iterator> InodeStorage::GetAllInode() {
    std::string prefix = converter_->SerializeToString(Pre4AllInode());
    return kvStorage_->SSeek(tablename_, prefix);
}

size_t InodeStorage::Size() {
    return kvStorage_->HSize(tablename_);
}

MetaStatusCode InodeStorage::Clear() {
    ReadLockGuard w(rwLock_);
    Status s = kvStorage_->HClear(tablename_);
    if (!s.ok()) {
        return MetaStatusCode::STORAGE_INTERNAL_ERROR;
    }
    keySet_.clear();
    return MetaStatusCode::OK;
}

void InodeStorage::GetInodeIdList(std::list<uint64_t>* inodeIdList) {
    ReadLockGuard readLockGuard(rwLock_);
    for (const auto& key : counter_) {
        auto pair = ExtractKey(key);
        inodeIdList->push_back(pair.second);
    }
}

}  // namespace metaserver
}  // namespace curvefs
