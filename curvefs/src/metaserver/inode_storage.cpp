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

namespace curvefs {
namespace metaserver {

using ::curve::common::StringToUl;
using ::curve::common::StringToUll;
using ::curve::common::SplitString;
using ::curve::common::StringStartWith;
using ::curvefs::metaserver::storage::Status;
using ::curvefs::metaserver::storage::KVStorage;
using Transaction = std::shared<StorageTransaction>;
using Key4Inode = KeyGenerator::Key4Inode;
using Key4ChunkInfoList = KeyGenerator::Key4ChunkInfoList;
using Prefix4InodeChunkInfoList = KeyGenerator::Prefix4InodeChunkInfoList;

const size_t KeyGenerator::kMaxUint64Len_ =
    std::to_string(std::numeric_limits<uint64_t>::max()).size();

// kTypeInode:fsId:InodeId
inline std::string KeyGenerator::SerializeToString(Key4Inode key) {
    std::ostringstream oss;
    oss << kTypeInode << ":" << key.fsId << ":" << key.inodeId;
    return oss.str();
}

// kTypeS3ChunkInfo:fsId:inodeId:chunkIndex:firstChunkId:lastChunkId
inline std::string KeyGenerator::SerializeToString(Key4ChunkInfoList key) {
    std::ostringstream oss;
    oss << kTypeS3ChunkInfo << ":" << key.fsId << ":"
        << key.inodeId << ":" << key.chunkIndex << ":"
        << std::setw(kMaxUint64Len_) << std::setfill('0') << key.firstChunkId
        << std::setw(kMaxUint64Len_) << std::setfill('0') << key.lastChunkId;
    return oss.str();
}

// kTypeS3ChunkInfo:fsId:inodeId:
inline std::string KeyGenerator::SerializeToString(Prefix4InodeChunkInfoList key) {
    std::ostringstream oss;
    oss << kTypeS3ChunkInfo << ":" << key.fsId << ":" << key.inodeId << ":";
    return oss.str();
}

std::string KeyGenerator::SerializeToString(Prefix4AllS3ChunkInfoList key) {
    std::ostringstream oss;
    oss << kTypeS3ChunkInfo << ":";
    return oss.str();
}

inline bool KeyGenerator::PraseFromString(const std::string& s,
                                          Key4Inode* key) {
    std::vector<std::string> items;
    SplitString(key, ":", &items);
    if (items.size() != 3 ||
        !StringToUl(items[1], &key->fsId) ||
        !StringToUll(items[2], &key->inodeId) {
        return false;
    }
    return true;
}

inline bool KeyGenerator::PraseFromString(const std::string& s,
                                          Key4ChunkInfoList* key) {
    std::vector<std::string> items;
    SplitString(key, ":", &items);
    if (items.size() != 6 ||
        !StringToUl(items[1], &key->fsId) ||
        !StringToUll(items[2], &key->inodeId ||
        !StringToUll(items[3], &key->chunkIndex ||
        !StringToUll(items[4], &key->firstChunkId ||
        !StringToUll(items[5], &key->lastChunkId) {
        return false;
    }
    return true;
}

inline bool ValueGenerator::SerializeToString(Inode& inode,
                                              std::string* value) {
    if (!inode.IsInitialized()) {
        return false;
    }
    return inode.SerializeToString(value);
}

inline bool ValueGenerator::SerializeToString(S3ChunkInfoList& list,
                                              std::string* value) {
    if (!list.IsInitialized()) {
        return false;
    }
    return list.SerializeToString(value);
}


inline bool ValueGenerator::PraseFromString(std::string& value,
                                            Inode* inode) {
    return inode->ParseFromString(value);
}

inline bool ValueGenerator::PraseFromString(std::string& value,
                                            S3ChunkInfoList* list) {
    return list->ParseFromString(value);
}

InodeStorage::InodeStorage(std::shared_ptr<KVStorage> kvStorage,
                           const std::string& tablename)
    : kvStorage_(kvStorage),
      tablename_(tablename) {}


inline void InodeStorage::AddInodeId(const std::string& key) {
    counter_.insert(key);
}

inline void InodeStorage::DeleteInodeId(const std::string& key) {
    counter_.erase(key);
}

inline bool InodeStorage::InodeIdExist(const std::string& key) {
    return counter_.find(key) != counter_.end();
}

MetaStatusCode InodeStorage::Insert(const Inode& inode) {
    WriteLockGuard writeLockGuard(rwLock_);
    std::string key = keyGenerator_.SerializeToString(
        Key4Inode(inode.fsid(), inode.inodeid()))
    std::string value;
    if (InodeIdExist(key)) {
        return MetaStatusCode::INODE_EXIST;
    } else if (!valueGenerator_.SerializeToString(inode, &value)) {
        return MetaStatusCode::ENCODE_INODE_FAILED;
    }

    Status s = kvStorage_->HSet(tablename_, key, value);
    if (s.ok()) {
        AddInodeId(key);
        return MetaStatusCode::OK;
    }
    return MetaStatusCode::STORAGE_INTERNAL_ERROR;
}

MetaStatusCode InodeStorage::Get(const InodeKey& inodeKey, Inode* inode) {
    ReadLockGuard readLockGuard(rwLock_);
    std::string key = keyGenerator_.SerializeToString(
        Key4Inode(Inodek.fsid(), inode.inodeid()))
    if (!InodeIdExist(key)) {
        return MetaStatusCode::NOT_FOUND;
    }

    std::string value;
    Status s = kvStorage_->HGet(tablename_, key, &value);
    if (!s.ok()) {
        return MetaStatusCode::STORAGE_INTERNAL_ERROR;
    } else if (!Str2Inode(value, inode)) {
        return MetaStatusCode::DECODE_INODE_FAILED;
    }
    return MetaStatusCode::OK;
}

MetaStatusCode InodeStorage::GetAttr(const InodeKey& inodeKey,
    InodeAttr *attr) {
    ReadLockGuard readLockGuard(rwLock_);
    std::string key = Key2Str(inodeKey);
    if (!InodeIdExist(key)) {
        return MetaStatusCode::NOT_FOUND;
    }

    Inode inode;
    std::string value;
    Status s = kvStorage_->HGet(tablename_, key, &value);
    if (!s.ok()) {
        return MetaStatusCode::STORAGE_INTERNAL_ERROR;
    } else if (!Str2Inode(value, &inode)) {
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

MetaStatusCode InodeStorage::GetXAttr(const InodeKey& inodeKey, XAttr *xattr) {
    ReadLockGuard readLockGuard(rwLock_);
    std::string key = Key2Str(inodeKey);
    if (!InodeIdExist(key)) {
        return MetaStatusCode::NOT_FOUND;
    }

    Inode inode;
    std::string value;
    Status s = kvStorage_->HGet(tablename_, key, &value);
    if (!s.ok()) {
        return MetaStatusCode::STORAGE_INTERNAL_ERROR;
    } else if (!Str2Inode(value, &inode)) {
        return MetaStatusCode::DECODE_INODE_FAILED;
    }

    if (!inode.xattr().empty()) {
        *(xattr->mutable_xattrinfos()) = inode.xattr();
    }
    return MetaStatusCode::OK;
}

MetaStatusCode InodeStorage::Delete(const InodeKey& inodeKey) {
    WriteLockGuard writeLockGuard(rwLock_);
    std::string key = Key2Str(inodeKey);
    if (!InodeIdExist(key)) {
        return MetaStatusCode::NOT_FOUND;
    }

    Status s = kvStorage_->HDel(tablename_, key);
    if (s.ok()) {
        DeleteInodeId(key);
        return MetaStatusCode::OK;
    }
    return MetaStatusCode::STORAGE_INTERNAL_ERROR;
}

MetaStatusCode InodeStorage::Update(const Inode& inode) {
    WriteLockGuard writeLockGuard(rwLock_);
    std::string value;
    std::string key = Key2Str(InodeKey(inode.fsid(), inode.inodeid()));
    if (!InodeIdExist(key)) {
        return MetaStatusCode::NOT_FOUND;
    } else if (!Inode2Str(inode, &value)) {
        return MetaStatusCode::ENCODE_INODE_FAILED;
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
    std::string key = keyGenerator_ Key4AppendS3ChunkInfo(
        fsId, inodeId, chunkIndex, firstChunkId, lastChunkId);

    // value
    std::string value;
    if (!list2add.SerializeToString(&value)) {
        LOG(ERROR) << "Serialize S3ChunkInfoList failed."
        return MetaStatusCode::ENCODE_ENTRY_FAILED;
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
    std::string prefix = Key4GetS3ChunkInfo(fsId, inodeId, chunkIndex);
    auto iterator = kvStorage_->SSeek(prefix);
    if (iterator->Status() != 0) {
        return MetaStatusCode::STORAGE_INTERNAL_ERROR;
    }

    uint64_t lastChunkId;
    for (iterator->SeekToFirst(); iterator->Valid(); iterator->Next()) {
        std::string key = iterator->Key();
        if (!StringStartWith(key, prefix)) {
            break;
        } else if (!ParseKey4AppendS3ChunkInfoList(key, &lastChunkId)) {
            return  MetaStatusCode::DECODE_KEY_FAILED;
        } else if (lastChunkId > minChunkId) {
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
    std::string prefix = kg_.SerializeToString(Prefix4AllS3ChunkInfoList());
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
            auto info = to->add_s3chunks();
            info->CopyFrom(item);
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

std::shared_ptr<Iterator> InodeStorage::GetAll() {
    return kvStorage_->HGetAll(tablename_);
}

MetaStatusCode InodeStorage::Clear() {
    ReadLockGuard w(rwLock_);
    Status s = kvStorage_->HClear(tablename_);
    if (s.ok()) {
        return MetaStatusCode::OK;
    }
    return MetaStatusCode::STORAGE_INTERNAL_ERROR;
}

size_t InodeStorage::Size() {
    return kvStorage_->HSize(tablename_);
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
