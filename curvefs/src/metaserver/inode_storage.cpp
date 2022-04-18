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
#include "curvefs/src/metaserver/storage/converter.h"

namespace curvefs {
namespace metaserver {

using ::curve::common::StringStartWith;
using ::curvefs::metaserver::storage::Status;
using ::curvefs::metaserver::storage::KVStorage;
using ::curvefs::metaserver::storage::Key4S3ChunkInfoList;
using ::curvefs::metaserver::storage::Prefix4ChunkIndexS3ChunkInfoList;
using ::curvefs::metaserver::storage::Prefix4InodeS3ChunkInfoList;
using ::curvefs::metaserver::storage::Prefix4AllInode;
using Transaction = std::shared_ptr<StorageTransaction>;

InodeStorage::InodeStorage(std::shared_ptr<KVStorage> kvStorage,
                           const std::string& tablename)
    : kvStorage_(kvStorage),
      table4inode_(RealTablename(kTypeInode, tablename)),
      table4s3chunkinfo_(RealTablename(kTypeS3ChunkInfo, tablename)),
      conv_(std::make_shared<Converter>()) {}

MetaStatusCode InodeStorage::Insert(const Inode& inode) {
    WriteLockGuard writeLockGuard(rwLock_);
    Key4Inode key(inode.fsid(), inode.inodeid());
    std::string skey = conv_->SerializeToString(key);
    if (FindKey(skey)) {
        return MetaStatusCode::INODE_EXIST;
    }

    Status s = kvStorage_->HSet(table4inode_, skey, inode);
    if (s.ok()) {
        InsertKey(skey);
        return MetaStatusCode::OK;
    }
    LOG(ERROR) << "Insert inode failed, status = " << s.ToString();
    return MetaStatusCode::STORAGE_INTERNAL_ERROR;
}

MetaStatusCode InodeStorage::Get(const Key4Inode& key, Inode* inode) {
    ReadLockGuard readLockGuard(rwLock_);
    std::string skey = conv_->SerializeToString(key);
    if (!FindKey(skey)) {
        return MetaStatusCode::NOT_FOUND;
    }

    Status s = kvStorage_->HGet(table4inode_, skey, inode);
    if (s.ok()) {
        return MetaStatusCode::OK;
    }
    LOG(ERROR) << "Get inode failed, status = " << s.ToString();
    return MetaStatusCode::STORAGE_INTERNAL_ERROR;
}

MetaStatusCode InodeStorage::GetAttr(const Key4Inode& key,
    InodeAttr *attr) {
    ReadLockGuard readLockGuard(rwLock_);
    std::string skey = conv_->SerializeToString(key);
    if (!FindKey(skey)) {
        return MetaStatusCode::NOT_FOUND;
    }

    Inode inode;
    Status s = kvStorage_->HGet(table4inode_, skey, &inode);
    if (!s.ok()) {
        return MetaStatusCode::STORAGE_INTERNAL_ERROR;
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

MetaStatusCode InodeStorage::GetXAttr(const Key4Inode& key, XAttr *xattr) {
    ReadLockGuard readLockGuard(rwLock_);
    std::string skey = conv_->SerializeToString(key);
    if (!FindKey(skey)) {
        return MetaStatusCode::NOT_FOUND;
    }

    Inode inode;
    Status s = kvStorage_->HGet(table4inode_, skey, &inode);
    if (!s.ok()) {
        return MetaStatusCode::STORAGE_INTERNAL_ERROR;
    }

    if (!inode.xattr().empty()) {
        *(xattr->mutable_xattrinfos()) = inode.xattr();
    }
    return MetaStatusCode::OK;
}

MetaStatusCode InodeStorage::Delete(const Key4Inode& key) {
    WriteLockGuard writeLockGuard(rwLock_);
    std::string skey = conv_->SerializeToString(key);
    if (!FindKey(skey)) {
        return MetaStatusCode::NOT_FOUND;
    }

    Status s = kvStorage_->HDel(table4inode_, skey);
    if (s.ok()) {
        EraseKey(skey);
        return MetaStatusCode::OK;
    }
    return MetaStatusCode::STORAGE_INTERNAL_ERROR;
}

MetaStatusCode InodeStorage::Update(const Inode& inode) {
    WriteLockGuard writeLockGuard(rwLock_);
    Key4Inode key(inode.fsid(), inode.inodeid());
    std::string skey = conv_->SerializeToString(key);
    if (!FindKey(skey)) {
        return MetaStatusCode::NOT_FOUND;
    }

    Status s = kvStorage_->HSet(table4inode_, skey, inode);
    if (s.ok()) {
        return MetaStatusCode::OK;
    }
    return MetaStatusCode::STORAGE_INTERNAL_ERROR;
}

MetaStatusCode InodeStorage::AddS3ChunkInfoList(
    Transaction txn,
    uint32_t fsId,
    uint64_t inodeId,
    uint64_t chunkIndex,
    const S3ChunkInfoList& list2add) {
    if (list2add.s3chunks_size() == 0) {
        return MetaStatusCode::OK;
    }

    size_t size = list2add.s3chunks_size();
    uint64_t firstChunkId = list2add.s3chunks(0).chunkid();
    uint64_t lastChunkId = list2add.s3chunks(size - 1).chunkid();
    Key4S3ChunkInfoList key(fsId, inodeId, chunkIndex,
                            firstChunkId, lastChunkId);
    std::string skey = conv_->SerializeToString(key);

    Status s = txn->SSet(table4s3chunkinfo_, skey, list2add);
    return s.ok() ? MetaStatusCode::OK :
                    MetaStatusCode::STORAGE_INTERNAL_ERROR;
}

bool InodeStorage::SplitS3ChunkInfoList(const std::string& value,
                                        uint64_t delFirstChunkId,
                                        S3ChunkInfoList* list2add) {
    if (!conv_->ParseFromString(value, list2add)) {
        LOG(ERROR) << "ParseFromString failed for S3ChunkInfoList";
        return false;
    }

    size_t size = list2add->s3chunks_size();
    for (size_t i = 0; i < size; i++) {
        if (list2add->s3chunks(i).chunkid() >= delFirstChunkId) {
            list2add->mutable_s3chunks()->DeleteSubrange(i, size - i);
            break;
        }
    }
    return true;
}

MetaStatusCode InodeStorage::DelS3ChunkInfoList(
    Transaction txn,
    uint32_t fsId,
    uint64_t inodeId,
    uint64_t chunkIndex,
    const S3ChunkInfoList& list2del) {
    if (list2del.s3chunks_size() == 0) {
        return MetaStatusCode::OK;
    }

    size_t size = list2del.s3chunks_size();
    uint64_t delFirstChunkId = list2del.s3chunks(0).chunkid();
    uint64_t delLastChunkId = list2del.s3chunks(size - 1).chunkid();

    // prefix
    Prefix4ChunkIndexS3ChunkInfoList prefix(fsId, inodeId, chunkIndex);
    std::string sprefix = conv_->SerializeToString(prefix);
    auto iterator = txn->SSeek(table4s3chunkinfo_, sprefix);
    if (iterator->Status() != 0) {
        return MetaStatusCode::STORAGE_INTERNAL_ERROR;
    }

    Key4S3ChunkInfoList key;
    std::vector<std::string> key2del;
    std::vector<S3ChunkInfoList> lists2add;
    for (iterator->SeekToFirst(); iterator->Valid(); iterator->Next()) {
        std::string skey = iterator->Key();
        if (!StringStartWith(skey, sprefix)) {
            break;
        } else if (!conv_->ParseFromString(skey, &key)) {
            return MetaStatusCode::PARSE_FROM_STRING_FAILED;
        }

        // delete list range :  [     ]
        // current list range:   [  ]
        if (delFirstChunkId <= key.firstChunkId &&
            delLastChunkId >= key.lastChunkId) {
            key2del.push_back(skey);
        // delete list range :    [   ]
        // current list range:  [  ]
        } else if (delFirstChunkId >= key.firstChunkId &&
            delFirstChunkId <= key.lastChunkId &&
            delLastChunkId >= key.lastChunkId) {
            S3ChunkInfoList list;
            bool succ = SplitS3ChunkInfoList(
                iterator->Value(), delFirstChunkId, &list);
            if (!succ) {
                return MetaStatusCode::STORAGE_INTERNAL_ERROR;
            }
            key2del.push_back(skey);
            lists2add.push_back(list);
        // delete list range :        [  ]
        // current list range:  [  ]
        }  else if (delFirstChunkId > key.lastChunkId) {
            continue;
        // delete list range :  [  ]
        // current list range:       [  ]
        } else if (delLastChunkId < key.firstChunkId) {
            continue;
        } else {
            LOG(ERROR) << "wrong delete list range (" << delFirstChunkId
                       << "," << delLastChunkId << "), skey=" << skey;
            return MetaStatusCode::STORAGE_INTERNAL_ERROR;
        }
    }

    for (const auto& list : lists2add) {
        MetaStatusCode rc = AddS3ChunkInfoList(
            txn, fsId, inodeId, chunkIndex, list);
        if (rc != MetaStatusCode::OK) {
            return rc;
        }
    }

    for (const auto& skey : key2del) {
        if (!txn->SDel(table4s3chunkinfo_, skey).ok()) {
            return MetaStatusCode::STORAGE_INTERNAL_ERROR;
        }
    }
    return MetaStatusCode::OK;
}

MetaStatusCode InodeStorage::ModifyInodeS3ChunkInfoList(
    uint32_t fsId,
    uint64_t inodeId,
    uint64_t chunkIndex,
    const S3ChunkInfoList& list2add,
    const S3ChunkInfoList& list2del) {
    WriteLockGuard writeLockGuard(rwLock_);
    auto txn = kvStorage_->BeginTransaction();
    if (nullptr == txn) {
        return MetaStatusCode::STORAGE_INTERNAL_ERROR;
    }

    auto rc = AddS3ChunkInfoList(txn, fsId, inodeId, chunkIndex, list2add);
    if (rc == MetaStatusCode::OK) {
        rc = DelS3ChunkInfoList(txn, fsId, inodeId, chunkIndex, list2del);
    }

    if (rc != MetaStatusCode::OK) {
        txn->Rollback();
    } else if (!txn->Commit().ok()) {
        rc = MetaStatusCode::STORAGE_INTERNAL_ERROR;
    }

    if (rc != MetaStatusCode::OK) {
        return rc;
    }

    // rc == MetaStatusCode::OK
    uint64_t size4add = list2add.s3chunks_size();
    uint64_t size4del = list2del.s3chunks_size();
    if (!UpdateInodeS3MetaSize(fsId, inodeId, size4add, size4del)) {
        return MetaStatusCode::STORAGE_INTERNAL_ERROR;
    }
    return rc;
}

MetaStatusCode InodeStorage::PaddingInodeS3ChunkInfo(int32_t fsId,
                                                     uint64_t inodeId,
                                                     S3ChunkInfoMap* m,
                                                     uint64_t limit) {
    ReadLockGuard readLockGuard(rwLock_);
    if (limit != 0 && GetInodeS3MetaSize(fsId, inodeId) > limit) {
        return MetaStatusCode::INODE_S3_META_TOO_LARGE;
    }

    auto iterator = GetInodeS3ChunkInfoList(fsId, inodeId);
    if (iterator->Status() != 0) {
        LOG(ERROR) << "Get inode s3chunkinfo failed";
        return MetaStatusCode::STORAGE_INTERNAL_ERROR;
    }

    auto merge = [](const S3ChunkInfoList& from, S3ChunkInfoList* to) {
        for (int i = 0; i < from.s3chunks_size(); i++) {
            auto chunkinfo = to->add_s3chunks();
            chunkinfo->CopyFrom(from.s3chunks(i));
        }
    };

    Key4S3ChunkInfoList key;
    S3ChunkInfoList list;
    for (iterator->SeekToFirst(); iterator->Valid(); iterator->Next()) {
        std::string skey = iterator->Key();
        std::string svalue = iterator->Value();
        if (!conv_->ParseFromString(skey, &key)) {
            return MetaStatusCode::PARSE_FROM_STRING_FAILED;
        } else if (!iterator->ParseFromValue(&list)) {
            return MetaStatusCode::PARSE_FROM_STRING_FAILED;
        }

        uint64_t chunkIndex = key.chunkIndex;
        auto iter = m->find(chunkIndex);
        if (iter == m->end()) {
            m->insert({chunkIndex, list});
        } else {
            merge(list, &iter->second);
        }
    }

    return MetaStatusCode::OK;
}

std::shared_ptr<Iterator> InodeStorage::GetInodeS3ChunkInfoList(
    uint32_t fsId, uint64_t inodeId) {
    ReadLockGuard readLockGuard(rwLock_);
    Prefix4InodeS3ChunkInfoList prefix(fsId, inodeId);
    std::string sprefix = conv_->SerializeToString(prefix);
    return kvStorage_->SSeek(table4s3chunkinfo_, sprefix);
}

std::shared_ptr<Iterator> InodeStorage::GetAllS3ChunkInfoList() {
    ReadLockGuard readLockGuard(rwLock_);
    return kvStorage_->SGetAll(table4s3chunkinfo_);
}

std::shared_ptr<Iterator> InodeStorage::GetAllInode() {
    ReadLockGuard readLockGuard(rwLock_);
    std::string sprefix = conv_->SerializeToString(Prefix4AllInode());
    return kvStorage_->HGetAll(table4inode_);
}

size_t InodeStorage::Size() {
    return kvStorage_->HSize(table4inode_);
}

MetaStatusCode InodeStorage::Clear() {
    ReadLockGuard w(rwLock_);
    Status s = kvStorage_->HClear(table4inode_);
    if (!s.ok()) {
        LOG(ERROR) << "InodeStorage HClear() failed";
        return MetaStatusCode::STORAGE_INTERNAL_ERROR;
    }
    s = kvStorage_->SClear(table4s3chunkinfo_);
    if (!s.ok()) {
        LOG(ERROR) << "InodeStorage SClear() failed";
        return MetaStatusCode::STORAGE_INTERNAL_ERROR;
    }

    keySet_.clear();
    return MetaStatusCode::OK;
}

bool InodeStorage::GetInodeIdList(std::list<uint64_t>* inodeIdList) {
    ReadLockGuard readLockGuard(rwLock_);
    Key4Inode key;
    for (const auto& skey : keySet_) {
        if (!conv_->ParseFromString(skey, &key)) {
            return false;
        }
        inodeIdList->push_back(key.inodeId);
    }
    return true;
}

}  // namespace metaserver
}  // namespace curvefs
