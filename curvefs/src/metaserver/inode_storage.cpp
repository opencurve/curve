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

#include "curvefs/src/metaserver/inode_storage.h"

#include <google/protobuf/empty.pb.h>

#include <algorithm>
#include <limits>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "curvefs/proto/common.pb.h"
#include "curvefs/proto/metaserver.pb.h"
#include "curvefs/src/metaserver/common/types.h"
#include "curvefs/src/metaserver/storage/converter.h"
#include "curvefs/src/metaserver/storage/status.h"
#include "curvefs/src/metaserver/storage/storage.h"
#include "src/common/concurrent/rw_lock.h"
#include "src/common/string_util.h"

namespace curvefs {
namespace metaserver {

using ::curve::common::ReadLockGuard;
using ::curve::common::StringStartWith;
using ::curve::common::WriteLockGuard;
using ::curvefs::metaserver::storage::Key4DeallocatableBlockGroup;
using ::curvefs::metaserver::storage::Key4InodeAuxInfo;
using ::curvefs::metaserver::storage::Key4S3ChunkInfoList;
using ::curvefs::metaserver::storage::Key4VolumeExtentSlice;
using ::curvefs::metaserver::storage::KVStorage;
using ::curvefs::metaserver::storage::Prefix4AllDeallocatableBlockGroup;
using ::curvefs::metaserver::storage::Prefix4AllInode;
using ::curvefs::metaserver::storage::Prefix4ChunkIndexS3ChunkInfoList;
using ::curvefs::metaserver::storage::Prefix4InodeS3ChunkInfoList;
using ::curvefs::metaserver::storage::Prefix4InodeVolumeExtent;
using ::curvefs::metaserver::storage::Status;

const char* InodeStorage::kInodeCountKey("count");

const char* InodeStorage::kInodeAppliedKey("inode");

InodeStorage::InodeStorage(std::shared_ptr<KVStorage> kvStorage,
                           std::shared_ptr<NameGenerator> nameGenerator,
                           uint64_t nInode)
    : kvStorage_(std::move(kvStorage)),
      table4Inode_(nameGenerator->GetInodeTableName()),
      table4S3ChunkInfo_(nameGenerator->GetS3ChunkInfoTableName()),
      table4VolumeExtent_(nameGenerator->GetVolumeExtentTableName()),
      table4InodeAuxInfo_(nameGenerator->GetInodeAuxInfoTableName()),
      table4DeallocatableInode_(
          nameGenerator->GetDeallocatableInodeTableName()),
      table4DeallocatableBlockGroup_(
          nameGenerator->GetDeallocatableBlockGroupTableName()),
      table4AppliedIndex_(nameGenerator->GetAppliedIndexTableName()),
      table4InodeCount_(nameGenerator->GetInodeCountTableName()),
      nInode_(nInode),
      conv_() {
    // NOTE: for compatibility with older versions
    // we cannot ignore `nInode` argument
}

bool InodeStorage::Init() {
    // try get inode count for rocksdb
    // if we got it, replace old value
    auto s = GetInodeCount(&nInode_);
    return s.ok() || s.IsNotFound();
}

storage::Status InodeStorage::GetInodeCount(std::size_t* count) {
    common::ItemCount val;
    auto s = kvStorage_->SGet(table4InodeCount_, kInodeCountKey, &val);
    if (s.ok()) {
        *count = static_cast<std::size_t>(val.count());
    }
    return s;
}

storage::Status InodeStorage::SetInodeCount(storage::StorageTransaction* txn,
                                            std::size_t count) {
    common::ItemCount val;
    val.set_count(count);
    return txn->SSet(table4InodeCount_, kInodeCountKey, val);
}

storage::Status InodeStorage::DelInodeCount(storage::StorageTransaction* txn) {
    return txn->SDel(table4InodeCount_, kInodeCountKey);
}

storage::Status InodeStorage::SetAppliedIndex(
    storage::StorageTransaction* transaction, int64_t index) {
    common::AppliedIndex val;
    val.set_index(index);
    return transaction->SSet(table4AppliedIndex_, kInodeAppliedKey, val);
}

storage::Status InodeStorage::DelAppliedIndex(
    storage::StorageTransaction* transaction) {
    return transaction->SDel(table4AppliedIndex_, kInodeAppliedKey);
}

MetaStatusCode InodeStorage::GetAppliedIndex(int64_t* index) {
    common::AppliedIndex val;
    auto s = kvStorage_->SGet(table4AppliedIndex_, kInodeAppliedKey, &val);
    if (s.ok()) {
        *index = val.index();
        return MetaStatusCode::OK;
    }
    if (s.IsNotFound()) {
        return MetaStatusCode::NOT_FOUND;
    }
    return MetaStatusCode::STORAGE_INTERNAL_ERROR;
}

MetaStatusCode InodeStorage::Insert(const Inode& inode, int64_t logIndex) {
    WriteLockGuard lg(rwLock_);
    Key4Inode key(inode.fsid(), inode.inodeid());
    std::string skey = conv_.SerializeToString(key);
    // NOTE: HGet() is cheap, because the key not found in most cases,
    // so the rocksdb storage only should check bloom filter.
    Inode out;
    Status s = kvStorage_->HGet(table4Inode_, skey, &out);
    if (s.ok()) {
        return MetaStatusCode::INODE_EXIST;
    } else if (!s.IsNotFound()) {
        return MetaStatusCode::STORAGE_INTERNAL_ERROR;
    }
    // key not found
    const char* step = "Begin transaction";
    std::shared_ptr<storage::StorageTransaction> txn;
    do {
        txn = kvStorage_->BeginTransaction();
        if (txn == nullptr) {
            break;
        }
        s = txn->HSet(table4Inode_, skey, inode);
        if (!s.ok()) {
            step = "Insert inode to transaction";
            break;
        }
        s = SetAppliedIndex(txn.get(), logIndex);
        if (!s.ok()) {
            step = "Insert applied index to transaction";
            break;
        }
        s = SetInodeCount(txn.get(), nInode_ + 1);
        if (!s.ok()) {
            step = "Insert inode count to transaction";
            break;
        }
        s = txn->Commit();
        if (!s.ok()) {
            step = "Insert inode failed";
            break;
        }
        nInode_++;
        return MetaStatusCode::OK;
    } while (false);
    LOG(ERROR) << step << " failed, status = " << s.ToString();
    if (txn != nullptr && !txn->Rollback().ok()) {
        LOG(ERROR) << "Rollback transaction failed";
    }
    return MetaStatusCode::STORAGE_INTERNAL_ERROR;
}

MetaStatusCode InodeStorage::UpdateDeletingKey(const Inode& inode, int64_t logIndex) {
    WriteLockGuard lg(rwLock_);
    Key4Inode key(inode.fsid(), inode.inodeid());
    std::string skey = conv_.SerializeToString(key);
    VLOG(9) << "update deleting key, " << inode.inodeid();
    const char* step = "Begin transaction";
    std::shared_ptr<storage::StorageTransaction> txn;
    txn = kvStorage_->BeginTransaction();
    if (txn == nullptr) {
        LOG(ERROR) << "Begin transaction failed";
        return MetaStatusCode::STORAGE_INTERNAL_ERROR;
    }
    auto rc = txn->HSetDeleting(table4Inode_, skey , inode);
    step = "insert inode ";
    if (rc.ok()) {
        // delete key
       // rc = DeleteInternal(txn.get(), key);
       rc = txn->HDel(table4Inode_, skey);
        step = "delete inode ";
    }
     if (rc.ok()) {
        rc = SetAppliedIndex(txn.get(), logIndex);
        step = "Insert applied index to transaction";
    }
    if (rc.ok()) {
        rc = txn->Commit();
        step = "commit";
    }
    if (rc.ok()) {
        VLOG(0) << "update deleting key ok, " << inode.inodeid();
        return MetaStatusCode::OK;
    }
    LOG(ERROR) << step << "failed, status = " << rc.ToString();
    if (txn != nullptr && !txn->Rollback().ok()) {
        LOG(ERROR) << "Rollback delete inode transaction failed, status = "
                   << rc.ToString();
    }
    return MetaStatusCode::STORAGE_INTERNAL_ERROR;
}

MetaStatusCode InodeStorage::Get(const Key4Inode& key, Inode* inode) {
    ReadLockGuard lg(rwLock_);
    std::string skey = conv_.SerializeToString(key);
    Status s = kvStorage_->HGet(table4Inode_, skey, inode);
    if (s.ok()) {
        return MetaStatusCode::OK;
    } else if (s.IsNotFound()) {
        return MetaStatusCode::NOT_FOUND;
    } else if (s.IsDBClosed()) {
        return MetaStatusCode::STORAGE_CLOSED;
    }

    LOG(ERROR) << "Get inode failed, status = " << s.ToString();
    return MetaStatusCode::STORAGE_INTERNAL_ERROR;
}

MetaStatusCode InodeStorage::GetAttr(const Key4Inode& key, InodeAttr* attr) {
    ReadLockGuard lg(rwLock_);
    Inode inode;
    std::string skey = conv_.SerializeToString(key);
    Status s = kvStorage_->HGet(table4Inode_, skey, &inode);
    if (s.IsNotFound()) {
        return MetaStatusCode::NOT_FOUND;
    } else if (!s.ok()) {
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
    *(attr->mutable_parent()) = inode.parent();
    if (inode.has_symlink()) {
        attr->set_symlink(inode.symlink());
    }
    if (inode.has_rdev()) {
        attr->set_rdev(inode.rdev());
    }
    if (inode.has_dtime()) {
        attr->set_dtime(inode.dtime());
    }
    if (inode.xattr_size() > 0) {
        *(attr->mutable_xattr()) = inode.xattr();
    }
    return MetaStatusCode::OK;
}

MetaStatusCode InodeStorage::GetXAttr(const Key4Inode& key, XAttr* xattr) {
    ReadLockGuard lg(rwLock_);
    Inode inode;
    std::string skey = conv_.SerializeToString(key);
    Status s = kvStorage_->HGet(table4Inode_, skey, &inode);
    if (s.IsNotFound()) {
        return MetaStatusCode::NOT_FOUND;
    } else if (!s.ok()) {
        return MetaStatusCode::STORAGE_INTERNAL_ERROR;
    }

    if (!inode.xattr().empty()) {
        *(xattr->mutable_xattrinfos()) = inode.xattr();
    }
    return MetaStatusCode::OK;
}

// NOTE: if transaction success
// we will commit transaction
// it should be the last step of your operations
storage::Status InodeStorage::DeleteInternal(
    storage::StorageTransaction* transaction, const Key4Inode& key) {
    std::string skey = conv_.SerializeToString(key);
    Status s;
    const char* step = "Delete inode from transaction";
    do {
        s = transaction->HDel(table4Inode_, skey);
        if (!s.ok()) {
            break;
        }
        // NOTE: for rocksdb storage, it will never check whether
        // the key exist in delete(), so if the client delete the
        // non-exist inode in some abnormal cases, it will cause the
        // nInode less then the real value.
        if (nInode_ > 0) {
            s = SetInodeCount(transaction, nInode_ - 1);
            if (!s.ok()) {
                step = "Insert inode count to transaction";
                break;
            }
        }
        s = transaction->Commit();
        if (!s.ok()) {
            step = "Delete inode";
            break;
        }
        // NOTE: for rocksdb storage, it will never check whether
        // the key exist in delete(), so if the client delete the
        // non-exist inode in some abnormal cases, it will cause the
        // nInode less then the real value.
        if (nInode_ > 0) {
            nInode_--;
        }
        return s;
    } while (false);
    LOG(ERROR) << step << " failed, status = " << s.ToString();
    return s;
}

MetaStatusCode InodeStorage::Delete(const Key4Inode& key, int64_t logIndex) {
    WriteLockGuard lg(rwLock_);
    std::shared_ptr<storage::StorageTransaction> txn;
    Status s;
    const char* step = "Begin transaction";
    do {
        txn = kvStorage_->BeginTransaction();
        if (txn == nullptr) {
            break;
        }
        s = SetAppliedIndex(txn.get(), logIndex);
        if (!s.ok()) {
            step = "Insert applied index to transaction";
            break;
        }
        s = DeleteInternal(txn.get(), key);
        if (!s.ok()) {
            step = "Delete inode from transaction";
            break;
        }
        return MetaStatusCode::OK;
    } while (false);
    LOG(ERROR) << step << " failed, status = " << s.ToString();
    if (txn != nullptr && !txn->Rollback().ok()) {
        LOG(ERROR) << "Rollback delete inode transaction failed, status = "
                   << s.ToString();
    }
    return MetaStatusCode::STORAGE_INTERNAL_ERROR;
}

MetaStatusCode InodeStorage::ForceDelete(const Key4Inode& key) {
    WriteLockGuard lg(rwLock_);
    std::shared_ptr<storage::StorageTransaction> txn = nullptr;
    Status s;
    const char* step = "Begin transaction";
    do {
        txn = kvStorage_->BeginTransaction();
        if (txn == nullptr) {
            break;
        }
        s = DeleteInternal(txn.get(), key);
        if (!s.ok()) {
            step = "Delete inode from transaction";
            break;
        }
        return MetaStatusCode::OK;
    } while (false);
    LOG(ERROR) << step << " failed, status = " << s.ToString();
    if (txn != nullptr && !txn->Rollback().ok()) {
        LOG(ERROR) << "Rollback delete inode transaction failed, status = "
                   << s.ToString();
    }
    return MetaStatusCode::STORAGE_INTERNAL_ERROR;
}

MetaStatusCode InodeStorage::Update(
    std::shared_ptr<storage::StorageTransaction>* txn, const Inode& inode,
    int64_t logIndex, bool inodeDeallocate) {
    if (*txn == nullptr) {
        *txn = kvStorage_->BeginTransaction();
        if (*txn == nullptr) {
            LOG(ERROR) << "Begin transaction failed";
            return MetaStatusCode::STORAGE_INTERNAL_ERROR;
        }
    }
    WriteLockGuard lg(rwLock_);
    Key4Inode key(inode.fsid(), inode.inodeid());
    std::string skey = conv_.SerializeToString(key);
    storage::Status s;
    s = SetAppliedIndex(txn->get(), logIndex);
    if (!s.ok()) {
        LOG(ERROR) << "Insert applied index to transaction failed, status = "
                   << s.ToString();
        return MetaStatusCode::STORAGE_INTERNAL_ERROR;
    }
    if (!inodeDeallocate) {
        s = (*txn)->HSet(table4Inode_, skey, inode);
        if (s.ok()) {
            return MetaStatusCode::OK;
        }
        return MetaStatusCode::STORAGE_INTERNAL_ERROR;
    }
    google::protobuf::Empty value;
    std::string step = "update inode " + key.SerializeToString();
    s = (*txn)->HSet(table4Inode_, skey, inode);
    if (s.ok()) {
        s = (*txn)->HSet(table4DeallocatableInode_, skey, value);
        step = "add inode " + key.SerializeToString() +
               " to inode deallocatable list";
    }
    if (!s.ok()) {
        LOG(ERROR) << "txn is failed in " << step;
        return MetaStatusCode::STORAGE_INTERNAL_ERROR;
    }
    return MetaStatusCode::OK;
}

MetaStatusCode InodeStorage::Update(const Inode& inode, int64_t logIndex,
                                    bool inodeDeallocate) {
    std::shared_ptr<storage::StorageTransaction> txn;
    if (Update(&txn, inode, logIndex, inodeDeallocate) == MetaStatusCode::OK) {
        storage::Status s = txn->Commit();
        if (!s.ok()) {
            LOG(ERROR) << "Commit update inode transaction failed, status = "
                       << s.ToString();
        } else {
            return MetaStatusCode::OK;
        }
    }
    if (txn != nullptr && !txn->Rollback().ok()) {
        LOG(ERROR) << "Rollback transaction failed";
    }
    LOG(ERROR) << "Update inode failed";
    return MetaStatusCode::STORAGE_INTERNAL_ERROR;
}

std::shared_ptr<Iterator> InodeStorage::GetAllInode() {
    ReadLockGuard lg(rwLock_);
    std::string sprefix = conv_.SerializeToString(Prefix4AllInode());
    return kvStorage_->HGetAll(table4Inode_);
}

bool InodeStorage::GetAllInodeId(std::list<uint64_t>* ids) {
    ReadLockGuard lg(rwLock_);
    auto iterator = GetAllInode();
    if (iterator->Status() != 0) {
        LOG(ERROR) << "failed to get iterator for all inode";
        return false;
    }

    Key4Inode key;
    for (iterator->SeekToFirst(); iterator->Valid(); iterator->Next()) {
        if (!conv_.ParseFromString(iterator->Key(), &key)) {
            return false;
        }
        ids->push_back(key.inodeId);
    }
    return true;
}

size_t InodeStorage::Size() {
    ReadLockGuard lg(rwLock_);
    return nInode_;
}

bool InodeStorage::Empty() {
    ReadLockGuard lg(rwLock_);
    auto iterator = GetAllInode();
    if (iterator->Status() != 0) {
        LOG(ERROR) << "failed to get iterator for all inode";
        return false;
    }

    for (iterator->SeekToFirst(); iterator->Valid(); iterator->Next()) {
        return false;
    }
    return true;
}

// NOTE: we will clear all apply metadata in `Clear()`
// when follower replay logs on this snapshot, it may cause
// repeat apply log entries, and raise some errors
// but we know this partition will be clear at the end of logs
MetaStatusCode InodeStorage::Clear() {
    // FIXME: non-atomic clear operations
    // NOTE: clear operations non-atomic is acceptable
    // because if we fail stop, we will replay
    // raft logs and clear it again
    WriteLockGuard lg(rwLock_);
    Status s = kvStorage_->HClear(table4Inode_);
    if (!s.ok()) {
        LOG(ERROR) << "InodeStorage clear inode table failed, status = "
                   << s.ToString();
        return MetaStatusCode::STORAGE_INTERNAL_ERROR;
    }
    s = kvStorage_->SClear(table4S3ChunkInfo_);
    if (!s.ok()) {
        LOG(ERROR)
            << "InodeStorage clear inode s3chunkinfo table failed, status = "
            << s.ToString();
        return MetaStatusCode::STORAGE_INTERNAL_ERROR;
    }
    s = kvStorage_->SClear(table4VolumeExtent_);
    if (!s.ok()) {
        LOG(ERROR)
            << "InodeStorage clear inode volume extent table failed, status = "
            << s.ToString();
        return MetaStatusCode::STORAGE_INTERNAL_ERROR;
    }
    s = kvStorage_->HClear(table4InodeAuxInfo_);
    if (!s.ok()) {
        LOG(ERROR)
            << "InodeStorage clear inode aux info table failed, status = "
            << s.ToString();
        return MetaStatusCode::STORAGE_INTERNAL_ERROR;
    }
    std::shared_ptr<storage::StorageTransaction> txn;
    const char* step = "Begin transaction";
    do {
        txn = kvStorage_->BeginTransaction();
        if (txn == nullptr) {
            break;
        }
        s = DelInodeCount(txn.get());
        if (!s.ok()) {
            step = "Delete inode count";
            break;
        }
        s = DelAppliedIndex(txn.get());
        if (!s.ok()) {
            step = "Delete applied index";
            break;
        }
        s = txn->Commit();
        if (!s.ok()) {
            step = "Commit clear InodeStorage transaction";
            break;
        }
        nInode_ = 0;
        return MetaStatusCode::OK;
    } while (false);
    LOG(ERROR) << step << " failed, status = " << s.ToString();
    if (txn != nullptr && !txn->Rollback().ok()) {
        LOG(ERROR) << "Rollback transaction failed";
    }
    return MetaStatusCode::STORAGE_INTERNAL_ERROR;
}

MetaStatusCode InodeStorage::UpdateInodeS3MetaSize(Transaction txn,
                                                   uint32_t fsId,
                                                   uint64_t inodeId,
                                                   uint64_t size4add,
                                                   uint64_t size4del) {
    uint64_t size = 0;
    InodeAuxInfo out;
    Key4InodeAuxInfo key(fsId, inodeId);
    std::string skey = key.SerializeToString();
    Status s = txn->HGet(table4InodeAuxInfo_, skey, &out);
    if (s.ok()) {
        size = out.s3metasize();
    } else if (s.IsNotFound()) {
        size = 0;
    } else {
        LOG(ERROR) << "failed to get inode s3 meta size, status="
                   << s.ToString();
        return MetaStatusCode::STORAGE_INTERNAL_ERROR;
    }

    size += size4add;
    if (size < size4del) {
        LOG(ERROR) << "current inode s3 meta size is " << size << ", less than "
                   << size4del;
        return MetaStatusCode::STORAGE_INTERNAL_ERROR;
    }

    out.set_s3metasize(size - size4del);
    s = txn->HSet(table4InodeAuxInfo_, skey, out);
    if (!s.ok()) {
        LOG(ERROR) << "failed to set inode s3 meta size, status="
                   << s.ToString();
        return MetaStatusCode::STORAGE_INTERNAL_ERROR;
    }
    return MetaStatusCode::OK;
}

uint64_t InodeStorage::GetInodeS3MetaSize(uint32_t fsId, uint64_t inodeId) {
    InodeAuxInfo out;
    uint64_t size = std::numeric_limits<uint64_t>::max();
    Key4InodeAuxInfo key(fsId, inodeId);
    std::string skey = key.SerializeToString();

    Status s = kvStorage_->HGet(table4InodeAuxInfo_, skey, &out);
    if (s.ok()) {
        size = out.s3metasize();
    } else if (s.IsNotFound()) {
        size = 0;
    } else {
        LOG(ERROR) << "failed to get inode s3 meta size, status="
                   << s.ToString();
    }
    return size;
}

MetaStatusCode InodeStorage::AddS3ChunkInfoList(
    Transaction txn, uint32_t fsId, uint64_t inodeId, uint64_t chunkIndex,
    const S3ChunkInfoList* list2add) {
    if (nullptr == list2add || list2add->s3chunks_size() == 0) {
        return MetaStatusCode::OK;
    }

    size_t size = list2add->s3chunks_size();
    uint64_t firstChunkId = list2add->s3chunks(0).chunkid();
    uint64_t lastChunkId = list2add->s3chunks(size - 1).chunkid();

    Key4S3ChunkInfoList key(fsId, inodeId, chunkIndex, firstChunkId,
                            lastChunkId, size);
    std::string skey = conv_.SerializeToString(key);
    Status s;
    if (txn) {
        s = txn->SSet(table4S3ChunkInfo_, skey, *list2add);
    } else {
        s = kvStorage_->SSet(table4S3ChunkInfo_, skey, *list2add);
    }
    return s.ok() ? MetaStatusCode::OK : MetaStatusCode::STORAGE_INTERNAL_ERROR;
}

MetaStatusCode InodeStorage::DelS3ChunkInfoList(
    Transaction txn, uint32_t fsId, uint64_t inodeId, uint64_t chunkIndex,
    const S3ChunkInfoList* list2del) {
    if (nullptr == list2del || list2del->s3chunks_size() == 0) {
        return MetaStatusCode::OK;
    }

    size_t size = list2del->s3chunks_size();
    uint64_t delFirstChunkId = list2del->s3chunks(0).chunkid();
    uint64_t delLastChunkId = list2del->s3chunks(size - 1).chunkid();

    // prefix
    Prefix4ChunkIndexS3ChunkInfoList prefix(fsId, inodeId, chunkIndex);
    std::string sprefix = conv_.SerializeToString(prefix);
    auto iterator = txn->SSeek(table4S3ChunkInfo_, sprefix);
    if (iterator->Status() != 0) {
        LOG(ERROR) << "Get iterator failed, prefix=" << sprefix;
        return MetaStatusCode::STORAGE_INTERNAL_ERROR;
    }

    Key4S3ChunkInfoList key;
    std::vector<std::string> key2del;
    for (iterator->SeekToFirst(); iterator->Valid(); iterator->Next()) {
        std::string skey = iterator->Key();
        if (!StringStartWith(skey, sprefix)) {
            break;
        } else if (!conv_.ParseFromString(skey, &key)) {
            return MetaStatusCode::PARSE_FROM_STRING_FAILED;
        }

        // current list range:    [  ]
        // delete list range :  [      ]
        if (delFirstChunkId <= key.firstChunkId &&
            delLastChunkId >= key.lastChunkId) {
            key2del.push_back(skey);
            // current list range:       [  ]
            // delete list range :  [  ]
        } else if (delLastChunkId < key.firstChunkId) {
            continue;
        } else {
            LOG(ERROR) << "wrong delete list range (" << delFirstChunkId << ","
                       << delLastChunkId << "), skey=" << skey;
            return MetaStatusCode::STORAGE_INTERNAL_ERROR;
        }
    }

    for (const auto& skey : key2del) {
        if (!txn->SDel(table4S3ChunkInfo_, skey).ok()) {
            LOG(ERROR) << "Delete key failed, skey=" << skey;
            return MetaStatusCode::STORAGE_INTERNAL_ERROR;
        }
    }
    return MetaStatusCode::OK;
}

MetaStatusCode InodeStorage::ModifyInodeS3ChunkInfoList(
    std::shared_ptr<StorageTransaction>* txn, uint32_t fsId, uint64_t inodeId,
    uint64_t chunkIndex, const S3ChunkInfoList* list2add,
    const S3ChunkInfoList* list2del, int64_t logIndex) {
    if (*txn == nullptr) {
        *txn = kvStorage_->BeginTransaction();
        if (*txn == nullptr) {
            LOG(ERROR) << "Begin transaction failed";
            return MetaStatusCode::STORAGE_INTERNAL_ERROR;
        }
    }
    WriteLockGuard lg(rwLock_);
    std::string step;
    auto rc = DelS3ChunkInfoList(*txn, fsId, inodeId, chunkIndex, list2del);
    step = "del s3 chunkinfo list ";
    if (rc == MetaStatusCode::OK) {
        rc = AddS3ChunkInfoList(*txn, fsId, inodeId, chunkIndex, list2add);
        step = "add s3 chunkInfo list ";
    }

    if (rc == MetaStatusCode::OK) {
        uint64_t size4add =
            (nullptr == list2add) ? 0 : list2add->s3chunks_size();
        uint64_t size4del =
            (nullptr == list2del) ? 0 : list2del->s3chunks_size();
        // TODO(huyao): I don't think this place is idempotent. If the timeout
        // is retried, the size will increase.
        rc = UpdateInodeS3MetaSize(*txn, fsId, inodeId, size4add, size4del);
        step = "update inode s3 meta size ";
    }
    if (rc == MetaStatusCode::OK) {
        if (!SetAppliedIndex(txn->get(), logIndex).ok()) {
            step = "Insert applied index";
            rc = MetaStatusCode::STORAGE_INTERNAL_ERROR;
        }
    }
    if (rc != MetaStatusCode::OK) {
        LOG(ERROR) << "Modify inode transaction failed, step = " << step;
    }
    return rc;
}

MetaStatusCode InodeStorage::ModifyInodeS3ChunkInfoList(
    uint32_t fsId, uint64_t inodeId, uint64_t chunkIndex,
    const S3ChunkInfoList* list2add, const S3ChunkInfoList* list2del,
    int64_t logIndex) {
    std::shared_ptr<storage::StorageTransaction> txn;
    MetaStatusCode rc = ModifyInodeS3ChunkInfoList(
        &txn, fsId, inodeId, chunkIndex, list2add, list2del, logIndex);
    if (rc != MetaStatusCode::OK) {
        if (txn != nullptr && !txn->Rollback().ok()) {
            LOG(ERROR) << "Rollback transaction failed";
            rc = MetaStatusCode::STORAGE_INTERNAL_ERROR;
        }
        LOG(ERROR) << "Modify inode failed";
    } else if (!txn->Commit().ok()) {
        LOG(ERROR) << "Commit transaction failed";
        rc = MetaStatusCode::STORAGE_INTERNAL_ERROR;
    }
    return rc;
}

MetaStatusCode InodeStorage::PaddingInodeS3ChunkInfo(int32_t fsId,
                                                     uint64_t inodeId,
                                                     S3ChunkInfoMap* m,
                                                     uint64_t limit) {
    ReadLockGuard lg(rwLock_);
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
        if (!conv_.ParseFromString(skey, &key)) {
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
    ReadLockGuard lg(rwLock_);
    Prefix4InodeS3ChunkInfoList prefix(fsId, inodeId);
    std::string sprefix = conv_.SerializeToString(prefix);
    return kvStorage_->SSeek(table4S3ChunkInfo_, sprefix);
}

std::shared_ptr<Iterator> InodeStorage::GetAllS3ChunkInfoList() {
    ReadLockGuard lg(rwLock_);
    return kvStorage_->SGetAll(table4S3ChunkInfo_);
}

std::shared_ptr<Iterator> InodeStorage::GetAllVolumeExtentList() {
    ReadLockGuard guard(rwLock_);
    return kvStorage_->SGetAll(table4VolumeExtent_);
}

MetaStatusCode InodeStorage::UpdateVolumeExtentSlice(
    std::shared_ptr<storage::StorageTransaction>* txn, uint32_t fsId,
    uint64_t inodeId, const VolumeExtentSlice& slice, int64_t logIndex) {
    WriteLockGuard guard(rwLock_);
    if (*txn == nullptr) {
        *txn = kvStorage_->BeginTransaction();
        if (*txn == nullptr) {
            LOG(ERROR) << "Begin transaction failed";
            return MetaStatusCode::STORAGE_INTERNAL_ERROR;
        }
    }
    auto key = conv_.SerializeToString(
        Key4VolumeExtentSlice{fsId, inodeId, slice.offset()});
    auto st = (*txn)->SSet(table4VolumeExtent_, key, slice);
    if (!st.ok()) {
        LOG(ERROR)
            << "Update volume extent slice to transaction failed, status = "
            << st.ToString();
        return MetaStatusCode::STORAGE_INTERNAL_ERROR;
    }
    st = SetAppliedIndex(txn->get(), logIndex);
    if (!st.ok()) {
        LOG(ERROR) << "Insert applied index to transaction failed, status = "
                   << st.ToString();
        return MetaStatusCode::STORAGE_INTERNAL_ERROR;
    }
    return MetaStatusCode::OK;
}

MetaStatusCode InodeStorage::UpdateVolumeExtentSlice(
    uint32_t fsId, uint64_t inodeId, const VolumeExtentSlice& slice,
    int64_t logIndex) {
    std::shared_ptr<storage::StorageTransaction> txn;
    auto rc = UpdateVolumeExtentSlice(&txn, fsId, inodeId, slice, logIndex);
    if (rc != MetaStatusCode::OK) {
        if (txn != nullptr && !txn->Rollback().ok()) {
            LOG(ERROR) << "Rollback transaction failed";
        }
        return rc;
    }
    auto s = txn->Commit();
    if (!s.ok()) {
        LOG(ERROR) << "Commit transaction failed, status = " << s.ToString();
        if (!txn->Rollback().ok()) {
            LOG(ERROR) << "Rollback transaction failed";
        }
        return MetaStatusCode::STORAGE_INTERNAL_ERROR;
    }
    return MetaStatusCode::OK;
}

MetaStatusCode InodeStorage::GetAllVolumeExtent(
    uint32_t fsId, uint64_t inodeId, VolumeExtentSliceList* extents) {
    ReadLockGuard guard(rwLock_);
    auto key = conv_.SerializeToString(Prefix4InodeVolumeExtent{fsId, inodeId});
    auto iter = kvStorage_->SSeek(table4VolumeExtent_, key);

    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        auto* slice = extents->add_slices();
        if (iter->RawValue()) {
            slice->CopyFrom(*iter->RawValue());
            continue;
        }

        if (!slice->ParseFromString(iter->Value())) {
            LOG(ERROR) << "Parse ExtentSlice failed, fsId: " << fsId
                       << ", inodeId: " << inodeId;
            extents->Clear();
            return MetaStatusCode::STORAGE_INTERNAL_ERROR;
        }
    }

    return MetaStatusCode::OK;
}

std::shared_ptr<Iterator> InodeStorage::GetAllVolumeExtent(uint32_t fsId,
                                                           uint64_t inodeId) {
    ReadLockGuard guard(rwLock_);
    auto key = conv_.SerializeToString(Prefix4InodeVolumeExtent{fsId, inodeId});
    return kvStorage_->SSeek(table4VolumeExtent_, key);
}

MetaStatusCode InodeStorage::GetVolumeExtentByOffset(uint32_t fsId,
                                                     uint64_t inodeId,
                                                     uint64_t offset,
                                                     VolumeExtentSlice* slice) {
    ReadLockGuard guard(RWLock);
    auto key =
        conv_.SerializeToString(Key4VolumeExtentSlice{fsId, inodeId, offset});

    auto st = kvStorage_->SGet(table4VolumeExtent_, key, slice);

    if (st.ok()) {
        return MetaStatusCode::OK;
    } else if (st.IsNotFound()) {
        return MetaStatusCode::NOT_FOUND;
    }

    return MetaStatusCode::STORAGE_INTERNAL_ERROR;
}

MetaStatusCode InodeStorage::GetAllBlockGroup(
    std::vector<DeallocatableBlockGroup>* deallocatableBlockGroupVec) {
    auto iter = kvStorage_->HGetAll(table4DeallocatableBlockGroup_);
    if (iter->Status() != 0) {
        LOG(ERROR) << "InodeStorage failed to get iterator for all "
                      "deallocatable block group";
        return MetaStatusCode::STORAGE_INTERNAL_ERROR;
    }

    uint32_t count = 0;
    DeallocatableBlockGroup deallocatbleBlockGroup;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        if (!conv_.ParseFromString(iter->Value(), &deallocatbleBlockGroup)) {
            LOG(ERROR) << "InodeStorage failed to parse deallocatable block "
                          "group";
            return MetaStatusCode::STORAGE_INTERNAL_ERROR;
        }

        deallocatableBlockGroupVec->emplace_back(
            std::move(deallocatbleBlockGroup));
        count++;
    }

    return count > 0 ? MetaStatusCode::OK : MetaStatusCode::NOT_FOUND;
}

MetaStatusCode InodeStorage::UpdateDeallocatableBlockGroup(
    uint32_t fsId, const DeallocatableBlockGroupVec& update, int64_t logIndex) {
    auto txn = kvStorage_->BeginTransaction();

    MetaStatusCode st = MetaStatusCode::OK;
    std::string step;

    for (auto& item : update) {
        Key4DeallocatableBlockGroup key(fsId, item.blockgroupoffset());
        std::string skey(key.SerializeToString());

        DeallocatableBlockGroup out;
        auto s = txn->HGet(table4DeallocatableBlockGroup_, skey, &out);

        if (!s.ok() && !s.IsNotFound()) {
            step = "get deallocatable group skey=" + skey + " failed";
            st = MetaStatusCode::STORAGE_INTERNAL_ERROR;
            break;
        }

        if (item.has_increase()) {
            // for first increase call, set blockgroupoffset
            if (s.IsNotFound()) {
                out.set_blockgroupoffset(item.blockgroupoffset());
            }
            st = Increase(txn, fsId, item.increase(), &out);
        } else if (item.has_decrease()) {
            st = Decrease(item.decrease(), &out);
        } else if (item.has_mark()) {
            st = Mark(item.mark(), &out);
        }

        s = txn->HSet(table4DeallocatableBlockGroup_, skey, out);
        if (!s.ok()) {
            step = "update deallocatable group skey=" + skey + " failed";
            st = MetaStatusCode::STORAGE_INTERNAL_ERROR;
            break;
        }
    }
    if (st == MetaStatusCode::OK) {
        auto s = SetAppliedIndex(txn.get(), logIndex);
        if (!s.ok()) {
            st = MetaStatusCode::STORAGE_INTERNAL_ERROR;
            LOG(ERROR)
                << "Insert applied index to transaction failed, status = "
                << s.ToString();
        }
    }

    if (st != MetaStatusCode::OK) {
        LOG(ERROR) << "UpdateDeallocatableBlockGroup txn is failed at " << step;
        if (!txn->Rollback().ok()) {
            LOG(ERROR) << "UpdateDeallocatableBlockGroup rollback transaction "
                          "failed, fsId="
                       << fsId;
            st = MetaStatusCode::STORAGE_INTERNAL_ERROR;
        }
    } else if (!txn->Commit().ok()) {
        LOG(ERROR)
            << "UpdateDeallocatableBlockGroup commit transaction failed, fsId="
            << fsId;
        st = MetaStatusCode::STORAGE_INTERNAL_ERROR;
    }

    return st;
}

MetaStatusCode InodeStorage::Increase(
    Transaction txn, uint32_t fsId,
    const IncreaseDeallocatableBlockGroup& increase,
    DeallocatableBlockGroup* out) {
    MetaStatusCode st = MetaStatusCode::OK;

    // update DeallocatableBlockGroup
    VLOG(6) << "InodeStorage handle increase=" << increase.DebugString();

    uint64_t oldSize =
        out->has_deallocatablesize() ? out->deallocatablesize() : 0;
    out->set_deallocatablesize(oldSize + increase.increasedeallocatablesize());
    out->mutable_inodeidlist()->MergeFrom(increase.inodeidlistadd());
    std::set<uint64_t> unique_elements(out->inodeidlist().begin(),
                                       out->inodeidlist().end());
    out->mutable_inodeidlist()->Clear();
    for (auto& elem : unique_elements) {
        out->mutable_inodeidlist()->Add(elem);
    }

    VLOG(6) << "InodeStorage handle increase set out=" << out->DebugString();

    // remove related inode in table4DeallocatableInode_
    for (auto& inodeid : increase.inodeidlistadd()) {
        auto s = txn->HDel(table4DeallocatableInode_,
                           conv_.SerializeToString(Key4Inode{fsId, inodeid}));
        if (!s.ok()) {
            st = MetaStatusCode::STORAGE_INTERNAL_ERROR;
            VLOG(6) << "InodeStorage delete inodeid=" << inodeid << " from "
                    << table4DeallocatableInode_ << " fail";
            break;
        }

        VLOG(6) << "InodeStorage delete inodeid=" << inodeid << " from "
                << StringToHex(table4DeallocatableInode_) << " success";
    }

    return st;
}

MetaStatusCode InodeStorage::Decrease(
    const DecreaseDeallocatableBlockGroup& decrease,
    DeallocatableBlockGroup* out) {
    VLOG(6) << "InodeStorage handle increase=" << decrease.DebugString();
    if (!out->IsInitialized() || !out->has_deallocatablesize()) {
        LOG(ERROR)
            << "UpdateDeallocatableBlockGroup record missing required fields";
        return MetaStatusCode::STORAGE_INTERNAL_ERROR;
    }

    uint64_t oldSize = out->deallocatablesize();
    if (oldSize < decrease.decreasedeallocatablesize()) {
        LOG(ERROR) << "UpdateDeallocatableBlockGroup decrease size is too big, "
                      "oldSize="
                   << oldSize
                   << ", decreasesize=" << decrease.decreasedeallocatablesize();
        return MetaStatusCode::STORAGE_INTERNAL_ERROR;
    }

    // update dallocatablesize
    out->set_deallocatablesize(oldSize - decrease.decreasedeallocatablesize());

    // update deallocatableinode list
    auto inodeidlist = out->mutable_inodeidunderdeallocate();
    inodeidlist->erase(
        std::remove_if(inodeidlist->begin(), inodeidlist->end(),
                       [&decrease](uint64_t inodeid) {
                           auto search = decrease.inodeddeallocated();
                           return std::find(search.begin(), search.end(),
                                            inodeid) != search.end();
                       }),
        inodeidlist->end());

    VLOG(6) << "InodeStorage handle decrease ok, and set out="
            << out->DebugString();

    return MetaStatusCode::OK;
}

MetaStatusCode InodeStorage::Mark(const MarkDeallocatableBlockGroup& mark,
                                  DeallocatableBlockGroup* out) {
    MetaStatusCode st = MetaStatusCode::OK;

    VLOG(6) << "InodeStorage handle mark=" << mark.DebugString();
    if (!out->IsInitialized() || !out->has_deallocatablesize()) {
        LOG(ERROR)
            << "UpdateDeallocatableBlockGroup record missing required fields";
        st = MetaStatusCode::STORAGE_INTERNAL_ERROR;
    } else {
        // update inodeunderdeallocate
        out->mutable_inodeidunderdeallocate()->MergeFrom(
            mark.inodeidunderdeallocate());

        // update inodeidlist
        auto inodeidlist = out->mutable_inodeidlist();
        inodeidlist->erase(
            std::remove_if(inodeidlist->begin(), inodeidlist->end(),
                           [&mark](uint64_t inodeid) {
                               auto search = mark.inodeidunderdeallocate();
                               return std::find(search.begin(), search.end(),
                                                inodeid) != search.end();
                           }),
            inodeidlist->end());
        VLOG(6) << "InodeStorage handle mark ok, and set out="
                << out->DebugString();
    }
    return st;
}

}  // namespace metaserver
}  // namespace curvefs
