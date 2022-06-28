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

#include <limits>
#include <string>
#include <memory>
#include <vector>
#include <algorithm>

#include "src/common/concurrent/rw_lock.h"
#include "src/common/string_util.h"
#include "curvefs/proto/metaserver.pb.h"
#include "curvefs/src/metaserver/storage/status.h"
#include "curvefs/src/metaserver/inode_storage.h"
#include "curvefs/src/metaserver/storage/converter.h"

namespace curvefs {
namespace metaserver {

using ::curve::common::ReadLockGuard;
using ::curve::common::WriteLockGuard;
using ::curve::common::StringStartWith;
using ::curvefs::metaserver::storage::Status;
using ::curvefs::metaserver::storage::KVStorage;
using ::curvefs::metaserver::storage::Key4S3ChunkInfoList;
using ::curvefs::metaserver::storage::Key4VolumeExtentSlice;
using ::curvefs::metaserver::storage::Prefix4InodeVolumeExtent;
using ::curvefs::metaserver::storage::Prefix4ChunkIndexS3ChunkInfoList;
using ::curvefs::metaserver::storage::Prefix4InodeS3ChunkInfoList;
using ::curvefs::metaserver::storage::Prefix4AllInode;
using ::curvefs::metaserver::storage::Key4InodeAuxInfo;

using Transaction = std::shared_ptr<StorageTransaction>;
using S3ChunkInfoMap = google::protobuf::Map<uint64_t, S3ChunkInfoList>;

InodeStorage::InodeStorage(std::shared_ptr<KVStorage> kvStorage,
                           std::shared_ptr<NameGenerator> nameGenerator,
                           uint64_t nInode)
    : kvStorage_(std::move(kvStorage)),
      table4Inode_(nameGenerator->GetInodeTableName()),
      table4S3ChunkInfo_(nameGenerator->GetS3ChunkInfoTableName()),
      table4VolumeExtent_(nameGenerator->GetVolumeExtentTableName()),
      table4InodeAuxInfo_(nameGenerator->GetInodeAuxInfoTableName()),
      nInode_(nInode),
      conv_() {}

MetaStatusCode InodeStorage::Insert(const Inode& inode) {
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
    s = kvStorage_->HSet(table4Inode_, skey, inode);
    if (s.ok()) {
        nInode_++;
        return MetaStatusCode::OK;
    }
    LOG(ERROR) << "Insert inode failed, status = " << s.ToString();
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

MetaStatusCode InodeStorage::GetAttr(const Key4Inode& key,
                                     InodeAttr *attr) {
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
    if (inode.has_openmpcount()) {
        attr->set_openmpcount(inode.openmpcount());
    }
    if (inode.xattr_size() > 0) {
        *(attr->mutable_xattr()) = inode.xattr();
    }
    return MetaStatusCode::OK;
}

MetaStatusCode InodeStorage::GetXAttr(const Key4Inode& key, XAttr *xattr) {
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

MetaStatusCode InodeStorage::Delete(const Key4Inode& key) {
    WriteLockGuard lg(rwLock_);
    std::string skey = conv_.SerializeToString(key);
    Status s = kvStorage_->HDel(table4Inode_, skey);
    if (s.ok()) {
        // NOTE: for rocksdb storage, it will never check whether
        // the key exist in delete(), so if the client delete the
        // unexist inode in some anbormal cases, it will cause the
        // nInode less then the real value.
        if (nInode_ > 0) {
            nInode_--;
        }
        return MetaStatusCode::OK;
    }

    return MetaStatusCode::STORAGE_INTERNAL_ERROR;
}

MetaStatusCode InodeStorage::Update(const Inode& inode) {
    WriteLockGuard lg(rwLock_);
    Key4Inode key(inode.fsid(), inode.inodeid());
    std::string skey = conv_.SerializeToString(key);

    Status s = kvStorage_->HSet(table4Inode_, skey, inode);
    if (s.ok()) {
        return MetaStatusCode::OK;
    }
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

MetaStatusCode InodeStorage::Clear() {
    WriteLockGuard lg(rwLock_);
    Status s = kvStorage_->HClear(table4Inode_);
    if (!s.ok()) {
        LOG(ERROR) << "InodeStorage clear inode table failed";
        return MetaStatusCode::STORAGE_INTERNAL_ERROR;
    }
    nInode_ = 0;

    s = kvStorage_->SClear(table4S3ChunkInfo_);
    if (!s.ok()) {
        LOG(ERROR) << "InodeStorage clear inode s3chunkinfo table failed";
        return MetaStatusCode::STORAGE_INTERNAL_ERROR;
    }

    s = kvStorage_->SClear(table4VolumeExtent_);
    if (!s.ok()) {
        LOG(ERROR) << "InodeStorage clear inode volume extent table failed";
        return MetaStatusCode::STORAGE_INTERNAL_ERROR;
    }

    s = kvStorage_->HClear(table4InodeAuxInfo_);
    if (!s.ok()) {
        LOG(ERROR) << "InodeStorage clear inode aux info table failed";
        return MetaStatusCode::STORAGE_INTERNAL_ERROR;
    }

    return MetaStatusCode::OK;
}

bool InodeStorage::UpdateInodeS3MetaSize(uint32_t fsId, uint64_t inodeId,
                                         uint64_t size4add, uint64_t size4del) {
    uint64_t size = 0;
    InodeAuxInfo out;
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
        return false;
    }

    size += size4add;
    if (size < size4del) {
        LOG(ERROR) << "current inode s3 meta size is " << size
                   << ", less than " << size4del;
        return false;
    }

    out.set_s3metasize(size - size4del);
    s = kvStorage_->HSet(table4InodeAuxInfo_, skey, out);
    if (!s.ok()) {
        LOG(ERROR) << "failed to set inode s3 meta size, status="
                   << s.ToString();
        return false;
    }
    return true;
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
    Transaction txn,
    uint32_t fsId,
    uint64_t inodeId,
    uint64_t chunkIndex,
    const S3ChunkInfoList* list2add) {
    if (nullptr == list2add || list2add->s3chunks_size() == 0) {
        return MetaStatusCode::OK;
    }

    size_t size = list2add->s3chunks_size();
    uint64_t firstChunkId = list2add->s3chunks(0).chunkid();
    uint64_t lastChunkId = list2add->s3chunks(size - 1).chunkid();
    Key4S3ChunkInfoList key(fsId, inodeId, chunkIndex,
                            firstChunkId, lastChunkId, size);
    std::string skey = conv_.SerializeToString(key);

    Status s = txn->SSet(table4S3ChunkInfo_, skey, *list2add);
    return s.ok() ? MetaStatusCode::OK :
                    MetaStatusCode::STORAGE_INTERNAL_ERROR;
}

MetaStatusCode InodeStorage::DelS3ChunkInfoList(
    Transaction txn,
    uint32_t fsId,
    uint64_t inodeId,
    uint64_t chunkIndex,
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
            LOG(ERROR) << "wrong delete list range (" << delFirstChunkId
                       << "," << delLastChunkId << "), skey=" << skey;
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
    uint32_t fsId,
    uint64_t inodeId,
    uint64_t chunkIndex,
    const S3ChunkInfoList* list2add,
    const S3ChunkInfoList* list2del) {
    WriteLockGuard lg(rwLock_);
    auto txn = kvStorage_->BeginTransaction();
    if (nullptr == txn) {
        return MetaStatusCode::STORAGE_INTERNAL_ERROR;
    }

    auto rc = DelS3ChunkInfoList(txn, fsId, inodeId, chunkIndex, list2del);
    if (rc == MetaStatusCode::OK) {
        rc = AddS3ChunkInfoList(txn, fsId, inodeId, chunkIndex, list2add);
    }

    if (rc != MetaStatusCode::OK) {
        if (!txn->Rollback().ok()) {
            LOG(ERROR) << "Rollback transaction failed";
            rc = MetaStatusCode::STORAGE_INTERNAL_ERROR;
        }
    } else if (!txn->Commit().ok()) {
        LOG(ERROR) << "Commit transaction failed";
        rc = MetaStatusCode::STORAGE_INTERNAL_ERROR;
    }

    if (rc != MetaStatusCode::OK) {
        return rc;
    }

    // rc == MetaStatusCode::OK
    uint64_t size4add = (nullptr == list2add) ? 0 : list2add->s3chunks_size();
    uint64_t size4del = (nullptr == list2del) ? 0 : list2del->s3chunks_size();
    if (!UpdateInodeS3MetaSize(fsId, inodeId, size4add, size4del)) {
        LOG(ERROR) << "Update inode s3meta size failed";
        return MetaStatusCode::STORAGE_INTERNAL_ERROR;
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
    uint32_t fsId,
    uint64_t inodeId,
    const VolumeExtentSlice& slice) {
    WriteLockGuard guard(rwLock_);
    auto key = conv_.SerializeToString(
        Key4VolumeExtentSlice{fsId, inodeId, slice.offset()});

    auto st = kvStorage_->SSet(table4VolumeExtent_, key, slice);

    return st.ok() ? MetaStatusCode::OK
                   : MetaStatusCode::STORAGE_INTERNAL_ERROR;
}

MetaStatusCode InodeStorage::GetAllVolumeExtent(uint32_t fsId,
                                                uint64_t inodeId,
                                                VolumeExtentList* extents) {
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

}  // namespace metaserver
}  // namespace curvefs
