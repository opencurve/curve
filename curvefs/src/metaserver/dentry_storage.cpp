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

#include <butil/time.h>
#include <cstdint>
#include <vector>
#include <memory>
#include <algorithm>

#include "src/common/string_util.h"
#include "curvefs/src/metaserver/dentry_storage.h"

namespace curvefs {
namespace metaserver {

using ::curve::common::ReadLockGuard;
using ::curve::common::WriteLockGuard;
using ::curve::common::StringStartWith;
using ::curvefs::metaserver::storage::Status;
using ::curvefs::metaserver::storage::Key4Dentry;
using ::curvefs::metaserver::storage::Prefix4SameParentDentry;
using ::curvefs::metaserver::storage::Prefix4AllDentry;

bool operator==(const Dentry& lhs, const Dentry& rhs) {
    return EQUAL(fsid) && EQUAL(parentinodeid) && EQUAL(name) &&
           EQUAL(txid) && EQUAL(inodeid) && EQUAL(flag);
}

bool operator<(const Dentry& lhs, const Dentry& rhs) {
    return LESS(fsid) ||
           LESS2(fsid, parentinodeid) ||
           LESS3(fsid, parentinodeid, name) ||
           LESS4(fsid, parentinodeid, name, txid);
}

static bool BelongSomeOne(const Dentry& lhs, const Dentry& rhs) {
    return EQUAL(fsid) && EQUAL(parentinodeid) && EQUAL(name) &&
           EQUAL(inodeid);
}

static bool HasDeleteMarkFlag(const Dentry& dentry) {
    return (dentry.flag() & DentryFlag::DELETE_MARK_FLAG) != 0;
}

DentryVector::DentryVector(DentryVec* vec)
    : vec_(vec),
      nPendingAdd_(0),
      nPendingDel_(0) {}

void DentryVector::Insert(const Dentry& dentry) {
    for (const Dentry& item : vec_->dentrys()) {
        if (item == dentry) {
            return;
        }
    }
    vec_->add_dentrys()->CopyFrom(dentry);
    nPendingAdd_ += 1;
}

void DentryVector::Delete(const Dentry& dentry) {
    for (size_t i = 0; i < vec_->dentrys_size(); i++) {
        if (vec_->dentrys(i) == dentry) {
            vec_->mutable_dentrys()->DeleteSubrange(i, 1);
            nPendingDel_ += 1;
            break;
        }
    }
}

void DentryVector::Merge(const DentryVec& src) {
    for (const auto& dentry : src.dentrys()) {
        vec_->add_dentrys()->CopyFrom(dentry);
    }
    nPendingAdd_ = src.dentrys_size();
}

void DentryVector::Filter(uint64_t maxTxId, BTree* btree) {
    for (const Dentry& dentry : vec_->dentrys()) {
        if (dentry.txid() <= maxTxId) {
            btree->insert(dentry);
        }
    }
}

void DentryVector::Confirm(uint64_t* count) {
    if (nPendingDel_ > *count + nPendingAdd_) {
        LOG(ERROR) << "there are multi delete, count = " << *count
                   << ", nPendingAdd = " << nPendingAdd_
                   << ", nPendingDel = " << nPendingDel_;
        *count = 0;
        return;
    }
    *count = *count + nPendingAdd_ - nPendingDel_;
}

DentryList::DentryList(std::vector<Dentry>* list,
                       uint32_t limit,
                       const std::string& exclude,
                       uint64_t maxTxId,
                       bool onlyDir)
    : list_(list),
      size_(0),
      limit_(limit),
      exclude_(exclude),
      maxTxId_(maxTxId),
      onlyDir_(onlyDir) {}

void DentryList::PushBack(DentryVec* vec) {
    // NOTE: it's a cheap operation becacuse the size of
    // dentryVec must less than 2
    BTree dentrys;
    DentryVector vector(vec);
    vector.Filter(maxTxId_, &dentrys);
    auto last = dentrys.rbegin();
    if (IsFull()) {
        return;
    } else if (dentrys.size() == 0 || HasDeleteMarkFlag(*last)) {
        return;
    } else if (last->name() == exclude_) {
        return;
    }

    size_++;

    if (onlyDir_ && last->type() != FsFileType::TYPE_DIRECTORY) {
        // record the last even if it is not dir(will deal in client)
        if (IsFull()) {
            list_->push_back(*last);
        }
        return;
    }
    list_->push_back(*last);
    VLOG(9) << "Push dentry, dentry = (" << last->ShortDebugString() << ")";
}

uint32_t DentryList::Size() {
    return size_;
}

bool DentryList::IsFull() {
    return limit_ != 0 && size_ >= limit_;
}

DentryStorage::DentryStorage(std::shared_ptr<KVStorage> kvStorage,
                             std::shared_ptr<NameGenerator> nameGenerator,
                             uint64_t nDentry)
    : kvStorage_(kvStorage),
      table4Dentry_(nameGenerator->GetDentryTableName()),
      nDentry_(nDentry),
      conv_() {}

std::string DentryStorage::DentryKey(const Dentry& dentry) {
    Key4Dentry key(dentry.fsid(), dentry.parentinodeid(), dentry.name());
    return conv_.SerializeToString(key);
}

bool DentryStorage::CompressDentry(DentryVec* vec, BTree* dentrys) {
    DentryVector vector(vec);
    std::vector<Dentry> deleted;
    if (dentrys->size() == 2) {
        deleted.push_back(*dentrys->begin());
    }
    if (HasDeleteMarkFlag(*dentrys->rbegin())) {
        deleted.push_back(*dentrys->rbegin());
    }
    for (const auto& dentry : deleted) {
        vector.Delete(dentry);
    }

    Status s;
    std::string skey = DentryKey(*dentrys->begin());
    if (vec->dentrys_size() == 0) {  // delete directly
        s = kvStorage_->SDel(table4Dentry_, skey);
    } else {
        s = kvStorage_->SSet(table4Dentry_, skey, *vec);
    }

    if (s.ok()) {
        vector.Confirm(&nDentry_);
        return true;
    }
    return false;
}

// NOTE: Find() return the dentry which has the latest txid,
// and it will clean the old txid's dentry if you specify compress to true
MetaStatusCode DentryStorage::Find(const Dentry& in,
                                   Dentry* out,
                                   DentryVec* vec,
                                   bool compress) {
    std::string skey = DentryKey(in);
    Status s = kvStorage_->SGet(table4Dentry_, skey, vec);
    if (s.IsNotFound()) {
        return MetaStatusCode::NOT_FOUND;
    } else if (!s.ok()) {
        return MetaStatusCode::STORAGE_INTERNAL_ERROR;
    }

    // status = OK
    BTree dentrys;
    DentryVector vector(vec);
    vector.Filter(in.txid(), &dentrys);
    size_t size = dentrys.size();
    if (size > 2) {
        LOG(ERROR) << "There are more than 2 dentrys";
        return MetaStatusCode::NOT_FOUND;
    } else if (size == 0) {
        return MetaStatusCode::NOT_FOUND;
    }

    // size == 1 || size == 2
    MetaStatusCode rc;
    if (HasDeleteMarkFlag(*dentrys.rbegin())) {
        rc = MetaStatusCode::NOT_FOUND;
    } else {
        rc = MetaStatusCode::OK;
        *out = *dentrys.rbegin();
    }

    if (compress && !CompressDentry(vec, &dentrys)) {
        rc = MetaStatusCode::STORAGE_INTERNAL_ERROR;
    }
    return rc;
}

MetaStatusCode DentryStorage::Insert(const Dentry& dentry) {
    WriteLockGuard lg(rwLock_);

    Dentry out;
    DentryVec vec;
    MetaStatusCode rc = Find(dentry, &out, &vec, true);
    if (rc == MetaStatusCode::OK) {
        if (BelongSomeOne(out, dentry)) {
            return MetaStatusCode::IDEMPOTENCE_OK;
        }
        return MetaStatusCode::DENTRY_EXIST;
    } else if (rc != MetaStatusCode::NOT_FOUND) {
        return MetaStatusCode::STORAGE_INTERNAL_ERROR;
    }

    // rc == MetaStatusCode::NOT_FOUND
    DentryVector vector(&vec);
    vector.Insert(dentry);
    std::string skey = DentryKey(dentry);
    Status s = kvStorage_->SSet(table4Dentry_, skey, vec);
    if (!s.ok()) {
        LOG(ERROR) << "Insert dentry failed, status = " << s.ToString();
        return MetaStatusCode::STORAGE_INTERNAL_ERROR;
    }
    vector.Confirm(&nDentry_);
    return MetaStatusCode::OK;
}

MetaStatusCode DentryStorage::Insert(const DentryVec& vec, bool merge) {
    WriteLockGuard lg(rwLock_);

    Status s;
    DentryVec oldVec;
    std::string skey = DentryKey(vec.dentrys(0));
    if (merge) {  // for old version dumpfile (v1)
        s = kvStorage_->SGet(table4Dentry_, skey, &oldVec);
        if (s.IsNotFound()) {
            // do nothing
        } else if (!s.ok()) {
            return MetaStatusCode::STORAGE_INTERNAL_ERROR;
        }
    }

    DentryVector vector(&oldVec);
    vector.Merge(vec);
    s = kvStorage_->SSet(table4Dentry_, skey, oldVec);
    if (!s.ok()) {
        LOG(ERROR) << "Insert dentry vector failed, status = " << s.ToString();
        return MetaStatusCode::STORAGE_INTERNAL_ERROR;
    }
    vector.Confirm(&nDentry_);
    return MetaStatusCode::OK;
}

MetaStatusCode DentryStorage::Delete(const Dentry& dentry) {
    WriteLockGuard lg(rwLock_);

    Dentry out;
    DentryVec vec;
    MetaStatusCode rc = Find(dentry, &out, &vec, true);
    if (rc == MetaStatusCode::NOT_FOUND) {
        return MetaStatusCode::NOT_FOUND;
    } else if (rc != MetaStatusCode::OK) {
        return MetaStatusCode::STORAGE_INTERNAL_ERROR;
    }

    Status s;
    DentryVector vector(&vec);
    vector.Delete(out);
    std::string skey = DentryKey(dentry);
    if (vec.dentrys_size() == 0) {
        s = kvStorage_->SDel(table4Dentry_, skey);
    } else {
        s = kvStorage_->SSet(table4Dentry_, skey, vec);
    }

    if (s.ok()) {
        vector.Confirm(&nDentry_);
        return MetaStatusCode::OK;
    }
    return MetaStatusCode::STORAGE_INTERNAL_ERROR;
}

MetaStatusCode DentryStorage::Get(Dentry* dentry) {
    ReadLockGuard lg(rwLock_);

    Dentry out;
    DentryVec vec;
    MetaStatusCode rc = Find(*dentry, &out, &vec, false);
    if (rc == MetaStatusCode::NOT_FOUND) {
        return MetaStatusCode::NOT_FOUND;
    } else if (rc != MetaStatusCode::OK) {
        return MetaStatusCode::STORAGE_INTERNAL_ERROR;
    }

    // MetaStatusCode::OK
    *dentry = out;
    return MetaStatusCode::OK;
}

MetaStatusCode DentryStorage::List(const Dentry& dentry,
                                   std::vector<Dentry>* dentrys,
                                   uint32_t limit,
                                   bool onlyDir) {
    // TODO(all): consider store dir dentry and file dentry separately
    ReadLockGuard lg(rwLock_);

    // 1. precheck for dentry vector
    // NOTE: we should gurantee the vector is empty
    if (nullptr == dentrys || dentrys->size() > 0) {
        LOG(ERROR) << "input dentry vector is invalid";
        return MetaStatusCode::PARAM_ERROR;
    }

    // 2. prepare seek lower key
    uint32_t fsId = dentry.fsid();
    uint64_t parentInodeId = dentry.parentinodeid();
    std::string name = dentry.name();
    Prefix4SameParentDentry prefix(fsId, parentInodeId);
    std::string sprefix = conv_.SerializeToString(prefix);  // "1:1:"
    Key4Dentry key(fsId, parentInodeId, name);
    std::string lower = conv_.SerializeToString(key);  // "1:1:", "1:1:/a/b/c"

    // 3. iterator key/value pair one by one
    auto iterator = kvStorage_->SSeek(table4Dentry_, lower);
    iterator->DisablePrefixChecking();
    if (iterator->Status() < 0) {
        return MetaStatusCode::STORAGE_INTERNAL_ERROR;
    }

    DentryVec current;
    DentryList list(dentrys, limit, name, dentry.txid(), onlyDir);
    butil::Timer time;
    uint32_t seekTimes = 0;
    time.start();
    for (iterator->SeekToFirst(); iterator->Valid(); iterator->Next()) {
        seekTimes++;
        std::string skey = iterator->Key();
        std::string svalue = iterator->Value();
        if (!StringStartWith(skey, sprefix)) {
            break;
        } else if (!iterator->ParseFromValue(&current)) {
            return MetaStatusCode::PARSE_FROM_STRING_FAILED;
        }

        list.PushBack(&current);
        if (list.IsFull()) {
            break;
        }
    }
    time.stop();
    VLOG(1) << "ListDentry request: dentry = ("
            << dentry.ShortDebugString() << ")"
            << ", onlyDir = " << onlyDir
            << ", limit = " << limit
            << ", lower key = " << lower
            << ", seekTimes = " << seekTimes
            << ", dentrySize = " << dentrys->size()
            << ", costUs = " << time.u_elapsed();
    return MetaStatusCode::OK;
}

MetaStatusCode DentryStorage::HandleTx(TX_OP_TYPE type, const Dentry& dentry) {
    WriteLockGuard lg(rwLock_);

    Status s;
    Dentry out;
    DentryVec vec;
    DentryVector vector(&vec);
    std::string skey = DentryKey(dentry);
    MetaStatusCode rc = MetaStatusCode::OK;
    switch (type) {
        case TX_OP_TYPE::PREPARE:
            s = kvStorage_->SGet(table4Dentry_, skey, &vec);
            if (!s.ok() && !s.IsNotFound()) {
                rc = MetaStatusCode::STORAGE_INTERNAL_ERROR;
                break;
            }

            // OK || NOT_FOUND
            vector.Insert(dentry);
            s = kvStorage_->SSet(table4Dentry_, skey, vec);
            if (!s.ok()) {
                rc = MetaStatusCode::STORAGE_INTERNAL_ERROR;
            } else {
                vector.Confirm(&nDentry_);
            }
            break;

        case TX_OP_TYPE::COMMIT:
            rc = Find(dentry, &out, &vec, true);
            if (rc == MetaStatusCode::OK ||
                rc == MetaStatusCode::NOT_FOUND) {
                rc = MetaStatusCode::OK;
            }
            break;

        case TX_OP_TYPE::ROLLBACK:
            s = kvStorage_->SGet(table4Dentry_, skey, &vec);
            if (!s.ok() && !s.IsNotFound()) {
                rc = MetaStatusCode::STORAGE_INTERNAL_ERROR;
                break;
            }

            // OK || NOT_FOUND
            vector.Delete(dentry);
            if (vec.dentrys_size() == 0) {  // delete directly
                s = kvStorage_->SDel(table4Dentry_, skey);
            } else {
                s = kvStorage_->SSet(table4Dentry_, skey, vec);
            }
            if (!s.ok()) {
                rc = MetaStatusCode::STORAGE_INTERNAL_ERROR;
            } else {
                vector.Confirm(&nDentry_);
            }
            break;

        default:
            rc = MetaStatusCode::PARAM_ERROR;
    }

    return rc;
}

std::shared_ptr<Iterator> DentryStorage::GetAll() {
    ReadLockGuard lg(rwLock_);
    return kvStorage_->SGetAll(table4Dentry_);
}

size_t DentryStorage::Size() {
    ReadLockGuard lg(rwLock_);
    return nDentry_;
}

bool DentryStorage::Empty() {
    ReadLockGuard lg(rwLock_);

    std::string sprefix = conv_.SerializeToString(Prefix4AllDentry());
    auto iterator = kvStorage_->SSeek(table4Dentry_, sprefix);
    if (iterator->Status() != 0) {
        LOG(ERROR) << "failed to get iterator for all inode";
        return false;
    }

    for (iterator->SeekToFirst(); iterator->Valid(); iterator->Next()) {
        return false;
    }
    return true;
}

MetaStatusCode DentryStorage::Clear() {
    WriteLockGuard lg(rwLock_);
    Status s = kvStorage_->SClear(table4Dentry_);
    if (!s.ok()) {
        LOG(ERROR) << "failed to clear dentry table, status = " << s.ToString();
        return MetaStatusCode::STORAGE_INTERNAL_ERROR;
    }
    nDentry_ = 0;
    return MetaStatusCode::OK;
}

}  // namespace metaserver
}  // namespace curvefs
