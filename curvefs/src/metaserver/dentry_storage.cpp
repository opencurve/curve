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

#include "curvefs/src/metaserver/dentry_storage.h"

#include <butil/time.h>

#include <algorithm>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "curvefs/src/metaserver/storage/status.h"
#include "curvefs/src/metaserver/storage/storage.h"
#include "curvefs/src/metaserver/transaction.h"
#include "src/common/string_util.h"

namespace curvefs {
namespace metaserver {

using ::curve::common::ReadLockGuard;
using ::curve::common::StringStartWith;
using ::curve::common::WriteLockGuard;
using ::curvefs::metaserver::storage::Key4Dentry;
using ::curvefs::metaserver::storage::Prefix4AllDentry;
using ::curvefs::metaserver::storage::Prefix4SameParentDentry;
using ::curvefs::metaserver::storage::Status;

bool operator==(const Dentry& lhs, const Dentry& rhs) {
    return EQUAL(fsid) && EQUAL(parentinodeid) && EQUAL(name) && EQUAL(txid) &&
           EQUAL(inodeid) && EQUAL(flag);
}

bool operator<(const Dentry& lhs, const Dentry& rhs) {
    return LESS(fsid) || LESS2(fsid, parentinodeid) ||
           LESS3(fsid, parentinodeid, name) ||
           LESS4(fsid, parentinodeid, name, txid);
}

static bool BelongSomeOne(const Dentry& lhs, const Dentry& rhs) {
    return EQUAL(fsid) && EQUAL(parentinodeid) && EQUAL(name) && EQUAL(inodeid);
}

static bool HasDeleteMarkFlag(const Dentry& dentry) {
    return (dentry.flag() & DentryFlag::DELETE_MARK_FLAG) != 0;
}

DentryVector::DentryVector(DentryVec* vec)
    : vec_(vec), nPendingAdd_(0), nPendingDel_(0) {}

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
    for (int i = 0; i < vec_->dentrys_size(); i++) {
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

DentryList::DentryList(std::vector<Dentry>* list, uint32_t limit,
                       const std::string& exclude, uint64_t maxTxId,
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

uint32_t DentryList::Size() { return size_; }

bool DentryList::IsFull() { return limit_ != 0 && size_ >= limit_; }

const char* DentryStorage::kDentryAppliedKey("dentry");
const char* DentryStorage::kDentryCountKey("count");
const char* DentryStorage::kHandleTxKey("handleTx");
const char* DentryStorage::kPendingTxKey("pendingTx");

bool DentryStorage::Init() {
    auto s = GetDentryCount(&nDentry_);
    if (s.ok() || s.IsNotFound()) {
        s = GetHandleTxIndex(&handleTxIndex_);
        return s.ok() || s.IsNotFound();
    }
    return false;
}

DentryStorage::DentryStorage(std::shared_ptr<KVStorage> kvStorage,
                             std::shared_ptr<NameGenerator> nameGenerator,
                             uint64_t nDentry)
    : kvStorage_(kvStorage),
      table4Dentry_(nameGenerator->GetDentryTableName()),
      table4AppliedIndex_(nameGenerator->GetAppliedIndexTableName()),
      table4Transaction_(nameGenerator->GetTransactionTableName()),
      table4DentryCount_(nameGenerator->GetDentryCountTableName()),
      handleTxIndex_(-1),
      nDentry_(nDentry),
      conv_() {
    // NOTE: for compatibility with older versions
    // we cannot ignore `nDentry` argument
    // try get dentry count for rocksdb
    // if we got it, replace old value
}

std::string DentryStorage::DentryKey(const Dentry& dentry) {
    Key4Dentry key(dentry.fsid(), dentry.parentinodeid(), dentry.name());
    return conv_.SerializeToString(key);
}

bool DentryStorage::CompressDentry(storage::StorageTransaction* txn,
                                   DentryVec* vec, BTree* dentrys,
                                   uint64_t* outCount) {
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
    const char* step = "Compress dentry from transaction";
    Status s;
    std::string skey = DentryKey(*dentrys->begin());
    do {
        if (vec->dentrys_size() == 0) {  // delete directly
            s = txn->SDel(table4Dentry_, skey);
        } else {
            s = txn->SSet(table4Dentry_, skey, *vec);
        }
        if (!s.ok()) {
            break;
        }
        uint64_t countCopy = *outCount;
        vector.Confirm(&countCopy);
        s = SetDentryCount(txn, countCopy);
        if (!s.ok()) {
            step = "Insert dentry count to transaction";
            break;
        }
        *outCount = countCopy;
        return true;
    } while (false);
    LOG(ERROR) << step << " failed, status = " << s.ToString();
    return false;
}

// NOTE: Find() return the dentry which has the latest txid,
// and it will clean the old txid's dentry if you specify compress to true
MetaStatusCode DentryStorage::Find(const Dentry& in, Dentry* out,
                                   DentryVec* vec) {
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
    return rc;
}

// NOTE: Find() return the dentry which has the latest txid,
// and it will clean the old txid's dentry if you specify compressOutCount to
// non-nullptr compressOutCount must point to a variable that value is equal
// with `nDentry_`
MetaStatusCode DentryStorage::Find(storage::StorageTransaction* txn,
                                   const Dentry& in, Dentry* out,
                                   DentryVec* vec, uint64_t* compressOutCount) {
    std::string skey = DentryKey(in);
    Status s = txn->SGet(table4Dentry_, skey, vec);
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

    if (compressOutCount != nullptr &&
        !CompressDentry(txn, vec, &dentrys, compressOutCount)) {
        rc = MetaStatusCode::STORAGE_INTERNAL_ERROR;
    }
    return rc;
}

MetaStatusCode DentryStorage::GetAppliedIndex(int64_t* index) {
    common::AppliedIndex val;
    auto s = kvStorage_->SGet(table4AppliedIndex_, kDentryAppliedKey, &val);
    if (s.ok()) {
        *index = val.index();
        return MetaStatusCode::OK;
    }
    if (s.IsNotFound()) {
        return MetaStatusCode::NOT_FOUND;
    }
    return MetaStatusCode::STORAGE_INTERNAL_ERROR;
}

storage::Status DentryStorage::SetAppliedIndex(
    storage::StorageTransaction* transaction, int64_t index) {
    common::AppliedIndex val;
    val.set_index(index);
    return transaction->SSet(table4AppliedIndex_, kDentryAppliedKey, val);
}

storage::Status DentryStorage::DelAppliedIndex(
    storage::StorageTransaction* transaction) {
    return transaction->SDel(table4AppliedIndex_, kDentryAppliedKey);
}

storage::Status DentryStorage::SetHandleTxIndex(
    storage::StorageTransaction* transaction, int64_t index) {
    common::AppliedIndex val;
    val.set_index(index);
    return transaction->SSet(table4AppliedIndex_, kHandleTxKey, val);
}

storage::Status DentryStorage::DelHandleTxIndex(
    storage::StorageTransaction* transaction) {
    return transaction->SDel(table4AppliedIndex_, kHandleTxKey);
}

MetaStatusCode DentryStorage::GetPendingTx(
    metaserver::TransactionRequest* request) {
    auto s = kvStorage_->SGet(table4Transaction_, kPendingTxKey, request);
    if (s.ok()) {
        return MetaStatusCode::OK;
    }
    if (s.IsNotFound()) {
        return MetaStatusCode::NOT_FOUND;
    }
    return MetaStatusCode::STORAGE_INTERNAL_ERROR;
}

storage::Status DentryStorage::SetPendingTx(
    storage::StorageTransaction* txn,
    const metaserver::TransactionRequest& request) {
    return txn->SSet(table4Transaction_, kPendingTxKey, request);
}

storage::Status DentryStorage::DelPendingTx(storage::StorageTransaction* txn) {
    return txn->SDel(table4Transaction_, kPendingTxKey);
}

storage::Status DentryStorage::ClearPendingTx(
    storage::StorageTransaction* txn) {
    metaserver::TransactionRequest request;
    request.set_type(metaserver::TransactionRequest::None);
    request.set_rawpayload("");
    return txn->SSet(table4Transaction_, "transaction", request);
}

storage::Status DentryStorage::SetDentryCount(storage::StorageTransaction* txn,
                                              uint64_t count) {
    common::ItemCount val;
    val.set_count(count);
    return txn->SSet(table4DentryCount_, kDentryCountKey, val);
}

storage::Status DentryStorage::DelDentryCount(
    storage::StorageTransaction* txn) {
    return txn->SDel(table4DentryCount_, kDentryCountKey);
}

storage::Status DentryStorage::GetDentryCount(uint64_t* count) {
    common::ItemCount val;
    auto s = kvStorage_->SGet(table4DentryCount_, kDentryCountKey, &val);
    if (s.ok()) {
        *count = val.count();
    }
    return s;
}

storage::Status DentryStorage::GetHandleTxIndex(int64_t* index) {
    common::AppliedIndex val;
    auto s = kvStorage_->SGet(table4AppliedIndex_, kHandleTxKey, &val);
    if (s.ok()) {
        *index = val.index();
    }
    return s;
}

MetaStatusCode DentryStorage::Insert(const Dentry& dentry, int64_t logIndex) {
    WriteLockGuard lg(rwLock_);

    Dentry out;
    DentryVec vec;
    std::shared_ptr<storage::StorageTransaction> txn;
    storage::Status s;
    const char* step = "Begin transaction";
    do {
        txn = kvStorage_->BeginTransaction();
        if (txn == nullptr) {
            break;
        }
        uint64_t count = nDentry_;
        s = SetAppliedIndex(txn.get(), logIndex);
        if (!s.ok()) {
            step = "Insert applied index to transaction";
            break;
        }
        MetaStatusCode rc = Find(txn.get(), dentry, &out, &vec, &count);
        if (rc == MetaStatusCode::OK) {
            auto s = txn->Commit();
            if (!s.ok()) {
                step = "Commit compress dentry transaction";
                break;
            }
            // if compress is success
            // we use output dentry count to replace old one
            nDentry_ = count;
            if (BelongSomeOne(out, dentry)) {
                return MetaStatusCode::IDEMPOTENCE_OK;
            }
            return MetaStatusCode::DENTRY_EXIST;
        } else if (rc != MetaStatusCode::NOT_FOUND) {
            step = "Find dentry failed";
            break;
        }
        // rc == MetaStatusCode::NOT_FOUND

        // NOTE: `count` maybe already written by `Find()` in here
        // so we continue use `count` in follow operations
        DentryVector vector(&vec);
        vector.Insert(dentry);
        std::string skey = DentryKey(dentry);
        s = txn->SSet(table4Dentry_, skey, vec);
        if (!s.ok()) {
            step = "Insert dentry to transaction";
            break;
        }
        vector.Confirm(&count);
        s = SetDentryCount(txn.get(), count);
        if (!s.ok()) {
            step = "Insert dentry count to transaction";
            break;
        }
        s = txn->Commit();
        if (!s.ok()) {
            step = "Insert dentry";
            break;
        }
        nDentry_ = count;
        return MetaStatusCode::OK;
    } while (false);
    LOG(ERROR) << step << " failed, status = " << s.ToString();
    if (txn != nullptr && !txn->Rollback().ok()) {
        LOG(ERROR) << "Rollback insert dentry transaction failed, status = "
                   << s.ToString();
    }
    return MetaStatusCode::STORAGE_INTERNAL_ERROR;
}

MetaStatusCode DentryStorage::Insert(const DentryVec& vec, bool merge,
                                     int64_t logIndex) {
    WriteLockGuard lg(rwLock_);

    Status s;
    DentryVec oldVec;
    std::string skey = DentryKey(vec.dentrys(0));
    std::shared_ptr<storage::StorageTransaction> txn;
    const char* step = "Begin transaction";
    do {
        txn = kvStorage_->BeginTransaction();
        if (txn == nullptr) {
            break;
        }
        if (merge) {  // for old version dumpfile (v1)
            s = txn->SGet(table4Dentry_, skey, &oldVec);
            if (s.IsNotFound()) {
                // do nothing
            } else if (!s.ok()) {
                step = "Find old version from transaction";
                break;
            }
        }
        DentryVector vector(&oldVec);
        vector.Merge(vec);
        s = txn->SSet(table4Dentry_, skey, oldVec);
        if (!s.ok()) {
            step = "Insert dentry vector to tranasction";
            break;
        }
        s = SetAppliedIndex(txn.get(), logIndex);
        if (!s.ok()) {
            step = "Insert applied index to tranasction";
            break;
        }
        uint64_t count = nDentry_;
        vector.Confirm(&count);
        s = SetDentryCount(txn.get(), count);
        if (!s.ok()) {
            step = "Insert dentry count to transaction";
            break;
        }
        s = txn->Commit();
        if (!s.ok()) {
            step = "Insert dentry vector";
            break;
        }
        nDentry_ = count;
        return MetaStatusCode::OK;
    } while (false);
    LOG(ERROR) << step << " failed, status = " << s.ToString();
    if (txn != nullptr && !txn->Rollback().ok()) {
        LOG(ERROR) << "Rollback insert dentry transaction failed, status = "
                   << s.ToString();
    }
    return MetaStatusCode::STORAGE_INTERNAL_ERROR;
}

MetaStatusCode DentryStorage::Delete(const Dentry& dentry, int64_t logIndex) {
    WriteLockGuard lg(rwLock_);

    Dentry out;
    DentryVec vec;
    const char* step = "Begin transaction";
    std::shared_ptr<storage::StorageTransaction> txn;
    storage::Status s;
    do {
        txn = kvStorage_->BeginTransaction();
        if (txn == nullptr) {
            break;
        }
        uint64_t count = nDentry_;
        s = SetAppliedIndex(txn.get(), logIndex);
        if (!s.ok()) {
            step = "Insert applied index to transaction";
            break;
        }
        MetaStatusCode rc = Find(txn.get(), dentry, &out, &vec, &count);
        if (rc == MetaStatusCode::NOT_FOUND) {
            // NOTE: we should commit transaction
            // even if rc is NOT_FOUND
            // because Find() maybe write dentry count to rocksdb
            s = txn->Commit();
            if (!s.ok()) {
                step = "Commit transaction";
                break;
            }
            nDentry_ = count;
            return MetaStatusCode::NOT_FOUND;
        } else if (rc != MetaStatusCode::OK) {
            step = "Find dentry";
            break;
        }
        DentryVector vector(&vec);
        vector.Delete(out);
        std::string skey = DentryKey(dentry);
        if (vec.dentrys_size() == 0) {
            s = txn->SDel(table4Dentry_, skey);
        } else {
            s = txn->SSet(table4Dentry_, skey, vec);
        }
        if (!s.ok()) {
            step = "Delete dentry vector from transaction";
            break;
        }
        // NOTE: we should use count variable instead of nDentry_
        // (it means that we should not reset count to nDentry_)
        // count is newest version of dentry count
        vector.Confirm(&count);
        s = SetDentryCount(txn.get(), count);
        if (!s.ok()) {
            step = "Insert applied index to transaction";
            break;
        }
        s = txn->Commit();
        if (!s.ok()) {
            step = "Delete dentry vector";
            break;
        }
        nDentry_ = count;
        return MetaStatusCode::OK;
    } while (false);
    LOG(ERROR) << step << " failed, status = " << s.ToString();
    if (txn != nullptr && !txn->Rollback().ok()) {
        LOG(ERROR) << "Rollback transaction failed";
    }
    return MetaStatusCode::STORAGE_INTERNAL_ERROR;
}

MetaStatusCode DentryStorage::Get(Dentry* dentry) {
    ReadLockGuard lg(rwLock_);

    Dentry out;
    DentryVec vec;
    MetaStatusCode rc = Find(*dentry, &out, &vec);
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
                                   std::vector<Dentry>* dentrys, uint32_t limit,
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
    VLOG(1) << "ListDentry request: dentry = (" << dentry.ShortDebugString()
            << ")"
            << ", onlyDir = " << onlyDir << ", limit = " << limit
            << ", lower key = " << lower << ", seekTimes = " << seekTimes
            << ", dentrySize = " << dentrys->size()
            << ", costUs = " << time.u_elapsed();
    return MetaStatusCode::OK;
}

MetaStatusCode DentryStorage::PrepareTx(
    const std::vector<Dentry>& dentrys,
    const metaserver::TransactionRequest& txRequest, int64_t logIndex) {
    WriteLockGuard lg(rwLock_);
    uint64_t count = nDentry_;
    Status s;
    const char* step = "Begin transaction";
    std::shared_ptr<storage::StorageTransaction> txn;
    do {
        txn = kvStorage_->BeginTransaction();
        if (txn == nullptr) {
            break;
        }
        bool quit = false;
        for (const auto& dentry : dentrys) {
            DentryVec vec;
            DentryVector vector(&vec);
            std::string skey = DentryKey(dentry);
            s = txn->SGet(table4Dentry_, skey, &vec);
            if (!s.ok() && !s.IsNotFound()) {
                step = "Get dentry from transaction";
                quit = true;
                break;
            }
            // OK || NOT_FOUND
            vector.Insert(dentry);
            s = txn->SSet(table4Dentry_, skey, vec);
            if (!s.ok()) {
                step = "Insert dentry to transaction";
                quit = true;
                break;
            }
            vector.Confirm(&count);
        }
        if (quit) {
            break;
        }
        s = SetAppliedIndex(txn.get(), logIndex);
        if (!s.ok()) {
            step = "Insert applied index to transaction";
            break;
        }
        s = SetPendingTx(txn.get(), txRequest);
        if (!s.ok()) {
            step = "Insert tx request to transaction";
            break;
        }
        s = txn->Commit();
        if (!s.ok()) {
            step = "Commit transaction";
            break;
        }
        nDentry_ = count;
        return MetaStatusCode::OK;
    } while (false);
    LOG(ERROR) << step << " failed, status = " << s.ToString();
    if (txn != nullptr && !txn->Rollback().ok()) {
        LOG(ERROR) << "Rollback transaction fail";
    }
    return MetaStatusCode::STORAGE_INTERNAL_ERROR;
}

MetaStatusCode DentryStorage::CommitTx(const std::vector<Dentry>& dentrys,
                                       int64_t logIndex) {
    if (logIndex <= handleTxIndex_) {
        // NOTE: if we enter here
        // means that this log entry is "half apply"
        // there are two parts in HandleRenameTx:
        //      * Commit last one  (1)
        //      * Prepare this one (2)
        // if (1) already write to rocksdb, but (2) doesn't
        // we enter here
        LOG(INFO) << "Log entry already be applied, index = " << logIndex
                  << " handle tx index = " << handleTxIndex_;
        return MetaStatusCode::IDEMPOTENCE_OK;
    }
    WriteLockGuard lg(rwLock_);
    Status s;
    const char* step = "Begin transaction";
    std::shared_ptr<storage::StorageTransaction> txn;
    do {
        txn = kvStorage_->BeginTransaction();
        if (txn == nullptr) {
            break;
        }
        uint64_t count = nDentry_;
        bool quit = false;
        for (const auto& dentry : dentrys) {
            Dentry out;
            DentryVec vec;
            std::string skey = DentryKey(dentry);
            MetaStatusCode rc = MetaStatusCode::OK;
            rc = Find(txn.get(), dentry, &out, &vec, &count);
            if (rc != MetaStatusCode::OK && rc != MetaStatusCode::NOT_FOUND) {
                step = "Find dentry from transaction";
                quit = true;
                break;
            }
        }
        if (quit) {
            break;
        }
        s = SetHandleTxIndex(txn.get(), logIndex);
        if (!s.ok()) {
            step = "Insert handle tx index to transaction";
            break;
        }
        s = ClearPendingTx(txn.get());
        if (!s.ok()) {
            step = "Delete pending tx from transaction";
            break;
        }
        s = txn->Commit();
        if (!s.ok()) {
            step = "Commit transaction";
            break;
        }
        nDentry_ = count;
        return MetaStatusCode::OK;
    } while (false);
    LOG(ERROR) << step << " failed, status = " << s.ToString();
    if (txn != nullptr && !txn->Rollback().ok()) {
        LOG(ERROR) << "Rollback transaction failed";
    }
    return MetaStatusCode::STORAGE_INTERNAL_ERROR;
}

MetaStatusCode DentryStorage::RollbackTx(const std::vector<Dentry>& dentrys,
                                         int64_t logIndex) {
    if (logIndex <= handleTxIndex_) {
        // NOTE: if we enter here
        // means that this log entry is "half apply"
        // there are two parts in HandleRenameTx:
        //      * Commit last one  (1)
        //      * Prepare this one (2)
        // if (1) already write to rocksdb, but (2) doesn't
        // we enter here
        LOG(INFO) << "Log entry already be applied, index = " << logIndex
                  << " handle tx index = " << handleTxIndex_;
        return MetaStatusCode::IDEMPOTENCE_OK;
    }
    WriteLockGuard lg(rwLock_);
    Status s;
    const char* step = "Begin transaction";
    std::shared_ptr<storage::StorageTransaction> txn;
    do {
        txn = kvStorage_->BeginTransaction();
        if (txn == nullptr) {
            break;
        }
        uint64_t count = nDentry_;
        bool quit = false;
        for (const auto& dentry : dentrys) {
            DentryVec vec;
            DentryVector vector(&vec);
            std::string skey = DentryKey(dentry);
            s = txn->SGet(table4Dentry_, skey, &vec);
            if (!s.ok() && !s.IsNotFound()) {
                step = "Find dentry";
                quit = true;
                break;
            }
            // OK || NOT_FOUND
            vector.Delete(dentry);
            if (vec.dentrys_size() == 0) {  // delete directly
                s = txn->SDel(table4Dentry_, skey);
            } else {
                s = txn->SSet(table4Dentry_, skey, vec);
            }
            if (!s.ok()) {
                step = "Delete dentry from transaction";
                quit = true;
                break;
            }
            vector.Confirm(&count);
        }
        if (quit) {
            break;
        }
        s = SetDentryCount(txn.get(), count);
        if (!s.ok()) {
            step = "Insert dentry count to transaction";
            break;
        }
        s = SetHandleTxIndex(txn.get(), logIndex);
        if (!s.ok()) {
            step = "Insert handle tx index to transaction";
            break;
        }
        s = ClearPendingTx(txn.get());
        if (!s.ok()) {
            step = "Delete pending tx from transaction";
            break;
        }
        s = txn->Commit();
        if (!s.ok()) {
            step = "Commit transaction";
            break;
        }
        nDentry_ = count;
        return MetaStatusCode::OK;
    } while (false);
    LOG(ERROR) << step << " failed, status = " << s.ToString();
    if (txn != nullptr && !txn->Rollback().ok()) {
        LOG(ERROR) << "Rollback transaction failed";
    }
    return MetaStatusCode::STORAGE_INTERNAL_ERROR;
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

// NOTE: we will clear all apply metadata in `Clear()`
// when follower replay logs on this snapshot, it may cause
// repeat apply log entries, and raise some errors
// but we know this partition will be clear at the end of logs
MetaStatusCode DentryStorage::Clear() {
    // FIXME: non-atomic clear operations
    // NOTE: clear operations non-atomic is acceptable
    // because if we fail stop, we will replay
    // raft logs and clear it again
    WriteLockGuard lg(rwLock_);
    Status s = kvStorage_->SClear(table4Dentry_);
    if (!s.ok()) {
        LOG(ERROR) << "Clear dentry table failed, status = " << s.ToString();
        return MetaStatusCode::STORAGE_INTERNAL_ERROR;
    }
    std::shared_ptr<storage::StorageTransaction> txn;
    const char* step = "Begin transaction";
    do {
        txn = kvStorage_->BeginTransaction();
        if (txn == nullptr) {
            break;
        }
        s = DelDentryCount(txn.get());
        if (!s.ok()) {
            step = "Delete dentry count";
            break;
        }
        s = DelPendingTx(txn.get());
        if (!s.ok()) {
            step = "Delete pending tx";
            break;
        }
        s = DelAppliedIndex(txn.get());
        if (!s.ok()) {
            step = "Delete applied index";
            break;
        }
        s = DelHandleTxIndex(txn.get());
        if (!s.ok()) {
            step = "Delete handle tx index";
            break;
        }
        s = txn->Commit();
        if (!s.ok()) {
            step = "Commit clear dentry table transaction";
            break;
        }
        nDentry_ = 0;
        return MetaStatusCode::OK;
    } while (false);
    if (txn != nullptr && !txn->Rollback().ok()) {
        LOG(ERROR) << "Rollback transaction failed";
    }
    LOG(ERROR) << step << " failed, status = " << s.ToString();
    return MetaStatusCode::STORAGE_INTERNAL_ERROR;
}

}  // namespace metaserver
}  // namespace curvefs
