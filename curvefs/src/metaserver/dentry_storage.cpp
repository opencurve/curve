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

namespace storage {
    DECLARE_int32(tx_lock_ttl_ms);
}

using ::curve::common::ReadLockGuard;
using ::curve::common::StringStartWith;
using ::curve::common::WriteLockGuard;
using ::curvefs::metaserver::storage::Key4Dentry;
using ::curvefs::metaserver::storage::Prefix4AllDentry;
using ::curvefs::metaserver::storage::Prefix4SameParentDentry;
using ::curvefs::metaserver::storage::Prefix4TxWrite;
using ::curvefs::metaserver::storage::Key4TxWrite;
using ::curvefs::metaserver::storage::Status;
using ::curvefs::metaserver::storage::FLAGS_tx_lock_ttl_ms;

const char* DentryStorage::kDentryAppliedKey("dentry");
const char* DentryStorage::kDentryCountKey("count");
const char* DentryStorage::kHandleTxKey("handleTx");
const char* DentryStorage::kPendingTxKey("pendingTx");
const char* DentryStorage::kTxLatestCommit("latestCommit");

bool operator==(const Dentry& lhs, const Dentry& rhs) {
    return EQUAL(fsid) && EQUAL(parentinodeid) && EQUAL(name) && EQUAL(txid) &&
           EQUAL(inodeid);
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

/*
* DentryVector is a wrapper of DentryVec
*/
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

/*
* DentryList
*/
DentryList::DentryList(std::vector<Dentry>* list, uint32_t limit,
                       const std::string& exclude, uint64_t maxTxId,
                       bool onlyDir)
    : list_(list),
      size_(0),
      limit_(limit),
      exclude_(exclude),
      maxTxId_(maxTxId),
      onlyDir_(onlyDir) {}

void DentryList::PushBack(DentryVec* vec, bool* realEntry) {
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
    *realEntry = true;
    list_->push_back(*last);
    VLOG(9) << "Push dentry, dentry = (" << last->ShortDebugString() << ")";
}

uint32_t DentryList::Size() {
    return size_;
}

bool DentryList::IsFull() {
    return limit_ != 0 && size_ >= limit_;
}

/*
* DentryStorage
*/
DentryStorage::DentryStorage(std::shared_ptr<KVStorage> kvStorage,
                             std::shared_ptr<NameGenerator> nameGenerator,
                             uint64_t nDentry)
    : kvStorage_(kvStorage),
      table4Dentry_(nameGenerator->GetDentryTableName()),
      table4AppliedIndex_(nameGenerator->GetAppliedIndexTableName()),
      table4Transaction_(nameGenerator->GetTransactionTableName()),
      table4DentryCount_(nameGenerator->GetDentryCountTableName()),
      table4TxLock_(nameGenerator->GetTxLockTableName()),
      table4TxWrite_(nameGenerator->GetTxWriteTableName()),
      handleTxIndex_(-1),
      nDentry_(nDentry),
      conv_(),
      latestCommit_(0) {
    // NOTE: for compatibility with older versions
    // we cannot ignore `nDentry` argument
    // try get dentry count for rocksdb
    // if we got it, replace old value
}

bool DentryStorage::Init() {
    auto s = GetDentryCount(&nDentry_);
    if (!s.ok() && !s.IsNotFound()) {
        LOG(ERROR) << "Get dentry count failed, status = " << s.ToString();
        return false;
    }
    s = GetHandleTxIndex(&handleTxIndex_);
    if (!s.ok() && !s.IsNotFound()) {
        LOG(ERROR) << "Get handle tx index failed, status = " << s.ToString();
        return false;
    }
    s = GetLatestCommit(&latestCommit_);
    if (!s.ok() && !s.IsNotFound()) {
        LOG(ERROR) << "Get latest commit failed, status = " << s.ToString();
        return false;
    }
    return true;
}

std::string DentryStorage::DentryKey(const Dentry& dentry) {
    Key4Dentry key(dentry.fsid(), dentry.parentinodeid(), dentry.name());
    return conv_.SerializeToString(key);
}

std::string DentryStorage::TxWriteKey(const Dentry& dentry, uint64_t ts) {
    Key4TxWrite key(dentry.fsid(), dentry.parentinodeid(), dentry.name(), ts);
    return conv_.SerializeToString(key);
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
// and it will clean the old txid's dentry if you specify compressOutCount to
// non-nullptr compressOutCount must point to a variable that value is equal
// with `nDentry_`
MetaStatusCode DentryStorage::Find(storage::StorageTransaction* txn,
                                   const Dentry& in, Dentry* out,
                                   DentryVec* vec, uint64_t* compressOutCount,
                                   TxLock* txLock) {
    std::string skey = DentryKey(in);
    Status s;
    // check tx lock on dentry
    if (txLock != nullptr) {
        s = txn->SGet(table4TxLock_, skey, txLock);
        if (s.ok()) {
            return MetaStatusCode::TX_KEY_LOCKED;
        } else if (!s.IsNotFound()) {
            return MetaStatusCode::STORAGE_INTERNAL_ERROR;
        }
    }

    s = txn->SGet(table4Dentry_, skey, vec);
    if (s.IsNotFound()) {
        return MetaStatusCode::NOT_FOUND;
    } else if (!s.ok()) {
        return MetaStatusCode::STORAGE_INTERNAL_ERROR;
    }

    // status = OK
    // txId here means latest dentry version
    uint64_t txId = latestCommit_ > 0 ? latestCommit_ : in.txid();
    BTree dentrys;
    DentryVector vector(vec);
    vector.Filter(txId, &dentrys);
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

#define ON_ERROR(msg)                                  \
    do                                                 \
    {                                                  \
        LOG(ERROR) << msg;                             \
        if (txn != nullptr && !txn->Rollback().ok()) { \
            LOG(ERROR) << "Rollback transaction fail"; \
        }                                              \
        return rc;                                     \
    } while (false)

#define ON_COMMIT()                                                       \
    do                                                                    \
    {                                                                     \
        s = txn->Commit();                                                \
        if (!s.ok()) {                                                    \
            rc = MetaStatusCode::STORAGE_INTERNAL_ERROR;                  \
            ON_ERROR("Commit transaction failed, " + s.ToString());       \
        }                                                                 \
        nDentry_ = count;                                                 \
        return rc;                                                        \
    } while (false)


MetaStatusCode DentryStorage::Get(Dentry* dentry, TxLock* txLock) {
    ReadLockGuard lg(rwLock_);
    Status s;
    uint64_t count = nDentry_;
    MetaStatusCode rc = MetaStatusCode::STORAGE_INTERNAL_ERROR;
    std::shared_ptr<storage::StorageTransaction> txn;
    txn = kvStorage_->BeginTransaction();
    if (txn == nullptr) {
        ON_ERROR("Begin transaction failed");
    }

    DentryVec vec;
    rc = Find(txn.get(), *dentry, dentry, &vec, nullptr, txLock);
    ON_COMMIT();
}

MetaStatusCode DentryStorage::List(const Dentry& dentry,
                                   std::vector<Dentry>* dentrys, uint32_t limit,
                                   bool onlyDir,
                                   TxLock* txLock) {
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
    std::string lower = conv_.SerializeToString(key);  // "1:1:", "1:1:dir"

    // 3. iterator key/value pair one by one
    auto iterator = kvStorage_->SSeek(table4Dentry_, lower);
    iterator->DisablePrefixChecking();
    if (iterator->Status() < 0) {
        return MetaStatusCode::STORAGE_INTERNAL_ERROR;
    }

    // get newest dentry version
    uint64_t txId = latestCommit_ > 0 ? latestCommit_ : dentry.txid();
    DentryVec current;
    DentryList list(dentrys, limit, name, txId, onlyDir);
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

        bool realEntry = false;
        list.PushBack(&current, &realEntry);
        // check dentry tx lock
        if (txLock != nullptr && realEntry) {
            Status s = kvStorage_->SGet(table4TxLock_, skey, txLock);
            if (s.ok()) {
                return MetaStatusCode::TX_KEY_LOCKED;
            } else if (!s.IsNotFound()) {
                return MetaStatusCode::STORAGE_INTERNAL_ERROR;
            }
        }

        if (list.IsFull()) {
            break;
        }
    }
    time.stop();
    VLOG(1) << "ListDentry request: dentry = (" << dentry.ShortDebugString()
            << "), onlyDir = " << onlyDir << ", limit = " << limit
            << ", lower key = " << lower << ", seekTimes = " << seekTimes
            << ", dentrySize = " << dentrys->size()
            << ", costUs = " << time.u_elapsed();
    return MetaStatusCode::OK;
}

MetaStatusCode DentryStorage::Insert(
    const Dentry& dentry, int64_t logIndex, TxLock* txLock) {
    WriteLockGuard lg(rwLock_);
    storage::Status s;
    uint64_t count = nDentry_;
    MetaStatusCode rc = MetaStatusCode::STORAGE_INTERNAL_ERROR;
    std::shared_ptr<storage::StorageTransaction> txn;
    txn = kvStorage_->BeginTransaction();
    if (txn == nullptr) {
        ON_ERROR("Begin transaction failed");
    }
    // 1. set applied index
    s = SetAppliedIndex(txn.get(), logIndex);
    if (!s.ok()) {
        ON_ERROR("Insert applied index to transaction");
    }
    // find dentry
    Dentry out;
    DentryVec vec;
    rc = Find(txn.get(), dentry, &out, &vec, &count, txLock);
    if (rc == MetaStatusCode::TX_KEY_LOCKED) {
        ON_COMMIT();
    }
    if (rc == MetaStatusCode::OK) {
        if (BelongSomeOne(out, dentry)) {
            rc = MetaStatusCode::IDEMPOTENCE_OK;
        } else {
            rc = MetaStatusCode::DENTRY_EXIST;
        }
        ON_COMMIT();
    } else if (rc != MetaStatusCode::NOT_FOUND) {
        ON_ERROR("Find dentry failed");
    }
    // rc == MetaStatusCode::NOT_FOUND
    DentryVector vector(&vec);
    vector.Insert(dentry);
    s = txn->SSet(table4Dentry_, DentryKey(dentry), vec);
    if (!s.ok()) {
        ON_ERROR("Insert dentry to transaction");
    }
    vector.Confirm(&count);
    s = SetDentryCount(txn.get(), count);
    if (!s.ok()) {
        ON_ERROR("Insert dentry count to transaction");
    }
    rc = MetaStatusCode::OK;
    ON_COMMIT();
}

MetaStatusCode DentryStorage::Insert(const DentryVec& vec, bool merge,
                                     int64_t logIndex) {
    WriteLockGuard lg(rwLock_);
    storage::Status s;
    uint64_t count = nDentry_;
    MetaStatusCode rc = MetaStatusCode::STORAGE_INTERNAL_ERROR;
    std::shared_ptr<storage::StorageTransaction> txn;
    txn = kvStorage_->BeginTransaction();
    if (txn == nullptr) {
        ON_ERROR("Begin transaction failed");
    }

    DentryVec oldVec;
    std::string skey = DentryKey(vec.dentrys(0));
    if (merge) {  // for old version dumpfile (v1)
        s = txn->SGet(table4Dentry_, skey, &oldVec);
        if (s.IsNotFound()) {
            // do nothing
        } else if (!s.ok()) {
            ON_ERROR("Find old version from transaction");
        }
    }
    DentryVector vector(&oldVec);
    vector.Merge(vec);
    s = txn->SSet(table4Dentry_, skey, oldVec);
    if (!s.ok()) {
        ON_ERROR("Insert dentry vector to tranasction");
    }
    s = SetAppliedIndex(txn.get(), logIndex);
    if (!s.ok()) {
        ON_ERROR("Insert applied index to tranasction");
    }
    vector.Confirm(&count);
    s = SetDentryCount(txn.get(), count);
    if (!s.ok()) {
        ON_ERROR("Insert dentry count to transaction");
    }
    rc = MetaStatusCode::OK;
    ON_COMMIT();
}

MetaStatusCode DentryStorage::Delete(
    const Dentry& dentry, int64_t logIndex, TxLock* txLock) {
    WriteLockGuard lg(rwLock_);
    Status s;
    uint64_t count = nDentry_;
    MetaStatusCode rc = MetaStatusCode::STORAGE_INTERNAL_ERROR;
    std::shared_ptr<storage::StorageTransaction> txn;
    txn = kvStorage_->BeginTransaction();
    if (txn == nullptr) {
        ON_ERROR("Begin transaction failed");
    }
    s = SetAppliedIndex(txn.get(), logIndex);
    if (!s.ok()) {
        ON_ERROR("Insert applied index to transaction");
    }
    Dentry out;
    DentryVec vec;
    rc = Find(txn.get(), dentry, &out, &vec, &count, txLock);
    if (rc == MetaStatusCode::TX_KEY_LOCKED) {
        ON_COMMIT();
    }
    if (rc == MetaStatusCode::NOT_FOUND) {
        ON_COMMIT();
    } else if (rc != MetaStatusCode::OK) {
        ON_ERROR("Find dentry failed");
    }
    // OK
    DentryVector vector(&vec);
    vector.Delete(out);
    std::string skey = DentryKey(dentry);
    if (vec.dentrys_size() == 0) {
        s = txn->SDel(table4Dentry_, skey);
    } else {
        s = txn->SSet(table4Dentry_, skey, vec);
    }
    if (!s.ok()) {
        ON_ERROR("Delete dentry vector from transaction");
    }
    // NOTE: we should use count variable instead of nDentry_
    // (it means that we should not reset count to nDentry_)
    // count is newest version of dentry count
    vector.Confirm(&count);
    s = SetDentryCount(txn.get(), count);
    if (!s.ok()) {
        ON_ERROR("Insert dentry count to transaction");
    }
    rc = MetaStatusCode::OK;
    ON_COMMIT();
}

MetaStatusCode DentryStorage::PrepareTx(
    const std::vector<Dentry>& dentrys,
    const metaserver::TransactionRequest& txRequest, int64_t logIndex) {
    WriteLockGuard lg(rwLock_);
    Status s;
    uint64_t count = nDentry_;
    MetaStatusCode rc = MetaStatusCode::STORAGE_INTERNAL_ERROR;
    std::shared_ptr<storage::StorageTransaction> txn;
    txn = kvStorage_->BeginTransaction();
    if (txn == nullptr) {
        ON_ERROR("Begin transaction failed");
    }
    for (const auto& dentry : dentrys) {
        DentryVec vec;
        DentryVector vector(&vec);
        std::string skey = DentryKey(dentry);
        s = txn->SGet(table4Dentry_, skey, &vec);
        if (!s.ok() && !s.IsNotFound()) {
            ON_ERROR("Get dentry from transaction");
        }
        // OK || NOT_FOUND
        vector.Insert(dentry);
        s = txn->SSet(table4Dentry_, skey, vec);
        if (!s.ok()) {
            ON_ERROR("Insert dentry to transaction");
        }
        vector.Confirm(&count);
    }
    s = SetAppliedIndex(txn.get(), logIndex);
    if (!s.ok()) {
        ON_ERROR("Insert applied index to transaction");
    }
    s = SetPendingTx(txn.get(), txRequest);
    if (!s.ok()) {
        ON_ERROR("Insert tx request to transaction");
    }
    rc = MetaStatusCode::OK;
    ON_COMMIT();
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
    uint64_t count = nDentry_;
    MetaStatusCode rc = MetaStatusCode::STORAGE_INTERNAL_ERROR;
    std::shared_ptr<storage::StorageTransaction> txn;
    txn = kvStorage_->BeginTransaction();
    if (txn == nullptr) {
        ON_ERROR("Begin transaction failed");
    }
    for (const auto& dentry : dentrys) {
        Dentry out;
        DentryVec vec;
        rc = Find(txn.get(), dentry, &out, &vec, &count, nullptr);
        if (rc != MetaStatusCode::OK && rc != MetaStatusCode::NOT_FOUND) {
            ON_ERROR("Find dentry from transaction");
        }
    }
    s = SetHandleTxIndex(txn.get(), logIndex);
    if (!s.ok()) {
        ON_ERROR("Insert handle tx index to transaction");
    }
    s = ClearPendingTx(txn.get());
    if (!s.ok()) {
        ON_ERROR("Delete pending tx from transaction");
    }
    rc = MetaStatusCode::OK;
    ON_COMMIT();
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
    uint64_t count = nDentry_;
    MetaStatusCode rc = MetaStatusCode::STORAGE_INTERNAL_ERROR;
    std::shared_ptr<storage::StorageTransaction> txn;
    txn = kvStorage_->BeginTransaction();
    if (txn == nullptr) {
        ON_ERROR("Begin transaction failed");
    }
    for (const auto& dentry : dentrys) {
        DentryVec vec;
        DentryVector vector(&vec);
        std::string skey = DentryKey(dentry);
        s = txn->SGet(table4Dentry_, skey, &vec);
        if (!s.ok() && !s.IsNotFound()) {
            ON_ERROR("Get dentry from transaction");
        }
        // OK || NOT_FOUND
        vector.Delete(dentry);
        if (vec.dentrys_size() == 0) {  // delete directly
            s = txn->SDel(table4Dentry_, skey);
        } else {
            s = txn->SSet(table4Dentry_, skey, vec);
        }
        if (!s.ok()) {
            ON_ERROR("Delete dentry from transaction");
        }
        vector.Confirm(&count);
    }
    s = SetDentryCount(txn.get(), count);
    if (!s.ok()) {
        ON_ERROR("Insert dentry count to transaction");
    }
    s = SetHandleTxIndex(txn.get(), logIndex);
    if (!s.ok()) {
        ON_ERROR("Insert handle tx index to transaction");
    }
    s = ClearPendingTx(txn.get());
    if (!s.ok()) {
        ON_ERROR("Delete pending tx from transaction");
    }
    rc = MetaStatusCode::OK;
    ON_COMMIT();
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
    s = kvStorage_->SClear(table4TxWrite_);
    if (!s.ok()) {
        LOG(ERROR) << "Clear tx write table failed, status = " << s.ToString();
        return MetaStatusCode::STORAGE_INTERNAL_ERROR;
    }
    s = kvStorage_->SClear(table4TxLock_);
    if (!s.ok()) {
        LOG(ERROR) << "Clear tx lock table failed, status = " << s.ToString();
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

MetaStatusCode DentryStorage::GetLastTxWriteTs(storage::StorageTransaction* txn,
    const Dentry& dentry, uint64_t* commitTs) {
    // 1. prepare seek lower key
    Prefix4TxWrite prefix;
    prefix.fsId = dentry.fsid();
    prefix.parentInodeId = dentry.parentinodeid();
    prefix.name = dentry.name();
    std::string sprefix = conv_.SerializeToString(prefix);  // "1:1:name/"

    // 2. iterator key/value pair one by one
    auto iterator = txn->SSeek(table4TxWrite_, sprefix);
    if (iterator->Status() < 0) {
        LOG(ERROR) << "failed to get iterator for prefix" << sprefix;
        return MetaStatusCode::STORAGE_INTERNAL_ERROR;
    }

    std::string lastWriteKey;
    std::vector<std::string> toDelete;
    butil::Timer time;
    uint32_t seekTimes = 0;
    uint32_t compressCount = 0;
    time.start();
    for (iterator->SeekToFirst(); iterator->Valid(); iterator->Next()) {
        seekTimes++;
        lastWriteKey = iterator->Key();
        TxWrite value;
        if (!iterator->ParseFromValue(&value)) {
            LOG(ERROR) << "parse value failed, key = " << lastWriteKey;
            return MetaStatusCode::PARSE_FROM_STRING_FAILED;
        }
        if (value.kind() == TxWriteKind::Rollback) {
            continue;
        }
        toDelete.push_back(lastWriteKey);
    }
    time.stop();

    if (seekTimes == 0) {
        *commitTs = 0;
        return MetaStatusCode::OK;
    }
    Key4TxWrite key;
    if (!conv_.ParseFromString(lastWriteKey, &key)) {
        LOG(ERROR) << "parse key failed, key = " << lastWriteKey;
        return MetaStatusCode::PARSE_FROM_STRING_FAILED;
    }
    *commitTs = key.ts;
    compressCount = toDelete.size() == 0 ? 0 : toDelete.size() - 1;
    for (int i = 0; i < compressCount; i++) {
        auto s = txn->SDel(table4TxWrite_, toDelete[i]);
        if (!s.ok()) {
            LOG(ERROR) << "delete tx write failed, key = " << toDelete[i];
            return MetaStatusCode::STORAGE_INTERNAL_ERROR;
        }
    }
    VLOG(1)   << "GetLastTxWriteTs request: dentry = ("
              << dentry.ShortDebugString() << ")"
              << ", lower key = " << sprefix << ", seekTimes = " << seekTimes
              << ", costUs = " << time.u_elapsed()
              << ", compressCount = " << compressCount;
    return MetaStatusCode::OK;
}

storage::Status DentryStorage::GetLatestCommit(uint64_t* statTs) {
    TS commitTs;
    Status s = kvStorage_->SGet(table4TxWrite_, kTxLatestCommit, &commitTs);
    if (s.ok()) {
        *statTs = commitTs.ts();
    }
    return s;
}

storage::Status DentryStorage::SetLatestCommit(
    storage::StorageTransaction* txn, uint64_t ts) {
    if (latestCommit_ >= ts) {
        return Status::OK();
    }
    TS commitTs;
    commitTs.set_ts(ts);
    Status s = txn->SSet(table4TxWrite_, kTxLatestCommit, commitTs);
    if (s.ok()) {
        latestCommit_ = ts;
    }
    return s;
}

// based on tx lock not exist
MetaStatusCode DentryStorage::CheckTxStatus(storage::StorageTransaction* txn,
    const std::string& primaryKey, uint64_t ts) {
    Key4Dentry key;
    if (!key.ParseFromString(primaryKey)) {
        return MetaStatusCode::PARSE_FROM_STRING_FAILED;
    }
    Key4TxWrite wkey(key.fsId, key.parentInodeId, key.name, ts);
    std::string skey = wkey.SerializeToString();
    TxWrite txWrite;
    Status s = txn->SGet(table4TxWrite_, skey, &txWrite);
    if (s.IsNotFound()) {
        return MetaStatusCode::TX_COMMITTED;
    } else if (!s.ok()) {
        return MetaStatusCode::STORAGE_INTERNAL_ERROR;
    }

    // status = OK
    if (txWrite.startts() == ts && txWrite.kind() == TxWriteKind::Rollback) {
        return MetaStatusCode::TX_ROLLBACKED;
    }
    return MetaStatusCode::TX_COMMITTED;
}

storage::Status DentryStorage::SetTxWrite(storage::StorageTransaction* txn,
    const std::string& key, const TxWrite& txWrite) {
    return txn->SSet(table4TxWrite_, key, txWrite);
}

storage::Status DentryStorage::GetTxLock(
    storage::StorageTransaction* txn, const std::string& key, TxLock* out) {
    return txn->SGet(table4TxLock_, key, out);
}

storage::Status DentryStorage::SetTxLock(storage::StorageTransaction* txn,
    const std::string& key, const TxLock& txLock) {
    return txn->SSet(table4TxLock_, key, txLock);
}

storage::Status DentryStorage::DelTxLock(
    storage::StorageTransaction* txn, const std::string& key) {
    return txn->SDel(table4TxLock_, key);
}

MetaStatusCode DentryStorage::WriteTx(storage::StorageTransaction* txn,
    const Dentry& dentry, TxLock txLock, uint64_t* count) {
    // 1. set tx lock
    txLock.set_ttl(FLAGS_tx_lock_ttl_ms);
    Status s = SetTxLock(txn, DentryKey(dentry), txLock);
    if (!s.ok()) {
        return MetaStatusCode::STORAGE_INTERNAL_ERROR;
    }
    // 2. set dentry data and compress old version data
    Dentry out;
    DentryVec vec;
    auto rc = Find(txn, dentry, &out, &vec, count, nullptr);
    if (rc != MetaStatusCode::OK && rc != MetaStatusCode::NOT_FOUND) {
        return rc;
    }
    DentryVector vector(&vec);
    VLOG(3) << "WriteTx before insert = " << vec.DebugString();
    vector.Insert(dentry);
    VLOG(3) << "WriteTx after insert = " << vec.DebugString();
    s = txn->SSet(table4Dentry_, DentryKey(dentry), vec);
    if (!s.ok()) {
        return MetaStatusCode::STORAGE_INTERNAL_ERROR;
    }
    vector.Confirm(count);
    s = SetDentryCount(txn, *count);
    if (!s.ok()) {
        return MetaStatusCode::STORAGE_INTERNAL_ERROR;
    }
    return MetaStatusCode::OK;
}

MetaStatusCode DentryStorage::PrewriteTx(const std::vector<Dentry>& dentrys,
    TxLock txLock, int64_t logIndex, TxLock* out) {
    WriteLockGuard lg(rwLock_);
    Status s;
    uint64_t count = nDentry_;
    MetaStatusCode rc = MetaStatusCode::STORAGE_INTERNAL_ERROR;
    std::shared_ptr<storage::StorageTransaction> txn;
    txn = kvStorage_->BeginTransaction();
    if (txn == nullptr) {
        ON_ERROR("Begin transaction failed");
    }
    // 1. set applied index
    s = SetAppliedIndex(txn.get(), logIndex);
    if (!s.ok()) {
        ON_ERROR("Insert applied index to transaction failed");
    }
    for (int i = 0; i < dentrys.size(); i++) {
        // 2. check write confict
        uint64_t commitTs = 0;
        if (MetaStatusCode::OK !=
            GetLastTxWriteTs(txn.get(), dentrys[i], &commitTs)) {
            ON_ERROR("Get last tx write ts failed");
        }
        if (commitTs >= txLock.startts()) {
            rc = MetaStatusCode::TX_WRITE_CONFLICT;
            ON_ERROR("Tx write conflict");
        }
        // 3. check tx lock
        s = GetTxLock(txn.get(), DentryKey(dentrys[i]), out);
        if (s.ok()) {
            if (out->startts() == txLock.startts()) {
                continue;
            }
            out->set_index(i);
            rc = MetaStatusCode::TX_KEY_LOCKED;
            ON_COMMIT();
        } else if (!s.IsNotFound()) {
            ON_ERROR("Get tx lock failed");
        }
        // 4. write tx
        if (WriteTx(txn.get(), dentrys[i], txLock, &count)
            != MetaStatusCode::OK) {
            ON_ERROR("Write tx failed");
        }
    }
    rc = MetaStatusCode::OK;
    ON_COMMIT();
}

MetaStatusCode DentryStorage::CheckTxStatus(const std::string& primaryKey,
    uint64_t startTs, uint64_t curTimestamp, int64_t logIndex) {
    WriteLockGuard lg(rwLock_);
    Status s;
    uint64_t count = nDentry_;
    MetaStatusCode rc = MetaStatusCode::STORAGE_INTERNAL_ERROR;
    std::shared_ptr<storage::StorageTransaction> txn;
    txn = kvStorage_->BeginTransaction();
    if (txn == nullptr) {
        ON_ERROR("Begin transaction failed");
    }
    // 1. set applied index
    s = SetAppliedIndex(txn.get(), logIndex);
    if (!s.ok()) {
        ON_ERROR("Insert applied index to transaction failed");
    }
    // 2. check tx lock
    TxLock txLock;
    s = GetTxLock(txn.get(), primaryKey, &txLock);
    if (s.ok()) {
        // inprogress or timeout
        if (curTimestamp > txLock.timestamp() + txLock.ttl()) {
            rc = MetaStatusCode::TX_TIMEOUT;
            ON_COMMIT();
        } else {
            rc = MetaStatusCode::TX_INPROGRESS;
            ON_COMMIT();
        }
    } else if (s.IsNotFound()) {
        // committed or rollbacked
        rc = CheckTxStatus(txn.get(), primaryKey, startTs);
        ON_COMMIT();
    } else {
        ON_ERROR("Get tx lock failed");
    }
    rc = MetaStatusCode::OK;
    ON_COMMIT();
}

MetaStatusCode DentryStorage::ResolveTxLock(const Dentry& dentry,
    uint64_t startTs, uint64_t commitTs, int64_t logIndex) {
    WriteLockGuard lg(rwLock_);
    Status s;
    uint64_t count = nDentry_;
    MetaStatusCode rc = MetaStatusCode::STORAGE_INTERNAL_ERROR;
    std::shared_ptr<storage::StorageTransaction> txn;
    txn = kvStorage_->BeginTransaction();
    if (txn == nullptr) {
        ON_ERROR("Begin transaction failed");
    }
    // 1. set applied index
    s = SetAppliedIndex(txn.get(), logIndex);
    if (!s.ok()) {
        ON_ERROR("Insert applied index to transaction failed");
    }
    TxLock outLock;
    s = GetTxLock(txn.get(), DentryKey(dentry), &outLock);
    if (s.IsNotFound()) {
        rc = MetaStatusCode::OK;
        ON_COMMIT();
    } else if (!s.ok()) {
        ON_ERROR("Get tx lock failed");
    }
    if (outLock.startts() != startTs) {
        rc = MetaStatusCode::TX_MISMATCH;
        ON_ERROR("tx lock mismatch");
    }
    // roll forward
    if (commitTs > 0) {
        if (!DelTxLock(txn.get(), DentryKey(dentry)).ok()) {
            ON_ERROR("Delete tx lock failed");
        }
        TxWrite txWrite;
        txWrite.set_startts(startTs);
        txWrite.set_kind(TxWriteKind::Commit);
        if (!SetTxWrite(txn.get(),
            TxWriteKey(dentry, commitTs), txWrite).ok()) {
            ON_ERROR("Set tx write failed");
        }
        // update latest commit
        if (!SetLatestCommit(txn.get(), commitTs).ok()) {
            ON_ERROR("update latest commit failed");
        }
    } else {
        // 1. delete tx lock
        if (!DelTxLock(txn.get(), DentryKey(dentry)).ok()) {
            ON_ERROR("Delete tx lock failed");
        }
        // 2. delete tx data with startTs
        DentryVec vec;
        DentryVector vector(&vec);
        std::string skey = DentryKey(dentry);
        s = txn->SGet(table4Dentry_, skey, &vec);
        if (!s.ok() && !s.IsNotFound()) {
            ON_ERROR("Get dentry from transaction failed");
        }
        // OK || NOT_FOUND
        Dentry preDentry(dentry);
        preDentry.set_txid(startTs);
        vector.Delete(preDentry);
        if (vec.dentrys_size() == 0) {  // delete directly
            s = txn->SDel(table4Dentry_, skey);
        } else {
            s = txn->SSet(table4Dentry_, skey, vec);
        }
        if (!s.ok()) {
            ON_ERROR("Delete dentry from transaction failed");
        }
        vector.Confirm(&count);
        // 3. set tx write
        TxWrite txWrite;
        txWrite.set_startts(startTs);
        txWrite.set_kind(TxWriteKind::Rollback);
        if (!SetTxWrite(
            txn.get(), TxWriteKey(dentry, startTs), txWrite).ok()) {
            ON_ERROR("Set tx write failed");
        }
    }
    rc = MetaStatusCode::OK;
    ON_COMMIT();
}

MetaStatusCode DentryStorage::CommitTx(const std::vector<Dentry>& dentrys,
    uint64_t startTs, uint64_t commitTs, int64_t logIndex) {
    WriteLockGuard lg(rwLock_);
    Status s;
    uint64_t count = nDentry_;
    MetaStatusCode rc = MetaStatusCode::STORAGE_INTERNAL_ERROR;
    std::shared_ptr<storage::StorageTransaction> txn;
    txn = kvStorage_->BeginTransaction();
    if (txn == nullptr) {
        ON_ERROR("Begin transaction failed");
    }
    // 1. set applied index
    s = SetAppliedIndex(txn.get(), logIndex);
    if (!s.ok()) {
        ON_ERROR("Insert applied index to transaction failed");
    }
    for (const auto& dentry : dentrys) {
        // check tx lock
        TxLock txLock;
        s = GetTxLock(txn.get(), DentryKey(dentry), &txLock);
        if (s.IsNotFound()) {
            // commited or rollbacked
            rc = CheckTxStatus(txn.get(), DentryKey(dentry), startTs);
            if (rc == MetaStatusCode::TX_COMMITTED) {
                continue;
            } else {
                ON_ERROR("tx have been rollbacked when commit");
            }
        } else if (!s.ok()) {
            ON_ERROR("Get tx lock failed");
        }
        if (txLock.startts() != startTs) {
            rc = MetaStatusCode::TX_MISMATCH;
            ON_ERROR("tx lock mismatch");
        }
        // set tx write
        TxWrite txWrite;
        txWrite.set_startts(startTs);
        txWrite.set_kind(TxWriteKind::Commit);
        if (!SetTxWrite(
            txn.get(), TxWriteKey(dentry, commitTs), txWrite).ok()) {
            ON_ERROR("Set tx write failed");
        }
        // delete tx lock
        if (!DelTxLock(txn.get(), DentryKey(dentry)).ok()) {
            ON_ERROR("Delete tx lock failed");
        }
    }
    // update latest commit
    if (!SetLatestCommit(txn.get(), startTs).ok()) {
        ON_ERROR("update latest commit failed");
    }
    rc = MetaStatusCode::OK;
    ON_COMMIT();
}

}  // namespace metaserver
}  // namespace curvefs
