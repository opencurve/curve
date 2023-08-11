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

#ifndef CURVEFS_SRC_METASERVER_DENTRY_STORAGE_H_
#define CURVEFS_SRC_METASERVER_DENTRY_STORAGE_H_

#include <functional>
#include <list>
#include <memory>
#include <string>
#include <vector>

#include "absl/container/btree_set.h"
#include "curvefs/proto/metaserver.pb.h"
#include "curvefs/src/metaserver/storage/converter.h"
#include "curvefs/src/metaserver/storage/status.h"
#include "curvefs/src/metaserver/storage/storage.h"
#include "src/common/concurrent/rw_lock.h"

namespace curvefs {
namespace metaserver {

using ::curve::common::RWLock;
using ::curvefs::metaserver::storage::Converter;
using ::curvefs::metaserver::storage::Iterator;
using ::curvefs::metaserver::storage::NameGenerator;
using KVStorage = ::curvefs::metaserver::storage::KVStorage;
using BTree = absl::btree_set<Dentry>;

#define EQUAL(a) (lhs.a() == rhs.a())
#define LESS(a) (lhs.a() < rhs.a())
#define LESS2(a, b) (EQUAL(a) && LESS(b))
#define LESS3(a, b, c) (EQUAL(a) && LESS2(b, c))
#define LESS4(a, b, c, d) (EQUAL(a) && LESS3(b, c, d))

bool operator==(const Dentry& lhs, const Dentry& rhs);

bool operator<(const Dentry& lhs, const Dentry& rhs);

class DentryVector {
 public:
    explicit DentryVector(DentryVec* vec);

    void Insert(const Dentry& dentry);

    void Delete(const Dentry& dentry);

    void Merge(const DentryVec& src);

    void Filter(uint64_t maxTxId, BTree* btree);

    void Confirm(uint64_t* count);

 private:
    DentryVec* vec_;
    uint64_t nPendingAdd_;
    uint64_t nPendingDel_;
};

class DentryList {
 public:
    DentryList(std::vector<Dentry>* list, uint32_t limit,
               const std::string& exclude, uint64_t maxTxId, bool onlyDir);

    void PushBack(DentryVec* vec);

    uint32_t Size();

    bool IsFull();

 private:
    std::vector<Dentry>* list_;
    uint32_t size_;
    uint32_t limit_;
    std::string exclude_;
    uint64_t maxTxId_;
    bool onlyDir_;
};

class DentryStorage {
 public:
    DentryStorage(std::shared_ptr<KVStorage> kvStorage,
                  std::shared_ptr<NameGenerator> nameGenerator,
                  uint64_t nDentry);

    bool Init();

    MetaStatusCode Insert(const Dentry& dentry, int64_t logIndex);

    // only for loadding from snapshot
    MetaStatusCode Insert(const DentryVec& vec, bool merge, int64_t logIndex);

    MetaStatusCode Delete(const Dentry& dentry, int64_t logIndex);

    MetaStatusCode Get(Dentry* dentry);

    MetaStatusCode List(const Dentry& dentry, std::vector<Dentry>* dentrys,
                        uint32_t limit, bool onlyDir = false);

    MetaStatusCode PrepareTx(const std::vector<Dentry>& dentrys,
                             const metaserver::TransactionRequest& txRequest,
                             int64_t logIndex);

    MetaStatusCode CommitTx(const std::vector<Dentry>& dentrys,
                            int64_t logIndex);

    MetaStatusCode RollbackTx(const std::vector<Dentry>& dentrys,
                              int64_t logIndex);

    std::shared_ptr<Iterator> GetAll();

    size_t Size();

    bool Empty();

    MetaStatusCode Clear();

    MetaStatusCode GetPendingTx(metaserver::TransactionRequest* request);

    MetaStatusCode GetAppliedIndex(int64_t* index);

 private:
    std::string DentryKey(const Dentry& entry);

    bool CompressDentry(storage::StorageTransaction* txn, DentryVec* vec,
                        BTree* dentrys, uint64_t* outCount);

    MetaStatusCode Find(const Dentry& in, Dentry* out, DentryVec* vec);

    MetaStatusCode Find(storage::StorageTransaction* txn, const Dentry& in,
                        Dentry* out, DentryVec* vec,
                        uint64_t* compressOutCount);

    storage::Status SetAppliedIndex(storage::StorageTransaction* transaction,
                                    int64_t index);

    storage::Status DelAppliedIndex(storage::StorageTransaction* transaction);

    storage::Status SetHandleTxIndex(storage::StorageTransaction* transaction,
                                     int64_t index);

    storage::Status DelHandleTxIndex(storage::StorageTransaction* transaction);

    storage::Status SetPendingTx(storage::StorageTransaction* transaction,
                                 const metaserver::TransactionRequest& request);

    storage::Status DelPendingTx(storage::StorageTransaction* transaction);

    storage::Status ClearPendingTx(storage::StorageTransaction* transaction);

    storage::Status SetDentryCount(storage::StorageTransaction* transaction,
                                   uint64_t count);

    storage::Status DelDentryCount(storage::StorageTransaction* transaction);

    storage::Status GetDentryCount(uint64_t* count);

    storage::Status GetHandleTxIndex(int64_t* count);

 private:
    RWLock rwLock_;
    std::shared_ptr<KVStorage> kvStorage_;
    std::string table4Dentry_;
    std::string table4AppliedIndex_;
    std::string table4Transaction_;
    std::string table4DentryCount_;
    int64_t handleTxIndex_;
    uint64_t nDentry_;
    Converter conv_;

    static const char* kDentryCountKey;
    static const char* kDentryAppliedKey;
    static const char* kHandleTxKey;
    static const char* kPendingTxKey;
};

}  // namespace metaserver
}  // namespace curvefs

#endif  // CURVEFS_SRC_METASERVER_DENTRY_STORAGE_H_
