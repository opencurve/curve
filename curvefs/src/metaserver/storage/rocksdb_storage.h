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
 * Date: 2022-02-18
 * Author: Jingli Chen (Wine93)
 */

#ifndef CURVEFS_SRC_METASERVER_STORAGE_ROCKSDB_STORAGE_H_
#define CURVEFS_SRC_METASERVER_STORAGE_ROCKSDB_STORAGE_H_

#include <vector>
#include <memory>
#include <string>
#include <utility>
#include <unordered_map>

#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/table.h"
#include "rocksdb/options.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/utilities/transaction.h"
#include "rocksdb/utilities/transaction_db.h"
#include "src/common/concurrent/rw_lock.h"
#include "curvefs/src/metaserver/storage/utils.h"
#include "curvefs/src/metaserver/storage/storage.h"
#include "curvefs/src/metaserver/storage/rocksdb_perf.h"
#include "curvefs/src/metaserver/storage/rocksdb_storage.h"

namespace curvefs {
namespace metaserver {
namespace storage {

using ::curve::common::RWLock;
using ::curve::common::ReadLockGuard;
using ::curve::common::WriteLockGuard;
using ROCKSDB_NAMESPACE::DB;
using ROCKSDB_NAMESPACE::ColumnFamilyHandle;
using ROCKSDB_NAMESPACE::ColumnFamilyOptions;
using ROCKSDB_NAMESPACE::ColumnFamilyDescriptor;
using ROCKSDB_NAMESPACE::BlockBasedTableOptions;
using ROCKSDB_NAMESPACE::Transaction;
using ROCKSDB_NAMESPACE::TransactionDB;
using ROCKSDB_NAMESPACE::NewLRUCache;
using ROCKSDB_NAMESPACE::NewBloomFilterPolicy;
using ROCKSDB_NAMESPACE::NewFixedPrefixTransform;
using ROCKSDB_NAMESPACE::NewBlockBasedTableFactory;
using STORAGE_TYPE = KVStorage::STORAGE_TYPE;

class RocksDBStorageTest;

class RocksDBOptions {
 public:
    RocksDBOptions() {}

    explicit RocksDBOptions(StorageOptions options);

    ROCKSDB_NAMESPACE::Options DBOptions();

    ROCKSDB_NAMESPACE::TransactionDBOptions TransactionDBOptions();

    std::vector<ColumnFamilyDescriptor> ColumnFamilys();

    ROCKSDB_NAMESPACE::ReadOptions ReadOptions();

    ROCKSDB_NAMESPACE::WriteOptions WriteOptions();

 private:
    ROCKSDB_NAMESPACE::Options dbOptions_;

    std::vector<ColumnFamilyDescriptor> columnFamilies_;

    static const std::string kOrderedColumnFamilyName_;

    std::shared_ptr<rocksdb::Comparator> comparator_;
};

class RocksDBStorageComparator : public rocksdb::Comparator {
 public:
    // if slice1 < slice2, return -1
    // if slice1 > slice2, return 1
    // if slice1 == slice2, return 0
    int Compare(const rocksdb::Slice& slice1,
                const rocksdb::Slice& slice2) const override {
        std::string key1 = std::string(slice1.data(), slice1.size());
        std::string key2 = std::string(slice2.data(), slice2.size());
        size_t num1 = BinrayString2Number(key1);
        size_t num2 = BinrayString2Number(key2);
        if (num1 < num2) {
            return -1;
        } else if (num1 > num2) {
            return 1;
        }
        // n1 == n2
        if (key1 < key2) {
            return -1;
        } else if (key1 > key2) {
            return 1;
        }
        return 0;
    }

    // Ignore the following methods for now
    const char* Name() const override { return "RocksDBStorageComparator"; }

    void FindShortestSeparator(std::string*,
                               const rocksdb::Slice&) const override {}

    void FindShortSuccessor(std::string*) const override {}
};

class RocksDBStorage : public KVStorage, public StorageTransaction {
 public:
    using KeyPair = std::pair<std::string, std::string>;

 public:
    RocksDBStorage();

    explicit RocksDBStorage(StorageOptions options);

    RocksDBStorage(const RocksDBStorage& storage, Transaction* txn);

    bool Open() override;

    bool Close() override;

    STORAGE_TYPE Type() override;

    bool GetStatistics(StorageStatistics* Statistics) override;

    StorageOptions GetStorageOptions() const override;

    // unordered
    Status HGet(const std::string& name,
                const std::string& key,
                ValueType* value) override;

    Status HSet(const std::string& name,
                const std::string& key,
                const ValueType& value) override;

    Status HDel(const std::string& name, const std::string& key) override;

    std::shared_ptr<Iterator> HGetAll(const std::string& name) override;

    size_t HSize(const std::string& name) override;

    Status HClear(const std::string& name) override;

    // ordered
    Status SGet(const std::string& name,
                const std::string& key,
                ValueType* value) override;

    Status SSet(const std::string& name,
                const std::string& key,
                const ValueType& value) override;

    Status SDel(const std::string& name, const std::string& key) override;

    std::shared_ptr<Iterator> SSeek(const std::string& name,
                                    const std::string& prefix) override;

    std::shared_ptr<Iterator> SGetAll(const std::string& name) override;

    size_t SSize(const std::string& name) override;

    Status SClear(const std::string& name) override;

    std::shared_ptr<StorageTransaction> BeginTransaction() override;

    Status Commit() override;

    Status Rollback() override;

 private:
    ROCKSDB_NAMESPACE::Options DBOptions();

    ROCKSDB_NAMESPACE::TransactionDBOptions TransactionDBOptions();

    std::vector<ColumnFamilyDescriptor> ColumnFamilys();

    ROCKSDB_NAMESPACE::ReadOptions ReadOptions();

    ROCKSDB_NAMESPACE::WriteOptions WriteOptions();

    ColumnFamilyHandle* GetColumnFamilyHandle(bool ordered);

    Status ToStorageStatus(const ROCKSDB_NAMESPACE::Status& s);

    std::string ToInternalName(const std::string& name, bool ordered);

    std::string FormatInternalKey(size_t num4name, const std::string& key);

    std::string ToInternalKey(const std::string& iname, const std::string& key);

    std::string ToUserKey(const std::string& ikey);

    bool FindKey(std::string name, std::string key);

    void InsertKey(std::string name, std::string key);

    void EraseKey(std::string name, std::string key);

    size_t TableSize(std::string name);

    void ClearTable(std::string name);

    void CommitKeys();

    void RollbackKeys();

    Status Get(const std::string& name,
               const std::string& key,
               ValueType* value,
               bool ordered);

    Status Set(const std::string& name,
               const std::string& key,
               const ValueType& value,
               bool ordered);

    Status Del(const std::string& name,
               const std::string& key,
               bool ordered);

    std::shared_ptr<Iterator> Seek(const std::string& name,
                                   const std::string& prefix);

    // TODO(@Wine93): We do not support transactions for the
    // below 3 methods, maybe we should return Status::NotSupported
    // when user invoke it in transaction.
    std::shared_ptr<Iterator> GetAll(const std::string& name, bool ordered);

    size_t Size(const std::string& name, bool ordered);

    Status Clear(const std::string& name, bool ordered);

 private:
    friend class RocksDBStorageIterator;
    friend class RocksDBStorageTest;

 private:
    RWLock rwLock_;  // lock for Counter
    bool inited_;
    StorageOptions options_;
    RocksDBOptions rocksdbOptions_;
    DB* db_;
    TransactionDB* txnDB_;
    std::vector<ColumnFamilyHandle*> handles_;
    std::shared_ptr<Counter> counter_;

    // for transaction
    bool InTransaction_;
    Transaction* txn_;
    std::vector<KeyPair> pending4set_;
    std::vector<KeyPair> pending4del_;

    std::shared_ptr<RocksDBPerf> perf_;
};

inline Status RocksDBStorage::HGet(const std::string& name,
                                   const std::string& key,
                                   ValueType* value) {
    return Get(name, key, value, false);
}

inline Status RocksDBStorage::HSet(const std::string& name,
                                   const std::string& key,
                                   const ValueType& value) {
    return Set(name, key, value, false);
}

inline Status RocksDBStorage::HDel(const std::string& name,
                                   const std::string& key)  {
    return Del(name, key, false);
}

inline std::shared_ptr<Iterator> RocksDBStorage::HGetAll(
    const std::string& name) {
    return GetAll(name, false);
}

inline size_t RocksDBStorage::HSize(const std::string& name) {
    return Size(name, false);
}

inline Status RocksDBStorage::HClear(const std::string& name) {
    return Clear(name, false);
}

inline Status RocksDBStorage::SGet(const std::string& name,
                                   const std::string& key,
                                   ValueType* value) {
    return Get(name, key, value, true);
}

inline Status RocksDBStorage::SSet(const std::string& name,
                                   const std::string& key,
                                   const ValueType& value) {
    return Set(name, key, value, true);
}

inline Status RocksDBStorage::SDel(const std::string& name,
                                   const std::string& key) {
    return Del(name, key, true);
}

inline std::shared_ptr<Iterator> RocksDBStorage::SSeek(
    const std::string& name, const std::string& prefix) {
    return Seek(name, prefix);
}

inline std::shared_ptr<Iterator> RocksDBStorage::SGetAll(
    const std::string& name) {
    return GetAll(name, true);
}

inline size_t RocksDBStorage::SSize(const std::string& name) {
    return Size(name, true);
}

inline Status RocksDBStorage::SClear(const std::string& name) {
    return Clear(name, true);
}

class RocksDBStorageIterator : public Iterator {
 public:
    RocksDBStorageIterator(RocksDBStorage* storage,
                           const std::string& prefix,
                           size_t size,
                           int status,
                           bool ordered)
        : storage_(storage),
          prefix_(prefix),
          size_(size),
          status_(status),
          prefixChecking_(true),
          ordered_(ordered),
          iter_(nullptr) {
        storage_->perf_->Start(OP_GET_SNAPSHOT);
        if (status_ == 0) {
            readOptions_ = storage_->ReadOptions();
            if (storage_->InTransaction_) {
                readOptions_.snapshot = storage_->txn_->GetSnapshot();
            } else {
                readOptions_.snapshot = storage_->db_->GetSnapshot();
            }
        }
        storage_->perf_->Stop();
    }

    ~RocksDBStorageIterator() {
        storage_->perf_->Start(OP_CLEAR_SNAPSHOT);
        if (status_ == 0) {
            if (storage_->InTransaction_) {
                storage_->txn_->ClearSnapshot();
            } else {
                storage_->db_->ReleaseSnapshot(readOptions_.snapshot);
            }
        }
        storage_->perf_->Stop();
    }

    uint64_t Size() {
        return static_cast<uint64_t>(size_);
    }

    bool Valid() {
        if (nullptr == iter_) {
            return false;
        } else if (!iter_->Valid()) {
            return false;
        } else if (prefixChecking_ && !iter_->key().starts_with(prefix_)) {
            return false;
        }
        return true;
    }

    void SeekToFirst() {
        storage_->perf_->Start(OP_GET_ITERATOR);
        auto handler = storage_->GetColumnFamilyHandle(ordered_);
        if (storage_->InTransaction_) {
            iter_.reset(storage_->txn_->GetIterator(readOptions_, handler));
        } else {
            iter_.reset(storage_->db_->NewIterator(readOptions_, handler));
        }
        storage_->perf_->Stop();

        storage_->perf_->Start(OP_ITERATOR_SEEK_TO_FIRST);
        iter_->Seek(prefix_);
        storage_->perf_->Stop();
    }

    void Next() {
        storage_->perf_->Start(OP_ITERATOR_NEXT);
        iter_->Next();
        storage_->perf_->Stop();
    }

    std::string Key() {
        storage_->perf_->Start(OP_ITERATOR_GET_KEY);
        auto slice = iter_->key();
        storage_->perf_->Stop();
        auto ikey = std::string(slice.data(), slice.size());
        return storage_->ToUserKey(ikey);
    }

    std::string Value() {
        storage_->perf_->Start(OP_ITERATOR_GET_VALUE);
        auto slice = iter_->value();
        storage_->perf_->Stop();
        return std::string(slice.data(), slice.size());
    }

    bool ParseFromValue(ValueType* value) override {
        auto slice = iter_->value();
        std::string str(slice.data(), slice.size());
        if (!value->ParseFromString(str)) {
            return false;
        }
        return true;
    }

    int Status() {
        return status_;
    }

    void DisablePrefixChecking() {
        prefixChecking_ = false;
    }

 private:
    std::string prefix_;
    uint64_t size_;
    int status_;
    bool ordered_;
    bool prefixChecking_;
    std::unique_ptr<rocksdb::Iterator> iter_;
    RocksDBStorage* storage_;
    rocksdb::ReadOptions readOptions_;
};

}  // namespace storage
}  // namespace metaserver
}  // namespace curvefs

#endif  // CURVEFS_SRC_METASERVER_STORAGE_ROCKSDB_STORAGE_H_
