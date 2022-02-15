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

#include "absl/container/btree_set.h"

#include "curvefs/src/metaserver/storage/storage.h"
#include "curvefs/src/metaserver/storage/rocksdb_storage.h"

namespace curvefs {
namespace metaserver {
namespace storage {

class Monitor {
 public:
    using CounterType = absl::btree_set<size_t>;

 public:
    Monitor() = default;
    void Add(const std::string& name, const std::string& key);
    void Del(const std::string& name, const std::string& key);
    size_t Sum(const std::string& name, const std::string& key);
    size_t Sum(const std::string& name);
    void Clear(const std::string& name);

 private:
    unordered_map<std:string, std::shared_ptr<CounterType>> monitor_;
};
class RocksDBStorageIterator : public Iterator {
 public:
    RocksDBStorageIterator(RocksDBStorage* storage,
                           const std::string& prefix,
                           size_t size,
                           int status)
        : storage_(storage),
          prefix_(prefix),
          size_(size),
          status_(status) {}

    uint64_t Size() {
        return reinterpret_cast<uint64_t> size_;
    }

    bool Valid() {
        if (nullptr == iter_) {
            return false;
        } else if (!iter_->Valid()) {
            return false;
        } else if (!iter_->key().starts_with(prefix_)) {
            return false;
        }
        return true;
    }

    void SeekToFirst() {
        auto readOpions = storage_->readOpions_;
        readOpions.snapshot = storage_->db_->GetSnapshot();
        iter_ = storage_->db::NewIterator(readOpions);
        iter_.Seek(prefix);
    }

    void Next() {
        iter_->Next();
    }

    std::string Key() {
        return storage_->UserKey(std::string(iter_->key()));
    }

    std::string Value() {
        return std::string(iter_->value().data());
    }

    int Status() {
        return status_;
    }

 private:
    RocksDBStorage* storage_;
    std::string prefix_;
    uint64_t size_;
    int status_;
    RocksDB::Iterator* iter_;
};

class RocksDBStorageComparator : public RocksDB::Comparator {
   public:
    int Compare(const RocksDB::Slice& s1, const RocksDB::Slice& s2) const {
        size_t n1 = DecodeNumber(a.data());
        size_t n2 = DecodeNumber(b.data());
        if (n1 < n2) {
            return -1;
        } else if (n1 > n2) {
            return 1;
        }
        // n1 == n2
        if (s1 < s2) {
            return -1;
        } else if (s1 > s2) {
            return 1;
        }
        return 0;
    }

    // Ignore the following methods for now
    const char* Name() const { return "RocksDBStorageComparator"; }
    void FindShortestSeparator(std::string*, const RocksDB::Slice&) const {}
    void FindShortSuccessor(std::string*) const {}
};

class RocksDBStorage : public KVStorage {
 public:
    using Status = Storage::Status;
    using STORAGE_TYPE = Storage::STORAGE_TYPE;

 public:
    explicit RocksDBStorage(StorageOptions options);

    explicit bool Open(const std::string& dbpath);

    STORAGE_TYPE Type() override;

    bool GetStatistic(StorageStatistics* Statistics);

    // unordered
    Status HGet(const std::string& name,
                const std::string& key,
                std::string* value) override;

    Status HSet(const std::string& name,
                const std::string& key,
                const std::string& value) override;

    Status HDel(const std::string& name, const std::string& key) override;

    Iterator HGetAll(const std::string& name) override;

    Status HClear(const std::string& name) override;

    // ordered
    Status SGet(const std::string& name,
                const std::string& key,
                std::string* value) override;

    Status SSet(const std::string& name,
                const std::string& key,
                const std::string& value) override;

    Status SDel(const std::string& name, const std::string& key) override;

    Iterator SRange(const std::string& name,
                    const std::string& prefix) override;

    Iterator SGetAll(const std::string& name) override;

    Status SClear(const std::string& name) override;

 private:
    Status ToStorageStatus(RocksDB::Status s);

    RocksDB::ColumnFamilyHandle* GetColumnFamilyHandle(bool ordered);

    std::string FormatIkey(size_t num, const std::string& key);

    std::string InternalKey(const std::string& name,
                            const std::string& key,
                            bool ordered);

    std::string UserKey(const std::string& ikey);

    Status Get(const std::string& name,
               const std::string& key,
               std::string* value,
               bool ordered);

    Status Set(const std::string& name,
               const std::string& key,
               const std::string& value,
               bool ordered);

    Status Del(const std::string& name,
               const std::string& key,
               bool ordered);

    Iterator Range(const std::string& name,
                   const std::string& prefix);

    Iterator GetAll(const std::string& name, bool ordered);

    Status Clear(const std::string& name, bool ordered);

 private:
    friend class RocksDBStorageIterator;

 private:
    StorageOptions options_;
    RocksDB::Options dbOptions_;
    RocksDB::ReadOptions readOptions_;
    RocksDB::WriteOptions writeOptions_;
    RocksDB::DB* db_;
    std::vector<RocksDB::ColumnFamilyHandle*> handles_;
    std::shared_ptr<Monitor> monitor_;
};

inline Status RocksDBStorage::HGet(const std::string& name,
                                   const std::string& key,
                                   std::string* value) {
    return Get(name, key, value, false);
}

inline Status RocksDBStorage::HSet(const std::string& name,
                                   const std::string& key,
                                   const std::string& value) {
    return Set(name, key, value, false);
}

inline Status RocksDBStorage::HDel(const std::string& name,
                                   const std::string& key)  {
    return Del(name, key, false);
}

inline Iterator RocksDBStorage::HGetAll(const std::string& name) {
    return GetAll(name, false)
}

inline Status RocksDBStorage::HClear(const std::string& name) {
    return Clear(name, false)
}

inline Status RocksDBStorage::SGet(const std::string& name,
                                   const std::string& key,
                                   std::string* value) {
    return Get(name, key, value, true);
}

inline Status RocksDBStorage::SSet(const std::string& name,
                                   const std::string& key,
                                   const std::string& value) {
    return Set(name, key, value, true);
}

inline Status RocksDBStorage::SDel(const std::string& name,
                                   const std::string& key) {
    return Del(name, key, true);
}

inline Iterator RocksDBStorage::SRange(const std::string& name,
                                       const std::string& prefix) {
    return Range(name, prefix);
}

inline Iterator RocksDBStorage::SGetAll(const std::string& name) {
    return GetAll(name, true);
}

inline Status RocksDBStorage::SClear(const std::string& name) {
    return Clear(name, true);
}

}  // namespace storage
}  // namespace metaserver
}  // namespace curvefs

#endif  // CURVEFS_SRC_METASERVER_STORAGE_ROCKSDB_STORAGE_H_
