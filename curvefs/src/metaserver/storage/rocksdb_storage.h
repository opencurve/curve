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
#include <unordered_map>


#include "rocksdb/db.h"
#include "absl/container/btree_set.h"
#include "curvefs/src/metaserver/storage/utils.h"
#include "curvefs/src/metaserver/storage/storage.h"
#include "curvefs/src/metaserver/storage/rocksdb_storage.h"

namespace curvefs {
namespace metaserver {
namespace storage {

using STORAGE_TYPE = KVStorage::STORAGE_TYPE;

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
    std::unordered_map<std::string, std::shared_ptr<CounterType>> monitor_;
};

class RocksDBStorageComparator : public rocksdb::Comparator {
 public:
    int Compare(const rocksdb::Slice& slice1,
                const rocksdb::Slice& slice2) const {
        std::string s1 = std::string(slice1.data(), slice1.size());
        std::string s2 = std::string(slice2.data(), slice2.size());
        size_t n1 = DecodeNumber(s1.data());
        size_t n2 = DecodeNumber(s2.data());
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
    void FindShortestSeparator(std::string*, const rocksdb::Slice&) const {}
    void FindShortSuccessor(std::string*) const {}
};

class RocksDBStorage : public KVStorage {
 public:
    explicit RocksDBStorage(StorageOptions options);

    bool Open() override;

    bool Close() override;

    STORAGE_TYPE Type() override;

    bool GetStatistics(StorageStatistics* Statistics) override;

    // unordered
    Status HGet(const std::string& name,
                const std::string& key,
                std::string* value) override;

    Status HSet(const std::string& name,
                const std::string& key,
                const std::string& value) override;

    Status HDel(const std::string& name, const std::string& key) override;

    std::shared_ptr<Iterator> HGetAll(const std::string& name) override;

    size_t HSize(const std::string& name) override;

    Status HClear(const std::string& name) override;

    // ordered
    Status SGet(const std::string& name,
                const std::string& key,
                std::string* value) override;

    Status SSet(const std::string& name,
                const std::string& key,
                const std::string& value) override;

    Status SDel(const std::string& name, const std::string& key) override;

    std::shared_ptr<Iterator> SSeek(const std::string& name,
                                    const std::string& prefix) override;

    std::shared_ptr<Iterator> SGetAll(const std::string& name) override;

    size_t SSize(const std::string& name) override;

    Status SClear(const std::string& name) override;

 private:
    Status ToStorageStatus(const rocksdb::Status& s);

    rocksdb::ColumnFamilyHandle* GetColumnFamilyHandle(bool ordered);

    std::string FormatIkey(size_t num, const std::string& key);

    std::string InternalKey(const std::string& name,
                            const std::string& key,
                            bool ordered);

    std::string InternalName(const std::string& name, bool ordered);

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

    std::shared_ptr<Iterator> Seek(const std::string& name,
                                    const std::string& prefix);

    std::shared_ptr<Iterator> GetAll(const std::string& name, bool ordered);

    size_t Size(const std::string& name, bool ordered);

    Status Clear(const std::string& name, bool ordered);

 private:
    friend class RocksDBStorageIterator;

 private:
    bool inited_;
    StorageOptions options_;
    rocksdb::Options dbOptions_;
    rocksdb::ReadOptions readOptions_;
    rocksdb::WriteOptions writeOptions_;
    rocksdb::DB* db_;
    std::vector<rocksdb::ColumnFamilyHandle*> handles_;
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
                           int status)
        : storage_(storage),
          prefix_(prefix),
          size_(size),
          status_(status) {}

    ~RocksDBStorageIterator() {
        if (status_ == 0) {
            storage_->db_->ReleaseSnapshot(readOptions_.snapshot);
        }
    }

    uint64_t Size() {
        return static_cast<uint64_t>(size_);
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
        readOptions_ = storage_->readOptions_;
        readOptions_.snapshot = storage_->db_->GetSnapshot();
        iter_ = storage_->db_->NewIterator(readOptions_);
        iter_->Seek(prefix_);
    }

    void Next() {
        iter_->Next();
    }

    std::string Key() {
        auto ikey = std::string(iter_->key().data(), iter_->key().size());
        return storage_->UserKey(ikey);
    }

    std::string Value() {
        return std::string(iter_->value().data(), iter_->value().size());
    }

    int Status() {
        return status_;
    }

 private:
    std::string prefix_;
    uint64_t size_;
    int status_;
    rocksdb::Iterator* iter_;
    RocksDBStorage* storage_;
    rocksdb::ReadOptions readOptions_;
};

}  // namespace storage
}  // namespace metaserver
}  // namespace curvefs

#endif  // CURVEFS_SRC_METASERVER_STORAGE_ROCKSDB_STORAGE_H_
