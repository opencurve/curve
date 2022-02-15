
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

#include <unordered_map>

#include "curvefs/src/metserver/storage/memory_storage.h"

namespace curvefs {
namespace metaserver {
namespace storage {

void Monitor::Add(const std::string& name, const std::string& key) {
    auto iter = monitor_.find(name);
    if (nullptr == iter) {
        iter->second = std::make_shared<CounterType>();
    }
    iter->second->emplace(Hash(key));
}

void Monitor::Del(const std::string& name, const std::string& key) {
    auto iter = monitor_.find(name);
    if (nullptr == iter) {
        return;
    }
    iter->second->erase(Hash(key));
}

size_t Monitor::Sum(const std::string& name, const std::string& key) {
    auto iter = monitor_.find(name);
    if (nullptr == iter) {
        return 0;
    } else if (iter->second->find(Hash(key)) == iter->end()) {
        return 0;
    }
    return 1;
}

size_t Monitor::Sum(const std::string& name) {
    auto iter = monitor_.find(name);
    if (nullptr == iter) {
        return 0;
    }
    return iter->second->size();
}

void Monitor::Clear(const std::string& name) {
    auto iter = monitor_.find(name);
    if (nullptr == iter) {
        return;
    }
    iter->second->clear();
}

RocksDBStorage::RocksDBStorage(StorageOptions options)
    : options(options),
      db_(nullptr) { // TODO(@Wine93): add check for whether db_ is null
    // db options
    RocksDBStorageComparator cmp;
    dbOptions_ = RocksDB::Options;
    dbOptions_.prefix_extractor.reset(NewFixedPrefixTransform(3));
    dbOptions_.error_if_exists = true;
    dbOptions_.comparator = &cmp
    dbOptions_.enable_blob_files = true;

    // write options
    writeOptions_ = RocksDB::WriteOptions();
    writeOptions_.disableWAL = true;

    // read options
    readOptions_ = RocksDB::ReadOptions();

    Monitor_ = std::make_shared<Monitor>();
}

STORAGE_TYPE RocksDBStorage::Type() {
    return STORAGE_TYPE::ROCKSDB_STORAGE;
}

bool RocksDBStorage::Open(const std::string& dbpath) {
    std::vector<RocksDB::ColumnFamilyDescriptor> columnFamilies;
    columnFamilies.push_back(ColumnFamilyDescriptor(
        RocksDB::kDefaultColumnFamilyName, ColumnFamilyOptions()));
    columnFamilies.push_back(ColumnFamilyDescriptor(
        "orderedCF", ColumnFamilyOptions()));

    RocksDB::Status s = DB::Open(
        dbOptions_, dbpath, columnFamilies, &handles_, &db_);
    return s.ok();
}

Status RocksDBStorage::ToStorageStatus(RocksDB::Status s) {
    if (s.ok()) {
        return Status::OK();
    } else if (s.IsNotFound()) {
        return Status::NotFound();
    }
    return Status::InternalError();
}

RocksDB::ColumnFamilyHandle* RocksDBStorage::GetColumnFamilyHandle(
    bool ordered) {
    return ordered ? handles_[1] : handles_[0];
}


std::string RocksDBStorage::FormatIkey(size_t num, const std::string& key) {
    std::ostringstream oss;
    oss << EncodeNumber(num) << ":" << key;
    return oss.str();
}

// internal key = Hash(name):key
std::string RocksDBStorage::InternalKey(const std::string& name,
                                        const std::string& key,
                                        bool ordered) {
    std::string prefix = ordered ? "ordered" : "unordered";
    size_t num = std::hash<std::string>{}(prefix + name);
    return FormatIkey(num, key);
}

std::string RocksDBStorage::UserKey(const std::string& ikey) {
    size_t length = sizeof(size_t);
    return ikey.substr(length + 1);  // trim prefix "name:"
}

Status RocksDBStorage::Get(const std::string& name,
                           const std::string& key,
                           std::string* value,
                           bool ordered) {
    if (nullptr == db_) {
        return Status::DBClosed();
    } else if (Monitor_->Sum(name, key) == 0) {
        return Status::NotFound();
    }

    auto handle = GetColumnFamilyHandle(ordered);
    std::string ikey = InternalKey(name, key, ordered);
    auto readOptions = readOptions_;
    readOptions_.snapshot = dc_->GetSnapshot();
    RocksDB::Status s = db_->Get(readOptions_, handle, ikey, value);
    return ToStorageStatus(s);
}

Status RocksDBStorage::Set(const std::string& name,
                           const std::string& key,
                           const std::string& value,
                           bool ordered) {
    if (nullptr == db_) {
        return Status::DBClosed();
    }

    auto handle = GetColumnFamilyHandle(ordered);
    std::string ikey = InternalKey(name, key, ordered);
    RocksDB::Status s = db->Put(writeOptions_, handle, ikey, value);
    if (s.ok()) {
        Monitor_->Add(name, key);
    }
    return ToStorageStatus(s);
}

Status RocksDBStorage::Del(const std::string& name,
                           const std::string& key,
                           bool ordered) {

    if (nullptr == db_) {
        return Status::DBClosed();
    } else if (Monitor_->Sum(name, key) == 0) {
        return Status::OK();
    }

    auto handle = GetColumnFamilyHandle(ordered);
    std::string ikey = InternalKey(name, key, ordered);
    RocksDB::Status s = db->Delete(writeOptions_, handle, ikey);
    if (s.ok()) {
        Monitor_->Del(name, key);
    }
    return ToStorageStatus(s);
}

Iterator RocksDBStorage::Range(const std::string& name,
                               const std::string& prefix) {
    if (nullptr == db_) {
        return RocksDBStorageIterator(this, prefix, 0, -1);
    }

    auto ikey = InternalKey(name, prefix, true);
    return RocksDBStorageIterator(this, prefix, 0, 0);
}

Iterator RocksDBStorage::GetAll(const std::string& name, bool ordered) {
    if (nullptr == db_) {
        return RocksDBStorageIterator(this, prefix, 0, -1);
    }

    std::string prefix = InternalKey(name, "", ordered);  // name:
    return RocksDBStorageIterator(this, prefix, Monitor_->Sum(name), 0);
}

Status RocksDBStorage::Clear(const std::string& name, bool ordered) {
    if (nullptr == db_) {
        return Status::DBClosed();
    }

    std::string beginKey = InternalKey(name, "", ordered);
    size_t beginNum = DecodeNumber(beginKey);
    std::string endKey = FormatIkey(beginNum + 1, key);
    RocksDB::Status s = db_->DeleteRange(writeOptions_, beginKey, endKey);
    if (s.ok()) {
        Monitor_->Clear(name);
    }
    return ToStorageStatus(s);
}

bool RocksDBStorage::GetStatistic(StorageStatistics* statistics) {
    statistic->MaxMemoryBytes = options_.MaxMemoryBytes;
    statistic->MaxDiskQuotaBytes = options_.MaxDiskQuotaBytes;
    return true;
}

}  // namespace storage
}  // namespace metaserver
}  // namespace curvefs