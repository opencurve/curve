
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

#include <glog/logging.h>

#include <sstream>
#include <unordered_map>

#include "rocksdb/slice_transform.h"
#include "curvefs/src/metaserver/storage/utils.h"
#include "curvefs/src/metaserver/storage/storage.h"
#include "curvefs/src/metaserver/storage/rocksdb_storage.h"

namespace curvefs {
namespace metaserver {
namespace storage {

using rocksdb::ColumnFamilyDescriptor;
using rocksdb::ColumnFamilyOptions;
using rocksdb::ColumnFamilyHandle;

void Monitor::Add(const std::string& name, const std::string& key) {
    auto iter = monitor_.find(name);
    if (iter == monitor_.end()) {
        auto ret = monitor_.emplace(name, std::make_shared<CounterType>());
        iter = ret.first;
    }
    iter->second->emplace(Hash(key));
}

void Monitor::Del(const std::string& name, const std::string& key) {
    auto iter = monitor_.find(name);
    if (iter == monitor_.end()) {
        return;
    }
    iter->second->erase(Hash(key));
}

size_t Monitor::Sum(const std::string& name, const std::string& key) {
    auto iter = monitor_.find(name);
    if (iter == monitor_.end()) {
        return 0;
    } else if (iter->second->find(Hash(key)) == iter->second->end()) {
        return 0;
    }
    return 1;
}

size_t Monitor::Sum(const std::string& name) {
    auto iter = monitor_.find(name);
    if (iter == monitor_.end()) {
        return 0;
    }
    return iter->second->size();
}

void Monitor::Clear(const std::string& name) {
    auto iter = monitor_.find(name);
    if (iter == monitor_.end()) {
        return;
    }
    iter->second->clear();
}

RocksDBStorage::RocksDBStorage(StorageOptions options)
    : options_(options),
      db_(nullptr),
      inited_(false) {  // TODO(@Wine93): add check for whether db_ is null
    // db options
    RocksDBStorageComparator cmp;
    // TODO(@Wine93): add buffer_size and max_buffers
    dbOptions_ = rocksdb::Options();
    dbOptions_.prefix_extractor.reset(rocksdb::NewFixedPrefixTransform(3));
    dbOptions_.create_if_missing = true;
    dbOptions_.comparator = &cmp;
    dbOptions_.enable_blob_files = true;

    // write options
    writeOptions_ = rocksdb::WriteOptions();
    writeOptions_.disableWAL = true;

    // read options
    readOptions_ = rocksdb::ReadOptions();

    monitor_ = std::make_shared<Monitor>();
}

STORAGE_TYPE RocksDBStorage::Type() {
    return STORAGE_TYPE::ROCKSDB_STORAGE;
}

bool RocksDBStorage::Open() {
    if (inited_) {
        return true;
    }

    std::vector<ColumnFamilyDescriptor> columnFamilies;
    columnFamilies.push_back(ColumnFamilyDescriptor(
        rocksdb::kDefaultColumnFamilyName, ColumnFamilyOptions()));
    rocksdb::Status s = rocksdb::DB::Open(
        dbOptions_, options_.DataDir, columnFamilies, &handles_, &db_);
    if (!s.ok()) {
        LOG(ERROR) << "Open rocksdb database failed, status = "
                   << s.ToString();
        return false;
    }

    inited_ = true;
    return true;
}

bool RocksDBStorage::Close() {
    rocksdb::Status s = db_->Close();
    return s.ok();
}

Status RocksDBStorage::ToStorageStatus(const rocksdb::Status& s) {
    if (s.ok()) {
        return Status::OK();
    } else if (s.IsNotFound()) {
        return Status::NotFound();
    }
    return Status::InternalError();
}

ColumnFamilyHandle* RocksDBStorage::GetColumnFamilyHandle(
    bool ordered) {
    // @TODO(Wine93): create two column failmy
    return handles_[0];
}

std::string RocksDBStorage::FormatIkey(size_t num, const std::string& key) {
    std::ostringstream oss;
    oss << EncodeNumber(num) << ":" << key;
    return oss.str();
}

std::string RocksDBStorage::InternalName(const std::string& name,
                                         bool ordered) {
    std::string prefix = ordered ? "ordered" : "unordered";
    return prefix + ":" + name;
}

// internal key = Hash(name):key
std::string RocksDBStorage::InternalKey(const std::string& name,
                                        const std::string& key,
                                        bool ordered) {
    size_t num = std::hash<std::string>{}(InternalName(name, ordered));
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
    } else if (monitor_->Sum(InternalName(name, ordered), key) == 0) {
        return Status::NotFound();
    }

    auto handle = GetColumnFamilyHandle(ordered);
    std::string ikey = InternalKey(name, key, ordered);
    rocksdb::Status s = db_->Get(readOptions_, handle, ikey, value);
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
    rocksdb::Status s = db_->Put(writeOptions_, handle, ikey, value);
    if (s.ok()) {
        monitor_->Add(InternalName(name, ordered), key);
    }
    return ToStorageStatus(s);
}

Status RocksDBStorage::Del(const std::string& name,
                           const std::string& key,
                           bool ordered) {
    if (nullptr == db_) {
        return Status::DBClosed();
    } else if (monitor_->Sum(InternalName(name, ordered), key) == 0) {
        return Status::NotFound();
    }

    auto handle = GetColumnFamilyHandle(ordered);
    std::string ikey = InternalKey(name, key, ordered);
    rocksdb::Status s = db_->Delete(writeOptions_, handle, ikey);
    if (s.ok()) {
        monitor_->Del(InternalName(name, ordered), key);
    }
    return ToStorageStatus(s);
}

std::shared_ptr<Iterator> RocksDBStorage::Seek(const std::string& name,
                                                const std::string& prefix) {
    if (nullptr == db_) {
        return std::make_shared<RocksDBStorageIterator>(
            this, "", 0, -1);
    }

    auto ikey = InternalKey(name, prefix, true);
    return std::make_shared<RocksDBStorageIterator>(
        this, ikey, 0, 0);
}

std::shared_ptr<Iterator> RocksDBStorage::GetAll(const std::string& name,
                                                 bool ordered) {
    if (nullptr == db_) {
        return std::make_shared<RocksDBStorageIterator>(
            this, "", 0, -1);
    }

    std::string prefix = InternalKey(name, "", ordered);  // name:
    return std::make_shared<RocksDBStorageIterator>(
        this, prefix, monitor_->Sum(InternalName(name, ordered)), 0);
}

size_t RocksDBStorage::Size(const std::string& name, bool ordered) {
    return monitor_->Sum(InternalName(name, ordered));
}

Status RocksDBStorage::Clear(const std::string& name, bool ordered) {
    if (nullptr == db_) {
        return Status::DBClosed();
    }

    auto handle = GetColumnFamilyHandle(ordered);
    std::string beginKey = InternalKey(name, "", ordered);
    size_t beginNum = DecodeNumber(beginKey);
    std::string endKey = FormatIkey(beginNum + 1, "");
    rocksdb::Status s = db_->DeleteRange(
        writeOptions_, handle, beginKey, endKey);
    if (s.ok()) {
        monitor_->Clear(InternalName(name, ordered));
    }
    return ToStorageStatus(s);
}

bool RocksDBStorage::GetStatistics(StorageStatistics* statistics) {
    statistics->MaxMemoryBytes = options_.MaxMemoryBytes;
    statistics->MaxDiskQuotaBytes = options_.MaxDiskQuotaBytes;

    uint64_t total, available;
    if (!GetFileSystemSpaces(options_.DataDir, &total, &available)) {
        return false;
    }
    statistics->DiskUsageBytes = total - available;

    uint64_t rssBytes;
    if (!GetProcMemory(&rssBytes)) {
        return false;
    }
    statistics->MemoryUsageBytes = rssBytes;

    return true;
}

}  // namespace storage
}  // namespace metaserver
}  // namespace curvefs
