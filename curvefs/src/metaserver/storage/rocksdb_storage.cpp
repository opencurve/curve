
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



inline void Counter::Clear(const std::string& name) {
    auto container = GetContainer(name);
    container->clear();
}

const std::string RocksDBOptions::kOrderedColumnFamilyName_ =  // NOLINT
    "ordered_column_familiy";

RocksDBOptions::RocksDBOptions(StorageOptions options) {
    // db options
    RocksDBStorageComparator cmp;
    dbOptions_.comparator = &cmp;
    dbOptions_.create_if_missing = true;
    dbOptions_.create_missing_column_families = true;
    dbOptions_.enable_blob_files = true;
    dbOptions_.max_background_flushes = 2;
    dbOptions_.max_background_compactions = 4;
    dbOptions_.bytes_per_sync = 1048576;
    dbOptions_.compaction_pri = ROCKSDB_NAMESPACE::kMinOverlappingRatio;
    dbOptions_.prefix_extractor.reset(NewFixedPrefixTransform(3));

    // table options
    std::shared_ptr<ROCKSDB_NAMESPACE::Cache> cache =
        NewLRUCache(options.blockCacheCapacity);
    BlockBasedTableOptions tableOptions;
    tableOptions.block_cache = cache;
    tableOptions.block_size = 16 * 1024;  // 16MB
    tableOptions.cache_index_and_filter_blocks = true;
    tableOptions.pin_l0_filter_and_index_blocks_in_cache = true;
    dbOptions_.table_factory.reset(NewBlockBasedTableFactory(tableOptions));

    // column failmy options
    auto unorderedCFOptions = ColumnFamilyOptions();
    auto orderedCFOptions = ColumnFamilyOptions();
    unorderedCFOptions.write_buffer_size = options.unorderedWriteBufferSize;
    unorderedCFOptions.max_write_buffer_number =
        options.unorderedMaxWriteBufferNumber;
    unorderedCFOptions.level_compaction_dynamic_level_bytes = true;
    orderedCFOptions.write_buffer_size = options.orderedWriteBufferSize;
    orderedCFOptions.max_write_buffer_number =
        options.orderedMaxWriteBufferNumber;
    orderedCFOptions.level_compaction_dynamic_level_bytes = true;

    columnFamilies_.push_back(ColumnFamilyDescriptor(
        ROCKSDB_NAMESPACE::kDefaultColumnFamilyName, unorderedCFOptions));
    columnFamilies_.push_back(ColumnFamilyDescriptor(
        kOrderedColumnFamilyName_, orderedCFOptions));
}

inline std::vector<ColumnFamilyDescriptor>
RocksDBOptions::ColumnFamilys() {
    return columnFamilies_;
}

inline ROCKSDB_NAMESPACE::Options RocksDBOptions::DBOptions() {
    return dbOptions_;
}

inline ROCKSDB_NAMESPACE::TransactionDBOptions
    RocksDBOptions::TransactionDBOptions() {
    return ROCKSDB_NAMESPACE::TransactionDBOptions();
}

inline ROCKSDB_NAMESPACE::ReadOptions RocksDBOptions::ReadOptions() {
    return ROCKSDB_NAMESPACE::ReadOptions();
}

inline ROCKSDB_NAMESPACE::WriteOptions RocksDBOptions::WriteOptions() {
    auto options = ROCKSDB_NAMESPACE::WriteOptions();
    options.disableWAL = true;
    return options;
}

RocksDBStorage::RocksDBStorage(StorageOptions options)
    : inited_(false),
      options_(options),
      rocksdbOptions_(RocksDBOptions(options)),
      counter_(std::make_shared<Counter>()) {}

RocksDBStorage::RocksDBStorage(const RocksDBStorage& storage,
                               ROCKSDB_NAMESPACE::Transaction* txn)
    : inited_(storage.inited_),
      options_(storage.options_),
      rocksdbOptions_(storage.rocksdbOptions_),
      db_(storage.db_),
      txnDB_(storage.txnDB_),
      handles_(storage.handles_),
      counter_(storage.counter_),
      InTransaction_(true),
      txn_(txn) {}

STORAGE_TYPE RocksDBStorage::Type() {
    return STORAGE_TYPE::ROCKSDB_STORAGE;
}

inline ROCKSDB_NAMESPACE::Options RocksDBStorage::DBOptions() {
    return rocksdbOptions_.DBOptions();
}

inline ROCKSDB_NAMESPACE::TransactionDBOptions
    RocksDBStorage::TransactionDBOptions() {
    return rocksdbOptions_.TransactionDBOptions();
}

inline std::vector<ColumnFamilyDescriptor> RocksDBStorage::ColumnFamilys() {
    return rocksdbOptions_.ColumnFamilys();
}

inline ROCKSDB_NAMESPACE::ReadOptions RocksDBStorage::ReadOptions() {
    return rocksdbOptions_.ReadOptions();
}

inline ROCKSDB_NAMESPACE::WriteOptions RocksDBStorage::WriteOptions() {
    return rocksdbOptions_.WriteOptions();
}

bool RocksDBStorage::Open() {
    if (inited_) {
        return true;
    }

    ROCKSDB_NAMESPACE::Status s = TransactionDB::Open(
        DBOptions(), TransactionDBOptions(), options_.dataDir,
        ColumnFamilys(), &handles_, &txnDB_);
    if (!s.ok()) {
        LOG(ERROR) << "Open rocksdb database failed, status = "
                   << s.ToString();
        return false;
    }

    db_ = txnDB_->GetBaseDB();
    inited_ = true;
    return true;
}

bool RocksDBStorage::Close() {
    if (!inited_) {
        return true;
    }

    ROCKSDB_NAMESPACE::Status s;
    for (auto handle : handles_) {
        s = db_->DestroyColumnFamilyHandle(handle);
        if (!s.ok()) {
            LOG(ERROR) << "Destory column failmy failed, status = "
                       << s.ToString();
            return false;
        }
    }

    s = txnDB_->Close();
    if (!s.ok()) {
        LOG(ERROR) << "Close rocksdb failed, status = "
                    << s.ToString();
        return false;
    }
    inited_ = false;
    return s.ok();
}

inline ColumnFamilyHandle* RocksDBStorage::GetColumnFamilyHandle(bool ordered) {
    return ordered ? handles_[1] : handles_[0];
}

Status RocksDBStorage::ToStorageStatus(const ROCKSDB_NAMESPACE::Status& s) {
    if (s.ok()) {
        return Status::OK();
    } else if (s.IsNotFound()) {
        return Status::NotFound();
    }
    return Status::InternalError();
}

std::string RocksDBStorage::ToInternalName(const std::string& name,
                                           bool ordered) {
    std::ostringstream oss;
    oss << ordered << ":" << name;
    return oss.str();
}

std::string RocksDBStorage::FormatInternalKey(size_t num4name,
                                              const std::string& key) {
    std::ostringstream oss;
    oss << EncodeNumber(num4name) << ":" << key;
    return oss.str();
}

// NOTE: we will convert name to number for compare prefix
// eg: iname:key
std::string RocksDBStorage::ToInternalKey(const std::string& iname,
                                          const std::string& key) {
    size_t num4name = Hash(iname);
    return FormatInternalKey(num4name, key);
}

std::string RocksDBStorage::ToUserKey(const std::string& ikey) {
    size_t length = sizeof(size_t);
    return ikey.substr(length + 1);  // trim prefix "name:"
}

Status RocksDBStorage::Get(const std::string& name,
                           const std::string& key,
                           std::string* value,
                           bool ordered) {
    if (!inited_) {
        return Status::DBClosed();
    }

    std::string iname = ToInternalName(name, ordered);
    std::string ikey = ToInternalKey(iname, key);
    if (!counter_->Find(iname, ikey)) {
        return Status::NotFound();
    }

    auto handle = GetColumnFamilyHandle(ordered);
    ROCKSDB_NAMESPACE::Status s = db_->Get(ReadOptions(), handle, ikey, value);
    return ToStorageStatus(s);
}

Status RocksDBStorage::Set(const std::string& name,
                           const std::string& key,
                           const std::string& value,
                           bool ordered) {
    if (!inited_) {
        return Status::DBClosed();
    }

    auto handle = GetColumnFamilyHandle(ordered);
    std::string iname = ToInternalName(name, ordered);
    std::string ikey = ToInternalKey(iname, key);
    ROCKSDB_NAMESPACE::Status s = db_->Put(WriteOptions(), handle, ikey, value);
    if (s.ok()) {
        counter_->Insert(iname, ikey);
    }
    return ToStorageStatus(s);
}

Status RocksDBStorage::Del(const std::string& name,
                           const std::string& key,
                           bool ordered) {
    if (!inited_) {
        return Status::DBClosed();
    }

    std::string iname = ToInternalName(name, ordered);
    std::string ikey = ToInternalKey(iname, key);
    if (!counter_->Find(iname, ikey)) {
        return Status::NotFound();
    }

    auto handle = GetColumnFamilyHandle(ordered);
    ROCKSDB_NAMESPACE::Status s = db_->Delete(WriteOptions(), handle, ikey);
    if (s.ok()) {
        counter_->Erase(iname, ikey);
    }
    return ToStorageStatus(s);
}

std::shared_ptr<Iterator> RocksDBStorage::Seek(const std::string& name,
                                                const std::string& prefix) {
    size_t size = 0;
    int status = inited_ ? 0 : -1;
    std::string iname = ToInternalName(name, true);
    std::string ikey = ToInternalKey(iname, prefix);
    return std::make_shared<RocksDBStorageIterator>(
        this, ikey, size, status, true);
}

std::shared_ptr<Iterator> RocksDBStorage::GetAll(const std::string& name,
                                                 bool ordered) {
    int status = inited_ ? 0 : -1;
    std::string iname = ToInternalName(name, ordered);
    std::string ikey = ToInternalKey(iname, "");
    size_t size = counter_->Size(iname);
    return std::make_shared<RocksDBStorageIterator>(
        this, ikey, size, status, ordered);
}

size_t RocksDBStorage::Size(const std::string& name, bool ordered) {
    std::string iname = ToInternalName(name, ordered);
    return counter_->Size(iname);
}

Status RocksDBStorage::Clear(const std::string& name, bool ordered) {
    if (!inited_) {
        return Status::DBClosed();
    }

    auto handle = GetColumnFamilyHandle(ordered);
    std::string iname = ToInternalName(name, ordered);
    std::string beginKey = ToInternalKey(iname, "");  // "name:"
    size_t beginNum = DecodeNumber(beginKey);
    std::string endKey = FormatInternalKey(beginNum + 1, "");
    ROCKSDB_NAMESPACE::Status s = db_->DeleteRange(
        WriteOptions(), handle, beginKey, endKey);
    if (s.ok()) {
        counter_->Clear(iname);
    }
    return ToStorageStatus(s);
}

std::shared_ptr<StorageTransaction> RocksDBStorage::BeginTransaction() {
    ROCKSDB_NAMESPACE::Transaction* txn =
        txnDB_->BeginTransaction(WriteOptions());
    if (nullptr == txn) {
        return nullptr;
    }
    return std::make_shared<RocksDBStorage>(*this, txn);
}

Status RocksDBStorage::Commit() {
    if (!InTransaction_) {
        return Status::NotSupported();
    }
    return ToStorageStatus(txn_->Commit());
}

Status RocksDBStorage::Rollback()  {
    if (InTransaction_) {
        return Status::NotSupported();
    }
    return ToStorageStatus(txn_->Rollback());
}

bool RocksDBStorage::GetStatistics(StorageStatistics* statistics) {
    statistics->maxMemoryQuotaBytes = options_.maxMemoryQuotaBytes;
    statistics->maxDiskQuotaBytes = options_.maxDiskQuotaBytes;

    uint64_t vmRSS;
    if (!GetProcMemory(&vmRSS)) {
        LOG(ERROR) << "Get process memory failed.";
        return false;
    }
    statistics->memoryUsageBytes = vmRSS * 1024;  // unit of vmRSS is KB

    uint64_t total, available;
    if (!GetFileSystemSpaces(options_.dataDir, &total, &available)) {
        LOG(ERROR) << "Get filesystem space failed.";
        return false;
    }
    statistics->diskUsageBytes = total - available;

    return true;
}

}  // namespace storage
}  // namespace metaserver
}  // namespace curvefs
