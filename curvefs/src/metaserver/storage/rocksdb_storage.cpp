
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
#include "curvefs/src/metaserver/storage/converter.h"
#include "curvefs/src/metaserver/storage/rocksdb_storage.h"

namespace curvefs {
namespace metaserver {
namespace storage {

// RocksDBOptions
const std::string RocksDBOptions::kOrderedColumnFamilyName_ =  // NOLINT
    "ordered_column_familiy";

const std::string RocksDBStorage::kDelimiter_ = ":";  // NOLINT

RocksDBOptions::RocksDBOptions(StorageOptions options) {
    // db options
    // the database will be created if it is missing
    dbOptions_.create_if_missing = true;
    // missing column families will be automatically created
    dbOptions_.create_missing_column_families = true;
    // maximum number of concurrent background memtable flush jobs,
    // submitted by default to the HIGH priority thread pool
    dbOptions_.max_background_flushes = 4;
    // maximum number of concurrent background compaction jobs,
    // submitted to the default LOW priority thread pool.
    dbOptions_.max_background_compactions = 4;
    // allows OS to incrementally sync files to disk while they are being
    // written, asynchronously, in the background.
    dbOptions_.bytes_per_sync = 1048576;

    // table options
    std::shared_ptr<ROCKSDB_NAMESPACE::Cache> cache =
        NewLRUCache(options.blockCacheCapacity);
    BlockBasedTableOptions tableOptions;
    tableOptions.block_size = 16 * 1024;  // 16KB
    // default: an 8MB internal cache
    tableOptions.block_cache = cache;
    // whether to put index/filter blocks in the block cache
    tableOptions.cache_index_and_filter_blocks = true;
    // only evicted from cache when the table reader is freed
    tableOptions.pin_l0_filter_and_index_blocks_in_cache = true;
    // reset bloom filter
    tableOptions.filter_policy.reset(NewBloomFilterPolicy(10, false));

    // column failmy options
    ColumnFamilyOptions cfOptions = ColumnFamilyOptions();
    // large values (blobs) are written to separate blob files, and
    // only pointers to them are stored in SST files
    cfOptions.enable_blob_files = true;
    // RocksDB will pick target size of each level dynamically
    cfOptions.level_compaction_dynamic_level_bytes = true;
    cfOptions.compaction_pri = ROCKSDB_NAMESPACE::kMinOverlappingRatio;
    // use the specified function to determine the prefixes for keys
    cfOptions.prefix_extractor.reset(
        NewFixedPrefixTransform(options.keyPrefixLength));
    // The size in bytes of the filter for memtable is
    // write_buffer_size * memtable_prefix_bloom_size_ratio
    cfOptions.memtable_prefix_bloom_size_ratio =
        options.memtablePrefixBloomSizeRatio;
    // reset table options for column failmy
    cfOptions.table_factory.reset(NewBlockBasedTableFactory(tableOptions));

    ColumnFamilyOptions unorderedCFOptions = cfOptions;
    // amount of data to build up in memory (backed by an unsorted log
    // on disk) before converting to a sorted on-disk file.
    unorderedCFOptions.write_buffer_size = options.unorderedWriteBufferSize;
    // the maximum number of write buffers that are built up in memory.
    unorderedCFOptions.max_write_buffer_number =
        options.unorderedMaxWriteBufferNumber;

    ColumnFamilyOptions orderedCFOptions = cfOptions;
    orderedCFOptions.write_buffer_size = options.orderedWriteBufferSize;
    orderedCFOptions.max_write_buffer_number =
        options.orderedMaxWriteBufferNumber;

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

//  RocksDBStorage
RocksDBStorage::RocksDBStorage()
    : InTransaction_(false) {}

RocksDBStorage::RocksDBStorage(StorageOptions options)
    : inited_(false),
      options_(options),
      InTransaction_(false) {
    // we fake a table name and invoke ToInternale() to get key prefix length
    std::string tablename = std::string(NameGenerator::GetFixedLength(), '0');
    std::string iname = ToInternalName(tablename, true, true);
    options_.keyPrefixLength = iname.size();
    rocksdbOptions_ = RocksDBOptions(options_);
}

RocksDBStorage::RocksDBStorage(const RocksDBStorage& storage,
                               ROCKSDB_NAMESPACE::Transaction* txn)
    : inited_(storage.inited_),
      options_(storage.options_),
      rocksdbOptions_(storage.rocksdbOptions_),
      db_(storage.db_),
      txnDB_(storage.txnDB_),
      handles_(storage.handles_),
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

/* NOTE:
 * 1. we use suffix 0/1 to determine the key range:
 *    [ordered:name:0, ordered:name:1)
 * 2. please gurantee the length of name is fixed for
 *    we can determine the rocksdb's prefix key
 */
std::string RocksDBStorage::ToInternalName(const std::string& name,
                                           bool ordered,
                                           bool start) {
    std::ostringstream oss;
    oss << ordered << kDelimiter_ << name << kDelimiter_ << (start ? "0" : "1");
    return oss.str();
}

std::string RocksDBStorage::ToInternalKey(const std::string& name,
                                          const std::string& key,
                                          bool ordered) {
    std::string iname = ToInternalName(name, ordered, true);
    std::ostringstream oss;
    oss << iname << kDelimiter_ << key;
    std::string ikey = oss.str();
    VLOG(9) << "ikey = " << ikey << " (ordered = " << ordered
            << ", name = " << name << ", key = " << key << ")"
            << ", size=" << ikey.size();
    return ikey;
}

// extract user key from internal key: prefix:key => key
std::string RocksDBStorage::ToUserKey(const std::string& ikey) {
    return ikey.substr(options_.keyPrefixLength + kDelimiter_.size());
}

Status RocksDBStorage::Get(const std::string& name,
                           const std::string& key,
                           ValueType* value,
                           bool ordered) {
    if (!inited_) {
        return Status::DBClosed();
    }

    std::string svalue;
    std::string ikey = ToInternalKey(name, key, ordered);
    auto handle = GetColumnFamilyHandle(ordered);
    ROCKSDB_NAMESPACE::Status s = InTransaction_ ?
        txn_->Get(ReadOptions(), handle, ikey, &svalue) :
        db_->Get(ReadOptions(), handle, ikey, &svalue);
    if (s.ok() && !value->ParseFromString(svalue)) {
        return Status::ParsedFailed();
    }
    return ToStorageStatus(s);
}

Status RocksDBStorage::Set(const std::string& name,
                           const std::string& key,
                           const ValueType& value,
                           bool ordered) {
    std::string svalue;
    if (!inited_) {
        return Status::DBClosed();
    } else if (!value.SerializeToString(&svalue)) {
        return Status::SerializedFailed();
    }

    auto handle = GetColumnFamilyHandle(ordered);
    std::string ikey = ToInternalKey(name, key, ordered);
    ROCKSDB_NAMESPACE::Status s = InTransaction_ ?
        txn_->Put(handle, ikey, svalue) :
        db_->Put(WriteOptions(), handle, ikey, svalue);
    return ToStorageStatus(s);
}

Status RocksDBStorage::Del(const std::string& name,
                           const std::string& key,
                           bool ordered) {
    if (!inited_) {
        return Status::DBClosed();
    }

    std::string ikey = ToInternalKey(name, key, ordered);
    auto handle = GetColumnFamilyHandle(ordered);
    ROCKSDB_NAMESPACE::Status s = InTransaction_ ?
        txn_->Delete(handle, ikey) :
        db_->Delete(WriteOptions(), handle, ikey);
    return ToStorageStatus(s);
}

std::shared_ptr<Iterator> RocksDBStorage::Seek(const std::string& name,
                                               const std::string& prefix) {
    int status = inited_ ? 0 : -1;
    std::string ikey = ToInternalKey(name, prefix, true);
    return std::make_shared<RocksDBStorageIterator>(
        this, ikey, 0, status, true);
}

std::shared_ptr<Iterator> RocksDBStorage::GetAll(const std::string& name,
                                                 bool ordered) {
    int status = inited_ ? 0 : -1;
    std::string ikey = ToInternalKey(name, "", ordered);
    return std::make_shared<RocksDBStorageIterator>(
        this, ikey, 0, status, ordered);
}

size_t RocksDBStorage::Size(const std::string& name, bool ordered) {
    auto iterator = GetAll(name, ordered);
    if (iterator->Status() != 0) {
        return 0;
    }

    size_t size = 0;
    for (iterator->SeekToFirst(); iterator->Valid(); iterator->Next()) {
        size++;
    }
    return size;
}

Status RocksDBStorage::Clear(const std::string& name, bool ordered) {
    if (!inited_) {
        return Status::DBClosed();
    } else if (InTransaction_) {
        return Status::NotSupported();
    }

    auto handle = GetColumnFamilyHandle(ordered);
    std::string lower = ToInternalName(name, ordered, true);
    std::string upper = ToInternalName(name, ordered, false);
    ROCKSDB_NAMESPACE::Status s = db_->DeleteRange(
        WriteOptions(), handle, lower, upper);
    LOG(INFO) << "Clear(), tablename = " << name << ", ordered" << "ordered"
              << ", lower key = " << lower << ", upper key = " << upper;
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
    if (!InTransaction_ || nullptr == txn_) {
        return Status::NotSupported();
    }

    ROCKSDB_NAMESPACE::Status s = txn_->Commit();
    if (!s.ok()) {
        LOG(ERROR) << "RocksDBStorage commit transaction failed"
                   << ", status=" << s.ToString();
    }
    delete txn_;
    return ToStorageStatus(s);
}

Status RocksDBStorage::Rollback()  {
    if (!InTransaction_ || nullptr == txn_) {
        return Status::NotSupported();
    }
    ROCKSDB_NAMESPACE::Status s = txn_->Rollback();
    if (!s.ok()) {
        LOG(ERROR) << "RocksDBStorage rollback transaction failed"
                   << ", status=" << s.ToString();
    }
    delete txn_;
    return ToStorageStatus(s);
}

bool RocksDBStorage::GetStatistics(StorageStatistics* statistics) {
    statistics->maxMemoryQuotaBytes = options_.maxMemoryQuotaBytes;
    statistics->maxDiskQuotaBytes = options_.maxDiskQuotaBytes;

    if (!GetProcMemory(&statistics->memoryUsageBytes)) {
        LOG(ERROR) << "Get process memory failed.";
        return false;
    }

    uint64_t total, available;
    if (!GetFileSystemSpaces(options_.dataDir, &total, &available)) {
        LOG(ERROR) << "Get filesystem space failed.";
        return false;
    }
    statistics->diskUsageBytes = total - available;

    return true;
}

StorageOptions RocksDBStorage::GetStorageOptions() const {
    return options_;
}

}  // namespace storage
}  // namespace metaserver
}  // namespace curvefs
