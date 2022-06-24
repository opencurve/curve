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

#include <ostream>
#include <iostream>
#include <unordered_map>

#include "src/common/timeutility.h"
#include "curvefs/src/metaserver/storage/utils.h"
#include "curvefs/src/metaserver/storage/storage.h"
#include "curvefs/src/metaserver/storage/converter.h"
#include "curvefs/src/metaserver/storage/rocksdb_perf.h"
#include "curvefs/src/metaserver/storage/rocksdb_storage.h"
#include "curvefs/src/metaserver/storage/rocksdb_options.h"
#include "rocksdb/utilities/checkpoint.h"
#include "src/fs/local_filesystem.h"

namespace curvefs {
namespace metaserver {
namespace storage {

using ::curve::common::TimeUtility;

const std::string RocksDBStorage::kDelimiter_ = ":";  // NOLINT

Status ToStorageStatus(const ROCKSDB_NAMESPACE::Status& s) {
    if (s.ok()) {
        return Status::OK();
    } else if (s.IsNotFound()) {
        return Status::NotFound();
    }
    return Status::InternalError();
}

size_t RocksDBStorage::GetKeyPrefixLength() {
    static const size_t length = []() {
        const std::string tableName =
            std::string(NameGenerator::GetFixedLength(), '0');
        const std::string iname =
            RocksDBStorage::ToInternalName(tableName, true, true);
        return iname.size();
    }();

    return length;
}

//  RocksDBStorage
RocksDBStorage::RocksDBStorage()
    : InTransaction_(false) {
    InitDbOptions();
}

RocksDBStorage::RocksDBStorage(StorageOptions options)
    : inited_(false),
      options_(std::move(options)),
      InTransaction_(false) {
    InitDbOptions();
}

RocksDBStorage::RocksDBStorage(const RocksDBStorage& storage,
                               ROCKSDB_NAMESPACE::Transaction* txn)
    : inited_(storage.inited_),
      options_(storage.options_),
      db_(storage.db_),
      txnDB_(storage.txnDB_),
      handles_(storage.handles_),
      InTransaction_(true),
      txn_(txn),
      dbOptions_(storage.dbOptions_),
      dbTransOptions_(storage.dbTransOptions_),
      dbWriteOptions_(storage.dbWriteOptions_),
      dbReadOptions_(storage.dbReadOptions_),
      dbCfDescriptors_(storage.dbCfDescriptors_) {}

STORAGE_TYPE RocksDBStorage::Type() {
    return STORAGE_TYPE::ROCKSDB_STORAGE;
}

bool RocksDBStorage::Open() {
    if (inited_) {
        return true;
    }

    assert(txnDB_ == nullptr);
    assert(db_ == nullptr);

    if (cleanOpen_ && options_.localFileSystem->DirExists(options_.dataDir)) {
        int ret = options_.localFileSystem->Delete(options_.dataDir);
        if (ret != 0) {
            LOG(ERROR) << "Failed to clear database directory when open from a "
                          "clean database, dir: "
                       << options_.dataDir << ", error: " << berror();
            return false;
        }
    }

    ROCKSDB_NAMESPACE::Status s =
        TransactionDB::Open(dbOptions_, dbTransOptions_, options_.dataDir,
                            dbCfDescriptors_, &handles_, &txnDB_);
    if (!s.ok()) {
        LOG(ERROR) << "Open rocksdb database at `" << options_.dataDir
                   << "` failed, status = " << s.ToString();
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
            LOG(ERROR) << "Destroy column family failed, status = "
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

    handles_.clear();
    inited_ = false;

    delete txnDB_;
    db_ = nullptr;
    txnDB_ = nullptr;

    return true;
}

inline ColumnFamilyHandle* RocksDBStorage::GetColumnFamilyHandle(bool ordered) {
    return ordered ? handles_[1] : handles_[0];
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
            << ", size = " << ikey.size();
    return ikey;
}

// extract user key from internal key: prefix:key => key
std::string RocksDBStorage::ToUserKey(const std::string& ikey) {
    return ikey.substr(GetKeyPrefixLength() + kDelimiter_.size());
}

Status RocksDBStorage::Get(const std::string& name,
                           const std::string& key,
                           ValueType* value,
                           bool ordered) {
    if (!inited_) {
        return Status::DBClosed();
    }

    ROCKSDB_NAMESPACE::Status s;
    std::string svalue;
    std::string ikey = ToInternalKey(name, key, ordered);
    auto handle = GetColumnFamilyHandle(ordered);
    {
        RocksDBPerfGuard guard(OP_GET);
        s = InTransaction_ ? txn_->Get(dbReadOptions_, handle, ikey, &svalue) :
                             db_->Get(dbReadOptions_, handle, ikey, &svalue);
    }
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
    RocksDBPerfGuard guard(OP_PUT);
    ROCKSDB_NAMESPACE::Status s = InTransaction_ ?
        txn_->Put(handle, ikey, svalue) :
        db_->Put(dbWriteOptions_, handle, ikey, svalue);
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
    RocksDBPerfGuard guard(OP_DELETE);
    ROCKSDB_NAMESPACE::Status s = InTransaction_ ?
        txn_->Delete(handle, ikey) :
        db_->Delete(dbWriteOptions_, handle, ikey);
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
        this, std::move(ikey), 0, status, ordered);
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

    // TODO(all): Maybe we should let `Clear` just do nothing, because it's only
    // called when recover state machine from raft snapshot, and in this case,
    // out implementation is close and remove current database, and reopen from
    // database's checkpoint in raft snapshot
    // But, currently, many unittest cases depend it

    auto handle = GetColumnFamilyHandle(ordered);
    std::string lower = ToInternalName(name, ordered, true);
    std::string upper = ToInternalName(name, ordered, false);
    RocksDBPerfGuard guard(OP_DELETE_RANGE);
    ROCKSDB_NAMESPACE::Status s = db_->DeleteRange(
        dbWriteOptions_, handle, lower, upper);
    LOG(INFO) << "Clear(), tablename = " << name << ", ordered = " << ordered
              << ", lower key = " << lower << ", upper key = " << upper;
    return ToStorageStatus(s);
}

std::shared_ptr<StorageTransaction> RocksDBStorage::BeginTransaction() {
    RocksDBPerfGuard guard(OP_BEGIN_TRANSACTION);
    ROCKSDB_NAMESPACE::Transaction* txn =
        txnDB_->BeginTransaction(dbWriteOptions_);
    if (nullptr == txn) {
        return nullptr;
    }
    return std::make_shared<RocksDBStorage>(*this, txn);
}

Status RocksDBStorage::Commit() {
    if (!InTransaction_ || nullptr == txn_) {
        return Status::NotSupported();
    }

    RocksDBPerfGuard guard(OP_COMMIT_TRANSACTION);
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

    RocksDBPerfGuard guard(OP_ROLLBACK_TRANSACTION);
    ROCKSDB_NAMESPACE::Status s = txn_->Rollback();
    if (!s.ok()) {
        LOG(ERROR) << "RocksDBStorage rollback transaction failed"
                   << ", status=" << s.ToString();
    }
    delete txn_;
    return ToStorageStatus(s);
}

StorageOptions RocksDBStorage::GetStorageOptions() const {
    return options_;
}

void RocksDBStorage::InitDbOptions() {
    // if open from a clean database, the database shouldn't exists and we
    // should create one, otherwise, the database must be exists and we should
    // not create it
    const bool createIfMissing = cleanOpen_;
    const bool errorIfExists = cleanOpen_;
    InitRocksdbOptions(&dbOptions_, &dbCfDescriptors_, createIfMissing,
                       errorIfExists);

    dbTransOptions_ = rocksdb::TransactionDBOptions();

    // disable write wal and sync
    dbWriteOptions_.disableWAL = true;
    dbWriteOptions_.sync = false;

    dbReadOptions_ = rocksdb::ReadOptions();
}

namespace {

const char* const kRocksdbCheckpointPath = "rocksdb_checkpoint";

bool DoCheckpoint(rocksdb::DB* db, const std::string& dest) {
    rocksdb::Checkpoint* ckptr = nullptr;
    auto status  = rocksdb::Checkpoint::Create(db, &ckptr);
    if (!status.ok()) {
        LOG(ERROR) << "Failed to create checkpoint, " << status.ToString();
        return false;
    }

    std::unique_ptr<rocksdb::Checkpoint> ckptrGuard(ckptr);
    status = ckptr->CreateCheckpoint(dest);
    if (!status.ok()) {
        LOG(ERROR) << "Failed to create checkpoint at `" << dest << "`, "
                   << status.ToString();
        return false;
    }

    return true;
}

bool DuplicateRocksdbCheckpoint(const std::string& from,
                                const std::string& to) {
    LOG(INFO) << "Duplicating rocksdb storage from `" << from << "` to `" << to
              << "`";

    rocksdb::DB* db = nullptr;
    rocksdb::DBOptions dbOptions;
    std::vector<rocksdb::ColumnFamilyDescriptor> columnFamilies;

    InitRocksdbOptions(&dbOptions, &columnFamilies, /*createIfMissing*/ false);

    std::vector<rocksdb::ColumnFamilyHandle*> cfHandles;

    auto status = rocksdb::DB::OpenForReadOnly(
        dbOptions, from, columnFamilies, &cfHandles, &db,
        /* error_if_wal_file_exists */ true);

    if (!status.ok()) {
        LOG(ERROR) << "Failed to open checkpoint, error: " << status.ToString();
        return false;
    }

    std::unique_ptr<rocksdb::DB> dbGuard(db);
    for (auto* handle : cfHandles) {
        status = db->DestroyColumnFamilyHandle(handle);
        if (!status.ok()) {
            LOG(ERROR) << "Failed to destroy column family, error: "
                       << status.ToString();
            return false;
        }
    }

    return DoCheckpoint(db, to);
}

}  // namespace

bool RocksDBStorage::Checkpoint(const std::string& dir,
                                std::vector<std::string>* files) {
    rocksdb::FlushOptions options;
    options.wait = true;
    options.allow_write_stall = true;
    auto status = db_->Flush(options, handles_);
    if (!status.ok()) {
        LOG(ERROR) << "Failed to flush DB, " << status.ToString();
        return false;
    }

    const std::string dest = dir + "/" + kRocksdbCheckpointPath;
    if (!DoCheckpoint(db_, dest)) {
        return false;
    }

    std::vector<std::string> filenames;
    int ret = options_.localFileSystem->List(dest, &filenames);

    if (ret != 0) {
        LOG(ERROR) << "Failed to list checkpoint files at `" << dest << "`, "
                   << berror();
        return false;
    }

    files->reserve(filenames.size());
    for (const auto& f : filenames) {
        files->push_back(std::string(kRocksdbCheckpointPath) + "/" + f);
    }

    return true;
}

bool RocksDBStorage::Recover(const std::string& dir) {
    LOG(INFO) << "Recovering storage from `" << dir << "`";

    auto succ = Close();
    if (!succ) {
        LOG(ERROR) << "Failed to close storage before recover";
        return false;
    }

    int ret = options_.localFileSystem->Delete(options_.dataDir);
    if (ret != 0) {
        LOG(ERROR) << "Failed to delete storage dir: " << options_.dataDir;
        return false;
    }

    succ = DuplicateRocksdbCheckpoint(dir + "/" + kRocksdbCheckpointPath,
                                      options_.dataDir);
    if (!succ) {
        LOG(ERROR) << "Failed to duplicate rocksdb checkpoint";
        return false;
    }

    cleanOpen_ = false;
    InitDbOptions();
    succ = Open();
    if (!succ) {
        LOG(ERROR) << "Failed to open rocksdb";
        return false;
    }

    LOG(INFO) << "Recovered rocksdb from `" << dir << "`";
    return true;
}

}  // namespace storage
}  // namespace metaserver
}  // namespace curvefs
