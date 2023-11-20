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
 * @Project: curve
 * @Date: 2021-06-10 10:04:21
 * @Author: chenwei
 */

#include "curvefs/src/metaserver/dentry_storage.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "curvefs/src/metaserver/storage/storage.h"
#include "curvefs/src/metaserver/storage/rocksdb_storage.h"
#include "curvefs/test/metaserver/storage/utils.h"
#include "src/fs/ext4_filesystem_impl.h"
#include "src/common/timeutility.h"

namespace curvefs {
namespace metaserver {

using ::curvefs::metaserver::storage::KVStorage;
using ::curvefs::metaserver::storage::NameGenerator;
using ::curvefs::metaserver::storage::RandomStoragePath;
using ::curvefs::metaserver::storage::RocksDBStorage;
using ::curvefs::metaserver::storage::StorageOptions;
using ::curvefs::metaserver::storage::Status;
using ::curvefs::metaserver::storage::Key4Dentry;
using ::curvefs::metaserver::storage::Key4TxWrite;

namespace {
auto localfs = curve::fs::Ext4FileSystemImpl::getInstance();
}

class DentryStorageTest : public ::testing::Test {
 protected:
    void SetUp() override {
        nameGenerator_ = std::make_shared<NameGenerator>(1);
        dataDir_ = RandomStoragePath();

        StorageOptions options;
        options.dataDir = dataDir_;
        options.localFileSystem = localfs.get();
        kvStorage_ = std::make_shared<RocksDBStorage>(options);
        ASSERT_TRUE(kvStorage_->Open());
        logIndex_ = 0;
        table4TxWrite_ = nameGenerator_->GetTxWriteTableName();
        table4TxLock_ = nameGenerator_->GetTxLockTableName();
    }

    void TearDown() override {
        ASSERT_TRUE(kvStorage_->Close());
        auto output = execShell("rm -rf " + dataDir_);
        ASSERT_EQ(output.size(), 0);
    }

    std::string execShell(const std::string& cmd) {
        std::array<char, 128> buffer;
        std::string result;
        std::unique_ptr<FILE, decltype(&pclose)> pipe(popen(cmd.c_str(), "r"),
                                                      pclose);
        if (!pipe) {
            throw std::runtime_error("popen() failed!");
        }
        while (fgets(buffer.data(), buffer.size(), pipe.get()) != nullptr) {
            result += buffer.data();
        }
        return result;
    }

    Dentry GenDentry(uint32_t fsId, uint64_t parentId, const std::string& name,
                     uint64_t txId, uint64_t inodeId, bool deleteMarkFlag,
                     FsFileType type = FsFileType::TYPE_FILE) {
        Dentry dentry;
        dentry.set_fsid(fsId);
        dentry.set_parentinodeid(parentId);
        dentry.set_name(name);
        dentry.set_txid(txId);
        dentry.set_inodeid(inodeId);
        dentry.set_flag(deleteMarkFlag ? DentryFlag::DELETE_MARK_FLAG : 0);
        dentry.set_type(type);
        return dentry;
    }

    void InsertDentrys(DentryStorage* storage,
                       const std::vector<Dentry>&& dentrys) {
        // NOTE: store real transaction is unnecessary
        metaserver::TransactionRequest request;
        request.set_type(metaserver::TransactionRequest::None);
        request.set_rawpayload("");

        auto rc = storage->PrepareTx(dentrys, request, logIndex_++);
        ASSERT_EQ(rc, MetaStatusCode::OK);
        ASSERT_EQ(storage->Size(), dentrys.size());
    }

    void ASSERT_DENTRYS_EQ(const std::vector<Dentry>& lhs,
                           const std::vector<Dentry>&& rhs) {
        ASSERT_EQ(lhs, rhs);
    }

    std::string DentryKey(const Dentry& dentry) {
        Key4Dentry key(dentry.fsid(), dentry.parentinodeid(), dentry.name());
        return conv_.SerializeToString(key);
    }

    std::string TxWriteKey(const Dentry& dentry, uint64_t ts) {
        Key4TxWrite key(dentry.fsid(), dentry.parentinodeid(),
            dentry.name(), ts);
        return conv_.SerializeToString(key);
    }

 protected:
    std::string dataDir_;
    std::shared_ptr<NameGenerator> nameGenerator_;
    std::shared_ptr<KVStorage> kvStorage_;
    int64_t logIndex_;
    Converter conv_;
    std::string table4TxWrite_;
    std::string table4TxLock_;
};

TEST_F(DentryStorageTest, Insert) {
    DentryStorage storage(kvStorage_, nameGenerator_, 0);
    ASSERT_TRUE(storage.Init());
    Dentry dentry;
    dentry.set_fsid(1);
    dentry.set_parentinodeid(1);
    dentry.set_name("A");
    dentry.set_inodeid(2);
    dentry.set_txid(0);

    Dentry dentry2;
    dentry2.set_fsid(1);
    dentry2.set_parentinodeid(1);
    dentry2.set_name("A");
    dentry2.set_inodeid(3);
    dentry2.set_txid(0);

    // CASE 1: insert success
    ASSERT_EQ(storage.Insert(dentry, logIndex_++), MetaStatusCode::OK);

    // CASE 2: insert with dentry exist
    ASSERT_EQ(storage.Insert(dentry2, logIndex_++),
              MetaStatusCode::DENTRY_EXIST);
    ASSERT_EQ(storage.Size(), 1);

    // CASE 3: insert dentry failed with higher txid
    dentry.set_txid(1);
    ASSERT_EQ(storage.Insert(dentry, logIndex_++),
              MetaStatusCode::IDEMPOTENCE_OK);
    ASSERT_EQ(storage.Size(), 1);

    // CASE 4: direct insert success by handle tx
    // NOTE: store real transaction is unnecessary
    metaserver::TransactionRequest request;
    request.set_type(metaserver::TransactionRequest::None);
    request.set_rawpayload("");
    auto rc = storage.PrepareTx({dentry}, request, logIndex_++);
    ASSERT_EQ(rc, MetaStatusCode::OK);
    ASSERT_EQ(storage.Size(), 2);

    // CASE 5: insert idempotence
    ASSERT_EQ(storage.Insert(dentry, logIndex_++),
              MetaStatusCode::IDEMPOTENCE_OK);
    ASSERT_EQ(storage.Size(), 1);
}

TEST_F(DentryStorageTest, Delete) {
    DentryStorage storage(kvStorage_, nameGenerator_, 0);
    ASSERT_TRUE(storage.Init());

    // NOTE: store real transaction is unnecessary
    metaserver::TransactionRequest request;
    request.set_type(metaserver::TransactionRequest::None);
    request.set_rawpayload("");
    Dentry dentry;
    dentry.set_fsid(1);
    dentry.set_parentinodeid(1);
    dentry.set_name("A");
    dentry.set_inodeid(2);
    dentry.set_txid(0);

    // CASE 1: dentry not found
    ASSERT_EQ(storage.Delete(dentry, logIndex_++), MetaStatusCode::NOT_FOUND);
    ASSERT_EQ(storage.Size(), 0);

    // CASE 2: delete success
    ASSERT_EQ(storage.Insert(dentry, logIndex_++), MetaStatusCode::OK);
    ASSERT_EQ(storage.Size(), 1);

    ASSERT_EQ(storage.Delete(dentry, logIndex_++), MetaStatusCode::OK);
    ASSERT_EQ(storage.Size(), 0);

    // CASE 3: delete multi-dentrys with different txid
    ASSERT_EQ(storage.Insert(dentry, logIndex_++), MetaStatusCode::OK);
    dentry.set_txid(1);
    // NOTE: store real transaction is unnecessary
    auto rc = storage.PrepareTx({dentry}, request, logIndex_++);
    ASSERT_EQ(rc, MetaStatusCode::OK);
    ASSERT_EQ(storage.Size(), 2);

    dentry.set_txid(2);
    ASSERT_EQ(storage.Delete(dentry, logIndex_++), MetaStatusCode::OK);
    ASSERT_EQ(storage.Size(), 0);

    // CASE 4: delete by higher txid
    dentry.set_txid(2);
    ASSERT_EQ(storage.Insert(dentry, logIndex_++), MetaStatusCode::OK);
    ASSERT_EQ(storage.Size(), 1);

    dentry.set_txid(1);
    ASSERT_EQ(storage.Delete(dentry, logIndex_++), MetaStatusCode::NOT_FOUND);
    ASSERT_EQ(storage.Size(), 1);

    dentry.set_txid(2);
    ASSERT_EQ(storage.Delete(dentry, logIndex_++), MetaStatusCode::OK);
    ASSERT_EQ(storage.Size(), 0);

    // CASE 5: dentry deleted with DELETE_MARK_FLAG flag
    dentry.set_flag(DentryFlag::DELETE_MARK_FLAG);
    ASSERT_EQ(storage.Insert(dentry, logIndex_++), MetaStatusCode::OK);
    ASSERT_EQ(storage.Size(), 1);

    ASSERT_EQ(storage.Delete(dentry, logIndex_++), MetaStatusCode::NOT_FOUND);
    ASSERT_EQ(storage.Size(), 0);

    // CASE 6: delete by last dentry with DELETE_MARK_FLAG flag
    dentry.set_txid(0);
    ASSERT_EQ(storage.Insert(dentry, logIndex_++), MetaStatusCode::OK);
    dentry.set_txid(1);
    dentry.set_flag(DentryFlag::DELETE_MARK_FLAG);
    // NOTE: store real transaction is unnecessary
    rc = storage.PrepareTx({dentry}, request, logIndex_++);
    ASSERT_EQ(rc, MetaStatusCode::OK);
    ASSERT_EQ(storage.Size(), 2);

    ASSERT_EQ(storage.Delete(dentry, logIndex_++), MetaStatusCode::NOT_FOUND);
    ASSERT_EQ(storage.Size(), 0);
}

TEST_F(DentryStorageTest, Get) {
    DentryStorage storage(kvStorage_, nameGenerator_, 0);
    ASSERT_TRUE(storage.Init());
    Dentry dentry;

    // CASE 1: dentry not found
    dentry = GenDentry(1, 0, "A", 0, 0, false);
    ASSERT_EQ(storage.Get(&dentry), MetaStatusCode::NOT_FOUND);

    // CASE 2: get success
    InsertDentrys(&storage,
                  std::vector<Dentry>{
                      // { fsId, parentId, name, txId, inodeId, deleteMarkFlag }
                      GenDentry(1, 0, "A", 0, 1, false),
                      GenDentry(1, 0, "B", 0, 2, false),
                  });

    dentry = GenDentry(1, 0, "A", 0, 0, false);
    ASSERT_EQ(storage.Get(&dentry), MetaStatusCode::OK);
    ASSERT_EQ(dentry.inodeid(), 1);
    ASSERT_EQ(storage.Size(), 2);

    dentry = GenDentry(1, 0, "B", 0, 0, false);
    ASSERT_EQ(storage.Get(&dentry), MetaStatusCode::OK);
    ASSERT_EQ(dentry.inodeid(), 2);
    ASSERT_EQ(storage.Size(), 2);

    // CASE 3: get multi-dentrys with different txid
    storage.Clear();
    InsertDentrys(&storage,
                  std::vector<Dentry>{
                      // { fsId, parentId, name, txId, inodeId, deleteMarkFlag }
                      GenDentry(1, 0, "A", 0, 1, false),
                      GenDentry(1, 0, "A", 1, 2, false),
                  });

    dentry = GenDentry(1, 0, "A", 1, 0, false);
    ASSERT_EQ(storage.Get(&dentry), MetaStatusCode::OK);
    ASSERT_EQ(dentry.inodeid(), 2);
    ASSERT_EQ(storage.Size(), 2);

    // CASE 4: get dentry with DELETE_MARK_FLAG flag
    storage.Clear();
    InsertDentrys(&storage,
                  std::vector<Dentry>{
                      // { fsId, parentId, name, txId, inodeId, deleteMarkFlag }
                      GenDentry(1, 0, "A", 0, 1, false),
                      GenDentry(1, 0, "A", 1, 1, true),
                  });

    dentry = GenDentry(1, 0, "A", 1, 0, false);
    ASSERT_EQ(storage.Get(&dentry), MetaStatusCode::NOT_FOUND);
    ASSERT_EQ(dentry.inodeid(), 0);
    ASSERT_EQ(storage.Size(), 2);
}

TEST_F(DentryStorageTest, List) {
    DentryStorage storage(kvStorage_, nameGenerator_, 0);
    ASSERT_TRUE(storage.Init());
    std::vector<Dentry> dentrys;
    Dentry dentry;

    // CASE 1: basic list
    InsertDentrys(&storage,
                  std::vector<Dentry>{
                      // { fsId, parentId, name, txId, inodeId, deleteMarkFlag }
                      GenDentry(1, 0, "A1", 0, 1, false),
                      GenDentry(1, 0, "A2", 0, 2, false),
                      GenDentry(1, 0, "A3", 0, 3, false),
                      GenDentry(1, 0, "A4", 0, 4, false),
                      GenDentry(1, 0, "A5", 0, 5, false),
                  });

    dentry = GenDentry(1, 0, "", 0, 0, false);
    ASSERT_EQ(storage.List(dentry, &dentrys, 0), MetaStatusCode::OK);
    ASSERT_EQ(dentrys.size(), 5);
    ASSERT_DENTRYS_EQ(dentrys, std::vector<Dentry>{
                                   GenDentry(1, 0, "A1", 0, 1, false),
                                   GenDentry(1, 0, "A2", 0, 2, false),
                                   GenDentry(1, 0, "A3", 0, 3, false),
                                   GenDentry(1, 0, "A4", 0, 4, false),
                                   GenDentry(1, 0, "A5", 0, 5, false),
                               });

    // CASE 2: list by specify name
    dentrys.clear();
    dentry = GenDentry(1, 0, "A3", 0, 0, false);
    ASSERT_EQ(storage.List(dentry, &dentrys, 0), MetaStatusCode::OK);
    ASSERT_EQ(dentrys.size(), 2);
    ASSERT_DENTRYS_EQ(dentrys, std::vector<Dentry>{
                                   GenDentry(1, 0, "A4", 0, 4, false),
                                   GenDentry(1, 0, "A5", 0, 5, false),
                               });

    // CASE 3: list by lower txid
    storage.Clear();
    InsertDentrys(&storage,
                  std::vector<Dentry>{
                      // { fsId, parentId, name, txId, inodeId, deleteMarkFlag }
                      GenDentry(1, 0, "A1", 1, 1, false),
                      GenDentry(1, 0, "A2", 2, 2, false),
                      GenDentry(1, 0, "A3", 3, 3, false),
                  });

    dentrys.clear();
    dentry = GenDentry(1, 0, "", 2, 0, false);
    ASSERT_EQ(storage.List(dentry, &dentrys, 0), MetaStatusCode::OK);
    ASSERT_EQ(dentrys.size(), 2);
    ASSERT_DENTRYS_EQ(dentrys, std::vector<Dentry>{
                                   GenDentry(1, 0, "A1", 1, 1, false),
                                   GenDentry(1, 0, "A2", 2, 2, false),
                               });

    // CASE 4: list by higher txid
    dentrys.clear();
    dentry = GenDentry(1, 0, "", 4, 0, false);
    ASSERT_EQ(storage.List(dentry, &dentrys, 0), MetaStatusCode::OK);
    ASSERT_EQ(dentrys.size(), 3);
    ASSERT_DENTRYS_EQ(dentrys, std::vector<Dentry>{
                                   GenDentry(1, 0, "A1", 1, 1, false),
                                   GenDentry(1, 0, "A2", 2, 2, false),
                                   GenDentry(1, 0, "A3", 3, 3, false),
                               });

    // CASE 5: list dentrys which has DELETE_MARK_FLAG flag
    storage.Clear();
    InsertDentrys(&storage,
                  std::vector<Dentry>{
                      // { fsId, parentId, name, txId, inodeId, deleteMarkFlag }
                      GenDentry(1, 0, "A1", 1, 1, false),
                      GenDentry(1, 0, "A2", 2, 2, true),
                      GenDentry(1, 0, "A3", 3, 3, false),
                  });

    dentrys.clear();
    dentry = GenDentry(1, 0, "", 3, 0, false);
    ASSERT_EQ(storage.List(dentry, &dentrys, 0), MetaStatusCode::OK);
    ASSERT_EQ(dentrys.size(), 2);
    ASSERT_DENTRYS_EQ(dentrys, std::vector<Dentry>{
                                   GenDentry(1, 0, "A1", 1, 1, false),
                                   GenDentry(1, 0, "A3", 3, 3, false),
                               });

    // CASE 6: list same dentrys with different txid
    storage.Clear();
    InsertDentrys(&storage,
                  std::vector<Dentry>{
                      // { fsId, parentId, name, txId, inodeId, deleteMarkFlag }
                      GenDentry(1, 0, "A", 0, 1, false),
                      GenDentry(1, 0, "A", 1, 1, false),
                      GenDentry(1, 0, "A", 2, 1, false),
                  });

    dentrys.clear();
    dentry = GenDentry(1, 0, "", 2, 0, false);
    ASSERT_EQ(storage.List(dentry, &dentrys, 0), MetaStatusCode::OK);
    ASSERT_EQ(dentrys.size(), 1);
    ASSERT_DENTRYS_EQ(dentrys, std::vector<Dentry>{
                                   GenDentry(1, 0, "A", 2, 1, false),
                               });

    // CASE 7: list by dentry tree
    storage.Clear();
    InsertDentrys(&storage,
                  std::vector<Dentry>{
                      // { fsId, parentId, name, txId, inodeId, deleteMarkFlag }
                      GenDentry(1, 0, "A", 0, 1, false),
                      GenDentry(1, 0, "B", 0, 2, false),
                      GenDentry(1, 2, "C", 0, 3, false),
                      GenDentry(1, 2, "D", 0, 4, false),
                      GenDentry(1, 2, "E", 0, 5, false),
                      GenDentry(1, 4, "F", 0, 6, true),
                      GenDentry(1, 4, "G", 0, 7, false),
                  });

    dentrys.clear();
    dentry = GenDentry(1, 2, "", 0, 0, false);
    ASSERT_EQ(storage.List(dentry, &dentrys, 0), MetaStatusCode::OK);
    ASSERT_EQ(dentrys.size(), 3);
    ASSERT_DENTRYS_EQ(dentrys, std::vector<Dentry>{
                                   GenDentry(1, 2, "C", 0, 3, false),
                                   GenDentry(1, 2, "D", 0, 4, false),
                                   GenDentry(1, 2, "E", 0, 5, false),
                               });

    dentrys.clear();
    dentry = GenDentry(1, 4, "", 0, 0, false);
    ASSERT_EQ(storage.List(dentry, &dentrys, 0), MetaStatusCode::OK);
    ASSERT_EQ(dentrys.size(), 1);
    ASSERT_DENTRYS_EQ(dentrys, std::vector<Dentry>{
                                   GenDentry(1, 4, "G", 0, 7, false),
                               });

    // CASE 8: list empty directory
    storage.Clear();
    InsertDentrys(&storage,
                  std::vector<Dentry>{
                      // { fsId, parentId, name, txId, inodeId, deleteMarkFlag }
                      GenDentry(1, 0, "A", 0, 1, false),
                      GenDentry(1, 0, "B", 0, 2, false),
                      GenDentry(1, 2, "D", 0, 4, true),
                      GenDentry(1, 2, "E", 0, 5, true),
                  });

    dentrys.clear();
    dentry = GenDentry(1, 2, "", 0, 0, false);
    ASSERT_EQ(storage.List(dentry, &dentrys, 0), MetaStatusCode::OK);
    ASSERT_EQ(dentrys.size(), 0);

    dentrys.clear();
    dentry = GenDentry(1, 3, "", 0, 0, false);
    ASSERT_EQ(storage.List(dentry, &dentrys, 0), MetaStatusCode::OK);
    ASSERT_EQ(dentrys.size(), 0);

    dentrys.clear();
    dentry = GenDentry(2, 0, "", 0, 0, false);
    ASSERT_EQ(storage.List(dentry, &dentrys, 0), MetaStatusCode::OK);
    ASSERT_EQ(dentrys.size(), 0);

    // CASE 9: list directory only
    storage.Clear();
    InsertDentrys(
        &storage,
        std::vector<Dentry>{
            // { fsId, parentId, name, txId, inodeId, deleteMarkFlag }
            GenDentry(1, 0, "A", 0, 1, false, FsFileType::TYPE_DIRECTORY),
            GenDentry(1, 0, "B", 0, 2, true, FsFileType::TYPE_DIRECTORY),
            GenDentry(1, 0, "D", 0, 3, false),
            GenDentry(1, 0, "E", 0, 4, false),
        });

    dentrys.clear();
    dentry = GenDentry(1, 0, "", 0, 0, false);
    ASSERT_EQ(storage.List(dentry, &dentrys, 0, true), MetaStatusCode::OK);
    ASSERT_EQ(dentrys.size(), 1);

    // CASE 10: list directory only with limit
    storage.Clear();
    InsertDentrys(&storage,
                  std::vector<Dentry>{
                      // { fsId, parentId, name, txId, inodeId, deleteMarkFlag }
                      GenDentry(1, 0, "A", 0, 1, false),
                      GenDentry(1, 0, "B", 0, 2, false),
                      GenDentry(1, 0, "D", 0, 3, false),
                  });

    dentrys.clear();
    dentry = GenDentry(1, 0, "", 0, 0, false);
    ASSERT_EQ(storage.List(dentry, &dentrys, 1, true), MetaStatusCode::OK);
    ASSERT_EQ(dentrys.size(), 1);

    storage.Clear();
    InsertDentrys(
        &storage,
        std::vector<Dentry>{
            // { fsId, parentId, name, txId, inodeId, deleteMarkFlag }
            GenDentry(1, 0, "A", 0, 1, false, FsFileType::TYPE_DIRECTORY),
            GenDentry(1, 0, "B", 0, 2, false),
            GenDentry(1, 0, "D", 0, 3, false),
        });

    dentrys.clear();
    dentry = GenDentry(1, 0, "", 0, 0, false);
    ASSERT_EQ(storage.List(dentry, &dentrys, 3, true), MetaStatusCode::OK);
    ASSERT_EQ(dentrys.size(), 2);
}

TEST_F(DentryStorageTest, HandleTx) {
    DentryStorage storage(kvStorage_, nameGenerator_, 0);
    ASSERT_TRUE(storage.Init());
    std::vector<Dentry> dentrys;
    Dentry dentry;
    // NOTE: store real transaction is unnecessary
    metaserver::TransactionRequest request;
    request.set_type(metaserver::TransactionRequest::None);
    request.set_rawpayload("");
    // CASE 1: prepare success
    InsertDentrys(&storage,
                  std::vector<Dentry>{
                      // { fsId, parentId, name, txId, inodeId, deleteMarkFlag }
                      GenDentry(1, 0, "A", 0, 1, false),
                  });

    dentry = GenDentry(1, 0, "A", 1, 2, false);
    // NOTE: store real transaction is unnecessary
    auto rc = storage.PrepareTx({dentry}, request, logIndex_++);
    ASSERT_EQ(rc, MetaStatusCode::OK);
    ASSERT_EQ(storage.Size(), 2);

    // CASE 2: prepare with dentry exist
    dentry = GenDentry(1, 0, "A", 1, 2, false);
    /// NOTE: store real transaction is unnecessary
    rc = storage.PrepareTx({dentry}, request, logIndex_++);
    ASSERT_EQ(rc, MetaStatusCode::OK);
    ASSERT_EQ(storage.Size(), 2);

    // CASE 3: commit success
    rc = storage.CommitTx({dentry}, logIndex_++);
    ASSERT_EQ(rc, MetaStatusCode::OK);
    ASSERT_EQ(storage.Size(), 1);

    dentry = GenDentry(1, 0, "A", 1, 0, false);
    ASSERT_EQ(storage.Get(&dentry), MetaStatusCode::OK);
    ASSERT_EQ(dentry.inodeid(), 2);

    // CASE 3: commit dentry with DELETE_MARK_FLAG flag
    storage.Clear();
    InsertDentrys(&storage,
                  std::vector<Dentry>{
                      // { fsId, parentId, name, txId, inodeId, deleteMarkFlag }
                      GenDentry(1, 0, "A", 0, 1, false),
                      GenDentry(1, 0, "A", 1, 1, true),
                  });

    dentry = GenDentry(1, 0, "A", 1, 0, false);
    rc = storage.CommitTx({dentry}, logIndex_++);
    ASSERT_EQ(rc, MetaStatusCode::OK);
    ASSERT_EQ(storage.Size(), 0);

    // CASE 4: Rollback success
    storage.Clear();
    InsertDentrys(&storage,
                  std::vector<Dentry>{
                      // { fsId, parentId, name, txId, inodeId, deleteMarkFlag }
                      GenDentry(1, 0, "A", 0, 1, false),
                      GenDentry(1, 0, "A", 1, 2, false),
                  });
    ASSERT_EQ(storage.Size(), 2);

    dentry = GenDentry(1, 0, "A", 1, 2, false);
    rc = storage.RollbackTx({dentry}, logIndex_++);
    ASSERT_EQ(rc, MetaStatusCode::OK);
    ASSERT_EQ(storage.Size(), 1);

    dentry = GenDentry(1, 0, "A", 1, 0, false);
    ASSERT_EQ(storage.Get(&dentry), MetaStatusCode::OK);
    ASSERT_EQ(dentry.inodeid(), 1);
}

TEST_F(DentryStorageTest, PrewriteTx) {
    DentryStorage storage(kvStorage_, nameGenerator_, 0);
    ASSERT_TRUE(storage.Init());

    // 1. prepare original dentry
    // { fsId, parentId, name, txId, inodeId, deleteMarkFlag }
    Dentry dentry = GenDentry(1, 1, "A", 0, 2, false);
    ASSERT_EQ(storage.Insert(dentry, logIndex_++), MetaStatusCode::OK);
    ASSERT_EQ(storage.Size(), 1);

    // 2. prepare prewrite dentry
    uint64_t startTs = 2;
    Dentry dentryA = GenDentry(1, 1, "A", startTs, 2, true);
    Dentry dentryB = GenDentry(1, 1, "B", startTs, 3, false);
    std::vector<Dentry> dentrys = {dentryA, dentryB};
    TxLock txLock;
    txLock.set_primarykey(DentryKey(dentryA));
    txLock.set_startts(startTs);
    txLock.set_timestamp(curve::common::TimeUtility::GetTimeofDayMs());

    // 2.1 write conflict
    TxLock outLock;
    TxWrite txWrite;
    txWrite.set_startts(startTs);
    txWrite.set_kind(TxWriteKind::Commit);
    Status s = kvStorage_->SSet(table4TxWrite_,
        TxWriteKey(dentry, startTs + 1), txWrite);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(storage.PrewriteTx(dentrys, txLock, logIndex_++, &outLock),
              MetaStatusCode::TX_WRITE_CONFLICT);
    s = kvStorage_->SDel(table4TxWrite_, TxWriteKey(dentry, startTs + 1));
    ASSERT_TRUE(s.ok());

    // 2.2 key locked and IDEMPOTENCE OK
    s = kvStorage_->SSet(table4TxLock_, DentryKey(dentryA), txLock);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(storage.PrewriteTx(dentrys, txLock, logIndex_++, &outLock),
              MetaStatusCode::OK);
    s = kvStorage_->SDel(table4TxLock_, DentryKey(dentryA));
    ASSERT_TRUE(s.ok());

    // 2.3 key locked
    TxLock preLock(txLock);
    preLock.set_startts(startTs + 1);
    s = kvStorage_->SSet(table4TxLock_, DentryKey(dentryA), preLock);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(storage.PrewriteTx(dentrys, txLock, logIndex_++, &outLock),
              MetaStatusCode::TX_KEY_LOCKED);
    s = kvStorage_->SDel(table4TxLock_, DentryKey(dentryA));
    ASSERT_TRUE(s.ok());

    // 2.4 prewrite success
    ASSERT_EQ(storage.PrewriteTx(
        std::vector<Dentry>(dentrys.begin() + outLock.index(), dentrys.end()),
        txLock, logIndex_++, &outLock), MetaStatusCode::OK);
    ASSERT_EQ(storage.Size(), 3);
    Dentry entryOut;
    entryOut.set_fsid(1);
    entryOut.set_parentinodeid(1);
    entryOut.set_name("A");
    ASSERT_EQ(storage.Get(&entryOut), MetaStatusCode::OK);
    ASSERT_TRUE(dentry == entryOut);
    entryOut.set_name("B");
    ASSERT_EQ(storage.Get(&entryOut), MetaStatusCode::NOT_FOUND);
}

TEST_F(DentryStorageTest, CheckTxStatus) {
    DentryStorage storage(kvStorage_, nameGenerator_, 0);
    ASSERT_TRUE(storage.Init());

    // 1. tx lock exist, tx timeout
    uint64_t startTs = 1;
    uint64_t now = curve::common::TimeUtility::GetTimeofDayMs();
    Dentry dentry = GenDentry(1, 1, "A", startTs, 2, false);
    TxLock txLock;
    txLock.set_primarykey(DentryKey(dentry));
    txLock.set_startts(startTs);
    txLock.set_timestamp(now - 10);
    txLock.set_ttl(5);
    Status s = kvStorage_->SSet(table4TxLock_, DentryKey(dentry), txLock);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(
        storage.CheckTxStatus(DentryKey(dentry), startTs, now, logIndex_++),
        MetaStatusCode::TX_TIMEOUT);

    // 2. tx lock exist, tx in progress
    txLock.set_timestamp(now);
    s = kvStorage_->SSet(table4TxLock_, DentryKey(dentry), txLock);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(
        storage.CheckTxStatus(DentryKey(dentry), startTs, now, logIndex_++),
        MetaStatusCode::TX_INPROGRESS);
    s = kvStorage_->SDel(table4TxLock_, DentryKey(dentry));
    ASSERT_TRUE(s.ok());

    // 3. tx lock not exist, tx write not exit, committed
    ASSERT_EQ(
        storage.CheckTxStatus(DentryKey(dentry), startTs, now, logIndex_++),
        MetaStatusCode::TX_COMMITTED);
    TxWrite txWrite;
    txWrite.set_startts(startTs);
    txWrite.set_kind(TxWriteKind::Commit);
    s = kvStorage_->SSet(table4TxWrite_,
        TxWriteKey(dentry, startTs + 1), txWrite);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(
        storage.CheckTxStatus(DentryKey(dentry), startTs, now, logIndex_++),
        MetaStatusCode::TX_COMMITTED);

    // 4. tx lock not exist, rollbacked
    txWrite.set_kind(TxWriteKind::Rollback);
    s = kvStorage_->SSet(table4TxWrite_, TxWriteKey(dentry, startTs), txWrite);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(
        storage.CheckTxStatus(DentryKey(dentry), startTs, now, logIndex_++),
        MetaStatusCode::TX_ROLLBACKED);
}

TEST_F(DentryStorageTest, ResolveTxLock) {
    DentryStorage storage(kvStorage_, nameGenerator_, 0);
    ASSERT_TRUE(storage.Init());

    uint64_t preTxStartTs = 1;
    uint64_t startTs = 10;
    uint64_t commitTs = 11;
    uint64_t now = curve::common::TimeUtility::GetTimeofDayMs();
    Dentry dentry = GenDentry(1, 1, "A", startTs, 2, false);

    // 1. tx lock not exist
    ASSERT_EQ(storage.ResolveTxLock(dentry, preTxStartTs, commitTs,
        logIndex_++), MetaStatusCode::OK);

    // 2. roll forward
    // 2.1 tx lock exist but startts mismatch
    TxLock preTxLock;
    preTxLock.set_primarykey(DentryKey(dentry));
    preTxLock.set_startts(preTxStartTs + 1);
    preTxLock.set_timestamp(now-100);
    Status s = kvStorage_->SSet(table4TxLock_, DentryKey(dentry), preTxLock);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(storage.ResolveTxLock(dentry, preTxStartTs, commitTs,
        logIndex_++), MetaStatusCode::TX_MISMATCH);
    // 2.2 success
    preTxLock.set_startts(preTxStartTs);
    s = kvStorage_->SSet(table4TxLock_, DentryKey(dentry), preTxLock);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(storage.ResolveTxLock(dentry, preTxStartTs, commitTs,
        logIndex_++), MetaStatusCode::OK);
    TxLock lockOut;
    s = kvStorage_->SGet(table4TxLock_, DentryKey(dentry), &lockOut);
    ASSERT_TRUE(s.IsNotFound());
    TxWrite txWriteOut;
    s = kvStorage_->SGet(table4TxWrite_, TxWriteKey(dentry, commitTs),
        &txWriteOut);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(txWriteOut.kind(), TxWriteKind::Commit);
    ASSERT_EQ(txWriteOut.startts(), preTxStartTs);
    TS tsOut;
    s = kvStorage_->SGet(table4TxWrite_, "latestCommit", &tsOut);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(tsOut.ts(), commitTs);

    // 3. roll backward
    // prepare rollback site
    TxLock txLock(preTxLock);
    txLock.set_startts(startTs);
    s = kvStorage_->SSet(table4TxLock_, DentryKey(dentry), txLock);
    ASSERT_TRUE(s.ok());
    dentry.set_txid(startTs);
    ASSERT_EQ(storage.Insert(dentry, logIndex_++), MetaStatusCode::OK);
    ASSERT_EQ(storage.Size(), 1);
    ASSERT_EQ(storage.ResolveTxLock(dentry, startTs, 0,
        logIndex_++), MetaStatusCode::OK);
    s = kvStorage_->SGet(table4TxLock_, DentryKey(dentry), &lockOut);
    ASSERT_TRUE(s.IsNotFound());
    ASSERT_EQ(storage.Size(), 0);
    s = kvStorage_->SGet(table4TxWrite_, TxWriteKey(dentry, startTs),
        &txWriteOut);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(txWriteOut.kind(), TxWriteKind::Rollback);
    ASSERT_EQ(txWriteOut.startts(), startTs);
}

TEST_F(DentryStorageTest, CommitTx) {
    DentryStorage storage(kvStorage_, nameGenerator_, 0);
    ASSERT_TRUE(storage.Init());

    uint64_t startTs = 1;
    uint64_t commitTs = 2;
    uint64_t now = curve::common::TimeUtility::GetTimeofDayMs();
    Dentry dentryA = GenDentry(1, 1, "A", startTs, 2, true);
    Dentry dentryB = GenDentry(1, 1, "B", startTs, 2, false);

    // 1. tx lock not exist
    ASSERT_EQ(storage.CommitTx(
        {dentryA, dentryB}, startTs, commitTs, logIndex_++),
        MetaStatusCode::OK);
    TS tsOut;
    Status s = kvStorage_->SGet(table4TxWrite_, "latestCommit", &tsOut);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(tsOut.ts(), startTs);
    // 2. tx lock exist, but startts mismatch
    TxLock txLock;
    txLock.set_primarykey(DentryKey(dentryA));
    txLock.set_startts(startTs + 1);
    txLock.set_timestamp(now - 100);
    s = kvStorage_->SSet(table4TxLock_, DentryKey(dentryA), txLock);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(storage.CommitTx(
        {dentryA, dentryB}, startTs, commitTs, logIndex_++),
        MetaStatusCode::TX_MISMATCH);
    // 3. commit success
    TxLock txLockA;
    txLockA.set_primarykey(DentryKey(dentryA));
    txLockA.set_startts(startTs);
    txLockA.set_timestamp(now - 100);
    s = kvStorage_->SSet(table4TxLock_, DentryKey(dentryA), txLockA);
    ASSERT_TRUE(s.ok());
    TxLock txLockB;
    txLockB.set_primarykey(DentryKey(dentryB));
    txLockB.set_startts(startTs);
    txLockB.set_timestamp(now - 100);
    s = kvStorage_->SSet(table4TxLock_, DentryKey(dentryB), txLockB);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(storage.CommitTx(
        {dentryA, dentryB}, startTs, commitTs, logIndex_++),
        MetaStatusCode::OK);
    TxLock lockOut;
    s = kvStorage_->SGet(table4TxLock_, DentryKey(dentryA), &lockOut);
    ASSERT_TRUE(s.IsNotFound());
    s = kvStorage_->SGet(table4TxLock_, DentryKey(dentryB), &lockOut);
    ASSERT_TRUE(s.IsNotFound());
    TxWrite txWriteOut;
    s = kvStorage_->SGet(table4TxWrite_, TxWriteKey(dentryA, commitTs),
        &txWriteOut);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(txWriteOut.kind(), TxWriteKind::Commit);
    ASSERT_EQ(txWriteOut.startts(), startTs);
    s = kvStorage_->SGet(table4TxWrite_, TxWriteKey(dentryB, commitTs),
        &txWriteOut);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(txWriteOut.kind(), TxWriteKind::Commit);
    ASSERT_EQ(txWriteOut.startts(), startTs);
    s = kvStorage_->SGet(table4TxWrite_, "latestCommit", &tsOut);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(tsOut.ts(), startTs);
}


}  // namespace metaserver
}  // namespace curvefs
