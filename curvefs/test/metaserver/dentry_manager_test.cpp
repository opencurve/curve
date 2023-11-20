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
 * @Date: 2021-06-10 10:04:37
 * @Author: chenwei
 */

#include "curvefs/src/metaserver/dentry_manager.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <memory>

#include "curvefs/src/metaserver/storage/storage.h"
#include "curvefs/src/metaserver/storage/rocksdb_storage.h"
#include "curvefs/test/metaserver/storage/utils.h"
#include "src/fs/ext4_filesystem_impl.h"

namespace curvefs {
namespace metaserver {

namespace storage {
    DECLARE_int32(tx_lock_ttl_ms);
}

using ::curvefs::metaserver::storage::KVStorage;
using ::curvefs::metaserver::storage::RandomStoragePath;
using ::curvefs::metaserver::storage::RocksDBStorage;
using ::curvefs::metaserver::storage::StorageOptions;

namespace {
auto localfs = curve::fs::Ext4FileSystemImpl::getInstance();
const std::string kBaseTestDir = "./dentry_manager_test";  // NOLINT
}  // namespace

class DentryManagerTest : public ::testing::Test {
 protected:
    void SetUp() override {
        nameGenerator_ = std::make_shared<NameGenerator>(1);
        testDataDir_ = kBaseTestDir + "/" + RandomStoragePath();
        ASSERT_EQ(0, localfs->Mkdir(testDataDir_));

        StorageOptions options;
        options.dataDir = testDataDir_;
        options.type = "rocksdb";
        options.localFileSystem = localfs.get();
        kvStorage_ = std::make_shared<RocksDBStorage>(options);
        ASSERT_TRUE(kvStorage_->Open());
        dentryStorage_ =
            std::make_shared<DentryStorage>(kvStorage_, nameGenerator_, 0);
        common::PartitionInfo partitionInfo;
        partitionInfo.set_partitionid(1);
        txManager_ = std::make_shared<TxManager>(dentryStorage_, partitionInfo);
        dentryManager_ =
            std::make_shared<DentryManager>(dentryStorage_, txManager_);
        ASSERT_TRUE(dentryManager_->Init());
        ASSERT_TRUE(txManager_->Init());
        logIndex_ = 0;
    }

    void TearDown() override {
        ASSERT_TRUE(kvStorage_->Close());
        ASSERT_EQ(0, localfs->Delete(kBaseTestDir));
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
                     uint64_t txId, uint64_t inodeId, bool deleteMarkFlag) {
        Dentry dentry;
        dentry.set_fsid(fsId);
        dentry.set_parentinodeid(parentId);
        dentry.set_name(name);
        dentry.set_txid(txId);
        dentry.set_inodeid(inodeId);
        dentry.set_flag(deleteMarkFlag ? DentryFlag::DELETE_MARK_FLAG : 0);
        return dentry;
    }

 protected:
    std::shared_ptr<NameGenerator> nameGenerator_;
    std::string testDataDir_;
    std::string tablename_;
    std::shared_ptr<KVStorage> kvStorage_;
    std::shared_ptr<DentryStorage> dentryStorage_;
    std::shared_ptr<DentryManager> dentryManager_;
    std::shared_ptr<TxManager> txManager_;
    int64_t logIndex_;
};

TEST_F(DentryManagerTest, CreateDentry) {
    // CASE 1: CreateDentry: success
    auto dentry = GenDentry(1, 0, "A", 0, 1, false);
    ASSERT_EQ(dentryManager_->CreateDentry(dentry, logIndex_++),
              MetaStatusCode::OK);
    ASSERT_EQ(dentryStorage_->Size(), 1);

    // CASE 2: CreateDentry: dentry exist
    auto dentry2 = GenDentry(1, 0, "A", 0, 2, false);
    ASSERT_EQ(dentryManager_->CreateDentry(dentry2, logIndex_++),
              MetaStatusCode::DENTRY_EXIST);
    ASSERT_EQ(dentryStorage_->Size(), 1);
}

TEST_F(DentryManagerTest, DeleteDentry) {
    // CASE 1: DeleteDentry: not found
    auto dentry = GenDentry(1, 0, "A", 0, 1, false);
    ASSERT_EQ(dentryManager_->DeleteDentry(dentry, logIndex_++),
              MetaStatusCode::NOT_FOUND);

    // CASE 2: DeleteDentry: sucess
    ASSERT_EQ(dentryManager_->CreateDentry(dentry, logIndex_++),
              MetaStatusCode::OK);
    ASSERT_EQ(dentryStorage_->Size(), 1);
    ASSERT_EQ(dentryManager_->DeleteDentry(dentry, logIndex_++),
              MetaStatusCode::OK);
    ASSERT_EQ(dentryStorage_->Size(), 0);
}

TEST_F(DentryManagerTest, ClearDentry) {
    auto dentry = GenDentry(1, 0, "A", 0, 1, false);
    ASSERT_EQ(dentryManager_->CreateDentry(dentry, logIndex_++),
              MetaStatusCode::OK);
    ASSERT_EQ(dentryStorage_->Size(), 1);
    dentryManager_->ClearDentry();
    ASSERT_EQ(dentryStorage_->Size(), 0);
}

TEST_F(DentryManagerTest, GetDentry) {
    // CASE 1: GetDentry: not found
    auto dentry = GenDentry(1, 0, "A", 0, 1, false);
    ASSERT_EQ(dentryManager_->GetDentry(&dentry), MetaStatusCode::NOT_FOUND);

    // CASE 2: GetDentry: success
    ASSERT_EQ(dentryManager_->CreateDentry(dentry, logIndex_++),
              MetaStatusCode::OK);
    ASSERT_EQ(dentryStorage_->Size(), 1);
    dentry = GenDentry(1, 0, "A", 0, 0, false);
    ASSERT_EQ(dentryManager_->GetDentry(&dentry), MetaStatusCode::OK);
    ASSERT_EQ(dentry.inodeid(), 1);
}

TEST_F(DentryManagerTest, ListDentry) {
    auto dentry1 = GenDentry(1, 0, "A", 0, 1, false);
    auto dentry2 = GenDentry(1, 0, "B", 0, 2, false);
    ASSERT_EQ(dentryManager_->CreateDentry(dentry1, logIndex_++),
              MetaStatusCode::OK);
    ASSERT_EQ(dentryManager_->CreateDentry(dentry2, logIndex_++),
              MetaStatusCode::OK);
    ASSERT_EQ(dentryStorage_->Size(), 2);

    std::vector<Dentry> dentrys;
    auto dentry = GenDentry(1, 0, "", 0, 0, false);
    auto rc = dentryManager_->ListDentry(dentry, &dentrys, 0);
    ASSERT_EQ(rc, MetaStatusCode::OK);
    ASSERT_EQ(dentrys.size(), 2);
    ASSERT_EQ(dentrys[0].name(), "A");
    ASSERT_EQ(dentrys[1].name(), "B");
}

TEST_F(DentryManagerTest, HandleRenameTx) {
    // CASE 1: HandleRenameTx: param error
    auto dentrys = std::vector<Dentry>();
    auto rc = txManager_->HandleRenameTx(dentrys, logIndex_++);
    ASSERT_EQ(rc, MetaStatusCode::PARAM_ERROR);

    // CASE 2: HandleRenameTx success
    dentrys = std::vector<Dentry>{
        // { fsId, parentId, name, txId, inodeId, deleteMarkFlag }
        GenDentry(1, 0, "A", 1, 1, false),
    };
    rc = txManager_->HandleRenameTx(dentrys, logIndex_++);
    ASSERT_EQ(rc, MetaStatusCode::OK);
    ASSERT_EQ(dentryStorage_->Size(), 1);
}

TEST_F(DentryManagerTest, PrewriteRenameTx) {
    TxLock txLockIn;
    TxLock txLockOut;
    int64_t logIndex = 1;
    uint64_t startTs = 2;
    uint64_t commitTs = 3;
    Dentry dentryA = GenDentry(1, 0, "A", startTs, 1, false);
    // 1. prewrite success
    std::vector<Dentry> dentrys = std::vector<Dentry>{dentryA};
    txLockIn.set_primarykey(storage::Key4Dentry(1, 0, "A").SerializeToString());
    txLockIn.set_startts(startTs);
    txLockIn.set_timestamp(100);
    auto rc = dentryManager_->PrewriteRenameTx(dentrys, txLockIn, logIndex,
                                               &txLockOut);
    ASSERT_EQ(rc, MetaStatusCode::OK);
    // 2. tx locked
    txLockIn.set_startts(1);
    rc = dentryManager_->PrewriteRenameTx(dentrys, txLockIn, logIndex,
                                          &txLockOut);
    ASSERT_EQ(rc, MetaStatusCode::TX_KEY_LOCKED);
    ASSERT_EQ(txLockOut.startts(), startTs);
    ASSERT_EQ(txLockOut.primarykey(), txLockIn.primarykey());
    // 3. tx write conflict
    rc = dentryManager_->CommitTx(dentrys, startTs, commitTs, logIndex++);
    ASSERT_EQ(rc, MetaStatusCode::OK);
    rc = dentryManager_->PrewriteRenameTx(dentrys, txLockIn, logIndex,
                                          &txLockOut);
    ASSERT_EQ(rc, MetaStatusCode::TX_WRITE_CONFLICT);
}

TEST_F(DentryManagerTest, CheckTxStatus) {
    storage::FLAGS_tx_lock_ttl_ms = 100;

    TxLock txLockIn;
    TxLock txLockOut;
    int64_t logIndex = 1;
    uint64_t startTs = 2;
    uint64_t commitTs = 3;
    Dentry dentryA = GenDentry(1, 0, "A", startTs, 1, false);
    std::vector<Dentry> dentrys = std::vector<Dentry>{dentryA};
    txLockIn.set_primarykey(storage::Key4Dentry(1, 0, "A").SerializeToString());
    txLockIn.set_startts(startTs);
    txLockIn.set_timestamp(1000);
    auto rc = dentryManager_->PrewriteRenameTx(dentrys, txLockIn, logIndex,
                                               &txLockOut);
    ASSERT_EQ(rc, MetaStatusCode::OK);

    // timeout
    rc = dentryManager_->CheckTxStatus(txLockIn.primarykey(), startTs, 1500,
                                       logIndex);
    ASSERT_EQ(rc, MetaStatusCode::TX_TIMEOUT);
    // inprogress
    rc = dentryManager_->CheckTxStatus(txLockIn.primarykey(), startTs, 1050,
                                       logIndex);
    ASSERT_EQ(rc, MetaStatusCode::TX_INPROGRESS);
    // commited
    rc = dentryManager_->CommitTx(dentrys, startTs, commitTs, logIndex++);
    ASSERT_EQ(rc, MetaStatusCode::OK);
    rc = dentryManager_->CheckTxStatus(txLockIn.primarykey(), startTs, 1500,
                                       logIndex);
    ASSERT_EQ(rc, MetaStatusCode::TX_COMMITTED);
}


TEST_F(DentryManagerTest, ResolveTxLock) {
    TxLock txLockIn;
    TxLock txLockOut;
    int64_t logIndex = 1;
    uint64_t startTs = 2;
    uint64_t commitTs = 3;
    Dentry dentryA = GenDentry(1, 0, "A", startTs, 1, false);
    std::vector<Dentry> dentrys = std::vector<Dentry>{dentryA};
    txLockIn.set_primarykey(storage::Key4Dentry(1, 0, "A").SerializeToString());
    txLockIn.set_startts(startTs);
    txLockIn.set_timestamp(1000);

    // 1. tx lock not exist
    auto rc = dentryManager_->ResolveTxLock(dentryA, startTs, commitTs,
                                            logIndex++);
    ASSERT_EQ(rc, MetaStatusCode::OK);
    // 2. tx lock exist, but startts not match
    rc = dentryManager_->PrewriteRenameTx(dentrys, txLockIn, logIndex++,
                                               &txLockOut);
    ASSERT_EQ(rc, MetaStatusCode::OK);
    rc = dentryManager_->ResolveTxLock(dentryA, startTs + 1, commitTs,
                                            logIndex++);
    ASSERT_EQ(rc, MetaStatusCode::TX_MISMATCH);
    // 3. roll forward success
    rc = dentryManager_->ResolveTxLock(dentryA, startTs, commitTs, logIndex++);
    ASSERT_EQ(rc, MetaStatusCode::OK);
    rc = dentryManager_->CheckTxStatus(txLockIn.primarykey(), startTs, 1500,
                                       logIndex++);
    ASSERT_EQ(rc, MetaStatusCode::TX_COMMITTED);
    // 4. roll back success
    dentrys[0].set_txid(startTs + 2);
    txLockIn.set_startts(startTs + 2);
    commitTs++;
    rc = dentryManager_->PrewriteRenameTx(dentrys, txLockIn, logIndex++,
                                          &txLockOut);
    ASSERT_EQ(rc, MetaStatusCode::OK);
    rc = dentryManager_->ResolveTxLock(dentryA, startTs + 2, 0, logIndex++);
    ASSERT_EQ(rc, MetaStatusCode::OK);
    rc = dentryManager_->CheckTxStatus(txLockIn.primarykey(), startTs + 2, 1500,
                                       logIndex++);
    ASSERT_EQ(rc, MetaStatusCode::TX_ROLLBACKED);
}

TEST_F(DentryManagerTest, CommitTx) {
    TxLock txLockIn;
    TxLock txLockOut;
    int64_t logIndex = 1;
    uint64_t startTs = 2;
    uint64_t commitTs = 3;
    Dentry dentryA = GenDentry(1, 0, "A", startTs, 1, false);
    std::vector<Dentry> dentrys = std::vector<Dentry>{dentryA};

    // 1. tx lock not exist
    auto rc = dentryManager_->CommitTx(dentrys, startTs, commitTs, logIndex++);
    ASSERT_EQ(rc, MetaStatusCode::OK);
    // 2. tx lock exist, but startts not match
    txLockIn.set_primarykey(storage::Key4Dentry(1, 0, "A").SerializeToString());
    txLockIn.set_startts(startTs);
    txLockIn.set_timestamp(1000);
    rc = dentryManager_->PrewriteRenameTx(dentrys, txLockIn, logIndex++,
                                               &txLockOut);
    ASSERT_EQ(rc, MetaStatusCode::OK);
    rc = dentryManager_->CommitTx(dentrys, startTs + 1, commitTs, logIndex++);
    ASSERT_EQ(rc, MetaStatusCode::TX_MISMATCH);
    // 3. commit success
    rc = dentryManager_->CommitTx(dentrys, startTs, commitTs, logIndex++);
    ASSERT_EQ(rc, MetaStatusCode::OK);
    rc = dentryManager_->CheckTxStatus(txLockIn.primarykey(), startTs, 1500,
                                       logIndex++);
    ASSERT_EQ(rc, MetaStatusCode::TX_COMMITTED);
}

}  // namespace metaserver
}  // namespace curvefs
