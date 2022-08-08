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

using ::curvefs::metaserver::storage::KVStorage;
using ::curvefs::metaserver::storage::StorageOptions;
using ::curvefs::metaserver::storage::RocksDBStorage;
using ::curvefs::metaserver::storage::RandomStoragePath;

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
        dentryStorage_ = std::make_shared<DentryStorage>(
            kvStorage_, nameGenerator_, 0);
        txManager_ = std::make_shared<TxManager>(dentryStorage_);
        dentryManager_ = std::make_shared<DentryManager>(
            dentryStorage_, txManager_);
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

    Dentry GenDentry(uint32_t fsId,
                     uint64_t parentId,
                     const std::string& name,
                     uint64_t txId,
                     uint64_t inodeId,
                     bool deleteMarkFlag) {
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
};

TEST_F(DentryManagerTest, CreateDentry) {
    // CASE 1: CreateDentry: success
    auto dentry = GenDentry(1, 0, "A", 0, 1, false);
    ASSERT_EQ(dentryManager_->CreateDentry(dentry), MetaStatusCode::OK);
    ASSERT_EQ(dentryStorage_->Size(), 1);

    // CASE 2: CreateDentry: dentry exist
    auto dentry2 = GenDentry(1, 0, "A", 0, 2, false);
    ASSERT_EQ(dentryManager_->CreateDentry(dentry2),
              MetaStatusCode::DENTRY_EXIST);
    ASSERT_EQ(dentryStorage_->Size(), 1);
}

TEST_F(DentryManagerTest, DeleteDentry) {
    // CASE 1: DeleteDentry: not found
    auto dentry = GenDentry(1, 0, "A", 0, 1, false);
    ASSERT_EQ(dentryManager_->DeleteDentry(dentry), MetaStatusCode::NOT_FOUND);

    // CASE 2: DeleteDentry: sucess
    ASSERT_EQ(dentryManager_->CreateDentry(dentry), MetaStatusCode::OK);
    ASSERT_EQ(dentryStorage_->Size(), 1);
    ASSERT_EQ(dentryManager_->DeleteDentry(dentry), MetaStatusCode::OK);
    ASSERT_EQ(dentryStorage_->Size(), 0);
}

TEST_F(DentryManagerTest, ClearDentry) {
    auto dentry = GenDentry(1, 0, "A", 0, 1, false);
    ASSERT_EQ(dentryManager_->CreateDentry(dentry), MetaStatusCode::OK);
    ASSERT_EQ(dentryStorage_->Size(), 1);
    dentryManager_->ClearDentry();
    ASSERT_EQ(dentryStorage_->Size(), 0);
}

TEST_F(DentryManagerTest, GetDentry) {
    // CASE 1: GetDentry: not found
    auto dentry = GenDentry(1, 0, "A", 0, 1, false);
    ASSERT_EQ(dentryManager_->GetDentry(&dentry), MetaStatusCode::NOT_FOUND);

    // CASE 2: GetDentry: success
    ASSERT_EQ(dentryManager_->CreateDentry(dentry), MetaStatusCode::OK);
    ASSERT_EQ(dentryStorage_->Size(), 1);
    dentry = GenDentry(1, 0, "A", 0, 0, false);
    ASSERT_EQ(dentryManager_->GetDentry(&dentry), MetaStatusCode::OK);
    ASSERT_EQ(dentry.inodeid(), 1);
}

TEST_F(DentryManagerTest, ListDentry) {
    auto dentry1 = GenDentry(1, 0, "A", 0, 1, false);
    auto dentry2 = GenDentry(1, 0, "B", 0, 2, false);
    ASSERT_EQ(dentryManager_->CreateDentry(dentry1), MetaStatusCode::OK);
    ASSERT_EQ(dentryManager_->CreateDentry(dentry2), MetaStatusCode::OK);
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
    auto rc = txManager_->HandleRenameTx(dentrys);
    ASSERT_EQ(rc, MetaStatusCode::PARAM_ERROR);

    // CASE 2: HandleRenameTx success
    dentrys = std::vector<Dentry> {
        // { fsId, parentId, name, txId, inodeId, deleteMarkFlag }
        GenDentry(1, 0, "A", 1, 1, false),
    };
    rc = txManager_->HandleRenameTx(dentrys);
    ASSERT_EQ(rc, MetaStatusCode::OK);
    ASSERT_EQ(dentryStorage_->Size(), 1);
}

}  // namespace metaserver
}  // namespace curvefs
