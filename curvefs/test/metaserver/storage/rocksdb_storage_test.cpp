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
 * Date: 2022-02-28
 * Author: Jingli Chen (Wine93)
 */

#include <gtest/gtest.h>

#include <memory>

#include "curvefs/src/metaserver/storage/storage.h"
#include "curvefs/src/metaserver/storage/rocksdb_storage.h"
#include "curvefs/test/metaserver/storage/storage_test.h"

namespace curvefs {
namespace metaserver {
namespace storage {

using ::curvefs::metaserver::storage::KVStorage;
using ::curvefs::metaserver::storage::RocksDBStorage;
using ::curvefs::metaserver::storage::StorageOptions;

class RocksDBStorageTest : public testing::Test {
 protected:
    RocksDBStorageTest()
    : dirname_(".db"),
      dbpath_(".db/rocksdb.db") {}

    void SetUp() override {
        std::string ret;
        ASSERT_TRUE(ExecShell("mkdir -p " + dirname_, &ret));

        options_.DataDir = dbpath_;
        kvStorage_ = std::make_shared<RocksDBStorage>(options_);
        ASSERT_TRUE(kvStorage_->Open());
    }

    void TearDown() override {
        std::string ret;
        ASSERT_TRUE(kvStorage_->Close());
        ASSERT_TRUE(ExecShell("rm -rf " + dirname_, &ret));
    }

    bool ExecShell(const std::string& cmd, std::string* ret) {
        std::array<char, 128> buffer;
        std::unique_ptr<FILE, decltype(&pclose)>
            pipe(popen(cmd.c_str(), "r"), pclose);
        if (!pipe) {
            return false;
        }
        while (fgets(buffer.data(), buffer.size(), pipe.get()) != nullptr) {
            *ret += buffer.data();
        }
        return true;
    }

 protected:
    std::string dirname_;
    std::string dbpath_;
    StorageOptions options_;
    std::shared_ptr<KVStorage> kvStorage_;
};

TEST_F(RocksDBStorageTest, TypeTest) {
    ASSERT_EQ(kvStorage_->Type(), STORAGE_TYPE::ROCKSDB_STORAGE);
}

TEST_F(RocksDBStorageTest, HGetTest) { TestHGet(kvStorage_); }
TEST_F(RocksDBStorageTest, HSetTest) { TestHSet(kvStorage_); }
TEST_F(RocksDBStorageTest, HDelTest) { TestHDel(kvStorage_); }
TEST_F(RocksDBStorageTest, HGetAllTest) { TestHGetAll(kvStorage_); }
TEST_F(RocksDBStorageTest, HSizeTest) { TestHSize(kvStorage_); }
TEST_F(RocksDBStorageTest, HClearTest) { TestHClear(kvStorage_); }

TEST_F(RocksDBStorageTest, SGetTest) { TestSGet(kvStorage_); }
TEST_F(RocksDBStorageTest, SSetTest) { TestSSet(kvStorage_); }
TEST_F(RocksDBStorageTest, SDelTest) { TestSDel(kvStorage_); }
TEST_F(RocksDBStorageTest, SSeekTest) { TestSSeek(kvStorage_); }
TEST_F(RocksDBStorageTest, SGetAllTest) { TestSGetAll(kvStorage_); }
TEST_F(RocksDBStorageTest, SSizeTest) { TestSSize(kvStorage_); }
TEST_F(RocksDBStorageTest, SClearTest) { TestSClear(kvStorage_); }
TEST_F(RocksDBStorageTest, SMixOperator) { TestMixOperator(kvStorage_); }

}  // namespace storage
}  // namespace metaserver
}  // namespace curvefs
