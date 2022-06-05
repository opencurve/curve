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

#include "curvefs/src/metaserver/storage/utils.h"
#include "curvefs/src/metaserver/storage/storage.h"
#include "curvefs/src/metaserver/storage/rocksdb_storage.h"
#include "curvefs/test/metaserver/storage/storage_test.h"

namespace curvefs {
namespace metaserver {
namespace storage {

using ::curvefs::metaserver::storage::InitStorage;
using ::curvefs::metaserver::storage::GetStorageInstance;
using ::curvefs::metaserver::storage::KVStorage;
using ::curvefs::metaserver::storage::RocksDBStorage;
using ::curvefs::metaserver::storage::StorageOptions;
using ::curvefs::metaserver::storage::StorageStatistics;
using ROCKSDB_STATUS = ROCKSDB_NAMESPACE::Status;

using STORAGE_TYPE = ::curvefs::metaserver::storage::KVStorage::STORAGE_TYPE;

class RocksDBStorageTest : public testing::Test {
 protected:
    RocksDBStorageTest()
    : dirname_(".db"),
      dbpath_(".db/rocksdb.db") {}

    void SetUp() override {
        std::string ret;
        ASSERT_TRUE(ExecShell("mkdir -p " + dirname_, &ret));

        options_.maxMemoryQuotaBytes = 32212254720;
        options_.maxDiskQuotaBytes = 2199023255552;
        options_.dataDir = dbpath_;
        options_.compression = false;
        options_.unorderedWriteBufferSize = 134217728;
        options_.unorderedMaxWriteBufferNumber = 5;
        options_.orderedWriteBufferSize = 134217728;
        options_.orderedMaxWriteBufferNumber = 15;
        options_.blockCacheCapacity = 134217728;
        options_.memtablePrefixBloomSizeRatio = 0.1;

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

    Status ToStorageStatus(ROCKSDB_STATUS status) {
        auto storage = std::make_shared<RocksDBStorage>(options_);
        return storage->ToStorageStatus(status);
    }

 protected:
    std::string dirname_;
    std::string dbpath_;
    StorageOptions options_;
    std::shared_ptr<KVStorage> kvStorage_;
};

TEST_F(RocksDBStorageTest, OpenCloseTest) {
    // CASE 1: open twoce
    ASSERT_TRUE(kvStorage_->Open());
    ASSERT_TRUE(kvStorage_->Open());

    // CASE 2: close twice
    ASSERT_TRUE(kvStorage_->Open());
    ASSERT_TRUE(kvStorage_->Open());

    // CASE 3: operate after close
    Status s;
    size_t size;
    Dentry value;
    std::shared_ptr<Iterator> iterator;

    ASSERT_TRUE(kvStorage_->Close());

    s = kvStorage_->HSet("partition:1", "key1", Value("value1"));
    ASSERT_TRUE(s.IsDBClosed());
    s = kvStorage_->HGet("partition:1", "key1", &value);
    ASSERT_TRUE(s.IsDBClosed());
    s = kvStorage_->HDel("partition:1", "key1");
    ASSERT_TRUE(s.IsDBClosed());
    iterator = kvStorage_->HGetAll("partition:1");
    ASSERT_EQ(iterator->Status(), -1);
    size = kvStorage_->HSize("partition:1");
    ASSERT_EQ(size, 0);
    s = kvStorage_->HClear("partition:1");
    ASSERT_TRUE(s.IsDBClosed());

    s = kvStorage_->SSet("partition:1", "key1", Value("value1"));
    ASSERT_TRUE(s.IsDBClosed());
    s = kvStorage_->SGet("partition:1", "key1", &value);
    ASSERT_TRUE(s.IsDBClosed());
    s = kvStorage_->SDel("partition:1", "key1");
    ASSERT_TRUE(s.IsDBClosed());
    iterator = kvStorage_->SGetAll("partition:1");
    ASSERT_EQ(iterator->Status(), -1);
    iterator = kvStorage_->SSeek("partition:1", "key1");
    ASSERT_EQ(iterator->Status(), -1);
    size = kvStorage_->SSize("partition:1");
    ASSERT_EQ(size, 0);
    s = kvStorage_->SClear("partition:1");
    ASSERT_TRUE(s.IsDBClosed());
}

TEST_F(RocksDBStorageTest, GetStatisticsTest) {
    StorageStatistics statistics;
    ASSERT_TRUE(kvStorage_->GetStatistics(&statistics));
    ASSERT_EQ(statistics.maxMemoryQuotaBytes, options_.maxMemoryQuotaBytes);
    ASSERT_EQ(statistics.maxDiskQuotaBytes, options_.maxDiskQuotaBytes);
    ASSERT_GT(statistics.memoryUsageBytes, 0);
    ASSERT_GT(statistics.diskUsageBytes, 0);
}

TEST_F(RocksDBStorageTest, MiscTest) {
    // CASE 1: storage type
    ASSERT_EQ(kvStorage_->Type(), STORAGE_TYPE::ROCKSDB_STORAGE);

    // CASE 3: init global storage
    options_.type = "rocksdb";
    options_.dataDir = dirname_ + "/global_rocksdb_storage";
    InitStorage(options_);
    ASSERT_EQ(GetStorageInstance()->Type(), STORAGE_TYPE::ROCKSDB_STORAGE);
    ASSERT_TRUE(GetStorageInstance()->Close());

    // CASE 2: status converter
    Status s;
    ASSERT_TRUE(ToStorageStatus(ROCKSDB_STATUS::OK()).ok());
    ASSERT_TRUE(ToStorageStatus(ROCKSDB_STATUS::NotFound()).IsNotFound());
    ASSERT_TRUE(ToStorageStatus(ROCKSDB_STATUS::NotSupported()).
        IsInternalError());
    ASSERT_TRUE(ToStorageStatus(ROCKSDB_STATUS::IOError()).
        IsInternalError());
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
TEST_F(RocksDBStorageTest, MixOperatorTest) { TestMixOperator(kvStorage_); }
TEST_F(RocksDBStorageTest, TransactionTest) { TestTransaction(kvStorage_); }

}  // namespace storage
}  // namespace metaserver
}  // namespace curvefs
