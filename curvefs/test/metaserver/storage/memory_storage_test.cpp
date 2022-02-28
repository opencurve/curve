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
#include "curvefs/src/metaserver/storage/memory_storage.h"
#include "curvefs/test/metaserver/storage/storage_test.h"

namespace curvefs {
namespace metaserver {
namespace storage {

using ::curvefs::metaserver::storage::InitStorage;
using ::curvefs::metaserver::storage::GetStorageInstance;
using ::curvefs::metaserver::storage::KVStorage;
using ::curvefs::metaserver::storage::MemoryStorage;
using ::curvefs::metaserver::storage::StorageOptions;
using ::curvefs::metaserver::storage::StorageStatistics;
using STORAGE_TYPE = ::curvefs::metaserver::storage::KVStorage::STORAGE_TYPE;

class MemoryStorageTest : public testing::Test {
 protected:
    void SetUp() override {
        kvStorage_ = std::make_shared<MemoryStorage>(options_);
        ASSERT_TRUE(kvStorage_->Open());
    }

    void TearDown() override {}

 protected:
    StorageOptions options_;
    std::shared_ptr<KVStorage> kvStorage_;
};

TEST_F(MemoryStorageTest, BasicTest) {
    ASSERT_EQ(kvStorage_->Type(), STORAGE_TYPE::MEMORY_STORAGE);
    ASSERT_TRUE(kvStorage_->Open());
    ASSERT_TRUE(kvStorage_->Close());

    options_.Type = "memory";
    InitStorage(options_);
    ASSERT_EQ(GetStorageInstance()->Type(), STORAGE_TYPE::MEMORY_STORAGE);
}

TEST_F(MemoryStorageTest, GetStatisticsTest) {
    StorageStatistics statistics;
    ASSERT_TRUE(kvStorage_->GetStatistics(&statistics));
    ASSERT_EQ(statistics.MaxMemoryBytes, options_.MaxMemoryBytes);
    ASSERT_EQ(statistics.MaxDiskQuotaBytes, options_.MaxDiskQuotaBytes);
    ASSERT_GT(statistics.MemoryUsageBytes, 0);
    ASSERT_EQ(statistics.DiskUsageBytes, 0);
}

TEST_F(MemoryStorageTest, HGetTest) { TestHGet(kvStorage_); }
TEST_F(MemoryStorageTest, HSetTest) { TestHSet(kvStorage_); }
TEST_F(MemoryStorageTest, HDelTest) { TestHDel(kvStorage_); }
TEST_F(MemoryStorageTest, HGetAllTest) { TestHGetAll(kvStorage_); }
TEST_F(MemoryStorageTest, HSizeTest) { TestHSize(kvStorage_); }
TEST_F(MemoryStorageTest, HClearTest) { TestHClear(kvStorage_); }

TEST_F(MemoryStorageTest, SGetTest) { TestSGet(kvStorage_); }
TEST_F(MemoryStorageTest, SSetTest) { TestSSet(kvStorage_); }
TEST_F(MemoryStorageTest, SDelTest) { TestSDel(kvStorage_); }
TEST_F(MemoryStorageTest, SSeekTest) { TestSSeek(kvStorage_); }
TEST_F(MemoryStorageTest, SGetAllTest) { TestSGetAll(kvStorage_); }
TEST_F(MemoryStorageTest, SSizeTest) { TestSSize(kvStorage_); }
TEST_F(MemoryStorageTest, SClearTest) { TestSClear(kvStorage_); }
TEST_F(MemoryStorageTest, SMixOperator) { TestMixOperator(kvStorage_); }

}  // namespace storage
}  // namespace metaserver
}  // namespace curvefs
