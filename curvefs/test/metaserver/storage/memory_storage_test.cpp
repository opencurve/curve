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

using ::curvefs::metaserver::storage::KVStorage;
using ::curvefs::metaserver::storage::MemoryStorage;
using ::curvefs::metaserver::storage::StorageOptions;
using STORAGE_TYPE = ::curvefs::metaserver::storage::KVStorage::STORAGE_TYPE;

class MemoryStorageTest : public testing::Test {
 protected:
    void SetUp() override {
        options_.dataDir = "/tmp";
        options_.compression = false;

        kvStorage_ = std::make_shared<MemoryStorage>(options_);
        ASSERT_TRUE(kvStorage_->Open());

        options_.compression = true;
        kvStorage2_ = std::make_shared<MemoryStorage>(options_);
        ASSERT_TRUE(kvStorage_->Open());
    }

    void TearDown() override {}

 protected:
    StorageOptions options_;
    std::shared_ptr<KVStorage> kvStorage_;
    std::shared_ptr<KVStorage> kvStorage2_;
};

TEST_F(MemoryStorageTest, HGetTest) { TestHGet(kvStorage_);
                                      TestHGet(kvStorage2_);}
TEST_F(MemoryStorageTest, HSetTest) { TestHSet(kvStorage_);
                                      TestHSet(kvStorage2_); }
TEST_F(MemoryStorageTest, HDelTest) { TestHDel(kvStorage_);
                                      TestHDel(kvStorage2_); }
TEST_F(MemoryStorageTest, HGetAllTest) { TestHGetAll(kvStorage_);
                                         TestHGetAll(kvStorage2_); }
TEST_F(MemoryStorageTest, HSizeTest) { TestHSize(kvStorage_);
                                       TestHSize(kvStorage2_); }
TEST_F(MemoryStorageTest, HClearTest) { TestHClear(kvStorage_);
                                        TestHClear(kvStorage2_); }

TEST_F(MemoryStorageTest, SGetTest) { TestSGet(kvStorage_);
                                      TestSGet(kvStorage2_); }
TEST_F(MemoryStorageTest, SSetTest) { TestSSet(kvStorage_);
                                      TestSSet(kvStorage2_); }
TEST_F(MemoryStorageTest, SDelTest) { TestSDel(kvStorage_);
                                      TestSDel(kvStorage2_); }
TEST_F(MemoryStorageTest, SSeekTest) { TestSSeek(kvStorage_);
                                       TestSSeek(kvStorage2_); }
TEST_F(MemoryStorageTest, SGetAllTest) { TestSGetAll(kvStorage_);
                                         TestSGetAll(kvStorage2_); }
TEST_F(MemoryStorageTest, SSizeTest) { TestSSize(kvStorage_);
                                       TestSSize(kvStorage2_); }
TEST_F(MemoryStorageTest, SClearTest) { TestSClear(kvStorage_);
                                        TestSClear(kvStorage2_); }
TEST_F(MemoryStorageTest, MixOperatorTest) { TestMixOperator(kvStorage_);
                                             TestMixOperator(kvStorage2_); }

}  // namespace storage
}  // namespace metaserver
}  // namespace curvefs
