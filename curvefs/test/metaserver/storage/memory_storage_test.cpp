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

#include <memory>

#include "curvefs/test/metserver/storage/storage_test.h"

namespace curvefs {
namespace metaserver {
namespace storage {

class MemoryStorageTest : public testing::Test {
 protected:
    void SetUp() override {
        kvStorage = std::make_shared<MemoryStorage>(options_);
    }

    void TearDown() override {}

 protected:
    Storage::StorageOptions options_;
    std::shared_ptr<MemoryStorage> kvStorage;
};

TEST_F(MemoryStorageTest, TypeTest) {
    ASSERT_EQ(kvStorage_->Type(), STORAGE_TYPE::MEMORY_STORAGE)
}

TEST_F(MemoryStorageTest, HGetTest) { TestHGet(); }
TEST_F(MemoryStorageTest, HSetTest) { TestHSet(); }
TEST_F(MemoryStorageTest, HDelTest) { TestHDel(); }
TEST_F(MemoryStorageTest, HGetAllTest) { TestHGetAll(); }
TEST_F(MemoryStorageTest, HClearTest) { TestHClear(); }

TEST_F(MemoryStorageTest, SGetTest) { TestSGet(); }
TEST_F(MemoryStorageTest, SSetTest) { TestSSet(); }
TEST_F(MemoryStorageTest, SDelTest) { TestSDel(); }
TEST_F(MemoryStorageTest, SRangeTest) { TestSRange(); }
TEST_F(MemoryStorageTest, SGetAllTest) { TestSGetAll(); }
TEST_F(MemoryStorageTest, SClearTest) { TestSClear(); }
TEST_F(MemoryStorageTest, SMixOperator) { TestMixOperator(); }

}  // namespace storage
}  // namespace metaserver
}  // namespace curvefs