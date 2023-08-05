/*
 *  Copyright (c) 2023 NetEase Inc.
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
 * Created Date: 2023-04-03
 * Author: Jingli Chen (Wine93)
 */

#include <gtest/gtest.h>

#include "curvefs/src/client/filesystem/lookup_cache.h"

namespace curvefs {
namespace client {
namespace filesystem {

class LookupCacheTest : public ::testing::Test {
 protected:
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(LookupCacheTest, Basic) {
    auto option = LookupCacheOption{ lruSize: 10, negativeTimeoutSec: 1 };
    auto cache = std::make_shared<LookupCache>(option);

    ASSERT_FALSE(cache->Get(1, "f1"));

    cache->Put(1, "f1");
    ASSERT_TRUE(cache->Get(1, "f1"));
}

TEST_F(LookupCacheTest, Enable) {
    // CASE 1: cache off, negativeTimeoutSec = 0.
    auto option = LookupCacheOption{ lruSize: 10, negativeTimeoutSec: 0 };
    auto cache = std::make_shared<LookupCache>(option);
    cache->Put(1, "f1");
    ASSERT_FALSE(cache->Get(1, "f1"));

    // CASE 2: cache on, negativeTimeoutSec = 1.
    option = LookupCacheOption{ lruSize: 10, negativeTimeoutSec: 1 };
    cache = std::make_shared<LookupCache>(option);
    cache->Put(1, "f1");
    ASSERT_TRUE(cache->Get(1, "f1"));
}

TEST_F(LookupCacheTest, Timeout) {
    auto option = LookupCacheOption{ lruSize: 10, negativeTimeoutSec: 1 };
    auto cache = std::make_shared<LookupCache>(option);

    // CASE 1: cache hit.
    cache->Put(1, "f1");
    ASSERT_TRUE(cache->Get(1, "f1"));

    // CASE 2: cache miss due to expiration.
    std::this_thread::sleep_for(std::chrono::seconds(1));
    ASSERT_FALSE(cache->Get(1, "f1"));
}

TEST_F(LookupCacheTest, LRUSize) {
    auto option = LookupCacheOption{ lruSize: 1, negativeTimeoutSec: 1 };
    auto cache = std::make_shared<LookupCache>(option);

    // CASE 1: cache hit.
    cache->Put(1, "f1");
    ASSERT_TRUE(cache->Get(1, "f1"));

    // CASE 2: cache miss due to eviction.
    cache->Put(1, "f2");
    ASSERT_FALSE(cache->Get(1, "f1"));
    ASSERT_TRUE(cache->Get(1, "f2"));
}

}  // namespace filesystem
}  // namespace client
}  // namespace curvefs
