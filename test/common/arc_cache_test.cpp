/*
 *  Copyright (c) 2020 NetEase Inc.
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
 * Project: curve
 * Created Date: 20211010
 * Author: xuchaojie,lixiaocui
 */

#include <gtest/gtest.h>
#include <glog/logging.h>
#include <cstdint>

#include "src/common/lru_cache.h"

namespace curve {
namespace common {

static void
assert_cache_metrics(std::shared_ptr<ARCCache<int, int>> cache) {
    auto metrics = cache->GetCacheMetrics();
    auto arcSize = cache->ArcSize();
    /* sizeof(key) + sizeof(value), yet sizeof(int) + sizeof(value) */
    auto sizeofKey = sizeof(int);
    auto sizeofValue = sizeof(int);
    ASSERT_EQ(arcSize.BSize() * sizeofKey +
            arcSize.TSize() * (sizeofKey + sizeofValue),
            metrics->cacheBytes.get_value());
}

TEST(ArcTest, test_cache_create) {
    int maxCount = 5;
    auto cache = std::make_shared<ARCCache<int, int>>(maxCount,
        std::make_shared<CacheMetrics>("Cache"));

    ASSERT_EQ(cache->Capacity(), 5);
    ASSERT_EQ(cache->Size(), 0);
}

TEST(ArcTest, test_cache_put) {
    int maxCount = 5;
    auto cache = std::make_shared<ARCCache<int, int>>(maxCount,
        std::make_shared<CacheMetrics>("Cache"));
    auto metrics = cache->GetCacheMetrics();

    for (int i = 0; i < maxCount; ++i) {
        cache->Put(i, i);
    }

    ASSERT_TRUE(cache->Size() == maxCount);

    int v;
    for (int i = 0; i < maxCount; ++i) {
        ASSERT_TRUE(cache->Get(i, &v));
        ASSERT_EQ(v, i);
    }

    assert_cache_metrics(cache);
}

TEST(ArcTest, test_cache_retire) {
    int maxCount = 5;
    auto cache = std::make_shared<ARCCache<int, int>>(maxCount,
        std::make_shared<CacheMetrics>("Cache"));
    auto metrics = cache->GetCacheMetrics();

    for (int i = 0; i < maxCount+1; ++i) {
        cache->Put(i, i);
    }

    ASSERT_TRUE(cache->Size() == maxCount);

    int v;
    ASSERT_TRUE(cache->Get(0, &v) == false);
    for (int i = 1; i < maxCount+1; ++i) {
        ASSERT_TRUE(cache->Get(i, &v));
        ASSERT_EQ(v, i);
    }

    for (int i = 100; i < 200; ++i) {
        cache->Put(i, i);
    }

    auto s = cache->ArcSize();
    ASSERT_TRUE(s.BSize() + s.TSize() <= 2 * maxCount);

    assert_cache_metrics(cache);
}

TEST(ArcTest, test_cache_remove) {
    int maxCount = 5;
    auto cache = std::make_shared<ARCCache<int, int>>(maxCount,
        std::make_shared<CacheMetrics>("Cache"));
    auto metrics = cache->GetCacheMetrics();

    for (int i = 0; i < maxCount; ++i) {
        cache->Put(i, i);
    }

    cache->Remove(0);
    int v;
    ASSERT_FALSE(cache->Get(0, &v));
    ASSERT_TRUE(cache->Size() == maxCount-1);

    for (int i = 1; i < maxCount; ++i) {
        ASSERT_TRUE(cache->Get(i, &v));
        ASSERT_EQ(v, i);
    }

    for (int i = 100; i < 200; ++i) {
        cache->Put(i, i);
    }

    auto s = cache->ArcSize();
    ASSERT_TRUE(s.BSize() + s.TSize() <= 2 * maxCount);
    assert_cache_metrics(cache);
}

}  // namespace common
}  // namespace curve

