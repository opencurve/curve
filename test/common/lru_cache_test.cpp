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
#include "src/common/timeutility.h"

namespace curve {
namespace common {

TEST(TestCacheMetrics, testall) {
    CacheMetrics cacheMetrics("LRUCache");

    // 1. Add Data Item
    cacheMetrics.UpdateAddToCacheCount();
    ASSERT_EQ(1, cacheMetrics.cacheCount.get_value());

    cacheMetrics.UpdateAddToCacheBytes(1000);
    ASSERT_EQ(1000, cacheMetrics.cacheBytes.get_value());

    // 2. Remove Data Item
    cacheMetrics.UpdateRemoveFromCacheCount();
    ASSERT_EQ(0, cacheMetrics.cacheCount.get_value());

    cacheMetrics.UpdateRemoveFromCacheBytes(200);
    ASSERT_EQ(800, cacheMetrics.cacheBytes.get_value());

    // 3. cache hit
    ASSERT_EQ(0, cacheMetrics.cacheHit.get_value());
    cacheMetrics.OnCacheHit();
    ASSERT_EQ(1, cacheMetrics.cacheHit.get_value());

    // 4. cache Misses
    ASSERT_EQ(0, cacheMetrics.cacheMiss.get_value());
    cacheMetrics.OnCacheMiss();
    ASSERT_EQ(1, cacheMetrics.cacheMiss.get_value());
}

TEST(CaCheTest, test_cache_with_capacity_limit) {
    int maxCount = 5;
    auto cache = std::make_shared<LRUCache<std::string, std::string>>(maxCount,
        std::make_shared<CacheMetrics>("LruCache"));

    // 1. Test put/get
    uint64_t cacheSize = 0;
    for (int i = 1; i <= maxCount + 1; i++) {
        std::string eliminated;
        cache->Put(std::to_string(i), std::to_string(i), &eliminated);
        if (i <= maxCount) {
            cacheSize += std::to_string(i).size() * 2;
            ASSERT_EQ(i, cache->GetCacheMetrics()->cacheCount.get_value());
        } else {
            cacheSize +=
                std::to_string(i).size() * 2 - std::to_string(1).size() * 2;
            ASSERT_EQ(
                cacheSize, cache->GetCacheMetrics()->cacheBytes.get_value());
        }

        std::string res;
        ASSERT_TRUE(cache->Get(std::to_string(i), &res));
        ASSERT_EQ(std::to_string(i), res);
    }

    // 2. The first element is removed
    std::string res;
    ASSERT_FALSE(cache->Get(std::to_string(1), &res));
    for (int i = 2; i <= maxCount + 1; i++) {
        ASSERT_TRUE(cache->Get(std::to_string(i), &res));
        ASSERT_EQ(std::to_string(i), res);
    }

    // 3. Test Delete Element
    // Delete non-existent elements
    cache->Remove("1");
    // Delete elements present in the list
    cache->Remove("2");
    ASSERT_FALSE(cache->Get("2", &res));
    cacheSize -= std::to_string(2).size() * 2;
    ASSERT_EQ(maxCount - 1, cache->GetCacheMetrics()->cacheCount.get_value());
    ASSERT_EQ(cacheSize, cache->GetCacheMetrics()->cacheBytes.get_value());

    // 4. Repeat put
    std::string eliminated;
    cache->Put("4", "hello", &eliminated);
    ASSERT_TRUE(cache->Get("4", &res));
    ASSERT_EQ("hello", res);
    ASSERT_EQ(maxCount - 1, cache->GetCacheMetrics()->cacheCount.get_value());
    cacheSize -= std::to_string(4).size() * 2;
    cacheSize += std::to_string(4).size() + std::string("hello").size();
    ASSERT_EQ(cacheSize, cache->GetCacheMetrics()->cacheBytes.get_value());
}

TEST(CaCheTest, test_cache_with_capacity_no_limit) {
    auto cache = std::make_shared<LRUCache<std::string, std::string>>(
        std::make_shared<CacheMetrics>("LruCache"));

    // 1. Test put/get
    std::string res;
    for (int i = 1; i <= 10; i++) {
        std::string eliminated;
        cache->Put(std::to_string(i), std::to_string(i), &eliminated);
        ASSERT_TRUE(cache->Get(std::to_string(i), &res));
        ASSERT_EQ(std::to_string(i), res);
    }

    // 2. Test element deletion
    cache->Remove("1");
    ASSERT_FALSE(cache->Get("1", &res));
}

TEST(CaCheTest, TestCacheHitAndMissMetric) {
    auto cache = std::make_shared<LRUCache<std::string, std::string>>(
        std::make_shared<CacheMetrics>("LruCache"));
    ASSERT_EQ(0, cache->GetCacheMetrics()->cacheHit.get_value());
    ASSERT_EQ(0, cache->GetCacheMetrics()->cacheMiss.get_value());

    std::string existKey = "hello";
    std::string notExistKey = "world";
    std::string eliminated;
    cache->Put(existKey, existKey, &eliminated);

    std::string out;
    for (int i = 0; i < 10; ++i) {
        ASSERT_TRUE(cache->Get(existKey, &out));
        ASSERT_FALSE(cache->Get(notExistKey, &out));
    }

    ASSERT_EQ(10, cache->GetCacheMetrics()->cacheHit.get_value());
    ASSERT_EQ(10, cache->GetCacheMetrics()->cacheMiss.get_value());
    for (int i = 0; i < 5; ++i) {
        ASSERT_TRUE(cache->Get(existKey, &out));
    }

    ASSERT_EQ(15, cache->GetCacheMetrics()->cacheHit.get_value());
    ASSERT_EQ(10, cache->GetCacheMetrics()->cacheMiss.get_value());
}

TEST(CaCheTest, TestCacheGetLast) {
    auto cache = std::make_shared<LRUCache<int, bool>>(
        std::make_shared<CacheMetrics>("LruCache"));

    for (int i = 0; i < 3; ++i) {
        cache->Put(i, true);
    }
    cache->Put(3, false);
    for (int i = 4; i < 6; ++i) {
        cache->Put(i, true);
    }
    int out;
    cache->GetLast(false, &out);
    ASSERT_EQ(3, out);
    cache->GetLast(true, &out);
    ASSERT_EQ(0, out);
}

TEST(CaCheTest, TestCacheGetLastKV) {
    auto cache = std::make_shared<LRUCache<int, int>>(
        std::make_shared<CacheMetrics>("LruCache"));

    for (int i = 0; i < 3; i++) {
        cache->Put(i, i);
    }

    int k, v;
    cache->GetLast(&k, &v);
    ASSERT_EQ(0, k);
    ASSERT_EQ(0, v);
    cache->Remove(0);
    cache->GetLast(&k, &v);
    ASSERT_EQ(1, k);
    ASSERT_EQ(1, v);
}
bool TestFunction(const int& a) {
    return a > 1;
}
TEST(CaCheTest, TestCacheGetLastKVWithFunction) {
    auto cache = std::make_shared<LRUCache<int, int>>(
        std::make_shared<CacheMetrics>("LruCache"));

    for (int i = 0; i < 3; i++) {
        cache->Put(i, i);
    }

    int k, v;
    cache->GetLast(&k, &v, TestFunction);
    ASSERT_EQ(2, k);
    ASSERT_EQ(2, v);
    cache->Remove(2);
    bool ok = cache->GetLast(&k, &v, TestFunction);
    ASSERT_EQ(false, ok);
}

TEST(SglCaCheTest, TestGetBefore) {
    auto cache = std::make_shared<SglLRUCache<int>>(
        std::make_shared<CacheMetrics>("LruCache"));

    for (int i = 1; i <= 5; ++i) {
        cache->Put(i);
    }
    int keyBefore;
    for (int i = 1; i < 5; ++i) {
        cache->GetBefore(i, &keyBefore);
        ASSERT_EQ(i + 1, keyBefore);
    }
}

TEST(SglCaCheTest, test_cache_with_capacity_limit) {
    int maxCount = 5;
    auto cache = std::make_shared<SglLRUCache<std::string>>(maxCount,
        std::make_shared<CacheMetrics>("LruCache"));

    // 1. Test put/IsCached
    uint64_t cacheSize = 0;
    for (int i = 1; i <= maxCount; i++) {
        cache->Put(std::to_string(i));
        cacheSize++;
        ASSERT_EQ(i, cache->GetCacheMetrics()->cacheCount.get_value());
        ASSERT_TRUE(cache->IsCached(std::to_string(i)));
    }

    // 2. The first element is removed
    cache->Put(std::to_string(11));
    ASSERT_FALSE(cache->IsCached(std::to_string(1)));

    // 3. Test Delete Element
    // Delete non-existent elements
    cache->Remove("1");
    // Delete elements present in the list
    cache->Remove("2");
    ASSERT_FALSE(cache->IsCached("2"));
    ASSERT_EQ(maxCount - 1, cache->GetCacheMetrics()->cacheCount.get_value());

    // 4. Repeat put
    cache->Put("4");
    ASSERT_TRUE(cache->IsCached("4"));
    ASSERT_EQ(maxCount - 1, cache->GetCacheMetrics()->cacheCount.get_value());
}

TEST(SglCaCheTest, test_cache_with_capacity_no_limit) {
    auto cache = std::make_shared<SglLRUCache<std::string>>(
        std::make_shared<CacheMetrics>("LruCache"));

    // 1. Test put/IsCached
    std::string res;
    for (int i = 1; i <= 10; i++) {
        std::string eliminated;
        cache->Put(std::to_string(i));
        ASSERT_TRUE(cache->IsCached(std::to_string(i)));
        ASSERT_FALSE(cache->IsCached(std::to_string(100)));
    }

    // 2. Test element deletion
    cache->Remove("1");
    ASSERT_FALSE(cache->IsCached("1"));
}

TEST(SglCaCheTest, TestCacheHitAndMissMetric) {
    auto cache = std::make_shared<SglLRUCache<std::string>>(
        std::make_shared<CacheMetrics>("LruCache"));
    ASSERT_EQ(0, cache->GetCacheMetrics()->cacheHit.get_value());
    ASSERT_EQ(0, cache->GetCacheMetrics()->cacheMiss.get_value());

    std::string existKey = "hello";
    std::string notExistKey = "world";
    cache->Put(existKey);

    for (int i = 0; i < 10; ++i) {
        ASSERT_TRUE(cache->IsCached(existKey));
        ASSERT_FALSE(cache->IsCached(notExistKey));
    }

    ASSERT_EQ(10, cache->GetCacheMetrics()->cacheHit.get_value());
    ASSERT_EQ(10, cache->GetCacheMetrics()->cacheMiss.get_value());

    for (int i = 0; i < 5; ++i) {
        ASSERT_TRUE(cache->IsCached(existKey));
    }

    ASSERT_EQ(15, cache->GetCacheMetrics()->cacheHit.get_value());
    ASSERT_EQ(10, cache->GetCacheMetrics()->cacheMiss.get_value());
}

TEST(TimedCaCheTest, test_base) {
    int maxCount = 5;
    int timeOutSec = 0;
    auto cache = std::make_shared<TimedLRUCache<std::string, std::string>>(
        timeOutSec, maxCount, std::make_shared<CacheMetrics>("LruCache"));

    for (int i = 1; i <= maxCount + 1; i++) {
        std::string eliminated;
        cache->Put(std::to_string(i), std::to_string(i), &eliminated);
        if (i <= maxCount) {
            ASSERT_EQ(i, cache->GetCacheMetrics()->cacheCount.get_value());
        } else {
            ASSERT_EQ(maxCount,
                cache->GetCacheMetrics()->cacheCount.get_value());
        }
        std::string res;
        ASSERT_TRUE(cache->Get(std::to_string(i), &res));
        ASSERT_EQ(std::to_string(i), res);
    }

    std::string res;
    ASSERT_FALSE(cache->Get(std::to_string(1), &res));
    for (int i = 2; i <= maxCount + 1; i++) {
        ASSERT_TRUE(cache->Get(std::to_string(i), &res));
        ASSERT_EQ(std::to_string(i), res);
    }
}

TEST(TimedCaCheTest, test_timeout) {
    int maxCount = 0;
    int timeOutSec = 1;
    auto cache = std::make_shared<TimedLRUCache<std::string, std::string>>(
        timeOutSec, maxCount, std::make_shared<CacheMetrics>("LruCache"));

    std::string res;
    ASSERT_FALSE(cache->Get("k", &res));
    ASSERT_EQ(1, cache->GetCacheMetrics()->cacheMiss.get_value());
    cache->Put("k", "v");
    ASSERT_EQ(1, cache->Size());
    ASSERT_EQ(1, cache->GetCacheMetrics()->cacheCount.get_value());

    ASSERT_TRUE(cache->Get("k", &res));
    ASSERT_EQ(1, cache->GetCacheMetrics()->cacheHit.get_value());
    sleep(1);
    ASSERT_FALSE(cache->Get("k", &res));
    ASSERT_EQ(2, cache->GetCacheMetrics()->cacheMiss.get_value());
    ASSERT_EQ(0, cache->GetCacheMetrics()->cacheCount.get_value());
    ASSERT_EQ(0, cache->GetCacheMetrics()->cacheBytes.get_value());
    ASSERT_EQ(0, cache->Size());
}

}  // namespace common
}  // namespace curve


