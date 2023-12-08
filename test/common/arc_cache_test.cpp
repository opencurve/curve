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
 * Created Date: 20231208
 * Author: ZhelongZhao
 */
#include "src/common/arc_cache.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <cstring>
#include <string>

namespace curve {
namespace common {
TEST(TestARCCache, test_all) {
    const int maxCount = 6;
    auto cache = std::make_shared<ARCCache<std::string, std::string>>(
        maxCount, std::make_shared<CacheMetrics>("ARCCache"));

    for (int i = 0; i < maxCount; i++) {
        cache->Put(std::to_string(i), std::to_string(i));
    }
    std::string k, v;
    // now cache
    // t2:             b2:
    // t1: 5 4 3 2 1 0 b1:
    ASSERT_EQ(cache->Size(), maxCount);
    ASSERT_EQ(cache->GetLast(&k, &v), true);
    ASSERT_EQ(k, "0");
    ASSERT_EQ(v, "0");

    for (int i = 0; i < maxCount / 2; i++) {
        cache->Put(std::to_string(maxCount + i), std::to_string(maxCount + i));
    }
    // now cache
    // t2:             b2:
    // t1: 8 7 6 5 4 3 b1:
    ASSERT_EQ(cache->Size(), maxCount);
    ASSERT_EQ(cache->GetLast(&k, &v), true);
    ASSERT_EQ(k, "3");
    ASSERT_EQ(v, "3");

    for (int i = maxCount / 2; i < maxCount; i++) {
        cache->Get(std::to_string(i), &v);
    }
    // now cache
    // t2: 5 4 3 b2:
    // t1: 8 7 6 b1:
    ASSERT_EQ(cache->Size(), maxCount);
    ASSERT_EQ(cache->GetLast(&k, &v), true);
    ASSERT_EQ(k, "6");
    ASSERT_EQ(v, "6");

    for (int i = 0; i < maxCount / 2; i++) {
        cache->Put(std::to_string(i), std::to_string(i));
    }
    // now cache
    // t2: 5 4 3 b2:
    // t1: 2 1 0 b1: 8 7 6
    ASSERT_EQ(cache->Size(), maxCount);
    ASSERT_EQ(cache->GetLast(&k, &v), true);
    ASSERT_EQ(k, "0");
    ASSERT_EQ(v, "0");

    for (int i = 0; i < maxCount / 2; i++) {
        ASSERT_EQ(cache->Get(std::to_string(maxCount + i), &v), false);
        cache->Put(std::to_string(maxCount + i), std::to_string(maxCount + i));
    }
    // now cache
    // t2: 8 7 6 5 b2: 4 3
    // t1: 2 1     b1: 0
    ASSERT_EQ(cache->Size(), maxCount);
    ASSERT_EQ(cache->GetLast(&k, &v), true);
    ASSERT_EQ(k, "1");
    ASSERT_EQ(v, "1");

    ASSERT_EQ(cache->Get(std::to_string(0), &v), false);
    cache->Put(std::to_string(0), std::to_string(0));
    // now cache
    // t2: 0 8 7 6 b2: 5 4 3
    // t1: 2 1     b1:
    ASSERT_EQ(cache->Size(), maxCount);
    ASSERT_EQ(cache->GetLast(&k, &v), true);
    ASSERT_EQ(k, "1");
    ASSERT_EQ(v, "1");

    for (int i = maxCount / 2; i < maxCount; i++) {
        ASSERT_EQ(cache->Get(std::to_string(i), &v), false);
        cache->Put(std::to_string(i), std::to_string(i));
    }
    // now cache
    // t2: 5 4 3 0 8 b2: 7 6
    // t1: 2         b1: 1
    ASSERT_EQ(cache->Size(), maxCount);
    ASSERT_EQ(cache->GetLast(&k, &v), true);
    ASSERT_EQ(k, "2");
    ASSERT_EQ(v, "2");

    // remove
    {
        ASSERT_EQ(cache->Get(std::to_string(2), &v), true);
        cache->Remove(std::to_string(2));
        ASSERT_EQ(cache->Get(std::to_string(2), &v), false);

        ASSERT_EQ(cache->Get(std::to_string(8), &v), true);
        cache->Remove(std::to_string(8));
        ASSERT_EQ(cache->Get(std::to_string(8), &v), false);

        ASSERT_EQ(cache->Get(std::to_string(0), &v), true);
        cache->Remove(std::to_string(0));
        ASSERT_EQ(cache->Get(std::to_string(0), &v), false);

        ASSERT_EQ(cache->Get(std::to_string(3), &v), true);
        cache->Remove(std::to_string(3));
        ASSERT_EQ(cache->Get(std::to_string(3), &v), false);

        ASSERT_EQ(cache->Get(std::to_string(4), &v), true);
        cache->Remove(std::to_string(4));
        ASSERT_EQ(cache->Get(std::to_string(4), &v), false);

        ASSERT_EQ(cache->Get(std::to_string(5), &v), true);
        cache->Remove(std::to_string(5));
        ASSERT_EQ(cache->Get(std::to_string(5), &v), false);

        ASSERT_EQ(cache->Size(), 0);
    }
}

}  // namespace common
}  // namespace curve
