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
 * Created Date: 20191217
 * Author: lixiaocui
 */

#include <gtest/gtest.h>
#include "src/mds/nameserver2/nameserverMetrics.h"

namespace curve {
namespace mds {
TEST(TestNameserverCacheMetrics, testall) {
    NameserverCacheMetrics cacheMetrics;

    // 1. 新增数据项
    cacheMetrics.UpdateAddToCacheCount();
    ASSERT_EQ(1, cacheMetrics.cacheCount.get_value());

    cacheMetrics.UpdateAddToCacheBytes(1000);
    ASSERT_EQ(1000, cacheMetrics.cacheBytes.get_value());

    // 2. 移除数据项
    cacheMetrics.UpdateRemoveFromCacheCount();
    ASSERT_EQ(0, cacheMetrics.cacheCount.get_value());

    cacheMetrics.UpdateRemoveFromCacheBytes(200);
    ASSERT_EQ(800, cacheMetrics.cacheBytes.get_value());

    // 3. cache命中
    ASSERT_EQ(0, cacheMetrics.cacheHit.get_value());
    cacheMetrics.OnCacheHit();
    ASSERT_EQ(1, cacheMetrics.cacheHit.get_value());

    // 4. cache未命中
    ASSERT_EQ(0, cacheMetrics.cacheMiss.get_value());
    cacheMetrics.OnCacheMiss();
    ASSERT_EQ(1, cacheMetrics.cacheMiss.get_value());
}
}  // namespace mds
}  // namespace curve
