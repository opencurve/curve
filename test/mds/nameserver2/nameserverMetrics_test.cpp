/*
 * Project: curve
 * Created Date: 20191217
 * Author: lixiaocui
 * Copyright (c) 2019 netease
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
}
}  // namespace mds
}  // namespace curve
