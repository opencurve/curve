/*
 * Project: curve
 * Created Date: Thur Apr 16th 2019
 * Author: lixiaocui
 * Copyright (c) 2018 netease
 */

#include <gtest/gtest.h>
#include <glog/logging.h>
#include <memory>
#include <string>
#include "src/mds/nameserver2/namespace_storage_cache.h"

namespace curve {
namespace mds {
TEST(CaCheTest, test_cache_with_capacity_limit) {
    int maxCount = 5;
    std::shared_ptr<LRUCache> cache = std::make_shared<LRUCache>(maxCount);

    // 1. 测试 put/get
    for (int i = 1; i <= maxCount + 1; i++) {
        cache->Put(std::to_string(i), std::to_string(i));
        std::string res;
        ASSERT_TRUE(cache->Get(std::to_string(i), &res));
        ASSERT_EQ(std::to_string(i), res);
    }

    // 2. 第一个元素被剔出
    std::string res;
    ASSERT_FALSE(cache->Get(std::to_string(1), &res));
    for (int i = 2; i <= maxCount + 1; i++) {
        ASSERT_TRUE(cache->Get(std::to_string(i), &res));
        ASSERT_EQ(std::to_string(i), res);
    }

    // 3. 测试删除元素
    cache->Remove("1");
    cache->Remove("2");
    ASSERT_FALSE(cache->Get("2", &res));

    // 4. 重复put
    cache->Put("4", "hello");
    ASSERT_TRUE(cache->Get("4", &res));
    ASSERT_EQ("hello", res);
}

TEST(CaCheTest, test_cache_with_capacity_no_limit) {
    std::shared_ptr<LRUCache> cache = std::make_shared<LRUCache>();

    // 1. 测试 put/get
    std::string res;
    for (int i = 1; i <= 10; i++) {
        cache->Put(std::to_string(i), std::to_string(i));
        ASSERT_TRUE(cache->Get(std::to_string(i), &res));
        ASSERT_EQ(std::to_string(i), res);
    }

    // 2. 测试元素删除
    cache->Remove("1");
    ASSERT_FALSE(cache->Get("1", &res));
}
}  // namespace mds
}  // namespace curve


