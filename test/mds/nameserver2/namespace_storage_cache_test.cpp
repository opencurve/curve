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
#include "src/mds/nameserver2/namespace_storage.h"
#include "src/mds/nameserver2/helper/namespace_helper.h"
#include "src/common/timeutility.h"

namespace curve {
namespace mds {
TEST(CaCheTest, test_cache_with_capacity_limit) {
    int maxCount = 5;
    std::shared_ptr<LRUCache> cache = std::make_shared<LRUCache>(maxCount);

    // 1. 测试 put/get
    uint64_t cacheSize = 0;
    for (int i = 1; i <= maxCount + 1; i++) {
        cache->Put(std::to_string(i), std::to_string(i));
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

    // 2. 第一个元素被剔出
    std::string res;
    ASSERT_FALSE(cache->Get(std::to_string(1), &res));
    for (int i = 2; i <= maxCount + 1; i++) {
        ASSERT_TRUE(cache->Get(std::to_string(i), &res));
        ASSERT_EQ(std::to_string(i), res);
    }

    // 3. 测试删除元素
    // 删除不存在的元素
    cache->Remove("1");
    // 删除list中存在的元素
    cache->Remove("2");
    ASSERT_FALSE(cache->Get("2", &res));
    cacheSize -= std::to_string(2).size() * 2;
    ASSERT_EQ(maxCount - 1, cache->GetCacheMetrics()->cacheCount.get_value());
    ASSERT_EQ(cacheSize, cache->GetCacheMetrics()->cacheBytes.get_value());

    // 4. 重复put
    cache->Put("4", "hello");
    ASSERT_TRUE(cache->Get("4", &res));
    ASSERT_EQ("hello", res);
    ASSERT_EQ(maxCount - 1, cache->GetCacheMetrics()->cacheCount.get_value());
    cacheSize -= std::to_string(4).size() * 2;
    cacheSize += std::to_string(4).size() + std::string("hello").size();
    ASSERT_EQ(cacheSize, cache->GetCacheMetrics()->cacheBytes.get_value());
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
TEST(CaCheTest, test_cache_with_large_data_capacity_no_limit) {
    uint64_t DefaultChunkSize = 16 * kMB;
    std::shared_ptr<LRUCache> cache = std::make_shared<LRUCache>();

    int i = 1;
    FileInfo fileinfo;
    std::string filename = "helloword-" + std::to_string(i) + ".log";
    fileinfo.set_id(i);
    fileinfo.set_filename(filename);
    fileinfo.set_parentid(i << 8);
    fileinfo.set_filetype(FileType::INODE_PAGEFILE);
    fileinfo.set_chunksize(DefaultChunkSize);
    fileinfo.set_length(10 << 20);
    fileinfo.set_ctime(::curve::common::TimeUtility::GetTimeofDayUs());
    fileinfo.set_seqnum(1);
    std::string encodeFileInfo;
    ASSERT_TRUE(fileinfo.SerializeToString(&encodeFileInfo));
    std::string encodeKey =
            NameSpaceStorageCodec::EncodeFileStoreKey(i << 8, filename);

    // 1. put/get
    cache->Put(encodeKey, encodeFileInfo);
    std::string out;
    ASSERT_TRUE(cache->Get(encodeKey, &out));
    FileInfo fileinfoout;
    ASSERT_TRUE(NameSpaceStorageCodec::DecodeFileInfo(out, &fileinfoout));
    NameSpaceStorageCodec::DecodeFileInfo(out, &fileinfoout);
    ASSERT_EQ(filename, fileinfoout.filename());

    // 2. remove
    cache->Remove(encodeKey);
    ASSERT_FALSE(cache->Get(encodeKey, &out));
}

}  // namespace mds
}  // namespace curve


