/*
 *  Copyright (c) 2021 NetEase Inc.
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
 * Date: Friday Oct 22 15:09:30 CST 2021
 * Author: wuhanqing
 */

#include <gtest/gtest.h>

#include "curvefs/src/client/s3/client_s3_cache_manager.h"
#include "curvefs/test/client/mock_chunk_cache_manager.h"

namespace curvefs {
namespace client {

using ::testing::_;

TEST(FsCacheManagerTest, test_read_lru_cache_size) {
    uint64_t smallDataCacheByte = 128ull * 1024;      // 128KiB
    uint64_t dataCacheByte = 4ull * 1024 * 1024;      // 4MiB
    uint64_t maxReadCacheByte = 16ull * 1024 * 1024;  // 16MiB
    uint64_t maxWriteCacheByte = maxReadCacheByte;

    FsCacheManager manager(nullptr, maxReadCacheByte, maxWriteCacheByte);
    MockChunkCacheManager mockCacheMgr;

    {
        EXPECT_CALL(mockCacheMgr, ReleaseReadDataCache(_))
            .Times(0);

        for (size_t i = 0; i < maxReadCacheByte / smallDataCacheByte; ++i) {
            manager.Set(std::make_shared<DataCache>(nullptr, &mockCacheMgr, 0,
                                                    smallDataCacheByte));
        }
    }

    {
        EXPECT_CALL(mockCacheMgr, ReleaseReadDataCache(_))
            .Times(1);
        manager.Set(std::make_shared<DataCache>(nullptr, &mockCacheMgr, 0,
                                                dataCacheByte));
    }

    {
        EXPECT_CALL(mockCacheMgr, ReleaseReadDataCache(_))
            .Times(32);
        manager.Set(std::make_shared<DataCache>(nullptr, &mockCacheMgr, 0,
                                                dataCacheByte));
    }
}

}  // namespace client
}  // namespace curvefs
