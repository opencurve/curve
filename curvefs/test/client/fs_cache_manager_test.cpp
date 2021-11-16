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
#include "src/common/concurrent/count_down_event.h"

namespace curvefs {
namespace client {

using ::testing::_;
using ::testing::Invoke;

TEST(FsCacheManagerTest, test_read_lru_cache_size) {
    uint64_t smallDataCacheByte = 128ull * 1024;      // 128KiB
    uint64_t dataCacheByte = 4ull * 1024 * 1024;      // 4MiB
    uint64_t maxReadCacheByte = 16ull * 1024 * 1024;  // 16MiB
    uint64_t maxWriteCacheByte = maxReadCacheByte;

    FsCacheManager manager(nullptr, maxReadCacheByte, maxWriteCacheByte);
    auto mockCacheMgr = std::make_shared<MockChunkCacheManager>();

    {
        EXPECT_CALL(*mockCacheMgr, ReleaseReadDataCache(_))
            .Times(0);

        for (size_t i = 0; i < maxReadCacheByte / smallDataCacheByte; ++i) {
            manager.Set(std::make_shared<DataCache>(nullptr, mockCacheMgr.get(),
                                                    0, smallDataCacheByte));
        }
    }

    {
        const uint32_t expectCallTimes = 1;
        curve::common::CountDownEvent counter(expectCallTimes);

        EXPECT_CALL(*mockCacheMgr, ReleaseReadDataCache(_))
            .Times(expectCallTimes)
            .WillRepeatedly(Invoke([&counter](uint64_t) { counter.Signal(); }));
        manager.Set(std::make_shared<DataCache>(nullptr, mockCacheMgr.get(), 0,
                                                dataCacheByte));

        counter.Wait();
    }

    {
        const uint32_t expectCallTimes = 32;
        curve::common::CountDownEvent counter(expectCallTimes);

        EXPECT_CALL(*mockCacheMgr, ReleaseReadDataCache(_))
            .Times(expectCallTimes)
            .WillRepeatedly(Invoke([&counter](uint64_t) { counter.Signal(); }));

        manager.Set(std::make_shared<DataCache>(nullptr, mockCacheMgr.get(), 0,
                                                dataCacheByte));

        counter.Wait();
    }
}

}  // namespace client
}  // namespace curvefs
