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
 * Created Date: 20210127
 * Author: wuhanqing
 */

#include "src/common/leaky_bucket.h"

#include <gtest/gtest.h>

#include <chrono>
#include <thread>

namespace curve {
namespace common {

TEST(LeakyBucketTest, TestCommon) {
    uint64_t limit = 1000;
    uint64_t burst = 0;
    uint64_t burstLength = 0;

    LeakyBucket throttle;
    throttle.SetLimit(limit, burst, burstLength);

    throttle.Add(1);
    uint64_t count = 0;
    auto fn = [&](uint64_t testSeconds) {
        const auto endTime = std::chrono::high_resolution_clock::now() +
                             std::chrono::milliseconds(testSeconds * 1000);
        while (std::chrono::high_resolution_clock::now() < endTime) {
            throttle.Add(1);
            ++count;
        }
    };

    std::thread th(fn, 10);
    th.join();

    uint64_t expected = 10000;
    ASSERT_GE(count, expected * 0.9);
    ASSERT_LE(count, expected * 1.1);
}

TEST(LeakyBucketTest, TestExceedLimit) {
    uint64_t limit = 1000;
    uint64_t burst = 0;
    uint64_t burstLength = 0;

    LeakyBucket throttle;
    throttle.SetLimit(limit, burst, burstLength);

    auto start = std::chrono::high_resolution_clock::now();
    throttle.Add(10 * limit);
    auto end = std::chrono::high_resolution_clock::now();

    auto seconds =
        std::chrono::duration_cast<std::chrono::seconds>(end - start).count();

    ASSERT_GE(seconds, 8);
    ASSERT_LE(seconds, 12);
}

TEST(LeakyBucketTest, TestBurst) {
    uint64_t limit = 1000;
    uint64_t burst = 2000;
    uint64_t burstLength = 5;

    LeakyBucket throttle;
    throttle.SetLimit(limit, burst, burstLength);

    throttle.Add(1);
    uint64_t count = 0;
    auto fn = [&](uint64_t testSeconds) {
        const auto endTime = std::chrono::high_resolution_clock::now() +
                             std::chrono::milliseconds(testSeconds * 1000);
        while (std::chrono::high_resolution_clock::now() < endTime) {
            throttle.Add(1);
            ++count;
        }
    };

    std::thread th(fn, 20);
    th.join();

    // in the first 10 seconds, leak 2000 tokens per second
    // in the second 10 seconds, leak 1000 tokens per second
    uint64_t expected = 30000;
    ASSERT_GE(count, expected * 0.9);
    ASSERT_LE(count, expected * 1.1);
}

TEST(LeakyBucketTest, TestDynamicIncreaseLimit) {
    uint64_t limit = 1000;
    uint64_t newLimit = limit * 2;
    uint64_t burst = 0;
    uint64_t burstLength = 0;

    LeakyBucket throttle;
    throttle.SetLimit(limit, burst, burstLength);

    throttle.Add(1);

    uint64_t count = 0;
    auto fn = [&](uint64_t testSeconds) {
        const auto endTime = std::chrono::high_resolution_clock::now() +
                             std::chrono::milliseconds(testSeconds * 1000);
        while (std::chrono::high_resolution_clock::now() < endTime) {
            throttle.Add(1);
            ++count;
        }
    };

    std::thread th(fn, 10);

    std::this_thread::sleep_for(std::chrono::seconds(5));
    throttle.SetLimit(newLimit, burst, burstLength);

    th.join();

    uint64_t expected = 15000;
    ASSERT_GE(count, expected * 0.9);
    ASSERT_LE(count, expected * 1.2);
}

TEST(LeakyBucketTest, TestDynamicDecreaseLimit) {
    uint64_t limit = 1000;
    uint64_t newLimit = limit / 2;
    uint64_t burst = 0;
    uint64_t burstLength = 0;

    LeakyBucket throttle;
    throttle.SetLimit(limit, burst, burstLength);

    throttle.Add(1);

    uint64_t count = 0;
    auto fn = [&](uint64_t testSeconds) {
        const auto endTime = std::chrono::high_resolution_clock::now() +
                             std::chrono::milliseconds(testSeconds * 1000);
        while (std::chrono::high_resolution_clock::now() < endTime) {
            throttle.Add(1);
            ++count;
        }
    };

    std::thread th(fn, 10);

    std::this_thread::sleep_for(std::chrono::seconds(5));
    throttle.SetLimit(newLimit, burst, burstLength);

    th.join();

    uint64_t expected = 7500;
    ASSERT_GE(count, expected * 0.9);
    ASSERT_LE(count, expected * 1.1);
}

TEST(LeakyBucketTest, TestSetLimit) {
    LeakyBucket throttle;

    ASSERT_TRUE(throttle.SetLimit(100, 0, 0));
    ASSERT_TRUE(throttle.SetLimit(100, 200, 2));
    ASSERT_FALSE(throttle.SetLimit(100, 50, 10));
    ASSERT_FALSE(throttle.SetLimit(100, 50, 0));
    ASSERT_FALSE(throttle.SetLimit(100, 0, 10));
}

}  // namespace common
}  // namespace curve
