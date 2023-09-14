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
 * Created Date: 18-12-20
 * Author: wudemiao
 */

#include <gtest/gtest.h>

#include "src/common/concurrent/concurrent.h"
#include "src/chunkserver/inflight_throttle.h"

namespace curve {
namespace chunkserver {

using curve::common::Thread;

TEST(InflightThrottleTest, basic) {
    // Basic testing
    {
        uint64_t maxInflight = 1;
        InflightThrottle inflightThrottle(maxInflight);
        ASSERT_FALSE(inflightThrottle.IsOverLoad());
        inflightThrottle.Increment();
        ASSERT_FALSE(inflightThrottle.IsOverLoad());
        inflightThrottle.Increment();
        ASSERT_TRUE(inflightThrottle.IsOverLoad());

        inflightThrottle.Decrement();
        ASSERT_FALSE(inflightThrottle.IsOverLoad());
    }

    // Concurrent addition
    {
        uint64_t maxInflight = 10000;
        InflightThrottle inflightThrottle(maxInflight);
        const int kMaxLoop = 10000 / 4;

        auto func0 = [&] {
            for (int i = 0; i < kMaxLoop; ++i) {
                inflightThrottle.Increment();
            }
        };

        auto func1 = [&] {
            for (int i = 0; i < kMaxLoop + 1; ++i) {
                inflightThrottle.Increment();
            }
        };

        Thread t1(func0);
        Thread t2(func0);
        Thread t3(func0);
        Thread t4(func1);

        t1.join();
        t2.join();
        t3.join();
        t4.join();

        ASSERT_TRUE(inflightThrottle.IsOverLoad());
        inflightThrottle.Decrement();
        ASSERT_FALSE(inflightThrottle.IsOverLoad());
    }

    // Concurrent reduction
    {
        uint64_t maxInflight = 16;
        InflightThrottle inflightThrottle(maxInflight);
        const int kMaxLoop = maxInflight / 4;

        for (int i = 0; i < 2 * maxInflight; ++i) {
            inflightThrottle.Increment();
        }

        auto func0 = [&] {
            for (int i = 0; i < kMaxLoop; ++i) {
                inflightThrottle.Decrement();
            }
        };

        auto func1 = [&] {
            for (int i = 0; i < kMaxLoop - 1; ++i) {
                inflightThrottle.Decrement();
            }
        };

        Thread t1(func0);
        Thread t2(func0);
        Thread t3(func0);
        Thread t4(func1);

        t1.join();
        t2.join();
        t3.join();
        t4.join();

        ASSERT_TRUE(inflightThrottle.IsOverLoad());
        inflightThrottle.Decrement();
        ASSERT_FALSE(inflightThrottle.IsOverLoad());
    }
}

}  // namespace chunkserver
}  // namespace curve
