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
 * Date: Thu Sep  2 14:49:04 CST 2021
 * Author: wuhanqing
 */

#include "curvefs/src/metaserver/copyset/apply_queue.h"

#include <gtest/gtest.h>

#include <ctime>

#include "src/common/concurrent/count_down_event.h"

namespace curvefs {
namespace metaserver {
namespace copyset {

using ::curve::common::CountDownEvent;

TEST(ApplyQueueTest, StartAndStopTest) {
    ApplyQueue applyQueue;

    ApplyQueueOption option;
    option.workerCount = 0;
    option.queueDepth = 0;

    EXPECT_FALSE(applyQueue.Start(option));

    option.workerCount = 5;
    option.queueDepth = 100 * 100 * 100;

    ASSERT_TRUE(applyQueue.Start(option));
    EXPECT_TRUE(applyQueue.Start(option));

    std::atomic<bool> runned(false);
    auto task = [&]() {
        runned = true;
    };

    applyQueue.Push(time(nullptr), task);

    while (!runned) {}

    applyQueue.Stop();
    applyQueue.Stop();
}

TEST(ApplyQueueTest, FlushTest) {
    ApplyQueueOption option;
    option.workerCount = 10;
    option.queueDepth = 100 * 100;

    ApplyQueue applyQueue;
    ASSERT_TRUE(applyQueue.Start(option));

    int taskCount = option.workerCount * option.queueDepth;
    std::atomic<int> runned(0);

    auto task = [&runned]() { runned.fetch_add(1, std::memory_order_relaxed); };

    for (int i = 0; i < taskCount; ++i) {
        applyQueue.Push(i, task);
    }

    applyQueue.Flush();
    ASSERT_EQ(taskCount, runned.load(std::memory_order_relaxed));
    applyQueue.Stop();
}

}  // namespace copyset
}  // namespace metaserver
}  // namespace curvefs
