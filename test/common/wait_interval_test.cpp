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
 * Created Date: 20190805
 * Author: lixiaocui
 */

#include <gtest/gtest.h>
#include <thread> //NOLINT
#include <chrono> //NOLINT
#include "src/common/wait_interval.h"
#include "src/common/timeutility.h"

namespace curve {
namespace common {
TEST(WaitIntervalTest, test) {
    WaitInterval waitInterval;
    waitInterval.Init(100);

    int count = 0;
    uint64_t start = TimeUtility::GetTimeofDayMs();
    while (TimeUtility::GetTimeofDayMs() - start < 500) {
        count++;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
        waitInterval.WaitForNextExcution();
    }

    ASSERT_EQ(5, count);
}

TEST(IntervalTest, signalTest) {
    WaitInterval waitInterval;
    waitInterval.Init(1000);

    uint64_t start = TimeUtility::GetTimeofDayMs();
    bool isSignal = true;
    int count = 0;
    while (true) {
        waitInterval.WaitForNextExcution();
        if (isSignal) {
           sleep(1);
           waitInterval.StopWait();
           isSignal = false;
        }
        if (++count > 10) {
            break;
        }
    }
    uint64_t end = TimeUtility::GetTimeofDayMs();
    uint64_t dur = end - start;
    ASSERT_GT(dur, 5000);
}

}  // namespace common
}  // namespace curve

