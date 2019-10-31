/*
 * Project: curve
 * Created Date: 20190805
 * Author: lixiaocui
 * Copyright (c) 2018 netease
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

}  // namespace common
}  // namespace curve

