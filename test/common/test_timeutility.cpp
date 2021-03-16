/**
 * Project: curve
 * Date: Wed May 27 18:43:30 CST 2020
 * Author: wuhanqing
 * Copyright (c) 2020 netease
 */

#include <gtest/gtest.h>
#include <chrono>  //NOLINT
#include <thread>  //NOLINT
#include "src/common/timeutility.h"

namespace curve {
namespace common {

TEST(ExpiredTimeTest, CommonTest) {
    {
        ExpiredTime expiredTime;
        std::this_thread::sleep_for(std::chrono::seconds(2));
        auto expiredSec = expiredTime.ExpiredSec();
        ASSERT_TRUE(expiredSec >= 1.8 && expiredSec <= 2.2);
    }
    {
        ExpiredTime expiredTime;
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        auto expiredMs = expiredTime.ExpiredMs();
        ASSERT_TRUE(expiredMs >= (1000 - 10) && expiredMs <= (1000 + 10));
    }
    {
        ExpiredTime expiredTime;
        std::this_thread::sleep_for(std::chrono::microseconds(1000000));
        auto expiredUs = expiredTime.ExpiredUs();
        ASSERT_TRUE(expiredUs >= (1000000 - 200) &&
                    expiredUs <= (1000000 + 200));
    }
}

}  // namespace common

}  // namespace curve
