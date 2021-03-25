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
        double expected = 2;
        ASSERT_GE(expiredSec, expected * 0.9);
        ASSERT_LE(expiredSec, expected * 1.1);
    }
    {
        ExpiredTime expiredTime;
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        auto expiredMs = expiredTime.ExpiredMs();
        double expected = 1000;
        ASSERT_GE(expiredMs, expected * 0.9);
        ASSERT_LE(expiredMs, expected * 1.1);
    }
    {
        ExpiredTime expiredTime;
        std::this_thread::sleep_for(std::chrono::microseconds(1000000));
        auto expiredUs = expiredTime.ExpiredUs();
        double expected = 1000000;
        ASSERT_GE(expiredUs, expected * 0.9);
        ASSERT_LE(expiredUs, expected * 1.1);
    }
}

}  // namespace common

}  // namespace curve
