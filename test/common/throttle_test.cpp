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

#include "src/common/throttle.h"

#include <glog/logging.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <chrono> // NOLINT

namespace curve {
namespace common {

using curve::common::ThrottleParams;

class ThrottleTest : public ::testing::Test {
 protected:
    void SetUp() override {}

    void TearDown() override {}

    bool CheckThrottleState(const std::map<Throttle::Type, bool>& types) const {
        for (auto& t : types) {
            if (throttle_.IsThrottleEnabled(t.first) != t.second) {
                return false;
            }
        }

        return true;
    }

 protected:
    Throttle throttle_;
    ReadWriteThrottleParams params_;
};

TEST_F(ThrottleTest, TestEnabledAndDisableThrottle) {
    std::map<Throttle::Type, bool> throttleState{
        {Throttle::Type::IOPS_TOTAL, false}, {Throttle::Type::IOPS_READ, false},
        {Throttle::Type::IOPS_WRITE, false}, {Throttle::Type::BPS_TOTAL, false},
        {Throttle::Type::BPS_READ, false},   {Throttle::Type::BPS_WRITE, false},
    };

    ASSERT_TRUE(CheckThrottleState(throttleState));

    auto enable = [&](ThrottleParams* p, Throttle::Type type) {
        *p = ThrottleParams(1, 0, 0);
        throttle_.UpdateThrottleParams(params_);
        throttleState[type] = true;
        return this->CheckThrottleState(throttleState);
    };

    ASSERT_TRUE(enable(&params_.iopsTotal, Throttle::Type::IOPS_TOTAL));
    ASSERT_TRUE(enable(&params_.iopsRead, Throttle::Type::IOPS_READ));
    ASSERT_TRUE(enable(&params_.iopsWrite, Throttle::Type::IOPS_WRITE));
    ASSERT_TRUE(enable(&params_.bpsTotal, Throttle::Type::BPS_TOTAL));
    ASSERT_TRUE(enable(&params_.bpsRead, Throttle::Type::BPS_READ));
    ASSERT_TRUE(enable(&params_.bpsWrite, Throttle::Type::BPS_WRITE));

    auto disable = [&](ThrottleParams* p, Throttle::Type type) {
        *p = ThrottleParams(0, 1000, 1000);
        throttle_.UpdateThrottleParams(params_);
        throttleState[type] = false;
        return this->CheckThrottleState(throttleState);
    };

    ASSERT_TRUE(disable(&params_.iopsTotal, Throttle::Type::IOPS_TOTAL));
    ASSERT_TRUE(disable(&params_.iopsRead, Throttle::Type::IOPS_READ));
    ASSERT_TRUE(disable(&params_.iopsWrite, Throttle::Type::IOPS_WRITE));
    ASSERT_TRUE(disable(&params_.bpsTotal, Throttle::Type::BPS_TOTAL));
    ASSERT_TRUE(disable(&params_.bpsRead, Throttle::Type::BPS_READ));
    ASSERT_TRUE(disable(&params_.bpsWrite, Throttle::Type::BPS_WRITE));

    for (auto type : {Throttle::Type::IOPS_TOTAL, Throttle::Type::IOPS_READ,
                      Throttle::Type::IOPS_WRITE, Throttle::Type::BPS_TOTAL,
                      Throttle::Type::BPS_READ, Throttle::Type::BPS_WRITE}) {
        ASSERT_FALSE(throttle_.IsThrottleEnabled(type));
    }
}

TEST_F(ThrottleTest, TestIOPSAndBPSThrottle) {
    params_.iopsTotal = ThrottleParams(1000, 0, 0);     // iops limit is 1000
    params_.bpsTotal = ThrottleParams(4 * 1024, 0, 0);  // bps limit is 4096
    throttle_.UpdateThrottleParams(params_);

    auto start = std::chrono::high_resolution_clock::now();

    // consume 1 iops token and 40960 bps tokens
    // so this call will wait 10 seconds
    throttle_.Add(false, 4096 * 10);
    auto end = std::chrono::high_resolution_clock::now();
    auto seconds =
        std::chrono::duration_cast<std::chrono::seconds>(end - start).count();

    ASSERT_GE(seconds, 9);
    ASSERT_LE(seconds, 11);
}

TEST_F(ThrottleTest, TestReadAndWriteCannotExceedTotalLimit) {
    params_.iopsTotal = ThrottleParams(1000, 0, 0);  // 1000 iops limit
    params_.iopsRead = ThrottleParams(2000, 0, 0);   // 2000 iops limit
    params_.iopsWrite = ThrottleParams(4000, 0, 0);  // 4000 iops limit
    throttle_.UpdateThrottleParams(params_);

    auto fn = [&](bool isRead, uint64_t total) {
        uint64_t count = 0;
        while (count++ < total) {
            throttle_.Add(isRead, 1);
        }
    };

    {
        auto start = std::chrono::high_resolution_clock::now();
        fn(false, 10000);
        auto end = std::chrono::high_resolution_clock::now();
        auto seconds =
            std::chrono::duration_cast<std::chrono::seconds>(end - start)
                .count();

        // even read iops limit is 2000, but total limit is 1000
        // so fn will cost 10 seconds
        ASSERT_GE(seconds, 9);
        ASSERT_LE(seconds, 11);
    }

    {
        auto start = std::chrono::high_resolution_clock::now();
        fn(true, 10000);
        auto end = std::chrono::high_resolution_clock::now();
        auto seconds =
            std::chrono::duration_cast<std::chrono::seconds>(end - start)
                .count();

        // even write iops limit is 4000, but total limit is 1000
        // so fn will cost 10 seconds
        ASSERT_GE(seconds, 9);
        ASSERT_LE(seconds, 11);
    }
}

TEST_F(ThrottleTest, TestBpsBurst) {
    params_.bpsTotal = ThrottleParams(
      4 * 1024, 4 * 1024 * 2, 10);  // bps limit is 4096
    throttle_.UpdateThrottleParams(params_);
    auto start = std::chrono::high_resolution_clock::now();

    // consume 1 iops token and 40960 bps tokens
    // so this call will wait 10 seconds
    throttle_.Add(false, 4096 * 10);
    auto end = std::chrono::high_resolution_clock::now();
    auto seconds =
        std::chrono::duration_cast<std::chrono::seconds>(end - start).count();
    ASSERT_GE(seconds, 5);
    ASSERT_LE(seconds, 7);
}

TEST_F(ThrottleTest, TestIopsLimit) {
    // iops limit is 1000, burst is not enable default.
    params_.iopsTotal.limit = 1000;
    throttle_.UpdateThrottleParams(params_);
    auto fn = [&](bool isRead, uint64_t total) {
        uint64_t count = 0;
        while (count++ < total) {
            throttle_.Add(isRead, 1);
        }
    };
    auto start = std::chrono::high_resolution_clock::now();
    fn(true, 10000);
    auto end = std::chrono::high_resolution_clock::now();
    auto seconds =
        std::chrono::duration_cast<std::chrono::seconds>(end - start).count();
    // consume 10000 iops token, limit is 1000
    // so this call will wait 10 seconds
    ASSERT_GE(seconds, 9);
    ASSERT_LE(seconds, 11);
}

}  // namespace common
}  // namespace curve
