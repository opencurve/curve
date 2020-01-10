/*
 * Project: curve
 * File Created: Statday, 26th Oct 2019 3:24:40 pm
 * Author: tongguangxun
 * Copyright (c) 2019 NetEase
 */

#include <gtest/gtest.h>
#include <glog/logging.h>
#include "src/client/config_info.h"
#include "src/client/chunk_closure.h"

namespace curve {
namespace client {
TEST(ClientClosure, GetPowTimeTest) {
    ASSERT_EQ(0, ClientClosure::GetPowTime(0));
    ASSERT_EQ(0, ClientClosure::GetPowTime(1));
    ASSERT_EQ(2, ClientClosure::GetPowTime(4));
    ASSERT_EQ(1, ClientClosure::GetPowTime(2));
    ASSERT_EQ(1, ClientClosure::GetPowTime(3));
    ASSERT_EQ(2, ClientClosure::GetPowTime(7));
    ASSERT_EQ(3, ClientClosure::GetPowTime(10));
    ASSERT_EQ(3, ClientClosure::GetPowTime(15));
    ASSERT_EQ(5, ClientClosure::GetPowTime(32));
    ASSERT_EQ(5, ClientClosure::GetPowTime(63));
    ASSERT_EQ(6, ClientClosure::GetPowTime(64));
    ASSERT_EQ(7, ClientClosure::GetPowTime(255));
    ASSERT_EQ(8, ClientClosure::GetPowTime(256));
    ASSERT_EQ(8, ClientClosure::GetPowTime(257));
    ASSERT_EQ(10, ClientClosure::GetPowTime(1024));
    ASSERT_EQ(10, ClientClosure::GetPowTime(2047));
    ASSERT_EQ(11, ClientClosure::GetPowTime(2048));
    ASSERT_EQ(11, ClientClosure::GetPowTime(2049));
}

TEST(ClientClosure, OverLoadBackOffTest) {
    FailureRequestOption_t failopt;
    failopt.chunkserverMaxRetrySleepIntervalUS = 8000000;
    failopt.chunkserverOPRetryIntervalUS = 500000;

    ClientClosure::SetFailureRequestOption(failopt);

    WriteChunkClosure cc(nullptr, nullptr);

    for (int i = 1; i < 1000; i++) {
        if (i < ClientClosure::backoffParam_.maxOverloadPow) {
            uint64_t curTime = failopt.chunkserverOPRetryIntervalUS*std::pow(2, i);    // NOLINT
            ASSERT_LT(cc.OverLoadBackOff(i), curTime + 0.1 * curTime);
            ASSERT_GT(cc.OverLoadBackOff(i), curTime - 0.1 * curTime);
        } else {
            ASSERT_LT(cc.OverLoadBackOff(i),
            failopt.chunkserverMaxRetrySleepIntervalUS +
            0.1 * failopt.chunkserverMaxRetrySleepIntervalUS);
            ASSERT_GT(cc.OverLoadBackOff(i),
            failopt.chunkserverMaxRetrySleepIntervalUS -
            0.1 * failopt.chunkserverMaxRetrySleepIntervalUS);
        }
    }

    failopt.chunkserverMaxRetrySleepIntervalUS = 64000000;
    failopt.chunkserverOPRetryIntervalUS = 500000;

    ClientClosure::SetFailureRequestOption(failopt);

    for (int i = 1; i < 1000; i++) {
        if (i < ClientClosure::backoffParam_.maxOverloadPow) {
            uint64_t curTime = failopt.chunkserverOPRetryIntervalUS*std::pow(2, i);    // NOLINT
            ASSERT_LT(cc.OverLoadBackOff(i), curTime + 0.1 * curTime);
            ASSERT_GT(cc.OverLoadBackOff(i), curTime - 0.1 * curTime);
        } else {
            ASSERT_LT(cc.OverLoadBackOff(i),
            failopt.chunkserverMaxRetrySleepIntervalUS +
            0.1 * failopt.chunkserverMaxRetrySleepIntervalUS);
            ASSERT_GT(cc.OverLoadBackOff(i),
            failopt.chunkserverMaxRetrySleepIntervalUS -
            0.1 * failopt.chunkserverMaxRetrySleepIntervalUS);
        }
    }
}

TEST(ClientClosure, TimeoutBackOffTest) {
    FailureRequestOption_t failopt;
    failopt.chunkserverMaxRPCTimeoutMS = 3000;
    failopt.chunkserverRPCTimeoutMS = 500;

    ClientClosure::SetFailureRequestOption(failopt);

    WriteChunkClosure cc(nullptr, nullptr);

    for (int i = 1; i < 1000; i++) {
        if (i < ClientClosure::backoffParam_.maxTimeoutPow) {
            uint64_t curTime = failopt.chunkserverRPCTimeoutMS*std::pow(2, i);
            ASSERT_EQ(cc.TimeoutBackOff(i), curTime);
        } else {
            ASSERT_EQ(cc.TimeoutBackOff(i), 2000);
        }
    }

    failopt.chunkserverMaxRPCTimeoutMS = 4000;
    failopt.chunkserverRPCTimeoutMS = 500;

    ClientClosure::SetFailureRequestOption(failopt);

    for (int i = 1; i < 1000; i++) {
        if (i < ClientClosure::backoffParam_.maxTimeoutPow) {
            uint64_t curTime = failopt.chunkserverRPCTimeoutMS*std::pow(2, i);
            ASSERT_EQ(cc.TimeoutBackOff(i), curTime);
        } else {
            ASSERT_EQ(cc.TimeoutBackOff(i), 4000);
        }
    }
}

}   // namespace client
}   // namespace curve
