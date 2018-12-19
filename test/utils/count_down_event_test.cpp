/*
 * Project: curve
 * Created Date: 18-12-18
 * Author: wudemiao
 * Copyright (c) 2018 netease
 */

#include <gtest/gtest.h>

#include <thread>   //NOLINT
#include <atomic>

#include "test/utils/count_down_event.h"

namespace curve {
namespace test {

TEST(CountDownEventTest, basic) {
    {
        CountDownEvent cond(0);
        cond.Wait();
        ASSERT_EQ(1, 1);
    }

    {
        std::atomic<int> signalCount;
        signalCount.store(0, std::memory_order_release);

        CountDownEvent cond(1);

        auto func = [&] {
            signalCount.fetch_add(1, std::memory_order_acq_rel);
            cond.Signal();
        };

        std::thread t1(func);
        cond.Wait();
        ASSERT_EQ(1, signalCount.load(std::memory_order_acquire));

        t1.join();
    }

    /* 1. initCnt == Signal 次数 */
    {
        std::atomic<int> signalCount;
        signalCount.store(0, std::memory_order_release);

        const int kEventNum = 10;
        CountDownEvent cond(kEventNum);
        auto func = [&] {
            for (int i = 0; i < kEventNum; ++i) {
                signalCount.fetch_add(1, std::memory_order_acq_rel);
                cond.Signal();
            }
        };

        std::thread t1(func);
        cond.Wait();
        ASSERT_EQ(kEventNum, signalCount.load(std::memory_order_acquire));

        t1.join();
    }

    /* 2. initCnt < Signal 次数 */
    {
        std::atomic<int> signalCount;
        signalCount.store(0, std::memory_order_release);

        const int kEventNum = 20;
        const int kInitCnt  = kEventNum - 10;
        CountDownEvent cond(kInitCnt);
        auto func = [&] {
            for (int i = 0; i < kEventNum; ++i) {
                cond.Signal();
                signalCount.fetch_add(1, std::memory_order_acq_rel);
            }
        };

        std::thread t1(func);

        /* 等到 Signal 次数 > initCnt */
        while (true) {
            ::usleep(5);
            if (signalCount.load(std::memory_order_acquire) > kInitCnt) {
                break;
            }
        }
        ASSERT_GT(signalCount, kInitCnt);
        cond.Wait();

        t1.join();
    }

    /* 3. initCnt > Signal 次数 */
    {
        std::atomic<int> signalCount;
        signalCount.store(0, std::memory_order_release);

        const int kEventNum = 10;
        /* kSignalEvent1 + kSignalEvent2 等于 kEventNum */
        const int kSignalEvent1 = kEventNum - 5;
        const int kSignalEvent2 = 5;
        CountDownEvent cond(kEventNum);

        auto func1 = [&] {
            for (int i = 0; i < kSignalEvent1; ++i) {
                signalCount.fetch_add(1, std::memory_order_acq_rel);
                cond.Signal();
            }
        };

        std::thread t1(func1);
        std::atomic<bool> passWait(false);
        auto waitFunc = [&] {
            cond.Wait();
            passWait.store(true, std::memory_order_release);
        };
        std::thread waitThread(waitFunc);

        /* 由于 t1 唤醒的次数不够，所以 waitThread 会阻塞在 wait 那里 */
        ASSERT_EQ(false, passWait.load(std::memory_order_acquire));

        auto func2 = [&] {
            for (int i = 0; i < kSignalEvent2; ++i) {
                signalCount.fetch_add(1, std::memory_order_acq_rel);
                cond.Signal();
            }
        };
        /* 运行 t2，补上不够的唤醒次数 */
        std::thread t2(func2);

        t1.join();
        t2.join();
        waitThread.join();
        ASSERT_EQ(true, passWait);
        ASSERT_EQ(kEventNum, signalCount.load(std::memory_order_acquire));
    }
}

}  // namespace test
}  // namespace curve
