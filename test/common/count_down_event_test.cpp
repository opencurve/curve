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
 * Created Date: 18-12-18
 * Author: wudemiao
 */

#include <gtest/gtest.h>

#include <thread>   //NOLINT
#include <atomic>
#include <chrono>   //NOLINT

#include "src/common/concurrent/count_down_event.h"

namespace curve {
namespace common {

TEST(CountDownEventTest, basic) {
    {
        CountDownEvent cond(0);
        cond.Wait();
        ASSERT_EQ(1, 1);
    }
    {
        CountDownEvent cond(4);
        cond.Reset(0);
        cond.Wait();
        ASSERT_EQ(1, 1);
    }
    {
        CountDownEvent cond(0);
        ASSERT_TRUE(cond.WaitFor(1000));
    }
    {
        CountDownEvent cond(1);
        ASSERT_FALSE(cond.WaitFor(1000));
    }
    {
        CountDownEvent cond(8);
        std::atomic<bool> isRun(false);
        int sleepMs = 500;

        auto func = [&] {
            cond.WaitFor(sleepMs);
            isRun.store(true);
        };

        std::thread t1(func);
        std::this_thread::sleep_for(std::chrono::milliseconds(3*sleepMs));
        ASSERT_TRUE(isRun.load());

        t1.join();
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
    {
        CountDownEvent cond(0);
        cond.WaitFor(1000);
    }


    /*1 InitCnt==Signal count*/
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

    /*2 InitCnt<Signal count*/
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

        /*Wait until Signal count>initCnt*/
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

    /*3 InitCnt>Signal count*/
    {
        std::atomic<int> signalCount;
        signalCount.store(0, std::memory_order_release);

        const int kEventNum = 10;
        /*KSignalEvent1+kSignalEvent2 equals kEventNum*/
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

        /*Due to insufficient wake-up times for t1, waitThread will block at the wait location*/
        ASSERT_EQ(false, passWait.load(std::memory_order_acquire));

        auto func2 = [&] {
            for (int i = 0; i < kSignalEvent2; ++i) {
                signalCount.fetch_add(1, std::memory_order_acq_rel);
                cond.Signal();
            }
        };
        /*Run t2 to make up for insufficient wake-up times*/
        std::thread t2(func2);

        t1.join();
        t2.join();
        waitThread.join();
        ASSERT_EQ(true, passWait);
        ASSERT_EQ(kEventNum, signalCount.load(std::memory_order_acquire));
    }
    // WaitFor test: timeout
    {
        CountDownEvent cond(100);
        int waitForMs = 2000;

        auto SignalFunc = [&] {
            cond.Signal();
            cond.Signal();
        };

        std::thread t1(SignalFunc);

        auto start = std::chrono::high_resolution_clock::now();
        cond.WaitFor(waitForMs);
        auto end = std::chrono::high_resolution_clock::now();

        std::chrono::duration<double, std::milli> elpased = end - start;
        std::cerr << "elapsed: " << elpased.count() << std::endl;
        //The event did not arrive and returned after a timeout, allowing for a certain error
        ASSERT_GT(static_cast<int>(elpased.count()), waitForMs-1000);

        t1.join();
    }
    // WaitFor test: event arrive, not timeout
    {
        CountDownEvent cond(2);
        int waitForMs = 2000;

        auto SignalFunc = [&] {
            cond.Signal();
            cond.Signal();
        };

        std::thread t1(SignalFunc);

        auto start = std::chrono::high_resolution_clock::now();
        cond.WaitFor(waitForMs);
        auto end = std::chrono::high_resolution_clock::now();

        std::chrono::duration<double, std::milli> elpased = end - start;
        std::cerr << "elapsed: " << elpased.count() << std::endl;
        //Event reached, return early
        ASSERT_GT(waitForMs, static_cast<int>(elpased.count()));

        t1.join();
    }
}

}  // namespace common
}  // namespace curve
