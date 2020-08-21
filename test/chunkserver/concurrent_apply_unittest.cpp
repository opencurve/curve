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
 * File Created: Monday, 24th December 2018 9:57:24 am
 * Author: tongguangxun
 */

#include <gtest/gtest.h>

#include <atomic>
#include <functional>

#include "src/common/timeutility.h"
#include "src/chunkserver/concurrent_apply.h"

using curve::chunkserver::ConcurrentApplyModule;

TEST(ConcurrentApplyModule, ConcurrentApplyModuleInitTest) {
    /**
     * init twice test, exptect init success
     */

    ConcurrentApplyModule concurrentapply;
    ASSERT_TRUE(concurrentapply.Init(-1, 1, false));
    ASSERT_TRUE(concurrentapply.Init(1, -1, false));
    ASSERT_TRUE(concurrentapply.Init(-1, -1, false));
    ASSERT_TRUE(concurrentapply.Init(1, 1, false));
    ASSERT_TRUE(concurrentapply.Init(1, 1, false));
    concurrentapply.Stop();
}

TEST(ConcurrentApplyModule, ConcurrentApplyModuleRunTest) {
    /**
     * worker run the task correctly
     */

    ConcurrentApplyModule concurrentapply;
    int testnum = 0;
    auto runtask = [&testnum]() {
        testnum++;
    };
    ASSERT_FALSE(concurrentapply.Push(1, runtask));
    ASSERT_TRUE(concurrentapply.Init(2, 1, false));
    concurrentapply.Push(0, runtask);
    concurrentapply.Flush();
    ASSERT_EQ(1, testnum);
    concurrentapply.Push(1, runtask);
    concurrentapply.Flush();
    ASSERT_EQ(2, testnum);
    concurrentapply.Stop();
}

TEST(ConcurrentApplyModule, ConcurrentApplyModuleFlushTest) {
    /**
     * test flush interface will flush all undo task before return
     */

    ConcurrentApplyModule concurrentapply;
    ASSERT_TRUE(concurrentapply.Init(2, 10000, false));
    std::atomic<uint32_t> testnum(0);
    auto runtask = [&testnum]() {
        testnum.fetch_add(1);
    };

    for (int i = 0; i < 5000; i++) {
        concurrentapply.Push(i, runtask);
        concurrentapply.Push(i + 1, runtask);
    }

    concurrentapply.Flush();
    ASSERT_EQ(10000, testnum);
    concurrentapply.Stop();
}

TEST(ConcurrentApplyModule, ConcurrentApplyModuleFlushConcurrentTest) {
    /**
     * test flush interface in concurrent condition
     */
    std::atomic<bool> stop(false);
    std::atomic<uint32_t> testnum(0);

    ConcurrentApplyModule concurrentapply;
    ASSERT_TRUE(concurrentapply.Init(10, 1, false));
    auto testfunc = [&concurrentapply, &stop, &testnum]() {
        auto runtask = [&testnum]() {
            testnum.fetch_add(1);
        };
        while (!stop.load()) {
            for (int i = 0; i < 10; i++) {
                concurrentapply.Push(i, runtask);
            }
        }
    };

    auto flushtest = [&concurrentapply, &stop, &testnum]() {
        while (!stop.load()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            concurrentapply.Flush();
        }
    };

    std::thread t(testfunc);
    std::thread f(flushtest);

    while (testnum.load() <= 1000000) {
    }

    stop.store(true);

    std::cout << "thread exit, join" << std::endl;
    t.join();
    f.join();

    concurrentapply.Flush();
    ASSERT_GT(testnum, 1000000);
    concurrentapply.Stop();
}

// ci暫時不跑性能测试
#if 0
TEST(ConcurrentApplyModule, MultiCopysetPerformanceTest) {
    std::atomic<bool> start(false);
    std::atomic<uint32_t> donecount(0);

    std::mutex mtx;
    std::condition_variable startcv;

    ConcurrentApplyModule concurrentapply;
    ASSERT_TRUE(concurrentapply.Init(10, 1, false));
    auto testfunc = [&concurrentapply, &startcv, &start, &mtx, &donecount]() {
        {
            std::unique_lock<std::mutex> lk(mtx);
            startcv.wait(lk, [&start](){return start.load() == true;});
        }
        auto runtask = []() {};
        uint32_t count = 0;
        while (count < 1000000) {
            concurrentapply.Push(count%10, runtask);
            count++;
        }
        donecount.fetch_add(1);
    };
    std::vector<std::thread> threadvec;

    for (int i = 0; i < 10; i++) {
        threadvec.emplace_back(std::thread(testfunc));
    }

    uint64_t startime = curve::common::TimeUtility::GetTimeofDayUs();

    start.store(true);
    startcv.notify_all();

    while (donecount.load() < 10) {}
    uint64_t stoptime = curve::common::TimeUtility::GetTimeofDayUs();

    for (auto& iter : threadvec) {
        if (iter.joinable()) {
            iter.join();
        }
    }

    float usetime = (stoptime - startime)/1000000.0;
    std::cout << "multi copyet multi chunk concurrent module tps = "
              << 10000000/usetime
              << std::endl;
    concurrentapply.Stop();
}

TEST(ConcurrentApplyModule, SingleCopysetPerformanceTest) {
    std::atomic<bool> start(false);
    std::atomic<uint32_t> donecount(0);

    std::mutex mtx;
    std::condition_variable startcv;

    ConcurrentApplyModule concurrentapply;
    ASSERT_TRUE(concurrentapply.Init(10, 1, false));
    auto testfunc = [&concurrentapply, &startcv, &start, &mtx, &donecount]() {
        {
            std::unique_lock<std::mutex> lk(mtx);
            startcv.wait(lk, [&start](){return start.load() == true;});
        }
        auto runtask = []() {};
        uint32_t count = 0;
        while (count < 1000000) {
            concurrentapply.Push(count%10, runtask);
            count++;
        }
        donecount.fetch_add(1);
    };
    std::vector<std::thread> threadvec;

    for (int i = 0; i < 1; i++) {
        threadvec.emplace_back(std::thread(testfunc));
    }

    uint64_t startime = curve::common::TimeUtility::GetTimeofDayUs();

    start.store(true);
    startcv.notify_all();

    while (donecount.load() < 1) {}
    uint64_t stoptime = curve::common::TimeUtility::GetTimeofDayUs();

    for (auto& iter : threadvec) {
        if (iter.joinable()) {
            iter.join();
        }
    }

    float usetime = (stoptime - startime)/1000000.0;
    std::cout << "single copyet multi chunk concurrent module tps = "
              << 1000000/usetime
              << std::endl;
    concurrentapply.Stop();
}

TEST(ConcurrentApplyModule, SingleCopysetSingleChunkPerformanceTest) {
    std::atomic<bool> start(false);
    std::atomic<uint32_t> donecount(0);

    std::mutex mtx;
    std::condition_variable startcv;

    ConcurrentApplyModule concurrentapply;
    ASSERT_TRUE(concurrentapply.Init(10, 1, false));
    auto testfunc = [&concurrentapply, &startcv, &start, &mtx, &donecount]() {
        {
            std::unique_lock<std::mutex> lk(mtx);
            startcv.wait(lk, [&start](){return start.load() == true;});
        }
        auto runtask = []() {};
        uint32_t count = 0;
        while (count < 1000000) {
            concurrentapply.Push(1, runtask);
            count++;
        }
        donecount.fetch_add(1);
    };
    std::vector<std::thread> threadvec;

    for (int i = 0; i < 1; i++) {
        threadvec.emplace_back(std::thread(testfunc));
    }

    uint64_t startime = curve::common::TimeUtility::GetTimeofDayUs();

    start.store(true);
    startcv.notify_all();

    while (donecount.load() < 1) {}
    uint64_t stoptime = curve::common::TimeUtility::GetTimeofDayUs();

    for (auto& iter : threadvec) {
        if (iter.joinable()) {
            iter.join();
        }
    }

    float usetime = (stoptime - startime)/1000000.0;
    std::cout << "single copyet single chunk concurrent module tps = "
              << 1000000/usetime
              << std::endl;
    concurrentapply.Stop();
}

TEST(ConcurrentApplyModule, MultiCopysetSingleQueuePerformanceTest) {
    std::atomic<bool> start(false);
    std::atomic<uint32_t> donecount(0);

    std::mutex mtx;
    std::condition_variable startcv;

    ConcurrentApplyModule concurrentapply;
    ASSERT_TRUE(concurrentapply.Init(10, 1, false));
    auto testfunc = [&concurrentapply, &startcv, &start, &mtx, &donecount]() {
        {
            std::unique_lock<std::mutex> lk(mtx);
            startcv.wait(lk, [&start](){return start.load() == true;});
        }
        auto runtask = []() {};
        uint32_t count = 0;
        while (count < 1000000) {
            concurrentapply.Push(1, runtask);
            count++;
        }
        donecount.fetch_add(1);
    };
    std::vector<std::thread> threadvec;

    for (int i = 0; i < 10; i++) {
        threadvec.emplace_back(std::thread(testfunc));
    }

    uint64_t startime = curve::common::TimeUtility::GetTimeofDayUs();

    start.store(true);
    startcv.notify_all();

    while (donecount.load() < 10) {}
    uint64_t stoptime = curve::common::TimeUtility::GetTimeofDayUs();

    for (auto& iter : threadvec) {
        if (iter.joinable()) {
            iter.join();
        }
    }

    float usetime = (stoptime - startime)/1000000.0;
    std::cout << "multi copyet single queue concurrent module tps = "
              << 10000000/usetime
              << std::endl;
    concurrentapply.Stop();
}
#endif
