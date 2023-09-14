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
 * Created Date: 18-12-14
 * Author: wudemiao
 */

#include <gtest/gtest.h>

#include <iostream>
#include <atomic>

#include "src/common/concurrent/count_down_event.h"
#include "src/common/concurrent/task_thread_pool.h"

namespace curve {
namespace common {

using curve::common::CountDownEvent;

void TestAdd1(int a, double b, CountDownEvent *cond) {
    double c = a + b;
    (void)c;
    cond->Signal();
}

int TestAdd2(int a, double b, CountDownEvent *cond) {
    double c = a + b;
    (void)c;
    cond->Signal();
    return 0;
}

TEST(TaskThreadPool, basic) {
    /*Test thread pool start input parameter*/
    {
        TaskThreadPool<> taskThreadPool;
        ASSERT_EQ(-1, taskThreadPool.Start(2, 0));
    }

    {
        TaskThreadPool<> taskThreadPool;
        ASSERT_EQ(-1, taskThreadPool.Start(2, -4));
    }

    {
        TaskThreadPool<> taskThreadPool;
        ASSERT_EQ(-1, taskThreadPool.Start(0, 1));
    }

    {
        TaskThreadPool<> taskThreadPool;
        ASSERT_EQ(-1, taskThreadPool.Start(-2, 1));
    }

    {
        TaskThreadPool<> taskThreadPool;
        ASSERT_EQ(-1, taskThreadPool.Start(-2, -1));
    }

    {
        /*Test not set, at this time it is INT_ MAX*/
        TaskThreadPool<> taskThreadPool;
        ASSERT_EQ(0, taskThreadPool.Start(4));
        ASSERT_EQ(INT_MAX, taskThreadPool.QueueCapacity());
        ASSERT_EQ(4, taskThreadPool.ThreadOfNums());
        ASSERT_EQ(0, taskThreadPool.QueueSize());
        taskThreadPool.Stop();
    }

    {
        TaskThreadPool<> taskThreadPool;
        ASSERT_EQ(0, taskThreadPool.Start(4, 15));
        ASSERT_EQ(15, taskThreadPool.QueueCapacity());
        ASSERT_EQ(4, taskThreadPool.ThreadOfNums());
        ASSERT_EQ(0, taskThreadPool.QueueSize());
        CountDownEvent cond1(1);
        taskThreadPool.Enqueue(TestAdd1, 1, 1.234, &cond1);
        cond1.Wait();
        /*TestAdd2 is a function with a return value*/
        CountDownEvent cond2(1);
        taskThreadPool.Enqueue(TestAdd2, 1, 1.234, &cond2);
        cond2.Wait();
        ASSERT_EQ(0, taskThreadPool.QueueSize());
        taskThreadPool.Stop();
    }

    /*Basic task testing*/
    {
        std::atomic<int32_t> runTaskCount;
        runTaskCount.store(0, std::memory_order_release);
        const int kMaxLoop = 100;
        const int kQueueCapacity = 15;
        const int kThreadNums = 4;
        CountDownEvent cond(3 * kMaxLoop);

        auto task = [&] {
            runTaskCount.fetch_add(1, std::memory_order_acq_rel);
            cond.Signal();
        };

        TaskThreadPool<> taskThreadPool;
        ASSERT_EQ(0, taskThreadPool.Start(kThreadNums, kQueueCapacity));
        ASSERT_EQ(kQueueCapacity, taskThreadPool.QueueCapacity());
        ASSERT_EQ(kThreadNums, taskThreadPool.ThreadOfNums());

        auto threadFunc = [&] {
            for (int i = 0; i < kMaxLoop; ++i) {
                taskThreadPool.Enqueue(task);
            }
        };

        std::thread t1(threadFunc);
        std::thread t2(threadFunc);
        std::thread t3(threadFunc);

        t1.join();
        t2.join();
        t3.join();

        /*Wait for all tasks to complete execution*/
        cond.Wait();
        ASSERT_EQ(3 * kMaxLoop, runTaskCount.load(std::memory_order_acquire));

        taskThreadPool.Stop();
    }

    /*The test queue is full, push will block*/
    {
        std::atomic<int32_t> runTaskCount;
        runTaskCount.store(0, std::memory_order_release);
        const int kMaxLoop = 100;
        const int kQueueCapacity = 15;
        const int kThreadNums = 4;

        CountDownEvent cond1(1);
        CountDownEvent startRunCond1(1);
        CountDownEvent cond2(1);
        CountDownEvent startRunCond2(1);
        CountDownEvent cond3(1);
        CountDownEvent startRunCond3(1);
        CountDownEvent cond4(1);
        CountDownEvent startRunCond4(1);

        auto waitTask = [&](CountDownEvent* sigCond,
                            CountDownEvent* waitCond) {
            sigCond->Signal();
            waitCond->Wait();
            runTaskCount.fetch_add(1, std::memory_order_acq_rel);
        };

        TaskThreadPool<> taskThreadPool;
        ASSERT_EQ(0, taskThreadPool.Start(kThreadNums, kQueueCapacity));
        ASSERT_EQ(kQueueCapacity, taskThreadPool.QueueCapacity());
        ASSERT_EQ(kThreadNums, taskThreadPool.ThreadOfNums());

        /*Stuck all processing threads in the thread pool*/
        taskThreadPool.Enqueue(waitTask, &startRunCond1, &cond1);
        taskThreadPool.Enqueue(waitTask, &startRunCond2, &cond2);
        taskThreadPool.Enqueue(waitTask, &startRunCond3, &cond3);
        taskThreadPool.Enqueue(waitTask, &startRunCond4, &cond4);
        /*Wait for waitTask1, waitTask2, waitTask3, and waitTask4 to start running*/
        startRunCond1.Wait();
        startRunCond2.Wait();
        startRunCond3.Wait();
        startRunCond4.Wait();
        ASSERT_EQ(0, taskThreadPool.QueueSize());
        ASSERT_EQ(0, runTaskCount.load());

        auto task = [&] {
            runTaskCount.fetch_add(1, std::memory_order_acq_rel);
        };

        /*Record the number of tasks from thread push to thread pool queue*/
        std::atomic<int32_t> pushTaskCount1;
        std::atomic<int32_t> pushTaskCount2;
        std::atomic<int32_t> pushTaskCount3;
        CountDownEvent pushThreadCond(3);

        pushTaskCount1.store(0, std::memory_order_release);
        pushTaskCount2.store(0, std::memory_order_release);
        pushTaskCount3.store(0, std::memory_order_release);

        auto threadFunc = [&](std::atomic<int32_t>* pushTaskCount) {
            for (int i = 0; i < kMaxLoop; ++i) {
                taskThreadPool.Enqueue(task);
                pushTaskCount->fetch_add(1);
            }
            pushThreadCond.Signal();
        };

        std::thread t1(std::bind(threadFunc, &pushTaskCount1));
        std::thread t2(std::bind(threadFunc, &pushTaskCount2));
        std::thread t3(std::bind(threadFunc, &pushTaskCount3));

        /*Waiting for thread pool queue to be pushed full*/
        int pushTaskCount;
        while (true) {
            ::usleep(50);
            pushTaskCount = 0;
            pushTaskCount += pushTaskCount1.load(std::memory_order_acquire);
            pushTaskCount += pushTaskCount2.load(std::memory_order_acquire);
            pushTaskCount += pushTaskCount3.load(std::memory_order_acquire);

            if (pushTaskCount >= kQueueCapacity) {
                break;
            }
        }

        /*The tasks that were pushed in were not executed*/
        ASSERT_EQ(0, runTaskCount.load(std::memory_order_acquire));
        /**
         *At this point, the thread pool queue must be full of push, and the push
         *After it's full, it can't push anymore
         */
        ASSERT_EQ(pushTaskCount, taskThreadPool.QueueCapacity());
        ASSERT_EQ(taskThreadPool.QueueCapacity(), taskThreadPool.QueueSize());

        /*Wake up all threads in the thread pool*/
        cond1.Signal();
        cond2.Signal();
        cond3.Signal();
        cond4.Signal();

        /*Wait for all task executions to complete*/
        while (true) {
            ::usleep(10);
            if (runTaskCount.load(std::memory_order_acquire)
                >= 4 + 3 * kMaxLoop) {
                break;
            }
        }

        /**
         *Wait for all push threads to exit so that the pushThreadCount count is updated
         */
        pushThreadCond.Wait();

        pushTaskCount = 0;
        pushTaskCount += pushTaskCount1.load(std::memory_order_acquire);
        pushTaskCount += pushTaskCount2.load(std::memory_order_acquire);
        pushTaskCount += pushTaskCount3.load(std::memory_order_acquire);
        ASSERT_EQ(3 * kMaxLoop, pushTaskCount);
        ASSERT_EQ(4 + 3 * kMaxLoop,
                  runTaskCount.load(std::memory_order_acquire));

        t1.join();
        t2.join();
        t3.join();

        taskThreadPool.Stop();
    }
}

}  // namespace common
}  // namespace curve
