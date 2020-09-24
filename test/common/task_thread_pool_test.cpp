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
    cond->Signal();
}

int TestAdd2(int a, double b, CountDownEvent *cond) {
    double c = a + b;
    cond->Signal();
    return 0;
}

TEST(TaskThreadPool, basic) {
    /* 测试线程池 start 入参 */
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
        /* 测试不设置，此时为 INT_MAX */
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
        /* TestAdd2 是有返回值的 function */
        CountDownEvent cond2(1);
        taskThreadPool.Enqueue(TestAdd2, 1, 1.234, &cond2);
        cond2.Wait();
        ASSERT_EQ(0, taskThreadPool.QueueSize());
        taskThreadPool.Stop();
    }

    /* 基本运行 task 测试 */
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

        /* 等待所有 task 执行完毕 */
        cond.Wait();
        ASSERT_EQ(3 * kMaxLoop, runTaskCount.load(std::memory_order_acquire));

        taskThreadPool.Stop();
    }

    /* 测试队列满了，push会阻塞 */
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

        /* 把线程池的所有处理线程都卡住了 */
        taskThreadPool.Enqueue(waitTask, &startRunCond1, &cond1);
        taskThreadPool.Enqueue(waitTask, &startRunCond2, &cond2);
        taskThreadPool.Enqueue(waitTask, &startRunCond3, &cond3);
        taskThreadPool.Enqueue(waitTask, &startRunCond4, &cond4);
        /* 等待 waitTask1、waitTask2、waitTask3、waitTask4 都开始运行 */
        startRunCond1.Wait();
        startRunCond2.Wait();
        startRunCond3.Wait();
        startRunCond4.Wait();
        ASSERT_EQ(0, taskThreadPool.QueueSize());
        ASSERT_EQ(0, runTaskCount.load());

        auto task = [&] {
            runTaskCount.fetch_add(1, std::memory_order_acq_rel);
        };

        /* 记录线程 push 到线程池 queue 的 task 数量 */
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

        /* 等待线程池 queue 被 push 满 */
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

        /* push 进去的 task 都没有被执行 */
        ASSERT_EQ(0, runTaskCount.load(std::memory_order_acquire));
        /**
         * 此时，thread pool 的 queue 肯定 push 满了，且 push
         * 满了之后就没法再 push 了
         */
        ASSERT_EQ(pushTaskCount, taskThreadPool.QueueCapacity());
        ASSERT_EQ(taskThreadPool.QueueCapacity(), taskThreadPool.QueueSize());

        /* 将线程池中的线程都唤醒 */
        cond1.Signal();
        cond2.Signal();
        cond3.Signal();
        cond4.Signal();

        /* 等待所有 task 执行完成 */
        while (true) {
            ::usleep(10);
            if (runTaskCount.load(std::memory_order_acquire)
                >= 4 + 3 * kMaxLoop) {
                break;
            }
        }

        /**
         * 等待所有的 push thread 退出，这样才能保证 pushThreadCount 计数更新了
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
