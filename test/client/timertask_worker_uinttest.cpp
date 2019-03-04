/*
 * Project: curve
 * File Created: Friday, 1st March 2019 5:02:28 pm
 * Author: tongguangxun
 * Copyright (c)￼ 2018 netease
 */

#include <gtest/gtest.h>
#include <atomic>   // NOLINT
#include <mutex>    // NOLINT
#include <condition_variable>   // NOLINT
#include <functional>

#include "src/client/timertask_worker.h"

using curve::client::TimerTask;
using curve::client::TimerTaskWorker;

TEST(TimerThreadTest, AddandCancelTimer) {
    TimerTaskWorker tw;
    TimerTask t1(1000);
    TimerTask t2(1000);
    TimerTask t3(1000);

    tw.Start();
    ASSERT_TRUE(tw.AddTimerTask(&t1));
    ASSERT_FALSE(tw.AddTimerTask(&t1));
    ASSERT_TRUE(tw.AddTimerTask(&t2));
    ASSERT_TRUE(tw.AddTimerTask(&t3));

    ASSERT_TRUE(tw.HasTimerTask(&t1));
    ASSERT_TRUE(tw.HasTimerTask(&t2));
    ASSERT_TRUE(tw.HasTimerTask(&t3));

    ASSERT_TRUE(tw.CancelTimerTask(&t1));
    ASSERT_FALSE(tw.CancelTimerTask(&t1));
    ASSERT_TRUE(tw.CancelTimerTask(&t2));
    ASSERT_TRUE(tw.CancelTimerTask(&t3));

    ASSERT_FALSE(tw.HasTimerTask(&t1));
    ASSERT_FALSE(tw.HasTimerTask(&t2));
    ASSERT_FALSE(tw.HasTimerTask(&t3));
    tw.Stop();
}

TEST(TimerThreadTest, TestTimerSequence) {
    std::mutex mtx;
    std::atomic<bool> flag1(false);
    std::condition_variable cv1;
    std::atomic<bool> flag2(false);
    std::condition_variable cv2;
    int count1 = 0;
    int count2 = 0;

    auto t1f = [&count1, &mtx, &flag1, &cv1]() {
        count1+=1;
        {
            std::unique_lock<std::mutex> lk(mtx);
            flag1 = true;
            cv1.notify_one();
        }
    };

    auto t2f = [&count2, &mtx, &flag2, &cv2]() {
        count2+=1;
        {
            std::unique_lock<std::mutex> lk(mtx);
            flag2 = true;
            cv2.notify_one();
        }
    };

    TimerTaskWorker tw;
    TimerTask t1(1000);
    TimerTask t2(2500);

    t1.AddCallback(t1f);
    t2.AddCallback(t2f);

    ASSERT_TRUE(tw.AddTimerTask(&t1));
    ASSERT_FALSE(tw.AddTimerTask(&t1));
    ASSERT_TRUE(tw.AddTimerTask(&t2));

    tw.Start();

    {
        std::unique_lock<std::mutex> lk(mtx);
        cv1.wait(lk, [&flag1](){
            return flag1.load();
        });
        ASSERT_EQ(1, count1);
        ASSERT_EQ(0, count2);
    }

    {
        std::unique_lock<std::mutex> lk(mtx);
        cv2.wait(lk, [&flag2](){
            return flag2.load();
        });
        ASSERT_EQ(2, count1);
        ASSERT_EQ(1, count2);
    }

    ASSERT_TRUE(tw.CancelTimerTask(&t1));
    ASSERT_FALSE(tw.CancelTimerTask(&t1));
    ASSERT_TRUE(tw.CancelTimerTask(&t2));

    ASSERT_FALSE(tw.HasTimerTask(&t1));
    ASSERT_FALSE(tw.HasTimerTask(&t2));
    tw.Stop();
}
