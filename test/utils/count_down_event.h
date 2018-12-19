/*
 * Project: curve
 * Created Date: 18-12-18
 * Author: wudemiao
 * Copyright (c) 2018 netease
 */

#ifndef CURVE_COUNT_DOWN_EVENT_H
#define CURVE_COUNT_DOWN_EVENT_H

#include <mutex>                //NOLINT
#include <condition_variable>   //NOLINT

namespace curve {
namespace test {

/**
 * 同 Java 的 CountDownLatch，用于线程间同步，CountDownEvent 是通过一个计数器来
 * 实现的，计数器的初始值 initCnt 为需要等待 event 的总数，通过接口 Wait 等待。每
 * 当一个 event 发生，就会调用 Signal 接口，让计数器的值就会减 1。当计数器值到达 0
 * 时，则 Wait 等待就会结束。一般用于等待一些事件发生
 */
class CountDownEvent {
 public:
    explicit CountDownEvent(int initCnt) :
        mutex_(),
        cond_(),
        count_(initCnt) {
    }

    /**
     * 通知 wait event 发生了一次，计数减 1
     */
    void Signal() {
        std::unique_lock<std::mutex> guard(mutex_);
        --count_;
        if (count_ <= 0) {
            cond_.notify_all();
        }
    }

    /**
     * 等待 initCnt 的 event 发生之后，在唤醒
     */
    void Wait() {
        std::unique_lock<std::mutex> guard(mutex_);
        while (count_ > 0) {
            cond_.wait(guard);
        }
    }

 private:
    mutable std::mutex      mutex_;
    std::condition_variable cond_;
    int                     count_;
};

}  // namespace test
}  // namespace curve

#endif  // CURVE_COUNT_DOWN_EVENT_H
