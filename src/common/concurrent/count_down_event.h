/*
 * Project: curve
 * Created Date: 18-03-15
 * Author: wudemiao
 * Copyright (c) 2018 netease
 */

#ifndef CURVE_COUNT_DOWN_EVENT_H
#define CURVE_COUNT_DOWN_EVENT_H

#include <mutex>                //NOLINT
#include <condition_variable>   //NOLINT

namespace curve {
namespace common {

/**
 * 用于线程间同步，CountDownEvent是通过一个计数器来实现的，计数器的
 * 初始值initCnt为需要等待event的总数，通过接口Wait等待。每当一个
 * event发生，就会调用Signal接口，让计数器的值就会减 1。当计数器值到
 * 达0时，则Wait等待就会结束。一般用于等待一些事件发生
 */
class CountDownEvent {
 public:
    CountDownEvent() :
        mutex_(),
        cond_(),
        count_() {
    }

    explicit CountDownEvent(int initCnt) :
        mutex_(),
        cond_(),
        count_(initCnt) {
    }

    /**
     * 重新设置event计数
     * @param eventCount:事件计数
     */
    void Reset(int eventCount) {
        std::unique_lock<std::mutex> guard(mutex_);
        count_ = eventCount;
    }

    /**
     * 通知wait event发生了一次，计数减1
     */
    void Signal() {
        std::unique_lock<std::mutex> guard(mutex_);
        --count_;
        if (count_ <= 0) {
            cond_.notify_all();
        }
    }

    /**
     * 等待initCnt的event发生之后，再唤醒
     */
    void Wait() {
        std::unique_lock<std::mutex> guard(mutex_);
        while (count_ > 0) {
            cond_.wait(guard);
        }
    }

    /**
     * 等待initCnt的event发生，或者指定时长
     * @param elapsedMs: 等待的ms数
     * @return：如果所有等待的event都发生，那么就返回true，否则false
     */
     bool WaitFor(int elapsedMs) {
        std::unique_lock<std::mutex> guard(mutex_);
        if (count_ > 0) {
            cond_.wait_for(guard, std::chrono::microseconds(elapsedMs));
        }

        if (count_ > 0) {
            return false;
        } else {
            return true;
        }
     }

 private:
    mutable std::mutex      mutex_;
    std::condition_variable cond_;
    // 需要等待的事件计数
    int                     count_;
};

}  // namespace common
}  // namespace curve

#endif  // CURVE_COUNT_DOWN_EVENT_H
