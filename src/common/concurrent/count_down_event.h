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
 * Created Date: 18-03-15
 * Author: wudemiao
 */

#ifndef SRC_COMMON_CONCURRENT_COUNT_DOWN_EVENT_H_
#define SRC_COMMON_CONCURRENT_COUNT_DOWN_EVENT_H_

#include <mutex>                //NOLINT
#include <condition_variable>   //NOLINT
#include <chrono>               //NOLINT

namespace curve {
namespace common {

/**
 * Used for inter-thread synchronization, CountDownEvent is implemented using a counter
 * with an initial value (initCnt) representing the total number of events to wait for.
 * Threads can wait for events using the Wait interface. Each time an event occurs, the Signal interface is called, 
 * decrementing the counter by 1. When the counter reaches 0, the waiting in Wait will conclude. It is typically used to wait for certain events to occur.
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
     * Reset event count
     * @param eventCount: Event Count
     */
    void Reset(int eventCount) {
        std::unique_lock<std::mutex> guard(mutex_);
        count_ = eventCount;
    }

    /**
     * Notify that a wait event has occurred once, count minus 1
     */
    void Signal() {
        std::unique_lock<std::mutex> guard(mutex_);
        --count_;
        if (count_ <= 0) {
            cond_.notify_all();
        }
    }

    /**
     * Wait for the event of initCnt to occur before waking up
     */
    void Wait() {
        std::unique_lock<std::mutex> guard(mutex_);
        while (count_ > 0) {
            cond_.wait(guard);
        }
    }

    /**
     * Wait for the event of initCnt to occur, or specify a duration
     * @param waitMs: Number of ms waiting
     * @return: If all waiting events occur, then return true; otherwise, false
     */
    bool WaitFor(int waitMs) {
        std::unique_lock<std::mutex> guard(mutex_);
        auto start = std::chrono::high_resolution_clock::now();

        while (count_ > 0) {
            auto now = std::chrono::high_resolution_clock::now();
            std::chrono::duration<double, std::milli> elapsed = now - start;
            // Calculate how much time is left
            int leftMs = waitMs - static_cast<int>(elapsed.count());
            if (leftMs > 0) {
                auto ret = cond_.wait_for(guard,
                                          std::chrono::milliseconds(leftMs));
                (void)ret;
            } else {
                break;
            }
        }

        if (count_ > 0) {
            return false;
        } else {
            return true;
        }
    }

 private:
    mutable std::mutex mutex_;
    std::condition_variable cond_;
    // Count of events to wait for
    int count_;
};

}  // namespace common
}  // namespace curve

#endif  // SRC_COMMON_CONCURRENT_COUNT_DOWN_EVENT_H_
