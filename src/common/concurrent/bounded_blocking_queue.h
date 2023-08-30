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
 * Created Date: 18-9-26
 * Author: wudemiao
 */

#ifndef SRC_COMMON_CONCURRENT_BOUNDED_BLOCKING_QUEUE_H_
#define SRC_COMMON_CONCURRENT_BOUNDED_BLOCKING_QUEUE_H_

#include <cassert>
#include <cstdio>
#include <condition_variable>   //NOLINT
#include <deque>
#include <mutex>                //NOLINT
#include <atomic>
#include <utility>

#include "src/common/uncopyable.h"

namespace curve {
namespace common {

template<typename T>
class BBQItem {
 public:
    explicit BBQItem(const T &t, bool stop = false)
        : item_(t) {
        stop_.store(stop, std::memory_order_release);
    }
    BBQItem(const BBQItem &bbqItem) {
        item_ = bbqItem.item_;
        stop_.store(bbqItem.stop_, std::memory_order_release);
    }
    BBQItem &operator=(const BBQItem &bbqItem) {
        if (&bbqItem == this) {
            return *this;
        }
        item_ = bbqItem.item_;
        stop_.store(bbqItem.stop_, std::memory_order_release);
        return *this;
    }

    bool IsStop() const {
        return stop_.load(std::memory_order_acquire);
    }

    T Item() {
        return item_;
    }

 private:
    T item_;
    std::atomic<bool> stop_;
};

/**
 *Blocking queues with capacity restrictions, thread safe
 */
template<typename T>
class BoundedBlockingDeque : public Uncopyable {
 public:
    BoundedBlockingDeque()
        : mutex_(),
          notEmpty_(),
          notFull_(),
          deque_(),
          capacity_(0) {
    }

    int Init(const int capacity) {
        if (0 >= capacity) {
            return -1;
        }
        capacity_ = capacity;
        return 0;
    }

    void PutBack(const T &x) {
        std::unique_lock<std::mutex> guard(mutex_);
        while (deque_.size() == capacity_) {
            notFull_.wait(guard);
        }
        deque_.push_back(x);
        notEmpty_.notify_one();
    }

    void PutFront(const T &x) {
        std::unique_lock<std::mutex> guard(mutex_);
        while (deque_.size() == capacity_) {
            notFull_.wait(guard);
        }
        deque_.push_front(x);
        notEmpty_.notify_one();
    }

    T TakeFront() {
        std::unique_lock<std::mutex> guard(mutex_);
        while (deque_.empty()) {
            notEmpty_.wait(guard);
        }
        T front(std::move(deque_.front()));
        deque_.pop_front();
        notFull_.notify_one();
        return front;
    }

    T TakeBack() {
        std::unique_lock<std::mutex> guard(mutex_);
        while (deque_.empty()) {
            notEmpty_.wait(guard);
        }
        T back(std::move(deque_.back()));
        deque_.pop_back();
        notFull_.notify_one();
        return back;
    }

    bool Empty() const {
        std::lock_guard<std::mutex> guard(mutex_);
        return deque_.empty();
    }

    bool Full() const {
        std::lock_guard<std::mutex> guard(mutex_);
        return deque_.size() == capacity_;
    }

    size_t Size() const {
        std::lock_guard<std::mutex> guard(mutex_);
        return deque_.size();
    }

    size_t Capacity() const {
        std::lock_guard<std::mutex> guard(mutex_);
        return capacity_;
    }

 private:
    mutable std::mutex mutex_;
    std::condition_variable notEmpty_;
    std::condition_variable notFull_;
    std::deque<T> deque_;
    size_t capacity_;
};

}  // namespace common
}  // namespace curve

#endif  // SRC_COMMON_CONCURRENT_BOUNDED_BLOCKING_QUEUE_H_
