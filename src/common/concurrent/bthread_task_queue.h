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
 * File Created: 2020/08/21
 * Author: hzchenwei7
 */

#ifndef SRC_COMMON_CONCURRENT_BTHREAD_TASK_QUEUE_H_
#define SRC_COMMON_CONCURRENT_BTHREAD_TASK_QUEUE_H_

#include <future>       // NOLINT
#include <queue>        // NOLINT
#include <utility>
#include "bthread/mutex.h"
#include "bthread/condition_variable.h"


namespace curve {
namespace common {

class BthreadTaskQueue {
 public:
    using Task = std::function<void()>;
    explicit BthreadTaskQueue(size_t capacity): capacity_(capacity) {
    }

    ~BthreadTaskQueue() = default;

    template<class F, class... Args>
    void Push(F&& f, Args&&... args) {
        auto task = std::bind(std::forward<F>(f), std::forward<Args>(args)...);
        std::unique_lock<bthread::Mutex> lk(mtx_);
        while (this->tasks_.size() >= this->capacity_) {
            notemptycv_.wait(lk);
        }
        tasks_.push(task);
        notemptycv_.notify_one();
    }

    Task Pop() {
        std::unique_lock<bthread::Mutex> lk(mtx_);
        while (this->tasks_.size() <= 0) {
            notemptycv_.wait(lk);
        }

        Task t = tasks_.front();
        tasks_.pop();
        notfullcv_.notify_one();
        return t;
    }

 private:
    size_t capacity_;
    bthread::Mutex mtx_;
    bthread::ConditionVariable notemptycv_;
    bthread::ConditionVariable notfullcv_;
    std::queue< Task > tasks_;
};

}   // namespace common
}   // namespace curve
#endif  // SRC_COMMON_CONCURRENT_BTHREAD_TASK_QUEUE_H_
