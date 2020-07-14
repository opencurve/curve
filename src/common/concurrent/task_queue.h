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
 * File Created: Tuesday, 18th December 2018 4:52:44 pm
 * Author: tongguangxun
 */

#ifndef SRC_COMMON_CONCURRENT_TASK_QUEUE_H_
#define SRC_COMMON_CONCURRENT_TASK_QUEUE_H_

#include <future>       // NOLINT
#include <queue>        // NOLINT
#include <mutex>        // NOLINT
#include <functional>   // NOLINT
#include <utility>

namespace curve {
namespace common {

class TaskQueue {
 public:
    using Task = std::function<void()>;
    explicit TaskQueue(size_t capacity): capacity_(capacity) {
    }

    ~TaskQueue() = default;

    template<class F, class... Args>
    void Push(F&& f, Args&&... args) {
        auto task = std::bind(std::forward<F>(f), std::forward<Args>(args)...);
        std::unique_lock<std::mutex> lk(mtx_);
        notfullcv_.wait(lk, [this]()->bool{return this->tasks_.size() < this->capacity_;});     // NOLINT
        tasks_.push(task);
        notemptycv_.notify_one();
    };                                                                                          // NOLINT

    Task Pop() {
        std::unique_lock<std::mutex> lk(mtx_);
        notemptycv_.wait(lk, [this]()->bool{return this->tasks_.size() > 0;});                  // NOLINT
        Task t = tasks_.front();
        tasks_.pop();
        notfullcv_.notify_one();
        return t;
    }

 private:
    size_t capacity_;
    std::mutex  mtx_;
    std::condition_variable notemptycv_;
    std::condition_variable notfullcv_;
    std::queue< Task > tasks_;
};

}   // namespace common
}   // namespace curve
#endif  // SRC_COMMON_CONCURRENT_TASK_QUEUE_H_
