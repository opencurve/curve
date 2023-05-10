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
template <typename MutexT, typename CondVarT>
class GenericTaskQueue {
 public:
    using Task = std::function<void()>;
    explicit GenericTaskQueue(size_t capacity) : capacity_(capacity) {}
    template <class F, class... Args>
    void Push(F&& f, Args&&... args) {
        auto task = std::bind(std::forward<F>(f), std::forward<Args>(args)...);
        {
            std::unique_lock<MutexT> lk(mtx_);
            while (tasks_.size() >= capacity_) {
                notfullcv_.wait(lk);
            }
            tasks_.push(std::move(task));
        }
        notemptycv_.notify_one();
    }
    Task Pop() {
        std::unique_lock<MutexT> lk(mtx_);
        while (tasks_.empty()) {
            notemptycv_.wait(lk);
        }
        Task t = std::move(tasks_.front());
        tasks_.pop();
        notfullcv_.notify_one();
        return t;
    }

    size_t Size() {
        std::unique_lock<MutexT> lk(mtx_);
        return tasks_.size();
    }

 private:
    size_t capacity_;
    MutexT mtx_;
    CondVarT notemptycv_;
    CondVarT notfullcv_;
    std::queue< Task > tasks_;
};
using TaskQueue = GenericTaskQueue<std::mutex, std::condition_variable>;
}   // namespace common
}   // namespace curve
#endif  // SRC_COMMON_CONCURRENT_TASK_QUEUE_H_