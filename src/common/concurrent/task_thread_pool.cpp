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

#include "src/common/concurrent/task_thread_pool.h"

namespace curve {
namespace common {

TaskThreadPool::TaskThreadPool()
    : mutex_(),
      notEmpty_(),
      notFull_(),
      capacity_(-1),
      running_(false) {
}

TaskThreadPool::~TaskThreadPool() {
    if (running_.load(std::memory_order_acquire)) {
        Stop();
    }
}

int TaskThreadPool::QueueCapacity() const {
    return capacity_;
}

int TaskThreadPool::ThreadOfNums() const {
    return threads_.size();
}

int TaskThreadPool::QueueSize() const {
    std::unique_lock<std::mutex> guard(mutex_);
    return queue_.size();
}

int TaskThreadPool::Start(int numThreads, int queueCapacity) {
    if (0 >= queueCapacity) {
        return -1;
    }
    capacity_ = queueCapacity;

    if (0 >= numThreads) {
        return -1;
    }

    if (!running_.exchange(true, std::memory_order_acq_rel)) {
        threads_.reserve(numThreads);
        for (int i = 0; i < numThreads; ++i) {
            threads_.emplace_back(new std::thread(std::bind(&TaskThreadPool::ThreadFunc,  // NOLINT
                                                            this)));
        }
    }

    return 0;
}

void TaskThreadPool::Stop() {
    if (running_.exchange(false, std::memory_order_acq_rel)) {
        {
            std::lock_guard<std::mutex> guard(mutex_);
            notEmpty_.notify_all();
        }
        for (auto &thr : threads_) {
            thr->join();
        }
    }
}

TaskThreadPool::Task TaskThreadPool::Take() {
    std::unique_lock<std::mutex> guard(mutex_);
    while (queue_.empty() && running_.load(std::memory_order_acquire)) {
        notEmpty_.wait(guard);
    }
    Task task;
    if (!queue_.empty()) {
        task = queue_.front();
        queue_.pop_front();
        notFull_.notify_one();
    }
    return task;
}

void TaskThreadPool::ThreadFunc() {
    while (running_.load(std::memory_order_acquire)) {
        Task task(Take());
        /* ThreadPool 退出的时候，queue 为空，那么会返回无效的 task */
        if (task) {
            task();
        }
    }
}

bool TaskThreadPool::IsFullUnlock() const {
    return queue_.size() >= capacity_;
}

}  // namespace common
}  // namespace curve
