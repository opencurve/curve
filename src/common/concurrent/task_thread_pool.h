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

#ifndef SRC_COMMON_CONCURRENT_TASK_THREAD_POOL_H_
#define SRC_COMMON_CONCURRENT_TASK_THREAD_POOL_H_

#include <functional>
#include <condition_variable>   //NOLINT
#include <deque>
#include <vector>
#include <mutex>                //NOLINT
#include <atomic>
#include <thread>               //NOLINT
#include <climits>
#include <iostream>
#include <memory>
#include <utility>

#include "src/common/uncopyable.h"

namespace curve {
namespace common {


using Task = std::function<void()>;

// Thread pool for asynchronously running callbacks
template <typename MutexT = std::mutex,
          typename CondVarT = std::condition_variable>
class TaskThreadPool : public Uncopyable {
 public:
    TaskThreadPool()
        : mutex_(), notEmpty_(), notFull_(), capacity_(-1), running_(false) {}

    virtual ~TaskThreadPool() {
        if (running_.load(std::memory_order_acquire)) {
            Stop();
        }
    }

    /**
     * Start a thread pool
     * @param numThreads The number of threads in the thread pool must be greater than 0, otherwise it is INT_ MAX (not recommended)
     * @param queueCapacity The capacity of queue must be greater than 0
     * @return
     */
    int Start(int numThreads, int queueCapacity = INT_MAX) {
        if (0 >= queueCapacity) {
            return -1;
        }
        capacity_ = queueCapacity;

        if (0 >= numThreads) {
            return -1;
        }

        if (!running_.exchange(true, std::memory_order_acq_rel)) {
            threads_.clear();
            threads_.reserve(numThreads);
            for (int i = 0; i < numThreads; ++i) {
                threads_.emplace_back(new std::thread(
                    std::bind(&TaskThreadPool::ThreadFunc, this)));
            }
        }

        return 0;
    }

    /**
     * Close Thread Pool
     */
    void Stop() {
        if (running_.exchange(false, std::memory_order_acq_rel)) {
            {
                std::lock_guard<MutexT> guard(mutex_);
                notEmpty_.notify_all();
            }
            for (auto& thr : threads_) {
                thr->join();
            }
        }
    }

    /**
     * Push a task to the thread pool for processing. If the queue is full, the thread will block until the task is pushed in
     * It should be noted that users themselves need to ensure the effectiveness of the task. In addition, this TaskThreadPool
     * There is no provision for obtaining the return value of f, so if you need to obtain some additional information about running f, you need the user to
     * Add your own internal logic to f
     * @tparam F
     * @tparam Args
     * @param f
     * @param args
     */
    template <class F, class... Args>
    void Enqueue(F&& f, Args&&... args) {
        std::unique_lock<MutexT> guard(mutex_);
        while (IsFullUnlock()) {
            notFull_.wait(guard);
        }
        auto task = std::bind(std::forward<F>(f), std::forward<Args>(args)...);
        queue_.push_back(std::move(task));
        notEmpty_.notify_one();
    }

    /*Returns the capacity of the thread pool queue*/
    int QueueCapacity() const {
        return capacity_;
    }

    /*Returns the number of tasks in the current queue of the thread pool, thread safe*/
    int QueueSize() const {
        std::lock_guard<MutexT> guard(mutex_);
        return queue_.size();
    }

    /*Returns the number of threads in the thread pool*/
    int ThreadOfNums() const {
        return threads_.size();
    }

 protected:
    /*Functions executed during thread work*/
    virtual void ThreadFunc() {
        while (running_.load(std::memory_order_acquire)) {
            Task task(Take());
            /*When ThreadPool exits, if the queue is empty, an invalid task will be returned*/
            if (task) {
                task();
            }
        }
    }

    /*Determine if the thread pool queue is full, non thread safe, private internal use*/
    bool IsFullUnlock() const {
        return queue_.size() >= static_cast<size_t>(capacity_);
    }

    /*Taking a task from the queue in the thread pool is thread safe*/
    Task Take() {
        std::unique_lock<MutexT> guard(mutex_);
        while (queue_.empty() && running_.load(std::memory_order_acquire)) {
            notEmpty_.wait(guard);
        }
        Task task;
        if (!queue_.empty()) {
            task = std::move(queue_.front());
            queue_.pop_front();
            notFull_.notify_one();
        }
        return task;
    }

 protected:
    mutable MutexT      mutex_;
    CondVarT notEmpty_;
    CondVarT notFull_;
    std::vector<std::unique_ptr<std::thread>> threads_;
    std::deque<Task>        queue_;
    int                     capacity_;
    std::atomic<bool>       running_;
};

}  // namespace common
}  // namespace curve

#endif  // SRC_COMMON_CONCURRENT_TASK_THREAD_POOL_H_
