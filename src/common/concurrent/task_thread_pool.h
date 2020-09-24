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

// 异步运行回调的线程池
template <typename MutexT = std::mutex,
          typename CondVarT = std::condition_variable>
class TaskThreadPool : public Uncopyable {
 public:
    using Task = std::function<void()>;

    TaskThreadPool()
        : mutex_(), notEmpty_(), notFull_(), capacity_(-1), running_(false) {}

    virtual ~TaskThreadPool() {
        if (running_.load(std::memory_order_acquire)) {
            Stop();
        }
    }

    /**
     * 启动一个线程池
     * @param numThreads 线程池的线程数量，必须大于 0，不设置就是 INT_MAX (不推荐)
     * @param queueCapacity queue 的容量，必须大于 0
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
            threads_.reserve(numThreads);
            for (int i = 0; i < numThreads; ++i) {
                threads_.emplace_back(new std::thread(
                    std::bind(&TaskThreadPool::ThreadFunc, this)));
            }
        }

        return 0;
    }

    /**
     * 关闭线程池
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
     * push 一个 task 给线程池处理，如果队列满，线程阻塞，直到 task push 进去
     * 需要注意的是用户自己需要保证 task 的有效的。除此之外，此 TaskThreadPool
     * 并没有提供获取 f 的返回值，所以如果需要获取运行 f 的一些额外信息，需要用户
     * 自己在 f 内部逻辑添加
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

    /* 返回线程池 queue 的容量 */
    int QueueCapacity() const {
        return capacity_;
    }

    /* 返回线程池当前 queue 中的 task 数量，线程安全 */
    int QueueSize() const {
        std::lock_guard<MutexT> guard(mutex_);
        return queue_.size();
    }

    /* 返回线程池的线程数 */
    int ThreadOfNums() const {
        return threads_.size();
    }

 protected:
    /*线程工作时执行的函数*/
    virtual void ThreadFunc() {
        while (running_.load(std::memory_order_acquire)) {
            Task task(Take());
            /* ThreadPool 退出的时候，queue 为空，那么会返回无效的 task */
            if (task) {
                task();
            }
        }
    }

    /* 判断线程池 queue 是否已经满了, 非线程安全，私有内部使用 */
    bool IsFullUnlock() const {
        return queue_.size() >= capacity_;
    }

    /* 从线程池的 queue 中取一个 task 线程安全 */
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
