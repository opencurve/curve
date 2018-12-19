/*
 * Project: curve
 * Created Date: 18-12-14
 * Author: wudemiao
 * Copyright (c) 2018 netease
 */

#ifndef CURVE_COMMON_TASK_THREAD_POOL_H
#define CURVE_COMMON_TASK_THREAD_POOL_H

#include <functional>
#include <condition_variable>   //NOLINT
#include <deque>
#include <vector>
#include <mutex>                //NOLINT
#include <atomic>
#include <thread>               //NOLINT
#include <climits>
#include <iostream>

#include "src/common/uncopyable.h"

namespace curve {
namespace common {

// 异步运行回调的线程池
class TaskThreadPool : public Uncopyable {
 public:
    using Task = std::function<void()>;

    TaskThreadPool();
    ~TaskThreadPool();

    /**
     * 启动一个线程池
     * @param numThreads 线程池的线程数量，必须大于 0，不设置就是 INT_MAX (不推荐)
     * @param queueCapacity queue 的容量，必须大于 0
     * @return
     */
    int Start(int numThreads, int queueCapacity = INT_MAX);
    /**
     * 关闭线程池
     */
    void Stop();
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
    template<class F, class... Args>
    void Enqueue(F &&f, Args &&... args);

    /* 返回线程池 queue 的容量 */
    int QueueCapacity() const;
    /* 返回线程池当前 queue 中的 task 数量，线程安全 */
    int QueueSize() const;
    /* 返回线程池的线程数 */
    int ThreadOfNums() const;

 private:
    void ThreadFunc();
    /* 判断线程池 queue 是否已经满了, 非线程安全，私有内部使用 */
    bool IsFullUnlock() const;
    /* 从线程池的 queue 中取一个 task 线程安全 */
    Task Take();

 private:
    mutable std::mutex      mutex_;
    std::condition_variable notEmpty_;
    std::condition_variable notFull_;
    std::vector<std::unique_ptr<std::thread>> threads_;
    std::deque<Task>        queue_;
    int                     capacity_;
    std::atomic<bool>       running_;
};

template<class F, class... Args>
void TaskThreadPool::Enqueue(F &&f, Args &&... args) {
    std::unique_lock<std::mutex> guard(mutex_);
    while (IsFullUnlock()) {
        notFull_.wait(guard);
    }
    auto task = std::bind(std::forward<F>(f), std::forward<Args>(args)...);
    queue_.push_back(std::move(task));
    notEmpty_.notify_one();
}

}  // namespace common
}  // namespace curve

#endif  // CURVE_COMMON_TASK_THREAD_POOL_H
