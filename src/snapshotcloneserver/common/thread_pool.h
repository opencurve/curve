/*
 * Project: curve
 * Created Date: Wed Dec 12 2018
 * Author: xuchaojie
 * Copyright (c) 2018 netease
 */

#ifndef SRC_SNAPSHOTCLONESERVER_COMMON_THREAD_POOL_H_
#define SRC_SNAPSHOTCLONESERVER_COMMON_THREAD_POOL_H_

#include <memory>
#include "src/common/concurrent/task_thread_pool.h"
#include "src/snapshotcloneserver/common/task.h"

namespace curve {
namespace snapshotcloneserver {

/**
 * @brief 快照线程池
 */
class ThreadPool {
 public:
     /**
      * @brief 构造函数
      *
      * @param threadNum 最大线程数
      */
    explicit ThreadPool(int threadNum)
        : threadNum_(threadNum) {}
    /**
     * @brief 启动线程池
     */
    int Start();

    /**
     * @brief 停止线程池
     */
    void Stop();

    /**
     * @brief 添加快照任务
     *
     * @param task 快照任务
     */
    void PushTask(std::shared_ptr<Task> task) {
        threadPool_.Enqueue(task->clousre());
    }

    /**
     * @brief 添加快照任务
     *
     * @param task 快照任务
     */
    void PushTask(Task* task) {
        threadPool_.Enqueue(task->clousre());
    }

 private:
    /**
     * @brief 通用线程池
     */
    curve::common::TaskThreadPool threadPool_;
    /**
     * @brief 线程数
     */
    int threadNum_;
};

}  // namespace snapshotcloneserver
}  // namespace curve

#endif  // SRC_SNAPSHOTCLONESERVER_COMMON_THREAD_POOL_H_
