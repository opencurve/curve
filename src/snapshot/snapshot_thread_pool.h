/*
 * Project: curve
 * Created Date: Wed Dec 12 2018
 * Author: xuchaojie
 * Copyright (c) 2018 netease
 */

#ifndef CURVE_SRC_SNAPSHOT_SNAPSHOT_THREAD_POOL_H_
#define CURVE_SRC_SNAPSHOT_SNAPSHOT_THREAD_POOL_H_

#include "src/common/task_thread_pool.h"
#include "src/snapshot/snapshot_task.h"

namespace curve {
namespace snapshotserver {

/**
 * @brief 快照线程池
 */
class SnapshotThreadPool {
 public:
     /**
      * @brief 构造函数
      *
      * @param threadNum 最大线程数
      */
    explicit SnapshotThreadPool(int threadNum)
        : threadNum_(threadNum) {}
    /**
     * @brief 启动线程池
     */
    void Start();

    /**
     * @brief 停止线程池
     */
    void Stop();

    /**
     * @brief 添加快照任务
     *
     * @param task 快照任务
     */
    void PushTask(SnapshotTask *task) {
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

}  // namespace snapshotserver
}  // namespace curve

#endif  // CURVE_SRC_SNAPSHOT_SNAPSHOT_THREAD_POOL_H_
