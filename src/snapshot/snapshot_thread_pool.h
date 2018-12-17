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

class SnapshotThreadPool {
 public:
    explicit SnapshotThreadPool(int threadNum)
        : threadNum_(threadNum),
         isStop_(true) {}
    void Start();
    void Stop();

    void PushTask(SnapshotTask *task) {
        threadPool_.Enqueue(task->clousre());
    }
 private:
    curve::common::TaskThreadPool threadPool_;
    int threadNum_;
    bool isStop_;
};

}  // namespace snapshotserver
}  // namespace curve

#endif  // CURVE_SRC_SNAPSHOT_SNAPSHOT_THREAD_POOL_H_
