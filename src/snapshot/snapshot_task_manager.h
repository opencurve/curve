/*
 * Project: curve
 * Created Date: Fri Dec 14 2018
 * Author: xuchaojie
 * Copyright (c) 2018 netease
 */

#ifndef CURVE_SRC_SNAPSHOT_SNAPSHOT_TASK_MANAGER_H_
#define CURVE_SRC_SNAPSHOT_SNAPSHOT_TASK_MANAGER_H_

#include <memory>
#include <map>
#include <atomic>
#include <string>
#include <list>
#include <thread>  // NOLINT

#include "src/snapshot/snapshot_task.h"
#include "src/snapshot/snapshot_thread_pool.h"
#include "src/common/rw_lock.h"
#include "src/snapshot/snapshot_define.h"

using ::curve::common::RWLock;
using ::curve::common::ReadLockGuard;
using ::curve::common::WriteLockGuard;

namespace curve {
namespace snapshotserver {

const uint32_t kWaitingTaskScanTimeMs = 1000;

class SnapshotTaskManager {
 public:
    SnapshotTaskManager()
        : isStop(true) {}
    ~SnapshotTaskManager() {
        Stop();
    }

    int Init(uint32_t maxThreadNum);

    // move task from waiting queue to work map and thread pool.
    int Start();

    int Stop();

    // push task into waiting queue.
    int PushTask(std::shared_ptr<SnapshotTask> task);

    std::shared_ptr<SnapshotTask> GetTask(const TaskIdType &taskId) const;

    int CancelTask(const TaskIdType &taskId);

 private:
    void BackEndThreadFunc();
    void ScanWaitingTask();
    void ScanWorkingTask();

 private:
    std::thread backEndThread;

    std::map<TaskIdType, std::shared_ptr<SnapshotTask> > taskMap_;
    mutable RWLock taskMapLock_;

    std::list<std::shared_ptr<SnapshotTask> > waitingTasks_;
    mutable std::mutex waitingTasksLock_;

    // key is filename
    std::map<std::string, std::shared_ptr<SnapshotTask> > workingTasks_;
    mutable std::mutex workingTasksLock_;

    std::shared_ptr<SnapshotThreadPool> threadpool_;
    std::atomic_bool isStop;
};

}  // namespace snapshotserver
}  // namespace curve

#endif  // CURVE_SRC_SNAPSHOT_SNAPSHOT_TASK_MANAGER_H_
