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
 * Created Date: Fri Dec 14 2018
 * Author: xuchaojie
 */

#ifndef SRC_SNAPSHOTCLONESERVER_SNAPSHOT_SNAPSHOT_TASK_MANAGER_H_
#define SRC_SNAPSHOTCLONESERVER_SNAPSHOT_SNAPSHOT_TASK_MANAGER_H_

#include <memory>
#include <map>
#include <atomic>
#include <string>
#include <list>
#include <thread>  // NOLINT

#include "src/snapshotcloneserver/snapshot/snapshot_task.h"
#include "src/snapshotcloneserver/common/thread_pool.h"
#include "src/common/concurrent/rw_lock.h"
#include "src/common/snapshotclone/snapshotclone_define.h"
#include "src/snapshotcloneserver/common/config.h"
#include "src/snapshotcloneserver/common/snapshotclone_metric.h"
#include "src/snapshotcloneserver/snapshot/snapshot_core.h"

using ::curve::common::RWLock;
using ::curve::common::ReadLockGuard;
using ::curve::common::WriteLockGuard;
using ::curve::common::Mutex;

namespace curve {
namespace snapshotcloneserver {

/**
 * @brief Snapshot Task Manager Class
 */
class SnapshotTaskManager {
 public:
     /**
      * @brief default constructor
      */
    SnapshotTaskManager(
        std::shared_ptr<SnapshotCore> core,
        std::shared_ptr<SnapshotMetric> snapshotMetric)
        : isStop_(true),
          core_(core),
          snapshotMetric_(snapshotMetric),
          snapshotTaskManagerScanIntervalMs_(0) {}

    /**
     * @brief destructor
     */
    ~SnapshotTaskManager() {
        Stop();
    }

    int Init(std::shared_ptr<ThreadPool> pool,
        const SnapshotCloneServerOptions &option) {
        snapshotTaskManagerScanIntervalMs_ =
            option.snapshotTaskManagerScanIntervalMs;
        threadpool_ = pool;
        return kErrCodeSuccess;
    }

    /**
     * @brief start
     *
     * @return error code
     */
    int Start();

    /**
     * @brief Stop service
     *
     */
    void Stop();

    /**
     * @brief Add Task
     *
     * @param task snapshot task
     *
     * @return error code
     */
    int PushTask(std::shared_ptr<SnapshotTask> task);

    /**
     * @brief Get Task
     *
     * @param taskId Task ID
     *
     * @return Snapshot Task Pointer
     */
    std::shared_ptr<SnapshotTask> GetTask(const TaskIdType &taskId) const;

    /**
     * @brief Cancel Task
     *
     * @param taskId Task ID
     *
     * @return error code
     */
    int CancelTask(const TaskIdType &taskId);

 private:
    /**
     * @brief Background Thread Execution Function
     *
     *Regularly execute the scan wait queue function and scan work queue function.
     */
    void BackEndThreadFunc();
    /**
     * @brief Scan Waiting Task Queue Function
     *
     *Scan the waiting queue to determine the current file in the work queue
     *Are there any executing snapshots? If not, place them in the work queue
     *
     */
    void ScanWaitingTask();
    /**
     * @brief Scan Work Queue Function
     *
     *Scan the work queue to determine the current status in the work queue
     *Has the snapshot task been completed? If so, move it out of the work queue
     *
     */
    void ScanWorkingTask();

 private:
    //Backend Thread
    std::thread backEndThread;

    //Id ->Snapshot Task Table
    std::map<TaskIdType, std::shared_ptr<SnapshotTask> > taskMap_;
    mutable RWLock taskMapLock_;

    //Snapshot waiting queue
    std::list<std::shared_ptr<SnapshotTask> > waitingTasks_;
    mutable Mutex waitingTasksLock_;

    //The snapshot work queue is actually a map, where key is the file name for easy query
    std::map<std::string, std::shared_ptr<SnapshotTask> > workingTasks_;
    mutable Mutex workingTasksLock_;

    std::shared_ptr<ThreadPool> threadpool_;

    //Is the current task management stopped? Used to support start and stop functions
    std::atomic_bool isStop_;

    // snapshot core
    std::shared_ptr<SnapshotCore> core_;

    // metric
    std::shared_ptr<SnapshotMetric> snapshotMetric_;

    //Scanning cycle of snapshot background thread scanning waiting queue and work queue (unit: ms)
    int snapshotTaskManagerScanIntervalMs_;
};

}  // namespace snapshotcloneserver
}  // namespace curve

#endif  // SRC_SNAPSHOTCLONESERVER_SNAPSHOT_SNAPSHOT_TASK_MANAGER_H_
