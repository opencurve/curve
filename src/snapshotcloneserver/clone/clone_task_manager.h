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
 * Created Date: Wed Mar 20 2019
 * Author: xuchaojie
 */

#ifndef SRC_SNAPSHOTCLONESERVER_CLONE_CLONE_TASK_MANAGER_H_
#define SRC_SNAPSHOTCLONESERVER_CLONE_CLONE_TASK_MANAGER_H_

#include <atomic>
#include <list>
#include <map>
#include <memory>
#include <string>
#include <thread>  // NOLINT

#include "src/common/concurrent/rw_lock.h"
#include "src/common/snapshotclone/snapshotclone_define.h"
#include "src/snapshotcloneserver/clone/clone_core.h"
#include "src/snapshotcloneserver/clone/clone_task.h"
#include "src/snapshotcloneserver/common/config.h"
#include "src/snapshotcloneserver/common/snapshotclone_metric.h"
#include "src/snapshotcloneserver/common/thread_pool.h"

using ::curve::common::LockGuard;
using ::curve::common::Mutex;
using ::curve::common::ReadLockGuard;
using ::curve::common::RWLock;
using ::curve::common::WriteLockGuard;

namespace curve {
namespace snapshotcloneserver {

class CloneTaskManager {
 public:
    explicit CloneTaskManager(std::shared_ptr<CloneCore> core,
                              std::shared_ptr<CloneMetric> cloneMetric)
        : isStop_(true),
          core_(core),
          cloneMetric_(cloneMetric),
          cloneTaskManagerScanIntervalMs_(0) {}

    ~CloneTaskManager() { Stop(); }

    int Init(std::shared_ptr<ThreadPool> stage1Pool,
             std::shared_ptr<ThreadPool> stage2Pool,
             std::shared_ptr<ThreadPool> commonPool,
             const SnapshotCloneServerOptions& option) {
        cloneTaskManagerScanIntervalMs_ = option.cloneTaskManagerScanIntervalMs;
        stage1Pool_ = stage1Pool;
        stage2Pool_ = stage2Pool;
        commonPool_ = commonPool;
        return kErrCodeSuccess;
    }

    int Start();

    void Stop();

    /**
     * @brief Add a task to the task manager
     *
     * Request for non Lazy clones and other deletion of control surfaces such
     * as clones
     *
     * @param task: task
     *
     * @return error code
     */
    int PushCommonTask(std::shared_ptr<CloneTaskBase> task);

    /**
     * @brief Add LazyClone Phase 1 tasks to the task manager
     *
     * @param task: task
     *
     * @return error code
     */
    int PushStage1Task(std::shared_ptr<CloneTaskBase> task);

    /**
     * @brief: Add LazyClone Phase 2 tasks to the task manager
     *
     * At present, it is only used for adding tasks from the Lazy clone recovery
     * clone data stage to the task manager during restart recovery
     *
     * @param task: task
     *
     * @return error code
     */
    int PushStage2Task(std::shared_ptr<CloneTaskBase> task);

    std::shared_ptr<CloneTaskBase> GetTask(const TaskIdType& taskId) const;

 private:
    void BackEndThreadFunc();
    void ScanCommonTasks();
    void ScanStage1Tasks();
    void ScanStage2Tasks();

    /**
     * @brief pushes tasks to the corresponding thread pool and map
     *
     * @param task: Task
     * @param taskMap: Task table
     * @param taskMapMutex: Task table and thread pool locks
     * @param taskPool: Thread Pool
     *
     * @return error code
     */
    int PushTaskInternal(
        std::shared_ptr<CloneTaskBase> task,
        std::map<std::string, std::shared_ptr<CloneTaskBase> >* taskMap,
        Mutex* taskMapMutex, std::shared_ptr<ThreadPool> taskPool);

 private:
    // Backend Thread
    std::thread backEndThread;

    // ID -> Clone Task Table
    std::map<TaskIdType, std::shared_ptr<CloneTaskBase> > cloneTaskMap_;
    mutable RWLock cloneTaskMapLock_;

    // Storing stage1Pool_ The current task of the pool, with key as destination
    std::map<std::string, std::shared_ptr<CloneTaskBase> > stage1TaskMap_;
    mutable Mutex stage1TasksLock_;

    // Storage stage1Poo2_ The current task of the pool, with key as destination
    std::map<std::string, std::shared_ptr<CloneTaskBase> > stage2TaskMap_;
    mutable Mutex stage2TasksLock_;

    // Store commonPool_ Current task of the pool
    std::map<std::string, std::shared_ptr<CloneTaskBase> > commonTaskMap_;
    mutable Mutex commonTasksLock_;

    // Thread pool for Lazy clone metadata section
    std::shared_ptr<ThreadPool> stage1Pool_;

    // Thread pool for Lazy clone data section
    std::shared_ptr<ThreadPool> stage2Pool_;

    // Thread pool for requests for non Lazy clones and deletion of clones and
    // other control surfaces
    std::shared_ptr<ThreadPool> commonPool_;

    // Is the current task management stopped? Used to support start and stop
    // functions
    std::atomic_bool isStop_;

    // clone core
    std::shared_ptr<CloneCore> core_;

    // metric
    std::shared_ptr<CloneMetric> cloneMetric_;

    // CloneTaskManager backend thread scan interval
    uint32_t cloneTaskManagerScanIntervalMs_;
};

}  // namespace snapshotcloneserver
}  // namespace curve

#endif  // SRC_SNAPSHOTCLONESERVER_CLONE_CLONE_TASK_MANAGER_H_
