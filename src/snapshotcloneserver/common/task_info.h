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
 * Created Date: Thu Mar 21 2019
 * Author: xuchaojie
 */

#ifndef SRC_SNAPSHOTCLONESERVER_COMMON_TASK_INFO_H_
#define SRC_SNAPSHOTCLONESERVER_COMMON_TASK_INFO_H_


#include <string>
#include <memory>
#include <mutex> //NOLINT
#include <atomic>

#include "src/common/concurrent/concurrent.h"

namespace curve {
namespace snapshotcloneserver {

class TaskInfo {
 public:
    TaskInfo()
        : progress_(0),
          isFinish_(false),
          isCanceled_(false) {}

    virtual ~TaskInfo() {}
    TaskInfo(const TaskInfo&) = delete;
    TaskInfo& operator=(const TaskInfo&) = delete;
    TaskInfo(TaskInfo&&) = default;
    TaskInfo& operator=(TaskInfo&&) = default;

    /**
     * @brief Set task completion percentage
     *
     * @param persent task completion percentage
     */
    void SetProgress(uint32_t persent) {
        progress_ = persent;
    }

    /**
     * @brief Get task completion percentage
     *
     * @return Task completion percentage
     */
    uint32_t GetProgress() const {
        return progress_;
    }

    /**
     * @brief Complete the task
     */
    void Finish() {
        isFinish_.store(true);
    }

    /**
     * @brief: Is the task completed
     *
     * @retval true Task completed
     * @retval false Task not completed
     */
    bool IsFinish() const {
        return isFinish_.load();
    }

    /**
     * @brief Cancel Task
     */
    void Cancel() {
        isCanceled_ = true;
    }

    /**
     * @brief: Do you want to cancel the task
     *
     * @retval true The task has been canceled
     * @retval false The task was not canceled
     */
    bool IsCanceled() const {
        return isCanceled_;
    }

    /**
     * @brief reset task
     */
    void Reset() {
        isFinish_.store(false);
        isCanceled_ = false;
    }

    /**
     * @brief: Obtain a reference to the task lock for unlocking using LockGuard
     *
     * Used to synchronize task completion and cancellation functions
     *  1. Before completing the task, first lock the task and then determine whether the task is cancelled,
     *  If cancelled, release the lock,
     *  Otherwise, release the lock after completing the logic of the task.
     *  2. Before canceling a task, first lock the task and then determine whether the task is completed,
     *  If completed, release the lock,
     *  Otherwise, execute the task to cancel the logic and release the lock.
     */
    curve::common::Mutex& GetLockRef() {
        return lock_;
    }

 private:
    // Task completion percentage
    uint32_t progress_;
    // Is the task completed
    std::atomic_bool isFinish_;
    // Has the task been canceled
    bool isCanceled_;
    mutable curve::common::Mutex lock_;
};

}  // namespace snapshotcloneserver
}  // namespace curve

#endif  // SRC_SNAPSHOTCLONESERVER_COMMON_TASK_INFO_H_
