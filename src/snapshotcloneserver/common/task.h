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

#ifndef SRC_SNAPSHOTCLONESERVER_COMMON_TASK_H_
#define SRC_SNAPSHOTCLONESERVER_COMMON_TASK_H_

#include <functional>
#include <memory>
#include "src/snapshotcloneserver/common/define.h"
#include "src/snapshotcloneserver/common/task_tracker.h"

namespace curve {
namespace snapshotcloneserver {

class Task {
 public:
    explicit Task(const TaskIdType &taskId)
        : taskId_(taskId) {}

    virtual ~Task() {}

    Task(const Task&) = delete;
    Task& operator=(const Task&) = delete;
    Task(Task&&) = default;
    Task& operator=(Task&&) = default;

    /**
     * @brief 获取快照任务执行体闭包
     *
     * @return 快照任务执行体
     */
    virtual std::function<void()> clousre() {
        return [this] () {
            Run();
        };
    }

    /**
     * @brief 获取快照任务id
     *
     * @return 快照任务id
     */
    TaskIdType GetTaskId() const {
        return taskId_;
    }

    /**
     * @brief 快照执行函数接口
     */
    virtual void Run() = 0;

 private:
    // 快照id
    TaskIdType taskId_;
};

class TrackerTask : public Task {
 public:
    explicit TrackerTask(const TaskIdType &taskId)
        : Task(taskId) {}

    void SetTracker(std::shared_ptr<TaskTracker> tracker) {
        tracker_ = tracker;
    }

    std::shared_ptr<TaskTracker> GetTracker() {
        return tracker_;
    }

 private:
    std::shared_ptr<TaskTracker> tracker_;
};

}  // namespace snapshotcloneserver
}  // namespace curve


#endif  // SRC_SNAPSHOTCLONESERVER_COMMON_TASK_H_
