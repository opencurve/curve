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
 * Created Date: 2023-07-20
 * Author: xuchaojie
 */

#include "src/mds/nameserver2/task_manager.h"


namespace curve {
namespace mds {

bool TaskManager::Start(void) {
    // set the start flag
    stopFlag_ = false;

    // start worker thread
    taskWorkers_ = new ::curve::common::TaskThreadPool<>();

    if (taskWorkers_->Start(threadNum_) != 0) {
        LOG(ERROR) << "task worker thread start failed, threadNum_: " 
                   << threadNum_;
        return false;
    }

    // start check thread
    checkThread_ = new ::curve::common::Thread(&TaskManager::CheckResult, this);

    LOG(INFO) << "task manager check thread start success";

    return true;
}

bool TaskManager::Stop(void) {
    if (!stopFlag_.exchange(true)) {
        LOG(INFO) << "task manager stop begin";
        sleeper_.interrupt();

        taskWorkers_->Stop();
        delete taskWorkers_;

        checkThread_->join();
        delete checkThread_;

        LOG(INFO) << "task manager stop success";
    } 
    return true;
}

bool TaskManager::SubmitTask(
    const std::shared_ptr<CancelableTask> &task) {
    common::LockGuard lck(mutex_);
    if (stopFlag_) {
        LOG(ERROR) << "task manager not started, taskID = "
            << task->GetTaskID();
        return false;
    }

    if (taskMap_.find(task->GetTaskID()) != taskMap_.end()) {
        LOG(INFO) << "task duplicated, taskID = " << task->GetTaskID();
        return false;
    }

    taskMap_.emplace(task->GetTaskID(), task);

    taskWorkers_->Enqueue(task->Closure());

    LOG(INFO) << "Submit Task OK, TaskID = " << task->GetTaskID();
    return true;
}

std::shared_ptr<CancelableTask> TaskManager::GetTask(uint64_t taskId) {
    common::LockGuard lck(mutex_);
    auto it = taskMap_.find(taskId);
    if (it != taskMap_.end()) {
        return it->second;
    }
    return nullptr;
}

void TaskManager::CheckResult(void) {
    while (sleeper_.wait_for(std::chrono::milliseconds(checkPeriod_))) {
        common::LockGuard lck(mutex_);
        for (auto iter = taskMap_.begin(); iter != taskMap_.end();) {
            auto taskProgress = iter->second->GetTaskProgress();
            if (taskProgress.GetStatus() == TaskStatus::SUCCESS) {
                LOG(INFO) << "task success going to remove task, taskID = "
                    << iter->second->GetTaskID();
                iter = taskMap_.erase(iter);
                continue;
            } else if (taskProgress.GetStatus() == TaskStatus::FAILED) {
                if (!iter->second->RetryTimesExceed()) {
                    iter->second->Retry();
                    LOG(WARNING) << "task Failed,"
                                 << " retry,"
                                 << " taskID = "
                                 << iter->second->GetTaskID();
                    taskWorkers_->Enqueue(iter->second->Closure());
                } else {
                    LOG(ERROR) << "Task Failed,"
                                 << " retry times exceed,"
                                 << " going to remove task,"
                                 << " taskID = "
                                 << iter->second->GetTaskID();
                    iter = taskMap_.erase(iter);
                    continue;
                }
            }
            iter++;
        }
    }
    LOG(INFO) << "check thread exit";
}

}  // namespace mds
}  // namespace curve


