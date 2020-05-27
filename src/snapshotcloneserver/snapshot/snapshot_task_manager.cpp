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
 * Created Date: Fri Dec 21 2018
 * Author: xuchaojie
 */

#include "src/snapshotcloneserver/snapshot/snapshot_task_manager.h"
#include "src/common/snapshotclone/snapshotclone_define.h"
#include "src/common/concurrent/concurrent.h"


using curve::common::LockGuard;

namespace curve {
namespace snapshotcloneserver {

int SnapshotTaskManager::Start() {
    if (isStop_.load()) {
        int ret = threadpool_->Start();
        if (ret < 0) {
            LOG(ERROR) << "SnapshotTaskManager start thread pool fail"
                       << ", ret = " << ret;
            return ret;
        }
        isStop_.store(false);
        // isStop_标志先置，防止backEndThread先退出
        backEndThread =
            std::thread(&SnapshotTaskManager::BackEndThreadFunc, this);
    }
    return kErrCodeSuccess;
}

void SnapshotTaskManager::Stop() {
    if (!isStop_.exchange(true)) {
        backEndThread.join();
        // TODO(xuchaojie): to stop all task
        threadpool_->Stop();
    }
}

int SnapshotTaskManager::PushTask(std::shared_ptr<SnapshotTask> task) {
    if (isStop_.load()) {
        return kErrCodeServiceIsStop;
    }
    // 移除实际已完成的task，防止uuid冲突
    ScanWorkingTask();

    {
        WriteLockGuard taskMapWlock(taskMapLock_);
        LockGuard waitingTasksLock(waitingTasksLock_);
        auto ret = taskMap_.emplace(task->GetTaskId(), task);
        if (!ret.second) {
            LOG(ERROR) << "SnapshotTaskManager::PushTask, uuid duplicated.";
            return kErrCodeInternalError;
        }
        waitingTasks_.push_back(task);
    }
    snapshotMetric_->snapshotWaiting << 1;

    // 立即执行task
    ScanWaitingTask();
    return kErrCodeSuccess;
}

std::shared_ptr<SnapshotTask> SnapshotTaskManager::GetTask(
    const TaskIdType &taskId) const {
    ReadLockGuard taskMapRlock(taskMapLock_);
    auto it = taskMap_.find(taskId);
    if (it != taskMap_.end()) {
        return it->second;
    }
    return nullptr;
}

int SnapshotTaskManager::CancelTask(const TaskIdType &taskId) {
    {
        // 还在等待队列的Cancel直接移除
        WriteLockGuard taskMapWlock(taskMapLock_);
        LockGuard waitingTasksLock(waitingTasksLock_);
        for (auto it = waitingTasks_.begin();
            it != waitingTasks_.end();
            it++) {
            if ((*it)->GetTaskId() == taskId) {
                int ret = core_->HandleCancelUnSchduledSnapshotTask(
                    (*it)->GetTaskInfo());
                if (kErrCodeSuccess == ret) {
                    waitingTasks_.erase(it);
                    taskMap_.erase(taskId);
                    return kErrCodeSuccess;
                } else {
                    return kErrCodeInternalError;
                }
            }
        }
    }

    ReadLockGuard taskMapRlock(taskMapLock_);
    auto it = taskMap_.find(taskId);
    if (it != taskMap_.end()) {
        auto taskInfo = it->second->GetTaskInfo();
        LockGuard lockGuard(taskInfo->GetLockRef());
        if (!taskInfo->IsFinish()) {
            taskInfo->Cancel();
            return kErrCodeSuccess;
        }
    }
    return kErrCodeCannotCancelFinished;
}

void SnapshotTaskManager::BackEndThreadFunc() {
    while (!isStop_.load()) {
        ScanWorkingTask();
        ScanWaitingTask();
        std::this_thread::sleep_for(
            std::chrono::milliseconds(snapshotTaskManagerScanIntervalMs_));
    }
}

void SnapshotTaskManager::ScanWaitingTask() {
    LockGuard waitingTasksLock(waitingTasksLock_);
    LockGuard workingTasksLock(workingTasksLock_);
    for (auto it = waitingTasks_.begin();
        it != waitingTasks_.end();) {
        if (workingTasks_.find((*it)->GetTaskInfo()->GetFileName())
            == workingTasks_.end()) {
            workingTasks_.emplace((*it)->GetTaskInfo()->GetFileName(),
                *it);
            threadpool_->PushTask(*it);
            snapshotMetric_->snapshotDoing << 1;
            snapshotMetric_->snapshotWaiting << -1;
            it = waitingTasks_.erase(it);
        } else {
            it++;
        }
    }
}

void SnapshotTaskManager::ScanWorkingTask() {
    WriteLockGuard taskMapWlock(taskMapLock_);
    LockGuard workingTasksLock(workingTasksLock_);
    for (auto it = workingTasks_.begin();
            it != workingTasks_.end();) {
        auto taskInfo = it->second->GetTaskInfo();
        if (taskInfo->IsFinish()) {
            snapshotMetric_->snapshotDoing << -1;
            if (taskInfo->GetSnapshotInfo().GetStatus()
                != Status::done) {
                snapshotMetric_->snapshotFailed << 1;
            } else {
                snapshotMetric_->snapshotSucceed << 1;
            }
            taskMap_.erase(it->second->GetTaskId());
            it = workingTasks_.erase(it);
        } else {
            it++;
        }
    }
}

}  // namespace snapshotcloneserver
}  // namespace curve

