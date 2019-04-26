/*
 * Project: curve
 * Created Date: Fri Dec 21 2018
 * Author: xuchaojie
 * Copyright (c) 2018 netease
 */

#include "src/snapshotcloneserver/snapshot/snapshot_task_manager.h"
#include "src/snapshotcloneserver/common/define.h"

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
        std::lock_guard<std::mutex>
            waitingTasksLock(waitingTasksLock_);
        auto ret = taskMap_.emplace(task->GetTaskId(), task);
        if (!ret.second) {
            LOG(ERROR) << "SnapshotTaskManager::PushTask, uuid duplicated.";
            return kErrCodeInternalError;
        }
        waitingTasks_.push_back(task);
    }

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
    ReadLockGuard taskMapRlock(taskMapLock_);
    auto it = taskMap_.find(taskId);
    if (it != taskMap_.end()) {
        auto taskInfo = it->second->GetTaskInfo();
        taskInfo->Lock();
        if (!taskInfo->IsFinish()) {
            taskInfo->Cancel();
            return kErrCodeSuccess;
        }
        taskInfo->UnLock();
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
    std::lock_guard<std::mutex>
        waitingTasksLock(waitingTasksLock_);
    std::lock_guard<std::mutex>
        workingTasksLock(workingTasksLock_);
    uint32_t waitingNum = waitingTasks_.size();
    uint32_t workingNum = workingTasks_.size();
    VLOG(0) << "SnapshotTaskManager::ScanWaitingTask: "
              << " working task num = "
              << workingNum
              << " waiting task num = "
              << waitingNum;
    for (auto it = waitingTasks_.begin();
        it != waitingTasks_.end();) {
        if (workingTasks_.find((*it)->GetTaskInfo()->GetFileName())
            == workingTasks_.end()) {
            workingTasks_.emplace((*it)->GetTaskInfo()->GetFileName(),
                *it);
            threadpool_->PushTask(*it);
            it = waitingTasks_.erase(it);
        } else {
            it++;
        }
    }
}

void SnapshotTaskManager::ScanWorkingTask() {
    WriteLockGuard taskMapWlock(taskMapLock_);
    std::lock_guard<std::mutex>
        workingTasksLock(workingTasksLock_);
    uint32_t waitingNum = waitingTasks_.size();
    uint32_t workingNum = workingTasks_.size();
    VLOG(0) << "SnapshotTaskManager::ScanWorkingTask: "
              << " working task num = "
              << workingNum
              << " waiting task num = "
              << waitingNum;
    for (auto it = workingTasks_.begin();
            it != workingTasks_.end();) {
        auto taskInfo = it->second->GetTaskInfo();
        if (taskInfo->IsFinish()) {
            taskMap_.erase(it->second->GetTaskId());
            it = workingTasks_.erase(it);
        } else {
            it++;
        }
    }
}

}  // namespace snapshotcloneserver
}  // namespace curve

