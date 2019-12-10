/*
 * Project: curve
 * Created Date: Fri Mar 29 2019
 * Author: xuchaojie
 * Copyright (c) 2018 netease
 */

#include "src/snapshotcloneserver/clone/clone_task_manager.h"
#include "src/snapshotcloneserver/common/define.h"


namespace curve {
namespace snapshotcloneserver {

int CloneTaskManager::Start() {
    if (isStop_.load()) {
        int ret = threadpool_->Start();
        if (ret < 0) {
            LOG(ERROR) << "CloneTaskManager start thread pool fail"
                       << ", ret = " << ret;
            return ret;
        }
        isStop_.store(false);
        // isStop_标志先置，防止backEndThread先退出
        backEndThread =
            std::thread(&CloneTaskManager::BackEndThreadFunc, this);
    }
    return kErrCodeSuccess;
}

void CloneTaskManager::Stop() {
    if (!isStop_.exchange(true)) {
        backEndThread.join();
        // TODO(xuchaojie): to stop all task
        threadpool_->Stop();
    }
}

int CloneTaskManager::PushTask(std::shared_ptr<CloneTaskBase> task) {
    if (isStop_.load()) {
        return kErrCodeServiceIsStop;
    }
    ScanWorkingTask();

    WriteLockGuard taskMapWlock(cloneTaskMapLock_);
    std::lock_guard<std::mutex>
        workingTasksLockGuard(cloningTasksLock_);

    // TODO(xuchaojie): 目前超过线程数的克隆任务由于得不到调度，
    // 对上层来说都是卡住了，不如直接返回失败，下个版本解决这一问题。
    if (cloningTasks_.size() >= clonePoolThreadNum_) {
        LOG(ERROR) << "CloneTaskManager::PushTask fail, "
                   << "current task is full, num = " << cloningTasks_.size();
        return kErrCodeTaskIsFull;
    }

    std::string destination =
        task->GetTaskInfo()->GetCloneInfo().GetDest();
    auto ret = cloningTasks_.emplace(
        destination,
        task);
    if (!ret.second) {
        LOG(ERROR) << "CloneTaskManager::PushTask fail, "
                   << " same destination exist."
                   << " destination = " << destination;
        return kErrCodeTaskExist;
    }
    threadpool_->PushTask(task);
    cloneTaskMap_.emplace(task->GetTaskId(), task);
    if (task->GetTaskInfo()->GetCloneInfo().GetTaskType() ==
        CloneTaskType::kClone) {
        cloneMetric_->cloneDoing << 1;
    } else {
        cloneMetric_->recoverDoing << 1;
    }
    return kErrCodeSuccess;
}

std::shared_ptr<CloneTaskBase> CloneTaskManager::GetTask(
    const TaskIdType &taskId) const {
    ReadLockGuard taskMapRlock(cloneTaskMapLock_);
    auto it = cloneTaskMap_.find(taskId);
    if (it != cloneTaskMap_.end()) {
        return it->second;
    }
    return nullptr;
}

void CloneTaskManager::BackEndThreadFunc() {
    while (!isStop_.load()) {
        ScanWorkingTask();
        std::this_thread::sleep_for(
            std::chrono::milliseconds(cloneTaskManagerScanIntervalMs_));
    }
}

void CloneTaskManager::ScanWorkingTask() {
    WriteLockGuard taskMapWlock(cloneTaskMapLock_);
    std::lock_guard<std::mutex>
        workingTasksLock(cloningTasksLock_);
    for (auto it = cloningTasks_.begin();
            it != cloningTasks_.end();) {
        auto taskInfo = it->second->GetTaskInfo();
        if (taskInfo->IsFinish()) {
            if (taskInfo->GetCloneInfo().GetTaskType() ==
                CloneTaskType::kClone) {
                if (CloneStatus::done ==
                    taskInfo->GetCloneInfo().GetStatus()) {
                    cloneMetric_->cloneDoing << -1;
                    cloneMetric_->cloneSucceed << 1;
                    cloneTaskMap_.erase(it->second->GetTaskId());
                    it = cloningTasks_.erase(it);
                } else if (CloneStatus::retrying ==
                    taskInfo->GetCloneInfo().GetStatus()) {
                    taskInfo->GetCloneInfo().
                        SetStatus(CloneStatus::cloning);
                    taskInfo->Reset();
                    threadpool_->PushTask(it->second);
                } else {
                    cloneMetric_->cloneDoing << -1;
                    cloneMetric_->cloneFailed << 1;
                    cloneTaskMap_.erase(it->second->GetTaskId());
                    it = cloningTasks_.erase(it);
                }
            } else {
                if (CloneStatus::done ==
                    taskInfo->GetCloneInfo().GetStatus()) {
                    cloneMetric_->recoverDoing << -1;
                    cloneMetric_->recoverSucceed << 1;
                    cloneTaskMap_.erase(it->second->GetTaskId());
                    it = cloningTasks_.erase(it);
                } else if (CloneStatus::retrying ==
                    taskInfo->GetCloneInfo().GetStatus()) {
                    taskInfo->GetCloneInfo().
                        SetStatus(CloneStatus::recovering);
                    taskInfo->Reset();
                    threadpool_->PushTask(it->second);
                } else {
                    cloneMetric_->recoverDoing << -1;
                    cloneMetric_->recoverFailed << 1;
                    cloneTaskMap_.erase(it->second->GetTaskId());
                    it = cloningTasks_.erase(it);
                }
            }
        } else {
            it++;
        }
    }
}

}  // namespace snapshotcloneserver
}  // namespace curve

