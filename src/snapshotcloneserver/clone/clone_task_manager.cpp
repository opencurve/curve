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
        int ret = commonPool_->Start();
        if (ret < 0) {
            LOG(ERROR) << "CloneTaskManager start common pool fail"
                       << ", ret = " << ret;
            return ret;
        }
        ret = stage2Pool_->Start();
        if (ret < 0) {
            LOG(ERROR) << "CloneTaskManager start stage2 pool fail"
                       << ", ret = " << ret;
            return ret;
        }
        ret = stage1Pool_->Start();
        if (ret < 0) {
            LOG(ERROR) << "CloneTaskManager start stage1 pool fail"
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
        stage1Pool_->Stop();
        stage2Pool_->Stop();
        commonPool_->Stop();
    }
}

int CloneTaskManager::PushCommonTask(std::shared_ptr<CloneTaskBase> task) {
    return PushTaskInternal(task,
        &commonTaskMap_,
        &commonTasksLock_,
        commonPool_);
}

int CloneTaskManager::PushStage1Task(std::shared_ptr<CloneTaskBase> task) {
    return PushTaskInternal(task,
        &stage1TaskMap_,
        &stage1TasksLock_,
        stage1Pool_);
}

int CloneTaskManager::PushStage2Task(
    std::shared_ptr<CloneTaskBase> task) {
    return PushTaskInternal(task,
        &stage2TaskMap_,
        &stage2TasksLock_,
        stage2Pool_);
}

int CloneTaskManager::PushTaskInternal(std::shared_ptr<CloneTaskBase> task,
    std::map<std::string, std::shared_ptr<CloneTaskBase> > *taskMap,
    Mutex *taskMapMutex,
    std::shared_ptr<ThreadPool> taskPool) {
    if (isStop_.load()) {
        return kErrCodeServiceIsStop;
    }
    WriteLockGuard taskMapWlock(cloneTaskMapLock_);
    LockGuard workingTasksLockGuard(*taskMapMutex);

    std::string destination =
        task->GetTaskInfo()->GetCloneInfo().GetDest();

    auto ret = taskMap->emplace(
        destination,
        task);
    if (!ret.second) {
        LOG(ERROR) << "CloneTaskManager::PushTaskInternal fail, "
                   << " same destination exist."
                   << " destination = " << destination;
        return kErrCodeTaskExist;
    }
    taskPool->PushTask(task);
    cloneTaskMap_.emplace(task->GetTaskId(), task);
    cloneMetric_->UpdateBeforeTaskBegin(
        task->GetTaskInfo()->GetCloneInfo().GetTaskType());
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
        ScanStage2Tasks();
        ScanStage1Tasks();
        ScanCommonTasks();
        std::this_thread::sleep_for(
            std::chrono::milliseconds(cloneTaskManagerScanIntervalMs_));
    }
}

void CloneTaskManager::ScanCommonTasks() {
    WriteLockGuard taskMapWlock(cloneTaskMapLock_);
    LockGuard workingTasksLock(commonTasksLock_);
    for (auto it = commonTaskMap_.begin();
            it != commonTaskMap_.end();) {
        auto taskInfo = it->second->GetTaskInfo();
        // 处理已完成的任务
        if (taskInfo->IsFinish()) {
            CloneTaskType taskType =
                taskInfo->GetCloneInfo().GetTaskType();
            CloneStatus status =
                taskInfo->GetCloneInfo().GetStatus();
            // 移除任务并更新metric
            cloneMetric_->UpdateAfterTaskFinish(taskType, status);
            cloneTaskMap_.erase(it->second->GetTaskId());
            it = commonTaskMap_.erase(it);
        } else {
            it++;
        }
    }
}

void CloneTaskManager::ScanStage1Tasks() {
    WriteLockGuard taskMapWlock(cloneTaskMapLock_);
    LockGuard workingTasksLock(stage1TasksLock_);
    LockGuard workingTasksLockGuard(stage2TasksLock_);
    for (auto it = stage1TaskMap_.begin();
            it != stage1TaskMap_.end();) {
        auto taskInfo = it->second->GetTaskInfo();
        // 处理已完成的任务
        if (taskInfo->IsFinish()) {
            CloneTaskType taskType =
                taskInfo->GetCloneInfo().GetTaskType();
            CloneStatus status =
                taskInfo->GetCloneInfo().GetStatus();
            // 状态正常的任务需要放在第二个线程池中继续执行
            if (status == CloneStatus::done ||
                status == CloneStatus::cloning ||
                status == CloneStatus::recovering) {
                taskInfo->Reset();
                stage2TaskMap_.emplace(*it);
                stage2Pool_->PushTask(it->second);
                it = stage1TaskMap_.erase(it);
            // 失败任务移除并更新metric
            } else {
                cloneMetric_->UpdateAfterTaskFinish(taskType, status);
                cloneTaskMap_.erase(it->second->GetTaskId());
                it = stage1TaskMap_.erase(it);
            }
        } else {
            it++;
        }
    }
}

void CloneTaskManager::ScanStage2Tasks() {
    WriteLockGuard taskMapWlock(cloneTaskMapLock_);
    LockGuard workingTasksLockGuard(stage2TasksLock_);
    for (auto it = stage2TaskMap_.begin();
        it != stage2TaskMap_.end();) {
        auto taskInfo = it->second->GetTaskInfo();
        // 处理完成的任务
        if (taskInfo->IsFinish()) {
            CloneTaskType taskType =
                taskInfo->GetCloneInfo().GetTaskType();
            CloneStatus status =
                taskInfo->GetCloneInfo().GetStatus();
            // retrying 状态的任务需要重试
            if (CloneStatus::retrying == status) {
                if (CloneTaskType::kClone == taskType) {
                    taskInfo->GetCloneInfo().
                        SetStatus(CloneStatus::cloning);
                } else {
                    taskInfo->GetCloneInfo().
                        SetStatus(CloneStatus::recovering);
                }
                taskInfo->Reset();
                stage2Pool_->PushTask(it->second);
            // 其他任务结束更新metric
            } else {
                cloneMetric_->UpdateAfterTaskFinish(taskType, status);
                cloneTaskMap_.erase(it->second->GetTaskId());
                it = stage2TaskMap_.erase(it);
            }
        } else {
            it++;
        }
    }
}

}  // namespace snapshotcloneserver
}  // namespace curve

