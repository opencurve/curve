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
 * Created Date: Fri Mar 29 2019
 * Author: xuchaojie
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
    int ret =  PushTaskInternal(task,
        &commonTaskMap_,
        &commonTasksLock_,
        commonPool_);
    if (ret >= 0) {
        cloneMetric_->UpdateBeforeTaskBegin(
            task->GetTaskInfo()->GetCloneInfo().GetTaskType());
        LOG(INFO) << "Push Task Into Common Pool success,"
                  << " TaskInfo : " << *(task->GetTaskInfo());
    }
    return ret;
}

int CloneTaskManager::PushStage1Task(std::shared_ptr<CloneTaskBase> task) {
    int ret = PushTaskInternal(task,
        &stage1TaskMap_,
        &stage1TasksLock_,
        stage1Pool_);
    if (ret >= 0) {
        cloneMetric_->UpdateBeforeTaskBegin(
            task->GetTaskInfo()->GetCloneInfo().GetTaskType());
        LOG(INFO) << "Push Task Into Stage1 Pool for meta install success,"
                  << " TaskInfo : " << *(task->GetTaskInfo());
    }
    return ret;
}

int CloneTaskManager::PushStage2Task(
    std::shared_ptr<CloneTaskBase> task) {
    int ret = PushTaskInternal(task,
        &stage2TaskMap_,
        &stage2TasksLock_,
        stage2Pool_);
    if (ret >= 0) {
        cloneMetric_->UpdateFlattenTaskBegin();
        LOG(INFO) << "Push Task Into Stage2 Pool for data install success,"
                  << " TaskInfo : " << *(task->GetTaskInfo());
    }
    return ret;
}

int CloneTaskManager::PushTaskInternal(std::shared_ptr<CloneTaskBase> task,
    std::map<std::string, std::shared_ptr<CloneTaskBase> > *taskMap,
    Mutex *taskMapMutex,
    std::shared_ptr<ThreadPool> taskPool) {
    // 同一个clone的Stage1的Task和Stage2的Task的任务ID是一样的，
    // clean task的ID也是一样的,
    // 触发一次扫描，将已完成的任务Flush出去
    ScanStage2Tasks();
    ScanStage1Tasks();
    ScanCommonTasks();

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
                   << "same destination exist, "
                   << "New TaskInfo : " << *(task->GetTaskInfo())
                   << ", Exist TaskInfo : "
                   << *(ret.first->second->GetTaskInfo());
        return kErrCodeTaskExist;
    }
    taskPool->PushTask(task);
    auto ret2 = cloneTaskMap_.emplace(task->GetTaskId(), task);
    if (!ret2.second) {
        LOG(ERROR) << "CloneTaskManager::PushTaskInternal fail, "
                   << " same taskid exist, "
                   << "New TaskInfo : " << *(task->GetTaskInfo())
                   << ", Exist TaskInfo : "
                   << *(ret2.first->second->GetTaskInfo());
        return kErrCodeTaskExist;
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
            LOG(INFO) << "common task {"
                      << " TaskInfo : " << *taskInfo
                      << "} finish, going to remove.";
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
            cloneMetric_->UpdateAfterTaskFinish(taskType, status);
            LOG(INFO) << "stage1 task {"
                      << " TaskInfo : " << *taskInfo
                      << "} finish, going to remove.";
            cloneTaskMap_.erase(it->second->GetTaskId());
            it = stage1TaskMap_.erase(it);
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
                cloneMetric_->UpdateAfterFlattenTaskFinish(status);
                LOG(INFO) << "stage2 task {"
                          << " TaskInfo : " << *taskInfo
                          << "} finish, going to remove.";
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

