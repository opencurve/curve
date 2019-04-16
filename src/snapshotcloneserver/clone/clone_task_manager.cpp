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
    if (isStop.load()) {
        int ret = threadpool_->Start();
        if (ret < 0) {
            LOG(ERROR) << "CloneTaskManager start thread pool fail"
                       << ", ret = " << ret;
            return ret;
        }
        isStop.store(false);
        // isStop标志先置，防止backEndThread先退出
        backEndThread =
            std::thread(&CloneTaskManager::BackEndThreadFunc, this);
    }
    return kErrCodeSnapshotServerSuccess;
}

void CloneTaskManager::Stop() {
    if (!isStop.exchange(true)) {
        backEndThread.join();
        // TODO(xuchaojie): to stop all task
        threadpool_->Stop();
    }
}

int CloneTaskManager::PushTask(std::shared_ptr<CloneTask> task) {
    if (isStop.load()) {
        return kErrCodeSnapshotServiceIsStop;
    }
    ScanWorkingTask();

    WriteLockGuard taskMapWlock(cloneTaskMapLock_);
    std::lock_guard<std::mutex>
        workingTasksLockGuard(cloningTasksLock_);

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
    return kErrCodeSnapshotServerSuccess;
}

std::shared_ptr<CloneTask> CloneTaskManager::GetTask(
    const TaskIdType &taskId) const {
    ReadLockGuard taskMapRlock(cloneTaskMapLock_);
    auto it = cloneTaskMap_.find(taskId);
    if (it != cloneTaskMap_.end()) {
        return it->second;
    }
    return nullptr;
}

void CloneTaskManager::BackEndThreadFunc() {
    while (!isStop.load()) {
        ScanWorkingTask();
        std::this_thread::sleep_for(
            std::chrono::milliseconds(kCloneTaskManagerScanIntervalMs));
    }
}

void CloneTaskManager::ScanWorkingTask() {
    WriteLockGuard taskMapWlock(cloneTaskMapLock_);
    std::lock_guard<std::mutex>
        workingTasksLock(cloningTasksLock_);
    uint32_t workingNum = cloningTasks_.size();
    VLOG(0) << "CloneTaskManager::ScanWorkingTask: "
              << " working task num = "
              << workingNum;
    for (auto it = cloningTasks_.begin();
            it != cloningTasks_.end();) {
        auto taskInfo = it->second->GetTaskInfo();
        if (taskInfo->IsFinish()) {
            cloneTaskMap_.erase(it->second->GetTaskId());
            it = cloningTasks_.erase(it);
        } else {
            it++;
        }
    }
}

}  // namespace snapshotcloneserver
}  // namespace curve

