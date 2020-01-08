/*
 * Project: curve
 * Created Date: Wednesday December 19th 2018
 * Author: hzsunjianliang
 * Copyright (c) 2018 netease
 */
#include <utility>
#include <memory>
#include "src/mds/nameserver2/clean_task_manager.h"

namespace curve {
namespace mds {

CleanTaskManager::CleanTaskManager(int threadNum, int checkPeriod) {
    threadNum_ = threadNum;
    checkPeriod_ = checkPeriod;
    stopFlag_ = true;
}

void CleanTaskManager::CheckCleanResult(void) {
    while (sleeper_.wait_for(std::chrono::milliseconds(checkPeriod_))) {
        {
            common::LockGuard lck(mutex_);
            for (auto iter = cleanTasks_.begin(); iter != cleanTasks_.end();) {
                auto taskProgress = iter->second->GetTaskProgress();
                if (taskProgress.GetStatus() == TaskStatus::SUCCESS) {
                    LOG(INFO) << "going to remove task, taskID = "
                        << iter->second->GetTaskID();
                    iter = cleanTasks_.erase(iter);
                    continue;
                } else if (taskProgress.GetStatus() == TaskStatus::FAILED) {
                    LOG(WARNING) << "CleanTaskManager find Task Failed, retry,"
                                 << " taskID = " << iter->second->GetTaskID();
                    iter->second->SetTaskProgress(TaskProgress());
                    cleanWorkers_->Enqueue(iter->second->Closure());
                }
                ++iter;
            }
        }
    }
    LOG(INFO) << "check thread exit";
}

bool CleanTaskManager::Start(void) {
    // set the start flag
    stopFlag_ = false;

    // start worker thread
    cleanWorkers_ =  new ::curve::common::TaskThreadPool();

    if (cleanWorkers_->Start(threadNum_) != 0) {
        LOG(ERROR) << "thread pool start error";
        return false;
    }

    // start check thread
    checkThread_ = new common::Thread(&CleanTaskManager::CheckCleanResult,
                                        this);
    LOG(INFO) << "TaskManger check thread started";
    return true;
}

bool CleanTaskManager::Stop(void) {
    if (!stopFlag_.exchange(true)) {
        LOG(INFO) << "stop CleanTaskManager...";
        sleeper_.interrupt();

        // stop the thread pool
        cleanWorkers_->Stop();
        delete cleanWorkers_;

        // stop the check thread
        checkThread_->join();
        delete checkThread_;
        LOG(INFO) << "stop CleanTaskManager ok";
    }

    return true;
}

bool CleanTaskManager::PushTask(std::shared_ptr<Task> task) {
    common::LockGuard lck(mutex_);
    if (stopFlag_) {
        LOG(ERROR) << "task manager not started, taskID = "
            << task->GetTaskID();
        return false;
    }
    if (cleanTasks_.find(task->GetTaskID()) != cleanTasks_.end()) {
        LOG(INFO) << "task duplicated, taskID = " << task->GetTaskID();
        return false;
    }

    cleanWorkers_->Enqueue(task->Closure());

    cleanTasks_.insert(std::make_pair(task->GetTaskID(), task));
    LOG(INFO) << "Push Task OK, TaskID = " << task->GetTaskID();
    return true;
}

std::shared_ptr<Task> CleanTaskManager::GetTask(TaskIDType id) {
    common::LockGuard lck(mutex_);

    auto iter = cleanTasks_.begin();
    if ((iter = cleanTasks_.find(id)) == cleanTasks_.end()) {
        LOG(INFO) << "taskid = "<< id << ", not found";
        return nullptr;
    } else {
        return iter->second;
    }
}

}  // namespace mds
}  // namespace curve
