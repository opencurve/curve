/*
 * Project: curve
 * Created Date: Wednesday December 19th 2018
 * Author: hzsunjianliang
 * Copyright (c) 2018 netease
 */
#include "src/mds/nameserver2/clean_task_manager.h"

namespace curve {
namespace mds {

CleanTaskManager::CleanTaskManager(int threadNum, int checkPeriod) {
    threadNum_ = threadNum;
    checkPeriod_ = checkPeriod;
    stopFlag_ = true;
}

void CleanTaskManager::CheckCleanResult(void) {
    while (true) {
        {
            std::lock_guard<std::mutex> lck(mutex_);
            for (auto iter = cleanTasks_.begin();
                stopFlag_ != true && iter != cleanTasks_.end();) {
                // TODO(hzsunjianliang): check if task is too long to warning
                // TODO(hzsunjianliang): check if task is failed for retry?
                auto taskProgress = iter->second->GetTaskProgress();
                if ( taskProgress.GetStatus() == TaskStatus::SUCCESS ) {
                    LOG(INFO) << "going to remove task, taskID = "
                        << iter->second->GetTaskID();
                    iter = cleanTasks_.erase(iter);
                    continue;
                }
                ++iter;
            }
        }
        // check ret
        if (true == stopFlag_) {
            LOG(INFO) << "check thread exit";
            return;
        }
        // checkLoopTime
        std::this_thread::sleep_for(std::chrono::milliseconds(checkPeriod_));
    }
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
    checkThread_ = new std::thread(&CleanTaskManager::CheckCleanResult, this);
    LOG(INFO) << "TaskManger check thread started";
    return true;
}

bool CleanTaskManager::Stop(void) {
    stopFlag_ = true;

    // stop the thread pool
    cleanWorkers_->Stop();
    delete cleanWorkers_;

    // stop the check thread
    checkThread_->join();
    delete checkThread_;

    return true;
}

bool CleanTaskManager::PushTask(std::shared_ptr<Task> task) {
    std::lock_guard<std::mutex> lck(mutex_);
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
    std::lock_guard<std::mutex> lck(mutex_);

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
