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
 * Created Date: Wednesday December 19th 2018
 * Author: hzsunjianliang
 */
#include <utility>
#include <memory>
#include "src/mds/nameserver2/clean_task_manager.h"

namespace curve {
namespace mds {

CleanTaskManager::CleanTaskManager(std::shared_ptr<ChannelPool> channelPool,
                            int threadNum, int checkPeriod)
                                     : channelPool_(channelPool) {
    threadNum_ = threadNum;
    checkPeriod_ = checkPeriod;
    stopFlag_ = true;
}

void CleanTaskManager::CheckCleanResult(void) {
    while (sleeper_.wait_for(std::chrono::milliseconds(checkPeriod_))) {
        {
            common::LockGuard lck(mutex_);
            bool notEmptyBefore = !cleanTasks_.empty();
            for (auto iter = cleanTasks_.begin(); iter != cleanTasks_.end();) {
                auto taskProgress = iter->second->GetTaskProgress();
                if (taskProgress.GetStatus() == TaskStatus::SUCCESS) {
                    LOG(INFO) << "going to remove task, taskID = "
                        << iter->second->GetTaskID();
                    iter = cleanTasks_.erase(iter);
                    channelPool_->RemoveUser();
                    continue;
                } else if (taskProgress.GetStatus() == TaskStatus::FAILED) {
                    iter->second->Retry();
                    if (!iter->second->RetryTimesExceed()) {
                        LOG(WARNING) << "CleanTaskManager find Task Failed,"
                                     << " retry,"
                                     << " taskID = "
                                     << iter->second->GetTaskID();
                        cleanWorkers_->Enqueue(iter->second->Closure());
                    } else {
                        LOG(ERROR) << "CleanTaskManager find Task Failed,"
                                     << " retry times exceed,"
                                     << " going to remove task,"
                                     << " taskID = "
                                     << iter->second->GetTaskID();
                        iter = cleanTasks_.erase(iter);
                        channelPool_->RemoveUser();
                        continue;
                    }
                }
                ++iter;
            }
        }

        {
            common::LockGuard lckBatch(mutexBatch_);
            bool notEmptyBefore = !cleanBatchSnapTasks_.empty();
            for (auto iter = cleanBatchSnapTasks_.begin();
                iter != cleanBatchSnapTasks_.end();) {
                auto taskProgress = iter->second->GetTaskProgress();
                if (taskProgress.GetStatus() == TaskStatus::SUCCESS) {
                    // 该任务Run执行完成后，进入本check函数前
                    // 又有新的快照删除任务插入。
                    // 因此，在移除cleanBatchSnapTask前再检查一次，
                    // 否则该新的删除任务没有机会执行
                    if (iter->second->IsEmpty()) {
                        LOG(INFO) << "going to remove cleanBatchSnapTask"
                                  << ", taskID = "
                                  << iter->second->GetTaskID();
                        iter = cleanBatchSnapTasks_.erase(iter);
                        channelPool_->RemoveUser();
                        continue;
                    } else {
                        LOG(INFO) << "Found residual task in cleanBatchSnapTask"
                                  << " before erase, run it again";
                        // fix bug: task status should be set to PROGRESSING,
                        // or else this task may be running
                        //          concurrently and cause undefined behavior.
                        iter->second->GetMutableTaskProgress()->SetProgress(0);
                        iter->second->GetMutableTaskProgress()->SetStatus(
                            TaskStatus::PROGRESSING);
                        cleanWorkers_->Enqueue(iter->second->Closure());
                    }
                } else if (taskProgress.GetStatus() == TaskStatus::FAILED) {
                    iter->second->Retry();
                    // 这里的重试次数并没有因为多个快照删除任务的存在而累加
                    if (!iter->second->RetryTimesExceed()) {
                        LOG(WARNING) << "CleanTaskManager find failed status "
                                     << "cleanBatchSnapTask,"
                                     << " retry,"
                                     << " taskID = "
                                     << iter->second->GetTaskID();
                        cleanWorkers_->Enqueue(iter->second->Closure());
                    } else {
                        LOG(ERROR) << "CleanTaskManager find failed status "
                                   << "cleanBatchSnapTask,"
                                   << " retry times exceed,"
                                   << " going to remove task,"
                                   << " taskID = "
                                   << iter->second->GetTaskID();
                        iter = cleanBatchSnapTasks_.erase(iter);
                        channelPool_->RemoveUser();
                        continue;
                    }
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
    cleanWorkers_ =  new ::curve::common::TaskThreadPool<>();

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

    channelPool_->AddUser();
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

bool CleanTaskManager::PushTask(
    std::shared_ptr<SnapShotBatchCleanTask> task,
    const FileInfo &snapfileInfo) {
    common::LockGuard lck(mutexBatch_);
    if (stopFlag_) {
        LOG(ERROR) << "task manager not started, taskID = "
            << task->GetTaskID() << ", snapSn = " << snapfileInfo.seqnum();
        return false;
    }
    auto cleanBatchSnapTask = cleanBatchSnapTasks_.find(task->GetTaskID());
    if (cleanBatchSnapTask != cleanBatchSnapTasks_.end()) {
        if (!cleanBatchSnapTask->second->PushTask(snapfileInfo)) {
            LOG(ERROR) << "cleanBatchSnapTask duplicated, taskID = "
                      << task->GetTaskID()
                      << ", snapSn = " << snapfileInfo.seqnum();
            return false;
        } else {
            LOG(INFO) << "Push task to existing SnapShotBatchCleanTask OK"
                      << ", TaskID = " << task->GetTaskID()
                      << ", snapSn = " << snapfileInfo.seqnum();
            return true;
        }
    }

    if (!task->PushTask(snapfileInfo)) {
        LOG(ERROR) << "cleanBatchSnapTask duplicated, taskID = "
                    << task->GetTaskID()
                    << ", snapSn = " << snapfileInfo.seqnum();
        return false;
    }

    channelPool_->AddUser();
    cleanWorkers_->Enqueue(task->Closure());
    cleanBatchSnapTasks_.insert(std::make_pair(task->GetTaskID(), task));
    LOG(INFO) << "Push new created SnapShotBatchCleanTask OK, TaskID = "
              << task->GetTaskID() << ", snapSn = " << snapfileInfo.seqnum();
    return true;
}

std::shared_ptr<Task> CleanTaskManager::GetTask(TaskIDType id, TaskIDType sn) {
    common::LockGuard lck(mutex_);

    auto iter = cleanBatchSnapTasks_.find(id);
    if (iter == cleanBatchSnapTasks_.end()) {
        LOG(INFO) << "taskid = "<< id << ", not found in cleanBatchSnapTasks";
        return nullptr;
    } else {
        return iter->second->GetTask(sn);
    }
}

}  // namespace mds
}  // namespace curve
