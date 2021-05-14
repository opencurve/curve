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
 * Created Date: Wednesday December 5th 2018
 * Author: hzsunjianliang
 */

#include <memory>
#include <vector>
#include "src/mds/nameserver2/clean_manager.h"

namespace curve {
namespace mds {

CleanManager::CleanManager(std::shared_ptr<CleanCore> core,
                std::shared_ptr<CleanTaskManager> taskMgr,
                std::shared_ptr<NameServerStorage> storage)
                    : storage_(storage), cleanCore_(core), taskMgr_(taskMgr) {}

bool CleanManager::Start(void) {
    return taskMgr_->Start();
}

bool CleanManager::Stop(void) {
    return taskMgr_->Stop();
}

void CleanManager::InitDLockOptions(std::shared_ptr<DLockOpts> dlockOpts) {
    dlockOpts_ = dlockOpts;
}

bool CleanManager::SubmitDeleteSnapShotFileJob(const FileInfo &fileInfo,
                            std::shared_ptr<AsyncDeleteSnapShotEntity> entity) {
    auto taskID = static_cast<TaskIDType>(fileInfo.id());
    auto snapShotCleanTask =
        std::make_shared<SnapShotCleanTask>(taskID, cleanCore_,
                                            fileInfo, entity);
    return taskMgr_->PushTask(snapShotCleanTask);
}

bool CleanManager::SubmitDeleteCommonFileJob(const FileInfo &fileInfo) {
    auto taskID = static_cast<TaskIDType>(fileInfo.id());
    auto commonFileCleanTask =
        std::make_shared<CommonFileCleanTask>(taskID, cleanCore_,
                                            fileInfo);
    return taskMgr_->PushTask(commonFileCleanTask);
}

bool CleanManager::SubmitCleanDiscardSegmentJob(
    const std::string& cleanSegmentKey,
    const DiscardSegmentInfo& discardSegmentInfo) {
    // get dlock
    dlockOpts_->pfx = std::to_string(discardSegmentInfo.fileinfo().id());
    dlock_ = std::make_shared<DLock>(*dlockOpts_);
    if (0 == dlock_->Init()) {
        LOG(ERROR) << "Init DLock error"
                << ", pfx = " << dlockOpts_->pfx
                << ", retryTimes = " << dlockOpts_->retryTimes
                << ", ctx_timeoutMS = " << dlockOpts_->ctx_timeoutMS
                << ", ttlSec = " << dlockOpts_->ttlSec;
        return false;
    }

    auto task = std::make_shared<SegmentCleanTask>(
        cleanCore_, cleanSegmentKey, discardSegmentInfo, dlock_);
    task->SetTaskID(reinterpret_cast<TaskIDType>(task.get()));
    return taskMgr_->PushTask(task);
}

bool CleanManager::RecoverCleanTasks(void) {
    // load task from store
    std::vector<FileInfo> snapShotFiles;
    StoreStatus ret = storage_->LoadSnapShotFile(&snapShotFiles);
    if (ret != StoreStatus::OK) {
        LOG(ERROR) << "Load SnapShotFile error, ret = " << ret;
        return false;
    }

    // submit all tasks
    for (auto & file : snapShotFiles) {
        if (file.filestatus() == FileStatus::kFileDeleting) {
            SubmitDeleteSnapShotFileJob(file, nullptr);
        }
    }

    std::vector<FileInfo> commonFiles;
    StoreStatus ret1 = storage_->ListFile(RECYCLEBININODEID,
                        RECYCLEBININODEID + 1, &commonFiles);
    if (ret1 != StoreStatus::OK) {
        LOG(ERROR) << "Load recylce bin file error, ret = " << ret1;
        return false;
    }

    for (auto & file : commonFiles) {
        if (file.filestatus() == FileStatus::kFileDeleting) {
            SubmitDeleteCommonFileJob(file);
        }
    }

    return true;
}

std::shared_ptr<Task> CleanManager::GetTask(TaskIDType id) {
    return taskMgr_->GetTask(id);
}

bool CleanDiscardSegmentTask::Start() {
    if (!running_.exchange(true)) {
        LOG(INFO) << "start CleanDiscardSegmentTask";
        taskThread_ = curve::common::Thread(
            &CleanDiscardSegmentTask::ScanAndExecTask, this);
        return true;
    }

    LOG(WARNING) << "CleanDiscardSegmentTask has already started";
    return false;
}

bool CleanDiscardSegmentTask::Stop() {
    if (running_.exchange(false)) {
        LOG(INFO) << "stop CleanDiscardSegmentTask...";
        sleeper_.interrupt();
        taskThread_.join();
        LOG(INFO) << "stop CleanDiscardSegmentTask success";
        return true;
    }

    LOG(WARNING) << "CleanDiscardSegmentTask has already stopped";
    return false;
}

void CleanDiscardSegmentTask::ScanAndExecTask() {
    std::map<std::string, DiscardSegmentInfo> discardSegments;

    while (sleeper_.wait_for(std::chrono::milliseconds(scanIntervalMs_))) {
        discardSegments.clear();
        auto err = storage_->ListDiscardSegment(&discardSegments);
        if (err != StoreStatus::OK) {
            LOG(ERROR) << "ListDiscardSegment failed";
            continue;
        }

        for (const auto& kv : discardSegments) {
            if (!cleanManager_->SubmitCleanDiscardSegmentJob(kv.first,
                                                             kv.second)) {
                LOG(ERROR) << "SubmitCleanDiscardSegmentJob failed";
                continue;
            }
        }
    }
}

}  // namespace mds
}  // namespace curve
