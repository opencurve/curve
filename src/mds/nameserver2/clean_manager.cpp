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
                std::shared_ptr<NameServerStorage> storage,
                uint32_t mdsSessionTimeUs)
                    : storage_(storage), cleanCore_(core),
                    taskMgr_(taskMgr), mdsSessionTimeUs_(mdsSessionTimeUs) {}

bool CleanManager::Start(void) {
    return taskMgr_->Start();
}

bool CleanManager::Stop(void) {
    return taskMgr_->Stop();
}

bool CleanManager::SubmitDeleteSnapShotFileJob(const FileInfo &fileInfo,
                            std::shared_ptr<AsyncDeleteSnapShotEntity> entity) {
    auto taskID = static_cast<TaskIDType>(fileInfo.id());
    auto snapShotCleanTask =
        std::make_shared<SnapShotCleanTask>(taskID, cleanCore_,
                                            fileInfo, entity);
    return taskMgr_->PushTask(snapShotCleanTask);
}

bool CleanManager::SubmitDeleteBatchSnapShotFileJob(
    const FileInfo &snapfileInfo,
    std::shared_ptr<AsyncDeleteSnapShotEntity> entity) {
    auto taskID = static_cast<TaskIDType>(snapfileInfo.parentid());
    auto snapshotBatchCleanTask =
        std::make_shared<SnapShotBatchCleanTask>(taskID, cleanCore_, storage_,
                                             entity, mdsSessionTimeUs_);
    // SnapShotBatchCleanTask这个任务对外不可见（无法查询进度），
    // 真正可见的是其内部的
    // 快照删除子任务
    return taskMgr_->PushTask(snapshotBatchCleanTask, snapfileInfo);
}

bool CleanManager::SubmitDeleteCommonFileJob(const FileInfo &fileInfo) {
    auto taskID = static_cast<TaskIDType>(fileInfo.id());
    auto commonFileCleanTask =
        std::make_shared<CommonFileCleanTask>(taskID, cleanCore_,
                                            fileInfo);
    return taskMgr_->PushTask(commonFileCleanTask);
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

bool CleanManager::RecoverCleanTasks2(void) {
    // load task from store
    std::vector<FileInfo> snapShotFiles;
    FileInfo srcFileInfo;
    StoreStatus ret = storage_->LoadSnapShotFile(&snapShotFiles);
    if (ret != StoreStatus::OK) {
        LOG(ERROR) << "Load SnapShotFile error, ret = " << ret;
        return false;
    }

    // submit all tasks
    for (auto & file : snapShotFiles) {
        if (file.filestatus() == FileStatus::kFileDeleting) {
            SubmitDeleteBatchSnapShotFileJob(file, nullptr);
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

std::shared_ptr<Task> CleanManager::GetTask(TaskIDType id, TaskIDType sn) {
    return taskMgr_->GetTask(id, sn);
}

}  // namespace mds
}  // namespace curve
