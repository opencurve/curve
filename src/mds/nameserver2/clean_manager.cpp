/*
 * Project: curve
 * Created Date: Wednesday December 5th 2018
 * Author: hzsunjianliang
 * Copyright (c) 2018 netease
 */

#include <vector>
#include "src/mds/nameserver2/clean_manager.h"

namespace curve {
namespace mds {

CleanManager::CleanManager(std::shared_ptr<CleanCore> core,
                std::shared_ptr<CleanTaskManager> taskMgr,
                NameServerStorage *storage) {
    storage_ = storage;
    cleanCore_ = core;
    taskMgr_ = taskMgr;
}

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

    return true;
}

std::shared_ptr<Task> CleanManager::GetTask(TaskIDType id) {
    return taskMgr_->GetTask(id);
}

}  // namespace mds
}  // namespace curve
