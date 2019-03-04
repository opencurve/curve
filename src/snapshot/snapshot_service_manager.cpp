/*
 * Project: curve
 * Created Date: Mon Dec 24 2018
 * Author: xuchaojie
 * Copyright (c) 2018 netease
 */

#include "src/snapshot/snapshot_service_manager.h"

#include <glog/logging.h>

#include "src/snapshot/snapshot_core.h"
#include "src/snapshot/snapshot_task.h"
#include "src/snapshot/snapshot_define.h"

namespace curve {
namespace snapshotserver {

// TODO(xuchaojie) add config
const int kSnapsnotMaxTaskThreadNum = 8;

int SnapshotServiceManager::Init() {
    return taskMgr_->Init(kSnapsnotMaxTaskThreadNum);
}

int SnapshotServiceManager::Start() {
    return taskMgr_->Start();
}

int SnapshotServiceManager::Stop() {
    return taskMgr_->Stop();
}

int SnapshotServiceManager::CreateSnapshot(const std::string &file,
    const std::string &user,
    const std::string &snapshotName,
    UUID *uuid) {
    SnapshotInfo snapInfo;
    int ret = core_->CreateSnapshotPre(file, user, snapshotName, &snapInfo);
    if (ret < 0) {
        LOG(ERROR) << "CreateSnapshotPre error, "
                   << " ret ="
                   << ret
                   << ", file = "
                   << file
                   << ", snapshotName = "
                   << snapshotName
                   << ", uuid = "
                   << snapInfo.GetUuid();
        return ret;
    }
    *uuid = snapInfo.GetUuid();
    std::shared_ptr<SnapshotTaskInfo> taskInfo =
        std::make_shared<SnapshotTaskInfo>(snapInfo);
    std::shared_ptr<SnapshotCreateTask> task =
        std::make_shared<SnapshotCreateTask>(
            snapInfo.GetUuid(), taskInfo, core_);
    ret = taskMgr_->PushTask(task);
    if (ret < 0) {
        LOG(ERROR) << "Push Task error, "
                   << " ret = "
                   << ret;
        return ret;
    }
    return kErrCodeSnapshotServerSuccess;
}

int SnapshotServiceManager::CancelSnapshot(UUID uuid,
    const std::string &user,
    const std::string &file) {
    std::shared_ptr<SnapshotTask> task = taskMgr_->GetTask(uuid);
    if (task != nullptr) {
        if (user != task->GetTaskInfo()->GetSnapshotInfo().GetUser()) {
            LOG(ERROR) << "Can not cancel snapshot by different user.";
            return kErrCodeSnapshotUserNotMatch;
        }
        if (file != task->GetTaskInfo()->GetFileName()) {
            LOG(ERROR) << "Can not cancel, fileName is not matched.";
            return kErrCodeSnapshotFileNameNotMatch;
        }
    }

    int ret = taskMgr_->CancelTask(uuid);
    if (ret < 0) {
        LOG(ERROR) << "CancelSnapshot error, "
                   << " ret ="
                   << ret
                   << ", uuid = "
                   << uuid
                   << ", file ="
                   << file;
        return ret;
    }
    return kErrCodeSnapshotServerSuccess;
}

int SnapshotServiceManager::DeleteSnapshot(UUID uuid,
    const std::string &user,
    const std::string &file) {
    SnapshotInfo snapInfo;
    int ret = core_->DeleteSnapshotPre(uuid, user, file, &snapInfo);
    if (kErrCodeSnapshotDeleteTaskExist == ret) {
        return kErrCodeSnapshotServerSuccess;
    } else if (ret < 0) {
        LOG(ERROR) << "DeleteSnapshotPre error, "
                   << " ret ="
                   << ret
                   << ", uuid = "
                   << uuid
                   << ", file ="
                   << file;
        return ret;
    }
    std::shared_ptr<SnapshotTaskInfo> taskInfo =
        std::make_shared<SnapshotTaskInfo>(snapInfo);
    std::shared_ptr<SnapshotDeleteTask> task =
        std::make_shared<SnapshotDeleteTask>(
            snapInfo.GetUuid(), taskInfo, core_);
    ret = taskMgr_->PushTask(task);
    if (ret < 0) {
        LOG(ERROR) << "Push Task error, "
                   << " ret = "
                   << ret;
        return ret;
    }
    return kErrCodeSnapshotServerSuccess;
}

int SnapshotServiceManager::GetFileSnapshotInfo(const std::string &file,
    const std::string &user,
    std::vector<FileSnapshotInfo> *info) {
    std::vector<SnapshotInfo> snapInfos;
    int ret = core_->GetFileSnapshotInfo(file, &snapInfos);
    if (ret < 0) {
        LOG(ERROR) << "GetFileSnapshotInfo error, "
                   << " ret = "
                   << ret
                   << ", file = "
                   << file;
        return ret;
    }
    for (auto &snap : snapInfos) {
        if (snap.GetUser() == user) {
            Status st = snap.GetStatus();
            switch (st) {
                case Status::done: {
                    info->emplace_back(snap, 100);
                    break;
                }
                case Status::error: {
                    info->emplace_back(snap, 0);
                    break;
                }
                case Status::deleting:
                case Status::pending: {
                    UUID uuid = snap.GetUuid();
                    std::shared_ptr<SnapshotTask> task =
                        taskMgr_->GetTask(uuid);
                    if (task != nullptr) {
                        info->emplace_back(snap,
                            task->GetTaskInfo()->GetProgress());
                    } else {
                        // Can not access here posible.
                        LOG(ERROR) << "GetFileSnapshotInfo has "
                                   << "encounter a internal error: "
                                   << "Can not find the imcomplete async task."
                                   << " UUID = "
                                   << uuid;
                        info->emplace_back(snap, 0);
                        return kErrCodeSnapshotInternalError;
                    }
                    break;
                }
                default:
                    LOG(ERROR) << "can not reach here!";
                    break;
            }
        }
    }
    return kErrCodeSnapshotServerSuccess;
}

int SnapshotServiceManager::RecoverSnapshotTask() {
    std::vector<SnapshotInfo> list;
    int ret = core_->GetSnapshotList(&list);
    if (ret < 0) {
        LOG(ERROR) << "GetSnapshotList error";
        return ret;
    }
    for (auto &snap : list) {
        Status st = snap.GetStatus();
        switch (st) {
            case Status::pending : {
                std::shared_ptr<SnapshotTaskInfo> taskInfo =
                    std::make_shared<SnapshotTaskInfo>(snap);
                std::shared_ptr<SnapshotCreateTask> task =
                    std::make_shared<SnapshotCreateTask>(
                        snap.GetUuid(),
                        taskInfo,
                        core_);
                ret = taskMgr_->PushTask(task);
                if (ret < 0) {
                    LOG(ERROR) << "RecoverSnapshotTask push task error, ret = "
                               << ret
                               << ", uuid = "
                               << snap.GetUuid();
                    return ret;
                }
                break;
            }
            // 重启恢复的canceling等价于errorDeleting
            case Status::canceling :
            case Status::deleting :
            case Status::errorDeleting : {
                std::shared_ptr<SnapshotTaskInfo> taskInfo =
                    std::make_shared<SnapshotTaskInfo>(snap);
                std::shared_ptr<SnapshotDeleteTask> task =
                    std::make_shared<SnapshotDeleteTask>(
                        snap.GetUuid(),
                        taskInfo,
                        core_);
                ret = taskMgr_->PushTask(task);
                if (ret < 0) {
                    LOG(ERROR) << "RecoverSnapshotTask push task error, ret = "
                               << ret
                               << ", uuid = "
                               << snap.GetUuid();
                    return ret;
                }
                break;
            }
            default:
                break;
        }
    }
    return kErrCodeSnapshotServerSuccess;
}

}  // namespace snapshotserver
}  // namespace curve
