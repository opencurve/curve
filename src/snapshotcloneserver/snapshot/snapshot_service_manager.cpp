/*
 * Project: curve
 * Created Date: Mon Dec 24 2018
 * Author: xuchaojie
 * Copyright (c) 2018 netease
 */

#include "src/snapshotcloneserver/snapshot/snapshot_service_manager.h"

#include <glog/logging.h>

namespace curve {
namespace snapshotcloneserver {

int SnapshotServiceManager::Init(const SnapshotCloneServerOptions &option) {
    std::shared_ptr<ThreadPool> pool =
        std::make_shared<ThreadPool>(option.snapshotPoolThreadNum);
    return taskMgr_->Init(pool, option);
}

int SnapshotServiceManager::Start() {
    return taskMgr_->Start();
}

void SnapshotServiceManager::Stop() {
    taskMgr_->Stop();
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

    auto snapInfoMetric = std::make_shared<SnapshotInfoMetric>(*uuid);
    std::shared_ptr<SnapshotTaskInfo> taskInfo =
        std::make_shared<SnapshotTaskInfo>(snapInfo, snapInfoMetric);
    taskInfo->UpdateMetric();
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
    return kErrCodeSuccess;
}

int SnapshotServiceManager::CancelSnapshot(
    const UUID &uuid,
    const std::string &user,
    const std::string &file) {
    std::shared_ptr<SnapshotTask> task = taskMgr_->GetTask(uuid);
    if (task != nullptr) {
        if (user != task->GetTaskInfo()->GetSnapshotInfo().GetUser()) {
            LOG(ERROR) << "Can not cancel snapshot by different user.";
            return kErrCodeInvalidUser;
        }
        if ((!file.empty()) &&
            (file != task->GetTaskInfo()->GetFileName())) {
            LOG(ERROR) << "Can not cancel, fileName is not matched.";
            return kErrCodeFileNameNotMatch;
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
    return kErrCodeSuccess;
}

int SnapshotServiceManager::DeleteSnapshot(
    const UUID &uuid,
    const std::string &user,
    const std::string &file) {
    SnapshotInfo snapInfo;
    int ret = core_->DeleteSnapshotPre(uuid, user, file, &snapInfo);
    if (kErrCodeTaskExist == ret) {
        return kErrCodeSuccess;
    } else if (kErrCodeSnapshotCannotDeleteUnfinished == ret) {
        // 转Cancel
        ret = CancelSnapshot(uuid, user, file);
        if (kErrCodeCannotCancelFinished == ret) {
            // 防止这一过程中又执行完了
            ret = core_->DeleteSnapshotPre(uuid, user, file, &snapInfo);
            if (ret < 0) {
                LOG(ERROR) << "DeleteSnapshotPre fail"
                           << ", ret = " << ret
                           << ", uuid = " << uuid
                           << ", file =" << file;
                return ret;
            }
        } else {
            return ret;
        }
    } else if (ret < 0) {
        LOG(ERROR) << "DeleteSnapshotPre fail"
                   << ", ret = " << ret
                   << ", uuid = " << uuid
                   << ", file =" << file;
        return ret;
    }
    auto snapInfoMetric = std::make_shared<SnapshotInfoMetric>(uuid);
    std::shared_ptr<SnapshotTaskInfo> taskInfo =
        std::make_shared<SnapshotTaskInfo>(snapInfo, snapInfoMetric);
    taskInfo->UpdateMetric();
    std::shared_ptr<SnapshotDeleteTask> task =
        std::make_shared<SnapshotDeleteTask>(
            snapInfo.GetUuid(), taskInfo, core_);
    ret = taskMgr_->PushTask(task);
    if (ret < 0) {
        LOG(ERROR) << "Push Task error, "
                   << " ret = " << ret;
        return ret;
    }
    return kErrCodeSuccess;
}

int SnapshotServiceManager::GetFileSnapshotInfo(const std::string &file,
    const std::string &user,
    std::vector<FileSnapshotInfo> *info) {
    std::vector<SnapshotInfo> snapInfos;
    int ret = core_->GetFileSnapshotInfo(file, &snapInfos);
    if (ret < 0) {
        LOG(ERROR) << "GetFileSnapshotInfo error, "
                   << " ret = " << ret
                   << ", file = " << file;
        return ret;
    }
    return GetFileSnapshotInfoInner(snapInfos, user, info);
}

int SnapshotServiceManager::GetFileSnapshotInfoById(const std::string &file,
    const std::string &user,
    const UUID &uuid,
    std::vector<FileSnapshotInfo> *info) {
    std::vector<SnapshotInfo> snapInfos;
    SnapshotInfo snap;
    int ret = core_->GetSnapshotInfo(uuid, &snap);
    if (ret < 0) {
        LOG(ERROR) << "GetSnapshotInfo error, "
                   << " ret = " << ret
                   << ", file = " << file
                   << ", uuid = " << uuid;
        return kErrCodeFileNotExist;
    }
    if (snap.GetUser() != user) {
        return kErrCodeInvalidUser;
    }
    if ((!file.empty()) && (snap.GetFileName() != file)) {
        return kErrCodeFileNameNotMatch;
    }
    snapInfos.push_back(snap);
    return GetFileSnapshotInfoInner(snapInfos, user, info);
}

int SnapshotServiceManager::GetFileSnapshotInfoInner(
    std::vector<SnapshotInfo> snapInfos,
    const std::string &user,
    std::vector<FileSnapshotInfo> *info) {
    int ret = kErrCodeSuccess;
    for (auto &snap : snapInfos) {
        if (snap.GetUser() == user) {
            Status st = snap.GetStatus();
            switch (st) {
                case Status::done: {
                    info->emplace_back(snap, 100);
                    break;
                }
                case Status::error:
                case Status::canceling: {
                    info->emplace_back(snap, 0);
                    break;
                }
                case Status::deleting:
                case Status::errorDeleting:
                case Status::pending: {
                    UUID uuid = snap.GetUuid();
                    std::shared_ptr<SnapshotTask> task =
                        taskMgr_->GetTask(uuid);
                    if (task != nullptr) {
                        info->emplace_back(snap,
                            task->GetTaskInfo()->GetProgress());
                    } else {
                        // 刚刚完成
                        SnapshotInfo newInfo;
                        ret = core_->GetSnapshotInfo(uuid, &newInfo);
                        if (ret < 0) {
                            LOG(ERROR) << "GetSnapshotInfo fail"
                                       << ", ret = " << ret
                                       << ", uuid = " << uuid;
                            return ret;
                        }
                        switch (newInfo.GetStatus()) {
                            case Status::done: {
                                info->emplace_back(newInfo, 100);
                                break;
                            }
                            case Status::error: {
                                info->emplace_back(newInfo, 0);
                                break;
                            }
                            default:
                                LOG(ERROR) << "can not reach here!";
                                // 当更新数据库失败时，有可能进入这里
                                return kErrCodeInternalError;
                        }
                    }
                    break;
                }
                default:
                    LOG(ERROR) << "can not reach here!";
                    return kErrCodeInternalError;
            }
        }
    }
    return kErrCodeSuccess;
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
                auto snapInfoMetric =
                    std::make_shared<SnapshotInfoMetric>(snap.GetUuid());
                std::shared_ptr<SnapshotTaskInfo> taskInfo =
                    std::make_shared<SnapshotTaskInfo>(snap, snapInfoMetric);
                taskInfo->UpdateMetric();
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
                auto snapInfoMetric =
                    std::make_shared<SnapshotInfoMetric>(snap.GetUuid());
                std::shared_ptr<SnapshotTaskInfo> taskInfo =
                    std::make_shared<SnapshotTaskInfo>(snap, snapInfoMetric);
                taskInfo->UpdateMetric();
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
    return kErrCodeSuccess;
}

}  // namespace snapshotcloneserver
}  // namespace curve

