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
 * Created Date: Mon Dec 24 2018
 * Author: xuchaojie
 */

#include "src/snapshotcloneserver/snapshot/snapshot_service_manager.h"

#include <glog/logging.h>
#include "src/common/string_util.h"
#include "src/common/curve_version.h"

using ::curve::common::kSupportLocalSnapshotFileVersion;

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
    FInfo fInfo;
    int ret = core_->GetFileInfo(file, user, &fInfo);
    if (ret < 0) {
        return ret;
    }
    if (fInfo.version < kSupportLocalSnapshotFileVersion) {
        LOG(INFO) << "find file version is "
                  << fInfo.version
                  << ", not support local snapshot, use s3 snapshot";
        return CreateS3Snapshot(file, user, snapshotName, uuid);
    } else {
        LOG(INFO) << "find file version is "
                  << fInfo.version
                  << ", support local snapshot, use local snapshot";
        return CreateLocalSnapshot(file, user, snapshotName, uuid);
    }
}

int SnapshotServiceManager::CreateS3Snapshot(const std::string &file,
    const std::string &user,
    const std::string &snapshotName,
    UUID *uuid) {
    SnapshotInfo snapInfo;
    int ret = core_->CreateSnapshotPre(file, user, snapshotName, &snapInfo);
    if (ret < 0) {
        if (kErrCodeTaskExist == ret) {
            // 任务已存在的情况下返回成功，使接口幂等
            *uuid = snapInfo.GetUuid();
            return kErrCodeSuccess;
        }
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

int SnapshotServiceManager::CreateLocalSnapshot(const std::string &file,
    const std::string &user,
    const std::string &snapshotName,
    UUID *uuid) {
    SnapshotInfo snapInfo;
    int ret = core_->CreateLocalSnapshot(file, user, snapshotName, &snapInfo);
    if (ret < 0) {
        LOG(ERROR) << "CreateLocalSnapshot error, "
                   << " ret = " << ret
                   << ", file = " << file
                   << ", snapshotName = " << snapshotName
                   << ", uuid = " << snapInfo.GetUuid();
        return ret;
    }
    *uuid = snapInfo.GetUuid();
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

int SnapshotServiceManager::DeleteSnapshotBySnapshotName(
    const std::string &snapshotName,
    const std::string &user,
    const std::string &file) {
    SnapshotInfo info;
    int ret = core_->GetSnapshotInfo(file, snapshotName, &info);
    if (ret < 0) {
        LOG(INFO) << "snapshot not exist, file = "
                  << file
                  << ", snapshotName = "
                  << snapshotName;
        return kErrCodeSuccess;
    }
    if (info.GetLocation() == LocationType::kLocationCurve) {
        LOG(INFO) << "snapshot location is in curve, "
                  << "use local snapshot delete";
        return DeleteLocalSnapshot(info.GetUuid(), user, file);
    } else {
        LOG(INFO) << "snapshot location is in s3, "
                  << "use s3 snapshot delete";
        return DeleteS3Snapshot(info.GetUuid(), user, file);
    }
}

int SnapshotServiceManager::DeleteSnapshot(
    const std::string &uuid,
    const std::string &user,
    const std::string &file) {
    SnapshotInfo info;
    int ret = core_->GetSnapshotInfo(uuid, &info);
    if (ret < 0) {
        LOG(INFO) << "snapshot not exist, uuid = " << uuid;
        return kErrCodeSuccess;
    }
    if (info.GetLocation() == LocationType::kLocationCurve) {
        LOG(INFO) << "snapshot location is in curve, "
                  << "use local snapshot delete";
        return DeleteLocalSnapshot(info.GetUuid(), user, file);
    } else {
        LOG(INFO) << "snapshot location is in s3, "
                  << "use s3 snapshot delete";
        return DeleteS3Snapshot(info.GetUuid(), user, file);
    }
}

int SnapshotServiceManager::DeleteS3Snapshot(
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

int SnapshotServiceManager::DeleteLocalSnapshot(
    const UUID &uuid,
    const std::string &user,
    const std::string &file) {
    int ret = core_->DeleteLocalSnapshot(uuid, user, file);
    if (ret < 0) {
        LOG(ERROR) << "DeleteLocalSnapshot failed"
                   << " ret = " << ret
                   << ", file = " << file
                   << ", user = " << user
                   << ", uuid = " << uuid;
    }
    return ret;
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

int SnapshotServiceManager::GetFileSnapshotInfoBySnapshotName(
    const std::string &file,
    const std::string &user,
    const std::string &snapshotName,
    std::vector<FileSnapshotInfo> *info) {
    std::vector<SnapshotInfo> snapInfos;
    SnapshotInfo snap;
    int ret = core_->GetSnapshotInfo(file, snapshotName, &snap);
    if (ret < 0) {
        LOG(ERROR) << "GetSnapshotInfo error, "
                   << " ret ="
                   << ret
                   << ", file = "
                   << file
                   << ", snapshotName = "
                   << snapshotName;
        return kErrCodeFileNotExist;
    }
    if (snap.GetUser() != user) {
        return kErrCodeInvalidUser;
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
                    if (LocationType::kLocationCurve == snap.GetLocation()) {
                        Status st;
                        uint32_t progress;
                        ret = core_->GetLocalSnapshotStatus(snap.GetFileName(),
                            snap.GetUser(),
                            snap.GetSeqNum(),
                            &st, &progress);
                        if (kErrCodeSuccess == ret) {
                            SnapshotInfo newInfo = snap;
                            newInfo.SetStatus(st);
                            info->emplace_back(newInfo, progress);
                            continue;
                        } else if (kErrCodeFileNotExist == ret) {
                            continue;
                        } else {
                            LOG(ERROR) << "GetLocalSnapshotStatus error, "
                                       << " ret = " << ret
                                       << ", file = " << snap.GetFileName()
                                       << ", user = " << snap.GetUser()
                                       << ", seq = " << snap.GetSeqNum();
                            return ret;
                        }
                    }

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

bool SnapshotFilterCondition::IsMatchCondition(const SnapshotInfo &snapInfo) {
    if (user_ != nullptr && *user_ != snapInfo.GetUser()) {
        return false;
    }

    if (file_ != nullptr && *file_ != snapInfo.GetFileName()) {
        return false;
    }

    if (uuid_ != nullptr && *uuid_ != snapInfo.GetUuid()) {
        return false;
    }

    int status;
    if (status_ != nullptr
        && common::StringToInt(*status_, &status) == false) {
        return false;
    }

    if (status_ != nullptr
        && common::StringToInt(*status_, &status) == true
        && status != static_cast<int>(snapInfo.GetStatus())) {
        return false;
    }

    return true;
}

int SnapshotServiceManager::GetSnapshotListInner(
    std::vector<SnapshotInfo> snapInfos,
    SnapshotFilterCondition filter,
    std::vector<FileSnapshotInfo> *info) {
    int ret = kErrCodeSuccess;
    for (auto &snap : snapInfos) {
        if (filter.IsMatchCondition(snap)) {
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

int SnapshotServiceManager::GetSnapshotListByFilter(
                    const SnapshotFilterCondition &filter,
                    std::vector<FileSnapshotInfo> *info) {
    std::vector<SnapshotInfo> snapInfos;
    int ret = core_->GetSnapshotList(&snapInfos);
    if (ret < 0) {
        LOG(ERROR) << "GetFileSnapshotInfo error, "
                   << " ret = " << ret;
        return ret;
    }
    return GetSnapshotListInner(snapInfos, filter, info);
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

