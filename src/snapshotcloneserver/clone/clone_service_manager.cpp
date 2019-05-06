/*
 * Project: curve
 * Created Date: Fri 12 Apr 2019 05:34:07 PM CST
 * Author: xuchaojie
 * Copyright (c) 2019 netease
 */

#include "src/snapshotcloneserver/clone/clone_service_manager.h"

#include <glog/logging.h>

#include <string>
#include <vector>

namespace curve {
namespace snapshotcloneserver {

int CloneServiceManager::Init(const SnapshotCloneServerOptions &option) {
    std::shared_ptr<ThreadPool> pool =
        std::make_shared<ThreadPool>(option.clonePoolThreadNum);
    return cloneTaskMgr_->Init(pool, option);
}

int CloneServiceManager::Start() {
    return cloneTaskMgr_->Start();
}

void CloneServiceManager::Stop() {
    cloneTaskMgr_->Stop();
}

int CloneServiceManager::CloneFile(const UUID &source,
    const std::string &user,
    const std::string &destination,
    bool lazyFlag) {
    CloneInfo cloneInfo;
    int ret = cloneCore_->CloneOrRecoverPre(
        source, user, destination, lazyFlag,
        CloneTaskType::kClone, &cloneInfo);
    if (ret < 0) {
        LOG(ERROR) << "CloneOrRecoverPre error"
                   << ", ret = " << ret
                   << ", source = " << source
                   << ", user = " << user
                   << ", destination = " << destination
                   << ", lazyFlag = " << lazyFlag;
        return ret;
    }
    std::shared_ptr<CloneTaskInfo> taskInfo =
        std::make_shared<CloneTaskInfo>(cloneInfo);
    std::shared_ptr<CloneTask> task =
        std::make_shared<CloneTask>(
            cloneInfo.GetTaskId(), taskInfo, cloneCore_);
    ret = cloneTaskMgr_->PushTask(task);
    if (ret < 0) {
        LOG(ERROR) << "CloneTaskMgr Push Task error"
                   << ", ret = " << ret;
        return ret;
    }
    return kErrCodeSuccess;
}

int CloneServiceManager::RecoverFile(const UUID &source,
    const std::string &user,
    const std::string &destination,
    bool lazyFlag) {
    CloneInfo cloneInfo;
    int ret = cloneCore_->CloneOrRecoverPre(
        source, user, destination, lazyFlag,
        CloneTaskType::kRecover, &cloneInfo);
    if (ret < 0) {
        LOG(ERROR) << "CloneOrRecoverPre error"
                   << ", ret = " << ret
                   << ", source = " << source
                   << ", user = " << user
                   << ", destination = " << destination
                   << ", lazyFlag = " << lazyFlag;
        return ret;
    }
    std::shared_ptr<CloneTaskInfo> taskInfo =
        std::make_shared<CloneTaskInfo>(cloneInfo);
    std::shared_ptr<CloneTask> task =
        std::make_shared<CloneTask>(
            cloneInfo.GetTaskId(), taskInfo, cloneCore_);
    ret = cloneTaskMgr_->PushTask(task);
    if (ret < 0) {
        LOG(ERROR) << "CloneTaskMgr Push Task error"
                   << ", ret = " << ret;
        return ret;
    }
    return kErrCodeSuccess;
}

int CloneServiceManager::GetCloneTaskInfo(const std::string &user,
    std::vector<TaskCloneInfo> *info) {
    std::vector<CloneInfo> cloneInfos;
    int ret = cloneCore_->GetCloneInfoList(&cloneInfos);
    if (ret < 0) {
        LOG(ERROR) << "GetCloneInfoList fail"
                   << ", ret = " << ret;
        return ret;
    }
    for (auto &cloneInfo : cloneInfos) {
        if (cloneInfo.GetUser() == user) {
            switch (cloneInfo.GetStatus()) {
                case CloneStatus::done : {
                    info->emplace_back(cloneInfo, 100);
                    break;
                }
                case CloneStatus::error: {
                    info->emplace_back(cloneInfo, 0);
                    break;
                }
                case CloneStatus::cloning:
                case CloneStatus::recovering: {
                    TaskIdType taskId = cloneInfo.GetTaskId();
                    std::shared_ptr<CloneTaskBase> task =
                        cloneTaskMgr_->GetTask(taskId);
                    if (task != nullptr) {
                        info->emplace_back(cloneInfo,
                            task->GetTaskInfo()->GetProgress());
                    } else {
                        CloneInfo newInfo;
                        ret = cloneCore_->GetCloneInfo(taskId, &newInfo);
                        if (ret < 0) {
                            LOG(ERROR) << "GetCloneInfo fail"
                                       << ", ret = " << ret
                                       << ", taskId = " << taskId;
                            return ret;
                        }
                        switch (newInfo.GetStatus()) {
                            case CloneStatus::done : {
                                info->emplace_back(newInfo, 100);
                                break;
                            }
                            case CloneStatus::error: {
                                info->emplace_back(newInfo, 0);
                                break;
                            }
                            default:
                                LOG(ERROR) << "can not reach here!";
                                break;
                        }
                    }
                    break;
                }
                default:
                    LOG(ERROR) << "can not reach here!";
                    break;
            }
        }
    }
    return kErrCodeSuccess;
}

int CloneServiceManager::CleanCloneTask(const std::string &user,
    const TaskIdType &taskId) {
    CloneInfo cloneInfo;
    int ret = cloneCore_->CleanCloneOrRecoverTaskPre(user, taskId, &cloneInfo);
    if (kErrCodeTaskExist == ret) {
        return kErrCodeSuccess;
    } else if (ret < 0) {
        LOG(ERROR) << "CleanCloneOrRecoverTaskPre fail"
                   << ", ret = " << ret
                   << ", user = " << user
                   << ", taskid = " << taskId;
        return ret;
    }
    std::shared_ptr<CloneTaskInfo> taskInfo =
        std::make_shared<CloneTaskInfo>(cloneInfo);
    std::shared_ptr<CloneCleanTask> task =
        std::make_shared<CloneCleanTask>(
            cloneInfo.GetTaskId(), taskInfo, cloneCore_);
    ret = cloneTaskMgr_->PushTask(task);
    if (ret < 0) {
        LOG(ERROR) << "Push Task error, "
                   << " ret = " << ret;
        return ret;
    }
    return kErrCodeSuccess;
}

int CloneServiceManager::RecoverCloneTask() {
    std::vector<CloneInfo> list;
    int ret = cloneCore_->GetCloneInfoList(&list);
    if (ret < 0) {
        LOG(ERROR) << "GetCloneInfoList fail";
        return ret;
    }
    for (auto &cloneInfo : list) {
        switch (cloneInfo.GetStatus()) {
            case CloneStatus::cloning:
            case CloneStatus::recovering: {
                std::shared_ptr<CloneTaskInfo> taskInfo =
                    std::make_shared<CloneTaskInfo>(cloneInfo);
                std::shared_ptr<CloneTask> task =
                    std::make_shared<CloneTask>(
                        cloneInfo.GetTaskId(), taskInfo, cloneCore_);
                if (CloneFileType::kSnapshot ==
                    taskInfo->GetCloneInfo().GetFileType()) {
                    cloneCore_->GetSnapshotRef()->IncrementSnapshotRef(
                        taskInfo->GetCloneInfo().GetSrc());
                } else {
                    // TODO(xuchaojie): 镜像管理
                }
                ret = cloneTaskMgr_->PushTask(task);
                if (ret < 0) {
                    LOG(ERROR) << "CloneTaskMgr Push Task error"
                               << ", ret = " << ret;
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

