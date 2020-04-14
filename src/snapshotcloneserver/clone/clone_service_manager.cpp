/*
 * Project: curve
 * Created Date: Fri 12 Apr 2019 05:34:07 PM CST
 * Author: xuchaojie
 * Copyright (c) 2019 netease
 */

#include "src/snapshotcloneserver/clone/clone_service_manager.h"

#include <glog/logging.h>

#include <string>
#include <memory>
#include <vector>

#include "src/snapshotcloneserver/common/snapshotclone_metric.h"
#include "src/snapshotcloneserver/common/define.h"

namespace curve {
namespace snapshotcloneserver {

int CloneServiceManager::Init(const SnapshotCloneServerOptions &option) {
    std::shared_ptr<ThreadPool> stage1Pool =
        std::make_shared<ThreadPool>(option.stage1PoolThreadNum);
    std::shared_ptr<ThreadPool> stage2Pool =
        std::make_shared<ThreadPool>(option.stage2PoolThreadNum);
    std::shared_ptr<ThreadPool> commonPool =
        std::make_shared<ThreadPool>(option.commonPoolThreadNum);
    return cloneTaskMgr_->Init(stage1Pool, stage2Pool, commonPool, option);
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
    bool lazyFlag,
    std::shared_ptr<CloneClosure> closure,
    TaskIdType *taskId) {
    brpc::ClosureGuard guard(closure.get());
    CloneInfo cloneInfo;
    int ret = cloneCore_->CloneOrRecoverPre(
        source, user, destination, lazyFlag,
        CloneTaskType::kClone, &cloneInfo);
    if (ret < 0) {
        if (kErrCodeTaskExist == ret) {
            // 任务已存在的情况下返回成功，使接口幂等
            *taskId = cloneInfo.GetTaskId();
            closure->SetTaskId(*taskId);
            closure->SetErrCode(kErrCodeSuccess);
            return kErrCodeSuccess;
        }
        LOG(ERROR) << "CloneOrRecoverPre error"
                   << ", ret = " << ret
                   << ", source = " << source
                   << ", user = " << user
                   << ", destination = " << destination
                   << ", lazyFlag = " << lazyFlag;
        closure->SetErrCode(ret);
        return ret;
    }
    *taskId = cloneInfo.GetTaskId();

    if (lazyFlag) {
        ret = BuildAndPushCloneOrRecoverLazyTask(cloneInfo, closure);
    } else {
        ret = BuildAndPushCloneOrRecoverNotLazyTask(cloneInfo, closure);
    }
    guard.release();
    return ret;
}

int CloneServiceManager::RecoverFile(const UUID &source,
    const std::string &user,
    const std::string &destination,
    bool lazyFlag,
    std::shared_ptr<CloneClosure> closure,
    TaskIdType *taskId) {
    brpc::ClosureGuard guard(closure.get());
    CloneInfo cloneInfo;
    int ret = cloneCore_->CloneOrRecoverPre(
        source, user, destination, lazyFlag,
        CloneTaskType::kRecover, &cloneInfo);
    if (ret < 0) {
        if (kErrCodeTaskExist == ret) {
            // 任务已存在的情况下返回成功，使接口幂等
            *taskId = cloneInfo.GetTaskId();
            closure->SetTaskId(*taskId);
            closure->SetErrCode(kErrCodeSuccess);
            return kErrCodeSuccess;
        }
        LOG(ERROR) << "CloneOrRecoverPre error"
                   << ", ret = " << ret
                   << ", source = " << source
                   << ", user = " << user
                   << ", destination = " << destination
                   << ", lazyFlag = " << lazyFlag;
        closure->SetErrCode(ret);
        return ret;
    }
    *taskId = cloneInfo.GetTaskId();

    if (lazyFlag) {
        ret = BuildAndPushCloneOrRecoverLazyTask(cloneInfo, closure);
    } else {
        ret = BuildAndPushCloneOrRecoverNotLazyTask(cloneInfo, closure);
    }
    guard.release();
    return ret;
}

int CloneServiceManager::BuildAndPushCloneOrRecoverLazyTask(
    CloneInfo cloneInfo,
    std::shared_ptr<CloneClosure> closure) {
    brpc::ClosureGuard guard(closure.get());
    TaskIdType taskId = cloneInfo.GetTaskId();
    auto cloneInfoMetric =
        std::make_shared<CloneInfoMetric>(taskId);
    closure->SetTaskId(taskId);

    std::shared_ptr<CloneTaskInfo> taskInfo =
        std::make_shared<CloneTaskInfo>(cloneInfo,
                    cloneInfoMetric, closure);
    taskInfo->UpdateMetric();

    std::shared_ptr<CloneTask> task =
        std::make_shared<CloneTask>(
            taskId, taskInfo, cloneCore_);
    int ret = cloneTaskMgr_->PushStage1Task(task);
    if (ret < 0) {
        LOG(ERROR) << "CloneTaskMgr Push Task error"
                   << ", ret = " << ret
                   << ", going to remove task info.";
        int ret2 = cloneCore_->HandleRemoveCloneOrRecoverTask(
            taskInfo);
        if (ret2 < 0) {
            LOG(ERROR) << "CloneServiceManager has encouter an internal error,"
                       << "remove taskInfo fail !";
        }
        closure->SetErrCode(ret);
        return ret;
    }
    guard.release();
    return kErrCodeSuccess;
}

int CloneServiceManager::BuildAndPushCloneOrRecoverNotLazyTask(
    CloneInfo cloneInfo,
    std::shared_ptr<CloneClosure> closure) {
    brpc::ClosureGuard guard(closure.get());
    TaskIdType taskId = cloneInfo.GetTaskId();
    auto cloneInfoMetric =
        std::make_shared<CloneInfoMetric>(taskId);
    closure->SetTaskId(taskId);

    std::shared_ptr<CloneTaskInfo> taskInfo =
        std::make_shared<CloneTaskInfo>(cloneInfo,
                cloneInfoMetric, nullptr);
    taskInfo->UpdateMetric();

    std::shared_ptr<CloneTask> task =
        std::make_shared<CloneTask>(
            taskId, taskInfo, cloneCore_);
    int ret = cloneTaskMgr_->PushCommonTask(task);
    if (ret < 0) {
        LOG(ERROR) << "CloneTaskMgr Push Task error"
                   << ", ret = " << ret
                   << ", going to remove task info.";
        int ret2 = cloneCore_->HandleRemoveCloneOrRecoverTask(
            taskInfo);
        if (ret2 < 0) {
            LOG(ERROR) << "CloneServiceManager has encouter an internal error,"
                       << "remove taskInfo fail !";
        }
    }
    closure->SetErrCode(ret);
    return kErrCodeSuccess;
}

int CloneServiceManager::Flatten(
    const std::string &user,
    const TaskIdType &taskId) {
    CloneInfo cloneInfo;
    int ret = cloneCore_->FlattenPre(user, taskId, &cloneInfo);
    if (kErrCodeTaskExist == ret) {
        return kErrCodeSuccess;
    } else if (ret < 0) {
        LOG(ERROR) << "FlattenPre error"
                   << ", ret = " << ret
                   << ", user = " << user
                   << ", taskId = " << taskId;
        return ret;
    }

    auto cloneInfoMetric = std::make_shared<CloneInfoMetric>(taskId);
    auto closure = std::make_shared<CloneClosure>();
    std::shared_ptr<CloneTaskInfo> taskInfo =
        std::make_shared<CloneTaskInfo>(
            cloneInfo, cloneInfoMetric, closure);
    std::shared_ptr<CloneTask> task =
        std::make_shared<CloneTask>(
            cloneInfo.GetTaskId(), taskInfo, cloneCore_);
    ret = cloneTaskMgr_->PushStage2Task(task);
    if (ret < 0) {
        LOG(ERROR) << "CloneTaskMgr Push Stage2 Task error"
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
        return kErrCodeFileNotExist;
    }
    return GetCloneTaskInfoInner(cloneInfos, user, info);
}

int CloneServiceManager::GetCloneTaskInfoById(
    const std::string &user,
    const TaskIdType &taskId,
    std::vector<TaskCloneInfo> *info) {
    std::vector<CloneInfo> cloneInfos;
    CloneInfo cloneInfo;
    int ret = cloneCore_->GetCloneInfo(taskId, &cloneInfo);
    if (ret < 0) {
        LOG(ERROR) << "GetCloneInfo fail"
                   << ", ret = " << ret
                   << ", taskId = " << taskId;
        return kErrCodeFileNotExist;
    }
    if (cloneInfo.GetUser() != user) {
        return kErrCodeInvalidUser;
    }
    cloneInfos.push_back(cloneInfo);
    return GetCloneTaskInfoInner(cloneInfos, user, info);
}

int CloneServiceManager::GetCloneTaskInfoByName(
    const std::string &user,
    const std::string &fileName,
    std::vector<TaskCloneInfo> *info) {
    std::vector<CloneInfo> cloneInfos;
    int ret = cloneCore_->GetCloneInfoByFileName(fileName, &cloneInfos);
    if (ret < 0) {
        LOG(ERROR) << "GetCloneInfoByFileName fail"
                   << ", ret = " << ret
                   << ", fileName = " << fileName;
        return kErrCodeFileNotExist;
    }
    return GetCloneTaskInfoInner(cloneInfos, user, info);
}

int CloneServiceManager::GetCloneTaskInfoInner(
    std::vector<CloneInfo> cloneInfos,
    const std::string &user,
    std::vector<TaskCloneInfo> *info) {
    int ret = kErrCodeSuccess;
    for (auto &cloneInfo : cloneInfos) {
        if (cloneInfo.GetUser() == user) {
            switch (cloneInfo.GetStatus()) {
                case CloneStatus::done : {
                    info->emplace_back(cloneInfo, kProgressCloneComplete);
                    break;
                }
                case CloneStatus::cleaning:
                case CloneStatus::errorCleaning:
                case CloneStatus::error:
                case CloneStatus::retrying: {
                    info->emplace_back(cloneInfo, kProgressCloneError);
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
                        TaskCloneInfo tcInfo;
                        ret = GetFinishedCloneTask(taskId, &tcInfo);
                        if (ret < 0) {
                            return ret;
                        }
                        info->emplace_back(tcInfo);
                    }
                    break;
                }
                case CloneStatus::metaInstalled: {
                    info->emplace_back(cloneInfo, kProgressMetaInstalled);
                    break;
                }
                default:
                    LOG(ERROR) << "can not reach here!, status = "
                               << static_cast<int>(cloneInfo.GetStatus());
                    return kErrCodeInternalError;
            }
        }
    }
    return kErrCodeSuccess;
}

int CloneServiceManager::GetFinishedCloneTask(
    const TaskIdType &taskId,
    TaskCloneInfo *taskCloneInfoOut) {
    CloneInfo newInfo;
    int ret = cloneCore_->GetCloneInfo(taskId, &newInfo);
    if (ret < 0) {
        LOG(ERROR) << "GetCloneInfo fail"
                   << ", ret = " << ret
                   << ", taskId = " << taskId;
        return ret;
    }
    switch (newInfo.GetStatus()) {
        case CloneStatus::done : {
            taskCloneInfoOut->SetCloneInfo(newInfo);
            taskCloneInfoOut->SetCloneProgress(kProgressCloneComplete);
            break;
        }
        case CloneStatus::error: {
            taskCloneInfoOut->SetCloneInfo(newInfo);
            taskCloneInfoOut->SetCloneProgress(kProgressCloneError);
            break;
        }
        case CloneStatus::metaInstalled: {
            taskCloneInfoOut->SetCloneInfo(newInfo);
            taskCloneInfoOut->SetCloneProgress(kProgressMetaInstalled);
            break;
        }
        default:
            LOG(ERROR) << "can not reach here!"
                       << " status = " << static_cast<int>(
                               newInfo.GetStatus());
            // 当更新数据库失败时，有可能进入这里
            return kErrCodeInternalError;
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
        std::make_shared<CloneTaskInfo>(cloneInfo, nullptr, nullptr);
    std::shared_ptr<CloneCleanTask> task =
        std::make_shared<CloneCleanTask>(
            cloneInfo.GetTaskId(), taskInfo, cloneCore_);
    ret = cloneTaskMgr_->PushCommonTask(task);
    if (ret < 0) {
        LOG(ERROR) << "Push Task error, "
                   << " ret = " << ret;
        return ret;
    }
    return kErrCodeSuccess;
}

int CloneServiceManager::RecoverCloneTaskInternal(const CloneInfo &cloneInfo) {
    auto cloneInfoMetric =
        std::make_shared<CloneInfoMetric>(cloneInfo.GetTaskId());
    auto closure = std::make_shared<CloneClosure>();
    std::shared_ptr<CloneTaskInfo> taskInfo =
        std::make_shared<CloneTaskInfo>(
            cloneInfo, cloneInfoMetric, closure);
    taskInfo->UpdateMetric();
    std::shared_ptr<CloneTask> task =
        std::make_shared<CloneTask>(
            cloneInfo.GetTaskId(), taskInfo, cloneCore_);
    bool isLazy = cloneInfo.GetIsLazy();
    int ret = kErrCodeSuccess;
    // Lazy 克隆/恢复
    if (isLazy) {
        CloneStep step = cloneInfo.GetNextStep();
        // 处理这三个阶段的Push到stage2Pool
        if (CloneStep::kRecoverChunk == step ||
            CloneStep::kCompleteCloneFile == step ||
            CloneStep::kEnd == step) {
            ret = cloneTaskMgr_->PushStage2Task(task);
            if (ret < 0) {
                LOG(ERROR) << "CloneTaskMgr Push Stage2 Task error"
                           << ", ret = " << ret;
                return ret;
            }
        // 否则push到stage1Pool
        } else {
            ret = cloneTaskMgr_->PushStage1Task(task);
            if (ret < 0) {
                LOG(ERROR) << "CloneTaskMgr Push Stage1 Task error"
                           << ", ret = " << ret;
                return ret;
            }
        }
    // 非Lazy 克隆/恢复push到commonPool
    } else {
        ret = cloneTaskMgr_->PushCommonTask(task);
        if (ret < 0) {
            LOG(ERROR) << "CloneTaskMgr Push Task error"
                       << ", ret = " << ret;
            return ret;
        }
    }
    return kErrCodeSuccess;
}

int CloneServiceManager::RecoverCleanTaskInternal(const CloneInfo &cloneInfo) {
    std::shared_ptr<CloneTaskInfo> taskInfo =
        std::make_shared<CloneTaskInfo>(
            cloneInfo, nullptr, nullptr);
    std::shared_ptr<CloneTask> task =
        std::make_shared<CloneTask>(
            cloneInfo.GetTaskId(), taskInfo, cloneCore_);
    int ret = cloneTaskMgr_->PushCommonTask(task);
    if (ret < 0) {
        LOG(ERROR) << "CloneTaskMgr Push Task error"
                   << ", ret = " << ret;
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
            case CloneStatus::retrying: {
                // 重置重试任务的状态
                if (cloneInfo.GetTaskType() == CloneTaskType::kClone) {
                    cloneInfo.SetStatus(CloneStatus::cloning);
                } else {
                    cloneInfo.SetStatus(CloneStatus::recovering);
                }
            }
            case CloneStatus::cloning:
            case CloneStatus::recovering: {
                // 建立快照或镜像的引用关系
                if (CloneFileType::kSnapshot == cloneInfo.GetFileType()) {
                    cloneCore_->GetSnapshotRef()->IncrementSnapshotRef(
                        cloneInfo.GetSrc());
                } else {
                    cloneCore_->GetCloneRef()->IncrementRef(
                        cloneInfo.GetSrc());
                }
                ret = RecoverCloneTaskInternal(cloneInfo);
                if (ret < 0) {
                    return ret;
                }
                break;
            }
            case CloneStatus::cleaning:
            case CloneStatus::errorCleaning: {
                ret = RecoverCleanTaskInternal(cloneInfo);
                if (ret < 0) {
                    return ret;
                }
                break;
            }
            case CloneStatus::metaInstalled: {
                // metaInstalled 状态下的克隆对文件仍然有依赖，需要建立引用关系
                if (CloneFileType::kSnapshot == cloneInfo.GetFileType()) {
                    cloneCore_->GetSnapshotRef()->IncrementSnapshotRef(
                        cloneInfo.GetSrc());
                } else {
                    cloneCore_->GetCloneRef()->IncrementRef(
                        cloneInfo.GetSrc());
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

