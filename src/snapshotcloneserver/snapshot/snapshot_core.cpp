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

#include "src/snapshotcloneserver/snapshot/snapshot_core.h"

#include <glog/logging.h>
#include <utility>
#include <algorithm>
#include <list>

#include "src/common/snapshotclone/snapshotclone_define.h"
#include "src/snapshotcloneserver/snapshot/snapshot_task.h"

#include "src/common/uuid.h"

using ::curve::common::UUIDGenerator;
using ::curve::common::NameLockGuard;
using ::curve::common::LockGuard;

namespace curve {
namespace snapshotcloneserver {

int SnapshotCoreImpl::Init() {
    stopFlag_ = false;

    int ret = threadPool_->Start();
    if (ret < 0) {
        LOG(ERROR) << "SnapshotCoreImpl, thread start fail, ret = " << ret;
        return ret;
    }

    // start check thread
    checkThread_ = new ::curve::common::Thread
        (&SnapshotCoreImpl::CheckLocalSnapshot, this);
    LOG(INFO) << "snapshot core start check local snapshot thread success.";

    return kErrCodeSuccess;
}

void SnapshotCoreImpl::CheckLocalSnapshot() {
    while (sleeper_.wait_for(std::chrono::milliseconds(checkPeriod_))) {
        std::vector<SnapshotInfo> tmp;
        metaStore_->GetSnapshotList(&tmp);
        for (auto it = tmp.begin(); it != tmp.end(); ++it) {
            if (it->GetLocation() != LocationType::kLocationCurve) {
                continue;
            }
            NameLockGuard lockGuard(snapshotNameLock_, it->GetFileName());
            Status status;
            uint32_t progress;
            int ret = GetLocalSnapshotStatus(it->GetFileName(),
                it->GetUser(),
                it->GetSeqNum(),
                &status,
                &progress);
            if (kErrCodeFileNotExist == ret) {
                ret = metaStore_->DeleteSnapshot(it->GetUuid());
                if (kErrCodeSuccess == ret) {
                    tmp.erase(it);
                    LOG(INFO) << "DeleteSnapshot from metastore success"
                              << ", uuid = " << it->GetUuid()
                              << ", fileName = " << it->GetFileName()
                              << ", user = " << it->GetUser()
                              << ", snapName = " << it->GetSnapshotName()
                              << ", seqNum = " << it->GetSeqNum();
                } else {
                    LOG(ERROR) << "DeleteSnapshot from metastore error"
                               << ", ret = " << ret
                               << ", uuid = " << it->GetUuid()
                               << ", fileName = " << it->GetFileName()
                               << ", user = " << it->GetUser()
                               << ", snapName = " << it->GetSnapshotName()
                               << ", seqNum = " << it->GetSeqNum();
                }
            } else if (kErrCodeSuccess == ret) {
                // exist
            } else {
                LOG(ERROR) << "GetLocalSnapshotStatus error"
                           << ", ret = " << ret
                           << ", uuid = " << it->GetUuid()
                           << ", fileName = " << it->GetFileName()
                           << ", user = " << it->GetUser()
                           << ", snapName = " << it->GetSnapshotName()
                           << ", seqNum = " << it->GetSeqNum();
            }
        }
    }
}

int SnapshotCoreImpl::CreateSnapshotPre(const std::string &file,
    const std::string &user,
    const std::string &snapshotName,
    SnapshotInfo *snapInfo) {
    NameLockGuard lockGuard(snapshotNameLock_, file);
    std::vector<SnapshotInfo> fileInfo;
    metaStore_->GetSnapshotList(file, &fileInfo);
    int snapshotNum = fileInfo.size();
    for (auto& snap : fileInfo) {
        if (Status::pending == snap.GetStatus()) {
            if ((snap.GetUser() == user) &&
                (snap.GetSnapshotName() == snapshotName)) {
                LOG(INFO) << "CreateSnapshotPre find same snap task"
                          << ", file = " << file
                          << ", user = " << user
                          << ", snapshotName = " << snapshotName
                          << ", Exist SnapInfo : " << snap;
                // 视为同一个快照，返回任务已存在
                *snapInfo = snap;
                return kErrCodeTaskExist;
            }
        }
        if (Status::error == snap.GetStatus()) {
            snapshotNum--;
        }
    }
    if (snapshotNum >= maxSnapshotLimit_) {
        LOG(ERROR) << "Snapshot count reach the max limit.";
        return kErrCodeSnapshotCountReachLimit;
    }

    FInfo fInfo;
    int ret = client_->GetFileInfo(file, user, &fInfo);
    ret = LibCurveErrToSnapshotCloneErr(ret);
    if (ret != kErrCodeSuccess) {
        LOG(ERROR) << "GetFileInfo encounter an error"
                   << ", ret = " << ret
                   << ", file = " << file
                   << ", user = " << user;
        return ret;
    }

    if (fInfo.filestatus != FileStatus::Created &&
        fInfo.filestatus != FileStatus::Cloned) {
        LOG(ERROR) << "Can not create snapshot when file status = "
                   << static_cast<int>(fInfo.filestatus);
        return kErrCodeFileStatusInvalid;
    }

    UUID uuid = UUIDGenerator().GenerateUUID();
    SnapshotInfo info(uuid, user, file, snapshotName);
    info.SetLocation(LocationType::kLocationS3);
    info.SetPoolset(fInfo.poolset);
    info.SetStatus(Status::pending);
    ret = metaStore_->AddSnapshot(info);
    if (ret < 0) {
        LOG(ERROR) << "AddSnapshot error,"
                   << " ret = " << ret
                   << ", uuid = " << uuid
                   << ", fileName = " << file
                   << ", snapshotName = " << snapshotName;
        return ret;
    }
    *snapInfo = info;
    return kErrCodeSuccess;
}

SnapshotInfo BuildNewSnapshotInfo(const std::string &file,
    const std::string &user,
    const std::string &snapshotName,
    const FInfo &fi) {
    UUID uuid = UUIDGenerator().GenerateUUID();
    SnapshotInfo info(uuid, user, file, snapshotName);
    info.SetLocation(LocationType::kLocationCurve);
    info.SetSeqNum(fi.seqnum);
    info.SetChunkSize(fi.chunksize);
    info.SetSegmentSize(fi.segmentsize);
    info.SetFileLength(fi.length);
    info.SetStripeUnit(fi.stripeUnit);
    info.SetStripeCount(fi.stripeCount);
    info.SetPoolset(fi.poolset);
    info.SetCreateTime(fi.ctime);
    info.SetStatus(Status::done);
    return info;
}

int SnapshotCoreImpl::CreateLocalSnapshot(const std::string &file,
    const std::string &user,
    const std::string &snapshotName,
    SnapshotInfo *snapInfo) {
    NameLockGuard lockGuard(snapshotNameLock_, file);
    std::string snapPath = MakeSnapshotPath(file, snapshotName);
    FInfo fi;
    int ret =
        client_->CreateSnapshot(snapPath, user, &fi);
    ret = LibCurveErrToSnapshotCloneErr(ret);
    if (ret != kErrCodeSuccess &&
        ret != kErrCodeFileExist) {
        LOG(ERROR) << "CreateLocalSnapshot error"
                   << ", ret = " << ret
                   << ", file = " << file
                   << ", user = " << user
                   << ", snapshotName = " << snapshotName;
        return ret;
    }

    if (kErrCodeFileExist == ret) {
        if (fi.filestatus != FileStatus::Created) {
            LOG(ERROR) << "CreateLocalSnapshot find exist snapshot"
                       << ", maybe snapshot is deleting"
                       << ", file = " << file
                       << ", user = " << user
                       << ", snapshotName = " << snapshotName
                       << ", snap file status = "
                       << static_cast<int>(fi.filestatus);
            return kErrCodeFileExist;
        }

        LOG(INFO) << "CreateLocalSnapshot find exist snapshot"
                  << ", file = " << file
                  << ", user = " << user
                  << ", snapshotName = " << snapshotName
                  << ", snap file status = "
                  << static_cast<int>(fi.filestatus);
    }

    ret = client_->ProtectSnapshot(snapPath, user);
    ret = LibCurveErrToSnapshotCloneErr(ret);
    if (ret != kErrCodeSuccess) {
        LOG(ERROR) << "ProtectSnapshot fail, ret = " << ret
                   << ", file = " << file
                   << ", user = " << user
                   << ", snapshotName = " << snapshotName;
        return ret;
    }

    uint64_t seqNum = fi.seqnum;
    ret = metaStore_->GetSnapshotInfo(file, snapshotName, snapInfo);
    if (ret == 0) {
        LOG(INFO) << "CreateLocalSnapshot find exist snapInfo"
                  << ", file = " << file
                  << ", user = " << user
                  << ", snapshotName = " << snapshotName
                  << ", seqNum = " << seqNum
                  << ", Exist SnapInfo : " << *snapInfo;
        // exist old deleteing SnapshotInfo, replace it
        if (snapInfo->GetSeqNum() != seqNum) {
            ret = metaStore_->DeleteSnapshot(snapInfo->GetUuid());
            if (ret < 0) {
                LOG(ERROR) << "DeleteSnapshot error,"
                           << " ret = " << ret
                           << ", uuid = " << snapInfo->GetUuid()
                           << ", fileName = " << file
                           << ", snapshotName = " << snapshotName;
                return kErrCodeInternalError;
            }
        } else {
            return kErrCodeSuccess;
        }
    }

    *snapInfo = BuildNewSnapshotInfo(file, user, snapshotName, fi);
    // maybe exist old deleteing snapInfo, use UpdateSnapshot
    ret = metaStore_->AddSnapshot(*snapInfo);
    if (ret < 0) {
        LOG(ERROR) << "UpdateSnapshot error,"
                   << " ret = " << ret
                   << ", uuid = " << snapInfo->GetUuid()
                   << ", fileName = " << file
                   << ", user = " << user
                   << ", snapshotName = " << snapshotName;
        return kErrCodeInternalError;
    }
    LOG(INFO) << "CreateLocalSnapshot success, seq = " << seqNum
              << ", uuid = " << snapInfo->GetUuid()
              << ", fileName = " << file
              << ", user = " << user
              << ", snapshotName = " << snapshotName;
    return kErrCodeSuccess;
}

constexpr uint32_t kProgressCreateSnapshotOnCurvefsComplete = 5;
constexpr uint32_t kProgressBuildChunkIndexDataComplete = 6;
constexpr uint32_t kProgressBuildSnapshotMapComplete = 10;
constexpr uint32_t kProgressTransferSnapshotDataStart =
                       kProgressBuildSnapshotMapComplete;
constexpr uint32_t kProgressTransferSnapshotDataComplete = 99;
constexpr uint32_t kProgressComplete = 100;

/**
 * @brief 异步执行创建快照任务并更新任务进度
 *
 * 快照进度规划如下:
 *
 *  |CreateSnapshotOnCurvefs| BuildChunkIndexData | BuildSnapshotMap | TransferSnapshotData | UpdateSnapshot | //NOLINT
 *  | 5%                    | 6%                  | 10%              | 10%~99%              | 100%           | //NOLINT
 *
 *
 *  异步执行期间发生error与cancel情况说明：
 *  1. 发生error将导致整个异步任务直接中断，并且不做任何清理动作:
 *  发生error时，一般系统存在异常，清理动作很可能不能完成，
 *  因此，不进行任何清理，只置状态，待人工干预排除异常之后，
 *  使用DeleteSnapshot功能去手动删除error状态的快照。
 *  2. 发生cancel时则以创建功能相反的顺序依次进行清理动作，
 *  若清理过程发生error,则立即中断，之后同error过程。
 *
 * @param task 快照任务
 */
void SnapshotCoreImpl::HandleCreateSnapshotTask(
    std::shared_ptr<SnapshotTaskInfo> task) {
    std::string fileName = task->GetFileName();

    // 如果当前有失败的快照，需先清理失败的快照，否则快照会再次失败
    int ret = ClearErrorSnapBeforeCreateSnapshot(task);
    if (ret < 0) {
        HandleCreateSnapshotError(task);
        return;
    }

    // 为支持任务重启，这里有三种情况需要处理
    // 1. 没打过快照, 没有seqNum且curve上没有快照
    // 2. 打过快照, 有seqNum且curve上有快照
    // 3. 打过快照并已经转储完删除快照, 有seqNum但curve上没有快照

    SnapshotInfo *info = &(task->GetSnapshotInfo());
    UUID uuid = task->GetUuid();
    uint64_t seqNum = info->GetSeqNum();
    bool existIndexData = false;
    if (kUnInitializeSeqNum == seqNum) {
        ret = CreateSnapshotOnCurvefs(fileName, info, task);
        if (ret < 0) {
            LOG(ERROR) << "CreateSnapshotOnCurvefs error, "
                       << " ret = " << ret
                       << ", fileName = " << fileName
                       << ", uuid = " << task->GetUuid();
            HandleCreateSnapshotError(task);
            return;
        }
        seqNum = info->GetSeqNum();
        existIndexData = false;
    } else {
        FInfo snapInfo;
        ret = client_->GetSnapshot(fileName,
            info->GetUser(),
            seqNum, &snapInfo);
        if (-LIBCURVE_ERROR::NOTEXIST == ret) {
            HandleCreateSnapshotSuccess(task);
            return;
        } else if (LIBCURVE_ERROR::OK == ret) {
            ChunkIndexDataName name(fileName, seqNum);
            // judge Is Exist indexData
            existIndexData = dataStore_->ChunkIndexDataExist(name);
        } else {
            LOG(ERROR) << "GetSnapShot on curvefs fail, "
                       << " ret = " << ret
                       << ", fileName = " << fileName
                       << ", user = " << info->GetUser()
                       << ", seqNum = " << seqNum
                       << ", uuid = " << task->GetUuid();
            HandleCreateSnapshotError(task);
            return;
        }
    }

    task->SetProgress(kProgressCreateSnapshotOnCurvefsComplete);
    task->UpdateMetric();
    if (task->IsCanceled()) {
        return CancelAfterCreateSnapshotOnCurvefs(task);
    }

    ChunkIndexData indexData;
    ChunkIndexDataName name(fileName, seqNum);
    // the key is segment index
    std::map<uint64_t, SegmentInfo> segInfos;
    if (existIndexData) {
        ret = dataStore_->GetChunkIndexData(name, &indexData);
        if (ret < 0) {
            LOG(ERROR) << "GetChunkIndexData error, "
                       << " ret = " << ret
                       << ", fileName = " << fileName
                       << ", seqNum = " << seqNum
                       << ", uuid = " << task->GetUuid();
            HandleCreateSnapshotError(task);
            return;
        }

        task->SetProgress(kProgressBuildChunkIndexDataComplete);
        task->UpdateMetric();

        ret = BuildSegmentInfo(*info, &segInfos);
        if (ret < 0) {
            LOG(ERROR) << "BuildSegmentInfo error,"
                       << " ret = " << ret
                       << ", uuid = " << task->GetUuid();
            HandleCreateSnapshotError(task);
            return;
        }
    } else {
        ret = BuildChunkIndexData(*info, &indexData, &segInfos, task);
        if (ret < 0) {
            LOG(ERROR) << "BuildChunkIndexData error, "
                       << " ret = " << ret
                       << ", uuid = " << task->GetUuid();
            HandleCreateSnapshotError(task);
            return;
        }

        ret = dataStore_->PutChunkIndexData(name, indexData);
        if (ret < 0) {
            LOG(ERROR) << "PutChunkIndexData error, "
                       << " ret = " << ret
                       << ", uuid = " << task->GetUuid();
            HandleCreateSnapshotError(task);
            return;
        }

        task->SetProgress(kProgressBuildChunkIndexDataComplete);
        task->UpdateMetric();
    }

    if (task->IsCanceled()) {
        return CancelAfterCreateChunkIndexData(task);
    }

    FileSnapMap fileSnapshotMap;
    ret = BuildSnapshotMap(fileName,
        seqNum,
        &fileSnapshotMap);
    if (ret < 0) {
        LOG(ERROR) << "BuildSnapshotMap error, "
                   << " fileName = " << task->GetFileName()
                   << ", seqNum = " << seqNum
                   << ", uuid = " << task->GetUuid();
        HandleCreateSnapshotError(task);
        return;
    }
    task->SetProgress(kProgressBuildSnapshotMapComplete);
    task->UpdateMetric();

    if (existIndexData) {
        ret = TransferSnapshotData(indexData,
            *info,
            segInfos,
            [this] (const ChunkDataName &chunkDataName) {
                return dataStore_->ChunkDataExist(chunkDataName);
            },
            task);
    } else {
        ret = TransferSnapshotData(indexData,
            *info,
            segInfos,
            [&fileSnapshotMap] (const ChunkDataName &chunkDataName) {
                return fileSnapshotMap.IsExistChunk(chunkDataName);
            },
            task);
    }
    if (ret < 0) {
        LOG(ERROR) << "TransferSnapshotData error, "
                   << " ret = " << ret
                   << ", uuid = " << task->GetUuid();
        HandleCreateSnapshotError(task);
        return;
    }
    task->SetProgress(kProgressTransferSnapshotDataComplete);
    task->UpdateMetric();

    if (task->IsCanceled()) {
        return CancelAfterTransferSnapshotData(
            task, indexData, fileSnapshotMap);
    }

    ret = DeleteSnapshotOnCurvefs(*info);
    if (ret < 0) {
        LOG(ERROR) << "DeleteSnapshotOnCurvefs fail"
                   << ", uuid = " << task->GetUuid();
        HandleCreateSnapshotError(task);
        return;
    }

    LockGuard lockGuard(task->GetLockRef());
    if (task->IsCanceled()) {
        return CancelAfterTransferSnapshotData(
            task, indexData, fileSnapshotMap);
    }

    HandleCreateSnapshotSuccess(task);
    return;
}

int SnapshotCoreImpl::ClearErrorSnapBeforeCreateSnapshot(
    std::shared_ptr<SnapshotTaskInfo> task) {
    std::vector<SnapshotInfo> snapVec;
    metaStore_->GetSnapshotList(task->GetFileName(), &snapVec);
    for (auto& snap : snapVec) {
        if (Status::error == snap.GetStatus()) {
            auto snapInfoMetric =
                std::make_shared<SnapshotInfoMetric>(snap.GetUuid());
            std::shared_ptr<SnapshotTaskInfo> taskInfo =
                std::make_shared<SnapshotTaskInfo>(snap, snapInfoMetric);
            taskInfo->GetSnapshotInfo().SetStatus(Status::errorDeleting);
            taskInfo->UpdateMetric();
            // 处理删除快照
            HandleDeleteSnapshotTask(taskInfo);
            // 仍然失败，则本次快照失败
            if (taskInfo->GetSnapshotInfo().GetStatus() != Status::done) {
                LOG(ERROR) << "Find error Snapshot and Delete Fail"
                           << ", error snapshot Id = " << snap.GetUuid()
                           << ", uuid = " << task->GetUuid();

                return kErrCodeSnapshotCannotCreateWhenError;
            }
        }
    }
    return kErrCodeSuccess;
}

int SnapshotCoreImpl::StartCancel(
    std::shared_ptr<SnapshotTaskInfo> task) {
    auto &snapInfo = task->GetSnapshotInfo();
    snapInfo.SetStatus(Status::canceling);
    int ret = metaStore_->UpdateSnapshot(snapInfo);
    if (ret < 0) {
        LOG(ERROR) << "UpdateSnapshot Task Cancel Fail!"
                   << " ret = " << ret
                   << ", uuid = " << task->GetUuid();
        HandleCreateSnapshotError(task);
        return kErrCodeInternalError;
    }
    return kErrCodeSuccess;
}

void SnapshotCoreImpl::CancelAfterTransferSnapshotData(
    std::shared_ptr<SnapshotTaskInfo> task,
    const ChunkIndexData &indexData,
    const FileSnapMap &fileSnapshotMap) {
    LOG(INFO) << "Cancel After TransferSnapshotData"
              << ", uuid = " << task->GetUuid();
    std::vector<ChunkIndexType> chunkIndexVec = indexData.GetAllChunkIndex();
    for (auto &chunkIndex : chunkIndexVec) {
        ChunkDataName chunkDataName;
        indexData.GetChunkDataName(chunkIndex, &chunkDataName);
        if ((!fileSnapshotMap.IsExistChunk(chunkDataName)) &&
            (dataStore_->ChunkDataExist(chunkDataName))) {
            int ret =  dataStore_->DeleteChunkData(chunkDataName);
            if (ret < 0) {
                LOG(ERROR) << "DeleteChunkData error"
                           << "while canceling CreateSnapshot, "
                           << " ret = " << ret
                           << ", fileName = " << task->GetFileName()
                           << ", seqNum = " << chunkDataName.chunkSeqNum_
                           << ", chunkIndex = " << chunkDataName.chunkIndex_
                           << ", uuid = " << task->GetUuid();
                HandleCreateSnapshotError(task);
                return;
            }
        }
    }
    CancelAfterCreateChunkIndexData(task);
}

void SnapshotCoreImpl::CancelAfterCreateChunkIndexData(
    std::shared_ptr<SnapshotTaskInfo> task) {
    LOG(INFO) << "Cancel After CreateChunkIndexData"
              << ", uuid = " << task->GetUuid();
    SnapshotInfo &info = task->GetSnapshotInfo();
    UUID uuid = task->GetUuid();
    uint64_t seqNum = info.GetSeqNum();
    ChunkIndexDataName name(task->GetFileName(),
        seqNum);
    int ret = dataStore_->DeleteChunkIndexData(name);
    if (ret < 0) {
        LOG(ERROR) << "DeleteChunkIndexData error "
                   << "while canceling CreateSnapshot, "
                   << " ret = " << ret
                   << ", fileName = " << task->GetFileName()
                   << ", seqNum = " << seqNum
                  << ", uuid = " << task->GetUuid();
        HandleCreateSnapshotError(task);
        return;
    }
    CancelAfterCreateSnapshotOnCurvefs(task);
}

void SnapshotCoreImpl::CancelAfterCreateSnapshotOnCurvefs(
    std::shared_ptr<SnapshotTaskInfo> task) {
    LOG(INFO) << "Cancel After CreateSnapshotOnCurvefs"
              << ", uuid = " << task->GetUuid();
    SnapshotInfo &info = task->GetSnapshotInfo();
    UUID uuid = task->GetUuid();

    int ret = DeleteSnapshotOnCurvefs(info);
    if (ret < 0) {
        LOG(ERROR) << "DeleteSnapshotOnCurvefs fail"
                   << ", uuid = " << task->GetUuid();
        HandleCreateSnapshotError(task);
        return;
    }
    HandleClearSnapshotOnMateStore(task);
}

void SnapshotCoreImpl::HandleClearSnapshotOnMateStore(
    std::shared_ptr<SnapshotTaskInfo> task) {
    int ret = metaStore_->DeleteSnapshot(task->GetUuid());
    if (ret < 0) {
        LOG(ERROR) << "MetaStore DeleteSnapshot error "
                   << "while cancel CreateSnapshot, "
                   << " ret = " << ret
                   << ", uuid = " << task->GetUuid();
        HandleCreateSnapshotError(task);
        return;
    }

    auto &snapInfo = task->GetSnapshotInfo();
    LOG(INFO) << "CancelSnapshot Task Success"
              << ", uuid = " << snapInfo.GetUuid()
              << ", fileName = " << snapInfo.GetFileName()
              << ", snapshotName = " << snapInfo.GetSnapshotName()
              << ", seqNum = " << snapInfo.GetSeqNum()
              << ", createTime = " << snapInfo.GetCreateTime();
    task->GetSnapshotInfo().SetStatus(Status::done);
    task->Finish();
    return;
}

void SnapshotCoreImpl::HandleCreateSnapshotSuccess(
    std::shared_ptr<SnapshotTaskInfo> task) {
    auto &snapInfo = task->GetSnapshotInfo();
    snapInfo.SetStatus(Status::done);
    int ret = metaStore_->UpdateSnapshot(snapInfo);
    if (ret < 0) {
        LOG(ERROR) << "UpdateSnapshot Task Success Fail!"
                   << " ret = " << ret
                   << ", uuid = " << task->GetUuid();
    }
    task->SetProgress(kProgressComplete);

    LOG(INFO) << "CreateSnapshot Task Success"
              << ", uuid = " << snapInfo.GetUuid()
              << ", fileName = " << snapInfo.GetFileName()
              << ", snapshotName = " << snapInfo.GetSnapshotName()
              << ", seqNum = " << snapInfo.GetSeqNum()
              << ", createTime = " << snapInfo.GetCreateTime();
    task->Finish();
    return;
}

void SnapshotCoreImpl::HandleCreateSnapshotError(
    std::shared_ptr<SnapshotTaskInfo> task) {
    auto &snapInfo = task->GetSnapshotInfo();
    snapInfo.SetStatus(Status::error);
    int ret = metaStore_->UpdateSnapshot(snapInfo);
    if (ret < 0) {
        LOG(ERROR) << "UpdateSnapshot Task Error Fail!"
                   << " ret = " << ret
                   << ", uuid = " << task->GetUuid();
    }

    LOG(INFO) << "CreateSnapshot Task Fail"
              << ", uuid = " << snapInfo.GetUuid()
              << ", fileName = " << snapInfo.GetFileName()
              << ", snapshotName = " << snapInfo.GetSnapshotName()
              << ", seqNum = " << snapInfo.GetSeqNum()
              << ", createTime = " << snapInfo.GetCreateTime();
    task->Finish();
    return;
}

int SnapshotCoreImpl::CreateSnapshotOnCurvefs(
    const std::string &fileName,
    SnapshotInfo *info,
    std::shared_ptr<SnapshotTaskInfo> task) {
    FInfo snapInfo;
    int ret =
        client_->CreateSnapshot(fileName, info->GetUser(), &snapInfo);
    if (LIBCURVE_ERROR::OK == ret ||
        -LIBCURVE_ERROR::UNDER_SNAPSHOT == ret) {
        // ok
    } else if (-LIBCURVE_ERROR::CLIENT_NOT_SUPPORT_SNAPSHOT == ret) {
        LOG(ERROR) << "CreateSnapshot on curvefs fail, "
                   << "because the client which open file is not support snap,"
                   << " ret = " << ret;
        return kErrCodeNotSupport;
    } else {
        LOG(ERROR) << "CreateSnapshot on curvefs fail, "
                   << " ret = " << ret
                   << ", uuid = " << task->GetUuid();
        return kErrCodeInternalError;
    }
    uint64_t seqNum = snapInfo.seqnum;
    LOG(INFO) << "CreateSnapshot on curvefs success, seq = " << seqNum
              << ", uuid = " << task->GetUuid();

    info->SetSeqNum(seqNum);
    info->SetChunkSize(snapInfo.chunksize);
    info->SetSegmentSize(snapInfo.segmentsize);
    info->SetFileLength(snapInfo.length);
    info->SetStripeUnit(snapInfo.stripeUnit);
    info->SetStripeCount(snapInfo.stripeCount);
    info->SetPoolset(snapInfo.poolset);
    info->SetCreateTime(snapInfo.ctime);

    auto compareAndSet = [&](SnapshotInfo* snapinfo) {
        if (nullptr != snapinfo) {
            auto status = snapinfo->GetStatus();
            if (info->GetStatus() != status) {
                info->SetStatus(status);
            }
        }
        return info;
    };

    auto uuid = info->GetUuid();
    ret = metaStore_->CASSnapshot(uuid, compareAndSet);
    if (ret < 0) {
        LOG(ERROR) << "CASSnapshot error, "
                   << " ret = " << ret
                   << ", fileName = " << fileName
                   << ", uuid = " << task->GetUuid();
        return ret;
    }

    return kErrCodeSuccess;
}

int SnapshotCoreImpl::DeleteSnapshotOnCurvefs(const SnapshotInfo &info) {
    std::string fileName = info.GetFileName();
    std::string user = info.GetUser();
    uint64_t seqNum = info.GetSeqNum();
    int ret = client_->DeleteSnapshot(fileName,
        user,
        seqNum);
    if (ret != LIBCURVE_ERROR::OK &&
        ret != -LIBCURVE_ERROR::NOTEXIST &&
        ret != -LIBCURVE_ERROR::DELETING) {
        LOG(ERROR) << "DeleteSnapshot error, "
                   << " ret = " << ret
                   << ", fileName = " << fileName
                   << ", user = " << user
                   << ", seqNum = " << seqNum
                   << ", uuid = " << info.GetUuid();
        return kErrCodeInternalError;
    }
    do {
        FileStatus status;
        uint32_t progress = 0;
        ret = client_->CheckSnapShotStatus(info.GetFileName(),
            info.GetUser(),
            seqNum,
            &status,
            &progress);
        LOG(INFO) << "Doing CheckSnapShotStatus, fileName = "
                  << info.GetFileName()
                  << ", user = " << info.GetUser()
                  << ", seqNum = " << seqNum
                  << ", status = " << static_cast<int>(status)
                  << ", underlying progress = " << progress
                  << ", uuid = " << info.GetUuid();
        // NOTEXIST means delete succeed.
        if (-LIBCURVE_ERROR::NOTEXIST == ret) {
            LOG(INFO) << "Check snapShot delete success"
                      << ", uuid = " << info.GetUuid();
            break;
        } else if (LIBCURVE_ERROR::OK == ret) {
            if (status != FileStatus::Deleting) {
                LOG(ERROR) << "CheckSnapShotStatus fail"
                           << ", ret = " << ret
                           << ", status = " << static_cast<int>(status)
                           << ", uuid = " << info.GetUuid();
                return kErrCodeInternalError;
            }
        } else {
            LOG(ERROR) << "CheckSnapShotStatus fail"
                       << ", ret = " << ret
                       << ", uuid = " << info.GetUuid();
            return kErrCodeInternalError;
        }
        std::this_thread::sleep_for(
            std::chrono::milliseconds(checkSnapshotStatusIntervalMs_));
    } while (LIBCURVE_ERROR::OK == ret);
    return kErrCodeSuccess;
}

int SnapshotCoreImpl::BuildChunkIndexData(
    const SnapshotInfo &info,
    ChunkIndexData *indexData,
    std::map<uint64_t, SegmentInfo> *segInfos,
    std::shared_ptr<SnapshotTaskInfo> task) {
    std::string fileName = info.GetFileName();
    std::string user = info.GetUser();
    uint64_t seqNum = info.GetSeqNum();
    uint64_t fileLength = info.GetFileLength();
    uint64_t segmentSize = info.GetSegmentSize();
    uint64_t chunkSize = info.GetChunkSize();

    indexData->SetFileName(fileName);

    uint64_t chunkIndex = 0;
    for (uint64_t i = 0; i < fileLength/segmentSize; i++) {
        uint64_t offset = i * segmentSize;
        SegmentInfo segInfo;
        int ret = client_->GetSnapshotSegmentInfo(
            fileName,
            user,
            seqNum,
            offset,
            &segInfo);

        if (LIBCURVE_ERROR::OK == ret) {
            segInfos->emplace(i, segInfo);
            for (std::vector<uint64_t>::size_type j = 0;
                j < segInfo.chunkvec.size();
                j++) {
                ChunkInfoDetail chunkInfo;
                ChunkIDInfo cidInfo = segInfo.chunkvec[j];
                ret = client_->GetChunkInfo(cidInfo,
                    &chunkInfo);
                if (ret != LIBCURVE_ERROR::OK) {
                    LOG(ERROR) << "GetChunkInfo error, "
                               << " ret = " << ret
                               << ", logicalPoolId = " << cidInfo.lpid_
                               << ", copysetId = " << cidInfo.cpid_
                               << ", chunkId = " << cidInfo.cid_
                               << ", uuid = " << task->GetUuid();
                    return kErrCodeInternalError;
                }
                // 2个sn，小的是snap sn，大的是快照之后的写
                // 1个sn，有两种情况：
                //    小于等于seqNum时为snap sn, 且快照之后未写过;
                //    大于时, 表示打快照时为空，是快照之后首次写的版本(seqNum+1)
                // 没有sn，从未写过
                // 大于2个sn，错误，报错
                if (chunkInfo.chunkSn.size() == 2) {
                    uint64_t seq =
                        std::min(chunkInfo.chunkSn[0],
                                chunkInfo.chunkSn[1]);
                    chunkIndex = i * (segmentSize / chunkSize) + j;
                    ChunkDataName chunkDataName(fileName, seq, chunkIndex);
                    indexData->PutChunkDataName(chunkDataName);
                } else if (chunkInfo.chunkSn.size() == 1) {
                    uint64_t seq = chunkInfo.chunkSn[0];
                    if (seq <= seqNum) {
                        chunkIndex = i * (segmentSize / chunkSize) + j;
                        ChunkDataName chunkDataName(fileName, seq, chunkIndex);
                        indexData->PutChunkDataName(chunkDataName);
                    }
                } else if (chunkInfo.chunkSn.size() == 0) {
                    // nothing
                } else {
                    // should not reach here
                    LOG(ERROR) << "GetChunkInfo return chunkInfo.chunkSn.size()"
                               << " invalid, size = "
                               << chunkInfo.chunkSn.size()
                               << ", uuid = " << task->GetUuid();
                    return kErrCodeInternalError;
                }
                if (task->IsCanceled()) {
                    return kErrCodeSuccess;
                }
            }
        } else if (-LIBCURVE_ERROR::NOT_ALLOCATE == ret) {
            // nothing
        } else {
            LOG(ERROR) << "GetSnapshotSegmentInfo error,"
                       << " ret = " << ret
                       << ", fileName = " << fileName
                       << ", user = " << user
                       << ", seq = " << seqNum
                       << ", offset = " << offset
                       << ", uuid = " << task->GetUuid();
            return kErrCodeInternalError;
        }
    }

    return kErrCodeSuccess;
}

int SnapshotCoreImpl::BuildSegmentInfo(
    const SnapshotInfo &info,
    std::map<uint64_t, SegmentInfo> *segInfos) {
    int ret = kErrCodeSuccess;
    std::string fileName = info.GetFileName();
    std::string user = info.GetUser();
    uint64_t seq = info.GetSeqNum();
    uint64_t fileLength = info.GetFileLength();
    uint64_t segmentSize = info.GetSegmentSize();
    for (uint64_t i = 0;
        i < fileLength/segmentSize;
        i++) {
        uint64_t offset = i * segmentSize;
        SegmentInfo segInfo;
        ret = client_->GetSnapshotSegmentInfo(
            fileName,
            user,
            seq,
            offset,
            &segInfo);

        if (LIBCURVE_ERROR::OK == ret) {
            segInfos->emplace(i, std::move(segInfo));
        } else if (-LIBCURVE_ERROR::NOT_ALLOCATE == ret) {
            // nothing
        } else {
            LOG(ERROR) << "GetSnapshotSegmentInfo error,"
                       << " ret = " << ret
                       << ", fileName = " << fileName
                       << ", user = " << user
                       << ", seq = " << seq
                       << ", offset = " << offset
                       << ", uuid = " << info.GetUuid();
            return kErrCodeInternalError;
        }
    }
    return kErrCodeSuccess;
}

int SnapshotCoreImpl::TransferSnapshotData(
    const ChunkIndexData indexData,
    const SnapshotInfo &info,
    const std::map<uint64_t, SegmentInfo> &segInfos,
    const ChunkDataExistFilter &filter,
    std::shared_ptr<SnapshotTaskInfo> task) {
    int ret = 0;
    uint64_t segmentSize = info.GetSegmentSize();
    uint64_t chunkSize = info.GetChunkSize();
    uint64_t chunkPerSegment = segmentSize/chunkSize;

    if (0 == chunkSplitSize_ || chunkSize % chunkSplitSize_ != 0) {
        LOG(ERROR) << "error!, ChunkSize is not align to chunkSplitSize"
                   << ", uuid = " << task->GetUuid();
        return kErrCodeChunkSizeNotAligned;
    }

    std::vector<ChunkIndexType> chunkIndexVec = indexData.GetAllChunkIndex();

    uint32_t totalProgress = kProgressTransferSnapshotDataComplete -
        kProgressTransferSnapshotDataStart;
    uint32_t transferDataNum = chunkIndexVec.size();
    double progressPerData =
        static_cast<double>(totalProgress) / transferDataNum;
    uint32_t index = 0;

    for (auto &chunkIndex : chunkIndexVec) {
        uint64_t segNum = chunkIndex / chunkPerSegment;

        auto it = segInfos.find(segNum);
        if (it == segInfos.end()) {
            LOG(ERROR) << "TransferSnapshotData has encounter an interanl error"
                       << ": The ChunkIndexData is not match to SegmentInfo!!!"
                       << " chunkIndex = " << chunkIndex
                       << ", segNum = " << segNum
                       << ", uuid = " << task->GetUuid();
            return kErrCodeInternalError;
        }

        uint64_t chunkIndexInSegment = chunkIndex % chunkPerSegment;
        if (chunkIndexInSegment >= it->second.chunkvec.size()) {
            LOG(ERROR) << "TransferSnapshotData, "
                       << "chunkIndexInSegment >= "
                       << "segInfos[segNum].chunkvec.size()"
                       << ", chunkIndexInSegment = "
                       << chunkIndexInSegment
                       << ", size = "
                       << it->second.chunkvec.size()
                       << ", uuid = " << task->GetUuid();
            return kErrCodeInternalError;
        }
    }

    auto tracker = std::make_shared<TaskTracker>();
    for (auto &chunkIndex : chunkIndexVec) {
        ChunkDataName chunkDataName;
        indexData.GetChunkDataName(chunkIndex, &chunkDataName);
        uint64_t segNum = chunkIndex / chunkPerSegment;
        uint64_t chunkIndexInSegment = chunkIndex % chunkPerSegment;

        auto it = segInfos.find(segNum);
        if (it != segInfos.end()) {
            ChunkIDInfo cidInfo =
                it->second.chunkvec[chunkIndexInSegment];
            if (!filter(chunkDataName)) {
                auto taskInfo =
                    std::make_shared<TransferSnapshotDataChunkTaskInfo>(
                        chunkDataName, chunkSize, cidInfo, chunkSplitSize_,
                        clientAsyncMethodRetryTimeSec_,
                        clientAsyncMethodRetryIntervalMs_,
                        readChunkSnapshotConcurrency_);
                UUID taskId = UUIDGenerator().GenerateUUID();
                auto task = new TransferSnapshotDataChunkTask(
                    taskId,
                    taskInfo,
                    client_,
                    dataStore_);
                task->SetTracker(tracker);
                tracker->AddOneTrace();
                threadPool_->PushTask(task);
            }
        }
        if (tracker->GetTaskNum() >= snapshotCoreThreadNum_) {
            tracker->WaitSome(1);
        }
        ret = tracker->GetResult();
        if (ret < 0) {
            LOG(ERROR) << "TransferSnapshotDataChunk tracker GetResult fail"
                       << ", ret = " << ret
                       << ", uuid = " << task->GetUuid();
            return ret;
        }

        task->SetProgress(static_cast<uint32_t>(
                kProgressTransferSnapshotDataStart + index * progressPerData));
        task->UpdateMetric();
        index++;
        if (task->IsCanceled()) {
            return kErrCodeSuccess;
        }
    }
    // 最后剩余数量不足的任务
    tracker->Wait();
    ret = tracker->GetResult();
    if (ret < 0) {
        LOG(ERROR) << "TransferSnapshotDataChunk tracker GetResult fail"
                   << ", ret = " << ret
                   << ", uuid = " << task->GetUuid();
        return ret;
    }

    return kErrCodeSuccess;
}


int SnapshotCoreImpl::DeleteSnapshotPre(
    UUID uuid,
    const std::string &user,
    const std::string &fileName,
    SnapshotInfo *snapInfo) {
    NameLockGuard lockSnapGuard(snapshotRef_->GetSnapshotLock(), uuid);
    int ret = metaStore_->GetSnapshotInfo(uuid, snapInfo);
    if (ret < 0) {
        // 快照不存在时直接返回删除成功，使接口幂等
        return kErrCodeSuccess;
    }
    if (snapInfo->GetUser() != user) {
        LOG(ERROR) << "Can not delete snapshot by different user.";
        return kErrCodeInvalidUser;
    }
    if ((!fileName.empty()) &&
        (fileName != snapInfo->GetFileName())) {
        LOG(ERROR) << "Can not delete, fileName is not matched.";
        return kErrCodeFileNameNotMatch;
    }

    switch (snapInfo->GetStatus()) {
        case Status::done:
            snapInfo->SetStatus(Status::deleting);
            break;
        case Status::error:
            snapInfo->SetStatus(Status::errorDeleting);
            break;
        case Status::canceling:
        case Status::deleting:
        case Status::errorDeleting:
            return kErrCodeTaskExist;
        case Status::pending:
            return kErrCodeSnapshotCannotDeleteUnfinished;
        default:
            LOG(ERROR) << "Can not reach here!";
            return kErrCodeInternalError;
    }

    if (snapshotRef_->GetSnapshotRef(uuid) > 0) {
        return kErrCodeSnapshotCannotDeleteCloning;
    }

    ret = metaStore_->UpdateSnapshot(*snapInfo);
    if (ret < 0) {
        LOG(ERROR) << "UpdateSnapshot error,"
                   << " ret = " << ret
                   << ", uuid = " << uuid;
        return ret;
    }
    return kErrCodeSuccess;
}

int SnapshotCoreImpl::DeleteLocalSnapshot(
    UUID uuid,
    const std::string &user,
    const std::string &fileName) {
    NameLockGuard lockGuard(snapshotNameLock_, fileName);
    SnapshotInfo snapInfo;
    int ret = metaStore_->GetSnapshotInfo(uuid, &snapInfo);
    if (ret < 0) {
        LOG(INFO) << "DeleteLocalSnapshot found snapInfo not exist"
                  << ", uuid = " << uuid
                  << ", user = " << user
                  << ", fileName = " << fileName;
        return kErrCodeSuccess;
    }
    if (snapInfo.GetUser() != user) {
        LOG(ERROR) << "Can not delete snapshot by different user."
                   << ", uuid = " << uuid
                   << ", user = " << user
                   << ", fileName = " << fileName
                   << ", SnapInfo : " << snapInfo;
        return kErrCodeInvalidUser;
    }
    if ((!fileName.empty()) &&
        (fileName != snapInfo.GetFileName())) {
        LOG(ERROR) << "Can not delete, fileName is not matched."
                   << ", uuid = " << uuid
                   << ", user = " << user
                   << ", fileName = " << fileName
                   << ", SnapInfo : " << snapInfo;
        return kErrCodeFileNameNotMatch;
    }

    if (snapInfo.GetStatus() == Status::deleting) {
        LOG(INFO) << "DeleteLocalSnapshot found snapInfo is deleting"
                  << ", uuid = " << uuid
                  << ", user = " << user
                  << ", fileName = " << fileName
                  << ", SnapInfo : " << snapInfo;
        return kErrCodeSuccess;
    }

    std::string snapshotName = snapInfo.GetSnapshotName();
    std::string snapPath = MakeSnapshotPath(fileName, snapshotName);
    ret = client_->UnprotectSnapshot(snapPath, user);
    ret = LibCurveErrToSnapshotCloneErr(ret);
    if (ret != kErrCodeSuccess) {
        if (kErrCodeFileNotExist == ret) {
            LOG(INFO) << "UnprotectSnapshot file not exist, ret = " << ret
                      << ", file = " << fileName
                      << ", snapshotName = " << snapshotName
                      << ", user = " << user
                      << ", uuid = " << uuid;
            ret = metaStore_->DeleteSnapshot(uuid);
            if (ret < 0) {
                LOG(ERROR) << "DeleteSnapshot fail, ret = " << ret
                           << ", file = " << fileName
                           << ", snapshotName = " << snapshotName
                           << ", user = " << user
                           << ", uuid = " << uuid;
                return kErrCodeInternalError;
            }
            return kErrCodeSuccess;
        }
        LOG(ERROR) << "UnprotectSnapshot fail, ret = " << ret
                   << ", file = " << fileName
                   << ", snapshotName = " << snapshotName
                   << ", user = " << user
                   << ", uuid = " << uuid;
        return ret;
    }

    uint64_t seq = snapInfo.GetSeqNum();
    ret = client_->DeleteSnapshot(fileName, user, seq);
    ret = LibCurveErrToSnapshotCloneErr(ret);
    if (ret != kErrCodeSuccess) {
        if (kErrCodeFileNotExist == ret) {
            LOG(INFO) << "DeleteSnapshot file not exist, ret = " << ret
                      << ", file = " << fileName
                      << ", snapshotName = " << snapshotName
                      << ", user = " << user
                      << ", seq = " << seq
                      << ", uuid = " << uuid;
            ret = metaStore_->DeleteSnapshot(uuid);
            if (ret < 0) {
                LOG(ERROR) << "DeleteSnapshot fail, ret = " << ret
                           << ", file = " << fileName
                           << ", snapshotName = " << snapshotName
                           << ", user = " << user
                           << ", seq = " << seq
                           << ", uuid = " << uuid;
                return kErrCodeInternalError;
            }
            return kErrCodeSuccess;
        }
        LOG(ERROR) << "DeleteSnapshot fail, ret = " << ret
                   << ", file = " << fileName
                   << ", snapshotName = " << snapshotName
                   << ", user = " << user
                   << ", seq = " << seq
                   << ", uuid = " << uuid;
        return ret;
    }

    snapInfo.SetStatus(Status::deleting);
    ret = metaStore_->UpdateSnapshot(snapInfo);
    if (ret < 0) {
        LOG(ERROR) << "UpdateSnapshot error,"
                   << " ret = " << ret
                   << ", uuid = " << uuid;
        return kErrCodeInternalError;
    }
    return kErrCodeSuccess;
}

constexpr uint32_t kDelProgressBuildSnapshotMapComplete = 10;
constexpr uint32_t kDelProgressDeleteChunkDataStart =
                       kDelProgressBuildSnapshotMapComplete;
constexpr uint32_t kDelProgressDeleteChunkDataComplete = 80;
constexpr uint32_t kDelProgressDeleteChunkIndexDataComplete = 90;

/**
 * @brief 异步执行删除快照任务并更新任务进度
 *
 * 删除快照进度规划如下：
 *
 * |BuildSnapshotMap|DeleteChunkData|DeleteChunkIndexData|DeleteSnapshot|
 * | 10%            | 10%~80%       | 90%                | 100%         |
 *
 * @param task 快照任务
 */
void SnapshotCoreImpl::HandleDeleteSnapshotTask(
    std::shared_ptr<SnapshotTaskInfo> task) {
    SnapshotInfo &info = task->GetSnapshotInfo();
    UUID uuid = task->GetUuid();
    uint64_t seqNum = info.GetSeqNum();
    FileSnapMap fileSnapshotMap;
    int ret = BuildSnapshotMap(task->GetFileName(), seqNum, &fileSnapshotMap);
    if (ret < 0) {
        LOG(ERROR) << "BuildSnapshotMap error, "
                   << " fileName = " << task->GetFileName()
                   << ", seqNum = " << seqNum
                   << ", uuid = " << task->GetUuid();
        HandleDeleteSnapshotError(task);
        return;
    }
    task->SetProgress(kDelProgressBuildSnapshotMapComplete);
    task->UpdateMetric();
    ChunkIndexDataName name(task->GetFileName(),
        seqNum);
    ChunkIndexData indexData;
    if (dataStore_->ChunkIndexDataExist(name)) {
        ret = dataStore_->GetChunkIndexData(name, &indexData);
        if (ret < 0) {
            LOG(ERROR) << "GetChunkIndexData error ,"
                       << " fileName = " << task->GetFileName()
                       << ", seqNum = " << seqNum
                       << ", uuid = " << task->GetUuid();
            HandleDeleteSnapshotError(task);
            return;
        }

        auto chunkIndexVec = indexData.GetAllChunkIndex();

        uint32_t totalProgress = kDelProgressDeleteChunkDataComplete -
            kDelProgressDeleteChunkDataStart;
        uint32_t chunkDataNum = chunkIndexVec.size();
        double progressPerData = static_cast<double> (totalProgress) /
            chunkDataNum;
        uint32_t index = 0;

        LOG(INFO) << "HandleDeleteSnapshotTask GetChunkIndexData success, "
                  << "begin to DeleteChunkData, "
                  << "chunkDataNum =  " << chunkIndexVec.size();

        for (auto &chunkIndex : chunkIndexVec) {
            ChunkDataName chunkDataName;
            indexData.GetChunkDataName(chunkIndex, &chunkDataName);
            if ((!fileSnapshotMap.IsExistChunk(chunkDataName)) &&
                (dataStore_->ChunkDataExist(chunkDataName))) {
                ret =  dataStore_->DeleteChunkData(chunkDataName);
                if (ret < 0) {
                    LOG(ERROR) << "DeleteChunkData error, "
                               << " ret = " << ret
                               << ", fileName = " << task->GetFileName()
                               << ", seqNum = " << seqNum
                               << ", chunkIndex = "
                               << chunkDataName.chunkIndex_
                               << ", uuid = " << task->GetUuid();
                    HandleDeleteSnapshotError(task);
                    return;
                }
            }
            task->SetProgress(static_cast<uint32_t>(
                kDelProgressDeleteChunkDataStart + index * progressPerData));
            task->UpdateMetric();
            index++;
        }
        task->SetProgress(kDelProgressDeleteChunkDataComplete);
        ret = dataStore_->DeleteChunkIndexData(name);
        if (ret < 0) {
            LOG(ERROR) << "DeleteChunkIndexData error, "
                       << " ret = " << ret
                       << ", fileName = " << task->GetFileName()
                       << ", seqNum = " << seqNum
                       << ", uuid = " << task->GetUuid();
            HandleDeleteSnapshotError(task);
            return;
        }
    } else {
        LOG(INFO) << "HandleDeleteSnapshotTask find chunkindexdata not exist.";
    }
    // ClearSnapshotOnCurvefs  when errorDeleting
    if ((Status::errorDeleting == info.GetStatus()) ||
        (Status::canceling == info.GetStatus())) {
        ret = DeleteSnapshotOnCurvefs(info);
        if (ret < 0) {
            LOG(ERROR) << "DeleteSnapshotOnCurvefs fail"
                       << ", uuid = " << task->GetUuid();
            HandleDeleteSnapshotError(task);
            return;
        }
    }

    task->SetProgress(kDelProgressDeleteChunkIndexDataComplete);
    task->UpdateMetric();
    ret = metaStore_->DeleteSnapshot(uuid);
    if (ret < 0) {
        LOG(ERROR) << "DeleteSnapshot error, "
                   << " ret = " << ret
                   << ", uuid = " << uuid;
        HandleDeleteSnapshotError(task);
        return;
    }

    task->SetProgress(kProgressComplete);
    task->GetSnapshotInfo().SetStatus(Status::done);

    auto &snapInfo = task->GetSnapshotInfo();
    LOG(INFO) << "DeleteSnapshot Task Success"
              << ", uuid = " << snapInfo.GetUuid()
              << ", fileName = " << snapInfo.GetFileName()
              << ", snapshotName = " << snapInfo.GetSnapshotName()
              << ", seqNum = " << snapInfo.GetSeqNum()
              << ", createTime = " << snapInfo.GetCreateTime();
    task->Finish();
    return;
}


void SnapshotCoreImpl::HandleDeleteSnapshotError(
    std::shared_ptr<SnapshotTaskInfo> task) {
    SnapshotInfo &info = task->GetSnapshotInfo();
    info.SetStatus(Status::error);
    int ret = metaStore_->UpdateSnapshot(info);
    if (ret < 0) {
        LOG(ERROR) << "UpdateSnapshot Task Error Fail!"
                   << " ret = " << ret
                   << ", uuid = " << task->GetUuid();
    }

    auto &snapInfo = task->GetSnapshotInfo();
    LOG(INFO) << "DeleteSnapshot Task Fail"
              << ", uuid = " << snapInfo.GetUuid()
              << ", fileName = " << snapInfo.GetFileName()
              << ", snapshotName = " << snapInfo.GetSnapshotName()
              << ", seqNum = " << snapInfo.GetSeqNum()
              << ", createTime = " << snapInfo.GetCreateTime();
    task->Finish();
    return;
}

int SnapshotCoreImpl::GetFileInfo(const std::string &file,
    const std::string &user,
    FInfo *fInfo) {
    int ret = client_->GetFileInfo(file, user, fInfo);
    ret = LibCurveErrToSnapshotCloneErr(ret);
    if (ret != kErrCodeSuccess) {
        LOG(ERROR) << "GetFileInfo encounter an error"
                   << ", ret = " << ret
                   << ", file = " << file
                   << ", user = " << user;
        return ret;
    }

    return kErrCodeSuccess;
}

int SnapshotCoreImpl::GetFileSnapshotInfo(const std::string &file,
    const std::string &user,
    std::vector<SnapshotInfo> *info) {
    metaStore_->GetSnapshotList(file, info);
    if (!info->empty()) {
        if (info->front().GetLocation() == LocationType::kLocationCurve) {
            std::map<uint64_t, FInfo> snapif;
            int ret = client_->ListSnapshot(file, user, &snapif);
            if (ret != kErrCodeSuccess) {
                LOG(ERROR) << "ListSnapshot encounter an error"
                           << ", ret = " << ret
                           << ", file = " << file;
                return ret;
            }
            for (auto it = info->begin(); it != info->end();) {
                auto snap = snapif.find(it->GetSeqNum());
                if (snap == snapif.end()) {
                    it = info->erase(it);
                } else {
                    if (FileStatus::Deleting == snap->second.filestatus) {
                        it->SetStatus(Status::deleting);
                    }
                    it++;
                }
            }
        }
    }
    return kErrCodeSuccess;
}

int SnapshotCoreImpl::GetLocalSnapshotStatus(const std::string &file,
    const std::string &user,
    uint64_t seq,
    Status *status,
    uint32_t *progress) {
    FileStatus fileStatus;
    int ret = client_->CheckSnapShotStatus(file,
        user,
        seq,
        &fileStatus,
        progress);
    LOG(INFO) << "Doing CheckSnapShotStatus, fileName = "
              << file
              << ", user = " << user
              << ", seqNum = " << seq
              << ", fileStatus = " << static_cast<int>(fileStatus)
              << ", underlying progress = " << progress;
    // NOTEXIST means delete succeed.
    if (-LIBCURVE_ERROR::NOTEXIST == ret) {
        LOG(INFO) << "Check snapShot delete success"
                  << ", fileName = " << file
                  << ", user = " << user
                  << ", seqNum = " << seq;
        *progress = 100;
        return kErrCodeFileNotExist;
    } else if (LIBCURVE_ERROR::OK == ret) {
        if (fileStatus == FileStatus::Deleting) {
            *status = Status::deleting;
        } else {
            *status = Status::done;
        }
    } else {
        LOG(ERROR) << "CheckSnapShotStatus fail"
                   << ", ret = " << ret
                   << ", fileName = " << file
                   << ", user = " << user
                   << ", seqNum = " << seq;
        return kErrCodeInternalError;
    }
    return kErrCodeSuccess;
}

int SnapshotCoreImpl::GetSnapshotInfo(const UUID uuid,
    SnapshotInfo *info) {
    return metaStore_->GetSnapshotInfo(uuid, info);
}

int SnapshotCoreImpl::GetSnapshotInfo(const std::string &file,
    const std::string &snapshotName,
    SnapshotInfo *info) {
    return metaStore_->GetSnapshotInfo(file, snapshotName, info);
}

int SnapshotCoreImpl::BuildSnapshotMap(const std::string &fileName,
    uint64_t seqNum,
    FileSnapMap *fileSnapshotMap) {
    std::vector<SnapshotInfo> snapInfos;
    int ret = metaStore_->GetSnapshotList(fileName, &snapInfos);
    for (auto &snap : snapInfos) {
        if (snap.GetSeqNum() != seqNum) {
            ChunkIndexDataName name(snap.GetFileName(), snap.GetSeqNum());
            ChunkIndexData indexData;
            ret = dataStore_->GetChunkIndexData(name, &indexData);
            if (ret < 0) {
                LOG(ERROR) << "GetChunkIndexData error, "
                           << " ret = " << ret
                           << ", fileName = " <<  snap.GetFileName()
                           << ", seqNum = " << snap.GetSeqNum();
                // 此处不能返回错误，
                // 否则一旦某个失败的快照没有indexdata，所有快照都无法删除
            } else {
                fileSnapshotMap->maps.push_back(std::move(indexData));
            }
        }
    }
    return kErrCodeSuccess;
}

int SnapshotCoreImpl::GetSnapshotList(std::vector<SnapshotInfo> *list) {
    metaStore_->GetSnapshotList(list);
    return kErrCodeSuccess;
}

int SnapshotCoreImpl::HandleCancelUnSchduledSnapshotTask(
    std::shared_ptr<SnapshotTaskInfo> task) {
    auto &snapInfo = task->GetSnapshotInfo();
    int ret = metaStore_->DeleteSnapshot(snapInfo.GetUuid());
    if (ret < 0) {
        LOG(ERROR) << "HandleCancelUnSchduledSnapshotTask fail, "
                   << " ret = " << ret
                   << ", uuid = " << snapInfo.GetUuid()
                   << ", fileName = " << snapInfo.GetFileName()
                   << ", snapshotName = " << snapInfo.GetSnapshotName()
                   << ", seqNum = " << snapInfo.GetSeqNum()
                   << ", createTime = " << snapInfo.GetCreateTime();
        return kErrCodeInternalError;
    }
    return kErrCodeSuccess;
}


int SnapshotCoreImpl::HandleCancelScheduledSnapshotTask(
    std::shared_ptr<SnapshotTaskInfo> task) {
    LockGuard lockGuard(task->GetLockRef());

    if (task->IsFinish()) {
        return kErrCodeCannotCancelFinished;
    }

    auto ret = StartCancel(task);
    if (kErrCodeSuccess == ret) {
        task->Cancel();
    } else {
        auto& snapInfo = task->GetSnapshotInfo();
        LOG(ERROR) << "HandleCancelSchduledSnapshotTask failed: "
                   << ", ret = " << ret
                   << ", uuid = " << snapInfo.GetUuid()
                   << ", fileName = " << snapInfo.GetFileName()
                   << ", snapshotName = " << snapInfo.GetSnapshotName()
                   << ", seqNum = " << snapInfo.GetSeqNum()
                   << ", createTime = " << snapInfo.GetCreateTime();
    }

    return ret;
}

}  // namespace snapshotcloneserver
}  // namespace curve
