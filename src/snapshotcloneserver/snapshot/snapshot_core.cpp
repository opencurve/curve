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

#include "src/common/snapshotclone/snapshotclone_define.h"
#include "src/snapshotcloneserver/snapshot/snapshot_task.h"

#include "src/common/uuid.h"

using ::curve::common::UUIDGenerator;
using ::curve::common::NameLockGuard;
using ::curve::common::LockGuard;

namespace curve {
namespace snapshotcloneserver {

int SnapshotCoreImpl::Init() {
    int ret = threadPool_->Start();
    if (ret < 0) {
        LOG(ERROR) << "SnapshotCoreImpl, thread start fail, ret = " << ret;
        return ret;
    }
    return kErrCodeSuccess;
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
                //Treat as the same snapshot, return task already exists
                *snapInfo = snap;
                return kErrCodeTaskExist;
            }
        }
        if (Status::error == snap.GetStatus()) {
            snapshotNum--;
        }
    }
    if (snapshotNum >= static_cast<int>(maxSnapshotLimit_)) {
        LOG(ERROR) << "Snapshot count reach the max limit.";
        return kErrCodeSnapshotCountReachLimit;
    }

    FInfo fInfo;
    int ret = client_->GetFileInfo(file, user, &fInfo);
    switch (ret) {
        case LIBCURVE_ERROR::OK:
            break;
        case -LIBCURVE_ERROR::NOTEXIST:
            LOG(ERROR) << "create snapshot file not exist"
                       << ", file = " << file
                       << ", user = " << user
                       << ", snapshotName = " << snapshotName;
            return kErrCodeFileNotExist;
        case -LIBCURVE_ERROR::AUTHFAIL:
            LOG(ERROR) << "create snapshot by invalid user"
                       << ", file = " << file
                       << ", user = " << user
                       << ", snapshotName = " << snapshotName;
            return kErrCodeInvalidUser;
        default:
            LOG(ERROR) << "GetFileInfo encounter an error"
                       << ", ret = " << ret
                       << ", file = " << file
                       << ", user = " << user;
            return kErrCodeInternalError;
    }

    if (fInfo.filestatus != FileStatus::Created &&
        fInfo.filestatus != FileStatus::Cloned) {
        LOG(ERROR) << "Can not create snapshot when file status = "
                   << static_cast<int>(fInfo.filestatus);
        return kErrCodeFileStatusInvalid;
    }

    UUID uuid = UUIDGenerator().GenerateUUID();
    SnapshotInfo info(uuid, user, file, snapshotName);
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

constexpr uint32_t kProgressCreateSnapshotOnCurvefsComplete = 5;
constexpr uint32_t kProgressBuildChunkIndexDataComplete = 6;
constexpr uint32_t kProgressBuildSnapshotMapComplete = 10;
constexpr uint32_t kProgressTransferSnapshotDataStart =
                       kProgressBuildSnapshotMapComplete;
constexpr uint32_t kProgressTransferSnapshotDataComplete = 99;
constexpr uint32_t kProgressComplete = 100;

/**
 * @brief Asynchronous execution of snapshot creation task and update of task progress
 *
 *The snapshot schedule is planned as follows:
 *
 *  |CreateSnapshotOnCurvefs| BuildChunkIndexData | BuildSnapshotMap | TransferSnapshotData | UpdateSnapshot | //NOLINT
 *  | 5%                    | 6%                  | 10%              | 10%~99%              | 100%           | //NOLINT
 *
 *
 *Explanation of errors and cancellations during asynchronous execution:
 *1 The occurrence of an error will cause the entire asynchronous task to be directly interrupted without any cleaning action:
 *When an error occurs, there is usually an abnormality in the system, and the cleaning action may not be completed,
 *Therefore, no cleaning will be carried out, only the status will be set, and after manual intervention to eliminate anomalies,
 *Use the DeleteSnapshot function to manually delete snapshots with error status.
 *2 When a cancel occurs, the cleaning actions are carried out in reverse order of creating functions,
 *If an error occurs during the cleaning process, it will be immediately interrupted, followed by the same error process.
 *
 * @param task snapshot task
 */
void SnapshotCoreImpl::HandleCreateSnapshotTask(
    std::shared_ptr<SnapshotTaskInfo> task) {
    std::string fileName = task->GetFileName();

    //If there are currently failed snapshots, it is necessary to clean up the failed snapshots first, otherwise the snapshots will fail again
    int ret = ClearErrorSnapBeforeCreateSnapshot(task);
    if (ret < 0) {
        HandleCreateSnapshotError(task);
        return;
    }

    //To support task restart, there are three situations that need to be addressed
    //1 I haven't taken a snapshot, there's no seqNum, and there's no snapshot on the curve
    //2 I have taken a snapshot, and there is seqNum and a snapshot on the curve
    //3 I have taken a snapshot and have completed the dump to delete it. There is seqNum, but there is no snapshot on the curve

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
            //Processing deletion of snapshots
            HandleDeleteSnapshotTask(taskInfo);
            //If it still fails, the current snapshot fails
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
    uint64_t seqNum = 0;
    int ret =
        client_->CreateSnapshot(fileName, info->GetUser(), &seqNum);
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
    LOG(INFO) << "CreateSnapshot on curvefs success, seq = " << seqNum
              << ", uuid = " << task->GetUuid();

    FInfo snapInfo;
    ret = client_->GetSnapshot(fileName,
        info->GetUser(),
        seqNum, &snapInfo);
    if (ret != LIBCURVE_ERROR::OK) {
        LOG(ERROR) << "GetSnapShot on curvefs fail, "
                   << " ret = " << ret
                   << ", fileName = " << fileName
                   << ", user = " << info->GetUser()
                   << ", seqNum = " << seqNum
                   << ", uuid = " << task->GetUuid();
        return kErrCodeInternalError;
    }
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

    //After taking a snapshot, you need to wait for 2 sessions to ensure that the seq is synchronized to all clients
    std::this_thread::sleep_for(
        std::chrono::microseconds(mdsSessionTimeUs_ * 2));

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
        ret = client_->CheckSnapShotStatus(info.GetFileName(),
            info.GetUser(),
            seqNum,
            &status);
        LOG(INFO) << "Doing CheckSnapShotStatus, fileName = "
                  << info.GetFileName()
                  << ", user = " << info.GetUser()
                  << ", seqNum = " << seqNum
                  << ", status = " << static_cast<int>(status)
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
                //2 Sns, the smaller one is the snap snap snap, and the larger one is the write after the snapshot
                //1 SN, there are two situations:
                //      When it is less than or equal to seqNum, it is a snap snap and has not been written since the snapshot;
                //      When greater than, it indicates that it was blank when taking a snapshot, and is the version written for the first time after the snapshot (seqNum+1)
                //No sn, never written before
                //Greater than 2 sns, error, error reported
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
            } else {
                DLOG(INFO) << "find data object exist, skip chunkDataName = "
                           << chunkDataName.ToDataChunkKey();
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
    //Tasks with insufficient remaining quantity in the end
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
        //When the snapshot does not exist, it directly returns deletion success, making the interface idempotent
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

constexpr uint32_t kDelProgressBuildSnapshotMapComplete = 10;
constexpr uint32_t kDelProgressDeleteChunkDataStart =
                       kDelProgressBuildSnapshotMapComplete;
constexpr uint32_t kDelProgressDeleteChunkDataComplete = 80;
constexpr uint32_t kDelProgressDeleteChunkIndexDataComplete = 90;

/**
 * @brief Asynchronous execution of delete snapshot task and update task progress
 *
 *Delete the snapshot schedule as follows:
 *
 * |BuildSnapshotMap|DeleteChunkData|DeleteChunkIndexData|DeleteSnapshot|
 * | 10%            | 10%~80%       | 90%                | 100%         |
 *
 * @param task snapshot task
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

int SnapshotCoreImpl::GetFileSnapshotInfo(const std::string &file,
    std::vector<SnapshotInfo> *info) {
    metaStore_->GetSnapshotList(file, info);
    return kErrCodeSuccess;
}

int SnapshotCoreImpl::GetSnapshotInfo(const UUID uuid,
    SnapshotInfo *info) {
    return metaStore_->GetSnapshotInfo(uuid, info);
}

int SnapshotCoreImpl::BuildSnapshotMap(const std::string &fileName,
    uint64_t seqNum,
    FileSnapMap *fileSnapshotMap) {
    std::vector<SnapshotInfo> snapInfos;
    int ret = metaStore_->GetSnapshotList(fileName, &snapInfos);
    for (auto &snap : snapInfos) {
        if (snap.GetSeqNum() != seqNum &&
            snap.GetSeqNum() != kUnInitializeSeqNum) {
            ChunkIndexDataName name(snap.GetFileName(), snap.GetSeqNum());
            ChunkIndexData indexData;
            ret = dataStore_->GetChunkIndexData(name, &indexData);
            if (ret < 0) {
                LOG(ERROR) << "GetChunkIndexData error, "
                           << " ret = " << ret
                           << ", fileName = " <<  snap.GetFileName()
                           << ", seqNum = " << snap.GetSeqNum();
                //An error cannot be returned here,
                //Otherwise, once a failed snapshot does not have indexdata, all snapshots cannot be deleted
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
