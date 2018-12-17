/*
 * Project: curve
 * Created Date: Mon Dec 24 2018
 * Author: xuchaojie
 * Copyright (c) 2018 netease
 */

#include "src/snapshot/snapshot_core.h"

#include <glog/logging.h>

#include <algorithm>

#include "src/snapshot/snapshot_define.h"
#include "src/snapshot/snapshot_task.h"

namespace curve {
namespace snapshotserver {

int SnapshotCoreImpl::CreateSnapshotPre(const std::string &file,
    const std::string &user,
    const std::string &snapshotName,
    SnapshotInfo *snapInfo) {
    UUID uuid = UUIDGenerator_->GenerateUUID();
    SnapshotInfo info(uuid, user, file, snapshotName);
    info.SetStatus(Status::pending);
    int ret = metaStore_->AddSnapshot(info);
    if (ret < 0) {
        LOG(ERROR) << "AddSnapshot error,"
                   << " ret = " << ret
                   << ", uuid = " << uuid
                   << ", fileName = " << file
                   << ", snapshotName = " << snapshotName;
        return ret;
    }
    *snapInfo = info;
    return kErrCodeSnapshotServerSuccess;
}

constexpr uint32_t kProgressCreateSnapshotOnCurvefsComplete = 10;
constexpr uint32_t kProgressBuildChunkIndexDataComplete = 20;
constexpr uint32_t kProgressBuildSnapshotMapComplete = 30;
constexpr uint32_t kProgressTransferSnapshotDataStart =
                       kProgressBuildSnapshotMapComplete;
constexpr uint32_t kProgressTransferSnapshotDataComplete = 90;
constexpr uint32_t kProgressComplete = 100;

/**
 * @brief 异步执行创建快照任务并更新任务进度
 *
 * 快照进度规划如下:
 *
 *  |CreateSnapshotOnCurvefs| BuildChunkIndexData | BuildSnapshotMap | TransferSnapshotData | UpdateSnapshot | //NOLINT
 *  | 10%                   | 20%                 | 30%              | 30%~90%              | 100%           | //NOLINT
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
    int ret = kErrCodeSnapshotServerSuccess;
    SnapshotInfo *info = &(task->GetSnapshotInfo());
    UUID uuid = task->GetUuid();
    uint64_t seqNum = info->GetSeqNum();
    std::string fileName = task->GetFileName();
    bool existIndexData = false;
    if (kUnInitializeSeqNum ==  seqNum) {
        ret = CreateSnapshotOnCurvefs(fileName, info, task);
        if (ret < 0) {
            LOG(ERROR) << "CreateSnapshotOnCurvefs error, "
                       << " ret = " << ret
                       << ", fileName = " << fileName;
            HandleCreateSnapshotError(task);
            return;
        }
        seqNum = info->GetSeqNum();
        existIndexData = false;
    } else {
        ChunkIndexDataName name(fileName, seqNum);
        // judge Is Exist indexData
        existIndexData = dataStore_->ChunkIndexDataExist(name);
    }

    task->SetProgress(kProgressCreateSnapshotOnCurvefsComplete);
    if (task->IsCanceled()) {
        CancelAfterCreateSnapshotOnCurvefs(task);
        return;
    }

    ChunkIndexData indexData;
    ChunkIndexDataName name(fileName, seqNum);
    std::vector<SegmentInfo> segInfos;
    if (existIndexData) {
        ret = dataStore_->GetChunkIndexData(name, &indexData);
        if (ret < 0) {
            LOG(ERROR) << "GetChunkIndexData error, "
                       << " ret = " << ret
                       << ", fileName = " << fileName
                       << ", seqNum = " << seqNum;
            HandleCreateSnapshotError(task);
            return;
        }

        task->SetProgress(kProgressBuildChunkIndexDataComplete);
        if (task->IsCanceled()) {
            CancelAfterCreateChunkIndexData(task);
            return;
        }

        ret = BuildSegmentInfo(*info, &segInfos);
        if (ret < 0) {
            LOG(ERROR) << "BuildSegmentInfo error,"
                       << " ret = " << ret;
            HandleCreateSnapshotError(task);
            return;
        }
    } else {
        ret = BuildChunkIndexData(*info, &indexData, &segInfos, task);
        if (ret < 0) {
            LOG(ERROR) << "BuildChunkIndexData error, "
                       << " ret = " << ret;
            HandleCreateSnapshotError(task);
            return;
        }

        ret = dataStore_->PutChunkIndexData(name, indexData);
        if (ret < 0) {
            LOG(ERROR) << "PutChunkIndexData error, "
                       << " ret = " << ret;
            HandleCreateSnapshotError(task);
            return;
        }

        task->SetProgress(kProgressBuildChunkIndexDataComplete);
        if (task->IsCanceled()) {
            CancelAfterCreateChunkIndexData(task);
            return;
        }
    }

    FileSnapMap fileSnapshotMap;
    ret = BuildSnapshotMap(fileName,
        seqNum,
        &fileSnapshotMap);
    if (ret < 0) {
        LOG(ERROR) << "BuildSnapshotMap error, "
                   << " fileName = " << task->GetFileName()
                   << ", seqNum = " << seqNum;
        HandleCreateSnapshotError(task);
        return;
    }
    task->SetProgress(kProgressBuildSnapshotMapComplete);
    if (task->IsCanceled()) {
        CancelAfterCreateChunkIndexData(task);
        return;
    }

    ret = TransferSnapshotData(indexData,
        *info,
        segInfos,
        [&fileSnapshotMap] (const ChunkDataName &chunkDataName) {
            return fileSnapshotMap.IsExistChunk(chunkDataName);
        },
        task);
    if (ret < 0) {
        LOG(ERROR) << "TransferSnapshotData error, "
                   << " ret = " << ret;
        HandleCreateSnapshotError(task);
        return;
    }
    task->SetProgress(kProgressTransferSnapshotDataComplete);
    if (task->IsCanceled()) {
        CancelAfterTransferSnapshotData(task, indexData, fileSnapshotMap);
        return;
    }

    info->SetStatus(Status::done);
    ret = metaStore_->UpdateSnapshot(*info);
    if (ret < 0) {
        LOG(ERROR) << "UpdateSnapshot error, "
                   << " ret = " << ret;
        HandleCreateSnapshotError(task);
        return;
    }
    task->SetProgress(kProgressComplete);

    task->Lock();
    if (task->IsCanceled()) {
        task->UnLock();
        CancelAfterTransferSnapshotData(task, indexData, fileSnapshotMap);
        return;
    }
    LOG(INFO) << "CreateSnapshot Success.";
    task->Finish();
    task->UnLock();
    return;
}

void SnapshotCoreImpl::CancelAfterTransferSnapshotData(
    std::shared_ptr<SnapshotTaskInfo> task,
    const ChunkIndexData &indexData,
    const FileSnapMap &fileSnapshotMap) {
    auto chunkDataVec = indexData.GetAllChunkDataName();
    for (auto &chunkDataName : chunkDataVec) {
        if ((!fileSnapshotMap.IsExistChunk(chunkDataName)) &&
            (dataStore_->ChunkDataExist(chunkDataName))) {
            int ret =  dataStore_->DeleteChunkData(chunkDataName);
            if (ret < 0) {
                LOG(ERROR) << "DeleteChunkData error"
                           << "while canceling CreateSnapshot, "
                           << " ret = " << ret
                           << ", fileName = " << task->GetFileName()
                           << ", seqNum = " << chunkDataName.chunkSeqNum_
                           << ", chunkIndex = " << chunkDataName.chunkIndex_;
                HandleCreateSnapshotError(task);
                return;
            }
        }
    }
    CancelAfterCreateChunkIndexData(task);
}

void SnapshotCoreImpl::CancelAfterCreateChunkIndexData(
    std::shared_ptr<SnapshotTaskInfo> task) {
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
                   << ", seqNum = " << seqNum;
        HandleCreateSnapshotError(task);
        return;
    }
    CancelAfterCreateSnapshotOnCurvefs(task);
}

void SnapshotCoreImpl::CancelAfterCreateSnapshotOnCurvefs(
    std::shared_ptr<SnapshotTaskInfo> task) {
    SnapshotInfo &info = task->GetSnapshotInfo();
    UUID uuid = task->GetUuid();
    uint64_t seqNum = info.GetSeqNum();
    int ret = client_->DeleteSnapshot(info.GetFileName(),
        seqNum);
    if (ret < 0) {
        LOG(ERROR) << "Client DeleteSnapshot error "
                   << "while canceling CreateSnapshot, "
                   << " ret = " << ret
                   << ", fileName = " << info.GetFileName()
                   << ", seqNum = " << seqNum;
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
    }
    LOG(INFO) << "CreateSnapshot Canceled Success.";
    task->Finish();
    return;
}

void SnapshotCoreImpl::HandleCreateSnapshotError(
    std::shared_ptr<SnapshotTaskInfo> task) {
    SnapshotInfo &info = task->GetSnapshotInfo();
    LOG(ERROR) << "CreateSnapshot fail.";
    info.SetStatus(Status::error);
    metaStore_->UpdateSnapshot(info);
    task->Finish();
    return;
}

int SnapshotCoreImpl::CreateSnapshotOnCurvefs(
    const std::string &fileName,
    SnapshotInfo *info,
    std::shared_ptr<SnapshotTaskInfo> task) {
    uint64_t seqNum = 0;
    int ret =
        client_->CreateSnapshot(fileName, &seqNum);
    if (ret < 0) {
        LOG(ERROR) << "CreateSnapshot on curvefs fail, "
                   << " ret = " << ret;
        return ret;
    }

    FInfo snapInfo;
    ret = client_->GetSnapshot(fileName,
        seqNum, &snapInfo);
    if (ret < 0) {
        LOG(ERROR) << "GetSnapShot on curvefs fail, "
                   << " ret = " << ret
                   << ", fileName = " << fileName
                   << ", seqNum = " << seqNum;
        return ret;
    }
    info->SetSeqNum(seqNum);
    info->SetChunkSize(snapInfo.chunksize);
    info->SetSegmentSize(snapInfo.segmentsize);
    info->SetFileLength(snapInfo.length);
    info->SetCreateTime(snapInfo.ctime);

    ret = metaStore_->UpdateSnapshot(*info);
    if (ret < 0) {
        LOG(ERROR) << "UpdateSnapshot error, "
                   << " ret = " << ret
                   << ", fileName = " << fileName;
        return ret;
    }

    return kErrCodeSnapshotServerSuccess;
}

int SnapshotCoreImpl::BuildChunkIndexData(
    const SnapshotInfo &info,
    ChunkIndexData *indexData,
    std::vector<SegmentInfo> *segInfos,
    std::shared_ptr<SnapshotTaskInfo> task) {
    int ret = 0;
    std::string fileName = info.GetFileName();
    uint64_t seqNum = info.GetSeqNum();
    uint64_t fileLength = info.GetFileLength();
    uint64_t segmentSize = info.GetSegmentSize();
    uint64_t chunkSize = info.GetChunkSize();

    uint64_t chunkIndex = 0;
    for (uint64_t i = 0; i < fileLength/segmentSize; i++) {
        uint64_t offset = i * segmentSize;
        SegmentInfo segInfo;
        ret = client_->GetSnapshotSegmentInfo(
            fileName,
            seqNum,
            offset,
            &segInfo);
        if (ret < 0) {
            LOG(ERROR) << "GetSnapshotSegmentInfo error, "
                       << " ret = " << ret
                       << ", fileName = " << fileName
                       << ", seqNum = " << seqNum
                       << ", offset = " << offset;
            return ret;
        }
        segInfos->push_back(segInfo);
        for (std::vector<uint64_t>::size_type j = 0;
            j < segInfo.chunkvec.size();
            j++) {
            ChunkInfoDetail chunkInfo;
            ChunkIDInfo cidInfo = segInfo.chunkvec[j];
            ret = client_->GetChunkInfo(cidInfo,
                &chunkInfo);
            if (ret < 0) {
                LOG(ERROR) << "GetChunkInfo error, "
                           << " ret = " << ret
                           << ", logicalPoolId = " << cidInfo.lpid_
                           << ", copysetId = " << cidInfo.cpid_
                           << ", chunkId = " << cidInfo.cid_;
                return ret;
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
                LOG(ERROR) << "GetChunkInfo return chunkInfo.chunkSn.size() "
                           << "invalid, size = "
                           << chunkInfo.chunkSn.size();
                return kErrCodeSnapshotServerFail;
            }
            if (task->IsCanceled()) {
                return kErrCodeSnapshotServerSuccess;
            }
        }
    }

    return kErrCodeSnapshotServerSuccess;
}

int SnapshotCoreImpl::BuildSegmentInfo(
    const SnapshotInfo &info,
    std::vector<SegmentInfo> *segInfos) {
    int ret = kErrCodeSnapshotServerSuccess;
    std::string fileName = info.GetFileName();
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
            seq,
            offset,
            &segInfo);
        if (ret < 0) {
            LOG(ERROR) << "GetSnapshotSegmentInfo error,"
                       << " ret = " << ret
                       << ", fileName = " << fileName
                       << ", seq = " << seq
                       << ", offset = " << offset;
            return ret;
        }
        // todo(xuchaojie): 后续考虑是否将segInfos改成map
        segInfos->push_back(segInfo);
    }
    return kErrCodeSnapshotServerSuccess;
}

int SnapshotCoreImpl::TransferSnapshotDataChunk(
    const ChunkDataName &name,
    uint64_t chunkSize,
    const ChunkIDInfo &cidInfo) {
    std::shared_ptr<TransferTask> transferTask =
        std::make_shared<TransferTask>();
    int ret = dataStore_->DataChunkTranferInit(name,
            transferTask);
    if (ret < 0) {
        LOG(ERROR) << "DataChunkTranferInit error, "
                   << " ret = " << ret
                   << ", fileName = " << name.fileName_
                   << ", chunkSeqNum = " << name.chunkSeqNum_
                   << ", chunkIndex = " << name.chunkIndex_;
        return ret;
    }
    auto buf =
        std::make_shared<std::array<char, kChunkSplitSize> >();
    bool hasAddPart = false;
    for (uint64_t i = 0;
        i < chunkSize / kChunkSplitSize;
        i++) {
        uint64_t offset = i * kChunkSplitSize;
        ret = client_->ReadChunkSnapshot(
            cidInfo,
            name.chunkSeqNum_,
            offset,
            kChunkSplitSize,
            buf->data());
        if (ret < 0) {
            LOG(ERROR) << "ReadChunkSnapshot error, "
                       << " ret = " << ret
                       << ", logicalPool = " << cidInfo.lpid_
                       << ", copysetId = " << cidInfo.cpid_
                       << ", chunkId = " << cidInfo.cid_
                       << ", seqNum = " << name.chunkSeqNum_
                       << ", offset = " << offset;
            break;
        }
        ret = dataStore_->DataChunkTranferAddPart(name,
            transferTask,
            i,
            kChunkSplitSize,
            buf->data());
        hasAddPart = true;
        if (ret < 0) {
            LOG(ERROR) << "DataChunkTranferAddPart error, "
                       << " ret = " << ret
                       << ", index = " << i;
            break;
        }
    }
    if (ret >= 0) {
        ret =
            dataStore_->DataChunkTranferComplete(name, transferTask);
        if (ret < 0) {
            LOG(ERROR) << "DataChunkTranferComplete error, "
                       << " ret = " << ret
                       << ", fileName = " << name.fileName_
                       << ", chunkSeqNum = " << name.chunkSeqNum_
                       << ", chunkIndex = " << name.chunkIndex_;
        }
    }
    if (ret < 0) {
        if (hasAddPart) {
            int ret2 =
                dataStore_->DataChunkTranferAbort(
                name,
                transferTask);
            if (ret2 < 0) {
                LOG(ERROR) << "DataChunkTranferAbort error, "
                           << " ret = " << ret2
                           << ", fileName = " << name.fileName_
                           << ", chunkSeqNum = " << name.chunkSeqNum_
                           << ", chunkIndex = " << name.chunkIndex_;
            }
        }
        return ret;
    }
    return kErrCodeSnapshotServerSuccess;
}

int SnapshotCoreImpl::TransferSnapshotData(
    const ChunkIndexData indexData,
    const SnapshotInfo &info,
    const std::vector<SegmentInfo> &segInfos,
    const ChunkDataExistFilter &filter,
    std::shared_ptr<SnapshotTaskInfo> task) {
    int ret = 0;
    uint64_t seqNum = info.GetSeqNum();
    uint64_t segmentSize = info.GetSegmentSize();
    uint64_t chunkSize = info.GetChunkSize();
    uint64_t chunkPerSegment = segmentSize/chunkSize;

    if (chunkSize % kChunkSplitSize != 0) {
        LOG(ERROR) << "error!, ChunkSize is not align to kChunkSplitSize.";
        return kErrCodeSnapshotServerFail;
    }

    auto chunkDataVec = indexData.GetAllChunkDataName();

    uint32_t totalProgress = kProgressTransferSnapshotDataComplete -
        kProgressTransferSnapshotDataStart;
    uint32_t transferDataNum = chunkDataVec.size();
    double progressPerData =
        static_cast<double>(totalProgress) / transferDataNum;
    uint32_t index = 0;

    for (auto &chunkDataName : chunkDataVec) {
        int chunkIndex = chunkDataName.chunkIndex_;
        uint64_t segNum = chunkIndex / chunkPerSegment;
        if (segNum >= segInfos.size()) {
            LOG(ERROR) << "TransferSnapshotData, segNum >= segInfos.size()"
                       << " segNum = " << segNum
                       << ", size = " << segInfos.size();
            return kErrCodeSnapshotServerFail;
        }
        uint64_t chunkIndexInSegment = chunkIndex % chunkPerSegment;
        if (chunkIndexInSegment >= segInfos[segNum].chunkvec.size()) {
            LOG(ERROR) << "TransferSnapshotData, "
                       << "chunkIndexInSegment >= "
                       << "segInfos[segNum].chunkvec.size()"
                       << ", chunkIndexInSegment = "
                       << chunkIndexInSegment
                       << ", size = "
                       << segInfos[segNum].chunkvec.size();
            return kErrCodeSnapshotServerFail;
        }
    }

    for (auto &chunkDataName : chunkDataVec) {
        int chunkIndex = chunkDataName.chunkIndex_;
        uint64_t segNum = chunkIndex / chunkPerSegment;
        uint64_t chunkIndexInSegment = chunkIndex % chunkPerSegment;

        ChunkIDInfo cidInfo =
            segInfos[segNum].chunkvec[chunkIndexInSegment];

        if (!filter(chunkDataName)) {
            ret = TransferSnapshotDataChunk(chunkDataName, chunkSize, cidInfo);
            if (ret < 0) {
                return ret;
            }
        }
        // delete chunk on curvefs
        ret = client_->DeleteChunkSnapshot(
                cidInfo,
                seqNum);
        if (ret < 0) {
            LOG(ERROR) << "DeleteChunkSnapshot error, "
                       << " ret = " << ret
                       << ", logicalPool = " << cidInfo.lpid_
                       << ", copysetId = " << cidInfo.cpid_
                       << ", chunkId = " << cidInfo.cid_
                       << ", seqNum = " << seqNum;
            return ret;
        }
        task->SetProgress(static_cast<uint32_t>(
                kProgressTransferSnapshotDataStart + index * progressPerData));
        index++;
        if (task->IsCanceled()) {
            return kErrCodeSnapshotServerSuccess;
        }
    }
    ret = client_->DeleteSnapshot(info.GetFileName(),
        seqNum);
    if (ret < 0) {
        LOG(ERROR) << "DeleteSnapshot error, "
                   << " ret = " << ret
                   << ", fileName = " << info.GetFileName()
                   << ", seqNum = " << seqNum;
        return ret;
    }
    return kErrCodeSnapshotServerSuccess;
}

int SnapshotCoreImpl::DeleteSnapshotPre(
    UUID uuid,
    const std::string &user,
    const std::string &fileName,
    SnapshotInfo *snapInfo) {
    int ret = metaStore_->GetSnapshotInfo(uuid, snapInfo);
    if (ret < 0) {
        // snapshot not exist.
        return kErrCodeSnapshotServerSuccess;
    }
    if (snapInfo->GetUser() != user) {
        LOG(ERROR) << "Can not delete snapshot by different user.";
        return kErrCodeSnapshotServerFail;
    }
    if (fileName != snapInfo->GetFileName()) {
        LOG(ERROR) << "Can not delete, fileName is not matched.";
        return kErrCodeSnapshotServerFail;
    }


    switch (snapInfo->GetStatus()) {
        case Status::done :
            snapInfo->SetStatus(Status::deleting);
            break;
        case Status::error:
            snapInfo->SetStatus(Status::errorDeleting);
            break;
        case Status::pending:
            LOG(ERROR) << "Can not delete snapshot unfinished.";
            return kErrCodeSnapshotServerFail;
        case Status::canceling:
        case Status::deleting:
        case Status::errorDeleting:
            return kErrCodeSnapshotTaskExist;
            break;
        default:
            break;
    }

    ret = metaStore_->UpdateSnapshot(*snapInfo);
    if (ret < 0) {
        LOG(ERROR) << "UpdateSnapshot error,"
                   << " ret = " << ret
                   << ", uuid = " << uuid;
        return ret;
    }
    return kErrCodeSnapshotServerSuccess;
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
                   << ", seqNum = " << seqNum;
        HandleDeleteSnapshotError(task);
        return;
    }
    task->SetProgress(kDelProgressBuildSnapshotMapComplete);
    ChunkIndexDataName name(task->GetFileName(),
        seqNum);
    ChunkIndexData indexData;
    if (dataStore_->ChunkIndexDataExist(name)) {
        ret = dataStore_->GetChunkIndexData(name, &indexData);
        if (ret < 0) {
            LOG(ERROR) << "GetChunkIndexData error ,"
                       << " fileName = " << task->GetFileName()
                       << ", seqNum = " << seqNum;
            HandleDeleteSnapshotError(task);
            return;
        }

        auto chunkDataVec = indexData.GetAllChunkDataName();

        uint32_t totalProgress = kDelProgressDeleteChunkDataComplete -
            kDelProgressDeleteChunkDataStart;
        uint32_t chunkDataNum = chunkDataVec.size();
        double progressPerData = static_cast<double> (totalProgress) /
            chunkDataNum;
        uint32_t index = 0;

        for (auto &chunkDataName : chunkDataVec) {
            if ((!fileSnapshotMap.IsExistChunk(chunkDataName)) &&
                (dataStore_->ChunkDataExist(chunkDataName))) {
                ret =  dataStore_->DeleteChunkData(chunkDataName);
                if (ret < 0) {
                    LOG(ERROR) << "DeleteChunkData error, "
                               << " ret = " << ret
                               << ", fileName = " << task->GetFileName()
                               << ", seqNum = " << seqNum
                               << ", chunkIndex = "
                               << chunkDataName.chunkIndex_;
                    HandleDeleteSnapshotError(task);
                    return;
                }
            }
            task->SetProgress(static_cast<uint32_t>(
                kDelProgressDeleteChunkDataStart + index * progressPerData));
            index++;
        }
        task->SetProgress(kDelProgressDeleteChunkDataComplete);
        ret = dataStore_->DeleteChunkIndexData(name);
        if (ret < 0) {
            LOG(ERROR) << "DeleteChunkIndexData error, "
                       << " ret = " << ret
                       << ", fileName = " << task->GetFileName()
                       << ", seqNum = " << seqNum;
            HandleDeleteSnapshotError(task);
            return;
        }
    }
    // ClearSnapshotOnCurvefs  when errorDeleting
    if ((Status::errorDeleting == info.GetStatus()) ||
        (Status::canceling == info.GetStatus())) {
        ret = client_->DeleteSnapshot(info.GetFileName(),
            seqNum);
        if (ret < 0) {
            LOG(ERROR) << "Client DeleteSnapshot error "
                       << "while DeleteSnapshot, "
                       << " ret = " << ret
                       << " fileName = " << info.GetFileName()
                       << " seqNum = " << seqNum;
            HandleDeleteSnapshotError(task);
            return;
        }
    }

    task->SetProgress(kDelProgressDeleteChunkIndexDataComplete);
    ret = metaStore_->DeleteSnapshot(uuid);
    if (ret < 0) {
        LOG(ERROR) << "DeleteSnapshot error, "
                   << " ret = " << ret
                   << ", uuid = " << uuid;
        HandleDeleteSnapshotError(task);
        return;
    }

    task->SetProgress(kProgressComplete);
    task->Finish();
    return;
}


void SnapshotCoreImpl::HandleDeleteSnapshotError(
    std::shared_ptr<SnapshotTaskInfo> task) {
    SnapshotInfo &info = task->GetSnapshotInfo();
    LOG(ERROR) << "HandleDeleteSnapshotTask fail.";
    info.SetStatus(Status::error);
    metaStore_->UpdateSnapshot(info);
    task->Finish();
    return;
}

int SnapshotCoreImpl::GetFileSnapshotInfo(const std::string &file,
    std::vector<SnapshotInfo> *info) {
    metaStore_->GetSnapshotList(file, info);
    return kErrCodeSnapshotServerSuccess;
}

int SnapshotCoreImpl::BuildSnapshotMap(const std::string &fileName,
    uint64_t seqNum,
    FileSnapMap *fileSnapshotMap) {
    std::vector<SnapshotInfo> snapInfos;
    int ret = metaStore_->GetSnapshotList(fileName, &snapInfos);
    if (ret < 0) {
        LOG(ERROR) << "GetSnapshotList error,"
                   << " ret = " << ret
                   << ", fileName = " << fileName;
        return ret;
    }

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
                return ret;
            }
            fileSnapshotMap->maps.push_back(std::move(indexData));
        }
    }
    return kErrCodeSnapshotServerSuccess;
}

int SnapshotCoreImpl::GetSnapshotList(std::vector<SnapshotInfo> *list) {
    return metaStore_->GetSnapshotList(list);
}

}  // namespace snapshotserver
}  // namespace curve
