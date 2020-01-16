/*
 * Project: curve
 * Created Date: Tue Mar 26 2019
 * Author: xuchaojie
 * Copyright (c) 2018 netease
 */

#include "src/snapshotcloneserver/clone/clone_core.h"

#include <memory>
#include <string>
#include <vector>
#include <list>

#include "src/snapshotcloneserver/clone/clone_task.h"
#include "src/common/location_operator.h"
#include "src/common/uuid.h"
#include "src/common/concurrent/name_lock.h"

using ::curve::common::UUIDGenerator;
using ::curve::common::LocationOperator;
using ::curve::common::NameLockGuard;

namespace curve {
namespace snapshotcloneserver {

int CloneCoreImpl::Init() {
    int ret = client_->Mkdir(cloneTempDir_, mdsRootUser_);
    if (ret != LIBCURVE_ERROR::OK &&
        ret != -LIBCURVE_ERROR::EXISTS) {
        LOG(ERROR) << "Mkdir fail, ret = " << ret
                   << ", dirpath = " << cloneTempDir_;
        return kErrCodeServerInitFail;
    }
    return kErrCodeSuccess;
}

int CloneCoreImpl::CloneOrRecoverPre(const UUID &source,
    const std::string &user,
    const std::string &destination,
    bool lazyFlag,
    CloneTaskType taskType,
    CloneInfo *cloneInfo) {
    // 目标文件已存在不能clone, 不存在不能recover
    FInfo destFInfo;
    int ret = client_->GetFileInfo(destination, mdsRootUser_, &destFInfo);
    switch (ret) {
        case LIBCURVE_ERROR::OK:
            if (CloneTaskType::kClone == taskType) {
                LOG(ERROR) << "Clone dest file must not exist"
                           << ", source = " << source
                           << ", user = " << user
                           << ", destination = " << destination;
                return kErrCodeFileExist;
            }
            break;
        case -LIBCURVE_ERROR::NOTEXIST:
            if (CloneTaskType::kRecover == taskType) {
                LOG(ERROR) << "Recover dest file must exist"
                           << ", source = " << source
                           << ", user = " << user
                           << ", destination = " << destination;
                return kErrCodeFileNotExist;
            }
            break;
        default:
            LOG(ERROR) << "GetFileInfo encounter an error"
                       << ", ret = " << ret
                       << ", source = " << source
                       << ", user = " << user;
            return kErrCodeInternalError;
    }

    //是否为快照
    SnapshotInfo snapInfo;
    CloneFileType fileType;

    {
        NameLockGuard lockSnapGuard(snapshotRef_->GetSnapshotLock(), source);
        ret = metaStore_->GetSnapshotInfo(source, &snapInfo);
        if (0 == ret) {
            if (snapInfo.GetStatus() != Status::done) {
                LOG(ERROR) << "Can not clone by snapshot has status:"
                           << static_cast<int>(snapInfo.GetStatus());
                return kErrCodeInvalidSnapshot;
            }
            if (snapInfo.GetUser() != user) {
                LOG(ERROR) << "Clone snapshot by invalid user"
                           << ", source = " << source
                           << ", user = " << user
                           << ", destination = " << destination
                           << ", snapshot.user = " << snapInfo.GetUser();
                return kErrCodeInvalidUser;
            }
            fileType = CloneFileType::kSnapshot;
            snapshotRef_->IncrementSnapshotRef(source);
        }
    }
    if (ret < 0) {
        FInfo fInfo;
        ret = client_->GetFileInfo(source, mdsRootUser_, &fInfo);
        switch (ret) {
            case LIBCURVE_ERROR::OK:
                fileType = CloneFileType::kFile;
                break;
            case -LIBCURVE_ERROR::NOTEXIST:
            case -LIBCURVE_ERROR::PARAM_ERROR:
                LOG(ERROR) << "Clone source file not exist"
                           << ", source = " << source
                           << ", user = " << user
                           << ", destination = " << destination;
                return kErrCodeFileNotExist;
            default:
                LOG(ERROR) << "GetFileInfo encounter an error"
                           << ", ret = " << ret
                           << ", source = " << source
                           << ", user = " << user;
                return kErrCodeInternalError;
        }
        if (fInfo.filestatus != FileStatus::Created &&
            fInfo.filestatus != FileStatus::Cloned &&
            fInfo.filestatus != FileStatus::BeingCloned) {
            LOG(ERROR) << "Can not clone when file status = "
                       << static_cast<int>(fInfo.filestatus);
            return kErrCodeFileStatusInvalid;
        }

        // TODO(镜像克隆的用户认证待完善)
    }

    UUID uuid = UUIDGenerator().GenerateUUID();
    CloneInfo info(uuid, user, taskType,
        source, destination, fileType, lazyFlag);
    if (CloneTaskType::kClone == taskType) {
        info.SetStatus(CloneStatus::cloning);
    } else {
        info.SetStatus(CloneStatus::recovering);
    }
    // 这里必须先AddCloneInfo， 因为如果先SetCloneFileStatus，然后AddCloneInfo，
    // 如果AddCloneInfo失败又意外重启，将没人知道SetCloneFileStatus调用过，造成
    // 镜像无法删除
    ret = metaStore_->AddCloneInfo(info);
    if (ret < 0) {
        LOG(ERROR) << "AddCloneInfo error"
                   << ", ret = " << ret
                   << ", taskId = " << uuid
                   << ", user = " << user
                   << ", source = " << source
                   << ", destination = " << destination;
        if (CloneFileType::kSnapshot == fileType) {
            snapshotRef_->DecrementSnapshotRef(source);
        }
        return ret;
    }
    if (CloneFileType::kFile == fileType) {
        NameLockGuard lockGuard(cloneRef_->GetLock(), source);
        ret = client_->SetCloneFileStatus(source,
            FileStatus::BeingCloned,
            mdsRootUser_);
        if (ret < 0) {
            // 这里不处理SetCloneFileStatus的错误，
            // 因为SetCloneFileStatus失败的所有结果都是可接受的，
            // 相比于处理SetCloneFileStatus失败的情况更直接:
            // 比如调用DeleteCloneInfo删除任务，
            // 一旦DeleteCloneInfo失败，给用户返回error之后，
            // 重启服务将造成Clone继续进行,
            // 跟用户结果返回的结果不一致，造成用户的困惑
            LOG(WARNING) << "SetCloneFileStatus encounter an error"
                         << ", ret = " << ret
                         << ", source = " << source
                         << ", user = " << user;
        }
        cloneRef_->IncrementRef(source);
    }

    *cloneInfo = info;
    return kErrCodeSuccess;
}

constexpr uint32_t kProgressCreateCloneFile = 1;
constexpr uint32_t kProgressCreateCloneMeta = 2;
constexpr uint32_t kProgressCreateCloneChunk = 5;
constexpr uint32_t kProgressRecoverChunkBegin = kProgressCreateCloneChunk;
constexpr uint32_t kProgressRecoverChunkEnd = 95;
constexpr uint32_t kProgressCloneComplete = 100;

void CloneCoreImpl::HandleCloneOrRecoverTask(
    std::shared_ptr<CloneTaskInfo> task) {
    brpc::ClosureGuard doneGuard(task->GetClosure().get());
    int ret = kErrCodeSuccess;
    FInfo newFileInfo;
    CloneSegmentMap segInfos;
    if (IsSnapshot(task)) {
        ret = BuildFileInfoFromSnapshot(task, &newFileInfo, &segInfos);
        if (ret < 0) {
            HandleCloneError(task, ret);
            return;
        }
    } else {
        ret = BuildFileInfoFromFile(task, &newFileInfo, &segInfos);
        if (ret < 0) {
            HandleCloneError(task, ret);
            return;
        }
    }

    // 在kCreateCloneMeta以后的步骤还需更新CloneChunkInfo信息中的chunkIdInfo
    if (NeedUpdateCloneMeta(task)) {
        ret = CreateOrUpdateCloneMeta(task, &newFileInfo, &segInfos);
        if (ret < 0) {
            HandleCloneError(task, ret);
            return;
        }
    }

    // 非lazy直接返回rpc
    if (!IsLazy(task)) {
        task->GetClosure()->SetErrCode(ret);
        task->GetClosure()->Run();
        doneGuard.release();
    }

    CloneStep step = task->GetCloneInfo().GetNextStep();
    while (step != CloneStep::kEnd) {
        switch (step) {
            case CloneStep::kCreateCloneFile:
                ret = CreateCloneFile(task, newFileInfo);
                if (ret < 0) {
                    HandleCloneError(task, ret);
                    return;
                }
                task->SetProgress(kProgressCreateCloneFile);
                break;
            case CloneStep::kCreateCloneMeta:
                ret = CreateCloneMeta(task, &newFileInfo, &segInfos);
                if (ret < 0) {
                    HandleCloneError(task, ret);
                    return;
                }
                task->SetProgress(kProgressCreateCloneMeta);
                break;
            case CloneStep::kCreateCloneChunk:
                ret = CreateCloneChunk(task, newFileInfo, segInfos);
                if (ret < 0) {
                    HandleCloneError(task, ret);
                    return;
                }
                task->SetProgress(kProgressCreateCloneChunk);
                break;
            case CloneStep::kCompleteCloneMeta:
                ret = CompleteCloneMeta(task, newFileInfo, segInfos);
                if (ret < 0) {
                    HandleCloneError(task, ret);
                    return;
                }
                break;
            case CloneStep::kRecoverChunk:
                ret = RecoverChunk(task, newFileInfo, segInfos);
                if (ret < 0) {
                    HandleCloneError(task, ret);
                    return;
                }
                break;
            case CloneStep::kChangeOwner:
                ret = ChangeOwner(task, newFileInfo);
                if (ret < 0) {
                    HandleCloneError(task, ret);
                    return;
                }
                break;
            case CloneStep::kRenameCloneFile:
                ret = RenameCloneFile(task, newFileInfo);
                if (ret < 0) {
                    HandleCloneError(task, ret);
                    return;
                }
                if (IsLazy(task)) {
                    task->GetClosure()->SetErrCode(ret);
                    task->GetClosure()->Run();
                    doneGuard.release();
                }
                break;
            case CloneStep::kCompleteCloneFile:
                ret = CompleteCloneFile(task, newFileInfo, segInfos);
                if (ret < 0) {
                    HandleCloneError(task, ret);
                    return;
                }
                break;
            default:
                LOG(ERROR) << "can not reach here"
                           << ", taskid = " << task->GetTaskId();
                HandleCloneError(task, ret);
                return;
        }
        task->UpdateMetric();
        step = task->GetCloneInfo().GetNextStep();
    }
    HandleCloneSuccess(task);
}

int CloneCoreImpl::BuildFileInfoFromSnapshot(
    std::shared_ptr<CloneTaskInfo> task,
    FInfo *newFileInfo,
    CloneSegmentMap *segInfos) {
    segInfos->clear();
    UUID source = task->GetCloneInfo().GetSrc();

    SnapshotInfo snapInfo;
    int ret = metaStore_->GetSnapshotInfo(source, &snapInfo);
    if (ret < 0) {
        LOG(ERROR) << "GetSnapshotInfo error"
                   << ", source = " << source
                   << ", taskid = " << task->GetTaskId();
        return ret;
    }
    newFileInfo->chunksize = snapInfo.GetChunkSize();
    newFileInfo->segmentsize = snapInfo.GetSegmentSize();
    newFileInfo->length = snapInfo.GetFileLength();
    if (IsRecover(task)) {
        FInfo fInfo;
        std::string destination = task->GetCloneInfo().GetDest();
        std::string user = task->GetCloneInfo().GetUser();
        ret = client_->GetFileInfo(destination, mdsRootUser_, &fInfo);
        if (ret != LIBCURVE_ERROR::OK) {
            LOG(ERROR) << "GetFileInfo fail"
                       << ", ret = " << ret
                       << ", destination = " << destination
                       << ", user = " << user
                       << ", taskid = " << task->GetTaskId();
            return kErrCodeInternalError;
        }
        // 从快照恢复的destinationId为目标文件的id
        task->GetCloneInfo().SetDestId(fInfo.id);
        // 从快照恢复seqnum+1
        newFileInfo->seqnum = fInfo.seqnum + 1;
    } else {
        newFileInfo->seqnum = kInitializeSeqNum;
    }
    newFileInfo->owner = task->GetCloneInfo().GetUser();

    ChunkIndexDataName indexName(snapInfo.GetFileName(),
         snapInfo.GetSeqNum());
    ChunkIndexData snapMeta;
    ret = dataStore_->GetChunkIndexData(indexName, &snapMeta);
    if (ret < 0) {
         LOG(ERROR) << "GetChunkIndexData error"
                    << ", fileName = " << snapInfo.GetFileName()
                    << ", seqNum = " << snapInfo.GetSeqNum()
                    << ", taskid = " << task->GetTaskId();
         return ret;
    }

    uint64_t segmentSize = snapInfo.GetSegmentSize();
    uint64_t chunkSize = snapInfo.GetChunkSize();
    uint64_t chunkPerSegment = segmentSize / chunkSize;

    std::vector<ChunkIndexType> chunkIndexs =
        snapMeta.GetAllChunkIndex();
    for (auto &chunkIndex : chunkIndexs) {
        ChunkDataName chunkDataName;
        snapMeta.GetChunkDataName(chunkIndex, &chunkDataName);
        uint64_t segmentIndex = chunkIndex / chunkPerSegment;
        CloneChunkInfo info;
        info.location = chunkDataName.ToDataChunkKey();
        if (IsRecover(task)) {
            info.seqNum = chunkDataName.chunkSeqNum_;
        } else {
            info.seqNum = kInitializeSeqNum;
        }

        auto it = segInfos->find(segmentIndex);
        if (it == segInfos->end()) {
            CloneSegmentInfo segInfo;
            segInfo.emplace(chunkIndex % chunkPerSegment, info);
            segInfos->emplace(segmentIndex, segInfo);
        } else {
            it->second.emplace(chunkIndex % chunkPerSegment, info);
        }
    }
    return kErrCodeSuccess;
}

int CloneCoreImpl::BuildFileInfoFromFile(
    std::shared_ptr<CloneTaskInfo> task,
    FInfo *newFileInfo,
    CloneSegmentMap *segInfos) {
    segInfos->clear();
    UUID source = task->GetCloneInfo().GetSrc();
    std::string user = task->GetCloneInfo().GetUser();

    FInfo fInfo;
    int ret = client_->GetFileInfo(source, mdsRootUser_, &fInfo);
    if (ret != LIBCURVE_ERROR::OK) {
        LOG(ERROR) << "GetFileInfo fail"
                   << ", ret = " << ret
                   << ", source = " << source
                   << ", user = " << user
                   << ", taskid = " << task->GetTaskId();
        return kErrCodeInternalError;
    }
    // GetOrAllocateSegment依赖fullPathName
    fInfo.fullPathName = source;

    newFileInfo->chunksize = fInfo.chunksize;
    newFileInfo->segmentsize = fInfo.segmentsize;
    newFileInfo->length = fInfo.length;
    newFileInfo->seqnum = kInitializeSeqNum;
    newFileInfo->owner = task->GetCloneInfo().GetUser();

    uint64_t fileLength = fInfo.length;
    uint64_t segmentSize = fInfo.segmentsize;
    uint64_t chunkSize = fInfo.chunksize;

    if (0 == segmentSize) {
        LOG(ERROR) << "GetFileInfo return invalid fileInfo, segmentSize == 0"
                   << ", taskid = " << task->GetTaskId();
        return kErrCodeInternalError;
    }
    if (fileLength%segmentSize != 0) {
        LOG(ERROR) << "GetFileInfo return invalid fileInfo, "
                   << "fileLength is not align to SegmentSize"
                   << ", taskid = " << task->GetTaskId();
        return kErrCodeInternalError;
    }

    for (uint64_t i = 0; i< fileLength/segmentSize; i++) {
        uint64_t offset = i * segmentSize;
        SegmentInfo segInfoOut;
        ret = client_->GetOrAllocateSegmentInfo(
                false, offset, &fInfo, mdsRootUser_, &segInfoOut);
        if (ret != LIBCURVE_ERROR::OK &&
            ret != -LIBCURVE_ERROR::NOT_ALLOCATE) {
            LOG(ERROR) << "GetOrAllocateSegmentInfo fail"
                       << ", ret = " << ret
                       << ", filename = " << source
                       << ", user = " << user
                       << ", offset = " << offset
                       << ", allocateIfNotExist = " << "false"
                       << ", taskid = " << task->GetTaskId();
            return kErrCodeInternalError;
        }
        if (segInfoOut.chunkvec.size() != 0) {
            CloneSegmentInfo segInfo;
            for (std::vector<ChunkIDInfo>::size_type j = 0;
                    j < segInfoOut.chunkvec.size(); j++) {
                CloneChunkInfo info;
                info.location = std::to_string(offset + j * chunkSize);
                info.seqNum = kInitializeSeqNum;
                segInfo.emplace(j, info);
            }
            segInfos->emplace(i, segInfo);
        }
    }
    return kErrCodeSuccess;
}


int CloneCoreImpl::CreateCloneFile(
    std::shared_ptr<CloneTaskInfo> task,
    const FInfo &fInfo) {
    std::string fileName =
        cloneTempDir_ + "/" + task->GetCloneInfo().GetTaskId();
    std::string user = fInfo.owner;
    uint64_t fileLength = fInfo.length;
    uint64_t seqNum = fInfo.seqnum;
    uint32_t chunkSize = fInfo.chunksize;

    FInfo fInfoOut;
    int ret = client_->CreateCloneFile(fileName,
        mdsRootUser_, fileLength, seqNum, chunkSize, &fInfoOut);
    if (ret == LIBCURVE_ERROR::OK) {
        // nothing
    } else if (ret == -LIBCURVE_ERROR::EXISTS) {
        ret = client_->GetFileInfo(fileName,
            mdsRootUser_, &fInfoOut);
        if (ret != LIBCURVE_ERROR::OK) {
            LOG(ERROR) << "GetFileInfo fail"
                       << ", ret = " << ret
                       << ", fileName = " << fileName
                       << ", taskid = " << task->GetTaskId();
            return kErrCodeInternalError;
        }
    } else {
        LOG(ERROR) << "CreateCloneFile file"
                   << ", ret = " << ret
                   << ", destination = " << fileName
                   << ", user = " << user
                   << ", fileLength = " << fileLength
                   << ", seqNum = " << seqNum
                   << ", chunkSize = " << chunkSize
                   << ", return fileId = " << fInfoOut.id
                   << ", taskid = " << task->GetTaskId();
        return kErrCodeInternalError;
    }
    task->GetCloneInfo().SetOriginId(fInfoOut.id);
    if (IsClone(task)) {
        // 克隆情况下destinationId = originId;
        task->GetCloneInfo().SetDestId(fInfoOut.id);
    }
    task->GetCloneInfo().SetTime(fInfoOut.ctime);
    task->GetCloneInfo().SetNextStep(CloneStep::kCreateCloneMeta);
    ret = metaStore_->UpdateCloneInfo(task->GetCloneInfo());
    if (ret < 0) {
        LOG(ERROR) << "UpdateCloneInfo after CreateCloneFile error."
                   << " ret = " << ret
                   << ", taskid = " << task->GetTaskId();
        return ret;
    }
    return kErrCodeSuccess;
}

int CloneCoreImpl::CreateCloneMeta(
    std::shared_ptr<CloneTaskInfo> task,
    FInfo *fInfo,
    CloneSegmentMap *segInfos) {
    int ret = CreateOrUpdateCloneMeta(task, fInfo, segInfos);
    if (ret < 0) {
        return ret;
    }
    task->GetCloneInfo().SetNextStep(CloneStep::kCreateCloneChunk);
    ret = metaStore_->UpdateCloneInfo(task->GetCloneInfo());
    if (ret < 0) {
        LOG(ERROR) << "UpdateCloneInfo after CreateCloneMeta error."
                   << " ret = " << ret
                   << ", taskid = " << task->GetTaskId();
        return ret;
    }
    return kErrCodeSuccess;
}

int CloneCoreImpl::CreateCloneChunk(
    std::shared_ptr<CloneTaskInfo> task,
    const FInfo &fInfo,
    const CloneSegmentMap &segInfos) {
    int ret = kErrCodeSuccess;
    uint32_t chunkSize = fInfo.chunksize;
    uint32_t correctSn = 0;
    // 克隆时correctSn为0，恢复时为新产生的文件版本
    if (IsClone(task)) {
        correctSn = 0;
    } else {
        correctSn = fInfo.seqnum;
    }
    auto tracker = std::make_shared<CreateCloneChunkTaskTracker>();
    for (auto & cloneSegmentInfo : segInfos) {
        for (auto & cloneChunkInfo : cloneSegmentInfo.second) {
            std::string location;
            if (IsSnapshot(task)) {
                location = LocationOperator::GenerateS3Location(
                    cloneChunkInfo.second.location);
            } else {
                location = LocationOperator::GenerateCurveLocation(
                    task->GetCloneInfo().GetSrc(),
                    std::stoull(cloneChunkInfo.second.location));
            }
            ChunkIDInfo cidInfo = cloneChunkInfo.second.chunkIdInfo;

            auto context = std::make_shared<CreateCloneChunkContext>();
            context->location = location;
            context->cidInfo = cidInfo;
            context->sn = cloneChunkInfo.second.seqNum;
            context->csn = correctSn;
            context->chunkSize = chunkSize;
            context->taskid = task->GetTaskId();
            context->startTime = TimeUtility::GetTimeofDaySec();
            context->clientAsyncMethodRetryTimeSec =
                clientAsyncMethodRetryTimeSec_;

            ret = StartAsyncCreateCloneChunk(task, tracker, context);
            if (ret < 0) {
                return kErrCodeInternalError;
            }

            if (tracker->GetTaskNum() >= createCloneChunkConcurrency_) {
                tracker->WaitSome(1);
            }
            std::list<CreateCloneChunkContextPtr> results =
                tracker->PopResultContexts();
            for (auto context : results) {
                if (context->retCode != LIBCURVE_ERROR::OK) {
                    uint64_t nowTime = TimeUtility::GetTimeofDaySec();
                    if (nowTime - context->startTime <
                        context->clientAsyncMethodRetryTimeSec) {
                        // retry
                        std::this_thread::sleep_for(
                            std::chrono::milliseconds(
                                clientAsyncMethodRetryIntervalMs_));
                        ret = StartAsyncCreateCloneChunk(
                            task, tracker, context);
                        if (ret < 0) {
                            return kErrCodeInternalError;
                        }
                    } else {
                        LOG(ERROR) << "CreateCloneChunk tracker GetResult fail"
                                   << ", ret = " << ret
                                   << ", taskid = " << task->GetTaskId();
                        return kErrCodeInternalError;
                    }
                }
            }
        }
    }
    // 最后剩余数量不足的任务
    do {
        tracker->WaitSome(1);
        std::list<CreateCloneChunkContextPtr> results =
            tracker->PopResultContexts();
        if (0 == results.size()) {
            // 已经完成，没有新的结果了
            break;
        }
        for (auto context : results) {
            if (context->retCode != LIBCURVE_ERROR::OK) {
                uint64_t nowTime = TimeUtility::GetTimeofDaySec();
                if (nowTime - context->startTime <
                    context->clientAsyncMethodRetryTimeSec) {
                    // retry
                    std::this_thread::sleep_for(
                        std::chrono::milliseconds(
                            clientAsyncMethodRetryIntervalMs_));
                    ret = StartAsyncCreateCloneChunk(
                        task, tracker, context);
                    if (ret < 0) {
                        return kErrCodeInternalError;
                    }
                } else {
                    LOG(ERROR) << "CreateCloneChunk tracker GetResult fail"
                               << ", ret = " << ret
                               << ", taskid = " << task->GetTaskId();
                    return kErrCodeInternalError;
                }
            }
        }
    } while (true);

    task->GetCloneInfo().SetNextStep(CloneStep::kCompleteCloneMeta);
    ret = metaStore_->UpdateCloneInfo(task->GetCloneInfo());
    if (ret < 0) {
        LOG(ERROR) << "UpdateCloneInfo after CreateCloneChunk error."
                   << " ret = " << ret
                   << ", taskid = " << task->GetTaskId();
        return kErrCodeInternalError;
    }
    return kErrCodeSuccess;
}

int CloneCoreImpl::StartAsyncCreateCloneChunk(
    std::shared_ptr<CloneTaskInfo> task,
    std::shared_ptr<CreateCloneChunkTaskTracker> tracker,
    std::shared_ptr<CreateCloneChunkContext> context) {
    CreateCloneChunkClosure *cb =
        new CreateCloneChunkClosure(tracker, context);
    tracker->AddOneTrace();
    LOG(INFO) << "Doing CreateCloneChunk"
              << ", location = " << context->location
              << ", logicalPoolId = " << context->cidInfo.lpid_
              << ", copysetId = " << context->cidInfo.cpid_
              << ", chunkId = " << context->cidInfo.cid_
              << ", seqNum = " << context->sn
              << ", csn = " << context->csn
              << ", taskid = " << task->GetTaskId();
    int ret = client_->CreateCloneChunk(context->location,
        context->cidInfo,
        context->sn,
        context->csn,
        context->chunkSize,
        cb);

    if (ret != LIBCURVE_ERROR::OK) {
        LOG(ERROR) << "CreateCloneChunk fail"
                   << ", ret = " << ret
                   << ", location = " << context->location
                   << ", logicalPoolId = " << context->cidInfo.lpid_
                   << ", copysetId = " << context->cidInfo.cpid_
                   << ", chunkId = " << context->cidInfo.cid_
                   << ", seqNum = " << context->sn
                   << ", csn = " << context->csn
                   << ", taskid = " << task->GetTaskId();
        return ret;
    }
    return kErrCodeSuccess;
}

int CloneCoreImpl::CompleteCloneMeta(
    std::shared_ptr<CloneTaskInfo> task,
    const FInfo &fInfo,
    const CloneSegmentMap &segInfos) {
    std::string origin =
        cloneTempDir_ + "/" + task->GetCloneInfo().GetTaskId();
    std::string user = task->GetCloneInfo().GetUser();
    int ret = client_->CompleteCloneMeta(origin, mdsRootUser_);
    if (ret != LIBCURVE_ERROR::OK) {
        LOG(ERROR) << "CompleteCloneMeta fail"
                   << ", ret = " << ret
                   << ", filename = " << origin
                   << ", user = " << user
                   << ", taskid = " << task->GetTaskId();
        return kErrCodeInternalError;
    }
    if (IsLazy(task)) {
        task->GetCloneInfo().SetNextStep(
            CloneStep::kChangeOwner);
    } else {
        task->GetCloneInfo().SetNextStep(
            CloneStep::kRecoverChunk);
    }
    ret = metaStore_->UpdateCloneInfo(task->GetCloneInfo());
    if (ret < 0) {
        LOG(ERROR) << "UpdateCloneInfo after CompleteCloneMeta error."
                   << " ret = " << ret
                   << ", taskid = " << task->GetTaskId();
        return ret;
    }
    return kErrCodeSuccess;
}

int CloneCoreImpl::RecoverChunk(
    std::shared_ptr<CloneTaskInfo> task,
    const FInfo &fInfo,
    const CloneSegmentMap &segInfos) {
    int ret = kErrCodeSuccess;
    uint32_t chunkSize = fInfo.chunksize;

    uint32_t totalProgress =
        kProgressRecoverChunkEnd - kProgressRecoverChunkBegin;
    uint32_t segNum = segInfos.size();
    double progressPerData = static_cast<double>(totalProgress) / segNum;
    uint32_t index = 0;

    if (0 == cloneChunkSplitSize_ ||
        chunkSize % cloneChunkSplitSize_ != 0) {
        LOG(ERROR) << "chunk is not align to cloneChunkSplitSize"
                   << ", taskid = " << task->GetTaskId();
        return kErrCodeChunkSizeNotAligned;
    }

    auto tracker = std::make_shared<RecoverChunkTaskTracker>();
    uint64_t workingChunkNum = 0;
    // 为避免发往同一个chunk碰撞，异步请求不同的chunk
    for (auto & cloneSegmentInfo : segInfos) {
        for (auto & cloneChunkInfo : cloneSegmentInfo.second) {
            // 当前并发工作的chunk数已大于要求的并发数时，先消化一部分
            while (workingChunkNum >= recoverChunkConcurrency_) {
                uint64_t completeChunkNum = 0;
                ret = ContinueAsyncRecoverChunkPartAndWaitSomeChunkEnd(task,
                    tracker,
                    &completeChunkNum);
                if (ret < 0) {
                    return kErrCodeInternalError;
                }
                workingChunkNum -= completeChunkNum;
            }
            // 加入新的工作的chunk
            workingChunkNum++;
            auto context = std::make_shared<RecoverChunkContext>();
            context->cidInfo = cloneChunkInfo.second.chunkIdInfo;
            context->totalPartNum = chunkSize / cloneChunkSplitSize_;
            context->partIndex = 0;
            context->partSize = cloneChunkSplitSize_;
            context->taskid = task->GetTaskId();
            context->startTime = TimeUtility::GetTimeofDaySec();
            context->clientAsyncMethodRetryTimeSec =
                clientAsyncMethodRetryTimeSec_;

            LOG(INFO) << "RecoverChunk start"
                       << ", logicalPoolId = "
                       << context->cidInfo.lpid_
                       << ", copysetId = " << context->cidInfo.cpid_
                       << ", chunkId = " << context->cidInfo.cid_
                       << ", len = " << context->partSize
                       << ", taskid = " << task->GetTaskId();

            ret = StartAsyncRecoverChunkPart(task, tracker, context);
            if (ret < 0) {
                return kErrCodeInternalError;
            }
        }
        task->SetProgress(static_cast<uint32_t>(
            kProgressRecoverChunkBegin + index * progressPerData));
        task->UpdateMetric();
        index++;
    }

    while (workingChunkNum > 0) {
        uint64_t completeChunkNum = 0;
        ret = ContinueAsyncRecoverChunkPartAndWaitSomeChunkEnd(task,
            tracker,
            &completeChunkNum);
        if (ret < 0) {
            return kErrCodeInternalError;
        }
        workingChunkNum -= completeChunkNum;
    }

    task->GetCloneInfo().SetNextStep(CloneStep::kCompleteCloneFile);
    ret = metaStore_->UpdateCloneInfo(task->GetCloneInfo());
    if (ret < 0) {
        LOG(ERROR) << "UpdateCloneInfo after RecoverChunk error."
                   << " ret = " << ret
                   << ", taskid = " << task->GetTaskId();
        return kErrCodeInternalError;
    }
    return kErrCodeSuccess;
}

int CloneCoreImpl::StartAsyncRecoverChunkPart(
    std::shared_ptr<CloneTaskInfo> task,
    std::shared_ptr<RecoverChunkTaskTracker> tracker,
    std::shared_ptr<RecoverChunkContext> context) {
    RecoverChunkClosure *cb = new RecoverChunkClosure(tracker, context);
    tracker->AddOneTrace();
    uint64_t offset = context->partIndex * context->partSize;
    LOG_EVERY_SECOND(INFO) << "Doing RecoverChunk"
               << ", logicalPoolId = "
               << context->cidInfo.lpid_
               << ", copysetId = " << context->cidInfo.cpid_
               << ", chunkId = " << context->cidInfo.cid_
               << ", offset = " << offset
               << ", len = " << context->partSize
               << ", taskid = " << task->GetTaskId();
    int ret = client_->RecoverChunk(context->cidInfo,
        offset,
        context->partSize,
        cb);
    if (ret != LIBCURVE_ERROR::OK) {
        LOG(ERROR) << "RecoverChunk fail"
                   << ", ret = " << ret
                   << ", logicalPoolId = "
                   << context->cidInfo.lpid_
                   << ", copysetId = " << context->cidInfo.cpid_
                   << ", chunkId = " << context->cidInfo.cid_
                   << ", offset = " << offset
                   << ", len = " << context->partSize
                   << ", taskid = " << task->GetTaskId();
        return ret;
    }
    return kErrCodeSuccess;
}

int CloneCoreImpl::ContinueAsyncRecoverChunkPartAndWaitSomeChunkEnd(
    std::shared_ptr<CloneTaskInfo> task,
    std::shared_ptr<RecoverChunkTaskTracker> tracker,
    uint64_t *completeChunkNum) {
    *completeChunkNum = 0;
    tracker->WaitSome(1);
    std::list<RecoverChunkContextPtr> results =
        tracker->PopResultContexts();
    for (auto context : results) {
        if (context->retCode != LIBCURVE_ERROR::OK) {
            uint64_t nowTime = TimeUtility::GetTimeofDaySec();
            if (nowTime - context->startTime <
                context->clientAsyncMethodRetryTimeSec) {
                // retry
                std::this_thread::sleep_for(
                    std::chrono::milliseconds(
                        clientAsyncMethodRetryIntervalMs_));
                int ret = StartAsyncRecoverChunkPart(task, tracker, context);
                if (ret < 0) {
                    return ret;
                }
            } else {
                LOG(ERROR) << "RecoverChunk tracker GetResult fail"
                           << ", ret = " << context->retCode
                           << ", taskid = " << task->GetTaskId();
                return context->retCode;
            }
        } else {
            // 启动一个新的分片，index++，并重置开始时间
            context->partIndex++;
            context->startTime = TimeUtility::GetTimeofDaySec();
            if (context->partIndex < context->totalPartNum) {
                int ret = StartAsyncRecoverChunkPart(task, tracker, context);
                if (ret < 0) {
                    return ret;
                }
            } else {
                LOG(INFO) << "RecoverChunk Complete"
                           << ", logicalPoolId = "
                           << context->cidInfo.lpid_
                           << ", copysetId = " << context->cidInfo.cpid_
                           << ", chunkId = " << context->cidInfo.cid_
                           << ", len = " << context->partSize
                           << ", taskid = " << task->GetTaskId();
                (*completeChunkNum)++;
            }
        }
    }
    return kErrCodeSuccess;
}

int CloneCoreImpl::ChangeOwner(
    std::shared_ptr<CloneTaskInfo> task,
    const FInfo &fInfo) {
    std::string user = task->GetCloneInfo().GetUser();
    std::string origin =
        cloneTempDir_ + "/" + task->GetCloneInfo().GetTaskId();

    int ret = client_->ChangeOwner(origin, user);
    if (ret != LIBCURVE_ERROR::OK) {
        LOG(ERROR) << "ChangeOwner fail, ret = " << ret
                   << ", fileName = " << origin
                   << ", newOwner = " << user
                   << ", taskid = " << task->GetTaskId();
        return kErrCodeInternalError;
    }

    task->GetCloneInfo().SetNextStep(CloneStep::kRenameCloneFile);
    ret = metaStore_->UpdateCloneInfo(task->GetCloneInfo());
    if (ret < 0) {
        LOG(ERROR) << "UpdateCloneInfo after ChangeOwner error."
                   << " ret = " << ret
                   << ", taskid = " << task->GetTaskId();
        return kErrCodeInternalError;
    }
    return kErrCodeSuccess;
}

int CloneCoreImpl::RenameCloneFile(
    std::shared_ptr<CloneTaskInfo> task,
    const FInfo &fInfo) {
    std::string user = fInfo.owner;
    uint64_t originId = task->GetCloneInfo().GetOriginId();
    uint64_t destinationId = task->GetCloneInfo().GetDestId();
    std::string origin =
        cloneTempDir_ + "/" + task->GetCloneInfo().GetTaskId();
    std::string destination = task->GetCloneInfo().GetDest();

    // 判断原始文件存不存在
    FInfo oriFInfo;
    int ret = client_->GetFileInfo(origin, mdsRootUser_, &oriFInfo);

    if (LIBCURVE_ERROR::OK == ret) {
        if (oriFInfo.id != originId) {
            LOG(ERROR) << "Origin File is missing, when rename clone file, "
                       << "exist file id = " << oriFInfo.id
                       << ", originId = " << originId
                       << ", taskid = " << task->GetTaskId();
            return kErrCodeInternalError;
        }
        ret = client_->RenameCloneFile(mdsRootUser_,
            originId,
            destinationId,
            origin,
            destination);
        if (ret != LIBCURVE_ERROR::OK) {
            LOG(ERROR) << "RenameCloneFile fail"
                       << ", ret = " << ret
                       << ", user = " << user
                       << ", originId = " << originId
                       << ", origin = " << origin
                       << ", destination = " << destination
                       << ", taskid = " << task->GetTaskId();
            return kErrCodeInternalError;
        }
    } else if (-LIBCURVE_ERROR::NOTEXIST == ret) {
        // 有可能是已经rename过了
        FInfo destFInfo;
        ret = client_->GetFileInfo(destination, mdsRootUser_, &destFInfo);
        if (ret != LIBCURVE_ERROR::OK) {
            LOG(ERROR) << "Origin File is missing, when rename clone file,"
                       << "GetFileInfo fail, ret = " << ret
                       << ", destination filename = " << destination
                       << ", taskid = " << task->GetTaskId();
            return kErrCodeInternalError;
        }
        if (destFInfo.id != originId) {
            LOG(ERROR) << "Origin File is missing, when rename clone file, "
                       << "originId = " << originId
                       << ", taskid = " << task->GetTaskId();
            return kErrCodeInternalError;
        }
    } else {
            LOG(ERROR) << "GetFileInfo fail, ret = " << ret
                       << ", origin filename = " << origin
                       << ", taskid = " << task->GetTaskId();
            return ret;
    }

    if (IsLazy(task)) {
        task->GetCloneInfo().SetNextStep(CloneStep::kRecoverChunk);
    } else {
        task->GetCloneInfo().SetNextStep(CloneStep::kEnd);
    }
    ret = metaStore_->UpdateCloneInfo(task->GetCloneInfo());
    if (ret < 0) {
        LOG(ERROR) << "UpdateCloneInfo after RenameCloneFile error."
                   << " ret = " << ret
                   << ", taskid = " << task->GetTaskId();
        return ret;
    }
    return kErrCodeSuccess;
}

int CloneCoreImpl::CompleteCloneFile(
    std::shared_ptr<CloneTaskInfo> task,
    const FInfo &fInfo,
    const CloneSegmentMap &segInfos) {
    std::string fileName;
    if (IsLazy(task)) {
        fileName = task->GetCloneInfo().GetDest();
    } else {
        fileName =
            cloneTempDir_ + "/" + task->GetCloneInfo().GetTaskId();
    }
    std::string user = task->GetCloneInfo().GetUser();
    int ret = client_->CompleteCloneFile(fileName, mdsRootUser_);
    if (ret != LIBCURVE_ERROR::OK) {
        LOG(ERROR) << "CompleteCloneFile fail"
                   << ", ret = " << ret
                   << ", fileName = " << fileName
                   << ", user = " << user
                   << ", taskid = " << task->GetTaskId();
        return kErrCodeInternalError;
    }
    if (IsLazy(task)) {
        task->GetCloneInfo().SetNextStep(CloneStep::kEnd);
    } else {
        task->GetCloneInfo().SetNextStep(CloneStep::kChangeOwner);
    }
    ret = metaStore_->UpdateCloneInfo(task->GetCloneInfo());
    if (ret < 0) {
        LOG(ERROR) << "UpdateCloneInfo after CompleteCloneFile error."
                   << " ret = " << ret
                   << ", taskid = " << task->GetTaskId();
        return ret;
    }
    return kErrCodeSuccess;
}

void CloneCoreImpl::HandleCloneSuccess(std::shared_ptr<CloneTaskInfo> task) {
    int ret = kErrCodeSuccess;
    if (IsSnapshot(task)) {
        snapshotRef_->DecrementSnapshotRef(task->GetCloneInfo().GetSrc());
    } else {
        std::string source = task->GetCloneInfo().GetSrc();
        cloneRef_->DecrementRef(source);
        NameLockGuard lockGuard(cloneRef_->GetLock(), source);
        if (cloneRef_->GetRef(source) == 0) {
            int ret = client_->SetCloneFileStatus(source,
                    FileStatus::Created, mdsRootUser_);
            if (ret < 0) {
                task->GetCloneInfo().SetStatus(CloneStatus::error);
                int ret2 = metaStore_->UpdateCloneInfo(task->GetCloneInfo());
                if (ret2 < 0) {
                    LOG(ERROR) << "UpdateCloneInfo Task error Fail!"
                               << " ret = " << ret2
                               << ", uuid = " << task->GetTaskId();
                }
                LOG(ERROR) << "Task Fail cause by SetCloneFileStatus fail"
                           << ", ret = " << ret
                           << ", taskid = " << task->GetCloneInfo().GetTaskId()
                           << ", source = " << task->GetCloneInfo().GetSrc()
                           << ", dest = " << task->GetCloneInfo().GetDest()
                           << ", IsLazy = " << task->GetCloneInfo().GetIsLazy()
                           << ", step = "
                           << static_cast<int>(
                                   task->GetCloneInfo().GetNextStep());
                task->Finish();
                return;
            }
        }
    }
    task->GetCloneInfo().SetStatus(CloneStatus::done);
    ret = metaStore_->UpdateCloneInfo(task->GetCloneInfo());
    if (ret < 0) {
        LOG(ERROR) << "UpdateCloneInfo Task Success Fail!"
                   << " ret = " << ret
                   << ", uuid = " << task->GetTaskId();
    }
    task->SetProgress(kProgressCloneComplete);

    LOG(INFO) << "Task Success"
              << ", taskid = " << task->GetCloneInfo().GetTaskId()
              << ", source = " << task->GetCloneInfo().GetSrc()
              << ", dest = " << task->GetCloneInfo().GetDest()
              << ", IsLazy = " << task->GetCloneInfo().GetIsLazy();
    task->Finish();
    return;
}

void CloneCoreImpl::HandleCloneError(std::shared_ptr<CloneTaskInfo> task,
    int retCode) {
    int ret = kErrCodeSuccess;
    if (NeedRetry(task)) {
        HandleCloneToRetry(task);
        return;
    }

    task->GetClosure()->SetErrCode(retCode);
    if (IsSnapshot(task)) {
        snapshotRef_->DecrementSnapshotRef(task->GetCloneInfo().GetSrc());
    } else {
        std::string source = task->GetCloneInfo().GetSrc();
        cloneRef_->DecrementRef(source);
        NameLockGuard lockGuard(cloneRef_->GetLock(), source);
        if (cloneRef_->GetRef(source) == 0) {
            ret = client_->SetCloneFileStatus(source,
                    FileStatus::Created, mdsRootUser_);
            if (ret < 0) {
                LOG(ERROR) << "SetCloneFileStatus fail, ret = " << ret
                           << ", taskid = " << task->GetTaskId();
            }
        }
    }
    task->GetCloneInfo().SetStatus(CloneStatus::error);
    ret = metaStore_->UpdateCloneInfo(task->GetCloneInfo());
    if (ret < 0) {
        LOG(ERROR) << "UpdateCloneInfo Task error Fail!"
                   << " ret = " << ret
                   << ", uuid = " << task->GetTaskId();
    }
    LOG(ERROR) << "Task Fail"
               << ", taskid = " << task->GetCloneInfo().GetTaskId()
               << ", source = " << task->GetCloneInfo().GetSrc()
               << ", dest = " << task->GetCloneInfo().GetDest()
               << ", IsLazy = " << task->GetCloneInfo().GetIsLazy()
               << ", step = "
               << static_cast<int>(
                       task->GetCloneInfo().GetNextStep());
    task->Finish();
    return;
}

void CloneCoreImpl::HandleCloneToRetry(std::shared_ptr<CloneTaskInfo> task) {
    task->GetCloneInfo().SetStatus(CloneStatus::retrying);
    int ret = metaStore_->UpdateCloneInfo(task->GetCloneInfo());
    if (ret < 0) {
        LOG(ERROR) << "UpdateCloneInfo Task retrying Fail!"
                   << " ret = " << ret
                   << ", uuid = " << task->GetTaskId();
    }
    LOG(WARNING) << "Task Fail, Retrying"
               << ", taskid = " << task->GetCloneInfo().GetTaskId()
               << ", source = " << task->GetCloneInfo().GetSrc()
               << ", dest = " << task->GetCloneInfo().GetDest()
               << ", IsLazy = " << task->GetCloneInfo().GetIsLazy()
               << ", step = "
               << static_cast<int>(
                       task->GetCloneInfo().GetNextStep());
    task->Finish();
    return;
}

void CloneCoreImpl::HandleCleanSuccess(std::shared_ptr<CloneTaskInfo> task) {
    TaskIdType taskId = task->GetCloneInfo().GetTaskId();
    int ret = metaStore_->DeleteCloneInfo(taskId);
    if (ret < 0) {
        LOG(ERROR) << "DeleteCloneInfo failed"
                   << ", ret = " << ret
                   << ", taskId = " << taskId;
    } else {
        LOG(INFO) << "Clean Task Success"
                  << ", taskid = " << task->GetCloneInfo().GetTaskId()
                  << ", source = " << task->GetCloneInfo().GetSrc()
                  << ", dest = " << task->GetCloneInfo().GetDest()
                  << ", IsLazy = " << task->GetCloneInfo().GetIsLazy();
    }
    task->SetProgress(kProgressCloneComplete);
    task->GetCloneInfo().SetStatus(CloneStatus::done);

    task->Finish();
    return;
}

void CloneCoreImpl::HandleCleanError(std::shared_ptr<CloneTaskInfo> task) {
    task->GetCloneInfo().SetStatus(CloneStatus::error);
    int ret = metaStore_->UpdateCloneInfo(task->GetCloneInfo());
    if (ret < 0) {
        LOG(ERROR) << "UpdateCloneInfo Task error Fail!"
                   << " ret = " << ret
                   << ", uuid = " << task->GetTaskId();
    }
    LOG(ERROR) << "Clean Task Fail"
               << ", taskid = " << task->GetCloneInfo().GetTaskId()
               << ", source = " << task->GetCloneInfo().GetSrc()
               << ", dest = " << task->GetCloneInfo().GetDest()
               << ", IsLazy = " << task->GetCloneInfo().GetIsLazy()
               << ", step = "
               << static_cast<int>(
                       task->GetCloneInfo().GetNextStep());
    task->Finish();
    return;
}

int CloneCoreImpl::GetCloneInfoList(std::vector<CloneInfo> *taskList) {
    metaStore_->GetCloneInfoList(taskList);
    return kErrCodeSuccess;
}

int CloneCoreImpl::GetCloneInfo(TaskIdType taskId, CloneInfo *cloneInfo) {
    return metaStore_->GetCloneInfo(taskId, cloneInfo);
}

int CloneCoreImpl::GetCloneInfoByFileName(
    const std::string &fileName, CloneInfo *cloneInfo) {
    return metaStore_->GetCloneInfoByFileName(fileName, cloneInfo);
}

inline bool CloneCoreImpl::IsLazy(std::shared_ptr<CloneTaskInfo> task) {
    return task->GetCloneInfo().GetIsLazy();
}

inline bool CloneCoreImpl::IsSnapshot(std::shared_ptr<CloneTaskInfo> task) {
    return CloneFileType::kSnapshot == task->GetCloneInfo().GetFileType();
}

inline bool CloneCoreImpl::IsFile(std::shared_ptr<CloneTaskInfo> task) {
    return CloneFileType::kFile == task->GetCloneInfo().GetFileType();
}

inline bool CloneCoreImpl::IsRecover(std::shared_ptr<CloneTaskInfo> task) {
    return CloneTaskType::kRecover == task->GetCloneInfo().GetTaskType();
}

inline bool CloneCoreImpl::IsClone(std::shared_ptr<CloneTaskInfo> task) {
    return CloneTaskType::kClone == task->GetCloneInfo().GetTaskType();
}

bool CloneCoreImpl::NeedUpdateCloneMeta(
    std::shared_ptr<CloneTaskInfo> task) {
    bool ret = true;
    CloneStep step = task->GetCloneInfo().GetNextStep();
    if (CloneStep::kCreateCloneFile == step ||
        CloneStep::kCreateCloneMeta == step ||
        CloneStep::kEnd == step) {
        ret = false;
    }
    return ret;
}

bool CloneCoreImpl::NeedRetry(std::shared_ptr<CloneTaskInfo> task) {
    if (IsLazy(task)) {
        CloneStep step = task->GetCloneInfo().GetNextStep();
        if (CloneStep::kRecoverChunk == step ||
            CloneStep::kCompleteCloneFile == step ||
            CloneStep::kEnd == step) {
            return true;
        }
    }
    return false;
}

int CloneCoreImpl::CreateOrUpdateCloneMeta(
    std::shared_ptr<CloneTaskInfo> task,
    FInfo *fInfo,
    CloneSegmentMap *segInfos) {
    std::string newFileName =
        cloneTempDir_ + "/" + task->GetCloneInfo().GetTaskId();
    std::string user = fInfo->owner;
    FInfo fInfoOut;
    int ret = client_->GetFileInfo(newFileName, mdsRootUser_, &fInfoOut);
    if (LIBCURVE_ERROR::OK == ret) {
        // nothing
    } else if (-LIBCURVE_ERROR::NOTEXIST == ret) {
        // 可能已经rename过了
        newFileName = task->GetCloneInfo().GetDest();
        ret = client_->GetFileInfo(newFileName, mdsRootUser_, &fInfoOut);
        if (ret != LIBCURVE_ERROR::OK) {
            LOG(ERROR) << "File is missing, "
                       << "when CreateOrUpdateCloneMeta, "
                       << "GetFileInfo fail, ret = " << ret
                       << ", filename = " << newFileName
                       << ", taskid = " << task->GetTaskId();
            return kErrCodeInternalError;
        }
        // 如果是已经rename过，那么id应该一致
        uint64_t originId = task->GetCloneInfo().GetOriginId();
        if (fInfoOut.id != originId) {
            LOG(ERROR) << "File is missing, "
                       << "when CreateOrUpdateCloneMeta, "
                       << "originId = " << originId
                       << ", filename = " << newFileName
                       << ", taskid = " << task->GetTaskId();
            return kErrCodeInternalError;
        }
    } else {
        LOG(ERROR) << "GetFileInfo fail"
                   << ", ret = " << ret
                   << ", filename = " << newFileName
                   << ", user = " << user
                   << ", taskid = " << task->GetTaskId();
        return kErrCodeInternalError;
    }
    // 更新fInfo
    *fInfo = fInfoOut;
    // GetOrAllocateSegment 依赖fullPathName，需要在此处更新
    fInfo->fullPathName = newFileName;

    uint32_t segmentSize = fInfo->segmentsize;
    for (auto &segInfo : *segInfos) {
        SegmentInfo segInfoOut;
        uint64_t offset = segInfo.first * segmentSize;
        ret = client_->GetOrAllocateSegmentInfo(
            true, offset, fInfo, mdsRootUser_, &segInfoOut);
        if (ret != LIBCURVE_ERROR::OK) {
            LOG(ERROR) << "GetOrAllocateSegmentInfo fail"
                       << ", newFileName = " << newFileName
                       << ", user = " << user
                       << ", offset = " << offset
                       << ", allocateIfNotExist = " << "true"
                       << ", taskid = " << task->GetTaskId();
            return kErrCodeInternalError;
        }

        for (auto &cloneChunkInfo : segInfo.second) {
            if (cloneChunkInfo.first > segInfoOut.chunkvec.size()) {
                LOG(ERROR) << "can not find chunkIndexInSeg = "
                           << cloneChunkInfo.first
                           << ", segmentIndex = " << segInfo.first
                           << ", logicalPoolId = "
                           << cloneChunkInfo.second.chunkIdInfo.lpid_
                           << ", copysetId = "
                           << cloneChunkInfo.second.chunkIdInfo.cpid_
                           << ", chunkId = "
                           << cloneChunkInfo.second.chunkIdInfo.cid_
                           << ", taskid = " << task->GetTaskId();
                return kErrCodeInternalError;
            }
            cloneChunkInfo.second.chunkIdInfo =
                segInfoOut.chunkvec[cloneChunkInfo.first];
        }
    }
    return kErrCodeSuccess;
}

int CloneCoreImpl::CleanCloneOrRecoverTaskPre(const std::string &user,
    const TaskIdType &taskId,
    CloneInfo *cloneInfo) {
    int ret = metaStore_->GetCloneInfo(taskId, cloneInfo);
    if (ret < 0) {
        // 不存在时直接返回成功，使接口幂等
        return kErrCodeSuccess;
    }
    if (cloneInfo->GetUser() != user) {
        LOG(ERROR) << "CleanCloneOrRecoverTaskPre by Invalid user";
        return kErrCodeInvalidUser;
    }
    switch (cloneInfo->GetStatus()) {
        case CloneStatus::done:
            cloneInfo->SetStatus(CloneStatus::cleaning);
            break;
        case CloneStatus::error:
            cloneInfo->SetStatus(CloneStatus::errorCleaning);
            break;
        case CloneStatus::cleaning:
        case CloneStatus::errorCleaning:
            return kErrCodeTaskExist;
            break;
        default:
            LOG(ERROR) << "Can not clean clone/recover task unfinished.";
            return kErrCodeCannotCleanCloneUnfinished;
            break;
    }

    ret = metaStore_->UpdateCloneInfo(*cloneInfo);
    if (ret < 0) {
        LOG(ERROR) << "UpdateCloneInfo fail"
                   << ", ret = " << ret
                   << ", taskId = " << taskId;
        return ret;
    }
    return kErrCodeSuccess;
}

void CloneCoreImpl::HandleCleanCloneOrRecoverTask(
    std::shared_ptr<CloneTaskInfo> task) {
    // 只有错误的clone/recover任务才清理临时文件
    if (CloneStatus::errorCleaning == task->GetCloneInfo().GetStatus()) {
        // 错误情况下可能未清除镜像被克隆标志
        if (IsFile(task)) {
            //重新发送
            std::string source = task->GetCloneInfo().GetSrc();
            NameLockGuard lockGuard(cloneRef_->GetLock(), source);
            if (cloneRef_->GetRef(source) == 0) {
                int ret = client_->SetCloneFileStatus(source,
                        FileStatus::Created, mdsRootUser_);
                if (ret < 0) {
                    LOG(ERROR) << "SetCloneFileStatus fail, ret = " << ret
                               << ", taskid = " << task->GetTaskId();
                    HandleCleanError(task);
                    return;
                }
            }
        }
        std::string tempFileName =
            cloneTempDir_ + "/" + task->GetCloneInfo().GetTaskId();
        uint64_t fileId = task->GetCloneInfo().GetOriginId();
        std::string user =
            task->GetCloneInfo().GetUser();
        int ret = client_->DeleteFile(tempFileName, mdsRootUser_, fileId);
        if (ret != LIBCURVE_ERROR::OK &&
            ret != -LIBCURVE_ERROR::NOTEXIST) {
            LOG(ERROR) << "DeleteFile failed"
                       << ", ret = " << ret
                       << ", fileName = " << tempFileName
                       << ", user = " << user
                       << ", fileId = " << fileId
                       << ", taskid = " << task->GetTaskId();
            HandleCleanError(task);
            return;
        }
    }
    HandleCleanSuccess(task);
    return;
}

}  // namespace snapshotcloneserver
}  // namespace curve

