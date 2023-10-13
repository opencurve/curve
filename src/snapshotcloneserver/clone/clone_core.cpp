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
 * Created Date: Tue Mar 26 2019
 * Author: xuchaojie
 */

#include "src/snapshotcloneserver/clone/clone_core.h"

#include <list>
#include <memory>
#include <string>
#include <vector>

#include "src/common/concurrent/name_lock.h"
#include "src/common/location_operator.h"
#include "src/common/uuid.h"
#include "src/snapshotcloneserver/clone/clone_task.h"

using ::curve::common::LocationOperator;
using ::curve::common::NameLock;
using ::curve::common::NameLockGuard;
using ::curve::common::UUIDGenerator;

namespace curve {
namespace snapshotcloneserver {

int CloneCoreImpl::Init() {
    int ret = client_->Mkdir(cloneTempDir_, mdsRootUser_);
    if (ret != LIBCURVE_ERROR::OK && ret != -LIBCURVE_ERROR::EXISTS) {
        LOG(ERROR) << "Mkdir fail, ret = " << ret
                   << ", dirpath = " << cloneTempDir_;
        return kErrCodeServerInitFail;
    }
    return kErrCodeSuccess;
}

int CloneCoreImpl::CloneOrRecoverPre(const UUID& source,
                                     const std::string& user,
                                     const std::string& destination,
                                     bool lazyFlag, CloneTaskType taskType,
                                     std::string poolset,
                                     CloneInfo* cloneInfo) {
    // Check if there are tasks executing in the database
    std::vector<CloneInfo> cloneInfoList;
    metaStore_->GetCloneInfoByFileName(destination, &cloneInfoList);
    bool needJudgeFileExist = false;
    std::vector<CloneInfo> existCloneInfos;
    for (auto& info : cloneInfoList) {
        LOG(INFO) << "CloneOrRecoverPre find same clone task"
                  << ", source = " << source << ", user = " << user
                  << ", destination = " << destination
                  << ", poolset = " << poolset
                  << ", Exist CloneInfo : " << info;
        // is clone
        if (taskType == CloneTaskType::kClone) {
            if (info.GetStatus() == CloneStatus::cloning ||
                info.GetStatus() == CloneStatus::retrying) {
                if ((info.GetUser() == user) && (info.GetSrc() == source) &&
                    (info.GetIsLazy() == lazyFlag) &&
                    (info.GetTaskType() == taskType)) {
                    // Treat as the same clone
                    *cloneInfo = info;
                    return kErrCodeTaskExist;
                } else {
                    // Treat it as a different clone, then the file is actually
                    // occupied and the return file already exists
                    return kErrCodeFileExist;
                }
            } else if (info.GetStatus() == CloneStatus::done ||
                       info.GetStatus() == CloneStatus::error ||
                       info.GetStatus() == CloneStatus::metaInstalled) {
                // It may have been deleted, and it is necessary to determine
                // whether the file exists again, Allowing further cloning under
                // deleted conditions
                existCloneInfos.push_back(info);
                needJudgeFileExist = true;
            } else {
                // At this point, the same clone task is being deleted and the
                // return file is occupied
                return kErrCodeFileExist;
            }
        } else {  // is recover
            if (info.GetStatus() == CloneStatus::recovering ||
                info.GetStatus() == CloneStatus::retrying) {
                if ((info.GetUser() == user) && (info.GetSrc() == source) &&
                    (info.GetIsLazy() == lazyFlag) &&
                    (info.GetTaskType() == taskType)) {
                    // Treat as the same clone, return task already exists
                    *cloneInfo = info;
                    return kErrCodeTaskExist;
                } else {
                    // Treat it as a different clone, then the file is actually
                    // occupied and the return file already exists
                    return kErrCodeFileExist;
                }
            } else if (info.GetStatus() == CloneStatus::done ||
                       info.GetStatus() == CloneStatus::error ||
                       info.GetStatus() == CloneStatus::metaInstalled) {
                // nothing
            } else {
                // At this point, the same task is being deleted and the return
                // file is occupied
                return kErrCodeFileExist;
            }
        }
    }

    // The target file already exists and cannot be cloned or recovered if it
    // does not exist
    FInfo destFInfo;
    int ret = client_->GetFileInfo(destination, mdsRootUser_, &destFInfo);
    switch (ret) {
        case LIBCURVE_ERROR::OK:
            if (CloneTaskType::kClone == taskType) {
                if (needJudgeFileExist) {
                    bool match = false;
                    // Find the cloneInfo that matches the inodeid
                    for (auto& existInfo : existCloneInfos) {
                        if (destFInfo.id == existInfo.GetDestId()) {
                            *cloneInfo = existInfo;
                            match = true;
                            break;
                        }
                    }
                    if (match) {
                        return kErrCodeTaskExist;
                    } else {
                        // If not found, then none of the dest files were
                        // created by these clone tasks, It means the file has a
                        // duplicate name
                        LOG(ERROR)
                            << "Clone dest file exist, "
                            << "but task not match! "
                            << "source = " << source << ", user = " << user
                            << ", destination = " << destination
                            << ", poolset = " << poolset;
                        return kErrCodeFileExist;
                    }
                } else {
                    // There is no corresponding cloneInfo, which means the file
                    // has a duplicate name
                    LOG(ERROR) << "Clone dest file must not exist"
                               << ", source = " << source << ", user = " << user
                               << ", destination = " << destination
                               << ", poolset = " << poolset;
                    return kErrCodeFileExist;
                }
            } else if (CloneTaskType::kRecover == taskType) {
                // The recover task keeps the poolset information of the volume
                // unchanged
                poolset = destFInfo.poolset;
            } else {
                assert(false);
            }
            break;
        case -LIBCURVE_ERROR::NOTEXIST:
            if (CloneTaskType::kRecover == taskType) {
                LOG(ERROR) << "Recover dest file must exist"
                           << ", source = " << source << ", user = " << user
                           << ", destination = " << destination;
                return kErrCodeFileNotExist;
            }
            break;
        default:
            LOG(ERROR) << "GetFileInfo encounter an error"
                       << ", ret = " << ret << ", source = " << source
                       << ", user = " << user;
            return kErrCodeInternalError;
    }

    // Is it a snapshot
    SnapshotInfo snapInfo;
    CloneFileType fileType;

    {
        NameLockGuard lockSnapGuard(snapshotRef_->GetSnapshotLock(), source);
        ret = metaStore_->GetSnapshotInfo(source, &snapInfo);
        if (0 == ret) {
            if (CloneTaskType::kRecover == taskType &&
                destination != snapInfo.GetFileName()) {
                LOG(ERROR) << "Can not recover from the snapshot "
                           << "which is not belong to the destination volume.";
                return kErrCodeInvalidSnapshot;
            }
            if (snapInfo.GetStatus() != Status::done) {
                LOG(ERROR) << "Can not clone by snapshot has status:"
                           << static_cast<int>(snapInfo.GetStatus());
                return kErrCodeInvalidSnapshot;
            }
            if (snapInfo.GetUser() != user) {
                LOG(ERROR) << "Clone snapshot by invalid user"
                           << ", source = " << source << ", user = " << user
                           << ", destination = " << destination
                           << ", poolset = " << poolset
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
                           << ", source = " << source << ", user = " << user
                           << ", destination = " << destination
                           << ", poolset = " << poolset;
                return kErrCodeFileNotExist;
            default:
                LOG(ERROR) << "GetFileInfo encounter an error"
                           << ", ret = " << ret << ", source = " << source
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

        // TODO (User authentication for mirror cloning to be improved)
    }

    UUID uuid = UUIDGenerator().GenerateUUID();
    CloneInfo info(uuid, user, taskType, source, destination, poolset, fileType,
                   lazyFlag);
    if (CloneTaskType::kClone == taskType) {
        info.SetStatus(CloneStatus::cloning);
    } else {
        info.SetStatus(CloneStatus::recovering);
    }
    // Here, you must first AddCloneInfo because if you first set
    // CloneFileStatus and then AddCloneInfo, If AddCloneInfo fails and
    // unexpectedly restarts, no one will know that SetCloneFileStatus has been
    // called, causing Mirror cannot be deleted
    ret = metaStore_->AddCloneInfo(info);
    if (ret < 0) {
        LOG(ERROR) << "AddCloneInfo error"
                   << ", ret = " << ret << ", taskId = " << uuid
                   << ", user = " << user << ", source = " << source
                   << ", destination = " << destination
                   << ", poolset = " << poolset;
        if (CloneFileType::kSnapshot == fileType) {
            snapshotRef_->DecrementSnapshotRef(source);
        }
        return ret;
    }
    if (CloneFileType::kFile == fileType) {
        NameLockGuard lockGuard(cloneRef_->GetLock(), source);
        ret = client_->SetCloneFileStatus(source, FileStatus::BeingCloned,
                                          mdsRootUser_);
        if (ret < 0) {
            // The SetCloneFileStatus error is not handled here,
            // Because all results of SetCloneFileStatus failure are acceptable,
            // Compared to handling SetCloneFileStatus failure, it is more
            // direct: For example, calling DeleteCloneInfo to delete a task,
            // Once DeleteCloneInfo fails and an error is returned to the user,
            // Restarting the service will cause Clone to continue,
            // Inconsistency with the results returned by the user, causing
            // confusion for the user
            LOG(WARNING) << "SetCloneFileStatus encounter an error"
                         << ", ret = " << ret << ", source = " << source
                         << ", user = " << user;
        }
        cloneRef_->IncrementRef(source);
    }

    *cloneInfo = info;
    return kErrCodeSuccess;
}

int CloneCoreImpl::FlattenPre(const std::string& user, const TaskIdType& taskId,
                              CloneInfo* cloneInfo) {
    (void)user;
    int ret = metaStore_->GetCloneInfo(taskId, cloneInfo);
    if (ret < 0) {
        return kErrCodeFileNotExist;
    }
    switch (cloneInfo->GetStatus()) {
        case CloneStatus::done:
        case CloneStatus::cloning:
        case CloneStatus::recovering: {
            // A task exists is returned for completed or in progress,
            // indicating that it does not need to be processed
            return kErrCodeTaskExist;
        }
        case CloneStatus::metaInstalled: {
            if (CloneTaskType::kClone == cloneInfo->GetTaskType()) {
                cloneInfo->SetStatus(CloneStatus::cloning);
            } else {
                cloneInfo->SetStatus(CloneStatus::recovering);
            }
            break;
        }
        case CloneStatus::cleaning:
        case CloneStatus::errorCleaning:
        case CloneStatus::error:
        default: {
            LOG(ERROR) << "FlattenPre find clone task status Invalid"
                       << ", status = "
                       << static_cast<int>(cloneInfo->GetStatus());
            return kErrCodeFileStatusInvalid;
        }
    }
    ret = metaStore_->UpdateCloneInfo(*cloneInfo);
    if (ret < 0) {
        LOG(ERROR) << "UpdateCloneInfo fail"
                   << ", ret = " << ret
                   << ", taskId = " << cloneInfo->GetTaskId();
        return ret;
    }
    return kErrCodeSuccess;
}

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

    // In the steps after kCreateCloneMeta, it is necessary to update the
    // chunkIdInfo in the CloneChunkInfo information
    if (NeedUpdateCloneMeta(task)) {
        ret = CreateOrUpdateCloneMeta(task, &newFileInfo, &segInfos);
        if (ret < 0) {
            HandleCloneError(task, ret);
            return;
        }
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
                ret = CreateCloneChunk(task, newFileInfo, &segInfos);
                if (ret < 0) {
                    HandleCloneError(task, ret);
                    return;
                }
                break;
            case CloneStep::kCompleteCloneMeta:
                ret = CompleteCloneMeta(task, newFileInfo, segInfos);
                if (ret < 0) {
                    HandleCloneError(task, ret);
                    return;
                }
                task->SetProgress(kProgressMetaInstalled);
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
                    HandleLazyCloneStage1Finish(task);
                    doneGuard.release();
                    return;
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
    std::shared_ptr<CloneTaskInfo> task, FInfo* newFileInfo,
    CloneSegmentMap* segInfos) {
    segInfos->clear();
    UUID source = task->GetCloneInfo().GetSrc();

    SnapshotInfo snapInfo;
    int ret = metaStore_->GetSnapshotInfo(source, &snapInfo);
    if (ret < 0) {
        LOG(ERROR) << "GetSnapshotInfo error"
                   << ", source = " << source
                   << ", taskid = " << task->GetTaskId();
        return kErrCodeFileNotExist;
    }
    newFileInfo->chunksize = snapInfo.GetChunkSize();
    newFileInfo->segmentsize = snapInfo.GetSegmentSize();
    newFileInfo->length = snapInfo.GetFileLength();
    newFileInfo->stripeUnit = snapInfo.GetStripeUnit();
    newFileInfo->stripeCount = snapInfo.GetStripeCount();

    if (task->GetCloneInfo().GetTaskType() == CloneTaskType::kRecover &&
        task->GetCloneInfo().GetPoolset().empty()) {
        LOG(ERROR) << "Recover task's poolset should not be empty";
        return kErrCodeInternalError;
    }
    newFileInfo->poolset = !task->GetCloneInfo().GetPoolset().empty()
                               ? task->GetCloneInfo().GetPoolset()
                               : snapInfo.GetPoolset();

    if (IsRecover(task)) {
        FInfo fInfo;
        std::string destination = task->GetCloneInfo().GetDest();
        std::string user = task->GetCloneInfo().GetUser();
        ret = client_->GetFileInfo(destination, mdsRootUser_, &fInfo);
        switch (ret) {
            case LIBCURVE_ERROR::OK:
                break;
            case -LIBCURVE_ERROR::NOTEXIST:
                LOG(ERROR) << "BuildFileInfoFromSnapshot "
                           << "find dest file not exist, maybe deleted"
                           << ", ret = " << ret
                           << ", destination = " << destination
                           << ", user = " << user
                           << ", taskid = " << task->GetTaskId();
                return kErrCodeFileNotExist;
            default:
                LOG(ERROR) << "GetFileInfo fail"
                           << ", ret = " << ret
                           << ", destination = " << destination
                           << ", user = " << user
                           << ", taskid = " << task->GetTaskId();
                return kErrCodeInternalError;
        }
        // The destinationId recovered from the snapshot is the ID of the target
        // file
        task->GetCloneInfo().SetDestId(fInfo.id);
        // Restore seqnum+1 from snapshot
        newFileInfo->seqnum = fInfo.seqnum + 1;
    } else {
        newFileInfo->seqnum = kInitializeSeqNum;
    }
    newFileInfo->owner = task->GetCloneInfo().GetUser();

    ChunkIndexDataName indexName(snapInfo.GetFileName(), snapInfo.GetSeqNum());
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

    std::vector<ChunkIndexType> chunkIndexs = snapMeta.GetAllChunkIndex();
    for (auto& chunkIndex : chunkIndexs) {
        ChunkDataName chunkDataName;
        snapMeta.GetChunkDataName(chunkIndex, &chunkDataName);
        uint64_t segmentIndex = chunkIndex / chunkPerSegment;
        CloneChunkInfo info;
        info.location = chunkDataName.ToDataChunkKey();
        info.needRecover = true;
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

int CloneCoreImpl::BuildFileInfoFromFile(std::shared_ptr<CloneTaskInfo> task,
                                         FInfo* newFileInfo,
                                         CloneSegmentMap* segInfos) {
    segInfos->clear();
    UUID source = task->GetCloneInfo().GetSrc();
    std::string user = task->GetCloneInfo().GetUser();

    FInfo fInfo;
    int ret = client_->GetFileInfo(source, mdsRootUser_, &fInfo);
    if (ret != LIBCURVE_ERROR::OK) {
        LOG(ERROR) << "GetFileInfo fail"
                   << ", ret = " << ret << ", source = " << source
                   << ", user = " << user << ", taskid = " << task->GetTaskId();
        return kErrCodeFileNotExist;
    }
    // GetOrAllocateSegment depends on fullPathName
    fInfo.fullPathName = source;

    newFileInfo->chunksize = fInfo.chunksize;
    newFileInfo->segmentsize = fInfo.segmentsize;
    newFileInfo->length = fInfo.length;
    newFileInfo->seqnum = kInitializeSeqNum;
    newFileInfo->owner = task->GetCloneInfo().GetUser();
    newFileInfo->stripeUnit = fInfo.stripeUnit;
    newFileInfo->stripeCount = fInfo.stripeCount;

    if (task->GetCloneInfo().GetTaskType() == CloneTaskType::kRecover &&
        task->GetCloneInfo().GetPoolset().empty()) {
        LOG(ERROR) << "Recover task's poolset should not be empty";
        return kErrCodeInternalError;
    }
    newFileInfo->poolset = !task->GetCloneInfo().GetPoolset().empty()
                               ? task->GetCloneInfo().GetPoolset()
                               : fInfo.poolset;

    uint64_t fileLength = fInfo.length;
    uint64_t segmentSize = fInfo.segmentsize;
    uint64_t chunkSize = fInfo.chunksize;

    if (0 == segmentSize) {
        LOG(ERROR) << "GetFileInfo return invalid fileInfo, segmentSize == 0"
                   << ", taskid = " << task->GetTaskId();
        return kErrCodeInternalError;
    }
    if (fileLength % segmentSize != 0) {
        LOG(ERROR) << "GetFileInfo return invalid fileInfo, "
                   << "fileLength is not align to SegmentSize"
                   << ", taskid = " << task->GetTaskId();
        return kErrCodeInternalError;
    }

    for (uint64_t i = 0; i < fileLength / segmentSize; i++) {
        uint64_t offset = i * segmentSize;
        SegmentInfo segInfoOut;
        ret = client_->GetOrAllocateSegmentInfo(false, offset, &fInfo,
                                                mdsRootUser_, &segInfoOut);
        if (ret != LIBCURVE_ERROR::OK && ret != -LIBCURVE_ERROR::NOT_ALLOCATE) {
            LOG(ERROR) << "GetOrAllocateSegmentInfo fail"
                       << ", ret = " << ret << ", filename = " << source
                       << ", user = " << user << ", offset = " << offset
                       << ", allocateIfNotExist = "
                       << "false"
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
                info.needRecover = true;
                segInfo.emplace(j, info);
            }
            segInfos->emplace(i, segInfo);
        }
    }
    return kErrCodeSuccess;
}

int CloneCoreImpl::CreateCloneFile(std::shared_ptr<CloneTaskInfo> task,
                                   const FInfo& fInfo) {
    std::string fileName =
        cloneTempDir_ + "/" + task->GetCloneInfo().GetTaskId();
    std::string user = fInfo.owner;
    uint64_t fileLength = fInfo.length;
    uint64_t seqNum = fInfo.seqnum;
    uint32_t chunkSize = fInfo.chunksize;
    uint64_t stripeUnit = fInfo.stripeUnit;
    uint64_t stripeCount = fInfo.stripeCount;
    const auto& poolset = fInfo.poolset;

    std::string source = "";
    // Clone source is only available when cloning from a file
    if (CloneFileType::kFile == task->GetCloneInfo().GetFileType()) {
        source = task->GetCloneInfo().GetSrc();
    }

    FInfo fInfoOut;
    int ret = client_->CreateCloneFile(
        source, fileName, mdsRootUser_, fileLength, seqNum, chunkSize,
        stripeUnit, stripeCount, poolset, &fInfoOut);
    if (ret == LIBCURVE_ERROR::OK) {
        // nothing
    } else if (ret == -LIBCURVE_ERROR::EXISTS) {
        ret = client_->GetFileInfo(fileName, mdsRootUser_, &fInfoOut);
        if (ret != LIBCURVE_ERROR::OK) {
            LOG(ERROR) << "GetFileInfo fail"
                       << ", ret = " << ret << ", fileName = " << fileName
                       << ", taskid = " << task->GetTaskId();
            return kErrCodeInternalError;
        }
    } else {
        LOG(ERROR) << "CreateCloneFile file"
                   << ", ret = " << ret << ", destination = " << fileName
                   << ", user = " << user << ", fileLength = " << fileLength
                   << ", seqNum = " << seqNum << ", chunkSize = " << chunkSize
                   << ", return fileId = " << fInfoOut.id
                   << ", taskid = " << task->GetTaskId();
        return kErrCodeInternalError;
    }
    task->GetCloneInfo().SetOriginId(fInfoOut.id);
    if (IsClone(task)) {
        // In the case of cloning, destinationId = originId;
        task->GetCloneInfo().SetDestId(fInfoOut.id);
    }
    task->GetCloneInfo().SetTime(fInfoOut.ctime);
    // If it is a lazy&non snapshot, do not createCloneMeta or createCloneChunk
    // yet Wait until stage 2 recoveryChunk, go to createCloneMeta,
    // createCloneChunk
    if (IsLazy(task) && IsFile(task)) {
        task->GetCloneInfo().SetNextStep(CloneStep::kCompleteCloneMeta);
    } else {
        task->GetCloneInfo().SetNextStep(CloneStep::kCreateCloneMeta);
    }

    ret = metaStore_->UpdateCloneInfo(task->GetCloneInfo());
    if (ret < 0) {
        LOG(ERROR) << "UpdateCloneInfo after CreateCloneFile error."
                   << " ret = " << ret << ", taskid = " << task->GetTaskId();
        return ret;
    }
    return kErrCodeSuccess;
}

int CloneCoreImpl::CreateCloneMeta(std::shared_ptr<CloneTaskInfo> task,
                                   FInfo* fInfo, CloneSegmentMap* segInfos) {
    int ret = CreateOrUpdateCloneMeta(task, fInfo, segInfos);
    if (ret < 0) {
        return ret;
    }

    task->GetCloneInfo().SetNextStep(CloneStep::kCreateCloneChunk);

    ret = metaStore_->UpdateCloneInfo(task->GetCloneInfo());
    if (ret < 0) {
        LOG(ERROR) << "UpdateCloneInfo after CreateCloneMeta error."
                   << " ret = " << ret << ", taskid = " << task->GetTaskId();
        return ret;
    }
    return kErrCodeSuccess;
}

int CloneCoreImpl::CreateCloneChunk(std::shared_ptr<CloneTaskInfo> task,
                                    const FInfo& fInfo,
                                    CloneSegmentMap* segInfos) {
    int ret = kErrCodeSuccess;
    uint32_t chunkSize = fInfo.chunksize;
    uint32_t correctSn = 0;
    // When cloning, correctSn is 0, and when restoring, it is the newly
    // generated file version
    if (IsClone(task)) {
        correctSn = 0;
    } else {
        correctSn = fInfo.seqnum;
    }
    auto tracker = std::make_shared<CreateCloneChunkTaskTracker>();
    for (auto& cloneSegmentInfo : *segInfos) {
        for (auto& cloneChunkInfo : cloneSegmentInfo.second) {
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
            context->cloneChunkInfo = &cloneChunkInfo.second;
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
            ret = HandleCreateCloneChunkResultsAndRetry(task, tracker, results);
            if (ret < 0) {
                return kErrCodeInternalError;
            }
        }
    }
    // Tasks with insufficient remaining quantity in the end
    do {
        tracker->WaitSome(1);
        std::list<CreateCloneChunkContextPtr> results =
            tracker->PopResultContexts();
        if (0 == results.size()) {
            // Completed, no new results
            break;
        }
        ret = HandleCreateCloneChunkResultsAndRetry(task, tracker, results);
        if (ret < 0) {
            return kErrCodeInternalError;
        }
    } while (true);

    if (IsLazy(task) && IsFile(task)) {
        task->GetCloneInfo().SetNextStep(CloneStep::kRecoverChunk);
    } else {
        task->GetCloneInfo().SetNextStep(CloneStep::kCompleteCloneMeta);
    }
    ret = metaStore_->UpdateCloneInfo(task->GetCloneInfo());
    if (ret < 0) {
        LOG(ERROR) << "UpdateCloneInfo after CreateCloneChunk error."
                   << " ret = " << ret << ", taskid = " << task->GetTaskId();
        return kErrCodeInternalError;
    }
    return kErrCodeSuccess;
}

int CloneCoreImpl::StartAsyncCreateCloneChunk(
    std::shared_ptr<CloneTaskInfo> task,
    std::shared_ptr<CreateCloneChunkTaskTracker> tracker,
    std::shared_ptr<CreateCloneChunkContext> context) {
    CreateCloneChunkClosure* cb = new CreateCloneChunkClosure(tracker, context);
    tracker->AddOneTrace();
    LOG(INFO) << "Doing CreateCloneChunk"
              << ", location = " << context->location
              << ", logicalPoolId = " << context->cidInfo.lpid_
              << ", copysetId = " << context->cidInfo.cpid_
              << ", chunkId = " << context->cidInfo.cid_
              << ", seqNum = " << context->sn << ", csn = " << context->csn
              << ", taskid = " << task->GetTaskId();
    int ret = client_->CreateCloneChunk(context->location, context->cidInfo,
                                        context->sn, context->csn,
                                        context->chunkSize, cb);

    if (ret != LIBCURVE_ERROR::OK) {
        LOG(ERROR) << "CreateCloneChunk fail"
                   << ", ret = " << ret << ", location = " << context->location
                   << ", logicalPoolId = " << context->cidInfo.lpid_
                   << ", copysetId = " << context->cidInfo.cpid_
                   << ", chunkId = " << context->cidInfo.cid_
                   << ", seqNum = " << context->sn << ", csn = " << context->csn
                   << ", taskid = " << task->GetTaskId();
        return ret;
    }
    return kErrCodeSuccess;
}

int CloneCoreImpl::HandleCreateCloneChunkResultsAndRetry(
    std::shared_ptr<CloneTaskInfo> task,
    std::shared_ptr<CreateCloneChunkTaskTracker> tracker,
    const std::list<CreateCloneChunkContextPtr>& results) {
    int ret = kErrCodeSuccess;
    for (auto context : results) {
        if (context->retCode == -LIBCURVE_ERROR::EXISTS) {
            LOG(INFO) << "CreateCloneChunk chunk exist"
                      << ", location = " << context->location
                      << ", logicalPoolId = " << context->cidInfo.lpid_
                      << ", copysetId = " << context->cidInfo.cpid_
                      << ", chunkId = " << context->cidInfo.cid_
                      << ", seqNum = " << context->sn
                      << ", csn = " << context->csn
                      << ", taskid = " << task->GetTaskId();
            context->cloneChunkInfo->needRecover = false;
        } else if (context->retCode != LIBCURVE_ERROR::OK) {
            uint64_t nowTime = TimeUtility::GetTimeofDaySec();
            if (nowTime - context->startTime <
                context->clientAsyncMethodRetryTimeSec) {
                // retry
                std::this_thread::sleep_for(std::chrono::milliseconds(
                    clientAsyncMethodRetryIntervalMs_));
                ret = StartAsyncCreateCloneChunk(task, tracker, context);
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
    return ret;
}

int CloneCoreImpl::CompleteCloneMeta(std::shared_ptr<CloneTaskInfo> task,
                                     const FInfo& fInfo,
                                     const CloneSegmentMap& segInfos) {
    (void)fInfo;
    (void)segInfos;
    std::string origin = cloneTempDir_ + "/" + task->GetCloneInfo().GetTaskId();
    std::string user = task->GetCloneInfo().GetUser();
    int ret = client_->CompleteCloneMeta(origin, mdsRootUser_);
    if (ret != LIBCURVE_ERROR::OK) {
        LOG(ERROR) << "CompleteCloneMeta fail"
                   << ", ret = " << ret << ", filename = " << origin
                   << ", user = " << user << ", taskid = " << task->GetTaskId();
        return kErrCodeInternalError;
    }
    if (IsLazy(task)) {
        task->GetCloneInfo().SetNextStep(CloneStep::kChangeOwner);
    } else {
        task->GetCloneInfo().SetNextStep(CloneStep::kRecoverChunk);
    }
    ret = metaStore_->UpdateCloneInfo(task->GetCloneInfo());
    if (ret < 0) {
        LOG(ERROR) << "UpdateCloneInfo after CompleteCloneMeta error."
                   << " ret = " << ret << ", taskid = " << task->GetTaskId();
        return ret;
    }
    return kErrCodeSuccess;
}

int CloneCoreImpl::RecoverChunk(std::shared_ptr<CloneTaskInfo> task,
                                const FInfo& fInfo,
                                const CloneSegmentMap& segInfos) {
    int ret = kErrCodeSuccess;
    uint32_t chunkSize = fInfo.chunksize;

    uint32_t totalProgress =
        kProgressRecoverChunkEnd - kProgressRecoverChunkBegin;
    uint32_t segNum = segInfos.size();
    double progressPerData = static_cast<double>(totalProgress) / segNum;
    uint32_t index = 0;

    if (0 == cloneChunkSplitSize_ || chunkSize % cloneChunkSplitSize_ != 0) {
        LOG(ERROR) << "chunk is not align to cloneChunkSplitSize"
                   << ", taskid = " << task->GetTaskId();
        return kErrCodeChunkSizeNotAligned;
    }

    auto tracker = std::make_shared<RecoverChunkTaskTracker>();
    uint64_t workingChunkNum = 0;
    // To avoid collisions with the same chunk, asynchronous requests for
    // different chunks
    for (auto& cloneSegmentInfo : segInfos) {
        for (auto& cloneChunkInfo : cloneSegmentInfo.second) {
            if (!cloneChunkInfo.second.needRecover) {
                continue;
            }
            // When the current number of chunks for concurrent work exceeds the
            // required number of concurrent tasks, digest a portion first
            while (workingChunkNum >= recoverChunkConcurrency_) {
                uint64_t completeChunkNum = 0;
                ret = ContinueAsyncRecoverChunkPartAndWaitSomeChunkEnd(
                    task, tracker, &completeChunkNum);
                if (ret < 0) {
                    return kErrCodeInternalError;
                }
                workingChunkNum -= completeChunkNum;
            }
            // Chunk joining a new job
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
                      << ", logicalPoolId = " << context->cidInfo.lpid_
                      << ", copysetId = " << context->cidInfo.cpid_
                      << ", chunkId = " << context->cidInfo.cid_
                      << ", len = " << context->partSize
                      << ", taskid = " << task->GetTaskId();

            ret = StartAsyncRecoverChunkPart(task, tracker, context);
            if (ret < 0) {
                return kErrCodeInternalError;
            }
        }
        task->SetProgress(static_cast<uint32_t>(kProgressRecoverChunkBegin +
                                                index * progressPerData));
        task->UpdateMetric();
        index++;
    }

    while (workingChunkNum > 0) {
        uint64_t completeChunkNum = 0;
        ret = ContinueAsyncRecoverChunkPartAndWaitSomeChunkEnd(
            task, tracker, &completeChunkNum);
        if (ret < 0) {
            return kErrCodeInternalError;
        }
        workingChunkNum -= completeChunkNum;
    }

    task->GetCloneInfo().SetNextStep(CloneStep::kCompleteCloneFile);
    ret = metaStore_->UpdateCloneInfo(task->GetCloneInfo());
    if (ret < 0) {
        LOG(ERROR) << "UpdateCloneInfo after RecoverChunk error."
                   << " ret = " << ret << ", taskid = " << task->GetTaskId();
        return kErrCodeInternalError;
    }
    return kErrCodeSuccess;
}

int CloneCoreImpl::StartAsyncRecoverChunkPart(
    std::shared_ptr<CloneTaskInfo> task,
    std::shared_ptr<RecoverChunkTaskTracker> tracker,
    std::shared_ptr<RecoverChunkContext> context) {
    RecoverChunkClosure* cb = new RecoverChunkClosure(tracker, context);
    tracker->AddOneTrace();
    uint64_t offset = context->partIndex * context->partSize;
    LOG_EVERY_SECOND(INFO) << "Doing RecoverChunk"
                           << ", logicalPoolId = " << context->cidInfo.lpid_
                           << ", copysetId = " << context->cidInfo.cpid_
                           << ", chunkId = " << context->cidInfo.cid_
                           << ", offset = " << offset
                           << ", len = " << context->partSize
                           << ", taskid = " << task->GetTaskId();
    int ret =
        client_->RecoverChunk(context->cidInfo, offset, context->partSize, cb);
    if (ret != LIBCURVE_ERROR::OK) {
        LOG(ERROR) << "RecoverChunk fail"
                   << ", ret = " << ret
                   << ", logicalPoolId = " << context->cidInfo.lpid_
                   << ", copysetId = " << context->cidInfo.cpid_
                   << ", chunkId = " << context->cidInfo.cid_
                   << ", offset = " << offset << ", len = " << context->partSize
                   << ", taskid = " << task->GetTaskId();
        return ret;
    }
    return kErrCodeSuccess;
}

int CloneCoreImpl::ContinueAsyncRecoverChunkPartAndWaitSomeChunkEnd(
    std::shared_ptr<CloneTaskInfo> task,
    std::shared_ptr<RecoverChunkTaskTracker> tracker,
    uint64_t* completeChunkNum) {
    *completeChunkNum = 0;
    tracker->WaitSome(1);
    std::list<RecoverChunkContextPtr> results = tracker->PopResultContexts();
    for (auto context : results) {
        if (context->retCode != LIBCURVE_ERROR::OK) {
            uint64_t nowTime = TimeUtility::GetTimeofDaySec();
            if (nowTime - context->startTime <
                context->clientAsyncMethodRetryTimeSec) {
                // retry
                std::this_thread::sleep_for(std::chrono::milliseconds(
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
            // Start a new shard, index++, and reset the start time
            context->partIndex++;
            context->startTime = TimeUtility::GetTimeofDaySec();
            if (context->partIndex < context->totalPartNum) {
                int ret = StartAsyncRecoverChunkPart(task, tracker, context);
                if (ret < 0) {
                    return ret;
                }
            } else {
                LOG(INFO) << "RecoverChunk Complete"
                          << ", logicalPoolId = " << context->cidInfo.lpid_
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

int CloneCoreImpl::ChangeOwner(std::shared_ptr<CloneTaskInfo> task,
                               const FInfo& fInfo) {
    (void)fInfo;
    std::string user = task->GetCloneInfo().GetUser();
    std::string origin = cloneTempDir_ + "/" + task->GetCloneInfo().GetTaskId();

    int ret = client_->ChangeOwner(origin, user);
    if (ret != LIBCURVE_ERROR::OK) {
        LOG(ERROR) << "ChangeOwner fail, ret = " << ret
                   << ", fileName = " << origin << ", newOwner = " << user
                   << ", taskid = " << task->GetTaskId();
        return kErrCodeInternalError;
    }

    task->GetCloneInfo().SetNextStep(CloneStep::kRenameCloneFile);
    ret = metaStore_->UpdateCloneInfo(task->GetCloneInfo());
    if (ret < 0) {
        LOG(ERROR) << "UpdateCloneInfo after ChangeOwner error."
                   << " ret = " << ret << ", taskid = " << task->GetTaskId();
        return kErrCodeInternalError;
    }
    return kErrCodeSuccess;
}

int CloneCoreImpl::RenameCloneFile(std::shared_ptr<CloneTaskInfo> task,
                                   const FInfo& fInfo) {
    std::string user = fInfo.owner;
    uint64_t originId = task->GetCloneInfo().GetOriginId();
    uint64_t destinationId = task->GetCloneInfo().GetDestId();
    std::string origin = cloneTempDir_ + "/" + task->GetCloneInfo().GetTaskId();
    std::string destination = task->GetCloneInfo().GetDest();

    // Rename first
    int ret = client_->RenameCloneFile(mdsRootUser_, originId, destinationId,
                                       origin, destination);
    if (-LIBCURVE_ERROR::NOTEXIST == ret) {
        // It is possible that it has already been renamed
        FInfo destFInfo;
        ret = client_->GetFileInfo(destination, mdsRootUser_, &destFInfo);
        if (ret != LIBCURVE_ERROR::OK) {
            LOG(ERROR) << "RenameCloneFile return NOTEXIST,"
                       << "And get dest fileInfo fail, ret = " << ret
                       << ", destination filename = " << destination
                       << ", taskid = " << task->GetTaskId();
            return kErrCodeInternalError;
        }
        if (destFInfo.id != originId) {
            LOG(ERROR) << "RenameCloneFile return NOTEXIST,"
                       << "And get dest file id not equal, ret = " << ret
                       << "originId = " << originId
                       << "destFInfo.id = " << destFInfo.id
                       << ", taskid = " << task->GetTaskId();
            return kErrCodeInternalError;
        }
    } else if (ret != LIBCURVE_ERROR::OK) {
        LOG(ERROR) << "RenameCloneFile fail"
                   << ", ret = " << ret << ", user = " << user
                   << ", originId = " << originId << ", origin = " << origin
                   << ", destination = " << destination
                   << ", taskid = " << task->GetTaskId();
        return kErrCodeInternalError;
    }

    if (IsLazy(task)) {
        if (IsFile(task)) {
            task->GetCloneInfo().SetNextStep(CloneStep::kCreateCloneMeta);
        } else {
            task->GetCloneInfo().SetNextStep(CloneStep::kRecoverChunk);
        }
        task->GetCloneInfo().SetStatus(CloneStatus::metaInstalled);
    } else {
        task->GetCloneInfo().SetNextStep(CloneStep::kEnd);
    }
    ret = metaStore_->UpdateCloneInfo(task->GetCloneInfo());
    if (ret < 0) {
        LOG(ERROR) << "UpdateCloneInfo after RenameCloneFile error."
                   << " ret = " << ret << ", taskid = " << task->GetTaskId();
        return ret;
    }
    return kErrCodeSuccess;
}

int CloneCoreImpl::CompleteCloneFile(std::shared_ptr<CloneTaskInfo> task,
                                     const FInfo& fInfo,
                                     const CloneSegmentMap& segInfos) {
    (void)fInfo;
    (void)segInfos;
    std::string fileName;
    if (IsLazy(task)) {
        fileName = task->GetCloneInfo().GetDest();
    } else {
        fileName = cloneTempDir_ + "/" + task->GetCloneInfo().GetTaskId();
    }
    std::string user = task->GetCloneInfo().GetUser();
    int ret = client_->CompleteCloneFile(fileName, mdsRootUser_);
    switch (ret) {
        case LIBCURVE_ERROR::OK:
            break;
        case -LIBCURVE_ERROR::NOTEXIST:
            LOG(ERROR) << "CompleteCloneFile "
                       << "find dest file not exist, maybe deleted"
                       << ", ret = " << ret << ", destination = " << fileName
                       << ", user = " << user
                       << ", taskid = " << task->GetTaskId();
            return kErrCodeFileNotExist;
        default:
            LOG(ERROR) << "CompleteCloneFile fail"
                       << ", ret = " << ret << ", fileName = " << fileName
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
                   << " ret = " << ret << ", taskid = " << task->GetTaskId();
        return ret;
    }
    return kErrCodeSuccess;
}

void CloneCoreImpl::HandleLazyCloneStage1Finish(
    std::shared_ptr<CloneTaskInfo> task) {
    LOG(INFO) << "Task Lazy Stage1 Success"
              << ", TaskInfo : " << *task;
    task->GetClosure()->SetErrCode(kErrCodeSuccess);
    task->Finish();
    task->GetClosure()->Run();
    return;
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
            int ret = client_->SetCloneFileStatus(source, FileStatus::Created,
                                                  mdsRootUser_);
            if (ret < 0) {
                task->GetCloneInfo().SetStatus(CloneStatus::error);
                int ret2 = metaStore_->UpdateCloneInfo(task->GetCloneInfo());
                if (ret2 < 0) {
                    LOG(ERROR) << "UpdateCloneInfo Task error Fail!"
                               << " ret = " << ret2
                               << ", uuid = " << task->GetTaskId();
                }
                LOG(ERROR) << "Task Fail cause by SetCloneFileStatus fail"
                           << ", ret = " << ret << ", TaskInfo : " << *task;
                task->Finish();
                return;
            }
        }
    }
    task->GetCloneInfo().SetStatus(CloneStatus::done);
    ret = metaStore_->UpdateCloneInfo(task->GetCloneInfo());
    if (ret < 0) {
        LOG(ERROR) << "UpdateCloneInfo Task Success Fail!"
                   << " ret = " << ret << ", uuid = " << task->GetTaskId();
    }
    task->SetProgress(kProgressCloneComplete);

    LOG(INFO) << "Task Success"
              << ", TaskInfo : " << *task;
    task->Finish();
    return;
}

void CloneCoreImpl::HandleCloneError(std::shared_ptr<CloneTaskInfo> task,
                                     int retCode) {
    int ret = kErrCodeSuccess;
    if (NeedRetry(task, retCode)) {
        HandleCloneToRetry(task);
        return;
    }

    if (IsLazy(task)) {
        task->GetClosure()->SetErrCode(retCode);
    }
    if (IsSnapshot(task)) {
        snapshotRef_->DecrementSnapshotRef(task->GetCloneInfo().GetSrc());
    } else {
        std::string source = task->GetCloneInfo().GetSrc();
        cloneRef_->DecrementRef(source);
        NameLockGuard lockGuard(cloneRef_->GetLock(), source);
        if (cloneRef_->GetRef(source) == 0) {
            ret = client_->SetCloneFileStatus(source, FileStatus::Created,
                                              mdsRootUser_);
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
                   << " ret = " << ret << ", uuid = " << task->GetTaskId();
    }
    LOG(ERROR) << "Task Fail"
               << ", TaskInfo : " << *task;
    task->Finish();
    return;
}

void CloneCoreImpl::HandleCloneToRetry(std::shared_ptr<CloneTaskInfo> task) {
    task->GetCloneInfo().SetStatus(CloneStatus::retrying);
    int ret = metaStore_->UpdateCloneInfo(task->GetCloneInfo());
    if (ret < 0) {
        LOG(ERROR) << "UpdateCloneInfo Task retrying Fail!"
                   << " ret = " << ret << ", uuid = " << task->GetTaskId();
    }
    LOG(WARNING) << "Task Fail, Retrying"
                 << ", TaskInfo : " << *task;
    task->Finish();
    return;
}

void CloneCoreImpl::HandleCleanSuccess(std::shared_ptr<CloneTaskInfo> task) {
    TaskIdType taskId = task->GetCloneInfo().GetTaskId();
    int ret = metaStore_->DeleteCloneInfo(taskId);
    if (ret < 0) {
        LOG(ERROR) << "DeleteCloneInfo failed"
                   << ", ret = " << ret << ", taskId = " << taskId;
    } else {
        LOG(INFO) << "Clean Task Success"
                  << ", TaskInfo : " << *task;
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
                   << " ret = " << ret << ", uuid = " << task->GetTaskId();
    }
    LOG(ERROR) << "Clean Task Fail"
               << ", TaskInfo : " << *task;
    task->Finish();
    return;
}

int CloneCoreImpl::GetCloneInfoList(std::vector<CloneInfo>* taskList) {
    metaStore_->GetCloneInfoList(taskList);
    return kErrCodeSuccess;
}

int CloneCoreImpl::GetCloneInfo(TaskIdType taskId, CloneInfo* cloneInfo) {
    return metaStore_->GetCloneInfo(taskId, cloneInfo);
}

int CloneCoreImpl::GetCloneInfoByFileName(const std::string& fileName,
                                          std::vector<CloneInfo>* list) {
    return metaStore_->GetCloneInfoByFileName(fileName, list);
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

bool CloneCoreImpl::NeedUpdateCloneMeta(std::shared_ptr<CloneTaskInfo> task) {
    bool ret = true;
    CloneStep step = task->GetCloneInfo().GetNextStep();
    if (CloneStep::kCreateCloneFile == step ||
        CloneStep::kCreateCloneMeta == step || CloneStep::kEnd == step) {
        ret = false;
    }
    return ret;
}

bool CloneCoreImpl::NeedRetry(std::shared_ptr<CloneTaskInfo> task,
                              int retCode) {
    if (IsLazy(task)) {
        CloneStep step = task->GetCloneInfo().GetNextStep();
        if (CloneStep::kRecoverChunk == step ||
            CloneStep::kCompleteCloneFile == step || CloneStep::kEnd == step) {
            // In scenarios where the file does not exist, there is no need to
            // retry as it may have been deleted
            if (retCode != kErrCodeFileNotExist) {
                return true;
            }
        }
    }
    return false;
}

int CloneCoreImpl::CreateOrUpdateCloneMeta(std::shared_ptr<CloneTaskInfo> task,
                                           FInfo* fInfo,
                                           CloneSegmentMap* segInfos) {
    std::string newFileName =
        cloneTempDir_ + "/" + task->GetCloneInfo().GetTaskId();
    std::string user = fInfo->owner;
    FInfo fInfoOut;
    int ret = client_->GetFileInfo(newFileName, mdsRootUser_, &fInfoOut);
    if (LIBCURVE_ERROR::OK == ret) {
        // nothing
    } else if (-LIBCURVE_ERROR::NOTEXIST == ret) {
        // Perhaps it has already been renamed
        newFileName = task->GetCloneInfo().GetDest();
        ret = client_->GetFileInfo(newFileName, mdsRootUser_, &fInfoOut);
        if (ret != LIBCURVE_ERROR::OK) {
            LOG(ERROR) << "File is missing, "
                       << "when CreateOrUpdateCloneMeta, "
                       << "GetFileInfo fail, ret = " << ret
                       << ", filename = " << newFileName
                       << ", taskid = " << task->GetTaskId();
            return kErrCodeFileNotExist;
        }
        // If it has already been renamed, then the id should be consistent
        uint64_t originId = task->GetCloneInfo().GetOriginId();
        if (fInfoOut.id != originId) {
            LOG(ERROR) << "File is missing, fileId not equal, "
                       << "when CreateOrUpdateCloneMeta"
                       << ", fileId = " << fInfoOut.id
                       << ", originId = " << originId
                       << ", filename = " << newFileName
                       << ", taskid = " << task->GetTaskId();
            return kErrCodeFileNotExist;
        }
    } else {
        LOG(ERROR) << "GetFileInfo fail"
                   << ", ret = " << ret << ", filename = " << newFileName
                   << ", user = " << user << ", taskid = " << task->GetTaskId();
        return kErrCodeInternalError;
    }
    // Update fInfo
    *fInfo = fInfoOut;
    // GetOrAllocateSegment depends on fullPathName and needs to be updated here
    fInfo->fullPathName = newFileName;

    uint32_t segmentSize = fInfo->segmentsize;
    for (auto& segInfo : *segInfos) {
        SegmentInfo segInfoOut;
        uint64_t offset = segInfo.first * segmentSize;
        ret = client_->GetOrAllocateSegmentInfo(true, offset, fInfo,
                                                mdsRootUser_, &segInfoOut);
        if (ret != LIBCURVE_ERROR::OK) {
            LOG(ERROR) << "GetOrAllocateSegmentInfo fail"
                       << ", newFileName = " << newFileName
                       << ", user = " << user << ", offset = " << offset
                       << ", allocateIfNotExist = "
                       << "true"
                       << ", taskid = " << task->GetTaskId();
            return kErrCodeInternalError;
        }

        for (auto& cloneChunkInfo : segInfo.second) {
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

int CloneCoreImpl::CleanCloneOrRecoverTaskPre(const std::string& user,
                                              const TaskIdType& taskId,
                                              CloneInfo* cloneInfo) {
    int ret = metaStore_->GetCloneInfo(taskId, cloneInfo);
    if (ret < 0) {
        // Directly returns success when it does not exist, making the interface
        // idempotent
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
                   << ", ret = " << ret << ", taskId = " << taskId;
        return ret;
    }
    return kErrCodeSuccess;
}

void CloneCoreImpl::HandleCleanCloneOrRecoverTask(
    std::shared_ptr<CloneTaskInfo> task) {
    // Only the wrong clone/recover task cleans up temporary files
    if (CloneStatus::errorCleaning == task->GetCloneInfo().GetStatus()) {
        // In the event of an error, the mirror being cloned flag may not be
        // cleared
        if (IsFile(task)) {
            // Resend
            std::string source = task->GetCloneInfo().GetSrc();
            NameLockGuard lockGuard(cloneRef_->GetLock(), source);
            if (cloneRef_->GetRef(source) == 0) {
                int ret = client_->SetCloneFileStatus(
                    source, FileStatus::Created, mdsRootUser_);
                if (ret != LIBCURVE_ERROR::OK &&
                    ret != -LIBCURVE_ERROR::NOTEXIST) {
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
        std::string user = task->GetCloneInfo().GetUser();
        int ret = client_->DeleteFile(tempFileName, mdsRootUser_, fileId);
        if (ret != LIBCURVE_ERROR::OK && ret != -LIBCURVE_ERROR::NOTEXIST) {
            LOG(ERROR) << "DeleteFile failed"
                       << ", ret = " << ret << ", fileName = " << tempFileName
                       << ", user = " << user << ", fileId = " << fileId
                       << ", taskid = " << task->GetTaskId();
            HandleCleanError(task);
            return;
        }
    }
    HandleCleanSuccess(task);
    return;
}

int CloneCoreImpl::HandleRemoveCloneOrRecoverTask(
    std::shared_ptr<CloneTaskInfo> task) {
    TaskIdType taskId = task->GetCloneInfo().GetTaskId();
    int ret = metaStore_->DeleteCloneInfo(taskId);
    if (ret < 0) {
        LOG(ERROR) << "DeleteCloneInfo failed"
                   << ", ret = " << ret << ", taskId = " << taskId;
        return kErrCodeInternalError;
    }

    if (IsSnapshot(task)) {
        snapshotRef_->DecrementSnapshotRef(task->GetCloneInfo().GetSrc());
    } else {
        std::string source = task->GetCloneInfo().GetSrc();
        cloneRef_->DecrementRef(source);
        NameLockGuard lockGuard(cloneRef_->GetLock(), source);
        if (cloneRef_->GetRef(source) == 0) {
            int ret = client_->SetCloneFileStatus(source, FileStatus::Created,
                                                  mdsRootUser_);
            if (ret < 0) {
                LOG(ERROR) << "Task Fail cause by SetCloneFileStatus fail"
                           << ", ret = " << ret << ", TaskInfo : " << *task;
                return kErrCodeInternalError;
            }
        }
    }

    return kErrCodeSuccess;
}

int CloneCoreImpl::CheckFileExists(const std::string& filename,
                                   uint64_t inodeId) {
    FInfo destFInfo;
    int ret = client_->GetFileInfo(filename, mdsRootUser_, &destFInfo);
    if (ret == LIBCURVE_ERROR::OK) {
        if (destFInfo.id == inodeId) {
            return kErrCodeFileExist;
        } else {
            return kErrCodeFileNotExist;
        }
    }

    if (ret == -LIBCURVE_ERROR::NOTEXIST) {
        return kErrCodeFileNotExist;
    }

    return kErrCodeInternalError;
}

// When adding or subtracting reference counts, the interface will lock the
// reference count map; When adding a reference count and reducing the reference
// count to 0, an additional lock needs to be added to the modified record.
int CloneCoreImpl::HandleDeleteCloneInfo(const CloneInfo& cloneInfo) {
    // First, reduce the reference count. If you are cloning from a mirror and
    // the reference count is reduced to 0, you need to modify the status of the
    // source mirror to 'created'
    std::string source = cloneInfo.GetSrc();
    if (cloneInfo.GetFileType() == CloneFileType::kSnapshot) {
        snapshotRef_->DecrementSnapshotRef(source);
    } else {
        cloneRef_->DecrementRef(source);
        NameLockGuard lockGuard(cloneRef_->GetLock(), source);
        if (cloneRef_->GetRef(source) == 0) {
            int ret = client_->SetCloneFileStatus(source, FileStatus::Created,
                                                  mdsRootUser_);
            if (ret == -LIBCURVE_ERROR::NOTEXIST) {
                LOG(WARNING) << "SetCloneFileStatus, file not exist, filename: "
                             << source;
            } else if (ret != LIBCURVE_ERROR::OK) {
                cloneRef_->IncrementRef(source);
                LOG(ERROR) << "SetCloneFileStatus fail"
                           << ", ret = " << ret
                           << ", cloneInfo : " << cloneInfo;
                return kErrCodeInternalError;
            }
        }
    }

    // Delete this record. If the deletion fails, add back the previously
    // subtracted reference count
    int ret = metaStore_->DeleteCloneInfo(cloneInfo.GetTaskId());
    if (ret != 0) {
        if (cloneInfo.GetFileType() == CloneFileType::kSnapshot) {
            NameLockGuard lockSnapGuard(snapshotRef_->GetSnapshotLock(),
                                        source);
            snapshotRef_->IncrementSnapshotRef(source);
        } else {
            NameLockGuard lockGuard(cloneRef_->GetLock(), source);
            cloneRef_->IncrementRef(source);
        }
        LOG(ERROR) << "DeleteCloneInfo failed"
                   << ", ret = " << ret << ", CloneInfo = " << cloneInfo;
        return kErrCodeInternalError;
    }

    LOG(INFO) << "HandleDeleteCloneInfo success"
              << ", cloneInfo = " << cloneInfo;

    return kErrCodeSuccess;
}

}  // namespace snapshotcloneserver
}  // namespace curve
