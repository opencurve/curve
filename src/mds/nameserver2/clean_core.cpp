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
 * Created Date: Wednesday December 19th 2018
 * Author: hzsunjianliang
 */

#include "src/mds/nameserver2/clean_core.h"

#include <vector>

namespace curve {
namespace mds {
StatusCode CleanCore::CleanSnapShotFile(const FileInfo & fileInfo,
                                        TaskProgress* progress) {
    if (fileInfo.segmentsize() == 0) {
        LOG(ERROR) << "cleanSnapShot File Error, segmentsize = 0";
        return StatusCode::KInternalError;
    }
    uint32_t  segmentNum = fileInfo.length() / fileInfo.segmentsize();
    uint64_t segmentSize = fileInfo.segmentsize();
    for (uint32_t i = 0; i < segmentNum; i++) {
        // load  segment
        PageFileSegment segment;
        StoreStatus storeRet = storage_->GetSegment(fileInfo.parentid(),
                                                    i * segmentSize,
                                                    &segment);
        if (storeRet == StoreStatus::KeyNotExist) {
            continue;
        } else if (storeRet !=  StoreStatus::OK) {
            LOG(ERROR) << "cleanSnapShot File Error: "
            << "GetSegment Error, inodeid = " << fileInfo.id()
            << ", filename = " << fileInfo.filename()
            << ", offset = " << i * segmentSize
            << ", sequenceNum = " << fileInfo.seqnum();
            progress->SetStatus(TaskStatus::FAILED);
            return StatusCode::kSnapshotFileDeleteError;
        }

        // delete chunks in chunkserver
        LogicalPoolID logicalPoolID = segment.logicalpoolid();
        uint32_t chunkNum = segment.chunks_size();
        for (uint32_t j = 0; j != chunkNum; j++) {
            // 删除快照时如果chunk不存在快照，则需要修改chunk的correctedSn
            // 防止删除快照后，后续的写触发chunk的快照
            // correctSn为创建快照后文件的版本号，也就是快照版本号+1
            SeqNum correctSn = fileInfo.seqnum() + 1;
            int ret = copysetClient_->DeleteChunkSnapshotOrCorrectSn(
                logicalPoolID,
                segment.chunks()[j].copysetid(),
                segment.chunks()[j].chunkid(),
                correctSn);
            if (ret != 0) {
                LOG(ERROR) << "CleanSnapShotFile Error: "
                    << "DeleteChunkSnapshotOrCorrectSn Error"
                    << ", ret = " << ret
                    << ", inodeid = " << fileInfo.id()
                    << ", filename = " << fileInfo.filename()
                    << ", correctSn = " << correctSn;
                progress->SetStatus(TaskStatus::FAILED);
                return StatusCode::kSnapshotFileDeleteError;
            }
        }
        progress->SetProgress(100 * (i+1) / segmentNum);
    }

    // delete the storage
    StoreStatus ret =  storage_->DeleteSnapshotFile(fileInfo.parentid(),
                                                fileInfo.filename());
    if (ret != StoreStatus::OK) {
        LOG(INFO) << "delete snapshotfile error, retCode = " << ret;
        progress->SetStatus(TaskStatus::FAILED);
        return StatusCode::kSnapshotFileDeleteError;
    } else {
        LOG(INFO) << "inodeid = " << fileInfo.id()
            << ", filename = " << fileInfo.filename()
            << ", seq = " << fileInfo.seqnum() << ", deleted";
    }

    progress->SetProgress(100);
    progress->SetStatus(TaskStatus::SUCCESS);
    return StatusCode::kOK;
}

StatusCode CleanCore::CleanSnapShotFile2(const FileInfo & fileInfo,
                                        TaskProgress* progress) {
    if (fileInfo.segmentsize() == 0) {
        LOG(ERROR) << "cleanSnapShot2 File Error, segmentsize = 0";
        return StatusCode::KInternalError;
    }
    uint32_t  segmentNum = fileInfo.length() / fileInfo.segmentsize();
    uint64_t segmentSize = fileInfo.segmentsize();
    for (uint32_t i = 0; i < segmentNum; i++) {
        // load  segment
        PageFileSegment segment;
        StoreStatus storeRet = storage_->GetSegment(fileInfo.parentid(),
                                                    i * segmentSize,
                                                    &segment);
        if (storeRet == StoreStatus::KeyNotExist) {
            progress->SetProgress(100 * (i+1) / segmentNum);
            LOG(INFO) << "CleanSnapShotFile2 skip non-existed segment num = "
                      << i+1
                      << ", total segment num = " << segmentNum
                      << ", progress = " << progress->GetProgress();
            continue;
        } else if (storeRet !=  StoreStatus::OK) {
            LOG(ERROR) << "cleanSnapShot2 File Error: "
            << "GetSegment Error, inodeid = " << fileInfo.id()
            << ", filename = " << fileInfo.filename()
            << ", offset = " << i * segmentSize
            << ", sequenceNum = " << fileInfo.seqnum();
            progress->SetStatus(TaskStatus::FAILED);
            return StatusCode::kSnapshotFileDeleteError;
        }

        // delete chunks in chunkserver
        LogicalPoolID logicalPoolID = segment.logicalpoolid();
        uint32_t chunkNum = segment.chunks_size();
        for (uint32_t j = 0; j != chunkNum; j++) {
            // 删除本地快照时，传入待删除的快照版本号和当前文件现存的所有快照版本号，
            // 用于chunkserver进行快照数据的搬迁
            SeqNum deleteSn = fileInfo.seqnum();
            std::vector<SeqNum> snaps;
            for (auto i = 0; i < fileInfo.snaps_size(); ++i) {
                snaps.push_back(fileInfo.snaps(i));
            }
            int ret = copysetClient_->DeleteChunkSnapshot(
                fileInfo.parentid(),
                segment.originfileid(),
                i * chunkNum + j,
                logicalPoolID,
                segment.chunks()[j].copysetid(),
                segment.chunks()[j].chunkid(),
                deleteSn,
                snaps);
            if (ret != 0) {
                LOG(ERROR) << "CleanSnapShotFile2 Error: "
                    << "DeleteChunkSnapshot Error"
                    << ", ret = " << ret
                    << ", inodeid = " << fileInfo.id()
                    << ", filename = " << fileInfo.filename()
                    << ", snapSn = " << deleteSn
                    << ", snaps = [" << Snaps2Str(snaps) << "]";
                progress->SetStatus(TaskStatus::FAILED);
                return StatusCode::kSnapshotFileDeleteError;
            }
        }
        progress->SetProgress(100 * (i+1) / segmentNum);
        LOG(INFO) << "CleanSnapShotFile2 finished clean segment num = " << i+1
                  << ", total segment num = " << segmentNum
                  << ", progress = " << progress->GetProgress();
    }

    // delete the storage
    StoreStatus ret =  storage_->DeleteSnapshotFile(fileInfo.parentid(),
                                                fileInfo.filename());
    if (ret != StoreStatus::OK) {
        LOG(INFO) << "delete snapshotfile error, retCode = " << ret;
        progress->SetStatus(TaskStatus::FAILED);
        return StatusCode::kSnapshotFileDeleteError;
    } else {
        LOG(INFO) << "inodeid = " << fileInfo.id()
            << ", filename = " << fileInfo.filename()
            << ", seq = " << fileInfo.seqnum() << ", deleted";
    }

    progress->SetProgress(100);
    progress->SetStatus(TaskStatus::SUCCESS);
    return StatusCode::kOK;
}

StatusCode CleanCore::CleanFile(const FileInfo & commonFile,
                                TaskProgress* progress) {
    if (commonFile.segmentsize() == 0) {
        LOG(ERROR) << "Clean commonFile File Error, segmentsize = 0";
        return StatusCode::KInternalError;
    }

    int  segmentNum = commonFile.length() / commonFile.segmentsize();
    uint64_t segmentSize = commonFile.segmentsize();
    for (int i = 0; i != segmentNum; i++) {
        // load  segment
        PageFileSegment segment;
        StoreStatus storeRet = storage_->GetSegment(commonFile.id(),
                                    i * segmentSize, &segment);
        if (storeRet == StoreStatus::KeyNotExist) {
            continue;
        } else if (storeRet !=  StoreStatus::OK) {
            LOG(ERROR) << "Clean common File Error: "
                << "GetSegment Error, inodeid = " << commonFile.id()
                << ", filename = " << commonFile.filename()
                << ", offset = " << i * segmentSize;
            progress->SetStatus(TaskStatus::FAILED);
            return StatusCode::kCommonFileDeleteError;
        }

        // delete chunks in chunkserver
        LogicalPoolID logicalPoolID = segment.logicalpoolid();
        uint32_t chunkNum = segment.chunks_size();
        for (uint32_t j = 0; j != chunkNum; j++) {
            SeqNum seq = commonFile.seqnum();
            int ret = copysetClient_->DeleteChunk(
                commonFile.id(),
                segment.originfileid(),
                i * chunkNum + j,
                logicalPoolID,
                segment.chunks()[j].copysetid(),
                segment.chunks()[j].chunkid(),
                seq);
            if (ret != 0) {
                LOG(ERROR) << "Clean common File Error: "
                    << "DeleteChunk Error"
                    << ", ret = " << ret
                    << ", inodeid = " << commonFile.id()
                    << ", filename = " << commonFile.filename()
                    << ", sequenceNum = " << seq;
                progress->SetStatus(TaskStatus::FAILED);
                return StatusCode::kCommonFileDeleteError;
            }
        }

        // delete segment
        int64_t revision;
        storeRet = storage_->DeleteSegment(
            commonFile.id(), i * segmentSize, &revision);
        if (storeRet != StoreStatus::OK) {
            LOG(ERROR) << "Clean common File Error: "
            << "DeleteSegment Error, inodeid = " << commonFile.id()
            << ", filename = " << commonFile.filename()
            << ", offset = " << i * segmentSize
            << ", sequenceNum = " << commonFile.seqnum();
            progress->SetStatus(TaskStatus::FAILED);
            return StatusCode::kCommonFileDeleteError;
        }
        allocStatistic_->DeAllocSpace(segment.logicalpoolid(),
            segment.segmentsize(), revision);
        progress->SetProgress(100 * (i + 1) / segmentNum);
    }

    // delete the storage
    StoreStatus ret =  storage_->DeleteFile(commonFile.parentid(),
                                                   commonFile.filename());
    if (ret != StoreStatus::OK) {
        LOG(INFO) << "delete common file error, retCode = " << ret;
        progress->SetStatus(TaskStatus::FAILED);
        return StatusCode::kCommonFileDeleteError;
    } else {
        LOG(INFO) << "inodeid = " << commonFile.id()
            << ", filename = " << commonFile.filename()
            << ", seq = " << commonFile.seqnum() << ", deleted";
    }

    progress->SetProgress(100);
    progress->SetStatus(TaskStatus::SUCCESS);
    return StatusCode::kOK;
}
}  // namespace mds
}  // namespace curve
