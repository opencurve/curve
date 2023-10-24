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
                           << ", ret = " << PrintMdsDescByErrorCode(ret)
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

        int ret = DeleteChunksInSegment(segment, commonFile.seqnum());
        if (ret != 0) {
            LOG(ERROR) << "Clean common File Error: "
                       << ", ret = " << PrintMdsDescByErrorCode(ret)
                       << ", inodeid = " << commonFile.id()
                       << ", filename = " << commonFile.filename()
                       << ", sequenceNum = " << commonFile.seqnum();
            progress->SetStatus(TaskStatus::FAILED);
            return StatusCode::kCommonFileDeleteError;
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
        LOG(INFO) << "delete common file error, retDesc = " << ret;
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

StatusCode CleanCore::CleanDiscardSegment(
    const std::string& cleanSegmentKey,
    const DiscardSegmentInfo& discardSegmentInfo, TaskProgress* progress) {
    const FileInfo& fileInfo = discardSegmentInfo.fileinfo();
    const PageFileSegment& segment = discardSegmentInfo.pagefilesegment();
    const SeqNum seq = fileInfo.seqnum();

    LOG(INFO) << "Start CleanDiscardSegment, filename = " << fileInfo.filename()
              << ", inodeid = " << fileInfo.id()
              << ", segment offset = " << segment.startoffset();

    butil::Timer timer;
    timer.start();

    // delete chunks
    int ret = DeleteChunksInSegment(segment, seq);
    if (ret != 0) {
        LOG(ERROR) << "CleanDiscardSegment failed, DeleteChunk Error, ret = "
                   << PrintMdsDescByErrorCode(ret)
                   << ", filename = " << fileInfo.filename()
                   << ", inodeid = " << fileInfo.id()
                   << ", segment offset = " << segment.startoffset();
        progress->SetStatus(TaskStatus::FAILED);
        return StatusCode::KInternalError;
    }

    // delete segment
    int64_t revision;
    auto storeRet = storage_->CleanDiscardSegment(segment.segmentsize(),
                                                  cleanSegmentKey, &revision);
    if (storeRet != StoreStatus::OK) {
        LOG(ERROR) << "CleanDiscardSegment failed, filename = "
                   << fileInfo.filename()
                   << ", offset = " << segment.startoffset();
        progress->SetStatus(TaskStatus::FAILED);
        return StatusCode::KInternalError;
    }

    allocStatistic_->DeAllocSpace(segment.logicalpoolid(),
                                  segment.segmentsize(), revision);
    progress->SetProgress(100);
    progress->SetStatus(TaskStatus::SUCCESS);

    timer.stop();
    LOG(INFO) << "CleanDiscardSegment success, filename = "
              << fileInfo.filename() << ", inodeid = " << fileInfo.id()
              << ", segment offset = " << segment.startoffset() << ", cost "
              << timer.m_elapsed(1.0) << " ms";

    return StatusCode::kOK;
}

int CleanCore::DeleteChunksInSegment(const PageFileSegment& segment,
                                     const SeqNum& seq) {
    const LogicalPoolID logicalPoolId = segment.logicalpoolid();
    for (int i = 0; i < segment.chunks_size(); ++i) {
        int ret = copysetClient_->DeleteChunk(
            logicalPoolId,
            segment.chunks()[i].copysetid(),
            segment.chunks()[i].chunkid(),
            seq);

        if (ret != 0) {
            LOG(ERROR) << "DeleteChunk failed, ret = "
                       << PrintMdsDescByErrorCode(ret)
                       << ", logicalpoolid = " << logicalPoolId
                       << ", copysetid = " << segment.chunks()[i].copysetid()
                       << ", chunkid = " << segment.chunks()[i].chunkid()
                       << ", seq = " << seq;
            return ret;
        }
    }

    return 0;
}

}  // namespace mds
}  // namespace curve
