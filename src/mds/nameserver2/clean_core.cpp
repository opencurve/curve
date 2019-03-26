/*
 * Project: curve
 * Created Date: Wednesday December 19th 2018
 * Author: hzsunjianliang
 * Copyright (c) 2018 netease
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
    for (uint32_t i = 0; i < segmentNum; i++) {
        // load  segment
        auto storeKey = EncodeSegmentStoreKey(fileInfo.id(),
            i * fileInfo.segmentsize());
        PageFileSegment segment;
        StoreStatus storeRet = storage_->GetSegment(storeKey, &segment);
        if (storeRet == StoreStatus::KeyNotExist) {
            continue;
        } else if (storeRet !=  StoreStatus::OK) {
            LOG(ERROR) << "cleanSnapShot File Error: "
            << "GetSegment Error,  filename = " << fileInfo.fullpathname()
            << ", sequenceNum = " << fileInfo.seqnum();
            progress->SetStatus(TaskStatus::FAILED);
            return StatusCode::kSnapshotFileDeleteError;
        }

        // delete chunks in chunkserver
        LogicalPoolID logicalPoolID = segment.logicalpoolid();
        uint32_t chunkNum = segment.chunks_size();
        for (uint32_t j = 0; j != chunkNum; j++) {
            SeqNum seq = fileInfo.seqnum();
            int ret = copysetClient_->DeleteSnapShotChunk(logicalPoolID,
                segment.chunks()[j].copysetid(),
                segment.chunks()[j].chunkid(),
                seq);
            if (ret != 0) {
                LOG(ERROR) << "CleanSnapShotFile Error: "
                    << "DeleteChunk Error,  filename = "
                    << fileInfo.fullpathname()
                    << ", sequenceNum = " << seq;
                progress->SetStatus(TaskStatus::FAILED);
                return StatusCode::kSnapshotFileDeleteError;
            }
        }

        // delete segment
        storeRet = storage_->DeleteSegment(storeKey);
        if (storeRet != StoreStatus::OK) {
            LOG(ERROR) << "cleanSnapShot File Error: "
            << "DeleteSegment Error,  filename = " << fileInfo.fullpathname()
            << ", sequenceNum = " << fileInfo.seqnum();
            progress->SetStatus(TaskStatus::FAILED);
            return StatusCode::kSnapshotFileDeleteError;
        }

        progress->SetProgress(100 * (i+1) / segmentNum);
    }

    // delete the storage
    auto snapShotFileStoreKey =
        EncodeSnapShotFileStoreKey(fileInfo.parentid(), fileInfo.filename());

    StoreStatus ret =  storage_->DeleteFile(snapShotFileStoreKey);
    if (ret != StoreStatus::OK) {
        LOG(INFO) << "delete snapshotfile error, retCode = " << ret;
        progress->SetStatus(TaskStatus::FAILED);
        return StatusCode::kSnapshotFileDeleteError;
    } else {
        LOG(INFO) << "filename = " << fileInfo.fullpathname()
            << ", seq = " << fileInfo.seqnum() << ", deleted";
    }

    progress->SetProgress(100);
    progress->SetStatus(TaskStatus::SUCCESS);
    return StatusCode::kOK;
}

void CleanCore::CleanCommonFile(const FileInfo & commonFile,
                                TaskProgress* progress) {
    // TODO(hzsunjianliang): need
    progress->SetStatus(TaskStatus::FAILED);
    return;
}

}  // namespace mds
}  // namespace curve
