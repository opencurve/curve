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
 * Created Date: Wed Sep 18 2019
 * Author: xuchaojie
 */

#include "src/snapshotcloneserver/snapshot/snapshot_task.h"

#include <list>

#include "src/common/timeutility.h"

namespace curve {
namespace snapshotcloneserver {

void ReadChunkSnapshotClosure::Run() {
    std::unique_ptr<ReadChunkSnapshotClosure> self_guard(this);
    context_->retCode = GetRetCode();
    if (context_->retCode < 0) {
        LOG(WARNING) << "ReadChunkSnapshotClosure return fail"
                     << ", ret = " << context_->retCode
                     << ", index = " << context_->partIndex
                     << ", logicalPool = " << context_->cidInfo.lpid_
                     << ", copysetId = " << context_->cidInfo.cpid_
                     << ", chunkId = " << context_->cidInfo.cid_
                     << ", seqNum = " << context_->seqNum;
    }
    tracker_->PushResultContext(context_);
    tracker_->HandleResponse(context_->retCode);
    return;
}

/**
 * @brief Dump a single chunk of a snapshot
 * @detail
 *  Since a single chunk is too large, chunk dumping is done in segments, with
 * each segment size being chunkSplitSize_. The steps are as follows:
 *  1. Create a dump task transferTask and initialize it using
 * DataChunkTransferInit.
 *  2. Call ReadChunkSnapshot to read a segment of the chunk from CurveFS.
 *  3. Call DataChunkTransferAddPart to dump a segment.
 *  4. Repeat steps 2 and 3 until all segments have been dumped, and then call
 * DataChunkTransferComplete to end the dump task.
 *  5. If there are any errors during reading or dumping in the process, call
 * DataChunkTransferAbort to abandon the dump and return an error code.
 *
 * @return Error code
 */
int TransferSnapshotDataChunkTask::TransferSnapshotDataChunk() {
    ChunkDataName name = taskInfo_->name_;
    uint64_t chunkSize = taskInfo_->chunkSize_;
    ChunkIDInfo cidInfo = taskInfo_->cidInfo_;
    uint64_t chunkSplitSize = taskInfo_->chunkSplitSize_;

    std::shared_ptr<TransferTask> transferTask =
        std::make_shared<TransferTask>();
    int ret = dataStore_->DataChunkTranferInit(name, transferTask);
    if (ret < 0) {
        LOG(ERROR) << "DataChunkTranferInit error, "
                   << " ret = " << ret
                   << ", chunkDataName = " << name.ToDataChunkKey()
                   << ", logicalPool = " << cidInfo.lpid_
                   << ", copysetId = " << cidInfo.cpid_
                   << ", chunkId = " << cidInfo.cid_;
        return ret;
    }

    auto tracker = std::make_shared<ReadChunkSnapshotTaskTracker>();
    for (uint64_t i = 0; i < chunkSize / chunkSplitSize; i++) {
        auto context = std::make_shared<ReadChunkSnapshotContext>();
        context->cidInfo = taskInfo_->cidInfo_;
        context->seqNum = taskInfo_->name_.chunkSeqNum_;
        context->partIndex = i;
        context->buf = std::unique_ptr<char[]>(new char[chunkSplitSize]);
        context->len = chunkSplitSize;
        context->startTime = TimeUtility::GetTimeofDaySec();
        context->clientAsyncMethodRetryTimeSec =
            taskInfo_->clientAsyncMethodRetryTimeSec_;
        ret = StartAsyncReadChunkSnapshot(tracker, context);
        if (ret < 0) {
            break;
        }
        if (tracker->GetTaskNum() >= taskInfo_->readChunkSnapshotConcurrency_) {
            tracker->WaitSome(1);
        }
        std::list<ReadChunkSnapshotContextPtr> results =
            tracker->PopResultContexts();
        ret = HandleReadChunkSnapshotResultsAndRetry(tracker, transferTask,
                                                     results);
        if (ret < 0) {
            break;
        }
    }
    if (ret >= 0) {
        do {
            tracker->WaitSome(1);
            std::list<ReadChunkSnapshotContextPtr> results =
                tracker->PopResultContexts();
            if (0 == results.size()) {
                // Completed, no new results
                break;
            }
            ret = HandleReadChunkSnapshotResultsAndRetry(tracker, transferTask,
                                                         results);
            if (ret < 0) {
                break;
            }
        } while (true);
        if (ret >= 0) {
            ret = dataStore_->DataChunkTranferComplete(name, transferTask);
            if (ret < 0) {
                LOG(ERROR) << "DataChunkTranferComplete fail"
                           << ", ret = " << ret
                           << ", chunkDataName = " << name.ToDataChunkKey()
                           << ", logicalPool = " << cidInfo.lpid_
                           << ", copysetId = " << cidInfo.cpid_
                           << ", chunkId = " << cidInfo.cid_;
            }
        }
    }
    if (ret < 0) {
        int ret2 = dataStore_->DataChunkTranferAbort(name, transferTask);
        if (ret2 < 0) {
            LOG(ERROR) << "DataChunkTranferAbort fail"
                       << ", ret = " << ret2
                       << ", chunkDataName = " << name.ToDataChunkKey()
                       << ", logicalPool = " << cidInfo.lpid_
                       << ", copysetId = " << cidInfo.cpid_
                       << ", chunkId = " << cidInfo.cid_;
        }
        return ret;
    }
    return kErrCodeSuccess;
}

int TransferSnapshotDataChunkTask::StartAsyncReadChunkSnapshot(
    std::shared_ptr<ReadChunkSnapshotTaskTracker> tracker,
    std::shared_ptr<ReadChunkSnapshotContext> context) {
    ReadChunkSnapshotClosure* cb =
        new ReadChunkSnapshotClosure(tracker, context);
    tracker->AddOneTrace();
    uint64_t offset = context->partIndex * context->len;
    LOG_EVERY_SECOND(INFO) << "Doing ReadChunkSnapshot"
                           << ", logicalPool = " << context->cidInfo.lpid_
                           << ", copysetId = " << context->cidInfo.cpid_
                           << ", chunkId = " << context->cidInfo.cid_
                           << ", seqNum = " << context->seqNum
                           << ", offset = " << offset;
    int ret =
        client_->ReadChunkSnapshot(context->cidInfo, context->seqNum, offset,
                                   context->len, context->buf.get(), cb);
    if (ret < 0) {
        LOG(ERROR) << "ReadChunkSnapshot error, "
                   << " ret = " << ret
                   << ", logicalPool = " << context->cidInfo.lpid_
                   << ", copysetId = " << context->cidInfo.cpid_
                   << ", chunkId = " << context->cidInfo.cid_
                   << ", seqNum = " << context->seqNum
                   << ", offset = " << offset;
        return ret;
    }
    return kErrCodeSuccess;
}

int TransferSnapshotDataChunkTask::HandleReadChunkSnapshotResultsAndRetry(
    std::shared_ptr<ReadChunkSnapshotTaskTracker> tracker,
    std::shared_ptr<TransferTask> transferTask,
    const std::list<ReadChunkSnapshotContextPtr>& results) {
    int ret = kErrCodeSuccess;
    for (auto context : results) {
        if (context->retCode < 0) {
            uint64_t nowTime = TimeUtility::GetTimeofDaySec();
            if (nowTime - context->startTime <
                context->clientAsyncMethodRetryTimeSec) {
                // retry
                std::this_thread::sleep_for(std::chrono::milliseconds(
                    taskInfo_->clientAsyncMethodRetryIntervalMs_));
                ret = StartAsyncReadChunkSnapshot(tracker, context);
                if (ret < 0) {
                    return ret;
                }
            } else {
                ret = context->retCode;
                LOG(ERROR) << "ReadChunkSnapshot tracker GetResult fail"
                           << ", ret = " << ret;
                return ret;
            }
        } else {
            ret = dataStore_->DataChunkTranferAddPart(
                taskInfo_->name_, transferTask, context->partIndex,
                context->len, context->buf.get());
            if (ret < 0) {
                LOG(ERROR) << "DataChunkTranferAddPart fail"
                           << ", ret = " << ret << ", chunkDataName = "
                           << taskInfo_->name_.ToDataChunkKey()
                           << ", index = " << context->partIndex;
                return ret;
            }
        }
    }
    return ret;
}

}  // namespace snapshotcloneserver
}  // namespace curve
