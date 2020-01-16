/*
 * Project: curve
 * Created Date: Wed Sep 18 2019
 * Author: xuchaojie
 * Copyright (c) 2019 netease
 */

#include <list>

#include "src/common/timeutility.h"
#include "src/snapshotcloneserver/snapshot/snapshot_task.h"

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
 * @brief 转储快照的单个chunk
 * @detail
 *  由于单个chunk过大，chunk转储分片进行，分片大小为chunkSplitSize_，
 *  步骤如下：
 *  1. 创建一个转储任务transferTask，并调用DataChunkTranferInit初始化
 *  2. 调用ReadChunkSnapshot从curvefs读取chunk的一个分片
 *  3. 调用DataChunkTranferAddPart转储一个分片
 *  4. 重复2、3直到所有分片转储完成，调用DataChunkTranferComplete结束转储任务
 *  5. 中间如有读取或转储发生错误，则调用DataChunkTranferAbort放弃转储，
 *  并返回错误码
 *
 * @return 错误码
 */
int TransferSnapshotDataChunkTask::TransferSnapshotDataChunk() {
    ChunkDataName name = taskInfo_->name_;
    uint64_t chunkSize = taskInfo_->chunkSize_;
    ChunkIDInfo cidInfo = taskInfo_->cidInfo_;
    uint64_t chunkSplitSize = taskInfo_->chunkSplitSize_;

    std::shared_ptr<TransferTask> transferTask =
        std::make_shared<TransferTask>();
    int ret = dataStore_->DataChunkTranferInit(name,
            transferTask);
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
    for (uint64_t i = 0;
        i < chunkSize / chunkSplitSize;
        i++) {
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
    }
    if (ret >= 0) {
        do {
            tracker->WaitSome(1);
            std::list<ReadChunkSnapshotContextPtr> results =
                tracker->PopResultContexts();
            if (0 == results.size()) {
                // 已经完成，没有新的结果了
                break;
            }
            for (auto context : results) {
                if (context->retCode < 0) {
                    uint64_t nowTime = TimeUtility::GetTimeofDaySec();
                    if (nowTime - context->startTime <
                        context->clientAsyncMethodRetryTimeSec) {
                        // retry
                        std::this_thread::sleep_for(
                            std::chrono::milliseconds(
                                taskInfo_->clientAsyncMethodRetryIntervalMs_));
                        ret = StartAsyncReadChunkSnapshot(tracker, context);
                        if (ret < 0) {
                            break;
                        }
                    } else {
                        ret = context->retCode;
                        LOG(ERROR) << "ReadChunkSnapshot tracker GetResult fail"
                                   << ", ret = " << ret;
                        break;
                    }
                } else {
                    ret = dataStore_->DataChunkTranferAddPart(
                        taskInfo_->name_,
                        transferTask,
                        context->partIndex,
                        context->len,
                        context->buf.get());
                    if (ret < 0) {
                        LOG(ERROR) << "DataChunkTranferAddPart fail"
                                   << ", ret = " << ret
                                   << ", chunkDataName = "
                                   << taskInfo_->name_.ToDataChunkKey()
                                   << ", index = " << context->partIndex;
                        break;
                    }
                }
            }
        } while (true);
        if (ret >= 0) {
            ret =
                dataStore_->DataChunkTranferComplete(name, transferTask);
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
            int ret2 =
                dataStore_->DataChunkTranferAbort(
                name,
                transferTask);
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
    ReadChunkSnapshotClosure *cb =
        new ReadChunkSnapshotClosure(tracker, context);
    tracker->AddOneTrace();
    uint64_t offset = context->partIndex * context->len;
    LOG_EVERY_SECOND(INFO) << "Doing ReadChunkSnapshot"
                           << ", logicalPool = " << context->cidInfo.lpid_
                           << ", copysetId = " << context->cidInfo.cpid_
                           << ", chunkId = " << context->cidInfo.cid_
                           << ", seqNum = " << context->seqNum
                           << ", offset = " << offset;
    int ret = client_->ReadChunkSnapshot(
        context->cidInfo,
        context->seqNum,
        offset,
        context->len,
        context->buf.get(),
        cb);
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

}  // namespace snapshotcloneserver
}  // namespace curve
