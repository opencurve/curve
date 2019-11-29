/*
 * Project: curve
 * Created Date: Wed Sep 18 2019
 * Author: xuchaojie
 * Copyright (c) 2019 netease
 */

#include "src/snapshotcloneserver/snapshot/snapshot_task.h"

namespace curve {
namespace snapshotcloneserver {

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
                   << ", fileName = " << name.fileName_
                   << ", chunkSeqNum = " << name.chunkSeqNum_
                   << ", chunkIndex = " << name.chunkIndex_;
        return ret;
    }

    auto buf = std::unique_ptr<char[]>(new char[chunkSplitSize]);
    bool hasAddPart = false;
    for (uint64_t i = 0;
        i < chunkSize / chunkSplitSize;
        i++) {
        uint64_t offset = i * chunkSplitSize;
        ret = client_->ReadChunkSnapshot(
            cidInfo,
            name.chunkSeqNum_,
            offset,
            chunkSplitSize,
            buf.get());
        if (ret < 0) {
            LOG(ERROR) << "ReadChunkSnapshot error, "
                       << " ret = " << ret
                       << ", logicalPool = " << cidInfo.lpid_
                       << ", copysetId = " << cidInfo.cpid_
                       << ", chunkId = " << cidInfo.cid_
                       << ", seqNum = " << name.chunkSeqNum_
                       << ", offset = " << offset;
            ret = kErrCodeInternalError;
            break;
        }
        ret = dataStore_->DataChunkTranferAddPart(name,
            transferTask,
            i,
            chunkSplitSize,
            buf.get());
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
    return kErrCodeSuccess;
}

}  // namespace snapshotcloneserver
}  // namespace curve
