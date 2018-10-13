/*
 * Project: curve
 * Created Date: 18-9-25
 * Author: wudemiao
 * Copyright (c) 2018 netease
 */

#include "src/client/copyset_client.h"

#include <glog/logging.h>
#include <unistd.h>

#include <utility>

#include "src/client/request_sender.h"
#include "src/client/metacache.h"

namespace curve {
namespace client {

DEFINE_int32(client_chunk_op_retry_interval_us,
             200 * 1000,
             "client chunk op retry interval us ");
DEFINE_int32(client_chunk_op_max_retry, 3, "client chunk op max retry ");

int CopysetClient::Init(RequestSenderManager *senderManager,
                        MetaCache *metaCache) {
    if (nullptr == senderManager || nullptr == metaCache) {
        return -1;
    }
    senderManager_ = senderManager;
    metaCache_ = metaCache;
    return 0;
}

int CopysetClient::ReadChunk(LogicPoolID logicPoolId,
                             CopysetID copysetId,
                             ChunkID chunkId,
                             off_t offset,
                             size_t length,
                             google::protobuf::Closure *done,
                             int retriedTimes) {
    brpc::ClosureGuard doneGuard(done);

    std::shared_ptr<RequestSender> senderPtr = nullptr;
    ChunkServerID leaderId;
    butil::EndPoint leaderAddr;

    for (int i = retriedTimes; i < FLAGS_client_chunk_op_max_retry; ++i) {
        /* cache 中找 */
        if (-1 == metaCache_->GetLeader(logicPoolId, copysetId,
                                        &leaderId, &leaderAddr, false)) {
            /* 没找到刷新 cache */
            if (-1 == metaCache_->GetLeader(logicPoolId, copysetId,
                                            &leaderId, &leaderAddr, true)) {
                LOG(WARNING) << "Get leader address form cache failed, but "
                             << "also refresh leader address failed from mds."
                             << "(write <" << logicPoolId << ", " << copysetId
                             << ">)";
                /* 刷新 cache 失败，再等一定时间再重试 */
                usleep(FLAGS_client_chunk_op_retry_interval_us);
                continue;
            }
        }

        /* 成功获取了 leader id */
        auto senderPtr = senderManager_->GetOrCreateSender(leaderId, leaderAddr);   //NOLINT
        if (nullptr != senderPtr) {
            /* 如果建立连接成功 */
            ReadChunkClosure *readDone =
                new ReadChunkClosure(this, doneGuard.release());
            readDone->SetRetriedTimes(i);
            senderPtr->ReadChunk(logicPoolId, copysetId, chunkId,
                                 offset, length, readDone);
            /* 成功发起 read，break出去，重试逻辑进入 ReadChunkClosure 回调 */
            break;
        } else {
            /* 如果建立连接失败，再等一定时间再重试 */
            usleep(FLAGS_client_chunk_op_retry_interval_us);
            continue;
        }
    }

    return 0;
}

int CopysetClient::WriteChunk(LogicPoolID logicPoolId,
                              CopysetID copysetId,
                              ChunkID chunkId,
                              const char *buf,
                              off_t offset,
                              size_t length,
                              google::protobuf::Closure *done,
                              int retriedTimes) {
    std::shared_ptr<RequestSender> senderPtr = nullptr;
    ChunkServerID leaderId;
    butil::EndPoint leaderAddr;
    brpc::ClosureGuard doneGuard(done);

    for (int i = retriedTimes; i < FLAGS_client_chunk_op_max_retry; ++i) {
        /* cache 中找 */
        if (-1 == metaCache_->GetLeader(logicPoolId, copysetId,
                                        &leaderId, &leaderAddr, false)) {
            /* 没找到刷新 cache */
            if (-1 == metaCache_->GetLeader(logicPoolId, copysetId,
                                            &leaderId, &leaderAddr, true)) {
                LOG(WARNING) << "Get leader address form cache failed, but "
                             << "also refresh leader address failed from mds."
                             << "(write <" << logicPoolId << ", " << copysetId
                             << ">)";
                /* 刷新 cache 失败，再等一定时间再重试 */
                usleep(FLAGS_client_chunk_op_retry_interval_us);
                continue;
            }
        }

        auto senderPtr = senderManager_->GetOrCreateSender(leaderId, leaderAddr);   //NOLINT
        if (nullptr != senderPtr) {
            WriteChunkClosure *writeDone
                = new WriteChunkClosure(this, doneGuard.release());
            writeDone->SetRetriedTimes(i);
            senderPtr->WriteChunk(logicPoolId, copysetId, chunkId,
                                  buf, offset, length, writeDone);
            /* 成功发起 write，break出去，重试逻辑进入 WriteChunkClosure 回调 */
            break;
        } else {
            /* 如果建立连接失败，再等一定时间再重试 */
            LOG(ERROR) << "create or reset sender failed (write <"
                       << logicPoolId << ", " << copysetId << ">)";
            usleep(FLAGS_client_chunk_op_retry_interval_us);
            continue;
        }
    }

    return 0;
}

}   // namespace client
}   // namespace curve
