/*
 * Project: curve
 * Created Date: 18-9-28
 * Author: wudemiao
 * Copyright (c) 2018 netease
 */

#include "src/client/chunk_closure.h"

#include <unistd.h>
#include <braft/configuration.h>

#include <string>

#include "src/client/client_common.h"
#include "src/client/request_sender.h"
#include "src/client/request_sender_manager.h"
#include "src/client/copyset_client.h"
#include "src/client/metacache.h"
#include "src/client/request_closure.h"
#include "src/client/request_context.h"

namespace curve {
namespace client {

void WriteChunkClosure::Run() {
    std::unique_ptr<WriteChunkClosure> selfGuard(this);
    std::unique_ptr<brpc::Controller> cntlGuard(cntl_);
    std::unique_ptr<ChunkResponse> responseGuard(response_);
    brpc::ClosureGuard doneGuard(done_);

    MetaCache* metaCache = client_->GetMetaCache();
    RequestClosure *reqDone = dynamic_cast<RequestClosure *>(done_);
    RequestContext *reqCtx = reqDone->GetReqCtx();
    LogicPoolID logicPoolId = reqCtx->logicpoolid_;
    CopysetID copysetId = reqCtx->copysetid_;

    int status = -1;
    ChunkServerID leaderId;
    butil::EndPoint leaderAddr;

    if (cntl_->Failed()) {
        /* 如果连接失败，再等一定时间再重试 */
        status = cntl_->ErrorCode();
        LOG(ERROR) << "write failed, error code: " << cntl_->ErrorCode()
                   << ", error: " << cntl_->ErrorText();
        /*It will be invoked in brpc's bthread, so*/
        metaCache->UpdateAppliedIndex(reqCtx->logicpoolid_,
                                        reqCtx->copysetid_,
                                        0);
        bthread_usleep(FLAGS_client_chunk_op_retry_interval_us);
        goto write_retry;
    }

    /* 1. write success，返回成功 */
    status = response_->status();
    if (CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS == status) {
        reqDone->SetFailed(0);
        metaCache->UpdateAppliedIndex(reqCtx->logicpoolid_,
                                        reqCtx->copysetid_,
                                        response_->appliedindex());
        return;
    }
    /* 2. 处理 chunkserver 返回的错误 */
    /* 2.1. 不是 leader */
    if (CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED == status) {
        if (response_->has_redirect()) {
            std::string redirect = response_->redirect();
            braft::PeerId leader;
            if (0 == leader.parse(redirect)) {
                /**
                 * TODO(tongguangxun): if raw copyset info is A,B,C,
                 * chunkserver side copyset info is A,B,D, and D is leader
                 * copyset client need tell cache, just insert new leader.
                 */ 
                metaCache->UpdateLeader(logicPoolId,
                                         copysetId,
                                         &leaderId,
                                         leader.addr);
                goto write_retry;
            }
        }
        if (-1 == metaCache->GetLeader(logicPoolId,
                                        copysetId,
                                        &leaderId,
                                        &leaderAddr,
                                        true)) {
            usleep(FLAGS_client_chunk_op_retry_interval_us);
        }
        goto write_retry;
    }
    /* 2.2. Copyset不存在,大概率都是配置变更了 */
    if (CHUNK_OP_STATUS::CHUNK_OP_STATUS_COPYSET_NOTEXIST == status) {
        if (-1 == metaCache->GetLeader(logicPoolId,
                                        copysetId,
                                        &leaderId,
                                        &leaderAddr,
                                        true)) {
            bthread_usleep(FLAGS_client_chunk_op_retry_interval_us);
        }
        goto write_retry;
    }
    /* 2.3. 非法参数 */
    if (CHUNK_OP_STATUS::CHUNK_OP_STATUS_INVALID_REQUEST == status) {
        reqDone->SetFailed(status);
        LOG(ERROR) << "write failed for invalid format, write info: "
                   << "<" << reqCtx->logicpoolid_ << ", "  << reqCtx->copysetid_
                   << ", " << reqCtx->chunkid_ << "> offset=" << reqCtx->offset_
                   << ", length=" << reqCtx->rawlength_
                   << ", status=" << status;
        return;
    }
    /* 2.4. 其他错误，过一段时间再重试 */
    LOG(ERROR) << "write failed for UNKNOWN reason, write info: "
               << "<" << reqCtx->logicpoolid_ << ", "  << reqCtx->copysetid_
               << ", " << reqCtx->chunkid_ << "> offset=" << reqCtx->offset_
               << ", length=" << reqCtx->rawlength_
               << ", status=" << status;
    bthread_usleep(FLAGS_client_chunk_op_retry_interval_us);
    goto write_retry;

write_retry:
    if (retriedTimes_ + 1 >= FLAGS_client_chunk_op_max_retry) {
        reqDone->SetFailed(status);
        metaCache->UpdateAppliedIndex(reqCtx->logicpoolid_,
                                        reqCtx->copysetid_,
                                        0);
        LOG(ERROR) << "retried times exceeds";
        return;
    }
    client_->WriteChunk(logicPoolId,
                        copysetId,
                        reqCtx->chunkid_,
                        reqCtx->data_,
                        reqCtx->offset_,
                        reqCtx->rawlength_,
                        doneGuard.release(),
                        retriedTimes_ + 1);
}

void ReadChunkClosure::Run() {
    std::unique_ptr<ReadChunkClosure> selfGuard(this);
    std::unique_ptr<brpc::Controller> cntlGuard(cntl_);
    std::unique_ptr<ChunkResponse> responseGuard(response_);
    brpc::ClosureGuard doneGuard(done_);

    MetaCache* metaCache = client_->GetMetaCache();
    RequestClosure *reqDone = dynamic_cast<RequestClosure *>(done_);
    RequestContext *reqCtx = reqDone->GetReqCtx();
    LogicPoolID logicPoolId = reqCtx->logicpoolid_;
    CopysetID copysetId = reqCtx->copysetid_;

    int status = -1;
    ChunkServerID leaderId;
    butil::EndPoint leaderAddr;

    if (cntl_->Failed()) {
        /* 如果连接失败，再等一定时间再重试 */
        status = cntl_->ErrorCode();
        LOG(ERROR) << "read failed, error: " << cntl_->ErrorText();
        bthread_usleep(FLAGS_client_chunk_op_retry_interval_us);
        goto read_retry;
    }

    /* 1. write success，返回成功 */
    status = response_->status();
    if (CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS == status) {
        reqDone->SetFailed(0);
        ::memcpy(const_cast<void*>(reinterpret_cast<const void*>(reqCtx->data_)),  //NOLINT
                 cntl_->response_attachment().to_string().c_str(),
                 cntl_->response_attachment().size());
        return;
    }
    /* 2. 处理 chunkserver 返回的错误 */
    /* 2.1. 不是 leader */
    if (CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED == status) {
        if (response_->has_redirect()) {
            std::string redirect = response_->redirect();
            braft::PeerId leader;
            if (0 == leader.parse(redirect)) {
                metaCache->UpdateLeader(logicPoolId,
                                         copysetId,
                                         &leaderId,
                                         leader.addr);
                goto read_retry;
            }
        }
        if (-1 == metaCache->GetLeader(logicPoolId,
                                          copysetId,
                                          &leaderId,
                                          &leaderAddr,
                                          true)) {
            bthread_usleep(FLAGS_client_chunk_op_retry_interval_us);
        }

        goto read_retry;
    }
    /* 2.2. Copyset 不存在大概率都是配置变更了 */
    if (CHUNK_OP_STATUS::CHUNK_OP_STATUS_COPYSET_NOTEXIST == status) {
        if (-1 == metaCache->GetLeader(logicPoolId,
                                        copysetId,
                                        &leaderId,
                                        &leaderAddr,
                                        true)) {
            bthread_usleep(FLAGS_client_chunk_op_retry_interval_us);
        }
        goto read_retry;
    }
    /* 2.3. 非法参数 */
    if (CHUNK_OP_STATUS::CHUNK_OP_STATUS_INVALID_REQUEST == status) {
        reqDone->SetFailed(status);
        LOG(ERROR) << "read failed for invalid format, write info: "
                   << "<" << reqCtx->logicpoolid_ << ", "  << reqCtx->copysetid_
                   << ", " << reqCtx->chunkid_ << "> offset=" << reqCtx->offset_
                   << ", length=" << reqCtx->rawlength_
                   << ", status=" << status;
        return;
    }
    /* 2.4. 其他错误，过一段时间再重试 */
    LOG(ERROR) << "read failed for UNKNOWN reason, read info: "
               << "<" << reqCtx->logicpoolid_ << ", "  << reqCtx->copysetid_
               << ", " << reqCtx->chunkid_ << "> offset=" << reqCtx->offset_
               << ", length=" << reqCtx->rawlength_
               << ", status=" << status;
    bthread_usleep(FLAGS_client_chunk_op_retry_interval_us);
    goto read_retry;

read_retry:
    if (retriedTimes_ + 1 >= FLAGS_client_chunk_op_max_retry) {
        reqDone->SetFailed(status);
        LOG(ERROR) << "retried times exceeds";
        return;
    }
    client_->ReadChunk(logicPoolId,
                       copysetId,
                       reqCtx->chunkid_,
                       reqCtx->offset_,
                       reqCtx->rawlength_,
                       reqCtx->appliedindex_,
                       doneGuard.release(),
                       retriedTimes_ + 1);
}

}   // namespace client
}   // namespace curve
