/*
 * Project: curve
 * Created Date: 18-9-28
 * Author: wudemiao
 * Copyright (c) 2018 netease
 */

#include "src/client/chunk_closure.h"

#include <unistd.h>
#include <bthread/bthread.h>
#include <braft/configuration.h>

#include <string>
#include <memory>

#include "src/client/client_common.h"
#include "src/client/request_sender.h"
#include "src/client/request_sender_manager.h"
#include "src/client/copyset_client.h"
#include "src/client/metacache.h"
#include "src/client/request_closure.h"
#include "src/client/request_context.h"

// TODO(tongguangxun) :优化重试逻辑，将重试逻辑与RPC返回逻辑拆开
namespace curve {
namespace client {
FailureRequestOption_t  ClientClosure::failReqOpt_;
ChunkserverClientMetric_t ClientClosure::chunkserverClientMetric_;

void WriteChunkClosure::Run() {
    std::unique_ptr<WriteChunkClosure> selfGuard(this);
    std::unique_ptr<brpc::Controller> cntlGuard(cntl_);
    std::unique_ptr<ChunkResponse> responseGuard(response_);
    brpc::ClosureGuard doneGuard(done_);

    MetaCache *metaCache = client_->GetMetaCache();
    RequestClosure *reqDone = dynamic_cast<RequestClosure *>(done_);
    RequestContext *reqCtx = reqDone->GetReqCtx();
    LogicPoolID logicPoolId = reqCtx->idinfo_.lpid_;
    CopysetID copysetId = reqCtx->idinfo_.cpid_;
    ChunkID chunkid = reqCtx->idinfo_.cid_;

    int status = -1;
    ChunkServerID leaderId;
    butil::EndPoint leaderAddr;

    if (cntl_->Failed()) {
        /* 如果连接失败，再等一定时间再重试 */
        status = cntl_->ErrorCode();
        LOG(ERROR) << "write failed, error code: " << cntl_->ErrorCode()
                   << ", error: " << cntl_->ErrorText();
        /* It will be invoked in brpc's bthread, so */
        metaCache->UpdateAppliedIndex(logicPoolId,
                                      copysetId,
                                      0);
        /**
         * 考虑到leader可能挂了，所以会尝试去获取新leader，保证client端
         * 能够自动切换到新的leader上面
         */
        if (-1 == metaCache->GetLeader(logicPoolId,
                                       copysetId,
                                       &leaderId,
                                       &leaderAddr,
                                       true)) {
            bthread_usleep(failReqOpt_.opRetryIntervalUs);
        } else {
            chunkserverClientMetric_.leaderChangeTimes << 1;
        }
        goto write_retry;
    }

    /* 1. write success，返回成功 */
    status = response_->status();
    if (CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS == status) {
        reqDone->SetFailed(0);
        metaCache->UpdateAppliedIndex(logicPoolId,
                                      copysetId,
                                      response_->appliedindex());
        return;
    }
    /* 2. 处理chunkserver返回的错误 */
    /* 2.1.不是 leader */
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
                chunkserverClientMetric_.leaderChangeTimes << 1;
                goto write_retry;
            }
        }
        if (-1 == metaCache->GetLeader(logicPoolId,
                                       copysetId,
                                       &leaderId,
                                       &leaderAddr,
                                       true)) {
            bthread_usleep(failReqOpt_.opRetryIntervalUs);
        } else {
            chunkserverClientMetric_.leaderChangeTimes << 1;
        }
        goto write_retry;
    }
    /* 2.2.Copyset不存在,大概率都是配置变更了 */
    if (CHUNK_OP_STATUS::CHUNK_OP_STATUS_COPYSET_NOTEXIST == status) {
        if (-1 == metaCache->GetLeader(logicPoolId,
                                       copysetId,
                                       &leaderId,
                                       &leaderAddr,
                                       true)) {
            bthread_usleep(failReqOpt_.opRetryIntervalUs);
        } else {
            chunkserverClientMetric_.leaderChangeTimes << 1;
        }
        goto write_retry;
    }
    /* 2.3.非法参数 */
    if (CHUNK_OP_STATUS::CHUNK_OP_STATUS_INVALID_REQUEST == status) {
        reqDone->SetFailed(status);
        LOG(ERROR) << "write failed for invalid format, write info: "
                   << "<" << logicPoolId << ", " << copysetId
                   << ", " << chunkid << "> offset=" << reqCtx->offset_
                   << ", length=" << reqCtx->rawlength_
                   << ", status=" << status;
        return;
    }
    /* 2.4.其他错误，过一段时间再重试 */
    LOG(ERROR) << "write failed for UNKNOWN reason, write info: "
               << "<" << logicPoolId << ", " << copysetId
               << ", " << chunkid << "> offset=" << reqCtx->offset_
               << ", length=" << reqCtx->rawlength_
               << ", status=" << status;
    bthread_usleep(failReqOpt_.opRetryIntervalUs);
    goto write_retry;

write_retry:
    if (unsigned(retriedTimes_ + 1) >= failReqOpt_.opMaxRetry) {
        reqDone->SetFailed(status);
        metaCache->UpdateAppliedIndex(logicPoolId,
                                      copysetId,
                                      0);
        LOG(ERROR) << "retried times exceeds";
        return;
    }
    chunkserverClientMetric_.retryCount << 1;
    chunkserverClientMetric_.retryBytes << reqCtx->rawlength_;
    client_->WriteChunk(reqCtx->idinfo_,
                        reqCtx->seq_,
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

    MetaCache *metaCache = client_->GetMetaCache();
    RequestClosure *reqDone = dynamic_cast<RequestClosure *>(done_);
    RequestContext *reqCtx = reqDone->GetReqCtx();
    LogicPoolID logicPoolId = reqCtx->idinfo_.lpid_;
    CopysetID copysetId = reqCtx->idinfo_.cpid_;
    ChunkID chunkid = reqCtx->idinfo_.cid_;

    int status = -1;
    ChunkServerID leaderId;
    butil::EndPoint leaderAddr;

    if (cntl_->Failed()) {
        /* 如果连接失败，再等一定时间再重试*/
        status = cntl_->ErrorCode();
        LOG(ERROR) << "read failed, error: " << cntl_->ErrorText();
        /**
         * 考虑到 leader 可能挂了，所以会尝试去获取新 leader，保证 client 端
         * 能够自动切换到新的 leader 上面
         */
        if (-1 == metaCache->GetLeader(logicPoolId,
                                       copysetId,
                                       &leaderId,
                                       &leaderAddr,
                                       true)) {
            bthread_usleep(failReqOpt_.opRetryIntervalUs);
        } else {
            chunkserverClientMetric_.leaderChangeTimes << 1;
        }
        goto read_retry;
    }

    /* 1.read success，返回成功 */
    status = response_->status();
    if (CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS == status) {
        reqDone->SetFailed(0);
        ::memcpy(const_cast<void *>(reinterpret_cast<const void *>(reqCtx->data_)),  //NOLINT
                 cntl_->response_attachment().to_string().c_str(),
                 cntl_->response_attachment().size());

        metaCache->UpdateAppliedIndex(logicPoolId,
                                      copysetId,
                                      response_->appliedindex());
        return;
    }
    /* 2.处理chunkserver返回的错误 */
    /* 2.1.不是leader */
    if (CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED == status) {
        if (response_->has_redirect()) {
            std::string redirect = response_->redirect();
            braft::PeerId leader;
            if (0 == leader.parse(redirect)) {
                metaCache->UpdateLeader(logicPoolId,
                                        copysetId,
                                        &leaderId,
                                        leader.addr);
                chunkserverClientMetric_.leaderChangeTimes << 1;
                goto read_retry;
            }
        }
        if (-1 == metaCache->GetLeader(logicPoolId,
                                       copysetId,
                                       &leaderId,
                                       &leaderAddr,
                                       true)) {
            bthread_usleep(failReqOpt_.opRetryIntervalUs);
        } else {
            chunkserverClientMetric_.leaderChangeTimes << 1;
        }

        goto read_retry;
    }
    /* 2.2.Copyset不存在大概率都是配置变更了 */
    if (CHUNK_OP_STATUS::CHUNK_OP_STATUS_COPYSET_NOTEXIST == status) {
        if (-1 == metaCache->GetLeader(logicPoolId,
                                       copysetId,
                                       &leaderId,
                                       &leaderAddr,
                                       true)) {
            bthread_usleep(failReqOpt_.opRetryIntervalUs);
        } else {
            chunkserverClientMetric_.leaderChangeTimes << 1;
        }
        goto read_retry;
    }
    /* 2.3.非法参数 */
    if (CHUNK_OP_STATUS::CHUNK_OP_STATUS_INVALID_REQUEST == status) {
        reqDone->SetFailed(status);
        LOG(ERROR) << "read failed for invalid format, read info: "
                   << "<" << logicPoolId << ", " << copysetId
                   << ", " << chunkid << "> offset=" << reqCtx->offset_
                   << ", length=" << reqCtx->rawlength_
                   << ", status=" << status;
        return;
    }
    /* 2.4.chunk not exist */
    if (CHUNK_OP_STATUS::CHUNK_OP_STATUS_CHUNK_NOTEXIST == status) {
        reqDone->SetFailed(0);
        ::memset(const_cast<void *>(reinterpret_cast<const void *>(reqCtx->data_)),  //NOLINT
                 0,
                 reqCtx->rawlength_);
        metaCache->UpdateAppliedIndex(logicPoolId,
                                      copysetId,
                                      response_->appliedindex());
        return;
    }
    /* 2.5.其他错误，过一段时间再重试 */
    LOG(ERROR) << "read failed for UNKNOWN reason, read info: "
               << "<" << logicPoolId << ", " << copysetId
               << ", " << chunkid << "> offset=" << reqCtx->offset_
               << ", length=" << reqCtx->rawlength_
               << ", status=" << status;
    bthread_usleep(failReqOpt_.opRetryIntervalUs);
    goto read_retry;

read_retry:
    if (unsigned(retriedTimes_ + 1) >= failReqOpt_.opMaxRetry) {
        reqDone->SetFailed(status);
        LOG(ERROR) << "retried times exceeds";
        return;
    }

    chunkserverClientMetric_.retryCount << 1;
    client_->ReadChunk(reqCtx->idinfo_,
                       reqCtx->seq_,
                       reqCtx->offset_,
                       reqCtx->rawlength_,
                       reqCtx->appliedindex_,
                       doneGuard.release(),
                       retriedTimes_ + 1);
}

void ReadChunkSnapClosure::Run() {
    std::unique_ptr<ReadChunkSnapClosure> selfGuard(this);
    std::unique_ptr<brpc::Controller> cntlGuard(cntl_);
    std::unique_ptr<ChunkResponse> responseGuard(response_);
    brpc::ClosureGuard doneGuard(done_);

    MetaCache *metaCache = client_->GetMetaCache();
    RequestClosure *reqDone = dynamic_cast<RequestClosure *>(done_);
    RequestContext *reqCtx = reqDone->GetReqCtx();
    LogicPoolID logicPoolId = reqCtx->idinfo_.lpid_;
    CopysetID copysetId = reqCtx->idinfo_.cpid_;
    ChunkID chunkid = reqCtx->idinfo_.cid_;

    int status = -1;
    ChunkServerID leaderId;
    butil::EndPoint leaderAddr;

    if (cntl_->Failed()) {
        /* 如果连接失败，再等一定时间再重试 */
        status = cntl_->ErrorCode();
        LOG(ERROR) << "read snapshot failed, error: " << cntl_->ErrorText();
        /**
         * 考虑到 leader 可能挂了，所以会尝试去获取新 leader，保证 client 端
         * 能够自动切换到新的 leader 上面
         */
        if (-1 == metaCache->GetLeader(logicPoolId,
                                       copysetId,
                                       &leaderId,
                                       &leaderAddr,
                                       true)) {
            bthread_usleep(failReqOpt_.opRetryIntervalUs);
        } else {
            chunkserverClientMetric_.leaderChangeTimes << 1;
        }
        goto read_retry;
    }

    /* 1.read snapshot success，返回成功 */
    status = response_->status();
    if (CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS == status) {
        reqDone->SetFailed(0);
        ::memcpy(const_cast<void *>(reinterpret_cast<const void *>(reqCtx->data_)),  //NOLINT
                 cntl_->response_attachment().to_string().c_str(),
                 cntl_->response_attachment().size());
        return;
    }
    /* 2. 处理chunkserver返回的错误 */
    /* 2.1.不是 leader */
    if (CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED == status) {
        if (response_->has_redirect()) {
            std::string redirect = response_->redirect();
            braft::PeerId leader;
            if (0 == leader.parse(redirect)) {
                metaCache->UpdateLeader(logicPoolId,
                                        copysetId,
                                        &leaderId,
                                        leader.addr);
                chunkserverClientMetric_.leaderChangeTimes << 1;
                goto read_retry;
            }
        }
        if (-1 == metaCache->GetLeader(logicPoolId,
                                       copysetId,
                                       &leaderId,
                                       &leaderAddr,
                                       true)) {
            bthread_usleep(failReqOpt_.opRetryIntervalUs);
        } else {
            chunkserverClientMetric_.leaderChangeTimes << 1;
        }

        goto read_retry;
    }
    /* 2.2.Copyset不存在大概率都是配置变更了 */
    if (CHUNK_OP_STATUS::CHUNK_OP_STATUS_COPYSET_NOTEXIST == status) {
        if (-1 == metaCache->GetLeader(logicPoolId,
                                       copysetId,
                                       &leaderId,
                                       &leaderAddr,
                                       true)) {
            bthread_usleep(failReqOpt_.opRetryIntervalUs);
        } else {
            chunkserverClientMetric_.leaderChangeTimes << 1;
        }
        goto read_retry;
    }
    /* 2.3.非法参数 */
    if (CHUNK_OP_STATUS::CHUNK_OP_STATUS_INVALID_REQUEST == status) {
        reqDone->SetFailed(status);
        LOG(ERROR) << "read snapshot failed for invalid request, read info: "
                   << "<" << logicPoolId << ", " << copysetId
                   << ">, " << chunkid
                   << ", sn=" << reqCtx->seq_
                   << ", offset=" << reqCtx->offset_
                   << ", length=" << reqCtx->rawlength_
                   << ", status=" << status;
        return;
    }

    /* 2.4.chunk snapshot不存在，直接返回错误码 */
    if (CHUNK_OP_STATUS::CHUNK_OP_STATUS_CHUNK_NOTEXIST == status) {
        reqDone->SetFailed(status);
        LOG(ERROR) << "read snapshot not exist, write info: "
                   << "<" << logicPoolId << ", " << copysetId
                   << ">, " << chunkid
                   << ", sn=" << reqCtx->seq_
                   << ", offset=" << reqCtx->offset_
                   << ", length=" << reqCtx->rawlength_
                   << ", status=" << status;
        return;
    }

    /* 2.5.其他错误，过一段时间再重试 */
    LOG(ERROR) << "read snapshot failed for UNKNOWN reason, read info: "
               << "<" << logicPoolId << ", " << copysetId
               << ">, " << chunkid
               << ", sn=" << reqCtx->seq_
               << ", offset=" << reqCtx->offset_
               << ", length=" << reqCtx->rawlength_
               << ", status=" << status;
    bthread_usleep(failReqOpt_.opRetryIntervalUs);
    goto read_retry;

read_retry:
    if (unsigned(retriedTimes_ + 1) >= failReqOpt_.opMaxRetry) {
        reqDone->SetFailed(status);
        LOG(ERROR) << "read snapshot retried times exceeds";
        return;
    }

    chunkserverClientMetric_.retryCount << 1;
    client_->ReadChunkSnapshot(reqCtx->idinfo_,
                               reqCtx->seq_,
                               reqCtx->offset_,
                               reqCtx->rawlength_,
                               doneGuard.release(),
                               retriedTimes_ + 1);
}

void DeleteChunkSnapClosure::Run() {
    std::unique_ptr<DeleteChunkSnapClosure> selfGuard(this);
    std::unique_ptr<brpc::Controller> cntlGuard(cntl_);
    std::unique_ptr<ChunkResponse> responseGuard(response_);
    brpc::ClosureGuard doneGuard(done_);

    MetaCache *metaCache = client_->GetMetaCache();
    RequestClosure *reqDone = dynamic_cast<RequestClosure *>(done_);
    RequestContext *reqCtx = reqDone->GetReqCtx();
    LogicPoolID logicPoolId = reqCtx->idinfo_.lpid_;
    CopysetID copysetId = reqCtx->idinfo_.cpid_;
    ChunkID chunkid = reqCtx->idinfo_.cid_;

    int status = -1;
    ChunkServerID leaderId;
    butil::EndPoint leaderAddr;

    if (cntl_->Failed()) {
        /* 如果连接失败，再等一定时间再重试 */
        status = cntl_->ErrorCode();
        LOG(ERROR) << "delete snapshot failed, error: " << cntl_->ErrorText();
        /**
         * 考虑到 leader 可能挂了，所以会尝试去获取新 leader，保证 client 端
         * 能够自动切换到新的 leader 上面
         */
        if (-1 == metaCache->GetLeader(logicPoolId,
                                       copysetId,
                                       &leaderId,
                                       &leaderAddr,
                                       true)) {
            bthread_usleep(failReqOpt_.opRetryIntervalUs);
        } else {
            chunkserverClientMetric_.leaderChangeTimes << 1;
        }
        goto delete_retry;
    }

    /* 1.delete snapshot success，返回成功 */
    status = response_->status();
    if (CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS == status) {
        reqDone->SetFailed(0);
        return;
    }
    /* 2.处理chunkserver返回的错误 */
    /* 2.1.不是 leader */
    if (CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED == status) {
        if (response_->has_redirect()) {
            std::string redirect = response_->redirect();
            braft::PeerId leader;
            if (0 == leader.parse(redirect)) {
                metaCache->UpdateLeader(logicPoolId,
                                        copysetId,
                                        &leaderId,
                                        leader.addr);
                chunkserverClientMetric_.leaderChangeTimes << 1;
                goto delete_retry;
            }
        }
        if (-1 == metaCache->GetLeader(logicPoolId,
                                       copysetId,
                                       &leaderId,
                                       &leaderAddr,
                                       true)) {
            bthread_usleep(failReqOpt_.opRetryIntervalUs);
        } else {
            chunkserverClientMetric_.leaderChangeTimes << 1;
        }

        goto delete_retry;
    }
    /* 2.2.Copyset不存在大概率都是配置变更了 */
    if (CHUNK_OP_STATUS::CHUNK_OP_STATUS_COPYSET_NOTEXIST == status) {
        if (-1 == metaCache->GetLeader(logicPoolId,
                                       copysetId,
                                       &leaderId,
                                       &leaderAddr,
                                       true)) {
            bthread_usleep(failReqOpt_.opRetryIntervalUs);
        } else {
            chunkserverClientMetric_.leaderChangeTimes << 1;
        }
        goto delete_retry;
    }
    /* 2.3.非法参数 */
    if (CHUNK_OP_STATUS::CHUNK_OP_STATUS_INVALID_REQUEST == status) {
        reqDone->SetFailed(status);
        LOG(ERROR) << "delete snapshot failed for invalid format, write info: "
                   << "<" << logicPoolId << ", " << copysetId
                   << ">, " << chunkid
                   << ", sn=" << reqCtx->seq_
                   << ", status=" << status;
        return;
    }
    /* 2.4.其他错误，过一段时间再重试 */
    LOG(ERROR) << "delete snapshot failed for UNKNOWN reason, read info: "
               << "<" << logicPoolId << ", " << copysetId
               << ">, " << chunkid
               << ", sn=" << reqCtx->seq_
               << ", status=" << status;
    bthread_usleep(failReqOpt_.opRetryIntervalUs);
    goto delete_retry;

delete_retry:
    if (unsigned(retriedTimes_ + 1) >= failReqOpt_.opMaxRetry) {
        reqDone->SetFailed(status);
        LOG(ERROR) << "delete snapshot retried times exceeds";
        return;
    }

    chunkserverClientMetric_.retryCount << 1;
    client_->DeleteChunkSnapshotOrCorrectSn(reqCtx->idinfo_,
                                            reqCtx->correctedSeq_,
                                            doneGuard.release(),
                                            retriedTimes_ + 1);
}

void GetChunkInfoClosure::Run() {
    std::unique_ptr<GetChunkInfoClosure> selfGuard(this);
    std::unique_ptr<brpc::Controller> cntlGuard(cntl_);
    std::unique_ptr<GetChunkInfoResponse> responseGuard(chunkinforesponse_);
    brpc::ClosureGuard doneGuard(done_);

    MetaCache *metaCache = client_->GetMetaCache();
    RequestClosure *reqDone = dynamic_cast<RequestClosure *>(done_);
    RequestContext *reqCtx = reqDone->GetReqCtx();
    LogicPoolID logicPoolId = reqCtx->idinfo_.lpid_;
    CopysetID copysetId = reqCtx->idinfo_.cpid_;
    ChunkID chunkid = reqCtx->idinfo_.cid_;

    int status = -1;
    ChunkServerID leaderId;
    butil::EndPoint leaderAddr;

    if (cntl_->Failed()) {
        /* 如果连接失败，再等一定时间再重试 */
        status = cntl_->ErrorCode();
        LOG(ERROR) << "get chunk info failed, error: " << cntl_->ErrorText();
        /**
         * 考虑到 leader 可能挂了，所以会尝试去获取新 leader，保证 client 端
         * 能够自动切换到新的 leader 上面
         */
        if (-1 == metaCache->GetLeader(logicPoolId,
                                       copysetId,
                                       &leaderId,
                                       &leaderAddr,
                                       true)) {
            bthread_usleep(failReqOpt_.opRetryIntervalUs);
        } else {
            chunkserverClientMetric_.leaderChangeTimes << 1;
        }
        goto get_retry;
    }

    /* 1.get chunk info success，返回成功 */
    status = chunkinforesponse_->status();
    if (CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS == status) {
        reqDone->SetFailed(0);
        for (unsigned int i = 0; i < chunkinforesponse_->chunksn_size(); ++i) {
            reqCtx->chunkinfodetail_->chunkSn.push_back(
                                        chunkinforesponse_->chunksn(i));
        }
        return;
    }
    /* 2.处理chunkserver返回的错误 */
    /* 2.1.不是 leader */
    if (CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED == status) {
        if (chunkinforesponse_->has_redirect()) {
            std::string redirect = chunkinforesponse_->redirect();
            braft::PeerId leader;
            if (0 == leader.parse(redirect)) {
                metaCache->UpdateLeader(logicPoolId,
                                        copysetId,
                                        &leaderId,
                                        leader.addr);
                chunkserverClientMetric_.leaderChangeTimes << 1;
                goto get_retry;
            }
        }
        if (-1 == metaCache->GetLeader(logicPoolId,
                                       copysetId,
                                       &leaderId,
                                       &leaderAddr,
                                       true)) {
            bthread_usleep(failReqOpt_.opRetryIntervalUs);
        } else {
            chunkserverClientMetric_.leaderChangeTimes << 1;
        }

        goto get_retry;
    }
    /* 2.2.Copyset不存在大概率都是配置变更了 */
    if (CHUNK_OP_STATUS::CHUNK_OP_STATUS_COPYSET_NOTEXIST == status) {
        if (-1 == metaCache->GetLeader(logicPoolId,
                                       copysetId,
                                       &leaderId,
                                       &leaderAddr,
                                       true)) {
            bthread_usleep(failReqOpt_.opRetryIntervalUs);
        } else {
            chunkserverClientMetric_.leaderChangeTimes << 1;
        }
        goto get_retry;
    }
    /* 2.3.非法参数 */
    if (CHUNK_OP_STATUS::CHUNK_OP_STATUS_INVALID_REQUEST == status) {
        reqDone->SetFailed(status);
        LOG(ERROR) << "get chunk info failed for invalid format, write info: "
                   << "<" << logicPoolId << ", " << copysetId
                   << ">, " << chunkid
                   << ", status=" << status;
        return;
    }
    /* 2.4.其他错误，过一段时间再重试 */
    LOG(ERROR) << "get chunk info failed for UNKNOWN reason, read info: "
               << "<" << logicPoolId << ", " << copysetId
               << ">, " << chunkid
               << ", status=" << status;
    bthread_usleep(failReqOpt_.opRetryIntervalUs);
    goto get_retry;

get_retry:
    if (unsigned(retriedTimes_ + 1) >= failReqOpt_.opMaxRetry) {
        reqDone->SetFailed(status);
        LOG(ERROR) << "get chunk info retried times exceeds";
        return;
    }

    chunkserverClientMetric_.retryCount << 1;
    client_->GetChunkInfo(reqCtx->idinfo_,
                          doneGuard.release(),
                          retriedTimes_ + 1);
}

void CreateCloneChunkClosure::Run() {
    std::unique_ptr<CreateCloneChunkClosure> selfGuard(this);
    std::unique_ptr<brpc::Controller> cntlGuard(cntl_);
    std::unique_ptr<ChunkResponse> responseGuard(response_);
    brpc::ClosureGuard doneGuard(done_);

    MetaCache *metaCache = client_->GetMetaCache();
    RequestClosure *reqDone = dynamic_cast<RequestClosure *>(done_);
    RequestContext *reqCtx = reqDone->GetReqCtx();
    LogicPoolID logicPoolId = reqCtx->idinfo_.lpid_;
    CopysetID copysetId = reqCtx->idinfo_.cpid_;
    ChunkID chunkid = reqCtx->idinfo_.cid_;

    int status = -1;
    ChunkServerID leaderId;
    butil::EndPoint leaderAddr;

    if (cntl_->Failed()) {
        /* 如果连接失败，再等一定时间再重试 */
        status = cntl_->ErrorCode();
        LOG(ERROR) << "create clone failed, error: " << cntl_->ErrorText();
        /**
         * 考虑到 leader 可能挂了，所以会尝试去获取新 leader，保证 client 端
         * 能够自动切换到新的 leader 上面
         */
        if (-1 == metaCache->GetLeader(logicPoolId,
                                       copysetId,
                                       &leaderId,
                                       &leaderAddr,
                                       true)) {
            bthread_usleep(failReqOpt_.opRetryIntervalUs);
        } else {
            chunkserverClientMetric_.leaderChangeTimes << 1;
        }
        goto create_retry;
    }

    /* 1.create clone chunk success，返回成功 */
    status = response_->status();
    if (CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS == status) {
        reqDone->SetFailed(0);
        return;
    }
    /* 2.处理chunkserver返回的错误 */
    /* 2.1.不是 leader */
    if (CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED == status) {
        if (response_->has_redirect()) {
            std::string redirect = response_->redirect();
            braft::PeerId leader;
            if (0 == leader.parse(redirect)) {
                metaCache->UpdateLeader(logicPoolId,
                                        copysetId,
                                        &leaderId,
                                        leader.addr);
                chunkserverClientMetric_.leaderChangeTimes << 1;
                goto create_retry;
            }
        }
        if (-1 == metaCache->GetLeader(logicPoolId,
                                       copysetId,
                                       &leaderId,
                                       &leaderAddr,
                                       true)) {
            bthread_usleep(failReqOpt_.opRetryIntervalUs);
        } else {
            chunkserverClientMetric_.leaderChangeTimes << 1;
        }

        goto create_retry;
    }
    /* 2.2.Copyset不存在大概率都是配置变更了 */
    if (CHUNK_OP_STATUS::CHUNK_OP_STATUS_COPYSET_NOTEXIST == status) {
        if (-1 == metaCache->GetLeader(logicPoolId,
                                       copysetId,
                                       &leaderId,
                                       &leaderAddr,
                                       true)) {
            bthread_usleep(failReqOpt_.opRetryIntervalUs);
        } else {
            chunkserverClientMetric_.leaderChangeTimes << 1;
        }
        goto create_retry;
    }
    /* 2.3.非法参数 */
    if (CHUNK_OP_STATUS::CHUNK_OP_STATUS_INVALID_REQUEST == status) {
        reqDone->SetFailed(status);
        LOG(ERROR) << "create clone failed for invalid format, write info: "
                   << "<" << logicPoolId << ", " << copysetId
                   << ">, " << chunkid
                   << ", sn=" << reqCtx->seq_
                   << ", status=" << status;
        return;
    }
    /* 2.4.其他错误，过一段时间再重试 */
    LOG(ERROR) << "create clone failed for UNKNOWN reason, read info: "
               << "<" << logicPoolId << ", " << copysetId
               << ">, " << chunkid
               << ", sn=" << reqCtx->seq_
               << ", status=" << status;
    bthread_usleep(failReqOpt_.opRetryIntervalUs);
    goto create_retry;

create_retry:
    if (unsigned(retriedTimes_ + 1) >= failReqOpt_.opMaxRetry) {
        reqDone->SetFailed(status);
        LOG(ERROR) << "create clone retried times exceeds";
        return;
    }

    chunkserverClientMetric_.retryCount << 1;
    client_->CreateCloneChunk(reqCtx->idinfo_,
                            reqCtx->location_,
                            reqCtx->seq_,
                            reqCtx->correctedSeq_,
                            reqCtx->chunksize_,
                            doneGuard.release(),
                            retriedTimes_ + 1);
}

void RecoverChunkClosure::Run() {
    std::unique_ptr<RecoverChunkClosure> selfGuard(this);
    std::unique_ptr<brpc::Controller> cntlGuard(cntl_);
    std::unique_ptr<ChunkResponse> responseGuard(response_);
    brpc::ClosureGuard doneGuard(done_);

    MetaCache *metaCache = client_->GetMetaCache();
    RequestClosure *reqDone = dynamic_cast<RequestClosure *>(done_);
    RequestContext *reqCtx = reqDone->GetReqCtx();
    LogicPoolID logicPoolId = reqCtx->idinfo_.lpid_;
    CopysetID copysetId = reqCtx->idinfo_.cpid_;
    ChunkID chunkid = reqCtx->idinfo_.cid_;

    int status = -1;
    ChunkServerID leaderId;
    butil::EndPoint leaderAddr;

    if (cntl_->Failed()) {
        /* 如果连接失败，再等一定时间再重试 */
        status = cntl_->ErrorCode();
        LOG(ERROR) << "recover chunk failed, error: " << cntl_->ErrorText();
        /**
         * 考虑到 leader 可能挂了，所以会尝试去获取新 leader，保证 client 端
         * 能够自动切换到新的 leader 上面
         */
        if (-1 == metaCache->GetLeader(logicPoolId,
                                       copysetId,
                                       &leaderId,
                                       &leaderAddr,
                                       true)) {
            bthread_usleep(failReqOpt_.opRetryIntervalUs);
        } else {
            chunkserverClientMetric_.leaderChangeTimes << 1;
        }
        goto recover_retry;
    }

    /* 1.recover chunk success，返回成功 */
    status = response_->status();
    if (CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS == status) {
        reqDone->SetFailed(0);
        return;
    }
    /* 2.处理chunkserver返回的错误 */
    /* 2.1.不是 leader */
    if (CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED == status) {
        if (response_->has_redirect()) {
            std::string redirect = response_->redirect();
            braft::PeerId leader;
            if (0 == leader.parse(redirect)) {
                metaCache->UpdateLeader(logicPoolId,
                                        copysetId,
                                        &leaderId,
                                        leader.addr);
                chunkserverClientMetric_.leaderChangeTimes << 1;
                goto recover_retry;
            }
        }
        if (-1 == metaCache->GetLeader(logicPoolId,
                                       copysetId,
                                       &leaderId,
                                       &leaderAddr,
                                       true)) {
            bthread_usleep(failReqOpt_.opRetryIntervalUs);
        } else {
            chunkserverClientMetric_.leaderChangeTimes << 1;
        }

        goto recover_retry;
    }
    /* 2.2.Copyset不存在大概率都是配置变更了 */
    if (CHUNK_OP_STATUS::CHUNK_OP_STATUS_COPYSET_NOTEXIST == status) {
        if (-1 == metaCache->GetLeader(logicPoolId,
                                       copysetId,
                                       &leaderId,
                                       &leaderAddr,
                                       true)) {
            bthread_usleep(failReqOpt_.opRetryIntervalUs);
        } else {
            chunkserverClientMetric_.leaderChangeTimes << 1;
        }
        goto recover_retry;
    }
    /* 2.3.非法参数 */
    if (CHUNK_OP_STATUS::CHUNK_OP_STATUS_INVALID_REQUEST == status) {
        reqDone->SetFailed(status);
        LOG(ERROR) << "recover chunk failed for invalid format, write info: "
                   << "<" << logicPoolId << ", " << copysetId
                   << ">, " << chunkid
                   << ", sn=" << reqCtx->seq_
                   << ", status=" << status;
        return;
    }
    /* 2.4.其他错误，过一段时间再重试 */
    LOG(ERROR) << "recover chunk failed for UNKNOWN reason, read info: "
               << "<" << logicPoolId << ", " << copysetId
               << ">, " << chunkid
               << ", sn=" << reqCtx->seq_
               << ", status=" << status;
    bthread_usleep(failReqOpt_.opRetryIntervalUs);
    goto recover_retry;

recover_retry:
    if (unsigned(retriedTimes_ + 1) >= failReqOpt_.opMaxRetry) {
        reqDone->SetFailed(status);
        LOG(ERROR) << "recover chunk retried times exceeds";
        return;
    }
    chunkserverClientMetric_.retryCount << 1;
    client_->RecoverChunk(reqCtx->idinfo_,
                        reqCtx->offset_,
                        reqCtx->rawlength_,
                        doneGuard.release(),
                        retriedTimes_ + 1);
}
}   // namespace client
}   // namespace curve
