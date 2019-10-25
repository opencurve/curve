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

#include <cmath>
#include <string>
#include <memory>
#include <cstdlib>
#include <algorithm>

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
ClientClosure::BackoffParam  ClientClosure::backoffParam_;
FailureRequestOption_t  ClientClosure::failReqOpt_;
void ClientClosure::PreProcessBeforeRetry(int rpcstatus, int cntlstatus) {
    RequestClosure *reqDone = dynamic_cast<RequestClosure *>(done_);

    RequestContext *reqCtx = reqDone->GetReqCtx();
    LogicPoolID logicPoolId = reqCtx->idinfo_.lpid_;
    CopysetID copysetId = reqCtx->idinfo_.cpid_;
    ChunkID chunkid = reqCtx->idinfo_.cid_;

    if (cntlstatus == brpc::ERPCTIMEDOUT) {
        uint64_t nextTimeout = TimeoutBackOff(reqDone->GetRetriedTimes());
        reqDone->SetNextTimeOutMS(nextTimeout);
        LOG(INFO) << "rpc timeout, next timeout = " << nextTimeout
                  << ", copysetid = " << copysetId
                  << ", logicPoolId = " << logicPoolId
                  << ", chunkid = " << chunkid
                  << ", offset = " << reqCtx->offset_
                  << ", reteied times = " << reqDone->GetRetriedTimes();
        return;
    } else if (rpcstatus == CHUNK_OP_STATUS::CHUNK_OP_STATUS_OVERLOAD) {
        uint64_t nextsleeptime = OverLoadBackOff(reqDone->GetRetriedTimes());
        LOG(INFO) << "chunkserver overload, sleep(us) = " << nextsleeptime
                  << ", copysetid = " << copysetId
                  << ", logicPoolId = " << logicPoolId
                  << ", chunkid = " << chunkid
                  << ", offset = " << reqCtx->offset_
                  << ", reteied times = " << reqDone->GetRetriedTimes();
        bthread_usleep(nextsleeptime);
        return;
    } else {
        LOG(INFO) << "rpc failed, sleep(us) = " << failReqOpt_.opRetryIntervalUs
                    << ", copysetid = " << copysetId
                    << ", logicPoolId = " << logicPoolId
                    << ", chunkid = " << chunkid
                    << ", offset = " << reqCtx->offset_
                    << ", reteied times = " << reqDone->GetRetriedTimes();
        bthread_usleep(failReqOpt_.opRetryIntervalUs);
    }
}

uint64_t ClientClosure::OverLoadBackOff(uint64_t currentRetryTimes) {
    uint64_t curpowTime = std::min(currentRetryTimes,
                          backoffParam_.maxOverloadPow);

    uint64_t nextsleeptime = failReqOpt_.opRetryIntervalUs
                           * std::pow(2, curpowTime);

    int random_time = std::rand() % (nextsleeptime/5 + 1);
    random_time -= nextsleeptime/10;
    nextsleeptime += random_time;

    nextsleeptime = std::min(nextsleeptime, failReqOpt_.maxRetrySleepIntervalUs);   // NOLINT
    nextsleeptime = std::max(nextsleeptime, failReqOpt_.opRetryIntervalUs);

    return nextsleeptime;
}

uint64_t ClientClosure::TimeoutBackOff(uint64_t currentRetryTimes) {
    uint64_t curpowTime = std::min(currentRetryTimes,
                          backoffParam_.maxTimeoutPow);

    uint64_t nextTimeout = failReqOpt_.rpcTimeoutMs
                         * std::pow(2, curpowTime);

    nextTimeout = std::min(nextTimeout, failReqOpt_.maxTimeoutMS);
    nextTimeout = std::max(nextTimeout, failReqOpt_.rpcTimeoutMs);

    return nextTimeout;
}

void WriteChunkClosure::Run() {
    std::unique_ptr<WriteChunkClosure> selfGuard(this);
    std::unique_ptr<brpc::Controller> cntlGuard(cntl_);
    std::unique_ptr<ChunkResponse> responseGuard(response_);
    brpc::ClosureGuard doneGuard(done_);

    MetaCache *metaCache = client_->GetMetaCache();
    RequestClosure *reqDone = dynamic_cast<RequestClosure *>(done_);
    FileMetric_t* fm = reqDone->GetMetric();
    RequestContext *reqCtx = reqDone->GetReqCtx();
    LogicPoolID logicPoolId = reqCtx->idinfo_.lpid_;
    CopysetID copysetId = reqCtx->idinfo_.cpid_;
    ChunkID chunkid = reqCtx->idinfo_.cid_;

    int status = -1;
    ChunkServerID leaderId;
    butil::EndPoint leaderAddr;

    int cntlstatus = cntl_->ErrorCode();

    if (cntl_->Failed()) {
        status = cntl_->ErrorCode();
        /* 如果连接失败，再等一定时间再重试 */
        if (cntlstatus == brpc::ERPCTIMEDOUT) {
            MetricHelper::IncremTimeOutRPCCount(fm, OpType::WRITE);
        }

        LOG(ERROR) << "write failed, error code: " << cntl_->ErrorCode()
                   << ", error: " << cntl_->ErrorText()
                   << ", chunk id = " << chunkid
                   << ", copyset id = " << copysetId
                   << ", logicpool id = " << logicPoolId;
        /* It will be invoked in brpc's bthread, so */
        metaCache->UpdateAppliedIndex(logicPoolId,
                                      copysetId,
                                      0);

        if (-1 == metaCache->GetLeader(logicPoolId,
                                    copysetId,
                                    &leaderId,
                                    &leaderAddr,
                                    true,
                                    fm)) {
            LOG(WARNING) << "Refresh leader failed, "
                        << "copyset id = " << copysetId
                        << ", logicPoolId = " << logicPoolId
                        << ", currrent op return status = " << cntlstatus;
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

        uint64_t duration = TimeUtility::GetTimeofDayUs()
                          - reqDone->GetStartTime();
        MetricHelper::LatencyRecord(fm, duration, OpType::WRITE);
        MetricHelper::IncremRPCQPSCount(fm, reqCtx->rawlength_, OpType::WRITE);
        return;
    }
    /* 2. 处理chunkserver返回的错误 */
    /* 2.1.不是 leader */
    // 当chunkserver返回redirected的时候，这个chunkserver是正常工作状态的
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
                                       true,
                                       fm)) {
            LOG(WARNING) << "Refresh leader failed, "
                         << "copyset id = " << copysetId
                         << ", logicPoolId = " << logicPoolId
                         << ", currrent op return status = " << status;
        }
        goto write_retry;
    }
    /* 2.2.Copyset不存在,大概率都是配置变更了 */
    if (CHUNK_OP_STATUS::CHUNK_OP_STATUS_COPYSET_NOTEXIST == status) {
        if (-1 == metaCache->GetLeader(logicPoolId,
                                       copysetId,
                                       &leaderId,
                                       &leaderAddr,
                                       true,
                                       fm)) {
            LOG(WARNING) << "Refresh leader failed, "
                         << "copyset id = " << copysetId
                         << ", logicPoolId = " << logicPoolId
                         << ", currrent op return status = " << status;
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
        MetricHelper::IncremFailRPCCount(fm, OpType::WRITE);
        return;
    }
    /* 2.4.其他错误，过一段时间再重试 */
    LOG(ERROR) << "write failed, write info: "
               << "<" << logicPoolId << ", " << copysetId
               << ", " << chunkid << "> offset=" << reqCtx->offset_
               << ", length =" << reqCtx->rawlength_
               << ", status ="
               << curve::chunkserver::CHUNK_OP_STATUS_Name(
                   static_cast<CHUNK_OP_STATUS>(status));
    goto write_retry;

write_retry:
    MetricHelper::IncremFailRPCCount(fm, OpType::WRITE);
    if (reqDone->GetRetriedTimes() >= failReqOpt_.opMaxRetry) {
        reqDone->SetFailed(status);
        metaCache->UpdateAppliedIndex(logicPoolId,
                                      copysetId,
                                      0);
        LOG(ERROR) << "retried times exceeds";
        return;
    }

    PreProcessBeforeRetry(status, cntlstatus);
    client_->WriteChunk(reqCtx->idinfo_,
                        reqCtx->seq_,
                        reqCtx->writeBuffer_,
                        reqCtx->offset_,
                        reqCtx->rawlength_,
                        doneGuard.release());
}

void ReadChunkClosure::Run() {
    std::unique_ptr<ReadChunkClosure> selfGuard(this);
    std::unique_ptr<brpc::Controller> cntlGuard(cntl_);
    std::unique_ptr<ChunkResponse> responseGuard(response_);
    brpc::ClosureGuard doneGuard(done_);

    MetaCache *metaCache = client_->GetMetaCache();
    RequestClosure *reqDone = dynamic_cast<RequestClosure *>(done_);
    FileMetric_t* fm = reqDone->GetMetric();
    RequestContext *reqCtx = reqDone->GetReqCtx();
    LogicPoolID logicPoolId = reqCtx->idinfo_.lpid_;
    CopysetID copysetId = reqCtx->idinfo_.cpid_;
    ChunkID chunkid = reqCtx->idinfo_.cid_;

    int status = -1;
    ChunkServerID leaderId;
    butil::EndPoint leaderAddr;

    int cntlstatus = cntl_->ErrorCode();

    if (cntl_->Failed()) {
        /* 如果连接失败，再等一定时间再重试*/
        status = cntl_->ErrorCode();
        if (status == brpc::ERPCTIMEDOUT) {
            MetricHelper::IncremTimeOutRPCCount(fm, OpType::READ);
        }

        LOG(ERROR) << "read failed, error: " << cntl_->ErrorText()
                   << ", chunk id = " << chunkid
                   << ", copyset id = " << copysetId
                   << ", logicpool id = " << logicPoolId;

        if (-1 == metaCache->GetLeader(logicPoolId,
                                       copysetId,
                                       &leaderId,
                                       &leaderAddr,
                                       true,
                                       fm)) {
                LOG(WARNING) << "Refresh leader failed, "
                            << "copyset id = " << copysetId
                            << ", logicPoolId = " << logicPoolId
                            << ", currrent op return status = " << status;
        }

        goto read_retry;
    }

    /* 1.read success，返回成功 */
    status = response_->status();
    if (CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS == status) {
        reqDone->SetFailed(0);
        ::memcpy(reqCtx->readBuffer_,
                 cntl_->response_attachment().to_string().c_str(),
                 cntl_->response_attachment().size());

        metaCache->UpdateAppliedIndex(logicPoolId,
                                      copysetId,
                                      response_->appliedindex());

        uint64_t duration = TimeUtility::GetTimeofDayUs()
                          - reqDone->GetStartTime();
        MetricHelper::LatencyRecord(fm, duration, OpType::READ);
        MetricHelper::IncremRPCQPSCount(fm, reqCtx->rawlength_, OpType::READ);
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
                goto read_retry;
            }
        }
        if (-1 == metaCache->GetLeader(logicPoolId,
                                       copysetId,
                                       &leaderId,
                                       &leaderAddr,
                                       true,
                                       fm)) {
            LOG(WARNING) << "Refresh leader failed, "
                         << "copyset id = " << copysetId
                         << ", logicPoolId = " << logicPoolId
                         << ", currrent op return status = " << status;
        }
        goto read_retry;
    }
    /* 2.2.Copyset不存在大概率都是配置变更了 */
    if (CHUNK_OP_STATUS::CHUNK_OP_STATUS_COPYSET_NOTEXIST == status) {
        if (-1 == metaCache->GetLeader(logicPoolId,
                                       copysetId,
                                       &leaderId,
                                       &leaderAddr,
                                       true,
                                       fm)) {
            LOG(WARNING) << "Refresh leader failed, "
                         << "copyset id = " << copysetId
                         << ", logicPoolId = " << logicPoolId
                         << ", currrent op return status = " << status;
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
        MetricHelper::IncremFailRPCCount(fm, OpType::READ);
        return;
    }
    /* 2.4.chunk not exist */
    if (CHUNK_OP_STATUS::CHUNK_OP_STATUS_CHUNK_NOTEXIST == status) {
        reqDone->SetFailed(0);
        ::memset(reqCtx->readBuffer_, 0, reqCtx->rawlength_);
        metaCache->UpdateAppliedIndex(logicPoolId, copysetId,
                                      response_->appliedindex());
        uint64_t duration = TimeUtility::GetTimeofDayUs()
                          - reqDone->GetStartTime();
        MetricHelper::LatencyRecord(fm, duration, OpType::READ);
        MetricHelper::IncremRPCQPSCount(fm, reqCtx->rawlength_, OpType::READ);
        return;
    }
    /* 2.5.其他错误，过一段时间再重试 */
    LOG(ERROR) << "read failed , read info: "
               << "<" << logicPoolId << ", " << copysetId
               << ", " << chunkid << "> offset=" << reqCtx->offset_
               << ", length=" << reqCtx->rawlength_
               << ", status="
               << curve::chunkserver::CHUNK_OP_STATUS_Name(
                   static_cast<CHUNK_OP_STATUS>(status));
    goto read_retry;

read_retry:
    MetricHelper::IncremFailRPCCount(fm, OpType::READ);

    if (reqDone->GetRetriedTimes() >= failReqOpt_.opMaxRetry) {
        reqDone->SetFailed(status);
        LOG(ERROR) << "retried times exceeds";
        return;
    }

    PreProcessBeforeRetry(status, cntlstatus);
    client_->ReadChunk(reqCtx->idinfo_,
                       reqCtx->seq_,
                       reqCtx->offset_,
                       reqCtx->rawlength_,
                       reqCtx->appliedindex_,
                       doneGuard.release());
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
        LOG(ERROR) << "read snapshot failed, error: " << cntl_->ErrorText()
                   << ", chunk id = " << chunkid
                   << ", copyset id = " << copysetId
                   << ", logicpool id = " << logicPoolId;
        /**
         * 考虑到 leader 可能挂了，所以会尝试去获取新 leader，保证 client 端
         * 能够自动切换到新的 leader 上面
         */
        if (-1 == metaCache->GetLeader(logicPoolId,
                                       copysetId,
                                       &leaderId,
                                       &leaderAddr,
                                       true)) {
            LOG(WARNING) << "Refresh leader failed, "
                         << "copyset id = " << copysetId
                         << ", logicPoolId = " << logicPoolId
                         << ", currrent op return status = " << status;
        }
        goto read_retry;
    }

    /* 1.read snapshot success，返回成功 */
    status = response_->status();
    if (CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS == status) {
        reqDone->SetFailed(0);
        ::memcpy(reqCtx->readBuffer_,
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
                goto read_retry;
            }
        }
        if (-1 == metaCache->GetLeader(logicPoolId,
                                       copysetId,
                                       &leaderId,
                                       &leaderAddr,
                                       true)) {
            LOG(WARNING) << "Refresh leader failed, "
                         << "copyset id = " << copysetId
                         << ", logicPoolId = " << logicPoolId
                         << ", currrent op return status = " << status;
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
            LOG(WARNING) << "Refresh leader failed, "
                         << "copyset id = " << copysetId
                         << ", logicPoolId = " << logicPoolId
                         << ", currrent op return status = " << status;
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
    goto read_retry;

read_retry:
    if (unsigned(retriedTimes_ + 1) >= failReqOpt_.opMaxRetry) {
        reqDone->SetFailed(status);
        LOG(ERROR) << "read snapshot retried times exceeds";
        return;
    }

    // 先睡眠，再重试
    bthread_usleep(failReqOpt_.opRetryIntervalUs);
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
        LOG(ERROR) << "delete snapshot failed, error: " << cntl_->ErrorText()
                   << ", chunk id = " << chunkid
                   << ", copyset id = " << copysetId
                   << ", logicpool id = " << logicPoolId;
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
                goto delete_retry;
            }
        }
        if (-1 == metaCache->GetLeader(logicPoolId,
                                       copysetId,
                                       &leaderId,
                                       &leaderAddr,
                                       true)) {
            bthread_usleep(failReqOpt_.opRetryIntervalUs);
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
        LOG(ERROR) << "get chunk info failed, error: " << cntl_->ErrorText()
                   << ", chunk id = " << chunkid
                   << ", copyset id = " << copysetId
                   << ", logicpool id = " << logicPoolId;
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
                goto get_retry;
            }
        }
        if (-1 == metaCache->GetLeader(logicPoolId,
                                       copysetId,
                                       &leaderId,
                                       &leaderAddr,
                                       true)) {
            bthread_usleep(failReqOpt_.opRetryIntervalUs);
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
        LOG(ERROR) << "create clone failed, error: " << cntl_->ErrorText()
                   << ", chunk id = " << chunkid
                   << ", copyset id = " << copysetId
                   << ", logicpool id = " << logicPoolId;
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
                goto create_retry;
            }
        }
        if (-1 == metaCache->GetLeader(logicPoolId,
                                       copysetId,
                                       &leaderId,
                                       &leaderAddr,
                                       true)) {
            bthread_usleep(failReqOpt_.opRetryIntervalUs);
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
        LOG(ERROR) << "recover chunk failed, error: " << cntl_->ErrorText()
                   << ", chunk id = " << chunkid
                   << ", copyset id = " << copysetId
                   << ", logicpool id = " << logicPoolId;
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
                goto recover_retry;
            }
        }
        if (-1 == metaCache->GetLeader(logicPoolId,
                                       copysetId,
                                       &leaderId,
                                       &leaderAddr,
                                       true)) {
            bthread_usleep(failReqOpt_.opRetryIntervalUs);
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
    client_->RecoverChunk(reqCtx->idinfo_,
                        reqCtx->offset_,
                        reqCtx->rawlength_,
                        doneGuard.release(),
                        retriedTimes_ + 1);
}
}   // namespace client
}   // namespace curve
