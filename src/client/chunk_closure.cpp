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

// 如果chunkserver宕机或者网络不可达, 发往对应chunkserver的rpc会超时
// 返回之后, 回去refresh leader然后再去发送请求
// 这种情况下不同copyset上的请求，总会先rpc timedout然后重新refresh leader
// 为了避免一次多余的rpc timedout
// 记录一下发往同一个chunkserver上超时请求的次数
// 如果超过一定的阈值，则通知所有leader在这台chunkserver上的copyset
// 主动去refresh leader，而不是根据缓存的leader信息直接发送rpc
uint64_t UnstableChunkServerHelper::maxStableChunkServerTimeoutTimes_ = 0;
SpinLock UnstableChunkServerHelper::timeoutLock_;
std::unordered_map<ChunkServerID, uint64_t> UnstableChunkServerHelper::timeoutTimes_;  // NOLINT

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
            // 如果RPC超时, 对应的chunkserver超时请求次数+1
            UnstableChunkServerHelper::IncreTimeout(chunkserverID_);
            MetricHelper::IncremTimeOutRPCCount(fm, OpType::WRITE);
        }

        LOG_EVERY_N(ERROR, 10) << "write failed, error code: "
                   << cntl_->ErrorCode()
                   << ", error: " << cntl_->ErrorText()
                   << ", chunk id = " << chunkid
                   << ", copyset id = " << copysetId
                   << ", logicpool id = " << logicPoolId;
        /* It will be invoked in brpc's bthread, so */
        metaCache->UpdateAppliedIndex(logicPoolId,
                                      copysetId,
                                      0);
        if (UnstableChunkServerHelper::IsTimeoutExceed(chunkserverID_)) {
            // chunkserver上RPC超时次数超过上限(由于宕机或网络原因导致RPC超时)
            // 此时发往这一台chunkserver的其余RPC大概率也会超时
            // 为了避免无效的超时请求和重试, 将这台shunkserver标记为unstable
            // unstable chunkserver上leader所在的copyset会标记为leaderMayChange
            // 当这些copyset再有IO请求时会先进行refresh leader
            // 避免一次无效的RPC请求
            metaCache->SetChunkserverUnstable(chunkserverID_);
        } else {
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
        }

        goto write_retry;
    }

    // 只要正常rpc返回，就清空超时计数
    UnstableChunkServerHelper::ClearTimeout(chunkserverID_);

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
    LOG_EVERY_N(ERROR, 10) << "write failed, write info: "
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
            UnstableChunkServerHelper::IncreTimeout(chunkserverID_);
            MetricHelper::IncremTimeOutRPCCount(fm, OpType::READ);
        }

        LOG_EVERY_N(ERROR, 10) << "read failed, error: "
                   << cntl_->ErrorText()
                   << ", chunk id = " << chunkid
                   << ", copyset id = " << copysetId
                   << ", logicpool id = " << logicPoolId;

        if (UnstableChunkServerHelper::IsTimeoutExceed(chunkserverID_)) {
            metaCache->SetChunkserverUnstable(chunkserverID_);
        } else {
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
        }

        goto read_retry;
    }

    // 只要正常rpc返回，就清空超时计数
    UnstableChunkServerHelper::ClearTimeout(chunkserverID_);

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
        LOG(WARNING) << "read failed for invalid format, read info: "
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
    LOG_EVERY_N(ERROR, 10) << "read failed , read info: "
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
        LOG(WARNING) << "read snapshot failed, error: " << cntl_->ErrorText()
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
    LOG(WARNING) << "read snapshot failed for UNKNOWN reason, read info: "
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
        LOG(WARNING) << "delete snapshot failed, error: " << cntl_->ErrorText()
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
    LOG(WARNING) << "delete snapshot failed for UNKNOWN reason, read info: "
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
        LOG(WARNING) << "get chunk info failed, error: " << cntl_->ErrorText()
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
    LOG(WARNING) << "get chunk info failed for UNKNOWN reason, read info: "
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
        LOG(WARNING) << "create clone failed, error: " << cntl_->ErrorText()
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
        LOG(WARNING) << "recover chunk failed, error: " << cntl_->ErrorText()
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
    LOG(WARNING) << "recover chunk failed for UNKNOWN reason, read info: "
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
