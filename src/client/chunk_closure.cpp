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
#include "src/client/io_tracker.h"

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

    // 如果对应的cooysetId leader可能发生变更
    // 那么设置这次重试请求超时时间为默认值
    // 这是为了尽快重试这次请求
    // 从copysetleader迁移到client GetLeader获取到新的leader会有1~2s的延迟
    // 对于一个请求来说，GetLeader仍然可能返回旧的Leader
    // rpc timeout时间可能会被设置成2s/4s，等到超时后再去获取leader信息
    // 为了尽快在新的Leader上重试请求，将rpc timeout时间设置为默认值
    if (cntlstatus == brpc::ERPCTIMEDOUT || cntlstatus == ETIMEDOUT) {
        uint64_t nextTimeout = 0;
        uint64_t retriedTimes = reqDone->GetRetriedTimes();
        bool leaderMayChange = metaCache_->IsLeaderMayChange(
            logicPoolId, copysetId);

        // 当某一个IO重试超过一定次数后，超时时间一定进行指数退避
        // 当底层chunkserver压力大时，可能也会触发unstable
        // 由于copyset leader may change，会导致请求超时时间设置为默认值
        // 而chunkserver在这个时间内处理不了，导致IO hang
        // 真正宕机的情况下，请求重试一定次数后会处理完成
        // 如果一直重试，则不是宕机情况，这时候超时时间还是要进入指数退避逻辑
        if (retriedTimes < failReqOpt_.chunkserverMinRetryTimesForceTimeoutBackoff &&  // NOLINT
            leaderMayChange) {
            nextTimeout = failReqOpt_.chunkserverRPCTimeoutMS;
        } else {
            nextTimeout = TimeoutBackOff(retriedTimes);
        }

        reqDone->SetNextTimeOutMS(nextTimeout);
        LOG(WARNING) << "rpc timeout, next timeout = " << nextTimeout
                  << ", " << *reqCtx
                  << ", reteied times = " << reqDone->GetRetriedTimes()
                  << ", IO id = " << reqDone->GetIOTracker()->GetID()
                  << ", request id = " << reqCtx->id_;
        return;
    }

    if (rpcstatus == CHUNK_OP_STATUS::CHUNK_OP_STATUS_OVERLOAD) {
        uint64_t nextsleeptime = OverLoadBackOff(reqDone->GetRetriedTimes());
        LOG(WARNING) << "chunkserver overload, sleep(us) = " << nextsleeptime
                  << ", " << *reqCtx
                  << ", reteied times = " << reqDone->GetRetriedTimes()
                  << ", IO id = " << reqDone->GetIOTracker()->GetID()
                  << ", request id = " << reqCtx->id_;
        bthread_usleep(nextsleeptime);
        return;
    } else if (rpcstatus == CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED) {
        LOG(WARNING) << "leader redirect, retry directly, " << *reqCtx
                  << ", reteied times = " << reqDone->GetRetriedTimes()
                  << ", IO id = " << reqDone->GetIOTracker()->GetID()
                  << ", request id = " << reqCtx->id_;
        return;
    }

    LOG(WARNING) << "rpc failed, sleep(us) = "
                << failReqOpt_.chunkserverOPRetryIntervalUS << ", " << *reqCtx
                << ", cntl status = " << cntlstatus
                << ", response status = " << rpcstatus
                << ", reteied times = " << reqDone->GetRetriedTimes()
                << ", IO id = " << reqDone->GetIOTracker()->GetID()
                << ", request id = " << reqCtx->id_;
    bthread_usleep(failReqOpt_.chunkserverOPRetryIntervalUS);
}

uint64_t ClientClosure::OverLoadBackOff(uint64_t currentRetryTimes) {
    uint64_t curpowTime = std::min(currentRetryTimes,
                          backoffParam_.maxOverloadPow);

    uint64_t nextsleeptime = failReqOpt_.chunkserverOPRetryIntervalUS
                           * std::pow(2, curpowTime);

    int random_time = std::rand() % (nextsleeptime/5 + 1);
    random_time -= nextsleeptime/10;
    nextsleeptime += random_time;

    nextsleeptime = std::min(nextsleeptime, failReqOpt_.chunkserverMaxRetrySleepIntervalUS);   // NOLINT
    nextsleeptime = std::max(nextsleeptime, failReqOpt_.chunkserverOPRetryIntervalUS);   // NOLINT

    return nextsleeptime;
}

uint64_t ClientClosure::TimeoutBackOff(uint64_t currentRetryTimes) {
    uint64_t curpowTime = std::min(currentRetryTimes,
                          backoffParam_.maxTimeoutPow);

    uint64_t nextTimeout = failReqOpt_.chunkserverRPCTimeoutMS
                         * std::pow(2, curpowTime);
    nextTimeout = std::min(nextTimeout, failReqOpt_.chunkserverMaxRPCTimeoutMS);   // NOLINT
    nextTimeout = std::max(nextTimeout, failReqOpt_.chunkserverRPCTimeoutMS);

    return nextTimeout;
}

// 统一请求回调函数入口
// 整体处理逻辑与之前相同
// 针对不同的请求类型和返回状态码，进行相应的处理
// 各子类需要实现SendRetryRequest，进行重试请求
void ClientClosure::Run() {
    std::unique_ptr<ClientClosure> selfGuard(this);
    std::unique_ptr<brpc::Controller> cntlGuard(cntl_);
    brpc::ClosureGuard doneGuard(done_);

    metaCache_ = client_->GetMetaCache();
    reqDone_ = dynamic_cast<RequestClosure*>(done_);
    fileMetric_ = reqDone_->GetMetric();
    reqCtx_ = reqDone_->GetReqCtx();
    chunkIdInfo_ = reqCtx_->idinfo_;
    status_ = -1;
    cntlstatus_ = cntl_->ErrorCode();
    remoteAddress_ = butil::endpoint2str(cntl_->remote_side()).c_str();

    bool needRetry = false;

    if (cntl_->Failed()) {
        needRetry = true;
        OnRpcFailed();
    } else {
        // 只要rpc正常返回，就清空超时计数器
        UnstableChunkServerHelper::ClearTimeout(chunkserverID_);

        status_ = GetResponseStatus();

        switch (status_) {
        // 1. 请求成功
        case CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS:
            OnSuccess();
            break;

        // 2.1 不是leader
        case CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED:
            needRetry = true;
            OnRedirected();
            break;

        // 2.2 Copyset不存在，大概率都是配置变更了
        case CHUNK_OP_STATUS::CHUNK_OP_STATUS_COPYSET_NOTEXIST:
            needRetry = true;
            OnCopysetNotExist();
            break;

        // 2.3 chunk not exist，直接返回，不用重试
        case CHUNK_OP_STATUS::CHUNK_OP_STATUS_CHUNK_NOTEXIST:
            OnChunkNotExist();
            break;

        // 2.4 非法参数，直接返回，不用重试
        case CHUNK_OP_STATUS::CHUNK_OP_STATUS_INVALID_REQUEST:
            OnInvalidRequest();
            break;

        default:
            needRetry = true;
            LOG_EVERY_N(ERROR, 10) << OpTypeToString(reqCtx_->optype_)
                << " failed for UNKNOWN reason, " << *reqCtx_
                << ", status="
                << curve::chunkserver::CHUNK_OP_STATUS_Name(
                        static_cast<CHUNK_OP_STATUS>(status_))
                << ", IO id = " << reqDone_->GetIOTracker()->GetID()
                << ", request id = " << reqCtx_->id_
                << ", remoet side = " << remoteAddress_;
        }
    }

    if (needRetry) {
        doneGuard.release();
        OnRetry();
    }
}

void ClientClosure::OnRpcFailed() {
    status_ = cntl_->ErrorCode();

    // 如果连接失败，再等一定时间再重试
    if (cntlstatus_ == brpc::ERPCTIMEDOUT) {
        // 如果RPC超时, 对应的chunkserver超时请求次数+1
        UnstableChunkServerHelper::IncreTimeout(chunkserverID_);
        MetricHelper::IncremTimeOutRPCCount(fileMetric_, reqCtx_->optype_);
    }

    LOG_EVERY_N(ERROR, 10) << OpTypeToString(reqCtx_->optype_)
        << " failed, error code: "
        << cntl_->ErrorCode()
        << ", error: " << cntl_->ErrorText()
        << ", " << *reqCtx_
        << ", IO id = " << reqDone_->GetIOTracker()->GetID()
        << ", request id = " << reqCtx_->id_
        << ", remoet side = " << remoteAddress_;


    // it will be invoked in brpc's bthread
    if (reqCtx_->optype_ == OpType::WRITE) {
        metaCache_->UpdateAppliedIndex(
            chunkIdInfo_.lpid_, chunkIdInfo_.cpid_, 0);
    }

    if (UnstableChunkServerHelper::IsTimeoutExceed(chunkserverID_)) {
        // chunkserver上RPC超时次数超过上限(由于宕机或网络原因导致RPC超时)
        // 此时发往这一台chunkserver的其余RPC大概率也会超时
        // 为了避免无效的超时请求和重试, 将这台shunkserver标记为unstable
        // unstable chunkserver上leader所在的copyset会标记为leaderMayChange  // NOLINT
        // 当这些copyset再有IO请求时会先进行refresh leader
        // 避免一次无效的RPC请求
        metaCache_->SetChunkserverUnstable(chunkserverID_);
    } else {
        RefreshLeader();
    }
}

void ClientClosure::OnSuccess() {
    reqDone_->SetFailed(0);

    auto duration = TimeUtility::GetTimeofDayUs() - reqDone_->GetStartTime();
    MetricHelper::LatencyRecord(fileMetric_, duration, reqCtx_->optype_);
    MetricHelper::IncremRPCQPSCount(
        fileMetric_, reqCtx_->rawlength_, reqCtx_->optype_);
}

void ClientClosure::OnChunkNotExist() {
    reqDone_->SetFailed(status_);

    LOG(WARNING) << OpTypeToString(reqCtx_->optype_)
        << " not exists, " << *reqCtx_
        << ", status=" << status_
        << ", IO id = " << reqDone_->GetIOTracker()->GetID()
        << ", request id = " << reqCtx_->id_
        << ", remoet side = " << remoteAddress_;

    auto duration = TimeUtility::GetTimeofDayUs() - reqDone_->GetStartTime();
    MetricHelper::LatencyRecord(fileMetric_, duration, reqCtx_->optype_);
    MetricHelper::IncremRPCQPSCount(
        fileMetric_, reqCtx_->rawlength_, reqCtx_->optype_);
}

void ClientClosure::OnRedirected() {
    LOG(WARNING) << OpTypeToString(reqCtx_->optype_) << " redirected, "
        << *reqCtx_
        << ", status = " << status_
        << ", IO id = " << reqDone_->GetIOTracker()->GetID()
        << ", request id = " << reqCtx_->id_
        << ", redirect leader is "
        << (response_->has_redirect() ? response_->redirect() : "empty")
        << ", remoet side = " << remoteAddress_;

    ChunkServerID leaderId;
    butil::EndPoint leaderAddr;

    if (response_->has_redirect()) {
        braft::PeerId leader;
        int ret = leader.parse(response_->redirect());
        if (0 == ret) {
            metaCache_->UpdateLeader(chunkIdInfo_.lpid_, chunkIdInfo_.cpid_,
                                    &leaderId, leader.addr);
            return;
        }
    }

    RefreshLeader();
}

void ClientClosure::OnCopysetNotExist() {
    LOG(WARNING) << OpTypeToString(reqCtx_->optype_) << " copyset not exists, "
        << *reqCtx_
        << ", status = " << status_
        << ", IO id = " << reqDone_->GetIOTracker()->GetID()
        << ", request id = " << reqCtx_->id_
        << ", remoet side = " << remoteAddress_;

    RefreshLeader();
}

void ClientClosure::OnRetry() {
    MetricHelper::IncremFailRPCCount(fileMetric_, reqCtx_->optype_);

    if (reqDone_->GetRetriedTimes() >= failReqOpt_.chunkserverOPMaxRetry) {
        reqDone_->SetFailed(status_);
        LOG(ERROR) << OpTypeToString(reqCtx_->optype_)
                   << " retried times exceeds"
                   << ", IO id = " << reqDone_->GetIOTracker()->GetID()
                   << ", request id = " << reqCtx_->id_;
        done_->Run();
        return;
    }

    if (!reqDone_->IsSuspendRPC() && reqDone_->GetRetriedTimes() >=
        failReqOpt_.chunkserverMaxRetryTimesBeforeConsiderSuspend) {
        reqDone_->SetSuspendRPCFlag();
        MetricHelper::IncremIOSuspendNum(fileMetric_);
        LOG(WARNING) << "IO Retried "
                    << failReqOpt_.chunkserverMaxRetryTimesBeforeConsiderSuspend
                    << " times, set suspend flag! " << *reqCtx_
                    << ", IO id = " << reqDone_->GetIOTracker()->GetID()
                    << ", request id = " << reqCtx_->id_;
    }

    PreProcessBeforeRetry(status_, cntlstatus_);
    SendRetryRequest();
}

void ClientClosure::RefreshLeader() const {
    ChunkServerID leaderId;
    butil::EndPoint leaderAddr;

    if (-1 == metaCache_->GetLeader(
            chunkIdInfo_.lpid_, chunkIdInfo_.cpid_,
            &leaderId, &leaderAddr, true, fileMetric_)) {
        LOG(WARNING) << "Refresh leader failed, "
            << "logicpool id = " << chunkIdInfo_.lpid_
            << ", copyset id = " << chunkIdInfo_.cpid_
            << ", current op return status = "
            << status_
            << ", IO id = " << reqDone_->GetIOTracker()->GetID()
            << ", request id = " << reqCtx_->id_;
    }
}

void ClientClosure::OnInvalidRequest() {
    reqDone_->SetFailed(status_);
    LOG(ERROR) << OpTypeToString(reqCtx_->optype_)
        << " failed for invalid format, " << *reqCtx_
        << ", status=" << status_
        << ", IO id = " << reqDone_->GetIOTracker()->GetID()
        << ", request id = " << reqCtx_->id_
        << ", remoet side = " << remoteAddress_;
    MetricHelper::IncremFailRPCCount(fileMetric_, reqCtx_->optype_);
}

void WriteChunkClosure::SendRetryRequest() {
    client_->WriteChunk(reqCtx_->idinfo_, reqCtx_->seq_,
                        reqCtx_->writeBuffer_,
                        reqCtx_->offset_,
                        reqCtx_->rawlength_,
                        done_);
}

void WriteChunkClosure::OnSuccess() {
    ClientClosure::OnSuccess();

    metaCache_->UpdateAppliedIndex(
        chunkIdInfo_.lpid_,
        chunkIdInfo_.cpid_,
        response_->appliedindex());
}

void ReadChunkClosure::SendRetryRequest() {
    client_->ReadChunk(reqCtx_->idinfo_, reqCtx_->seq_,
                       reqCtx_->offset_,
                       reqCtx_->rawlength_,
                       reqCtx_->appliedindex_,
                       done_);
}

void ReadChunkClosure::OnSuccess() {
    ClientClosure::OnSuccess();

    memcpy(reqCtx_->readBuffer_,
           cntl_->response_attachment().to_string().c_str(),
           cntl_->response_attachment().size());

    metaCache_->UpdateAppliedIndex(
        reqCtx_->idinfo_.lpid_,
        reqCtx_->idinfo_.cpid_,
        response_->appliedindex());
}

void ReadChunkClosure::OnChunkNotExist() {
    ClientClosure::OnChunkNotExist();

    reqDone_->SetFailed(0);
    memset(reqCtx_->readBuffer_, 0, reqCtx_->rawlength_);
    metaCache_->UpdateAppliedIndex(chunkIdInfo_.lpid_, chunkIdInfo_.cpid_,
                                   response_->appliedindex());
}

void ReadChunkSnapClosure::SendRetryRequest() {
    client_->ReadChunkSnapshot(reqCtx_->idinfo_, reqCtx_->seq_,
                               reqCtx_->offset_,
                               reqCtx_->rawlength_,
                               done_);
}

void ReadChunkSnapClosure::OnSuccess() {
    ClientClosure::OnSuccess();

    memcpy(reqCtx_->readBuffer_,
           cntl_->response_attachment().to_string().c_str(),
           cntl_->response_attachment().size());
}

void DeleteChunkSnapClosure::SendRetryRequest() {
    client_->DeleteChunkSnapshotOrCorrectSn(
        reqCtx_->idinfo_,
        reqCtx_->correctedSeq_,
        done_);
}

void GetChunkInfoClosure::SendRetryRequest() {
    client_->GetChunkInfo(reqCtx_->idinfo_, done_);
}

void GetChunkInfoClosure::OnSuccess() {
    ClientClosure::OnSuccess();

    for (int i = 0; i < chunkinforesponse_->chunksn_size(); ++i) {
        reqCtx_->chunkinfodetail_->chunkSn.push_back(
            chunkinforesponse_->chunksn(i));
    }
}

void GetChunkInfoClosure::OnRedirected() {
    LOG(WARNING) << OpTypeToString(reqCtx_->optype_)
        << " redirected, " << *reqCtx_
        << ", status = " << status_
        << ", IO id = " << reqDone_->GetIOTracker()->GetID()
        << ", request id = " << reqCtx_->id_
        << ", redirect leader is "
        << (chunkinforesponse_->has_redirect() ? chunkinforesponse_->redirect()
                                               : "empty")
        << ", remoet side = " << remoteAddress_;

    ChunkServerID leaderId;
    butil::EndPoint leaderAddr;

    if (chunkinforesponse_->has_redirect()) {
        braft::PeerId leader;
        int ret = leader.parse(chunkinforesponse_->redirect());
        if (0 == ret) {
            metaCache_->UpdateLeader(chunkIdInfo_.lpid_, chunkIdInfo_.cpid_,
                                     &leaderId, leader.addr);
            return;
        }
    }

    RefreshLeader();
}

void CreateCloneChunkClosure::SendRetryRequest() {
    client_->CreateCloneChunk(reqCtx_->idinfo_,
                              reqCtx_->location_,
                              reqCtx_->seq_,
                              reqCtx_->correctedSeq_,
                              reqCtx_->chunksize_,
                              done_);
}

void RecoverChunkClosure::SendRetryRequest() {
    client_->RecoverChunk(reqCtx_->idinfo_,
                          reqCtx_->offset_,
                          reqCtx_->rawlength_,
                          done_);
}

}   // namespace client
}   // namespace curve
