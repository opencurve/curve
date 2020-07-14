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
 * Created Date: 18-9-28
 * Author: wudemiao
 */

#include "src/client/chunk_closure.h"

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

UnstableState UnstableHelper::GetCurrentUnstableState(
    ChunkServerID csId,
    const butil::EndPoint& csEndPoint) {

    std::string ip = butil::ip2str(csEndPoint.ip).c_str();

    lock_.Lock();
    // 如果当前ip已经超过阈值，则直接返回chunkserver unstable
    int unstabled = serverUnstabledChunkservers_[ip].size();
    if (unstabled >= option_.serverUnstableThreshold) {
        serverUnstabledChunkservers_[ip].emplace(csId);
        lock_.UnLock();
        return UnstableState::ChunkServerUnstable;
    }

    bool exceed =
        timeoutTimes_[csId] > option_.maxStableChunkServerTimeoutTimes;
    lock_.UnLock();

    if (exceed == false) {
        return UnstableState::NoUnstable;
    }

    bool health = CheckChunkServerHealth(csEndPoint);
    if (health) {
        ClearTimeout(csId, csEndPoint);
        return UnstableState::NoUnstable;
    }

    lock_.Lock();
    auto ret = serverUnstabledChunkservers_[ip].emplace(csId);
    unstabled = serverUnstabledChunkservers_[ip].size();
    lock_.UnLock();

    if (ret.second && unstabled == option_.serverUnstableThreshold) {
        return UnstableState::ServerUnstable;
    } else {
        return UnstableState::ChunkServerUnstable;
    }
}

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
                  << ", retried times = " << reqDone->GetRetriedTimes()
                  << ", IO id = " << reqDone->GetIOTracker()->GetID()
                  << ", request id = " << reqCtx->id_
                  << ", remote side = " << remoteAddress_;
        return;
    }

    if (rpcstatus == CHUNK_OP_STATUS::CHUNK_OP_STATUS_OVERLOAD) {
        uint64_t nextsleeptime = OverLoadBackOff(reqDone->GetRetriedTimes());
        LOG(WARNING) << "chunkserver overload, sleep(us) = " << nextsleeptime
                  << ", " << *reqCtx
                  << ", retried times = " << reqDone->GetRetriedTimes()
                  << ", IO id = " << reqDone->GetIOTracker()->GetID()
                  << ", request id = " << reqCtx->id_
                  << ", remote side = " << remoteAddress_;
        bthread_usleep(nextsleeptime);
        return;
    }

    auto nextSleepUS = 0;

    if (!retryDirectly_) {
        nextSleepUS = failReqOpt_.chunkserverOPRetryIntervalUS;
        if (rpcstatus == CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED) {
            nextSleepUS /= 10;
        }
    }

    LOG(WARNING)
        << "Rpc failed "
        << (retryDirectly_ ? "retry directly, "
                           : "sleep " + std::to_string(nextSleepUS) + " us, ")
        << *reqCtx << ", cntl status = " << cntlstatus
        << ", response status = "
        << curve::chunkserver::CHUNK_OP_STATUS_Name(
               static_cast<curve::chunkserver::CHUNK_OP_STATUS>(rpcstatus))
        << ", retried times = " << reqDone->GetRetriedTimes()
        << ", IO id = " << reqDone->GetIOTracker()->GetID()
        << ", request id = " << reqCtx->id_
        << ", remote side = " << remoteAddress_;

    if (nextSleepUS != 0) {
        bthread_usleep(nextSleepUS);
    }
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
        UnstableHelper::GetInstance().ClearTimeout(
            chunkserverID_, chunkserverEndPoint_);

        status_ = GetResponseStatus();

        switch (status_) {
        // 1. 请求成功
        case CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS:
            OnSuccess();
            break;

        // 2.1 不是leader
        case CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED:
            MetricHelper::IncremRedirectRPCCount(fileMetric_, reqCtx_->optype_);
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

        // 2.5 返回backward
        case CHUNK_OP_STATUS::CHUNK_OP_STATUS_BACKWARD:
            if (reqCtx_->optype_ == OpType::WRITE) {
                needRetry = true;
                OnBackward();
            } else {
                LOG(ERROR) << OpTypeToString(reqCtx_->optype_)
                    << " return backward, "
                    << *reqCtx_
                    << ", status=" << status_
                    << ", retried times = " << reqDone_->GetRetriedTimes()
                    << ", IO id = " << reqDone_->GetIOTracker()->GetID()
                    << ", request id = " << reqCtx_->id_
                    << ", remote side = " << remoteAddress_;
            }
            break;

        // 2.6 返回chunk exist，直接返回，不用重试
        case CHUNK_OP_STATUS::CHUNK_OP_STATUS_CHUNK_EXIST:
            OnChunkExist();
            break;

        default:
            needRetry = true;
            LOG_EVERY_N(ERROR, 10) << OpTypeToString(reqCtx_->optype_)
                << " failed for UNKNOWN reason, " << *reqCtx_
                << ", status="
                << curve::chunkserver::CHUNK_OP_STATUS_Name(
                        static_cast<CHUNK_OP_STATUS>(status_))
                << ", retried times = " << reqDone_->GetRetriedTimes()
                << ", IO id = " << reqDone_->GetIOTracker()->GetID()
                << ", request id = " << reqCtx_->id_
                << ", remote side = " << remoteAddress_;
        }
    }

    if (needRetry) {
        doneGuard.release();
        OnRetry();
    }
}

void ClientClosure::OnRpcFailed() {
    client_->ResetSenderIfNotHealth(chunkserverID_);

    status_ = cntl_->ErrorCode();

    // 如果连接失败，再等一定时间再重试
    if (cntlstatus_ == brpc::ERPCTIMEDOUT) {
        // 如果RPC超时, 对应的chunkserver超时请求次数+1
        UnstableHelper::GetInstance().IncreTimeout(chunkserverID_);
        MetricHelper::IncremTimeOutRPCCount(fileMetric_, reqCtx_->optype_);
    }

    LOG_EVERY_N(ERROR, 10) << OpTypeToString(reqCtx_->optype_)
        << " failed, error code: "
        << cntl_->ErrorCode()
        << ", error: " << cntl_->ErrorText()
        << ", " << *reqCtx_
        << ", retried times = " << reqDone_->GetRetriedTimes()
        << ", IO id = " << reqDone_->GetIOTracker()->GetID()
        << ", request id = " << reqCtx_->id_
        << ", remote side = " << remoteAddress_;

    // it will be invoked in brpc's bthread
    if (reqCtx_->optype_ == OpType::WRITE) {
        metaCache_->UpdateAppliedIndex(
            chunkIdInfo_.lpid_, chunkIdInfo_.cpid_, 0);
    }

    ProcessUnstableState();
}

void ClientClosure::ProcessUnstableState() {
    UnstableState state = UnstableHelper::GetInstance().GetCurrentUnstableState(
        chunkserverID_, chunkserverEndPoint_);

    switch (state) {
    case UnstableState::ServerUnstable: {
        std::string ip = butil::ip2str(chunkserverEndPoint_.ip).c_str();
        int ret = metaCache_->SetServerUnstable(ip);
        if (ret != 0) {
            LOG(WARNING) << "Set server(" << ip << ") unstable failed, "
                << "now set chunkserver(" << chunkserverID_ <<  ") unstable";
            metaCache_->SetChunkserverUnstable(chunkserverID_);
        }
        break;
    }
    case UnstableState::ChunkServerUnstable: {
        metaCache_->SetChunkserverUnstable(chunkserverID_);
        break;
    }
    case UnstableState::NoUnstable: {
        RefreshLeader();
        break;
    }
    default:
        break;
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
        << ", retried times = " << reqDone_->GetRetriedTimes()
        << ", IO id = " << reqDone_->GetIOTracker()->GetID()
        << ", request id = " << reqCtx_->id_
        << ", remote side = " << remoteAddress_;

    auto duration = TimeUtility::GetTimeofDayUs() - reqDone_->GetStartTime();
    MetricHelper::LatencyRecord(fileMetric_, duration, reqCtx_->optype_);
    MetricHelper::IncremRPCQPSCount(
        fileMetric_, reqCtx_->rawlength_, reqCtx_->optype_);
}

void ClientClosure::OnChunkExist() {
    reqDone_->SetFailed(status_);

    LOG(WARNING) << OpTypeToString(reqCtx_->optype_)
        << " exists, " << *reqCtx_
        << ", status=" << status_
        << ", retried times = " << reqDone_->GetRetriedTimes()
        << ", IO id = " << reqDone_->GetIOTracker()->GetID()
        << ", request id = " << reqCtx_->id_
        << ", remote side = " << remoteAddress_;
}

void ClientClosure::OnRedirected() {
    LOG(WARNING) << OpTypeToString(reqCtx_->optype_) << " redirected, "
        << *reqCtx_
        << ", status = " << status_
        << ", retried times = " << reqDone_->GetRetriedTimes()
        << ", IO id = " << reqDone_->GetIOTracker()->GetID()
        << ", request id = " << reqCtx_->id_
        << ", redirect leader is "
        << (response_->has_redirect() ? response_->redirect() : "empty")
        << ", remote side = " << remoteAddress_;

    if (response_->has_redirect()) {
        int ret = UpdateLeaderWithRedirectInfo(response_->redirect());
        if (ret == 0) {
            return;
        }
    }

    RefreshLeader();
}

void ClientClosure::OnCopysetNotExist() {
    LOG(WARNING) << OpTypeToString(reqCtx_->optype_) << " copyset not exists, "
        << *reqCtx_
        << ", status = " << status_
        << ", retried times = " << reqDone_->GetRetriedTimes()
        << ", IO id = " << reqDone_->GetIOTracker()->GetID()
        << ", request id = " << reqCtx_->id_
        << ", remote side = " << remoteAddress_;

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

void ClientClosure::RefreshLeader() {
    ChunkServerID leaderId = 0;
    butil::EndPoint leaderAddr;

    if (-1 == metaCache_->GetLeader(chunkIdInfo_.lpid_, chunkIdInfo_.cpid_,
                                    &leaderId, &leaderAddr, true,
                                    fileMetric_)) {
        LOG(WARNING) << "Refresh leader failed, "
                     << "logicpool id = " << chunkIdInfo_.lpid_
                     << ", copyset id = " << chunkIdInfo_.cpid_
                     << ", current op return status = " << status_
                     << ", IO id = " << reqDone_->GetIOTracker()->GetID()
                     << ", request id = " << reqCtx_->id_;
    } else {
        // 如果refresh leader获取到了新的leader信息
        // 则重试之前不进行睡眠
        retryDirectly_ = (leaderId != chunkserverID_);
    }
}

void ClientClosure::OnBackward() {
    const auto latestSn = metaCache_->GetLatestFileSn();
    LOG(WARNING) << OpTypeToString(reqCtx_->optype_)
        << " return BACKWARD, "
        << *reqCtx_
        << ", status = " << status_
        << ", retried times = " << reqDone_->GetRetriedTimes()
        << ", IO id = " << reqDone_->GetIOTracker()->GetID()
        << ", request id = " << reqCtx_->id_
        << ", remote side = " << remoteAddress_;

    reqCtx_->seq_ = latestSn;
}

void ClientClosure::OnInvalidRequest() {
    reqDone_->SetFailed(status_);
    LOG(ERROR) << OpTypeToString(reqCtx_->optype_)
        << " failed for invalid format, " << *reqCtx_
        << ", status=" << status_
        << ", retried times = " << reqDone_->GetRetriedTimes()
        << ", IO id = " << reqDone_->GetIOTracker()->GetID()
        << ", request id = " << reqCtx_->id_
        << ", remote side = " << remoteAddress_;
    MetricHelper::IncremFailRPCCount(fileMetric_, reqCtx_->optype_);
}

void WriteChunkClosure::SendRetryRequest() {
    client_->WriteChunk(reqCtx_->idinfo_, reqCtx_->seq_,
                        reqCtx_->writeBuffer_,
                        reqCtx_->offset_,
                        reqCtx_->rawlength_,
                        reqCtx_->sourceInfo_,
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
                       reqCtx_->sourceInfo_,
                       done_);
}

void ReadChunkClosure::OnSuccess() {
    ClientClosure::OnSuccess();

    cntl_->response_attachment().copy_to(
        reqCtx_->readBuffer_,
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

    cntl_->response_attachment().copy_to(
        reqCtx_->readBuffer_,
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
        << ", retried times = " << reqDone_->GetRetriedTimes()
        << ", IO id = " << reqDone_->GetIOTracker()->GetID()
        << ", request id = " << reqCtx_->id_
        << ", redirect leader is "
        << (chunkinforesponse_->has_redirect() ? chunkinforesponse_->redirect()
                                               : "empty")
        << ", remote side = " << remoteAddress_;

    if (chunkinforesponse_->has_redirect()) {
        int ret = UpdateLeaderWithRedirectInfo(chunkinforesponse_->redirect());
        if (0 == ret) {
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

int ClientClosure::UpdateLeaderWithRedirectInfo(const std::string& leaderInfo) {
    ChunkServerID leaderId = 0;
    ChunkServerAddr leaderAddr;

    int ret = leaderAddr.Parse(leaderInfo);
    if (0 == ret) {
        ret = metaCache_->UpdateLeader(chunkIdInfo_.lpid_, chunkIdInfo_.cpid_,
                                       &leaderId, leaderAddr.addr_);
        if (0 == ret) {
            retryDirectly_ = (leaderId != chunkserverID_);
            return 0;
        }
    }

    return -1;
}

}   // namespace client
}   // namespace curve
