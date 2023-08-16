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
 * Created Date: Fri Mar 08 2019
 * Author: xuchaojie
 */

#include "src/mds/chunkserverclient/chunkserver_client.h"

#include <string>
#include <chrono>  //NOLINT
#include <thread>  //NOLINT
#include <utility>

using ::curve::mds::topology::ChunkServer;
using ::curve::mds::topology::SplitPeerId;
using ::curve::mds::topology::UNINTIALIZE_ID;
using ::curve::mds::topology::ONLINE;

using ::curve::chunkserver::ChunkService_Stub;
using ::curve::chunkserver::ChunkRequest;
using ::curve::chunkserver::ChunkResponse;
using ::curve::chunkserver::CHUNK_OP_TYPE;
using ::curve::chunkserver::CHUNK_OP_STATUS;

using ::curve::chunkserver::CliService2_Stub;
using ::curve::chunkserver::GetLeaderRequest2;
using ::curve::chunkserver::GetLeaderResponse2;



namespace curve {
namespace mds {
namespace chunkserverclient {

int ChunkServerClient::DeleteChunkSnapshotOrCorrectSn(
    ChunkServerIdType leaderId,
    LogicalPoolID logicalPoolId,
    CopysetID copysetId,
    ChunkID chunkId,
    uint64_t snapSn) {
    ChannelPtr channelPtr;
    int res = GetOrInitChannel(leaderId, &channelPtr);
    if (res != kMdsSuccess) {
        return res;
    }
    ChunkService_Stub stub(channelPtr.get());

    brpc::Controller cntl;
    cntl.set_timeout_ms(retryOps_.rpcTimeoutMs);

    ChunkRequest request;
    request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_DELETE_SNAP);
    request.set_logicpoolid(logicalPoolId);
    request.set_copysetid(copysetId);
    request.set_chunkid(chunkId);
    request.set_snapsn(snapSn);

    ChunkResponse response;
    uint32_t retry = 0;
    do {
        cntl.Reset();
        cntl.set_timeout_ms(retryOps_.rpcTimeoutMs);
        stub.DeleteChunkSnapshotOrCorrectSn(&cntl,
            &request,
            &response,
            nullptr);
        LOG(INFO) << "Send DeleteChunkSnapshotOrCorrectSn[log_id="
                  << cntl.log_id()
                  << "] from " << cntl.local_side()
                  << " to " << cntl.remote_side()
                  << ". [ChunkRequest] "
                  << request.DebugString();
        if (cntl.Failed()) {
            LOG(WARNING) << "Send DeleteChunkSnapshotOrCorrectSn error, "
                       << "cntl.errorText = "
                       << cntl.ErrorText()
                       << ", retry, time = "
                       << retry;
            std::this_thread::sleep_for(
                std::chrono::milliseconds(retryOps_.rpcRetryIntervalMs));
        }
        retry++;
    } while (cntl.Failed() && retry < retryOps_.rpcRetryTimes);

    if (cntl.Failed()) {
        LOG(ERROR) << "Send DeleteChunkSnapshotOrCorrectSn error, retry fail,"
                   << "cntl.errorText = "
                   << cntl.ErrorText() << std::endl;
        return kRpcFail;
    } else {
        switch (response.status()) {
            case CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS:
            case CHUNK_OP_STATUS::CHUNK_OP_STATUS_CHUNK_NOTEXIST: {
                LOG(INFO) << "Received DeleteChunkSnapshotOrCorrectSn[log_id="
                          << cntl.log_id()
                          << "] from " << cntl.remote_side()
                          << " to " << cntl.local_side()
                          << ". [ChunkResponse] "
                          << response.DebugString();
                    return kMdsSuccess;
                }
            case CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED: {
                LOG(INFO) << "Received DeleteChunkSnapshotOrCorrectSn,"
                          << " not leader, redirect."
                          << " [log_id=" << cntl.log_id()
                          << "] from " << cntl.remote_side()
                          << " to " << cntl.local_side()
                          << ". [ChunkResponse] "
                          << response.DebugString();
                    return kCsClientNotLeader;
                }
            default: {
                LOG(ERROR) << "Received DeleteChunkSnapshotOrCorrectSn "
                           << "error, [log_id="
                           << cntl.log_id()
                           << "] from " << cntl.remote_side()
                           << " to " << cntl.local_side()
                           << ". [ChunkResponse] "
                           << response.DebugString();
                    return kCsClientReturnFail;
                }
        }
    }
    return kMdsSuccess;
}

int ChunkServerClient::DeleteChunkSnapshot(
    ChunkServerIdType leaderId,
    uint64_t fileId,
    uint64_t originFileId,
    uint64_t chunkIndex,
    LogicalPoolID logicalPoolId,
    CopysetID copysetId,
    ChunkID chunkId,
    uint64_t snapSn,
    const std::vector<uint64_t>& snaps) {
    ChannelPtr channelPtr;
    int res = GetOrInitChannel(leaderId, &channelPtr);
    if (res != kMdsSuccess) {
        return res;
    }
    ChunkService_Stub stub(channelPtr.get());

    brpc::Controller cntl;
    cntl.set_timeout_ms(retryOps_.rpcTimeoutMs);

    ChunkRequest request;
    request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_DELETE_SNAP);
    request.set_fileid(fileId);
    if (originFileId != 0) {
        request.set_originfileid(originFileId);
    }
    request.set_chunkindex(chunkIndex);
    request.set_logicpoolid(logicalPoolId);
    request.set_copysetid(copysetId);
    request.set_chunkid(chunkId);
    request.set_snapsn(snapSn);
    for (auto snap: snaps) {
        request.add_snaps(snap);
    }

    ChunkResponse response;
    uint32_t retry = 0;
    do {
        cntl.Reset();
        cntl.set_timeout_ms(retryOps_.rpcTimeoutMs);
        //stub.DeleteChunkSnapshot(&cntl,
        // 这里目前还是维持的旧的删除RPC接口，后面根据新旧快照兼容方案来修改
        stub.DeleteChunkSnapshotOrCorrectSn(&cntl,
            &request,
            &response,
            nullptr);
        LOG(INFO) << "Send DeleteChunkSnapshot[log_id="
                  << cntl.log_id()
                  << "] from " << cntl.local_side()
                  << " to " << cntl.remote_side()
                  << ". [ChunkRequest] "
                  << request.DebugString();
        if (cntl.Failed()) {
            LOG(WARNING) << "Send DeleteChunkSnapshot error, "
                       << "cntl.errorText = "
                       << cntl.ErrorText()
                       << ", retry, time = "
                       << retry;
            std::this_thread::sleep_for(
                std::chrono::milliseconds(retryOps_.rpcRetryIntervalMs));
        }
        retry++;
    } while (cntl.Failed() && retry < retryOps_.rpcRetryTimes);

    if (cntl.Failed()) {
        LOG(ERROR) << "Send DeleteChunkSnapshot error, retry fail,"
                   << "cntl.errorText = "
                   << cntl.ErrorText() << std::endl;
        return kRpcFail;
    } else {
        switch (response.status()) {
            case CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS:
            case CHUNK_OP_STATUS::CHUNK_OP_STATUS_CHUNK_NOTEXIST: {
                LOG(INFO) << "Received DeleteChunkSnapshot[log_id="
                          << cntl.log_id()
                          << "] from " << cntl.remote_side()
                          << " to " << cntl.local_side()
                          << ". [ChunkResponse] "
                          << response.DebugString();
                    return kMdsSuccess;
                }
            case CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED: {
                LOG(INFO) << "Received DeleteChunkSnapshot,"
                          << " not leader, redirect."
                          << " [log_id=" << cntl.log_id()
                          << "] from " << cntl.remote_side()
                          << " to " << cntl.local_side()
                          << ". [ChunkResponse] "
                          << response.DebugString();
                    return kCsClientNotLeader;
                }
            default: {
                LOG(ERROR) << "Received DeleteChunkSnapshot "
                           << "error, [log_id="
                           << cntl.log_id()
                           << "] from " << cntl.remote_side()
                           << " to " << cntl.local_side()
                           << ". [ChunkResponse] "
                           << response.DebugString();
                    return kCsClientReturnFail;
                }
        }
    }
    return kMdsSuccess;
}

int ChunkServerClient::DeleteChunk(ChunkServerIdType leaderId,
    uint64_t fileId,
    uint64_t originFileId,
    uint64_t chunkIndex,
    LogicalPoolID logicalPoolId,
    CopysetID copysetId,
    ChunkID chunkId,
    uint64_t sn) {
    ChannelPtr channelPtr;
    int res = GetOrInitChannel(leaderId, &channelPtr);
    if (res != kMdsSuccess) {
        return res;
    }
    ChunkService_Stub stub(channelPtr.get());

    brpc::Controller cntl;
    cntl.set_timeout_ms(retryOps_.rpcTimeoutMs);

    ChunkRequest request;
    request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_DELETE);
    request.set_fileid(fileId);
    if (originFileId != 0) {
        request.set_originfileid(originFileId);
    }
    request.set_chunkindex(chunkIndex);
    request.set_logicpoolid(logicalPoolId);
    request.set_copysetid(copysetId);
    request.set_chunkid(chunkId);
    request.set_sn(sn);

    ChunkResponse response;
    uint32_t retry = 0;
    do {
        cntl.Reset();
        cntl.set_timeout_ms(retryOps_.rpcTimeoutMs);
        stub.DeleteChunk(&cntl,
            &request,
            &response,
            nullptr);
        LOG(INFO) << "Send DeleteChunk[log_id=" << cntl.log_id()
                  << "] from " << cntl.local_side()
                  << " to " << cntl.remote_side()
                  << ". [ChunkRequest] "
                  << request.DebugString();
        if (cntl.Failed()) {
            LOG(WARNING) << "Send DeleteChunk error, "
                       << "cntl.errorText = "
                       << cntl.ErrorText()
                       << ", retry, time = "
                       << retry;
            std::this_thread::sleep_for(
                std::chrono::milliseconds(retryOps_.rpcRetryIntervalMs));
        }
        retry++;
    } while (cntl.Failed() && retry < retryOps_.rpcRetryTimes);

    if (cntl.Failed()) {
        LOG(ERROR) << "Send DeleteChunk error, retry fail,"
                   << "cntl.errorText = "
                   << cntl.ErrorText() << std::endl;
        return kRpcFail;
    } else {
        switch (response.status()) {
            case CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS:
            case CHUNK_OP_STATUS::CHUNK_OP_STATUS_CHUNK_NOTEXIST: {
                    LOG(INFO) << "Received DeleteChunk[log_id="
                          << cntl.log_id()
                          << "] from " << cntl.remote_side()
                          << " to " << cntl.local_side()
                          << ". [ChunkResponse] "
                          << response.DebugString();
                    return kMdsSuccess;
                }
            case CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED: {
                    LOG(INFO) << "Received DeleteChunk, not leader, redirect."
                              << " [log_id=" << cntl.log_id()
                              << "] from " << cntl.remote_side()
                              << " to " << cntl.local_side()
                              << ". [ChunkResponse] "
                              << response.DebugString();
                    return kCsClientNotLeader;
                }
            default: {
                    LOG(ERROR) << "Received DeleteChunk error, [log_id="
                              << cntl.log_id()
                              << "] from " << cntl.remote_side()
                              << " to " << cntl.local_side()
                              << ". [ChunkResponse] "
                              << response.DebugString();
                    return kCsClientReturnFail;
                }
        }
    }
    return kMdsSuccess;
}

int ChunkServerClient::GetLeader(ChunkServerIdType csId,
    LogicalPoolID logicalPoolId,
    CopysetID copysetId,
    ChunkServerIdType * leader) {
    ChannelPtr channelPtr;
    int res = GetOrInitChannel(csId, &channelPtr);
    if (res != kMdsSuccess) {
        return res;
    }
    CliService2_Stub stub(channelPtr.get());

    brpc::Controller cntl;
    cntl.set_timeout_ms(retryOps_.rpcTimeoutMs);

    GetLeaderRequest2 request;
    request.set_logicpoolid(logicalPoolId);
    request.set_copysetid(copysetId);

    GetLeaderResponse2 response;
    uint32_t retry = 0;
    do {
        cntl.Reset();
        cntl.set_timeout_ms(retryOps_.rpcTimeoutMs);
        stub.GetLeader(&cntl,
            &request,
            &response,
            nullptr);
        LOG(INFO) << "Send GetLeader[log_id=" << cntl.log_id()
                  << "] from " << cntl.local_side()
                  << " to " << cntl.remote_side()
                  << ". [GetLeaderRequest] "
                  << request.DebugString();
        if (cntl.Failed()) {
            LOG(WARNING) << "Send GetLeader error, "
                       << "cntl.errorText = "
                       << cntl.ErrorText()
                       << ", retry, time = "
                       << retry;
            std::this_thread::sleep_for(
                std::chrono::milliseconds(retryOps_.rpcRetryIntervalMs));
        }
        retry++;
    } while (cntl.Failed() && retry < retryOps_.rpcRetryTimes);

    if (cntl.Failed()) {
        LOG(ERROR) << "Send GetLeader error, retry fail,"
                   << "cntl.errorText = "
                   << cntl.ErrorText() << std::endl;
        return kRpcFail;
    } else {
        LOG(INFO) << "Received GetLeaderResponse[log_id="
                  << cntl.log_id()
                  << "] from " << cntl.remote_side()
                  << " to " << cntl.local_side()
                  << ". [GetLeaderResponse] "
                  << response.DebugString();

        // TODO(xuchaojie): use ID directly when new protocol supported
        std::string leaderPeer = response.leader().address();
        std::string leaderIp;
        uint32_t leaderPort;
        if (SplitPeerId(leaderPeer, &leaderIp, &leaderPort)) {
            *leader = topology_->FindChunkServerNotRetired(
                leaderIp, leaderPort);
            if (UNINTIALIZE_ID == *leader) {
                LOG(WARNING) << "GetLeader failed on FindChunkServer,"
                           << "leaderIp = " << leaderIp
                           << "leaderPort = " << leaderPort;
                return kMdsFail;
            }
        } else {
            LOG(ERROR) << "GetLeader failed on SplitPeerId, "
                       << "peerId string = " << leaderPeer;
            return kMdsFail;
        }
    }
    return kMdsSuccess;
}

int ChunkServerClient::GetChunkServerAddress(ChunkServerIdType csId,
                                             std::string* csAddr) {
    ChunkServer chunkServer;
    if (true != topology_->GetChunkServer(csId, &chunkServer)) {
        LOG(ERROR) << "GetChunkServer from topology fail, csId = " << csId;
        return kMdsFail;
    }
    if (chunkServer.GetOnlineState() != ONLINE) {
        return kCsClientCSOffline;
    }

    std::string ip = chunkServer.GetHostIp();
    int port = chunkServer.GetPort();
    *csAddr = ip + ":" + std::to_string(port);
    return kMdsSuccess;
}

int ChunkServerClient::GetOrInitChannel(ChunkServerIdType csId,
                                        ChannelPtr* channelPtr) {
    std::string csAddr;
    int res = GetChunkServerAddress(csId, &csAddr);
    if (res != kMdsSuccess) {
        return res;
    }
    res = channelPool_->GetOrInitChannel(csAddr, channelPtr);
    if (res != 0) {
        LOG(ERROR) << "Fail to get or init channel to " << csAddr;
        return kRpcChannelInitFail;
    }
    return kMdsSuccess;
}

int ChunkServerClient::FlattenChunk(
    ChunkServerIdType leaderId,
    const std::shared_ptr<FlattenChunkContext> &ctx, 
    ChunkServerClientClosure* done) {
    std::unique_ptr<FlattenChunkRpcContext> rpcCtx(new FlattenChunkRpcContext);

    int res = GetOrInitChannel(leaderId, &rpcCtx->channelPtr);
    if (res != kMdsSuccess) {
        done->SetErrCode(kRpcChannelInitFail);
        return res;
    }

    rpcCtx->cntl.set_timeout_ms(retryOps_.rpcTimeoutMs);

    rpcCtx->request.set_optype(curve::chunkserver::CHUNK_OP_FLATTEN);
    rpcCtx->request.set_fileid(ctx->fileId);
    rpcCtx->request.set_logicpoolid(ctx->logicalPoolId);
    rpcCtx->request.set_copysetid(ctx->copysetId);
    rpcCtx->request.set_chunkid(ctx->chunkId);
    rpcCtx->request.set_sn(ctx->seqNum);
    rpcCtx->request.set_offset(ctx->partIndex * ctx->partSize);
    rpcCtx->request.set_size(ctx->partSize);

    if (ctx->originSegmentExist) {
        rpcCtx->request.set_originchunkid(ctx->originChunkId);
    }
    rpcCtx->request.set_originfileid(ctx->originFileId);
    rpcCtx->request.set_chunkindex(ctx->chunkIndex);
    rpcCtx->request.set_cloneno(ctx->cloneNo);
    for (auto &clone : ctx->clones) {
        auto cinfo = rpcCtx->request.add_clones();
        cinfo->set_cloneno(clone.cloneNo);
        cinfo->set_clonesn(clone.cloneSn);
    }

    rpcCtx->done = done;
    rpcCtx->curTry = 1;
    rpcCtx->retryOps_ = retryOps_;

    google::protobuf::Closure* rpcDone = brpc::NewCallback(
        OnFlattenChunkReturned, rpcCtx.get());

    ChunkService_Stub stub(rpcCtx->channelPtr.get());
    stub.FlattenChunk(&rpcCtx->cntl,
                      &rpcCtx->request,
                      &rpcCtx->response,
                      rpcDone);
    rpcCtx.release();
    return kMdsSuccess;
}

void ChunkServerClient::OnFlattenChunkReturned(FlattenChunkRpcContext *ctx) {
    std::unique_ptr<FlattenChunkRpcContext> ctxGuard(ctx);
    brpc::ClosureGuard doneGuard(ctx->done);
    if (ctx->cntl.Failed()) {
        LOG(WARNING) << "Send FlattenChunk failed"
                     << ", request = " << ctx->request.ShortDebugString()
                     << ", error = " << ctx->cntl.ErrorCode()
                     << ", errorText = " << ctx->cntl.ErrorText()
                     << ", retry, time = " << ctx->curTry;
        if (ctx->cntl.ErrorCode() == brpc::ELOGOFF ||
            ctx->cntl.ErrorCode() == EHOSTDOWN ||
            ctx->cntl.ErrorCode() == ECONNRESET ||
            ctx->cntl.ErrorCode() == ECONNREFUSED) {
            ctx->done->SetErrCode(kCsClientCSOffline);
            return; 
        }

        if (ctx->curTry < ctx->retryOps_.rpcRetryTimes) {
            bthread_usleep(ctx->retryOps_.rpcRetryIntervalMs);
            ctx->cntl.Reset();
            uint32_t nextTimeOutMs =
                std::min(ctx->retryOps_.rpcTimeoutMs << ctx->curTry,
                         ctx->retryOps_.rpcMaxTimeoutMs);
            ctx->cntl.set_timeout_ms(nextTimeOutMs);
            ctx->response.Clear();
            ctx->curTry++;
            google::protobuf::Closure* rpcDone = brpc::NewCallback(
                        OnFlattenChunkReturned, ctx);

            ChunkService_Stub stub(ctx->channelPtr.get());
            stub.FlattenChunk(&ctx->cntl,
                              &ctx->request,
                              &ctx->response,
                              rpcDone);
            doneGuard.release();
            ctxGuard.release();
            return;
        } else {
            LOG(WARNING) << "Send FlattenChunk failed"
                         << ", request = " << ctx->request.ShortDebugString()
                         << ", error = " << ctx->cntl.ErrorCode()
                         << ", errorText = " << ctx->cntl.ErrorText()
                         << ", retry fail";
            ctx->done->SetErrCode(kRpcFail);
            return;
        }
    } else {
        switch (ctx->response.status()) {
            case CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS:
            case CHUNK_OP_STATUS::CHUNK_OP_STATUS_CHUNK_NOTEXIST: {
                ctx->done->SetErrCode(kMdsSuccess);
                LOG(INFO) << "Received FlattenChunk "
                          << "success, [log_id="
                          << ctx->cntl.log_id()
                          << "] from " << ctx->cntl.remote_side()
                          << " to " << ctx->cntl.local_side()
                          << ". [ChunkResponse] "
                          << ctx->response.ShortDebugString();
                return;
            }
            case CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED: {
                LOG(WARNING) << "Received FlattenChunk "
                           << "error, [log_id="
                           << ctx->cntl.log_id()
                           << "] from " << ctx->cntl.remote_side()
                           << " to " << ctx->cntl.local_side()
                           << ". [ChunkResponse] "
                           << ctx->response.ShortDebugString();
                ctx->done->SetErrCode(kCsClientNotLeader);
                return;
            }
            default: {
                LOG(ERROR) << "Received FlattenChunk "
                           << "error, [log_id="
                           << ctx->cntl.log_id()
                           << "] from " << ctx->cntl.remote_side()
                           << " to " << ctx->cntl.local_side()
                           << ". [ChunkResponse] "
                           << ctx->response.ShortDebugString();
                ctx->done->SetErrCode(kCsClientReturnFail);
                return;
            }
        }
    }
}

}  // namespace chunkserverclient
}  // namespace mds
}  // namespace curve

