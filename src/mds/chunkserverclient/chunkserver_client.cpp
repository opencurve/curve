/*
 * Project: curve
 * Created Date: Fri Mar 08 2019
 * Author: xuchaojie
 * Copyright (c) 2018 netease
 */

#include "src/mds/chunkserverclient/chunkserver_client.h"

#include <string>
#include <chrono>  //NOLINT
#include <thread>  //NOLINT

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
    uint64_t correctedSn) {
    ChunkServer chunkServer;
    if (true != topology_->GetChunkServer(leaderId, &chunkServer)) {
        return kMdsFail;
    }
    if (chunkServer.GetChunkServerState().GetOnlineState() != ONLINE) {
        return kCsClientCSOffline;
    }

    std::string ip = chunkServer.GetHostIp();
    int port = chunkServer.GetPort();

    brpc::Channel channel;
    if (channel.Init(ip.c_str(), port, NULL) != 0) {
        LOG(ERROR) << "Fail to init channel to ip: "
                   << ip
                   << " port "
                   << port;
        return kRpcChannelInitFail;
    }
    ChunkService_Stub stub(&channel);

    brpc::Controller cntl;
    cntl.set_timeout_ms(kRpcTimeoutMs);

    ChunkRequest request;
    request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_DELETE_SNAP);
    request.set_logicpoolid(logicalPoolId);
    request.set_copysetid(copysetId);
    request.set_chunkid(chunkId);
    request.set_correctedsn(correctedSn);

    ChunkResponse response;
    uint32_t retry = 0;
    do {
        LOG(INFO) << "Send DeleteChunkSnapshotOrCorrectSn[log_id="
                  << cntl.log_id()
                  << "] from " << cntl.local_side()
                  << " to " << cntl.remote_side()
                  << ". [ChunkRequest] "
                  << request.DebugString();
        cntl.Reset();
        stub.DeleteChunkSnapshotOrCorrectSn(&cntl,
            &request,
            &response,
            nullptr);
        if (cntl.Failed()) {
            LOG(WARNING) << "Received ChunkResponse error, "
                       << "cntl.errorText = "
                       << cntl.ErrorText()
                       << ", retry, time = "
                       << retry;
            std::this_thread::sleep_for(
                std::chrono::milliseconds(kRpcRetryIntervalMs));
        }
        retry++;
    } while (cntl.Failed() && retry < kRpcRetryTime);

    if (cntl.Failed()) {
        LOG(ERROR) << "Received ChunkResponse error, retry fail,"
                   << "cntl.errorText = "
                   << cntl.ErrorText() << std::endl;
        return kRpcFail;
    } else {
        switch (response.status()) {
            case CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS:
            case CHUNK_OP_STATUS::CHUNK_OP_STATUS_CHUNK_NOTEXIST: {
                    LOG(INFO) << "Received ChunkResponse[log_id="
                          << cntl.log_id()
                          << "] from " << cntl.remote_side()
                          << " to " << cntl.local_side()
                          << ". [ChunkResponse] "
                          << response.DebugString();
                    return kMdsSuccess;
                }
            case CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED: {
                    LOG(INFO) << "Received ChunkResponse, not leader, redirect."
                              << " [log_id=" << cntl.log_id()
                              << "] from " << cntl.remote_side()
                              << " to " << cntl.local_side()
                              << ". [ChunkResponse] "
                              << response.DebugString();
                    return kCsClientNotLeader;
                }
            default: {
                    LOG(ERROR) << "Received ChunkResponse error, [log_id="
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
    LogicalPoolID logicalPoolId,
    CopysetID copysetId,
    ChunkID chunkId,
    uint64_t sn) {
    ChunkServer chunkServer;
    if (true != topology_->GetChunkServer(leaderId, &chunkServer)) {
        return kMdsFail;
    }
    if (chunkServer.GetChunkServerState().GetOnlineState() != ONLINE) {
        return kCsClientCSOffline;
    }

    std::string ip = chunkServer.GetHostIp();
    int port = chunkServer.GetPort();

    brpc::Channel channel;
    if (channel.Init(ip.c_str(), port, NULL) != 0) {
        LOG(ERROR) << "Fail to init channel to ip: "
                   << ip
                   << " port "
                   << port;
        return kRpcChannelInitFail;
    }
    ChunkService_Stub stub(&channel);

    brpc::Controller cntl;
    cntl.set_timeout_ms(kRpcTimeoutMs);

    ChunkRequest request;
    request.set_optype(CHUNK_OP_TYPE::CHUNK_OP_DELETE);
    request.set_logicpoolid(logicalPoolId);
    request.set_copysetid(copysetId);
    request.set_chunkid(chunkId);
    request.set_sn(sn);

    ChunkResponse response;
    uint32_t retry = 0;
    do {
        LOG(INFO) << "Send DeleteChunk[log_id=" << cntl.log_id()
                  << "] from " << cntl.local_side()
                  << " to " << cntl.remote_side()
                  << ". [ChunkRequest] "
                  << request.DebugString();
        cntl.Reset();
        stub.DeleteChunk(&cntl,
            &request,
            &response,
            nullptr);
        if (cntl.Failed()) {
            LOG(WARNING) << "Received ChunkResponse error, "
                       << "cntl.errorText = "
                       << cntl.ErrorText()
                       << ", retry, time = "
                       << retry;
            std::this_thread::sleep_for(
                std::chrono::milliseconds(kRpcRetryIntervalMs));
        }
        retry++;
    } while (cntl.Failed() && retry < kRpcRetryTime);

    if (cntl.Failed()) {
        LOG(ERROR) << "Received ChunkResponse error, retry fail,"
                   << "cntl.errorText = "
                   << cntl.ErrorText() << std::endl;
        return kRpcFail;
    } else {
        switch (response.status()) {
            case CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS:
            case CHUNK_OP_STATUS::CHUNK_OP_STATUS_CHUNK_NOTEXIST: {
                    LOG(INFO) << "Received ChunkResponse[log_id="
                          << cntl.log_id()
                          << "] from " << cntl.remote_side()
                          << " to " << cntl.local_side()
                          << ". [ChunkResponse] "
                          << response.DebugString();
                    return kMdsSuccess;
                }
            case CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED: {
                    LOG(INFO) << "Received ChunkResponse, not leader, redirect."
                              << " [log_id=" << cntl.log_id()
                              << "] from " << cntl.remote_side()
                              << " to " << cntl.local_side()
                              << ". [ChunkResponse] "
                              << response.DebugString();
                    return kCsClientNotLeader;
                }
            default: {
                    LOG(ERROR) << "Received ChunkResponse error, [log_id="
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
    ChunkServer chunkServer;
    if (true != topology_->GetChunkServer(csId, &chunkServer)) {
        return kMdsFail;
    }

    if (chunkServer.GetChunkServerState().GetOnlineState() != ONLINE) {
        return kCsClientCSOffline;
    }

    std::string ip = chunkServer.GetHostIp();
    int port = chunkServer.GetPort();

    brpc::Channel channel;
    if (channel.Init(ip.c_str(), port, NULL) != 0) {
        LOG(ERROR) << "Fail to init channel to ip: "
                   << ip
                   << " port "
                   << port;
        return kRpcChannelInitFail;
    }

    CliService2_Stub stub(&channel);

    brpc::Controller cntl;
    cntl.set_timeout_ms(kRpcTimeoutMs);

    GetLeaderRequest2 request;
    request.set_logicpoolid(logicalPoolId);
    request.set_copysetid(copysetId);

    GetLeaderResponse2 response;
    uint32_t retry = 0;
    do {
        LOG(INFO) << "Send GetLeader[log_id=" << cntl.log_id()
                  << "] from " << cntl.local_side()
                  << " to " << cntl.remote_side()
                  << ". [GetLeaderRequest] "
                  << request.DebugString();
        cntl.Reset();
        stub.GetLeader(&cntl,
            &request,
            &response,
            nullptr);
        if (cntl.Failed()) {
            LOG(WARNING) << "Received GetLeaderResponse error, "
                       << "cntl.errorText = "
                       << cntl.ErrorText()
                       << ", retry, time = "
                       << retry;
            std::this_thread::sleep_for(
                std::chrono::milliseconds(kRpcRetryIntervalMs));
        }
        retry++;
    } while (cntl.Failed() && retry < kRpcRetryTime);

    if (cntl.Failed()) {
        LOG(ERROR) << "Received GetLeaderResponse error, retry fail,"
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

        // TODO(xuchaojie) : 后续支持新的协议之后可直接使用id
        std::string leaderPeer = response.leader().address();
        std::string leaderIp;
        uint32_t leaderPort;
        if (SplitPeerId(leaderPeer, &leaderIp, &leaderPort)) {
            *leader = topology_->FindChunkServer(leaderIp, leaderPort);
            if (UNINTIALIZE_ID == *leader) {
                LOG(ERROR) << "GetLeader failed on FindChunkServer,"
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

}  // namespace chunkserverclient
}  // namespace mds
}  // namespace curve

