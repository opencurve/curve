/*
 * Project: curve
 * Created Date: 2019-11-28
 * Author: charisu
 * Copyright (c) 2018 netease
 */
#include "src/tools/chunkserver_client.h"

DECLARE_uint64(rpcTimeout);
DECLARE_uint64(rpcRetryTimes);

namespace curve {
namespace tool {

std::ostream& operator<<(std::ostream& os, const Chunk& chunk) {
    uint64_t groupId = (static_cast<uint64_t>(chunk.logicPoolId) << 32) |
                                                    chunk.copysetId;
    os << "logicalPoolId:" << chunk.logicPoolId
       << ",copysetId:" << chunk.copysetId
       << ",groupId:" << groupId
       << ",chunkId:" << chunk.chunkId;
    return os;
}

int ChunkServerClient::Init(const std::string& csAddr) {
    csAddr_ = csAddr;
    if (channel_.Init(csAddr.c_str(), nullptr) != 0) {
        std::cout << "Init channel to chunkserver: " << csAddr
                  << " failed!" << std::endl;
        return -1;
    }
    return 0;
}

int ChunkServerClient::GetRaftStatus(butil::IOBuf* iobuf) {
    if (!iobuf) {
        std::cout << "The argument is a null pointer!" << std::endl;
        return -1;
    }
    braft::IndexRequest idxRequest;
    braft::IndexResponse idxResponse;
    brpc::Controller cntl;
    braft::raft_stat_Stub raft_stub(&channel_);

    uint64_t retryTimes = 0;
    while (retryTimes < FLAGS_rpcRetryTimes) {
        cntl.Reset();
        cntl.set_timeout_ms(FLAGS_rpcTimeout);
        raft_stub.default_method(&cntl, &idxRequest, &idxResponse, nullptr);
        if (!cntl.Failed()) {
            iobuf->clear();
            iobuf->append(cntl.response_attachment());
            return 0;
        }
        retryTimes++;
    }
    // 只打最后一次失败的原因
    std::cout << "Send RPC to chunkserver fail, error content: "
              << cntl.ErrorText() << std::endl;
    return -1;
}

bool ChunkServerClient::CheckChunkServerOnline() {
    curve::chunkserver::GetChunkInfoRequest request;
    curve::chunkserver::GetChunkInfoResponse response;
    request.set_logicpoolid(1);
    request.set_copysetid(1);
    request.set_chunkid(1);
    brpc::Controller cntl;
    curve::chunkserver::ChunkService_Stub stub(&channel_);

    uint64_t retryTimes = 0;
    while (retryTimes < FLAGS_rpcRetryTimes) {
        cntl.Reset();
        cntl.set_timeout_ms(FLAGS_rpcTimeout);
        stub.GetChunkInfo(&cntl, &request, &response, nullptr);
        if (!cntl.Failed()) {
            return true;
        }
        retryTimes++;
    }
    return false;
}

int ChunkServerClient::GetCopysetStatus(
                                const CopysetStatusRequest& request,
                                CopysetStatusResponse* response) {
    brpc::Controller cntl;
    curve::chunkserver::CopysetService_Stub stub(&channel_);
    uint64_t retryTimes = 0;
    while (retryTimes < FLAGS_rpcRetryTimes) {
        cntl.Reset();
        cntl.set_timeout_ms(FLAGS_rpcTimeout);
        stub.GetCopysetStatus(&cntl, &request, response, nullptr);
        if (cntl.Failed()) {
            retryTimes++;
            continue;
        }
        if (response->status() !=
                        COPYSET_OP_STATUS::COPYSET_OP_STATUS_SUCCESS) {
            std::cout << "GetCopysetStatus fail, request: "
                      << request.DebugString()
                      << ", errCode: "
                      << response->status() << std::endl;
            return -1;
        } else {
            return 0;
        }
    }
    // 只打最后一次失败的原因
    std::cout << "Send RPC to chunkserver fail, error content: "
              << cntl.ErrorText() << std::endl;
    return -1;
}

int ChunkServerClient::GetChunkHash(const Chunk& chunk,
                                    std::string* chunkHash) {
    brpc::Controller cntl;
    curve::chunkserver::ChunkService_Stub stub(&channel_);
    uint64_t retryTimes = 0;
    while (retryTimes < FLAGS_rpcRetryTimes) {
        cntl.Reset();
        cntl.set_timeout_ms(FLAGS_rpcTimeout);
        GetChunkHashRequest request;
        request.set_logicpoolid(chunk.logicPoolId);
        request.set_copysetid(chunk.copysetId);
        request.set_chunkid(chunk.chunkId);
        request.set_offset(0);
        request.set_length(FLAGS_chunkSize);
        GetChunkHashResponse response;
        stub.GetChunkHash(&cntl, &request, &response, nullptr);
        if (cntl.Failed()) {
            retryTimes++;
            continue;
        }
        if (response.status() != CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS) {
            std::cout << "GetCopysetStatus fail, request: "
                      << request.DebugString()
                      << ", errCode: "
                      << response.status() << std::endl;
            return -1;
        } else {
            *chunkHash = response.hash();
            return 0;
        }
    }
    // 只打最后一次失败的原因
    std::cout << "Send RPC to chunkserver fail, error content: "
              << cntl.ErrorText() << std::endl;
    return -1;
}

}  // namespace tool
}  // namespace curve
