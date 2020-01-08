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

int ChunkServerClient::Init(const std::string& csAddr) {
    csAddr_ = csAddr;
    if (channel_.Init(csAddr.c_str(), nullptr) != 0) {
        std::cout << "Init channel to chunkserver: " << csAddr
                  << " failed!" << std::endl;
        return -1;
    }
    return 0;
}

int ChunkServerClient::GetCopysetStatus(butil::IOBuf* iobuf) {
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

}  // namespace tool
}  // namespace curve
