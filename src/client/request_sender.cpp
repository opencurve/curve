/*
 * Project: curve
 * Created Date: 18-9-25
 * Author: wudemiao
 * Copyright (c) 2018 netease
 */

#include "src/client/request_sender.h"

#include <glog/logging.h>

#include "proto/chunk.pb.h"

namespace curve {
namespace client {

int RequestSender::ReadChunk(LogicPoolID logicPoolId,
                                  CopysetID copysetId,
                                  ChunkID chunkId,
                                  off_t offset,
                                  size_t length,
                                  ReadChunkClosure *done) {
    brpc::ClosureGuard doneGuard(done);

    brpc::Controller *cntl = new brpc::Controller();
    cntl->set_timeout_ms(senderOptions_.timeoutMs);
    cntl->set_max_retry(senderOptions_.maxRetry);
    done->SetCntl(cntl);
    curve::chunkserver::ChunkResponse *response = new ChunkResponse();
    done->SetResponse(response);

    curve::chunkserver::ChunkRequest request;
    request.set_optype(curve::chunkserver::CHUNK_OP_TYPE::CHUNK_OP_READ);
    request.set_logicpoolid(logicPoolId);
    request.set_copysetid(copysetId);
    request.set_chunkid(chunkId);
    request.set_offset(offset);
    request.set_size(length);
    curve::chunkserver::ChunkService_Stub stub(&channel_);
    stub.ReadChunk(cntl, &request, response, doneGuard.release());
    return 0;
}

int RequestSender::WriteChunk(LogicPoolID logicPoolId,
                                   CopysetID copysetId,
                                   ChunkID chunkId,
                                   const char *buf,
                                   off_t offset,
                                   size_t length,
                                   WriteChunkClosure *done) {
    brpc::ClosureGuard doneGuard(done);

    brpc::Controller *cntl = new brpc::Controller();
    cntl->set_timeout_ms(senderOptions_.timeoutMs);
    cntl->set_max_retry(senderOptions_.maxRetry);
    done->SetCntl(cntl);
    curve::chunkserver::ChunkResponse *response = new ChunkResponse();
    done->SetResponse(response);

    curve::chunkserver::ChunkRequest request;
    request.set_optype(curve::chunkserver::CHUNK_OP_TYPE::CHUNK_OP_WRITE);
    request.set_logicpoolid(logicPoolId);
    request.set_copysetid(copysetId);
    request.set_chunkid(chunkId);
    request.set_offset(offset);
    request.set_size(length);
    cntl->request_attachment().append(buf);
    curve::chunkserver::ChunkService_Stub stub(&channel_);
    stub.WriteChunk(cntl, &request, response, doneGuard.release());

    return 0;
}

int RequestSender::Init() {
    if (0 != channel_.Init(serverEndPoint_, NULL)) {
        LOG(ERROR) << "failed to init channel to server, id: " << chunkServerId_
                   << ", "<< serverEndPoint_.ip << ":" << serverEndPoint_.port;
        return -1;
    }
    return 0;
}

int RequestSender::ResetSender(ChunkServerID chunkServerId,
                                    butil::EndPoint serverEndPoint) {
    chunkServerId_ = chunkServerId;
    serverEndPoint_ = serverEndPoint;
    return Init();
}

}   // namespace client
}   // namespace curve
