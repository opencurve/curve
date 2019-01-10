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

int RequestSender::Init(IOSenderOption_t iosenderopt) {
    if (0 != channel_.Init(serverEndPoint_, NULL)) {
        LOG(ERROR) << "failed to init channel to server, id: " << chunkServerId_
                   << ", "<< serverEndPoint_.ip << ":" << serverEndPoint_.port;
        return -1;
    }
    iosenderopt_ = iosenderopt;
    ClientClosure::SetFailureRequestOption(iosenderopt_.failreqopt);

    return 0;
}

int RequestSender::ReadChunk(LogicPoolID logicPoolId,
                             CopysetID copysetId,
                             ChunkID chunkId,
                             uint64_t sn,
                             off_t offset,
                             size_t length,
                             uint64_t appliedindex,
                             ClientClosure *done) {
    brpc::ClosureGuard doneGuard(done);

    brpc::Controller *cntl = new brpc::Controller();
    cntl->set_timeout_ms(iosenderopt_.rpc_timeout_ms);
    cntl->set_max_retry(iosenderopt_.rpc_retry_times);
    done->SetCntl(cntl);
    ChunkResponse *response = new ChunkResponse();
    done->SetResponse(response);

    ChunkRequest request;
    request.set_optype(curve::chunkserver::CHUNK_OP_TYPE::CHUNK_OP_READ);
    request.set_logicpoolid(logicPoolId);
    request.set_copysetid(copysetId);
    request.set_chunkid(chunkId);
    request.set_offset(offset);
    request.set_size(length);
    if (iosenderopt_.enable_applied_index_read && appliedindex > 0) {
        request.set_appliedindex(appliedindex);
    }
    ChunkService_Stub stub(&channel_);
    stub.ReadChunk(cntl, &request, response, doneGuard.release());

    return 0;
}

int RequestSender::WriteChunk(LogicPoolID logicPoolId,
                              CopysetID copysetId,
                              ChunkID chunkId,
                              uint64_t sn,
                              const char *buf,
                              off_t offset,
                              size_t length,
                              ClientClosure *done) {
    brpc::ClosureGuard doneGuard(done);

    DVLOG(9) << "Sending request, buf header: "
             << " buf: " << *(unsigned int *)buf;
    brpc::Controller *cntl = new brpc::Controller();
    cntl->set_timeout_ms(iosenderopt_.rpc_timeout_ms);
    cntl->set_max_retry(iosenderopt_.rpc_retry_times);
    done->SetCntl(cntl);
    ChunkResponse *response = new ChunkResponse();
    done->SetResponse(response);

    ChunkRequest request;
    request.set_optype(curve::chunkserver::CHUNK_OP_TYPE::CHUNK_OP_WRITE);
    request.set_logicpoolid(logicPoolId);
    request.set_copysetid(copysetId);
    request.set_chunkid(chunkId);
    request.set_sn(sn);
    request.set_offset(offset);
    request.set_size(length);
    cntl->request_attachment().append(buf, length);
    ChunkService_Stub stub(&channel_);
    stub.WriteChunk(cntl, &request, response, doneGuard.release());

    return 0;
}

int RequestSender::ReadChunkSnapshot(LogicPoolID logicPoolId,
                                     CopysetID copysetId,
                                     ChunkID chunkId,
                                     uint64_t sn,
                                     off_t offset,
                                     size_t length,
                                     ClientClosure *done) {
    brpc::ClosureGuard doneGuard(done);

    brpc::Controller *cntl = new brpc::Controller();
    cntl->set_timeout_ms(iosenderopt_.rpc_timeout_ms);
    cntl->set_max_retry(iosenderopt_.rpc_retry_times);
    done->SetCntl(cntl);
    ChunkResponse *response = new ChunkResponse();
    done->SetResponse(response);

    ChunkRequest request;
    request.set_optype(curve::chunkserver::CHUNK_OP_TYPE::CHUNK_OP_READ_SNAP);
    request.set_logicpoolid(logicPoolId);
    request.set_copysetid(copysetId);
    request.set_chunkid(chunkId);
    request.set_sn(sn);
    request.set_offset(offset);
    request.set_size(length);
    ChunkService_Stub stub(&channel_);
    stub.ReadChunkSnapshot(cntl, &request, response, doneGuard.release());

    return 0;
}

int RequestSender::DeleteChunkSnapshot(LogicPoolID logicPoolId,
                                       CopysetID copysetId,
                                       ChunkID chunkId,
                                       uint64_t sn,
                                       ClientClosure *done) {
    brpc::ClosureGuard doneGuard(done);

    brpc::Controller *cntl = new brpc::Controller();
    cntl->set_timeout_ms(iosenderopt_.rpc_timeout_ms);
    cntl->set_max_retry(iosenderopt_.rpc_retry_times);
    done->SetCntl(cntl);
    ChunkResponse *response = new ChunkResponse();
    done->SetResponse(response);

    ChunkRequest request;
    request.set_optype(curve::chunkserver::CHUNK_OP_TYPE::CHUNK_OP_DELETE_SNAP);
    request.set_logicpoolid(logicPoolId);
    request.set_copysetid(copysetId);
    request.set_chunkid(chunkId);
    request.set_sn(sn);
    ChunkService_Stub stub(&channel_);
    stub.DeleteChunkSnapshot(cntl, &request, response, doneGuard.release());
    return 0;
}

int RequestSender::GetChunkInfo(LogicPoolID logicPoolId,
                                CopysetID copysetId,
                                ChunkID chunkId,
                                ClientClosure *done) {
    brpc::ClosureGuard doneGuard(done);

    brpc::Controller *cntl = new brpc::Controller();
    cntl->set_timeout_ms(iosenderopt_.rpc_timeout_ms);
    cntl->set_max_retry(iosenderopt_.rpc_retry_times);
    done->SetCntl(cntl);
    GetChunkInfoResponse *response = new GetChunkInfoResponse();
    done->SetResponse(response);

    GetChunkInfoRequest request;
    request.set_logicpoolid(logicPoolId);
    request.set_copysetid(copysetId);
    request.set_chunkid(chunkId);
    ChunkService_Stub stub(&channel_);
    stub.GetChunkInfo(cntl, &request, response, doneGuard.release());
    return 0;
}

int RequestSender::ResetSender(ChunkServerID chunkServerId,
                               butil::EndPoint serverEndPoint) {
    chunkServerId_ = chunkServerId;
    serverEndPoint_ = serverEndPoint;
    return Init(iosenderopt_);
}

}   // namespace client
}   // namespace curve
