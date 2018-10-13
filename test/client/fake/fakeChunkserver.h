/*
 * Project: curve
 * File Created: Wednesday, 17th October 2018 10:51:42 am
 * Author: tongguangxun
 * Copyright (c) 2018 NetEase
 */

#ifndef CURVE_CLIENT_FAKE_CHUNKSERVICE_H
#define CURVE_CLIENT_FAKE_CHUNKSERVICE_H

#include <brpc/controller.h>
#include <brpc/server.h>
#include <glog/logging.h>

#include "proto/chunk.pb.h"

using curve::chunkserver::ChunkService;
using curve::chunkserver::CHUNK_OP_STATUS;

class FakeChunkService : public ChunkService {
 public:
    FakeChunkService() {
    }
    virtual ~FakeChunkService() {}

    void WriteChunk(::google::protobuf::RpcController *controller,
                    const ::curve::chunkserver::ChunkRequest *request,
                    ::curve::chunkserver::ChunkResponse *response,
                    google::protobuf::Closure *done) {
        brpc::ClosureGuard doneGuard(done);

        brpc::Controller *cntl = dynamic_cast<brpc::Controller *>(controller);
        ::memcpy(chunk_ + request->offset(),
                 cntl->request_attachment().to_string().c_str(),
                 request->size());
        response->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
    }

    void ReadChunk(::google::protobuf::RpcController *controller,
                   const ::curve::chunkserver::ChunkRequest *request,
                   ::curve::chunkserver::ChunkResponse *response,
                   google::protobuf::Closure *done) {
        brpc::ClosureGuard doneGuard(done);

        brpc::Controller *cntl = dynamic_cast<brpc::Controller *>(controller);
        char buff[8192] = {0};
        ::memcpy(buff, chunk_ + request->offset(), request->size());
        cntl->response_attachment().append(buff, request->size());
        response->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
    }

 private:
    char chunk_[8192];
};

#endif  // CURVE_CLIENT_FAKECHUNKSERVICE_H
