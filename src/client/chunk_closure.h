/*
 * Project: curve
 * Created Date: 18-9-27
 * Author: wudemiao
 * Copyright (c) 2018 netease
 */

#ifndef CURVE_CLIENT_CHUNK_CLOSURE_H
#define CURVE_CLIENT_CHUNK_CLOSURE_H

#include <google/protobuf/stubs/callback.h>
#include <brpc/controller.h>
#include <bthread/bthread.h>

#include "proto/chunk.pb.h"

namespace curve {
namespace client {

using curve::chunkserver::CHUNK_OP_STATUS;
using curve::chunkserver::ChunkResponse;

class RequestSenderManager;
class MetaCache;
class CopysetClient;

class WriteChunkClosure : public google::protobuf::Closure {
 public:
    WriteChunkClosure(CopysetClient *client, google::protobuf::Closure *done)
        : client_(client),
          done_(done) {}

    void Run();
    void SetCntl(brpc::Controller* cntl) {
        cntl_ = cntl;
    }
    void SetResponse(ChunkResponse* response) {
        response_ = response;
    }
    void SetRetriedTimes(int retriedTimes) {
        retriedTimes_ = retriedTimes;
    }

 private:
    int                         retriedTimes_;
    brpc::Controller            *cntl_;
    ChunkResponse               *response_;
    CopysetClient               *client_;
    google::protobuf::Closure   *done_;
};

class ReadChunkClosure : public google::protobuf::Closure {
 public:
    ReadChunkClosure(CopysetClient *client, google::protobuf::Closure *done)
        : client_(client),
          done_(done) {}

    void Run();
    void SetCntl(brpc::Controller* cntl) {
        cntl_ = cntl;
    }
    void SetResponse(ChunkResponse* response) {
        response_ = response;
    }
    void SetRetriedTimes(int retriedTimes) {
        retriedTimes_ = retriedTimes;
    }

 private:
    int                         retriedTimes_;
    brpc::Controller            *cntl_;
    ChunkResponse               *response_;
    CopysetClient               *client_;
    google::protobuf::Closure   *done_;
};

}   // namespace client
}   // namespace curve

#endif  // CURVE_CLIENT_CHUNK_CLOSURE_H
