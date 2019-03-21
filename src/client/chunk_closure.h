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
#include "src/client/client_config.h"

namespace curve {
namespace client {

using curve::chunkserver::CHUNK_OP_STATUS;
using curve::chunkserver::ChunkResponse;
using curve::chunkserver::GetChunkInfoResponse;
using ::google::protobuf::Message;
using ::google::protobuf::Closure;

class RequestSenderManager;
class MetaCache;
class CopysetClient;

/**
 * ClientClosure，负责保存Rpc上下文，
 * 包含cntl和response已经重试次数
 */
class ClientClosure : public Closure {
 public:
    virtual void SetCntl(brpc::Controller* cntl) = 0;
    virtual void SetResponse(Message* response) = 0;
    virtual void SetRetriedTimes(uint16_t retriedTimes) = 0;

    static  void SetFailureRequestOption(FailureRequestOption_t  failreqopt) {
        failReqOpt_ = failreqopt;
    }

 protected:
    static FailureRequestOption_t  failReqOpt_;
};

class WriteChunkClosure : public ClientClosure {
 public:
    WriteChunkClosure(CopysetClient *client, Closure *done)
        : client_(client),
          done_(done) {}

    void Run();
    void SetCntl(brpc::Controller* cntl) {
        cntl_ = cntl;
    }
    void SetResponse(Message* response) {
        response_ = dynamic_cast<ChunkResponse *>(response);
    }
    void SetRetriedTimes(uint16_t retriedTimes) {
        retriedTimes_ = retriedTimes;
    }

 private:
    // 已经重试了几次
    uint16_t             retriedTimes_;
    brpc::Controller    *cntl_;
    ChunkResponse       *response_;
    CopysetClient       *client_;
    Closure             *done_;
};

class ReadChunkClosure : public ClientClosure {
 public:
    ReadChunkClosure(CopysetClient *client, Closure *done)
        : client_(client),
          done_(done) {}

    void Run();
    void SetCntl(brpc::Controller* cntl) {
        cntl_ = cntl;
    }
    void SetResponse(Message* response) {
        response_ = dynamic_cast<ChunkResponse *>(response);
    }
    void SetRetriedTimes(uint16_t retriedTimes) {
        retriedTimes_ = retriedTimes;
    }

 private:
    // 已经重试了几次
    uint16_t             retriedTimes_;
    brpc::Controller    *cntl_;
    ChunkResponse       *response_;
    CopysetClient       *client_;
    Closure             *done_;
};

class ReadChunkSnapClosure : public ClientClosure {
 public:
    ReadChunkSnapClosure(CopysetClient *client, Closure *done)
        : client_(client),
          done_(done) {}

    void Run();
    void SetCntl(brpc::Controller* cntl) {
        cntl_ = cntl;
    }
    void SetResponse(Message* response) {
        response_ = dynamic_cast<ChunkResponse *>(response);
    }
    void SetRetriedTimes(uint16_t retriedTimes) {
        retriedTimes_ = retriedTimes;
    }

 private:
    // 已经重试了几次
    uint16_t             retriedTimes_;
    brpc::Controller    *cntl_;
    ChunkResponse       *response_;
    CopysetClient       *client_;
    Closure             *done_;
};

class DeleteChunkSnapClosure : public ClientClosure {
 public:
    DeleteChunkSnapClosure(CopysetClient *client, Closure *done)
        : client_(client),
          done_(done) {}

    void Run();
    void SetCntl(brpc::Controller* cntl) {
        cntl_ = cntl;
    }
    void SetResponse(Message* response) {
        response_ = dynamic_cast<ChunkResponse *>(response);
    }
    void SetRetriedTimes(uint16_t retriedTimes) {
        retriedTimes_ = retriedTimes;
    }

 private:
    // 已经重试了几次
    uint16_t             retriedTimes_;
    brpc::Controller    *cntl_;
    ChunkResponse       *response_;
    CopysetClient       *client_;
    Closure             *done_;
};

class GetChunkInfoClusure : public ClientClosure {
 public:
    GetChunkInfoClusure(CopysetClient *client,
                        Closure *done)
        : client_(client),
          done_(done) {}

    void Run();
    void SetCntl(brpc::Controller* cntl) {
        cntl_ = cntl;
    }
    void SetResponse(Message* response) {
        response_ = dynamic_cast<GetChunkInfoResponse *> (response);
    }
    void SetRetriedTimes(uint16_t retriedTimes) {
        retriedTimes_ = retriedTimes;
    }

 private:
    // 已经重试了几次
    uint16_t                 retriedTimes_;
    brpc::Controller        *cntl_;
    GetChunkInfoResponse    *response_;
    CopysetClient           *client_;
    Closure                 *done_;
};

}   // namespace client
}   // namespace curve

#endif  // CURVE_CLIENT_CHUNK_CLOSURE_H
