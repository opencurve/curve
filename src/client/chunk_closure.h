/*
 * Project: curve
 * Created Date: 18-9-27
 * Author: wudemiao
 * Copyright (c) 2018 netease
 */

#ifndef SRC_CLIENT_CHUNK_CLOSURE_H_
#define SRC_CLIENT_CHUNK_CLOSURE_H_

#include <google/protobuf/stubs/callback.h>
#include <brpc/controller.h>
#include <bthread/bthread.h>
#include <brpc/errno.pb.h>

#include "proto/chunk.pb.h"
#include "src/client/client_config.h"
#include "src/client/client_common.h"

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
    ClientClosure(CopysetClient *client, Closure *done):
                                        client_(client),
                                        done_(done) {
    }
    virtual void SetCntl(brpc::Controller* cntl) = 0;
    virtual void SetResponse(Message* response) = 0;
    virtual void SetRetriedTimes(uint16_t retriedTimes) = 0;

    static void SetFailureRequestOption(FailureRequestOption_t failRequestOpt) {
        failReqOpt_ = failRequestOpt;
    }

 protected:
    static FailureRequestOption_t  failReqOpt_;
    static ChunkserverClientMetric_t chunkserverClientMetric_;

    // 已经重试了几次
    uint16_t                 retriedTimes_;
    brpc::Controller*        cntl_;
    ChunkResponse*           response_;
    CopysetClient*           client_;
    Closure*                 done_;
};

class WriteChunkClosure : public ClientClosure {
 public:
    WriteChunkClosure(CopysetClient *client, Closure *done)
        : ClientClosure(client, done) {}

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
};

class ReadChunkClosure : public ClientClosure {
 public:
    ReadChunkClosure(CopysetClient *client, Closure *done)
        : ClientClosure(client, done) {}

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
};

class ReadChunkSnapClosure : public ClientClosure {
 public:
    ReadChunkSnapClosure(CopysetClient *client, Closure *done)
        : ClientClosure(client, done) {}

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
};

class DeleteChunkSnapClosure : public ClientClosure {
 public:
    DeleteChunkSnapClosure(CopysetClient *client, Closure *done)
        : ClientClosure(client, done) {}

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
};

class GetChunkInfoClosure : public ClientClosure {
 public:
    GetChunkInfoClosure(CopysetClient *client,
                        Closure *done)
        : ClientClosure(client, done) {}

    void Run();
    void SetCntl(brpc::Controller* cntl) {
        cntl_ = cntl;
    }
    void SetResponse(Message* response) {
        chunkinforesponse_ = dynamic_cast<GetChunkInfoResponse *> (response);
    }
    void SetRetriedTimes(uint16_t retriedTimes) {
        retriedTimes_ = retriedTimes;
    }

 private:
    GetChunkInfoResponse*    chunkinforesponse_;
};

class CreateCloneChunkClosure : public ClientClosure {
 public:
    CreateCloneChunkClosure(CopysetClient *client,
                        Closure *done)
        : ClientClosure(client, done) {}

    void Run();
    void SetCntl(brpc::Controller* cntl) {
        cntl_ = cntl;
    }
    void SetResponse(Message* response) {
        response_ = dynamic_cast<ChunkResponse *> (response);
    }
    void SetRetriedTimes(uint16_t retriedTimes) {
        retriedTimes_ = retriedTimes;
    }
};

class RecoverChunkClosure : public ClientClosure {
 public:
    RecoverChunkClosure(CopysetClient *client,
                        Closure *done)
        : ClientClosure(client, done) {}

    void Run();
    void SetCntl(brpc::Controller* cntl) {
        cntl_ = cntl;
    }
    void SetResponse(Message* response) {
        response_ = dynamic_cast<ChunkResponse *> (response);
    }
    void SetRetriedTimes(uint16_t retriedTimes) {
        retriedTimes_ = retriedTimes;
    }
};

}   // namespace client
}   // namespace curve

#endif  // SRC_CLIENT_CHUNK_CLOSURE_H_
