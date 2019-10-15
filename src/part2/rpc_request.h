/*
 * Project: nebd
 * File Created: 2019-09-30
 * Author: hzwuhongsong
 * Copyright (c) 2019 NetEase
 */

#ifndef SRC_PART2_RPC_REQUEST_H_
#define SRC_PART2_RPC_REQUEST_H_

#include <gflags/gflags.h>
#include <butil/logging.h>
#include <brpc/server.h>
#include <rbd/librbd.h>
#include <rados/librados.h>
#include <string>
#include "src/common/client.pb.h"

struct AsyncWrite {
    google::protobuf::Closure* done;
    const nebd::client::WriteRequest* request;
    nebd::client::WriteResponse* response;
    brpc::Controller* cntl;
    std::string buf;
};

struct AsyncRead {
    google::protobuf::Closure* done;
    const nebd::client::ReadRequest* request;
    nebd::client::ReadResponse* response;
    brpc::Controller* cntl;
    char* buf;
};

struct AsyncDiscard {
    google::protobuf::Closure* done;
    const nebd::client::DiscardRequest* request;
    nebd::client::DiscardResponse* response;
};

struct AsyncFlush {
    google::protobuf::Closure* done;
    const nebd::client::FlushRequest* request;
    nebd::client::FlushResponse* response;
};

class RpcRequest {
 public:
    RpcRequest() {}
    virtual ~RpcRequest() {}
    virtual int OpenFile(char* filename) {
        return 0;
    }

    virtual int Write(AsyncWrite* writejob) {
        return 0;
    }

    virtual int Read(AsyncRead* readjob) {
        return 0;
    }

    virtual int StatFile(int fd, nebd::client::StatFileResponse* response) {
        return 0;
    }

    virtual int GetInfo(int fd, nebd::client::GetInfoResponse* response) {
        return 0;
    }

    virtual int CloseFile(int fd) {
        return 0;
    }

    virtual int Flush(AsyncFlush* flushjob) {
        return 0;
    }

    virtual int Resize(int fd, uint64_t size) {
        return 0;
    }

    virtual int Discard(AsyncDiscard* discardjob) {
        return 0;
    }

    virtual int InvalidateCache(int fd) {
        return 0;
    }
};

#endif  // SRC_PART2_RPC_REQUEST_H_
