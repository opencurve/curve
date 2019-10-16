/*
 * Project: nebd
 * Created Date: 2019-08-12
 * Author: hzchenwei7
 * Copyright (c) 2018 netease
 */

#ifndef  TESTS_PART1_FAKE_CLIENT_SERVICE_H_
#define  TESTS_PART1_FAKE_CLIENT_SERVICE_H_

#include <brpc/closure_guard.h>
#include <brpc/controller.h>
#include <string>
#include "src/common/client.pb.h"

namespace nebd {
namespace client {

class ClientService: public QemuClientService {
 public:
    ClientService() {}

    virtual ~ClientService() {}

    void OpenFile(::google::protobuf::RpcController* controller,
                       const ::nebd::client::OpenFileRequest* request,
                       ::nebd::client::OpenFileResponse* response,
                       ::google::protobuf::Closure* done) override;

    void CloseFile(::google::protobuf::RpcController* controller,
                       const ::nebd::client::CloseFileRequest* request,
                       ::nebd::client::CloseFileResponse* response,
                       ::google::protobuf::Closure* done) override;

    void Read(::google::protobuf::RpcController* controller,
                       const ::nebd::client::ReadRequest* request,
                       ::nebd::client::ReadResponse* response,
                       ::google::protobuf::Closure* done) override;

    void Write(::google::protobuf::RpcController* controller,
                       const ::nebd::client::WriteRequest* request,
                       ::nebd::client::WriteResponse* response,
                       ::google::protobuf::Closure* done) override;

    void Discard(::google::protobuf::RpcController* controller,
                       const ::nebd::client::DiscardRequest* request,
                       ::nebd::client::DiscardResponse* response,
                       ::google::protobuf::Closure* done) override;

    void StatFile(::google::protobuf::RpcController* controller,
                       const ::nebd::client::StatFileRequest* request,
                       ::nebd::client::StatFileResponse* response,
                       ::google::protobuf::Closure* done) override;

    void ResizeFile(::google::protobuf::RpcController* controller,
                       const ::nebd::client::ResizeRequest* request,
                       ::nebd::client::ResizeResponse* response,
                       ::google::protobuf::Closure* done) override;

    void Flush(::google::protobuf::RpcController* controller,
                       const ::nebd::client::FlushRequest* request,
                       ::nebd::client::FlushResponse* response,
                       ::google::protobuf::Closure* done) override;

    void GetInfo(::google::protobuf::RpcController* controller,
                       const ::nebd::client::GetInfoRequest* request,
                       ::nebd::client::GetInfoResponse* response,
                       ::google::protobuf::Closure* done) override;

    void InvalidateCache(::google::protobuf::RpcController* controller,
                       const ::nebd::client::InvalidateCacheRequest* request,
                       ::nebd::client::InvalidateCacheResponse* response,
                       ::google::protobuf::Closure* done) override;
};
}  // namespace client
}  // namespace nebd
#endif   // TESTS_PART1_FAKE_CLIENT_SERVICE_H_
