/*
 * Project: nebd
 * File Created: 2019-09-30
 * Author: hzwuhongsong
 * Copyright (c) 2019 NetEase
 */

#ifndef SRC_PART2_RPC_SERVER_H_
#define SRC_PART2_RPC_SERVER_H_

#include <gflags/gflags.h>
#include <butil/logging.h>
#include <brpc/server.h>
#include <vector>
#include <string>
#include "src/common/client.pb.h"
#include "src/part2/rpc_ceph.h"

class QemuClientServiceImpl : public nebd::client::QemuClientService {
 public:
    QemuClientServiceImpl() {request_ceph = new RpcRequestCeph;}
    virtual ~QemuClientServiceImpl() {delete request_ceph;}
    virtual void OpenFile(google::protobuf::RpcController* cntl_base,
                          const nebd::client::OpenFileRequest* request,
                          nebd::client::OpenFileResponse* response,
                          google::protobuf::Closure* done);

    virtual void Write(google::protobuf::RpcController* cntl_base,
                       const nebd::client::WriteRequest* request,
                       nebd::client::WriteResponse* response,
                       google::protobuf::Closure* done);

    virtual void Read(google::protobuf::RpcController* cntl_base,
                      const nebd::client::ReadRequest* request,
                      nebd::client::ReadResponse* response,
                      google::protobuf::Closure* done);

    virtual void StatFile(google::protobuf::RpcController* cntl_base,
                          const nebd::client::StatFileRequest* request,
                          nebd::client::StatFileResponse* response,
                          google::protobuf::Closure* done);

    virtual void GetInfo(google::protobuf::RpcController* cntl_base,
                         const nebd::client::GetInfoRequest* request,
                         nebd::client::GetInfoResponse* response,
                         google::protobuf::Closure* done);

    virtual void Flush(google::protobuf::RpcController* cntl_base,
                       const nebd::client::FlushRequest* request,
                       nebd::client::FlushResponse* response,
                       google::protobuf::Closure* done);

    virtual void CloseFile(google::protobuf::RpcController* cntl_base,
                           const nebd::client::CloseFileRequest* request,
                           nebd::client::CloseFileResponse* response,
                           google::protobuf::Closure* done);

    virtual void Discard(google::protobuf::RpcController* cntl_base,
                         const nebd::client::DiscardRequest* request,
                         nebd::client::DiscardResponse* response,
                         google::protobuf::Closure* done);

    virtual void ResizeFile(google::protobuf::RpcController* cntl_base,
                            const nebd::client::ResizeRequest* request,
                            nebd::client::ResizeResponse* response,
                            google::protobuf::Closure* done);

    virtual void InvalidateCache(google::protobuf::RpcController* cntl_base,
                            const nebd::client::InvalidateCacheRequest* request,
                            nebd::client::InvalidateCacheResponse* response,
                            google::protobuf::Closure* done);

    void SetRpcRequest(RpcRequestCeph *request) {request_ceph = request;}

 private:
    RpcRequestCeph* request_ceph;
    std::vector<std::string> opening_image;
};

#endif  // SRC_PART2_RPC_SERVER_H_
