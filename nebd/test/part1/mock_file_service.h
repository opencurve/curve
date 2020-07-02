/*
 * Project: nebd
 * Created Date: 2019-10-11
 * Author: hzchenwei7
 * Copyright (c) 2018 netease
 */

#ifndef TESTS_PART1_MOCK_FILE_SERVICE_H_
#define TESTS_PART1_MOCK_FILE_SERVICE_H_

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <string>
#include "nebd/proto/client.pb.h"

namespace nebd {
namespace client {

class MockNebdFileService : public NebdFileService {
 public:
    MockNebdFileService() : NebdFileService() {}
    ~MockNebdFileService() = default;

    MOCK_METHOD4(OpenFile, void(::google::protobuf::RpcController* controller,
                       const ::nebd::client::OpenFileRequest* request,
                       ::nebd::client::OpenFileResponse* response,
                       ::google::protobuf::Closure* done));
    MOCK_METHOD4(CloseFile, void(::google::protobuf::RpcController* controller,
                       const ::nebd::client::CloseFileRequest* request,
                       ::nebd::client::CloseFileResponse* response,
                       ::google::protobuf::Closure* done));
    MOCK_METHOD4(Read, void(::google::protobuf::RpcController* controller,
                       const ::nebd::client::ReadRequest* request,
                       ::nebd::client::ReadResponse* response,
                       ::google::protobuf::Closure* done));
    MOCK_METHOD4(Write, void(::google::protobuf::RpcController* controller,
                       const ::nebd::client::WriteRequest* request,
                       ::nebd::client::WriteResponse* response,
                       ::google::protobuf::Closure* done));
    MOCK_METHOD4(Discard, void(::google::protobuf::RpcController* controller,
                       const ::nebd::client::DiscardRequest* request,
                       ::nebd::client::DiscardResponse* response,
                       ::google::protobuf::Closure* done));
    MOCK_METHOD4(ResizeFile, void(::google::protobuf::RpcController* controller,
                       const ::nebd::client::ResizeRequest* request,
                       ::nebd::client::ResizeResponse* response,
                       ::google::protobuf::Closure* done));
    MOCK_METHOD4(Flush, void(::google::protobuf::RpcController* controller,
                       const ::nebd::client::FlushRequest* request,
                       ::nebd::client::FlushResponse* response,
                       ::google::protobuf::Closure* done));
    MOCK_METHOD4(GetInfo, void(::google::protobuf::RpcController* controller,
                       const ::nebd::client::GetInfoRequest* request,
                       ::nebd::client::GetInfoResponse* response,
                       ::google::protobuf::Closure* done));
    MOCK_METHOD4(InvalidateCache, void(
                        ::google::protobuf::RpcController* controller,
                       const ::nebd::client::InvalidateCacheRequest* request,
                       ::nebd::client::InvalidateCacheResponse* response,
                       ::google::protobuf::Closure* done));
};
}   // namespace client
}   // namespace nebd

#endif  // TESTS_PART1_MOCK_FILE_SERVICE_H_
