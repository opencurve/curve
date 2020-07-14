/*
 *  Copyright (c) 2020 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * Project: nebd
 * Created Date: 2019-08-12
 * Author: hzchenwei7
 */

#ifndef  NEBD_TEST_PART1_FAKE_FILE_SERVICE_H_
#define  NEBD_TEST_PART1_FAKE_FILE_SERVICE_H_

#include <brpc/closure_guard.h>
#include <brpc/controller.h>
#include <string>
#include "nebd/proto/client.pb.h"

namespace nebd {
namespace client {

class FakeNebdFileService: public NebdFileService {
 public:
    FakeNebdFileService() {}

    virtual ~FakeNebdFileService() {}

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

 private:
    int64_t fileSize_;
};
}  // namespace client
}  // namespace nebd
#endif   // NEBD_TEST_PART1_FAKE_FILE_SERVICE_H_
