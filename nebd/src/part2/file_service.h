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
 * Created Date: Thursday January 16th 2020
 * Author: yangyaokai
 */

#ifndef NEBD_SRC_PART2_FILE_SERVICE_H_
#define NEBD_SRC_PART2_FILE_SERVICE_H_

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <vector>
#include <string>
#include <memory>

#include "nebd/proto/client.pb.h"
#include "nebd/src/part2/file_manager.h"

namespace nebd {
namespace server {

void NebdFileServiceCallback(NebdServerAioContext* context);

class NebdFileServiceImpl : public nebd::client::NebdFileService {
 public:
    explicit NebdFileServiceImpl(std::shared_ptr<NebdFileManager> fileManager,
                                 const bool returnRpcWhenIoError)
                                 : fileManager_(fileManager),
                                 returnRpcWhenIoError_(returnRpcWhenIoError) {}

    virtual ~NebdFileServiceImpl() {}

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

 private:
    std::shared_ptr<NebdFileManager> fileManager_;
    const bool returnRpcWhenIoError_;
};

}  // namespace server
}  // namespace nebd

#endif  // NEBD_SRC_PART2_FILE_SERVICE_H_
