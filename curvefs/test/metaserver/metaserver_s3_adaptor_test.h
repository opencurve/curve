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
 * Project: curve
 * Created Date: 2021-8-13
 * Author: chengyi
 */
#ifndef CURVEFS_TEST_METASERVER_METASERVER_S3_ADAPTOR_TEST_H_
#define CURVEFS_TEST_METASERVER_METASERVER_S3_ADAPTOR_TEST_H_

#include <brpc/server.h>
#include <google/protobuf/util/message_differencer.h>
#include <gtest/gtest.h>

#include <functional>
#include <set>
#include <string>

#include "curvefs/src/client/s3/client_s3_adaptor.h"
#include "curvefs/src/metaserver/s3/metaserver_s3_adaptor.h"
#include "curvefs/test/client/mock_client_s3.h"
#include "curvefs/test/client/mock_metaserver_service.h"
#include "curvefs/test/client/mock_spacealloc_service.h"
#include "curvefs/test/metaserver/mock_metaserver_s3.h"

namespace curvefs {
namespace metaserver {

using ::testing::_;
using ::testing::DoAll;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::SetArgPointee;

template <typename RpcRequestType, typename RpcResponseType,
          bool RpcFailed = false>
void S3RpcService(google::protobuf::RpcController* cntl_base,
                  const RpcRequestType* request, RpcResponseType* response,
                  google::protobuf::Closure* done) {
    if (RpcFailed) {
        brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);
        cntl->SetFailed(112, "Not connected to");
    }
    LOG(INFO) << "run s3 prc service, response.chunkid:" << response->chunkid();
    done->Run();
}

// use global_chunk_id_ to record chunk id
uint64_t global_chunk_id_ = 0;
template <typename RpcRequestType, typename RpcResponseType,
          bool RpcFailed = false>
void S3RpcService_ChunkId(google::protobuf::RpcController* cntl_base,
                          const RpcRequestType* request,
                          RpcResponseType* response,
                          google::protobuf::Closure* done) {
    if (RpcFailed) {
        brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);
        cntl->SetFailed(112, "Not connected to");
    }
    response->set_chunkid(++global_chunk_id_);
    LOG(INFO) << "run s3 prc service, response.chunkid: "
              << response->chunkid();
    done->Run();
}

// use global_version_ to record version
uint64_t global_version_ = 0;
template <typename RpcRequestType, typename RpcResponseType,
          bool RpcFailed = false>
void S3RpcService_Version(google::protobuf::RpcController* cntl_base,
                          const RpcRequestType* request,
                          RpcResponseType* response,
                          google::protobuf::Closure* done) {
    if (RpcFailed) {
        brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);
        cntl->SetFailed(112, "Not connected to");
    }
    response->set_version(++global_version_);
    LOG(INFO) << "run s3 prc service, response.version:" << response->version();
    done->Run();
}

}  // namespace metaserver
}  // namespace curvefs
#endif  // CURVEFS_TEST_METASERVER_METASERVER_S3_ADAPTOR_TEST_H_
