/*
 *  Copyright (c) 2023 NetEase Inc.
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

#ifndef TEST_CLIENT_MOCK_MOCK_TOPOLOGY_SERVICE_H_
#define TEST_CLIENT_MOCK_MOCK_TOPOLOGY_SERVICE_H_

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "proto/topology.pb.h"

namespace curve {
namespace client {

class MockTopologyService : public mds::topology::TopologyService {
 public:
    MOCK_METHOD4(ListPoolset,
        void(::google::protobuf::RpcController* controller,
            const curve::mds::topology::ListPoolsetRequest* request,
            curve::mds::topology::ListPoolsetResponse* response,
            ::google::protobuf::Closure* done));

    MOCK_METHOD4(GetChunkServerListInCopySets,
        void(::google::protobuf::RpcController* controller,
            const curve::mds::topology::GetChunkServerListInCopySetsRequest*
                request,
            curve::mds::topology::GetChunkServerListInCopySetsResponse*
                response,
            ::google::protobuf::Closure* done));

    MOCK_METHOD4(GetClusterInfo,
        void(::google::protobuf::RpcController* controller,
            const curve::mds::topology::GetClusterInfoRequest* request,
            curve::mds::topology::GetClusterInfoResponse* response,
            ::google::protobuf::Closure* done));

    MOCK_METHOD4(GetChunkServer,
        void(::google::protobuf::RpcController* controller,
            const curve::mds::topology::GetChunkServerInfoRequest* request,
            curve::mds::topology::GetChunkServerInfoResponse* response,
            ::google::protobuf::Closure* done));

    MOCK_METHOD4(ListChunkServer,
      void(::google::protobuf::RpcController* controller,
          const curve::mds::topology::ListChunkServerRequest* request,
          curve::mds::topology::ListChunkServerResponse* response,
          ::google::protobuf::Closure* done));
};

}  // namespace client
}  // namespace curve

#endif  // TEST_CLIENT_MOCK_MOCK_TOPOLOGY_SERVICE_H_
