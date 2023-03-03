/*
 *  Copyright (c) 2021 NetEase Inc.
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
 * @Project: curve
 * @Date: 2021-09-14 16:54:35
 * @Author: chenwei
 */

#ifndef CURVEFS_TEST_CLIENT_RPCCLIENT_MOCK_TOPOLOGY_SERVICE_H_
#define CURVEFS_TEST_CLIENT_RPCCLIENT_MOCK_TOPOLOGY_SERVICE_H_

#include <gmock/gmock.h>
#include "curvefs/proto/topology.pb.h"

namespace curvefs {
namespace client {
namespace rpcclient {
class MockTopologyService : public curvefs::mds::topology::TopologyService {
 public:
    MockTopologyService() : TopologyService() {}
    ~MockTopologyService() = default;

    MOCK_METHOD4(
        GetMetaServer,
        void(::google::protobuf::RpcController* controller,
             const ::curvefs::mds::topology::GetMetaServerInfoRequest* request,
             ::curvefs::mds::topology::GetMetaServerInfoResponse* response,
             ::google::protobuf::Closure* done));
    MOCK_METHOD4(
        GetMetaServerListInCopysets,
        void(::google::protobuf::RpcController* controller,
             const ::curvefs::mds::topology::GetMetaServerListInCopySetsRequest*
                 request,
             ::curvefs::mds::topology::GetMetaServerListInCopySetsResponse*
                 response,
             ::google::protobuf::Closure* done));
    MOCK_METHOD4(
        CreatePartition,
        void(::google::protobuf::RpcController* controller,
             const ::curvefs::mds::topology::CreatePartitionRequest* request,
             ::curvefs::mds::topology::CreatePartitionResponse* response,
             ::google::protobuf::Closure* done));
    MOCK_METHOD4(
        ListPartition,
        void(::google::protobuf::RpcController* controller,
             const ::curvefs::mds::topology::ListPartitionRequest* request,
             ::curvefs::mds::topology::ListPartitionResponse* response,
             ::google::protobuf::Closure* done));
    MOCK_METHOD4(
        GetCopysetOfPartition,
        void(::google::protobuf::RpcController* controller,
             const ::curvefs::mds::topology::GetCopysetOfPartitionRequest*
                 request,
             ::curvefs::mds::topology::GetCopysetOfPartitionResponse* response,
             ::google::protobuf::Closure* done));
    MOCK_METHOD4(
        AllocOrGetMemcacheCluster,
        void(::google::protobuf::RpcController* controller,
             const ::curvefs::mds::topology::AllocOrGetMemcacheClusterRequest*
                 request,
             ::curvefs::mds::topology::AllocOrGetMemcacheClusterResponse*
                 response,
             ::google::protobuf::Closure* done));
};
}  // namespace rpcclient
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_TEST_CLIENT_RPCCLIENT_MOCK_TOPOLOGY_SERVICE_H_
