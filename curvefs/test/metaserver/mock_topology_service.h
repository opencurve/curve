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
 * @Date: 2021-09-24 14:05:44
 * @Author: chenwei
 */

#ifndef CURVEFS_TEST_METASERVER_MOCK_TOPOLOGY_SERVICE_H_
#define CURVEFS_TEST_METASERVER_MOCK_TOPOLOGY_SERVICE_H_

#include <gmock/gmock.h>
#include "curvefs/proto/topology.pb.h"

namespace curvefs {
namespace mds {
namespace topology {

class MockTopologyService : public TopologyService {
 public:
    MockTopologyService() : TopologyService() {}
    ~MockTopologyService() = default;

    MOCK_METHOD4(RegistMetaServer,
                 void(google::protobuf::RpcController* cntl_base,
                      const MetaServerRegistRequest* request,
                      MetaServerRegistResponse* response,
                      google::protobuf::Closure* done));
};
}  // namespace topology
}  // namespace mds
}  // namespace curvefs

#endif  // CURVEFS_TEST_METASERVER_MOCK_TOPOLOGY_SERVICE_H_
