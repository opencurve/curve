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
 * Project: curve
 * Created Date: 2021-09-05
 * Author: wanghai01
 */


#ifndef CURVEFS_TEST_TOOLS_MOCK_TOPOLOGY_SERVICE_H_
#define CURVEFS_TEST_TOOLS_MOCK_TOPOLOGY_SERVICE_H_

#include <gmock/gmock.h>
#include "curvefs/proto/topology.pb.h"

namespace curvefs {
namespace mds {
namespace topology {

using ::google::protobuf::RpcController;
using ::google::protobuf::Closure;

class MockTopologyService : public TopologyService {
 public:
    MockTopologyService() : TopologyService() {}
    ~MockTopologyService() = default;

    MOCK_METHOD4(ListPool, void(RpcController *controller,
                                const ListPoolRequest *request,
                                ListPoolResponse *response,
                                Closure *done));
    MOCK_METHOD4(CreatePool, void(RpcController *controller,
                                  const CreatePoolRequest *request,
                                  CreatePoolResponse *response,
                                  Closure *done));
    MOCK_METHOD4(ListPoolZone, void(RpcController *controller,
                                    const ListPoolZoneRequest *request,
                                    ListPoolZoneResponse *response,
                                    Closure *done));
    MOCK_METHOD4(CreateZone, void(RpcController *controller,
                                  const CreateZoneRequest *request,
                                  CreateZoneResponse *response,
                                  Closure *done));
    MOCK_METHOD4(ListZoneServer, void(RpcController *controller,
                                      const ListZoneServerRequest *request,
                                      ListZoneServerResponse *response,
                                      Closure *done));
    MOCK_METHOD4(RegistServer, void(RpcController *controller,
                                    const ServerRegistRequest *request,
                                    ServerRegistResponse *response,
                                    Closure *done));
    MOCK_METHOD4(ListMetaServer, void(RpcController *controller,
                                      const ListMetaServerRequest *request,
                                      ListMetaServerResponse *response,
                                      Closure *done));
    MOCK_METHOD4(RegistMetaServer, void(RpcController *controller,
                                        const MetaServerRegistRequest *request,
                                        MetaServerRegistResponse *response,
                                        Closure *done));
};

}  // namespace topology
}  // namespace mds
}  // namespace curvefs

#endif  // CURVEFS_TEST_TOOLS_MOCK_TOPOLOGY_SERVICE_H_
