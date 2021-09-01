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
 * Created Date: Thur Jun 17 2021
 * Author: lixiaocui
 */


#ifndef CURVEFS_TEST_CLIENT_RPCCLIENT_MOCK_SPACEALLOC_SERVICE_H_
#define CURVEFS_TEST_CLIENT_RPCCLIENT_MOCK_SPACEALLOC_SERVICE_H_

#include <gmock/gmock.h>
#include "curvefs/proto/space.pb.h"

namespace curvefs {
namespace client {
namespace rpcclient {
class MockSpaceAllocService : public curvefs::space::SpaceAllocService {
 public:
    MockSpaceAllocService() : SpaceAllocService() {}
    ~MockSpaceAllocService() = default;

    MOCK_METHOD4(AllocateSpace,
                 void(::google::protobuf::RpcController *controller,
                      const ::curvefs::space::AllocateSpaceRequest *request,
                      ::curvefs::space::AllocateSpaceResponse *response,
                      ::google::protobuf::Closure *done));
    MOCK_METHOD4(DeallocateSpace,
                 void(::google::protobuf::RpcController *controller,
                      const ::curvefs::space::DeallocateSpaceRequest *request,
                      ::curvefs::space::DeallocateSpaceResponse *response,
                      ::google::protobuf::Closure *done));
    MOCK_METHOD4(AllocateS3Chunk,
                 void(::google::protobuf::RpcController *controller,
                      const ::curvefs::space::AllocateS3ChunkRequest *request,
                      ::curvefs::space::AllocateS3ChunkResponse *response,
                      ::google::protobuf::Closure *done));
};
}  // namespace rpcclient
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_TEST_CLIENT_RPCCLIENT_MOCK_SPACEALLOC_SERVICE_H_
