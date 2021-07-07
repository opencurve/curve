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
 * @Date: 2021-06-11 14:43:05
 * @Author: chenwei
 */

#ifndef CURVEFS_TEST_MDS_MOCK_SPACE_H_
#define CURVEFS_TEST_MDS_MOCK_SPACE_H_
#include <gmock/gmock.h>
#include "curvefs/proto/space.pb.h"

namespace curvefs {
namespace space {
class MockSpaceService : public SpaceAllocService {
 public:
    MOCK_METHOD4(InitSpace,
                void(::google::protobuf::RpcController* controller,
                const ::curvefs::space::InitSpaceRequest* request,
                ::curvefs::space::InitSpaceResponse* response,
                ::google::protobuf::Closure* done));
    MOCK_METHOD4(AllocateSpace,
                void(::google::protobuf::RpcController* controller,
                const ::curvefs::space::AllocateSpaceRequest* request,
                ::curvefs::space::AllocateSpaceResponse* response,
                ::google::protobuf::Closure* done));
    MOCK_METHOD4(DeallocateSpace,
                void(::google::protobuf::RpcController* controller,
                const ::curvefs::space::DeallocateSpaceRequest* request,
                ::curvefs::space::DeallocateSpaceResponse* response,
                ::google::protobuf::Closure* done));
    MOCK_METHOD4(StatSpace,
                void(::google::protobuf::RpcController* controller,
                const ::curvefs::space::StatSpaceRequest* request,
                ::curvefs::space::StatSpaceResponse* response,
                ::google::protobuf::Closure* done));
    MOCK_METHOD4(UnInitSpace,
                void(::google::protobuf::RpcController* controller,
                const ::curvefs::space::UnInitSpaceRequest* request,
                ::curvefs::space::UnInitSpaceResponse* response,
                ::google::protobuf::Closure* done));
};
}  // namespace space
}  // namespace curvefs
#endif  // CURVEFS_TEST_MDS_MOCK_SPACE_H_
