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
 * @Date: 2021-06-11 16:53:49
 * @Author: chenwei
 */
#ifndef CURVEFS_TEST_MDS_FAKE_SPACE_H_
#define CURVEFS_TEST_MDS_FAKE_SPACE_H_

#include <brpc/closure_guard.h>
#include <brpc/controller.h>
#include "curvefs/proto/space.pb.h"

namespace curvefs {
namespace space {
class FakeSpaceImpl : public SpaceAllocService {
 public:
    FakeSpaceImpl() {
        initCount = 0;
        allocCount = 0;
        deallocateCount = 0;
        statCount = 0;
        uninitCount = 0;
    }
    void InitSpace(::google::protobuf::RpcController* controller,
                       const ::curvefs::space::InitSpaceRequest* request,
                       ::curvefs::space::InitSpaceResponse* response,
                       ::google::protobuf::Closure* done);
    void AllocateSpace(::google::protobuf::RpcController* controller,
                       const ::curvefs::space::AllocateSpaceRequest* request,
                       ::curvefs::space::AllocateSpaceResponse* response,
                       ::google::protobuf::Closure* done);
    void DeallocateSpace(::google::protobuf::RpcController* controller,
                       const ::curvefs::space::DeallocateSpaceRequest* request,
                       ::curvefs::space::DeallocateSpaceResponse* response,
                       ::google::protobuf::Closure* done);
    void StatSpace(::google::protobuf::RpcController* controller,
                       const ::curvefs::space::StatSpaceRequest* request,
                       ::curvefs::space::StatSpaceResponse* response,
                       ::google::protobuf::Closure* done);
    void UnInitSpace(::google::protobuf::RpcController* controller,
                       const ::curvefs::space::UnInitSpaceRequest* request,
                       ::curvefs::space::UnInitSpaceResponse* response,
                       ::google::protobuf::Closure* done);
    uint32_t initCount;
    uint32_t allocCount;
    uint32_t deallocateCount;
    uint32_t statCount;
    uint32_t uninitCount;
};

}  // namespace space
}  // namespace curvefs
#endif  // CURVEFS_TEST_MDS_FAKE_SPACE_H_
