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

#ifndef CURVEFS_SRC_SPACE_ALLOCATOR_SPACE_ALLOC_SERVICE_H_
#define CURVEFS_SRC_SPACE_ALLOCATOR_SPACE_ALLOC_SERVICE_H_

#include "curvefs/proto/space.pb.h"
#include "curvefs/src/space_allocator/space_manager.h"

namespace curvefs {
namespace space {

class SpaceAllocServiceImpl : public curvefs::space::SpaceAllocService {
 public:
    explicit SpaceAllocServiceImpl(SpaceManager* space);

    ~SpaceAllocServiceImpl() = default;

    void InitSpace(::google::protobuf::RpcController* controller,
                   const ::curvefs::space::InitSpaceRequest* request,
                   ::curvefs::space::InitSpaceResponse* response,
                   ::google::protobuf::Closure* done) override;

    void UnInitSpace(::google::protobuf::RpcController* controller,
                     const ::curvefs::space::UnInitSpaceRequest* request,
                     ::curvefs::space::UnInitSpaceResponse* response,
                     ::google::protobuf::Closure* done) override;

    void AllocateSpace(::google::protobuf::RpcController* controller,
                       const ::curvefs::space::AllocateSpaceRequest* request,
                       ::curvefs::space::AllocateSpaceResponse* response,
                       ::google::protobuf::Closure* done) override;

    void DeallocateSpace(
        ::google::protobuf::RpcController* controller,
        const ::curvefs::space::DeallocateSpaceRequest* request,
        ::curvefs::space::DeallocateSpaceResponse* response,
        ::google::protobuf::Closure* done) override;

    void StatSpace(::google::protobuf::RpcController* controller,
                   const ::curvefs::space::StatSpaceRequest* request,
                   ::curvefs::space::StatSpaceResponse* response,
                   ::google::protobuf::Closure* done) override;

 private:
    static SpaceAllocateHint ToSpaceAllocateHint(const AllocateHint& hint);

 private:
    SpaceManager* space_;
};

}  // namespace space
}  // namespace curvefs

#endif  // CURVEFS_SRC_SPACE_ALLOCATOR_SPACE_ALLOC_SERVICE_H_
