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

#include "curvefs/src/space_allocator/space_alloc_service.h"

#include <brpc/closure_guard.h>
#include <brpc/controller.h>

#include <vector>

namespace curvefs {
namespace space {

SpaceAllocServiceImpl::SpaceAllocServiceImpl(SpaceManager* space)
    : space_(space) {}

void SpaceAllocServiceImpl::InitSpace(
    ::google::protobuf::RpcController* controller,
    const ::curvefs::space::InitSpaceRequest* request,
    ::curvefs::space::InitSpaceResponse* response,
    ::google::protobuf::Closure* done) {
    brpc::ClosureGuard guard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);

    auto status =
        space_->InitSpace(
            request->fsinfo().fsid(),
            request->fsinfo().capacity(),
            request->fsinfo().blocksize(),
            request->fsinfo().rootinodeid());

    response->set_status(status);
    if (status != SPACE_OK) {
        LOG(ERROR) << "InitSpace failure, logid: " << cntl->log_id()
                   << ", request: " << request->ShortDebugString()
                   << ", error: " << SpaceStatusCode_Name(status);
    } else {
        LOG(INFO) << "InitSpace success, logid: " << cntl->log_id()
                  << ", request: " << request->ShortDebugString();
    }
}

void SpaceAllocServiceImpl::UnInitSpace(
    ::google::protobuf::RpcController* controller,
    const ::curvefs::space::UnInitSpaceRequest* request,
    ::curvefs::space::UnInitSpaceResponse* response,
    ::google::protobuf::Closure* done) {
    brpc::ClosureGuard guard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);

    auto status = space_->UnInitSpace(request->fsid());
    response->set_status(status);
    if (status != SPACE_OK) {
        LOG(ERROR) << "UnInitSpace failure, " << cntl->log_id()
                   << ", request: " << request->ShortDebugString()
                   << ", error: " << SpaceStatusCode_Name(status);
    } else {
        LOG(INFO) << "UnInitSpace success, " << cntl->log_id()
                  << ", request: " << request->ShortDebugString();
    }
}

void SpaceAllocServiceImpl::AllocateSpace(
    ::google::protobuf::RpcController* controller,
    const ::curvefs::space::AllocateSpaceRequest* request,
    ::curvefs::space::AllocateSpaceResponse* response,
    ::google::protobuf::Closure* done) {
    brpc::ClosureGuard guard(done);

    std::vector<PExtent> exts;
    SpaceAllocateHint hint;
    if (request->has_allochint()) {
        hint = ToSpaceAllocateHint(request->allochint());
    }

    auto status =
        space_->AllocateSpace(request->fsid(), request->size(), hint, &exts);

    response->set_status(status);
    if (status != SPACE_OK) {
        LOG(ERROR) << "AllocateSpace failure, request: "
                   << request->ShortDebugString()
                   << ", error: " << SpaceStatusCode_Name(status);
    } else {
        for (auto& e : exts) {
            auto* p = response->add_extents();
            p->set_offset(e.offset);
            p->set_length(e.len);
        }

        LOG(INFO) << "AllocateSpace success, request: "
                  << request->ShortDebugString()
                  << ", response: " << response->ShortDebugString();
    }
}

void SpaceAllocServiceImpl::DeallocateSpace(
    ::google::protobuf::RpcController* controller,
    const ::curvefs::space::DeallocateSpaceRequest* request,
    ::curvefs::space::DeallocateSpaceResponse* response,
    ::google::protobuf::Closure* done) {
    brpc::ClosureGuard guard(done);

    auto status = space_->DeallocateSpace(request->fsid(), request->extents());

    response->set_status(status);
    if (status != SPACE_OK) {
        LOG(ERROR) << "DeallocateSpace failure, request: "
                   << request->ShortDebugString()
                   << ", error: " << SpaceStatusCode_Name(status);
    }
}

void SpaceAllocServiceImpl::StatSpace(
    ::google::protobuf::RpcController* controller,
    const ::curvefs::space::StatSpaceRequest* request,
    ::curvefs::space::StatSpaceResponse* response,
    ::google::protobuf::Closure* done) {

    brpc::ClosureGuard guard(done);

    uint64_t total = 0;
    uint64_t available = 0;
    uint64_t blkSize = 0;

    auto status =
        space_->StatSpace(request->fsid(), &total, &available, &blkSize);

    response->set_status(status);
    if (status != SPACE_OK) {
        LOG(ERROR) << "StatSpace failure, request: "
                   << request->ShortDebugString()
                   << ", error: " << SpaceStatusCode_Name(status);
    } else {
        response->set_totalblock(total / blkSize);
        response->set_availableblock(available / blkSize);
        response->set_usedblock((total - available) / blkSize);
    }
}

SpaceAllocateHint SpaceAllocServiceImpl::ToSpaceAllocateHint(
    const AllocateHint& hint) {
    SpaceAllocateHint spaceHint;

    if (hint.has_alloctype()) {
        spaceHint.allocType = hint.alloctype();
    }

    if (hint.has_leftoffset()) {
        spaceHint.leftOffset = hint.leftoffset();
    }

    if (hint.has_rightoffset()) {
        spaceHint.rightOffset = hint.rightoffset();
    }

    return spaceHint;
}

}  // namespace space
}  // namespace curvefs
