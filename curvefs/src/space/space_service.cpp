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

/**
 * Project: curve
 * File Created: Fri Jul 16 21:22:40 CST 2021
 * Author: wuhanqing
 */

#include "curvefs/src/space/space_service.h"

#include <brpc/closure_guard.h>
#include <brpc/controller.h>

#include "curvefs/src/space/utils.h"

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

    auto status = space_->InitSpace(request->fsinfo());

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

    Extents exts;
    SpaceAllocateHint hint;
    if (request->has_allochint()) {
        hint = ConvertToSpaceAllocateHint(request->allochint());
    }

    auto status =
        space_->AllocateSpace(request->fsid(), request->size(), hint, &exts);

    response->set_status(status);
    if (status != SPACE_OK) {
        LOG(ERROR) << "AllocateSpace failure, request: "
                   << request->ShortDebugString()
                   << ", error: " << SpaceStatusCode_Name(status);
    } else {
        *response->mutable_extents() = ConvertToProtoExtents(exts);

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
    } else {
        LOG(INFO) << "DeallocateSpace success, request: "
                  << request->ShortDebugString();
    }
}

void SpaceAllocServiceImpl::StatSpace(
    ::google::protobuf::RpcController* controller,
    const ::curvefs::space::StatSpaceRequest* request,
    ::curvefs::space::StatSpaceResponse* response,
    ::google::protobuf::Closure* done) {
    brpc::ClosureGuard guard(done);

    SpaceStat stat;
    auto status = space_->StatSpace(request->fsid(), &stat);

    response->set_status(status);
    if (status != SPACE_OK) {
        LOG(ERROR) << "StatSpace failure, request: "
                   << request->ShortDebugString()
                   << ", error: " << SpaceStatusCode_Name(status);
    } else {
        response->set_totalblock(stat.total / stat.blockSize);
        response->set_availableblock(stat.available / stat.blockSize);
        response->set_usedblock((stat.total - stat.available) / stat.blockSize);
    }
}

}  // namespace space
}  // namespace curvefs
