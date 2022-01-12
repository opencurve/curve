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
#include "curvefs/test/mds/fake_space.h"

namespace curvefs {
namespace space {
void FakeSpaceImpl::InitSpace(::google::protobuf::RpcController* controller,
                              const ::curvefs::space::InitSpaceRequest* request,
                              ::curvefs::space::InitSpaceResponse* response,
                              ::google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    // brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
    initCount++;
    SpaceStatusCode status = SpaceStatusCode::SPACE_OK;
    response->set_status(status);
    return;
}

void FakeSpaceImpl::AllocateSpace(
    ::google::protobuf::RpcController* controller,
    const ::curvefs::space::AllocateSpaceRequest* request,
    ::curvefs::space::AllocateSpaceResponse* response,
    ::google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    // brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
    allocCount++;
    SpaceStatusCode status = SpaceStatusCode::SPACE_OK;
    response->set_status(status);
    return;
}

void FakeSpaceImpl::DeallocateSpace(
    ::google::protobuf::RpcController* controller,  // NOLINT
    const ::curvefs::space::DeallocateSpaceRequest* request,
    ::curvefs::space::DeallocateSpaceResponse* response,
    ::google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    // brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
    deallocateCount++;
    SpaceStatusCode status = SpaceStatusCode::SPACE_OK;
    response->set_status(status);
    return;
}

void FakeSpaceImpl::StatSpace(::google::protobuf::RpcController* controller,
                              const ::curvefs::space::StatSpaceRequest* request,
                              ::curvefs::space::StatSpaceResponse* response,
                              ::google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    // brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
    statCount++;
    SpaceStatusCode status = SpaceStatusCode::SPACE_OK;
    response->set_status(status);
    return;
}

void FakeSpaceImpl::UnInitSpace(
    ::google::protobuf::RpcController* controller,
    const ::curvefs::space::UnInitSpaceRequest* request,
    ::curvefs::space::UnInitSpaceResponse* response,
    ::google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    // brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
    uninitCount++;
    SpaceStatusCode status = SpaceStatusCode::SPACE_OK;
    response->set_status(status);
    return;
}
}  // namespace space
}  // namespace curvefs
