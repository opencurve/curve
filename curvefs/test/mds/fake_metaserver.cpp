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
 * @Date: 2021-06-24 16:55:17
 * @Author: chenwei
 */
#include "curvefs/test/mds/fake_metaserver.h"

namespace curvefs {
namespace metaserver {
void FakeMetaserverImpl::GetDentry(
    ::google::protobuf::RpcController* controller,
    const ::curvefs::metaserver::GetDentryRequest* request,
    ::curvefs::metaserver::GetDentryResponse* response,
    ::google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    // brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
    MetaStatusCode status = MetaStatusCode::OK;
    response->set_statuscode(status);
    return;
}

void FakeMetaserverImpl::ListDentry(
    ::google::protobuf::RpcController* controller,
    const ::curvefs::metaserver::ListDentryRequest* request,
    ::curvefs::metaserver::ListDentryResponse* response,
    ::google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    // brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
    MetaStatusCode status = MetaStatusCode::OK;
    response->set_statuscode(status);
    return;
}

void FakeMetaserverImpl::CreateDentry(
    ::google::protobuf::RpcController* controller,
    const ::curvefs::metaserver::CreateDentryRequest* request,
    ::curvefs::metaserver::CreateDentryResponse* response,
    ::google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    // brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
    MetaStatusCode status = MetaStatusCode::OK;
    response->set_statuscode(status);
    return;
}

void FakeMetaserverImpl::DeleteDentry(
    ::google::protobuf::RpcController* controller,
    const ::curvefs::metaserver::DeleteDentryRequest* request,
    ::curvefs::metaserver::DeleteDentryResponse* response,
    ::google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    // brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
    MetaStatusCode status = MetaStatusCode::OK;
    response->set_statuscode(status);
    return;
}

void FakeMetaserverImpl::GetInode(
    ::google::protobuf::RpcController* controller,
    const ::curvefs::metaserver::GetInodeRequest* request,
    ::curvefs::metaserver::GetInodeResponse* response,
    ::google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    // brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
    MetaStatusCode status = MetaStatusCode::OK;
    response->set_statuscode(status);
    return;
}

void FakeMetaserverImpl::CreateInode(
    ::google::protobuf::RpcController* controller,
    const ::curvefs::metaserver::CreateInodeRequest* request,
    ::curvefs::metaserver::CreateInodeResponse* response,
    ::google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    // brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
    MetaStatusCode status = MetaStatusCode::OK;
    response->set_statuscode(status);
    return;
}

void FakeMetaserverImpl::CreateRootInode(
    ::google::protobuf::RpcController* controller,
    const ::curvefs::metaserver::CreateRootInodeRequest* request,
    ::curvefs::metaserver::CreateRootInodeResponse* response,
    ::google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    // brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
    MetaStatusCode status = MetaStatusCode::OK;
    response->set_statuscode(status);
    return;
}

void FakeMetaserverImpl::UpdateInode(
    ::google::protobuf::RpcController* controller,
    const ::curvefs::metaserver::UpdateInodeRequest* request,
    ::curvefs::metaserver::UpdateInodeResponse* response,
    ::google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    // brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
    MetaStatusCode status = MetaStatusCode::OK;
    response->set_statuscode(status);
    return;
}

void FakeMetaserverImpl::DeleteInode(
    ::google::protobuf::RpcController* controller,
    const ::curvefs::metaserver::DeleteInodeRequest* request,
    ::curvefs::metaserver::DeleteInodeResponse* response,
    ::google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    // brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
    MetaStatusCode status = MetaStatusCode::OK;
    response->set_statuscode(status);
    return;
}
}  // namespace metaserver
}  // namespace curvefs
