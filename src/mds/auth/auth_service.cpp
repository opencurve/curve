/*
 *  Copyright (c) 2023 NetEase Inc.
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
 * Created Date: 2023-05-26
 * Author: wanghai (SeanHai)
 */

#include "src/mds/auth/auth_service.h"

namespace curve {
namespace mds {
namespace auth {

void AuthServiceImpl::AddKey(google::protobuf::RpcController* cntl_base,
    const AddKeyRequest *request,
    AddKeyResponse* response,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);

    brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);
    authMgr_->AddKey(*request, response);

    if (AuthStatusCode::AUTH_OK != response->status()) {
        LOG(ERROR) << "Send response[log_id=" << cntl->log_id()
                   << "] from " << cntl->local_side()
                   << " to " << cntl->remote_side()
                   << ". [AddKeyResponse] "
                   << response->DebugString();
    }
}

void AuthServiceImpl::DeleteKey(google::protobuf::RpcController* cntl_base,
    const DeleteKeyRequest *request,
    DeleteKeyResponse* response,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);

    brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);
    authMgr_->DeleteKey(*request, response);

    if (AuthStatusCode::AUTH_OK != response->status()) {
        LOG(ERROR) << "Send response[log_id=" << cntl->log_id()
                   << "] from " << cntl->local_side()
                   << " to " << cntl->remote_side()
                   << ". [DeleteKeyResponse] "
                   << response->DebugString();
    }
}

void AuthServiceImpl::GetKey(google::protobuf::RpcController* cntl_base,
    const GetKeyRequest *request,
    GetKeyResponse* response,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);

    brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);
    authMgr_->GetKey(*request, response);

    if (AuthStatusCode::AUTH_OK != response->status()) {
        LOG(ERROR) << "Send response[log_id=" << cntl->log_id()
                   << "] from " << cntl->local_side()
                   << " to " << cntl->remote_side()
                   << ". [GetKeyResponse] "
                   << response->DebugString();
    }
}

void AuthServiceImpl::UpdateKey(google::protobuf::RpcController* cntl_base,
    const UpdateKeyRequest *request,
    UpdateKeyResponse* response,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);

    brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);
    authMgr_->UpdateKey(*request, response);

    if (AuthStatusCode::AUTH_OK != response->status()) {
        LOG(ERROR) << "Send response[log_id=" << cntl->log_id()
                   << "] from " << cntl->local_side()
                   << " to " << cntl->remote_side()
                   << ". [UpdateKeyResponse] "
                   << response->DebugString();
    }
}

void AuthServiceImpl::GetTicket(google::protobuf::RpcController* cntl_base,
    const GetTicketRequest *request,
    GetTicketResponse* response,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);

    brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);
    authMgr_->GetTicket(*request, response);

    if (AuthStatusCode::AUTH_OK != response->status()) {
        LOG(ERROR) << "Send response[log_id=" << cntl->log_id()
                   << "] from " << cntl->local_side()
                   << " to " << cntl->remote_side()
                   << ". [GetTicketResponse] "
                   << response->DebugString();
    }
}

}  // namespace auth
}  // namespace mds
}  // namespace curve