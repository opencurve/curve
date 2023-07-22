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

#include <string>
#include "src/mds/auth/auth_service_manager.h"

namespace curve {
namespace mds {
namespace auth {

void AuthServiceManager::AddKey(const AddKeyRequest &request,
    AddKeyResponse* response) {
    AuthStatusCode ret = AuthStatusCode::AUTH_OK;
    for (auto const &item : request.enckey()) {
        ret = authNode_->AddKey(item);
        if (ret != AuthStatusCode::AUTH_OK) {
            break;
        }
    }
    response->set_status(ret);
    return;
}

void AuthServiceManager::DeleteKey(const DeleteKeyRequest &request,
    DeleteKeyResponse* response) {
    AuthStatusCode ret = AuthStatusCode::AUTH_OK;
    ret = authNode_->DeleteKey(request.enckeyid());
    response->set_status(ret);
}

void AuthServiceManager::GetKey(const GetKeyRequest &request,
    GetKeyResponse *response) {
    AuthStatusCode ret = AuthStatusCode::AUTH_OK;
    ret = authNode_->GetKey(request.enckeyid(), response->mutable_enckey());
    response->set_status(ret);
}

void AuthServiceManager::UpdateKey(const UpdateKeyRequest &request,
    UpdateKeyResponse *response) {
    AuthStatusCode ret = AuthStatusCode::AUTH_OK;
    ret = authNode_->UpdateKey(request.enckey());
    response->set_status(ret);
}

void AuthServiceManager::GetTicket(const GetTicketRequest &request,
    GetTicketResponse* response) {
    std::string encTicket, encAttach;
    auto ret = authNode_->GetTicket(request.cid(), request.sid(),
        response->mutable_encticket(), response->mutable_encticketattach());
    response->set_status(ret);
    return;
}

}  // namespace auth
}  // namespace mds
}  // namespace curve