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
 * Created Date: 2023-05-24
 * Author: wanghai (SeanHai)
 */

#ifndef SRC_MDS_AUTH_AUTH_SERVICE_MANAGER_H_
#define SRC_MDS_AUTH_AUTH_SERVICE_MANAGER_H_

#include <memory>
#include "src/mds/auth/authnode.h"

namespace curve {
namespace mds {
namespace auth {

class AuthServiceManager {
 public:
    explicit AuthServiceManager(std::shared_ptr<AuthNode> authNode)
        : authNode_(authNode) {}
    virtual ~AuthServiceManager() = default;

    virtual void AddKey(const AddKeyRequest &request, AddKeyResponse* response);
    virtual void DeleteKey(const DeleteKeyRequest &request,
        DeleteKeyResponse* response);
    virtual void GetKey(const GetKeyRequest &request,
        GetKeyResponse* response);
    virtual void UpdateKey(const UpdateKeyRequest &request,
        UpdateKeyResponse* response);
    virtual void GetTicket(const GetTicketRequest &request,
        GetTicketResponse* response);

 private:
    std::shared_ptr<AuthNode> authNode_;
};

}  // namespace auth
}  // namespace mds
}  // namespace curve

#endif  // SRC_MDS_AUTH_AUTH_SERVICE_MANAGER_H_
