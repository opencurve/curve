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
 * Created Date: 2023-06-21
 * Author: wanghai (SeanHai)
 */

#ifndef TEST_MDS_AUTH_MOCK_AUTH_MANAGER_H_
#define TEST_MDS_AUTH_MOCK_AUTH_MANAGER_H_

#include <gmock/gmock.h>
#include "src/mds/auth/auth_service_manager.h"

namespace curve {
namespace mds {
namespace auth {

class MockAuthManger : public AuthServiceManager {
 public:
    MockAuthManger() : AuthServiceManager(nullptr) {}
    ~MockAuthManger() {}

    MOCK_METHOD(void, AddKey, (const AddKeyRequest &request,
        AddKeyResponse* response), (override));
    MOCK_METHOD(void, DeleteKey, (const DeleteKeyRequest &request,
        DeleteKeyResponse* response), (override));
    MOCK_METHOD(void, GetKey, (const GetKeyRequest &request,
        GetKeyResponse* response), (override));
    MOCK_METHOD(void, UpdateKey, (const UpdateKeyRequest &request,
        UpdateKeyResponse* response), (override));
    MOCK_METHOD(void, GetTicket, (const GetTicketRequest &request,
        GetTicketResponse* response), (override));
};

}  // namespace auth
}  // namespace mds
}  // namespace curve

#endif  // TEST_MDS_AUTH_MOCK_AUTH_MANAGER_H_
