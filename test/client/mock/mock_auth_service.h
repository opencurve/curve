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
 * Created Date: 2023-06-12
 * Author: wanghai (SeanHai)
*/

#ifndef TEST_CLIENT_MOCK_MOCK_AUTH_SERVICE_H_
#define TEST_CLIENT_MOCK_MOCK_AUTH_SERVICE_H_

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include "proto/auth.pb.h"

namespace curve {
namespace mds {
namespace auth {

class MockAuthService : public AuthService {
 public:
    MOCK_METHOD(void, GetTicket, (google::protobuf::RpcController* cntl,
                              const GetTicketRequest* request,
                              GetTicketResponse* response,
                              google::protobuf::Closure* done), (override));
    MOCK_METHOD(void, AddKey, (google::protobuf::RpcController* cntl,
                              const AddKeyRequest* request,
                              AddKeyResponse* response,
                              google::protobuf::Closure* done), (override));
    MOCK_METHOD(void, DeleteKey, (google::protobuf::RpcController* cntl,
                              const DeleteKeyRequest* request,
                              DeleteKeyResponse* response,
                              google::protobuf::Closure* done), (override));
    MOCK_METHOD(void, GetKey, (google::protobuf::RpcController* cntl,
                              const GetKeyRequest* request,
                              GetKeyResponse* response,
                              google::protobuf::Closure* done), (override));
    MOCK_METHOD(void, UpdateKey, (google::protobuf::RpcController* cntl,
                              const UpdateKeyRequest* request,
                              UpdateKeyResponse* response,
                              google::protobuf::Closure* done), (override));
};

}  // namespace auth
}  // namespace mds
}  // namespace curve

#endif  // TEST_CLIENT_MOCK_MOCK_AUTH_SERVICE_H_
