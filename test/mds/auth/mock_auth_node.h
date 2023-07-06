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

#ifndef TEST_MDS_AUTH_MOCK_AUTH_NODE_H_
#define TEST_MDS_AUTH_MOCK_AUTH_NODE_H_

#include <gmock/gmock.h>
#include <string>
#include "src/mds/auth/authnode.h"

namespace curve {
namespace mds {
namespace auth {

class MockAuthNode : public AuthNode {
 public:
    MOCK_METHOD(AuthStatusCode, AddKey, (const std::string &encKey),
        (override));
    MOCK_METHOD(AuthStatusCode, DeleteKey, (const std::string &encKeyId),
        (override));
    MOCK_METHOD(AuthStatusCode, UpdateKey, (const std::string &encKey),
        (override));
    MOCK_METHOD(AuthStatusCode, GetKey, (const std::string &encKeyId,
        std::string *encKey), (override));
    MOCK_METHOD(AuthStatusCode, GetTicket, (const std::string &cId,
        const std::string &sId, std::string *encTicket,
        std::string *encSK), (override));
};

}  // namespace auth
}  // namespace mds
}  // namespace curve

#endif  // TEST_MDS_AUTH_MOCK_AUTH_NODE_H_
