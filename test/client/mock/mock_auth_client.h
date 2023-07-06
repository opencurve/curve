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
 * Created Date: 2023-06-13
 * Author: wanghai (SeanHai)
*/

#ifndef TEST_CLIENT_MOCK_MOCK_AUTH_CLIENT_H_
#define TEST_CLIENT_MOCK_MOCK_AUTH_CLIENT_H_

#include <string>
#include "src/client/auth_client.h"
namespace curve {
namespace client {

class MockAuthClient : public AuthClient {
 public:
    MOCK_METHOD2(GetToken, bool(const std::string &serverId, Token *token));
};

}  // namespace client
}  // namespace curve

#endif  // TEST_CLIENT_MOCK_MOCK_AUTH_CLIENT_H_
