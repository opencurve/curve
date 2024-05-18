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
 * Project: curve
 * Created Date: Fri Nov 25 2022
 * Author: lixiaocui
 */

#ifndef CURVEFS_TEST_CLIENT_MOCK_KVCLIENT_H_
#define CURVEFS_TEST_CLIENT_MOCK_KVCLIENT_H_

#include <gmock/gmock.h>

#include <cstdint>
#include <string>

#include "curvefs/src/client/kvclient/kvclient.h"

namespace curvefs {
namespace client {
class MockKVClient : public KVClient {
 public:
    MockKVClient() : KVClient() {}
    ~MockKVClient() = default;

    MOCK_METHOD4(Set, bool(const std::string &, const char *, const uint64_t,
                           std::string *));
    MOCK_METHOD7(Get,
                 bool(const std::string&, char*, uint64_t, uint64_t,
                      std::string*, uint64_t*, memcached_return_t* retCod));
    MOCK_METHOD1(Exist, bool(const std::string&));
};

}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_TEST_CLIENT_MOCK_KVCLIENT_H_
