/*
 *  Copyright (c) 2020 NetEase Inc.
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

/**
 * Project: curve
 * Date: Sat Dec 19 23:01:09 CST 2020
 */

#ifndef TEST_CLIENT_MOCK_MOCK_MDSCLIENT_H_
#define TEST_CLIENT_MOCK_MOCK_MDSCLIENT_H_

#include <gmock/gmock.h>
#include <memory>

#include "src/client/auth_client.h"
#include "src/client/mds_client.h"

namespace curve {
namespace client {

class MockMDSClient : public MDSClient {
 public:
    MockMDSClient() {}
    MOCK_METHOD2(DeAllocateSegment, LIBCURVE_ERROR(const FInfo*, uint64_t));
};

}  // namespace client
}  // namespace curve

#endif  // TEST_CLIENT_MOCK_MOCK_MDSCLIENT_H_
