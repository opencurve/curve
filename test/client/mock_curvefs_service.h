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
 * Date: Mon Apr 27 11:34:41 CST 2020
 * Author: wuhanqing
 */

#ifndef TEST_CLIENT_MOCK_CURVEFS_SERVICE_H_
#define TEST_CLIENT_MOCK_CURVEFS_SERVICE_H_

#include <gmock/gmock.h>
#include "proto/nameserver2.pb.h"

namespace curve {
namespace client {

class MockCurveFsService : public ::curve::mds::CurveFSService {
 public:
    MockCurveFsService() = default;
    ~MockCurveFsService() = default;

    MOCK_METHOD4(RefreshSession,
                 void(::google::protobuf::RpcController* controller,
                      const curve::mds::ReFreshSessionRequest* request,
                      curve::mds::ReFreshSessionResponse* response,
                      ::google::protobuf::Closure* done));
};

}  // namespace client
}  // namespace curve

#endif  // TEST_CLIENT_MOCK_CURVEFS_SERVICE_H_
