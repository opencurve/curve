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

/**
 * Project: curve
 * File Created: Fri Jul 30 10:25:00 CST 2021
 * Author: wuhanqing
 */

#ifndef CURVEFS_TEST_SPACE_MOCK_MOCK_METASERVER_H_
#define CURVEFS_TEST_SPACE_MOCK_MOCK_METASERVER_H_

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "curvefs/proto/metaserver.pb.h"

namespace curvefs {
namespace space {

class MockMetaServerService : public curvefs::metaserver::MetaServerService {
 public:
    MockMetaServerService() = default;
    ~MockMetaServerService() = default;

    MOCK_METHOD4(ListDentry,
                 void(::google::protobuf::RpcController* controller,
                      const ::curvefs::metaserver::ListDentryRequest* request,
                      ::curvefs::metaserver::ListDentryResponse* response,
                      ::google::protobuf::Closure* done));
};

}  // namespace space
}  // namespace curvefs

#endif  // CURVEFS_TEST_SPACE_MOCK_MOCK_METASERVER_H_
