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
 * Date: Mon Sep  6 19:01:35 CST 2021
 * Author: wuhanqing
 */

#ifndef CURVEFS_TEST_METASERVER_COPYSET_MOCK_MOCK_RAFT_CLI2_SERVICE_H_
#define CURVEFS_TEST_METASERVER_COPYSET_MOCK_MOCK_RAFT_CLI2_SERVICE_H_

#include <gmock/gmock.h>

#include "curvefs/proto/cli2.pb.h"

namespace curvefs {
namespace metaserver {
namespace copyset {

class MockRaftCli2Service : public copyset::CliService2 {
 public:
    MOCK_METHOD4(
        GetLeader,
        void(::google::protobuf::RpcController* controller,
             const ::curvefs::metaserver::copyset::GetLeaderRequest2* request,
             ::curvefs::metaserver::copyset::GetLeaderResponse2* response,
             ::google::protobuf::Closure* done));
};

}  // namespace copyset
}  // namespace metaserver
}  // namespace curvefs

#endif  // CURVEFS_TEST_METASERVER_COPYSET_MOCK_MOCK_RAFT_CLI2_SERVICE_H_
