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
 * Project: nebd
 * Created Date: 2020-01-20
 * Author: wuhanqing
 */

#ifndef NEBD_TEST_PART1_MOCK_HEARTBEAT_SERVICE_H_
#define NEBD_TEST_PART1_MOCK_HEARTBEAT_SERVICE_H_

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <string>
#include "nebd/src/common/heartbeat.pb.h"

namespace nebd {
namespace client {

class MockHeartBeatService : public NebdHeartbeatService {
 public:
    MOCK_METHOD4(KeepAlive, void(::google::protobuf::RpcController* cntl,
                                 const HeartbeatRequest* request,
                                 HeartbeatResponse* response,
                                 ::google::protobuf::Closure* done));
};

}  // namespace client
}  // namespace nebd

#endif  // NEBD_TEST_PART1_MOCK_HEARTBEAT_SERVICE_H_
