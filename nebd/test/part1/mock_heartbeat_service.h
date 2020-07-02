/**
 * Project: nebd
 * Created Date: 2020-01-20
 * Author: wuhanqing
 * Copyright (c) 2019 netease
 */

#ifndef TESTS_PART1_MOCK_HEARTBEAT_SERVICE_H_
#define TESTS_PART1_MOCK_HEARTBEAT_SERVICE_H_

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

#endif  // TESTS_PART1_MOCK_HEARTBEAT_SERVICE_H_
