/**
 * Project: curve
 * Date: Mon Apr 27 11:34:41 CST 2020
 * Author: wuhanqing
 * Copyright (c) 2020 Netease
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
