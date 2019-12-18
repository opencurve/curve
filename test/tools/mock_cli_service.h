/*
 * Project: curve
 * File Created: 2019-12-23
 * Author: charisu
 * Copyright (c)￼ 2018 netease
 */

#ifndef TEST_TOOLS_MOCK_CLI_SERVICE_H_
#define TEST_TOOLS_MOCK_CLI_SERVICE_H_

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include "proto/cli2.pb.h"

namespace curve {
namespace tool {

using ::google::protobuf::RpcController;
using ::google::protobuf::Closure;
using ::curve::chunkserver::CliService2;
using ::curve::chunkserver::GetLeaderRequest2;
using ::curve::chunkserver::GetLeaderResponse2;
using ::curve::chunkserver::RemovePeerRequest2;
using ::curve::chunkserver::RemovePeerResponse2;
using ::curve::chunkserver::TransferLeaderRequest2;
using ::curve::chunkserver::TransferLeaderResponse2;
using ::curve::chunkserver::ResetPeerRequest2;
using ::curve::chunkserver::ResetPeerResponse2;

class MockCliService : public CliService2 {
 public:
    MOCK_METHOD4(GetLeader,
        void(RpcController *controller,
        const GetLeaderRequest2 *request,
        GetLeaderResponse2 *response,
        Closure *done));

    MOCK_METHOD4(RemovePeer,
        void(RpcController *controller,
        const RemovePeerRequest2 *request,
        RemovePeerResponse2 *response,
        Closure *done));

    MOCK_METHOD4(TransferLeader,
        void(RpcController *controller,
        const TransferLeaderRequest2 *request,
        TransferLeaderResponse2 *response,
        Closure *done));

    MOCK_METHOD4(ResetPeer,
        void(RpcController *controller,
        const ResetPeerRequest2 *request,
        ResetPeerResponse2 *response,
        Closure *done));
};
}  // namespace tool
}  // namespace curve
#endif  // TEST_TOOLS_MOCK_CLI_SERVICE_H_
