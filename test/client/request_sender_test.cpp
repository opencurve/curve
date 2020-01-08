/*
 * Project: curve
 * Created Date: 18-11-12
 * Author: wudemiao
 * Copyright (c) 2018 netease
 */

#include <gtest/gtest.h>

#include "src/client/request_sender.h"
#include "src/client/client_common.h"

namespace curve {
namespace client {

TEST(RequestSenderTest, basic_test) {
    /* 非法的 port */
    IOSenderOption_t ioSenderOpt;
    ioSenderOpt.failRequestOpt.chunkserverOPMaxRetry = 3;
    ioSenderOpt.failRequestOpt.chunkserverOPRetryIntervalUS = 500;
    ioSenderOpt.chunkserverEnableAppliedIndexRead = 1;
    butil::EndPoint leaderAddr;
    std::string leaderStr = "127.0.0.1:65539";
    ChunkServerID leaderId = 1;
    butil::str2endpoint(leaderStr.c_str(), &leaderAddr);
    RequestSender requestSender(leaderId, leaderAddr);
    ASSERT_EQ(-1, requestSender.Init(ioSenderOpt));
}

}   // namespace client
}   // namespace curve
