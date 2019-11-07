/*
 * Project: curve
 * Created Date: 18-10-11
 * Author: wudemiao
 * Copyright (c) 2018 netease
 */

#include <gtest/gtest.h>

#include "src/client/request_sender_manager.h"
#include "src/client/client_common.h"

namespace curve {
namespace client {

TEST(RequestSenderManagerTest, basic_test) {
    IOSenderOption_t ioSenderOpt;
    ioSenderOpt.failRequestOpt.chunkserverOPMaxRetry = 3;
    ioSenderOpt.failRequestOpt.chunkserverOPRetryIntervalUS = 500;
    ioSenderOpt.chunkserverEnableAppliedIndexRead = 1;

    std::unique_ptr<RequestSenderManager> senderManager(
        new RequestSenderManager());
    ChunkServerID leaderId = 123456789;
    butil::EndPoint leaderAddr;
    std::string leaderStr = "127.0.0.1:9109";
    butil::str2endpoint(leaderStr.c_str(), &leaderAddr);

    for (int i = leaderId; i <= leaderId + 10000; ++i) {
        auto senderPtr1 = senderManager->GetOrCreateSender(leaderId,
                                                           leaderAddr,
                                                           ioSenderOpt);
        ASSERT_TRUE(nullptr != senderPtr1);
    }
}

TEST(RequestSenderManagerTest, fail_test) {
    IOSenderOption_t ioSenderOpt;
    ioSenderOpt.failRequestOpt.chunkserverOPMaxRetry = 3;
    ioSenderOpt.failRequestOpt.chunkserverOPRetryIntervalUS = 500;
    ioSenderOpt.chunkserverEnableAppliedIndexRead = 1;

    std::unique_ptr<RequestSenderManager> senderManager(
        new RequestSenderManager());
    ChunkServerID leaderId = 123456789;
    butil::EndPoint leaderAddr;
    leaderAddr.ip = {0U};
    leaderAddr.port = -1;

    ASSERT_EQ(nullptr, senderManager->GetOrCreateSender(
        leaderId, leaderAddr, ioSenderOpt));
}

}   // namespace client
}   // namespace curve
