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
    IOSenderOption_t iosenderopt;
    iosenderopt.failreqopt.client_chunk_op_max_retry = 3;
    iosenderopt.failreqopt.client_chunk_op_retry_interval_us = 500;
    iosenderopt.enable_applied_index_read = 1;

    std::unique_ptr<RequestSenderManager> senderManager(
        new RequestSenderManager());
    ChunkServerID leaderId = 123456789;
    butil::EndPoint leaderAddr;
    std::string leaderStr = "127.0.0.1:8200";
    butil::str2endpoint(leaderStr.c_str(), &leaderAddr);

    for (int i = leaderId; i <= leaderId + 10000; ++i) {
        auto senderPtr1 = senderManager->GetOrCreateSender(leaderId,
                                                           leaderAddr,
                                                           iosenderopt);
        ASSERT_TRUE(nullptr != senderPtr1);
    }
}

}   // namespace client
}   // namespace curve
