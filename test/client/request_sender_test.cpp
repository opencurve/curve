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

/*
 * Project: curve
 * Created Date: 18-11-12
 * Author: wudemiao
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
