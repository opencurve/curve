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
 * Created Date: Thur Sept 3 2021
 * Author: lixiaocui
 */

#include <gtest/gtest.h>

#include "curvefs/src/client/rpcclient/channel_manager.h"

namespace curvefs {
namespace client {
namespace rpcclient {

TEST(ChannelManagerTest, basic_test) {
    std::unique_ptr<ChannelManager<uint32_t>> channelManager(
        new ChannelManager<uint32_t>());
    uint32_t leaderId = 123456789;
    butil::EndPoint leaderAddr;
    std::string leaderStr = "127.0.0.1:9109";
    butil::str2endpoint(leaderStr.c_str(), &leaderAddr);

    for (int i = leaderId; i <= leaderId + 10000; ++i) {
        ASSERT_TRUE(nullptr !=
                    channelManager->GetOrCreateChannel(leaderId, leaderAddr));
    }
}

TEST(ChannelManagerTest, fail_test) {
    std::unique_ptr<ChannelManager<uint32_t>> channelManager(
        new ChannelManager<uint32_t>());
    uint32_t leaderId = 123456789;
    butil::EndPoint leaderAddr;
    leaderAddr.ip = {0U};
    leaderAddr.port = -1;

    ASSERT_EQ(nullptr,
              channelManager->GetOrCreateChannel(leaderId, leaderAddr));
}

}  // namespace rpcclient
}  // namespace client
}  // namespace curvefs
