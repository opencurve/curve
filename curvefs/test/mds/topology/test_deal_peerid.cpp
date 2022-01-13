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
 * @Project: curve
 * @Date: 2021-09-23
 * @Author: wanghai01
 */

#include <gtest/gtest.h>
#include "curvefs/src/mds/topology/deal_peerid.h"

namespace curvefs {
namespace mds {
namespace topology {

TEST(DealPeerIdTest, testBuildPeerIdWithIpPort) {
    std::string ip = "127.0.0.1";
    uint32_t port = 10000;
    uint32_t index = 1;
    ASSERT_EQ("127.0.0.1:10000:0", BuildPeerIdWithIpPort(ip, port));
    ASSERT_EQ("127.0.0.1:10000:1", BuildPeerIdWithIpPort(ip, port, index));
}

TEST(DealPeerIdTest, BuildPeerIdWithAddr) {
    std::string addr = "127.0.0.1:10000";
    uint32_t index = 1;
    ASSERT_EQ("127.0.0.1:10000:0", BuildPeerIdWithAddr(addr));
    ASSERT_EQ("127.0.0.1:10000:1", BuildPeerIdWithAddr(addr, index));
}

TEST(DealPeerIdTest, SplitPeerIdSuccess) {
    std::string peerId = "127.0.0.1:10000:0";
    std::string ip;
    uint32_t port;
    uint32_t index;
    ASSERT_TRUE(SplitPeerId(peerId, &ip, &port, &index));
    ASSERT_EQ("127.0.0.1", ip);
    ASSERT_EQ(10000, port);
    ASSERT_EQ(0, index);
}

TEST(DealPeerIdTest, SplitPeerIdSuccess1) {
    std::string peerId = "127.0.0.1:10000:0";
    std::string ip;
    uint32_t port;
    ASSERT_TRUE(SplitPeerId(peerId, &ip, &port));
    ASSERT_EQ("127.0.0.1", ip);
    ASSERT_EQ(10000, port);
}

TEST(DealPeerIdTest, SplitPeerIdFail) {
    std::string peerId = "127.0.0.1:10000:0:0";
    std::string ip;
    uint32_t port;
    uint32_t index;
    ASSERT_FALSE(SplitPeerId(peerId, &ip, &port, &index));
}

TEST(DealPeerIdTest, SplitPeerIdFail1) {
    std::string peerId = "127.0.0.1:q10000:0";
    std::string ip;
    uint32_t port;
    uint32_t index;
    ASSERT_FALSE(SplitPeerId(peerId, &ip, &port, &index));
}

TEST(DealPeerIdTest, SplitPeerIdFail2) {
    std::string peerId = "127.0.0.1:10000:q1";
    std::string ip;
    uint32_t port;
    uint32_t index;
    ASSERT_FALSE(SplitPeerId(peerId, &ip, &port, &index));
}

}  // namespace topology
}  // namespace mds
}  // namespace curvefs
