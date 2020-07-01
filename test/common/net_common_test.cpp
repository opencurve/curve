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
 * File Created: Tuesday, 7th May 2019 1:16:08 pm
 * Author: tongguangxun
 */

#include <glog/logging.h>
#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <string>
#include "src/common/net_common.h"

namespace curve {
namespace common {
TEST(Common, NetCommon) {
    std::string addr = "123.0.0.1";
    ASSERT_FALSE(NetCommon::CheckAddressValid(addr));
    addr = "123.0.0.1:65537";
    ASSERT_FALSE(NetCommon::CheckAddressValid(addr));
    addr = "123.0.q.1:65537";
    ASSERT_FALSE(NetCommon::CheckAddressValid(addr));
    addr = "123.0.0.1:657";
    ASSERT_TRUE(NetCommon::CheckAddressValid(addr));
    std::string ip;
    uint32_t port;
    addr = "123.0.0.1";
    ASSERT_FALSE(NetCommon::SplitAddrToIpPort(addr, &ip, &port));
    addr = "123.0.0.1:65537";
    ASSERT_FALSE(NetCommon::SplitAddrToIpPort(addr, &ip, &port));
    addr = "123.0.q.1:65537";
    ASSERT_FALSE(NetCommon::SplitAddrToIpPort(addr, &ip, &port));
    addr = "123.0.0.1:657";
    ASSERT_TRUE(NetCommon::SplitAddrToIpPort(addr, &ip, &port));
    ASSERT_EQ("123.0.0.1", ip);
    ASSERT_EQ(657, port);
}

TEST(Common, GetLocalIP) {
    std::string ip;
    ASSERT_TRUE(NetCommon::GetLocalIP(&ip));
    LOG(INFO) << "IP = " << ip.c_str();
}
}   // namespace common
}   // namespace curve
