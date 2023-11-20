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
 * File Created: Tuesday, 23rd April 2019 9:15:46 am
 * Author: tongguangxun
 */

#include "src/client/client_common.h"

#include <gtest/gtest.h>

namespace curve {
namespace client {

TEST(ClientCommon, PeerAddrTest) {
    // The member variable content created by the default constructor is empty
    PeerAddr chunkaddr;
    ASSERT_TRUE(chunkaddr.IsEmpty());

    EndPoint ep;
    str2endpoint("127.0.0.1:8000", &ep);

    // Create PeerAddr from an existing endpoint, with non empty variable
    // content
    PeerAddr caddr(ep);
    ASSERT_FALSE(caddr.IsEmpty());
    ASSERT_EQ(caddr.addr_.port, 8000);
    ASSERT_STREQ("127.0.0.1:8000:0", caddr.ToString().c_str());

    // After resetting, the member variable content is empty
    caddr.Reset();
    ASSERT_TRUE(caddr.IsEmpty());

    std::string ipaddr("127.0.0.1:9000:0");
    PeerAddr caddr2;
    ASSERT_TRUE(caddr2.IsEmpty());

    // Resolve address information from the string, if the string does not
    // conform to the parsing format, return -1, "ip:port:index"
    std::string ipaddr1("127.0.0.1");
    ASSERT_EQ(-1, caddr2.Parse(ipaddr1));
    std::string ipaddr2("127.0.0.q:9000:0");
    ASSERT_EQ(-1, caddr2.Parse(ipaddr2));
    std::string ipaddr3("127.0.0.1:9000:a");
    ASSERT_EQ(0, caddr2.Parse(ipaddr3));
    std::string ipaddr4("827.0.0.1:9000:0");
    ASSERT_EQ(-1, caddr2.Parse(ipaddr4));
    std::string ipaddr5("127.0.0.1001:9000:0");
    ASSERT_EQ(-1, caddr2.Parse(ipaddr5));

    // After successfully resolving the address from the string, the member
    // variable becomes non empty
    ASSERT_EQ(0, caddr2.Parse(ipaddr));
    ASSERT_FALSE(caddr2.IsEmpty());

    // Verify if the non empty member variable is the expected value
    EndPoint ep1;
    str2endpoint("127.0.0.1:9000", &ep1);
    ASSERT_EQ(caddr2.addr_, ep1);
}

}  // namespace client
}  // namespace curve
