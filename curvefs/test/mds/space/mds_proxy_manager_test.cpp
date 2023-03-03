/*
 *  Copyright (c) 2022 NetEase Inc.
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
 * Date: Tuesday Jul 19 16:43:37 CST 2022
 * Author: wuhanqing
 */

#include "curvefs/src/mds/space/mds_proxy_manager.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <random>

namespace curvefs {
namespace mds {
namespace space {

TEST(MdsProxyManagerTest, GetSameProxy) {
    MdsProxyManager manager;
    std::vector<std::string> hosts{"127.0.0.1:6666", "127.0.0.1:6667",
                                   "127.0.0.1:6668"};

    auto* proxy = manager.GetOrCreateProxy(hosts);
    ASSERT_NE(nullptr, proxy);

    std::random_device rd;
    std::shuffle(hosts.begin(), hosts.end(), rd);

    auto* proxy2 = manager.GetOrCreateProxy(hosts);
    ASSERT_EQ(proxy, proxy2);

    size_t count = 0;
    for (const auto& slot : manager.slots_) {
        count += slot.proxies.size();
    }

    ASSERT_EQ(1, count);
}

TEST(MdsProxyManagerTest, GetDifferentProxy) {
    MdsProxyManager manager;
    std::vector<std::string> hosts{"127.0.0.1:6666", "127.0.0.1:6667",
                                   "127.0.0.1:6668"};

    auto* proxy = manager.GetOrCreateProxy(hosts);
    ASSERT_NE(nullptr, proxy);

    std::random_device rd;
    hosts.erase(hosts.begin() + rd() % hosts.size());
    hosts.push_back("127.0.0.1:6669");
    std::shuffle(hosts.begin(), hosts.end(), rd);

    auto* proxy2 = manager.GetOrCreateProxy(hosts);
    ASSERT_NE(proxy, proxy2);

    size_t count = 0;
    for (const auto& slot : manager.slots_) {
        count += slot.proxies.size();
    }

    ASSERT_EQ(2, count);
}

TEST(MdsProxyManagerTest, EmptyHosts) {
    MdsProxyManager manager;
    std::vector<std::string> hosts;

    ASSERT_EQ(nullptr, manager.GetOrCreateProxy(hosts));
}

}  // namespace space
}  // namespace mds
}  // namespace curvefs
