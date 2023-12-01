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
 * Created Date: 2020-01-07
 * Author: charisu
 */

#include "src/common/channel_pool.h"

#include <gtest/gtest.h>

namespace curve {
namespace common {

TEST(Common, ChannelPool) {
    ChannelPool channelPool;
    ChannelPtr channelPtr;
    // Illegal address, init failed
    std::string addr = "127.0.0.1:80000";
    ASSERT_EQ(-1, channelPool.GetOrInitChannel(addr, &channelPtr));
    ASSERT_FALSE(channelPtr);
    // The address is legal, init succeeded
    addr = "127.0.0.1:8000";
    ASSERT_EQ(0, channelPool.GetOrInitChannel(addr, &channelPtr));
    ASSERT_TRUE(channelPtr);
    // The same address should return the same channelPtr
    ChannelPtr channelPtr2;
    ASSERT_EQ(0, channelPool.GetOrInitChannel(addr, &channelPtr2));
    ASSERT_TRUE(channelPtr2);
    ASSERT_EQ(channelPtr, channelPtr2);
    // Clear
    channelPool.Clear();
}

}  // namespace common
}  // namespace curve
