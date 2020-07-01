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

#include <gtest/gtest.h>

#include "src/common/channel_pool.h"

namespace curve {
namespace common {

TEST(Common, ChannelPool) {
    ChannelPool channelPool;
    ChannelPtr channelPtr;
    // 地址非法，init失败
    std::string addr = "127.0.0.1:80000";
    ASSERT_EQ(-1, channelPool.GetOrInitChannel(addr, &channelPtr));
    ASSERT_FALSE(channelPtr);
    // 地址合法，init成功
    addr = "127.0.0.1:8000";
    ASSERT_EQ(0, channelPool.GetOrInitChannel(addr, &channelPtr));
    ASSERT_TRUE(channelPtr);
    // 同一个地址应该返回同一个channelPtr
    ChannelPtr channelPtr2;
    ASSERT_EQ(0, channelPool.GetOrInitChannel(addr, &channelPtr2));
    ASSERT_TRUE(channelPtr2);
    ASSERT_EQ(channelPtr, channelPtr2);
    // 清空
    channelPool.Clear();
}

}  // namespace common
}  // namespace curve
