/*
 * Project: curve
 * Created Date: 2020-01-07
 * Author: charisu
 * Copyright (c) 2018 netease
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
