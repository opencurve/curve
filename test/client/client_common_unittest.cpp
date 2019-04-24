/*
 * Project: curve
 * File Created: Tuesday, 23rd April 2019 9:15:46 am
 * Author: tongguangxun
 * Copyright (c)￼ 2018 netease
 */

#include <gtest/gtest.h>

#include "src/client/client_common.h"

using curve::client::EndPoint;
using curve::client::ChunkServerAddr;

TEST(ClientCommon, ChunkServerAddrTest) {
    // 默认构造函数创建的成员变量内容为空
    ChunkServerAddr chunkaddr;
    ASSERT_TRUE(chunkaddr.IsEmpty());

    EndPoint ep;
    str2endpoint("127.0.0.1:8000", &ep);

    // 从已有的endpoint创建ChunkServerAddr，变量内容非空
    ChunkServerAddr caddr(ep);
    ASSERT_FALSE(caddr.IsEmpty());
    ASSERT_EQ(caddr.addr_.port, 8000);
    ASSERT_STREQ("127.0.0.1:8000:0", caddr.ToString().c_str());

    // reset置位后成员变量内容为空
    caddr.Reset();
    ASSERT_TRUE(caddr.IsEmpty());

    std::string ipaddr("127.0.0.1:9000:0");
    ChunkServerAddr caddr2;
    ASSERT_TRUE(caddr2.IsEmpty());

    // 从字符串中解析出地址信息，字符串不符合解析格式返回-1，"ip:port:index"
    std::string ipaddr1("127.0.0.1");
    ASSERT_EQ(-1, caddr2.Parse(ipaddr1));
    std::string ipaddr2("127.0.0.q:9000:0");
    ASSERT_EQ(-1, caddr2.Parse(ipaddr2));

    // 从字符串解析地址成功后，成员变量即为非空
    ASSERT_EQ(0, caddr2.Parse(ipaddr));
    ASSERT_FALSE(caddr2.IsEmpty());

    // 验证非空成员变量是否为预期值
    EndPoint ep1;
    str2endpoint("127.0.0.1:9000", &ep1);
    ASSERT_EQ(caddr2.addr_, ep1);
}
