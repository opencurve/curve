/*
 * Project: nebd
 * Created Date: 2020-01-16
 * Author: lixiaocui
 * Copyright (c) 2020 netease
 */

#include <gtest/gtest.h>
#include "src/part2/nebd_server.h"

namespace nebd {
namespace server {

TEST(TestNebdServer, test_Init_Run_Fini) {
    NebdServer server;

    std::string confPath;
    // 1. 配置文件不存在, init失败
    confPath = "./nebd.conf";
    ASSERT_EQ(-1, server.Init(confPath));

    // 2. 配置文件存在, 监听端口未设置
    confPath = "./nebd-server.err.conf";
    ASSERT_EQ(-1, server.Init(confPath));

    // 3. 初始化成功
    confPath = "./etc/nebd/nebd-server.conf";
    ASSERT_EQ(0, server.Init(confPath));

    // 4. run成功
    std::thread nebdServerThread(&NebdServer::RunUntilAskedToQuit, &server);
    sleep(1);

    // 5. stop成功
    ASSERT_EQ(0, server.Fini());

    // 6. 再次stop不会重复释放资源
    ASSERT_EQ(0, server.Fini());
    nebdServerThread.join();
}

}  // namespace server
}  // namespace nebd

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    RUN_ALL_TESTS();
    return 0;
}
