/*
 * Project: nebd
 * Created Date: 2020-01-16
 * Author: lixiaocui
 * Copyright (c) 2020 netease
 */

#include <gtest/gtest.h>
#include "src/part2/nebd_server.h"
#include "tests/part2/mock_curve_client.h"

namespace nebd {
namespace server {

using ::testing::Return;
using ::testing::_;
using ::testing::SetArgPointee;
using ::testing::DoAll;

TEST(TestNebdServer, test_Init_Run_Fini) {
    NebdServer server;
    auto curveClient = std::make_shared<MockCurveClient>();

    std::string confPath;
    // 1. 配置文件不存在, init失败
    EXPECT_CALL(*curveClient, Init(_)).Times(0);
    confPath = "./nebd.conf";
    ASSERT_EQ(-1, server.Init(confPath, curveClient));

    // 2. 配置文件存在, 监听端口未设置
    EXPECT_CALL(*curveClient, Init(_)).Times(0);
    confPath = "./nebd-server.err.conf";
    ASSERT_EQ(-1, server.Init(confPath, curveClient));

    // 3. curveclient init失败
    EXPECT_CALL(*curveClient, Init(_)).WillOnce(Return(-1));
    confPath = "./etc/nebd/nebd-server.conf";
    ASSERT_EQ(-1, server.Init(confPath, curveClient));

    // 4. 初始化成功
    EXPECT_CALL(*curveClient, Init(_)).WillOnce(Return(0));
    ASSERT_EQ(0, server.Init(confPath, curveClient));

    // 5. run成功
    EXPECT_CALL(*curveClient, UnInit()).Times(2);
    std::thread nebdServerThread(&NebdServer::RunUntilAskedToQuit, &server);
    sleep(1);

    // 6、再次Run会失败
    ASSERT_EQ(-1, server.RunUntilAskedToQuit());

    // 7. stop成功
    ASSERT_EQ(0, server.Fini());

    // 8. 再次stop不会重复释放资源
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
