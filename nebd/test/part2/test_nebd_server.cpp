/*
 * Project: nebd
 * Created Date: 2020-01-16
 * Author: lixiaocui
 * Copyright (c) 2020 netease
 */

#include <gtest/gtest.h>
#include "nebd/src/part2/nebd_server.h"
#include "nebd/test/part2/mock_curve_client.h"

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
    confPath = "./nebd.conf";
    ASSERT_EQ(-1, server.Init(confPath));

    // 2. 配置文件存在, 监听端口未设置
    confPath = "./nebd/test/part2/nebd-server-err.conf";
    Configuration conf;
    conf.SetConfigPath(confPath);
    conf.SaveConfig();
    ASSERT_EQ(-1, server.Init(confPath));

    // 3、配置文件中没有client配置
    conf.SetStringValue("listen.address", "/tmp/nebd-server.sock");
    conf.SaveConfig();
    ASSERT_EQ(-1, server.Init(confPath));

    // 4. curveclient init失败
    conf.SetStringValue("curveclient.confPath", "/etc/curve/client.conf");
    conf.SaveConfig();
    EXPECT_CALL(*curveClient, Init(_)).WillOnce(Return(-1));
    ASSERT_EQ(-1, server.Init(confPath, curveClient));

    // 5、初始化fileManager失败
    EXPECT_CALL(*curveClient, Init(_)).WillOnce(Return(0));
    ASSERT_EQ(-1, server.Init(confPath, curveClient));

    // 6、没有heartbeat.timeout字段
    EXPECT_CALL(*curveClient, Init(_)).WillOnce(Return(0));
    conf.SetStringValue("meta.file.path", "./nebd-server-test.meta");
    conf.SaveConfig();
    ASSERT_EQ(-1, server.Init(confPath, curveClient));

    // 7、没有heartbeat.check.interval.ms字段
    EXPECT_CALL(*curveClient, Init(_)).WillOnce(Return(0));
    conf.SetIntValue("heartbeat.timeout.sec", 30);
    conf.SaveConfig();
    ASSERT_EQ(-1, server.Init(confPath, curveClient));


    // 8. 初始化成功
    EXPECT_CALL(*curveClient, Init(_)).WillOnce(Return(0));
    conf.SetIntValue("heartbeat.check.interval.ms", 3000);
    conf.SaveConfig();
    ASSERT_EQ(0, server.Init(confPath, curveClient));

    // 9. run成功
    EXPECT_CALL(*curveClient, UnInit()).Times(2);
    std::thread nebdServerThread(&NebdServer::RunUntilAskedToQuit, &server);
    sleep(1);

    // 10、再次Run会失败
    ASSERT_EQ(-1, server.RunUntilAskedToQuit());

    // 11、Run之后Init会失败
    ASSERT_EQ(-1, server.Init(confPath, curveClient));

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
