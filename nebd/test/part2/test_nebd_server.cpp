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
 * Project: nebd
 * Created Date: 2020-01-16
 * Author: lixiaocui
 */

#include <gtest/gtest.h>

#include "nebd/src/part2/nebd_server.h"
#include "nebd/test/part2/mock_curve_client.h"

namespace nebd {
namespace server {

using ::testing::_;
using ::testing::DoAll;
using ::testing::Return;
using ::testing::SetArgPointee;

TEST(TestNebdServer, test_Init_Run_Fini) {
    NebdServer server;
    auto curveClient = std::make_shared<MockCurveClient>();

    std::string confPath;
    // 1. Configuration file does not exist, init failed
    confPath = "./nebd.conf";
    ASSERT_EQ(-1, server.Init(confPath));

    // 2. Configuration file exists, listening port not set
    confPath = "./nebd/test/part2/nebd-server-err.conf";
    Configuration conf;
    conf.SetBoolValue("response.returnRpcWhenIoError", false);
    conf.SetConfigPath(confPath);
    conf.SaveConfig();
    ASSERT_EQ(-1, server.Init(confPath));

    // 3. There is no client configuration in the configuration file
    conf.SetStringValue("listen.address", "/tmp/nebd-server.sock");
    conf.SaveConfig();
    ASSERT_EQ(-1, server.Init(confPath));

    // 4. Curveclient init failed
    conf.SetStringValue("curveclient.confPath", "/etc/curve/client.conf");
    conf.SaveConfig();
    EXPECT_CALL(*curveClient, Init(_)).WillOnce(Return(-1));
    ASSERT_EQ(-1, server.Init(confPath, curveClient));

    // 5. Failed to initialize fileManager
    EXPECT_CALL(*curveClient, Init(_)).WillOnce(Return(0));
    ASSERT_EQ(-1, server.Init(confPath, curveClient));

    // 6. There is no heartbeat.timeout field
    EXPECT_CALL(*curveClient, Init(_)).WillOnce(Return(0));
    conf.SetStringValue("meta.file.path", "./nebd-server-test.meta");
    conf.SaveConfig();
    ASSERT_EQ(-1, server.Init(confPath, curveClient));

    // 7. No heartbeat.check.interval.ms field
    EXPECT_CALL(*curveClient, Init(_)).WillOnce(Return(0));
    conf.SetIntValue("heartbeat.timeout.sec", 30);
    conf.SaveConfig();
    ASSERT_EQ(-1, server.Init(confPath, curveClient));

    // 8. Initialized successfully
    EXPECT_CALL(*curveClient, Init(_)).WillOnce(Return(0));
    conf.SetIntValue("heartbeat.check.interval.ms", 3000);
    conf.SaveConfig();
    ASSERT_EQ(0, server.Init(confPath, curveClient));

    // 9. Run successful
    EXPECT_CALL(*curveClient, UnInit()).Times(2);
    std::thread nebdServerThread(&NebdServer::RunUntilAskedToQuit, &server);
    sleep(1);

    // 10. Running again will fail
    ASSERT_EQ(-1, server.RunUntilAskedToQuit());

    // 11. Init will fail after Run
    ASSERT_EQ(-1, server.Init(confPath, curveClient));

    // 7. Stop successful
    ASSERT_EQ(0, server.Fini());

    // 8. Stopping again will not repeatedly release resources
    ASSERT_EQ(0, server.Fini());
    nebdServerThread.join();
}

}  // namespace server
}  // namespace nebd

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
