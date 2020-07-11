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
 * Created Date: 2020-02-03
 * Author: charisu
 */

#include <gtest/gtest.h>
#include <brpc/channel.h>
#include <brpc/server.h>
#include <string>

#include "nebd/proto/heartbeat.pb.h"
#include "nebd/src/part2/heartbeat_service.h"
#include "nebd/test/part2/mock_heartbeat_manager.h"
using ::testing::_;
using ::testing::Return;

namespace nebd {
namespace server {

const std::string kSockFile_ = "/tmp/heartbeat_service_test.sock";  // NOLINT

class HeartbeatServiceTest : public ::testing::Test {
 protected:
    void SetUp() override {
        heartbeatManager_ = std::make_shared<MockHeartbeatManager>();
    }
    std::shared_ptr<MockHeartbeatManager>  heartbeatManager_;
};

TEST_F(HeartbeatServiceTest, KeepAlive) {
    // 启动server
    brpc::Server server;
    NebdHeartbeatServiceImpl heartbeatService(heartbeatManager_);
    ASSERT_EQ(0, server.AddService(&heartbeatService,
                        brpc::SERVER_DOESNT_OWN_SERVICE));
    brpc::ServerOptions option;
    option.idle_timeout_sec = -1;
    ASSERT_EQ(0, server.StartAtSockFile(kSockFile_.c_str(), &option));

    nebd::client::HeartbeatRequest request;
    request.set_pid(12345);
    request.set_nebdversion("0.0.1");
    nebd::client::HeartbeatResponse response;
    for (int i = 0; i < 3; ++i) {
        auto* info = request.add_info();
        info->set_fd(i);
        info->set_name("/test/file" + std::to_string(i));
    }
    brpc::Channel channel;
    ASSERT_EQ(0, channel.InitWithSockFile(kSockFile_.c_str(), nullptr));
    nebd::client::NebdHeartbeatService_Stub stub(&channel);
    brpc::Controller cntl;

    // 正常情况
    EXPECT_CALL(*heartbeatManager_, UpdateFileTimestamp(_, _))
        .Times(3)
        .WillRepeatedly(Return(true));
    stub.KeepAlive(&cntl, &request, &response, nullptr);
    ASSERT_FALSE(cntl.Failed());
    ASSERT_EQ(nebd::client::RetCode::kOK, response.retcode());

    // 有文件更新时间戳失败
    EXPECT_CALL(*heartbeatManager_, UpdateFileTimestamp(_, _))
        .Times(3)
        .WillOnce(Return(false))
        .WillRepeatedly(Return(true));
    cntl.Reset();
    stub.KeepAlive(&cntl, &request, &response, nullptr);
    ASSERT_FALSE(cntl.Failed());
    ASSERT_EQ(nebd::client::RetCode::kNoOK, response.retcode());

    // 停止server
    server.Stop(0);
    server.Join();
}
}  // namespace server
}  // namespace nebd

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
