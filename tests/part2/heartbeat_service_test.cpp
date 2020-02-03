/*
 * Project: nebd
 * Created Date: 2020-02-03
 * Author: charisu
 * Copyright (c) 2020 netease
 */

#include <gtest/gtest.h>
#include <brpc/channel.h>
#include <brpc/server.h>
#include <string>

#include "src/common/heartbeat.pb.h"
#include "src/part2/heartbeat_service.h"
#include "tests/part2/mock_file_manager.h"

using ::testing::_;
using ::testing::Return;

namespace nebd {
namespace server {

class HeartbeatServiceTest : public ::testing::Test {
 protected:
    void SetUp() override {
        fileManager_ = std::make_shared<MockFileManager>();
    }
    std::shared_ptr<MockFileManager>  fileManager_;
    const std::string kSockFile_ = "/tmp/heartbeat_service_test.sock";
};

TEST_F(HeartbeatServiceTest, KeepAlive) {
    // 启动server
    brpc::Server server;
    NebdHeartbeatServiceImpl heartbeatService(fileManager_);
    ASSERT_EQ(0, server.AddService(&heartbeatService,
                        brpc::SERVER_DOESNT_OWN_SERVICE));
    brpc::ServerOptions option;
    option.idle_timeout_sec = -1;
    ASSERT_EQ(0, server.StartAtSockFile(kSockFile_.c_str(), &option));

    nebd::client::HeartbeatRequest request;
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
    EXPECT_CALL(*fileManager_, UpdateFileTimestamp(_))
        .Times(3)
        .WillRepeatedly(Return(0));
    stub.KeepAlive(&cntl, &request, &response, nullptr);
    ASSERT_FALSE(cntl.Failed());
    ASSERT_EQ(nebd::client::RetCode::kOK, response.retcode());

    // 有文件更新时间戳失败
    EXPECT_CALL(*fileManager_, UpdateFileTimestamp(_))
        .Times(3)
        .WillOnce(Return(-1))
        .WillRepeatedly(Return(0));
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

