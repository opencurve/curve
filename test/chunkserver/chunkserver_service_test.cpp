/*
 * Project: curve
 * Created Date: 2020-01-14
 * Author: lixiaocui1
 * Copyright (c) 2018 netease
 */

#include <gmock/gmock.h>
#include <brpc/controller.h>
#include <brpc/channel.h>
#include <brpc/server.h>
#include <gtest/gtest.h>
#include "src/chunkserver/chunkserver_service.h"
#include "test/chunkserver/mock_copyset_node_manager.h"
#include "proto/chunkserver.pb.h"

namespace curve {
namespace chunkserver {

using ::testing::Return;
using ::testing::_;

TEST(ChunkServerServiceImplTest, test_ChunkServerStatus) {
    // 启动ChunkServerService
    auto server = new brpc::Server();
    MockCopysetNodeManager* copysetNodeManager = new MockCopysetNodeManager();
    ChunkServerServiceImpl* chunkserverService =
        new ChunkServerServiceImpl(copysetNodeManager);
    ASSERT_EQ(0,
        server->AddService(chunkserverService, brpc::SERVER_OWNS_SERVICE));
    ASSERT_EQ(0, server->Start("127.0.0.1", {5900, 5999}, nullptr));
    auto listenAddr = butil::endpoint2str(server->listen_address()).c_str();


    brpc::Channel channel;
    ASSERT_EQ(0, channel.Init(listenAddr, NULL));
    ChunkServerService_Stub stub(&channel);
    ChunkServerStatusRequest request;
    ChunkServerStatusResponse response;

    // 1. 指定chunkserver加载copyset完成
    {
        EXPECT_CALL(*copysetNodeManager, LoadFinished())
            .WillOnce(Return(false));
        brpc::Controller cntl;
        stub.ChunkServerStatus(&cntl, &request, &response, nullptr);
        ASSERT_FALSE(cntl.Failed());
        ASSERT_FALSE(response.copysetloadfin());
    }

    // 2. 指定chunkserver加载copyset未完成
    {
        EXPECT_CALL(*copysetNodeManager, LoadFinished())
            .WillOnce(Return(true));
        brpc::Controller cntl;
        stub.ChunkServerStatus(&cntl, &request, &response, nullptr);
        ASSERT_FALSE(cntl.Failed());
        ASSERT_TRUE(response.copysetloadfin());
    }

    // 停止chunkserver service
    server->Stop(0);
    server->Join();
    delete server;
    server = nullptr;

    // 3. 未获取到指定chunkserver加载copyset状态
    {
        brpc::Controller cntl;
        stub.ChunkServerStatus(&cntl, &request, &response, nullptr);
        ASSERT_TRUE(cntl.Failed());
    }

    delete copysetNodeManager;
}

}  // namespace chunkserver
}  // namespace curve
