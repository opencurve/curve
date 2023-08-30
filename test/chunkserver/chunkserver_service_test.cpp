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
 * Created Date: 2020-01-14
 * Author: lixiaocui1
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
    //Start ChunkServerService
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

    //1 Specify chunkserver to load copyset complete
    {
        EXPECT_CALL(*copysetNodeManager, LoadFinished())
            .WillOnce(Return(false));
        brpc::Controller cntl;
        stub.ChunkServerStatus(&cntl, &request, &response, nullptr);
        ASSERT_FALSE(cntl.Failed());
        ASSERT_FALSE(response.copysetloadfin());
    }

    //2 The specified chunkserver loading copyset did not complete
    {
        EXPECT_CALL(*copysetNodeManager, LoadFinished())
            .WillOnce(Return(true));
        brpc::Controller cntl;
        stub.ChunkServerStatus(&cntl, &request, &response, nullptr);
        ASSERT_FALSE(cntl.Failed());
        ASSERT_TRUE(response.copysetloadfin());
    }

    //Stop chunkserver service
    server->Stop(0);
    server->Join();
    delete server;
    server = nullptr;

    //3 Unable to obtain the specified chunkserver loading copyset status
    {
        brpc::Controller cntl;
        stub.ChunkServerStatus(&cntl, &request, &response, nullptr);
        ASSERT_TRUE(cntl.Failed());
    }

    delete copysetNodeManager;
}

}  // namespace chunkserver
}  // namespace curve
