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
 * Created Date: 18-8-27
 * Author: wudemiao
 */

#include <gtest/gtest.h>
#include <unistd.h>
#include <brpc/server.h>

#include <cstdio>
#include <cstdlib>

#include "src/chunkserver/copyset_node_manager.h"
#include "src/chunkserver/copyset_node.h"
#include "test/chunkserver/mock_copyset_node.h"

namespace curve {
namespace chunkserver {

using ::testing::_;
using ::testing::Return;
using ::testing::NotNull;
using ::testing::Mock;
using ::testing::DoAll;
using ::testing::ReturnArg;
using ::testing::SetArgPointee;

butil::AtExitManager atExitManager;

using curve::fs::FileSystemType;

const char copysetUri[] = "local://./node_manager_test";
const int port = 9043;

class CopysetNodeManagerTest : public ::testing::Test {
 protected:
    void SetUp() {
        defaultOptions_.ip = "127.0.0.1";
        defaultOptions_.port = port;
        defaultOptions_.electionTimeoutMs = 1000;
        defaultOptions_.snapshotIntervalS = 30;
        defaultOptions_.catchupMargin = 50;
        defaultOptions_.chunkDataUri = copysetUri;
        defaultOptions_.chunkSnapshotUri = copysetUri;
        defaultOptions_.logUri = copysetUri;
        defaultOptions_.raftMetaUri = copysetUri;
        defaultOptions_.raftSnapshotUri = copysetUri;
        defaultOptions_.loadConcurrency = 5;
        defaultOptions_.checkRetryTimes = 3;
        defaultOptions_.finishLoadMargin = 1000;

        defaultOptions_.concurrentapply = &concurrentModule_;
        std::shared_ptr<LocalFileSystem> fs =
            LocalFsFactory::CreateFs(FileSystemType::EXT4, "");
        ASSERT_TRUE(nullptr != fs);
        defaultOptions_.localFileSystem = fs;
        defaultOptions_.chunkfilePool =
            std::make_shared<ChunkfilePool>(fs);
        defaultOptions_.trash = std::make_shared<Trash>();
    }

    void TearDown() {
        CopysetNodeManager *copysetNodeManager =
            &CopysetNodeManager::GetInstance();
        copysetNodeManager->Fini();
        ::system("rm -rf node_manager_test");
    }

 protected:
    CopysetNodeOptions  defaultOptions_;
    ConcurrentApplyModule concurrentModule_;
};

TEST_F(CopysetNodeManagerTest, ErrorOptionsTest) {
    LogicPoolID logicPoolId = 1;
    CopysetID copysetId = 10001;
    Configuration conf;
    CopysetNodeManager *copysetNodeManager = &CopysetNodeManager::GetInstance();

    defaultOptions_.chunkDataUri = "//.";
    defaultOptions_.logUri = "//.";
    ASSERT_EQ(0, copysetNodeManager->Init(defaultOptions_));

    ASSERT_FALSE(copysetNodeManager->CreateCopysetNode(logicPoolId,
                                                       copysetId,
                                                       conf));
}

TEST_F(CopysetNodeManagerTest, ServiceNotStartTest) {
    LogicPoolID logicPoolId = 1;
    CopysetID copysetId = 10001;
    Configuration conf;
    CopysetNodeManager *copysetNodeManager = &CopysetNodeManager::GetInstance();

    ASSERT_EQ(0, copysetNodeManager->Init(defaultOptions_));
    ASSERT_FALSE(copysetNodeManager->LoadFinished());
    ASSERT_EQ(0, copysetNodeManager->Run());
    ASSERT_FALSE(copysetNodeManager->CreateCopysetNode(logicPoolId,
                                                       copysetId,
                                                       conf));
    ASSERT_TRUE(copysetNodeManager->LoadFinished());

    /* null server */
    {
        brpc::Server *server = nullptr;
        int port = 9000;
        butil::EndPoint addr(butil::IP_ANY, port);
        ASSERT_EQ(-1, copysetNodeManager->AddService(server, addr));
    }
}

TEST_F(CopysetNodeManagerTest, NormalTest) {
    LogicPoolID logicPoolId = 1;
    CopysetID copysetId = 10001;
    Configuration conf;
    CopysetNodeManager *copysetNodeManager = &CopysetNodeManager::GetInstance();

    // start server
    brpc::Server server;
    butil::EndPoint addr(butil::IP_ANY, port);
    ASSERT_EQ(0, copysetNodeManager->AddService(&server, addr));
    if (server.Start(port, NULL) != 0) {
        LOG(FATAL) << "Fail to start Server";
    }

    ASSERT_EQ(0, copysetNodeManager->Init(defaultOptions_));

    // 本地 copyset 未加载完成，则无法创建新的copyset
    ASSERT_FALSE(copysetNodeManager->CreateCopysetNode(logicPoolId,
                                                       copysetId,
                                                       conf));
    ASSERT_EQ(0, copysetNodeManager->Run());
    ASSERT_TRUE(copysetNodeManager->CreateCopysetNode(logicPoolId,
                                                       copysetId,
                                                       conf));
    ASSERT_TRUE(copysetNodeManager->IsExist(logicPoolId, copysetId));
    // 重复创建
    ASSERT_FALSE(copysetNodeManager->CreateCopysetNode(logicPoolId,
                                                       copysetId,
                                                       conf));
    auto copysetNode1 =
            copysetNodeManager->GetCopysetNode(logicPoolId, copysetId);
    ASSERT_TRUE(nullptr != copysetNode1);
    auto copysetNode2 =
        copysetNodeManager->GetCopysetNode(logicPoolId + 1, copysetId + 1);
    ASSERT_TRUE(nullptr == copysetNode2);
    ASSERT_EQ(false, copysetNodeManager->DeleteCopysetNode(logicPoolId + 1,
                                                           copysetId + 1));
    std::vector<std::shared_ptr<CopysetNode>> copysetNodes;
    copysetNodeManager->GetAllCopysetNodes(&copysetNodes);
    ASSERT_EQ(1, copysetNodes.size());

    ASSERT_TRUE(copysetNodeManager->DeleteCopysetNode(logicPoolId,
                                                      copysetId));
    ASSERT_FALSE(copysetNodeManager->IsExist(logicPoolId, copysetId));

    ASSERT_EQ(0, copysetNodeManager->Fini());
    ASSERT_EQ(0, server.Stop(0));
    ASSERT_EQ(0, server.Join());
}

TEST_F(CopysetNodeManagerTest, CheckCopysetTest) {
    CopysetNodeManager *copysetNodeManager = &CopysetNodeManager::GetInstance();

    ASSERT_EQ(0, copysetNodeManager->Init(defaultOptions_));

    std::shared_ptr<MockCopysetNode> mockNode
        = std::make_shared<MockCopysetNode>();

    // 测试copyset node manager还没运行
    EXPECT_CALL(*mockNode, GetStatus(_)).Times(0);
    EXPECT_CALL(*mockNode, GetLeaderStatus(_)).Times(0);
    ASSERT_FALSE(copysetNodeManager->CheckCopysetUntilLoadFinished(mockNode));

    // 启动copyset node manager
    ASSERT_EQ(0, copysetNodeManager->Run());

    // 测试node为空
    EXPECT_CALL(*mockNode, GetStatus(_)).Times(0);
    EXPECT_CALL(*mockNode, GetLeaderStatus(_)).Times(0);
    ASSERT_FALSE(copysetNodeManager->CheckCopysetUntilLoadFinished(nullptr));

    // 测试无法获取到leader status的情况
    EXPECT_CALL(*mockNode, GetStatus(_)).Times(0);
    NodeStatus leaderStatus;
    EXPECT_CALL(*mockNode, GetLeaderStatus(_))
    .Times(defaultOptions_.checkRetryTimes)
    .WillRepeatedly(DoAll(SetArgPointee<0>(leaderStatus), Return(false)));
    ASSERT_FALSE(copysetNodeManager->CheckCopysetUntilLoadFinished(mockNode));

    leaderStatus.leader_id.parse("127.0.0.1:9043:0");
    // 测试leader first_index 大于 follower last_index的情况
    leaderStatus.first_index = 1000;
    NodeStatus followerStatus;
    followerStatus.last_index = 999;
    EXPECT_CALL(*mockNode, GetStatus(_)).Times(1)
    .WillOnce(SetArgPointee<0>(followerStatus));
    EXPECT_CALL(*mockNode, GetLeaderStatus(_))
    .WillOnce(DoAll(SetArgPointee<0>(leaderStatus), Return(true)));
    ASSERT_FALSE(copysetNodeManager->CheckCopysetUntilLoadFinished(mockNode));

    // 测试可以获取到leader status,且follower当前不在安装快照 的情况
    leaderStatus.first_index = 1;
    leaderStatus.committed_index = 2000;
    NodeStatus status1;
    NodeStatus status2;
    NodeStatus status3;
    NodeStatus status4;
    status1.last_index = 1666;
    status1.known_applied_index = 100;
    status2.last_index = 1666;
    status2.known_applied_index = 999;
    status3.last_index = 1666;
    status3.known_applied_index = 1000;
    status4.last_index = 1666;
    status4.known_applied_index = 1001;
    EXPECT_CALL(*mockNode, GetStatus(_))
    .Times(4)
    .WillOnce(SetArgPointee<0>(status1))
    .WillOnce(SetArgPointee<0>(status2))
    .WillOnce(SetArgPointee<0>(status3))
    .WillOnce(SetArgPointee<0>(status4));
    EXPECT_CALL(*mockNode, GetLeaderStatus(_))
    .Times(4)
    .WillRepeatedly(DoAll(SetArgPointee<0>(leaderStatus), Return(true)));
    ASSERT_TRUE(copysetNodeManager->CheckCopysetUntilLoadFinished(mockNode));
}

TEST_F(CopysetNodeManagerTest, ReloadTest) {
    LogicPoolID logicPoolId = 1;
    CopysetID copysetId = 10001;
    Configuration conf;
    CopysetNodeManager *copysetNodeManager = &CopysetNodeManager::GetInstance();

    // start server
    brpc::Server server;
    butil::EndPoint addr(butil::IP_ANY, port);
    ASSERT_EQ(0, copysetNodeManager->AddService(&server, addr));
    if (server.Start(port, NULL) != 0) {
        LOG(FATAL) << "Fail to start Server";
    }

    // 构造初始环境
    ASSERT_EQ(0, copysetNodeManager->Init(defaultOptions_));
    ASSERT_EQ(0, copysetNodeManager->Run());
    // 创建多个copyset
    int copysetNum = 5;
    for (int i = 0; i < copysetNum; ++i) {
        ASSERT_TRUE(copysetNodeManager->CreateCopysetNode(logicPoolId,
                                                          copysetId + i,
                                                          conf));
    }
    std::vector<std::shared_ptr<CopysetNode>> copysetNodes;
    copysetNodeManager->GetAllCopysetNodes(&copysetNodes);
    ASSERT_EQ(5, copysetNodes.size());
    ASSERT_EQ(0, copysetNodeManager->Fini());
    copysetNodes.clear();
    copysetNodeManager->GetAllCopysetNodes(&copysetNodes);
    ASSERT_EQ(0, copysetNodes.size());


    // 本地 copyset 未加载完成，则无法创建新的copyset
    ASSERT_FALSE(copysetNodeManager->CreateCopysetNode(logicPoolId,
                                                       copysetId + 5,
                                                       conf));

    // reload copysets when loadConcurrency < copysetNum
    std::cout << "Test ReloadCopysets when loadConcurrency=3" << std::endl;
    defaultOptions_.loadConcurrency = 3;
    ASSERT_EQ(0, copysetNodeManager->Init(defaultOptions_));
    ASSERT_EQ(0, copysetNodeManager->Run());
    copysetNodes.clear();
    copysetNodeManager->GetAllCopysetNodes(&copysetNodes);
    ASSERT_EQ(5, copysetNodes.size());
    ASSERT_EQ(0, copysetNodeManager->Fini());
    copysetNodes.clear();
    copysetNodeManager->GetAllCopysetNodes(&copysetNodes);
    ASSERT_EQ(0, copysetNodes.size());

    // reload copysets when loadConcurrency == copysetNum
    std::cout << "Test ReloadCopysets when loadConcurrency=5" << std::endl;
    defaultOptions_.loadConcurrency = 5;
    ASSERT_EQ(0, copysetNodeManager->Init(defaultOptions_));
    ASSERT_EQ(0, copysetNodeManager->Run());
    copysetNodes.clear();
    copysetNodeManager->GetAllCopysetNodes(&copysetNodes);
    ASSERT_EQ(5, copysetNodes.size());
    ASSERT_EQ(0, copysetNodeManager->Fini());
    copysetNodes.clear();
    copysetNodeManager->GetAllCopysetNodes(&copysetNodes);
    ASSERT_EQ(0, copysetNodes.size());

    // reload copysets when loadConcurrency > copysetNum
    std::cout << "Test ReloadCopysets when loadConcurrency=10" << std::endl;
    defaultOptions_.loadConcurrency = 10;
    ASSERT_EQ(0, copysetNodeManager->Init(defaultOptions_));
    ASSERT_EQ(0, copysetNodeManager->Run());
    copysetNodes.clear();
    copysetNodeManager->GetAllCopysetNodes(&copysetNodes);
    ASSERT_EQ(5, copysetNodes.size());
    ASSERT_EQ(0, copysetNodeManager->Fini());
    copysetNodes.clear();
    copysetNodeManager->GetAllCopysetNodes(&copysetNodes);
    ASSERT_EQ(0, copysetNodes.size());

    // reload copysets when loadConcurrency == 0
    std::cout << "Test ReloadCopysets when loadConcurrency=0" << std::endl;
    defaultOptions_.loadConcurrency = 0;
    ASSERT_EQ(0, copysetNodeManager->Init(defaultOptions_));
    ASSERT_EQ(0, copysetNodeManager->Run());
    copysetNodes.clear();
    copysetNodeManager->GetAllCopysetNodes(&copysetNodes);
    ASSERT_EQ(5, copysetNodes.size());
    ASSERT_EQ(0, copysetNodeManager->Fini());
    copysetNodes.clear();
    copysetNodeManager->GetAllCopysetNodes(&copysetNodes);
    ASSERT_EQ(0, copysetNodes.size());
}

}  // namespace chunkserver
}  // namespace curve
