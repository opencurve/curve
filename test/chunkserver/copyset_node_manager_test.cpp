/*
 * Project: curve
 * Created Date: 18-8-27
 * Author: wudemiao
 * Copyright (c) 2018 netease
 */

#include <gtest/gtest.h>
#include <unistd.h>
#include <brpc/server.h>

#include <cstdio>
#include <cstdlib>

#include "src/chunkserver/copyset_node_manager.h"
#include "src/chunkserver/copyset_node.h"

namespace curve {
namespace chunkserver {

TEST(CopysetNodeManager, basic) {
    // for is exist
    LogicPoolID  logicPoolId = 1;
    CopysetID copysetId = 10001;
    Configuration conf;
    GroupId groupId = ToGroupId(logicPoolId, copysetId).c_str();
    CopysetNodeManager *copysetNodeManager = &CopysetNodeManager::GetInstance();

    ASSERT_FALSE(copysetNodeManager->IsExist(logicPoolId, copysetId));

    int port = 9000;
    CopysetNodeOptions copysetNodeOptions;
    copysetNodeOptions.ip = "127.0.0.1";
    copysetNodeOptions.port = port;
    copysetNodeOptions.snapshotIntervalS = 30;
    copysetNodeOptions.catchupMargin = 50;
    copysetNodeOptions.chunkDataUri = "local://.";
    copysetNodeOptions.chunkSnapshotUri = "local://.";
    copysetNodeOptions.logUri = "local://.";
    copysetNodeOptions.raftMetaUri = "local://.";
    copysetNodeOptions.raftSnapshotUri = "local://.";
    ASSERT_EQ(0, copysetNodeManager->Init(copysetNodeOptions));

    brpc::Server server;
    butil::EndPoint addr(butil::IP_ANY,  port);
    ASSERT_EQ(0, copysetNodeManager->AddService(&server, addr));
    if (server.Start(port, NULL) != 0) {
        LOG(FATAL) << "Fail to start Server";
    }
    ASSERT_TRUE(copysetNodeManager->CreateCopysetNode(logicPoolId, copysetId, conf));
    ASSERT_TRUE(copysetNodeManager->IsExist(logicPoolId, copysetId));
    ASSERT_FALSE(copysetNodeManager->CreateCopysetNode(logicPoolId, copysetId, conf));

    auto copysetNode = copysetNodeManager->GetCopysetNode(logicPoolId, copysetId);
    ASSERT_TRUE(nullptr != copysetNode);
    std::vector<std::shared_ptr<CopysetNode>> copysetNodes;
    copysetNodeManager->GetAllCopysetNodes(&copysetNodes);
    ASSERT_EQ(1, copysetNodes.size());

    ASSERT_TRUE(copysetNodeManager->DeleteCopysetNode(logicPoolId, copysetId));
    ASSERT_FALSE(copysetNodeManager->IsExist(logicPoolId, copysetId));
}

}  // namespace chunkserver
}  // namespace curve
