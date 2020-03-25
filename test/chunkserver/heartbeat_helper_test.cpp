/*
 * Project: curve
 * Created Date: 2019-12-04
 * Author: lixiaocui
 * Copyright (c) 2018 netease
 */

#include <gmock/gmock.h>
#include <brpc/controller.h>
#include <brpc/channel.h>
#include <brpc/server.h>
#include <gtest/gtest.h>
#include <butil/endpoint.h>
#include "src/chunkserver/heartbeat_helper.h"
#include "src/chunkserver/chunkserver_service.h"
#include "test/chunkserver/mock_copyset_node.h"
#include "test/chunkserver/mock_copyset_node_manager.h"

using ::testing::_;
using ::testing::Return;
using ::testing::SetArgPointee;
using ::testing::DoAll;
using ::testing::Mock;

namespace curve {
namespace chunkserver {
TEST(HeartbeatHelperTest, test_BuildNewPeers) {
    CopySetConf conf;
    conf.set_logicalpoolid(1);
    conf.set_copysetid(1);
    conf.set_epoch(2);
    std::vector<Peer> newPeers;

    // 1. 目标节点格式错误
    {
        // 目标节点为空
        ASSERT_FALSE(HeartbeatHelper::BuildNewPeers(conf, &newPeers));

        // 目标节点不为空但格式有误
        auto replica = new ::curve::common::Peer();
        replica->set_address("192.0.0.4");
        conf.set_allocated_configchangeitem(replica);
        ASSERT_FALSE(HeartbeatHelper::BuildNewPeers(conf, &newPeers));

        conf.clear_configchangeitem();
        replica = new ::curve::common::Peer();
        replica->set_address("192.0.0.4:8200:0");
        conf.set_allocated_configchangeitem(replica);
    }

    // 2. 待删除节点格式错误
    {
        // 待删除节点为空
        ASSERT_FALSE(HeartbeatHelper::BuildNewPeers(conf, &newPeers));

        // 待删除接节点不为空但格式有误
        auto replica = new ::curve::common::Peer();
        replica->set_address("192.0.0.1");
        conf.set_allocated_oldpeer(replica);
        ASSERT_FALSE(HeartbeatHelper::BuildNewPeers(conf, &newPeers));

        conf.clear_oldpeer();
        replica = new ::curve::common::Peer();
        replica->set_address("192.0.0.1:8200:0");
        conf.set_allocated_oldpeer(replica);
    }

    // 3. 生成新配置成功
    {
        for (int i = 0; i < 3; i++) {
             auto replica = conf.add_peers();
             replica->set_id(i + 1);
             replica->set_address(
                 "192.0.0." + std::to_string(i + 1) + ":8200:0");
        }
        ASSERT_TRUE(HeartbeatHelper::BuildNewPeers(conf, &newPeers));
        ASSERT_EQ(3, newPeers.size());
        ASSERT_EQ("192.0.0.2:8200:0", newPeers[0].address());
        ASSERT_EQ("192.0.0.3:8200:0", newPeers[1].address());
        ASSERT_EQ("192.0.0.4:8200:0", newPeers[2].address());
    }
}

TEST(HeartbeatHelperTest, test_PeerVaild) {
    ASSERT_FALSE(HeartbeatHelper::PeerVaild("192.0.0.1"));
    ASSERT_TRUE(HeartbeatHelper::PeerVaild("192.0.0.1:8200"));
    ASSERT_TRUE(HeartbeatHelper::PeerVaild("192.0.0.1:8200:0"));
}

TEST(HeartbeatHelperTest, test_CopySetConfValid) {
    CopySetConf conf;
    conf.set_logicalpoolid(1);
    conf.set_copysetid(1);
    conf.set_epoch(2);

    std::shared_ptr<MockCopysetNode> copyset;

    // 1. chunkserver中不存在需要变更的copyset
    {
        ASSERT_FALSE(HeartbeatHelper::CopySetConfValid(conf, copyset));
    }

    // 2. mds下发copysetConf的epoch是落后的
    {
        copyset = std::make_shared<MockCopysetNode>();
        EXPECT_CALL(*copyset, GetConfEpoch()).Times(2).WillOnce(Return(3));
        ASSERT_FALSE(HeartbeatHelper::CopySetConfValid(conf, copyset));
    }

    // 3. mds下发copysetConf正常
    {
        EXPECT_CALL(*copyset, GetConfEpoch()).WillOnce(Return(2));
        ASSERT_TRUE(HeartbeatHelper::CopySetConfValid(conf, copyset));
    }
}

TEST(HeartbeatHelperTest, test_NeedPurge) {
    butil::EndPoint csEp;
    butil::str2endpoint("192.0.0.1:8200", &csEp);

    CopySetConf conf;
    conf.set_logicalpoolid(1);
    conf.set_copysetid(1);
    conf.set_epoch(2);

    auto copyset = std::make_shared<MockCopysetNode>();

    // 1. mds下发空配置
    {
        conf.set_epoch(0);
        ASSERT_TRUE(HeartbeatHelper::NeedPurge(csEp, conf, copyset));
    }

    // 2. 该副本不在复制组中
    {
        conf.set_epoch(2);
        for (int i = 2; i <= 4; i++) {
             auto replica = conf.add_peers();
             replica->set_id(i);
             replica->set_address("192.0.0." + std::to_string(i) + ":8200:0");
        }
        ASSERT_TRUE(HeartbeatHelper::NeedPurge(csEp, conf, copyset));
    }

    // 3. 该副本在复制组中
    {
        butil::str2endpoint("192.0.0.4:8200", &csEp);
        ASSERT_FALSE(HeartbeatHelper::NeedPurge(csEp, conf, copyset));
    }
}

TEST(HeartbeatHelperTest, test_ChunkServerLoadCopySetFin) {
    // 1. peerId的格式不对
    {
        std::string peerId = "127.0.0:5555:0";
        ASSERT_FALSE(HeartbeatHelper::ChunkServerLoadCopySetFin(peerId));
    }

    // 2. 对端的chunkserver_service未起起来
    {
        std::string peerId = "127.0.0.1:8888:0";
        ASSERT_FALSE(HeartbeatHelper::ChunkServerLoadCopySetFin(peerId));
    }


    auto server = new brpc::Server();
    MockCopysetNodeManager* copysetNodeManager = new MockCopysetNodeManager();
    ChunkServerServiceImpl* chunkserverService =
        new ChunkServerServiceImpl(copysetNodeManager);
    ASSERT_EQ(0,
        server->AddService(chunkserverService, brpc::SERVER_OWNS_SERVICE));
    ASSERT_EQ(0, server->Start("127.0.0.1", {5900, 5999}, nullptr));
    string listenAddr(butil::endpoint2str(server->listen_address()).c_str());

    // 3. 对端copyset未加载完成
    {
        EXPECT_CALL(*copysetNodeManager, LoadFinished())
            .WillOnce(Return(false));
        ASSERT_FALSE(HeartbeatHelper::ChunkServerLoadCopySetFin(listenAddr));
    }

    // 4. 对端copyset加载完成
    {
        EXPECT_CALL(*copysetNodeManager, LoadFinished())
            .WillOnce(Return(true));
        ASSERT_TRUE(HeartbeatHelper::ChunkServerLoadCopySetFin(listenAddr));
    }

    server->Stop(0);
    server->Join();
    delete server;
    server = nullptr;
    delete copysetNodeManager;
}

}  // namespace chunkserver
}  // namespace curve

