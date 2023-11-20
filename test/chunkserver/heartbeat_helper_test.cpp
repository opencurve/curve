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
 * Created Date: 2019-12-04
 * Author: lixiaocui
 */

#include "src/chunkserver/heartbeat_helper.h"

#include <brpc/channel.h>
#include <brpc/controller.h>
#include <brpc/server.h>
#include <butil/endpoint.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "src/chunkserver/chunkserver_service.h"
#include "test/chunkserver/mock_copyset_node.h"
#include "test/chunkserver/mock_copyset_node_manager.h"

using ::testing::_;
using ::testing::DoAll;
using ::testing::Mock;
using ::testing::Return;
using ::testing::SetArgPointee;

namespace curve {
namespace chunkserver {
TEST(HeartbeatHelperTest, test_BuildNewPeers) {
    CopySetConf conf;
    conf.set_logicalpoolid(1);
    conf.set_copysetid(1);
    conf.set_epoch(2);
    std::vector<Peer> newPeers;

    // 1. Destination node format error
    {
        // The target node is empty
        ASSERT_FALSE(HeartbeatHelper::BuildNewPeers(conf, &newPeers));

        // The target node is not empty but has incorrect format
        auto replica = new ::curve::common::Peer();
        replica->set_address("192.0.0.4");
        conf.set_allocated_configchangeitem(replica);
        ASSERT_FALSE(HeartbeatHelper::BuildNewPeers(conf, &newPeers));

        conf.clear_configchangeitem();
        replica = new ::curve::common::Peer();
        replica->set_address("192.0.0.4:8200:0");
        conf.set_allocated_configchangeitem(replica);
    }

    // 2. The format of the node to be deleted is incorrect
    {
        // The node to be deleted is empty
        ASSERT_FALSE(HeartbeatHelper::BuildNewPeers(conf, &newPeers));

        // The node to be deleted is not empty but has incorrect format
        auto replica = new ::curve::common::Peer();
        replica->set_address("192.0.0.1");
        conf.set_allocated_oldpeer(replica);
        ASSERT_FALSE(HeartbeatHelper::BuildNewPeers(conf, &newPeers));

        conf.clear_oldpeer();
        replica = new ::curve::common::Peer();
        replica->set_address("192.0.0.1:8200:0");
        conf.set_allocated_oldpeer(replica);
    }

    // 3. Successfully generated new configuration
    {
        for (int i = 0; i < 3; i++) {
            auto replica = conf.add_peers();
            replica->set_id(i + 1);
            replica->set_address("192.0.0." + std::to_string(i + 1) +
                                 ":8200:0");
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

    // 1. There is no copyset that needs to be changed in chunkserver
    { ASSERT_FALSE(HeartbeatHelper::CopySetConfValid(conf, copyset)); }

    // 2. The epoch of copysetConf issued by mds is outdated
    {
        copyset = std::make_shared<MockCopysetNode>();
        EXPECT_CALL(*copyset, GetConfEpoch()).Times(2).WillOnce(Return(3));
        ASSERT_FALSE(HeartbeatHelper::CopySetConfValid(conf, copyset));
    }

    // 3. Mds sends copysetConf normally
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

    // 1. MDS issued empty configuration
    {
        conf.set_epoch(0);
        ASSERT_TRUE(HeartbeatHelper::NeedPurge(csEp, conf, copyset));
    }

    // 2. The replica is not in the replication group
    {
        conf.set_epoch(2);
        for (int i = 2; i <= 4; i++) {
            auto replica = conf.add_peers();
            replica->set_id(i);
            replica->set_address("192.0.0." + std::to_string(i) + ":8200:0");
        }
        ASSERT_TRUE(HeartbeatHelper::NeedPurge(csEp, conf, copyset));
    }

    // 3. This replica is in the replication group
    {
        butil::str2endpoint("192.0.0.4:8200", &csEp);
        ASSERT_FALSE(HeartbeatHelper::NeedPurge(csEp, conf, copyset));
    }
}

TEST(HeartbeatHelperTest, test_ChunkServerLoadCopySetFin) {
    // 1. The format of peerId is incorrect
    {
        std::string peerId = "127.0.0:5555:0";
        ASSERT_FALSE(HeartbeatHelper::ChunkServerLoadCopySetFin(peerId));
    }

    // 2. Opposite chunkserver_service not started
    {
        std::string peerId = "127.0.0.1:8888:0";
        ASSERT_FALSE(HeartbeatHelper::ChunkServerLoadCopySetFin(peerId));
    }

    auto server = new brpc::Server();
    MockCopysetNodeManager* copysetNodeManager = new MockCopysetNodeManager();
    ChunkServerServiceImpl* chunkserverService =
        new ChunkServerServiceImpl(copysetNodeManager);
    ASSERT_EQ(
        0, server->AddService(chunkserverService, brpc::SERVER_OWNS_SERVICE));
    ASSERT_EQ(0, server->Start("127.0.0.1", {5900, 5999}, nullptr));
    string listenAddr(butil::endpoint2str(server->listen_address()).c_str());

    // 3. Peer copyset not loaded completed
    {
        EXPECT_CALL(*copysetNodeManager, LoadFinished())
            .WillOnce(Return(false));
        ASSERT_FALSE(HeartbeatHelper::ChunkServerLoadCopySetFin(listenAddr));
    }

    // 4. End to end copyset loading completed
    {
        EXPECT_CALL(*copysetNodeManager, LoadFinished()).WillOnce(Return(true));
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
