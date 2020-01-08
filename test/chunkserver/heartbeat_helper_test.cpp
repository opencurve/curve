/*
 * Project: curve
 * Created Date: 2019-12-04
 * Author: lixiaocui
 * Copyright (c) 2018 netease
 */

#include <gtest/gtest.h>
#include "src/chunkserver/heartbeat_helper.h"
#include "test/chunkserver/mock_copyset_node.h"

using ::testing::_;
using ::testing::Return;
using ::testing::SetArgPointee;
using ::testing::DoAll;

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

}  // namespace chunkserver
}  // namespace curve

