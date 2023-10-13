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
 * Created Date: 2019-12-05
 * Author: lixiaocui
 */

#include "src/chunkserver/heartbeat.h"

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <string>

#include "include/chunkserver/chunkserver_common.h"
#include "src/chunkserver/cli.h"
#include "src/common/configuration.h"
#include "test/chunkserver/heartbeat_test_common.h"
#include "test/client/fake/fakeMDS.h"

std::string mdsMetaServerAddr = "127.0.0.1:9300";  // NOLINT

namespace curve {
namespace chunkserver {
const LogicPoolID poolId = 666;
const CopysetID copysetId = 888;

class HeartbeatTest : public ::testing::Test {
 public:
    void SetUp() {
        RemovePeersData();
        std::string filename = "fakemds";
        hbtest_ = std::make_shared<HeartbeatTestCommon>(filename);
    }

    void TearDown() {
        hbtest_->CleanPeer(poolId, copysetId, "127.0.0.1:8200:0");
        hbtest_->CleanPeer(poolId, copysetId, "127.0.0.1:8201:0");
        hbtest_->CleanPeer(poolId, copysetId, "127.0.0.1:8202:0");

        hbtest_->ReleaseHeartbeat();
        hbtest_->UnInitializeMds();
    }

 protected:
    std::shared_ptr<HeartbeatTestCommon> hbtest_;
};

TEST_F(HeartbeatTest, TransferLeader) {
    // Create copyset
    std::vector<string> cslist{"127.0.0.1:8200", "127.0.0.1:8201",
                               "127.0.0.1:8202"};
    std::string confStr = "127.0.0.1:8200:0,127.0.0.1:8201:0,127.0.0.1:8202:0";
    std::string dest1 = "127.0.0.1:8200:0";
    std::string dest2 = "127.0.0.1:8201:0";

    hbtest_->CreateCopysetPeers(poolId, copysetId, cslist, confStr);
    hbtest_->WaitCopysetReady(poolId, copysetId, confStr);

    // Construct the expected CopySetInfo for req, with the expectleader being
    // dst1
    ::curve::mds::heartbeat::CopySetInfo expect;
    expect.set_logicalpoolid(poolId);
    expect.set_copysetid(copysetId);
    for (int j = 0; j < 3; j++) {
        auto replica = expect.add_peers();
        replica->set_address("127.0.0.1:820" + std::to_string(j) + ":0");
    }
    expect.set_epoch(2);
    auto peer = new ::curve::common::Peer();
    peer->set_address(dest1);
    expect.set_allocated_leaderpeer(peer);

    // Construct CopySetConf in resp, transfer to dst
    CopySetConf conf;
    conf.set_logicalpoolid(poolId);
    conf.set_copysetid(copysetId);
    for (int j = 0; j < 3; j++) {
        auto replica = conf.add_peers();
        replica->set_address("127.0.0.1:820" + std::to_string(j) + ":0");
    }
    peer = new ::curve::common::Peer();
    peer->set_address(dest1);
    conf.set_allocated_configchangeitem(peer);
    conf.set_type(curve::mds::heartbeat::TRANSFER_LEADER);

    // Waiting for successful change
    ASSERT_TRUE(hbtest_->WailForConfigChangeOk(conf, expect, 30 * 1000));

    // Construct the expected CopySetInfo for req, with the expectleader being
    // dst2
    peer = new ::curve::common::Peer();
    peer->set_address(dest2);
    expect.set_allocated_leaderpeer(peer);

    // Construct CopySetConf in resp, transfer to dst2
    peer = new ::curve::common::Peer();
    peer->set_address(dest2);
    conf.set_allocated_configchangeitem(peer);

    // Waiting for successful change
    ASSERT_TRUE(hbtest_->WailForConfigChangeOk(conf, expect, 30 * 1000));
}

TEST_F(HeartbeatTest, RemovePeer) {
    // Create copyset
    std::vector<string> cslist{"127.0.0.1:8200", "127.0.0.1:8201",
                               "127.0.0.1:8202"};
    std::string confStr = "127.0.0.1:8200:0,127.0.0.1:8201:0,127.0.0.1:8202:0";
    std::string leaderPeer = "127.0.0.1:8200:0";
    std::string destPeer = "127.0.0.1:8202:0";

    hbtest_->CreateCopysetPeers(poolId, copysetId, cslist, confStr);
    hbtest_->WaitCopysetReady(poolId, copysetId, confStr);
    hbtest_->TransferLeaderSync(poolId, copysetId, confStr, leaderPeer);

    // Construct the CopySetInfo expected in req
    ::curve::mds::heartbeat::CopySetInfo expect;
    expect.set_logicalpoolid(poolId);
    expect.set_copysetid(copysetId);
    for (int j = 0; j < 2; j++) {
        auto replica = expect.add_peers();
        replica->set_address("127.0.0.1:820" + std::to_string(j) + ":0");
    }
    expect.set_epoch(2);

    // Construct CopySetConf in resp
    CopySetConf conf;
    conf.set_logicalpoolid(poolId);
    conf.set_copysetid(copysetId);
    for (int j = 0; j < 3; j++) {
        auto replica = conf.add_peers();
        replica->set_address("127.0.0.1:820" + std::to_string(j) + ":0");
    }
    auto peer = new ::curve::common::Peer();
    peer->set_address(destPeer);
    conf.set_allocated_configchangeitem(peer);
    conf.set_type(curve::mds::heartbeat::REMOVE_PEER);

    // Waiting for successful change
    ASSERT_TRUE(hbtest_->WailForConfigChangeOk(conf, expect, 30 * 1000));
}

TEST_F(HeartbeatTest, CleanPeer_after_Configchange) {
    // Create copyset
    std::vector<string> cslist{"127.0.0.1:8200"};
    std::string confStr = "127.0.0.1:8200:0";

    hbtest_->CreateCopysetPeers(poolId, copysetId, cslist, confStr);
    hbtest_->WaitCopysetReady(poolId, copysetId, confStr);

    // Construct the CopySetInfo expected in req
    ::curve::mds::heartbeat::CopySetInfo expect;

    // Construct CopySetConf in resp
    CopySetConf conf;
    conf.set_logicalpoolid(poolId);
    conf.set_copysetid(copysetId);

    // Waiting for successful change
    ASSERT_TRUE(hbtest_->WailForConfigChangeOk(conf, expect, 30 * 1000));
}

TEST_F(HeartbeatTest, CleanPeer_not_exist_in_MDS) {
    // Create a copyset on chunkserver
    std::vector<string> cslist{"127.0.0.1:8202"};
    std::string confStr = "127.0.0.1:8202:0";

    hbtest_->CreateCopysetPeers(poolId, copysetId, cslist, confStr);
    hbtest_->WaitCopysetReady(poolId, copysetId, confStr);

    // Construct the CopySetInfo expected in req
    ::curve::mds::heartbeat::CopySetInfo expect;

    // Construct CopySetConf in resp
    CopySetConf conf;
    conf.set_logicalpoolid(poolId);
    conf.set_copysetid(copysetId);
    conf.set_epoch(0);

    // Waiting for successful change
    ASSERT_TRUE(hbtest_->WailForConfigChangeOk(conf, expect, 30 * 1000));
}

TEST_F(HeartbeatTest, AddPeer) {
    // Create copyset
    std::vector<string> cslist{"127.0.0.1:8200", "127.0.0.1:8201",
                               "127.0.0.1:8202"};
    std::string confStr = "127.0.0.1:8200:0,127.0.0.1:8201:0";
    std::string addPeer = "127.0.0.1:8202:0";

    hbtest_->CreateCopysetPeers(poolId, copysetId, cslist, confStr);
    hbtest_->WaitCopysetReady(poolId, copysetId, confStr);

    // Construct the CopySetInfo expected in req
    ::curve::mds::heartbeat::CopySetInfo expect;
    expect.set_logicalpoolid(poolId);
    expect.set_copysetid(copysetId);
    for (int j = 0; j < 3; j++) {
        auto replica = expect.add_peers();
        replica->set_address("127.0.0.1:820" + std::to_string(j) + ":0");
    }
    expect.set_epoch(2);

    // Construct CopySetConf in resp
    CopySetConf conf;
    conf.set_logicalpoolid(poolId);
    conf.set_copysetid(copysetId);
    for (int j = 0; j < 2; j++) {
        auto replica = conf.add_peers();
        replica->set_address("127.0.0.1:820" + std::to_string(j) + ":0");
    }
    auto peer = new ::curve::common::Peer();
    peer->set_address(addPeer);
    conf.set_allocated_configchangeitem(peer);
    conf.set_type(curve::mds::heartbeat::ADD_PEER);

    // Waiting for successful change
    ASSERT_TRUE(hbtest_->WailForConfigChangeOk(conf, expect, 30 * 1000));
}

TEST_F(HeartbeatTest, ChangePeer) {
    // Create copyset
    std::vector<string> cslist{"127.0.0.1:8200", "127.0.0.1:8201",
                               "127.0.0.1:8202"};
    std::string oldConf = "127.0.0.1:8200:0,127.0.0.1:8202:0";
    std::string addOne = "127.0.0.1:8201:0";
    std::string rmOne = "127.0.0.1:8202:0";

    hbtest_->CreateCopysetPeers(poolId, copysetId, cslist, oldConf);
    hbtest_->WaitCopysetReady(poolId, copysetId, oldConf);

    // Construct the CopySetInfo expected in req
    ::curve::mds::heartbeat::CopySetInfo expect;
    expect.set_logicalpoolid(poolId);
    expect.set_copysetid(copysetId);
    auto replica = expect.add_peers();
    replica->set_address("127.0.0.1:8200:0");
    replica = expect.add_peers();
    replica->set_address("127.0.0.1:8201:0");
    expect.set_epoch(2);

    // Construct CopySetConf in resp
    CopySetConf conf;
    conf.set_logicalpoolid(poolId);
    conf.set_copysetid(copysetId);
    replica = conf.add_peers();
    replica->set_address("127.0.0.1:8200:0");
    replica = conf.add_peers();
    replica->set_address("127.0.0.1:8202:0");

    auto peer = new ::curve::common::Peer();
    peer->set_address(addOne);
    conf.set_allocated_configchangeitem(peer);
    peer = new ::curve::common::Peer();
    peer->set_address(rmOne);
    conf.set_allocated_oldpeer(peer);
    conf.set_type(curve::mds::heartbeat::CHANGE_PEER);

    // Waiting for successful change
    ASSERT_TRUE(hbtest_->WailForConfigChangeOk(conf, expect, 30 * 1000));
}

}  // namespace chunkserver
}  // namespace curve
