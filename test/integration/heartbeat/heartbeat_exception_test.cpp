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
 * Created Date: 2019-11-28
 * Author: lixiaocui
 */

#include <gtest/gtest.h>
#include <glog/logging.h>

#include "test/integration/heartbeat/common.h"

using std::string;

namespace curve {
namespace mds {
class HeartbeatExceptionTest : public ::testing::Test {
 protected:
    void InitConfiguration(Configuration *conf) {
        conf->SetIntValue("mds.topology.ChunkServerStateUpdateSec", 0);

        //Heartbeat related configuration settings
        conf->SetIntValue("mds.heartbeat.intervalMs", 100);
        conf->SetIntValue("mds.heartbeat.misstimeoutMs", 3000);
        conf->SetIntValue("mds.heartbeat.offlinetimeoutMs", 5000);
        conf->SetIntValue("mds.heartbeat.clean_follower_afterMs", sleepTimeMs_);

        //Mds listening port number
        conf->SetStringValue("mds.listen.addr", "127.0.0.1:6880");

        //Schedule related content
        conf->SetBoolValue("mds.enable.copyset.scheduler", false);
        conf->SetBoolValue("mds.enable.leader.scheduler", false);
        conf->SetBoolValue("mds.enable.recover.scheduler", false);
        conf->SetBoolValue("mds.replica.replica.scheduler", false);

        conf->SetIntValue("mds.copyset.scheduler.intervalSec", 300);
        conf->SetIntValue("mds.leader.scheduler.intervalSec", 300);
        conf->SetIntValue("mds.recover.scheduler.intervalSec", 300);
        conf->SetIntValue("mds.replica.scheduler.intervalSec", 300);
        conf->SetIntValue("mds.scheduler.change.limitSec", 300);

        conf->SetIntValue("mds.schduler.operator.concurrent", 4);
        conf->SetIntValue("mds.schduler.transfer.limitSec", 10);
        conf->SetIntValue("mds.scheduler.add.limitSec", 10);
        conf->SetIntValue("mds.scheduler.remove.limitSec", 10);
        conf->SetDoubleValue("mds.scheduler.copysetNumRangePercent", 0.05);
        conf->SetDoubleValue("mds.schduler.scatterWidthRangePerent", 0.2);
        conf->SetIntValue("mds.scheduler.minScatterWidth", 50);
    }

    void BuildCopySetInfo(CopySetInfo *info, uint64_t epoch,
                          ChunkServerIdType leader,
                          const std::set<ChunkServerIdType> &members,
                          ChunkServerIdType candidateId = UNINTIALIZE_ID) {
        info->SetEpoch(epoch);
        info->SetLeader(leader);
        info->SetCopySetMembers(members);
        if (candidateId != UNINTIALIZE_ID) {
            info->SetCandidate(candidateId);
        }
    }

    void SetUp() override {
        sleepTimeMs_ = 500;
        Configuration conf;
        InitConfiguration(&conf);
        hbtest_ = std::make_shared<HeartbeatIntegrationCommon>(conf);
        hbtest_->BuildBasicCluster();
    }

    void TearDown() {
        ASSERT_EQ(0, hbtest_->server_.Stop(100));
        ASSERT_EQ(0, hbtest_->server_.Join());
    }

 protected:
    std::shared_ptr<HeartbeatIntegrationCommon> hbtest_;
    int64_t sleepTimeMs_;
};

/*
 *Bug description: Stability testing environment, setting pending after one machine is down, and switching between mds during replica recovery process
 *Finally, it was found that 5 pending chunkservers did not complete the migration
 *Analysis:
 *1 When mds1 provides services, an operator is generated and sent to copyset-1 {A, B, C}+
 *Change of D, C is offline status
 *2 Copyset-1 completes the configuration change, and the configuration on the leader is updated to epoch=2/{A, B, C, D},
 *The configuration on candidate is epoch=1/{A, B, C}, while the configuration recorded in mds1 is epoch=1/{A, B, C}
 *3 Mds1 crashes, mds2 provides services, and loads copyset from the database. The configuration of copyset-1 in mds2
 *  Epoch=1/{A, B, C}
 *4 Candidate-D reports the heartbeat, and the configuration of copyset-1 is epoch=1/{A, B, C}. Mds2 found that D reported
 *The epoch and mds2 records in the copyset are the same, but D is not in the copy group of the mds2 record and the scheduling module does not either
 *Corresponding operator, issuing command to delete copyset-1 on D caused D to be mistakenly deleted
 *
 *Solution:
 *Under normal circumstances, the heartbeat module can only issue the deletion copyset after a certain period of time (currently configured as 20 minutes) after MDS is started
 *The command ensures that the configuration on the copyset header is updated to mds with a high probability during this period,
 *Prevent data on newly joined replica groups from being mistakenly deleted
 *
 *The starting point of this time should be the time when mds officially provides services to the public, rather than the start time of mds. If set to mds startup
 *If the backup mds can provide services after a long time of startup, it can be immediately deleted, causing bugs
 */
TEST_F(HeartbeatExceptionTest, test_mdsRestart_opLost) {
    // 1. copyset-1(epoch=2, peers={1,2,3}, leader=1)
    //There are+4 operators in the scheduler
    CopySetKey key{ 1, 1 };
    int startEpoch = 2;
    ChunkServerIdType leader = 1;
    ChunkServerIdType candidate = 4;
    std::set<ChunkServerIdType> peers{ 1, 2, 3 };
    ChunkServer cs(4, "testtoekn", "nvme", 3, "10.198.100.3", 9001, "/");
    hbtest_->PrepareAddChunkServer(cs);
    Operator op(2, key, OperatorPriority::NormalPriority,
                std::chrono::steady_clock::now(), std::make_shared<AddPeer>(4));
    op.timeLimit = std::chrono::seconds(3);
    hbtest_->AddOperatorToOpController(op);

    //2 Leader reports copyset-1 (epoch=2, peers={1,2,3}, leader=1)
    //MDS issued configuration changes
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(leader, &req);
    CopySetInfo csInfo(key.first, key.second);
    BuildCopySetInfo(&csInfo, startEpoch, leader, peers);
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    //Check the response and issue configuration changes for+D
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    CopySetConf conf = rep.needupdatecopysets(0);
    ASSERT_EQ(key.first, conf.logicalpoolid());
    ASSERT_EQ(key.second, conf.copysetid());
    ASSERT_EQ(peers.size(), conf.peers_size());
    ASSERT_EQ(startEpoch, conf.epoch());
    ASSERT_EQ(ConfigChangeType::ADD_PEER, conf.type());
    ASSERT_EQ("10.198.100.3:9001:0", conf.configchangeitem().address());

    //3 Clear the optimizer in mds (simulate mds restart)
    hbtest_->RemoveOperatorFromOpController(key);

    //4 Candidate reports outdated and MDS configurations (the old configuration will be applied one by one when the candidate plays back the logs):
    //    copyset-1(epoch=1, peers={1,2,3}, leader=1)
    //Due to mds. heartbeat. clean_ Follower_ The afterMs time has not arrived yet, and the mds cannot be distributed yet
    //Delete command. The mds distribution is empty, and the data on the candidate will not be deleted by mistake
    rep.Clear();
    req.Clear();
    hbtest_->BuildBasicChunkServerRequest(candidate, &req);
    BuildCopySetInfo(&csInfo, startEpoch - 1, leader, peers);
    hbtest_->AddCopySetToRequest(&req, csInfo);
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    //Check the response, it is empty
    ASSERT_EQ(0, rep.needupdatecopysets_size());

    //5 Sleep mds. heartbeat. clean_ Follower_ After Ms+10ms
    //Candidate reports staled copyset-1 (epoch=1, peers={1,2,3}, leader=1)
    //MDS issues a deletion configuration, and the data on the candidate will be mistakenly deleted
    usleep((sleepTimeMs_ + 10) * 1000);
    rep.Clear();
    req.Clear();
    hbtest_->BuildBasicChunkServerRequest(candidate, &req);
    BuildCopySetInfo(&csInfo, startEpoch - 1, leader, peers);
    hbtest_->AddCopySetToRequest(&req, csInfo);
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    conf = rep.needupdatecopysets(0);
    ASSERT_EQ(key.first, conf.logicalpoolid());
    ASSERT_EQ(key.second, conf.copysetid());
    ASSERT_EQ(peers.size(), conf.peers_size());
    ASSERT_EQ(startEpoch, conf.epoch());

    //6 The leader reports the latest configuration copyset-1 (epoch=3, peers={1,2,3,4}, leader=1)
    auto newPeers = peers;
    newPeers.emplace(candidate);
    auto newEpoch = startEpoch + 1;
    rep.Clear();
    req.Clear();
    hbtest_->BuildBasicChunkServerRequest(leader, &req);
    BuildCopySetInfo(&csInfo, startEpoch + 1, leader, newPeers);
    hbtest_->AddCopySetToRequest(&req, csInfo);
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    //Check the response, it is empty
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check the data of mdstopology
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(hbtest_->topology_->GetCopySet(key, &copysetInfo));
    ASSERT_EQ(newEpoch, copysetInfo.GetEpoch());
    ASSERT_EQ(leader, copysetInfo.GetLeader());
    ASSERT_EQ(newPeers, copysetInfo.GetCopySetMembers());

    //7 Candidate reports staled copyset-1 (epoch=1, peers={1,2,3}, leader=1)
    //MDS does not distribute configuration
    rep.Clear();
    req.Clear();
    hbtest_->BuildBasicChunkServerRequest(candidate, &req);
    BuildCopySetInfo(&csInfo, startEpoch - 1, leader, peers);
    hbtest_->AddCopySetToRequest(&req, csInfo);
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    //Check the response and issue the copyset current configuration guide candidate to delete data
    ASSERT_EQ(0, rep.needupdatecopysets_size());
}

}  // namespace mds
}  // namespace curve
