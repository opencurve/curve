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

        // heartbeat related configuration settings
        conf->SetIntValue("mds.heartbeat.intervalMs", 100);
        conf->SetIntValue("mds.heartbeat.misstimeoutMs", 3000);
        conf->SetIntValue("mds.heartbeat.offlinetimeoutMs", 5000);
        conf->SetIntValue("mds.heartbeat.clean_follower_afterMs", sleepTimeMs_);

        // Mds listening port number
        conf->SetStringValue("mds.listen.addr", "127.0.0.1:6880");

        // scheduler related content
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
 * Bug Description: In a stability testing environment, when one machine crashes, it is set to "pending," and during the replica recovery process, there is MDS switching. 
                    Eventually, it was found that there were 5 "pending" chunk servers that did not complete migration.
 
 * Analysis:
 * 1. When MDS1 is providing services, it generates an operator and sends it to 
 *    copyset-1 {A, B, C} + D for modification, where C is in an offline state.
 * 2. Copyset-1 completes the configuration change. At this point, the configuration on the leader is updated to epoch=2/{A, B, C, D}, 
 *    the configuration on the candidate is epoch=1/{A, B, C}, and the configuration recorded in MDS1 is epoch=1/{A, B, C}.
 * 3. MDS1 crashes, and MDS2 takes over the service. MDS2 loads copysets from the database, and the configuration for copyset-1 in MDS2 is epoch=1/{A, B, C}.
 * 4. Candidate-D reports a heartbeat, and the configuration for copyset-1 is epoch=1/{A, B, C}. 
 *    MDS2 finds that the epoch reported by D matches the one recorded in MDS2, but D is not in the replication group recorded by MDS2, 
 *    and there is no corresponding operator in the scheduling module. As a result, a command is issued to delete copyset-1 on D, leading to an accidental deletion of D.
 *
 *  Solution:
 *  Under normal circumstances, the heartbeat module should wait for a certain period (currently configured as 20 minutes) after MDS starts before issuing a command to delete a copyset. 
 *  This greatly ensures that during this time, the configuration on the copyset-leader is updated in MDS, 
 *  preventing the accidental deletion of data on replicas that have just joined the replication group.
 *
 * The starting point for this time should be when MDS officially starts providing external services, 
 * rather than the MDS startup time. If it is set based on the MDS startup time, then if the standby MDS starts much later but can still provide services, it could be deleted immediately, leading to the bug.
 */
TEST_F(HeartbeatExceptionTest, test_mdsRestart_opLost) {
    // 1. copyset-1(epoch=2, peers={1,2,3}, leader=1)
    // In the scheduler, there is an operator that +4
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

    // 2. leader reports copyset-1(epoch=2, peers={1,2,3}, leader=1)
    //    mds issued configuration changes
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(leader, &req);
    CopySetInfo csInfo(key.first, key.second);
    BuildCopySetInfo(&csInfo, startEpoch, leader, peers);
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    // Check the response and issue configuration changes for+D
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

    // 4. The candidate reports the outdated configuration compared to MDS (the candidate replays logs one by one to apply the old configuration):
    //    copyset-1(epoch=1, peers={1,2,3}, leader=1)
    //    Because mds.heartbeat.clean_follower_afterMs time has not yet elapsed, MDS cannot issue
    //    deletion commands. MDS issues no commands, so the data on the candidate will not be accidentally deleted.
    rep.Clear();
    req.Clear();
    hbtest_->BuildBasicChunkServerRequest(candidate, &req);
    BuildCopySetInfo(&csInfo, startEpoch - 1, leader, peers);
    hbtest_->AddCopySetToRequest(&req, csInfo);
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    // Check the response, it is empty
    ASSERT_EQ(0, rep.needupdatecopysets_size());

    // 5. Sleep mds.heartbeat.clean_follower_afterMs + 10ms, 
    //    candidate reports staled copyset-1(epoch=1, peers={1,2,3}, leader=1)
    //    mds issues a deletion configuration, and the data on the candidate will be mistakenly deleted
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

    // 6. leader reports the latest configuration copyset-1(epoch=3, peers={1,2,3,4}, leader=1)
    auto newPeers = peers;
    newPeers.emplace(candidate);
    auto newEpoch = startEpoch + 1;
    rep.Clear();
    req.Clear();
    hbtest_->BuildBasicChunkServerRequest(leader, &req);
    BuildCopySetInfo(&csInfo, startEpoch + 1, leader, newPeers);
    hbtest_->AddCopySetToRequest(&req, csInfo);
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    // Check the response, it is empty
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // Check the data of mdstopology
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(hbtest_->topology_->GetCopySet(key, &copysetInfo));
    ASSERT_EQ(newEpoch, copysetInfo.GetEpoch());
    ASSERT_EQ(leader, copysetInfo.GetLeader());
    ASSERT_EQ(newPeers, copysetInfo.GetCopySetMembers());

    // 7. candidate reports staled copyset-1(epoch=1, peers={1,2,3}, leader=1)
    //    mds does not distribute configuration
    rep.Clear();
    req.Clear();
    hbtest_->BuildBasicChunkServerRequest(candidate, &req);
    BuildCopySetInfo(&csInfo, startEpoch - 1, leader, peers);
    hbtest_->AddCopySetToRequest(&req, csInfo);
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    // Check the response and issue the copyset current configuration guide candidate to delete data
    ASSERT_EQ(0, rep.needupdatecopysets_size());
}

}  // namespace mds
}  // namespace curve
