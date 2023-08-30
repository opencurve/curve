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
 * Created Date: 2019-06-11
 * Author: lixiaocui
 */

#include <gtest/gtest.h>
#include <glog/logging.h>

#include "test/integration/heartbeat/common.h"

using std::string;

namespace curve {
namespace mds {

class HeartbeatBasicTest : public ::testing::Test {
 protected:
    void InitConfiguration(Configuration *conf) {
        conf->SetIntValue("mds.topology.ChunkServerStateUpdateSec", 0);

        //Heartbeat related configuration settings
        conf->SetIntValue("mds.heartbeat.intervalMs", 100);
        conf->SetIntValue("mds.heartbeat.misstimeoutMs", 300);
        conf->SetIntValue("mds.heartbeat.offlinetimeoutMs", 500);
        conf->SetIntValue("mds.heartbeat.clean_follower_afterMs", 0);

        //Mds listening port number
        conf->SetStringValue("mds.listen.addr", "127.0.0.1:6879");

        //Schedule related content
        conf->SetBoolValue("mds.enable.copyset.scheduler", false);
        conf->SetBoolValue("mds.enable.leader.scheduler", false);
        conf->SetBoolValue("mds.enable.recover.scheduler", false);
        conf->SetBoolValue("mds.replica.replica.scheduler", false);

        conf->SetIntValue("mds.copyset.scheduler.intervalSec", 300);
        conf->SetIntValue("mds.leader.scheduler.intervalSec", 300);
        conf->SetIntValue("mds.recover.scheduler.intervalSec", 300);
        conf->SetIntValue("mds.replica.scheduler.intervalSec", 300);

        conf->SetIntValue("mds.schduler.operator.concurrent", 4);
        conf->SetIntValue("mds.schduler.transfer.limitSec", 10);
        conf->SetIntValue("mds.scheduler.add.limitSec", 10);
        conf->SetIntValue("mds.scheduler.remove.limitSec", 10);
        conf->SetDoubleValue("mds.scheduler.copysetNumRangePercent", 0.05);
        conf->SetDoubleValue("mds.schduler.scatterWidthRangePerent", 0.2);
        conf->SetIntValue("mds.scheduler.minScatterWidth", 50);
    }

    void PrepareMdsWithCandidateOpOnGoing() {
        //Construct the current state of copyset in mds
        ChunkServer cs(10, "testtoekn", "nvme", 3, "10.198.100.3", 9001, "/");
        hbtest_->PrepareAddChunkServer(cs);
        hbtest_->UpdateCopysetTopo(1, 1, 5, 1,
                                   std::set<ChunkServerIdType>{ 1, 2, 3 }, 10);

        //Construct the current state of the scheduler
        Operator op(5, CopySetKey{ 1, 1 }, OperatorPriority::NormalPriority,
                    std::chrono::steady_clock::now(),
                    std::make_shared<AddPeer>(10));
        op.timeLimit = std::chrono::seconds(3);
        hbtest_->AddOperatorToOpController(op);
    }

    void PrepareMdsNoCnandidateOpOnGoing() {
        //Construct the current state of copyset in mds
        // copyset-1(epoch=5, peers={1,2,3}, leader=1);
        ChunkServer cs(10, "testtoekn", "nvme", 3, "10.198.100.3", 9001, "/");
        hbtest_->PrepareAddChunkServer(cs);
        hbtest_->UpdateCopysetTopo(1, 1, 5, 1,
                                   std::set<ChunkServerIdType>{ 1, 2, 3 });

        //Construct the current state of the scheduler
        Operator op(5, CopySetKey{ 1, 1 }, OperatorPriority::NormalPriority,
                    std::chrono::steady_clock::now(),
                    std::make_shared<AddPeer>(10));
        op.timeLimit = std::chrono::seconds(3);
        hbtest_->AddOperatorToOpController(op);
    }

    void PrepareMdsWithRemoveOp() {
        //Mds has copyset-1 (epoch=5, peers={1,2,3,4}, leader=1);
        //There is an operator in copyset-1 in the scheduler: startEpoch=5, step=RemovePeer<4>
        ChunkServer cs(4, "testtoken", "nvme", 3, "10.198.100.3", 9001, "/");
        hbtest_->PrepareAddChunkServer(cs);
        hbtest_->UpdateCopysetTopo(1, 1, 5, 1,
                                   std::set<ChunkServerIdType>{ 1, 2, 3, 4 });

        Operator op(5, CopySetKey{ 1, 1 }, OperatorPriority::NormalPriority,
                    std::chrono::steady_clock::now(),
                    std::make_shared<RemovePeer>(4));
        op.timeLimit = std::chrono::seconds(3);
        hbtest_->AddOperatorToOpController(op);
    }

    void PrepareMdsWithRemoveOpOnGoing() {
        //Mds has copyset-1 (epoch=5, peers={1,2,3,4}, leader=1, candidate=4);
        //There is an operator in copyset-1 in the scheduler: startEpoch=5, step=RemovePeer<4>
        ChunkServer cs(4, "testtoken", "nvme", 3, "10.198.100.3", 9001, "/");
        hbtest_->PrepareAddChunkServer(cs);
        hbtest_->UpdateCopysetTopo(
            1, 1, 5, 1, std::set<ChunkServerIdType>{ 1, 2, 3, 4 }, 4);

        Operator op(5, CopySetKey{ 1, 1 }, OperatorPriority::NormalPriority,
                    std::chrono::steady_clock::now(),
                    std::make_shared<RemovePeer>(4));
        op.timeLimit = std::chrono::seconds(3);
        hbtest_->AddOperatorToOpController(op);
    }

    void PrepareMdsWithTransferOp() {
        //Mds has copyset-1 (epoch=5, peers={1,2,3}, leader=1);
        //Copyset-1 in the scheduler has operator: startEpoch=5, step=TransferLeader {1>2}
        hbtest_->UpdateCopysetTopo(1, 1, 5, 1,
                                   std::set<ChunkServerIdType>{ 1, 2, 3 });

        Operator op(5, CopySetKey{ 1, 1 }, OperatorPriority::NormalPriority,
                    std::chrono::steady_clock::now(),
                    std::make_shared<TransferLeader>(1, 2));
        op.timeLimit = std::chrono::seconds(3);
        hbtest_->AddOperatorToOpController(op);
    }

    void PrepareMdsWithTransferOpOnGoing() {
        //Mds has copyset-1 (epoch=5, peers={1,2,3}, leader=1, candidate=2);
        //Copyset-1 in the scheduler has operator: startEpoch=5, step=TransferLeader {1>2}
        hbtest_->UpdateCopysetTopo(1, 1, 5, 1,
                                   std::set<ChunkServerIdType>{ 1, 2, 3 }, 2);

        Operator op(5, CopySetKey{ 1, 1 }, OperatorPriority::NormalPriority,
                    std::chrono::steady_clock::now(),
                    std::make_shared<TransferLeader>(1, 2));
        op.timeLimit = std::chrono::seconds(3);
        hbtest_->AddOperatorToOpController(op);
    }

    void PrePareMdsWithCandidateNoOp() {
        //Mds has copyset-1 (epoch=5, peers={1,2,3}, leader=1, candidate=4);
        ChunkServer cs(4, "testtoken", "nvme", 3, "10.198.100.3", 9001, "/");
        hbtest_->PrepareAddChunkServer(cs);
        hbtest_->UpdateCopysetTopo(1, 1, 5, 1,
                                   std::set<ChunkServerIdType>{ 1, 2, 3 }, 4);
    }

    void PrepareMdsWithChangeOp() {
        //Mds has copyset-1 (epoch=5, peers={1,2,3}, leader=1);
        //Copyset-1 in the scheduler has operator: startEpoch=5, step=ChangePeer {3>4}
        ChunkServer cs(4, "testtoken", "nvme", 3, "10.198.100.3", 9001, "/");
        hbtest_->PrepareAddChunkServer(cs);
        hbtest_->UpdateCopysetTopo(1, 1, 5, 1,
                                   std::set<ChunkServerIdType>{ 1, 2, 3 });

        Operator op(5, CopySetKey{ 1, 1 }, OperatorPriority::NormalPriority,
                    std::chrono::steady_clock::now(),
                    std::make_shared<ChangePeer>(3, 4));
        op.timeLimit = std::chrono::seconds(3);
        hbtest_->AddOperatorToOpController(op);
    }

    void PrepareMdsWithChangeOpOnGoing() {
        //Mds has copyset-1 (epoch=5, peers={1,2,3}, leader=1, candidate=4);
        //In the scheduler, copyset-1 has operator: startEpoch=5, step=ChangePeer {3>4}
        ChunkServer cs(4, "testtoken", "nvme", 3, "10.198.100.3", 9001, "/");
        hbtest_->PrepareAddChunkServer(cs);
        hbtest_->UpdateCopysetTopo(1, 1, 5, 1,
                                   std::set<ChunkServerIdType>{ 1, 2, 3 }, 4);

        Operator op(5, CopySetKey{ 1, 1 }, OperatorPriority::NormalPriority,
                    std::chrono::steady_clock::now(),
                    std::make_shared<ChangePeer>(3, 4));
        op.timeLimit = std::chrono::seconds(3);
        hbtest_->AddOperatorToOpController(op);
    }

    bool ValidateCopySet(const ::curve::mds::topology::CopySetInfo &expected) {
        ::curve::mds::topology::CopySetInfo copysetInfo;
        if (!hbtest_->topology_->GetCopySet(
                CopySetKey{ expected.GetLogicalPoolId(), expected.GetId() },
                &copysetInfo)) {
            return false;
        }

        if (expected.GetLeader() != copysetInfo.GetLeader()) {
            return false;
        }

        if (expected.GetEpoch() != copysetInfo.GetEpoch()) {
            return false;
        }

        if (expected.HasCandidate() && !copysetInfo.HasCandidate()) {
            return false;
        }

        if (!expected.HasCandidate() && copysetInfo.HasCandidate()) {
            return false;
        }

        if (expected.HasCandidate()) {
            if (expected.GetCandidate() != copysetInfo.GetCandidate()) {
                return false;
            }
        }

        if (expected.GetCopySetMembers() != copysetInfo.GetCopySetMembers()) {
            return false;
        }

        return true;
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
};

TEST_F(HeartbeatBasicTest, test_request_no_chunkserverID) {
    //Empty HeartbeatRequest
    ChunkServerHeartbeatRequest req;
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBFAIL, &rep);
}

TEST_F(HeartbeatBasicTest, test_mds_donnot_has_this_chunkserver) {
    //The chunkserver does not exist in the mds
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(3, &req);
    req.set_chunkserverid(4);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    ASSERT_EQ(::curve::mds::heartbeat::hbChunkserverUnknown, rep.statuscode());
}

TEST_F(HeartbeatBasicTest, test_chunkserver_ip_port_not_match) {
    //The id reported by chunkserver is the same, but the IP and port do not match
    //IP mismatch
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(3, &req);
    req.set_ip("127.0.0.1");
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    ASSERT_EQ(::curve::mds::heartbeat::hbChunkserverIpPortNotMatch,
              rep.statuscode());

    //Port mismatch
    req.set_ip("10.198.100.3");
    req.set_port(1111);
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    ASSERT_EQ(::curve::mds::heartbeat::hbChunkserverIpPortNotMatch,
              rep.statuscode());

    //Token mismatch
    req.set_ip("10.198.100.3");
    req.set_port(9000);
    req.set_token("youdao");
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    ASSERT_EQ(::curve::mds::heartbeat::hbChunkserverTokenNotMatch,
              rep.statuscode());
}

TEST_F(HeartbeatBasicTest, test_chunkserver_offline_then_online) {
    //Chunkserver reports that the heartbeat time interval is greater than offline
    //Sleep 800ms, the chunkserver offline status
    std::this_thread::sleep_for(std::chrono::milliseconds(800));
    ChunkServer out;
    hbtest_->topology_->GetChunkServer(1, &out);
    ASSERT_EQ(OnlineState::OFFLINE, out.GetOnlineState());

    //Chunkserver reports heartbeat, chunkserver online
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(out.GetId(), &req);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //The backend health check program updates chunksrver to online status
    uint64_t now = ::curve::common::TimeUtility::GetTimeofDaySec();
    bool updateSuccess = false;
    while (::curve::common::TimeUtility::GetTimeofDaySec() - now <= 2) {
        hbtest_->topology_->GetChunkServer(1, &out);
        if (OnlineState::ONLINE == out.GetOnlineState()) {
            updateSuccess = true;
            break;
        }
    }
    ASSERT_TRUE(updateSuccess);
}

TEST_F(HeartbeatBasicTest, test_copysets_in_mds_is_initial_state_condition1) {
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(1, &req);
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(
        hbtest_->topology_->GetCopySet(CopySetKey{ 1, 1 }, &copysetInfo));

    // copyset-1(epoch=1, peers={1,2,3}, leader=1)
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 1, 1, copysetInfo.GetCopySetMembers());
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    ASSERT_TRUE(
        hbtest_->topology_->GetCopySet(CopySetKey{ 1, 1 }, &copysetInfo));
    ASSERT_EQ(1, copysetInfo.GetEpoch());
    ASSERT_EQ(1, copysetInfo.GetLeader());
}

TEST_F(HeartbeatBasicTest, test_copysets_in_mds_is_initial_state_condition2) {
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(1, &req);
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(
        hbtest_->topology_->GetCopySet(CopySetKey{ 1, 1 }, &copysetInfo));
    ChunkServer cs(4, "testtoken", "nvme", 3, "10.198.100.3", 9001, "/");
    hbtest_->PrepareAddChunkServer(cs);

    // copyset-1(epoch=1, peers={1,2,3}, leader=1,
    // conf.gChangeInfo={peer: 4, type: AddPeer})
    CopySetInfo csInfo(copysetInfo.GetLogicalPoolId(), copysetInfo.GetId());
    BuildCopySetInfo(&csInfo, 1, 1, copysetInfo.GetCopySetMembers(), 4);
    hbtest_->AddCopySetToRequest(&req, csInfo, ConfigChangeType::ADD_PEER);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    ASSERT_TRUE(
        hbtest_->topology_->GetCopySet(CopySetKey{ 1, 1 }, &copysetInfo));
    ASSERT_EQ(1, copysetInfo.GetEpoch());
    ASSERT_EQ(1, copysetInfo.GetLeader());
    ASSERT_TRUE(copysetInfo.HasCandidate());
    ASSERT_EQ(4, copysetInfo.GetCandidate());
}

TEST_F(HeartbeatBasicTest, test_copysets_in_mds_is_initial_state_condition3) {
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(1, &req);
    ::curve::mds::topology::CopySetInfo copysetInfo;
    CopySetKey key{ 1, 1 };
    ASSERT_TRUE(hbtest_->topology_->GetCopySet(key, &copysetInfo));
    ChunkServer cs(5, "testtoken", "nvme", 3, "10.198.100.3", 9001, "/");
    hbtest_->PrepareAddChunkServer(cs);

    // copyset-1(epoch=2, peers={1,2,3,5}, leader=1)
    auto copysetMembers = copysetInfo.GetCopySetMembers();
    copysetMembers.emplace(5);
    CopySetInfo csInfo(key.first, key.second);
    BuildCopySetInfo(&csInfo, 1, 1, copysetMembers);
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    ASSERT_TRUE(
        hbtest_->topology_->GetCopySet(CopySetKey{ 1, 1 }, &copysetInfo));
    ASSERT_EQ(1, copysetInfo.GetEpoch());
    ASSERT_EQ(1, copysetInfo.GetLeader());
    std::set<ChunkServerIdType> res{ 1, 2, 3, 5 };
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
    ASSERT_EQ(0, rep.needupdatecopysets_size());
}

TEST_F(HeartbeatBasicTest, test_copysets_in_mds_initial_state_condition4) {
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(1, &req);
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(
        hbtest_->topology_->GetCopySet(CopySetKey{ 1, 1 }, &copysetInfo));
    ChunkServer cs4(4, "testtoken", "nvme", 3, "10.198.100.3", 9001, "/");
    hbtest_->PrepareAddChunkServer(cs4);
    ChunkServer cs5(5, "testtoken", "nvme", 3, "10.198.100.3", 9002, "/");
    hbtest_->PrepareAddChunkServer(cs5);

    // copyset-1(epoch=2, peers={1,2,3,5}, leader=1,
    // conf.ChangeInfo={peer: 4, type: AddPeer})
    auto copysetMembers = copysetInfo.GetCopySetMembers();
    copysetMembers.emplace(5);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 1, 1, copysetMembers, 4);
    hbtest_->AddCopySetToRequest(&req, csInfo, ConfigChangeType::ADD_PEER);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    ASSERT_TRUE(
        hbtest_->topology_->GetCopySet(CopySetKey{ 1, 1 }, &copysetInfo));
    ASSERT_EQ(1, copysetInfo.GetEpoch());
    ASSERT_EQ(1, copysetInfo.GetLeader());
    std::set<ChunkServerIdType> res{ 1, 2, 3, 5 };
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
    ASSERT_EQ(4, copysetInfo.GetCandidate());
    ASSERT_EQ(0, rep.needupdatecopysets_size());
}

TEST_F(HeartbeatBasicTest, test_copysets_in_mds_initial_state_condition5) {
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(
        hbtest_->topology_->GetCopySet(CopySetKey{ 1, 1 }, &copysetInfo));

    // copyset-1(epoch=0, peers={1,2,3}, leader=0)
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 0, copysetInfo.GetCopySetMembers());
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    ASSERT_TRUE(
        hbtest_->topology_->GetCopySet(CopySetKey{ 1, 1 }, &copysetInfo));
    ASSERT_EQ(0, copysetInfo.GetEpoch());
    ASSERT_EQ(0, copysetInfo.GetLeader());
    std::set<ChunkServerIdType> res{ 1, 2, 3 };
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
    ASSERT_EQ(0, rep.needupdatecopysets_size());
}

TEST_F(HeartbeatBasicTest, test_copysets_in_mds_initial_state_condition6) {
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(
        hbtest_->topology_->GetCopySet(CopySetKey{ 1, 1 }, &copysetInfo));

    // copyset-1(epoch=0, peers={1,2,3}, leader=1)
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 1, copysetInfo.GetCopySetMembers());
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    ASSERT_TRUE(
        hbtest_->topology_->GetCopySet(CopySetKey{ 1, 1 }, &copysetInfo));
    ASSERT_EQ(0, copysetInfo.GetEpoch());
    ASSERT_EQ(0, copysetInfo.GetLeader());
    std::set<ChunkServerIdType> res{ 1, 2, 3 };
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
    ASSERT_EQ(0, rep.needupdatecopysets_size());
}

TEST_F(HeartbeatBasicTest, test_copysets_in_mds_initial_state_condition7) {
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(
        hbtest_->topology_->GetCopySet(CopySetKey{ 1, 1 }, &copysetInfo));

    // copyset-1(epoch=0, peers={1,2,3}, leader=1)
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 1, copysetInfo.GetCopySetMembers());
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    ASSERT_TRUE(
        hbtest_->topology_->GetCopySet(CopySetKey{ 1, 1 }, &copysetInfo));
    ASSERT_EQ(0, copysetInfo.GetEpoch());
    ASSERT_EQ(0, copysetInfo.GetLeader());
    std::set<ChunkServerIdType> res{ 1, 2, 3 };
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
    ASSERT_EQ(0, rep.needupdatecopysets_size());
}

TEST_F(HeartbeatBasicTest, test_copysets_in_mds_initial_state_condition8) {
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(
        hbtest_->topology_->GetCopySet(CopySetKey{ 1, 1 }, &copysetInfo));

    // copyset-1(epoch=1, peers={1,2,3}, leader=0)
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 1, 0, copysetInfo.GetCopySetMembers());
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    ASSERT_TRUE(
        hbtest_->topology_->GetCopySet(CopySetKey{ 1, 1 }, &copysetInfo));
    ASSERT_EQ(0, copysetInfo.GetEpoch());
    ASSERT_EQ(0, copysetInfo.GetLeader());
    std::set<ChunkServerIdType> res{ 1, 2, 3 };
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
    ASSERT_EQ(0, rep.needupdatecopysets_size());
}

TEST_F(HeartbeatBasicTest, test_copysets_in_mds_initial_state_condition9) {
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(
        hbtest_->topology_->GetCopySet(CopySetKey{ 1, 1 }, &copysetInfo));
    ChunkServer cs(4, "testtoken", "nvme", 3, "10.198.100.3", 9001, "/");
    hbtest_->PrepareAddChunkServer(cs);

    //Report copyset-1 (epoch=2, peers={1,2,3,4}, leader=1)
    auto copysetMembers = copysetInfo.GetCopySetMembers();
    copysetMembers.emplace(4);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 2, 1, copysetMembers);
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    ASSERT_TRUE(
        hbtest_->topology_->GetCopySet(CopySetKey{ 1, 1 }, &copysetInfo));
    ASSERT_EQ(0, copysetInfo.GetEpoch());
    ASSERT_EQ(0, copysetInfo.GetLeader());
    std::set<ChunkServerIdType> res{ 1, 2, 3 };
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
    ASSERT_EQ(0, rep.needupdatecopysets_size());
}

TEST_F(HeartbeatBasicTest, test_copysets_in_mds_initial_state_condition10) {
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(
        hbtest_->topology_->GetCopySet(CopySetKey{ 1, 1 }, &copysetInfo));
    ChunkServer cs(4, "testtoken", "nvme", 3, "10.198.100.3", 9001, "/");
    hbtest_->PrepareAddChunkServer(cs);

    // copyset-1(epoch=2, peers={1,2,3，4}, leader=0)
    auto copysetMembers = copysetInfo.GetCopySetMembers();
    copysetMembers.emplace(4);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 2, 0, copysetMembers);
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    ASSERT_TRUE(
        hbtest_->topology_->GetCopySet(CopySetKey{ 1, 1 }, &copysetInfo));
    ASSERT_EQ(0, copysetInfo.GetEpoch());
    ASSERT_EQ(0, copysetInfo.GetLeader());
    std::set<ChunkServerIdType> res{ 1, 2, 3 };
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
    ASSERT_EQ(0, rep.needupdatecopysets_size());
}

//Reported as the leader
TEST_F(HeartbeatBasicTest, test_leader_report_consistent_with_mds) {
    //Update copyset-1 in topology (epoch=2, peers={1,2,3}, leader=1)
    hbtest_->UpdateCopysetTopo(1, 1, 2, 1,
                               std::set<ChunkServerIdType>{ 1, 2, 3 });

    //Copyset-1 reported by chunkserver1 (epoch=2, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(1, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 2, 1, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(
        hbtest_->topology_->GetCopySet(CopySetKey{ 1, 1 }, &copysetInfo));
    ASSERT_EQ(2, copysetInfo.GetEpoch());
    ASSERT_EQ(1, copysetInfo.GetLeader());
    std::set<ChunkServerIdType> res{ 1, 2, 3 };
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_leader_report_epoch_bigger) {
    //Update copyset-1 in topology (epoch=2, peers={1,2,3}, leader=1)
    hbtest_->UpdateCopysetTopo(1, 1, 2, 1,
                               std::set<ChunkServerIdType>{ 1, 2, 3 });

    //Copyset-1 reported by chunkserver1 (epoch=5, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(1, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 1, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Response is empty, mds updates epoch to 5
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(
        hbtest_->topology_->GetCopySet(CopySetKey{ 1, 1 }, &copysetInfo));
    ASSERT_EQ(5, copysetInfo.GetEpoch());
    ASSERT_EQ(1, copysetInfo.GetLeader());
    std::set<ChunkServerIdType> res{ 1, 2, 3 };
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_leader_report_epoch_bigger_leader_not_same) {
    //Update copyset-1 in topology (epoch=2, peers={1,2,3}, leader=1)
    hbtest_->UpdateCopysetTopo(1, 1, 2, 1,
                               std::set<ChunkServerIdType>{ 1, 2, 3 });

    //Copyset-1 reported by chunkserver2 (epoch=5, peers={1,2,3}, leader=2)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 2, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Response is empty, mds updates epoch to 5, and leader to 2
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(
        hbtest_->topology_->GetCopySet(CopySetKey{ 1, 1 }, &copysetInfo));
    ASSERT_EQ(5, copysetInfo.GetEpoch());
    ASSERT_EQ(2, copysetInfo.GetLeader());
    std::set<ChunkServerIdType> res{ 1, 2, 3 };
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
}

//Reported as a follower
TEST_F(HeartbeatBasicTest, test_follower_report_consistent_with_mds) {
    //Update copyset-1 in topology (epoch=2, peers={1,2,3}, leader=1)
    hbtest_->UpdateCopysetTopo(1, 1, 2, 1,
                               std::set<ChunkServerIdType>{ 1, 2, 3 });

    //Copyset-1 reported by chunkserver2 (epoch=2, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 2, 1, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Response is empty
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(
        hbtest_->topology_->GetCopySet(CopySetKey{ 1, 1 }, &copysetInfo));
    ASSERT_EQ(2, copysetInfo.GetEpoch());
    ASSERT_EQ(1, copysetInfo.GetLeader());
    std::set<ChunkServerIdType> res{ 1, 2, 3 };
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
}

//Reported as a follower
TEST_F(HeartbeatBasicTest, test_follower_report_leader_0) {
    //Update copyset-1 in topology (epoch=2, peers={1,2,3}, leader=1)
    hbtest_->UpdateCopysetTopo(1, 1, 2, 1,
                               std::set<ChunkServerIdType>{ 1, 2, 3 });

    //Copyset-1 reported by chunkserver2 (epoch=2, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 2, 0, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Response is empty
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(
        hbtest_->topology_->GetCopySet(CopySetKey{ 1, 1 }, &copysetInfo));
    ASSERT_EQ(2, copysetInfo.GetEpoch());
    ASSERT_EQ(1, copysetInfo.GetLeader());
    std::set<ChunkServerIdType> res{ 1, 2, 3 };
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_follower_report_epoch_bigger) {
    //Update copyset-1 in topology (epoch=2, peers={1,2,3}, leader=1)
    hbtest_->UpdateCopysetTopo(1, 1, 2, 1,
                               std::set<ChunkServerIdType>{ 1, 2, 3 });

    //Copyset-1 reported by chunkserver2 (epoch=3, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 3, 1, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(
        hbtest_->topology_->GetCopySet(CopySetKey{ 1, 1 }, &copysetInfo));
    ASSERT_EQ(2, copysetInfo.GetEpoch());
    ASSERT_EQ(1, copysetInfo.GetLeader());
    std::set<ChunkServerIdType> res{ 1, 2, 3 };
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_follower_report_epoch_bigger_leader_0) {
    //Update copyset-1 in topology (epoch=2, peers={1,2,3}, leader=1)
    hbtest_->UpdateCopysetTopo(1, 1, 2, 1,
                               std::set<ChunkServerIdType>{ 1, 2, 3 });

    //Copyset-1 reported by chunkserver2 (epoch=3, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 3, 0, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(
        hbtest_->topology_->GetCopySet(CopySetKey{ 1, 1 }, &copysetInfo));
    ASSERT_EQ(2, copysetInfo.GetEpoch());
    ASSERT_EQ(1, copysetInfo.GetLeader());
    std::set<ChunkServerIdType> res{ 1, 2, 3 };
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_follower_report_epoch_bigger_peers_not_same) {
    //Update copyset-1 in topology (epoch=2, peers={1,2,3}, leader=1)
    hbtest_->UpdateCopysetTopo(1, 1, 2, 1,
                               std::set<ChunkServerIdType>{ 1, 2, 3 });
    ChunkServer cs(4, "testtoken", "nvme", 3, "10.198.100.3", 9001, "/");
    hbtest_->PrepareAddChunkServer(cs);

    //Copyset-1 reported by chunkserver2 (epoch=3, peers={1,2,3,4}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 3, 1, std::set<ChunkServerIdType>{ 1, 2, 3, 4 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(
        hbtest_->topology_->GetCopySet(CopySetKey{ 1, 1 }, &copysetInfo));
    ASSERT_EQ(2, copysetInfo.GetEpoch());
    ASSERT_EQ(1, copysetInfo.GetLeader());
    std::set<ChunkServerIdType> res{ 1, 2, 3 };
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_follower_report_epoch_smaller) {
    //Update copyset-1 in topology (epoch=2, peers={1,2,3}, leader=1)
    hbtest_->UpdateCopysetTopo(1, 1, 2, 1,
                               std::set<ChunkServerIdType>{ 1, 2, 3 });

    //Copyset-1 reported by chunkserver2 (epoch=1, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 1, 1, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(
        hbtest_->topology_->GetCopySet(CopySetKey{ 1, 1 }, &copysetInfo));
    ASSERT_EQ(2, copysetInfo.GetEpoch());
    ASSERT_EQ(1, copysetInfo.GetLeader());
    std::set<ChunkServerIdType> res{ 1, 2, 3 };
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_follower_report_epoch_smaller_leader_0) {
    //Update copyset-1 in topology (epoch=2, peers={1,2,3}, leader=1)
    hbtest_->UpdateCopysetTopo(1, 1, 2, 1,
                               std::set<ChunkServerIdType>{ 1, 2, 3 });

    //Copyset-1 reported by chunkserver2 (epoch=1, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 1, 0, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(
        hbtest_->topology_->GetCopySet(CopySetKey{ 1, 1 }, &copysetInfo));
    ASSERT_EQ(2, copysetInfo.GetEpoch());
    ASSERT_EQ(1, copysetInfo.GetLeader());
    std::set<ChunkServerIdType> res{ 1, 2, 3 };
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_follower_report_epoch_smaller_peers_not_same1) {
    //Update copyset-1 in topology (epoch=2, peers={1,2,3}, leader=1)
    hbtest_->UpdateCopysetTopo(1, 1, 2, 1,
                               std::set<ChunkServerIdType>{ 1, 2, 3 });
    ChunkServer cs(4, "testtoken", "nvme", 3, "10.198.100.3", 9001, "/");
    hbtest_->PrepareAddChunkServer(cs);
    //Copyset-1 reported by chunkserver2 (epoch=1, peers={1,2,3,4}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 1, 1, std::set<ChunkServerIdType>{ 1, 2, 3, 4 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(
        hbtest_->topology_->GetCopySet(CopySetKey{ 1, 1 }, &copysetInfo));
    ASSERT_EQ(2, copysetInfo.GetEpoch());
    ASSERT_EQ(1, copysetInfo.GetLeader());
    std::set<ChunkServerIdType> res{ 1, 2, 3 };
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_follower_report_epoch_smaller_peers_not_same2) {
    //Update copyset-1 in topology (epoch=2, peers={1,2,3}, leader=1)
    hbtest_->UpdateCopysetTopo(1, 1, 2, 1,
                               std::set<ChunkServerIdType>{ 1, 2, 3 });
    ChunkServer cs(4, "testtoken", "nvme", 3, "10.198.100.3", 9001, "/");
    hbtest_->PrepareAddChunkServer(cs);
    //Copyset-1 reported by chunkserver2 (epoch=1, peers={1,2,3,4}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 1, 0, std::set<ChunkServerIdType>{ 1, 2, 3, 4 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(
        hbtest_->topology_->GetCopySet(CopySetKey{ 1, 1 }, &copysetInfo));
    ASSERT_EQ(2, copysetInfo.GetEpoch());
    ASSERT_EQ(1, copysetInfo.GetLeader());
    std::set<ChunkServerIdType> res{ 1, 2, 3 };
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_follower_report_epoch_0) {
    //Update copyset-1 in topology (epoch=2, peers={1,2,3}, leader=1)
    hbtest_->UpdateCopysetTopo(1, 1, 2, 1,
                               std::set<ChunkServerIdType>{ 1, 2, 3 });

    //Copyset-1 reported by chunkserver2 (epoch=0, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 1, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(
        hbtest_->topology_->GetCopySet(CopySetKey{ 1, 1 }, &copysetInfo));
    ASSERT_EQ(2, copysetInfo.GetEpoch());
    ASSERT_EQ(1, copysetInfo.GetLeader());
    std::set<ChunkServerIdType> res{ 1, 2, 3 };
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_follower_report_epoch_0_leader_0) {
    //Update copyset-1 in topology (epoch=2, peers={1,2,3}, leader=1)
    hbtest_->UpdateCopysetTopo(1, 1, 2, 1,
                               std::set<ChunkServerIdType>{ 1, 2, 3 });
    //Copyset-1 reported by chunkserver2 (epoch=0, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 0, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(
        hbtest_->topology_->GetCopySet(CopySetKey{ 1, 1 }, &copysetInfo));
    ASSERT_EQ(2, copysetInfo.GetEpoch());
    ASSERT_EQ(1, copysetInfo.GetLeader());
    std::set<ChunkServerIdType> res{ 1, 2, 3 };
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_follower_report_epoch_0_peers_not_same1) {
    //Update copyset-1 in topology (epoch=2, peers={1,2,3}, leader=1)
    hbtest_->UpdateCopysetTopo(1, 1, 2, 1,
                               std::set<ChunkServerIdType>{ 1, 2, 3 });
    ChunkServer cs(4, "testtoken", "nvme", 3, "10.198.100.3", 9001, "/");
    hbtest_->PrepareAddChunkServer(cs);
    //Copyset-1 reported by chunkserver2 (epoch=0, peers={1,2,3,4}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 1, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(
        hbtest_->topology_->GetCopySet(CopySetKey{ 1, 1 }, &copysetInfo));
    ASSERT_EQ(2, copysetInfo.GetEpoch());
    ASSERT_EQ(1, copysetInfo.GetLeader());
    std::set<ChunkServerIdType> res{ 1, 2, 3 };
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_follower_report_epoch_0_peers_not_same2) {
    //Update copyset-1 in topology (epoch=2, peers={1,2,3}, leader=1)
    hbtest_->UpdateCopysetTopo(1, 1, 2, 1,
                               std::set<ChunkServerIdType>{ 1, 2, 3 });
    ChunkServer cs(4, "testtoken", "nvme", 3, "10.198.100.3", 9001, "/");
    hbtest_->PrepareAddChunkServer(cs);
    //Copyset-1 reported by chunkserver2 (epoch=0, peers={1,2,3,4}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 0, std::set<ChunkServerIdType>{ 1, 2, 3, 4 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(
        hbtest_->topology_->GetCopySet(CopySetKey{ 1, 1 }, &copysetInfo));
    ASSERT_EQ(2, copysetInfo.GetEpoch());
    ASSERT_EQ(1, copysetInfo.GetLeader());
    std::set<ChunkServerIdType> res{ 1, 2, 3 };
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
}

//The reported member is not a replication group member
TEST_F(HeartbeatBasicTest, test_other_report_consistent_with_mds) {
    //Update copyset-1 in topology (epoch=2, peers={1,2,3}, leader=1)
    hbtest_->UpdateCopysetTopo(1, 1, 2, 1,
                               std::set<ChunkServerIdType>{ 1, 2, 3 });
    ChunkServer cs(4, "testtoken", "nvme", 3, "10.198.100.3", 9001, "/");
    hbtest_->PrepareAddChunkServer(cs);
    //Copyset-1 reported by chunkserver4 (epoch=2, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 2, 1, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    //Check response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    CopySetConf conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(3, conf.peers_size());
    ASSERT_EQ(2, conf.epoch());
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(
        hbtest_->topology_->GetCopySet(CopySetKey{ 1, 1 }, &copysetInfo));
    ASSERT_EQ(2, copysetInfo.GetEpoch());
    ASSERT_EQ(1, copysetInfo.GetLeader());
    std::set<ChunkServerIdType> res{ 1, 2, 3 };
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_other_report_epoch_smaller) {
    //Update copyset-1 in topology (epoch=2, peers={1,2,3}, leader=1)
    hbtest_->UpdateCopysetTopo(1, 1, 2, 1,
                               std::set<ChunkServerIdType>{ 1, 2, 3 });
    ChunkServer cs(4, "testtoken", "nvme", 3, "10.198.100.3", 9001, "/");
    hbtest_->PrepareAddChunkServer(cs);
    //Copyset-1 reported by chunkserver4 (epoch=1, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 1, 1, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    //Check response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    CopySetConf conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(3, conf.peers_size());
    ASSERT_EQ(2, conf.epoch());
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(
        hbtest_->topology_->GetCopySet(CopySetKey{ 1, 1 }, &copysetInfo));
    ASSERT_EQ(2, copysetInfo.GetEpoch());
    ASSERT_EQ(1, copysetInfo.GetLeader());
    std::set<ChunkServerIdType> res{ 1, 2, 3 };
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_other_report_epoch_smaller_peers_not_same) {
    //Update copyset-1 in topology (epoch=2, peers={1,2,3}, leader=1)
    hbtest_->UpdateCopysetTopo(1, 1, 2, 1,
                               std::set<ChunkServerIdType>{ 1, 2, 3 });
    ChunkServer cs(4, "testtoken", "nvme", 3, "10.198.100.3", 9001, "/");
    hbtest_->PrepareAddChunkServer(cs);
    //Copyset-1 reported by chunkserver4 (epoch=1, peers={1,2}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 1, 1, std::set<ChunkServerIdType>{ 1, 2 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    //Check response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    CopySetConf conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(3, conf.peers_size());
    ASSERT_EQ(2, conf.epoch());
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(
        hbtest_->topology_->GetCopySet(CopySetKey{ 1, 1 }, &copysetInfo));
    ASSERT_EQ(2, copysetInfo.GetEpoch());
    ASSERT_EQ(1, copysetInfo.GetLeader());
    std::set<ChunkServerIdType> res{ 1, 2, 3 };
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_other_report_epoch_0_leader_0) {
    //Update copyset-1 in topology (epoch=2, peers={1,2,3}, leader=1)
    hbtest_->UpdateCopysetTopo(1, 1, 2, 1,
                               std::set<ChunkServerIdType>{ 1, 2, 3 });
    ChunkServer cs(4, "testtoken", "nvme", 3, "10.198.100.3", 9001, "/");
    hbtest_->PrepareAddChunkServer(cs);

    //Copyset-1 reported by chunkserver4 (epoch=0, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 0, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    //Check response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    CopySetConf conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(3, conf.peers_size());
    ASSERT_EQ(2, conf.epoch());
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(
        hbtest_->topology_->GetCopySet(CopySetKey{ 1, 1 }, &copysetInfo));
    ASSERT_EQ(2, copysetInfo.GetEpoch());
    ASSERT_EQ(1, copysetInfo.GetLeader());
    std::set<ChunkServerIdType> res{ 1, 2, 3 };
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_other_report_epoch_0_leader_0_peers_not_same) {
    //Update copyset-1 in topology (epoch=2, peers={1,2,3}, leader=1)
    hbtest_->UpdateCopysetTopo(1, 1, 2, 1,
                               std::set<ChunkServerIdType>{ 1, 2, 3 });
    ChunkServer cs4(4, "testtoken", "nvme", 3, "10.198.100.3", 9001, "/");
    hbtest_->PrepareAddChunkServer(cs4);
    ChunkServer cs5(5, "testtoken", "nvme", 3, "10.198.100.3", 9090, "/");
    hbtest_->PrepareAddChunkServer(cs5);

    //Copyset-1 reported by chunkserver4 (epoch=0, peers={1,2,3,5}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 0, std::set<ChunkServerIdType>{ 1, 2, 3, 5 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    //Check response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    CopySetConf conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(3, conf.peers_size());
    ASSERT_EQ(2, conf.epoch());
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(
        hbtest_->topology_->GetCopySet(CopySetKey{ 1, 1 }, &copysetInfo));
    ASSERT_EQ(2, copysetInfo.GetEpoch());
    ASSERT_EQ(1, copysetInfo.GetLeader());
    std::set<ChunkServerIdType> res{ 1, 2, 3 };
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_mdsNoCandidate_OpOnGoing_condition1) {
    PrepareMdsNoCnandidateOpOnGoing();

    //Copyset-1 reported by chunkserver1 (epoch=5, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(1, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 1, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    //Check the operator in sheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    //Check response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    CopySetConf conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(3, conf.peers_size());
    ASSERT_EQ(5, conf.epoch());
    ASSERT_EQ(ConfigChangeType::ADD_PEER, conf.type());
    ASSERT_EQ("10.198.100.3:9001:0", conf.configchangeitem().address());
}

TEST_F(HeartbeatBasicTest, test_mdsNoCandidate_OpOnGoing_condition2) {
    PrepareMdsNoCnandidateOpOnGoing();

    //Copyset-1 reported by chunkserver1 (epoch=5, peers={1,2,3}, leader=1)
    // conf.gChangeInfo={peer: 10, type: AddPeer} )
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(1, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 1, std::set<ChunkServerIdType>{ 1, 2, 3 }, 10);
    hbtest_->AddCopySetToRequest(&req, csInfo, ConfigChangeType::ADD_PEER);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check topology
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(
        hbtest_->topology_->GetCopySet(CopySetKey{ 1, 1 }, &copysetInfo));
    ASSERT_EQ(5, copysetInfo.GetEpoch());
    ASSERT_EQ(1, copysetInfo.GetLeader());
    std::set<ChunkServerIdType> res{ 1, 2, 3 };
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
    ASSERT_EQ(10, copysetInfo.GetCandidate());
}

TEST_F(HeartbeatBasicTest, test_mdsNoCandidate_OpOnGoing_condition3) {
    PrepareMdsNoCnandidateOpOnGoing();

    //Copyset-1 reported by chunkserver1 (epoch=6, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(1, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 1, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    //Check the operator in sheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check topology
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(
        hbtest_->topology_->GetCopySet(CopySetKey{ 1, 1 }, &copysetInfo));
    ASSERT_EQ(6, copysetInfo.GetEpoch());
    ASSERT_EQ(1, copysetInfo.GetLeader());
    std::set<ChunkServerIdType> res{ 1, 2, 3 };
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_mdsNoCandidate_OpOnGoing_condition4) {
    PrepareMdsNoCnandidateOpOnGoing();

    //Copyset-1 reported by chunkserver2 (epoch=6, peers={1,2,3}, leader=2)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 2, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    //Check the operator in sheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check topology
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(
        hbtest_->topology_->GetCopySet(CopySetKey{ 1, 1 }, &copysetInfo));
    ASSERT_EQ(6, copysetInfo.GetEpoch());
    ASSERT_EQ(2, copysetInfo.GetLeader());
    std::set<ChunkServerIdType> res{ 1, 2, 3 };
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_mdsNoCandidate_OpOnGoing_condition5) {
    PrepareMdsNoCnandidateOpOnGoing();

    //Copyset-1 reported by chunkserver1 (epoch=6, peers={1,2,3,10}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(1, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 1, std::set<ChunkServerIdType>{ 1, 2, 3, 10 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    //Check the operator in sheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check topology
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(
        hbtest_->topology_->GetCopySet(CopySetKey{ 1, 1 }, &copysetInfo));
    ASSERT_EQ(6, copysetInfo.GetEpoch());
    ASSERT_EQ(1, copysetInfo.GetLeader());
    std::set<ChunkServerIdType> res{ 1, 2, 3, 10 };
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_mdsNoCandidate_OpOnGoing_condition6) {
    PrepareMdsNoCnandidateOpOnGoing();

    //Copyset-1 reported by chunkserver2 (epoch=5, peers={1,2,3}, leader=1)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 1, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
}

TEST_F(HeartbeatBasicTest, test_mdsNoCandidate_OpOnGoing_condition7) {
    PrepareMdsNoCnandidateOpOnGoing();

    //Copyset-1 reported by chunkserver2 (epoch=5, peers={1,2,3}, leader=0)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 0, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check topology
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(
        hbtest_->topology_->GetCopySet(CopySetKey{ 1, 1 }, &copysetInfo));
    ASSERT_EQ(5, copysetInfo.GetEpoch());
    ASSERT_EQ(1, copysetInfo.GetLeader());
    std::set<ChunkServerIdType> res{ 1, 2, 3 };
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_mdsNoCandidate_OpOnGoing_condition8) {
    PrepareMdsNoCnandidateOpOnGoing();

    //Copyset-1 reported by chunkserver2 (epoch=6, peers={1,2,3}, leader=1)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 1, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check topology
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(
        hbtest_->topology_->GetCopySet(CopySetKey{ 1, 1 }, &copysetInfo));
    ASSERT_EQ(5, copysetInfo.GetEpoch());
    ASSERT_EQ(1, copysetInfo.GetLeader());
    std::set<ChunkServerIdType> res{ 1, 2, 3 };
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_mdsNoCandidate_OpOnGoing_condition9) {
    PrepareMdsNoCnandidateOpOnGoing();

    //Copyset-1 reported by chunkserver2 (epoch=6, peers={1,2,3}, leader=0)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 0, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check topology
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(
        hbtest_->topology_->GetCopySet(CopySetKey{ 1, 1 }, &copysetInfo));
    ASSERT_EQ(5, copysetInfo.GetEpoch());
    ASSERT_EQ(1, copysetInfo.GetLeader());
    std::set<ChunkServerIdType> res{ 1, 2, 3 };
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_mdsNoCandidate_OpOnGoing_condition10) {
    PrepareMdsNoCnandidateOpOnGoing();

    //Copyset-1 reported by chunkserver2 (epoch=6, peers={1,2,3,10}, leader=1)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 1, std::set<ChunkServerIdType>{ 1, 2, 3, 10 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check topology
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(
        hbtest_->topology_->GetCopySet(CopySetKey{ 1, 1 }, &copysetInfo));
    ASSERT_EQ(5, copysetInfo.GetEpoch());
    ASSERT_EQ(1, copysetInfo.GetLeader());
    std::set<ChunkServerIdType> res{ 1, 2, 3 };
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_mdsNoCandidate_OpOnGoing_condition11) {
    PrepareMdsNoCnandidateOpOnGoing();

    //Copyset-1 reported by chunkserver2 (epoch=6, peers={1,2,3,10}, leader=0)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 0, std::set<ChunkServerIdType>{ 1, 2, 3, 10 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check topology
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(
        hbtest_->topology_->GetCopySet(CopySetKey{ 1, 1 }, &copysetInfo));
    ASSERT_EQ(5, copysetInfo.GetEpoch());
    ASSERT_EQ(1, copysetInfo.GetLeader());
    std::set<ChunkServerIdType> res{ 1, 2, 3 };
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_mdsNoCandidate_OpOnGoing_condition12) {
    PrepareMdsNoCnandidateOpOnGoing();

    //Copyset-1 reported by chunkserver2 (epoch=4, peers={1,2,3}, leader=1)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 1, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
}

TEST_F(HeartbeatBasicTest, test_mdsNoCandidate_OpOnGoing_condition13) {
    PrepareMdsNoCnandidateOpOnGoing();

    //Copyset-1 reported by chunkserver2 (epoch=4, peers={1,2,3}, leader=0)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 0, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
}

TEST_F(HeartbeatBasicTest, test_mdsNoCandidate_OpOnGoing_condition14) {
    PrepareMdsNoCnandidateOpOnGoing();
    ChunkServer cs(4, "testtoekn", "nvme", 3, "10.198.100.3", 9002, "/");
    hbtest_->PrepareAddChunkServer(cs);

    //Copyset-1 reported by chunkserver2 (epoch=4, peers={1,2,3,4}, leader=1)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 1, std::set<ChunkServerIdType>{ 1, 2, 3, 4 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
}

TEST_F(HeartbeatBasicTest, test_mdsNoCandidate_OpOnGoing_condition15) {
    PrepareMdsNoCnandidateOpOnGoing();
    ChunkServer cs(4, "testtoekn", "nvme", 3, "10.198.100.3", 9002, "/");
    hbtest_->PrepareAddChunkServer(cs);

    //Copyset-1 reported by chunkserver2 (epoch=4, peers={1,2,3,4}, leader=0)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 0, std::set<ChunkServerIdType>{ 1, 2, 3, 4 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
}

TEST_F(HeartbeatBasicTest, test_mdsNoCandidate_OpOnGoing_condition16) {
    PrepareMdsNoCnandidateOpOnGoing();

    //Copyset-1 reported by chunkserver2 (epoch=0, peers={1,2,3}, leader=1)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 1, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
}

TEST_F(HeartbeatBasicTest, test_mdsNoCandidate_OpOnGoing_condition17) {
    PrepareMdsNoCnandidateOpOnGoing();

    //Copyset-1 reported by chunkserver2 (epoch=0, peers={1,2,3}, leader=0)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 0, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
}

TEST_F(HeartbeatBasicTest, test_mdsNoCandidate_OpOnGoing_condition18) {
    PrepareMdsNoCnandidateOpOnGoing();
    ChunkServer cs(4, "testtoekn", "nvme", 3, "10.198.100.3", 9002, "/");
    hbtest_->PrepareAddChunkServer(cs);

    //Copyset-1 reported by chunkserver2 (epoch=0, peers={1,2,3,4}, leader=1)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 1, std::set<ChunkServerIdType>{ 1, 2, 3, 4 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
}

TEST_F(HeartbeatBasicTest, test_mdsNoCandidate_OpOnGoing_condition19) {
    PrepareMdsNoCnandidateOpOnGoing();
    ChunkServer cs(4, "testtoekn", "nvme", 3, "10.198.100.3", 9002, "/");
    hbtest_->PrepareAddChunkServer(cs);

    //Copyset-1 reported by chunkserver2 (epoch=0, peers={1,2,3,4}, leader=0)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 0, std::set<ChunkServerIdType>{ 1, 2, 3, 4 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
}

TEST_F(HeartbeatBasicTest, test_mdsNoCandidate_OpOnGoing_condition20) {
    PrepareMdsNoCnandidateOpOnGoing();

    //Copyset-1 reported by chunkserver10 (epoch=5, peers={1,2,3}, leader=1)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(10, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 1, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
}

TEST_F(HeartbeatBasicTest, test_mdsNoCandidate_OpOnGoing_condition21) {
    PrepareMdsNoCnandidateOpOnGoing();

    //Copyset-1 reported by chunkserver10 (epoch=5, peers={1,2,3}, leader=0)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(10, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 0, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
}

TEST_F(HeartbeatBasicTest, test_mdsNoCandidate_OpOnGoing_condition22) {
    PrepareMdsNoCnandidateOpOnGoing();

    //Copyset-1 reported by chunkserver10 (epoch=6, peers={1,2,3,10}, leader=1)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(10, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 1, std::set<ChunkServerIdType>{ 1, 2, 3, 10 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check topology
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(
        hbtest_->topology_->GetCopySet(CopySetKey{ 1, 1 }, &copysetInfo));
    ASSERT_EQ(5, copysetInfo.GetEpoch());
    ASSERT_EQ(1, copysetInfo.GetLeader());
    std::set<ChunkServerIdType> res{ 1, 2, 3 };
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_mdsNoCandidate_OpOnGoing_condition23) {
    PrepareMdsNoCnandidateOpOnGoing();

    //Copyset-1 reported by chunkserver10 (epoch=6, peers={1,2,3,10}, leader=0)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(10, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 0, std::set<ChunkServerIdType>{ 1, 2, 3, 10 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check topology
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(
        hbtest_->topology_->GetCopySet(CopySetKey{ 1, 1 }, &copysetInfo));
    ASSERT_EQ(5, copysetInfo.GetEpoch());
    ASSERT_EQ(1, copysetInfo.GetLeader());
    std::set<ChunkServerIdType> res{ 1, 2, 3 };
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_mdsNoCandidate_OpOnGoing_condition24) {
    PrepareMdsNoCnandidateOpOnGoing();

    //Copyset-1 reported by chunkserver10 (epoch=4, peers={1,2,3}, leader=1)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(10, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 1, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
}

TEST_F(HeartbeatBasicTest, test_mdsNoCandidate_OpOnGoing_condition25) {
    PrepareMdsNoCnandidateOpOnGoing();

    //Copyset-1 reported by chunkserver10 (epoch=4, peers={1,2,3}, leader=0)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(10, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 0, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
}

TEST_F(HeartbeatBasicTest, test_mdsNoCandidate_OpOnGoing_condition26) {
    PrepareMdsNoCnandidateOpOnGoing();
    ChunkServer cs(4, "testtoekn", "nvme", 3, "10.198.100.3", 9002, "/");
    hbtest_->PrepareAddChunkServer(cs);

    //Copyset-1 reported by chunkserver10 (epoch=4, peers={1,2,3,4}, leader=1)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(10, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 1, std::set<ChunkServerIdType>{ 1, 2, 3, 4 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
}

TEST_F(HeartbeatBasicTest, test_mdsNoCandidate_OpOnGoing_condition27) {
    PrepareMdsNoCnandidateOpOnGoing();
    ChunkServer cs(4, "testtoekn", "nvme", 3, "10.198.100.3", 9002, "/");
    hbtest_->PrepareAddChunkServer(cs);

    //Copyset-1 reported by chunkserver10 (epoch=4, peers={1,2,3,4}, leader=0)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(10, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 0, std::set<ChunkServerIdType>{ 1, 2, 3, 4 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
}

TEST_F(HeartbeatBasicTest, test_mdsNoCandidate_OpOnGoing_condition28) {
    PrepareMdsNoCnandidateOpOnGoing();

    //Copyset-1 reported by chunkserver10 (epoch=0, peers={1,2,3}, leader=1)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(10, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 1, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
}

TEST_F(HeartbeatBasicTest, test_mdsNoCandidate_OpOnGoing_condition29) {
    PrepareMdsNoCnandidateOpOnGoing();

    //Copyset-1 reported by chunkserver10 (epoch=0, peers={1,2,3}, leader=0)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(10, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 0, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
}

TEST_F(HeartbeatBasicTest, test_mdsNoCandidate_OpOnGoing_condition30) {
    PrepareMdsNoCnandidateOpOnGoing();
    ChunkServer cs(4, "testtoekn", "nvme", 3, "10.198.100.3", 9002, "/");
    hbtest_->PrepareAddChunkServer(cs);

    //Copyset-1 reported by chunkserver10 (epoch=0, peers={1,2,3,4}, leader=1)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(10, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 1, std::set<ChunkServerIdType>{ 1, 2, 3, 4 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
}

TEST_F(HeartbeatBasicTest, test_mdsNoCandidate_OpOnGoing_condition31) {
    PrepareMdsNoCnandidateOpOnGoing();
    ChunkServer cs(4, "testtoekn", "nvme", 3, "10.198.100.3", 9002, "/");
    hbtest_->PrepareAddChunkServer(cs);

    //Copyset-1 reported by chunkserver10 (epoch=0, peers={1,2,3,4}, leader=0)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(10, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 0, std::set<ChunkServerIdType>{ 1, 2, 3, 4 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
}

TEST_F(HeartbeatBasicTest, test_mdsNoCandidate_OpOnGoing_condition32) {
    PrepareMdsNoCnandidateOpOnGoing();
    ChunkServer cs(4, "testtoekn", "nvme", 3, "10.198.100.3", 9002, "/");
    hbtest_->PrepareAddChunkServer(cs);

    //Copyset-1 reported by chunkserver4 (epoch=5, peers={1,2,3}, leader=0)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 0, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    CopySetConf conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(3, conf.peers_size());
    ASSERT_EQ(5, conf.epoch());
}

TEST_F(HeartbeatBasicTest, test_mdsNoCandidate_OpOnGoing_condition33) {
    PrepareMdsNoCnandidateOpOnGoing();
    ChunkServer cs(4, "testtoekn", "nvme", 3, "10.198.100.3", 9002, "/");
    hbtest_->PrepareAddChunkServer(cs);

    //Copyset-1 reported by chunkserver4 (epoch=4, peers={1,2,3}, leader=0)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 0, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    CopySetConf conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(3, conf.peers_size());
    ASSERT_EQ(5, conf.epoch());
}

TEST_F(HeartbeatBasicTest, test_mdsNoCandidate_OpOnGoing_condition34) {
    PrepareMdsNoCnandidateOpOnGoing();
    ChunkServer cs4(4, "testtoekn", "nvme", 3, "10.198.100.3", 9002, "/");
    hbtest_->PrepareAddChunkServer(cs4);
    ChunkServer cs5(5, "testtoekn", "nvme", 3, "10.198.100.3", 9003, "/");
    hbtest_->PrepareAddChunkServer(cs5);

    //Copyset-1 reported by chunkserver4 (epoch=4, peers={1,2,3,5}, leader=0)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 0, std::set<ChunkServerIdType>{ 1, 2, 3, 5 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    CopySetConf conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(3, conf.peers_size());
    ASSERT_EQ(5, conf.epoch());
}

TEST_F(HeartbeatBasicTest, test_mdsNoCandidate_OpOnGoing_condition35) {
    PrepareMdsNoCnandidateOpOnGoing();
    ChunkServer cs(4, "testtoekn", "nvme", 3, "10.198.100.3", 9002, "/");
    hbtest_->PrepareAddChunkServer(cs);

    //Copyset-1 reported by chunkserver4 (epoch=0, peers={1,2,3}, leader=0)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 0, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    CopySetConf conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(3, conf.peers_size());
    ASSERT_EQ(5, conf.epoch());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidate_OpOnGoing_condition1) {
    PrepareMdsWithCandidateOpOnGoing();

    //Chunkserver1 reports copyset-1 (epoch=5, peers={1,2,3}, leader=1)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(1, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 1, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    ASSERT_EQ(ConfigChangeType::ADD_PEER, rep.needupdatecopysets(0).type());
    ASSERT_EQ("10.198.100.3:9001:0",
              rep.needupdatecopysets(0).configchangeitem().address());
}

TEST_F(HeartbeatBasicTest, test_test_mdsWithCandidate_OpOnGoing_condition2) {
    PrepareMdsWithCandidateOpOnGoing();

    //Chunkserver1 reporting

    // copyset-1(epoch=5, peers={1,2,3}, leader=1,
    //           conf.gChangeInfo={peer: 10, type: AddPeer} )
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(1, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 1, std::set<ChunkServerIdType>{ 1, 2, 3 }, 10);
    hbtest_->AddCopySetToRequest(&req, csInfo, ConfigChangeType::ADD_PEER);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidate_OpOnGoing_condition3) {
    PrepareMdsWithCandidateOpOnGoing();

    //Chunkserver2 reports copyset-1 (epoch=6, peers={1,2,3}, leader=2)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 2, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check the operator in sheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check the status of copyset in topology
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(
        hbtest_->topology_->GetCopySet(CopySetKey{ 1, 1 }, &copysetInfo));
    ASSERT_EQ(2, copysetInfo.GetLeader());
    ASSERT_EQ(6, copysetInfo.GetEpoch());
    ASSERT_EQ(UNINTIALIZE_ID, copysetInfo.GetCandidate());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidate_OpOnGoing_condition4) {
    PrepareMdsWithCandidateOpOnGoing();

    //Chunkserver1 reports copyset-1 (epoch=6, peers={1,2,3,10}, leader=2)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(1, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 1, std::set<ChunkServerIdType>{ 1, 2, 3, 10 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check the operator in sheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check the status of copyset in topology
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(
        hbtest_->topology_->GetCopySet(CopySetKey{ 1, 1 }, &copysetInfo));
    ASSERT_EQ(1, copysetInfo.GetLeader());
    ASSERT_EQ(6, copysetInfo.GetEpoch());
    ASSERT_EQ(UNINTIALIZE_ID, copysetInfo.GetCandidate());
    std::set<ChunkServerIdType> peers{ 1, 2, 3, 10 };
    ASSERT_EQ(peers, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidate_OpOnGoing_condition5) {
    PrepareMdsWithCandidateOpOnGoing();

    //Chunkserver2 reports copyset-1 (epoch=7, peers={1,2,3,10}, leader=2)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 7, 2, std::set<ChunkServerIdType>{ 1, 2, 3, 10 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check the operator in sheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check the status of copyset in topology
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(
        hbtest_->topology_->GetCopySet(CopySetKey{ 1, 1 }, &copysetInfo));
    ASSERT_EQ(2, copysetInfo.GetLeader());
    ASSERT_EQ(7, copysetInfo.GetEpoch());
    ASSERT_EQ(UNINTIALIZE_ID, copysetInfo.GetCandidate());
    std::set<ChunkServerIdType> peers{ 1, 2, 3, 10 };
    ASSERT_EQ(peers, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidate_OpOnGoing_condition6) {
    PrepareMdsWithCandidateOpOnGoing();

    //Chunkserver2 reports copyset-1 (epoch=5, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 1, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check the operator in sheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidate_OpOnGoing_condition7) {
    PrepareMdsWithCandidateOpOnGoing();

    //Chunkserver2 reports copyset-1 (epoch=5, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 0, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check the operator in sheduler
     auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidate_OpOnGoing_condition8) {
    PrepareMdsWithCandidateOpOnGoing();

    //Chunkserver2 reports copyset-1 (epoch=6, peers={1,2,3,10}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 1, std::set<ChunkServerIdType>{ 1, 2, 3, 10 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check the operator in sheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(
        hbtest_->topology_->GetCopySet(CopySetKey{ 1, 1 }, &copysetInfo));
    ASSERT_EQ(1, copysetInfo.GetLeader());
    ASSERT_EQ(5, copysetInfo.GetEpoch());
    ASSERT_EQ(10, copysetInfo.GetCandidate());
    std::set<ChunkServerIdType> peers{ 1, 2, 3 };
    ASSERT_EQ(peers, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidate_OpOnGoing_condition9) {
    PrepareMdsWithCandidateOpOnGoing();

    //Chunkserver2 reports copyset-1 (epoch=6, peers={1,2,3,10}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 0, std::set<ChunkServerIdType>{ 1, 2, 3, 10 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check the operator in sheduler
     auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(
        hbtest_->topology_->GetCopySet(CopySetKey{ 1, 1 }, &copysetInfo));
    ASSERT_EQ(1, copysetInfo.GetLeader());
    ASSERT_EQ(5, copysetInfo.GetEpoch());
    ASSERT_EQ(10, copysetInfo.GetCandidate());
    std::set<ChunkServerIdType> peers{ 1, 2, 3 };
    ASSERT_EQ(peers, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidate_OpOnGoing_condition10) {
    PrepareMdsWithCandidateOpOnGoing();

    //Chunkserver2 reports copyset-1 (epoch=4, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 1, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check the operator in sheduler
     auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(
        hbtest_->topology_->GetCopySet(CopySetKey{ 1, 1 }, &copysetInfo));
    ASSERT_EQ(1, copysetInfo.GetLeader());
    ASSERT_EQ(5, copysetInfo.GetEpoch());
    ASSERT_EQ(10, copysetInfo.GetCandidate());
    std::set<ChunkServerIdType> peers{ 1, 2, 3 };
    ASSERT_EQ(peers, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidate_OpOnGoing_condition11) {
    PrepareMdsWithCandidateOpOnGoing();

    //Chunkserver2 reports copyset-1 (epoch=4, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 0, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check the operator in sheduler
     auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(
        hbtest_->topology_->GetCopySet(CopySetKey{ 1, 1 }, &copysetInfo));
    ASSERT_EQ(1, copysetInfo.GetLeader());
    ASSERT_EQ(5, copysetInfo.GetEpoch());
    ASSERT_EQ(10, copysetInfo.GetCandidate());
    std::set<ChunkServerIdType> peers{ 1, 2, 3 };
    ASSERT_EQ(peers, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidate_OpOnGoing_condition12) {
    PrepareMdsWithCandidateOpOnGoing();
    ChunkServer cs(4, "testtoekn", "nvme", 3, "10.198.100.3", 9002, "/");
    hbtest_->PrepareAddChunkServer(cs);

    //Chunkserver2 reports copyset-1 (epoch=4, peers={1,2,3,4}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 1, std::set<ChunkServerIdType>{ 1, 2, 3, 4 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check the operator in sheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(
        hbtest_->topology_->GetCopySet(CopySetKey{ 1, 1 }, &copysetInfo));
    ASSERT_EQ(1, copysetInfo.GetLeader());
    ASSERT_EQ(5, copysetInfo.GetEpoch());
    ASSERT_EQ(10, copysetInfo.GetCandidate());
    std::set<ChunkServerIdType> peers{ 1, 2, 3 };
    ASSERT_EQ(peers, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidate_OpOnGoing_condition13) {
    PrepareMdsWithCandidateOpOnGoing();
    ChunkServer cs(4, "testtoekn", "nvme", 3, "10.198.100.3", 9002, "/");
    hbtest_->PrepareAddChunkServer(cs);

    //Chunkserver2 reports copyset-1 (epoch=4, peers={1,2,3,4}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 0, std::set<ChunkServerIdType>{ 1, 2, 3, 4 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check the operator in sheduler
     auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(
        hbtest_->topology_->GetCopySet(CopySetKey{ 1, 1 }, &copysetInfo));
    ASSERT_EQ(1, copysetInfo.GetLeader());
    ASSERT_EQ(5, copysetInfo.GetEpoch());
    ASSERT_EQ(10, copysetInfo.GetCandidate());
    std::set<ChunkServerIdType> peers{ 1, 2, 3 };
    ASSERT_EQ(peers, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidate_OpOnGoing_condition14) {
    PrepareMdsWithCandidateOpOnGoing();

    //Chunkserver2 reports copyset-1 (epoch=0, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 1, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check the operator in sheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(
        hbtest_->topology_->GetCopySet(CopySetKey{ 1, 1 }, &copysetInfo));
    ASSERT_EQ(1, copysetInfo.GetLeader());
    ASSERT_EQ(5, copysetInfo.GetEpoch());
    ASSERT_EQ(10, copysetInfo.GetCandidate());
    std::set<ChunkServerIdType> peers{ 1, 2, 3 };
    ASSERT_EQ(peers, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidate_OpOnGoing_condition15) {
    PrepareMdsWithCandidateOpOnGoing();

    //Chunkserver2 reports copyset-1 (epoch=0, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 0, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check the operator in sheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(
        hbtest_->topology_->GetCopySet(CopySetKey{ 1, 1 }, &copysetInfo));
    ASSERT_EQ(1, copysetInfo.GetLeader());
    ASSERT_EQ(5, copysetInfo.GetEpoch());
    ASSERT_EQ(10, copysetInfo.GetCandidate());
    std::set<ChunkServerIdType> peers{ 1, 2, 3 };
    ASSERT_EQ(peers, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidate_OpOnGoing_condition16) {
    PrepareMdsWithCandidateOpOnGoing();
    ChunkServer cs(4, "testtoekn", "nvme", 3, "10.198.100.3", 9002, "/");
    hbtest_->PrepareAddChunkServer(cs);

    //Chunkserver2 reports copyset-1 (epoch=0, peers={1,2,3,4}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 1, std::set<ChunkServerIdType>{ 1, 2, 3, 4 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check the operator in sheduler
     auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(
        hbtest_->topology_->GetCopySet(CopySetKey{ 1, 1 }, &copysetInfo));
    ASSERT_EQ(1, copysetInfo.GetLeader());
    ASSERT_EQ(5, copysetInfo.GetEpoch());
    ASSERT_EQ(10, copysetInfo.GetCandidate());
    std::set<ChunkServerIdType> peers{ 1, 2, 3 };
    ASSERT_EQ(peers, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidate_OpOnGoing_condition17) {
    PrepareMdsWithCandidateOpOnGoing();
    ChunkServer cs(4, "testtoekn", "nvme", 3, "10.198.100.3", 9002, "/");
    hbtest_->PrepareAddChunkServer(cs);

    //Chunkserver2 reports copyset-1 (epoch=0, peers={1,2,3,4}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 0, std::set<ChunkServerIdType>{ 1, 2, 3, 4 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check the operator in sheduler
     auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(
        hbtest_->topology_->GetCopySet(CopySetKey{ 1, 1 }, &copysetInfo));
    ASSERT_EQ(1, copysetInfo.GetLeader());
    ASSERT_EQ(5, copysetInfo.GetEpoch());
    ASSERT_EQ(10, copysetInfo.GetCandidate());
    std::set<ChunkServerIdType> peers{ 1, 2, 3 };
    ASSERT_EQ(peers, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidate_OpOnGoing_condition18) {
    PrepareMdsWithCandidateOpOnGoing();

    //Chunkserver10 reports copyset-1 (epoch=5, peers={1,2,3}, leader=0)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(10, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 0, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check the operator in sheduler
     auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(
        hbtest_->topology_->GetCopySet(CopySetKey{ 1, 1 }, &copysetInfo));
    ASSERT_EQ(1, copysetInfo.GetLeader());
    ASSERT_EQ(5, copysetInfo.GetEpoch());
    ASSERT_EQ(10, copysetInfo.GetCandidate());
    std::set<ChunkServerIdType> peers{ 1, 2, 3 };
    ASSERT_EQ(peers, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidate_OpOnGoing_condition19) {
    PrepareMdsWithCandidateOpOnGoing();

    //Chunkserver10 reports copyset-1 (epoch=5, peers={1,2,3}, leader=1)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(10, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 1, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check the operator in sheduler
     auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(
        hbtest_->topology_->GetCopySet(CopySetKey{ 1, 1 }, &copysetInfo));
    ASSERT_EQ(1, copysetInfo.GetLeader());
    ASSERT_EQ(5, copysetInfo.GetEpoch());
    ASSERT_EQ(10, copysetInfo.GetCandidate());
    std::set<ChunkServerIdType> peers{ 1, 2, 3 };
    ASSERT_EQ(peers, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidate_OpOnGoing_condition20) {
    PrepareMdsWithCandidateOpOnGoing();

    //Chunkserver10 reports copyset-1 (epoch=6, peers={1,2,3,10}, leader=1)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(10, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 1, std::set<ChunkServerIdType>{ 1, 2, 3, 10 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check the operator in sheduler
     auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(
        hbtest_->topology_->GetCopySet(CopySetKey{ 1, 1 }, &copysetInfo));
    ASSERT_EQ(1, copysetInfo.GetLeader());
    ASSERT_EQ(5, copysetInfo.GetEpoch());
    ASSERT_EQ(10, copysetInfo.GetCandidate());
    std::set<ChunkServerIdType> peers{ 1, 2, 3 };
    ASSERT_EQ(peers, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidate_OpOnGoing_condition21) {
    PrepareMdsWithCandidateOpOnGoing();

    //Chunkserver10 reports copyset-1 (epoch=6, peers={1,2,3,10}, leader=0)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(10, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 0, std::set<ChunkServerIdType>{ 1, 2, 3, 10 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check the operator in sheduler
     auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(
        hbtest_->topology_->GetCopySet(CopySetKey{ 1, 1 }, &copysetInfo));
    ASSERT_EQ(1, copysetInfo.GetLeader());
    ASSERT_EQ(5, copysetInfo.GetEpoch());
    ASSERT_EQ(10, copysetInfo.GetCandidate());
    std::set<ChunkServerIdType> peers{ 1, 2, 3 };
    ASSERT_EQ(peers, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidate_OpOnGoing_condition22) {
    PrepareMdsWithCandidateOpOnGoing();

    //Chunkserver10 reports copyset-1 (epoch=4, peers={1,2,3}, leader=1)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(10, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 1, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check the operator in sheduler
     auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(
        hbtest_->topology_->GetCopySet(CopySetKey{ 1, 1 }, &copysetInfo));
    ASSERT_EQ(1, copysetInfo.GetLeader());
    ASSERT_EQ(5, copysetInfo.GetEpoch());
    ASSERT_EQ(10, copysetInfo.GetCandidate());
    std::set<ChunkServerIdType> peers{ 1, 2, 3 };
    ASSERT_EQ(peers, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidate_OpOnGoing_condition23) {
    PrepareMdsWithCandidateOpOnGoing();

    //Chunkserver10 reports copyset-1 (epoch=4, peers={1,2,3}, leader=0)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(10, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 0, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check the operator in sheduler
     auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(
        hbtest_->topology_->GetCopySet(CopySetKey{ 1, 1 }, &copysetInfo));
    ASSERT_EQ(1, copysetInfo.GetLeader());
    ASSERT_EQ(5, copysetInfo.GetEpoch());
    ASSERT_EQ(10, copysetInfo.GetCandidate());
    std::set<ChunkServerIdType> peers{ 1, 2, 3 };
    ASSERT_EQ(peers, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidate_OpOnGoing_condition24) {
    PrepareMdsWithCandidateOpOnGoing();
    ChunkServer cs(4, "testtoekn", "nvme", 3, "10.198.100.3", 9002, "/");
    hbtest_->PrepareAddChunkServer(cs);

    //Chunkserver10 reports copyset-1 (epoch=4, peers={1,2,3,4}, leader=1)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(10, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 1, std::set<ChunkServerIdType>{ 1, 2, 3, 4 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check the operator in sheduler
     auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(
        hbtest_->topology_->GetCopySet(CopySetKey{ 1, 1 }, &copysetInfo));
    ASSERT_EQ(1, copysetInfo.GetLeader());
    ASSERT_EQ(5, copysetInfo.GetEpoch());
    ASSERT_EQ(10, copysetInfo.GetCandidate());
    std::set<ChunkServerIdType> peers{ 1, 2, 3 };
    ASSERT_EQ(peers, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidate_OpOnGoing_condition25) {
    PrepareMdsWithCandidateOpOnGoing();
    ChunkServer cs(4, "testtoekn", "nvme", 3, "10.198.100.3", 9002, "/");
    hbtest_->PrepareAddChunkServer(cs);

    //Chunkserver10 reports copyset-1 (epoch=4, peers={1,2,3,4}, leader=0)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(10, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 0, std::set<ChunkServerIdType>{ 1, 2, 3, 4 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check the operator in sheduler
     auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(
        hbtest_->topology_->GetCopySet(CopySetKey{ 1, 1 }, &copysetInfo));
    ASSERT_EQ(1, copysetInfo.GetLeader());
    ASSERT_EQ(5, copysetInfo.GetEpoch());
    ASSERT_EQ(10, copysetInfo.GetCandidate());
    std::set<ChunkServerIdType> peers{ 1, 2, 3 };
    ASSERT_EQ(peers, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidate_OpOnGoing_condition26) {
    PrepareMdsWithCandidateOpOnGoing();

    //Chunkserver10 reports copyset-1 (epoch=0, peers={1,2,3}, leader=1)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(10, &req);
    hbtest_->BuildBasicChunkServerRequest(10, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 1, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check the operator in sheduler
     auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(
        hbtest_->topology_->GetCopySet(CopySetKey{ 1, 1 }, &copysetInfo));
    ASSERT_EQ(1, copysetInfo.GetLeader());
    ASSERT_EQ(5, copysetInfo.GetEpoch());
    ASSERT_EQ(10, copysetInfo.GetCandidate());
    std::set<ChunkServerIdType> peers{ 1, 2, 3 };
    ASSERT_EQ(peers, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidate_OpOnGoing_condition27) {
    PrepareMdsWithCandidateOpOnGoing();

    //Chunkserver10 reports copyset-1 (epoch=0, peers={1,2,3}, leader=0)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(10, &req);
    hbtest_->BuildBasicChunkServerRequest(10, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 0, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check the operator in sheduler
     auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(
        hbtest_->topology_->GetCopySet(CopySetKey{ 1, 1 }, &copysetInfo));
    ASSERT_EQ(1, copysetInfo.GetLeader());
    ASSERT_EQ(5, copysetInfo.GetEpoch());
    ASSERT_EQ(10, copysetInfo.GetCandidate());
    std::set<ChunkServerIdType> peers{ 1, 2, 3 };
    ASSERT_EQ(peers, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidate_OpOnGoing_condition28) {
    PrepareMdsWithCandidateOpOnGoing();
    ChunkServer cs(4, "testtoekn", "nvme", 3, "10.198.100.3", 9002, "/");
    hbtest_->PrepareAddChunkServer(cs);

    //Chunkserver4 reports copyset-1 (epoch=5, peers={1,2,3}, leader=0)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 0, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check the operator in sheduler
     auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    //Check response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    ASSERT_EQ(1, rep.needupdatecopysets(0).copysetid());
    ASSERT_EQ(5, rep.needupdatecopysets(0).epoch());
    ASSERT_EQ("10.198.100.1:9000:0",
              rep.needupdatecopysets(0).peers(0).address());
    ASSERT_EQ("10.198.100.2:9000:0",
              rep.needupdatecopysets(0).peers(1).address());
    ASSERT_EQ("10.198.100.3:9000:0",
              rep.needupdatecopysets(0).peers(2).address());
    //Check copyset
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(
        hbtest_->topology_->GetCopySet(CopySetKey{ 1, 1 }, &copysetInfo));
    ASSERT_EQ(1, copysetInfo.GetLeader());
    ASSERT_EQ(5, copysetInfo.GetEpoch());
    ASSERT_EQ(10, copysetInfo.GetCandidate());
    std::set<ChunkServerIdType> peers{ 1, 2, 3 };
    ASSERT_EQ(peers, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidate_OpOnGoing_condition29) {
    PrepareMdsWithCandidateOpOnGoing();
    ChunkServer cs(4, "testtoekn", "nvme", 3, "10.198.100.3", 9002, "/");
    hbtest_->PrepareAddChunkServer(cs);

    //Chunkserver4 reports copyset-1 (epoch=4, peers={1,2,3}, leader=0)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 0, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check the operator in sheduler
     auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    //Check response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    ASSERT_EQ(1, rep.needupdatecopysets(0).copysetid());
    ASSERT_EQ(5, rep.needupdatecopysets(0).epoch());
    ASSERT_EQ("10.198.100.1:9000:0",
              rep.needupdatecopysets(0).peers(0).address());
    ASSERT_EQ("10.198.100.2:9000:0",
              rep.needupdatecopysets(0).peers(1).address());
    ASSERT_EQ("10.198.100.3:9000:0",
              rep.needupdatecopysets(0).peers(2).address());
    //Check copyset
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(
        hbtest_->topology_->GetCopySet(CopySetKey{ 1, 1 }, &copysetInfo));
    ASSERT_EQ(1, copysetInfo.GetLeader());
    ASSERT_EQ(5, copysetInfo.GetEpoch());
    ASSERT_EQ(10, copysetInfo.GetCandidate());
    std::set<ChunkServerIdType> peers{ 1, 2, 3 };
    ASSERT_EQ(peers, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidate_OpOnGoing_condition30) {
    PrepareMdsWithCandidateOpOnGoing();
    ChunkServer cs(4, "testtoekn", "nvme", 3, "10.198.100.3", 9002, "/");
    hbtest_->PrepareAddChunkServer(cs);

    //Chunkserver4 reports copyset-1 (epoch=0, peers={1,2,3}, leader=0)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 0, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check the operator in sheduler
     auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    //Check response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    ASSERT_EQ(1, rep.needupdatecopysets(0).copysetid());
    ASSERT_EQ(5, rep.needupdatecopysets(0).epoch());
    ASSERT_EQ("10.198.100.1:9000:0",
              rep.needupdatecopysets(0).peers(0).address());
    ASSERT_EQ("10.198.100.2:9000:0",
              rep.needupdatecopysets(0).peers(1).address());
    ASSERT_EQ("10.198.100.3:9000:0",
              rep.needupdatecopysets(0).peers(2).address());
    //Check copyset
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(
        hbtest_->topology_->GetCopySet(CopySetKey{ 1, 1 }, &copysetInfo));
    ASSERT_EQ(1, copysetInfo.GetLeader());
    ASSERT_EQ(5, copysetInfo.GetEpoch());
    ASSERT_EQ(10, copysetInfo.GetCandidate());
    std::set<ChunkServerIdType> peers{ 1, 2, 3 };
    ASSERT_EQ(peers, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_mdsWithRemoveOp_1) {
    PrepareMdsWithRemoveOp();

    //Chunkserver-1 reports copyset-1 (epoch=5, peers={1,2,3,4}, leader=1)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(1, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 1, std::set<ChunkServerIdType>{ 1, 2, 3, 4 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    ASSERT_EQ(1, rep.needupdatecopysets(0).copysetid());
    ASSERT_EQ(5, rep.needupdatecopysets(0).epoch());
    ASSERT_EQ("10.198.100.1:9000:0",
              rep.needupdatecopysets(0).peers(0).address());
    ASSERT_EQ("10.198.100.2:9000:0",
              rep.needupdatecopysets(0).peers(1).address());
    ASSERT_EQ("10.198.100.3:9000:0",
              rep.needupdatecopysets(0).peers(2).address());
    ASSERT_EQ("10.198.100.3:9001:0",
              rep.needupdatecopysets(0).peers(3).address());
    ASSERT_EQ(ConfigChangeType::REMOVE_PEER, rep.needupdatecopysets(0).type());
    ASSERT_EQ("10.198.100.3:9001:0",
              rep.needupdatecopysets(0).configchangeitem().address());
    //Check copyset
    ASSERT_TRUE(ValidateCopySet(csInfo));
}

TEST_F(HeartbeatBasicTest, test_mdsWithRemoveOp_2) {
    PrepareMdsWithRemoveOp();

    //Chunkserver-1 reports copyset-1 (epoch=5, peers={1,2,3,4}, leader=1,

    //                           cofigChangeInfo={peer: 4, type:REMOVE_PEER})
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(1, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 1, std::set<ChunkServerIdType>{ 1, 2, 3, 4 },
                     4);
    hbtest_->AddCopySetToRequest(&req, csInfo, ConfigChangeType::REMOVE_PEER);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    ASSERT_TRUE(ValidateCopySet(csInfo));
}

TEST_F(HeartbeatBasicTest, test_mdsWithRemoveOp_3) {
    PrepareMdsWithRemoveOp();

    //Chunkserver-1 reports copyset-1 (epoch=6, peers={1,2,3,4}, leader=1)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(1, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 1, std::set<ChunkServerIdType>{ 1, 2, 3, 4 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(6);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsWithRemoveOp_4) {
    PrepareMdsWithRemoveOp();

    //Chunkserver-1 reports copyset-1 (epoch=6, peers={1,2,3}, leader=1)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(1, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 1, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(6);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{ 1, 2, 3 });
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsWithRemoveOp_5) {
    PrepareMdsWithRemoveOp();

    //Chunkserver-2 reports copyset-1 (epoch=7, peers={1,2,3}, leader=2)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 7, 2, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(7);
    csInfo.SetLeader(2);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{ 1, 2, 3 });
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsWithRemoveOp_6) {
    PrepareMdsWithRemoveOp();

    //Chunkserver-2 reports copyset-1 (epoch=5, peers={1,2,3,4}, leader=1)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 1, std::set<ChunkServerIdType>{ 1, 2, 3, 4 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<RemovePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithRemoveOp_7) {
    PrepareMdsWithRemoveOp();

    //Chunkserver-2 reports copyset-1 (epoch=5, peers={1,2,3,4}, leader=0)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 0, std::set<ChunkServerIdType>{ 1, 2, 3, 4 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetLeader(1);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<RemovePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithRemoveOp_8) {
    PrepareMdsWithRemoveOp();

    //Chunkserver-2 reports copyset-1 (epoch=6, peers={1,2,3,4}, leader=1)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 1, std::set<ChunkServerIdType>{ 1, 2, 3, 4 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<RemovePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithRemoveOp_9) {
    PrepareMdsWithRemoveOp();

    //Chunkserver-2 reports copyset-1 (epoch=6, peers={1,2,3,4}, leader=0)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 0, std::set<ChunkServerIdType>{ 1, 2, 3, 4 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<RemovePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithRemoveOp_10) {
    PrepareMdsWithRemoveOp();

    //Chunkserver-2 reports copyset-1 (epoch=6, peers={1,2,3}, leader=1)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 1, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{ 1, 2, 3, 4 });
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<RemovePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithRemoveOp_11) {
    PrepareMdsWithRemoveOp();

    //Chunkserver-2 reports copyset-1 (epoch=6, peers={1,2,3}, leader=0)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 0, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{ 1, 2, 3, 4 });
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<RemovePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithRemoveOp_12) {
    PrepareMdsWithRemoveOp();

    //Chunkserver-2 reports copyset-1 (epoch=4, peers={1,2,3,4}, leader=1)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 1, std::set<ChunkServerIdType>{ 1, 2, 3, 4 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<RemovePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());

    //Chunkserver-2 reports copyset-1 (epoch=4, peers={1,2,3,4}, leader=0)

    hbtest_->BuildBasicChunkServerRequest(2, &req);
    BuildCopySetInfo(&csInfo, 4, 0, std::set<ChunkServerIdType>{ 1, 2, 3, 4 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    step = dynamic_cast<RemovePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithRemoveOp_13) {
    PrepareMdsWithRemoveOp();

    //Chunkserver-2 report (epoch=4, peers={1,2,3}, leader=1)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 1, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{ 1, 2, 3, 4 });
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<RemovePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());

    //Chunkserver-2 report (epoch=4, peers={1,2,3}, leader=0)

    hbtest_->BuildBasicChunkServerRequest(2, &req);
    BuildCopySetInfo(&csInfo, 4, 0, std::set<ChunkServerIdType>{ 1, 2, 3, 4 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    step = dynamic_cast<RemovePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithRemoveOp_14) {
    PrepareMdsWithRemoveOp();

    //Chunkserver-2 reports copyset-1 (epoch=0, peers={1,2,3,4}, leader=1)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 1, std::set<ChunkServerIdType>{ 1, 2, 3, 4 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<RemovePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());

    //Chunkserver-2 reports copyset-1 (epoch=0, peers={1,2,3}, leader=1)

    hbtest_->BuildBasicChunkServerRequest(2, &req);
    BuildCopySetInfo(&csInfo, 0, 1, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{ 1, 2, 3, 4 });
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    step = dynamic_cast<RemovePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithRemoveOp_15) {
    PrepareMdsWithRemoveOp();

    //Chunkserver-2 reports copyset-1 (epoch=0, peers={1,2,3,4}, leader=0)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 0, std::set<ChunkServerIdType>{ 1, 2, 3, 4 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<RemovePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());

    //Chunkserver-2 reports copyset-1 (epoch=0, peers={1,2,3}, leader=0)

    hbtest_->BuildBasicChunkServerRequest(2, &req);
    BuildCopySetInfo(&csInfo, 0, 0, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{ 1, 2, 3, 4 });
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    step = dynamic_cast<RemovePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithRemoveOp_16) {
    PrepareMdsWithRemoveOp();
    ChunkServer cs(5, "testtoken", "nvme", 3, "10.198.100.3", 9002, "/");
    hbtest_->PrepareAddChunkServer(cs);

    //Non replication group member chunkserver-5 reporting

    // copyset-1(epoch=5, peers={1,2,3,4}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(5, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 0, std::set<ChunkServerIdType>{ 1, 2, 3, 4 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    CopySetConf conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(4, conf.peers_size());
    ASSERT_EQ("10.198.100.1:9000:0", conf.peers(0).address());
    ASSERT_EQ("10.198.100.2:9000:0", conf.peers(1).address());
    ASSERT_EQ("10.198.100.3:9000:0", conf.peers(2).address());
    ASSERT_EQ("10.198.100.3:9001:0", conf.peers(3).address());
    ASSERT_EQ(5, conf.epoch());
    //Check copyset
    csInfo.SetLeader(1);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<RemovePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithRemoveOp_17) {
    PrepareMdsWithRemoveOp();
    ChunkServer cs(5, "testtoken", "nvme", 3, "10.198.100.3", 9002, "/");
    hbtest_->PrepareAddChunkServer(cs);

    //Non replication group member chunkserver-5 reporting

    // copyset-1(epoch=4, peers={1,2,3,4}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(5, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 0, std::set<ChunkServerIdType>{ 1, 2, 3, 4 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    CopySetConf conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(4, conf.peers_size());
    ASSERT_EQ("10.198.100.1:9000:0", conf.peers(0).address());
    ASSERT_EQ("10.198.100.2:9000:0", conf.peers(1).address());
    ASSERT_EQ("10.198.100.3:9000:0", conf.peers(2).address());
    ASSERT_EQ("10.198.100.3:9001:0", conf.peers(3).address());
    ASSERT_EQ(5, conf.epoch());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<RemovePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithRemoveOp_18) {
    PrepareMdsWithRemoveOp();
    ChunkServer cs(5, "testtoken", "nvme", 3, "10.198.100.3", 9002, "/");
    hbtest_->PrepareAddChunkServer(cs);

    //Non replication group member chunkserver-5 reporting

    // copyset-1(epoch=0, peers={1,2,3,4}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(5, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 0, std::set<ChunkServerIdType>{ 1, 2, 3, 4 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    CopySetConf conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(4, conf.peers_size());
    ASSERT_EQ("10.198.100.1:9000:0", conf.peers(0).address());
    ASSERT_EQ("10.198.100.2:9000:0", conf.peers(1).address());
    ASSERT_EQ("10.198.100.3:9000:0", conf.peers(2).address());
    ASSERT_EQ("10.198.100.3:9001:0", conf.peers(3).address());
    ASSERT_EQ(5, conf.epoch());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<RemovePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithRemoveOp_19) {
    PrepareMdsWithRemoveOp();
    ChunkServer cs(5, "testtoken", "nvme", 3, "10.198.100.3", 9002, "/");
    hbtest_->PrepareAddChunkServer(cs);

    //Non replication group member chunkserver-5 reporting

    // copyset-1(epoch=0, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(5, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 0, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    CopySetConf conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(4, conf.peers_size());
    ASSERT_EQ("10.198.100.1:9000:0", conf.peers(0).address());
    ASSERT_EQ("10.198.100.2:9000:0", conf.peers(1).address());
    ASSERT_EQ("10.198.100.3:9000:0", conf.peers(2).address());
    ASSERT_EQ("10.198.100.3:9001:0", conf.peers(3).address());
    ASSERT_EQ(5, conf.epoch());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{ 1, 2, 3, 4 });
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<RemovePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithRemoveOpOnGoing_1) {
    PrepareMdsWithRemoveOpOnGoing();
    //Chunkserver-1 reports copyset-1 (epoch=5, peers={1,2,3,4}, leader=1,

    //                          configChangeInfo={peer: 4, type: REMOVE_PEER})
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(1, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 1, std::set<ChunkServerIdType>{ 1, 2, 3, 4 },
                     4);
    hbtest_->AddCopySetToRequest(&req, csInfo, ConfigChangeType::REMOVE_PEER);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<RemovePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithRemoveOpOnGoing_2) {
    PrepareMdsWithRemoveOpOnGoing();
    //Chunkserver-1 reports copyset-1 (epoch=6, peers={1,2,3,4}, leader=1)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(1, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 1, std::set<ChunkServerIdType>{ 1, 2, 3, 4 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(6);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsWithRemoveOpOnGoing_3) {
    PrepareMdsWithRemoveOpOnGoing();
    //Chunkserver-2 reports copyset-1 (epoch=6, peers={1,2,3,4}, leader=1)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 2, std::set<ChunkServerIdType>{ 1, 2, 3, 4 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsWithRemoveOpOnGoing_4) {
    PrepareMdsWithRemoveOpOnGoing();
    //Chunkserver-2 reports copyset-1 (epoch=6, peers={1,2,3}, leader=1)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 2, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsWithRemoveOpOnGoing_5) {
    PrepareMdsWithRemoveOpOnGoing();
    //Chunkserver-2 reports copyset-1 (epoch=7, peers={1,2,3}, leader=2)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 7, 2, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsWithRemoveOpOnGoing_6) {
    PrepareMdsWithRemoveOpOnGoing();
    //Chunkserver-2 reports copyset-1 (epoch=5, peers={1,2,3,4}, leader=1)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 1, std::set<ChunkServerIdType>{ 1, 2, 3, 4 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<RemovePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithRemoveOpOnGoing_7) {
    PrepareMdsWithRemoveOpOnGoing();
    //Chunkserver-2 reports copyset-1 (epoch=5, peers={1,2,3,4}, leader=0)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 0, std::set<ChunkServerIdType>{ 1, 2, 3, 4 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetLeader(1);
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<RemovePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithRemoveOpOnGoing_8) {
    PrepareMdsWithRemoveOpOnGoing();
    //Chunkserver-2 reports copyset-1 (epoch=6, peers={1,2,3,4}, leader=1)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 1, std::set<ChunkServerIdType>{ 1, 2, 3, 4 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{ 1, 2, 3, 4 });
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<RemovePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithRemoveOpOnGoing_9) {
    PrepareMdsWithRemoveOpOnGoing();
    //Chunkserver-2 reports copyset-1 (epoch=6, peers={1,2,3,4}, leader=0)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 0, std::set<ChunkServerIdType>{ 1, 2, 3, 4 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{ 1, 2, 3, 4 });
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<RemovePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithRemoveOpOnGoing_10) {
    PrepareMdsWithRemoveOpOnGoing();
    //Chunkserver-2 reports copyset-1 (epoch=6, peers={1,2,3}, leader=1)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 1, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{ 1, 2, 3, 4 });
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<RemovePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithRemoveOpOnGoing_11) {
    PrepareMdsWithRemoveOpOnGoing();
    //Chunkserver-2 reports copyset-1 (epoch=6, peers={1,2,3}, leader=0)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 0, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{ 1, 2, 3, 4 });
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<RemovePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithRemoveOpOnGoing_12) {
    PrepareMdsWithRemoveOpOnGoing();
    //Chunkserver-2 reports copyset-1 (epoch=4, peers={1,2,3,4}, leader=1)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 1, std::set<ChunkServerIdType>{ 1, 2, 3, 4 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{ 1, 2, 3, 4 });
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<RemovePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithRemoveOpOnGoing_13) {
    PrepareMdsWithRemoveOpOnGoing();
    //Chunkserver-2 reports copyset-1 (epoch=4, peers={1,2,3,4}, leader=0)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 0, std::set<ChunkServerIdType>{ 1, 2, 3, 4 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<RemovePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithRemoveOpOnGoing_14) {
    PrepareMdsWithRemoveOpOnGoing();
    //Chunkserver-2 reports copyset-1 (epoch=4, peers={1,2,3}, leader=1)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 1, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{ 1, 2, 3, 4 });
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<RemovePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithRemoveOpOnGoing_15) {
    PrepareMdsWithRemoveOpOnGoing();
    //Chunkserver-2 reports copyset-1 (epoch=4, peers={1,2,3}, leader=0)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 0, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{ 1, 2, 3, 4 });
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<RemovePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithRemoveOpOnGoing_16) {
    PrepareMdsWithRemoveOpOnGoing();
    //Chunkserver-2 reports copyset-1 (epoch=0, peers={1,2,3,4}, leader=1)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 1, std::set<ChunkServerIdType>{ 1, 2, 3, 4 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<RemovePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithRemoveOpOnGoing_17) {
    PrepareMdsWithRemoveOpOnGoing();
    //Chunkserver-2 reports copyset-1 (epoch=0, peers={1,2,3,4}, leader=0)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 0, std::set<ChunkServerIdType>{ 1, 2, 3, 4 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<RemovePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithRemoveOpOnGoing_18) {
    PrepareMdsWithRemoveOpOnGoing();
    //Chunkserver-2 reports copyset-1 (epoch=0, peers={1,2,3}, leader=1)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 1, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{ 1, 2, 3, 4 });
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<RemovePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithRemoveOpOnGoing_19) {
    PrepareMdsWithRemoveOpOnGoing();
    //Chunkserver-2 reports copyset-1 (epoch=0, peers={1,2,3}, leader=0)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 0, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{ 1, 2, 3, 4 });
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<RemovePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithRemoveOpOnGoing_20) {
    PrepareMdsWithRemoveOpOnGoing();
    //Chunkserver-4 reports copyset-1 (epoch=5, peers={1,2,3,4}, leader=1)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 1, std::set<ChunkServerIdType>{ 1, 2, 3, 4 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<RemovePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithRemoveOpOnGoing_21) {
    PrepareMdsWithRemoveOpOnGoing();
    //Chunkserver-4 reports copyset-1 (epoch=5, peers={1,2,3,4}, leader=0)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 0, std::set<ChunkServerIdType>{ 1, 2, 3, 4 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetLeader(1);
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<RemovePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithRemoveOpOnGoing_22) {
    PrepareMdsWithRemoveOpOnGoing();
    //Chunkserver-4 reports copyset-1 (epoch=6, peers={1,2,3,4}, leader=1)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 1, std::set<ChunkServerIdType>{ 1, 2, 3, 4 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<RemovePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithRemoveOpOnGoing_23) {
    PrepareMdsWithRemoveOpOnGoing();
    //Chunkserver-4 reports copyset-1 (epoch=6, peers={1,2,3,4}, leader=0)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 0, std::set<ChunkServerIdType>{ 1, 2, 3, 4 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<RemovePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithRemoveOpOnGoing_24) {
    PrepareMdsWithRemoveOpOnGoing();
    //Chunkserver-4 reports copyset-1 (epoch=6, peers={1,2,3}, leader=1)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 1, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{ 1, 2, 3, 4 });
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<RemovePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithRemoveOpOnGoing_25) {
    PrepareMdsWithRemoveOpOnGoing();
    //Chunkserver-4 reports copyset-1 (epoch=6, peers={1,2,3}, leader=0)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 0, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{ 1, 2, 3, 4 });
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<RemovePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithRemoveOpOnGoing_26) {
    PrepareMdsWithRemoveOpOnGoing();
    //Chunkserver-4 reports copyset-1 (epoch=4, peers={1,2,3,4}, leader=1)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 1, std::set<ChunkServerIdType>{ 1, 2, 3, 4 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{ 1, 2, 3, 4 });
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<RemovePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithRemoveOpOnGoing_27) {
    PrepareMdsWithRemoveOpOnGoing();
    //Chunkserver-4 reports copyset-1 (epoch=4, peers={1,2,3,4}, leader=0)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 0, std::set<ChunkServerIdType>{ 1, 2, 3, 4 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<RemovePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithRemoveOpOnGoing_28) {
    PrepareMdsWithRemoveOpOnGoing();
    //Chunkserver-4 reports copyset-1 (epoch=4, peers={1,2,3}, leader=1)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 1, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{ 1, 2, 3, 4 });
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<RemovePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithRemoveOpOnGoing_29) {
    PrepareMdsWithRemoveOpOnGoing();
    //Chunkserver-4 reports copyset-1 (epoch=4, peers={1,2,3}, leader=0)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 0, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{ 1, 2, 3, 4 });
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<RemovePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithRemoveOpOnGoing_30) {
    PrepareMdsWithRemoveOpOnGoing();
    //Chunkserver-4 reports copyset-1 (epoch=0, peers={1,2,3,4}, leader=1)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 1, std::set<ChunkServerIdType>{ 1, 2, 3, 4 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<RemovePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithRemoveOpOnGoing_31) {
    PrepareMdsWithRemoveOpOnGoing();
    //Chunkserver-4 reports copyset-1 (epoch=0, peers={1,2,3,4}, leader=0)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 0, std::set<ChunkServerIdType>{ 1, 2, 3, 4 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<RemovePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithRemoveOpOnGoing_32) {
    PrepareMdsWithRemoveOpOnGoing();
    //Chunkserver-4 reports copyset-1 (epoch=0, peers={1,2,3}, leader=1)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 1, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{ 1, 2, 3, 4 });
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<RemovePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithRemoveOpOnGoing_33) {
    PrepareMdsWithRemoveOpOnGoing();
    //Chunkserver-4 reports copyset-1 (epoch=0, peers={1,2,3}, leader=0)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 0, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{ 1, 2, 3, 4 });
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<RemovePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithRemoveOpOnGoing_34) {
    PrepareMdsWithRemoveOpOnGoing();
    ChunkServer cs(5, "testtoken", "nvme", 3, "10.198.100.3", 9002, "/");
    hbtest_->PrepareAddChunkServer(cs);

    //Non replication group member chunkserver-5 reporting

    // copyset-1(epoch=5, peers={1,2,3,4}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(5, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 0, std::set<ChunkServerIdType>{ 1, 2, 3, 4 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    CopySetConf conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(4, conf.peers_size());
    ASSERT_EQ("10.198.100.1:9000:0", conf.peers(0).address());
    ASSERT_EQ("10.198.100.2:9000:0", conf.peers(1).address());
    ASSERT_EQ("10.198.100.3:9000:0", conf.peers(2).address());
    ASSERT_EQ("10.198.100.3:9001:0", conf.peers(3).address());
    ASSERT_EQ(5, conf.epoch());
    //Check copyset
    csInfo.SetLeader(1);
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<RemovePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithRemoveOpOnGoing_35) {
    PrepareMdsWithRemoveOpOnGoing();
    ChunkServer cs(5, "testtoken", "nvme", 3, "10.198.100.3", 9002, "/");
    hbtest_->PrepareAddChunkServer(cs);

    //Non replication group member chunkserver-5 reporting

    // copyset-1(epoch=4, peers={1,2,3,4}, leader=0）
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(5, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 0, std::set<ChunkServerIdType>{ 1, 2, 3, 4 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    CopySetConf conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(4, conf.peers_size());
    ASSERT_EQ("10.198.100.1:9000:0", conf.peers(0).address());
    ASSERT_EQ("10.198.100.2:9000:0", conf.peers(1).address());
    ASSERT_EQ("10.198.100.3:9000:0", conf.peers(2).address());
    ASSERT_EQ("10.198.100.3:9001:0", conf.peers(3).address());
    ASSERT_EQ(5, conf.epoch());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<RemovePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());

    //Non replication group member chunkserver-5 reporting

    // copyset-1(epoch=4, peers={1,2,3}, leader=0）
    rep.Clear();
    req.Clear();
    hbtest_->BuildBasicChunkServerRequest(5, &req);
    BuildCopySetInfo(&csInfo, 4, 0, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    //Check response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(4, conf.peers_size());
    ASSERT_EQ("10.198.100.1:9000:0", conf.peers(0).address());
    ASSERT_EQ("10.198.100.2:9000:0", conf.peers(1).address());
    ASSERT_EQ("10.198.100.3:9000:0", conf.peers(2).address());
    ASSERT_EQ("10.198.100.3:9001:0", conf.peers(3).address());
    ASSERT_EQ(5, conf.epoch());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{ 1, 2, 3, 4 });
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    step = dynamic_cast<RemovePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithRemoveOpOnGoing_36) {
    PrepareMdsWithRemoveOpOnGoing();
    ChunkServer cs(5, "testtoken", "nvme", 3, "10.198.100.3", 9002, "/");
    hbtest_->PrepareAddChunkServer(cs);

    //Non replication group member chunkserver-5 reporting

    // copyset-1(epoch=0, peers={1,2,3,4}, leader=0
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(5, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 0, std::set<ChunkServerIdType>{ 1, 2, 3, 4 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    CopySetConf conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(4, conf.peers_size());
    ASSERT_EQ(5, conf.epoch());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<RemovePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());

    //Non replication group member chunkserver-5 reporting

    // copyset-1(epoch=0, peers={1,2,3}, leader=0）
    rep.Clear();
    req.Clear();
    hbtest_->BuildBasicChunkServerRequest(5, &req);
    BuildCopySetInfo(&csInfo, 0, 0, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    //Check response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(4, conf.peers_size());
    ASSERT_EQ(5, conf.epoch());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{ 1, 2, 3, 4 });
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    step = dynamic_cast<RemovePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithTransferOp_1) {
    PrepareMdsWithTransferOp();
    //Chunkserver-1 reports copyset-1 (epoch=5, peers={1,2,3}, leader=1)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(1, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 1, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    //Check response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    ASSERT_EQ(1, rep.needupdatecopysets(0).copysetid());
    ASSERT_EQ(5, rep.needupdatecopysets(0).epoch());
    ASSERT_EQ("10.198.100.1:9000:0",
              rep.needupdatecopysets(0).peers(0).address());
    ASSERT_EQ("10.198.100.2:9000:0",
              rep.needupdatecopysets(0).peers(1).address());
    ASSERT_EQ("10.198.100.3:9000:0",
              rep.needupdatecopysets(0).peers(2).address());
    ASSERT_EQ(ConfigChangeType::TRANSFER_LEADER,
              rep.needupdatecopysets(0).type());
    ASSERT_EQ("10.198.100.2:9000:0",
              rep.needupdatecopysets(0).configchangeitem().address());
    //Check copyset
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<TransferLeader *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(2, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithTransferOp_2) {
    PrepareMdsWithTransferOp();
    //Chunkserver-1 reports copyset-1 (epoch=5, peers={1,2,3}, leader=1)

    //                  configChangeInfo={peer: 2, type: TRANSFER_LEADER})
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(1, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 1, std::set<ChunkServerIdType>{ 1, 2, 3 }, 2);
    hbtest_->AddCopySetToRequest(&req, csInfo,
                                 ConfigChangeType::TRANSFER_LEADER);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<TransferLeader *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(2, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithTransferOp_3) {
    PrepareMdsWithTransferOp();
    //Chunkserver-1 reports copyset-1 (epoch=6, peers={1,2,3}, leader=1)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(1, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 1, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsWithTransferOp_4) {
    PrepareMdsWithTransferOp();
    //Chunkserver-2 reports copyset-1 (epoch=6, peers={1,2,3}, leader=2)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 2, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsWithTransferOp_5) {
    PrepareMdsWithTransferOp();
    //Chunkserver-2 reports copyset-1 (epoch=5, peers={1,2,3}, leader=1)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 1, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<TransferLeader *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(2, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithTransferOp_6) {
    PrepareMdsWithTransferOp();
    //Chunkserver-2 reports copyset-1 (epoch=5, peers={1,2,3}, leader=0)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 0, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetLeader(1);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<TransferLeader *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(2, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithTransferOp_7) {
    PrepareMdsWithTransferOp();
    //Chunkserver-2 reports copyset-1 (epoch=6, peers={1,2,3}, leader=1)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 1, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<TransferLeader *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(2, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithTransferOp_8) {
    PrepareMdsWithTransferOp();
    //Chunkserver-2 reports copyset-1 (epoch=6, peers={1,2,3}, leader=0)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 0, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<TransferLeader *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(2, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithTransferOp_9) {
    PrepareMdsWithTransferOp();
    //Chunkserver-2 reports copyset-1 (epoch=4, peers={1,2,3}, leader=1)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 1, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<TransferLeader *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(2, step->GetTargetPeer());

    //Chunkserver-2 reports copyset-1 (epoch=4, peers={1,2,3,4}, leader=1)

    ChunkServer cs(4, "testtoken", "nvme", 3, "10.198.100.3", 9001, "/");
    hbtest_->PrepareAddChunkServer(cs);
    rep.Clear();
    req.Clear();
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    BuildCopySetInfo(&csInfo, 4, 1, std::set<ChunkServerIdType>{ 1, 2, 3, 4 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{ 1, 2, 3 });
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    step = dynamic_cast<TransferLeader *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(2, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithTransferOp_10) {
    PrepareMdsWithTransferOp();
    //Chunkserver-2 reports copyset-1 (epoch=4, peers={1,2,3}, leader=0)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 0, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<TransferLeader *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(2, step->GetTargetPeer());

    //Chunkserver-2 reports copyset-1 (epoch=4, peers={1,2,3,4}, leader=0)

    ChunkServer cs(4, "testtoken", "nvme", 3, "10.198.100.3", 9001, "/");
    hbtest_->PrepareAddChunkServer(cs);
    rep.Clear();
    req.Clear();
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    BuildCopySetInfo(&csInfo, 4, 0, std::set<ChunkServerIdType>{ 1, 2, 3, 4 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{ 1, 2, 3 });
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    step = dynamic_cast<TransferLeader *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(2, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithTransferOp_11) {
    PrepareMdsWithTransferOp();
    //Chunkserver-2 reports copyset-1 (epoch=0, peers={1,2,3}, leader=1)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 1, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<TransferLeader *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(2, step->GetTargetPeer());

    //Chunkserver-2 reports copyset-1 (epoch=0, peers={1,2,3,4}, leader=1)

    ChunkServer cs(4, "testtoken", "nvme", 3, "10.198.100.3", 9001, "/");
    hbtest_->PrepareAddChunkServer(cs);
    rep.Clear();
    req.Clear();
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    BuildCopySetInfo(&csInfo, 0, 1, std::set<ChunkServerIdType>{ 1, 2, 3, 4 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{ 1, 2, 3 });
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    step = dynamic_cast<TransferLeader *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(2, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithTransferOp_12) {
    PrepareMdsWithTransferOp();
    //Chunkserver-2 reports copyset-1 (epoch=0, peers={1,2,3}, leader=0)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 0, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<TransferLeader *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(2, step->GetTargetPeer());

    //Chunkserver-2 reports copyset-1 (epoch=0, peers={1,2,3,4}, leader=0)

    ChunkServer cs(4, "testtoken", "nvme", 3, "10.198.100.3", 9001, "/");
    hbtest_->PrepareAddChunkServer(cs);
    rep.Clear();
    req.Clear();
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    BuildCopySetInfo(&csInfo, 0, 0, std::set<ChunkServerIdType>{ 1, 2, 3, 4 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{ 1, 2, 3 });
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    step = dynamic_cast<TransferLeader *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(2, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithTransferOp_13) {
    PrepareMdsWithTransferOp();
    ChunkServer cs(4, "testtoken", "nvme", 3, "10.198.100.3", 9001, "/");
    hbtest_->PrepareAddChunkServer(cs);
    //Chunkserver-4 reports copyset-1 (epoch=5, peers={1,2,3}, leader=0)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 0, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    //Check response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    auto conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(3, conf.peers_size());
    ASSERT_EQ("10.198.100.1:9000:0", conf.peers(0).address());
    ASSERT_EQ("10.198.100.2:9000:0", conf.peers(1).address());
    ASSERT_EQ("10.198.100.3:9000:0", conf.peers(2).address());
    ASSERT_EQ(5, conf.epoch());
    //Check copyset
    csInfo.SetLeader(1);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<TransferLeader *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(2, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithTransferOp_14) {
    PrepareMdsWithTransferOp();
    ChunkServer cs(4, "testtoken", "nvme", 3, "10.198.100.3", 9001, "/");
    hbtest_->PrepareAddChunkServer(cs);
    //Chunkserver-4 reports copyset-1 (epoch=4, peers={1,2,3}, leader=0)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 0, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    //Check response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    auto conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(3, conf.peers_size());
    ASSERT_EQ("10.198.100.1:9000:0", conf.peers(0).address());
    ASSERT_EQ("10.198.100.2:9000:0", conf.peers(1).address());
    ASSERT_EQ("10.198.100.3:9000:0", conf.peers(2).address());
    ASSERT_EQ(5, conf.epoch());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<TransferLeader *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(2, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithTransferOp_15) {
    PrepareMdsWithTransferOp();
    ChunkServer cs1(4, "testtoken", "nvme", 3, "10.198.100.3", 9001, "/");
    hbtest_->PrepareAddChunkServer(cs1);
    ChunkServer cs2(5, "testtoken", "nvme", 3, "10.198.100.3", 9002, "/");
    hbtest_->PrepareAddChunkServer(cs2);
    //Chunkserver-5 reports copyset-1 (epoch=4, peers={1,2,3,4}, leader=0)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(5, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 0, std::set<ChunkServerIdType>{ 1, 2, 3, 4 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    //Check response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    auto conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(3, conf.peers_size());
    ASSERT_EQ("10.198.100.1:9000:0", conf.peers(0).address());
    ASSERT_EQ("10.198.100.2:9000:0", conf.peers(1).address());
    ASSERT_EQ("10.198.100.3:9000:0", conf.peers(2).address());
    ASSERT_EQ(5, conf.epoch());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{ 1, 2, 3 });
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<TransferLeader *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(2, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithTransferOp_16) {
    PrepareMdsWithTransferOp();
    ChunkServer cs1(4, "testtoken", "nvme", 3, "10.198.100.3", 9001, "/");
    hbtest_->PrepareAddChunkServer(cs1);
    ChunkServer cs2(5, "testtoken", "nvme", 3, "10.198.100.3", 9002, "/");
    hbtest_->PrepareAddChunkServer(cs2);

    //Chunkserver-5 reports copyset-1 (epoch=0, peers={1,2,3}, leader=0)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(5, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 0, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    //Check response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    auto conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(3, conf.peers_size());
    ASSERT_EQ("10.198.100.1:9000:0", conf.peers(0).address());
    ASSERT_EQ("10.198.100.2:9000:0", conf.peers(1).address());
    ASSERT_EQ("10.198.100.3:9000:0", conf.peers(2).address());
    ASSERT_EQ(5, conf.epoch());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<TransferLeader *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(2, step->GetTargetPeer());

    //Chunkserver-5 reports copyset-1 (epoch=0, peers={1,2,3,4}, leader=0)

    req.Clear();
    rep.Clear();
    hbtest_->BuildBasicChunkServerRequest(5, &req);
    BuildCopySetInfo(&csInfo, 0, 0, std::set<ChunkServerIdType>{ 1, 2, 3, 4 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    //Check response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(3, conf.peers_size());
    ASSERT_EQ("10.198.100.1:9000:0", conf.peers(0).address());
    ASSERT_EQ("10.198.100.2:9000:0", conf.peers(1).address());
    ASSERT_EQ("10.198.100.3:9000:0", conf.peers(2).address());
    ASSERT_EQ(5, conf.epoch());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{ 1, 2, 3 });
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    step = dynamic_cast<TransferLeader *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(2, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithTransferOpOnGoing_1) {
    PrepareMdsWithTransferOpOnGoing();
    //Chunkserver-1 reports copyset-1 (epoch=5, peers={1,2,3}, leader=1,

    //                  configChangeInfo={peer: 2, type: TRANSFER_LEADER})
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(1, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 1, std::set<ChunkServerIdType>{ 1, 2, 3 }, 2);
    hbtest_->AddCopySetToRequest(&req, csInfo,
                                 ConfigChangeType::TRANSFER_LEADER);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<TransferLeader *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(2, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithTransferOpOnGoing_2) {
    PrepareMdsWithTransferOpOnGoing();
    //Chunkserver-1 reports copyset-1 (epoch=6, peers={1,2,3}, leader=1)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(1, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 1, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsWithTransferOpOnGoing_3) {
    PrepareMdsWithTransferOpOnGoing();
    //Chunkserver-2 reports copyset-1 (epoch=6, peers={1,2,3}, leader=2)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 2, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsWithTransferOpOnGoing_4) {
    PrepareMdsWithTransferOpOnGoing();
    //Chunkserver-2 reports copyset-1 (epoch=5, peers={1,2,3}, leader=1)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 1, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetCandidate(2);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<TransferLeader *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(2, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithTransferOpOnGoing_5) {
    PrepareMdsWithTransferOpOnGoing();
    //Chunkserver-2 reports copyset-1 (epoch=5, peers={1,2,3}, leader=0)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 0, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetLeader(1);
    csInfo.SetCandidate(2);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<TransferLeader *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(2, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithTransferOpOnGoing_6) {
    PrepareMdsWithTransferOpOnGoing();
    //Chunkserver-2 reports copyset-1 (epoch=6, peers={1,2,3}, leader=1)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 1, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetCandidate(2);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<TransferLeader *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(2, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithTransferOpOnGoing_7) {
    PrepareMdsWithTransferOpOnGoing();
    //Chunkserver-2 reports copyset-1 (epoch=4, peers={1,2,3}, leader=1)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 1, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetCandidate(2);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<TransferLeader *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(2, step->GetTargetPeer());

    //Chunkserver-2 reports copyset-1 (epoch=4, peers={1,2,3,4}, leader=1)

    ChunkServer cs(4, "testtoken", "nvme", 3, "10.198.100.3", 9001, "/");
    hbtest_->PrepareAddChunkServer(cs);
    rep.Clear();
    req.Clear();
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    BuildCopySetInfo(&csInfo, 4, 1, std::set<ChunkServerIdType>{ 1, 2, 3, 4 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetCandidate(2);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{ 1, 2, 3 });
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    step = dynamic_cast<TransferLeader *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(2, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithTransferOpOnGoing_8) {
    PrepareMdsWithTransferOpOnGoing();
    //Chunkserver-2 reports copyset-1 (epoch=4, peers={1,2,3}, leader=0)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 0, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCandidate(2);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<TransferLeader *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(2, step->GetTargetPeer());

    //Chunkserver-2 reports copyset-1 (epoch=4, peers={1,2,3,4}, leader=0)

    ChunkServer cs(4, "testtoken", "nvme", 3, "10.198.100.3", 9001, "/");
    hbtest_->PrepareAddChunkServer(cs);
    rep.Clear();
    req.Clear();
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    BuildCopySetInfo(&csInfo, 4, 0, std::set<ChunkServerIdType>{ 1, 2, 3, 4 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCandidate(2);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{ 1, 2, 3 });
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    step = dynamic_cast<TransferLeader *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(2, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithTransferOpOnGoing_9) {
    PrepareMdsWithTransferOpOnGoing();
    //Chunkserver-2 reports copyset-1 (epoch=0, peers={1,2,3}, leader=1)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 1, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetCandidate(2);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<TransferLeader *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(2, step->GetTargetPeer());

    //Chunkserver-2 reports copyset-1 (epoch=0, peers={1,2,3,4}, leader=1)

    ChunkServer cs(4, "testtoken", "nvme", 3, "10.198.100.3", 9001, "/");
    hbtest_->PrepareAddChunkServer(cs);
    rep.Clear();
    req.Clear();
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    BuildCopySetInfo(&csInfo, 0, 1, std::set<ChunkServerIdType>{ 1, 2, 3, 4 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetCandidate(2);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{ 1, 2, 3 });
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    step = dynamic_cast<TransferLeader *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(2, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithTransferOpOnGoing_10) {
    PrepareMdsWithTransferOpOnGoing();
    //Chunkserver-2 reports copyset-1 (epoch=0, peers={1,2,3}, leader=0)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 0, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCandidate(2);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<TransferLeader *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(2, step->GetTargetPeer());

    //Chunkserver-2 reports copyset-1 (epoch=0, peers={1,2,3,4}, leader=0)

    ChunkServer cs(4, "testtoken", "nvme", 3, "10.198.100.3", 9001, "/");
    hbtest_->PrepareAddChunkServer(cs);
    rep.Clear();
    req.Clear();
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    BuildCopySetInfo(&csInfo, 0, 0, std::set<ChunkServerIdType>{ 1, 2, 3, 4 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCandidate(2);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{ 1, 2, 3 });
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    step = dynamic_cast<TransferLeader *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(2, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithTransferOpOnGoing_11) {
    PrepareMdsWithTransferOpOnGoing();
    ChunkServer cs(4, "testtoken", "nvme", 3, "10.198.100.3", 9001, "/");
    hbtest_->PrepareAddChunkServer(cs);
    //Chunkserver-4 reports copyset-1 (epoch=5, peers={1,2,3}, leader=0)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 0, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    //Check response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    auto conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(3, conf.peers_size());
    ASSERT_EQ("10.198.100.1:9000:0", conf.peers(0).address());
    ASSERT_EQ("10.198.100.2:9000:0", conf.peers(1).address());
    ASSERT_EQ("10.198.100.3:9000:0", conf.peers(2).address());
    ASSERT_EQ(5, conf.epoch());
    //Check copyset
    csInfo.SetLeader(1);
    csInfo.SetCandidate(2);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<TransferLeader *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(2, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithTransferOpOnGoing_12) {
    PrepareMdsWithTransferOpOnGoing();
    ChunkServer cs(4, "testtoken", "nvme", 3, "10.198.100.3", 9001, "/");
    hbtest_->PrepareAddChunkServer(cs);
    //Chunkserver-4 reports copyset-1 (epoch=4, peers={1,2,3}, leader=0)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 0, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    //Check response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    auto conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(3, conf.peers_size());
    ASSERT_EQ("10.198.100.1:9000:0", conf.peers(0).address());
    ASSERT_EQ("10.198.100.2:9000:0", conf.peers(1).address());
    ASSERT_EQ("10.198.100.3:9000:0", conf.peers(2).address());
    ASSERT_EQ(5, conf.epoch());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCandidate(2);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<TransferLeader *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(2, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithTransferOpOnGoing_13) {
    PrepareMdsWithTransferOpOnGoing();
    ChunkServer cs1(4, "testtoken", "nvme", 3, "10.198.100.3", 9001, "/");
    hbtest_->PrepareAddChunkServer(cs1);
    ChunkServer cs2(5, "testtoken", "nvme", 3, "10.198.100.3", 9002, "/");
    hbtest_->PrepareAddChunkServer(cs2);

    //Chunkserver-5 reports copyset-1 (epoch=0, peers={1,2,3}, leader=0)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(5, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 0, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    //Check response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    auto conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(3, conf.peers_size());
    ASSERT_EQ("10.198.100.1:9000:0", conf.peers(0).address());
    ASSERT_EQ("10.198.100.2:9000:0", conf.peers(1).address());
    ASSERT_EQ("10.198.100.3:9000:0", conf.peers(2).address());
    ASSERT_EQ(5, conf.epoch());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCandidate(2);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<TransferLeader *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(2, step->GetTargetPeer());

    //Chunkserver-5 reports copyset-1 (epoch=0, peers={1,2,3,4}, leader=0)

    req.Clear();
    rep.Clear();
    hbtest_->BuildBasicChunkServerRequest(5, &req);
    BuildCopySetInfo(&csInfo, 0, 0, std::set<ChunkServerIdType>{ 1, 2, 3, 4 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    //Check response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(3, conf.peers_size());
    ASSERT_EQ("10.198.100.1:9000:0", conf.peers(0).address());
    ASSERT_EQ("10.198.100.2:9000:0", conf.peers(1).address());
    ASSERT_EQ("10.198.100.3:9000:0", conf.peers(2).address());
    ASSERT_EQ(5, conf.epoch());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCandidate(2);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{ 1, 2, 3 });
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    step = dynamic_cast<TransferLeader *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(2, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidateNoOp_1) {
    PrePareMdsWithCandidateNoOp();
    //Chunkserver-1 reports copyset-1 (epoch=5, peers={1,2,3}, leader=1)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(1, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 1, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidateNoOp_2) {
    PrePareMdsWithCandidateNoOp();
    //Chunkserver-1 reports copyset-1 (epoch=5, peers={1,2,3}, leader=1)

    //                          configChangeInfo={peer: 4, type: ADD_PEER})
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(1, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 1, std::set<ChunkServerIdType>{ 1, 2, 3 }, 4);
    hbtest_->AddCopySetToRequest(&req, csInfo, ConfigChangeType::ADD_PEER);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidateNoOp_3) {
    PrePareMdsWithCandidateNoOp();
    //Chunkserver-1 reports copyset-1 (epoch=6, peers={1,2,3}, leader=1)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(1, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 1, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidateNoOp_4) {
    PrePareMdsWithCandidateNoOp();
    //Chunkserver-2 reports copyset-1 (epoch=6, peers={1,2,3}, leader=2)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 2, std::set<ChunkServerIdType>{ 1, 2, 3 });

    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidateNoOp_5) {
    PrePareMdsWithCandidateNoOp();
    //Chunkserver-1 reports copyset-1 (epoch=6, peers={1,2,3,4}, leader=1)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(1, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 1, std::set<ChunkServerIdType>{ 1, 2, 3, 4 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidateNoOp_6) {
    PrePareMdsWithCandidateNoOp();
    //Chunkserver-2 reports copyset-1 (epoch=7, peers={1,2,3,4}, leader=2)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 7, 2, std::set<ChunkServerIdType>{ 1, 2, 3, 4 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidateNoOp_7) {
    PrePareMdsWithCandidateNoOp();
    //Chunkserver-2 reports copyset-1 (epoch=5, peers={1,2,3}, leader=1)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 1, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidateNoOp_8) {
    PrePareMdsWithCandidateNoOp();
    //Chunkserver-2 reports copyset-1 (epoch=5, peers={1,2,3}, leader=0)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 0, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetLeader(1);
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidateNoOp_9) {
    PrePareMdsWithCandidateNoOp();
    //Chunkserver-2 reports copyset-1 (epoch=6, peers={1,2,3}, leader=1)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 1, std::set<ChunkServerIdType>{ 1, 2, 3 });

    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidateNoOp_10) {
    PrePareMdsWithCandidateNoOp();
    //Chunkserver-2 reports copyset-1 (epoch=6, peers={1,2,3}, leader=0)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 0, std::set<ChunkServerIdType>{ 1, 2, 3 });

    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidateNoOp_11) {
    PrePareMdsWithCandidateNoOp();
    //Chunkserver-2 reports copyset-1 (epoch=6, peers={1,2,3,4}, leader=1)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 1, std::set<ChunkServerIdType>{ 1, 2, 3, 4 });

    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetCandidate(4);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{ 1, 2, 3 });
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidateNoOp_12) {
    PrePareMdsWithCandidateNoOp();
    //Chunkserver-2 reports copyset-1 (epoch=6, peers={1,2,3,4}, leader=0)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 0, std::set<ChunkServerIdType>{ 1, 2, 3, 4 });

    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCandidate(4);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{ 1, 2, 3 });
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidateNoOp_13) {
    PrePareMdsWithCandidateNoOp();
    //Chunkserver-2 reports copyset-1 (epoch=4, peers={1,2,3}, leader=1)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 1, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCandidate(4);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{ 1, 2, 3 });
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());

    //Chunkserver-2 reports copyset-1 (epoch=4, peers={1,2,3,4}, leader=1)

    req.Clear();
    rep.Clear();
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    BuildCopySetInfo(&csInfo, 4, 1, std::set<ChunkServerIdType>{ 1, 2, 3, 4 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetCandidate(4);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{ 1, 2, 3 });
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidateNoOp_14) {
    PrePareMdsWithCandidateNoOp();
    //Chunkserver-2 reports copyset-1 (epoch=4, peers={1,2,3}, leader=0)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 0, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());

    //Chunkserver-2 reports copyset-1 (epoch=4, peers={1,2,3,4}, leader=0)

    req.Clear();
    rep.Clear();
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    BuildCopySetInfo(&csInfo, 4, 0, std::set<ChunkServerIdType>{ 1, 2, 3, 4 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCandidate(4);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{ 1, 2, 3 });
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidateNoOp_15) {
    PrePareMdsWithCandidateNoOp();
    //Chunkserver-2 reports copyset-1 (epoch=0, peers={1,2,3}, leader=1)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 1, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());

    //Chunkserver-2 reports copyset-1 (epoch=0, peers={1,2,3,4}, leader=1)

    req.Clear();
    rep.Clear();
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    BuildCopySetInfo(&csInfo, 0, 1, std::set<ChunkServerIdType>{ 1, 2, 3, 4 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetCandidate(4);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{ 1, 2, 3 });
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidateNoOp_16) {
    PrePareMdsWithCandidateNoOp();
    //Chunkserver-2 reports copyset-1 (epoch=0, peers={1,2,3}, leader=0)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 0, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());

    //Chunkserver-2 reports copyset-1 (epoch=0, peers={1,2,3,4}, leader=0)

    req.Clear();
    rep.Clear();
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    BuildCopySetInfo(&csInfo, 0, 0, std::set<ChunkServerIdType>{ 1, 2, 3, 4 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCandidate(4);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{ 1, 2, 3 });
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidateNoOp_17) {
    PrePareMdsWithCandidateNoOp();
    //Chunkserver-4 reports copyset-1 (epoch=5, peers={1,2,3}, leader=1)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 1, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidateNoOp_18) {
    PrePareMdsWithCandidateNoOp();
    //Chunkserver-4 reports copyset-1 (epoch=5, peers={1,2,3}, leader=0)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 0, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetLeader(1);
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidateNoOp_19) {
    PrePareMdsWithCandidateNoOp();
    //Chunkserver-4 reports copyset-1 (epoch=6, peers={1,2,3,4}, leader=1)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 1, std::set<ChunkServerIdType>{ 1, 2, 3, 4 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetCandidate(4);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{ 1, 2, 3 });
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidateNoOp_20) {
    PrePareMdsWithCandidateNoOp();
    //Chunkserver-4 reports copyset-1 (epoch=6, peers={1,2,3,4}, leader=0)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 0, std::set<ChunkServerIdType>{ 1, 2, 3, 4 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCandidate(4);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{ 1, 2, 3 });
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidateNoOp_21) {
    PrePareMdsWithCandidateNoOp();
    //Chunkserver-4 reports copyset-1 (epoch=4, peers={1,2,3,4}, leader=1)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 1, std::set<ChunkServerIdType>{ 1, 2, 3, 4 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetCandidate(4);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{ 1, 2, 3 });
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidateNoOp_22) {
    PrePareMdsWithCandidateNoOp();
    //Chunkserver-4 reports copyset-1 (epoch=4, peers={1,2,3,4}, leader=0)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 0, std::set<ChunkServerIdType>{ 1, 2, 3, 4 });

    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCandidate(4);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{ 1, 2, 3 });
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidateNoOp_23) {
    PrePareMdsWithCandidateNoOp();
    //Chunkserver-4 reports copyset-1 (epoch=0, peers={1,2,3}, leader=1)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 1, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());

    //Chunkserver-4 reports copyset-1 (epoch=0, peers={1,2,3,5}, leader=1)

    ChunkServer cs(5, "testtoken", "nvme", 3, "10.198.100.3", 9002, "/");
    hbtest_->PrepareAddChunkServer(cs);
    req.Clear();
    rep.Clear();
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    BuildCopySetInfo(&csInfo, 0, 1, std::set<ChunkServerIdType>{ 1, 2, 3, 5 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetCandidate(4);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{ 1, 2, 3 });
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidateNoOp_24) {
    PrePareMdsWithCandidateNoOp();
    //Chunkserver-4 reports copyset-1 (epoch=0, peers={1,2,3}, leader=0)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 0, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());

    //Chunkserver-4 reports copyset-1 (epoch=0, peers={1,2,3,5}, leader=0)

    ChunkServer cs(5, "testtoken", "nvme", 3, "10.198.100.3", 9002, "/");
    hbtest_->PrepareAddChunkServer(cs);
    req.Clear();
    rep.Clear();
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    BuildCopySetInfo(&csInfo, 0, 0, std::set<ChunkServerIdType>{ 1, 2, 3, 5 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCandidate(4);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{ 1, 2, 3 });
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidateNoOp_25) {
    PrePareMdsWithCandidateNoOp();
    ChunkServer cs(5, "testtoken", "nvme", 3, "10.198.100.3", 9002, "/");
    hbtest_->PrepareAddChunkServer(cs);
    //Chunkserver-5 reports copyset-1 (epoch=5, peers={1,2,3}, leader=0)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(5, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 0, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    auto conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(3, conf.peers_size());
    ASSERT_EQ("10.198.100.1:9000:0", conf.peers(0).address());
    ASSERT_EQ("10.198.100.2:9000:0", conf.peers(1).address());
    ASSERT_EQ("10.198.100.3:9000:0", conf.peers(2).address());
    ASSERT_EQ(5, conf.epoch());
    //Check copyset
    csInfo.SetLeader(1);
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidateNoOp_26) {
    PrePareMdsWithCandidateNoOp();
    ChunkServer cs(5, "testtoken", "nvme", 3, "10.198.100.3", 9002, "/");
    hbtest_->PrepareAddChunkServer(cs);
    //Chunkserver-5 reports copyset-1 (epoch=4, peers={1,2,3}, leader=0)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(5, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 0, std::set<ChunkServerIdType>{ 1, 2, 3 });

    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    auto conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(3, conf.peers_size());
    ASSERT_EQ("10.198.100.1:9000:0", conf.peers(0).address());
    ASSERT_EQ("10.198.100.2:9000:0", conf.peers(1).address());
    ASSERT_EQ("10.198.100.3:9000:0", conf.peers(2).address());
    ASSERT_EQ(5, conf.epoch());
    //Check copyset
    csInfo.SetLeader(1);
    csInfo.SetEpoch(5);
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidateNoOp_27) {
    PrePareMdsWithCandidateNoOp();
    ChunkServer cs(5, "testtoken", "nvme", 3, "10.198.100.3", 9002, "/");
    hbtest_->PrepareAddChunkServer(cs);
    //Chunkserver-5 reports copyset-1 (epoch=4, peers={1,2,3,4}, leader=0)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(5, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 0, std::set<ChunkServerIdType>{ 1, 2, 3, 4 });

    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    auto conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(3, conf.peers_size());
    ASSERT_EQ("10.198.100.1:9000:0", conf.peers(0).address());
    ASSERT_EQ("10.198.100.2:9000:0", conf.peers(1).address());
    ASSERT_EQ("10.198.100.3:9000:0", conf.peers(2).address());
    ASSERT_EQ(5, conf.epoch());
    //Check copyset
    csInfo.SetLeader(1);
    csInfo.SetEpoch(5);
    csInfo.SetCandidate(4);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{ 1, 2, 3 });
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidateNoOp_28) {
    PrePareMdsWithCandidateNoOp();
    ChunkServer cs(5, "testtoken", "nvme", 3, "10.198.100.3", 9002, "/");
    hbtest_->PrepareAddChunkServer(cs);
    //Chunkserver-5 reports copyset-1 (epoch=0, peers={1,2,3}, leader=0)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(5, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 0, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    //Check response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    auto conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(3, conf.peers_size());
    ASSERT_EQ("10.198.100.1:9000:0", conf.peers(0).address());
    ASSERT_EQ("10.198.100.2:9000:0", conf.peers(1).address());
    ASSERT_EQ("10.198.100.3:9000:0", conf.peers(2).address());
    ASSERT_EQ(5, conf.epoch());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());

    //Chunkserver-4 reports copyset-1 (epoch=0, peers={1,2,3,4}, leader=0)

    req.Clear();
    rep.Clear();
    hbtest_->BuildBasicChunkServerRequest(5, &req);
    BuildCopySetInfo(&csInfo, 0, 0, std::set<ChunkServerIdType>{ 1, 2, 3, 4 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    //Check response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(3, conf.peers_size());
    ASSERT_EQ("10.198.100.1:9000:0", conf.peers(0).address());
    ASSERT_EQ("10.198.100.2:9000:0", conf.peers(1).address());
    ASSERT_EQ("10.198.100.3:9000:0", conf.peers(2).address());
    ASSERT_EQ(5, conf.epoch());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCandidate(4);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{ 1, 2, 3 });
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsCopysetNoLeader_1) {
    //Update copyset-1 in topology (epoch=5, peers={1,2,3}, leader=0)

    hbtest_->UpdateCopysetTopo(1, 1, 5, 0,
                               std::set<ChunkServerIdType>{ 1, 2, 3 });
    //Chunkserver-1 reports copyset-1 (epoch=5, peers={1,2,3}, leader=1)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(1, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 1, std::set<ChunkServerIdType>{ 1, 2, 3 });

    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsCopysetNoLeader_2) {
    //Update copyset-1 in topology (epoch=5, peers={1,2,3}, leader=0)

    hbtest_->UpdateCopysetTopo(1, 1, 5, 0,
                               std::set<ChunkServerIdType>{ 1, 2, 3 });
    //Chunkserver-1 reports copyset-1 (epoch=5, peers={1,2,3}, leader=1)

    //                  configChangeInfo={peer: 2, type: TRANSFER_LEADER})
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(1, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 1, std::set<ChunkServerIdType>{ 1, 2, 3 }, 2);
    hbtest_->AddCopySetToRequest(&req, csInfo,
                                 ConfigChangeType::TRANSFER_LEADER);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsCopysetNoLeader_3) {
    //Update copyset-1 in topology (epoch=5, peers={1,2,3}, leader=0)

    hbtest_->UpdateCopysetTopo(1, 1, 5, 0,
                               std::set<ChunkServerIdType>{ 1, 2, 3 });
    //Chunkserver-1 reports copyset-1 (epoch=6, peers={1,2,3}, leader=1)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(1, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 1, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsCopysetNoLeader_4) {
    //Update copyset-1 in topology (epoch=5, peers={1,2,3}, leader=0)
    hbtest_->UpdateCopysetTopo(1, 1, 5, 0,
                               std::set<ChunkServerIdType>{ 1, 2, 3 });
    //Chunkserver-1 reports copyset-1 (epoch=6, peers={1,2,3}, leader=1)
    //                  configChangeInfo={peer: 2, type: TRANSFER_LEADER})
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(1, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 1, std::set<ChunkServerIdType>{ 1, 2, 3 }, 2);
    hbtest_->AddCopySetToRequest(&req, csInfo,
                                 ConfigChangeType::TRANSFER_LEADER);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsCopysetNoLeader_5) {
    //Update copyset-1 in topology (epoch=5, peers={1,2,3}, leader=0)

    hbtest_->UpdateCopysetTopo(1, 1, 5, 0,
                               std::set<ChunkServerIdType>{ 1, 2, 3 });
    ChunkServer cs(4, "testtoken", "nvme", 3, "10.198.100.3", 9001, "/");
    hbtest_->PrepareAddChunkServer(cs);
    //Chunkserver-1 reports copyset-1 (epoch=6, peers={1,2,3,4}, leader=1)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(1, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 1, std::set<ChunkServerIdType>{ 1, 2, 3, 4 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsCopysetNoLeader_6) {
    //Update copyset-1 in topology (epoch=5, peers={1,2,3}, leader=0)

    hbtest_->UpdateCopysetTopo(1, 1, 5, 0,
                               std::set<ChunkServerIdType>{ 1, 2, 3 });
    ChunkServer cs(4, "testtoken", "nvme", 3, "10.198.100.3", 9001, "/");
    hbtest_->PrepareAddChunkServer(cs);
    //Chunkserver-1 reports copyset-1 (epoch=6, peers={1,2,3,4}, leader=1)

    //                  configChangeInfo={peer: 2, type: TRANSFER_LEADER})
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(1, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 1, std::set<ChunkServerIdType>{ 1, 2, 3, 4 },
                     2);
    hbtest_->AddCopySetToRequest(&req, csInfo,
                                 ConfigChangeType::TRANSFER_LEADER);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsCopysetNoLeader_7) {
    //Update copyset-1 in topology (epoch=5, peers={1,2,3}, leader=0)
    hbtest_->UpdateCopysetTopo(1, 1, 5, 0,
                               std::set<ChunkServerIdType>{ 1, 2, 3 });
    //Chunkserver-2 reports copyset-1 (epoch=5, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 1, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetLeader(0);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsCopysetNoLeader_8) {
    //Update copyset-1 in topology (epoch=5, peers={1,2,3}, leader=0)
    hbtest_->UpdateCopysetTopo(1, 1, 5, 0,
                               std::set<ChunkServerIdType>{ 1, 2, 3 });
    //Chunkserver-2 reports copyset-1 (epoch=5, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 0, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsCopysetNoLeader_9) {
    //Update copyset-1 in topology (epoch=5, peers={1,2,3}, leader=0)
    hbtest_->UpdateCopysetTopo(1, 1, 5, 0,
                               std::set<ChunkServerIdType>{ 1, 2, 3 });
    //Chunkserver-2 reports copyset-1 (epoch=6, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 1, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetLeader(0);
    csInfo.SetEpoch(5);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());

    //Chunkserver-2 reports copyset-1 (epoch=6, peers={1,2,3}, leader=0)
    req.Clear();
    rep.Clear();
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    BuildCopySetInfo(&csInfo, 6, 0, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsCopysetNoLeader_10) {
    //Update copyset-1 in topology (epoch=5, peers={1,2,3}, leader=0)
    hbtest_->UpdateCopysetTopo(1, 1, 5, 0,
                               std::set<ChunkServerIdType>{ 1, 2, 3 });
    //Chunkserver-2 reports copyset-1 (epoch=6, peers={1,2,3,4}, leader=1)
    ChunkServer cs(4, "testtoken", "nvme", 3, "10.198.100.3", 9001, "/");
    hbtest_->PrepareAddChunkServer(cs);
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 1, std::set<ChunkServerIdType>{ 1, 2, 3, 4 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetLeader(0);
    csInfo.SetEpoch(5);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{ 1, 2, 3 });
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());

    //Chunkserver-2 reports copyset-1 (epoch=6, peers={1,2,3,4}, leader=0)

    req.Clear();
    rep.Clear();
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    BuildCopySetInfo(&csInfo, 6, 0, std::set<ChunkServerIdType>{ 1, 2, 3, 4 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{ 1, 2, 3 });
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsCopysetNoLeader_11) {
    //Update copyset-1 in topology (epoch=5, peers={1,2,3}, leader=0)

    hbtest_->UpdateCopysetTopo(1, 1, 5, 0,
                               std::set<ChunkServerIdType>{ 1, 2, 3 });
    //Chunkserver-2 reports copyset-1 (epoch=4, peers={1,2,3}, leader=1)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 1, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetLeader(0);
    csInfo.SetEpoch(5);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());

    //Chunkserver-2 reports copyset-1 (epoch=4, peers={1,2,3}, leader=0)

    req.Clear();
    rep.Clear();
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    BuildCopySetInfo(&csInfo, 4, 0, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsCopysetNoLeader_12) {
    //Update copyset-1 in topology (epoch=5, peers={1,2,3}, leader=0)

    hbtest_->UpdateCopysetTopo(1, 1, 5, 0,
                               std::set<ChunkServerIdType>{ 1, 2, 3 });
    //Chunkserver-2 reports copyset-1 (epoch=4, peers={1,2,3,4}, leader=1)

    ChunkServer cs(4, "testtoken", "nvme", 3, "10.198.100.3", 9001, "/");
    hbtest_->PrepareAddChunkServer(cs);
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 1, std::set<ChunkServerIdType>{ 1, 2, 3, 4 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetLeader(0);
    csInfo.SetEpoch(5);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{ 1, 2, 3 });
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());

    //Chunkserver-2 reports copyset-1 (epoch=4, peers={1,2,3,4}, leader=0)

    req.Clear();
    rep.Clear();
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    BuildCopySetInfo(&csInfo, 4, 0, std::set<ChunkServerIdType>{ 1, 2, 3, 4 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{ 1, 2, 3 });
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsCopysetNoLeader_13) {
    //Update copyset-1 in topology (epoch=5, peers={1,2,3}, leader=0)

    hbtest_->UpdateCopysetTopo(1, 1, 5, 0,
                               std::set<ChunkServerIdType>{ 1, 2, 3 });
    //Chunkserver-2 reports copyset-1 (epoch=0, peers={1,2,3}, leader=1)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 1, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetLeader(0);
    csInfo.SetEpoch(5);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());

    //Chunkserver-2 reports copyset-1 (epoch=0, peers={1,2,3}, leader=0)

    req.Clear();
    rep.Clear();
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    BuildCopySetInfo(&csInfo, 0, 0, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsCopysetNoLeader_14) {
    //Update copyset-1 in topology (epoch=5, peers={1,2,3}, leader=0)

    hbtest_->UpdateCopysetTopo(1, 1, 5, 0,
                               std::set<ChunkServerIdType>{ 1, 2, 3 });
    //Chunkserver-2 reports copyset-1 (epoch=0, peers={1,2,3,4}, leader=1)

    ChunkServer cs(4, "testtoken", "nvme", 3, "10.198.100.3", 9001, "/");
    hbtest_->PrepareAddChunkServer(cs);
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 1, std::set<ChunkServerIdType>{ 1, 2, 3, 4 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetLeader(0);
    csInfo.SetEpoch(5);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{ 1, 2, 3 });
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());

    //Chunkserver-2 reports copyset-1 (epoch=0, peers={1,2,3,4}, leader=0)

    req.Clear();
    rep.Clear();
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    BuildCopySetInfo(&csInfo, 0, 0, std::set<ChunkServerIdType>{ 1, 2, 3, 4 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{ 1, 2, 3 });
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsCopysetNoLeader_15) {
    //Update copyset-1 in topology (epoch=5, peers={1,2,3}, leader=0)

    hbtest_->UpdateCopysetTopo(1, 1, 5, 0,
                               std::set<ChunkServerIdType>{ 1, 2, 3 });
    //Chunkserver-4 reports copyset-1 (epoch=5, peers={1,2,3}, leader=1)

    ChunkServer cs(4, "testtoken", "nvme", 3, "10.198.100.3", 9001, "/");
    hbtest_->PrepareAddChunkServer(cs);
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 1, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    auto conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(3, conf.peers_size());
    ASSERT_EQ("10.198.100.1:9000:0", conf.peers(0).address());
    ASSERT_EQ("10.198.100.2:9000:0", conf.peers(1).address());
    ASSERT_EQ("10.198.100.3:9000:0", conf.peers(2).address());
    ASSERT_EQ(5, conf.epoch());
    //Check copyset
    csInfo.SetLeader(0);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsCopysetNoLeader_16) {
    //Update copyset-1 in topology (epoch=5, peers={1,2,3}, leader=0)

    hbtest_->UpdateCopysetTopo(1, 1, 5, 0,
                               std::set<ChunkServerIdType>{ 1, 2, 3 });
    //Chunkserver-4 reports copyset-1 (epoch=5, peers={1,2,3}, leader=0)

    ChunkServer cs(4, "testtoken", "nvme", 3, "10.198.100.3", 9001, "/");
    hbtest_->PrepareAddChunkServer(cs);
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 0, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    //Check response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    auto conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(3, conf.peers_size());
    ASSERT_EQ("10.198.100.1:9000:0", conf.peers(0).address());
    ASSERT_EQ("10.198.100.2:9000:0", conf.peers(1).address());
    ASSERT_EQ("10.198.100.3:9000:0", conf.peers(2).address());
    ASSERT_EQ(5, conf.epoch());
    //Check copyset
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsCopysetNoLeader_17) {
    //Update copyset-1 in topology (epoch=5, peers={1,2,3}, leader=0)

    hbtest_->UpdateCopysetTopo(1, 1, 5, 0,
                               std::set<ChunkServerIdType>{ 1, 2, 3 });
    //Chunkserver-4 reports copyset-1 (epoch=6, peers={1,2,3}, leader=1)

    ChunkServer cs1(4, "testtoken", "nvme", 3, "10.198.100.3", 9001, "/");
    hbtest_->PrepareAddChunkServer(cs1);
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 1, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetLeader(0);
    csInfo.SetEpoch(5);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());

    //Chunkserver-4 reports copyset-1 (epoch=6, peers={1,2,3,5}, leader=1)

    req.Clear();
    rep.Clear();
    ChunkServer cs2(5, "testtoken", "nvme", 3, "10.198.100.3", 9002, "/");
    hbtest_->PrepareAddChunkServer(cs2);
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    BuildCopySetInfo(&csInfo, 6, 1, std::set<ChunkServerIdType>{ 1, 2, 3, 5 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetLeader(0);
    csInfo.SetEpoch(5);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{ 1, 2, 3 });
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsCopysetNoLeader_18) {
    //Update copyset-1 in topology (epoch=5, peers={1,2,3}, leader=0)

    hbtest_->UpdateCopysetTopo(1, 1, 5, 0,
                               std::set<ChunkServerIdType>{ 1, 2, 3 });
    //Chunkserver-4 reports copyset-1 (epoch=6, peers={1,2,3}, leader=0)

    ChunkServer cs1(4, "testtoken", "nvme", 3, "10.198.100.3", 9001, "/");
    hbtest_->PrepareAddChunkServer(cs1);
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 0, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());

    //Chunkserver-4 reports copyset-1 (epoch=6, peers={1,2,3,5}, leader=0)

    req.Clear();
    rep.Clear();
    ChunkServer cs2(5, "testtoken", "nvme", 3, "10.198.100.3", 9002, "/");
    hbtest_->PrepareAddChunkServer(cs2);
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    BuildCopySetInfo(&csInfo, 6, 0, std::set<ChunkServerIdType>{ 1, 2, 3, 5 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{ 1, 2, 3 });
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsCopysetNoLeader_19) {
    //Update copyset-1 in topology (epoch=5, peers={1,2,3}, leader=0)

    hbtest_->UpdateCopysetTopo(1, 1, 5, 0,
                               std::set<ChunkServerIdType>{ 1, 2, 3 });

    //Chunkserver-4 reports copyset-1 (epoch=4, peers={1,2,3}, leader=1)

    ChunkServer cs1(4, "testtoken", "nvme", 3, "10.198.100.3", 9001, "/");
    hbtest_->PrepareAddChunkServer(cs1);
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 1, std::set<ChunkServerIdType>{ 1, 2, 3 });

    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    //Check response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    auto conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(3, conf.peers_size());
    ASSERT_EQ("10.198.100.1:9000:0", conf.peers(0).address());
    ASSERT_EQ("10.198.100.2:9000:0", conf.peers(1).address());
    ASSERT_EQ("10.198.100.3:9000:0", conf.peers(2).address());
    ASSERT_EQ(5, conf.epoch());
    //Check copyset
    csInfo.SetLeader(0);
    csInfo.SetEpoch(5);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());

    //Chunkserver-4 reports copyset-1 (epoch=4, peers={1,2,3,5}, leader=1)

    req.Clear();
    rep.Clear();
    ChunkServer cs2(5, "testtoken", "nvme", 3, "10.198.100.3", 9002, "/");
    hbtest_->PrepareAddChunkServer(cs2);
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    BuildCopySetInfo(&csInfo, 4, 1, std::set<ChunkServerIdType>{ 1, 2, 3, 5 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    //Check response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(3, conf.peers_size());
    ASSERT_EQ("10.198.100.1:9000:0", conf.peers(0).address());
    ASSERT_EQ("10.198.100.2:9000:0", conf.peers(1).address());
    ASSERT_EQ("10.198.100.3:9000:0", conf.peers(2).address());
    ASSERT_EQ(5, conf.epoch());
    //Check copyset
    csInfo.SetLeader(0);
    csInfo.SetEpoch(5);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{ 1, 2, 3 });
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsCopysetNoLeader_20) {
    //Update copyset-1 in topology (epoch=5, peers={1,2,3}, leader=0)

    hbtest_->UpdateCopysetTopo(1, 1, 5, 0,
                               std::set<ChunkServerIdType>{ 1, 2, 3 });
    //Chunkserver-4 reports copyset-1 (epoch=4, peers={1,2,3}, leader=0)

    ChunkServer cs1(4, "testtoken", "nvme", 3, "10.198.100.3", 9001, "/");
    hbtest_->PrepareAddChunkServer(cs1);
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 0, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    //Check response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    auto conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(3, conf.peers_size());
    ASSERT_EQ("10.198.100.1:9000:0", conf.peers(0).address());
    ASSERT_EQ("10.198.100.2:9000:0", conf.peers(1).address());
    ASSERT_EQ("10.198.100.3:9000:0", conf.peers(2).address());
    ASSERT_EQ(5, conf.epoch());
    //Check copyset
    csInfo.SetEpoch(5);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());

    //Chunkserver-4 reports copyset-1 (epoch=4, peers={1,2,3,5}, leader=0)
    req.Clear();
    rep.Clear();
    ChunkServer cs2(5, "testtoken", "nvme", 3, "10.198.100.3", 9002, "/");
    hbtest_->PrepareAddChunkServer(cs2);
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    BuildCopySetInfo(&csInfo, 4, 0, std::set<ChunkServerIdType>{ 1, 2, 3, 5 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    //Check response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(3, conf.peers_size());
    ASSERT_EQ("10.198.100.1:9000:0", conf.peers(0).address());
    ASSERT_EQ("10.198.100.2:9000:0", conf.peers(1).address());
    ASSERT_EQ("10.198.100.3:9000:0", conf.peers(2).address());
    ASSERT_EQ(5, conf.epoch());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{ 1, 2, 3 });
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsCopysetNoLeader_21) {
    //Update copyset-1 in topology (epoch=5, peers={1,2,3}, leader=0)
    hbtest_->UpdateCopysetTopo(1, 1, 5, 0,
                               std::set<ChunkServerIdType>{ 1, 2, 3 });

    //Chunkserver-4 reports copyset-1 (epoch=0, peers={1,2,3}, leader=1)
    ChunkServer cs1(4, "testtoken", "nvme", 3, "10.198.100.3", 9001, "/");
    hbtest_->PrepareAddChunkServer(cs1);
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 1, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    //Check response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    auto conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(3, conf.peers_size());
    ASSERT_EQ("10.198.100.1:9000:0", conf.peers(0).address());
    ASSERT_EQ("10.198.100.2:9000:0", conf.peers(1).address());
    ASSERT_EQ("10.198.100.3:9000:0", conf.peers(2).address());
    ASSERT_EQ(5, conf.epoch());
    //Check copyset
    csInfo.SetLeader(0);
    csInfo.SetEpoch(5);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());

    //Chunkserver-4 reports copyset-1 (epoch=0, peers={1,2,3,5}, leader=1)
    req.Clear();
    rep.Clear();
    ChunkServer cs2(5, "testtoken", "nvme", 3, "10.198.100.3", 9002, "/");
    hbtest_->PrepareAddChunkServer(cs2);
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    BuildCopySetInfo(&csInfo, 0, 1, std::set<ChunkServerIdType>{ 1, 2, 3, 5 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    //Check response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(3, conf.peers_size());
    ASSERT_EQ("10.198.100.1:9000:0", conf.peers(0).address());
    ASSERT_EQ("10.198.100.2:9000:0", conf.peers(1).address());
    ASSERT_EQ("10.198.100.3:9000:0", conf.peers(2).address());
    ASSERT_EQ(5, conf.epoch());
    //Check copyset
    csInfo.SetLeader(0);
    csInfo.SetEpoch(5);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{ 1, 2, 3 });
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsCopysetNoLeader_22) {
    //Update copyset-1 in topology (epoch=5, peers={1,2,3}, leader=0)
    hbtest_->UpdateCopysetTopo(1, 1, 5, 0,
                               std::set<ChunkServerIdType>{ 1, 2, 3 });
    //Chunkserver-4 reports copyset-1 (epoch=0, peers={1,2,3}, leader=0)
    ChunkServer cs1(4, "testtoken", "nvme", 3, "10.198.100.3", 9001, "/");
    hbtest_->PrepareAddChunkServer(cs1);
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 0, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    //Check response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    auto conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(3, conf.peers_size());
    ASSERT_EQ("10.198.100.1:9000:0", conf.peers(0).address());
    ASSERT_EQ("10.198.100.2:9000:0", conf.peers(1).address());
    ASSERT_EQ("10.198.100.3:9000:0", conf.peers(2).address());
    ASSERT_EQ(5, conf.epoch());
    //Check copyset
    csInfo.SetEpoch(5);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());

    //Chunkserver-4 reports copyset-1 (epoch=0, peers={1,2,3,5}, leader=0)
    req.Clear();
    rep.Clear();
    ChunkServer cs2(5, "testtoken", "nvme", 3, "10.198.100.3", 9002, "/");
    hbtest_->PrepareAddChunkServer(cs2);
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    BuildCopySetInfo(&csInfo, 0, 0, std::set<ChunkServerIdType>{ 1, 2, 3, 5 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    //Check response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(3, conf.peers_size());
    ASSERT_EQ("10.198.100.1:9000:0", conf.peers(0).address());
    ASSERT_EQ("10.198.100.2:9000:0", conf.peers(1).address());
    ASSERT_EQ("10.198.100.3:9000:0", conf.peers(2).address());
    ASSERT_EQ(5, conf.epoch());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{ 1, 2, 3 });
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOp_1) {
    PrepareMdsWithChangeOp();

    //Chunkserver-1 reports copyset-1 (epoch=5, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(1, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 1, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    //Check response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    auto conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(3, conf.peers_size());
    ASSERT_EQ("10.198.100.1:9000:0", conf.peers(0).address());
    ASSERT_EQ("10.198.100.2:9000:0", conf.peers(1).address());
    ASSERT_EQ("10.198.100.3:9000:0", conf.peers(2).address());
    ASSERT_EQ(5, conf.epoch());
    ASSERT_EQ(ConfigChangeType::CHANGE_PEER, conf.type());
    ASSERT_EQ("10.198.100.3:9001:0", conf.configchangeitem().address());
    ASSERT_EQ("10.198.100.3:9000:0", conf.oldpeer().address());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(3, step->GetOldPeer());
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOp_2) {
    PrepareMdsWithChangeOp();

    //Chunkserver-1 reports copyset-1 (epoch=5, peers={1,2,3}, leader=1)

    //                  configChangeInfo={peer: 4, type: CHANGE_PEER})
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(1, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 1, std::set<ChunkServerIdType>{ 1, 2, 3 }, 4);
    hbtest_->AddCopySetToRequest(&req, csInfo, ConfigChangeType::CHANGE_PEER);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOp_3) {
    PrepareMdsWithChangeOp();

    //Chunkserver-1 reports copyset-1 (epoch=6, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(1, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 1, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOp_4) {
    PrepareMdsWithChangeOp();

    //Chunkserver-2 reports copyset-1 (epoch=6, peers={1,2,3}, leader=2)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 2, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOp_5) {
    PrepareMdsWithChangeOp();
    ChunkServer cs(5, "testtoken", "nvme", 3, "10.198.100.3", 9002, "/");
    hbtest_->PrepareAddChunkServer(cs);

    //Chunkserver-1 reports copyset-1 (epoch=6, peers={1,2,5}, leader=1)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(1, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 1, std::set<ChunkServerIdType>{ 1, 2, 5 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOp_6) {
    PrepareMdsWithChangeOp();

    //Chunkserver-1 reports copyset-1 (epoch=6, peers={1,2,4}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(1, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 1, std::set<ChunkServerIdType>{ 1, 2, 4 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOp_7) {
    PrepareMdsWithChangeOp();

    //Chunkserver-2 reports copyset-1 (epoch=7, peers={1,2,4}, leader=2)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 7, 2, std::set<ChunkServerIdType>{ 1, 2, 4 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOp_8) {
    PrepareMdsWithChangeOp();

    //Chunkserver-4 reports copyset-1 (epoch=7, peers={1,2,4}, leader=4)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 7, 4, std::set<ChunkServerIdType>{ 1, 2, 4 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOp_9) {
    PrepareMdsWithChangeOp();

    //Chunkserver-2 reports copyset-1 (epoch=5, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 1, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOp_10) {
    PrepareMdsWithChangeOp();

    //Chunkserver-2 reports copyset-1 (epoch=5, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 0, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetLeader(1);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOp_11) {
    PrepareMdsWithChangeOp();

    //Chunkserver-2 reports copyset-1 (epoch=6, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 1, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOp_12) {
    PrepareMdsWithChangeOp();

    //Chunkserver-2 reports copyset-1 (epoch=6, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 0, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetLeader(1);
    csInfo.SetEpoch(5);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOp_13) {
    PrepareMdsWithChangeOp();

    //Chunkserver-2 reports copyset-1 (epoch=6, peers={1,2,4}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 1, std::set<ChunkServerIdType>{ 1, 2, 4 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{ 1, 2, 3 });
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOp_14) {
    PrepareMdsWithChangeOp();

    //Chunkserver-2 reports copyset-1 (epoch=6, peers={1,2,4}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 0, std::set<ChunkServerIdType>{ 1, 2, 4 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetLeader(1);
    csInfo.SetEpoch(5);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{ 1, 2, 3 });
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOp_15) {
    PrepareMdsWithChangeOp();
    ChunkServer cs(5, "testtoken", "nvme", 3, "10.198.100.3", 9002, "/");
    hbtest_->PrepareAddChunkServer(cs);

    //Chunkserver-2 reports copyset-1 (epoch=6, peers={1,2,5}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 1, std::set<ChunkServerIdType>{ 1, 2, 5 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{ 1, 2, 3 });
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOp_16) {
    PrepareMdsWithChangeOp();
    ChunkServer cs(5, "testtoken", "nvme", 3, "10.198.100.3", 9002, "/");
    hbtest_->PrepareAddChunkServer(cs);

    //Chunkserver-2 reports copyset-1 (epoch=6, peers={1,2,5}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 0, std::set<ChunkServerIdType>{ 1, 2, 5 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetLeader(1);
    csInfo.SetEpoch(5);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{ 1, 2, 3 });
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOp_17) {
    PrepareMdsWithChangeOp();

    //Chunkserver-2 reports copyset-1 (epoch=4, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 1, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOp_18) {
    PrepareMdsWithChangeOp();

    //Chunkserver-2 reports copyset-1 (epoch=4, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 0, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetLeader(1);
    csInfo.SetEpoch(5);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOp_19) {
    PrepareMdsWithChangeOp();
    ChunkServer cs(5, "testtoken", "nvme", 3, "10.198.100.3", 9002, "/");
    hbtest_->PrepareAddChunkServer(cs);

    //Chunkserver-2 reports copyset-1 (epoch=4, peers={1,2,5}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 1, std::set<ChunkServerIdType>{ 1, 2, 5 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{ 1, 2, 3 });
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOp_20) {
    PrepareMdsWithChangeOp();
    ChunkServer cs(5, "testtoken", "nvme", 3, "10.198.100.3", 9002, "/");
    hbtest_->PrepareAddChunkServer(cs);

    //Chunkserver-2 reports copyset-1 (epoch=4, peers={1,2,5}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 0, std::set<ChunkServerIdType>{ 1, 2, 5 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetLeader(1);
    csInfo.SetEpoch(5);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{ 1, 2, 3 });
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOp_21) {
    PrepareMdsWithChangeOp();

    //Chunkserver-2 reports copyset-1 (epoch=0, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 1, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOp_22) {
    PrepareMdsWithChangeOp();

    //Chunkserver-2 reports copyset-1 (epoch=0, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 0, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetLeader(1);
    csInfo.SetEpoch(5);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOp_23) {
    PrepareMdsWithChangeOp();
    ChunkServer cs(5, "testtoken", "nvme", 3, "10.198.100.3", 9002, "/");
    hbtest_->PrepareAddChunkServer(cs);

    //Chunkserver-2 reports copyset-1 (epoch=0, peers={1,2,5}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 1, std::set<ChunkServerIdType>{ 1, 2, 5 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{ 1, 2, 3 });
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOp_24) {
    PrepareMdsWithChangeOp();
    ChunkServer cs(5, "testtoken", "nvme", 3, "10.198.100.3", 9002, "/");
    hbtest_->PrepareAddChunkServer(cs);

    //Chunkserver-2 reports copyset-1 (epoch=0, peers={1,2,5}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 0, std::set<ChunkServerIdType>{ 1, 2, 5 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetLeader(1);
    csInfo.SetEpoch(5);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{ 1, 2, 3 });
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOp_25) {
    PrepareMdsWithChangeOp();

    //Chunkserver-4 reports copyset-1 (epoch=5, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 1, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOp_26) {
    PrepareMdsWithChangeOp();

    //Chunkserver-4 reports copyset-1 (epoch=5, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 0, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetLeader(1);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOp_27) {
    PrepareMdsWithChangeOp();

    //Chunkserver-4 reports copyset-1 (epoch=6, peers={1,2,4}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 1, std::set<ChunkServerIdType>{ 1, 2, 4 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{ 1, 2, 3 });
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOp_28) {
    PrepareMdsWithChangeOp();

    //Chunkserver-4 reports copyset-1 (epoch=6, peers={1,2,4}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 0, std::set<ChunkServerIdType>{ 1, 2, 4 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{ 1, 2, 3 });
    csInfo.SetLeader(1);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOp_29) {
    PrepareMdsWithChangeOp();

    //Chunkserver-4 reports copyset-1 (epoch=4, peers={1,2,3}, leader=1)

    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 1, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOp_30) {
    PrepareMdsWithChangeOp();

    //Chunkserver-4 reports copyset-1 (epoch=4, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 0, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOp_31) {
    PrepareMdsWithChangeOp();
    ChunkServer cs(5, "testtoken", "nvme", 3, "10.198.100.3", 9002, "/");
    hbtest_->PrepareAddChunkServer(cs);

    //Chunkserver-4 reports copyset-1 (epoch=4, peers={1,2,5}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 1, std::set<ChunkServerIdType>{ 1, 2, 5 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{ 1, 2, 3 });
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOp_32) {
    PrepareMdsWithChangeOp();
    ChunkServer cs(5, "testtoken", "nvme", 3, "10.198.100.3", 9002, "/");
    hbtest_->PrepareAddChunkServer(cs);

    //Chunkserver-4 reports copyset-1 (epoch=4, peers={1,2,5}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 0, std::set<ChunkServerIdType>{ 1, 2, 5 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{ 1, 2, 3 });
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOp_33) {
    PrepareMdsWithChangeOp();

    //Chunkserver-4 reports copyset-1 (epoch=0, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 1, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOp_34) {
    PrepareMdsWithChangeOp();

    //Chunkserver-4 reports copyset-1 (epoch=0, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 0, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOp_35) {
    PrepareMdsWithChangeOp();
    ChunkServer cs(5, "testtoken", "nvme", 3, "10.198.100.3", 9002, "/");
    hbtest_->PrepareAddChunkServer(cs);

    //Chunkserver-4 reports copyset-1 (epoch=0, peers={1,2,5}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 1, std::set<ChunkServerIdType>{ 1, 2, 5 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{ 1, 2, 3 });
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOp_36) {
    PrepareMdsWithChangeOp();
    ChunkServer cs(5, "testtoken", "nvme", 3, "10.198.100.3", 9002, "/");
    hbtest_->PrepareAddChunkServer(cs);

    //Chunkserver-4 reports copyset-1 (epoch=0, peers={1,2,5}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 0, std::set<ChunkServerIdType>{ 1, 2, 5 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{ 1, 2, 3 });
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOp_37) {
    PrepareMdsWithChangeOp();
    ChunkServer cs(5, "testtoken", "nvme", 3, "10.198.100.3", 9002, "/");
    hbtest_->PrepareAddChunkServer(cs);

    //Chunkserver-5 reports copyset-1 (epoch=0, peers={1,2,5}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(5, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 0, std::set<ChunkServerIdType>{ 1, 2, 5 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    CopySetConf conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(3, conf.peers_size());
    ASSERT_EQ("10.198.100.1:9000:0", conf.peers(0).address());
    ASSERT_EQ("10.198.100.2:9000:0", conf.peers(1).address());
    ASSERT_EQ("10.198.100.3:9000:0", conf.peers(2).address());
    ASSERT_EQ(5, conf.epoch());
    //Check copyset
    csInfo.SetLeader(1);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{ 1, 2, 3 });
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOp_38) {
    PrepareMdsWithChangeOp();
    ChunkServer cs(5, "testtoken", "nvme", 3, "10.198.100.3", 9002, "/");
    hbtest_->PrepareAddChunkServer(cs);

    //Chunkserver-5 reports copyset-1 (epoch=4, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(5, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 1, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    CopySetConf conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(3, conf.peers_size());
    ASSERT_EQ("10.198.100.1:9000:0", conf.peers(0).address());
    ASSERT_EQ("10.198.100.2:9000:0", conf.peers(1).address());
    ASSERT_EQ("10.198.100.3:9000:0", conf.peers(2).address());
    ASSERT_EQ(5, conf.epoch());
    //Check copyset
    csInfo.SetEpoch(5);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());

    ChunkServer cs2(6, "testtoken", "nvme", 3, "10.198.100.3", 9003, "/");
    hbtest_->PrepareAddChunkServer(cs2);
    req.Clear();
    rep.Clear();
    //Chunkserver-5 reports copyset-1 (epoch=4, peers={1,2,6}, leader=1)
    hbtest_->BuildBasicChunkServerRequest(5, &req);
    BuildCopySetInfo(&csInfo, 4, 1, std::set<ChunkServerIdType>{ 1, 2, 6 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(3, conf.peers_size());
    ASSERT_EQ("10.198.100.1:9000:0", conf.peers(0).address());
    ASSERT_EQ("10.198.100.2:9000:0", conf.peers(1).address());
    ASSERT_EQ("10.198.100.3:9000:0", conf.peers(2).address());
    ASSERT_EQ(5, conf.epoch());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{ 1, 2, 3 });
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOp_39) {
    PrepareMdsWithChangeOp();
    ChunkServer cs(5, "testtoken", "nvme", 3, "10.198.100.3", 9002, "/");
    hbtest_->PrepareAddChunkServer(cs);

    //Chunkserver-5 reports copyset-1 (epoch=4, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(5, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 0, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    CopySetConf conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(3, conf.peers_size());
    ASSERT_EQ("10.198.100.1:9000:0", conf.peers(0).address());
    ASSERT_EQ("10.198.100.2:9000:0", conf.peers(1).address());
    ASSERT_EQ("10.198.100.3:9000:0", conf.peers(2).address());
    ASSERT_EQ(5, conf.epoch());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());

    ChunkServer cs2(6, "testtoken", "nvme", 3, "10.198.100.3", 9003, "/");
    hbtest_->PrepareAddChunkServer(cs2);
    //Chunkserver-5 reports copyset-1 (epoch=4, peers={1,2,6}, leader=0)
    req.Clear();
    rep.Clear();
    hbtest_->BuildBasicChunkServerRequest(5, &req);
    BuildCopySetInfo(&csInfo, 4, 0, std::set<ChunkServerIdType>{ 1, 2, 6 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(3, conf.peers_size());
    ASSERT_EQ("10.198.100.1:9000:0", conf.peers(0).address());
    ASSERT_EQ("10.198.100.2:9000:0", conf.peers(1).address());
    ASSERT_EQ("10.198.100.3:9000:0", conf.peers(2).address());
    ASSERT_EQ(5, conf.epoch());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{ 1, 2, 3 });
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOp_40) {
    PrepareMdsWithChangeOp();
    ChunkServer cs(5, "testtoken", "nvme", 3, "10.198.100.3", 9002, "/");
    hbtest_->PrepareAddChunkServer(cs);

    //Chunkserver-5 reports copyset-1 (epoch=0, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(5, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 0, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    CopySetConf conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(3, conf.peers_size());
    ASSERT_EQ("10.198.100.1:9000:0", conf.peers(0).address());
    ASSERT_EQ("10.198.100.2:9000:0", conf.peers(1).address());
    ASSERT_EQ("10.198.100.3:9000:0", conf.peers(2).address());
    ASSERT_EQ(5, conf.epoch());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());

    //Chunkserver-5 reports copyset-1 (epoch=0, peers={1,2,4}, leader=0)
    req.Clear();
    rep.Clear();
    hbtest_->BuildBasicChunkServerRequest(5, &req);
    BuildCopySetInfo(&csInfo, 0, 0, std::set<ChunkServerIdType>{ 1, 2, 4 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(3, conf.peers_size());
    ASSERT_EQ("10.198.100.1:9000:0", conf.peers(0).address());
    ASSERT_EQ("10.198.100.2:9000:0", conf.peers(1).address());
    ASSERT_EQ("10.198.100.3:9000:0", conf.peers(2).address());
    ASSERT_EQ(5, conf.epoch());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{ 1, 2, 3 });
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOpOnGoing_1) {
    PrepareMdsWithChangeOpOnGoing();

    //Chunkserver-1 reports copyset-1 (epoch=5, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(1, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 1, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    //Check response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    auto conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(3, conf.peers_size());
    ASSERT_EQ("10.198.100.1:9000:0", conf.peers(0).address());
    ASSERT_EQ("10.198.100.2:9000:0", conf.peers(1).address());
    ASSERT_EQ("10.198.100.3:9000:0", conf.peers(2).address());
    ASSERT_EQ(5, conf.epoch());
    ASSERT_EQ(ConfigChangeType::CHANGE_PEER, conf.type());
    ASSERT_EQ("10.198.100.3:9001:0", conf.configchangeitem().address());
    ASSERT_EQ("10.198.100.3:9000:0", conf.oldpeer().address());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(3, step->GetOldPeer());
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOpOnGoing_2) {
    PrepareMdsWithChangeOpOnGoing();

    //Chunkserver-1 reports copyset-1 (epoch=5, peers={1,2,3}, leader=1)
    //                  configChangeInfo={peer: 4, type: CHANGE_PEER})
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(1, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 1, std::set<ChunkServerIdType>{ 1, 2, 3 }, 4);
    hbtest_->AddCopySetToRequest(&req, csInfo, ConfigChangeType::CHANGE_PEER);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOpOnGoing_3) {
    PrepareMdsWithChangeOpOnGoing();

    //Chunkserver-1 reports copyset-1 (epoch=6, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(1, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 1, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOpOnGoing_4) {
    PrepareMdsWithChangeOpOnGoing();

    //Chunkserver-2 reports copyset-1 (epoch=6, peers={1,2,3}, leader=2)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 2, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOpOnGoing_5) {
    PrepareMdsWithChangeOpOnGoing();
    ChunkServer cs(5, "testtoken", "nvme", 3, "10.198.100.3", 9002, "/");
    hbtest_->PrepareAddChunkServer(cs);

    //Chunkserver-1 reports copyset-1 (epoch=6, peers={1,2,5}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(1, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 1, std::set<ChunkServerIdType>{ 1, 2, 5 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOpOnGoing_6) {
    PrepareMdsWithChangeOpOnGoing();

    //Chunkserver-1 reports copyset-1 (epoch=6, peers={1,2,4}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(1, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 1, std::set<ChunkServerIdType>{ 1, 2, 4 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOpOnGoing_7) {
    PrepareMdsWithChangeOpOnGoing();

    //Chunkserver-2 reports copyset-1 (epoch=7, peers={1,2,4}, leader=2)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 7, 2, std::set<ChunkServerIdType>{ 1, 2, 4 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOpOnGoing_8) {
    PrepareMdsWithChangeOpOnGoing();

    //Chunkserver-4 reports copyset-1 (epoch=7, peers={1,2,4}, leader=4)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 7, 4, std::set<ChunkServerIdType>{ 1, 2, 4 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOpOnGoing_9) {
    PrepareMdsWithChangeOpOnGoing();

    //Chunkserver-2 reports copyset-1 (epoch=5, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 1, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOpOnGoing_10) {
    PrepareMdsWithChangeOpOnGoing();

    //Chunkserver-2 reports copyset-1 (epoch=5, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 0, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetLeader(1);
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOpOnGoing_11) {
    PrepareMdsWithChangeOpOnGoing();

    //Chunkserver-2 reports copyset-1 (epoch=6, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 1, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOpOnGoing_12) {
    PrepareMdsWithChangeOpOnGoing();

    //Chunkserver-2 reports copyset-1 (epoch=6, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 0, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetLeader(1);
    csInfo.SetEpoch(5);
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOpOnGoing_13) {
    PrepareMdsWithChangeOpOnGoing();

    //Chunkserver-2 reports copyset-1 (epoch=6, peers={1,2,4}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 1, std::set<ChunkServerIdType>{ 1, 2, 4 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetCandidate(4);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{ 1, 2, 3 });
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOpOnGoing_14) {
    PrepareMdsWithChangeOpOnGoing();

    //Chunkserver-2 reports copyset-1 (epoch=6, peers={1,2,4}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 0, std::set<ChunkServerIdType>{ 1, 2, 4 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetLeader(1);
    csInfo.SetEpoch(5);
    csInfo.SetCandidate(4);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{ 1, 2, 3 });
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOpOnGoing_15) {
    PrepareMdsWithChangeOpOnGoing();
    ChunkServer cs(5, "testtoken", "nvme", 3, "10.198.100.3", 9002, "/");
    hbtest_->PrepareAddChunkServer(cs);

    //Chunkserver-2 reports copyset-1 (epoch=6, peers={1,2,5}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 1, std::set<ChunkServerIdType>{ 1, 2, 5 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetCandidate(4);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{ 1, 2, 3 });
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOpOnGoing_16) {
    PrepareMdsWithChangeOpOnGoing();
    ChunkServer cs(5, "testtoken", "nvme", 3, "10.198.100.3", 9002, "/");
    hbtest_->PrepareAddChunkServer(cs);

    //Chunkserver-2 reports copyset-1 (epoch=6, peers={1,2,5}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 0, std::set<ChunkServerIdType>{ 1, 2, 5 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetLeader(1);
    csInfo.SetEpoch(5);
    csInfo.SetCandidate(4);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{ 1, 2, 3 });
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOpOnGoing_17) {
    PrepareMdsWithChangeOpOnGoing();

    //Chunkserver-2 reports copyset-1 (epoch=4, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 1, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOpOnGoing_18) {
    PrepareMdsWithChangeOpOnGoing();

    //Chunkserver-2 reports copyset-1 (epoch=4, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 0, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetLeader(1);
    csInfo.SetEpoch(5);
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOpOnGoing_19) {
    PrepareMdsWithChangeOpOnGoing();
    ChunkServer cs(5, "testtoken", "nvme", 3, "10.198.100.3", 9002, "/");
    hbtest_->PrepareAddChunkServer(cs);

    //Chunkserver-2 reports copyset-1 (epoch=4, peers={1,2,5}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 1, std::set<ChunkServerIdType>{ 1, 2, 5 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetCandidate(4);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{ 1, 2, 3 });
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOpOnGoing_20) {
    PrepareMdsWithChangeOpOnGoing();
    ChunkServer cs(5, "testtoken", "nvme", 3, "10.198.100.3", 9002, "/");
    hbtest_->PrepareAddChunkServer(cs);

    //Chunkserver-2 reports copyset-1 (epoch=4, peers={1,2,5}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 0, std::set<ChunkServerIdType>{ 1, 2, 5 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetLeader(1);
    csInfo.SetEpoch(5);
    csInfo.SetCandidate(4);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{ 1, 2, 3 });
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOpOnGoing_21) {
    PrepareMdsWithChangeOpOnGoing();

    //Chunkserver-2 reports copyset-1 (epoch=0, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 1, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOpOnGoing_22) {
    PrepareMdsWithChangeOpOnGoing();

    //Chunkserver-2 reports copyset-1 (epoch=0, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 0, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetLeader(1);
    csInfo.SetEpoch(5);
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOpOnGoing_23) {
    PrepareMdsWithChangeOpOnGoing();
    ChunkServer cs(5, "testtoken", "nvme", 3, "10.198.100.3", 9002, "/");
    hbtest_->PrepareAddChunkServer(cs);

    //Chunkserver-2 reports copyset-1 (epoch=0, peers={1,2,5}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 1, std::set<ChunkServerIdType>{ 1, 2, 5 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetCandidate(4);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{ 1, 2, 3 });
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOpOnGoing_24) {
    PrepareMdsWithChangeOpOnGoing();
    ChunkServer cs(5, "testtoken", "nvme", 3, "10.198.100.3", 9002, "/");
    hbtest_->PrepareAddChunkServer(cs);

    //Chunkserver-2 reports copyset-1 (epoch=0, peers={1,2,5}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 0, std::set<ChunkServerIdType>{ 1, 2, 5 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetLeader(1);
    csInfo.SetEpoch(5);
    csInfo.SetCandidate(4);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{ 1, 2, 3 });
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOpOnGoing_25) {
    PrepareMdsWithChangeOpOnGoing();

    //Chunkserver-4 reports copyset-1 (epoch=5, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 1, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOpOnGoing_26) {
    PrepareMdsWithChangeOpOnGoing();

    //Chunkserver-4 reports copyset-1 (epoch=5, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 0, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetLeader(1);
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOpOnGoing_27) {
    PrepareMdsWithChangeOpOnGoing();

    //Chunkserver-4 reports copyset-1 (epoch=6, peers={1,2,4}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 1, std::set<ChunkServerIdType>{ 1, 2, 4 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetCandidate(4);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{ 1, 2, 3 });
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOpOnGoing_28) {
    PrepareMdsWithChangeOpOnGoing();

    //Chunkserver-4 reports copyset-1 (epoch=6, peers={1,2,4}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 0, std::set<ChunkServerIdType>{ 1, 2, 4 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetCandidate(4);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{ 1, 2, 3 });
    csInfo.SetLeader(1);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOpOnGoing_29) {
    PrepareMdsWithChangeOpOnGoing();

    //Chunkserver-4 reports copyset-1 (epoch=4, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 1, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOpOnGoing_30) {
    PrepareMdsWithChangeOpOnGoing();

    //Chunkserver-4 reports copyset-1 (epoch=4, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 0, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOpOnGoing_31) {
    PrepareMdsWithChangeOpOnGoing();
    ChunkServer cs(5, "testtoken", "nvme", 3, "10.198.100.3", 9002, "/");
    hbtest_->PrepareAddChunkServer(cs);

    //Chunkserver-4 reports copyset-1 (epoch=4, peers={1,2,5}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 1, std::set<ChunkServerIdType>{ 1, 2, 5 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetCandidate(4);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{ 1, 2, 3 });
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOpOnGoing_32) {
    PrepareMdsWithChangeOpOnGoing();
    ChunkServer cs(5, "testtoken", "nvme", 3, "10.198.100.3", 9002, "/");
    hbtest_->PrepareAddChunkServer(cs);

    //Chunkserver-4 reports copyset-1 (epoch=4, peers={1,2,5}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 0, std::set<ChunkServerIdType>{ 1, 2, 5 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCandidate(4);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{ 1, 2, 3 });
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOpOnGoing_33) {
    PrepareMdsWithChangeOpOnGoing();

    //Chunkserver-4 reports copyset-1 (epoch=0, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 1, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOpOnGoing_34) {
    PrepareMdsWithChangeOpOnGoing();

    //Chunkserver-4 reports copyset-1 (epoch=0, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 0, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOpOnGoing_35) {
    PrepareMdsWithChangeOpOnGoing();
    ChunkServer cs(5, "testtoken", "nvme", 3, "10.198.100.3", 9002, "/");
    hbtest_->PrepareAddChunkServer(cs);

    //Chunkserver-4 reports copyset-1 (epoch=0, peers={1,2,5}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 1, std::set<ChunkServerIdType>{ 1, 2, 5 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetCandidate(4);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{ 1, 2, 3 });
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOpOnGoing_36) {
    PrepareMdsWithChangeOpOnGoing();
    ChunkServer cs(5, "testtoken", "nvme", 3, "10.198.100.3", 9002, "/");
    hbtest_->PrepareAddChunkServer(cs);

    //Chunkserver-4 reports copyset-1 (epoch=0, peers={1,2,5}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 0, std::set<ChunkServerIdType>{ 1, 2, 5 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCandidate(4);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{ 1, 2, 3 });
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOpOnGoing_37) {
    PrepareMdsWithChangeOpOnGoing();
    ChunkServer cs(5, "testtoken", "nvme", 3, "10.198.100.3", 9002, "/");
    hbtest_->PrepareAddChunkServer(cs);

    //Chunkserver-5 reports copyset-1 (epoch=0, peers={1,2,5}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(5, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 0, std::set<ChunkServerIdType>{ 1, 2, 5 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    CopySetConf conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(3, conf.peers_size());
    ASSERT_EQ("10.198.100.1:9000:0", conf.peers(0).address());
    ASSERT_EQ("10.198.100.2:9000:0", conf.peers(1).address());
    ASSERT_EQ("10.198.100.3:9000:0", conf.peers(2).address());
    ASSERT_EQ(5, conf.epoch());
    //Check copyset
    csInfo.SetLeader(1);
    csInfo.SetCandidate(4);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{ 1, 2, 3 });
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOpOnGoing_38) {
    PrepareMdsWithChangeOpOnGoing();
    ChunkServer cs(5, "testtoken", "nvme", 3, "10.198.100.3", 9002, "/");
    hbtest_->PrepareAddChunkServer(cs);

    //Chunkserver-5 reports copyset-1 (epoch=4, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(5, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 0, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    CopySetConf conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(3, conf.peers_size());
    ASSERT_EQ("10.198.100.1:9000:0", conf.peers(0).address());
    ASSERT_EQ("10.198.100.2:9000:0", conf.peers(1).address());
    ASSERT_EQ("10.198.100.3:9000:0", conf.peers(2).address());
    ASSERT_EQ(5, conf.epoch());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());

    ChunkServer cs2(6, "testtoken", "nvme", 3, "10.198.100.3", 9003, "/");
    hbtest_->PrepareAddChunkServer(cs2);
    req.Clear();
    rep.Clear();
    //Chunkserver-5 reports copyset-1 (epoch=4, peers={1,2,6}, leader=0)
    hbtest_->BuildBasicChunkServerRequest(5, &req);
    BuildCopySetInfo(&csInfo, 4, 0, std::set<ChunkServerIdType>{ 1, 2, 6 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(3, conf.peers_size());
    ASSERT_EQ("10.198.100.1:9000:0", conf.peers(0).address());
    ASSERT_EQ("10.198.100.2:9000:0", conf.peers(1).address());
    ASSERT_EQ("10.198.100.3:9000:0", conf.peers(2).address());
    ASSERT_EQ(5, conf.epoch());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCandidate(4);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{ 1, 2, 3 });
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOpOnGoing_39) {
    PrepareMdsWithChangeOpOnGoing();
    ChunkServer cs(5, "testtoken", "nvme", 3, "10.198.100.3", 9002, "/");
    hbtest_->PrepareAddChunkServer(cs);

    //Chunkserver-5 reports copyset-1 (epoch=4, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(5, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 1, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    CopySetConf conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(3, conf.peers_size());
    ASSERT_EQ("10.198.100.1:9000:0", conf.peers(0).address());
    ASSERT_EQ("10.198.100.2:9000:0", conf.peers(1).address());
    ASSERT_EQ("10.198.100.3:9000:0", conf.peers(2).address());
    ASSERT_EQ(5, conf.epoch());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());

    ChunkServer cs2(6, "testtoken", "nvme", 3, "10.198.100.3", 9003, "/");
    hbtest_->PrepareAddChunkServer(cs2);
    //Chunkserver-5 reports copyset-1 (epoch=4, peers={1,2,6}, leader=1)
    req.Clear();
    rep.Clear();
    hbtest_->BuildBasicChunkServerRequest(5, &req);
    BuildCopySetInfo(&csInfo, 4, 1, std::set<ChunkServerIdType>{ 1, 2, 6 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(3, conf.peers_size());
    ASSERT_EQ("10.198.100.1:9000:0", conf.peers(0).address());
    ASSERT_EQ("10.198.100.2:9000:0", conf.peers(1).address());
    ASSERT_EQ("10.198.100.3:9000:0", conf.peers(2).address());
    ASSERT_EQ(5, conf.epoch());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetCandidate(4);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{ 1, 2, 3 });
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOpOnGoing_40) {
    PrepareMdsWithChangeOpOnGoing();
    ChunkServer cs(5, "testtoken", "nvme", 3, "10.198.100.3", 9002, "/");
    hbtest_->PrepareAddChunkServer(cs);

    //Chunkserver-5 reports copyset-1 (epoch=0, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(5, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 0, std::set<ChunkServerIdType>{ 1, 2, 3 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    CopySetConf conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(3, conf.peers_size());
    ASSERT_EQ("10.198.100.1:9000:0", conf.peers(0).address());
    ASSERT_EQ("10.198.100.2:9000:0", conf.peers(1).address());
    ASSERT_EQ("10.198.100.3:9000:0", conf.peers(2).address());
    ASSERT_EQ(5, conf.epoch());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());

    //Chunkserver-5 reports copyset-1 (epoch=0, peers={1,2,4}, leader=0)
    req.Clear();
    rep.Clear();
    hbtest_->BuildBasicChunkServerRequest(5, &req);
    BuildCopySetInfo(&csInfo, 0, 0, std::set<ChunkServerIdType>{ 1, 2, 4 });
    hbtest_->AddCopySetToRequest(&req, csInfo);
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    //Check response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(3, conf.peers_size());
    ASSERT_EQ("10.198.100.1:9000:0", conf.peers(0).address());
    ASSERT_EQ("10.198.100.2:9000:0", conf.peers(1).address());
    ASSERT_EQ("10.198.100.3:9000:0", conf.peers(2).address());
    ASSERT_EQ(5, conf.epoch());
    //Check copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCandidate(4);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{ 1, 2, 3 });
    ASSERT_TRUE(ValidateCopySet(csInfo));
    //Check op in scheduler
    ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

}  // namespace mds
}  // namespace curve
