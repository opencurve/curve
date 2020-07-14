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

namespace curve {
namespace mds {
class HeartbeatBasicTest : public ::testing::Test {
 protected:
    void InitConfiguration(Configuration *conf) {
        // db相关配置设置
        conf->SetStringValue("mds.DbName", "heartbeat_basic_test_mds");
        conf->SetStringValue("mds.DbUser", "root");
        conf->SetStringValue("mds.DbUrl", "localhost");
        conf->SetStringValue("mds.DbPassword", "qwer");
        conf->SetIntValue("mds.DbPoolSize", 16);
        conf->SetIntValue("mds.topology.ChunkServerStateUpdateSec", 0);

        // heartbeat相关配置设置
        conf->SetIntValue("mds.heartbeat.intervalMs", 100);
        conf->SetIntValue("mds.heartbeat.misstimeoutMs", 300);
        conf->SetIntValue("mds.heartbeat.offlinetimeoutMs", 500);
        conf->SetIntValue("mds.heartbeat.clean_follower_afterMs", 0);

        // mds监听端口号
        conf->SetStringValue("mds.listen.addr", "127.0.0.1:6879");

        // scheduler相关的内容
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
        // 构造mds中copyset当前状
        ChunkServer cs(10, "testtoekn", "nvme", 3, "10.198.100.3", 9001, "/");
        hbtest_->PrepareAddChunkServer(cs);
        hbtest_->UpdateCopysetTopo(
            1, 1, 5, 1, std::set<ChunkServerIDType>{1, 2, 3}, 10);

        // 构造scheduler当前的状态
        Operator op(5, CopySetKey{1, 1}, OperatorPriority::NormalPriority,
            std::chrono::steady_clock::now(), std::make_shared<AddPeer>(10));
        op.timeLimit = std::chrono::seconds(3);
        hbtest_->AddOperatorToOpController(op);
    }

    void PrepareMdsNoCnandidateOpOnGoing() {
        // 构造mds中copyset当前状态
        // copyset-1(epoch=5, peers={1,2,3}, leader=1);
        ChunkServer cs(10, "testtoekn", "nvme", 3, "10.198.100.3", 9001, "/");
        hbtest_->PrepareAddChunkServer(cs);
        hbtest_->UpdateCopysetTopo(
            1, 1, 5, 1, std::set<ChunkServerIDType>{1, 2, 3});

        // 构造scheduler当前的状态
        Operator op(5, CopySetKey{1, 1}, OperatorPriority::NormalPriority,
            std::chrono::steady_clock::now(), std::make_shared<AddPeer>(10));
        op.timeLimit = std::chrono::seconds(3);
        hbtest_->AddOperatorToOpController(op);
    }

    void PrepareMdsWithRemoveOp() {
        // mds存在copyset-1(epoch=5, peers={1,2,3,4}, leader=1);
        // scheduler中copyset-1有operator: startEpoch=5, step=RemovePeer<4>
        ChunkServer cs(4, "testtoken", "nvme", 3, "10.198.100.3", 9001, "/");
        hbtest_->PrepareAddChunkServer(cs);
        hbtest_->UpdateCopysetTopo(
            1, 1, 5, 1, std::set<ChunkServerIdType>{1, 2, 3, 4});

        Operator op(5, CopySetKey{1, 1}, OperatorPriority::NormalPriority,
            std::chrono::steady_clock::now(), std::make_shared<RemovePeer>(4));
        op.timeLimit = std::chrono::seconds(3);
        hbtest_->AddOperatorToOpController(op);
    }

    void PrepareMdsWithRemoveOpOnGoing() {
        // mds存在copyset-1(epoch=5, peers={1,2,3,4}, leader=1, , candidate=4);
        // scheduler中copyset-1有operator: startEpoch=5, step=RemovePeer<4>
        ChunkServer cs(4, "testtoken", "nvme", 3, "10.198.100.3", 9001, "/");
        hbtest_->PrepareAddChunkServer(cs);
        hbtest_->UpdateCopysetTopo(
            1, 1, 5, 1, std::set<ChunkServerIdType>{1, 2, 3, 4}, 4);

        Operator op(5, CopySetKey{1, 1}, OperatorPriority::NormalPriority,
            std::chrono::steady_clock::now(), std::make_shared<RemovePeer>(4));
        op.timeLimit = std::chrono::seconds(3);
        hbtest_->AddOperatorToOpController(op);
    }

    void PrepareMdsWithTransferOp() {
        // mds存在copyset-1(epoch=5, peers={1,2,3}, leader=1);
        // scheduler中copyset-1有operator:startEpoch=5,step=TransferLeader{1>2}
        hbtest_->UpdateCopysetTopo(
            1, 1, 5, 1, std::set<ChunkServerIdType>{1, 2, 3});

        Operator op(5, CopySetKey{1, 1}, OperatorPriority::NormalPriority,
            std::chrono::steady_clock::now(),
            std::make_shared<TransferLeader>(1, 2));
        op.timeLimit = std::chrono::seconds(3);
        hbtest_->AddOperatorToOpController(op);
    }

    void PrepareMdsWithTransferOpOnGoing() {
        // mds存在copyset-1(epoch=5, peers={1,2,3}, leader=1, candidate=2);
        // scheduler中copyset-1有operator:startEpoch=5,step=TransferLeader{1>2}
        hbtest_->UpdateCopysetTopo(
            1, 1, 5, 1, std::set<ChunkServerIdType>{1, 2, 3}, 2);

        Operator op(5, CopySetKey{1, 1}, OperatorPriority::NormalPriority,
            std::chrono::steady_clock::now(),
            std::make_shared<TransferLeader>(1, 2));
        op.timeLimit = std::chrono::seconds(3);
        hbtest_->AddOperatorToOpController(op);
    }

    void PrePareMdsWithCandidateNoOp() {
        // mds存在copyset-1(epoch=5, peers={1,2,3}, leader=1, candidate=4);
        ChunkServer cs(4, "testtoken", "nvme", 3, "10.198.100.3", 9001, "/");
        hbtest_->PrepareAddChunkServer(cs);
        hbtest_->UpdateCopysetTopo(
            1, 1, 5, 1, std::set<ChunkServerIdType>{1, 2, 3}, 4);
    }

    void PrepareMdsWithChangeOp() {
        // mds存在copyset-1(epoch=5, peers={1,2,3}, leader=1);
        // scheduler中copyset-1有operator:startEpoch=5,step=ChangePeer{3>4}
        ChunkServer cs(4, "testtoken", "nvme", 3, "10.198.100.3", 9001, "/");
        hbtest_->PrepareAddChunkServer(cs);
        hbtest_->UpdateCopysetTopo(
            1, 1, 5, 1, std::set<ChunkServerIdType>{1, 2, 3});

        Operator op(5, CopySetKey{1, 1}, OperatorPriority::NormalPriority,
            std::chrono::steady_clock::now(),
            std::make_shared<ChangePeer>(3, 4));
        op.timeLimit = std::chrono::seconds(3);
        hbtest_->AddOperatorToOpController(op);
    }

    void PrepareMdsWithChangeOpOnGoing() {
        // mds存在copyset-1(epoch=5, peers={1,2,3}, leader=1, candidate=4);
        // scheduler中copyset-1有operator:startEpoch=5,step=step=ChangePeer{3>4}
        ChunkServer cs(4, "testtoken", "nvme", 3, "10.198.100.3", 9001, "/");
        hbtest_->PrepareAddChunkServer(cs);
        hbtest_->UpdateCopysetTopo(
            1, 1, 5, 1, std::set<ChunkServerIdType>{1, 2, 3}, 4);

        Operator op(5, CopySetKey{1, 1}, OperatorPriority::NormalPriority,
            std::chrono::steady_clock::now(),
            std::make_shared<ChangePeer>(3, 4));
        op.timeLimit = std::chrono::seconds(3);
        hbtest_->AddOperatorToOpController(op);
    }

    bool ValidateCopySet(const ::curve::mds::topology::CopySetInfo &expected) {
        ::curve::mds::topology::CopySetInfo copysetInfo;
        if (!hbtest_->topology_->GetCopySet(
            CopySetKey{expected.GetLogicalPoolId(), expected.GetId()},
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

    void BuildCopySetInfo(
        CopySetInfo *info, uint64_t epoch, ChunkServerIdType leader,
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
    // 空的HeartbeatRequest
    ChunkServerHeartbeatRequest req;
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBFAIL, &rep);
}

TEST_F(HeartbeatBasicTest, test_mds_donnot_has_this_chunkserver) {
    // mds不存在该chunkserver
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(3, &req);
    req.set_chunkserverid(4);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    ASSERT_EQ(::curve::mds::heartbeat::hbChunkserverUnknown, rep.statuscode());
}

TEST_F(HeartbeatBasicTest, test_chunkserver_ip_port_not_match) {
    // chunkserver上报的id相同，ip和port不匹配
    // ip不匹配
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(3, &req);
    req.set_ip("127.0.0.1");
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    ASSERT_EQ(
        ::curve::mds::heartbeat::hbChunkserverIpPortNotMatch, rep.statuscode());

    // port不匹配
    req.set_ip("10.198.100.3");
    req.set_port(1111);
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    ASSERT_EQ(
        ::curve::mds::heartbeat::hbChunkserverIpPortNotMatch, rep.statuscode());

    // token不匹配
    req.set_ip("10.198.100.3");
    req.set_port(9000);
    req.set_token("youdao");
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    ASSERT_EQ(
        ::curve::mds::heartbeat::hbChunkserverTokenNotMatch, rep.statuscode());
}

TEST_F(HeartbeatBasicTest, test_chunkserver_offline_then_online) {
    // chunkserver上报心跳时间间隔大于offline
    // sleep 800ms, 该chunkserver onffline状态
    std::this_thread::sleep_for(std::chrono::milliseconds(800));
    ChunkServer out;
    hbtest_->topology_->GetChunkServer(1, &out);
    ASSERT_EQ(OnlineState::OFFLINE, out.GetOnlineState());

    // chunkserver上报心跳，chunkserver online
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(out.GetId(), &req);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 后台健康检查程序把chunksrver更新为onlinne状态
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
    ASSERT_TRUE(hbtest_->topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));

    // copyset-1(epoch=1, peers={1,2,3}, leader=1)
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 1, 1, copysetInfo.GetCopySetMembers());
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    ASSERT_TRUE(hbtest_->topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(1, copysetInfo.GetEpoch());
    ASSERT_EQ(1, copysetInfo.GetLeader());
}

TEST_F(HeartbeatBasicTest, test_copysets_in_mds_is_initial_state_condition2) {
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(1, &req);
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(hbtest_->topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ChunkServer cs(4, "testtoken", "nvme", 3, "10.198.100.3", 9001, "/");
    hbtest_->PrepareAddChunkServer(cs);

    // copyset-1(epoch=1, peers={1,2,3}, leader=1,
    // conf.gChangeInfo={peer: 4, type: AddPeer})
    CopySetInfo csInfo(copysetInfo.GetLogicalPoolId(), copysetInfo.GetId());
    BuildCopySetInfo(&csInfo, 1, 1, copysetInfo.GetCopySetMembers(), 4);
    hbtest_->AddCopySetToRequest(&req, csInfo, ConfigChangeType::ADD_PEER);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    ASSERT_TRUE(hbtest_->topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(1, copysetInfo.GetEpoch());
    ASSERT_EQ(1, copysetInfo.GetLeader());
    ASSERT_TRUE(copysetInfo.HasCandidate());
    ASSERT_EQ(4, copysetInfo.GetCandidate());
}

TEST_F(HeartbeatBasicTest, test_copysets_in_mds_is_initial_state_condition3) {
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(1, &req);
    ::curve::mds::topology::CopySetInfo copysetInfo;
    CopySetKey key{1, 1};
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

    ASSERT_TRUE(hbtest_->topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(1, copysetInfo.GetEpoch());
    ASSERT_EQ(1, copysetInfo.GetLeader());
    std::set<ChunkServerIDType> res{1, 2, 3, 5};
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
    ASSERT_EQ(0, rep.needupdatecopysets_size());
}

TEST_F(HeartbeatBasicTest, test_copysets_in_mds_initial_state_condition4) {
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(1, &req);
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(hbtest_->topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
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

    ASSERT_TRUE(hbtest_->topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(1, copysetInfo.GetEpoch());
    ASSERT_EQ(1, copysetInfo.GetLeader());
    std::set<ChunkServerIDType> res{1, 2, 3, 5};
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
    ASSERT_EQ(4, copysetInfo.GetCandidate());
    ASSERT_EQ(0, rep.needupdatecopysets_size());
}

TEST_F(HeartbeatBasicTest, test_copysets_in_mds_initial_state_condition5) {
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(hbtest_->topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));

    // copyset-1(epoch=0, peers={1,2,3}, leader=0)
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 0, copysetInfo.GetCopySetMembers());
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    ASSERT_TRUE(hbtest_->topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
     ASSERT_EQ(0, copysetInfo.GetEpoch());
    ASSERT_EQ(0, copysetInfo.GetLeader());
    std::set<ChunkServerIDType> res{1, 2, 3};
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
    ASSERT_EQ(0, rep.needupdatecopysets_size());
}

TEST_F(HeartbeatBasicTest, test_copysets_in_mds_initial_state_condition6) {
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(hbtest_->topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));

    // copyset-1(epoch=0, peers={1,2,3}, leader=1)
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 1, copysetInfo.GetCopySetMembers());
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    ASSERT_TRUE(hbtest_->topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(0, copysetInfo.GetEpoch());
    ASSERT_EQ(0, copysetInfo.GetLeader());
    std::set<ChunkServerIDType> res{1, 2, 3};
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
    ASSERT_EQ(0, rep.needupdatecopysets_size());
}

TEST_F(HeartbeatBasicTest, test_copysets_in_mds_initial_state_condition7) {
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(hbtest_->topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));

    // copyset-1(epoch=0, peers={1,2,3}, leader=1)
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 1, copysetInfo.GetCopySetMembers());
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    ASSERT_TRUE(hbtest_->topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(0, copysetInfo.GetEpoch());
    ASSERT_EQ(0, copysetInfo.GetLeader());
    std::set<ChunkServerIDType> res{1, 2, 3};
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
    ASSERT_EQ(0, rep.needupdatecopysets_size());
}

TEST_F(HeartbeatBasicTest, test_copysets_in_mds_initial_state_condition8) {
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(hbtest_->topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));

    // copyset-1(epoch=1, peers={1,2,3}, leader=0)
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 1, 0, copysetInfo.GetCopySetMembers());
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    ASSERT_TRUE(hbtest_->topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(0, copysetInfo.GetEpoch());
    ASSERT_EQ(0, copysetInfo.GetLeader());
    std::set<ChunkServerIDType> res{1, 2, 3};
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
    ASSERT_EQ(0, rep.needupdatecopysets_size());
}

TEST_F(HeartbeatBasicTest, test_copysets_in_mds_initial_state_condition9) {
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(hbtest_->topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ChunkServer cs(4, "testtoken", "nvme", 3, "10.198.100.3", 9001, "/");
    hbtest_->PrepareAddChunkServer(cs);

    // 上报copyset-1(epoch=2, peers={1,2,3,4}, leader=1)
    auto copysetMembers = copysetInfo.GetCopySetMembers();
    copysetMembers.emplace(4);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 2, 1, copysetMembers);
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    ASSERT_TRUE(hbtest_->topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(0, copysetInfo.GetEpoch());
    ASSERT_EQ(0, copysetInfo.GetLeader());
    std::set<ChunkServerIDType> res{1, 2, 3};
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
    ASSERT_EQ(0, rep.needupdatecopysets_size());
}

TEST_F(HeartbeatBasicTest, test_copysets_in_mds_initial_state_condition10) {
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(hbtest_->topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
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

    ASSERT_TRUE(hbtest_->topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(0, copysetInfo.GetEpoch());
    ASSERT_EQ(0, copysetInfo.GetLeader());
    std::set<ChunkServerIDType> res{1, 2, 3};
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
    ASSERT_EQ(0, rep.needupdatecopysets_size());
}

// 上报的是leader
TEST_F(HeartbeatBasicTest, test_leader_report_consistent_with_mds) {
    // 更新topology中的copyset-1(epoch=2, peers={1,2,3}, leader=1)
    hbtest_->UpdateCopysetTopo(
        1, 1, 2, 1, std::set<ChunkServerIdType>{1, 2, 3});

    // chunkserver1上报的copyset-1(epoch=2, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(1, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 2, 1, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(hbtest_->topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(2, copysetInfo.GetEpoch());
    ASSERT_EQ(1, copysetInfo.GetLeader());
    std::set<ChunkServerIDType> res{1, 2, 3};
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_leader_report_epoch_bigger) {
    // 更新topology中的copyset-1(epoch=2, peers={1,2,3}, leader=1)
    hbtest_->UpdateCopysetTopo(
        1, 1, 2, 1, std::set<ChunkServerIdType>{1, 2, 3});

    // chunkserver1上报的copyset-1(epoch=5, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(1, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 1, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // response为空，mds更新epoch为5
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(hbtest_->topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(5, copysetInfo.GetEpoch());
    ASSERT_EQ(1, copysetInfo.GetLeader());
    std::set<ChunkServerIDType> res{1, 2, 3};
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_leader_report_epoch_bigger_leader_not_same) {
    // 更新topology中的copyset-1(epoch=2, peers={1,2,3}, leader=1)
    hbtest_->UpdateCopysetTopo(
        1, 1, 2, 1, std::set<ChunkServerIdType>{1, 2, 3});

    // chunkserver2上报的copyset-1(epoch=5, peers={1,2,3}, leader=2)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 2, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // response为空，mds更新epoch为5，leader为2
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(hbtest_->topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(5, copysetInfo.GetEpoch());
    ASSERT_EQ(2, copysetInfo.GetLeader());
    std::set<ChunkServerIDType> res{1, 2, 3};
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
}

// 上报的是follower
TEST_F(HeartbeatBasicTest, test_follower_report_consistent_with_mds) {
    // 更新topology中的copyset-1(epoch=2, peers={1,2,3}, leader=1)
    hbtest_->UpdateCopysetTopo(
        1, 1, 2, 1, std::set<ChunkServerIdType>{1, 2, 3});

    // chunkserver2上报的copyset-1(epoch=2, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 2, 1, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // response为空
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(hbtest_->topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(2, copysetInfo.GetEpoch());
    ASSERT_EQ(1, copysetInfo.GetLeader());
    std::set<ChunkServerIDType> res{1, 2, 3};
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
}

// 上报的是follower
TEST_F(HeartbeatBasicTest, test_follower_report_leader_0) {
    // 更新topology中的copyset-1(epoch=2, peers={1,2,3}, leader=1)
    hbtest_->UpdateCopysetTopo(
        1, 1, 2, 1, std::set<ChunkServerIdType>{1, 2, 3});

    // chunkserver2上报的copyset-1(epoch=2, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 2, 0, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // response为空
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(hbtest_->topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(2, copysetInfo.GetEpoch());
    ASSERT_EQ(1, copysetInfo.GetLeader());
    std::set<ChunkServerIDType> res{1, 2, 3};
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_follower_report_epoch_bigger) {
    // 更新topology中的copyset-1(epoch=2, peers={1,2,3}, leader=1)
    hbtest_->UpdateCopysetTopo(
        1, 1, 2, 1, std::set<ChunkServerIdType>{1, 2, 3});

    // chunkserver2上报的copyset-1(epoch=3, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 3, 1, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(hbtest_->topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(2, copysetInfo.GetEpoch());
    ASSERT_EQ(1, copysetInfo.GetLeader());
    std::set<ChunkServerIDType> res{1, 2, 3};
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_follower_report_epoch_bigger_leader_0) {
    // 更新topology中的copyset-1(epoch=2, peers={1,2,3}, leader=1)
    hbtest_->UpdateCopysetTopo(
        1, 1, 2, 1, std::set<ChunkServerIdType>{1, 2, 3});

    // chunkserver2上报的copyset-1(epoch=3, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 3, 0, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(hbtest_->topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(2, copysetInfo.GetEpoch());
    ASSERT_EQ(1, copysetInfo.GetLeader());
    std::set<ChunkServerIDType> res{1, 2, 3};
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_follower_report_epoch_bigger_peers_not_same) {
    // 更新topology中的copyset-1(epoch=2, peers={1,2,3}, leader=1)
    hbtest_->UpdateCopysetTopo(
        1, 1, 2, 1, std::set<ChunkServerIdType>{1, 2, 3});
    ChunkServer cs(4, "testtoken", "nvme", 3, "10.198.100.3", 9001, "/");
    hbtest_->PrepareAddChunkServer(cs);

    // chunkserver2上报的copyset-1(epoch=3, peers={1,2,3,4}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 3, 1, std::set<ChunkServerIdType>{1, 2, 3, 4});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(hbtest_->topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(2, copysetInfo.GetEpoch());
    ASSERT_EQ(1, copysetInfo.GetLeader());
    std::set<ChunkServerIDType> res{1, 2, 3};
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_follower_report_epoch_smaller) {
    // 更新topology中的copyset-1(epoch=2, peers={1,2,3}, leader=1)
    hbtest_->UpdateCopysetTopo(
        1, 1, 2, 1, std::set<ChunkServerIdType>{1, 2, 3});

    // chunkserver2上报的copyset-1(epoch=1, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 1, 1, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(hbtest_->topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(2, copysetInfo.GetEpoch());
    ASSERT_EQ(1, copysetInfo.GetLeader());
    std::set<ChunkServerIDType> res{1, 2, 3};
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_follower_report_epoch_smaller_leader_0) {
    // 更新topology中的copyset-1(epoch=2, peers={1,2,3}, leader=1)
    hbtest_->UpdateCopysetTopo(
        1, 1, 2, 1, std::set<ChunkServerIdType>{1, 2, 3});

    // chunkserver2上报的copyset-1(epoch=1, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 1, 0, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(hbtest_->topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(2, copysetInfo.GetEpoch());
    ASSERT_EQ(1, copysetInfo.GetLeader());
    std::set<ChunkServerIDType> res{1, 2, 3};
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_follower_report_epoch_smaller_peers_not_same1) {
    // 更新topology中的copyset-1(epoch=2, peers={1,2,3}, leader=1)
    hbtest_->UpdateCopysetTopo(
        1, 1, 2, 1, std::set<ChunkServerIdType>{1, 2, 3});
    ChunkServer cs(4, "testtoken", "nvme", 3, "10.198.100.3", 9001, "/");
    hbtest_->PrepareAddChunkServer(cs);
    // chunkserver2上报的copyset-1(epoch=1, peers={1,2,3,4}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 1, 1, std::set<ChunkServerIdType>{1, 2, 3, 4});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(hbtest_->topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(2, copysetInfo.GetEpoch());
    ASSERT_EQ(1, copysetInfo.GetLeader());
    std::set<ChunkServerIDType> res{1, 2, 3};
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_follower_report_epoch_smaller_peers_not_same2) {
    // 更新topology中的copyset-1(epoch=2, peers={1,2,3}, leader=1)
    hbtest_->UpdateCopysetTopo(
        1, 1, 2, 1, std::set<ChunkServerIdType>{1, 2, 3});
    ChunkServer cs(4, "testtoken", "nvme", 3, "10.198.100.3", 9001, "/");
    hbtest_->PrepareAddChunkServer(cs);
    // chunkserver2上报的copyset-1(epoch=1, peers={1,2,3,4}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 1, 0, std::set<ChunkServerIdType>{1, 2, 3, 4});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(hbtest_->topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(2, copysetInfo.GetEpoch());
    ASSERT_EQ(1, copysetInfo.GetLeader());
    std::set<ChunkServerIDType> res{1, 2, 3};
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_follower_report_epoch_0) {
    // 更新topology中的copyset-1(epoch=2, peers={1,2,3}, leader=1)
    hbtest_->UpdateCopysetTopo(
        1, 1, 2, 1, std::set<ChunkServerIdType>{1, 2, 3});

    // chunkserver2上报的copyset-1(epoch=0, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 1, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(hbtest_->topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(2, copysetInfo.GetEpoch());
    ASSERT_EQ(1, copysetInfo.GetLeader());
    std::set<ChunkServerIDType> res{1, 2, 3};
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_follower_report_epoch_0_leader_0) {
    // 更新topology中的copyset-1(epoch=2, peers={1,2,3}, leader=1)
    hbtest_->UpdateCopysetTopo(
        1, 1, 2, 1, std::set<ChunkServerIdType>{1, 2, 3});
    // chunkserver2上报的copyset-1(epoch=0, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 0, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(hbtest_->topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(2, copysetInfo.GetEpoch());
    ASSERT_EQ(1, copysetInfo.GetLeader());
    std::set<ChunkServerIDType> res{1, 2, 3};
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_follower_report_epoch_0_peers_not_same1) {
    // 更新topology中的copyset-1(epoch=2, peers={1,2,3}, leader=1)
    hbtest_->UpdateCopysetTopo(
        1, 1, 2, 1, std::set<ChunkServerIdType>{1, 2, 3});
    ChunkServer cs(4, "testtoken", "nvme", 3, "10.198.100.3", 9001, "/");
    hbtest_->PrepareAddChunkServer(cs);
    // chunkserver2上报的copyset-1(epoch=0, peers={1,2,3,4}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 1, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(hbtest_->topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(2, copysetInfo.GetEpoch());
    ASSERT_EQ(1, copysetInfo.GetLeader());
    std::set<ChunkServerIDType> res{1, 2, 3};
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_follower_report_epoch_0_peers_not_same2) {
    // 更新topology中的copyset-1(epoch=2, peers={1,2,3}, leader=1)
    hbtest_->UpdateCopysetTopo(
        1, 1, 2, 1, std::set<ChunkServerIdType>{1, 2, 3});
    ChunkServer cs(4, "testtoken", "nvme", 3, "10.198.100.3", 9001, "/");
    hbtest_->PrepareAddChunkServer(cs);
    // chunkserver2上报的copyset-1(epoch=0, peers={1,2,3,4}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 0, std::set<ChunkServerIdType>{1, 2, 3, 4});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(hbtest_->topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(2, copysetInfo.GetEpoch());
    ASSERT_EQ(1, copysetInfo.GetLeader());
    std::set<ChunkServerIDType> res{1, 2, 3};
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
}

// 上报的不是复制组成员
TEST_F(HeartbeatBasicTest, test_other_report_consistent_with_mds) {
    // 更新topology中的copyset-1(epoch=2, peers={1,2,3}, leader=1)
    hbtest_->UpdateCopysetTopo(
        1, 1, 2, 1, std::set<ChunkServerIdType>{1, 2, 3});
    ChunkServer cs(4, "testtoken", "nvme", 3, "10.198.100.3", 9001, "/");
    hbtest_->PrepareAddChunkServer(cs);
    // chunkserver4上报的copyset-1(epoch=2, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 2, 1, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    // 检查response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    CopySetConf conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(3, conf.peers_size());
    ASSERT_EQ(2, conf.epoch());
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(hbtest_->topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(2, copysetInfo.GetEpoch());
    ASSERT_EQ(1, copysetInfo.GetLeader());
    std::set<ChunkServerIDType> res{1, 2, 3};
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_other_report_epoch_smaller) {
    // 更新topology中的copyset-1(epoch=2, peers={1,2,3}, leader=1)
    hbtest_->UpdateCopysetTopo(
        1, 1, 2, 1, std::set<ChunkServerIdType>{1, 2, 3});
    ChunkServer cs(4, "testtoken", "nvme", 3, "10.198.100.3", 9001, "/");
    hbtest_->PrepareAddChunkServer(cs);
    // chunkserver4上报的copyset-1(epoch=1, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 1, 1, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    // 检查response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    CopySetConf conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(3, conf.peers_size());
    ASSERT_EQ(2, conf.epoch());
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(hbtest_->topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(2, copysetInfo.GetEpoch());
    ASSERT_EQ(1, copysetInfo.GetLeader());
    std::set<ChunkServerIDType> res{1, 2, 3};
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_other_report_epoch_smaller_peers_not_same) {
    // 更新topology中的copyset-1(epoch=2, peers={1,2,3}, leader=1)
    hbtest_->UpdateCopysetTopo(
        1, 1, 2, 1, std::set<ChunkServerIdType>{1, 2, 3});
    ChunkServer cs(4, "testtoken", "nvme", 3, "10.198.100.3", 9001, "/");
    hbtest_->PrepareAddChunkServer(cs);
    // chunkserver4上报的copyset-1(epoch=1, peers={1,2}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 1, 1, std::set<ChunkServerIdType>{1, 2});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    // 检查response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    CopySetConf conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(3, conf.peers_size());
    ASSERT_EQ(2, conf.epoch());
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(hbtest_->topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(2, copysetInfo.GetEpoch());
    ASSERT_EQ(1, copysetInfo.GetLeader());
    std::set<ChunkServerIDType> res{1, 2, 3};
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_other_report_epoch_0_leader_0) {
    // 更新topology中的copyset-1(epoch=2, peers={1,2,3}, leader=1)
    hbtest_->UpdateCopysetTopo(
        1, 1, 2, 1, std::set<ChunkServerIdType>{1, 2, 3});
    ChunkServer cs(4, "testtoken", "nvme", 3, "10.198.100.3", 9001, "/");
    hbtest_->PrepareAddChunkServer(cs);

    // chunkserver4上报的copyset-1(epoch=0, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 0, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    // 检查response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    CopySetConf conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(3, conf.peers_size());
    ASSERT_EQ(2, conf.epoch());
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(hbtest_->topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(2, copysetInfo.GetEpoch());
    ASSERT_EQ(1, copysetInfo.GetLeader());
    std::set<ChunkServerIDType> res{1, 2, 3};
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_other_report_epoch_0_leader_0_peers_not_same) {
    // 更新topology中的copyset-1(epoch=2, peers={1,2,3}, leader=1)
    hbtest_->UpdateCopysetTopo(
        1, 1, 2, 1, std::set<ChunkServerIdType>{1, 2, 3});
    ChunkServer cs4(4, "testtoken", "nvme", 3, "10.198.100.3", 9001, "/");
    hbtest_->PrepareAddChunkServer(cs4);
    ChunkServer cs5(5, "testtoken", "nvme", 3, "10.198.100.3", 9090, "/");
    hbtest_->PrepareAddChunkServer(cs5);

    // chunkserver4上报的copyset-1(epoch=0, peers={1,2,3,5}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 0, std::set<ChunkServerIdType>{1, 2, 3, 5});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    // 检查response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    CopySetConf conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(3, conf.peers_size());
    ASSERT_EQ(2, conf.epoch());
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(hbtest_->topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(2, copysetInfo.GetEpoch());
    ASSERT_EQ(1, copysetInfo.GetLeader());
    std::set<ChunkServerIDType> res{1, 2, 3};
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_mdsNoCandidate_OpOnGoing_condition1) {
    PrepareMdsNoCnandidateOpOnGoing();

    // chunkserver1上报的copyset-1(epoch=5, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(1, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 1, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    // 检查sheduler中的operator
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    // 检查response
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

    // chunkserver1上报的copyset-1(epoch=5, peers={1,2,3}, leader=1)
    // conf.gChangeInfo={peer: 10, type: AddPeer} )
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(1, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 1, std::set<ChunkServerIdType>{1, 2, 3}, 10);
    hbtest_->AddCopySetToRequest(&req, csInfo, ConfigChangeType::ADD_PEER);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查topology
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(hbtest_->topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(5, copysetInfo.GetEpoch());
    ASSERT_EQ(1, copysetInfo.GetLeader());
    std::set<ChunkServerIDType> res{1, 2, 3};
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
    ASSERT_EQ(10, copysetInfo.GetCandidate());
}

TEST_F(HeartbeatBasicTest, test_mdsNoCandidate_OpOnGoing_condition3) {
    PrepareMdsNoCnandidateOpOnGoing();

    // chunkserver1上报的copyset-1(epoch=6, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(1, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 1, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    // 检查sheduler中的operator
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查topology
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(hbtest_->topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(6, copysetInfo.GetEpoch());
    ASSERT_EQ(1, copysetInfo.GetLeader());
    std::set<ChunkServerIDType> res{1, 2, 3};
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_mdsNoCandidate_OpOnGoing_condition4) {
    PrepareMdsNoCnandidateOpOnGoing();

    // chunkserver2上报的copyset-1(epoch=6, peers={1,2,3}, leader=2)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 2, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    // 检查sheduler中的operator
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查topology
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(hbtest_->topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(6, copysetInfo.GetEpoch());
    ASSERT_EQ(2, copysetInfo.GetLeader());
    std::set<ChunkServerIDType> res{1, 2, 3};
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_mdsNoCandidate_OpOnGoing_condition5) {
    PrepareMdsNoCnandidateOpOnGoing();

    // chunkserver1上报的copyset-1(epoch=6, peers={1,2,3,10}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(1, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 1, std::set<ChunkServerIdType>{1, 2, 3, 10});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    // 检查sheduler中的operator
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查topology
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(hbtest_->topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(6, copysetInfo.GetEpoch());
    ASSERT_EQ(1, copysetInfo.GetLeader());
    std::set<ChunkServerIDType> res{1, 2, 3, 10};
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_mdsNoCandidate_OpOnGoing_condition6) {
    PrepareMdsNoCnandidateOpOnGoing();

    // chunkserver2上报的copyset-1(epoch=5, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 1, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
}

TEST_F(HeartbeatBasicTest, test_mdsNoCandidate_OpOnGoing_condition7) {
    PrepareMdsNoCnandidateOpOnGoing();

    // chunkserver2上报的copyset-1(epoch=5, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 0, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查topology
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(hbtest_->topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(5, copysetInfo.GetEpoch());
    ASSERT_EQ(1, copysetInfo.GetLeader());
    std::set<ChunkServerIDType> res{1, 2, 3};
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_mdsNoCandidate_OpOnGoing_condition8) {
    PrepareMdsNoCnandidateOpOnGoing();

    // chunkserver2上报的copyset-1(epoch=6, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 1, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查topology
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(hbtest_->topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(5, copysetInfo.GetEpoch());
    ASSERT_EQ(1, copysetInfo.GetLeader());
    std::set<ChunkServerIDType> res{1, 2, 3};
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_mdsNoCandidate_OpOnGoing_condition9) {
    PrepareMdsNoCnandidateOpOnGoing();

    // chunkserver2上报的copyset-1(epoch=6, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 0, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查topology
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(hbtest_->topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(5, copysetInfo.GetEpoch());
    ASSERT_EQ(1, copysetInfo.GetLeader());
    std::set<ChunkServerIDType> res{1, 2, 3};
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_mdsNoCandidate_OpOnGoing_condition10) {
    PrepareMdsNoCnandidateOpOnGoing();

    // chunkserver2上报的copyset-1(epoch=6, peers={1,2,3,10}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 1, std::set<ChunkServerIdType>{1, 2, 3, 10});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查topology
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(hbtest_->topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(5, copysetInfo.GetEpoch());
    ASSERT_EQ(1, copysetInfo.GetLeader());
    std::set<ChunkServerIDType> res{1, 2, 3};
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_mdsNoCandidate_OpOnGoing_condition11) {
    PrepareMdsNoCnandidateOpOnGoing();

    // chunkserver2上报的copyset-1(epoch=6, peers={1,2,3,10}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 0, std::set<ChunkServerIdType>{1, 2, 3, 10});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查topology
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(hbtest_->topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(5, copysetInfo.GetEpoch());
    ASSERT_EQ(1, copysetInfo.GetLeader());
    std::set<ChunkServerIDType> res{1, 2, 3};
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_mdsNoCandidate_OpOnGoing_condition12) {
    PrepareMdsNoCnandidateOpOnGoing();

    // chunkserver2上报的copyset-1(epoch=4, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 1, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
}

TEST_F(HeartbeatBasicTest, test_mdsNoCandidate_OpOnGoing_condition13) {
    PrepareMdsNoCnandidateOpOnGoing();

    // chunkserver2上报的copyset-1(epoch=4, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 0, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
}

TEST_F(HeartbeatBasicTest, test_mdsNoCandidate_OpOnGoing_condition14) {
    PrepareMdsNoCnandidateOpOnGoing();
    ChunkServer cs(4, "testtoekn", "nvme", 3, "10.198.100.3", 9002, "/");
    hbtest_->PrepareAddChunkServer(cs);

    // chunkserver2上报的copyset-1(epoch=4, peers={1,2,3,4}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 1, std::set<ChunkServerIdType>{1, 2, 3, 4});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
}

TEST_F(HeartbeatBasicTest, test_mdsNoCandidate_OpOnGoing_condition15) {
    PrepareMdsNoCnandidateOpOnGoing();
    ChunkServer cs(4, "testtoekn", "nvme", 3, "10.198.100.3", 9002, "/");
    hbtest_->PrepareAddChunkServer(cs);

    // chunkserver2上报的copyset-1(epoch=4, peers={1,2,3,4}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 0, std::set<ChunkServerIdType>{1, 2, 3, 4});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
}

TEST_F(HeartbeatBasicTest, test_mdsNoCandidate_OpOnGoing_condition16) {
    PrepareMdsNoCnandidateOpOnGoing();

    // chunkserver2上报的copyset-1(epoch=0, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 1, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
}

TEST_F(HeartbeatBasicTest, test_mdsNoCandidate_OpOnGoing_condition17) {
    PrepareMdsNoCnandidateOpOnGoing();

    // chunkserver2上报的copyset-1(epoch=0, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 0, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
}

TEST_F(HeartbeatBasicTest, test_mdsNoCandidate_OpOnGoing_condition18) {
    PrepareMdsNoCnandidateOpOnGoing();
    ChunkServer cs(4, "testtoekn", "nvme", 3, "10.198.100.3", 9002, "/");
    hbtest_->PrepareAddChunkServer(cs);

    // chunkserver2上报的copyset-1(epoch=0, peers={1,2,3,4}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 1, std::set<ChunkServerIdType>{1, 2, 3, 4});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
}

TEST_F(HeartbeatBasicTest, test_mdsNoCandidate_OpOnGoing_condition19) {
    PrepareMdsNoCnandidateOpOnGoing();
    ChunkServer cs(4, "testtoekn", "nvme", 3, "10.198.100.3", 9002, "/");
    hbtest_->PrepareAddChunkServer(cs);

    // chunkserver2上报的copyset-1(epoch=0, peers={1,2,3,4}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 0, std::set<ChunkServerIdType>{1, 2, 3, 4});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
}

TEST_F(HeartbeatBasicTest, test_mdsNoCandidate_OpOnGoing_condition20) {
    PrepareMdsNoCnandidateOpOnGoing();

    // chunkserver10上报的copyset-1(epoch=5, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(10, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 1, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
}

TEST_F(HeartbeatBasicTest, test_mdsNoCandidate_OpOnGoing_condition21) {
    PrepareMdsNoCnandidateOpOnGoing();

    // chunkserver10上报的copyset-1(epoch=5, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(10, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 0, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
}

TEST_F(HeartbeatBasicTest, test_mdsNoCandidate_OpOnGoing_condition22) {
    PrepareMdsNoCnandidateOpOnGoing();

    // chunkserver10上报的copyset-1(epoch=6, peers={1,2,3,10}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(10, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 1, std::set<ChunkServerIdType>{1, 2, 3, 10});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查topology
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(hbtest_->topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(5, copysetInfo.GetEpoch());
    ASSERT_EQ(1, copysetInfo.GetLeader());
    std::set<ChunkServerIDType> res{1, 2, 3};
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_mdsNoCandidate_OpOnGoing_condition23) {
    PrepareMdsNoCnandidateOpOnGoing();

    // chunkserver10上报的copyset-1(epoch=6, peers={1,2,3,10}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(10, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 0, std::set<ChunkServerIdType>{1, 2, 3, 10});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查topology
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(hbtest_->topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(5, copysetInfo.GetEpoch());
    ASSERT_EQ(1, copysetInfo.GetLeader());
    std::set<ChunkServerIDType> res{1, 2, 3};
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_mdsNoCandidate_OpOnGoing_condition24) {
    PrepareMdsNoCnandidateOpOnGoing();

    // chunkserver10上报的copyset-1(epoch=4, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(10, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 1, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
}

TEST_F(HeartbeatBasicTest, test_mdsNoCandidate_OpOnGoing_condition25) {
    PrepareMdsNoCnandidateOpOnGoing();

    // chunkserver10上报的copyset-1(epoch=4, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(10, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 0, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
}

TEST_F(HeartbeatBasicTest, test_mdsNoCandidate_OpOnGoing_condition26) {
    PrepareMdsNoCnandidateOpOnGoing();
    ChunkServer cs(4, "testtoekn", "nvme", 3, "10.198.100.3", 9002, "/");
    hbtest_->PrepareAddChunkServer(cs);

    // chunkserver10上报的copyset-1(epoch=4, peers={1,2,3,4}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(10, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 1, std::set<ChunkServerIdType>{1, 2, 3, 4});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
}

TEST_F(HeartbeatBasicTest, test_mdsNoCandidate_OpOnGoing_condition27) {
    PrepareMdsNoCnandidateOpOnGoing();
    ChunkServer cs(4, "testtoekn", "nvme", 3, "10.198.100.3", 9002, "/");
    hbtest_->PrepareAddChunkServer(cs);

    // chunkserver10上报的copyset-1(epoch=4, peers={1,2,3,4}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(10, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 0, std::set<ChunkServerIdType>{1, 2, 3, 4});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
}

TEST_F(HeartbeatBasicTest, test_mdsNoCandidate_OpOnGoing_condition28) {
    PrepareMdsNoCnandidateOpOnGoing();

    // chunkserver10上报的copyset-1(epoch=0, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(10, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 1, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
}

TEST_F(HeartbeatBasicTest, test_mdsNoCandidate_OpOnGoing_condition29) {
    PrepareMdsNoCnandidateOpOnGoing();

    // chunkserver10上报的copyset-1(epoch=0, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(10, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 0, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
}

TEST_F(HeartbeatBasicTest, test_mdsNoCandidate_OpOnGoing_condition30) {
    PrepareMdsNoCnandidateOpOnGoing();
    ChunkServer cs(4, "testtoekn", "nvme", 3, "10.198.100.3", 9002, "/");
    hbtest_->PrepareAddChunkServer(cs);

    // chunkserver10上报的copyset-1(epoch=0, peers={1,2,3,4}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(10, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 1, std::set<ChunkServerIdType>{1, 2, 3, 4});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
}

TEST_F(HeartbeatBasicTest, test_mdsNoCandidate_OpOnGoing_condition31) {
    PrepareMdsNoCnandidateOpOnGoing();
    ChunkServer cs(4, "testtoekn", "nvme", 3, "10.198.100.3", 9002, "/");
    hbtest_->PrepareAddChunkServer(cs);

    // chunkserver10上报的copyset-1(epoch=0, peers={1,2,3,4}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(10, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 0, std::set<ChunkServerIdType>{1, 2, 3, 4});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
}

TEST_F(HeartbeatBasicTest, test_mdsNoCandidate_OpOnGoing_condition32) {
    PrepareMdsNoCnandidateOpOnGoing();
    ChunkServer cs(4, "testtoekn", "nvme", 3, "10.198.100.3", 9002, "/");
    hbtest_->PrepareAddChunkServer(cs);

    // chunkserver4上报的copyset-1(epoch=5, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 0, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
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

    // chunkserver4上报的copyset-1(epoch=4, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 0, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
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

    // chunkserver4上报的copyset-1(epoch=4, peers={1,2,3,5}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 0, std::set<ChunkServerIdType>{1, 2, 3, 5});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
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

    // chunkserver4上报的copyset-1(epoch=0, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 0, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    CopySetConf conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(3, conf.peers_size());
    ASSERT_EQ(5, conf.epoch());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidate_OpOnGoing_condition1) {
    PrepareMdsWithCandidateOpOnGoing();

    // chunkserver1上报copyset-1(epoch=5, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(1, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 1, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    ASSERT_EQ(ConfigChangeType::ADD_PEER, rep.needupdatecopysets(0).type());
    ASSERT_EQ("10.198.100.3:9001:0",
        rep.needupdatecopysets(0).configchangeitem().address());
}

TEST_F(HeartbeatBasicTest, test_test_mdsWithCandidate_OpOnGoing_condition2) {
    PrepareMdsWithCandidateOpOnGoing();

    // chunkserver1上报
    // copyset-1(epoch=5, peers={1,2,3}, leader=1,
    //           conf.gChangeInfo={peer: 10, type: AddPeer} )
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(1, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 1, std::set<ChunkServerIdType>{1, 2, 3}, 10);
    hbtest_->AddCopySetToRequest(&req, csInfo, ConfigChangeType::ADD_PEER);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidate_OpOnGoing_condition3) {
    PrepareMdsWithCandidateOpOnGoing();

    // chunkserver2上报copyset-1(epoch=6, peers={1,2,3}, leader=2)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
     CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 2, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查sheduler中的operator
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查topology中copyset的状态
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(hbtest_->topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(2, copysetInfo.GetLeader());
    ASSERT_EQ(6, copysetInfo.GetEpoch());
    ASSERT_EQ(UNINTIALIZE_ID, copysetInfo.GetCandidate());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidate_OpOnGoing_condition4) {
    PrepareMdsWithCandidateOpOnGoing();

    // chunkserver1上报copyset-1(epoch=6, peers={1,2,3,10}, leader=2)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(1, &req);
         CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 1, std::set<ChunkServerIdType>{1, 2, 3, 10});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查sheduler中的operator
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查topology中copyset的状态
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(hbtest_->topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(1, copysetInfo.GetLeader());
    ASSERT_EQ(6, copysetInfo.GetEpoch());
    ASSERT_EQ(UNINTIALIZE_ID, copysetInfo.GetCandidate());
    std::set<ChunkServerIdType> peers{1, 2 , 3, 10};
    ASSERT_EQ(peers, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidate_OpOnGoing_condition5) {
    PrepareMdsWithCandidateOpOnGoing();

    // chunkserver2上报copyset-1(epoch=7, peers={1,2,3, 10}, leader=2)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
         CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 7, 2, std::set<ChunkServerIdType>{1, 2, 3, 10});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查sheduler中的operator
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查topology中copyset的状态
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(hbtest_->topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(2, copysetInfo.GetLeader());
    ASSERT_EQ(7, copysetInfo.GetEpoch());
    ASSERT_EQ(UNINTIALIZE_ID, copysetInfo.GetCandidate());
    std::set<ChunkServerIdType> peers{1, 2 , 3, 10};
    ASSERT_EQ(peers, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidate_OpOnGoing_condition6) {
    PrepareMdsWithCandidateOpOnGoing();

    // chunkserver2上报copyset-1(epoch=5, peers={1,2,3}, leader=1)
     ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 1, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查sheduler中的operator
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidate_OpOnGoing_condition7) {
    PrepareMdsWithCandidateOpOnGoing();

    // chunkserver2上报copyset-1(epoch=5, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 0, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查sheduler中的operator
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidate_OpOnGoing_condition8) {
    PrepareMdsWithCandidateOpOnGoing();

    // chunkserver2上报copyset-1(epoch=6, peers={1,2,3,10}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 1, std::set<ChunkServerIdType>{1, 2, 3, 10});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查sheduler中的operator
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(hbtest_->topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(1, copysetInfo.GetLeader());
    ASSERT_EQ(5, copysetInfo.GetEpoch());
    ASSERT_EQ(10, copysetInfo.GetCandidate());
    std::set<ChunkServerIdType> peers{1, 2 , 3};
    ASSERT_EQ(peers, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidate_OpOnGoing_condition9) {
    PrepareMdsWithCandidateOpOnGoing();

    // chunkserver2上报copyset-1(epoch=6, peers={1,2,3,10}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 0, std::set<ChunkServerIdType>{1, 2, 3, 10});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查sheduler中的operator
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(hbtest_->topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(1, copysetInfo.GetLeader());
    ASSERT_EQ(5, copysetInfo.GetEpoch());
    ASSERT_EQ(10, copysetInfo.GetCandidate());
    std::set<ChunkServerIdType> peers{1, 2 , 3};
    ASSERT_EQ(peers, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidate_OpOnGoing_condition10) {
    PrepareMdsWithCandidateOpOnGoing();

    // chunkserver2上报copyset-1(epoch=4, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 1, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

     // 检查sheduler中的operator
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(hbtest_->topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(1, copysetInfo.GetLeader());
    ASSERT_EQ(5, copysetInfo.GetEpoch());
    ASSERT_EQ(10, copysetInfo.GetCandidate());
    std::set<ChunkServerIdType> peers{1, 2 , 3};
    ASSERT_EQ(peers, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidate_OpOnGoing_condition11) {
    PrepareMdsWithCandidateOpOnGoing();

    // chunkserver2上报copyset-1(epoch=4, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 0, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

     // 检查sheduler中的operator
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(hbtest_->topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(1, copysetInfo.GetLeader());
    ASSERT_EQ(5, copysetInfo.GetEpoch());
    ASSERT_EQ(10, copysetInfo.GetCandidate());
    std::set<ChunkServerIdType> peers{1, 2 , 3};
    ASSERT_EQ(peers, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidate_OpOnGoing_condition12) {
    PrepareMdsWithCandidateOpOnGoing();
    ChunkServer cs(4, "testtoekn", "nvme", 3, "10.198.100.3", 9002, "/");
    hbtest_->PrepareAddChunkServer(cs);

    // chunkserver2上报copyset-1(epoch=4, peers={1,2,3,4}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 1, std::set<ChunkServerIdType>{1, 2, 3, 4});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查sheduler中的operator
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(hbtest_->topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(1, copysetInfo.GetLeader());
    ASSERT_EQ(5, copysetInfo.GetEpoch());
    ASSERT_EQ(10, copysetInfo.GetCandidate());
    std::set<ChunkServerIdType> peers{1, 2 , 3};
    ASSERT_EQ(peers, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidate_OpOnGoing_condition13) {
    PrepareMdsWithCandidateOpOnGoing();
    ChunkServer cs(4, "testtoekn", "nvme", 3, "10.198.100.3", 9002, "/");
    hbtest_->PrepareAddChunkServer(cs);

    // chunkserver2上报copyset-1(epoch=4, peers={1,2,3,4}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 0, std::set<ChunkServerIdType>{1, 2, 3, 4});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查sheduler中的operator
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(hbtest_->topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(1, copysetInfo.GetLeader());
    ASSERT_EQ(5, copysetInfo.GetEpoch());
    ASSERT_EQ(10, copysetInfo.GetCandidate());
    std::set<ChunkServerIdType> peers{1, 2 , 3};
    ASSERT_EQ(peers, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidate_OpOnGoing_condition14) {
    PrepareMdsWithCandidateOpOnGoing();

    // chunkserver2上报copyset-1(epoch=0, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
        CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 1, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查sheduler中的operator
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(hbtest_->topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(1, copysetInfo.GetLeader());
    ASSERT_EQ(5, copysetInfo.GetEpoch());
    ASSERT_EQ(10, copysetInfo.GetCandidate());
    std::set<ChunkServerIdType> peers{1, 2 , 3};
    ASSERT_EQ(peers, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidate_OpOnGoing_condition15) {
    PrepareMdsWithCandidateOpOnGoing();

    // chunkserver2上报copyset-1(epoch=0, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 0, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查sheduler中的operator
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(hbtest_->topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(1, copysetInfo.GetLeader());
    ASSERT_EQ(5, copysetInfo.GetEpoch());
    ASSERT_EQ(10, copysetInfo.GetCandidate());
    std::set<ChunkServerIdType> peers{1, 2 , 3};
    ASSERT_EQ(peers, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidate_OpOnGoing_condition16) {
    PrepareMdsWithCandidateOpOnGoing();
    ChunkServer cs(4, "testtoekn", "nvme", 3, "10.198.100.3", 9002, "/");
    hbtest_->PrepareAddChunkServer(cs);

    // chunkserver2上报copyset-1(epoch=0, peers={1,2,3,4}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 1, std::set<ChunkServerIdType>{1, 2, 3, 4});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查sheduler中的operator
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(hbtest_->topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(1, copysetInfo.GetLeader());
    ASSERT_EQ(5, copysetInfo.GetEpoch());
    ASSERT_EQ(10, copysetInfo.GetCandidate());
    std::set<ChunkServerIdType> peers{1, 2 , 3};
    ASSERT_EQ(peers, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidate_OpOnGoing_condition17) {
    PrepareMdsWithCandidateOpOnGoing();
    ChunkServer cs(4, "testtoekn", "nvme", 3, "10.198.100.3", 9002, "/");
    hbtest_->PrepareAddChunkServer(cs);

    // chunkserver2上报copyset-1(epoch=0, peers={1,2,3,4}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 0, std::set<ChunkServerIdType>{1, 2, 3, 4});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查sheduler中的operator
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(hbtest_->topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(1, copysetInfo.GetLeader());
    ASSERT_EQ(5, copysetInfo.GetEpoch());
    ASSERT_EQ(10, copysetInfo.GetCandidate());
    std::set<ChunkServerIdType> peers{1, 2 , 3};
    ASSERT_EQ(peers, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidate_OpOnGoing_condition18) {
    PrepareMdsWithCandidateOpOnGoing();

    // chunkserver10上报copyset-1(epoch=5, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(10, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 0, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查sheduler中的operator
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(hbtest_->topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(1, copysetInfo.GetLeader());
    ASSERT_EQ(5, copysetInfo.GetEpoch());
    ASSERT_EQ(10, copysetInfo.GetCandidate());
    std::set<ChunkServerIdType> peers{1, 2 , 3};
    ASSERT_EQ(peers, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidate_OpOnGoing_condition19) {
    PrepareMdsWithCandidateOpOnGoing();

    // chunkserver10上报copyset-1(epoch=5, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(10, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 1, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查sheduler中的operator
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(hbtest_->topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(1, copysetInfo.GetLeader());
    ASSERT_EQ(5, copysetInfo.GetEpoch());
    ASSERT_EQ(10, copysetInfo.GetCandidate());
    std::set<ChunkServerIdType> peers{1, 2 , 3};
    ASSERT_EQ(peers, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidate_OpOnGoing_condition20) {
    PrepareMdsWithCandidateOpOnGoing();

    // chunkserver10上报copyset-1(epoch=6, peers={1,2,3,10}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(10, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 1, std::set<ChunkServerIdType>{1, 2, 3, 10});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查sheduler中的operator
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(hbtest_->topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(1, copysetInfo.GetLeader());
    ASSERT_EQ(5, copysetInfo.GetEpoch());
    ASSERT_EQ(10, copysetInfo.GetCandidate());
    std::set<ChunkServerIdType> peers{1, 2 , 3};
    ASSERT_EQ(peers, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidate_OpOnGoing_condition21) {
    PrepareMdsWithCandidateOpOnGoing();

    // chunkserver10上报copyset-1(epoch=6, peers={1,2,3,10}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(10, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 0, std::set<ChunkServerIdType>{1, 2, 3, 10});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查sheduler中的operator
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(hbtest_->topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(1, copysetInfo.GetLeader());
    ASSERT_EQ(5, copysetInfo.GetEpoch());
    ASSERT_EQ(10, copysetInfo.GetCandidate());
    std::set<ChunkServerIdType> peers{1, 2 , 3};
    ASSERT_EQ(peers, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidate_OpOnGoing_condition22) {
    PrepareMdsWithCandidateOpOnGoing();

    // chunkserver10上报copyset-1(epoch=4, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(10, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 1, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查sheduler中的operator
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(hbtest_->topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(1, copysetInfo.GetLeader());
    ASSERT_EQ(5, copysetInfo.GetEpoch());
    ASSERT_EQ(10, copysetInfo.GetCandidate());
    std::set<ChunkServerIdType> peers{1, 2 , 3};
    ASSERT_EQ(peers, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidate_OpOnGoing_condition23) {
    PrepareMdsWithCandidateOpOnGoing();

    // chunkserver10上报copyset-1(epoch=4, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(10, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 0, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查sheduler中的operator
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(hbtest_->topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(1, copysetInfo.GetLeader());
    ASSERT_EQ(5, copysetInfo.GetEpoch());
    ASSERT_EQ(10, copysetInfo.GetCandidate());
    std::set<ChunkServerIdType> peers{1, 2 , 3};
    ASSERT_EQ(peers, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidate_OpOnGoing_condition24) {
    PrepareMdsWithCandidateOpOnGoing();
    ChunkServer cs(4, "testtoekn", "nvme", 3, "10.198.100.3", 9002, "/");
    hbtest_->PrepareAddChunkServer(cs);

    // chunkserver10上报copyset-1(epoch=4, peers={1,2,3,4}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(10, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 1, std::set<ChunkServerIdType>{1, 2, 3, 4});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查sheduler中的operator
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(hbtest_->topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(1, copysetInfo.GetLeader());
    ASSERT_EQ(5, copysetInfo.GetEpoch());
    ASSERT_EQ(10, copysetInfo.GetCandidate());
    std::set<ChunkServerIdType> peers{1, 2 , 3};
    ASSERT_EQ(peers, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidate_OpOnGoing_condition25) {
    PrepareMdsWithCandidateOpOnGoing();
    ChunkServer cs(4, "testtoekn", "nvme", 3, "10.198.100.3", 9002, "/");
    hbtest_->PrepareAddChunkServer(cs);

    // chunkserver10上报copyset-1(epoch=4, peers={1,2,3,4}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(10, &req);
        CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 0, std::set<ChunkServerIdType>{1, 2, 3, 4});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查sheduler中的operator
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(hbtest_->topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(1, copysetInfo.GetLeader());
    ASSERT_EQ(5, copysetInfo.GetEpoch());
    ASSERT_EQ(10, copysetInfo.GetCandidate());
    std::set<ChunkServerIdType> peers{1, 2 , 3};
    ASSERT_EQ(peers, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidate_OpOnGoing_condition26) {
    PrepareMdsWithCandidateOpOnGoing();

    // chunkserver10上报copyset-1(epoch=0, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(10, &req);
    hbtest_->BuildBasicChunkServerRequest(10, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 1, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查sheduler中的operator
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(hbtest_->topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(1, copysetInfo.GetLeader());
    ASSERT_EQ(5, copysetInfo.GetEpoch());
    ASSERT_EQ(10, copysetInfo.GetCandidate());
    std::set<ChunkServerIdType> peers{1, 2 , 3};
    ASSERT_EQ(peers, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidate_OpOnGoing_condition27) {
    PrepareMdsWithCandidateOpOnGoing();

    // chunkserver10上报copyset-1(epoch=0, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(10, &req);
    hbtest_->BuildBasicChunkServerRequest(10, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 0, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查sheduler中的operator
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(hbtest_->topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(1, copysetInfo.GetLeader());
    ASSERT_EQ(5, copysetInfo.GetEpoch());
    ASSERT_EQ(10, copysetInfo.GetCandidate());
    std::set<ChunkServerIdType> peers{1, 2 , 3};
    ASSERT_EQ(peers, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidate_OpOnGoing_condition28) {
    PrepareMdsWithCandidateOpOnGoing();
    ChunkServer cs(4, "testtoekn", "nvme", 3, "10.198.100.3", 9002, "/");
    hbtest_->PrepareAddChunkServer(cs);

    // chunkserver4上报copyset-1(epoch=5, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 0, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查sheduler中的operator
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    // 检查response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    ASSERT_EQ(1, rep.needupdatecopysets(0).copysetid());
    ASSERT_EQ(5, rep.needupdatecopysets(0).epoch());
    ASSERT_EQ(
        "10.198.100.1:9000:0", rep.needupdatecopysets(0).peers(0).address());
    ASSERT_EQ(
        "10.198.100.2:9000:0", rep.needupdatecopysets(0).peers(1).address());
    ASSERT_EQ(
        "10.198.100.3:9000:0", rep.needupdatecopysets(0).peers(2).address());
    // 检查copyset
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(hbtest_->topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(1, copysetInfo.GetLeader());
    ASSERT_EQ(5, copysetInfo.GetEpoch());
    ASSERT_EQ(10, copysetInfo.GetCandidate());
    std::set<ChunkServerIdType> peers{1, 2 , 3};
    ASSERT_EQ(peers, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidate_OpOnGoing_condition29) {
    PrepareMdsWithCandidateOpOnGoing();
    ChunkServer cs(4, "testtoekn", "nvme", 3, "10.198.100.3", 9002, "/");
    hbtest_->PrepareAddChunkServer(cs);

    // chunkserver4上报copyset-1(epoch=4, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 0, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查sheduler中的operator
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    // 检查response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    ASSERT_EQ(1, rep.needupdatecopysets(0).copysetid());
    ASSERT_EQ(5, rep.needupdatecopysets(0).epoch());
    ASSERT_EQ(
        "10.198.100.1:9000:0", rep.needupdatecopysets(0).peers(0).address());
    ASSERT_EQ(
        "10.198.100.2:9000:0", rep.needupdatecopysets(0).peers(1).address());
    ASSERT_EQ(
        "10.198.100.3:9000:0", rep.needupdatecopysets(0).peers(2).address());
    // 检查copyset
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(hbtest_->topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(1, copysetInfo.GetLeader());
    ASSERT_EQ(5, copysetInfo.GetEpoch());
    ASSERT_EQ(10, copysetInfo.GetCandidate());
    std::set<ChunkServerIdType> peers{1, 2 , 3};
    ASSERT_EQ(peers, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidate_OpOnGoing_condition30) {
    PrepareMdsWithCandidateOpOnGoing();
    ChunkServer cs(4, "testtoekn", "nvme", 3, "10.198.100.3", 9002, "/");
    hbtest_->PrepareAddChunkServer(cs);

    // chunkserver4上报copyset-1(epoch=0, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 0, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查sheduler中的operator
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    // 检查response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    ASSERT_EQ(1, rep.needupdatecopysets(0).copysetid());
    ASSERT_EQ(5, rep.needupdatecopysets(0).epoch());
    ASSERT_EQ(
        "10.198.100.1:9000:0", rep.needupdatecopysets(0).peers(0).address());
    ASSERT_EQ(
        "10.198.100.2:9000:0", rep.needupdatecopysets(0).peers(1).address());
    ASSERT_EQ(
        "10.198.100.3:9000:0", rep.needupdatecopysets(0).peers(2).address());
    // 检查copyset
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(hbtest_->topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(1, copysetInfo.GetLeader());
    ASSERT_EQ(5, copysetInfo.GetEpoch());
    ASSERT_EQ(10, copysetInfo.GetCandidate());
    std::set<ChunkServerIdType> peers{1, 2 , 3};
    ASSERT_EQ(peers, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_mdsWithRemoveOp_1) {
    PrepareMdsWithRemoveOp();

    // chunkserver-1上报copyset-1(epoch=5, peers={1,2,3,4}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(1, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 1, std::set<ChunkServerIdType>{1, 2, 3, 4});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    ASSERT_EQ(1, rep.needupdatecopysets(0).copysetid());
    ASSERT_EQ(5, rep.needupdatecopysets(0).epoch());
    ASSERT_EQ(
        "10.198.100.1:9000:0", rep.needupdatecopysets(0).peers(0).address());
    ASSERT_EQ(
        "10.198.100.2:9000:0", rep.needupdatecopysets(0).peers(1).address());
    ASSERT_EQ(
        "10.198.100.3:9000:0", rep.needupdatecopysets(0).peers(2).address());
    ASSERT_EQ(
        "10.198.100.3:9001:0", rep.needupdatecopysets(0).peers(3).address());
    ASSERT_EQ(ConfigChangeType::REMOVE_PEER, rep.needupdatecopysets(0).type());
    ASSERT_EQ("10.198.100.3:9001:0",
        rep.needupdatecopysets(0).configchangeitem().address());
    // 检查copyset
    ASSERT_TRUE(ValidateCopySet(csInfo));
}

TEST_F(HeartbeatBasicTest, test_mdsWithRemoveOp_2) {
    PrepareMdsWithRemoveOp();

    // chunkserver-1上报copyset-1(epoch=5, peers={1,2,3,4}, leader=1,
    //                           cofigChangeInfo={peer: 4, type:REMOVE_PEER})
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(1, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 1, std::set<ChunkServerIdType>{1, 2, 3, 4}, 4);
    hbtest_->AddCopySetToRequest(&req, csInfo, ConfigChangeType::REMOVE_PEER);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    ASSERT_TRUE(ValidateCopySet(csInfo));
}

TEST_F(HeartbeatBasicTest, test_mdsWithRemoveOp_3) {
    PrepareMdsWithRemoveOp();

    // chunkserver-1上报上报copyset-1(epoch=6, peers={1,2,3,4}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(1, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 1, std::set<ChunkServerIdType>{1, 2, 3, 4});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(6);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsWithRemoveOp_4) {
    PrepareMdsWithRemoveOp();

    // chunkserver-1上报copyset-1(epoch=6, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(1, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 1, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(6);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{1, 2 , 3});
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsWithRemoveOp_5) {
    PrepareMdsWithRemoveOp();

    // chunkserver-2上报copyset-1(epoch=7, peers={1,2,3}, leader=2)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 7, 2, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(7);
    csInfo.SetLeader(2);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{1, 2 , 3});
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsWithRemoveOp_6) {
    PrepareMdsWithRemoveOp();

    // chunkserver-2上报copyset-1(epoch=5, peers={1,2,3,4}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 1, std::set<ChunkServerIdType>{1, 2, 3, 4});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<RemovePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithRemoveOp_7) {
    PrepareMdsWithRemoveOp();

    // chunkserver-2上报copyset-1(epoch=5, peers={1,2,3,4}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 0, std::set<ChunkServerIdType>{1, 2, 3, 4});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetLeader(1);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<RemovePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithRemoveOp_8) {
    PrepareMdsWithRemoveOp();

    // chunkserver-2上报copyset-1(epoch=6, peers={1,2,3,4}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 1, std::set<ChunkServerIdType>{1, 2, 3, 4});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<RemovePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithRemoveOp_9) {
    PrepareMdsWithRemoveOp();

    // chunkserver-2上报copyset-1(epoch=6, peers={1,2,3,4}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 0, std::set<ChunkServerIdType>{1, 2, 3, 4});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<RemovePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithRemoveOp_10) {
    PrepareMdsWithRemoveOp();

    // chunkserver-2上报copyset-1(epoch=6, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 1, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{1, 2, 3, 4});
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<RemovePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithRemoveOp_11) {
    PrepareMdsWithRemoveOp();

    // chunkserver-2上报copyset-1(epoch=6, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 0, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{1, 2, 3, 4});
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<RemovePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithRemoveOp_12) {
    PrepareMdsWithRemoveOp();

    // chunkserver-2上报copyset-1(epoch=4, peers={1,2,3,4}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 1, std::set<ChunkServerIdType>{1, 2, 3, 4});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<RemovePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());

    // chunkserver-2上报copyset-1(epoch=4, peers={1,2,3,4}, leader=0)
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    BuildCopySetInfo(&csInfo, 4, 0, std::set<ChunkServerIdType>{1, 2, 3, 4});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    step = dynamic_cast<RemovePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithRemoveOp_13) {
    PrepareMdsWithRemoveOp();

    // chunkserver-2上报(epoch=4, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 1, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{1, 2, 3, 4});
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<RemovePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());

    // chunkserver-2上报(epoch=4, peers={1,2,3}, leader=0)
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    BuildCopySetInfo(&csInfo, 4, 0, std::set<ChunkServerIdType>{1, 2, 3, 4});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    step = dynamic_cast<RemovePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithRemoveOp_14) {
    PrepareMdsWithRemoveOp();

    // chunkserver-2上报copyset-1(epoch=0, peers={1,2,3,4}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 1, std::set<ChunkServerIdType>{1, 2, 3, 4});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<RemovePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());

    // chunkserver-2上报copyset-1(epoch=0, peers={1,2,3}, leader=1)
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    BuildCopySetInfo(&csInfo, 0, 1, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{1, 2, 3, 4});
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    step = dynamic_cast<RemovePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithRemoveOp_15) {
    PrepareMdsWithRemoveOp();

    // chunkserver-2上报copyset-1(epoch=0, peers={1,2,3,4}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 0, std::set<ChunkServerIdType>{1, 2, 3, 4});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<RemovePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());

    // chunkserver-2上报copyset-1(epoch=0, peers={1,2,3}, leader=0)
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    BuildCopySetInfo(&csInfo, 0, 0, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{1, 2, 3, 4});
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
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

    // 非复制组成员chunkserver-5上报
    // copyset-1(epoch=5, peers={1,2,3,4}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(5, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 0, std::set<ChunkServerIdType>{1, 2, 3, 4});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
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
    // 检查copyset
    csInfo.SetLeader(1);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
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

    // 非复制组成员chunkserver-5上报
    // copyset-1(epoch=4, peers={1,2,3,4}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(5, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 0, std::set<ChunkServerIdType>{1, 2, 3, 4});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
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
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
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

    // 非复制组成员chunkserver-5上报
    // copyset-1(epoch=0, peers={1,2,3,4}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(5, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 0, std::set<ChunkServerIdType>{1, 2, 3, 4});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
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
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
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

    // 非复制组成员chunkserver-5上报
    // copyset-1(epoch=0, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(5, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 0, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
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
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{1, 2, 3, 4});
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<RemovePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithRemoveOpOnGoing_1) {
    PrepareMdsWithRemoveOpOnGoing();
    // chunkserver-1上报copyset-1(epoch=5, peers={1,2,3,4}, leader=1,
    //                          configChangeInfo={peer: 4, type: REMOVE_PEER})
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(1, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 1, std::set<ChunkServerIdType>{1, 2, 3, 4}, 4);
    hbtest_->AddCopySetToRequest(&req, csInfo, ConfigChangeType::REMOVE_PEER);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<RemovePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithRemoveOpOnGoing_2) {
    PrepareMdsWithRemoveOpOnGoing();
    // chunkserver-1上报copyset-1(epoch=6, peers={1,2,3,4}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(1, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 1, std::set<ChunkServerIdType>{1, 2, 3, 4});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(6);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsWithRemoveOpOnGoing_3) {
    PrepareMdsWithRemoveOpOnGoing();
    // chunkserver-2上报copyset-1(epoch=6, peers={1,2,3,4}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 2, std::set<ChunkServerIdType>{1, 2, 3, 4});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsWithRemoveOpOnGoing_4) {
    PrepareMdsWithRemoveOpOnGoing();
    // chunkserver-2上报copyset-1(epoch=6, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 2, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsWithRemoveOpOnGoing_5) {
    PrepareMdsWithRemoveOpOnGoing();
    // chunkserver-2上报copyset-1(epoch=7, peers={1,2,3}, leader=2)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 7, 2, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsWithRemoveOpOnGoing_6) {
    PrepareMdsWithRemoveOpOnGoing();
    // chunkserver-2上报copyset-1(epoch=5, peers={1,2,3,4}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 1, std::set<ChunkServerIdType>{1, 2, 3, 4});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<RemovePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithRemoveOpOnGoing_7) {
    PrepareMdsWithRemoveOpOnGoing();
    // chunkserver-2上报copyset-1(epoch=5, peers={1,2,3,4}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 0, std::set<ChunkServerIdType>{1, 2, 3, 4});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetLeader(1);
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<RemovePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithRemoveOpOnGoing_8) {
    PrepareMdsWithRemoveOpOnGoing();
    // chunkserver-2上报copyset-1(epoch=6, peers={1,2,3,4}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 1, std::set<ChunkServerIdType>{1, 2, 3, 4});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{1, 2, 3, 4});
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<RemovePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithRemoveOpOnGoing_9) {
    PrepareMdsWithRemoveOpOnGoing();
    // chunkserver-2上报copyset-1(epoch=6, peers={1,2,3,4}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 0, std::set<ChunkServerIdType>{1, 2, 3, 4});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{1, 2, 3, 4});
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<RemovePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithRemoveOpOnGoing_10) {
    PrepareMdsWithRemoveOpOnGoing();
    // chunkserver-2上报copyset-1(epoch=6, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 1, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{1, 2, 3, 4});
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<RemovePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithRemoveOpOnGoing_11) {
    PrepareMdsWithRemoveOpOnGoing();
    // chunkserver-2上报copyset-1(epoch=6, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 0, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{1, 2, 3, 4});
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<RemovePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithRemoveOpOnGoing_12) {
    PrepareMdsWithRemoveOpOnGoing();
    // chunkserver-2上报copyset-1(epoch=4, peers={1,2,3,4}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 1, std::set<ChunkServerIdType>{1, 2, 3, 4});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{1, 2, 3, 4});
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<RemovePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithRemoveOpOnGoing_13) {
    PrepareMdsWithRemoveOpOnGoing();
    // chunkserver-2上报copyset-1(epoch=4, peers={1,2,3,4}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 0, std::set<ChunkServerIdType>{1, 2, 3, 4});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<RemovePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithRemoveOpOnGoing_14) {
    PrepareMdsWithRemoveOpOnGoing();
    // chunkserver-2上报copyset-1(epoch=4, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 1, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{1, 2, 3, 4});
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<RemovePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithRemoveOpOnGoing_15) {
    PrepareMdsWithRemoveOpOnGoing();
    // chunkserver-2上报copyset-1(epoch=4, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 0, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{1, 2, 3, 4});
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<RemovePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithRemoveOpOnGoing_16) {
    PrepareMdsWithRemoveOpOnGoing();
    // chunkserver-2上报copyset-1(epoch=0, peers={1,2,3,4}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 1, std::set<ChunkServerIdType>{1, 2, 3, 4});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<RemovePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithRemoveOpOnGoing_17) {
    PrepareMdsWithRemoveOpOnGoing();
    // chunkserver-2上报copyset-1(epoch=0, peers={1,2,3,4}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 0, std::set<ChunkServerIdType>{1, 2, 3, 4});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<RemovePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithRemoveOpOnGoing_18) {
    PrepareMdsWithRemoveOpOnGoing();
    // chunkserver-2上报copyset-1(epoch=0, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 1, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{1, 2, 3, 4});
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<RemovePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithRemoveOpOnGoing_19) {
    PrepareMdsWithRemoveOpOnGoing();
    // chunkserver-2上报copyset-1(epoch=0, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 0, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{1, 2, 3, 4});
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<RemovePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithRemoveOpOnGoing_20) {
    PrepareMdsWithRemoveOpOnGoing();
    // chunkserver-4上报copyset-1(epoch=5, peers={1,2,3,4}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 1, std::set<ChunkServerIdType>{1, 2, 3, 4});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<RemovePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithRemoveOpOnGoing_21) {
    PrepareMdsWithRemoveOpOnGoing();
    // chunkserver-4上报copyset-1(epoch=5, peers={1,2,3,4}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 0, std::set<ChunkServerIdType>{1, 2, 3, 4});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetLeader(1);
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<RemovePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithRemoveOpOnGoing_22) {
    PrepareMdsWithRemoveOpOnGoing();
    // chunkserver-4上报copyset-1(epoch=6, peers={1,2,3,4}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 1, std::set<ChunkServerIdType>{1, 2, 3, 4});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<RemovePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithRemoveOpOnGoing_23) {
    PrepareMdsWithRemoveOpOnGoing();
    // chunkserver-4上报copyset-1(epoch=6, peers={1,2,3,4}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 0, std::set<ChunkServerIdType>{1, 2, 3, 4});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<RemovePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithRemoveOpOnGoing_24) {
    PrepareMdsWithRemoveOpOnGoing();
    // chunkserver-4上报copyset-1(epoch=6, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 1, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{1, 2, 3, 4});
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<RemovePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithRemoveOpOnGoing_25) {
    PrepareMdsWithRemoveOpOnGoing();
    // chunkserver-4上报copyset-1(epoch=6, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 0, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{1, 2, 3, 4});
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<RemovePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithRemoveOpOnGoing_26) {
    PrepareMdsWithRemoveOpOnGoing();
    // chunkserver-4上报copyset-1(epoch=4, peers={1,2,3,4}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 1, std::set<ChunkServerIdType>{1, 2, 3, 4});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{1, 2, 3, 4});
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<RemovePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithRemoveOpOnGoing_27) {
    PrepareMdsWithRemoveOpOnGoing();
    // chunkserver-4上报copyset-1(epoch=4, peers={1,2,3,4}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 0, std::set<ChunkServerIdType>{1, 2, 3, 4});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<RemovePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithRemoveOpOnGoing_28) {
    PrepareMdsWithRemoveOpOnGoing();
    // chunkserver-4上报copyset-1(epoch=4, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 1, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{1, 2, 3, 4});
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<RemovePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithRemoveOpOnGoing_29) {
    PrepareMdsWithRemoveOpOnGoing();
    // chunkserver-4上报copyset-1(epoch=4, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 0, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{1, 2, 3, 4});
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<RemovePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithRemoveOpOnGoing_30) {
    PrepareMdsWithRemoveOpOnGoing();
    // chunkserver-4上报copyset-1(epoch=0, peers={1,2,3,4}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 1, std::set<ChunkServerIdType>{1, 2, 3, 4});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<RemovePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithRemoveOpOnGoing_31) {
    PrepareMdsWithRemoveOpOnGoing();
    // chunkserver-4上报copyset-1(epoch=0, peers={1,2,3,4}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 0, std::set<ChunkServerIdType>{1, 2, 3, 4});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<RemovePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithRemoveOpOnGoing_32) {
    PrepareMdsWithRemoveOpOnGoing();
    // chunkserver-4上报copyset-1(epoch=0, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 1, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{1, 2, 3, 4});
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<RemovePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithRemoveOpOnGoing_33) {
    PrepareMdsWithRemoveOpOnGoing();
    // chunkserver-4上报copyset-1(epoch=0, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 0, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{1, 2, 3, 4});
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
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

    // 非复制组成员chunkserver-5上报
    // copyset-1(epoch=5, peers={1,2,3,4}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(5, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 0, std::set<ChunkServerIdType>{1, 2, 3, 4});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
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
    // 检查copyset
    csInfo.SetLeader(1);
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
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

    // 非复制组成员chunkserver-5上报
    // copyset-1(epoch=4, peers={1,2,3,4}, leader=0）
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(5, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 0, std::set<ChunkServerIdType>{1, 2, 3, 4});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
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
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<RemovePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());

    // 非复制组成员chunkserver-5上报
    // copyset-1(epoch=4, peers={1,2,3}, leader=0）
    rep.Clear();
    req.Clear();
    hbtest_->BuildBasicChunkServerRequest(5, &req);
    BuildCopySetInfo(&csInfo, 4, 0, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    // 检查response
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
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{1, 2, 3, 4});
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
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

    // 非复制组成员chunkserver-5上报
    // copyset-1(epoch=0, peers={1,2,3,4}, leader=0
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(5, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 0, std::set<ChunkServerIdType>{1, 2, 3, 4});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    CopySetConf conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(4, conf.peers_size());
    ASSERT_EQ(5, conf.epoch());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<RemovePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());

    // 非复制组成员chunkserver-5上报
    // copyset-1(epoch=0, peers={1,2,3}, leader=0）
    rep.Clear();
    req.Clear();
    hbtest_->BuildBasicChunkServerRequest(5, &req);
    BuildCopySetInfo(&csInfo, 0, 0, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    // 检查response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(4, conf.peers_size());
    ASSERT_EQ(5, conf.epoch());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{1, 2, 3, 4});
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    step = dynamic_cast<RemovePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithTransferOp_1) {
    PrepareMdsWithTransferOp();
    // chunkserver-1上报copyset-1(epoch=5, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(1, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 1, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    // 检查response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    ASSERT_EQ(1, rep.needupdatecopysets(0).copysetid());
    ASSERT_EQ(5, rep.needupdatecopysets(0).epoch());
    ASSERT_EQ(
        "10.198.100.1:9000:0", rep.needupdatecopysets(0).peers(0).address());
    ASSERT_EQ(
        "10.198.100.2:9000:0", rep.needupdatecopysets(0).peers(1).address());
    ASSERT_EQ(
        "10.198.100.3:9000:0", rep.needupdatecopysets(0).peers(2).address());
    ASSERT_EQ(ConfigChangeType::TRANSFER_LEADER,
                rep.needupdatecopysets(0).type());
    ASSERT_EQ("10.198.100.2:9000:0",
        rep.needupdatecopysets(0).configchangeitem().address());
    // 检查copyset
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<TransferLeader *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(2, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithTransferOp_2) {
    PrepareMdsWithTransferOp();
    // chunkserver-1上报copyset-1(epoch=5, peers={1,2,3}, leader=1)
    //                  configChangeInfo={peer: 2, type: TRANSFER_LEADER})
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(1, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 1, std::set<ChunkServerIdType>{1, 2, 3}, 2);
    hbtest_->AddCopySetToRequest(&req, csInfo,
                                ConfigChangeType::TRANSFER_LEADER);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<TransferLeader *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(2, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithTransferOp_3) {
    PrepareMdsWithTransferOp();
    // chunkserver-1上报copyset-1(epoch=6, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(1, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 1, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsWithTransferOp_4) {
    PrepareMdsWithTransferOp();
    // chunkserver-2上报copyset-1(epoch=6, peers={1,2,3}, leader=2)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 2, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsWithTransferOp_5) {
    PrepareMdsWithTransferOp();
    // chunkserver-2上报copyset-1(epoch=5, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 1, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<TransferLeader *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(2, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithTransferOp_6) {
    PrepareMdsWithTransferOp();
    // chunkserver-2上报copyset-1(epoch=5, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 0, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetLeader(1);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<TransferLeader *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(2, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithTransferOp_7) {
    PrepareMdsWithTransferOp();
    // chunkserver-2上报copyset-1(epoch=6, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 1, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<TransferLeader *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(2, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithTransferOp_8) {
    PrepareMdsWithTransferOp();
    // chunkserver-2上报copyset-1(epoch=6, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 0, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<TransferLeader *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(2, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithTransferOp_9) {
    PrepareMdsWithTransferOp();
    // chunkserver-2上报copyset-1(epoch=4, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 1, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<TransferLeader *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(2, step->GetTargetPeer());

    // chunkserver-2上报copyset-1(epoch=4, peers={1,2,3,4}, leader=1)
    ChunkServer cs(4, "testtoken", "nvme", 3, "10.198.100.3", 9001, "/");
    hbtest_->PrepareAddChunkServer(cs);
    rep.Clear();
    req.Clear();
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    BuildCopySetInfo(&csInfo, 4, 1, std::set<ChunkServerIdType>{1, 2, 3, 4});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{1, 2, 3});
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    step = dynamic_cast<TransferLeader *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(2, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithTransferOp_10) {
    PrepareMdsWithTransferOp();
    // chunkserver-2上报copyset-1(epoch=4, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 0, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<TransferLeader *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(2, step->GetTargetPeer());

    // chunkserver-2上报copyset-1(epoch=4, peers={1,2,3,4}, leader=0)
    ChunkServer cs(4, "testtoken", "nvme", 3, "10.198.100.3", 9001, "/");
    hbtest_->PrepareAddChunkServer(cs);
    rep.Clear();
    req.Clear();
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    BuildCopySetInfo(&csInfo, 4, 0, std::set<ChunkServerIdType>{1, 2, 3, 4});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{1, 2, 3});
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    step = dynamic_cast<TransferLeader *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(2, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithTransferOp_11) {
    PrepareMdsWithTransferOp();
    // chunkserver-2上报copyset-1(epoch=0, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 1, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<TransferLeader *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(2, step->GetTargetPeer());

    // chunkserver-2上报copyset-1(epoch=0, peers={1,2,3,4}, leader=1)
    ChunkServer cs(4, "testtoken", "nvme", 3, "10.198.100.3", 9001, "/");
    hbtest_->PrepareAddChunkServer(cs);
    rep.Clear();
    req.Clear();
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    BuildCopySetInfo(&csInfo, 0, 1, std::set<ChunkServerIdType>{1, 2, 3, 4});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{1, 2, 3});
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    step = dynamic_cast<TransferLeader *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(2, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithTransferOp_12) {
    PrepareMdsWithTransferOp();
    // chunkserver-2上报copyset-1(epoch=0, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 0, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<TransferLeader *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(2, step->GetTargetPeer());

    // chunkserver-2上报copyset-1(epoch=0, peers={1,2,3,4}, leader=0)
    ChunkServer cs(4, "testtoken", "nvme", 3, "10.198.100.3", 9001, "/");
    hbtest_->PrepareAddChunkServer(cs);
    rep.Clear();
    req.Clear();
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    BuildCopySetInfo(&csInfo, 0, 0, std::set<ChunkServerIdType>{1, 2, 3, 4});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{1, 2, 3});
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
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
    // chunkserver-4上报copyset-1(epoch=5, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 0, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    // 检查response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    auto conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(3, conf.peers_size());
    ASSERT_EQ("10.198.100.1:9000:0", conf.peers(0).address());
    ASSERT_EQ("10.198.100.2:9000:0", conf.peers(1).address());
    ASSERT_EQ("10.198.100.3:9000:0", conf.peers(2).address());
    ASSERT_EQ(5, conf.epoch());
    // 检查copyset
    csInfo.SetLeader(1);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
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
    // chunkserver-4上报copyset-1(epoch=4, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 0, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    // 检查response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    auto conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(3, conf.peers_size());
    ASSERT_EQ("10.198.100.1:9000:0", conf.peers(0).address());
    ASSERT_EQ("10.198.100.2:9000:0", conf.peers(1).address());
    ASSERT_EQ("10.198.100.3:9000:0", conf.peers(2).address());
    ASSERT_EQ(5, conf.epoch());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
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
    // chunkserver-5上报copyset-1(epoch=4, peers={1,2,3,4}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(5, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 0, std::set<ChunkServerIdType>{1, 2, 3, 4});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    // 检查response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    auto conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(3, conf.peers_size());
    ASSERT_EQ("10.198.100.1:9000:0", conf.peers(0).address());
    ASSERT_EQ("10.198.100.2:9000:0", conf.peers(1).address());
    ASSERT_EQ("10.198.100.3:9000:0", conf.peers(2).address());
    ASSERT_EQ(5, conf.epoch());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{1, 2, 3});
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
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

    // chunkserver-5上报copyset-1(epoch=0, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(5, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 0, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    // 检查response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    auto conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(3, conf.peers_size());
    ASSERT_EQ("10.198.100.1:9000:0", conf.peers(0).address());
    ASSERT_EQ("10.198.100.2:9000:0", conf.peers(1).address());
    ASSERT_EQ("10.198.100.3:9000:0", conf.peers(2).address());
    ASSERT_EQ(5, conf.epoch());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<TransferLeader *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(2, step->GetTargetPeer());

    // chunkserver-5上报copyset-1(epoch=0, peers={1,2,3,4}, leader=0)
    req.Clear();
    rep.Clear();
    hbtest_->BuildBasicChunkServerRequest(5, &req);
    BuildCopySetInfo(&csInfo, 0, 0, std::set<ChunkServerIdType>{1, 2, 3, 4});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    // 检查response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(3, conf.peers_size());
    ASSERT_EQ("10.198.100.1:9000:0", conf.peers(0).address());
    ASSERT_EQ("10.198.100.2:9000:0", conf.peers(1).address());
    ASSERT_EQ("10.198.100.3:9000:0", conf.peers(2).address());
    ASSERT_EQ(5, conf.epoch());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{1, 2, 3});
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    step = dynamic_cast<TransferLeader *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(2, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithTransferOpOnGoing_1) {
    PrepareMdsWithTransferOpOnGoing();
    // chunkserver-1上报copyset-1(epoch=5, peers={1,2,3}, leader=1,
    //                  configChangeInfo={peer: 2, type: TRANSFER_LEADER})
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(1, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 1, std::set<ChunkServerIdType>{1, 2, 3}, 2);
    hbtest_->AddCopySetToRequest(&req, csInfo,
                                ConfigChangeType::TRANSFER_LEADER);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<TransferLeader *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(2, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithTransferOpOnGoing_2) {
    PrepareMdsWithTransferOpOnGoing();
    // chunkserver-1上报copyset-1(epoch=6, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(1, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 1, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsWithTransferOpOnGoing_3) {
    PrepareMdsWithTransferOpOnGoing();
    // chunkserver-2上报copyset-1(epoch=6, peers={1,2,3}, leader=2)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 2, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsWithTransferOpOnGoing_4) {
    PrepareMdsWithTransferOpOnGoing();
    // chunkserver-2上报copyset-1(epoch=5, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 1, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetCandidate(2);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<TransferLeader *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(2, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithTransferOpOnGoing_5) {
    PrepareMdsWithTransferOpOnGoing();
    // chunkserver-2上报copyset-1(epoch=5, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 0, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetLeader(1);
    csInfo.SetCandidate(2);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<TransferLeader *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(2, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithTransferOpOnGoing_6) {
    PrepareMdsWithTransferOpOnGoing();
    // chunkserver-2上报copyset-1(epoch=6, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 1, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetCandidate(2);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<TransferLeader *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(2, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithTransferOpOnGoing_7) {
    PrepareMdsWithTransferOpOnGoing();
    // chunkserver-2上报copyset-1(epoch=4, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 1, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetCandidate(2);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<TransferLeader *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(2, step->GetTargetPeer());

    // chunkserver-2上报copyset-1(epoch=4, peers={1,2,3,4}, leader=1)
    ChunkServer cs(4, "testtoken", "nvme", 3, "10.198.100.3", 9001, "/");
    hbtest_->PrepareAddChunkServer(cs);
    rep.Clear();
    req.Clear();
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    BuildCopySetInfo(&csInfo, 4, 1, std::set<ChunkServerIdType>{1, 2, 3, 4});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetCandidate(2);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{1, 2, 3});
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    step = dynamic_cast<TransferLeader *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(2, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithTransferOpOnGoing_8) {
    PrepareMdsWithTransferOpOnGoing();
    // chunkserver-2上报copyset-1(epoch=4, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 0, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCandidate(2);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<TransferLeader *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(2, step->GetTargetPeer());

    // chunkserver-2上报copyset-1(epoch=4, peers={1,2,3,4}, leader=0)
    ChunkServer cs(4, "testtoken", "nvme", 3, "10.198.100.3", 9001, "/");
    hbtest_->PrepareAddChunkServer(cs);
    rep.Clear();
    req.Clear();
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    BuildCopySetInfo(&csInfo, 4, 0, std::set<ChunkServerIdType>{1, 2, 3, 4});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCandidate(2);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{1, 2, 3});
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    step = dynamic_cast<TransferLeader *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(2, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithTransferOpOnGoing_9) {
    PrepareMdsWithTransferOpOnGoing();
    // chunkserver-2上报copyset-1(epoch=0, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 1, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetCandidate(2);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<TransferLeader *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(2, step->GetTargetPeer());

    // chunkserver-2上报copyset-1(epoch=0, peers={1,2,3,4}, leader=1)
    ChunkServer cs(4, "testtoken", "nvme", 3, "10.198.100.3", 9001, "/");
    hbtest_->PrepareAddChunkServer(cs);
    rep.Clear();
    req.Clear();
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    BuildCopySetInfo(&csInfo, 0, 1, std::set<ChunkServerIdType>{1, 2, 3, 4});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetCandidate(2);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{1, 2, 3});
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    step = dynamic_cast<TransferLeader *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(2, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithTransferOpOnGoing_10) {
    PrepareMdsWithTransferOpOnGoing();
    // chunkserver-2上报copyset-1(epoch=0, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 0, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCandidate(2);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<TransferLeader *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(2, step->GetTargetPeer());

    // chunkserver-2上报copyset-1(epoch=0, peers={1,2,3,4}, leader=0)
    ChunkServer cs(4, "testtoken", "nvme", 3, "10.198.100.3", 9001, "/");
    hbtest_->PrepareAddChunkServer(cs);
    rep.Clear();
    req.Clear();
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    BuildCopySetInfo(&csInfo, 0, 0, std::set<ChunkServerIdType>{1, 2, 3, 4});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCandidate(2);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{1, 2, 3});
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
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
    // chunkserver-4上报copyset-1(epoch=5, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 0, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    // 检查response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    auto conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(3, conf.peers_size());
    ASSERT_EQ("10.198.100.1:9000:0", conf.peers(0).address());
    ASSERT_EQ("10.198.100.2:9000:0", conf.peers(1).address());
    ASSERT_EQ("10.198.100.3:9000:0", conf.peers(2).address());
    ASSERT_EQ(5, conf.epoch());
    // 检查copyset
    csInfo.SetLeader(1);
    csInfo.SetCandidate(2);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
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
    // chunkserver-4上报copyset-1(epoch=4, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 0, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    // 检查response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    auto conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(3, conf.peers_size());
    ASSERT_EQ("10.198.100.1:9000:0", conf.peers(0).address());
    ASSERT_EQ("10.198.100.2:9000:0", conf.peers(1).address());
    ASSERT_EQ("10.198.100.3:9000:0", conf.peers(2).address());
    ASSERT_EQ(5, conf.epoch());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCandidate(2);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
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

    // chunkserver-5上报copyset-1(epoch=0, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(5, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 0, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    // 检查response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    auto conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(3, conf.peers_size());
    ASSERT_EQ("10.198.100.1:9000:0", conf.peers(0).address());
    ASSERT_EQ("10.198.100.2:9000:0", conf.peers(1).address());
    ASSERT_EQ("10.198.100.3:9000:0", conf.peers(2).address());
    ASSERT_EQ(5, conf.epoch());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCandidate(2);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<TransferLeader *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(2, step->GetTargetPeer());

    // chunkserver-5上报copyset-1(epoch=0, peers={1,2,3,4}, leader=0)
    req.Clear();
    rep.Clear();
    hbtest_->BuildBasicChunkServerRequest(5, &req);
    BuildCopySetInfo(&csInfo, 0, 0, std::set<ChunkServerIdType>{1, 2, 3, 4});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    // 检查response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(3, conf.peers_size());
    ASSERT_EQ("10.198.100.1:9000:0", conf.peers(0).address());
    ASSERT_EQ("10.198.100.2:9000:0", conf.peers(1).address());
    ASSERT_EQ("10.198.100.3:9000:0", conf.peers(2).address());
    ASSERT_EQ(5, conf.epoch());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCandidate(2);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{1, 2, 3});
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    step = dynamic_cast<TransferLeader *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(2, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidateNoOp_1) {
    PrePareMdsWithCandidateNoOp();
    // chunkserver-1上报copyset-1(epoch=5, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(1, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 1, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidateNoOp_2) {
    PrePareMdsWithCandidateNoOp();
    // chunkserver-1上报copyset-1(epoch=5, peers={1,2,3}, leader=1)
    //                          configChangeInfo={peer: 4, type: ADD_PEER})
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(1, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 1, std::set<ChunkServerIdType>{1, 2, 3}, 4);
    hbtest_->AddCopySetToRequest(&req, csInfo, ConfigChangeType::ADD_PEER);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidateNoOp_3) {
    PrePareMdsWithCandidateNoOp();
    // chunkserver-1上报copyset-1(epoch=6, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(1, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 1, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidateNoOp_4) {
    PrePareMdsWithCandidateNoOp();
    // chunkserver-2上报copyset-1(epoch=6, peers={1,2,3}, leader=2)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 2, std::set<ChunkServerIdType>{1, 2, 3});

    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}


TEST_F(HeartbeatBasicTest, test_mdsWithCandidateNoOp_5) {
    PrePareMdsWithCandidateNoOp();
    // chunkserver-1上报copyset-1(epoch=6, peers={1,2,3,4}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(1, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 1, std::set<ChunkServerIdType>{1, 2, 3, 4});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidateNoOp_6) {
    PrePareMdsWithCandidateNoOp();
    // chunkserver-2上报copyset-1(epoch=7, peers={1,2,3,4}, leader=2)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 7, 2, std::set<ChunkServerIdType>{1, 2, 3, 4});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidateNoOp_7) {
    PrePareMdsWithCandidateNoOp();
    // chunkserver-2上报copyset-1(epoch=5, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 1, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidateNoOp_8) {
    PrePareMdsWithCandidateNoOp();
    // chunkserver-2上报copyset-1(epoch=5, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 0, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetLeader(1);
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidateNoOp_9) {
    PrePareMdsWithCandidateNoOp();
    // chunkserver-2上报copyset-1(epoch=6, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 1, std::set<ChunkServerIdType>{1, 2, 3});

    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidateNoOp_10) {
    PrePareMdsWithCandidateNoOp();
    // chunkserver-2上报copyset-1(epoch=6, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 0, std::set<ChunkServerIdType>{1, 2, 3});

    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidateNoOp_11) {
    PrePareMdsWithCandidateNoOp();
    // chunkserver-2上报copyset-1(epoch=6, peers={1,2,3,4}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 1, std::set<ChunkServerIdType>{1, 2, 3, 4});

    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetCandidate(4);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{1, 2, 3});
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidateNoOp_12) {
    PrePareMdsWithCandidateNoOp();
    // chunkserver-2上报copyset-1(epoch=6, peers={1,2,3,4}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 0, std::set<ChunkServerIdType>{1, 2, 3, 4});

    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCandidate(4);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{1, 2, 3});
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidateNoOp_13) {
    PrePareMdsWithCandidateNoOp();
    // chunkserver-2上报copyset-1(epoch=4, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 1, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCandidate(4);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{1, 2, 3});
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());

    // chunkserver-2上报copyset-1(epoch=4, peers={1,2,3,4}, leader=1)
    req.Clear();
    rep.Clear();
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    BuildCopySetInfo(&csInfo, 4, 1, std::set<ChunkServerIdType>{1, 2, 3, 4});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetCandidate(4);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{1, 2, 3});
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidateNoOp_14) {
    PrePareMdsWithCandidateNoOp();
    // chunkserver-2上报copyset-1(epoch=4, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 0, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());

    // chunkserver-2上报copyset-1(epoch=4, peers={1,2,3,4}, leader=0)
    req.Clear();
    rep.Clear();
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    BuildCopySetInfo(&csInfo, 4, 0, std::set<ChunkServerIdType>{1, 2, 3, 4});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCandidate(4);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{1, 2, 3});
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidateNoOp_15) {
    PrePareMdsWithCandidateNoOp();
    // chunkserver-2上报copyset-1(epoch=0, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 1, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());

    // chunkserver-2上报copyset-1(epoch=0, peers={1,2,3,4}, leader=1)
    req.Clear();
    rep.Clear();
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    BuildCopySetInfo(&csInfo, 0, 1, std::set<ChunkServerIdType>{1, 2, 3, 4});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetCandidate(4);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{1, 2, 3});
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidateNoOp_16) {
    PrePareMdsWithCandidateNoOp();
    // chunkserver-2上报copyset-1(epoch=0, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 0, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());

    // chunkserver-2上报copyset-1(epoch=0, peers={1,2,3,4}, leader=0)
    req.Clear();
    rep.Clear();
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    BuildCopySetInfo(&csInfo, 0, 0, std::set<ChunkServerIdType>{1, 2, 3, 4});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCandidate(4);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{1, 2, 3});
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidateNoOp_17) {
    PrePareMdsWithCandidateNoOp();
    // chunkserver-4上报copyset-1(epoch=5, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 1, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}


TEST_F(HeartbeatBasicTest, test_mdsWithCandidateNoOp_18) {
    PrePareMdsWithCandidateNoOp();
    // chunkserver-4上报copyset-1(epoch=5, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 0, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetLeader(1);
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidateNoOp_19) {
    PrePareMdsWithCandidateNoOp();
    // chunkserver-4上报copyset-1(epoch=6, peers={1,2,3,4}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 1, std::set<ChunkServerIdType>{1, 2, 3, 4});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetCandidate(4);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{1, 2, 3});
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}


TEST_F(HeartbeatBasicTest, test_mdsWithCandidateNoOp_20) {
    PrePareMdsWithCandidateNoOp();
    // chunkserver-4上报copyset-1(epoch=6, peers={1,2,3,4}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 0, std::set<ChunkServerIdType>{1, 2, 3, 4});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCandidate(4);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{1, 2, 3});
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidateNoOp_21) {
    PrePareMdsWithCandidateNoOp();
    // chunkserver-4上报copyset-1(epoch=4, peers={1,2,3,4}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 1, std::set<ChunkServerIdType>{1, 2, 3, 4});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetCandidate(4);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{1, 2, 3});
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidateNoOp_22) {
    PrePareMdsWithCandidateNoOp();
    // chunkserver-4上报copyset-1(epoch=4, peers={1,2,3,4}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 0, std::set<ChunkServerIdType>{1, 2, 3, 4});

    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCandidate(4);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{1, 2, 3});
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidateNoOp_23) {
    PrePareMdsWithCandidateNoOp();
    // chunkserver-4上报copyset-1(epoch=0, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 1, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());

    // chunkserver-4上报copyset-1(epoch=0, peers={1,2,3,5}, leader=1)
    ChunkServer cs(5, "testtoken", "nvme", 3, "10.198.100.3", 9002, "/");
    hbtest_->PrepareAddChunkServer(cs);
    req.Clear();
    rep.Clear();
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    BuildCopySetInfo(&csInfo, 0, 1, std::set<ChunkServerIdType>{1, 2, 3, 5});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetCandidate(4);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{1, 2, 3});
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidateNoOp_24) {
    PrePareMdsWithCandidateNoOp();
    // chunkserver-4上报copyset-1(epoch=0, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 0, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());

    // chunkserver-4上报copyset-1(epoch=0, peers={1,2,3,5}, leader=0)
    ChunkServer cs(5, "testtoken", "nvme", 3, "10.198.100.3", 9002, "/");
    hbtest_->PrepareAddChunkServer(cs);
    req.Clear();
    rep.Clear();
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    BuildCopySetInfo(&csInfo, 0, 0, std::set<ChunkServerIdType>{1, 2, 3, 5});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCandidate(4);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{1, 2, 3});
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidateNoOp_25) {
    PrePareMdsWithCandidateNoOp();
    ChunkServer cs(5, "testtoken", "nvme", 3, "10.198.100.3", 9002, "/");
    hbtest_->PrepareAddChunkServer(cs);
    // chunkserver-5上报copyset-1(epoch=5, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(5, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 0, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    auto conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(3, conf.peers_size());
    ASSERT_EQ("10.198.100.1:9000:0", conf.peers(0).address());
    ASSERT_EQ("10.198.100.2:9000:0", conf.peers(1).address());
    ASSERT_EQ("10.198.100.3:9000:0", conf.peers(2).address());
    ASSERT_EQ(5, conf.epoch());
    // 检查copyset
    csInfo.SetLeader(1);
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidateNoOp_26) {
    PrePareMdsWithCandidateNoOp();
    ChunkServer cs(5, "testtoken", "nvme", 3, "10.198.100.3", 9002, "/");
    hbtest_->PrepareAddChunkServer(cs);
    // chunkserver-5上报copyset-1(epoch=4, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(5, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 0, std::set<ChunkServerIdType>{1, 2, 3});

    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    auto conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(3, conf.peers_size());
    ASSERT_EQ("10.198.100.1:9000:0", conf.peers(0).address());
    ASSERT_EQ("10.198.100.2:9000:0", conf.peers(1).address());
    ASSERT_EQ("10.198.100.3:9000:0", conf.peers(2).address());
    ASSERT_EQ(5, conf.epoch());
    // 检查copyset
    csInfo.SetLeader(1);
    csInfo.SetEpoch(5);
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidateNoOp_27) {
    PrePareMdsWithCandidateNoOp();
    ChunkServer cs(5, "testtoken", "nvme", 3, "10.198.100.3", 9002, "/");
    hbtest_->PrepareAddChunkServer(cs);
    // chunkserver-5上报copyset-1(epoch=4, peers={1,2,3,4}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(5, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 0, std::set<ChunkServerIdType>{1, 2, 3, 4});

    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    auto conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(3, conf.peers_size());
    ASSERT_EQ("10.198.100.1:9000:0", conf.peers(0).address());
    ASSERT_EQ("10.198.100.2:9000:0", conf.peers(1).address());
    ASSERT_EQ("10.198.100.3:9000:0", conf.peers(2).address());
    ASSERT_EQ(5, conf.epoch());
    // 检查copyset
    csInfo.SetLeader(1);
    csInfo.SetEpoch(5);
    csInfo.SetCandidate(4);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{1, 2, 3});
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidateNoOp_28) {
    PrePareMdsWithCandidateNoOp();
    ChunkServer cs(5, "testtoken", "nvme", 3, "10.198.100.3", 9002, "/");
    hbtest_->PrepareAddChunkServer(cs);
    // chunkserver-5上报copyset-1(epoch=0, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(5, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 0, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    // 检查response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    auto conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(3, conf.peers_size());
    ASSERT_EQ("10.198.100.1:9000:0", conf.peers(0).address());
    ASSERT_EQ("10.198.100.2:9000:0", conf.peers(1).address());
    ASSERT_EQ("10.198.100.3:9000:0", conf.peers(2).address());
    ASSERT_EQ(5, conf.epoch());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());

    // chunkserver-4上报copyset-1(epoch=0, peers={1,2,3,4}, leader=0)
    req.Clear();
    rep.Clear();
    hbtest_->BuildBasicChunkServerRequest(5, &req);
    BuildCopySetInfo(&csInfo, 0, 0, std::set<ChunkServerIdType>{1, 2, 3, 4});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    // 检查response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(3, conf.peers_size());
    ASSERT_EQ("10.198.100.1:9000:0", conf.peers(0).address());
    ASSERT_EQ("10.198.100.2:9000:0", conf.peers(1).address());
    ASSERT_EQ("10.198.100.3:9000:0", conf.peers(2).address());
    ASSERT_EQ(5, conf.epoch());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCandidate(4);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{1, 2, 3});
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsCopysetNoLeader_1) {
    // 更新topology中的copyset-1(epoch=5, peers={1,2,3}, leader=0)
    hbtest_->UpdateCopysetTopo(
        1, 1, 5, 0, std::set<ChunkServerIdType>{1, 2, 3});
    // chunkserver-1上报copyset-1(epoch=5, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(1, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 1, std::set<ChunkServerIdType>{1, 2, 3});

    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsCopysetNoLeader_2) {
    // 更新topology中的copyset-1(epoch=5, peers={1,2,3}, leader=0)
    hbtest_->UpdateCopysetTopo(
        1, 1, 5, 0, std::set<ChunkServerIdType>{1, 2, 3});
    // chunkserver-1上报copyset-1(epoch=5, peers={1,2,3}, leader=1)
    //                  configChangeInfo={peer: 2, type: TRANSFER_LEADER})
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(1, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 1, std::set<ChunkServerIdType>{1, 2, 3}, 2);
    hbtest_->AddCopySetToRequest(&req, csInfo,
                            ConfigChangeType::TRANSFER_LEADER);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsCopysetNoLeader_3) {
    // 更新topology中的copyset-1(epoch=5, peers={1,2,3}, leader=0)
    hbtest_->UpdateCopysetTopo(
        1, 1, 5, 0, std::set<ChunkServerIdType>{1, 2, 3});
    // chunkserver-1上报copyset-1(epoch=6, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(1, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 1, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsCopysetNoLeader_4) {
    // 更新topology中的copyset-1(epoch=5, peers={1,2,3}, leader=0)
    hbtest_->UpdateCopysetTopo(
        1, 1, 5, 0, std::set<ChunkServerIdType>{1, 2, 3});
    // chunkserver-1上报copyset-1(epoch=6, peers={1,2,3}, leader=1)
    //                  configChangeInfo={peer: 2, type: TRANSFER_LEADER})
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(1, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 1, std::set<ChunkServerIdType>{1, 2, 3}, 2);
    hbtest_->AddCopySetToRequest(&req, csInfo,
                                ConfigChangeType::TRANSFER_LEADER);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsCopysetNoLeader_5) {
    // 更新topology中的copyset-1(epoch=5, peers={1,2,3}, leader=0)
    hbtest_->UpdateCopysetTopo(
        1, 1, 5, 0, std::set<ChunkServerIdType>{1, 2, 3});
    ChunkServer cs(4, "testtoken", "nvme", 3, "10.198.100.3", 9001, "/");
    hbtest_->PrepareAddChunkServer(cs);
    // chunkserver-1上报copyset-1(epoch=6, peers={1,2,3,4}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(1, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 1, std::set<ChunkServerIdType>{1, 2, 3, 4});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsCopysetNoLeader_6) {
    // 更新topology中的copyset-1(epoch=5, peers={1,2,3}, leader=0)
    hbtest_->UpdateCopysetTopo(
        1, 1, 5, 0, std::set<ChunkServerIdType>{1, 2, 3});
    ChunkServer cs(4, "testtoken", "nvme", 3, "10.198.100.3", 9001, "/");
    hbtest_->PrepareAddChunkServer(cs);
    // chunkserver-1上报copyset-1(epoch=6, peers={1,2,3,4}, leader=1)
    //                  configChangeInfo={peer: 2, type: TRANSFER_LEADER})
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(1, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 1, std::set<ChunkServerIdType>{1, 2, 3, 4}, 2);
    hbtest_->AddCopySetToRequest(&req, csInfo,
                            ConfigChangeType::TRANSFER_LEADER);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsCopysetNoLeader_7) {
    // 更新topology中的copyset-1(epoch=5, peers={1,2,3}, leader=0)
    hbtest_->UpdateCopysetTopo(
        1, 1, 5, 0, std::set<ChunkServerIdType>{1, 2, 3});
    // chunkserver-2上报copyset-1(epoch=5, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 1, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetLeader(0);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsCopysetNoLeader_8) {
    // 更新topology中的copyset-1(epoch=5, peers={1,2,3}, leader=0)
    hbtest_->UpdateCopysetTopo(
        1, 1, 5, 0, std::set<ChunkServerIdType>{1, 2, 3});
    // chunkserver-2上报copyset-1(epoch=5, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 0, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsCopysetNoLeader_9) {
    // 更新topology中的copyset-1(epoch=5, peers={1,2,3}, leader=0)
    hbtest_->UpdateCopysetTopo(
        1, 1, 5, 0, std::set<ChunkServerIdType>{1, 2, 3});
    // chunkserver-2上报copyset-1(epoch=6, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 1, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetLeader(0);
    csInfo.SetEpoch(5);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());

    // chunkserver-2上报copyset-1(epoch=6, peers={1,2,3}, leader=0)
    req.Clear();
    rep.Clear();
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    BuildCopySetInfo(&csInfo, 6, 0, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsCopysetNoLeader_10) {
    // 更新topology中的copyset-1(epoch=5, peers={1,2,3}, leader=0)
    hbtest_->UpdateCopysetTopo(
        1, 1, 5, 0, std::set<ChunkServerIdType>{1, 2, 3});
    // chunkserver-2上报copyset-1(epoch=6, peers={1,2,3,4}, leader=1)
    ChunkServer cs(4, "testtoken", "nvme", 3, "10.198.100.3", 9001, "/");
    hbtest_->PrepareAddChunkServer(cs);
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 1, std::set<ChunkServerIdType>{1, 2, 3, 4});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetLeader(0);
    csInfo.SetEpoch(5);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{1, 2, 3});
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());

    // chunkserver-2上报copyset-1(epoch=6, peers={1,2,3,4}, leader=0)
    req.Clear();
    rep.Clear();
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    BuildCopySetInfo(&csInfo, 6, 0, std::set<ChunkServerIdType>{1, 2, 3, 4});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{1, 2, 3});
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsCopysetNoLeader_11) {
    // 更新topology中的copyset-1(epoch=5, peers={1,2,3}, leader=0)
    hbtest_->UpdateCopysetTopo(
        1, 1, 5, 0, std::set<ChunkServerIdType>{1, 2, 3});
    // chunkserver-2上报copyset-1(epoch=4, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 1, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetLeader(0);
    csInfo.SetEpoch(5);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());

    // chunkserver-2上报copyset-1(epoch=4, peers={1,2,3}, leader=0)
    req.Clear();
    rep.Clear();
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    BuildCopySetInfo(&csInfo, 4, 0, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsCopysetNoLeader_12) {
    // 更新topology中的copyset-1(epoch=5, peers={1,2,3}, leader=0)
    hbtest_->UpdateCopysetTopo(
        1, 1, 5, 0, std::set<ChunkServerIdType>{1, 2, 3});
    // chunkserver-2上报copyset-1(epoch=4, peers={1,2,3,4}, leader=1)
    ChunkServer cs(4, "testtoken", "nvme", 3, "10.198.100.3", 9001, "/");
    hbtest_->PrepareAddChunkServer(cs);
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 1, std::set<ChunkServerIdType>{1, 2, 3, 4});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetLeader(0);
    csInfo.SetEpoch(5);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{1, 2, 3});
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());

    // chunkserver-2上报copyset-1(epoch=4, peers={1,2,3,4}, leader=0)
    req.Clear();
    rep.Clear();
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    BuildCopySetInfo(&csInfo, 4, 0, std::set<ChunkServerIdType>{1, 2, 3, 4});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{1, 2, 3});
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsCopysetNoLeader_13) {
    // 更新topology中的copyset-1(epoch=5, peers={1,2,3}, leader=0)
    hbtest_->UpdateCopysetTopo(
        1, 1, 5, 0, std::set<ChunkServerIdType>{1, 2, 3});
    // chunkserver-2上报copyset-1(epoch=0, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 1, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetLeader(0);
    csInfo.SetEpoch(5);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());

    // chunkserver-2上报copyset-1(epoch=0, peers={1,2,3}, leader=0)
    req.Clear();
    rep.Clear();
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    BuildCopySetInfo(&csInfo, 0, 0, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsCopysetNoLeader_14) {
    // 更新topology中的copyset-1(epoch=5, peers={1,2,3}, leader=0)
    hbtest_->UpdateCopysetTopo(
        1, 1, 5, 0, std::set<ChunkServerIdType>{1, 2, 3});
    // chunkserver-2上报copyset-1(epoch=0, peers={1,2,3,4}, leader=1)
    ChunkServer cs(4, "testtoken", "nvme", 3, "10.198.100.3", 9001, "/");
    hbtest_->PrepareAddChunkServer(cs);
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 1, std::set<ChunkServerIdType>{1, 2, 3, 4});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetLeader(0);
    csInfo.SetEpoch(5);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{1, 2, 3});
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());

    // chunkserver-2上报copyset-1(epoch=0, peers={1,2,3,4}, leader=0)
    req.Clear();
    rep.Clear();
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    BuildCopySetInfo(&csInfo, 0, 0, std::set<ChunkServerIdType>{1, 2, 3, 4});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{1, 2, 3});
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsCopysetNoLeader_15) {
    // 更新topology中的copyset-1(epoch=5, peers={1,2,3}, leader=0)
    hbtest_->UpdateCopysetTopo(
        1, 1, 5, 0, std::set<ChunkServerIdType>{1, 2, 3});
    // chunkserver-4上报copyset-1(epoch=5, peers={1,2,3}, leader=1)
    ChunkServer cs(4, "testtoken", "nvme", 3, "10.198.100.3", 9001, "/");
    hbtest_->PrepareAddChunkServer(cs);
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 1, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    auto conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(3, conf.peers_size());
    ASSERT_EQ("10.198.100.1:9000:0", conf.peers(0).address());
    ASSERT_EQ("10.198.100.2:9000:0", conf.peers(1).address());
    ASSERT_EQ("10.198.100.3:9000:0", conf.peers(2).address());
    ASSERT_EQ(5, conf.epoch());
    // 检查copyset
    csInfo.SetLeader(0);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsCopysetNoLeader_16) {
    // 更新topology中的copyset-1(epoch=5, peers={1,2,3}, leader=0)
    hbtest_->UpdateCopysetTopo(
        1, 1, 5, 0, std::set<ChunkServerIdType>{1, 2, 3});
    // chunkserver-4上报copyset-1(epoch=5, peers={1,2,3}, leader=0)
    ChunkServer cs(4, "testtoken", "nvme", 3, "10.198.100.3", 9001, "/");
    hbtest_->PrepareAddChunkServer(cs);
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 0, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    // 检查response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    auto conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(3, conf.peers_size());
    ASSERT_EQ("10.198.100.1:9000:0", conf.peers(0).address());
    ASSERT_EQ("10.198.100.2:9000:0", conf.peers(1).address());
    ASSERT_EQ("10.198.100.3:9000:0", conf.peers(2).address());
    ASSERT_EQ(5, conf.epoch());
    // 检查copyset
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsCopysetNoLeader_17) {
    // 更新topology中的copyset-1(epoch=5, peers={1,2,3}, leader=0)
    hbtest_->UpdateCopysetTopo(
        1, 1, 5, 0, std::set<ChunkServerIdType>{1, 2, 3});
    // chunkserver-4上报copyset-1(epoch=6, peers={1,2,3}, leader=1)
    ChunkServer cs1(4, "testtoken", "nvme", 3, "10.198.100.3", 9001, "/");
    hbtest_->PrepareAddChunkServer(cs1);
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 1, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetLeader(0);
    csInfo.SetEpoch(5);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());

    // chunkserver-4上报copyset-1(epoch=6, peers={1,2,3,5}, leader=1)
    req.Clear();
    rep.Clear();
    ChunkServer cs2(5, "testtoken", "nvme", 3, "10.198.100.3", 9002, "/");
    hbtest_->PrepareAddChunkServer(cs2);
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    BuildCopySetInfo(&csInfo, 6, 1, std::set<ChunkServerIdType>{1, 2, 3, 5});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetLeader(0);
    csInfo.SetEpoch(5);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{1, 2, 3});
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsCopysetNoLeader_18) {
    // 更新topology中的copyset-1(epoch=5, peers={1,2,3}, leader=0)
    hbtest_->UpdateCopysetTopo(
        1, 1, 5, 0, std::set<ChunkServerIdType>{1, 2, 3});
    // chunkserver-4上报copyset-1(epoch=6, peers={1,2,3}, leader=0)
    ChunkServer cs1(4, "testtoken", "nvme", 3, "10.198.100.3", 9001, "/");
    hbtest_->PrepareAddChunkServer(cs1);
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 0, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());

    // chunkserver-4上报copyset-1(epoch=6, peers={1,2,3,5}, leader=0)
    req.Clear();
    rep.Clear();
    ChunkServer cs2(5, "testtoken", "nvme", 3, "10.198.100.3", 9002, "/");
    hbtest_->PrepareAddChunkServer(cs2);
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    BuildCopySetInfo(&csInfo, 6, 0, std::set<ChunkServerIdType>{1, 2, 3, 5});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{1, 2, 3});
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsCopysetNoLeader_19) {
    // 更新topology中的copyset-1(epoch=5, peers={1,2,3}, leader=0)
    hbtest_->UpdateCopysetTopo(
        1, 1, 5, 0, std::set<ChunkServerIdType>{1, 2, 3});

    // chunkserver-4上报copyset-1(epoch=4, peers={1,2,3}, leader=1)
    ChunkServer cs1(4, "testtoken", "nvme", 3, "10.198.100.3", 9001, "/");
    hbtest_->PrepareAddChunkServer(cs1);
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 1, std::set<ChunkServerIdType>{1, 2, 3});

    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    // 检查response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    auto conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(3, conf.peers_size());
    ASSERT_EQ("10.198.100.1:9000:0", conf.peers(0).address());
    ASSERT_EQ("10.198.100.2:9000:0", conf.peers(1).address());
    ASSERT_EQ("10.198.100.3:9000:0", conf.peers(2).address());
    ASSERT_EQ(5, conf.epoch());
    // 检查copyset
    csInfo.SetLeader(0);
    csInfo.SetEpoch(5);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());

    // chunkserver-4上报copyset-1(epoch=4, peers={1,2,3,5}, leader=1)
    req.Clear();
    rep.Clear();
    ChunkServer cs2(5, "testtoken", "nvme", 3, "10.198.100.3", 9002, "/");
    hbtest_->PrepareAddChunkServer(cs2);
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    BuildCopySetInfo(&csInfo, 4, 1, std::set<ChunkServerIdType>{1, 2, 3, 5});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    // 检查response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(3, conf.peers_size());
    ASSERT_EQ("10.198.100.1:9000:0", conf.peers(0).address());
    ASSERT_EQ("10.198.100.2:9000:0", conf.peers(1).address());
    ASSERT_EQ("10.198.100.3:9000:0", conf.peers(2).address());
    ASSERT_EQ(5, conf.epoch());
    // 检查copyset
    csInfo.SetLeader(0);
    csInfo.SetEpoch(5);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{1, 2, 3});
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsCopysetNoLeader_20) {
    // 更新topology中的copyset-1(epoch=5, peers={1,2,3}, leader=0)
    hbtest_->UpdateCopysetTopo(
        1, 1, 5, 0, std::set<ChunkServerIdType>{1, 2, 3});
    // chunkserver-4上报copyset-1(epoch=4, peers={1,2,3}, leader=0)
    ChunkServer cs1(4, "testtoken", "nvme", 3, "10.198.100.3", 9001, "/");
    hbtest_->PrepareAddChunkServer(cs1);
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 0, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    // 检查response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    auto conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(3, conf.peers_size());
    ASSERT_EQ("10.198.100.1:9000:0", conf.peers(0).address());
    ASSERT_EQ("10.198.100.2:9000:0", conf.peers(1).address());
    ASSERT_EQ("10.198.100.3:9000:0", conf.peers(2).address());
    ASSERT_EQ(5, conf.epoch());
    // 检查copyset
    csInfo.SetEpoch(5);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());

    // chunkserver-4上报copyset-1(epoch=4, peers={1,2,3,5}, leader=0)
    req.Clear();
    rep.Clear();
    ChunkServer cs2(5, "testtoken", "nvme", 3, "10.198.100.3", 9002, "/");
    hbtest_->PrepareAddChunkServer(cs2);
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    BuildCopySetInfo(&csInfo, 4, 0, std::set<ChunkServerIdType>{1, 2, 3, 5});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    // 检查response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(3, conf.peers_size());
    ASSERT_EQ("10.198.100.1:9000:0", conf.peers(0).address());
    ASSERT_EQ("10.198.100.2:9000:0", conf.peers(1).address());
    ASSERT_EQ("10.198.100.3:9000:0", conf.peers(2).address());
    ASSERT_EQ(5, conf.epoch());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{1, 2, 3});
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsCopysetNoLeader_21) {
    // 更新topology中的copyset-1(epoch=5, peers={1,2,3}, leader=0)
    hbtest_->UpdateCopysetTopo(
        1, 1, 5, 0, std::set<ChunkServerIdType>{1, 2, 3});

    // chunkserver-4上报copyset-1(epoch=0, peers={1,2,3}, leader=1)
    ChunkServer cs1(4, "testtoken", "nvme", 3, "10.198.100.3", 9001, "/");
    hbtest_->PrepareAddChunkServer(cs1);
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 1, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    // 检查response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    auto conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(3, conf.peers_size());
    ASSERT_EQ("10.198.100.1:9000:0", conf.peers(0).address());
    ASSERT_EQ("10.198.100.2:9000:0", conf.peers(1).address());
    ASSERT_EQ("10.198.100.3:9000:0", conf.peers(2).address());
    ASSERT_EQ(5, conf.epoch());
    // 检查copyset
    csInfo.SetLeader(0);
    csInfo.SetEpoch(5);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());

    // chunkserver-4上报copyset-1(epoch=0, peers={1,2,3,5}, leader=1)
    req.Clear();
    rep.Clear();
    ChunkServer cs2(5, "testtoken", "nvme", 3, "10.198.100.3", 9002, "/");
    hbtest_->PrepareAddChunkServer(cs2);
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    BuildCopySetInfo(&csInfo, 0, 1, std::set<ChunkServerIdType>{1, 2, 3, 5});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    // 检查response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(3, conf.peers_size());
    ASSERT_EQ("10.198.100.1:9000:0", conf.peers(0).address());
    ASSERT_EQ("10.198.100.2:9000:0", conf.peers(1).address());
    ASSERT_EQ("10.198.100.3:9000:0", conf.peers(2).address());
    ASSERT_EQ(5, conf.epoch());
    // 检查copyset
    csInfo.SetLeader(0);
    csInfo.SetEpoch(5);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{1, 2, 3});
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsCopysetNoLeader_22) {
    // 更新topology中的copyset-1(epoch=5, peers={1,2,3}, leader=0)
    hbtest_->UpdateCopysetTopo(
        1, 1, 5, 0, std::set<ChunkServerIdType>{1, 2, 3});
    // chunkserver-4上报copyset-1(epoch=0, peers={1,2,3}, leader=0)
    ChunkServer cs1(4, "testtoken", "nvme", 3, "10.198.100.3", 9001, "/");
    hbtest_->PrepareAddChunkServer(cs1);
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 0, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    // 检查response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    auto conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(3, conf.peers_size());
    ASSERT_EQ("10.198.100.1:9000:0", conf.peers(0).address());
    ASSERT_EQ("10.198.100.2:9000:0", conf.peers(1).address());
    ASSERT_EQ("10.198.100.3:9000:0", conf.peers(2).address());
    ASSERT_EQ(5, conf.epoch());
    // 检查copyset
    csInfo.SetEpoch(5);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());

    // chunkserver-4上报copyset-1(epoch=0, peers={1,2,3,5}, leader=0)
    req.Clear();
    rep.Clear();
    ChunkServer cs2(5, "testtoken", "nvme", 3, "10.198.100.3", 9002, "/");
    hbtest_->PrepareAddChunkServer(cs2);
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    BuildCopySetInfo(&csInfo, 0, 0, std::set<ChunkServerIdType>{1, 2, 3, 5});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    // 检查response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(3, conf.peers_size());
    ASSERT_EQ("10.198.100.1:9000:0", conf.peers(0).address());
    ASSERT_EQ("10.198.100.2:9000:0", conf.peers(1).address());
    ASSERT_EQ("10.198.100.3:9000:0", conf.peers(2).address());
    ASSERT_EQ(5, conf.epoch());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{1, 2, 3});
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOp_1) {
    PrepareMdsWithChangeOp();

    // chunkserver-1上报copyset-1(epoch=5, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(1, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 1, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    // 检查response
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
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(3, step->GetOldPeer());
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOp_2) {
    PrepareMdsWithChangeOp();

    // chunkserver-1上报copyset-1(epoch=5, peers={1,2,3}, leader=1)
    //                  configChangeInfo={peer: 4, type: CHANGE_PEER})
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(1, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 1, std::set<ChunkServerIdType>{1, 2, 3}, 4);
    hbtest_->AddCopySetToRequest(&req, csInfo,
                                ConfigChangeType::CHANGE_PEER);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOp_3) {
    PrepareMdsWithChangeOp();

    // chunkserver-1上报copyset-1(epoch=6, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(1, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 1, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOp_4) {
    PrepareMdsWithChangeOp();

    // chunkserver-2上报copyset-1(epoch=6, peers={1,2,3}, leader=2)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 2, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOp_5) {
    PrepareMdsWithChangeOp();
    ChunkServer cs(5, "testtoken", "nvme", 3, "10.198.100.3", 9002, "/");
    hbtest_->PrepareAddChunkServer(cs);

    // chunkserver-1上报copyset-1(epoch=6, peers={1,2,5}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(1, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 1, std::set<ChunkServerIdType>{1, 2, 5});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOp_6) {
    PrepareMdsWithChangeOp();

    // chunkserver-1上报copyset-1(epoch=6, peers={1,2,4}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(1, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 1, std::set<ChunkServerIdType>{1, 2, 4});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOp_7) {
    PrepareMdsWithChangeOp();

    // chunkserver-2上报copyset-1(epoch=7, peers={1,2,4}, leader=2)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 7, 2, std::set<ChunkServerIdType>{1, 2, 4});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOp_8) {
    PrepareMdsWithChangeOp();

    // chunkserver-4上报copyset-1(epoch=7, peers={1,2,4}, leader=4)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 7, 4, std::set<ChunkServerIdType>{1, 2, 4});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOp_9) {
    PrepareMdsWithChangeOp();

    // chunkserver-2上报copyset-1(epoch=5, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 1, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOp_10) {
    PrepareMdsWithChangeOp();

    // chunkserver-2上报copyset-1(epoch=5, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 0, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetLeader(1);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOp_11) {
    PrepareMdsWithChangeOp();

    // chunkserver-2上报copyset-1(epoch=6, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 1, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOp_12) {
    PrepareMdsWithChangeOp();

    // chunkserver-2上报copyset-1(epoch=6, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 0, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetLeader(1);
    csInfo.SetEpoch(5);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOp_13) {
    PrepareMdsWithChangeOp();

    // chunkserver-2上报copyset-1(epoch=6, peers={1,2,4}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 1, std::set<ChunkServerIdType>{1, 2, 4});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{1, 2, 3});
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOp_14) {
    PrepareMdsWithChangeOp();

    // chunkserver-2上报copyset-1(epoch=6, peers={1,2,4}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 0, std::set<ChunkServerIdType>{1, 2, 4});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetLeader(1);
    csInfo.SetEpoch(5);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{1, 2, 3});
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
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

    // chunkserver-2上报copyset-1(epoch=6, peers={1,2,5}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 1, std::set<ChunkServerIdType>{1, 2, 5});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{1, 2, 3});
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
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

    // chunkserver-2上报copyset-1(epoch=6, peers={1,2,5}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 0, std::set<ChunkServerIdType>{1, 2, 5});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetLeader(1);
    csInfo.SetEpoch(5);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{1, 2, 3});
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOp_17) {
    PrepareMdsWithChangeOp();

    // chunkserver-2上报copyset-1(epoch=4, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 1, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOp_18) {
    PrepareMdsWithChangeOp();

    // chunkserver-2上报copyset-1(epoch=4, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 0, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetLeader(1);
    csInfo.SetEpoch(5);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
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

    // chunkserver-2上报copyset-1(epoch=4, peers={1,2,5}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 1, std::set<ChunkServerIdType>{1, 2, 5});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{1, 2, 3});
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
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

    // chunkserver-2上报copyset-1(epoch=4, peers={1,2,5}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 0, std::set<ChunkServerIdType>{1, 2, 5});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetLeader(1);
    csInfo.SetEpoch(5);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{1, 2, 3});
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOp_21) {
    PrepareMdsWithChangeOp();

    // chunkserver-2上报copyset-1(epoch=0, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 1, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOp_22) {
    PrepareMdsWithChangeOp();

    // chunkserver-2上报copyset-1(epoch=0, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 0, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetLeader(1);
    csInfo.SetEpoch(5);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
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

    // chunkserver-2上报copyset-1(epoch=0, peers={1,2,5}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 1, std::set<ChunkServerIdType>{1, 2, 5});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{1, 2, 3});
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
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

    // chunkserver-2上报copyset-1(epoch=0, peers={1,2,5}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 0, std::set<ChunkServerIdType>{1, 2, 5});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetLeader(1);
    csInfo.SetEpoch(5);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{1, 2, 3});
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOp_25) {
    PrepareMdsWithChangeOp();

    // chunkserver-4上报copyset-1(epoch=5, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 1, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOp_26) {
    PrepareMdsWithChangeOp();

    // chunkserver-4上报copyset-1(epoch=5, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 0, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetLeader(1);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOp_27) {
    PrepareMdsWithChangeOp();

    // chunkserver-4上报copyset-1(epoch=6, peers={1,2,4}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 1, std::set<ChunkServerIdType>{1, 2, 4});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{1, 2, 3});
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOp_28) {
    PrepareMdsWithChangeOp();

    // chunkserver-4上报copyset-1(epoch=6, peers={1,2,4}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 0, std::set<ChunkServerIdType>{1, 2, 4});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{1, 2, 3});
    csInfo.SetLeader(1);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOp_29) {
    PrepareMdsWithChangeOp();

    // chunkserver-4上报copyset-1(epoch=4, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 1, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOp_30) {
    PrepareMdsWithChangeOp();

    // chunkserver-4上报copyset-1(epoch=4, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 0, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
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

    // chunkserver-4上报copyset-1(epoch=4, peers={1,2,5}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 1, std::set<ChunkServerIdType>{1, 2, 5});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{1, 2, 3});
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
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

    // chunkserver-4上报copyset-1(epoch=4, peers={1,2,5}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 0, std::set<ChunkServerIdType>{1, 2, 5});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{1, 2, 3});
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOp_33) {
    PrepareMdsWithChangeOp();

    // chunkserver-4上报copyset-1(epoch=0, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 1, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOp_34) {
    PrepareMdsWithChangeOp();

    // chunkserver-4上报copyset-1(epoch=0, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 0, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
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

    // chunkserver-4上报copyset-1(epoch=0, peers={1,2,5}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 1, std::set<ChunkServerIdType>{1, 2, 5});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{1, 2, 3});
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
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

    // chunkserver-4上报copyset-1(epoch=0, peers={1,2,5}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 0, std::set<ChunkServerIdType>{1, 2, 5});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{1, 2, 3});
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
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

    // chunkserver-5上报copyset-1(epoch=0, peers={1,2,5}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(5, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 0, std::set<ChunkServerIdType>{1, 2, 5});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    CopySetConf conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(3, conf.peers_size());
    ASSERT_EQ("10.198.100.1:9000:0", conf.peers(0).address());
    ASSERT_EQ("10.198.100.2:9000:0", conf.peers(1).address());
    ASSERT_EQ("10.198.100.3:9000:0", conf.peers(2).address());
    ASSERT_EQ(5, conf.epoch());
    // 检查copyset
    csInfo.SetLeader(1);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{1, 2, 3});
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
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

    // chunkserver-5上报copyset-1(epoch=4, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(5, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 1, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    CopySetConf conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(3, conf.peers_size());
    ASSERT_EQ("10.198.100.1:9000:0", conf.peers(0).address());
    ASSERT_EQ("10.198.100.2:9000:0", conf.peers(1).address());
    ASSERT_EQ("10.198.100.3:9000:0", conf.peers(2).address());
    ASSERT_EQ(5, conf.epoch());
    // 检查copyset
    csInfo.SetEpoch(5);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());


    ChunkServer cs2(6, "testtoken", "nvme", 3, "10.198.100.3", 9003, "/");
    hbtest_->PrepareAddChunkServer(cs2);
    req.Clear();
    rep.Clear();
    // chunkserver-5上报copyset-1(epoch=4, peers={1,2,6}, leader=1)
    hbtest_->BuildBasicChunkServerRequest(5, &req);
    BuildCopySetInfo(&csInfo, 4, 1, std::set<ChunkServerIdType>{1, 2, 6});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(3, conf.peers_size());
    ASSERT_EQ("10.198.100.1:9000:0", conf.peers(0).address());
    ASSERT_EQ("10.198.100.2:9000:0", conf.peers(1).address());
    ASSERT_EQ("10.198.100.3:9000:0", conf.peers(2).address());
    ASSERT_EQ(5, conf.epoch());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{1, 2, 3});
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
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

    // chunkserver-5上报copyset-1(epoch=4, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(5, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 0, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    CopySetConf conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(3, conf.peers_size());
    ASSERT_EQ("10.198.100.1:9000:0", conf.peers(0).address());
    ASSERT_EQ("10.198.100.2:9000:0", conf.peers(1).address());
    ASSERT_EQ("10.198.100.3:9000:0", conf.peers(2).address());
    ASSERT_EQ(5, conf.epoch());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());


    ChunkServer cs2(6, "testtoken", "nvme", 3, "10.198.100.3", 9003, "/");
    hbtest_->PrepareAddChunkServer(cs2);
    // chunkserver-5上报copyset-1(epoch=4, peers={1,2,6}, leader=0)
    req.Clear();
    rep.Clear();
    hbtest_->BuildBasicChunkServerRequest(5, &req);
    BuildCopySetInfo(&csInfo, 4, 0, std::set<ChunkServerIdType>{1, 2, 6});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(3, conf.peers_size());
    ASSERT_EQ("10.198.100.1:9000:0", conf.peers(0).address());
    ASSERT_EQ("10.198.100.2:9000:0", conf.peers(1).address());
    ASSERT_EQ("10.198.100.3:9000:0", conf.peers(2).address());
    ASSERT_EQ(5, conf.epoch());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{1, 2, 3});
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
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

    // chunkserver-5上报copyset-1(epoch=0, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(5, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 0, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    CopySetConf conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(3, conf.peers_size());
    ASSERT_EQ("10.198.100.1:9000:0", conf.peers(0).address());
    ASSERT_EQ("10.198.100.2:9000:0", conf.peers(1).address());
    ASSERT_EQ("10.198.100.3:9000:0", conf.peers(2).address());
    ASSERT_EQ(5, conf.epoch());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());

    // chunkserver-5上报copyset-1(epoch=0, peers={1,2,4}, leader=0)
    req.Clear();
    rep.Clear();
    hbtest_->BuildBasicChunkServerRequest(5, &req);
    BuildCopySetInfo(&csInfo, 0, 0, std::set<ChunkServerIdType>{1, 2, 4});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(3, conf.peers_size());
    ASSERT_EQ("10.198.100.1:9000:0", conf.peers(0).address());
    ASSERT_EQ("10.198.100.2:9000:0", conf.peers(1).address());
    ASSERT_EQ("10.198.100.3:9000:0", conf.peers(2).address());
    ASSERT_EQ(5, conf.epoch());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{1, 2, 3});
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOpOnGoing_1) {
    PrepareMdsWithChangeOpOnGoing();

    // chunkserver-1上报copyset-1(epoch=5, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(1, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 1, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);
    // 检查response
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
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(3, step->GetOldPeer());
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOpOnGoing_2) {
    PrepareMdsWithChangeOpOnGoing();

    // chunkserver-1上报copyset-1(epoch=5, peers={1,2,3}, leader=1)
    //                  configChangeInfo={peer: 4, type: CHANGE_PEER})
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(1, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 1, std::set<ChunkServerIdType>{1, 2, 3}, 4);
    hbtest_->AddCopySetToRequest(&req, csInfo,
                                ConfigChangeType::CHANGE_PEER);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOpOnGoing_3) {
    PrepareMdsWithChangeOpOnGoing();

    // chunkserver-1上报copyset-1(epoch=6, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(1, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 1, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOpOnGoing_4) {
    PrepareMdsWithChangeOpOnGoing();

    // chunkserver-2上报copyset-1(epoch=6, peers={1,2,3}, leader=2)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 2, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOpOnGoing_5) {
    PrepareMdsWithChangeOpOnGoing();
    ChunkServer cs(5, "testtoken", "nvme", 3, "10.198.100.3", 9002, "/");
    hbtest_->PrepareAddChunkServer(cs);

    // chunkserver-1上报copyset-1(epoch=6, peers={1,2,5}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(1, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 1, std::set<ChunkServerIdType>{1, 2, 5});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOpOnGoing_6) {
    PrepareMdsWithChangeOpOnGoing();

    // chunkserver-1上报copyset-1(epoch=6, peers={1,2,4}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(1, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 1, std::set<ChunkServerIdType>{1, 2, 4});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOpOnGoing_7) {
    PrepareMdsWithChangeOpOnGoing();;

    // chunkserver-2上报copyset-1(epoch=7, peers={1,2,4}, leader=2)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 7, 2, std::set<ChunkServerIdType>{1, 2, 4});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOpOnGoing_8) {
    PrepareMdsWithChangeOpOnGoing();

    // chunkserver-4上报copyset-1(epoch=7, peers={1,2,4}, leader=4)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 7, 4, std::set<ChunkServerIdType>{1, 2, 4});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOpOnGoing_9) {
    PrepareMdsWithChangeOpOnGoing();

    // chunkserver-2上报copyset-1(epoch=5, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 1, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOpOnGoing_10) {
    PrepareMdsWithChangeOpOnGoing();

    // chunkserver-2上报copyset-1(epoch=5, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 0, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetLeader(1);
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOpOnGoing_11) {
    PrepareMdsWithChangeOpOnGoing();

    // chunkserver-2上报copyset-1(epoch=6, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 1, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOpOnGoing_12) {
    PrepareMdsWithChangeOpOnGoing();

    // chunkserver-2上报copyset-1(epoch=6, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 0, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetLeader(1);
    csInfo.SetEpoch(5);
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOpOnGoing_13) {
    PrepareMdsWithChangeOpOnGoing();

    // chunkserver-2上报copyset-1(epoch=6, peers={1,2,4}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 1, std::set<ChunkServerIdType>{1, 2, 4});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetCandidate(4);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{1, 2, 3});
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOpOnGoing_14) {
    PrepareMdsWithChangeOpOnGoing();

    // chunkserver-2上报copyset-1(epoch=6, peers={1,2,4}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 0, std::set<ChunkServerIdType>{1, 2, 4});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetLeader(1);
    csInfo.SetEpoch(5);
    csInfo.SetCandidate(4);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{1, 2, 3});
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
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

    // chunkserver-2上报copyset-1(epoch=6, peers={1,2,5}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 1, std::set<ChunkServerIdType>{1, 2, 5});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetCandidate(4);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{1, 2, 3});
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
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

    // chunkserver-2上报copyset-1(epoch=6, peers={1,2,5}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 0, std::set<ChunkServerIdType>{1, 2, 5});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetLeader(1);
    csInfo.SetEpoch(5);
    csInfo.SetCandidate(4);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{1, 2, 3});
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOpOnGoing_17) {
    PrepareMdsWithChangeOpOnGoing();

    // chunkserver-2上报copyset-1(epoch=4, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 1, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOpOnGoing_18) {
    PrepareMdsWithChangeOpOnGoing();

    // chunkserver-2上报copyset-1(epoch=4, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 0, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetLeader(1);
    csInfo.SetEpoch(5);
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
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

    // chunkserver-2上报copyset-1(epoch=4, peers={1,2,5}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 1, std::set<ChunkServerIdType>{1, 2, 5});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetCandidate(4);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{1, 2, 3});
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
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

    // chunkserver-2上报copyset-1(epoch=4, peers={1,2,5}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 0, std::set<ChunkServerIdType>{1, 2, 5});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetLeader(1);
    csInfo.SetEpoch(5);
    csInfo.SetCandidate(4);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{1, 2, 3});
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOpOnGoing_21) {
    PrepareMdsWithChangeOpOnGoing();

    // chunkserver-2上报copyset-1(epoch=0, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 1, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOpOnGoing_22) {
    PrepareMdsWithChangeOpOnGoing();

    // chunkserver-2上报copyset-1(epoch=0, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 0, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetLeader(1);
    csInfo.SetEpoch(5);
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
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

    // chunkserver-2上报copyset-1(epoch=0, peers={1,2,5}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 1, std::set<ChunkServerIdType>{1, 2, 5});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetCandidate(4);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{1, 2, 3});
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
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

    // chunkserver-2上报copyset-1(epoch=0, peers={1,2,5}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(2, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 0, std::set<ChunkServerIdType>{1, 2, 5});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetLeader(1);
    csInfo.SetEpoch(5);
    csInfo.SetCandidate(4);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{1, 2, 3});
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOpOnGoing_25) {
    PrepareMdsWithChangeOpOnGoing();

    // chunkserver-4上报copyset-1(epoch=5, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 1, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOpOnGoing_26) {
    PrepareMdsWithChangeOpOnGoing();

    // chunkserver-4上报copyset-1(epoch=5, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 0, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetLeader(1);
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOpOnGoing_27) {
    PrepareMdsWithChangeOpOnGoing();

    // chunkserver-4上报copyset-1(epoch=6, peers={1,2,4}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 1, std::set<ChunkServerIdType>{1, 2, 4});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetCandidate(4);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{1, 2, 3});
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOpOnGoing_28) {
    PrepareMdsWithChangeOpOnGoing();

    // chunkserver-4上报copyset-1(epoch=6, peers={1,2,4}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 6, 0, std::set<ChunkServerIdType>{1, 2, 4});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetCandidate(4);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{1, 2, 3});
    csInfo.SetLeader(1);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOpOnGoing_29) {
    PrepareMdsWithChangeOpOnGoing();

    // chunkserver-4上报copyset-1(epoch=4, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 1, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOpOnGoing_30) {
    PrepareMdsWithChangeOpOnGoing();

    // chunkserver-4上报copyset-1(epoch=4, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 0, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
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

    // chunkserver-4上报copyset-1(epoch=4, peers={1,2,5}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 1, std::set<ChunkServerIdType>{1, 2, 5});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetCandidate(4);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{1, 2, 3});
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
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

    // chunkserver-4上报copyset-1(epoch=4, peers={1,2,5}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 0, std::set<ChunkServerIdType>{1, 2, 5});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCandidate(4);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{1, 2, 3});
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOpOnGoing_33) {
    PrepareMdsWithChangeOpOnGoing();

    // chunkserver-4上报copyset-1(epoch=0, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 1, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

TEST_F(HeartbeatBasicTest, test_mdsWithChangePeerOpOnGoing_34) {
    PrepareMdsWithChangeOpOnGoing();

    // chunkserver-4上报copyset-1(epoch=0, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 0, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
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

    // chunkserver-4上报copyset-1(epoch=0, peers={1,2,5}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 1, std::set<ChunkServerIdType>{1, 2, 5});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetCandidate(4);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{1, 2, 3});
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
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

    // chunkserver-4上报copyset-1(epoch=0, peers={1,2,5}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(4, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 0, std::set<ChunkServerIdType>{1, 2, 5});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCandidate(4);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{1, 2, 3});
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
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

    // chunkserver-5上报copyset-1(epoch=0, peers={1,2,5}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(5, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 5, 0, std::set<ChunkServerIdType>{1, 2, 5});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    CopySetConf conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(3, conf.peers_size());
    ASSERT_EQ("10.198.100.1:9000:0", conf.peers(0).address());
    ASSERT_EQ("10.198.100.2:9000:0", conf.peers(1).address());
    ASSERT_EQ("10.198.100.3:9000:0", conf.peers(2).address());
    ASSERT_EQ(5, conf.epoch());
    // 检查copyset
    csInfo.SetLeader(1);
    csInfo.SetCandidate(4);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{1, 2, 3});
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
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

    // chunkserver-5上报copyset-1(epoch=4, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(5, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 0, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    CopySetConf conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(3, conf.peers_size());
    ASSERT_EQ("10.198.100.1:9000:0", conf.peers(0).address());
    ASSERT_EQ("10.198.100.2:9000:0", conf.peers(1).address());
    ASSERT_EQ("10.198.100.3:9000:0", conf.peers(2).address());
    ASSERT_EQ(5, conf.epoch());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());


    ChunkServer cs2(6, "testtoken", "nvme", 3, "10.198.100.3", 9003, "/");
    hbtest_->PrepareAddChunkServer(cs2);
    req.Clear();
    rep.Clear();
    // chunkserver-5上报copyset-1(epoch=4, peers={1,2,6}, leader=0)
    hbtest_->BuildBasicChunkServerRequest(5, &req);
    BuildCopySetInfo(&csInfo, 4, 0, std::set<ChunkServerIdType>{1, 2, 6});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(3, conf.peers_size());
    ASSERT_EQ("10.198.100.1:9000:0", conf.peers(0).address());
    ASSERT_EQ("10.198.100.2:9000:0", conf.peers(1).address());
    ASSERT_EQ("10.198.100.3:9000:0", conf.peers(2).address());
    ASSERT_EQ(5, conf.epoch());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCandidate(4);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{1, 2, 3});
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
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

    // chunkserver-5上报copyset-1(epoch=4, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(5, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 4, 1, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    CopySetConf conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(3, conf.peers_size());
    ASSERT_EQ("10.198.100.1:9000:0", conf.peers(0).address());
    ASSERT_EQ("10.198.100.2:9000:0", conf.peers(1).address());
    ASSERT_EQ("10.198.100.3:9000:0", conf.peers(2).address());
    ASSERT_EQ(5, conf.epoch());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());


    ChunkServer cs2(6, "testtoken", "nvme", 3, "10.198.100.3", 9003, "/");
    hbtest_->PrepareAddChunkServer(cs2);
    // chunkserver-5上报copyset-1(epoch=4, peers={1,2,6}, leader=1)
    req.Clear();
    rep.Clear();
    hbtest_->BuildBasicChunkServerRequest(5, &req);
    BuildCopySetInfo(&csInfo, 4, 1, std::set<ChunkServerIdType>{1, 2, 6});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(3, conf.peers_size());
    ASSERT_EQ("10.198.100.1:9000:0", conf.peers(0).address());
    ASSERT_EQ("10.198.100.2:9000:0", conf.peers(1).address());
    ASSERT_EQ("10.198.100.3:9000:0", conf.peers(2).address());
    ASSERT_EQ(5, conf.epoch());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetCandidate(4);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{1, 2, 3});
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
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

    // chunkserver-5上报copyset-1(epoch=0, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    hbtest_->BuildBasicChunkServerRequest(5, &req);
    CopySetInfo csInfo(1, 1);
    BuildCopySetInfo(&csInfo, 0, 0, std::set<ChunkServerIdType>{1, 2, 3});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    ChunkServerHeartbeatResponse rep;
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    CopySetConf conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(3, conf.peers_size());
    ASSERT_EQ("10.198.100.1:9000:0", conf.peers(0).address());
    ASSERT_EQ("10.198.100.2:9000:0", conf.peers(1).address());
    ASSERT_EQ("10.198.100.3:9000:0", conf.peers(2).address());
    ASSERT_EQ(5, conf.epoch());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCandidate(4);
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    auto ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    auto step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());


    // chunkserver-5上报copyset-1(epoch=0, peers={1,2,4}, leader=0)
    req.Clear();
    rep.Clear();
    hbtest_->BuildBasicChunkServerRequest(5, &req);
    BuildCopySetInfo(&csInfo, 0, 0, std::set<ChunkServerIdType>{1, 2, 4});
    hbtest_->AddCopySetToRequest(&req, csInfo);
    hbtest_->SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(3, conf.peers_size());
    ASSERT_EQ("10.198.100.1:9000:0", conf.peers(0).address());
    ASSERT_EQ("10.198.100.2:9000:0", conf.peers(1).address());
    ASSERT_EQ("10.198.100.3:9000:0", conf.peers(2).address());
    ASSERT_EQ(5, conf.epoch());
    // 检查copyset
    csInfo.SetEpoch(5);
    csInfo.SetLeader(1);
    csInfo.SetCandidate(4);
    csInfo.SetCopySetMembers(std::set<ChunkServerIdType>{1, 2, 3});
    ASSERT_TRUE(ValidateCopySet(csInfo));
    // 检查scheduler中的op
    ops = hbtest_->coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    step = dynamic_cast<ChangePeer *>(ops[0].step.get());
    ASSERT_TRUE(nullptr != step);
    ASSERT_EQ(4, step->GetTargetPeer());
}

}  // namespace mds
}  // namespace curve
