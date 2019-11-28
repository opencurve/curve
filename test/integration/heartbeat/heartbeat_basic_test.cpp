/*
 * Project: curve
 * Created Date: 2019-06-11
 * Author: lixiaocui
 * Copyright (c) 2018 netease
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
}  // namespace mds
}  // namespace curve


