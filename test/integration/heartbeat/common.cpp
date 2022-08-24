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
 * Created Date: 2019-11-29
 * Author: lixiaocui
 */

#include "test/integration/heartbeat/common.h"
#include "test/mds/mock/mock_alloc_statistic.h"

namespace curve {
namespace mds {

void HeartbeatIntegrationCommon::PrepareAddLogicalPool(
    const LogicalPool &lpool) {
    int ret = topology_->AddLogicalPool(lpool);
    EXPECT_EQ(topology::kTopoErrCodeSuccess, ret)
        << "should have PrepareAddLogicalPool()";
}

void HeartbeatIntegrationCommon::PrepareAddPhysicalPool(
    const PhysicalPool &ppool) {
    int ret = topology_->AddPhysicalPool(ppool);
    EXPECT_EQ(topology::kTopoErrCodeSuccess, ret);
}

void HeartbeatIntegrationCommon::PrepareAddZone(const Zone &zone) {
    int ret = topology_->AddZone(zone);
    EXPECT_EQ(topology::kTopoErrCodeSuccess, ret)
        << "should have PrepareAddPhysicalPool()";
}

void HeartbeatIntegrationCommon::PrepareAddServer(const Server &server) {
    int ret = topology_->AddServer(server);
    EXPECT_EQ(topology::kTopoErrCodeSuccess, ret)
        << "should have PrepareAddZone()";
}

void HeartbeatIntegrationCommon::PrepareAddChunkServer(
    const ChunkServer &chunkserver) {
    ChunkServer cs(chunkserver);
    cs.SetOnlineState(OnlineState::ONLINE);
    int ret = topology_->AddChunkServer(cs);
    EXPECT_EQ(topology::kTopoErrCodeSuccess, ret)
        << "should have PrepareAddServer()";
}

void HeartbeatIntegrationCommon::PrepareAddCopySet(
    CopySetIdType copysetId, PoolIdType logicalPoolId,
    const std::set<ChunkServerIdType> &members) {
    CopySetInfo cs(logicalPoolId, copysetId);
    cs.SetCopySetMembers(members);
    int ret = topology_->AddCopySet(cs);
    EXPECT_EQ(topology::kTopoErrCodeSuccess, ret)
        << "should have PrepareAddLogicalPool()";
}

void HeartbeatIntegrationCommon::UpdateCopysetTopo(
    CopySetIdType copysetId, PoolIdType logicalPoolId, uint64_t epoch,
    ChunkServerIdType leader, const std::set<ChunkServerIdType> &members,
    ChunkServerIdType candidate) {
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(topology_->GetCopySet(CopySetKey{ logicalPoolId, copysetId },
                                      &copysetInfo));
    copysetInfo.SetEpoch(epoch);
    copysetInfo.SetLeader(leader);
    copysetInfo.SetCopySetMembers(members);
    if (candidate != UNINTIALIZE_ID) {
        copysetInfo.SetCandidate(candidate);
    }
    ASSERT_EQ(0, topology_->UpdateCopySetTopo(copysetInfo));
}

void HeartbeatIntegrationCommon::SendHeartbeat(
    const ChunkServerHeartbeatRequest &request, bool expectFailed,
    ChunkServerHeartbeatResponse *response) {
    // init brpc client
    brpc::Channel channel;
    ASSERT_EQ(0, channel.Init(listenAddr_.c_str(), NULL));
    HeartbeatService_Stub stub(&channel);
    brpc::Controller cntl;
    cntl.set_timeout_ms(2000);

    // send request
    stub.ChunkServerHeartbeat(&cntl, &request, response, NULL);
    EXPECT_EQ(expectFailed, cntl.Failed())
        << "heartbeat fail: " << cntl.ErrorText();
}

void HeartbeatIntegrationCommon::BuildBasicChunkServerRequest(
    ChunkServerIdType id, ChunkServerHeartbeatRequest *req) {
    ChunkServer out;
    EXPECT_TRUE(topology_->GetChunkServer(id, &out))
        << "get chunkserver: " << id << " fail";

    req->set_chunkserverid(out.GetId());
    req->set_token(out.GetToken());
    req->set_ip(out.GetHostIp());
    req->set_port(out.GetPort());
    auto diskState = new ::curve::mds::heartbeat::DiskState();
    diskState->set_errtype(0);
    diskState->set_errmsg("disk ok");
    req->set_allocated_diskstate(diskState);
    req->set_diskcapacity(100);
    req->set_diskused(50);
    req->set_leadercount(0);
    req->set_copysetcount(0);
    auto stats = new ChunkServerStatisticInfo();
    stats->set_readrate(100);
    stats->set_writerate(100);
    stats->set_readiops(100);
    stats->set_writeiops(100);
    stats->set_chunksizeusedbytes(100);
    stats->set_chunksizeleftbytes(100);
    stats->set_chunksizetrashedbytes(100);
    stats->set_chunkfilepoolsize(200);
    req->set_allocated_stats(stats);
}

void HeartbeatIntegrationCommon::AddCopySetToRequest(
    ChunkServerHeartbeatRequest *req, const CopySetInfo &csInfo,
    ConfigChangeType type) {
    auto info = req->add_copysetinfos();
    info->set_logicalpoolid(csInfo.GetLogicalPoolId());
    info->set_copysetid(csInfo.GetId());
    info->set_epoch(csInfo.GetEpoch());

    std::string leaderStr;
    for (auto id : csInfo.GetCopySetMembers()) {
        ChunkServer out;
        EXPECT_TRUE(topology_->GetChunkServer(id, &out))
            << "get chunkserver: " << id << " error";
        std::string ipport =
            out.GetHostIp() + ":" + std::to_string(out.GetPort()) + ":0";
        if (csInfo.GetLeader() == id) {
            leaderStr = ipport;
        }
        auto replica = info->add_peers();
        replica->set_address(ipport.c_str());
    }

    auto replica = new ::curve::common::Peer();
    replica->set_address(leaderStr.c_str());
    info->set_allocated_leaderpeer(replica);

    if (csInfo.HasCandidate()) {
        ChunkServer out;
        EXPECT_TRUE(topology_->GetChunkServer(csInfo.GetCandidate(), &out))
            << "get chunkserver: " << csInfo.GetCandidate() << " error";
        std::string ipport =
            out.GetHostIp() + ":" + std::to_string(out.GetPort()) + ":0";
        ConfigChangeInfo *confChxInfo = new ConfigChangeInfo();
        auto replica = new ::curve::common::Peer();
        replica->set_address(ipport.c_str());
        confChxInfo->set_allocated_peer(replica);
        confChxInfo->set_type(type);
        confChxInfo->set_finished(false);
        info->set_allocated_configchangeinfo(confChxInfo);
    }
}

void HeartbeatIntegrationCommon::AddOperatorToOpController(const Operator &op) {
    auto opController = coordinator_->GetOpController();
    ASSERT_TRUE(opController->AddOperator(op));
}

void HeartbeatIntegrationCommon::RemoveOperatorFromOpController(
    const CopySetKey &id) {
    auto opController = coordinator_->GetOpController();
    opController->RemoveOperator(id);
}

void HeartbeatIntegrationCommon::PrepareBasicCluseter() {
    assert(topology_ != nullptr);

    // add physical pool
    PoolIdType physicalPoolId = 1;
    PhysicalPool ppool(1, "testPhysicalPool", "descPhysicalPool");
    PrepareAddPhysicalPool(ppool);

    // add logical pool
    PoolIdType logicalPoolId = 1;
    LogicalPool::RedundanceAndPlaceMentPolicy rap;
    rap.pageFileRAP.copysetNum = 3;
    rap.pageFileRAP.replicaNum = 3;
    rap.pageFileRAP.zoneNum = 3;
    LogicalPool lpool(logicalPoolId, "testLogicalPool", physicalPoolId,
                      LogicalPoolType::PAGEFILE, rap, LogicalPool::UserPolicy(),
                      0x888, true, true);
    PrepareAddLogicalPool(lpool);

    // add 3 zones
    std::string desc = "descZone";
    Zone zone1(1, "test-zone1", physicalPoolId, desc);
    PrepareAddZone(zone1);
    Zone zone2(2, "test-zone2", physicalPoolId, desc);
    PrepareAddZone(zone2);
    Zone zone3(3, "test-zone3", physicalPoolId, desc);
    PrepareAddZone(zone3);

    // add 3 servers
    Server server1(1, "test1", "10.198.100.1", 0, "10.198.100.1", 0, 1,
                   physicalPoolId, "");
    PrepareAddServer(server1);
    Server server2(2, "test2", "10.198.100.2", 0, "10.198.100.2", 0, 2,
                   physicalPoolId, "");
    PrepareAddServer(server2);
    Server server3(3, "test3", "10.198.100.3", 0, "10.198.100.3", 0, 3,
                   physicalPoolId, "");
    PrepareAddServer(server3);

    // add 3 chunkservers
    ChunkServer cs1(1, "testToken", "nvme", 1, "10.198.100.1", 9000, "/");
    PrepareAddChunkServer(cs1);
    ChunkServer cs2(2, "testToken", "nvme", 2, "10.198.100.2", 9000, "/");
    PrepareAddChunkServer(cs2);
    ChunkServer cs3(3, "testToken", "nvme", 3, "10.198.100.3", 9000, "/");
    PrepareAddChunkServer(cs3);

    // add copyset
    PrepareAddCopySet(1, 1, std::set<ChunkServerIdType>{ 1, 2, 3 });
}

void HeartbeatIntegrationCommon::InitHeartbeatOption(
    Configuration *conf, HeartbeatOption *heartbeatOption) {
    heartbeatOption->heartbeatIntervalMs =
        conf->GetIntValue("mds.heartbeat.intervalMs");
    heartbeatOption->heartbeatMissTimeOutMs =
        conf->GetIntValue("mds.heartbeat.misstimeoutMs");
    heartbeatOption->offLineTimeOutMs =
        conf->GetIntValue("mds.heartbeat.offlinetimeoutMs");
    heartbeatOption->cleanFollowerAfterMs =
        conf->GetIntValue("mds.heartbeat.clean_follower_afterMs");
}

void HeartbeatIntegrationCommon::InitSchedulerOption(
    Configuration *conf, ScheduleOption *scheduleOption) {
    scheduleOption->enableCopysetScheduler =
        conf->GetBoolValue("mds.enable.copyset.scheduler");
    scheduleOption->enableLeaderScheduler =
        conf->GetBoolValue("mds.enable.leader.scheduler");
    scheduleOption->enableRecoverScheduler =
        conf->GetBoolValue("mds.enable.recover.scheduler");
    scheduleOption->enableReplicaScheduler =
        conf->GetBoolValue("mds.replica.replica.scheduler");

    scheduleOption->copysetSchedulerIntervalSec =
        conf->GetIntValue("mds.copyset.scheduler.intervalSec");
    scheduleOption->leaderSchedulerIntervalSec =
        conf->GetIntValue("mds.leader.scheduler.intervalSec");
    scheduleOption->recoverSchedulerIntervalSec =
        conf->GetIntValue("mds.recover.scheduler.intervalSec");
    scheduleOption->replicaSchedulerIntervalSec =
        conf->GetIntValue("mds.replica.scheduler.intervalSec");

    scheduleOption->operatorConcurrent =
        conf->GetIntValue("mds.schduler.operator.concurrent");
    scheduleOption->transferLeaderTimeLimitSec =
        conf->GetIntValue("mds.schduler.transfer.limitSec");
    scheduleOption->addPeerTimeLimitSec =
        conf->GetIntValue("mds.scheduler.add.limitSec");
    scheduleOption->removePeerTimeLimitSec =
        conf->GetIntValue("mds.scheduler.remove.limitSec");

    scheduleOption->copysetNumRangePercent =
        conf->GetDoubleValue("mds.scheduler.copysetNumRangePercent");
    scheduleOption->scatterWithRangePerent =
        conf->GetDoubleValue("mds.schduler.scatterWidthRangePerent");
    scheduleOption->chunkserverFailureTolerance =
        conf->GetIntValue("mds.chunkserver.failure.tolerance");
    scheduleOption->chunkserverCoolingTimeSec =
        conf->GetIntValue("mds.scheduler.chunkserver.cooling.timeSec");
}

void HeartbeatIntegrationCommon::BuildBasicCluster() {
    // init topology
    TopologyOption topologyOption;
    topologyOption.TopologyUpdateToRepoSec =
        conf_.GetIntValue("mds.topology.TopologyUpdateToRepoSec");
    auto idGen = std::make_shared<DefaultIdGenerator>();
    auto tokenGen = std::make_shared<DefaultTokenGenerator>();

    auto topologyStorage =
        std::make_shared<topology::FakeTopologyStorage>();
    topology_ =
        std::make_shared<TopologyImpl>(idGen, tokenGen, topologyStorage);
    ASSERT_EQ(kTopoErrCodeSuccess, topology_->Init(topologyOption));

    // init topology manager
    topologyStat_ =
        std::make_shared<TopologyStatImpl>(topology_);
    topologyStat_->Init();
    auto copysetManager = std::make_shared<CopysetManager>(CopysetOption());
    auto allocStat = std::make_shared<MockAllocStatistic>();
    auto topologyServiceManager = std::make_shared<TopologyServiceManager>(
        topology_, topologyStat_, nullptr, copysetManager, nullptr);

    // 初始化basic集群
    PrepareBasicCluseter();

    // init coordinator
    ScheduleOption scheduleOption;
    InitSchedulerOption(&conf_, &scheduleOption);
    auto scheduleMetrics = std::make_shared<ScheduleMetrics>(topology_);
    auto topoAdapter = std::make_shared<TopoAdapterImpl>(
        topology_, topologyServiceManager, topologyStat_);
    coordinator_ = std::make_shared<Coordinator>(topoAdapter);
    coordinator_->InitScheduler(scheduleOption, scheduleMetrics);

    // init heartbeatManager
    HeartbeatOption heartbeatOption;
    InitHeartbeatOption(&conf_, &heartbeatOption);
    heartbeatOption.mdsStartTime = steady_clock::now();
    heartbeatManager_ = std::make_shared<HeartbeatManager>(
        heartbeatOption, topology_, topologyStat_, coordinator_);
    heartbeatManager_->Init();
    heartbeatManager_->Run();

    // 启动心跳rpc
    listenAddr_ = conf_.GetStringValue("mds.listen.addr");
    heartbeatService_ =
        std::make_shared<HeartbeatServiceImpl>(heartbeatManager_);
    ASSERT_EQ(0, server_.AddService(heartbeatService_.get(),
                                    brpc::SERVER_DOESNT_OWN_SERVICE));
    brpc::ServerOptions option;
    option.idle_timeout_sec = -1;
    ASSERT_EQ(0, server_.Start(listenAddr_.c_str(), &option));
}

}  // namespace mds
}  // namespace curve
