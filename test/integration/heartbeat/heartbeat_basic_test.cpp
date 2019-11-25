/*
 * Project: curve
 * Created Date: 2019-06-11
 * Author: lixiaocui
 * Copyright (c) 2018 netease
 */

#include <gtest/gtest.h>
#include <glog/logging.h>
#include <brpc/channel.h>
#include <brpc/server.h>

#include <memory>
#include <thread> //NOLINT
#include <chrono> //NOLINT
#include <string>
#include <vector>
#include <set>

#include "src/common/configuration.h"
#include "src/mds/topology/topology_config.h"
#include "src/mds/common/mds_define.h"
#include "src/mds/topology/topology_item.h"
#include "src/mds/topology/topology.h"
#include "src/mds/topology/topology_token_generator.h"
#include "src/mds/topology/topology_service_manager.h"
#include "src/mds/topology/topology_storge.h"
#include "src/mds/nameserver2/idgenerator/chunk_id_generator.h"
#include "src/mds/heartbeat/heartbeat_manager.h"
#include "src/mds/heartbeat/heartbeat_service.h"
#include "src/mds/heartbeat/chunkserver_healthy_checker.h"
#include "src/mds/schedule/topoAdapter.h"
#include "src/mds/schedule/operator.h"
#include "src/mds/copyset/copyset_manager.h"
#include "src/mds/copyset/copyset_config.h"
#include "src/mds/schedule/scheduleMetrics.h"
#include "proto/topology.pb.h"
#include "proto/heartbeat.pb.h"
#include "proto/common.pb.h"
#include "src/common/timeutility.h"

using ::curve::common::Configuration;

using ::curve::mds::topology::TopologyOption;
using ::curve::mds::topology::kTopoErrCodeSuccess;
using ::curve::mds::topology::PoolIdType;
using ::curve::mds::topology::ZoneIdType;
using ::curve::mds::topology::ChunkServerIdType;
using ::curve::mds::topology::ServerIdType;
using ::curve::mds::topology::CopySetIdType;
using ::curve::mds::topology::LogicalPoolType;
using ::curve::mds::topology::PhysicalPool;
using ::curve::mds::topology::LogicalPool;
using ::curve::mds::topology::Zone;
using ::curve::mds::topology::TopologyImpl;
using ::curve::mds::topology::TopologyStatImpl;
using ::curve::mds::topology::TopologyServiceManager;
using ::curve::mds::topology::DefaultTopologyStorage;
using ::curve::mds::topology::ChunkServerState;
using ::curve::mds::topology::DefaultIdGenerator;
using ::curve::mds::topology::DefaultTokenGenerator;
using ::curve::mds::topology::UNINTIALIZE_ID;

using ::curve::mds::heartbeat::HeartbeatManager;
using ::curve::mds::heartbeat::HeartbeatServiceImpl;
using ::curve::mds::heartbeat::HeartbeatOption;
using ::curve::mds::heartbeat::ChunkServerStatisticInfo;
using ::curve::mds::heartbeat::ChunkServerHeartbeatRequest;
using ::curve::mds::heartbeat::ChunkServerHeartbeatResponse;
using ::curve::mds::heartbeat::HeartbeatService_Stub;
using ::curve::mds::heartbeat::CopySetConf;
using ::curve::mds::heartbeat::ConfigChangeType;

using ::curve::mds::copyset::CopysetManager;
using ::curve::mds::copyset::CopysetOption;

using ::curve::mds::schedule::TopoAdapterImpl;
using ::curve::mds::schedule::ScheduleOption;
using ::curve::mds::schedule::OperatorPriority;
using ::curve::mds::schedule::Operator;
using ::curve::mds::schedule::OperatorStep;
using ::curve::mds::schedule::TransferLeader;
using ::curve::mds::schedule::AddPeer;
using ::curve::mds::schedule::RemovePeer;
using ::curve::mds::schedule::ScheduleMetrics;

namespace curve {
namespace mds {

#define SENDHBOK false
#define SENDHBFAIL true

class HeartbeatBasicTest : public ::testing::Test {
 protected:
    void PrepareAddLogicalPool(
        PoolIdType id = 0x01,
        const std::string &name = "testLogicalPool",
        PoolIdType phyPoolId = 0x11,
        LogicalPoolType type = LogicalPoolType::PAGEFILE,
        const LogicalPool::RedundanceAndPlaceMentPolicy &rap =
            LogicalPool::RedundanceAndPlaceMentPolicy(),
        const LogicalPool::UserPolicy &policy = LogicalPool::UserPolicy(),
        uint64_t createTime = 0x888) {
        LogicalPool pool(id, name, phyPoolId, type, rap, policy, createTime,
                                    true);
        int ret = topology_->AddLogicalPool(pool);
        EXPECT_EQ(topology::kTopoErrCodeSuccess, ret)
            << "should have PrepareAddPhysicalPool()";
    }

    void PrepareAddPhysicalPool(PoolIdType id = 0x11,
                            const std::string &name = "testPhysicalPool",
                            const std::string &desc = "descPhysicalPool") {
        PhysicalPool pool(id, name, desc);
        int ret = topology_->AddPhysicalPool(pool);
        EXPECT_EQ(topology::kTopoErrCodeSuccess, ret);
    }

    void PrepareAddZone(ZoneIdType id = 0x21,
                        const std::string &name = "testZone",
                        PoolIdType physicalPoolId = 0x11,
                        const std::string &desc = "descZone") {
        Zone zone(id, name, physicalPoolId, desc);
        int ret = topology_->AddZone(zone);
        EXPECT_EQ(topology::kTopoErrCodeSuccess, ret) <<
            "should have PrepareAddPhysicalPool()";
    }

    void PrepareAddServer(ServerIdType id = 0x31,
                            const std::string &hostName = "testServer",
                            const std::string &internalHostIp = "127.0.0.1",
                            const std::string &externalHostIp = "127.0.0.1",
                            ZoneIdType zoneId = 0x21,
                            PoolIdType physicalPoolId = 0x11,
                            const std::string &desc = "descServer") {
        Server server(id, hostName, internalHostIp, 0, externalHostIp, 0,
                            zoneId, physicalPoolId, desc);
        int ret = topology_->AddServer(server);
        EXPECT_EQ(topology::kTopoErrCodeSuccess, ret)
                            << "should have PrepareAddZone()";
    }

    void PrepareAddChunkServer(ChunkServerIdType id = 0x41,
                                const std::string &token = "testToken",
                                const std::string &diskType = "nvme",
                                ServerIdType serverId = 0x31,
                                const std::string &hostIp = "127.0.0.1",
                                uint32_t port = 1024,
                                const std::string &diskPath = "/",
                                OnlineState state = OnlineState::ONLINE ) {
        ChunkServer cs(id, token, diskType, serverId, hostIp, port, diskPath);
        cs.SetOnlineState(state);
        int ret = topology_->AddChunkServer(cs);
        EXPECT_EQ(topology::kTopoErrCodeSuccess, ret)
                            << "should have PrepareAddServer()";
    }

    void PrepareAddCopySet(CopySetIdType copysetId, PoolIdType logicalPoolId,
                            const std::set<ChunkServerIdType> &members) {
        CopySetInfo cs(logicalPoolId, copysetId);
        cs.SetCopySetMembers(members);
        int ret = topology_->AddCopySet(cs);
        EXPECT_EQ(topology::kTopoErrCodeSuccess, ret)
                        << "should have PrepareAddLogicalPool()";
    }

    void SendHeartbeat(const ChunkServerHeartbeatRequest& request,
        bool expectFailed, ChunkServerHeartbeatResponse* response) {
        // init brpc client
        brpc::Channel channel;
        ASSERT_EQ(0, channel.Init(listenAddr.c_str(), NULL));
        HeartbeatService_Stub stub(&channel);
        brpc::Controller cntl;
        cntl.set_timeout_ms(100);

        // send request
        stub.ChunkServerHeartbeat(&cntl, &request, response, NULL);
        EXPECT_EQ(expectFailed, cntl.Failed())
            << "heartbeat fail: " << cntl.ErrorText();
    }

    void BuildBasicChunkServerRequest(
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
        req->set_allocated_stats(stats);
    }

    void AddCopySetToRequest(ChunkServerHeartbeatRequest *req,
        CopySetIdType copysetId, PoolIdType logicalPoolId, uint64_t epoch,
        ChunkServerIdType leader, const std::set<ChunkServerIdType> &members,
        ChunkServerIdType candidateId = UNINTIALIZE_ID,
        ConfigChangeType type = ConfigChangeType::NONE, bool finished = false) {
        auto info = req->add_copysetinfos();
        info->set_logicalpoolid(logicalPoolId);
        info->set_copysetid(copysetId);
        info->set_epoch(epoch);

        std::string leaderStr;
        for (auto id : members) {
            ChunkServer out;
            EXPECT_TRUE(topology_->GetChunkServer(id, &out))
                << "get chunkserver: " << id << " error";
            std::string ipport =
                out.GetHostIp() + ":" + std::to_string(out.GetPort()) + ":0";
            if (leader == id) {
                leaderStr = ipport;
            }
            auto replica = info->add_peers();
            replica->set_address(ipport.c_str());
        }

        auto replica = new ::curve::common::Peer();
        replica->set_address(leaderStr.c_str());
        info->set_allocated_leaderpeer(replica);

        if (candidateId != UNINTIALIZE_ID) {
            ChunkServer out;
            EXPECT_TRUE(topology_->GetChunkServer(candidateId, &out))
                << "get chunkserver: " << candidateId << " error";
            std::string ipport =
                out.GetHostIp() + ":" + std::to_string(out.GetPort()) + ":0";
            ConfigChangeInfo *confChxInfo = new ConfigChangeInfo();
            auto replica = new ::curve::common::Peer();
            replica->set_address(ipport.c_str());
            confChxInfo->set_allocated_peer(replica);
            confChxInfo->set_type(type);
            confChxInfo->set_finished(finished);
            info->set_allocated_configchangeinfo(confChxInfo);
        }
    }

    void AddOperatorToOpController(
        EpochType startEpoch, const CopySetKey &id, OperatorPriority pri,
        std::shared_ptr<OperatorStep> step,
        steady_clock::duration timeLimit = std::chrono::seconds(3)) {
        Operator op(
            startEpoch, id, pri, std::chrono::steady_clock::now(), step);
        op.timeLimit = timeLimit;
        auto opController = coordinator_->GetOpController();
        ASSERT_TRUE(opController->AddOperator(op));
    }

    void PrepareBasicCluseter() {
        assert(topology_ != nullptr);

        // add physical pool
        PoolIdType physicalPoolId = 1;
        PrepareAddPhysicalPool(1);

        // add logical pool
        PoolIdType logicalPoolId = 1;
        LogicalPool::RedundanceAndPlaceMentPolicy rap;
        rap.pageFileRAP.copysetNum = 3;
        rap.pageFileRAP.replicaNum = 3;
        rap.pageFileRAP.zoneNum = 3;
        PrepareAddLogicalPool(
            1, "", logicalPoolId, LogicalPoolType::PAGEFILE, rap);

        // add 3 zones
        PrepareAddZone(1, "test-zone1", physicalPoolId);
        PrepareAddZone(2, "test-zone2", physicalPoolId);
        PrepareAddZone(3, "test-zone3", physicalPoolId);

        // add 3 servers
        PrepareAddServer(
            1, "test1", "10.198.100.1", "10.198.100.1", 1, physicalPoolId, "");
        PrepareAddServer(
            2, "test2", "10.198.100.2", "10.198.100.2", 2, physicalPoolId, "");
        PrepareAddServer(
            3, "test3", "10.198.100.3", "10.198.100.3", 3, physicalPoolId, "");

        // add 3 chunkservers
        PrepareAddChunkServer(1, "testToken", "nvme", 1, "10.198.100.1", 9000);
        PrepareAddChunkServer(2, "testToken", "nvme", 2, "10.198.100.2", 9000);
        PrepareAddChunkServer(3, "testToken", "nvme", 3, "10.198.100.3", 9000);

        // add copyset
        PrepareAddCopySet(1, 1, std::set<ChunkServerIdType>{1, 2, 3});
    }

    void InitConfiguration() {
        // db相关配置设置
        conf.SetStringValue("mds.DbName", "heartbeat_basic_test_mds");
        conf.SetStringValue("mds.DbUser", "root");
        conf.SetStringValue("mds.DbUrl", "localhost");
        conf.SetStringValue("mds.DbPassword", "qwer");
        conf.SetIntValue("mds.DbPoolSize", 16);
        conf.SetIntValue("mds.topology.ChunkServerStateUpdateSec", 0);

        // heartbeat相关配置设置
        conf.SetIntValue("mds.heartbeat.intervalMs", 100);
        conf.SetIntValue("mds.heartbeat.misstimeoutMs", 300);
        conf.SetIntValue("mds.heartbeat.offlinetimeoutMs", 500);
        conf.SetIntValue("mds.heartbeat.clean_follower_afterMs", 500);

        // mds监听端口号
        conf.SetStringValue("mds.listen.addr", "127.0.0.1:6879");

        // scheduler相关的内容
        conf.SetBoolValue("mds.enable.copyset.scheduler", false);
        conf.SetBoolValue("mds.enable.leader.scheduler", false);
        conf.SetBoolValue("mds.enable.recover.scheduler", false);
        conf.SetBoolValue("mds.replica.replica.scheduler", false);

        conf.SetIntValue("mds.copyset.scheduler.intervalSec", 300);
        conf.SetIntValue("mds.leader.scheduler.intervalSec", 300);
        conf.SetIntValue("mds.recover.scheduler.intervalSec", 300);
        conf.SetIntValue("mds.replica.scheduler.intervalSec", 300);

        conf.SetIntValue("mds.schduler.operator.concurrent", 4);
        conf.SetIntValue("mds.schduler.transfer.limitSec", 10);
        conf.SetIntValue("mds.scheduler.add.limitSec", 10);
        conf.SetIntValue("mds.scheduler.remove.limitSec", 10);
        conf.SetDoubleValue("mds.scheduler.copysetNumRangePercent", 0.05);
        conf.SetDoubleValue("mds.schduler.scatterWidthRangePerent", 0.2);
        conf.SetIntValue("mds.scheduler.minScatterWidth", 50);
    }

    void InitHeartbeatOption(
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

    void InitSchedulerOption(
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

    void UpdateCopysetTopo(CopySetIdType copysetId, PoolIdType logicalPoolId,
                           uint64_t epoch, ChunkServerIdType leader,
                           const std::set<ChunkServerIdType> &members,
                           ChunkServerIdType candidate = UNINTIALIZE_ID) {
        ::curve::mds::topology::CopySetInfo copysetInfo;
        ASSERT_TRUE(topology_->GetCopySet(
            CopySetKey{logicalPoolId, copysetId}, &copysetInfo));
        copysetInfo.SetEpoch(epoch);
        copysetInfo.SetLeader(leader);
        copysetInfo.SetCopySetMembers(members);
        if (candidate != UNINTIALIZE_ID) {
            copysetInfo.SetCandidate(candidate);
        }
        ASSERT_EQ(0, topology_->UpdateCopySetTopo(copysetInfo));
    }

    void PrepareMdsWithCandidateOpOnGoing() {
        // 构造mds中copyset当前状态
        // copyset-1(epoch=5, peers={1,2,3}, leader=1, candidate=10);
        PrepareAddChunkServer(10, "testtoekn", "nvme", 3, "10.198.100.3", 9001);
        UpdateCopysetTopo(1, 1, 5, 1, std::set<ChunkServerIDType>{1, 2, 3}, 10);

        // 构造scheduler当前的状态
        AddOperatorToOpController(5, CopySetKey{1, 1},
            OperatorPriority::NormalPriority, std::make_shared<AddPeer>(10));
    }

    void PrepareMdsNoCnandidateOpOnGoing() {
        // 构造mds中copyset当前状态
        // copyset-1(epoch=5, peers={1,2,3}, leader=1);
        PrepareAddChunkServer(10, "testtoekn", "nvme", 3, "10.198.100.3", 9001);
        UpdateCopysetTopo(1, 1, 5, 1, std::set<ChunkServerIDType>{1, 2, 3});

        // 构造scheduler当前的状态
        AddOperatorToOpController(5, CopySetKey{1, 1},
            OperatorPriority::NormalPriority, std::make_shared<AddPeer>(10));
    }

    void SetUp() override {
        // init configuration
        InitConfiguration();

        // init repo
        mdsRepo_ = std::make_shared<MdsRepo>();
        ASSERT_EQ(mdsRepo_->connectDB(
                conf.GetStringValue("mds.DbName"),
                conf.GetStringValue("mds.DbUser"),
                conf.GetStringValue("mds.DbUrl"),
                conf.GetStringValue("mds.DbPassword"),
                conf.GetIntValue("mds.DbPoolSize")), OperationOK);
        ASSERT_EQ(mdsRepo_->dropDataBase(), OperationOK);
        ASSERT_EQ(mdsRepo_->createDatabase(), OperationOK);
        ASSERT_EQ(mdsRepo_->useDataBase(), OperationOK);
        ASSERT_EQ(mdsRepo_->createAllTables(), OperationOK);
        topologyStorage_ =
            std::make_shared<DefaultTopologyStorage>(mdsRepo_);

        // init topology
        TopologyOption topologyOption;
        topologyOption.TopologyUpdateToRepoSec =
            conf.GetIntValue("mds.topology.TopologyUpdateToRepoSec");
        auto idGen = std::make_shared<DefaultIdGenerator>();
        auto tokenGen = std::make_shared<DefaultTokenGenerator>();
        topology_ =
            std::make_shared<TopologyImpl>(idGen, tokenGen, topologyStorage_);
        ASSERT_EQ(kTopoErrCodeSuccess, topology_->init(topologyOption));

        // init topology manager
        auto copysetManager = std::make_shared<CopysetManager>(CopysetOption());
        auto topologyServiceManager =
            std::make_shared<TopologyServiceManager>(topology_, copysetManager);

        topologyStat_ =
            std::make_shared<TopologyStatImpl>(topology_);
        topologyStat_->Init();

        // 初始化basic集群
        PrepareBasicCluseter();

        // init coordinator
        ScheduleOption scheduleOption;
        InitSchedulerOption(&conf, &scheduleOption);
        auto scheduleMetrics = std::make_shared<ScheduleMetrics>(topology_);
        auto topoAdapter = std::make_shared<TopoAdapterImpl>(
                topology_, topologyServiceManager, topologyStat_);
        coordinator_ = std::make_shared<Coordinator>(topoAdapter);
        coordinator_->InitScheduler(scheduleOption, scheduleMetrics);

        // init heartbeatManager
        HeartbeatOption heartbeatOption;
        InitHeartbeatOption(&conf, &heartbeatOption);
        heartbeatManager_  = std::make_shared<HeartbeatManager>(
                heartbeatOption, topology_, topologyStat_, coordinator_);
        heartbeatManager_->Init();
        heartbeatManager_->Run();

        // 启动心跳rpc
        listenAddr = conf.GetStringValue("mds.listen.addr");
        heartbeatService_ =
            std::make_shared<HeartbeatServiceImpl>(heartbeatManager_);
        ASSERT_EQ(0, server_.AddService(heartbeatService_.get(),
                                brpc::SERVER_DOESNT_OWN_SERVICE));
        brpc::ServerOptions option;
        option.idle_timeout_sec = -1;
        ASSERT_EQ(0, server_.Start(listenAddr.c_str(), &option));
    }

    void TearDown() {
        ASSERT_EQ(0, server_.Stop(100));
        ASSERT_EQ(0, server_.Join());
    }

 protected:
    Configuration conf;
    std::string listenAddr;
    brpc::Server server_;

    std::shared_ptr<TopologyImpl> topology_;
    std::shared_ptr<TopologyStatImpl> topologyStat_;
    std::shared_ptr<MdsRepo> mdsRepo_;
    std::shared_ptr<DefaultTopologyStorage> topologyStorage_;
    std::shared_ptr<HeartbeatManager> heartbeatManager_;
    std::shared_ptr<HeartbeatServiceImpl> heartbeatService_;
    std::shared_ptr<Coordinator> coordinator_;
};

TEST_F(HeartbeatBasicTest, test_request_no_chunkserverID) {
    // 空的HeartbeatRequest
    ChunkServerHeartbeatRequest req;
    ChunkServerHeartbeatResponse rep;
    SendHeartbeat(req, SENDHBFAIL, &rep);
}

TEST_F(HeartbeatBasicTest, test_mds_donnot_has_this_chunkserver) {
    // mds不存在该chunkserver
    ChunkServerHeartbeatRequest req;
    BuildBasicChunkServerRequest(3, &req);
    req.set_chunkserverid(4);
    ChunkServerHeartbeatResponse rep;
    SendHeartbeat(req, SENDHBOK, &rep);

    ASSERT_EQ(::curve::mds::heartbeat::hbChunkserverUnknown, rep.statuscode());
}

TEST_F(HeartbeatBasicTest, test_chunkserver_ip_port_not_match) {
    // chunkserver上报的id相同，ip和port不匹配
    // ip不匹配
    ChunkServerHeartbeatRequest req;
    BuildBasicChunkServerRequest(3, &req);
    req.set_ip("127.0.0.1");
    ChunkServerHeartbeatResponse rep;
    SendHeartbeat(req, SENDHBOK, &rep);
    ASSERT_EQ(
        ::curve::mds::heartbeat::hbChunkserverIpPortNotMatch, rep.statuscode());

    // port不匹配
    req.set_ip("10.198.100.3");
    req.set_port(1111);
    SendHeartbeat(req, SENDHBOK, &rep);
    ASSERT_EQ(
        ::curve::mds::heartbeat::hbChunkserverIpPortNotMatch, rep.statuscode());

    // token不匹配
    req.set_ip("10.198.100.3");
    req.set_port(9000);
    req.set_token("youdao");
    SendHeartbeat(req, SENDHBOK, &rep);
    ASSERT_EQ(
        ::curve::mds::heartbeat::hbChunkserverTokenNotMatch, rep.statuscode());
}

TEST_F(HeartbeatBasicTest, test_chunkserver_offline_then_online) {
    // chunkserver上报心跳时间间隔大于offline
    // sleep 800ms, 该chunkserver onffline状态
    std::this_thread::sleep_for(std::chrono::milliseconds(800));
    ChunkServer out;
    topology_->GetChunkServer(1, &out);
    ASSERT_EQ(OnlineState::OFFLINE, out.GetOnlineState());

    // chunkserver上报心跳，chunkserver online
    ChunkServerHeartbeatRequest req;
    BuildBasicChunkServerRequest(out.GetId(), &req);
    ChunkServerHeartbeatResponse rep;
    SendHeartbeat(req, SENDHBOK, &rep);

    // 后台健康检查程序把chunksrver更新为onlinne状态
    uint64_t now = ::curve::common::TimeUtility::GetTimeofDaySec();
    bool updateSuccess = false;
    while (::curve::common::TimeUtility::GetTimeofDaySec() - now <= 2) {
        topology_->GetChunkServer(1, &out);
        if (OnlineState::ONLINE == out.GetOnlineState()) {
            updateSuccess = true;
            break;
        }
    }
    ASSERT_TRUE(updateSuccess);
}

TEST_F(HeartbeatBasicTest, test_copysets_in_mds_is_initial_state_condition1) {
    ChunkServerHeartbeatRequest req;
    BuildBasicChunkServerRequest(1, &req);
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));

    // copyset-1(epoch=1, peers={1,2,3}, leader=1)
    AddCopySetToRequest(
        &req, copysetInfo.GetId(), copysetInfo.GetLogicalPoolId(),
        1, 1, copysetInfo.GetCopySetMembers());
    ChunkServerHeartbeatResponse rep;
    SendHeartbeat(req, SENDHBOK, &rep);
    ASSERT_TRUE(topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(1, copysetInfo.GetEpoch());
    ASSERT_EQ(1, copysetInfo.GetLeader());
}

TEST_F(HeartbeatBasicTest, test_copysets_in_mds_is_initial_state_condition2) {
    ChunkServerHeartbeatRequest req;
    BuildBasicChunkServerRequest(1, &req);
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    PrepareAddChunkServer(4, "testtoken", "nvme", 3, "10.198.100.3", 9001);

    // copyset-1(epoch=1, peers={1,2,3}, leader=1,
    // configChangeInfo={peer: 4, type: AddPeer})
    AddCopySetToRequest(
        &req, copysetInfo.GetId(), copysetInfo.GetLogicalPoolId(),
        1, 1, copysetInfo.GetCopySetMembers(), 4, ConfigChangeType::ADD_PEER);
    ChunkServerHeartbeatResponse rep;
    SendHeartbeat(req, SENDHBOK, &rep);
    ASSERT_TRUE(topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(1, copysetInfo.GetEpoch());
    ASSERT_EQ(1, copysetInfo.GetLeader());
    ASSERT_TRUE(copysetInfo.HasCandidate());
    ASSERT_EQ(4, copysetInfo.GetCandidate());
}

TEST_F(HeartbeatBasicTest, test_copysets_in_mds_is_initial_state_condition3) {
    ChunkServerHeartbeatRequest req;
    BuildBasicChunkServerRequest(1, &req);
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    PrepareAddChunkServer(5, "testtoken", "nvme", 3, "10.198.100.3", 9001);

    // copyset-1(epoch=2, peers={1,2,3,5}, leader=1)
    auto copysetMembers = copysetInfo.GetCopySetMembers();
    copysetMembers.emplace(5);
    AddCopySetToRequest(
        &req, copysetInfo.GetId(), copysetInfo.GetLogicalPoolId(),
        1, 1, copysetMembers);
    ChunkServerHeartbeatResponse rep;
    SendHeartbeat(req, SENDHBOK, &rep);

    ASSERT_TRUE(topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(1, copysetInfo.GetEpoch());
    ASSERT_EQ(1, copysetInfo.GetLeader());
    std::set<ChunkServerIDType> res{1, 2, 3, 5};
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
    ASSERT_EQ(0, rep.needupdatecopysets_size());
}

TEST_F(HeartbeatBasicTest, test_copysets_in_mds_initial_state_condition4) {
    ChunkServerHeartbeatRequest req;
    BuildBasicChunkServerRequest(1, &req);
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    PrepareAddChunkServer(4, "testtoken", "nvme", 3, "10.198.100.3", 9001);
    PrepareAddChunkServer(5, "testtoken", "nvme", 3, "10.198.100.3", 9002);

    // copyset-1(epoch=2, peers={1,2,3,5}, leader=1,
    // configChangeInfo={peer: 4, type: AddPeer})
    auto copysetMembers = copysetInfo.GetCopySetMembers();
    copysetMembers.emplace(5);
    AddCopySetToRequest(
        &req, copysetInfo.GetId(), copysetInfo.GetLogicalPoolId(),
        1, 1, copysetMembers, 4, ConfigChangeType::ADD_PEER);
    ChunkServerHeartbeatResponse rep;
    SendHeartbeat(req, SENDHBOK, &rep);

    ASSERT_TRUE(topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(1, copysetInfo.GetEpoch());
    ASSERT_EQ(1, copysetInfo.GetLeader());
    std::set<ChunkServerIDType> res{1, 2, 3, 5};
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
    ASSERT_EQ(4, copysetInfo.GetCandidate());
    ASSERT_EQ(0, rep.needupdatecopysets_size());
}

TEST_F(HeartbeatBasicTest, test_copysets_in_mds_initial_state_condition5) {
    ChunkServerHeartbeatRequest req;
    BuildBasicChunkServerRequest(2, &req);
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));

    // copyset-1(epoch=0, peers={1,2,3}, leader=0)
    AddCopySetToRequest(
        &req, copysetInfo.GetId(), copysetInfo.GetLogicalPoolId(),
        copysetInfo.GetEpoch(), copysetInfo.GetLeader(),
        copysetInfo.GetCopySetMembers());
    ChunkServerHeartbeatResponse rep;
    SendHeartbeat(req, SENDHBOK, &rep);

    ASSERT_TRUE(topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
     ASSERT_EQ(0, copysetInfo.GetEpoch());
    ASSERT_EQ(0, copysetInfo.GetLeader());
    std::set<ChunkServerIDType> res{1, 2, 3};
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
    ASSERT_EQ(0, rep.needupdatecopysets_size());
}

TEST_F(HeartbeatBasicTest, test_copysets_in_mds_initial_state_condition6) {
    ChunkServerHeartbeatRequest req;
    BuildBasicChunkServerRequest(2, &req);
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));

    // copyset-1(epoch=0, peers={1,2,3}, leader=1)
    AddCopySetToRequest(
        &req, copysetInfo.GetId(), copysetInfo.GetLogicalPoolId(),
        copysetInfo.GetEpoch(), 1, copysetInfo.GetCopySetMembers());
    ChunkServerHeartbeatResponse rep;
    SendHeartbeat(req, SENDHBOK, &rep);

    ASSERT_TRUE(topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(0, copysetInfo.GetEpoch());
    ASSERT_EQ(0, copysetInfo.GetLeader());
    std::set<ChunkServerIDType> res{1, 2, 3};
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
    ASSERT_EQ(0, rep.needupdatecopysets_size());
}

TEST_F(HeartbeatBasicTest, test_copysets_in_mds_initial_state_condition7) {
    ChunkServerHeartbeatRequest req;
    BuildBasicChunkServerRequest(2, &req);
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));

    // copyset-1(epoch=0, peers={1,2,3}, leader=1)
    AddCopySetToRequest(
        &req, copysetInfo.GetId(), copysetInfo.GetLogicalPoolId(),
        1, 1, copysetInfo.GetCopySetMembers());
    ChunkServerHeartbeatResponse rep;
    SendHeartbeat(req, SENDHBOK, &rep);

    ASSERT_TRUE(topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(0, copysetInfo.GetEpoch());
    ASSERT_EQ(0, copysetInfo.GetLeader());
    std::set<ChunkServerIDType> res{1, 2, 3};
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
    ASSERT_EQ(0, rep.needupdatecopysets_size());
}

TEST_F(HeartbeatBasicTest, test_copysets_in_mds_initial_state_condition8) {
    ChunkServerHeartbeatRequest req;
    BuildBasicChunkServerRequest(2, &req);
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));

    // copyset-1(epoch=1, peers={1,2,3}, leader=0)
    AddCopySetToRequest(
        &req, copysetInfo.GetId(), copysetInfo.GetLogicalPoolId(),
        1, 0, copysetInfo.GetCopySetMembers());
    ChunkServerHeartbeatResponse rep;
    SendHeartbeat(req, SENDHBOK, &rep);

    ASSERT_TRUE(topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(0, copysetInfo.GetEpoch());
    ASSERT_EQ(0, copysetInfo.GetLeader());
    std::set<ChunkServerIDType> res{1, 2, 3};
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
    ASSERT_EQ(0, rep.needupdatecopysets_size());
}

TEST_F(HeartbeatBasicTest, test_copysets_in_mds_initial_state_condition9) {
    ChunkServerHeartbeatRequest req;
    BuildBasicChunkServerRequest(2, &req);
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    PrepareAddChunkServer(4, "testtoken", "nvme", 3, "10.198.100.3", 9001);

    // 上报copyset-1(epoch=2, peers={1,2,3,4}, leader=1)
    auto copysetMembers = copysetInfo.GetCopySetMembers();
    copysetMembers.emplace(4);
    AddCopySetToRequest(
        &req, copysetInfo.GetId(), copysetInfo.GetLogicalPoolId(),
        2, 1, copysetMembers);
    ChunkServerHeartbeatResponse rep;
    SendHeartbeat(req, SENDHBOK, &rep);

    ASSERT_TRUE(topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(0, copysetInfo.GetEpoch());
    ASSERT_EQ(0, copysetInfo.GetLeader());
    std::set<ChunkServerIDType> res{1, 2, 3};
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
    ASSERT_EQ(0, rep.needupdatecopysets_size());
}

TEST_F(HeartbeatBasicTest, test_copysets_in_mds_initial_state_condition10) {
    ChunkServerHeartbeatRequest req;
    BuildBasicChunkServerRequest(2, &req);
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    PrepareAddChunkServer(4, "testtoken", "nvme", 3, "10.198.100.3", 9001);

    // copyset-1(epoch=2, peers={1,2,3，4}, leader=0)
    auto copysetMembers = copysetInfo.GetCopySetMembers();
    copysetMembers.emplace(4);
    AddCopySetToRequest(
        &req, copysetInfo.GetId(), copysetInfo.GetLogicalPoolId(),
        2, 0, copysetMembers);
    ChunkServerHeartbeatResponse rep;
    SendHeartbeat(req, SENDHBOK, &rep);

    ASSERT_TRUE(topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(0, copysetInfo.GetEpoch());
    ASSERT_EQ(0, copysetInfo.GetLeader());
    std::set<ChunkServerIDType> res{1, 2, 3};
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
    ASSERT_EQ(0, rep.needupdatecopysets_size());
}

// 上报的是leader
TEST_F(HeartbeatBasicTest, test_leader_report_consistent_with_mds) {
    // 更新topology中的copyset-1(epoch=2, peers={1,2,3}, leader=1)
    UpdateCopysetTopo(1, 1, 2, 1, std::set<ChunkServerIdType>{1, 2, 3});

    // chunkserver1上报的copyset-1(epoch=2, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    BuildBasicChunkServerRequest(1, &req);
    AddCopySetToRequest(
        &req, 1, 1, 2, 1, std::set<ChunkServerIdType>{1, 2, 3});
    ChunkServerHeartbeatResponse rep;
    SendHeartbeat(req, SENDHBOK, &rep);
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(2, copysetInfo.GetEpoch());
    ASSERT_EQ(1, copysetInfo.GetLeader());
    std::set<ChunkServerIDType> res{1, 2, 3};
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_leader_report_epoch_bigger) {
    // 更新topology中的copyset-1(epoch=2, peers={1,2,3}, leader=1)
    UpdateCopysetTopo(1, 1, 2, 1, std::set<ChunkServerIdType>{1, 2, 3});

    // chunkserver1上报的copyset-1(epoch=5, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    BuildBasicChunkServerRequest(1, &req);
    AddCopySetToRequest(
        &req, 1, 1, 5, 1, std::set<ChunkServerIdType>{1, 2, 3});
    ChunkServerHeartbeatResponse rep;
    SendHeartbeat(req, SENDHBOK, &rep);

    // response为空，mds更新epoch为5
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(5, copysetInfo.GetEpoch());
    ASSERT_EQ(1, copysetInfo.GetLeader());
    std::set<ChunkServerIDType> res{1, 2, 3};
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_leader_report_epoch_bigger_leader_not_same) {
    // 更新topology中的copyset-1(epoch=2, peers={1,2,3}, leader=1)
    UpdateCopysetTopo(1, 1, 2, 1, std::set<ChunkServerIdType>{1, 2, 3});

    // chunkserver2上报的copyset-1(epoch=5, peers={1,2,3}, leader=2)
    ChunkServerHeartbeatRequest req;
    BuildBasicChunkServerRequest(2, &req);
    AddCopySetToRequest(
        &req, 1, 1, 5, 2, std::set<ChunkServerIdType>{1, 2, 3});
    ChunkServerHeartbeatResponse rep;
    SendHeartbeat(req, SENDHBOK, &rep);

    // response为空，mds更新epoch为5，leader为2
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(5, copysetInfo.GetEpoch());
    ASSERT_EQ(2, copysetInfo.GetLeader());
    std::set<ChunkServerIDType> res{1, 2, 3};
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
}

// 上报的是follower
TEST_F(HeartbeatBasicTest, test_follower_report_consistent_with_mds) {
    // 更新topology中的copyset-1(epoch=2, peers={1,2,3}, leader=1)
    UpdateCopysetTopo(1, 1, 2, 1, std::set<ChunkServerIdType>{1, 2, 3});

    // chunkserver2上报的copyset-1(epoch=2, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    BuildBasicChunkServerRequest(2, &req);
    AddCopySetToRequest(
        &req, 1, 1, 2, 1, std::set<ChunkServerIdType>{1, 2, 3});
    ChunkServerHeartbeatResponse rep;
    SendHeartbeat(req, SENDHBOK, &rep);

    // response为空
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(2, copysetInfo.GetEpoch());
    ASSERT_EQ(1, copysetInfo.GetLeader());
    std::set<ChunkServerIDType> res{1, 2, 3};
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
}

// 上报的是follower
TEST_F(HeartbeatBasicTest, test_follower_report_leader_0) {
    // 更新topology中的copyset-1(epoch=2, peers={1,2,3}, leader=1)
    UpdateCopysetTopo(1, 1, 2, 1, std::set<ChunkServerIdType>{1, 2, 3});

    // chunkserver2上报的copyset-1(epoch=2, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    BuildBasicChunkServerRequest(2, &req);
    AddCopySetToRequest(
        &req, 1, 1, 2, 0, std::set<ChunkServerIdType>{1, 2, 3});
    ChunkServerHeartbeatResponse rep;
    SendHeartbeat(req, SENDHBOK, &rep);

    // response为空
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(2, copysetInfo.GetEpoch());
    ASSERT_EQ(1, copysetInfo.GetLeader());
    std::set<ChunkServerIDType> res{1, 2, 3};
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_follower_report_epoch_bigger) {
    // 更新topology中的copyset-1(epoch=2, peers={1,2,3}, leader=1)
    UpdateCopysetTopo(1, 1, 2, 1, std::set<ChunkServerIdType>{1, 2, 3});

    // chunkserver2上报的copyset-1(epoch=3, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    BuildBasicChunkServerRequest(2, &req);
    AddCopySetToRequest(
        &req, 1, 1, 3, 1, std::set<ChunkServerIdType>{1, 2, 3});
    ChunkServerHeartbeatResponse rep;
    SendHeartbeat(req, SENDHBOK, &rep);
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(2, copysetInfo.GetEpoch());
    ASSERT_EQ(1, copysetInfo.GetLeader());
    std::set<ChunkServerIDType> res{1, 2, 3};
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_follower_report_epoch_bigger_leader_0) {
    // 更新topology中的copyset-1(epoch=2, peers={1,2,3}, leader=1)
    UpdateCopysetTopo(1, 1, 2, 1, std::set<ChunkServerIdType>{1, 2, 3});

    // chunkserver2上报的copyset-1(epoch=3, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    BuildBasicChunkServerRequest(2, &req);
    AddCopySetToRequest(
        &req, 1, 1, 3, 0, std::set<ChunkServerIdType>{1, 2, 3});
    ChunkServerHeartbeatResponse rep;
    SendHeartbeat(req, SENDHBOK, &rep);
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(2, copysetInfo.GetEpoch());
    ASSERT_EQ(1, copysetInfo.GetLeader());
    std::set<ChunkServerIDType> res{1, 2, 3};
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_follower_report_epoch_bigger_peers_not_same) {
    // 更新topology中的copyset-1(epoch=2, peers={1,2,3}, leader=1)
    UpdateCopysetTopo(1, 1, 2, 1, std::set<ChunkServerIdType>{1, 2, 3});
    PrepareAddChunkServer(4, "testtoken", "nvme", 3, "10.198.100.3", 9001);
    // chunkserver2上报的copyset-1(epoch=3, peers={1,2,3,4}, leader=1)
    ChunkServerHeartbeatRequest req;
    BuildBasicChunkServerRequest(2, &req);
    AddCopySetToRequest(
        &req, 1, 1, 3, 1, std::set<ChunkServerIdType>{1, 2, 3, 4});
    ChunkServerHeartbeatResponse rep;
    SendHeartbeat(req, SENDHBOK, &rep);
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(2, copysetInfo.GetEpoch());
    ASSERT_EQ(1, copysetInfo.GetLeader());
    std::set<ChunkServerIDType> res{1, 2, 3};
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_follower_report_epoch_smaller) {
    // 更新topology中的copyset-1(epoch=2, peers={1,2,3}, leader=1)
    UpdateCopysetTopo(1, 1, 2, 1, std::set<ChunkServerIdType>{1, 2, 3});

    // chunkserver2上报的copyset-1(epoch=1, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    BuildBasicChunkServerRequest(2, &req);
    AddCopySetToRequest(
        &req, 1, 1, 1, 1, std::set<ChunkServerIdType>{1, 2, 3});
    ChunkServerHeartbeatResponse rep;
    SendHeartbeat(req, SENDHBOK, &rep);
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(2, copysetInfo.GetEpoch());
    ASSERT_EQ(1, copysetInfo.GetLeader());
    std::set<ChunkServerIDType> res{1, 2, 3};
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_follower_report_epoch_smaller_leader_0) {
    // 更新topology中的copyset-1(epoch=2, peers={1,2,3}, leader=1)
    UpdateCopysetTopo(1, 1, 2, 1, std::set<ChunkServerIdType>{1, 2, 3});

    // chunkserver2上报的copyset-1(epoch=1, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    BuildBasicChunkServerRequest(2, &req);
    AddCopySetToRequest(
        &req, 1, 1, 1, 0, std::set<ChunkServerIdType>{1, 2, 3});
    ChunkServerHeartbeatResponse rep;
    SendHeartbeat(req, SENDHBOK, &rep);
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(2, copysetInfo.GetEpoch());
    ASSERT_EQ(1, copysetInfo.GetLeader());
    std::set<ChunkServerIDType> res{1, 2, 3};
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_follower_report_epoch_smaller_peers_not_same1) {
    // 更新topology中的copyset-1(epoch=2, peers={1,2,3}, leader=1)
    UpdateCopysetTopo(1, 1, 2, 1, std::set<ChunkServerIdType>{1, 2, 3});
    PrepareAddChunkServer(4, "testtoken", "nvme", 3, "10.198.100.3", 9001);
    // chunkserver2上报的copyset-1(epoch=1, peers={1,2,3,4}, leader=1)
    ChunkServerHeartbeatRequest req;
    BuildBasicChunkServerRequest(2, &req);
    AddCopySetToRequest(
        &req, 1, 1, 1, 1, std::set<ChunkServerIdType>{1, 2, 3, 4});
    ChunkServerHeartbeatResponse rep;
    SendHeartbeat(req, SENDHBOK, &rep);
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(2, copysetInfo.GetEpoch());
    ASSERT_EQ(1, copysetInfo.GetLeader());
    std::set<ChunkServerIDType> res{1, 2, 3};
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_follower_report_epoch_smaller_peers_not_same2) {
    // 更新topology中的copyset-1(epoch=2, peers={1,2,3}, leader=1)
    UpdateCopysetTopo(1, 1, 2, 1, std::set<ChunkServerIdType>{1, 2, 3});
    PrepareAddChunkServer(4, "testtoken", "nvme", 3, "10.198.100.3", 9001);
    // chunkserver2上报的copyset-1(epoch=1, peers={1,2,3,4}, leader=0)
    ChunkServerHeartbeatRequest req;
    BuildBasicChunkServerRequest(2, &req);
    AddCopySetToRequest(
        &req, 1, 1, 1, 0, std::set<ChunkServerIdType>{1, 2, 3, 4});
    ChunkServerHeartbeatResponse rep;
    SendHeartbeat(req, SENDHBOK, &rep);
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(2, copysetInfo.GetEpoch());
    ASSERT_EQ(1, copysetInfo.GetLeader());
    std::set<ChunkServerIDType> res{1, 2, 3};
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_follower_report_epoch_0) {
    // 更新topology中的copyset-1(epoch=2, peers={1,2,3}, leader=1)
    UpdateCopysetTopo(1, 1, 2, 1, std::set<ChunkServerIdType>{1, 2, 3});
    // chunkserver2上报的copyset-1(epoch=0, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    BuildBasicChunkServerRequest(2, &req);
    AddCopySetToRequest(
        &req, 1, 1, 0, 1, std::set<ChunkServerIdType>{1, 2, 3});
    ChunkServerHeartbeatResponse rep;
    SendHeartbeat(req, SENDHBOK, &rep);
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(2, copysetInfo.GetEpoch());
    ASSERT_EQ(1, copysetInfo.GetLeader());
    std::set<ChunkServerIDType> res{1, 2, 3};
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_follower_report_epoch_0_leader_0) {
    // 更新topology中的copyset-1(epoch=2, peers={1,2,3}, leader=1)
    UpdateCopysetTopo(1, 1, 2, 1, std::set<ChunkServerIdType>{1, 2, 3});
    // chunkserver2上报的copyset-1(epoch=0, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    BuildBasicChunkServerRequest(2, &req);
    AddCopySetToRequest(
        &req, 1, 1, 0, 0, std::set<ChunkServerIdType>{1, 2, 3});
    ChunkServerHeartbeatResponse rep;
    SendHeartbeat(req, SENDHBOK, &rep);
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(2, copysetInfo.GetEpoch());
    ASSERT_EQ(1, copysetInfo.GetLeader());
    std::set<ChunkServerIDType> res{1, 2, 3};
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_follower_report_epoch_0_peers_not_same1) {
    // 更新topology中的copyset-1(epoch=2, peers={1,2,3}, leader=1)
    UpdateCopysetTopo(1, 1, 2, 1, std::set<ChunkServerIdType>{1, 2, 3});
    PrepareAddChunkServer(4, "testtoken", "nvme", 3, "10.198.100.3", 9001);
    // chunkserver2上报的copyset-1(epoch=0, peers={1,2,3,4}, leader=1)
    ChunkServerHeartbeatRequest req;
    BuildBasicChunkServerRequest(2, &req);
    AddCopySetToRequest(
        &req, 1, 1, 0, 1, std::set<ChunkServerIdType>{1, 2, 3, 4});
    ChunkServerHeartbeatResponse rep;
    SendHeartbeat(req, SENDHBOK, &rep);
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(2, copysetInfo.GetEpoch());
    ASSERT_EQ(1, copysetInfo.GetLeader());
    std::set<ChunkServerIDType> res{1, 2, 3};
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_follower_report_epoch_0_peers_not_same2) {
    // 更新topology中的copyset-1(epoch=2, peers={1,2,3}, leader=1)
    UpdateCopysetTopo(1, 1, 2, 1, std::set<ChunkServerIdType>{1, 2, 3});
    PrepareAddChunkServer(4, "testtoken", "nvme", 3, "10.198.100.3", 9001);
    // chunkserver2上报的copyset-1(epoch=0, peers={1,2,3,4}, leader=0)
    ChunkServerHeartbeatRequest req;
    BuildBasicChunkServerRequest(2, &req);
    AddCopySetToRequest(
        &req, 1, 1, 0, 0, std::set<ChunkServerIdType>{1, 2, 3, 4});
    ChunkServerHeartbeatResponse rep;
    SendHeartbeat(req, SENDHBOK, &rep);
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(2, copysetInfo.GetEpoch());
    ASSERT_EQ(1, copysetInfo.GetLeader());
    std::set<ChunkServerIDType> res{1, 2, 3};
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
}

// 上报的不是复制组成员
TEST_F(HeartbeatBasicTest, test_other_report_consistent_with_mds) {
    // 更新topology中的copyset-1(epoch=2, peers={1,2,3}, leader=1)
    UpdateCopysetTopo(1, 1, 2, 1, std::set<ChunkServerIdType>{1, 2, 3});
    PrepareAddChunkServer(4, "testtoken", "nvme", 3, "10.198.100.3", 9001);
    // chunkserver4上报的copyset-1(epoch=2, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    BuildBasicChunkServerRequest(4, &req);
    AddCopySetToRequest(
        &req, 1, 1, 2, 1, std::set<ChunkServerIdType>{1, 2, 3});
    ChunkServerHeartbeatResponse rep;
    SendHeartbeat(req, SENDHBOK, &rep);
    // 检查response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    CopySetConf conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(3, conf.peers_size());
    ASSERT_EQ(2, conf.epoch());
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(2, copysetInfo.GetEpoch());
    ASSERT_EQ(1, copysetInfo.GetLeader());
    std::set<ChunkServerIDType> res{1, 2, 3};
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_other_report_epoch_smaller) {
    // 更新topology中的copyset-1(epoch=2, peers={1,2,3}, leader=1)
    UpdateCopysetTopo(1, 1, 2, 1, std::set<ChunkServerIdType>{1, 2, 3});
    PrepareAddChunkServer(4, "testtoken", "nvme", 3, "10.198.100.3", 9001);
    // chunkserver4上报的copyset-1(epoch=1, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    BuildBasicChunkServerRequest(4, &req);
    AddCopySetToRequest(
        &req, 1, 1, 1, 1, std::set<ChunkServerIdType>{1, 2, 3});
    ChunkServerHeartbeatResponse rep;
    SendHeartbeat(req, SENDHBOK, &rep);
    // 检查response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    CopySetConf conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(3, conf.peers_size());
    ASSERT_EQ(2, conf.epoch());
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(2, copysetInfo.GetEpoch());
    ASSERT_EQ(1, copysetInfo.GetLeader());
    std::set<ChunkServerIDType> res{1, 2, 3};
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_other_report_epoch_smaller_peers_not_same) {
    // 更新topology中的copyset-1(epoch=2, peers={1,2,3}, leader=1)
    UpdateCopysetTopo(1, 1, 2, 1, std::set<ChunkServerIdType>{1, 2, 3});
    PrepareAddChunkServer(4, "testtoken", "nvme", 3, "10.198.100.3", 9001);
    // chunkserver4上报的copyset-1(epoch=1, peers={1,2}, leader=1)
    ChunkServerHeartbeatRequest req;
    BuildBasicChunkServerRequest(4, &req);
    AddCopySetToRequest(
        &req, 1, 1, 1, 1, std::set<ChunkServerIdType>{1, 2});
    ChunkServerHeartbeatResponse rep;
    SendHeartbeat(req, SENDHBOK, &rep);
    // 检查response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    CopySetConf conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(3, conf.peers_size());
    ASSERT_EQ(2, conf.epoch());
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(2, copysetInfo.GetEpoch());
    ASSERT_EQ(1, copysetInfo.GetLeader());
    std::set<ChunkServerIDType> res{1, 2, 3};
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_other_report_epoch_0_leader_0) {
    // 更新topology中的copyset-1(epoch=2, peers={1,2,3}, leader=1)
    UpdateCopysetTopo(1, 1, 2, 1, std::set<ChunkServerIdType>{1, 2, 3});
    PrepareAddChunkServer(4, "testtoken", "nvme", 3, "10.198.100.3", 9001);
    // chunkserver4上报的copyset-1(epoch=0, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    BuildBasicChunkServerRequest(4, &req);
    AddCopySetToRequest(
        &req, 1, 1, 0, 0, std::set<ChunkServerIdType>{1, 2, 3});
    ChunkServerHeartbeatResponse rep;
    SendHeartbeat(req, SENDHBOK, &rep);
    // 检查response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    CopySetConf conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(3, conf.peers_size());
    ASSERT_EQ(2, conf.epoch());
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(2, copysetInfo.GetEpoch());
    ASSERT_EQ(1, copysetInfo.GetLeader());
    std::set<ChunkServerIDType> res{1, 2, 3};
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_other_report_epoch_0_leader_0_peers_not_same) {
    // 更新topology中的copyset-1(epoch=2, peers={1,2,3}, leader=1)
    UpdateCopysetTopo(1, 1, 2, 1, std::set<ChunkServerIdType>{1, 2, 3});
    PrepareAddChunkServer(4, "testtoken", "nvme", 3, "10.198.100.3", 9001);
    PrepareAddChunkServer(5, "testtoken", "nvme", 3, "10.198.100.3", 9090);
    // chunkserver4上报的copyset-1(epoch=0, peers={1,2,3,5}, leader=0)
    ChunkServerHeartbeatRequest req;
    BuildBasicChunkServerRequest(4, &req);
    AddCopySetToRequest(
        &req, 1, 1, 0, 0, std::set<ChunkServerIdType>{1, 2, 3, 5});
    ChunkServerHeartbeatResponse rep;
    SendHeartbeat(req, SENDHBOK, &rep);
    // 检查response
    ASSERT_EQ(1, rep.needupdatecopysets_size());
    CopySetConf conf = rep.needupdatecopysets(0);
    ASSERT_EQ(1, conf.logicalpoolid());
    ASSERT_EQ(1, conf.copysetid());
    ASSERT_EQ(3, conf.peers_size());
    ASSERT_EQ(2, conf.epoch());
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(2, copysetInfo.GetEpoch());
    ASSERT_EQ(1, copysetInfo.GetLeader());
    std::set<ChunkServerIDType> res{1, 2, 3};
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_mdsNoCandidate_OpOnGoing_condition1) {
    PrepareMdsNoCnandidateOpOnGoing();

    // chunkserver1上报的copyset-1(epoch=5, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    BuildBasicChunkServerRequest(1, &req);
    AddCopySetToRequest(
        &req, 1, 1, 5, 1, std::set<ChunkServerIdType>{1, 2, 3});
    ChunkServerHeartbeatResponse rep;
    SendHeartbeat(req, SENDHBOK, &rep);
    // 检查sheduler中的operator
    auto ops = coordinator_->GetOpController()->GetOperators();
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
    // configChangeInfo={peer: 10, type: AddPeer} )
    ChunkServerHeartbeatRequest req;
    BuildBasicChunkServerRequest(1, &req);
    AddCopySetToRequest(
        &req, 1, 1, 5, 1, std::set<ChunkServerIdType>{1, 2, 3},
            10, ConfigChangeType::ADD_PEER);
    ChunkServerHeartbeatResponse rep;
    SendHeartbeat(req, SENDHBOK, &rep);
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查topology
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
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
    BuildBasicChunkServerRequest(1, &req);
    AddCopySetToRequest(
        &req, 1, 1, 6, 1, std::set<ChunkServerIdType>{1, 2, 3});
    ChunkServerHeartbeatResponse rep;
    SendHeartbeat(req, SENDHBOK, &rep);
    // 检查sheduler中的operator
    auto ops = coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查topology
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(6, copysetInfo.GetEpoch());
    ASSERT_EQ(1, copysetInfo.GetLeader());
    std::set<ChunkServerIDType> res{1, 2, 3};
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_mdsNoCandidate_OpOnGoing_condition4) {
    PrepareMdsNoCnandidateOpOnGoing();

    // chunkserver2上报的copyset-1(epoch=6, peers={1,2,3}, leader=2)
    ChunkServerHeartbeatRequest req;
    BuildBasicChunkServerRequest(2, &req);
    AddCopySetToRequest(
        &req, 1, 1, 6, 2, std::set<ChunkServerIdType>{1, 2, 3});
    ChunkServerHeartbeatResponse rep;
    SendHeartbeat(req, SENDHBOK, &rep);
    // 检查sheduler中的operator
    auto ops = coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查topology
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(6, copysetInfo.GetEpoch());
    ASSERT_EQ(2, copysetInfo.GetLeader());
    std::set<ChunkServerIDType> res{1, 2, 3};
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_mdsNoCandidate_OpOnGoing_condition5) {
    PrepareMdsNoCnandidateOpOnGoing();

    // chunkserver1上报的copyset-1(epoch=6, peers={1,2,3,10}, leader=1)
    ChunkServerHeartbeatRequest req;
    BuildBasicChunkServerRequest(1, &req);
    AddCopySetToRequest(
        &req, 1, 1, 6, 1, std::set<ChunkServerIdType>{1, 2, 3, 10});
    ChunkServerHeartbeatResponse rep;
    SendHeartbeat(req, SENDHBOK, &rep);
    // 检查sheduler中的operator
    auto ops = coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查topology
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(6, copysetInfo.GetEpoch());
    ASSERT_EQ(1, copysetInfo.GetLeader());
    std::set<ChunkServerIDType> res{1, 2, 3, 10};
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_mdsNoCandidate_OpOnGoing_condition6) {
    PrepareMdsNoCnandidateOpOnGoing();

    // chunkserver2上报的copyset-1(epoch=5, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    BuildBasicChunkServerRequest(2, &req);
    AddCopySetToRequest(
        &req, 1, 1, 5, 1, std::set<ChunkServerIdType>{1, 2, 3});
    ChunkServerHeartbeatResponse rep;
    SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
}

TEST_F(HeartbeatBasicTest, test_mdsNoCandidate_OpOnGoing_condition7) {
    PrepareMdsNoCnandidateOpOnGoing();

    // chunkserver2上报的copyset-1(epoch=5, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    BuildBasicChunkServerRequest(2, &req);
    AddCopySetToRequest(
        &req, 1, 1, 5, 0, std::set<ChunkServerIdType>{1, 2, 3});
    ChunkServerHeartbeatResponse rep;
    SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查topology
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(5, copysetInfo.GetEpoch());
    ASSERT_EQ(1, copysetInfo.GetLeader());
    std::set<ChunkServerIDType> res{1, 2, 3};
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_mdsNoCandidate_OpOnGoing_condition8) {
    PrepareMdsNoCnandidateOpOnGoing();

    // chunkserver2上报的copyset-1(epoch=6, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    BuildBasicChunkServerRequest(2, &req);
    AddCopySetToRequest(
        &req, 1, 1, 6, 1, std::set<ChunkServerIdType>{1, 2, 3});
    ChunkServerHeartbeatResponse rep;
    SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查topology
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(5, copysetInfo.GetEpoch());
    ASSERT_EQ(1, copysetInfo.GetLeader());
    std::set<ChunkServerIDType> res{1, 2, 3};
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_mdsNoCandidate_OpOnGoing_condition9) {
    PrepareMdsNoCnandidateOpOnGoing();

    // chunkserver2上报的copyset-1(epoch=6, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    BuildBasicChunkServerRequest(2, &req);
    AddCopySetToRequest(
        &req, 1, 1, 6, 0, std::set<ChunkServerIdType>{1, 2, 3});
    ChunkServerHeartbeatResponse rep;
    SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查topology
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(5, copysetInfo.GetEpoch());
    ASSERT_EQ(1, copysetInfo.GetLeader());
    std::set<ChunkServerIDType> res{1, 2, 3};
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_mdsNoCandidate_OpOnGoing_condition10) {
    PrepareMdsNoCnandidateOpOnGoing();

    // chunkserver2上报的copyset-1(epoch=6, peers={1,2,3,10}, leader=1)
    ChunkServerHeartbeatRequest req;
    BuildBasicChunkServerRequest(2, &req);
    AddCopySetToRequest(
        &req, 1, 1, 6, 1, std::set<ChunkServerIdType>{1, 2, 3, 10});
    ChunkServerHeartbeatResponse rep;
    SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查topology
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(5, copysetInfo.GetEpoch());
    ASSERT_EQ(1, copysetInfo.GetLeader());
    std::set<ChunkServerIDType> res{1, 2, 3};
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_mdsNoCandidate_OpOnGoing_condition11) {
    PrepareMdsNoCnandidateOpOnGoing();

    // chunkserver2上报的copyset-1(epoch=6, peers={1,2,3,10}, leader=0)
    ChunkServerHeartbeatRequest req;
    BuildBasicChunkServerRequest(2, &req);
    AddCopySetToRequest(
        &req, 1, 1, 6, 0, std::set<ChunkServerIdType>{1, 2, 3, 10});
    ChunkServerHeartbeatResponse rep;
    SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查topology
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(5, copysetInfo.GetEpoch());
    ASSERT_EQ(1, copysetInfo.GetLeader());
    std::set<ChunkServerIDType> res{1, 2, 3};
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_mdsNoCandidate_OpOnGoing_condition12) {
    PrepareMdsNoCnandidateOpOnGoing();

    // chunkserver2上报的copyset-1(epoch=4, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    BuildBasicChunkServerRequest(2, &req);
    AddCopySetToRequest(
        &req, 1, 1, 4, 1, std::set<ChunkServerIdType>{1, 2, 3});
    ChunkServerHeartbeatResponse rep;
    SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
}

TEST_F(HeartbeatBasicTest, test_mdsNoCandidate_OpOnGoing_condition13) {
    PrepareMdsNoCnandidateOpOnGoing();

    // chunkserver2上报的copyset-1(epoch=4, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    BuildBasicChunkServerRequest(2, &req);
    AddCopySetToRequest(
        &req, 1, 1, 4, 0, std::set<ChunkServerIdType>{1, 2, 3});
    ChunkServerHeartbeatResponse rep;
    SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
}

TEST_F(HeartbeatBasicTest, test_mdsNoCandidate_OpOnGoing_condition14) {
    PrepareMdsNoCnandidateOpOnGoing();
    PrepareAddChunkServer(4, "testtoekn", "nvme", 3, "10.198.100.3", 9002);

    // chunkserver2上报的copyset-1(epoch=4, peers={1,2,3,4}, leader=1)
    ChunkServerHeartbeatRequest req;
    BuildBasicChunkServerRequest(2, &req);
    AddCopySetToRequest(
        &req, 1, 1, 4, 1, std::set<ChunkServerIdType>{1, 2, 3, 4});
    ChunkServerHeartbeatResponse rep;
    SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
}

TEST_F(HeartbeatBasicTest, test_mdsNoCandidate_OpOnGoing_condition15) {
    PrepareMdsNoCnandidateOpOnGoing();
    PrepareAddChunkServer(4, "testtoekn", "nvme", 3, "10.198.100.3", 9002);

    // chunkserver2上报的copyset-1(epoch=4, peers={1,2,3,4}, leader=0)
    ChunkServerHeartbeatRequest req;
    BuildBasicChunkServerRequest(2, &req);
    AddCopySetToRequest(
        &req, 1, 1, 4, 0, std::set<ChunkServerIdType>{1, 2, 3, 4});
    ChunkServerHeartbeatResponse rep;
    SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
}

TEST_F(HeartbeatBasicTest, test_mdsNoCandidate_OpOnGoing_condition16) {
    PrepareMdsNoCnandidateOpOnGoing();

    // chunkserver2上报的copyset-1(epoch=0, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    BuildBasicChunkServerRequest(2, &req);
    AddCopySetToRequest(
        &req, 1, 1, 0, 1, std::set<ChunkServerIdType>{1, 2, 3});
    ChunkServerHeartbeatResponse rep;
    SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
}

TEST_F(HeartbeatBasicTest, test_mdsNoCandidate_OpOnGoing_condition17) {
    PrepareMdsNoCnandidateOpOnGoing();

    // chunkserver2上报的copyset-1(epoch=0, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    BuildBasicChunkServerRequest(2, &req);
    AddCopySetToRequest(
        &req, 1, 1, 0, 0, std::set<ChunkServerIdType>{1, 2, 3});
    ChunkServerHeartbeatResponse rep;
    SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
}

TEST_F(HeartbeatBasicTest, test_mdsNoCandidate_OpOnGoing_condition18) {
    PrepareMdsNoCnandidateOpOnGoing();
    PrepareAddChunkServer(4, "testtoekn", "nvme", 3, "10.198.100.3", 9002);

    // chunkserver2上报的copyset-1(epoch=0, peers={1,2,3,4}, leader=1)
    ChunkServerHeartbeatRequest req;
    BuildBasicChunkServerRequest(2, &req);
    AddCopySetToRequest(
        &req, 1, 1, 0, 1, std::set<ChunkServerIdType>{1, 2, 3, 4});
    ChunkServerHeartbeatResponse rep;
    SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
}

TEST_F(HeartbeatBasicTest, test_mdsNoCandidate_OpOnGoing_condition19) {
    PrepareMdsNoCnandidateOpOnGoing();
    PrepareAddChunkServer(4, "testtoekn", "nvme", 3, "10.198.100.3", 9002);

    // chunkserver2上报的copyset-1(epoch=0, peers={1,2,3,4}, leader=0)
    ChunkServerHeartbeatRequest req;
    BuildBasicChunkServerRequest(2, &req);
    AddCopySetToRequest(
        &req, 1, 1, 0, 0, std::set<ChunkServerIdType>{1, 2, 3, 4});
    ChunkServerHeartbeatResponse rep;
    SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
}

TEST_F(HeartbeatBasicTest, test_mdsNoCandidate_OpOnGoing_condition20) {
    PrepareMdsNoCnandidateOpOnGoing();

    // chunkserver10上报的copyset-1(epoch=5, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    BuildBasicChunkServerRequest(10, &req);
    AddCopySetToRequest(
        &req, 1, 1, 5, 1, std::set<ChunkServerIdType>{1, 2, 3});
    ChunkServerHeartbeatResponse rep;
    SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
}

TEST_F(HeartbeatBasicTest, test_mdsNoCandidate_OpOnGoing_condition21) {
    PrepareMdsNoCnandidateOpOnGoing();

    // chunkserver10上报的copyset-1(epoch=5, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    BuildBasicChunkServerRequest(10, &req);
    AddCopySetToRequest(
        &req, 1, 1, 5, 0, std::set<ChunkServerIdType>{1, 2, 3});
    ChunkServerHeartbeatResponse rep;
    SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
}

TEST_F(HeartbeatBasicTest, test_mdsNoCandidate_OpOnGoing_condition22) {
    PrepareMdsNoCnandidateOpOnGoing();

    // chunkserver10上报的copyset-1(epoch=6, peers={1,2,3,10}, leader=1)
    ChunkServerHeartbeatRequest req;
    BuildBasicChunkServerRequest(10, &req);
    AddCopySetToRequest(
        &req, 1, 1, 6, 1, std::set<ChunkServerIdType>{1, 2, 3, 10});
    ChunkServerHeartbeatResponse rep;
    SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查topology
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(5, copysetInfo.GetEpoch());
    ASSERT_EQ(1, copysetInfo.GetLeader());
    std::set<ChunkServerIDType> res{1, 2, 3};
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_mdsNoCandidate_OpOnGoing_condition23) {
    PrepareMdsNoCnandidateOpOnGoing();

    // chunkserver10上报的copyset-1(epoch=6, peers={1,2,3,10}, leader=0)
    ChunkServerHeartbeatRequest req;
    BuildBasicChunkServerRequest(10, &req);
    AddCopySetToRequest(
        &req, 1, 1, 6, 0, std::set<ChunkServerIdType>{1, 2, 3, 10});
    ChunkServerHeartbeatResponse rep;
    SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查topology
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(5, copysetInfo.GetEpoch());
    ASSERT_EQ(1, copysetInfo.GetLeader());
    std::set<ChunkServerIDType> res{1, 2, 3};
    ASSERT_EQ(res, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_mdsNoCandidate_OpOnGoing_condition24) {
    PrepareMdsNoCnandidateOpOnGoing();

    // chunkserver10上报的copyset-1(epoch=4, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    BuildBasicChunkServerRequest(10, &req);
    AddCopySetToRequest(
        &req, 1, 1, 4, 1, std::set<ChunkServerIdType>{1, 2, 3});
    ChunkServerHeartbeatResponse rep;
    SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
}

TEST_F(HeartbeatBasicTest, test_mdsNoCandidate_OpOnGoing_condition25) {
    PrepareMdsNoCnandidateOpOnGoing();

    // chunkserver10上报的copyset-1(epoch=4, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    BuildBasicChunkServerRequest(10, &req);
    AddCopySetToRequest(
        &req, 1, 1, 4, 0, std::set<ChunkServerIdType>{1, 2, 3});
    ChunkServerHeartbeatResponse rep;
    SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
}

TEST_F(HeartbeatBasicTest, test_mdsNoCandidate_OpOnGoing_condition26) {
    PrepareMdsNoCnandidateOpOnGoing();
    PrepareAddChunkServer(4, "testtoekn", "nvme", 3, "10.198.100.3", 9002);

    // chunkserver10上报的copyset-1(epoch=4, peers={1,2,3,4}, leader=1)
    ChunkServerHeartbeatRequest req;
    BuildBasicChunkServerRequest(10, &req);
    AddCopySetToRequest(
        &req, 1, 1, 4, 1, std::set<ChunkServerIdType>{1, 2, 3, 4});
    ChunkServerHeartbeatResponse rep;
    SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
}

TEST_F(HeartbeatBasicTest, test_mdsNoCandidate_OpOnGoing_condition27) {
    PrepareMdsNoCnandidateOpOnGoing();
    PrepareAddChunkServer(4, "testtoekn", "nvme", 3, "10.198.100.3", 9002);

    // chunkserver10上报的copyset-1(epoch=4, peers={1,2,3,4}, leader=0)
    ChunkServerHeartbeatRequest req;
    BuildBasicChunkServerRequest(10, &req);
    AddCopySetToRequest(
        &req, 1, 1, 4, 0, std::set<ChunkServerIdType>{1, 2, 3, 4});
    ChunkServerHeartbeatResponse rep;
    SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
}

TEST_F(HeartbeatBasicTest, test_mdsNoCandidate_OpOnGoing_condition28) {
    PrepareMdsNoCnandidateOpOnGoing();

    // chunkserver10上报的copyset-1(epoch=0, peers={1,2,3}, leader=1)
    ChunkServerHeartbeatRequest req;
    BuildBasicChunkServerRequest(10, &req);
    AddCopySetToRequest(
        &req, 1, 1, 0, 1, std::set<ChunkServerIdType>{1, 2, 3});
    ChunkServerHeartbeatResponse rep;
    SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
}

TEST_F(HeartbeatBasicTest, test_mdsNoCandidate_OpOnGoing_condition29) {
    PrepareMdsNoCnandidateOpOnGoing();

    // chunkserver10上报的copyset-1(epoch=0, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    BuildBasicChunkServerRequest(10, &req);
    AddCopySetToRequest(
        &req, 1, 1, 0, 0, std::set<ChunkServerIdType>{1, 2, 3});
    ChunkServerHeartbeatResponse rep;
    SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
}

TEST_F(HeartbeatBasicTest, test_mdsNoCandidate_OpOnGoing_condition30) {
    PrepareMdsNoCnandidateOpOnGoing();
    PrepareAddChunkServer(4, "testtoekn", "nvme", 3, "10.198.100.3", 9002);

    // chunkserver10上报的copyset-1(epoch=0, peers={1,2,3,4}, leader=1)
    ChunkServerHeartbeatRequest req;
    BuildBasicChunkServerRequest(10, &req);
    AddCopySetToRequest(
        &req, 1, 1, 0, 1, std::set<ChunkServerIdType>{1, 2, 3, 4});
    ChunkServerHeartbeatResponse rep;
    SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
}

TEST_F(HeartbeatBasicTest, test_mdsNoCandidate_OpOnGoing_condition31) {
    PrepareMdsNoCnandidateOpOnGoing();
    PrepareAddChunkServer(4, "testtoekn", "nvme", 3, "10.198.100.3", 9002);

    // chunkserver10上报的copyset-1(epoch=0, peers={1,2,3,4}, leader=0)
    ChunkServerHeartbeatRequest req;
    BuildBasicChunkServerRequest(10, &req);
    AddCopySetToRequest(
        &req, 1, 1, 0, 0, std::set<ChunkServerIdType>{1, 2, 3, 4});
    ChunkServerHeartbeatResponse rep;
    SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
}

TEST_F(HeartbeatBasicTest, test_mdsNoCandidate_OpOnGoing_condition32) {
    PrepareMdsNoCnandidateOpOnGoing();
    PrepareAddChunkServer(4, "testtoekn", "nvme", 3, "10.198.100.3", 9002);

    // chunkserver4上报的copyset-1(epoch=5, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    BuildBasicChunkServerRequest(4, &req);
    AddCopySetToRequest(
        &req, 1, 1, 5, 0, std::set<ChunkServerIdType>{1, 2, 3});
    ChunkServerHeartbeatResponse rep;
    SendHeartbeat(req, SENDHBOK, &rep);

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
    PrepareAddChunkServer(4, "testtoekn", "nvme", 3, "10.198.100.3", 9002);

    // chunkserver4上报的copyset-1(epoch=4, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    BuildBasicChunkServerRequest(4, &req);
    AddCopySetToRequest(
        &req, 1, 1, 4, 0, std::set<ChunkServerIdType>{1, 2, 3});
    ChunkServerHeartbeatResponse rep;
    SendHeartbeat(req, SENDHBOK, &rep);

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
    PrepareAddChunkServer(4, "testtoekn", "nvme", 3, "10.198.100.3", 9002);
    PrepareAddChunkServer(5, "testtoekn", "nvme", 3, "10.198.100.3", 9003);

    // chunkserver4上报的copyset-1(epoch=4, peers={1,2,3,5}, leader=0)
    ChunkServerHeartbeatRequest req;
    BuildBasicChunkServerRequest(4, &req);
    AddCopySetToRequest(
        &req, 1, 1, 4, 0, std::set<ChunkServerIdType>{1, 2, 3, 5});
    ChunkServerHeartbeatResponse rep;
    SendHeartbeat(req, SENDHBOK, &rep);

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
    PrepareAddChunkServer(4, "testtoekn", "nvme", 3, "10.198.100.3", 9002);

    // chunkserver4上报的copyset-1(epoch=0, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    BuildBasicChunkServerRequest(4, &req);
    AddCopySetToRequest(
        &req, 1, 1, 0, 0, std::set<ChunkServerIdType>{1, 2, 3});
    ChunkServerHeartbeatResponse rep;
    SendHeartbeat(req, SENDHBOK, &rep);

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
    BuildBasicChunkServerRequest(1, &req);
    AddCopySetToRequest(&req, 1, 1, 5, 1, std::set<ChunkServerIdType>{1, 2, 3});
    ChunkServerHeartbeatResponse rep;
    SendHeartbeat(req, SENDHBOK, &rep);

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
    //           configChangeInfo={peer: 10, type: AddPeer} )
    ChunkServerHeartbeatRequest req;
    BuildBasicChunkServerRequest(1, &req);
    AddCopySetToRequest(&req, 1, 1, 5, 1, std::set<ChunkServerIdType>{1, 2, 3},
        10, ConfigChangeType::ADD_PEER);
    ChunkServerHeartbeatResponse rep;
    SendHeartbeat(req, SENDHBOK, &rep);

    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidate_OpOnGoing_condition3) {
    PrepareMdsWithCandidateOpOnGoing();

    // chunkserver2上报copyset-1(epoch=6, peers={1,2,3}, leader=2)
    ChunkServerHeartbeatRequest req;
    BuildBasicChunkServerRequest(2, &req);
    AddCopySetToRequest(&req, 1, 1, 6, 2, std::set<ChunkServerIdType>{1, 2, 3});
    ChunkServerHeartbeatResponse rep;
    SendHeartbeat(req, SENDHBOK, &rep);

    // 检查sheduler中的operator
    auto ops = coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查topology中copyset的状态
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(2, copysetInfo.GetLeader());
    ASSERT_EQ(6, copysetInfo.GetEpoch());
    ASSERT_EQ(UNINTIALIZE_ID, copysetInfo.GetCandidate());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidate_OpOnGoing_condition4) {
    PrepareMdsWithCandidateOpOnGoing();

    // chunkserver1上报copyset-1(epoch=6, peers={1,2,3,10}, leader=2)
    ChunkServerHeartbeatRequest req;
    BuildBasicChunkServerRequest(1, &req);
    AddCopySetToRequest(
        &req, 1, 1, 6, 1, std::set<ChunkServerIdType>{1, 2, 3, 10});
    ChunkServerHeartbeatResponse rep;
    SendHeartbeat(req, SENDHBOK, &rep);

    // 检查sheduler中的operator
    auto ops = coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查topology中copyset的状态
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
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
    BuildBasicChunkServerRequest(2, &req);
    AddCopySetToRequest(
        &req, 1, 1, 7, 2, std::set<ChunkServerIdType>{1, 2, 3, 10});
    ChunkServerHeartbeatResponse rep;
    SendHeartbeat(req, SENDHBOK, &rep);

    // 检查sheduler中的operator
    auto ops = coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(0, ops.size());
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查topology中copyset的状态
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
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
    BuildBasicChunkServerRequest(2, &req);
    AddCopySetToRequest(&req, 1, 1, 5, 1, std::set<ChunkServerIdType>{1, 2, 3});
    ChunkServerHeartbeatResponse rep;
    SendHeartbeat(req, SENDHBOK, &rep);

    // 检查sheduler中的operator
    auto ops = coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidate_OpOnGoing_condition7) {
    PrepareMdsWithCandidateOpOnGoing();

    // chunkserver2上报copyset-1(epoch=5, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    BuildBasicChunkServerRequest(2, &req);
    AddCopySetToRequest(&req, 1, 1, 5, UNINTIALIZE_ID,
        std::set<ChunkServerIdType>{1, 2, 3});
    ChunkServerHeartbeatResponse rep;
    SendHeartbeat(req, SENDHBOK, &rep);

    // 检查sheduler中的operator
    auto ops = coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidate_OpOnGoing_condition8) {
    PrepareMdsWithCandidateOpOnGoing();

    // chunkserver2上报copyset-1(epoch=6, peers={1,2,3,10}, leader=1)
    ChunkServerHeartbeatRequest req;
    BuildBasicChunkServerRequest(2, &req);
    AddCopySetToRequest(&req, 1, 1, 6, 1,
        std::set<ChunkServerIdType>{1, 2, 3, 10});
    ChunkServerHeartbeatResponse rep;
    SendHeartbeat(req, SENDHBOK, &rep);

    // 检查sheduler中的operator
    auto ops = coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
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
    BuildBasicChunkServerRequest(2, &req);
    AddCopySetToRequest(&req, 1, 1, 6, UNINTIALIZE_ID,
        std::set<ChunkServerIdType>{1, 2, 3, 10});
    ChunkServerHeartbeatResponse rep;
    SendHeartbeat(req, SENDHBOK, &rep);

    // 检查sheduler中的operator
    auto ops = coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
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
    BuildBasicChunkServerRequest(2, &req);
    AddCopySetToRequest(&req, 1, 1, 4, 1, std::set<ChunkServerIdType>{1, 2, 3});
    ChunkServerHeartbeatResponse rep;
    SendHeartbeat(req, SENDHBOK, &rep);

     // 检查sheduler中的operator
    auto ops = coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
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
    BuildBasicChunkServerRequest(2, &req);
    AddCopySetToRequest(&req, 1, 1, 4, UNINTIALIZE_ID,
        std::set<ChunkServerIdType>{1, 2, 3});
    ChunkServerHeartbeatResponse rep;
    SendHeartbeat(req, SENDHBOK, &rep);

     // 检查sheduler中的operator
    auto ops = coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(1, copysetInfo.GetLeader());
    ASSERT_EQ(5, copysetInfo.GetEpoch());
    ASSERT_EQ(10, copysetInfo.GetCandidate());
    std::set<ChunkServerIdType> peers{1, 2 , 3};
    ASSERT_EQ(peers, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidate_OpOnGoing_condition12) {
    PrepareMdsWithCandidateOpOnGoing();
    PrepareAddChunkServer(4, "testtoekn", "nvme", 3, "10.198.100.3", 9002);

    // chunkserver2上报copyset-1(epoch=4, peers={1,2,3,4}, leader=1)
    ChunkServerHeartbeatRequest req;
    BuildBasicChunkServerRequest(2, &req);
    AddCopySetToRequest(&req, 1, 1, 4, 1,
        std::set<ChunkServerIdType>{1, 2, 3, 4});
    ChunkServerHeartbeatResponse rep;
    SendHeartbeat(req, SENDHBOK, &rep);

    // 检查sheduler中的operator
    auto ops = coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(1, copysetInfo.GetLeader());
    ASSERT_EQ(5, copysetInfo.GetEpoch());
    ASSERT_EQ(10, copysetInfo.GetCandidate());
    std::set<ChunkServerIdType> peers{1, 2 , 3};
    ASSERT_EQ(peers, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidate_OpOnGoing_condition13) {
    PrepareMdsWithCandidateOpOnGoing();
    PrepareAddChunkServer(4, "testtoekn", "nvme", 3, "10.198.100.3", 9002);

    // chunkserver2上报copyset-1(epoch=4, peers={1,2,3,4}, leader=0)
    ChunkServerHeartbeatRequest req;
    BuildBasicChunkServerRequest(2, &req);
    AddCopySetToRequest(&req, 1, 1, 4, UNINTIALIZE_ID,
        std::set<ChunkServerIdType>{1, 2, 3, 4});
    ChunkServerHeartbeatResponse rep;
    SendHeartbeat(req, SENDHBOK, &rep);

    // 检查sheduler中的operator
    auto ops = coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
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
    BuildBasicChunkServerRequest(2, &req);
    AddCopySetToRequest(&req, 1, 1, 0, 1, std::set<ChunkServerIdType>{1, 2, 3});
    ChunkServerHeartbeatResponse rep;
    SendHeartbeat(req, SENDHBOK, &rep);

    // 检查sheduler中的operator
    auto ops = coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
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
    BuildBasicChunkServerRequest(2, &req);
    AddCopySetToRequest(&req, 1, 1, 0, UNINTIALIZE_ID,
        std::set<ChunkServerIdType>{1, 2, 3});
    ChunkServerHeartbeatResponse rep;
    SendHeartbeat(req, SENDHBOK, &rep);

    // 检查sheduler中的operator
    auto ops = coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(1, copysetInfo.GetLeader());
    ASSERT_EQ(5, copysetInfo.GetEpoch());
    ASSERT_EQ(10, copysetInfo.GetCandidate());
    std::set<ChunkServerIdType> peers{1, 2 , 3};
    ASSERT_EQ(peers, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidate_OpOnGoing_condition16) {
    PrepareMdsWithCandidateOpOnGoing();
    PrepareAddChunkServer(4, "testtoekn", "nvme", 3, "10.198.100.3", 9002);

    // chunkserver2上报copyset-1(epoch=0, peers={1,2,3,4}, leader=1)
    ChunkServerHeartbeatRequest req;
    BuildBasicChunkServerRequest(2, &req);
    AddCopySetToRequest(&req, 1, 1, 0, 1,
        std::set<ChunkServerIdType>{1, 2, 3, 4});
    ChunkServerHeartbeatResponse rep;
    SendHeartbeat(req, SENDHBOK, &rep);

    // 检查sheduler中的operator
    auto ops = coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(1, copysetInfo.GetLeader());
    ASSERT_EQ(5, copysetInfo.GetEpoch());
    ASSERT_EQ(10, copysetInfo.GetCandidate());
    std::set<ChunkServerIdType> peers{1, 2 , 3};
    ASSERT_EQ(peers, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidate_OpOnGoing_condition17) {
    PrepareMdsWithCandidateOpOnGoing();
    PrepareAddChunkServer(4, "testtoekn", "nvme", 3, "10.198.100.3", 9002);

    // chunkserver2上报copyset-1(epoch=0, peers={1,2,3,4}, leader=0)
    ChunkServerHeartbeatRequest req;
    BuildBasicChunkServerRequest(2, &req);
    AddCopySetToRequest(&req, 1, 1, 0, 0,
        std::set<ChunkServerIdType>{1, 2, 3, 4});
    ChunkServerHeartbeatResponse rep;
    SendHeartbeat(req, SENDHBOK, &rep);

    // 检查sheduler中的operator
    auto ops = coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
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
    BuildBasicChunkServerRequest(10, &req);
    AddCopySetToRequest(&req, 1, 1, 5, 0, std::set<ChunkServerIdType>{1, 2, 3});
    ChunkServerHeartbeatResponse rep;
    SendHeartbeat(req, SENDHBOK, &rep);

    // 检查sheduler中的operator
    auto ops = coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
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
    BuildBasicChunkServerRequest(10, &req);
    AddCopySetToRequest(&req, 1, 1, 5, 1, std::set<ChunkServerIdType>{1, 2, 3});
    ChunkServerHeartbeatResponse rep;
    SendHeartbeat(req, SENDHBOK, &rep);

    // 检查sheduler中的operator
    auto ops = coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
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
    BuildBasicChunkServerRequest(10, &req);
    AddCopySetToRequest(&req, 1, 1, 6, 1,
        std::set<ChunkServerIdType>{1, 2, 3, 10});
    ChunkServerHeartbeatResponse rep;
    SendHeartbeat(req, SENDHBOK, &rep);

    // 检查sheduler中的operator
    auto ops = coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
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
    BuildBasicChunkServerRequest(10, &req);
    AddCopySetToRequest(&req, 1, 1, 6, 0,
        std::set<ChunkServerIdType>{1, 2, 3, 10});
    ChunkServerHeartbeatResponse rep;
    SendHeartbeat(req, SENDHBOK, &rep);

    // 检查sheduler中的operator
    auto ops = coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
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
    BuildBasicChunkServerRequest(10, &req);
    AddCopySetToRequest(&req, 1, 1, 4, 1,
        std::set<ChunkServerIdType>{1, 2, 3});
    ChunkServerHeartbeatResponse rep;
    SendHeartbeat(req, SENDHBOK, &rep);

    // 检查sheduler中的operator
    auto ops = coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
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
    BuildBasicChunkServerRequest(10, &req);
    AddCopySetToRequest(&req, 1, 1, 4, UNINTIALIZE_ID,
        std::set<ChunkServerIdType>{1, 2, 3});
    ChunkServerHeartbeatResponse rep;
    SendHeartbeat(req, SENDHBOK, &rep);

    // 检查sheduler中的operator
    auto ops = coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(1, copysetInfo.GetLeader());
    ASSERT_EQ(5, copysetInfo.GetEpoch());
    ASSERT_EQ(10, copysetInfo.GetCandidate());
    std::set<ChunkServerIdType> peers{1, 2 , 3};
    ASSERT_EQ(peers, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidate_OpOnGoing_condition24) {
    PrepareMdsWithCandidateOpOnGoing();
    PrepareAddChunkServer(4, "testtoekn", "nvme", 3, "10.198.100.3", 9002);

    // chunkserver10上报copyset-1(epoch=4, peers={1,2,3,4}, leader=1)
    ChunkServerHeartbeatRequest req;
    BuildBasicChunkServerRequest(10, &req);
    AddCopySetToRequest(&req, 1, 1, 4, 1,
        std::set<ChunkServerIdType>{1, 2, 3, 4});
    ChunkServerHeartbeatResponse rep;
    SendHeartbeat(req, SENDHBOK, &rep);

    // 检查sheduler中的operator
    auto ops = coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(1, copysetInfo.GetLeader());
    ASSERT_EQ(5, copysetInfo.GetEpoch());
    ASSERT_EQ(10, copysetInfo.GetCandidate());
    std::set<ChunkServerIdType> peers{1, 2 , 3};
    ASSERT_EQ(peers, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidate_OpOnGoing_condition25) {
    PrepareMdsWithCandidateOpOnGoing();
    PrepareAddChunkServer(4, "testtoekn", "nvme", 3, "10.198.100.3", 9002);

    // chunkserver10上报copyset-1(epoch=4, peers={1,2,3,4}, leader=0)
    ChunkServerHeartbeatRequest req;
    BuildBasicChunkServerRequest(10, &req);
    AddCopySetToRequest(&req, 1, 1, 4, UNINTIALIZE_ID,
        std::set<ChunkServerIdType>{1, 2, 3, 4});
    ChunkServerHeartbeatResponse rep;
    SendHeartbeat(req, SENDHBOK, &rep);

    // 检查sheduler中的operator
    auto ops = coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
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
    BuildBasicChunkServerRequest(10, &req);
    AddCopySetToRequest(&req, 1, 1, 0, 1, std::set<ChunkServerIdType>{1, 2, 3});
    ChunkServerHeartbeatResponse rep;
    SendHeartbeat(req, SENDHBOK, &rep);

    // 检查sheduler中的operator
    auto ops = coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
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
    BuildBasicChunkServerRequest(10, &req);
    AddCopySetToRequest(&req, 1, 1, 0, UNINTIALIZE_ID,
        std::set<ChunkServerIdType>{1, 2, 3});
    ChunkServerHeartbeatResponse rep;
    SendHeartbeat(req, SENDHBOK, &rep);

    // 检查sheduler中的operator
    auto ops = coordinator_->GetOpController()->GetOperators();
    ASSERT_EQ(1, ops.size());
    // 检查response
    ASSERT_EQ(0, rep.needupdatecopysets_size());
    // 检查copyset
    ::curve::mds::topology::CopySetInfo copysetInfo;
    ASSERT_TRUE(topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(1, copysetInfo.GetLeader());
    ASSERT_EQ(5, copysetInfo.GetEpoch());
    ASSERT_EQ(10, copysetInfo.GetCandidate());
    std::set<ChunkServerIdType> peers{1, 2 , 3};
    ASSERT_EQ(peers, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidate_OpOnGoing_condition28) {
    PrepareMdsWithCandidateOpOnGoing();
    PrepareAddChunkServer(4, "testtoekn", "nvme", 3, "10.198.100.3", 9002);

    // chunkserver4上报copyset-1(epoch=5, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    BuildBasicChunkServerRequest(4, &req);
    AddCopySetToRequest(&req, 1, 1, 5, UNINTIALIZE_ID,
        std::set<ChunkServerIdType>{1, 2, 3});
    ChunkServerHeartbeatResponse rep;
    SendHeartbeat(req, SENDHBOK, &rep);

    // 检查sheduler中的operator
    auto ops = coordinator_->GetOpController()->GetOperators();
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
    ASSERT_TRUE(topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(1, copysetInfo.GetLeader());
    ASSERT_EQ(5, copysetInfo.GetEpoch());
    ASSERT_EQ(10, copysetInfo.GetCandidate());
    std::set<ChunkServerIdType> peers{1, 2 , 3};
    ASSERT_EQ(peers, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidate_OpOnGoing_condition29) {
    PrepareMdsWithCandidateOpOnGoing();
    PrepareAddChunkServer(4, "testtoekn", "nvme", 3, "10.198.100.3", 9002);

    // chunkserver4上报copyset-1(epoch=4, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    BuildBasicChunkServerRequest(4, &req);
    AddCopySetToRequest(&req, 1, 1, 4, UNINTIALIZE_ID,
        std::set<ChunkServerIdType>{1, 2, 3});
    ChunkServerHeartbeatResponse rep;
    SendHeartbeat(req, SENDHBOK, &rep);

    // 检查sheduler中的operator
    auto ops = coordinator_->GetOpController()->GetOperators();
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
    ASSERT_TRUE(topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(1, copysetInfo.GetLeader());
    ASSERT_EQ(5, copysetInfo.GetEpoch());
    ASSERT_EQ(10, copysetInfo.GetCandidate());
    std::set<ChunkServerIdType> peers{1, 2 , 3};
    ASSERT_EQ(peers, copysetInfo.GetCopySetMembers());
}

TEST_F(HeartbeatBasicTest, test_mdsWithCandidate_OpOnGoing_condition30) {
    PrepareMdsWithCandidateOpOnGoing();
    PrepareAddChunkServer(4, "testtoekn", "nvme", 3, "10.198.100.3", 9002);

    // chunkserver4上报copyset-1(epoch=0, peers={1,2,3}, leader=0)
    ChunkServerHeartbeatRequest req;
    BuildBasicChunkServerRequest(4, &req);
    AddCopySetToRequest(&req, 1, 1, 0, UNINTIALIZE_ID,
        std::set<ChunkServerIdType>{1, 2, 3});
    ChunkServerHeartbeatResponse rep;
    SendHeartbeat(req, SENDHBOK, &rep);

    // 检查sheduler中的operator
    auto ops = coordinator_->GetOpController()->GetOperators();
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
    ASSERT_TRUE(topology_->GetCopySet(CopySetKey{1, 1}, &copysetInfo));
    ASSERT_EQ(1, copysetInfo.GetLeader());
    ASSERT_EQ(5, copysetInfo.GetEpoch());
    ASSERT_EQ(10, copysetInfo.GetCandidate());
    std::set<ChunkServerIdType> peers{1, 2 , 3};
    ASSERT_EQ(peers, copysetInfo.GetCopySetMembers());
}
}  // namespace mds
}  // namespace curve


