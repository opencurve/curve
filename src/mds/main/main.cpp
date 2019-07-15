/*
 * Project: curve
 * Created Date: Friday October 19th 2018
 * Author: hzsunjianliang
 * Copyright (c) 2018 netease
 */
#include <glog/logging.h>
#include <gflags/gflags.h>

#include <brpc/channel.h>
#include <brpc/server.h>

#include "src/mds/nameserver2/namespace_storage.h"
#include "src/mds/nameserver2/namespace_service.h"
#include "src/mds/nameserver2/curvefs.h"
#include "src/mds/nameserver2/clean_manager.h"
#include "src/mds/nameserver2/clean_core.h"
#include "src/mds/nameserver2/clean_task_manager.h"
#include "src/mds/nameserver2/session.h"
#include "src/mds/nameserver2/chunk_allocator.h"
#include "src/mds/nameserver2/inode_id_generator.h"
#include "src/mds/topology/topology_admin.h"
#include "src/mds/topology/topology_service.h"
#include "src/mds/topology/topology_id_generator.h"
#include "src/mds/topology/topology_token_generator.h"
#include "src/mds/topology/topology_config.h"
#include "src/mds/topology/topology_stat.h"
#include "src/mds/topology/topology_metric.h"
#include "src/mds/schedule/schedulerMetric.h"
#include "src/mds/copyset/copyset_manager.h"
#include "src/common/configuration.h"
#include "src/mds/heartbeat/heartbeat_service.h"
#include "src/mds/schedule/topoAdapter.h"
#include "proto/heartbeat.pb.h"

DEFINE_string(confPath, "conf/mds.conf", "mds confPath");

using ::curve::mds::topology::TopologyAdminImpl;
using ::curve::mds::topology::TopologyAdmin;
using ::curve::mds::topology::TopologyServiceImpl;
using ::curve::mds::topology::DefaultIdGenerator;
using ::curve::mds::topology::DefaultTokenGenerator;
using ::curve::mds::topology::DefaultTopologyStorage;
using ::curve::mds::topology::TopologyImpl;
using ::curve::mds::topology::TopologyOption;
using ::curve::mds::topology::TopologyStatImpl;
using ::curve::mds::topology::TopologyMetricService;
using ::curve::mds::copyset::CopysetManager;
using ::curve::mds::copyset::CopysetOption;
using ::curve::mds::heartbeat::HeartbeatServiceImpl;
using ::curve::mds::heartbeat::HeartbeatOption;
using ::curve::mds::schedule::TopoAdapterImpl;
using ::curve::mds::schedule::TopoAdapter;
using ::curve::mds::schedule::ScheduleOption;
using ::curve::mds::schedule::SchedulerMetric;
using ::curve::common::Configuration;

namespace curve {
namespace mds {
void InitSessionOptions(Configuration *conf,
                        struct SessionOptions *sessionOptions) {
    LOG_IF(FATAL, !conf->GetUInt32Value(
        "mds.session.leaseTimeUs", &sessionOptions->leaseTimeUs));
    LOG_IF(FATAL, !conf->GetUInt32Value(
        "mds.session.toleranceTimeUs", &sessionOptions->toleranceTimeUs));
    LOG_IF(FATAL, !conf->GetUInt32Value(
        "mds.session.intevalTimeUs", &sessionOptions->intevalTimeUs));
}

void InitAuthOptions(Configuration *conf,
                     struct RootAuthOption *authOptions) {
    authOptions->rootOwner = ROOTUSERNAME;
    LOG_IF(FATAL, !conf->GetStringValue(
        "mds.auth.rootPassword", &authOptions->rootPassword));
}

void InitScheduleOption(
    Configuration *conf, ScheduleOption *scheduleOption) {
    LOG_IF(FATAL, !conf->GetBoolValue("mds.enable.copyset.scheduler",
        &scheduleOption->enableCopysetScheduler));
    LOG_IF(FATAL, !conf->GetBoolValue("mds.enable.leader.scheduler",
        &scheduleOption->enableLeaderScheduler));
    LOG_IF(FATAL, !conf->GetBoolValue("mds.enable.recover.scheduler",
        &scheduleOption->enableRecoverScheduler));
    LOG_IF(FATAL, !conf->GetBoolValue("mds.enable.replica.scheduler",
        &scheduleOption->enableReplicaScheduler));

    LOG_IF(FATAL, !conf->GetUInt32Value("mds.copyset.scheduler.intervalSec",
        &scheduleOption->copysetSchedulerIntervalSec));
    LOG_IF(FATAL, !conf->GetUInt32Value("mds.leader.scheduler.intervalSec",
        &scheduleOption->leaderSchedulerIntervalSec));
    LOG_IF(FATAL, !conf->GetUInt32Value("mds.recover.scheduler.intervalSec",
        &scheduleOption->recoverSchedulerIntervalSec));
    LOG_IF(FATAL, !conf->GetUInt32Value("mds.replica.scheduler.intervalSec",
        &scheduleOption->replicaSchedulerIntervalSec));
    LOG_IF(FATAL, !conf->GetUInt32Value("mds.schduler.operator.concurrent",
        &scheduleOption->operatorConcurrent));
    LOG_IF(FATAL, !conf->GetUInt32Value("mds.schduler.transfer.limitSec",
        &scheduleOption->transferLeaderTimeLimitSec));
    LOG_IF(FATAL, !conf->GetUInt32Value("mds.scheduler.add.limitSec",
        &scheduleOption->addPeerTimeLimitSec));
    LOG_IF(FATAL, !conf->GetUInt32Value("mds.scheduler.remove.limitSec",
        &scheduleOption->removePeerTimeLimitSec));

    LOG_IF(FATAL, !conf->GetFloatValue("mds.scheduler.copysetNumRangePercent",
        &scheduleOption->copysetNumRangePercent));
    LOG_IF(FATAL, !conf->GetFloatValue("mds.schduler.scatterWidthRangePerent",
        &scheduleOption->scatterWithRangePerent));
    LOG_IF(FATAL, !conf->GetUInt32Value("mds.chunkserver.failure.tolerance",
        &scheduleOption->chunkserverFailureTolerance));
}

void InitHeartbeatOption(
    Configuration *conf, HeartbeatOption *heartbeatOption) {
    LOG_IF(FATAL, !conf->GetUInt64Value("mds.heartbeat.intervalMs",
        &heartbeatOption->heartbeatIntervalMs));
    LOG_IF(FATAL, !conf->GetUInt64Value("mds.heartbeat.misstimeoutMs",
        &heartbeatOption->heartbeatMissTimeOutMs));
    LOG_IF(FATAL, !conf->GetUInt64Value("mds.heartbeat.offlinetimeoutMs",
        &heartbeatOption->offLineTimeOutMs));
    LOG_IF(FATAL, !conf->GetUInt64Value("mds.heartbeat.clean_follower_afterMs",
        &heartbeatOption->cleanFollowerAfterMs));
}

void InitEtcdConf(Configuration *conf, EtcdConf *etcdConf) {
    std::string endpoint;
    LOG_IF(FATAL, !conf->GetStringValue("mds.etcd.endpoint", &endpoint));
    etcdConf->Endpoints = new char[endpoint.size()];
    std::memcpy(etcdConf->Endpoints, endpoint.c_str(), endpoint.size());
    etcdConf->len = endpoint.size();
    LOG_IF(FATAL, !conf->GetIntValue(
        "mds.etcd.dailtimeoutMs", &etcdConf->DialTimeout));
}

void InitTopologyOption(Configuration *conf, TopologyOption *topologyOption) {
    LOG_IF(FATAL, !conf->GetUInt32Value(
        "mds.topology.TopologyUpdateToRepoSec",
        &topologyOption->TopologyUpdateToRepoSec));
    LOG_IF(FATAL, !conf->GetUInt32Value(
        "mds.topology.CreateCopysetRpcTimeoutMs",
        &topologyOption->CreateCopysetRpcTimeoutMs));
    LOG_IF(FATAL, !conf->GetUInt32Value(
        "mds.topology.CreateCopysetRpcRetryTimes",
        &topologyOption->CreateCopysetRpcRetryTimes));
    LOG_IF(FATAL, !conf->GetUInt32Value(
        "mds.topology.CreateCopysetRpcRetrySleepTimeMs",
        &topologyOption->CreateCopysetRpcRetrySleepTimeMs));
    LOG_IF(FATAL, !conf->GetUInt32Value(
        "mds.topology.UpdateMetricIntervalSec",
        &topologyOption->UpdateMetricIntervalSec));
}

void InitCopysetOption(Configuration *conf, CopysetOption *copysetOption) {
    LOG_IF(FATAL, !conf->GetIntValue("mds.copyset.copysetRetryTimes",
        &copysetOption->copysetRetryTimes));
    LOG_IF(FATAL, !conf->GetDoubleValue("mds.copyset.scatterWidthVariance",
        &copysetOption->scatterWidthVariance));
    LOG_IF(FATAL, !conf->GetDoubleValue(
        "mds.copyset.scatterWidthStandardDevation",
        &copysetOption->scatterWidthStandardDevation));
    LOG_IF(FATAL, !conf->GetDoubleValue("mds.copyset.scatterWidthRange",
        &copysetOption->scatterWidthRange));
    LOG_IF(FATAL, !conf->GetDoubleValue(
        "mds.copyset.scatterWidthFloatingPercentage",
        &copysetOption->scatterWidthFloatingPercentage));
}

int curve_main(int argc, char **argv) {
    // google::InitGoogleLogging(argv[0]);
    google::ParseCommandLineFlags(&argc, &argv, false);

    // =========================加载配置===============================//
    LOG(INFO) << "load mds configuration.";

    std::string confPath = FLAGS_confPath.c_str();
    Configuration conf;
    conf.SetConfigPath(confPath);
    LOG_IF(FATAL, !conf.LoadConfig())
        << "load mds configuration fail, conf path = " << confPath;

    // ========================初始化各配置项==========================//
    SessionOptions sessionOptions;
    InitSessionOptions(&conf, &sessionOptions);

    RootAuthOption authOptions;
    InitAuthOptions(&conf, &authOptions);

    ScheduleOption scheduleOption;
    InitScheduleOption(&conf, &scheduleOption);

    HeartbeatOption heartbeatOption;
    InitHeartbeatOption(&conf, &heartbeatOption);
    heartbeatOption.mdsStartTime = steady_clock::now();

    EtcdConf etcdConf;
    InitEtcdConf(&conf, &etcdConf);

    TopologyOption topologyOption;
    InitTopologyOption(&conf, &topologyOption);

    CopysetOption copysetOption;
    InitCopysetOption(&conf, &copysetOption);

    // ===========================init curveFs========================//
    // init EtcdClient
    auto client = std::make_shared<EtcdClientImp>();
    int etcdTimeout;
    LOG_IF(FATAL, !conf.GetIntValue(
        "mds.etcd.operation.timeoutMs", &etcdTimeout));
    int retryTimes;
    LOG_IF(FATAL, !conf.GetIntValue("mds.etcd.retry.times", &retryTimes));
    auto res = client->Init(etcdConf, etcdTimeout, retryTimes);
    LOG_IF(FATAL, res != EtcdErrCode::OK)
        << "init etcd client err! "
        << "etcdaddr: " << etcdConf.Endpoints
        << ", etcdaddr len: " << etcdConf.len
        << ", etcdtimeout: " << etcdConf.DialTimeout
        << ", operation timeout: " << etcdTimeout
        << ", etcd retrytimes: " << retryTimes;

    std::string out;
    res = client->Get("test", &out);
    LOG_IF(FATAL, res != EtcdErrCode::OK && res != EtcdErrCode::KeyNotExist)
        << "Run mds err. Check if etcd is running.";

    LOG(INFO) << "init etcd client ok! "
            << "etcdaddr: " << etcdConf.Endpoints
            << ", etcdaddr len: " << etcdConf.len
            << ", etcdtimeout: " << etcdConf.DialTimeout
            << ", operation timeout: " << etcdTimeout
            << ", etcd retrytimes: " << retryTimes;


    // init InodeIDGenerator
    auto inodeIdGenerator = std::make_shared<InodeIdGeneratorImp>(client);

    // init ChunkIDGenerator
    auto chunkIdGenerator = std::make_shared<ChunkIDGeneratorImp>(client);

    // init LRUCache
    int mdsCacheCount;
    LOG_IF(FATAL, !conf.GetIntValue("mds.cache.count", &mdsCacheCount));
    auto cache = std::make_shared<LRUCache>(mdsCacheCount);

    // init NameServerStorage
    NameServerStorage *storage = new NameServerStorageImp(client, cache);

    // init recyclebindir
    LOG_IF(FATAL, !InitRecycleBinDir(storage)) << "init recyclebindir error";

    // init topology
    auto topologyIdGenerator  =
        std::make_shared<DefaultIdGenerator>();
    auto topologyTokenGenerator =
        std::make_shared<DefaultTokenGenerator>();

    std::string dbName;
    std::string dbUser;
    std::string dbUrl;
    std::string dbPassword;
    int dbPoolSize;
    LOG_IF(FATAL, !conf.GetStringValue("mds.DbName", &dbName));
    LOG_IF(FATAL, !conf.GetStringValue("mds.DbUser", &dbUser));
    LOG_IF(FATAL, !conf.GetStringValue("mds.DbUrl", &dbUrl));
    LOG_IF(FATAL, !conf.GetStringValue("mds.DbPassword", &dbPassword));
    LOG_IF(FATAL, !conf.GetIntValue("mds.DbPoolSize", &dbPoolSize));

    // init mdsRepo
    auto mdsRepo = std::make_shared<MdsRepo>();
    if (mdsRepo->connectDB(dbName, dbUser, dbUrl, dbPassword, dbPoolSize)
                                                    != OperationOK) {
        LOG(ERROR) << "connectDB fail";
        return -1;
    }

    if (mdsRepo->createDatabase() != OperationOK) {
        LOG(ERROR) << "createDatabase fail";
        return -1;
    }

    if (mdsRepo->useDataBase() != OperationOK) {
        LOG(ERROR) << "useDataBase fail";
        return -1;
    }

    if (mdsRepo->createAllTables() != OperationOK) {
        LOG(ERROR) << "createAllTables fail";
        return -1;
    }

    auto topologyStorage =
        std::make_shared<DefaultTopologyStorage>(mdsRepo);

    LOG_IF(FATAL, !topologyStorage->init(topologyOption))
        << "init topologyStorage fail.";

    auto topology =
        std::make_shared<TopologyImpl>(topologyIdGenerator,
                                           topologyTokenGenerator,
                                           topologyStorage);
    LOG_IF(FATAL, topology->init(topologyOption) < 0) << "init topology fail.";
    LOG_IF(FATAL, topology->Run()) << "run topology module fail";

    // init CopysetManager
    auto copysetManager =
        std::make_shared<CopysetManager>(copysetOption);

    // init TopologyStat
    auto topologyStat =
        std::make_shared<TopologyStatImpl>(topology);
    LOG_IF(FATAL, topologyStat->Init() < 0)
        << "init topologyStat fail.";

    // init TopoAdmin
    auto topologyAdmin =
          std::make_shared<TopologyAdminImpl>(topology);

    // init TopologyMetricService
    auto topologyMetricService =
        std::make_shared<TopologyMetricService>(topology, topologyStat);
    LOG_IF(FATAL, topologyMetricService->Init(topologyOption) < 0)
        << "init topologyMetricService fail.";
    LOG_IF(FATAL, topologyMetricService->Run() < 0)
        << "topologyMetricService start run fail";

    // init TopologyServiceManager
    auto topologyServiceManager =
        std::make_shared<TopologyServiceManager>(topology,
        copysetManager);
    topologyServiceManager->Init(topologyOption);

    // init ChunkSegmentAllocator
    ChunkSegmentAllocator *chunkSegmentAllocate =
        new ChunkSegmentAllocatorImpl(topologyAdmin, chunkIdGenerator);

    // TODO(hzsunjianliang): should add threadpoolsize & checktime from config
    // init CleanManager
    auto taskManager = std::make_shared<CleanTaskManager>();
    auto cleanCore = std::make_shared<CleanCore>(storage, topology);

    auto cleanManger = std::make_shared<CleanManager>(cleanCore,
                                                      taskManager, storage);

    // init SessionManager
    SessionManager *sessionManager = new SessionManager(mdsRepo);
    LOG_IF(FATAL, !kCurveFS.Init(storage, inodeIdGenerator.get(),
                  chunkSegmentAllocate, cleanManger,
                  sessionManager, sessionOptions, authOptions, mdsRepo))
        << "init session manager fail";


    // start clean manager
    LOG_IF(FATAL, !cleanManger->Start()) << "start cleanManager fail.";

    // =========================init scheduler======================//
    LOG_IF(FATAL, SchedulerMetric::GetInstance() == nullptr)
        << "init scheduler metric fail";
    auto topoAdapter = std::make_shared<TopoAdapterImpl>(
        topology, topologyServiceManager, topologyStat);
    auto coordinator = std::make_shared<Coordinator>(topoAdapter);
    coordinator->InitScheduler(scheduleOption);
    coordinator->Run();

    // =======================init heartbeat manager================//
    auto heartbeatManager = std::make_shared<HeartbeatManager>(
        heartbeatOption, topology, topologyStat, coordinator);
    heartbeatManager->Init();
    heartbeatManager->Run();

    // =========================add service========================//
    // add heartbeat service
    brpc::Server server;
    HeartbeatServiceImpl heartbeatService(heartbeatManager);
    LOG_IF(FATAL, server.AddService(&heartbeatService,
                          brpc::SERVER_DOESNT_OWN_SERVICE) != 0)
        << "add topologyService error";

    // add rpc service
    int mdsFilelockBucketNum;
    LOG_IF(FATAL, !conf.GetIntValue(
        "mds.filelock.bucketNum", &mdsFilelockBucketNum));
    NameSpaceService namespaceService(
        new FileLockManager(mdsFilelockBucketNum));
    LOG_IF(FATAL, server.AddService(&namespaceService,
                          brpc::SERVER_DOESNT_OWN_SERVICE) != 0)
        << "add namespaceService error";

    // add topology service
    TopologyServiceImpl topologyService(topologyServiceManager);
    LOG_IF(FATAL, server.AddService(&topologyService,
                          brpc::SERVER_DOESNT_OWN_SERVICE) != 0)
        << "add topologyService error";

    // start rpc server
    brpc::ServerOptions option;
    option.idle_timeout_sec = -1;
    std::string mdsListenAddr;
    LOG_IF(FATAL, !conf.GetStringValue("mds.listen.addr", &mdsListenAddr));
    LOG_IF(FATAL, server.Start(mdsListenAddr.c_str(), &option) != 0)
        << "start brpc server error";

    server.RunUntilAskedToQuit();

    kCurveFS.Uninit();
    if (!cleanManger->Stop()) {
        LOG(ERROR) << "stop cleanManager fail.";
        return -1;
    }
    return 0;
}
}  // namespace mds
}  // namespace curve

int main(int argc, char **argv) {
    curve::mds::curve_main(argc, argv);
}


