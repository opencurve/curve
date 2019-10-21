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
#include "src/mds/leader_election/leader_election.h"
#include "src/mds/topology/topology_chunk_allocator.h"
#include "src/mds/topology/topology_service.h"
#include "src/mds/topology/topology_id_generator.h"
#include "src/mds/topology/topology_token_generator.h"
#include "src/mds/topology/topology_config.h"
#include "src/mds/topology/topology_stat.h"
#include "src/mds/topology/topology_metric.h"
#include "src/mds/schedule/scheduleMetrics.h"
#include "src/mds/copyset/copyset_manager.h"
#include "src/common/configuration.h"
#include "src/mds/heartbeat/heartbeat_service.h"
#include "src/mds/schedule/topoAdapter.h"
#include "proto/heartbeat.pb.h"
#include "src/mds/chunkserverclient/chunkserverclient_config.h"
#include "src/mds/nameserver2/allocstatistic/alloc_statistic.h"

DEFINE_string(confPath, "conf/mds.conf", "mds confPath");
DEFINE_string(mdsAddr, "127.0.0.1:6666", "mds listen addr");
DEFINE_string(etcdAddr, "127.0.0.1:2379", "etcd client");

using ::curve::mds::topology::TopologyChunkAllocatorImpl;
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
using ::curve::mds::schedule::ScheduleMetrics;
using ::curve::mds::chunkserverclient::ChunkServerClientOption;
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

void InitCurveFSOptions(Configuration *conf,
                     struct CurveFSOption *curveFSOptions) {
    LOG_IF(FATAL, !conf->GetUInt64Value(
        "mds.curvefs.defaultChunkSize", &curveFSOptions->defaultChunkSize));
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
    LOG_IF(FATAL, !conf->GetUInt32Value(
        "mds.scheduler.chunkserver.cooling.timeSec",
        &scheduleOption->chunkserverCoolingTimeSec));
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
    LOG_IF(FATAL, !conf->GetUInt32Value(
        "mds.topology.PoolUsagePercentLimit",
        &topologyOption->PoolUsagePercentLimit));
    LOG_IF(FATAL, !conf->GetIntValue(
        "mds.topology.choosePoolPolicy",
        &topologyOption->choosePoolPolicy));
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

void InitLeaderElectionOption(
    Configuration *conf, LeaderElectionOptions *electionOp) {
    LOG_IF(FATAL, !conf->GetStringValue("mds.listen.addr",
        &electionOp->leaderUniqueName));
    LOG_IF(FATAL, !conf->GetUInt32Value("mds.leader.observeTimeoutMs",
        &electionOp->observeTimeoutMs));
    LOG_IF(FATAL, !conf->GetUInt32Value("mds.leader.sessionInterSec",
        &electionOp->sessionInterSec));
    LOG_IF(FATAL, !conf->GetUInt32Value("mds.leader.electionTimeoutMs",
        &electionOp->electionTimeoutMs));
}

void InitChunkServerClientOption(
    Configuration *conf, ChunkServerClientOption *option) {
    LOG_IF(FATAL, !conf->GetUInt32Value("mds.chunkserverclient.rpcTimeoutMs",
        &option->rpcTimeoutMs));
    LOG_IF(FATAL, !conf->GetUInt32Value("mds.chunkserverclient.rpcRetryTimes",
        &option->rpcRetryTimes));
    LOG_IF(FATAL, !conf->GetUInt32Value(
        "mds.chunkserverclient.rpcRetryIntervalMs",
        &option->rpcRetryIntervalMs));
    LOG_IF(FATAL, !conf->GetUInt32Value(
        "mds.chunkserverclient.updateLeaderRetryTimes",
        &option->updateLeaderRetryTimes));
    LOG_IF(FATAL, !conf->GetUInt32Value(
        "mds.chunkserverclient.updateLeaderRetryIntervalMs",
        &option->updateLeaderRetryIntervalMs));
}

void LoadConfigFromCmdline(Configuration *conf) {
    // 如果命令行有设置, 命令行覆盖配置文件中的字段
    google::CommandLineFlagInfo info;
    if (GetCommandLineFlagInfo("mdsAddr", &info) && !info.is_default) {
        conf->SetStringValue("mds.listen.addr", FLAGS_mdsAddr);
    }

    if (GetCommandLineFlagInfo("etcdAddr", &info) && !info.is_default) {
        conf->SetStringValue("mds.etcd.endpoint", FLAGS_etcdAddr);
    }

    // 设置日志存放文件夹
    if (FLAGS_log_dir.empty()) {
        if (!conf->GetStringValue("mds.common.logDir", &FLAGS_log_dir)) {
            LOG(WARNING) << "no mds.common.logDir in " << FLAGS_confPath
                         << ", will log to /tmp";
        }
    }
}

int curve_main(int argc, char **argv) {
    // google::InitGoogleLogging(argv[0]);
    google::ParseCommandLineFlags(&argc, &argv, false);

    // =========================加载配置===============================//
    LOG(INFO) << "load mds configuration.";

    // 从配置文件中获取
    std::string confPath = FLAGS_confPath.c_str();
    Configuration conf;
    conf.SetConfigPath(confPath);
    LOG_IF(FATAL, !conf.LoadConfig())
        << "load mds configuration fail, conf path = " << confPath;

    // 命令行覆盖配置文件中的参数
    LoadConfigFromCmdline(&conf);

    // 初始化日志模块
    google::InitGoogleLogging(argv[0]);

    // ========================初始化各配置项==========================//
    SessionOptions sessionOptions;
    InitSessionOptions(&conf, &sessionOptions);

    RootAuthOption authOptions;
    InitAuthOptions(&conf, &authOptions);

    struct CurveFSOption curveFSOptions;
    InitCurveFSOptions(&conf, &curveFSOptions);

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

    ChunkServerClientOption chunkServerClientOption;
    InitChunkServerClientOption(&conf, &chunkServerClientOption);

    LOG(INFO) << "load mds configuration success.";

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

    // leader election
    LeaderElectionOptions leaderElectionOp;
    InitLeaderElectionOption(&conf, &leaderElectionOp);
    leaderElectionOp.etcdCli = client;
    auto leaderElection = std::make_shared<LeaderElection>(leaderElectionOp);
    while (0 != leaderElection->CampaginLeader()) {
        LOG(INFO) << leaderElectionOp.leaderUniqueName
                  << " campaign for leader again";
    }
    leaderElection->StartObserverLeader();

    // init alloc staistic
    uint64_t retryInterTimes, periodicPersistInterMs;
    LOG_IF(FATAL, !conf.GetUInt64Value(
        "mds.segment.alloc.periodic.persistInterMs", &periodicPersistInterMs));
    LOG_IF(FATAL, !conf.GetUInt64Value(
        "mds.segment.alloc.retryInterMs", &retryInterTimes));
    auto segmentAllocStatistic = std::make_shared<AllocStatistic>(
        periodicPersistInterMs, retryInterTimes, client);
    res = segmentAllocStatistic->Init();
    LOG_IF(FATAL, res != 0) << "int segment alloc statistic fail";
    segmentAllocStatistic->Run();

    LOG(INFO) << "init segmentAllocStatistic success.";

    // init InodeIDGenerator
    auto inodeIdGenerator = std::make_shared<InodeIdGeneratorImp>(client);

    // init ChunkIDGenerator
    auto chunkIdGenerator = std::make_shared<ChunkIDGeneratorImp>(client);

    // init LRUCache
    int mdsCacheCount;
    LOG_IF(FATAL, !conf.GetIntValue("mds.cache.count", &mdsCacheCount));
    auto cache = std::make_shared<LRUCache>(mdsCacheCount);
    LOG(INFO) << "init LRUCache success.";

    // init NameServerStorage
    NameServerStorage *storage = new NameServerStorageImp(client, cache);
    LOG(INFO) << "init NameServerStorage success.";

    // init recyclebindir
    LOG_IF(FATAL, !InitRecycleBinDir(storage)) << "init recyclebindir error";
    LOG(INFO) << "init InitRecycleBinDir success.";

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
    LOG(INFO) << "init mdsRepo success.";

    auto topologyStorage =
        std::make_shared<DefaultTopologyStorage>(mdsRepo);

    LOG_IF(FATAL, !topologyStorage->init(topologyOption))
        << "init topologyStorage fail.";

    LOG(INFO) << "init topologyStorage success.";

    auto topology =
        std::make_shared<TopologyImpl>(topologyIdGenerator,
                                           topologyTokenGenerator,
                                           topologyStorage);
    LOG_IF(FATAL, topology->init(topologyOption) < 0) << "init topology fail.";
    LOG_IF(FATAL, topology->Run()) << "run topology module fail";

    LOG(INFO) << "init topology success.";

    // init CopysetManager
    auto copysetManager =
        std::make_shared<CopysetManager>(copysetOption);

    LOG(INFO) << "init copysetManager success.";

    // init TopologyStat
    auto topologyStat =
        std::make_shared<TopologyStatImpl>(topology);
    LOG_IF(FATAL, topologyStat->Init() < 0)
        << "init topologyStat fail.";
    LOG(INFO) << "init topologyStat success.";

    // init TopologyChunkAllocator
    auto topologyChunkAllocator =
          std::make_shared<TopologyChunkAllocatorImpl>(topology,
               segmentAllocStatistic, topologyOption);
    LOG(INFO) << "init topologyChunkAllocator success.";

    // init TopologyMetricService
    auto topologyMetricService =
        std::make_shared<TopologyMetricService>(topology,
            topologyStat,
            segmentAllocStatistic);
    LOG_IF(FATAL, topologyMetricService->Init(topologyOption) < 0)
        << "init topologyMetricService fail.";
    LOG_IF(FATAL, topologyMetricService->Run() < 0)
        << "topologyMetricService start run fail";
    LOG(INFO) << "init topologyMetricService success.";

    // init TopologyServiceManager
    auto topologyServiceManager =
        std::make_shared<TopologyServiceManager>(topology,
        copysetManager);
    topologyServiceManager->Init(topologyOption);
    LOG(INFO) << "init topologyServiceManager success.";

    // init ChunkSegmentAllocator
    ChunkSegmentAllocator *chunkSegmentAllocate =
        new ChunkSegmentAllocatorImpl(topologyChunkAllocator, chunkIdGenerator);
    LOG(INFO) << "init ChunkSegmentAllocator success.";

    // TODO(hzsunjianliang): should add threadpoolsize & checktime from config
    // init CleanManager
    auto taskManager = std::make_shared<CleanTaskManager>();
    auto copysetClient =
        std::make_shared<CopysetClient>(topology, chunkServerClientOption);

    auto cleanCore = std::make_shared<CleanCore>(storage,
                                                 copysetClient,
                                                 segmentAllocStatistic);

    auto cleanManger = std::make_shared<CleanManager>(cleanCore,
                                                      taskManager, storage);
    LOG(INFO) << "init CleanManager success.";

    // init SessionManager
    SessionManager *sessionManager = new SessionManager(mdsRepo);
    LOG_IF(FATAL, !kCurveFS.Init(storage, inodeIdGenerator.get(),
                  chunkSegmentAllocate, cleanManger,
                  sessionManager,
                  segmentAllocStatistic,
                  sessionOptions, authOptions,
                  curveFSOptions, mdsRepo))
        << "init session manager fail";
    LOG(INFO) << "init SessionManager success.";


    // start clean manager
    LOG_IF(FATAL, !cleanManger->Start()) << "start cleanManager fail.";

    cleanManger->RecoverCleanTasks();
    LOG(INFO) << "RecoverCleanTasks success.";

    // =========================init scheduler======================//
    auto scheduleMetrics = std::make_shared<ScheduleMetrics>(topology);
    auto topoAdapter = std::make_shared<TopoAdapterImpl>(
        topology, topologyServiceManager, topologyStat);
    auto coordinator = std::make_shared<Coordinator>(topoAdapter);
    coordinator->InitScheduler(scheduleOption, scheduleMetrics);
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

    // 要想实现SIGTERM的优雅退出，启动进程时需要指定参数：
    // --graceful_quit_on_sigterm
    server.RunUntilAskedToQuit();

    // 在退出之前把自己的节点删除
    leaderElection->LeaderResign();
    LOG(INFO) << "resign success";

    kCurveFS.Uninit();
    if (!cleanManger->Stop()) {
        LOG(ERROR) << "stop cleanManager fail.";
        return -1;
    }
    heartbeatManager->Stop();
    LOG(INFO) << "stop heartbeatManager success";
    segmentAllocStatistic->Stop();
    LOG(INFO) << "stop segment alloc success";
    coordinator->Stop();
    LOG(INFO) << "stop coordinator success";

    google::ShutdownGoogleLogging();

    return 0;
}
}  // namespace mds
}  // namespace curve

int main(int argc, char **argv) {
    curve::mds::curve_main(argc, argv);
}


