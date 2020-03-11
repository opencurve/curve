/*
 * Project: curve
 * Created Date: 2020-01-03
 * Author: charisu
 * Copyright (c) 2018 netease
 */

#include <glog/logging.h>
#include "src/mds/server/mds.h"
#include "src/mds/nameserver2/helper/namespace_helper.h"

namespace curve {
namespace mds {
MDS::~MDS() {
    if (etcdEndpoints_) {
        delete etcdEndpoints_;
    }
    if (fileLockManager_) {
        delete fileLockManager_;
    }
}

void MDS::InitMdsOptions(std::shared_ptr<Configuration> conf) {
    conf_ = conf;
    InitFileRecordOptions(&options_.fileRecordOptions);
    InitAuthOptions(&options_.authOptions);
    InitCurveFSOptions(&options_.curveFSOptions);
    InitScheduleOption(&options_.scheduleOption);
    InitHeartbeatOption(&options_.heartbeatOption);
    InitMdsRepoOption(&options_.mdsRepoOption);
    InitTopologyOption(&options_.topologyOption);
    InitCopysetOption(&options_.copysetOption);
    InitChunkServerClientOption(&options_.chunkServerClientOption);

    conf_->GetValueFatalIfFail(
        "mds.segment.alloc.retryInterMs", &options_.retryInterTimes);
    conf_->GetValueFatalIfFail(
        "mds.segment.alloc.periodic.persistInterMs",
        &options_.periodicPersistInterMs);

    // namestorage的缓存大小
    conf_->GetValueFatalIfFail("mds.cache.count", &options_.mdsCacheCount);

    // 获取mds监听地址
    conf_->GetValueFatalIfFail("mds.listen.addr", &options_.mdsListenAddr);
    // 获取dummy server的端口
    conf_->GetValueFatalIfFail(
        "mds.dummy.listen.port", &options_.dummyListenPort);
    // 获取mds的文件锁桶大小
    conf_->GetValueFatalIfFail(
        "mds.filelock.bucketNum", &options_.mdsFilelockBucketNum);
}

void MDS::StartDummy() {
    // 暴露版本信息,配置信息和角色（是否是leader）
    LOG(INFO) << "mds version: " << curve::common::CurveVersion();
    curve::common::ExposeCurveVersion();
    conf_->ExposeMetric("mds_config");
    conf_->UpdateMetric();
    status_.expose("mds_status");
    status_.set_value("follower");

    int ret = brpc::StartDummyServerAt(options_.dummyListenPort);
    if (ret != 0) {
        LOG(FATAL) << "StartMDSDummyServer Error";
    } else {
        LOG(INFO) << "StartDummyServer Success";
    }
}

void MDS::StartCompaginLeader() {
    // 初始化etcdclient
    // TODO(lixiaocui): 为什么不把配置都放在EtcdConf中?
    int etcdTimeout;
    conf_->GetValueFatalIfFail(
        "mds.etcd.operation.timeoutMs", &etcdTimeout);
    int etcdRetryTimes;
    conf_->GetValueFatalIfFail("mds.etcd.retry.times", &etcdRetryTimes);
    EtcdConf etcdConf;
    InitEtcdConf(&etcdConf);
    InitEtcdClient(etcdConf, etcdTimeout, etcdRetryTimes);

    // 进行leader选举
    LeaderElectionOptions leaderElectionOp;
    InitMdsLeaderElectionOption(&leaderElectionOp);
    leaderElectionOp.etcdCli = etcdClient_;
    leaderElectionOp.campaginPrefix = "";
    InitLeaderElection(leaderElectionOp);
    while (0 != leaderElection_->CampaginLeader()) {
        LOG(INFO) << leaderElection_->GetLeaderName()
                  << " campaign for leader again";
    }
    LOG(INFO) << "Campain leader ok, I am the leader now";
    status_.set_value("leader");
    leaderElection_->StartObserverLeader();
}

void MDS::Init() {
    // 初始化Segment统计模块
    InitSegmentAllocStatistic(options_.retryInterTimes,
                              options_.periodicPersistInterMs);
    // 初始化NameServer存储模块
    InitNameServerStorage(options_.mdsCacheCount);
    // 初始化数据库
    InitMdsRepo(options_.mdsRepoOption);
    // init topology
    InitTopology(options_.topologyOption);
    // init TopologyStat
    InitTopologyStat();
    // init TopologyChunkAllocator
    InitTopologyChunkAllocator(options_.topologyOption);
    // init topologyMetricService
    InitTopologyMetricService(options_.topologyOption);
    // init topologyServiceManager
    InitTopologyServiceManager(options_.topologyOption);
    // 初始化curveFs:
    InitCurveFS(options_.curveFSOptions);
    // 初始化调度模块
    InitCoordinator();
    // 初始化心跳模块
    InitHeartbeatManager();

    fileLockManager_ =
        new FileLockManager(options_.mdsFilelockBucketNum);
    inited_ = true;
}


void MDS::Run() {
    if (!inited_) {
        LOG(ERROR) << "MDS not inited yet!";
        return;
    }
    // 启动segmentAllocStatistic
    segmentAllocStatistic_->Run();
    // 启动topology
    LOG_IF(FATAL, topology_->Run()) << "run topology module fail";
    // 启动topologyMetricService
    LOG_IF(FATAL, topologyMetricService_->Run() < 0)
        << "topologyMetricService start run fail";
    // 启动curveFs
    kCurveFS.Run();
    // start clean manager
    LOG_IF(FATAL, !cleanManager_->Start()) << "start cleanManager fail.";
    // 恢复未完成的任务
    cleanManager_->RecoverCleanTasks();
    // 启动调度模块
    coordinator_->Run();
    // 开启brpc server
    StartServer();
}

void MDS::Stop() {
    if (!running_) {
        LOG(INFO) << "MDS is not running";
        return;
    }
    brpc::AskToQuit();

    // 在退出之前把自己的节点删除
    leaderElection_->LeaderResign();
    LOG(INFO) << "resign success";
    // 停止心跳模块
    heartbeatManager_->Stop();

    // 停止调度模块
    coordinator_->Stop();

    // 反初始化kCurveFS
    kCurveFS.Uninit();

    // 停止cleanManager
    cleanManager_->Stop();

    // 停止topologyMetricService
    topologyMetricService_->Stop();

    // 停止topology模块
    topology_->Stop();

    // 停止segment统计模块
    segmentAllocStatistic_->Stop();

    // 关闭etcd client
    etcdClient_->CloseClient();
}

void MDS::InitEtcdConf(EtcdConf* etcdConf) {
    std::string endpoint;
    conf_->GetValueFatalIfFail("mds.etcd.endpoint", &endpoint);
    etcdEndpoints_ = new char[endpoint.size()];
    etcdConf->Endpoints = etcdEndpoints_;
    std::memcpy(etcdConf->Endpoints, endpoint.c_str(), endpoint.size());
    etcdConf->len = endpoint.size();
    conf_->GetValueFatalIfFail(
        "mds.etcd.dailtimeoutMs", &etcdConf->DialTimeout);
}

void MDS::StartServer() {
    brpc::Server server;
    // add heartbeat service
    HeartbeatServiceImpl heartbeatService(heartbeatManager_);
    LOG_IF(FATAL, server.AddService(&heartbeatService,
                          brpc::SERVER_DOESNT_OWN_SERVICE) != 0)
        << "add heartbeatService error";

    // add namespace service
    NameSpaceService namespaceService(fileLockManager_);
    LOG_IF(FATAL, server.AddService(&namespaceService,
                          brpc::SERVER_DOESNT_OWN_SERVICE) != 0)
        << "add namespaceService error";

    // add topology service
    TopologyServiceImpl topologyService(topologyServiceManager_);
    LOG_IF(FATAL, server.AddService(&topologyService,
                          brpc::SERVER_DOESNT_OWN_SERVICE) != 0)
        << "add topologyService error";

    // add schedule service
    ScheduleServiceImpl scheduleService(coordinator_);
    LOG_IF(FATAL, server.AddService(&scheduleService,
                          brpc::SERVER_DOESNT_OWN_SERVICE) != 0)
        << "add scheduleService error";

    // start rpc server
    brpc::ServerOptions option;
    option.idle_timeout_sec = -1;
    LOG_IF(FATAL, server.Start(options_.mdsListenAddr.c_str(), &option) != 0)
        << "start brpc server error";
    running_ = true;

    // 要想实现SIGTERM的优雅退出，启动进程时需要指定参数：
    // --graceful_quit_on_sigterm
    server.RunUntilAskedToQuit();
}

void MDS::InitEtcdClient(const EtcdConf& etcdConf,
                         int etcdTimeout,
                         int retryTimes) {
    etcdClient_ = std::make_shared<EtcdClientImp>();
    auto res = etcdClient_->Init(etcdConf, etcdTimeout, retryTimes);
    LOG_IF(FATAL, res != EtcdErrCode::EtcdOK)
        << "init etcd client err! "
        << "etcdaddr: " << etcdConf.Endpoints
        << ", etcdaddr len: " << etcdConf.len
        << ", etcdtimeout: " << etcdConf.DialTimeout
        << ", operation timeout: " << etcdTimeout
        << ", etcd retrytimes: " << retryTimes;


    std::string out;
    res = etcdClient_->Get("test", &out);
    LOG_IF(FATAL, res != EtcdErrCode::EtcdOK &&
        res != EtcdErrCode::EtcdKeyNotExist)
        << "Run mds err. Check if etcd is running.";

    LOG(INFO) << "init etcd client ok! "
            << "etcdaddr: " << etcdConf.Endpoints
            << ", etcdaddr len: " << etcdConf.len
            << ", etcdtimeout: " << etcdConf.DialTimeout
            << ", operation timeout: " << etcdTimeout
            << ", etcd retrytimes: " << retryTimes;
}

void MDS::InitLeaderElection(const LeaderElectionOptions& leaderElectionOp) {
    leaderElection_ = std::make_shared<LeaderElection>(leaderElectionOp);
}

void MDS::InitMdsLeaderElectionOption(LeaderElectionOptions *electionOp) {
    conf_->GetValueFatalIfFail("mds.listen.addr",
        &electionOp->leaderUniqueName);
    conf_->GetValueFatalIfFail("mds.leader.sessionInterSec",
        &electionOp->sessionInterSec);
    conf_->GetValueFatalIfFail("mds.leader.electionTimeoutMs",
        &electionOp->electionTimeoutMs);
}

void MDS::InitSegmentAllocStatistic(uint64_t retryInterTimes,
                                    uint64_t periodicPersistInterMs) {
    segmentAllocStatistic_ = std::make_shared<AllocStatistic>(
        periodicPersistInterMs, retryInterTimes, etcdClient_);
    int res = segmentAllocStatistic_->Init();
    LOG_IF(FATAL, res != 0) << "int segment alloc statistic fail";
    LOG(INFO) << "init segmentAllocStatistic success.";
}

void MDS::InitMdsRepo(const MdsRepoOption& option) {
    // init mdsRepo
    mdsRepo_ = std::make_shared<MdsRepo>();
    int res = mdsRepo_->connectDB(option.dbName, option.dbUser, option.dbUrl,
                                     option.dbPassword, option.dbPoolSize);
    LOG_IF(FATAL, res != OperationOK) << "connectDB fail";

    res = mdsRepo_->useDataBase();
    LOG_IF(FATAL, res != OperationOK) << "useDataBase fail";

    LOG(INFO) << "init mdsRepo success.";
}

void MDS::InitTopologyOption(TopologyOption *topologyOption) {
    conf_->GetValueFatalIfFail(
        "mds.topology.TopologyUpdateToRepoSec",
        &topologyOption->TopologyUpdateToRepoSec);
    conf_->GetValueFatalIfFail(
        "mds.topology.CreateCopysetRpcTimeoutMs",
        &topologyOption->CreateCopysetRpcTimeoutMs);
    conf_->GetValueFatalIfFail(
        "mds.topology.CreateCopysetRpcRetryTimes",
        &topologyOption->CreateCopysetRpcRetryTimes);
    conf_->GetValueFatalIfFail(
        "mds.topology.CreateCopysetRpcRetrySleepTimeMs",
        &topologyOption->CreateCopysetRpcRetrySleepTimeMs);
    conf_->GetValueFatalIfFail(
        "mds.topology.UpdateMetricIntervalSec",
        &topologyOption->UpdateMetricIntervalSec);
    conf_->GetValueFatalIfFail(
        "mds.topology.PoolUsagePercentLimit",
        &topologyOption->PoolUsagePercentLimit);
    conf_->GetValueFatalIfFail(
        "mds.topology.choosePoolPolicy",
        &topologyOption->choosePoolPolicy);
}

void MDS::InitMdsRepoOption(MdsRepoOption* option) {
    conf_->GetValueFatalIfFail("mds.DbName", &option->dbName);
    conf_->GetValueFatalIfFail("mds.DbUser", &option->dbUser);
    conf_->GetValueFatalIfFail("mds.DbUrl", &option->dbUrl);
    conf_->GetValueFatalIfFail("mds.DbPassword", &option->dbPassword);
    conf_->GetValueFatalIfFail("mds.DbPoolSize", &option->dbPoolSize);
}

void MDS::InitTopology(const TopologyOption& option) {
    auto topologyIdGenerator  =
        std::make_shared<DefaultIdGenerator>();
    auto topologyTokenGenerator =
        std::make_shared<DefaultTokenGenerator>();

    auto topologyStorage =
        std::make_shared<DefaultTopologyStorage>(mdsRepo_);

    LOG_IF(FATAL, !topologyStorage->init(option))
        << "init topologyStorage fail.";

    LOG(INFO) << "init topologyStorage success.";

    topology_ =
        std::make_shared<TopologyImpl>(topologyIdGenerator,
                                           topologyTokenGenerator,
                                           topologyStorage);
    LOG_IF(FATAL, topology_->init(option) < 0) << "init topology fail.";

    LOG(INFO) << "init topology success.";
}

void MDS::InitTopologyStat() {
    topologyStat_ =
        std::make_shared<TopologyStatImpl>(topology_);
    LOG_IF(FATAL, topologyStat_->Init() < 0)
        << "init topologyStat fail.";
    LOG(INFO) << "init topologyStat success.";
}

void MDS::InitTopologyMetricService(const TopologyOption& option) {
    topologyMetricService_ =
        std::make_shared<TopologyMetricService>(topology_,
            topologyStat_,
            segmentAllocStatistic_);
    LOG_IF(FATAL, topologyMetricService_->Init(option) < 0)
        << "init topologyMetricService fail.";
    LOG(INFO) << "init topologyMetricService success.";
}

void MDS::InitTopologyServiceManager(const TopologyOption& option) {
    CopysetOption copysetOption;
    InitCopysetOption(&copysetOption);
    // init CopysetManager
    auto copysetManager =
        std::make_shared<CopysetManager>(copysetOption);

    LOG(INFO) << "init copysetManager success.";
    // init TopologyServiceManager
    topologyServiceManager_ =
        std::make_shared<TopologyServiceManager>(topology_,
        copysetManager);
    topologyServiceManager_->Init(option);
    LOG(INFO) << "init topologyServiceManager success.";
}

void MDS::InitCopysetOption(CopysetOption *copysetOption) {
    conf_->GetValueFatalIfFail("mds.copyset.copysetRetryTimes",
        &copysetOption->copysetRetryTimes);
    conf_->GetValueFatalIfFail("mds.copyset.scatterWidthVariance",
        &copysetOption->scatterWidthVariance);
    conf_->GetValueFatalIfFail(
        "mds.copyset.scatterWidthStandardDevation",
        &copysetOption->scatterWidthStandardDevation);
    conf_->GetValueFatalIfFail("mds.copyset.scatterWidthRange",
        &copysetOption->scatterWidthRange);
    conf_->GetValueFatalIfFail(
        "mds.copyset.scatterWidthFloatingPercentage",
        &copysetOption->scatterWidthFloatingPercentage);
}


void MDS::InitTopologyChunkAllocator(const TopologyOption& option) {
    topologyChunkAllocator_ =
          std::make_shared<TopologyChunkAllocatorImpl>(topology_,
               segmentAllocStatistic_, option);
    LOG(INFO) << "init topologyChunkAllocator success.";
}

void MDS::InitNameServerStorage(int mdsCacheCount) {
    // init LRUCache
    auto cache = std::make_shared<LRUCache>(mdsCacheCount);
    LOG(INFO) << "init LRUCache success.";

    // init NameServerStorage
    nameServerStorage_ = std::make_shared<NameServerStorageImp>(etcdClient_,
                                                                cache);
    LOG(INFO) << "init NameServerStorage success.";

    // init recyclebindir
    LOG_IF(FATAL, !InitRecycleBinDir(nameServerStorage_))
                            << "init recyclebindir error";
    LOG(INFO) << "init InitRecycleBinDir success.";
}

void MDS::InitCurveFS(const CurveFSOption& curveFSOptions) {
    // init InodeIDGenerator
    auto inodeIdGenerator = std::make_shared<InodeIdGeneratorImp>(etcdClient_);

    // init ChunkIDGenerator
    auto chunkIdGenerator = std::make_shared<ChunkIDGeneratorImp>(etcdClient_);

    // init ChunkSegmentAllocator
    auto chunkSegmentAllocate =
        std::make_shared<ChunkSegmentAllocatorImpl>(
                        topologyChunkAllocator_, chunkIdGenerator);
    LOG(INFO) << "init ChunkSegmentAllocator success.";

    // init clean manager
    InitCleanManager();

    // init FileRecordManager
    auto fileRecordManager = std::make_shared<FileRecordManager>();
    LOG_IF(FATAL, !kCurveFS.Init(nameServerStorage_, inodeIdGenerator,
                  chunkSegmentAllocate, cleanManager_,
                  fileRecordManager,
                  segmentAllocStatistic_,
                  curveFSOptions, mdsRepo_))
        << "init FileRecordManager fail";
    LOG(INFO) << "init FileRecordManager success.";

    LOG(INFO) << "RecoverCleanTasks success.";
}

void MDS::InitFileRecordOptions(FileRecordOptions *fileRecordOptions) {
    conf_->GetValueFatalIfFail(
        "mds.file.expiredTimeUs", &fileRecordOptions->fileRecordExpiredTimeUs);
    conf_->GetValueFatalIfFail(
        "mds.file.scanIntevalTimeUs", &fileRecordOptions->scanIntervalTimeUs);
}

void MDS::InitAuthOptions(RootAuthOption *authOptions) {
    authOptions->rootOwner = ROOTUSERNAME;
    conf_->GetValueFatalIfFail(
        "mds.auth.rootPassword", &authOptions->rootPassword);
}

void MDS::InitCurveFSOptions(CurveFSOption *curveFSOptions) {
    conf_->GetValueFatalIfFail(
        "mds.curvefs.defaultChunkSize", &curveFSOptions->defaultChunkSize);
    FileRecordOptions fileRecordOptions;
    InitFileRecordOptions(&curveFSOptions->fileRecordOptions);

    RootAuthOption authOptions;
    InitAuthOptions(&curveFSOptions->authOptions);
}

void MDS::InitCleanManager() {
    // TODO(hzsunjianliang): should add threadpoolsize & checktime from config
    auto channelPool = std::make_shared<ChannelPool>();
    auto taskManager = std::make_shared<CleanTaskManager>(channelPool);
    // init copysetClient
    ChunkServerClientOption chunkServerClientOption;
    InitChunkServerClientOption(&chunkServerClientOption);
    auto copysetClient =
        std::make_shared<CopysetClient>(topology_, chunkServerClientOption,
                                                        channelPool);

    auto cleanCore = std::make_shared<CleanCore>(nameServerStorage_,
                                                 copysetClient,
                                                 segmentAllocStatistic_);

    cleanManager_ = std::make_shared<CleanManager>(cleanCore,
                                            taskManager, nameServerStorage_);
    LOG(INFO) << "init CleanManager success.";
}

void MDS::InitChunkServerClientOption(ChunkServerClientOption *option) {
    conf_->GetValueFatalIfFail("mds.chunkserverclient.rpcTimeoutMs",
        &option->rpcTimeoutMs);
    conf_->GetValueFatalIfFail("mds.chunkserverclient.rpcRetryTimes",
        &option->rpcRetryTimes);
    conf_->GetValueFatalIfFail("mds.chunkserverclient.rpcRetryIntervalMs",
        &option->rpcRetryIntervalMs);
    conf_->GetValueFatalIfFail("mds.chunkserverclient.updateLeaderRetryTimes",
        &option->updateLeaderRetryTimes);
    conf_->GetValueFatalIfFail(
        "mds.chunkserverclient.updateLeaderRetryIntervalMs",
        &option->updateLeaderRetryIntervalMs);
}

void MDS::InitCoordinator() {
    // init option
    ScheduleOption scheduleOption;
    InitScheduleOption(&scheduleOption);

    auto scheduleMetrics = std::make_shared<ScheduleMetrics>(topology_);
    auto topoAdapter = std::make_shared<TopoAdapterImpl>(
        topology_, topologyServiceManager_, topologyStat_);
    coordinator_ = std::make_shared<Coordinator>(topoAdapter);
    coordinator_->InitScheduler(scheduleOption, scheduleMetrics);
}

void MDS::InitScheduleOption(ScheduleOption *scheduleOption) {
    conf_->GetValueFatalIfFail("mds.enable.copyset.scheduler",
        &scheduleOption->enableCopysetScheduler);
    conf_->GetValueFatalIfFail("mds.enable.leader.scheduler",
        &scheduleOption->enableLeaderScheduler);
    conf_->GetValueFatalIfFail("mds.enable.recover.scheduler",
        &scheduleOption->enableRecoverScheduler);
    conf_->GetValueFatalIfFail("mds.enable.replica.scheduler",
        &scheduleOption->enableReplicaScheduler);

    conf_->GetValueFatalIfFail("mds.copyset.scheduler.intervalSec",
        &scheduleOption->copysetSchedulerIntervalSec);
    conf_->GetValueFatalIfFail("mds.leader.scheduler.intervalSec",
        &scheduleOption->leaderSchedulerIntervalSec);
    conf_->GetValueFatalIfFail("mds.recover.scheduler.intervalSec",
        &scheduleOption->recoverSchedulerIntervalSec);
    conf_->GetValueFatalIfFail("mds.replica.scheduler.intervalSec",
        &scheduleOption->replicaSchedulerIntervalSec);
    conf_->GetValueFatalIfFail("mds.schduler.operator.concurrent",
        &scheduleOption->operatorConcurrent);
    conf_->GetValueFatalIfFail("mds.schduler.transfer.limitSec",
        &scheduleOption->transferLeaderTimeLimitSec);
    conf_->GetValueFatalIfFail("mds.scheduler.add.limitSec",
        &scheduleOption->addPeerTimeLimitSec);
    conf_->GetValueFatalIfFail("mds.scheduler.remove.limitSec",
        &scheduleOption->removePeerTimeLimitSec);
    conf_->GetValueFatalIfFail("mds.scheduler.change.limitSec",
        &scheduleOption->changePeerTimeLimitSec);
    conf_->GetValueFatalIfFail("mds.scheduler.copysetNumRangePercent",
        &scheduleOption->copysetNumRangePercent);
    conf_->GetValueFatalIfFail("mds.schduler.scatterWidthRangePerent",
        &scheduleOption->scatterWithRangePerent);
    conf_->GetValueFatalIfFail("mds.chunkserver.failure.tolerance",
        &scheduleOption->chunkserverFailureTolerance);
    conf_->GetValueFatalIfFail("mds.scheduler.chunkserver.cooling.timeSec",
        &scheduleOption->chunkserverCoolingTimeSec);
}

void MDS::InitHeartbeatManager() {
    // init option
    HeartbeatOption heartbeatOption;
    InitHeartbeatOption(&heartbeatOption);

    heartbeatOption.mdsStartTime = steady_clock::now();
    heartbeatManager_ = std::make_shared<HeartbeatManager>(
        heartbeatOption, topology_, topologyStat_, coordinator_);
    heartbeatManager_->Init();
    heartbeatManager_->Run();
}

void MDS::InitHeartbeatOption(HeartbeatOption* heartbeatOption) {
    conf_->GetValueFatalIfFail("mds.heartbeat.intervalMs",
                        &heartbeatOption->heartbeatIntervalMs);
    conf_->GetValueFatalIfFail("mds.heartbeat.misstimeoutMs",
                        &heartbeatOption->heartbeatMissTimeOutMs);
    conf_->GetValueFatalIfFail("mds.heartbeat.offlinetimeoutMs",
                        &heartbeatOption->offLineTimeOutMs);
    conf_->GetValueFatalIfFail("mds.heartbeat.clean_follower_afterMs",
                        &heartbeatOption->cleanFollowerAfterMs);
}
}  // namespace mds
}  // namespace curve
