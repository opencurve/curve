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
 * Created Date: 2020-01-03
 * Author: charisu
 */

#include <glog/logging.h>
#include "src/mds/server/mds.h"
#include "src/mds/nameserver2/helper/namespace_helper.h"
#include "src/mds/topology/topology_storge_etcd.h"
#include "src/common/lru_cache.h"

using ::curve::mds::topology::TopologyStorageEtcd;
using ::curve::mds::topology::TopologyStorageCodec;

namespace curve {
namespace mds {

using LRUCache = ::curve::common::LRUCache<std::string, std::string>;
using CacheMetrics = ::curve::common::CacheMetrics;

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
    InitTopologyOption(&options_.topologyOption);
    InitCopysetOption(&options_.copysetOption);
    InitChunkServerClientOption(&options_.chunkServerClientOption);
    InitSnapshotCloneClientOption(&options_.snapshotCloneClientOption);

    conf_->GetValueFatalIfFail(
        "mds.segment.alloc.retryInterMs", &options_.retryInterTimes);
    conf_->GetValueFatalIfFail(
        "mds.segment.alloc.periodic.persistInterMs",
        &options_.periodicPersistInterMs);

    // cache size of namestorage
    conf_->GetValueFatalIfFail("mds.cache.count", &options_.mdsCacheCount);

    conf_->GetValueFatalIfFail("mds.listen.addr", &options_.mdsListenAddr);

    conf_->GetValueFatalIfFail(
        "mds.dummy.listen.port", &options_.dummyListenPort);

    conf_->GetValueFatalIfFail(
        "mds.filelock.bucketNum", &options_.mdsFilelockBucketNum);
}

void MDS::StartDummy() {
    // expose version, configuration and role (leader or follower)
    LOG(INFO) << "mds version: " << curve::common::CurveVersion();
    curve::common::ExposeCurveVersion();
    conf_->ExposeMetric("mds_config");
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
    // initialize etcd client
    // TODO(lixiaocui): why not store all configuration in EtcdConf?
    int etcdTimeout;
    conf_->GetValueFatalIfFail(
        "mds.etcd.operation.timeoutMs", &etcdTimeout);
    int etcdRetryTimes;
    conf_->GetValueFatalIfFail("mds.etcd.retry.times", &etcdRetryTimes);
    EtcdConf etcdConf;
    InitEtcdConf(&etcdConf);
    InitEtcdClient(etcdConf, etcdTimeout, etcdRetryTimes);

    // leader election
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
    InitSegmentAllocStatistic(options_.retryInterTimes,
                              options_.periodicPersistInterMs);
    InitNameServerStorage(options_.mdsCacheCount);
    InitTopology(options_.topologyOption);
    InitTopologyStat();
    InitTopologyChunkAllocator(options_.topologyOption);
    InitTopologyMetricService(options_.topologyOption);
    InitTopologyServiceManager(options_.topologyOption);
    InitCurveFS(options_.curveFSOptions);
    InitCoordinator();
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
    segmentAllocStatistic_->Run();
    LOG_IF(FATAL, topology_->Run()) << "run topology module fail";
    LOG_IF(FATAL, topologyMetricService_->Run() < 0)
        << "topologyMetricService start run fail";
    kCurveFS.Run();
    LOG_IF(FATAL, !cleanManager_->Start()) << "start cleanManager fail.";
    // recover unfinished tasks
    cleanManager_->RecoverCleanTasks();

    cleanDiscardSegmentTask_->Start();

    // start scheduler module
    coordinator_->Run();
    // start brpc server
    StartServer();
}

void MDS::Stop() {
    if (!running_) {
        LOG(INFO) << "MDS is not running";
        return;
    }
    brpc::AskToQuit();

    leaderElection_->LeaderResign();
    LOG(INFO) << "resign success";

    heartbeatManager_->Stop();

    coordinator_->Stop();

    kCurveFS.Uninit();

    cleanDiscardSegmentTask_->Stop();

    cleanManager_->Stop();

    topologyMetricService_->Stop();

    topology_->Stop();

    segmentAllocStatistic_->Stop();

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

    // To achieve the graceful exit of SIGTERM, you need to specify parameters
    // when starting the process: --graceful_quit_on_sigterm
    server.RunUntilAskedToQuit();
}

void MDS::InitEtcdClient(const EtcdConf& etcdConf,
                         int etcdTimeout,
                         int retryTimes) {
    etcdClient_ = std::make_shared<EtcdClientImp>();
    auto res = etcdClient_->Init(etcdConf, etcdTimeout, retryTimes);
    LOG_IF(FATAL, res != EtcdErrCode::EtcdOK)
        << "init etcd client err! "
        << "etcdaddr: " << std::string{etcdConf.Endpoints, etcdConf.len}
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
            << "etcdaddr: " << std::string{etcdConf.Endpoints, etcdConf.len}
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
    conf_->GetValueFatalIfFail(
        "mds.topology.enableLogicalPoolStatus",
        &topologyOption->enableLogicalPoolStatus);
}

void MDS::InitTopology(const TopologyOption& option) {
    auto topologyIdGenerator  =
        std::make_shared<DefaultIdGenerator>();
    auto topologyTokenGenerator =
        std::make_shared<DefaultTokenGenerator>();

    auto codec = std::make_shared<TopologyStorageCodec>();
    auto topologyStorage =
        std::make_shared<TopologyStorageEtcd>(etcdClient_, codec);

    LOG(INFO) << "init topologyStorage success.";

    topology_ =
        std::make_shared<TopologyImpl>(topologyIdGenerator,
                                           topologyTokenGenerator,
                                           topologyStorage);
    LOG_IF(FATAL, topology_->Init(option) < 0) << "init topology fail.";

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

    auto cache = std::make_shared<LRUCache>(mdsCacheCount,
        std::make_shared<CacheMetrics>("mds_nameserver_cache_metric"));
    LOG(INFO) << "init LRUCache success.";

    // init NameServerStorage
    nameServerStorage_ = std::make_shared<NameServerStorageImp>(etcdClient_,
                                                                cache);
    LOG(INFO) << "init NameServerStorage success.";
}

void MDS::InitSnapshotCloneClientOption(SnapshotCloneClientOption *option) {
    if (!conf_->GetValue("mds.snapshotcloneclient.addr",
        &option->snapshotCloneAddr)) {
            option->snapshotCloneAddr = "";
        }
}

void MDS::InitSnapshotCloneClient() {
    snapshotCloneClient_ = std::make_shared<SnapshotCloneClient>();
    SnapshotCloneClientOption snapshotCloneClientOption;
    InitSnapshotCloneClientOption(&snapshotCloneClientOption);
    snapshotCloneClient_->Init(snapshotCloneClientOption);
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

    // init clean discard segment task
    uint32_t scanDiscardTaskIntervalMs = 0;
    if (!conf_->GetUInt32Value("mds.segment.discard.scanIntevalMs",
                               &scanDiscardTaskIntervalMs)) {
        LOG(FATAL) << "Load mds.segment.discard.scanIntevalMs from config "
                      "file failed";
    }

    cleanDiscardSegmentTask_ = std::make_shared<CleanDiscardSegmentTask>(
        cleanManager_, nameServerStorage_, scanDiscardTaskIntervalMs);

    // init FileRecordManager
    auto fileRecordManager = std::make_shared<FileRecordManager>();

    // init snapshotCloneClient
    InitSnapshotCloneClient();

    LOG_IF(FATAL, !kCurveFS.Init(nameServerStorage_, inodeIdGenerator,
                  chunkSegmentAllocate, cleanManager_,
                  fileRecordManager,
                  segmentAllocStatistic_,
                  curveFSOptions, topology_,
                  snapshotCloneClient_))
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
    conf_->GetValueFatalIfFail(
        "mds.auth.rootUserName", &authOptions->rootOwner);
    conf_->GetValueFatalIfFail(
        "mds.auth.rootPassword", &authOptions->rootPassword);
}

void MDS::InitThrottleOption(ThrottleOption* option) {
    conf_->GetValueFatalIfFail("mds.throttle.iopsMin", &option->iopsMin);
    conf_->GetValueFatalIfFail("mds.throttle.iopsMax", &option->iopsMax);
    conf_->GetValueFatalIfFail("mds.throttle.iopsPerGB", &option->iopsPerGB);

    conf_->GetValueFatalIfFail("mds.throttle.bpsMinInMB", &option->bpsMin);
    conf_->GetValueFatalIfFail("mds.throttle.bpsMaxInMB", &option->bpsMax);
    conf_->GetValueFatalIfFail("mds.throttle.bpsPerGBInMB", &option->bpsPerGB);

    option->bpsMin *= kMB;
    option->bpsMax *= kMB;
    option->bpsPerGB *= kMB;
}

void MDS::InitCurveFSOptions(CurveFSOption *curveFSOptions) {
    conf_->GetValueFatalIfFail(
        "mds.curvefs.defaultChunkSize", &curveFSOptions->defaultChunkSize);
    conf_->GetValueFatalIfFail(
        "mds.curvefs.defaultSegmentSize", &curveFSOptions->defaultSegmentSize);
    conf_->GetValueFatalIfFail(
        "mds.curvefs.minFileLength", &curveFSOptions->minFileLength);
    conf_->GetValueFatalIfFail(
        "mds.curvefs.maxFileLength", &curveFSOptions->maxFileLength);
    FileRecordOptions fileRecordOptions;
    InitFileRecordOptions(&curveFSOptions->fileRecordOptions);

    RootAuthOption authOptions;
    InitAuthOptions(&curveFSOptions->authOptions);

    InitThrottleOption(&curveFSOptions->throttleOption);
}

void MDS::InitDLockOption(std::shared_ptr<DLockOpts> dlockOpts) {
    conf_->GetValueFatalIfFail(
        "mds.etcd.retry.times", &dlockOpts->retryTimes);
    conf_->GetValueFatalIfFail(
        "mds.etcd.dlock.timeoutMs", &dlockOpts->ctx_timeoutMS);
    conf_->GetValueFatalIfFail(
        "mds.etcd.dlock.ttlSec", &dlockOpts->ttlSec);
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

    // init dlock options
    auto dlockOpts = std::make_shared<DLockOpts>();
    InitDLockOption(dlockOpts);

    cleanManager_ = std::make_shared<CleanManager>(cleanCore,
                                            taskManager, nameServerStorage_);
    cleanManager_->InitDLockOptions(dlockOpts);
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
    conf_->GetValueFatalIfFail("mds.enable.scan.scheduler",
        &scheduleOption->enableScanScheduler);

    conf_->GetValueFatalIfFail("mds.copyset.scheduler.intervalSec",
        &scheduleOption->copysetSchedulerIntervalSec);
    conf_->GetValueFatalIfFail("mds.leader.scheduler.intervalSec",
        &scheduleOption->leaderSchedulerIntervalSec);
    conf_->GetValueFatalIfFail("mds.recover.scheduler.intervalSec",
        &scheduleOption->recoverSchedulerIntervalSec);
    conf_->GetValueFatalIfFail("mds.replica.scheduler.intervalSec",
        &scheduleOption->replicaSchedulerIntervalSec);
    conf_->GetValueFatalIfFail("mds.scan.scheduler.intervalSec",
        &scheduleOption->scanSchedulerIntervalSec);

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
    conf_->GetValueFatalIfFail("mds.scheduler.scan.limitSec",
        &scheduleOption->scanPeerTimeLimitSec);

    conf_->GetValueFatalIfFail("mds.scheduler.copysetNumRangePercent",
        &scheduleOption->copysetNumRangePercent);
    conf_->GetValueFatalIfFail("mds.schduler.scatterWidthRangePerent",
        &scheduleOption->scatterWithRangePerent);
    conf_->GetValueFatalIfFail("mds.chunkserver.failure.tolerance",
        &scheduleOption->chunkserverFailureTolerance);
    conf_->GetValueFatalIfFail("mds.scheduler.chunkserver.cooling.timeSec",
        &scheduleOption->chunkserverCoolingTimeSec);
    conf_->GetValueFatalIfFail("mds.scheduler.scan.startHour",
        &scheduleOption->scanStartHour);
    conf_->GetValueFatalIfFail("mds.scheduler.scan.endHour",
        &scheduleOption->scanEndHour);
    conf_->GetValueFatalIfFail("mds.scheduler.scan.intervalSec",
        &scheduleOption->scanIntervalSec);
    conf_->GetValueFatalIfFail("mds.scheduler.scan.concurrent.per.pool",
        &scheduleOption->scanConcurrentPerPool);
    conf_->GetValueFatalIfFail("mds.scheduler.scan.concurrent.per.chunkserver",
        &scheduleOption->scanConcurrentPerChunkserver);
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
