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

#include "test/mds/nameserver2/fakes.h"
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
using ::curve::mds::copyset::CopysetManager;
using ::curve::mds::heartbeat::HeartbeatServiceImpl;
using ::curve::mds::heartbeat::HeartbeatOption;
using ::curve::mds::schedule::TopoAdapterImpl;
using ::curve::mds::schedule::TopoAdapter;
using ::curve::mds::schedule::ScheduleOption;
using ::curve::common::Configuration;

namespace curve {
namespace mds {
void InitSessionOptions(Configuration *conf,
                        struct SessionOptions *sessionOptions) {
    sessionOptions->sessionDbName = conf->GetStringValue("mds.DbName");
    sessionOptions->sessionUser = conf->GetStringValue("mds.DbUser");
    sessionOptions->sessionUrl = conf->GetStringValue("mds.DbUrl");
    sessionOptions->sessionPassword = conf->GetStringValue(
        "mds.DbPassword");
    sessionOptions->leaseTime = conf->GetIntValue("mds.session.leaseTime");
    sessionOptions->toleranceTime =
        conf->GetIntValue("mds.session.toleranceTime");
    sessionOptions->intevalTime = conf->GetIntValue("mds.session.intevalTime");
}

void InitAuthOptions(Configuration *conf,
                     struct RootAuthOption *authOptions) {
    authOptions->rootOwner = ROOTUSERNAME;
    authOptions->rootPassword = conf->GetStringValue("mds.auth.rootPassword");
}

void InitScheduleOption(Configuration *conf,
    ScheduleOption *scheduleOption) {
    scheduleOption->enableCopysetScheduler =
        conf->GetBoolValue("mds.enable.copyset.scheduler");
    scheduleOption->enableLeaderScheduler =
        conf->GetBoolValue("mds.enable.leader.scheduler");
    scheduleOption->enableRecoverScheduler =
        conf->GetBoolValue("mds.enable.recover.scheduler");
    scheduleOption->enableReplicaScheduler =
        conf->GetBoolValue("mds.replica.replica.scheduler");
    scheduleOption->copysetSchedulerInterval =
        conf->GetIntValue("mds.copyset.scheduler.interval");
    scheduleOption->leaderSchedulerInterval =
        conf->GetIntValue("mds.leader.scheduler.interval");
    scheduleOption->recoverSchedulerInterval =
        conf->GetIntValue("mds.recover.scheduler.interval");
    scheduleOption->replicaSchedulerInterval =
        conf->GetIntValue("mds.replica.scheduler.interval");

    scheduleOption->operatorConcurrent =
        conf->GetIntValue("mds.schduler.operator.concurrent");
    scheduleOption->transferLeaderTimeLimitSec =
        conf->GetIntValue("mds.schduler.transfer.limit");
    scheduleOption->addPeerTimeLimitSec =
        conf->GetIntValue("mds.scheduler.add.limit");
    scheduleOption->removePeerTimeLimitSec =
        conf->GetIntValue("mds.scheduler.remove.limit");
}

void InitHeartbeatOption(Configuration *conf,
    HeartbeatOption *heartbeatOption) {
    heartbeatOption->heartbeatIntervalMs =
        conf->GetIntValue("mds.heartbeat.interval");
    heartbeatOption->heartbeatMissTimeOutMs =
        conf->GetIntValue("mds.heartbeat.misstimeout");
    heartbeatOption->offLineTimeOutMs =
        conf->GetIntValue("mds.heartbeat.offlinetimeout");
    heartbeatOption->cleanFollowerAfterMs =
        conf->GetIntValue("mds.heartbeat.clean_follower_afterms");
}

void InitEtcdConf(Configuration *conf, EtcdConf *etcdConf) {
    std::string endpoint = conf->GetStringValue("mds.etcd.endpoint");
    etcdConf->Endpoints = new char[endpoint.size()];
    std::memcpy(etcdConf->Endpoints, endpoint.c_str(), endpoint.size());
    etcdConf->len = endpoint.size();
    etcdConf->DialTimeout = conf->GetIntValue("mds.etcd.dailtimeout");
}

void InitTopologyOption(Configuration *conf, TopologyOption *topologyOption) {
    topologyOption->dbName =
        conf->GetStringValue("mds.DbName");
    topologyOption->user =
        conf->GetStringValue("mds.DbUser");
    topologyOption->url =
        conf->GetStringValue("mds.DbUrl");
    topologyOption->password =
        conf->GetStringValue("mds.DbPassword");
    topologyOption->ChunkServerStateUpdateSec =
        conf->GetIntValue("mds.topology.ChunkServerStateUpdateSec");
}


int curve_main(int argc, char **argv) {
    // google::InitGoogleLogging(argv[0]);
    google::ParseCommandLineFlags(&argc, &argv, false);

    // =========================加载配置===============================//
    LOG(INFO) << "load mds configuration.";

    std::string confPath = FLAGS_confPath.c_str();
    Configuration conf;
    conf.SetConfigPath(confPath);
    if (!conf.LoadConfig()) {
        LOG(ERROR) << "load mds configuration fail, conf path = "
                   << confPath;
        return -1;
    }

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

    // ===========================init curveFs========================//
    // init EtcdClient
    auto client = std::make_shared<EtcdClientImp>();
    auto res = client->Init(
                etcdConf,
                conf.GetIntValue("mds.etcd.operation.timeout"),
                conf.GetIntValue("mds.etcd.retry.times"));
    if (res != EtcdErrCode::OK) {
        LOG(ERROR) << "init etcd client err! "
                  << "etcdaddr: " << etcdConf.Endpoints
                  << ", etcdaddr len: " << etcdConf.len
                  << ", etcdtimeout: " << etcdConf.DialTimeout
                  << ", operation timeout: "
                  << conf.GetIntValue("mds.etcd.operation.timeout")
                  << ", etcd retrytimes: "
                  << conf.GetIntValue("mds.etcd.retry.times");
        return -1;
    } else {
        LOG(INFO) << "init etcd client ok! "
                  << "etcdaddr: " << etcdConf.Endpoints
                  << ", etcdaddr len: " << etcdConf.len
                  << ", etcdtimeout: " << etcdConf.DialTimeout
                  << ", operation timeout: "
                  << conf.GetIntValue("mds.etcd.operation.timeout")
                  << ", etcd retrytimes: "
                  << conf.GetIntValue("mds.etcd.retry.times");
    }

    // init InodeIDGenerator
    auto inodeIdGenerator = std::make_shared<InodeIdGeneratorImp>(client);

    // init ChunkIDGenerator
    auto chunkIdGenerator = std::make_shared<ChunkIDGeneratorImp>(client);

    // init LRUCache
    auto cache =
        std::make_shared<LRUCache>(conf.GetIntValue("mds.cache.count"));

    // init NameServerStorage
    NameServerStorage *storage = new NameServerStorageImp(client, cache);

    // init recyclebindir
    if (!InitRecycleBinDir(storage))  {
        LOG(ERROR) << "init recyclebindir error";
        return -1;
    }

    // init topology
    auto topologyIdGenerator  =
        std::make_shared<DefaultIdGenerator>();
    auto topologyTokenGenerator =
        std::make_shared<DefaultTokenGenerator>();

    auto mdsRepo = std::make_shared<MdsRepo>();

    auto topologyStorage =
        std::make_shared<DefaultTopologyStorage>(mdsRepo);

    if (!topologyStorage->init(topologyOption)) {
        LOG(FATAL) << "init topologyStorage fail. dbName = "
                   << topologyOption.dbName
                   << " , user = "
                   << topologyOption.user
                   << " , url = "
                   << topologyOption.url
                   << " , password = "
                   << topologyOption.password;
        return -1;
    }

    auto topology =
        std::make_shared<TopologyImpl>(topologyIdGenerator,
                                           topologyTokenGenerator,
                                           topologyStorage);

    int errorCode = topology->init(topologyOption);
    if (errorCode < 0) {
        LOG(FATAL) << "init topology fail. errorCode = "
                   << errorCode;
        return errorCode;
    }

    // init CopysetManager
    auto copysetManager =
        std::make_shared<CopysetManager>();

    // init TopoAdmin
    auto topologyAdmin =
          std::make_shared<TopologyAdminImpl>(topology);

    // init TopologyServiceManager
    auto topologyServiceManager =
        std::make_shared<TopologyServiceManager>(topology,
        copysetManager);

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
    SessionManager *sessionManager =
        new SessionManager(std::make_shared<MdsRepo>());

    if (!kCurveFS.Init(storage, inodeIdGenerator.get(),
                  chunkSegmentAllocate, cleanManger,
                  sessionManager, sessionOptions, authOptions)) {
        return -1;
    }

    // start clean manager
    if (!cleanManger->Start()) {
        LOG(ERROR) << "start cleanManager fail.";
        return -1;
    }

    // =========================init scheduler======================//
    auto topoAdapter = std::make_shared<TopoAdapterImpl>(
        topology, topologyServiceManager);
    auto coordinator = std::make_shared<Coordinator>(topoAdapter);
    coordinator->InitScheduler(scheduleOption);
    coordinator->Run();

    // =======================init heartbeat manager================//
    auto heartbeatManager = std::make_shared<HeartbeatManager>(
        heartbeatOption, topology, coordinator);
    heartbeatManager->Init();
    heartbeatManager->Run();

    // =========================add service========================//
    // add heartbeat service
    brpc::Server server;
    HeartbeatServiceImpl heartbeatService(heartbeatManager);
    if (server.AddService(&heartbeatService,
                          brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        LOG(ERROR) << "add topologyService error";
        return -1;
    }

    // add rpc service
    NameSpaceService namespaceService(new FileLockManager(
        conf.GetIntValue("mds.filelock.bucketNum")));
    if (server.AddService(&namespaceService,
                          brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        LOG(ERROR) << "add namespaceService error";
        return -1;
    }

    // add topology service
    TopologyServiceImpl topologyService(topologyServiceManager);
    if (server.AddService(&topologyService,
                          brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        LOG(ERROR) << "add topologyService error";
        return -1;
    }

    // start rpc server
    brpc::ServerOptions option;
    option.idle_timeout_sec = -1;
    if (server.Start(
        conf.GetStringValue("mds.listen.addr").c_str(), &option) != 0) {
        LOG(ERROR) << "start brpc server error";
        return -1;
    }

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


