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
#include "src/mds/topology/topology_admin.h"
#include "src/mds/topology/topology_manager.h"
#include "src/mds/topology/topology_service.h"
#include "src/common/configuration.h"
#include "src/mds/heartbeat/heartbeat_service.h"
#include "src/mds/schedule/topoAdapter.h"
#include "proto/heartbeat.pb.h"

DEFINE_string(listenAddr, ":6666", "Initial  mds listen addr");
DEFINE_bool(enableCopySetScheduler, false, "can copySet scheduler run");
DEFINE_bool(enableLeaderScheduler, false, "can leader scheduler run");
DEFINE_bool(enableRecoverScheduler, true, "can recover scheduler run");
DEFINE_bool(enableReplicaScheduler, false, "can replica scheduler run");
DEFINE_int64(copySetInterval, 30, "copySet scheduler run interval");
DEFINE_int64(replicaInterval, 30, "replica scheduler run interval");
DEFINE_int64(leaderInterval, 30, "leader scheduler run interval");
DEFINE_int64(recoverInterval, 30, "recover scheduler run interval");
DEFINE_int32(opConcurrent, 4, "operator num on a chunkserver");
DEFINE_int32(transferLimit, 1800, "transfer leader time limit(second)");
DEFINE_int32(RemoveLimit, 1800, "remove peer time limit(second)");
DEFINE_int32(AddLimit, 7200, "add peer time limit(second)");
DEFINE_uint64(heartbeatInterval, 10, "heartbeat interval");
DEFINE_uint64(heartbeatMissTimeout, 30, "heartbeat miss interval");
DEFINE_uint64(offlineTimeout, 1800, "timeout to offline");
DEFINE_string(confPath, "deploy/local/mds/mds.conf", "mds confPath");

using ::curve::mds::topology::TopologyAdminImpl;
using ::curve::mds::topology::TopologyAdmin;
using ::curve::mds::topology::TopologyServiceImpl;
using ::curve::mds::topology::TopologyManager;
using ::curve::mds::heartbeat::HeartbeatServiceImpl;
using ::curve::mds::heartbeat::HeartbeatOption;
using ::curve::mds::schedule::TopoAdapterImpl;
using ::curve::mds::schedule::TopoAdapter;
using ::curve::mds::schedule::ScheduleConfig;

namespace curve {
namespace mds {
void InitSessionOptions(common::Configuration *conf,
                        struct SessionOptions *sessionOptions) {
    sessionOptions->sessionDbName = conf->GetStringValue("session.DbName");
    sessionOptions->sessionUser = conf->GetStringValue("session.DbUser");
    sessionOptions->sessionUrl = conf->GetStringValue("session.DbUrl");
    sessionOptions->sessionPassword =
                                    conf->GetStringValue("session.DbPassword");
    sessionOptions->leaseTime = conf->GetIntValue("session.leaseTime");
    sessionOptions->toleranceTime = conf->GetIntValue("session.toleranceTime");
    sessionOptions->intevalTime = conf->GetIntValue("session.intevalTime");
}

void InitAuthOptions(common::Configuration *conf,
                        struct RootAuthOption *authOptions) {
    authOptions->rootOwner = conf->GetStringValue("auth.rootOwner");
    authOptions->rootPassword = conf->GetStringValue("auth.rootPassword");
}

int curve_main(int argc, char **argv) {
    // google::InitGoogleLogging(argv[0]);
    google::ParseCommandLineFlags(&argc, &argv, false);

    // 加载配置
    LOG(INFO) << "load mds configuration.";

    std::string confPath = FLAGS_confPath.c_str();
    common::Configuration conf;
    conf.SetConfigPath(confPath);
    if (!conf.LoadConfig()) {
        LOG(ERROR) << "load mds configuration fail, conf path = "
                   << confPath;
        return -1;
    }

    struct SessionOptions sessionOptions;
    InitSessionOptions(&conf, &sessionOptions);

    struct RootAuthOption authOptions;
    InitAuthOptions(&conf, &authOptions);

    // init nameserver
    NameServerStorage *storage_;
    InodeIDGenerator *inodeGenerator_;
    ChunkSegmentAllocator *chunkSegmentAllocate_;
    SessionManager *sessionManager_;

    storage_ = new FakeNameServerStorage();
    inodeGenerator_ = new FakeInodeIDGenerator(0);

    std::shared_ptr<TopologyAdmin> topologyAdmin =
        TopologyManager::GetInstance()->GetTopologyAdmin();

    std::shared_ptr<FackChunkIDGenerator> chunkIdGenerator =
        std::make_shared<FackChunkIDGenerator>();
    chunkSegmentAllocate_ =
        new ChunkSegmentAllocatorImpl(topologyAdmin, chunkIdGenerator);

    // TODO(hzsunjianliang): should add threadpoolsize & checktime from config
    auto taskManager = std::make_shared<CleanTaskManager>();
    auto cleanCore = std::make_shared<CleanCore>(storage_);

    auto cleanManger = std::make_shared<CleanManager>(cleanCore,
                                                      taskManager, storage_);

    sessionManager_ = new SessionManager(std::make_shared<MdsRepo>());

    if (!kCurveFS.Init(storage_, inodeGenerator_,
                  chunkSegmentAllocate_, cleanManger,
                  sessionManager_, sessionOptions, authOptions)) {
        return -1;
    }

    if (!cleanManger->Start()) {
        LOG(ERROR) << "start cleanManager fail.";
        return -1;
    }

    // init scheduler
    auto topology = TopologyManager::GetInstance()->GetTopology();
    auto topoAdapter = std::make_shared<TopoAdapterImpl>(
        topology, TopologyManager::GetInstance()->GetServiceManager());
    auto coordinator = std::make_shared<Coordinator>(topoAdapter);
    ScheduleConfig scheduleConfig(
        FLAGS_enableCopySetScheduler, FLAGS_enableLeaderScheduler,
        FLAGS_enableRecoverScheduler, FLAGS_enableReplicaScheduler,
        FLAGS_copySetInterval, FLAGS_leaderInterval, FLAGS_recoverInterval,
        FLAGS_replicaInterval, FLAGS_opConcurrent, FLAGS_transferLimit,
        FLAGS_RemoveLimit, FLAGS_AddLimit);
    coordinator->InitScheduler(scheduleConfig);
    coordinator->Run();

    // init heartbeat manager
    auto heartbeatManager = std::make_shared<HeartbeatManager>(topology,
                                                               coordinator,
                                                               topoAdapter);
    HeartbeatOption heartbeatOption(FLAGS_heartbeatInterval,
                                    FLAGS_heartbeatMissTimeout,
                                    FLAGS_offlineTimeout);
    heartbeatManager->Init(heartbeatOption);
    heartbeatManager->Run();

    // add heartbeat service
    brpc::Server server;
    HeartbeatServiceImpl heartbeatService(heartbeatManager);
    if (server.AddService(&heartbeatService,
                          brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        LOG(ERROR) << "add topologyService error";
        return -1;
    }

    // add rpc service
    NameSpaceService namespaceService;
    if (server.AddService(&namespaceService,
                          brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        LOG(ERROR) << "add namespaceService error";
        return -1;
    }

    // add topology service
    TopologyServiceImpl topologyService;
    if (server.AddService(&topologyService,
                          brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        LOG(ERROR) << "add topologyService error";
        return -1;
    }

    // start rpc server
    brpc::ServerOptions option;
    option.idle_timeout_sec = -1;
    if (server.Start(FLAGS_listenAddr.c_str(), &option) != 0) {
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


