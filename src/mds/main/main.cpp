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
#include "src/mds/topology/topology_admin.h"
#include "src/mds/topology/topology_manager.h"
#include "src/mds/topology/topology_service.h"



DEFINE_string(listenAddr, ":6666", "Initial  mds listen addr");

using ::curve::mds::topology::TopologyAdminImpl;
using ::curve::mds::topology::TopologyAdmin;
using ::curve::mds::topology::TopologyServiceImpl;
using ::curve::mds::topology::TopologyManager;

namespace curve {
namespace mds {
int curve_main(int argc, char **argv) {
    google::InitGoogleLogging(argv[0]);
    google::ParseCommandLineFlags(&argc, &argv, false);


    // init nameserver
    NameServerStorage *storage_;
    InodeIDGenerator *inodeGenerator_;
    ChunkSegmentAllocator *chunkSegmentAllocate_;

    storage_ =  new FakeNameServerStorage();
    inodeGenerator_ = new FakeInodeIDGenerator(0);

    std::shared_ptr<TopologyAdmin> topologyAdmin =
       TopologyManager::GetInstance()->GetTopologyAdmin();

    std::shared_ptr<FackChunkIDGenerator> chunkIdGenerator =
                            std::make_shared<FackChunkIDGenerator>();
    chunkSegmentAllocate_ =
                new ChunkSegmentAllocatorImpl(topologyAdmin, chunkIdGenerator);

    // TODO(hzsunjianliang): should add threadpoolsize & checktime from config
    auto taskManager = std::make_shared<CleanTaskManager>();
    auto cleanCore   = std::make_shared<CleanCore>(storage_);

    auto cleanManger = std::make_shared<CleanManager>(cleanCore,
        taskManager, storage_);

    kCurveFS.Init(storage_, inodeGenerator_,
        chunkSegmentAllocate_, cleanManger);

    // add rpc service
    brpc::Server server;
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
    }

    server.RunUntilAskedToQuit();
    return 0;
}

}  // namespace mds
}  // namespace curve

int main(int argc, char **argv) {
    curve::mds::curve_main(argc, argv);
}


