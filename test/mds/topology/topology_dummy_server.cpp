/*
 * Project:
 * Created Date: Mon Sep 17 2018
 * Author: xuchaojie
 * Copyright (c) 2018 netease
 */

#include <gflags/gflags.h>
#include <brpc/server.h>

#include "src/mds/topology/topology_service.h"
#include "src/mds/topology/topology.h"
#include "src/mds/topology/topology_id_generator.h"
#include "src/mds/topology/topology_token_generator.h"
#include "src/mds/topology/topology_storge.h"
#include "src/mds/topology/topology_service_manager.h"
#include "src/mds/common/mds_define.h"
#include "src/mds/copyset/copyset_manager.h"
#include "src/mds/topology/topology_admin.h"
#include "test/mds/topology/mock_topology.h"
#include "proto/topology.pb.h"


DEFINE_int32(port, 8000, "TCP Port of this server");
DEFINE_int32(idle_timeout_s, -1, "Connection will be closed if there is no "
             "read/write operations during the last `idle_timeout_s'");


using ::curve::mds::topology::TopologyIdGenerator;
using ::curve::mds::topology::DefaultIdGenerator;
using ::curve::mds::topology::TopologyTokenGenerator;
using ::curve::mds::topology::DefaultTokenGenerator;
using ::curve::mds::topology::TopologyStorage;
using ::curve::mds::topology::DefaultTopologyStorage;
using ::curve::mds::topology::Topology;
using ::curve::mds::topology::TopologyImpl;
using ::curve::mds::copyset::CopysetManager;
using ::curve::mds::topology::MockTopologyServiceManager;
using ::curve::mds::topology::TopologyServiceImpl;


int main(int argc, char* argv[]) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    brpc::Server server;

    std::shared_ptr<TopologyIdGenerator> idGenerator_  =
        std::make_shared<DefaultIdGenerator>();
    std::shared_ptr<TopologyTokenGenerator> tokenGenerator_ =
        std::make_shared<DefaultTokenGenerator>();

    std::shared_ptr<::curve::mds::MdsRepo> repo_ =
        std::make_shared<::curve::mds::MdsRepo>();

    std::shared_ptr<TopologyStorage> storage_ =
        std::make_shared<DefaultTopologyStorage>(repo_);

    std::shared_ptr<MockTopologyServiceManager> manager_ =
        std::make_shared<MockTopologyServiceManager>(
            std::make_shared<TopologyImpl>(idGenerator_,
                                       tokenGenerator_,
                                       storage_),
            std::make_shared<CopysetManager>());

    TopologyServiceImpl *topoService = new TopologyServiceImpl(manager_);

    if (server.AddService(topoService,
                          brpc::SERVER_OWNS_SERVICE) != 0) {
        LOG(ERROR) << "Fail to add service";
        return -1;
    }

    // Start the server.
    brpc::ServerOptions options;
    options.idle_timeout_sec = FLAGS_idle_timeout_s;
    if (server.Start(FLAGS_port, &options) != 0) {
        LOG(ERROR) << "Fail to start topology Server";
        return -1;
    }

    // Wait until Ctrl-C is pressed, then Stop() and Join() the server.
    server.RunUntilAskedToQuit();
    return 0;
}



