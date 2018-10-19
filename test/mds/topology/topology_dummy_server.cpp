/*
 * Project:
 * Created Date: Mon Sep 17 2018
 * Author: xuchaojie
 * Copyright (c) 2018 netease
 */

#include <gflags/gflags.h>
#include <brpc/server.h>

#include "src/mds/topology/topology_manager.h"
#include "src/mds/topology/topology_service.h"
#include "proto/topology.pb.h"


DEFINE_int32(port, 8000, "TCP Port of this server");
DEFINE_int32(idle_timeout_s, -1, "Connection will be closed if there is no "
             "read/write operations during the last `idle_timeout_s'");


int main(int argc, char* argv[]) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    brpc::Server server;

    curve::mds::topology::TopologyServiceImpl topologyServiceImpl;

    if (server.AddService(&topologyServiceImpl,
                          brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
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



