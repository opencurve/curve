/*
 * Project: curve
 * Created Date: Fri Dec 14 2018
 * Author: xuchaojie
 * Copyright (c) 2018 netease
 */

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <iostream>

#include "src/snapshot/snapshot_server.h"

int main(int argc, char* argv[]) {
    google::ParseCommandLineFlags(&argc, &argv, true);
    google::InitGoogleLogging("snapshotserver");

    curve::snapshotserver::SnapshotServer server;
    if (server.Init() < 0) {
        LOG(ERROR) << "Failed to init snapshot server";
        return -1;
    }

    LOG(INFO) << "Snapshot server starting...";
    if (server.Start() < 0) {
        LOG(ERROR) << "Failed to start snapshot server";
        return -1;
    }

    server.RunUntilAskedToQuit();

    return 0;
}
