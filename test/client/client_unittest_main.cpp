/*
 * Project: curve
 * File Created: Wednesday, 3rd October 2018 5:08:08 pm
 * Author: tongguangxun
 * Copyright (c) 2018 NetEase
 */


#include <brpc/server.h>
#include <sys/types.h>
#include <unistd.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <gtest/gtest.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gmock/gmock.h>
#include <iostream>
#include <string>

#include "include/curve_compiler_specific.h"
#include "proto/nameserver2.pb.h"
#include "proto/topology.pb.h"
#include "src/client/io_condition_varaiable.h"
#include "src/client/iomanager4file.h"
#include "src/client/io_tracker.h"
#include "src/client/client_common.h"
#include "src/client/metacache.h"
#include "src/client/request_context.h"
#include "src/client/file_instance.h"
#include "src/client/splitor.h"
#include "src/client/libcurve_file.h"
#include "include/client/libcurve.h"
#include "test/integration/cluster_common/cluster.h"
#include "test/util/config_generator.h"

std::string mdsMetaServerAddr = "127.0.0.1:9104";     // NOLINT
uint32_t segment_size = 1 * 1024 * 1024 * 1024ul;   // NOLINT
uint32_t chunk_size = 4 * 1024 * 1024;   // NOLINT
std::string configpath = "./test/client/testConfig/client.conf";   // NOLINT
using curve::client::FileClient;

const std::vector<std::string> clientConf {
    std::string("mds.listen.addr=127.0.0.1:9104,127.0.0.1:9104"),
    std::string("global.logPath=./runlog/"),
    std::string("chunkserver.rpcTimeoutMS=1000"),
    std::string("chunkserver.opMaxRetry=3"),
    std::string("metacache.getLeaderRetry=3"),
    std::string("metacache.getLeaderTimeOutMS=1000"),
    std::string("global.fileMaxInFlightRPCNum=2048"),
    std::string("metacache.rpcRetryIntervalUS=500"),
    std::string("mds.rpcRetryIntervalUS=500"),
    std::string("schedule.threadpoolSize=2"),
};

int main(int argc, char ** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    ::testing::InitGoogleMock(&argc, argv);
    google::ParseCommandLineFlags(&argc, &argv, false);

    curve::CurveCluster* cluster = new curve::CurveCluster();

    cluster->PrepareConfig<curve::ClientConfigGenerator>(
        configpath, clientConf);

    int ret = RUN_ALL_TESTS();

    return ret;
}
