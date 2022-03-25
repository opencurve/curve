/*
 *  Copyright (c) 2021 NetEase Inc.
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

/**
 * Project: curve
 * File Created: Fri Jul 16 21:22:40 CST 2021
 * Author: wuhanqing
 */

#include <brpc/server.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "curvefs/src/space/config.h"
#include "curvefs/src/space/space_manager.h"
#include "curvefs/src/space/space_service.h"
#include "src/common/configuration.h"

using curve::common::Configuration;
using curvefs::space::SpaceAllocServiceImpl;
using curvefs::space::SpaceManagerImpl;
using curvefs::space::SpaceManagerOption;

DEFINE_string(conf, "/etc/curve/space.conf", "space config file path");
DEFINE_string(listenAddr, "127.0.0.1:9500", "space listen address");

void LoadConfigFromGflags(Configuration* conf) {
    if (FLAGS_log_dir.empty()) {
        if (!conf->GetStringValue("common.log_dir", &FLAGS_log_dir)) {
            LOG(WARNING) << "no common.log_dir found in "
                         << conf->GetConfigPath() << ", will log to /tmp";
        }
    }

    google::CommandLineFlagInfo info;
    if (google::GetCommandLineFlagInfo("listenAddr", &info) &&
        !info.is_default) {
        conf->SetStringValue("common.listen_address", FLAGS_listenAddr);
    }
}

void LoadSpaceManagerOption(Configuration* conf, SpaceManagerOption* opt) {
    LOG_IF(FATAL, !conf->GetUInt32Value("common.block_size", &opt->blockSize))
        << "common.block_size not found in " << conf->GetConfigPath();
    LOG_IF(FATAL, !conf->GetStringValue("allocator.type", &opt->allocatorType))
        << "allocator.type not found in " << conf->GetConfigPath();

    LOG_IF(FATAL,
           !conf->GetStringValue("metaserver.address",
                                 &opt->reloaderOption.metaServerOption.addr))
        << "metaserver.address not found in " << conf->GetConfigPath();

    if (opt->allocatorType == "bitmap") {
        LOG_IF(FATAL,
               !conf->GetUInt64Value(
                   "bitmapallocator.size_per_bit",
                   &opt->allocatorOption.bitmapAllocatorOption.sizePerBit))
            << "bitmapallocator.size_per_bit not found in "
            << conf->GetConfigPath();
        LOG_IF(FATAL,
               !conf->GetDoubleValue("bitmapallocator.small_alloc_proportion",
                                     &opt->allocatorOption.bitmapAllocatorOption
                                          .smallAllocProportion))
            << "bitmapallocator.small_alloc_proportion not found in "
            << conf->GetConfigPath();
    }
}

int Run(Configuration* conf) {
    SpaceManagerOption spaceManagerOpt;
    LoadSpaceManagerOption(conf, &spaceManagerOpt);

    SpaceManagerImpl space(spaceManagerOpt);
    SpaceAllocServiceImpl spaceAllocService(&space);

    brpc::Server server;
    int rv =
        server.AddService(&spaceAllocService, brpc::SERVER_DOESNT_OWN_SERVICE);
    if (rv != 0) {
        LOG(ERROR) << "AddService failed";
        return -1;
    }

    std::string listenAddr;
    LOG_IF(FATAL, !conf->GetStringValue("common.listen_address", &listenAddr))
        << "common.listen_address not found in " << conf->GetConfigPath();

    butil::EndPoint listenPoint;
    rv = butil::str2endpoint(listenAddr.c_str(), &listenPoint);
    if (rv != 0) {
        LOG(ERROR) << "parse endpoint from " << FLAGS_listenAddr << " failed";
        return -1;
    }

    rv = server.Start(listenPoint, nullptr);
    if (rv != 0) {
        LOG(ERROR) << "Start server failed";
        return -1;
    }

    LOG(INFO) << "Server started at " << butil::endpoint2str(listenPoint);
    server.RunUntilAskedToQuit();
    LOG(INFO) << "Server stopped";

    return 0;
}

int main(int argc, char* argv[]) {
    google::ParseCommandLineFlags(&argc, &argv, true);

    Configuration conf;
    conf.SetConfigPath(FLAGS_conf);

    LOG_IF(FATAL, !conf.LoadConfig())
        << "load space configuration failed, conf path: "
        << conf.GetConfigPath();

    LoadConfigFromGflags(&conf);

    google::InitGoogleLogging(argv[0]);
    int ret = Run(&conf);
    google::ShutdownGoogleLogging();

    return ret;
}
