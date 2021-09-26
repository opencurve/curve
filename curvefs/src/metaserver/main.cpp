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

/*
 * Project: curve
 * Created Date: 2021-05-19
 * Author: chenwei
 */

#include <glog/logging.h>
#include <gflags/gflags.h>
#include <butil/at_exit.h>  // butil::AtExitManager

#include "curvefs/src/common/process.h"
#include "curvefs/src/metaserver/metaserver.h"
#include "src/common/configuration.h"

DEFINE_string(confPath, "curvefs/conf/metaserver.conf", "metaserver confPath");
DEFINE_string(ip, "127.0.0.1", "metasetver listen ip");
DEFINE_int32(port, 16701, "metaserver listen port");

DEFINE_string(dataUri, "local:///mnt/data", "metaserver data uri");
DEFINE_string(trashUri, "local://mnt/data/recycler", "metaserver trash uri");
DEFINE_string(raftLogUri, "local://mnt/data/copysets",
              "metaserver raft log uri");
DEFINE_string(raftMetaUri, "local://mnt/data/copysets",
              "metaserver raft meta uri");
DEFINE_string(raftSnapshotUri, "local://mnt/data/copysets",
              "local://mnt/data/copysets");

using ::curve::common::Configuration;

void LoadConfigFromCmdline(Configuration *conf) {
    google::CommandLineFlagInfo info;
    if (GetCommandLineFlagInfo("ip", &info) && !info.is_default) {
        conf->SetStringValue("global.ip", FLAGS_ip);
    }
    if (GetCommandLineFlagInfo("port", &info) && !info.is_default) {
        conf->SetStringValue("global.port", FLAGS_ip);
    }

    if (GetCommandLineFlagInfo("dataUri", &info) && !info.is_default) {
        conf->SetStringValue("copyset.data_uri", FLAGS_dataUri);
    }

    if (GetCommandLineFlagInfo("trashUri", &info) && !info.is_default) {
        conf->SetStringValue("trash.uri", FLAGS_trashUri);
    }

    if (GetCommandLineFlagInfo("raftLogUri", &info) && !info.is_default) {
        conf->SetStringValue("copyset.raft_log_uri", FLAGS_raftLogUri);
    }

    if (GetCommandLineFlagInfo("raftMetaUri", &info) && !info.is_default) {
        conf->SetStringValue("copyset.raft_meta_uri", FLAGS_raftMetaUri);
    }

    if (GetCommandLineFlagInfo("raftSnapshotUri", &info) && !info.is_default) {
        conf->SetStringValue("copyset.raft_snapshot_uri",
                             FLAGS_raftSnapshotUri);
    }

    if (FLAGS_log_dir.empty()) {
        if (!conf->GetStringValue("metaserver.common.logDir", &FLAGS_log_dir)) {
            LOG(WARNING) << "no metaserver.common.logDir in " << FLAGS_confPath
                         << ", will log to /tmp";
        }
    }
}

int main(int argc, char **argv) {
    // config initialization
    google::ParseCommandLineFlags(&argc, &argv, false);

    ::curvefs::common::Process::InitSetProcTitle(argc, argv);
    butil::AtExitManager atExit;

    std::string confPath = FLAGS_confPath;
    auto conf = std::make_shared<Configuration>();
    conf->SetConfigPath(confPath);
    LOG_IF(FATAL, !conf->LoadConfig())
        << "load metaserver configuration fail, conf path = " << confPath;
    LoadConfigFromCmdline(conf.get());

    // initialize logging module
    google::InitGoogleLogging(argv[0]);

    conf->PrintConfig();

    curvefs::metaserver::Metaserver metaserver;
    // initialize metaserver options
    metaserver.InitOptions(conf);

    // Initialize other modules after winning election
    metaserver.Init();

    // start metaserver server and wait CTRL+C to quit
    metaserver.Run();

    // stop server and background threads
    metaserver.Stop();

    google::ShutdownGoogleLogging();
    return 0;
}
