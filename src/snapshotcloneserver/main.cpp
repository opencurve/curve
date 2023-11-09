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
 * Created Date: Fri Dec 14 2018
 * Author: xuchaojie
 */
#include <glog/logging.h>
#include <gflags/gflags.h>
#include "src/snapshotcloneserver/snapshotclone_server.h"
#include "src/common/log_util.h"

DEFINE_string(conf, "conf/snapshot_clone_server.conf", "snapshot&clone server config file path");  //NOLINT
DEFINE_string(addr, "127.0.0.1:5555", "snapshotcloneserver address");

using Configuration = ::curve::common::Configuration;
using SnapShotCloneServer = ::curve::snapshotcloneserver::SnapShotCloneServer;

void LoadConfigFromCmdline(Configuration *conf) {
    // 如果命令行有设置, 命令行覆盖配置文件中的字段
    google::CommandLineFlagInfo info;
    if (GetCommandLineFlagInfo("addr", &info) && !info.is_default) {
        conf->SetStringValue("server.address", FLAGS_addr);
    }
    // 设置日志存放文件夹
    if (FLAGS_log_dir.empty()) {
        if (!conf->GetStringValue("log.dir", &FLAGS_log_dir)) {
            LOG(WARNING) << "no log.dir in " << FLAGS_conf
                         << ", will log to /tmp";
        }
    }
}

int snapshotcloneserver_main(std::shared_ptr<Configuration> conf) {
    auto snapshotCloneServer = std::make_shared<SnapShotCloneServer>(conf);

    snapshotCloneServer->InitAllSnapshotCloneOptions();

    snapshotCloneServer->StartDummy();

    snapshotCloneServer->StartCompaginLeader();

    if (!snapshotCloneServer->Init()) {
        LOG(FATAL) << "snapshotCloneServer init error";
        return -1;
    }

    if (!snapshotCloneServer->Start()) {
        LOG(FATAL) << "snapshotCloneServer Start error";
        return -1;
    }
    snapshotCloneServer->RunUntilQuit();
    snapshotCloneServer->Stop();
    LOG(INFO) << "snapshotCloneServer Stopped";
    return 0;
}

int main(int argc, char **argv) {
    google::ParseCommandLineFlags(&argc, &argv, true);
    std::shared_ptr<Configuration> conf = std::make_shared<Configuration>();
    conf->SetConfigPath(FLAGS_conf);
    if (!conf->LoadConfig()) {
        LOG(ERROR) << "Failed to open config file: "
        << conf->GetConfigPath();
        return -1;
    }
    LoadConfigFromCmdline(conf.get());
    conf->PrintConfig();
    conf->ExposeMetric("snapshot_clone_server_config");
    curve::common::DisableLoggingToStdErr();
    google::InitGoogleLogging(argv[0]);
    snapshotcloneserver_main(conf);
}

