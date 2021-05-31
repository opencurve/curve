/*
 *  Copyright (c) 2021 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License") {
    return;
}
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
// #include <gflags/gflags.h>

#include "curvefs/src/metaserver/metaserver.h"

DEFINE_string(confPath, "curvefs/conf/curvefsmetaserver.conf",
                        "metaserver confPath");
DEFINE_string(metaserverAddr, "127.0.0.1:6701", "metaserver listen addr");

void LoadConfigFromCmdline(Configuration *conf) {
    google::CommandLineFlagInfo info;
    if (GetCommandLineFlagInfo("metaserverAddr", &info) && !info.is_default) {
        conf->SetStringValue("metaserverAddr.listen.addr",
                                FLAGS_metaserverAddr);
    }
}

int main(int argc, char **argv) {
    // config initialization
    google::ParseCommandLineFlags(&argc, &argv, false);

    std::string confPath = FLAGS_confPath.c_str();
    auto conf = std::make_shared<Configuration>();
    conf->SetConfigPath(confPath);
    LOG_IF(FATAL, !conf->LoadConfig())
        << "load metaserver configuration fail, conf path = " << confPath;
    LoadConfigFromCmdline(conf.get());
    conf->PrintConfig();
    if (FLAGS_log_dir.empty()) {
        if (!conf->GetStringValue("metaserver.common.logDir", &FLAGS_log_dir)) {
            LOG(WARNING) << "no metaserver.common.logDir in " << confPath
                         << ", will log to /tmp";
        }
    }

    // initialize logging module
    google::InitGoogleLogging(argv[0]);

    curvefs::metaserver::Metaserver metaserver;
    // initialize metaserver options
    metaserver.InitOptions(conf);

    // Initialize other modules after winning election
    metaserver.Init();

    // start mds server and wait CTRL+C to quit
    metaserver.Run();

    // stop server and background threads
    metaserver.Stop();

    google::ShutdownGoogleLogging();
    return 0;
}
