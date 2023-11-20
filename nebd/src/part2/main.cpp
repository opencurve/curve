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
 * Project: nebd
 * File Created: 2019-09-30
 * Author: hzwuhongsong
 */

#include <glog/logging.h>
#include <stdlib.h>
#include <unistd.h>

#include "nebd/src/part2/nebd_server.h"
#include "src/common/log_util.h"

DEFINE_string(confPath, "/etc/nebd/nebd-server.conf", "nebd server conf path");

int main(int argc, char* argv[]) {
    // Parsing parameters
    google::ParseCommandLineFlags(&argc, &argv, false);
    curve::common::DisableLoggingToStdErr();
    google::InitGoogleLogging(argv[0]);
    std::string confPath = FLAGS_confPath.c_str();

    // Start nebd server
    auto server = std::make_shared<::nebd::server::NebdServer>();
    int initRes = server->Init(confPath);
    if (initRes < 0) {
        LOG(ERROR) << "init nebd server fail";
        return -1;
    }
    server->RunUntilAskedToQuit();

    // Stop nebd server
    server->Fini();

    google::ShutdownGoogleLogging();
    return 0;
}
