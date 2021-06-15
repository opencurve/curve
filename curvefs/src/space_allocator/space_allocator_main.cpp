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

#include <brpc/server.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include <memory>

#include "curvefs/src/space_allocator/space_alloc_service.h"
#include "curvefs/src/space_allocator/space_manager.h"

namespace curvefs {
namespace space {

DEFINE_string(listenAddr, "0.0.0.0:19999", "space allocator service address");

int Run(int argc, char* argv[]) {
    google::ParseCommandLineFlags(&argc, &argv, true);

    FLAGS_log_dir = "/tmp";
    google::InitGoogleLogging(argv[0]);

    std::unique_ptr<SpaceManager> spaceManager(
        new DefaultSpaceManager(SpaceManagerOption{}));
    SpaceAllocServiceImpl spaceAllocService(spaceManager.get());

    brpc::Server server;
    int rv =
        server.AddService(&spaceAllocService, brpc::SERVER_DOESNT_OWN_SERVICE);
    if (rv != 0) {
        LOG(ERROR) << "AddService failed";
        return -1;
    }

    butil::EndPoint listenPoint;
    rv = butil::str2endpoint(FLAGS_listenAddr.c_str(), &listenPoint);
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

}  // namespace space
}  // namespace curvefs

int main(int argc, char* argv[]) {
    return curvefs::space::Run(argc, argv);
}
