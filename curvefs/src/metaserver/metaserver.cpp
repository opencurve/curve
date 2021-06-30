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

#include "curvefs/src/metaserver/metaserver.h"
#include <brpc/channel.h>
#include <brpc/server.h>
#include <glog/logging.h>
#include "curvefs/src/metaserver/metaserver_service.h"

namespace curvefs {
namespace metaserver {
void Metaserver::InitOptions(std::shared_ptr<Configuration> conf) {
    conf_ = conf;
    conf_->GetValueFatalIfFail("metaserver.listen.addr",
                               &options_.metaserverListenAddr);
}
void Metaserver::Init() {
    inodeStorage_ = std::make_shared<MemoryInodeStorage>();
    dentryStorage_ = std::make_shared<MemoryDentryStorage>();
    inodeManager_ = std::make_shared<InodeManager>(inodeStorage_);
    dentryManager_ = std::make_shared<DentryManager>(dentryStorage_);
    inited_ = true;
}

void Metaserver::Run() {
    if (!inited_) {
        LOG(ERROR) << "Metaserver not inited yet!";
        return;
    }

    brpc::Server server;
    // add metaserver service
    MetaServerServiceImpl metaserverService(inodeManager_, dentryManager_);
    LOG_IF(FATAL, server.AddService(&metaserverService,
                                    brpc::SERVER_DOESNT_OWN_SERVICE) != 0)
        << "add metaserverService error";

    // start rpc server
    brpc::ServerOptions option;
    LOG_IF(FATAL,
           server.Start(options_.metaserverListenAddr.c_str(), &option) != 0)
        << "start brpc server error";
    running_ = true;

    // To achieve the graceful exit of SIGTERM, you need to specify parameters
    // when starting the process: --graceful_quit_on_sigterm
    server.RunUntilAskedToQuit();
}

void Metaserver::Stop() {
    if (!running_) {
        LOG(WARNING) << "Metaserver is not running";
        return;
    }
    brpc::AskToQuit();
}
}  // namespace metaserver
}  // namespace curvefs
