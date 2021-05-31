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
#include <brpc/channel.h>
#include <brpc/server.h>
#include "curvefs/src/mds/mds.h"
#include "curvefs/src/mds/mds_service.h"

namespace curvefs {
namespace mds {
void Mds::InitOptions(std::shared_ptr<Configuration> conf) {
    conf_ = conf;
    conf_->GetValueFatalIfFail("mds.listen.addr", &options_.mdsListenAddr);
    conf_->GetValueFatalIfFail("space..addr",
                                &options_.spaceOptions.spaceAddr);
    conf_->GetValueFatalIfFail("space.rpcTimeoutMs",
                                &options_.spaceOptions.rpcTimeoutMs);
}

void Mds::Init() {
    LOG(INFO) << "Init MDS start";
    fsStorage_ = std::make_shared<MemoryFsStorage>();
    spaceClient_ = std::make_shared<SpaceClient>(options_.spaceOptions);
    fsManager_ = std::make_shared<FsManager>(fsStorage_, spaceClient_);
    LOG_IF(FATAL, spaceClient_->Init())
        << "spaceClient Init fail";
    inited_ = true;
    LOG(INFO) << "Init MDS success";
}

void Mds::Run() {
    LOG(INFO) << "Run MDS";
    if (!inited_) {
        LOG(ERROR) << "MDS not inited yet!";
        return;
    }

    brpc::Server server;
    // add heartbeat service
    MdsServiceImpl mdsService(fsManager_);
    LOG_IF(FATAL, server.AddService(&mdsService,
                          brpc::SERVER_DOESNT_OWN_SERVICE) != 0)
        << "add mdsService error";

    // start rpc server
    brpc::ServerOptions option;
    option.idle_timeout_sec = -1;
    LOG_IF(FATAL, server.Start(options_.mdsListenAddr.c_str(), &option) != 0)
        << "start brpc server error";
    running_ = true;

    // To achieve the graceful exit of SIGTERM, you need to specify parameters
    // when starting the process: --graceful_quit_on_sigterm
    server.RunUntilAskedToQuit();
}

void Mds::Stop() {
    LOG(INFO) << "Stop MDS";
    if (!running_) {
        LOG(WARNING) << "MDS is not running";
        return;
    }
    brpc::AskToQuit();
}
}  // namespace mds
}  // namespace curvefs
