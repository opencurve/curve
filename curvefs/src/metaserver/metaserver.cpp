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
#include "curvefs/src/metaserver/trash_manager.h"
#include "src/common/s3_adapter.h"

namespace curvefs {
namespace metaserver {
void Metaserver::InitOptions(std::shared_ptr<Configuration> conf) {
    conf_ = conf;
    conf_->GetValueFatalIfFail("metaserver.listen.addr",
                               &options_.metaserverListenAddr);
}

void InitS3Option(const std::shared_ptr<Configuration> &conf,
    S3ClientAdaptorOption *s3Opt) {
    LOG_IF(FATAL, !conf->GetUInt64Value("s3.blocksize", &s3Opt->blockSize));
    LOG_IF(FATAL, !conf->GetUInt64Value("s3.chunksize", &s3Opt->chunkSize));
}

void Metaserver::Init() {
    TrashOption  trashOption;
    trashOption.InitTrashOptionFromConf(conf_);
    s3Adaptor_ = std::make_shared<S3ClientAdaptorImpl>();

    S3ClientAdaptorOption s3ClientAdaptorOption;
    InitS3Option(conf_, &s3ClientAdaptorOption);
    curve::common::S3AdapterOption s3AdaptorOption;
    ::curve::common::InitS3AdaptorOption(conf_.get(), &s3AdaptorOption);
    auto s3Client_ = new S3ClientImpl;
    s3Client_->Init(s3AdaptorOption);
    // s3Adaptor_ own the s3Client_, and will delete it when destruct.
    s3Adaptor_->Init(s3ClientAdaptorOption, s3Client_);
    trashOption.s3Adaptor = s3Adaptor_;

    inodeStorage_ = std::make_shared<MemoryInodeStorage>();
    dentryStorage_ = std::make_shared<MemoryDentryStorage>();

    trash_ = std::make_shared<TrashImpl>(inodeStorage_);
    trash_->Init(trashOption);
    inodeManager_ = std::make_shared<InodeManager>(inodeStorage_, trash_);
    dentryManager_ = std::make_shared<DentryManager>(dentryStorage_);

    TrashManager::GetInstance().Init(trashOption);
    inited_ = true;
}

void Metaserver::Run() {
    if (!inited_) {
        LOG(ERROR) << "Metaserver not inited yet!";
        return;
    }

    TrashManager::GetInstance().Run();

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
    TrashManager::GetInstance().Fini();
    brpc::AskToQuit();
}
}  // namespace metaserver
}  // namespace curvefs
