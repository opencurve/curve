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
#include <gflags/gflags.h>
#include <glog/logging.h>
#include "absl/memory/memory.h"
#include "curvefs/src/metaserver/copyset/copyset_service.h"
#include "curvefs/src/metaserver/metaserver_service.h"
#include "curvefs/src/metaserver/register.h"
#include "curvefs/src/metaserver/s3compact_manager.h"
#include "curvefs/src/metaserver/trash_manager.h"
#include "src/common/s3_adapter.h"
#include "src/common/string_util.h"

namespace braft {

DECLARE_bool(raft_sync);
DECLARE_bool(raft_sync_meta);
DECLARE_bool(raft_sync_segments);
DECLARE_bool(raft_use_fsync_rather_than_fdatasync);
DECLARE_int32(raft_max_install_snapshot_tasks_num);

}  // namespace braft

namespace curvefs {
namespace metaserver {

using ::curve::fs::FileSystemType;
using ::curve::fs::LocalFsFactory;
using ::curve::fs::LocalFileSystemOption;

using ::curvefs::metaserver::copyset::ApplyQueueOption;
using ::curvefs::metaserver::copyset::CopysetTrashOptions;

void Metaserver::InitOptions(std::shared_ptr<Configuration> conf) {
    conf_ = conf;
    conf_->GetValueFatalIfFail("global.ip", &options_.ip);
    conf_->GetValueFatalIfFail("global.port", &options_.port);

    std::string value;
    conf_->GetValueFatalIfFail("bthread.worker_count", &value);
    if (value == "auto") {
        options_.bthreadWorkerCount = -1;
    } else if (!curve::common::StringToInt(value,
                                           &options_.bthreadWorkerCount)) {
        LOG(WARNING)
            << "Parse bthread.worker_count to int failed, string value: "
            << value;
    }

    InitBRaftFlags(conf);
}

void Metaserver::InitResgiterOptions() {
    conf_->GetValueFatalIfFail("mds.listen.addr",
                               &registerOptions_.mdsListenAddr);
    conf_->GetValueFatalIfFail("global.ip",
                               &registerOptions_.metaserverInternalIp);
    conf_->GetValueFatalIfFail("global.external_ip",
                               &registerOptions_.metaserverExternalIp);
    conf_->GetValueFatalIfFail("global.port", &registerOptions_.metaserverPort);
    conf_->GetValueFatalIfFail("mds.register_retries",
                               &registerOptions_.registerRetries);
    conf_->GetValueFatalIfFail("mds.register_timeoutMs",
                               &registerOptions_.registerTimeout);
}

void Metaserver::InitLocalFileSystem() {
    LocalFileSystemOption option;

    localFileSystem_ = LocalFsFactory::CreateFs(FileSystemType::EXT4, "");
    LOG_IF(FATAL, 0 != localFileSystem_->Init(option))
        << "Failed to initialize local filesystem";
}

void InitS3Option(const std::shared_ptr<Configuration>& conf,
                  S3ClientAdaptorOption* s3Opt) {
    LOG_IF(FATAL, !conf->GetUInt64Value("s3.blocksize", &s3Opt->blockSize));
    LOG_IF(FATAL, !conf->GetUInt64Value("s3.chunksize", &s3Opt->chunkSize));
}

void Metaserver::Init() {
    InitResgiterOptions();
    TrashOption trashOption;
    trashOption.InitTrashOptionFromConf(conf_);
    s3Adaptor_ = std::make_shared<S3ClientAdaptorImpl>();

    S3ClientAdaptorOption s3ClientAdaptorOption;
    InitS3Option(conf_, &s3ClientAdaptorOption);
    curve::common::S3AdapterOption s3AdaptorOption;
    ::curve::common::InitS3AdaptorOption(conf_.get(), &s3AdaptorOption);
    auto s3Client_ = new S3ClientImpl;
    s3Client_->SetAdaptor(std::make_shared<curve::common::S3Adapter>());
    s3Client_->Init(s3AdaptorOption);
    // s3Adaptor_ own the s3Client_, and will delete it when destruct.
    s3Adaptor_->Init(s3ClientAdaptorOption, s3Client_);
    trashOption.s3Adaptor = s3Adaptor_;
    TrashManager::GetInstance().Init(trashOption);

    // NOTE: Do not arbitrarily adjust the order, there are dependencies
    //       between different modules
    InitLocalFileSystem();
    InitCopysetTrash();
    InitCopysetNodeManager();
    InitHeartbeat();
    InitInflightThrottle();

    S3CompactManager::GetInstance().Init(conf_);
    inited_ = true;
}

void Metaserver::Run() {
    if (!inited_) {
        LOG(ERROR) << "Metaserver not inited yet!";
        return;
    }

    TrashManager::GetInstance().Run();

    // start heartbeat
    LOG_IF(FATAL, heartbeat_.Run() != 0)
        << "Failed to start heartbeat manager.";

    brpc::Server server;
    butil::ip_t ip;
    LOG_IF(FATAL, 0 != butil::str2ip(options_.ip.c_str(), &ip))
        << "convert " << options_.ip << " to ip failed";
    butil::EndPoint listenAddr(ip, options_.port);

    server_ = absl::make_unique<brpc::Server>();
    metaService_ = absl::make_unique<MetaServerServiceImpl>(
        copysetNodeManager_, inflightThrottle_.get());
    copysetService_ =
        absl::make_unique<CopysetServiceImpl>(copysetNodeManager_);

    // add metaserver service
    LOG_IF(FATAL, server_->AddService(metaService_.get(),
                                      brpc::SERVER_DOESNT_OWN_SERVICE) != 0)
        << "add metaserverService error";

    LOG_IF(FATAL, server_->AddService(copysetService_.get(),
                                      brpc::SERVER_DOESNT_OWN_SERVICE) != 0)
        << "add copysetservice error";

    // add raft-related service
    copysetNodeManager_->AddService(server_.get(), listenAddr);

    // start rpc server
    brpc::ServerOptions option;
    if (options_.bthreadWorkerCount != -1) {
        option.num_threads = options_.bthreadWorkerCount;
    }
    LOG_IF(FATAL, server_->Start(listenAddr, &option) != 0)
        << "start brpc server error";

    // try start s3compact wq
    LOG_IF(FATAL, S3CompactManager::GetInstance().Run() != 0);
    running_ = true;

    // start copyset node manager
    LOG_IF(FATAL, !copysetNodeManager_->Start())
        << "Failed to start copyset node manager";

    // To achieve the graceful exit of SIGTERM, you need to specify parameters
    // when starting the process: --graceful_quit_on_sigterm
    server_->RunUntilAskedToQuit();
}

void Metaserver::Stop() {
    if (!running_) {
        LOG(WARNING) << "Metaserver is not running";
        return;
    }

    LOG(INFO) << "MetaServer is going to quit";

    server_->Stop(0);
    server_->Join();

    LOG_IF(ERROR, heartbeat_.Fini() != 0);

    TrashManager::GetInstance().Fini();
    LOG_IF(ERROR, !copysetTrash_->Stop()) << "Failed to stop copyset trash";
    LOG_IF(ERROR, !copysetNodeManager_->Stop())
        << "Failed to stop copyset node manager";

    S3CompactManager::GetInstance().Stop();
    LOG(INFO) << "MetaServer stopped success";
}

void Metaserver::InitHeartbeatOptions() {
    LOG_IF(FATAL, !conf_->GetStringValue("copyset.data_uri",
                                         &heartbeatOptions_.storeUri));
    LOG_IF(FATAL, !conf_->GetStringValue("global.ip", &heartbeatOptions_.ip));
    LOG_IF(FATAL,
           !conf_->GetUInt32Value("global.port", &heartbeatOptions_.port));
    LOG_IF(FATAL, !conf_->GetStringValue("mds.listen.addr",
                                         &heartbeatOptions_.mdsListenAddr));
    LOG_IF(FATAL, !conf_->GetUInt32Value("mds.heartbeat_intervalSec",
                                         &heartbeatOptions_.intervalSec));
    LOG_IF(FATAL, !conf_->GetUInt32Value("mds.heartbeat_timeoutMs",
                                         &heartbeatOptions_.timeout));
}

void Metaserver::InitHeartbeat() {
    InitHeartbeatOptions();

    // register metaserver to mds, get metaserver id and token
    Register registerMDS(registerOptions_);
    LOG(INFO) << "register metaserver to mds";
    LOG_IF(FATAL, registerMDS.RegisterToMDS(&metadate_) != 0)
        << "Failed to register metaserver to MDS.";

    heartbeatOptions_.copysetNodeManager = copysetNodeManager_;
    heartbeatOptions_.metaserverId = metadate_.id();
    heartbeatOptions_.metaserverToken = metadate_.token();
    heartbeatOptions_.fs = localFileSystem_;
    LOG_IF(FATAL, heartbeat_.Init(heartbeatOptions_) != 0)
        << "Failed to init Heartbeat manager.";
}

void Metaserver::InitCopysetNodeManager() {
    InitCopysetNodeOptions();

    copysetNodeManager_ = &CopysetNodeManager::GetInstance();
    LOG_IF(FATAL, !copysetNodeManager_->Init(copysetNodeOptions_))
        << "Failed to initialize CopysetNodeManager";
}

void Metaserver::InitCopysetNodeOptions() {
    LOG_IF(FATAL, !conf_->GetStringValue("global.ip", &copysetNodeOptions_.ip));
    LOG_IF(FATAL,
           !conf_->GetUInt32Value("global.port", &copysetNodeOptions_.port));

    LOG_IF(FATAL,
           copysetNodeOptions_.port <= 0 || copysetNodeOptions_.port >= 65535)
        << "Invalid server port: " << copysetNodeOptions_.port;

    LOG_IF(FATAL, !conf_->GetStringValue("copyset.data_uri",
                                         &copysetNodeOptions_.dataUri));
    LOG_IF(FATAL,
           !conf_->GetIntValue(
               "copyset.election_timeout_ms",
               &copysetNodeOptions_.raftNodeOptions.election_timeout_ms));
    LOG_IF(FATAL,
           !conf_->GetIntValue(
               "copyset.snapshot_interval_s",
               &copysetNodeOptions_.raftNodeOptions.snapshot_interval_s));
    LOG_IF(FATAL, !conf_->GetIntValue(
                      "copyset.catchup_margin",
                      &copysetNodeOptions_.raftNodeOptions.catchup_margin));
    LOG_IF(FATAL, !conf_->GetStringValue(
                      "copyset.raft_log_uri",
                      &copysetNodeOptions_.raftNodeOptions.log_uri));
    LOG_IF(FATAL, !conf_->GetStringValue(
                      "copyset.raft_meta_uri",
                      &copysetNodeOptions_.raftNodeOptions.raft_meta_uri));
    LOG_IF(FATAL, !conf_->GetStringValue(
                      "copyset.raft_snapshot_uri",
                      &copysetNodeOptions_.raftNodeOptions.snapshot_uri));
    LOG_IF(FATAL, !conf_->GetUInt32Value("copyset.load_concurrency",
                                         &copysetNodeOptions_.loadConcurrency));
    LOG_IF(FATAL, !conf_->GetUInt32Value("copyset.check_retrytimes",
                                         &copysetNodeOptions_.checkRetryTimes));
    LOG_IF(FATAL,
           !conf_->GetUInt32Value("copyset.finishload_margin",
                                  &copysetNodeOptions_.finishLoadMargin));
    LOG_IF(FATAL, !conf_->GetUInt32Value(
                      "copyset.check_loadmargin_interval_ms",
                      &copysetNodeOptions_.checkLoadMarginIntervalMs));

    LOG_IF(FATAL, !conf_->GetUInt32Value(
                      "applyqueue.worker_count",
                      &copysetNodeOptions_.applyQueueOption.workerCount));
    LOG_IF(FATAL, !conf_->GetUInt32Value(
                      "applyqueue.queue_depth",
                      &copysetNodeOptions_.applyQueueOption.queueDepth));

    copysetNodeOptions_.localFileSystem = localFileSystem_.get();
    CHECK(copysetNodeOptions_.localFileSystem != nullptr);
}

void Metaserver::InitCopysetTrash() {
    CopysetTrashOptions options;
    LOG_IF(FATAL,
           !conf_->GetStringValue("copyset.trash.uri", &options.trashUri));
    LOG_IF(FATAL, !conf_->GetUInt32Value("copyset.trash.expired_aftersec",
                                         &options.expiredAfterSec));
    LOG_IF(FATAL, !conf_->GetUInt32Value("copyset.trash.scan_periodsec",
                                         &options.scanPeriodSec));
    options.localFileSystem = localFileSystem_.get();
    CHECK(options.localFileSystem != nullptr);

    copysetTrash_ = absl::make_unique<CopysetTrash>();
    LOG_IF(FATAL, !copysetTrash_->Init(options)) << "Failed to init trash";
}

void Metaserver::InitInflightThrottle() {
    uint64_t maxInflight = 0;
    LOG_IF(FATAL, !conf_->GetUInt64Value("service.max_inflight_request",
                                         &maxInflight));

    inflightThrottle_ = absl::make_unique<InflightThrottle>(maxInflight);
}

struct TakeValueFromConfIfCmdNotSet {
    template <typename T>
    void operator()(const std::shared_ptr<Configuration>& conf,
                    const std::string& cmdName, const std::string& confName,
                    T* value) {
        using ::google::CommandLineFlagInfo;
        using ::google::GetCommandLineFlagInfo;

        CommandLineFlagInfo info;
        if (GetCommandLineFlagInfo(cmdName.c_str(), &info) && info.is_default) {
            conf->GetValueFatalIfFail(confName, value);
        }
    }
};

void Metaserver::InitBRaftFlags(const std::shared_ptr<Configuration>& conf) {
    TakeValueFromConfIfCmdNotSet dummy;
    dummy(conf, "raft_sync", "braft.raft_sync", &braft::FLAGS_raft_sync);
    dummy(conf, "raft_sync_meta", "braft.raft_sync_meta",
          &braft::FLAGS_raft_sync_meta);
    dummy(conf, "raft_sync_segments", "braft.raft_sync_segments",
          &braft::FLAGS_raft_sync_segments);
    dummy(conf, "raft_use_fsync_rather_than_fdatasync",
          "braft.raft_use_fsync_rather_than_fdatasync",
          &braft::FLAGS_raft_use_fsync_rather_than_fdatasync);
    dummy(conf, "raft_max_install_snapshot_tasks_num",
          "braft.raft_max_install_snapshot_tasks_num",
          &braft::FLAGS_raft_max_install_snapshot_tasks_num);
}

}  // namespace metaserver
}  // namespace curvefs
