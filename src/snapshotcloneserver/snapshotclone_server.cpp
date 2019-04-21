/*
 * Project: curve
 * Created Date: Mon Dec 17 2018
 * Author: xuchaojie
 * Copyright (c) 2018 netease
 */

#include <gflags/gflags.h>

#include "src/snapshotcloneserver/snapshotclone_server.h"

#include "src/snapshotcloneserver/common/define.h"
#include "src/snapshotcloneserver/common/curvefs_client.h"

#include "src/snapshotcloneserver/dao/snapshotcloneRepo.h"

#include "src/snapshotcloneserver/snapshot/snapshot_data_store.h"
#include "src/snapshotcloneserver/snapshot/snapshot_data_store_s3.h"
#include "src/snapshotcloneserver/common/snapshotclone_meta_store.h"
#include "src/snapshotcloneserver/snapshot/snapshot_task_manager.h"
#include "src/snapshotcloneserver/snapshot/snapshot_core.h"
#include "src/snapshotcloneserver/common/config.h"

DEFINE_string(conf, "snapshot_clone_server.conf", "snapshot&clone server config file path");  //NOLINT

namespace curve {
namespace snapshotcloneserver {

int SnapshotCloneServer::Init() {
    LOG(INFO) << "Loading snapshot server configurations";
    conf_.SetConfigPath(FLAGS_conf);
    LOG_IF(FATAL, !conf_.LoadConfig())
              << "Failed to open config file: " << conf_.GetConfigPath();

    // init client options
    {
      clientOption_.mdsAddr = conf_.GetStringValue("client.mds_server_address");
      clientOption_.requestQueueCap =
          conf_.GetIntValue("client.request_queue_capacity");
      clientOption_.threadNum = conf_.GetIntValue("client.thread_number");
      clientOption_.requestMaxRetry =
          conf_.GetIntValue("client.request_max_retry");
      clientOption_.requestRetryIntervalUs =
          conf_.GetIntValue("client.request_retry_interval_us");
      clientOption_.getLeaderRetry =
          conf_.GetIntValue("client.get_leader_retry");
      clientOption_.enableApplyIndexRead =
          conf_.GetIntValue("client.enable_apply_index_read");
      clientOption_.ioSplitSize = conf_.GetIntValue("client.io_split_size");
      clientOption_.loglevel = conf_.GetIntValue("client.loglevel");
    }
    std::shared_ptr<CurveFsClient> client =
        std::make_shared<CurveFsClientImpl>();
    if (client->Init(clientOption_) < 0) {
        LOG(ERROR) << "curvefs_client init fail.";
        return kErrCodeSnapshotServerInitFail;
    }
    // init metastore options
    {
      metastoreOption_.dbName = conf_.GetStringValue("metastore.db_name");
      metastoreOption_.dbUser = conf_.GetStringValue("metastore.db_user");
      metastoreOption_.dbPassword =
          conf_.GetStringValue("metastore.db_passwd");
      metastoreOption_.dbAddr = conf_.GetStringValue("metastore.db_address");
    }
    std::shared_ptr<SnapshotCloneRepo> repo =
        std::make_shared<SnapshotCloneRepo>();

    std::shared_ptr<SnapshotCloneMetaStore> metaStore =
        std::make_shared<DBSnapshotCloneMetaStore>(repo);
    if (metaStore->Init(metastoreOption_) < 0) {
        LOG(ERROR) << "metaStore init fail.";
        return kErrCodeSnapshotServerInitFail;
    }
    std::string config = conf_.GetStringValue("s3.config_path");
    std::shared_ptr<SnapshotDataStore> dataStore =
        std::make_shared<S3SnapshotDataStore>();
    if (dataStore->Init(config) < 0) {
        LOG(ERROR) << "dataStore init fail.";
        return kErrCodeSnapshotServerInitFail;
    }

    std::shared_ptr<SnapshotTaskManager> taskMgr =
        std::make_shared<SnapshotTaskManager>();
    std::shared_ptr<SnapshotCore> core =
        std::make_shared<SnapshotCoreImpl>(
            client,
            metaStore,
            dataStore);
    snapshotServiceManager_ = std::make_shared<SnapshotServiceManager>(taskMgr,
            core);
    if (snapshotServiceManager_->Init() < 0) {
        LOG(ERROR) << "SnapshotServiceManager init fail.";
        return kErrCodeSnapshotServerInitFail;
    }

    std::shared_ptr<CloneTaskManager> cloneTaskMgr =
        std::make_shared<CloneTaskManager>();

    std::shared_ptr<CloneCore> cloneCore =
        std::make_shared<CloneCoreImpl>(
            client,
            metaStore,
            dataStore);

    cloneServiceManager_ = std::make_shared<CloneServiceManager>(cloneTaskMgr,
        cloneCore);
    if (cloneServiceManager_->Init() < 0) {
        LOG(ERROR) << "CloneServiceManager init fail.";
        return kErrCodeSnapshotServerInitFail;
    }

    server_ = std::make_shared<brpc::Server>();
    service_ = std::make_shared<SnapshotCloneServiceImpl>(
        snapshotServiceManager_,
        cloneServiceManager_);

    if (server_->AddService(service_.get(),
            brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        LOG(ERROR) << "Failed to add snapshot_service!\n";
        return kErrCodeSnapshotServerInitFail;
    }

    return kErrCodeSnapshotServerSuccess;
}

void SnapshotCloneServer::RunUntilAskedToQuit() {
    while (!brpc::IsAskedToQuit()) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    Stop();
}

int SnapshotCloneServer::Start() {
    int ret = snapshotServiceManager_->Start();
    if (ret < 0) {
        LOG(ERROR) << "snapshotServiceManager start fail"
                   << ", ret = " << ret;
        return ret;
    }
    ret = snapshotServiceManager_->RecoverSnapshotTask();
    if (ret < 0) {
        LOG(ERROR) << "RecoverSnapshotTask fail"
                   << ", ret = " << ret;
        return ret;
    }
    ret = cloneServiceManager_->Start();
    if (ret < 0) {
        LOG(ERROR) << "cloneServiceManager start fail"
                   << ", ret = " << ret;
        return ret;
    }
    ret = cloneServiceManager_->RecoverCloneTask();
    if (ret < 0) {
        LOG(ERROR) << "RecoverCloneTask fail"
                   << ", ret = " << ret;
        return ret;
    }

    brpc::ServerOptions option;
    option.idle_timeout_sec = -1;
    serverOption_.addr = conf_.GetStringValue("server.address");
    if (server_->Start(serverOption_.addr.c_str(), &option) != 0) {
        LOG(ERROR) << "snapshotclone server start fail.";
        return kErrCodeSnapshotServerStartFail;
    }
    return 0;
}

void SnapshotCloneServer::Stop() {
    server_->Stop(0);
    server_->Join();
    snapshotServiceManager_->Stop();
    cloneServiceManager_->Stop();
}

}  // namespace snapshotcloneserver
}  // namespace curve


