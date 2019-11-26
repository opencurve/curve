/*
 * Project: curve
 * Created Date: Fri Dec 14 2018
 * Author: xuchaojie
 * Copyright (c) 2018 netease
 */

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <brpc/channel.h>
#include <brpc/server.h>


#include "src/snapshotcloneserver/snapshotclone_service.h"
#include "src/snapshotcloneserver/snapshot/snapshot_service_manager.h"
#include "src/snapshotcloneserver/clone/clone_service_manager.h"
#include "src/common/configuration.h"

#include "src/snapshotcloneserver/common/define.h"
#include "src/snapshotcloneserver/common/curvefs_client.h"

#include "src/snapshotcloneserver/dao/snapshotcloneRepo.h"

#include "src/snapshotcloneserver/snapshot/snapshot_data_store.h"
#include "src/snapshotcloneserver/snapshot/snapshot_data_store_s3.h"
#include "src/snapshotcloneserver/common/snapshotclone_meta_store.h"
#include "src/snapshotcloneserver/snapshot/snapshot_task_manager.h"
#include "src/snapshotcloneserver/snapshot/snapshot_core.h"
#include "src/snapshotcloneserver/common/config.h"

DEFINE_string(conf, "conf/snapshot_clone_server.conf", "snapshot&clone server config file path");  //NOLINT

using ::curve::common::Configuration;

namespace curve {
namespace snapshotcloneserver {

void InitClientOption(Configuration *conf,
                      CurveClientOptions *clientOption) {
    LOG_IF(FATAL, !conf->GetStringValue("client.config_path",
                                        &clientOption->configPath));
    LOG_IF(FATAL, !conf->GetStringValue("mds.rootUser",
                                        &clientOption->mdsRootUser));
    LOG_IF(FATAL, !conf->GetStringValue("mds.rootPassword",
                                        &clientOption->mdsRootPassword));
}

void InitMetaStoreOptions(Configuration *conf,
                          SnapshotCloneMetaStoreOptions *metastoreOption_) {
    LOG_IF(FATAL, !conf->GetStringValue("metastore.db_name",
                                        &metastoreOption_->dbName));
    LOG_IF(FATAL, !conf->GetStringValue("metastore.db_user",
                                        &metastoreOption_->dbUser));
    LOG_IF(FATAL, !conf->GetStringValue("metastore.db_passwd",
                                        &metastoreOption_->dbPassword));
    LOG_IF(FATAL, !conf->GetStringValue("metastore.db_address",
                                        &metastoreOption_->dbAddr));
    LOG_IF(FATAL, !conf->GetUInt32Value("metastore.db_poolsize",
                                        &metastoreOption_->dbPoolSize));
}

void InitSnapshotCloneServerOptions(Configuration *conf,
                                    SnapshotCloneServerOptions *serverOption) {
    LOG_IF(FATAL, !conf->GetStringValue("server.address",
                                        &serverOption->addr));
    LOG_IF(FATAL, !conf->GetIntValue("server.snapshotPoolThreadNum",
                                     &serverOption->snapshotPoolThreadNum));
    LOG_IF(FATAL, !conf->GetUInt32Value(
               "server.snapshotTaskManagerScanIntervalMs",
               &serverOption->snapshotTaskManagerScanIntervalMs));
    LOG_IF(FATAL, !conf->GetUInt64Value("server.chunkSplitSize",
                                        &serverOption->chunkSplitSize));
    LOG_IF(FATAL, !conf->GetUInt32Value(
               "server.checkSnapshotStatusIntervalMs",
               &serverOption->checkSnapshotStatusIntervalMs));
    LOG_IF(FATAL, !conf->GetUInt32Value("server.maxSnapshotLimit",
                                        &serverOption->maxSnapshotLimit));
    LOG_IF(FATAL, !conf->GetIntValue("server.clonePoolThreadNum",
                                     &serverOption->clonePoolThreadNum));
    LOG_IF(FATAL, !conf->GetUInt32Value(
               "server.cloneTaskManagerScanIntervalMs",
               &serverOption->cloneTaskManagerScanIntervalMs));
    LOG_IF(FATAL, !conf->GetUInt64Value("server.cloneChunkSplitSize",
                                        &serverOption->cloneChunkSplitSize));
    LOG_IF(FATAL, !conf->GetStringValue("server.cloneTempDir",
                                        &serverOption->cloneTempDir));
    LOG_IF(FATAL, !conf->GetStringValue("mds.rootUser",
                                        &serverOption->mdsRootUser));
}

int snapshotcloneserver_main(int argc, char* argv[]) {
    google::ParseCommandLineFlags(&argc, &argv, true);

    Configuration conf_;

    LOG(INFO) << "Loading snapshot server configurations";

    conf_.SetConfigPath(FLAGS_conf);
    if (!conf_.LoadConfig()) {
        LOG(ERROR) << "Failed to open config file: " << conf_.GetConfigPath();
        return kErrCodeServerInitFail;
    }

    // init client options
    CurveClientOptions clientOption_;
    InitClientOption(&conf_, &clientOption_);

    std::shared_ptr<CurveFsClientImpl> client =
        std::make_shared<CurveFsClientImpl>();
    if (client->Init(clientOption_) < 0) {
        LOG(ERROR) << "curvefs_client init fail.";
        return kErrCodeServerInitFail;
    }
    // init metastore options
    SnapshotCloneMetaStoreOptions metastoreOption_;
    InitMetaStoreOptions(&conf_, &metastoreOption_);

    std::shared_ptr<SnapshotCloneRepo> repo =
        std::make_shared<SnapshotCloneRepo>();

    std::shared_ptr<SnapshotCloneMetaStore> metaStore =
        std::make_shared<DBSnapshotCloneMetaStore>(repo);
    if (metaStore->Init(metastoreOption_) < 0) {
        LOG(ERROR) << "metaStore init fail.";
        return kErrCodeServerInitFail;
    }

    std::string config = conf_.GetStringValue("s3.config_path");
    std::shared_ptr<SnapshotDataStore> dataStore =
        std::make_shared<S3SnapshotDataStore>();
    if (dataStore->Init(config) < 0) {
        LOG(ERROR) << "dataStore init fail.";
        return kErrCodeServerInitFail;
    }

    SnapshotCloneServerOptions serverOption_;
    InitSnapshotCloneServerOptions(&conf_, &serverOption_);

    std::shared_ptr<SnapshotReference> snapshotRef_ =
        std::make_shared<SnapshotReference>();

    std::shared_ptr<SnapshotTaskManager> taskMgr =
        std::make_shared<SnapshotTaskManager>();
    std::shared_ptr<SnapshotCore> core =
        std::make_shared<SnapshotCoreImpl>(
            client,
            metaStore,
            dataStore,
            snapshotRef_,
            serverOption_);
    std::shared_ptr<SnapshotServiceManager> snapshotServiceManager_ =
        std::make_shared<SnapshotServiceManager>(taskMgr,
                core);
    if (snapshotServiceManager_->Init(serverOption_) < 0) {
        LOG(ERROR) << "SnapshotServiceManager init fail.";
        return kErrCodeServerInitFail;
    }

    std::shared_ptr<CloneTaskManager> cloneTaskMgr =
        std::make_shared<CloneTaskManager>();

    auto cloneCore = std::make_shared<CloneCoreImpl>(
                         client,
                         metaStore,
                         dataStore,
                         snapshotRef_,
                         serverOption_);
    if (cloneCore->Init() < 0) {
        LOG(ERROR) << "CloneCore init fail.";
        return kErrCodeServerInitFail;
    }
    std::shared_ptr<CloneServiceManager> cloneServiceManager_ =
        std::make_shared<CloneServiceManager>(cloneTaskMgr,
                cloneCore);
    if (cloneServiceManager_->Init(serverOption_) < 0) {
        LOG(ERROR) << "CloneServiceManager init fail.";
        return kErrCodeServerInitFail;
    }
    std::shared_ptr<brpc::Server> server_ =
        std::make_shared<brpc::Server>();
    std::shared_ptr<SnapshotCloneServiceImpl> service_ =
        std::make_shared<SnapshotCloneServiceImpl>(
            snapshotServiceManager_,
            cloneServiceManager_);

    if (server_->AddService(service_.get(),
                            brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        LOG(ERROR) << "Failed to add snapshot_service!\n";
        return kErrCodeServerInitFail;
    }

    // 先启动clone服务再启动snapshot服务，因为删除快照依赖是否有clone引用
    int ret = cloneServiceManager_->Start();
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
    ret = snapshotServiceManager_->Start();
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

    brpc::ServerOptions option;
    option.idle_timeout_sec = -1;
    if (server_->Start(serverOption_.addr.c_str(), &option) != 0) {
        LOG(ERROR) << "snapshotclone server start fail.";
        return kErrCodeServerStartFail;
    }

    server_->RunUntilAskedToQuit();

    server_->Stop(0);
    server_->Join();
    snapshotServiceManager_->Stop();
    cloneServiceManager_->Stop();

    return kErrCodeSuccess;
}


}  // namespace snapshotcloneserver
}  // namespace curve


int main(int argc, char **argv) {
    curve::snapshotcloneserver::snapshotcloneserver_main(argc, argv);
}

