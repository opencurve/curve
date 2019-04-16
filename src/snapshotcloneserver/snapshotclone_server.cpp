/*
 * Project: curve
 * Created Date: Mon Dec 17 2018
 * Author: xuchaojie
 * Copyright (c) 2018 netease
 */


#include "src/snapshotcloneserver/snapshotclone_server.h"

#include "src/snapshotcloneserver/common/define.h"
#include "src/snapshotcloneserver/common/curvefs_client.h"

#include "src/snapshotcloneserver/dao/snapshotcloneRepo.h"

#include "src/snapshotcloneserver/snapshot/snapshot_data_store.h"
#include "src/snapshotcloneserver/snapshot/snapshot_data_store_s3.h"
#include "src/snapshotcloneserver/common/snapshotclone_meta_store.h"
#include "src/snapshotcloneserver/snapshot/snapshot_task_manager.h"
#include "src/snapshotcloneserver/snapshot/snapshot_core.h"

namespace curve {
namespace snapshotcloneserver {

int SnapshotCloneServer::Init() {
    std::shared_ptr<CurveFsClient> client =
        std::make_shared<CurveFsClientImpl>();
    if (client->Init() < 0) {
        LOG(ERROR) << "curvefs_client init fail.";
        return kErrCodeSnapshotServerInitFail;
    }

    std::shared_ptr<SnapshotCloneRepo> repo =
        std::make_shared<SnapshotCloneRepo>();

    std::shared_ptr<SnapshotCloneMetaStore> metaStore =
        std::make_shared<DBSnapshotCloneMetaStore>(repo);
    if (metaStore->Init() < 0) {
        LOG(ERROR) << "metaStore init fail.";
        return kErrCodeSnapshotServerInitFail;
    }
    std::shared_ptr<SnapshotDataStore> dataStore =
        std::make_shared<S3SnapshotDataStore>();
    if (dataStore->Init() < 0) {
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
    // TODO(xuchaojie): ip Port use config instead.
    if (server_->Start("127.0.0.1:5555", &option) != 0) {
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


