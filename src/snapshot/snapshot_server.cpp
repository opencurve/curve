/*
 * Project: curve
 * Created Date: Mon Dec 17 2018
 * Author: xuchaojie
 * Copyright (c) 2018 netease
 */


#include "src/snapshot/snapshot_server.h"

#include "src/snapshot/snapshot_define.h"
#include "src/snapshot/snapshot_task_manager.h"
#include "src/snapshot/snapshot_core.h"

#include "src/snapshot/repo/repo.h"

#include "src/snapshot/snapshot_data_store.h"
#include "src/snapshot/snapshot_data_store_s3.h"
#include "src/snapshot/snapshot_meta_store.h"
#include "src/snapshot/curvefs_client.h"

namespace curve {
namespace snapshotserver {

int SnapshotServer::Init() {
    std::shared_ptr<UUIDGenerator> idGen =
        std::make_shared<UUIDGenerator>();
    std::shared_ptr<CurveFsClient> client =
        std::make_shared<CurveFsClientImpl>();
    if (client->Init() < 0) {
        LOG(ERROR) << "curvefs_client init fail.";
    }

    std::shared_ptr<RepoInterface> repo =
        std::make_shared<Repo>();

    std::shared_ptr<SnapshotMetaStore> metaStore =
        std::make_shared<DBSnapshotMetaStore>(repo);
    if (metaStore->Init() < 0) {
        LOG(ERROR) << "metaStore init fail.";
    }
    std::shared_ptr<SnapshotDataStore> dataStore =
        std::make_shared<S3SnapshotDataStore>();
    if (dataStore->Init() < 0) {
        LOG(ERROR) << "dataStore init fail.";
    }

    std::shared_ptr<SnapshotTaskManager> taskMgr =
        std::make_shared<SnapshotTaskManager>();
    std::shared_ptr<SnapshotCore> core =
        std::make_shared<SnapshotCoreImpl>(idGen,
            client,
            metaStore,
            dataStore);
    serviceManager_ = std::make_shared<SnapshotServiceManager>(taskMgr,
            core);
    if (serviceManager_->Init() < 0) {
        LOG(ERROR) << "serviceManager init fail.";
    }

    server_ = std::make_shared<brpc::Server>();
    service_ = std::make_shared<SnapshotServiceImpl>(serviceManager_);

    if (server_->AddService(service_.get(),
            brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        LOG(ERROR) << "Failed to add snapshot_service!\n";
            return kErrCodeSnapshotServerFail;
    }

    return kErrCodeSnapshotServerSuccess;
}

void SnapshotServer::RunUntilAskedToQuit() {
    while (!brpc::IsAskedToQuit()) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    Stop();
}

int SnapshotServer::Start() {
    serviceManager_->Start();
    serviceManager_->RecoverSnapshotTask();
    brpc::ServerOptions option;
    option.idle_timeout_sec = -1;
    if (server_->Start("127.0.0.1:5555", &option) != 0) {
        LOG(ERROR) << "snapshot server start fail.";
    }
    return 0;
}

int SnapshotServer::Stop() {
    server_->Stop(0);
    server_->Join();
    return serviceManager_->Stop();
}

}  // namespace snapshotserver
}  // namespace curve

