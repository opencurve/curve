/*
 * Project: curve
 * Created Date: Mon Dec 23 2019
 * Author: xuchaojie
 * Copyright (c) 2019 netease
 */

#include <memory>

#include "test/integration/snapshotcloneserver/snapshotcloneserver_module.h"


namespace curve {
namespace snapshotcloneserver {

int SnapshotCloneServerModule::Start(
    const SnapshotCloneServerOptions &option) {
    serverOption_ = option;

    client_ = std::make_shared<FakeCurveFsClient>();
    CurveClientOptions cop;
    client_->Init(cop);

    metaStore_ = std::make_shared<FakeSnapshotCloneMetaStore>();
    dataStore_ = std::make_shared<FakeSnapshotDataStore>();

    auto snapshotRef_ = std::make_shared<SnapshotReference>();

    auto snapshotMetric = std::make_shared<SnapshotMetric>(metaStore_);

    auto cloneRef_ = std::make_shared<CloneReference>();

    auto taskMgr = std::make_shared<SnapshotTaskManager>(snapshotMetric);

    auto core =
        std::make_shared<SnapshotCoreImpl>(
            client_,
            metaStore_,
            dataStore_,
            snapshotRef_,
            serverOption_);

    if (core->Init() < 0) {
        LOG(ERROR) << "SnapshotCore init fail.";
        return kErrCodeServerInitFail;
    }

    snapshotServiceManager_ =
        std::make_shared<SnapshotServiceManager>(taskMgr,
                core);

    if (snapshotServiceManager_->Init(serverOption_) < 0) {
        LOG(ERROR) << "SnapshotServiceManager init fail.";
        return kErrCodeServerInitFail;
    }

    auto cloneMetric = std::make_shared<CloneMetric>();

    std::shared_ptr<CloneTaskManager> cloneTaskMgr =
        std::make_shared<CloneTaskManager>(cloneMetric);

    auto cloneCore = std::make_shared<CloneCoreImpl>(
                         client_,
                         metaStore_,
                         dataStore_,
                         snapshotRef_,
                         cloneRef_,
                         serverOption_);
    if (cloneCore->Init() < 0) {
        LOG(ERROR) << "CloneCore init fail.";
        return kErrCodeServerInitFail;
    }
    cloneServiceManager_ =
        std::make_shared<CloneServiceManager>(cloneTaskMgr,
                cloneCore);
    if (cloneServiceManager_->Init(serverOption_) < 0) {
        LOG(ERROR) << "CloneServiceManager init fail.";
        return kErrCodeServerInitFail;
    }
    server_ = std::make_shared<brpc::Server>();
    service_ =
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

    brpc::ServerOptions severOption;
    severOption.idle_timeout_sec = -1;
    if (server_->Start(serverOption_.addr.c_str(), &severOption) != 0) {
        LOG(ERROR) << "snapshotclone server start fail.";
        return kErrCodeServerStartFail;
    }
    return 0;
}

void SnapshotCloneServerModule::Stop() {
    server_->Stop(0);
    server_->Join();
    snapshotServiceManager_->Stop();
    cloneServiceManager_->Stop();
}

}  // namespace snapshotcloneserver
}  // namespace curve
