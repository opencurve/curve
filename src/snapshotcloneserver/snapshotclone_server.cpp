/*
 *  Copyright (c) 2020 NetEase Inc.
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
 * Created Date: Monday March 9th 2020
 * Author: hzsunjianliang
 */
#include <glog/logging.h>
#include <brpc/server.h>
#include <gflags/gflags.h>
#include <string>
#include <memory>

#include "src/common/snapshotclone/snapshotclone_define.h"
#include "src/snapshotcloneserver/snapshotclone_server.h"
#include "src/common/curve_version.h"

using LeaderElectionOptions = ::curve::election::LeaderElectionOptions;

namespace curve {
namespace snapshotcloneserver {

const char metricExposePrefix[] = "snapshotcloneserver";
const char configMetricName[] = "snapshotcloneserver_config";
const char statusMetricName[] = "snapshotcloneserver_status";
const char ACTIVE[] = "active";
const char STANDBY[] = "standby";

void InitClientOption(std::shared_ptr<Configuration> conf,
                      CurveClientOptions *clientOption) {
    conf->GetValueFatalIfFail("client.config_path",
                                        &clientOption->configPath);
    conf->GetValueFatalIfFail("mds.rootUser",
                                        &clientOption->mdsRootUser);
    conf->GetValueFatalIfFail("mds.rootPassword",
                                        &clientOption->mdsRootPassword);
    conf->GetValueFatalIfFail("client.methodRetryTimeSec",
        &clientOption->clientMethodRetryTimeSec);
    conf->GetValueFatalIfFail("client.methodRetryIntervalMs",
        &clientOption->clientMethodRetryIntervalMs);
}

void InitSnapshotCloneServerOptions(std::shared_ptr<Configuration> conf,
                                    SnapshotCloneServerOptions *serverOption) {
    conf->GetValueFatalIfFail("server.address",
                                        &serverOption->addr);
    conf->GetValueFatalIfFail("server.clientAsyncMethodRetryTimeSec",
        &serverOption->clientAsyncMethodRetryTimeSec);
    conf->GetValueFatalIfFail(
        "server.clientAsyncMethodRetryIntervalMs",
        &serverOption->clientAsyncMethodRetryIntervalMs);
    conf->GetValueFatalIfFail("server.snapshotPoolThreadNum",
                                     &serverOption->snapshotPoolThreadNum);
    conf->GetValueFatalIfFail(
               "server.snapshotTaskManagerScanIntervalMs",
               &serverOption->snapshotTaskManagerScanIntervalMs);
    conf->GetValueFatalIfFail("server.chunkSplitSize",
                                        &serverOption->chunkSplitSize);
    conf->GetValueFatalIfFail(
               "server.checkSnapshotStatusIntervalMs",
               &serverOption->checkSnapshotStatusIntervalMs);
    conf->GetValueFatalIfFail("server.maxSnapshotLimit",
                                        &serverOption->maxSnapshotLimit);
    conf->GetValueFatalIfFail("server.snapshotCoreThreadNum",
                                        &serverOption->snapshotCoreThreadNum);
    conf->GetValueFatalIfFail("server.mdsSessionTimeUs",
                                        &serverOption->mdsSessionTimeUs);
    conf->GetValueFatalIfFail("server.readChunkSnapshotConcurrency",
            &serverOption->readChunkSnapshotConcurrency);

    conf->GetValueFatalIfFail("server.stage1PoolThreadNum",
                                     &serverOption->stage1PoolThreadNum);
    conf->GetValueFatalIfFail("server.stage2PoolThreadNum",
                                     &serverOption->stage2PoolThreadNum);
    conf->GetValueFatalIfFail("server.commonPoolThreadNum",
                                     &serverOption->commonPoolThreadNum);

    conf->GetValueFatalIfFail(
               "server.cloneTaskManagerScanIntervalMs",
               &serverOption->cloneTaskManagerScanIntervalMs);
    conf->GetValueFatalIfFail("server.cloneChunkSplitSize",
                                        &serverOption->cloneChunkSplitSize);
    conf->GetValueFatalIfFail("server.cloneTempDir",
                                        &serverOption->cloneTempDir);
    conf->GetValueFatalIfFail("mds.rootUser",
                                        &serverOption->mdsRootUser);
    conf->GetValueFatalIfFail("server.createCloneChunkConcurrency",
                            &serverOption->createCloneChunkConcurrency);
    conf->GetValueFatalIfFail("server.recoverChunkConcurrency",
                            &serverOption->recoverChunkConcurrency);
    conf->GetValueFatalIfFail("server.backEndReferenceRecordScanIntervalMs",
                        &serverOption->backEndReferenceRecordScanIntervalMs);
    conf->GetValueFatalIfFail("server.backEndReferenceFuncScanIntervalMs",
                        &serverOption->backEndReferenceFuncScanIntervalMs);

    conf->GetValueFatalIfFail("etcd.retry.times",
                        &(serverOption->dlockOpts.retryTimes));
    conf->GetValueFatalIfFail("etcd.dlock.timeoutMs",
                        &(serverOption->dlockOpts.ctx_timeoutMS));
    conf->GetValueFatalIfFail("etcd.dlock.ttlSec",
                        &(serverOption->dlockOpts.ttlSec));
}

void InitEtcdConf(std::shared_ptr<Configuration> conf, EtcdConf* etcdConf) {
    std::string endpoint;
    conf->GetValueFatalIfFail("etcd.endpoint", &endpoint);
    char* etcdEndpoints_ = new char[endpoint.size()];
    etcdConf->Endpoints = etcdEndpoints_;
    std::memcpy(etcdConf->Endpoints, endpoint.c_str(), endpoint.size());
    etcdConf->len = endpoint.size();
    conf->GetValueFatalIfFail("etcd.dailtimeoutMs", &etcdConf->DialTimeout);
}

void SnapShotCloneServer::InitAllSnapshotCloneOptions(void) {
    InitClientOption(conf_, &(snapshotCloneServerOptions_.clientOptions));
    InitSnapshotCloneServerOptions(conf_,
        &(snapshotCloneServerOptions_.serverOption));
    InitEtcdConf(conf_, &(snapshotCloneServerOptions_.etcdConf));

    conf_->GetValueFatalIfFail("etcd.operation.timeoutMs",
        &(snapshotCloneServerOptions_.etcdClientTimeout));

    conf_->GetValueFatalIfFail("etcd.retry.times",
        &(snapshotCloneServerOptions_.etcdRetryTimes));

    conf_->GetValueFatalIfFail("server.dummy.listen.port",
        &(snapshotCloneServerOptions_.dummyPort));

    conf_->GetValueFatalIfFail("leader.campagin.prefix",
        &(snapshotCloneServerOptions_.campaginPrefix));

    conf_->GetValueFatalIfFail("leader.session.intersec",
        &(snapshotCloneServerOptions_.sessionInterSec));

    conf_->GetValueFatalIfFail("leader.election.timeoutms",
        &(snapshotCloneServerOptions_.electionTimeoutMs));

    conf_->GetValueFatalIfFail("s3.config_path",
        &(snapshotCloneServerOptions_.s3ConfPath));
}

void SnapShotCloneServer::StartDummy() {
    // Expose conf and version and role(standby or active)
    LOG(INFO) << "snapshotCloneServer version: "
        << curve::common::CurveVersion();
    curve::common::ExposeCurveVersion();
    conf_->ExposeMetric(configMetricName);
    status_.expose(statusMetricName);
    status_.set_value(STANDBY);

    int ret = brpc::StartDummyServerAt(snapshotCloneServerOptions_.dummyPort);
    if (ret != 0) {
        LOG(FATAL) << "StartDummyServer error";
    } else {
        LOG(INFO) << "StartDummyServer ok";
    }
}

bool SnapShotCloneServer::InitEtcdClient(void) {
    etcdClient_ = std::make_shared<EtcdClientImp>();
    auto res = etcdClient_->Init(snapshotCloneServerOptions_.etcdConf,
        snapshotCloneServerOptions_.etcdClientTimeout,
        snapshotCloneServerOptions_.etcdRetryTimes);
    if (res != EtcdErrCode::EtcdOK) {
        LOG(ERROR)
        << "init etcd client err! "
        << "etcdaddr: " << snapshotCloneServerOptions_.etcdConf.Endpoints
        << ", etcdaddr len: " << snapshotCloneServerOptions_.etcdConf.len
        << ", etcdtimeout: " << snapshotCloneServerOptions_.etcdConf.DialTimeout
        << ", operation timeout: "
            << snapshotCloneServerOptions_.etcdClientTimeout
        << ", etcd retrytimes: "
            << snapshotCloneServerOptions_.etcdRetryTimes;
        return false;
    }

    std::string out;
    res = etcdClient_->Get("test", &out);
    if (res != EtcdErrCode::EtcdOK && res != EtcdErrCode::EtcdKeyNotExist) {
        LOG(ERROR) <<
            "Run snapsthotcloneserver err. Check if etcd is running.";
        return false;
    }

    LOG(INFO) << "init etcd client ok! "
        << "etcdaddr: " << snapshotCloneServerOptions_.etcdConf.Endpoints
        << ", etcdaddr len: " << snapshotCloneServerOptions_.etcdConf.len
        << ", etcdtimeout: " <<
            snapshotCloneServerOptions_.etcdConf.DialTimeout
        << ", operation timeout: " <<
            snapshotCloneServerOptions_.etcdClientTimeout
        << ", etcd retrytimes: " <<
            snapshotCloneServerOptions_.etcdRetryTimes;
    return true;
}

void SnapShotCloneServer::StartCompaginLeader(void) {
    if (!InitEtcdClient()) {
        LOG(FATAL) << "InitEtcdClient error";
    }
    // init leader election options
    LeaderElectionOptions option;
    option.etcdCli = etcdClient_;
    option.leaderUniqueName = snapshotCloneServerOptions_.serverOption.addr;
    option.electionTimeoutMs = snapshotCloneServerOptions_.electionTimeoutMs;
    option.sessionInterSec = snapshotCloneServerOptions_.sessionInterSec;
    option.campaginPrefix = snapshotCloneServerOptions_.campaginPrefix;
    leaderElection_ = std::make_shared<LeaderElection>(option);

    // compagin leader and observe self then return
    while (0 != leaderElection_->CampaginLeader()) {
        LOG(INFO) << option.leaderUniqueName
                  << " campaign for leader again";
    }
    LOG(INFO) << "Campain leader ok, I am the active member now";
    status_.set_value(ACTIVE);
    leaderElection_->StartObserverLeader();
}

bool SnapShotCloneServer::Init() {
    snapClient_ = std::make_shared<SnapshotClient>();
    fileClient_ =  std::make_shared<FileClient>();
    client_ =  std::make_shared<CurveFsClientImpl>(snapClient_, fileClient_);

    if (client_->Init(snapshotCloneServerOptions_.clientOptions) < 0) {
        LOG(ERROR) << "curvefs_client init fail.";
        return false;
    }
    auto codec = std::make_shared<SnapshotCloneCodec>();

    metaStore_ = std::make_shared<SnapshotCloneMetaStoreEtcd>(etcdClient_,
        codec);
    if (metaStore_->Init() < 0) {
        LOG(ERROR) << "metaStore init fail.";
        return false;
    }

    dataStore_ = std::make_shared<S3SnapshotDataStore>();
    if (dataStore_->Init(snapshotCloneServerOptions_.s3ConfPath) < 0) {
        LOG(ERROR) << "dataStore init fail.";
        return false;
    }


    snapshotRef_ = std::make_shared<SnapshotReference>();
    snapshotMetric_ = std::make_shared<SnapshotMetric>(metaStore_);
    snapshotCore_ =  std::make_shared<SnapshotCoreImpl>(
                        client_,
                        metaStore_,
                        dataStore_,
                        snapshotRef_,
                        snapshotCloneServerOptions_.serverOption);
    if (snapshotCore_->Init() < 0) {
        LOG(ERROR) << "SnapshotCore init fail.";
        return false;
    }

    snapshotTaskManager_ = std::make_shared<SnapshotTaskManager>(snapshotCore_,
        snapshotMetric_);
    snapshotServiceManager_  =
        std::make_shared<SnapshotServiceManager>(snapshotTaskManager_,
        snapshotCore_);
    if (snapshotServiceManager_->Init(
            snapshotCloneServerOptions_.serverOption) < 0) {
        LOG(ERROR) << "SnapshotServiceManager init fail.";
        return false;
    }

    cloneMetric_ = std::make_shared<CloneMetric>();
    cloneRef_ = std::make_shared<CloneReference>();
    cloneCore_ = std::make_shared<CloneCoreImpl>(
                         client_,
                         metaStore_,
                         dataStore_,
                         snapshotRef_,
                         cloneRef_,
                         snapshotCloneServerOptions_.serverOption);
    if (cloneCore_->Init() < 0) {
        LOG(ERROR) << "CloneCore init fail.";
        return false;
    }
    cloneTaskMgr_ = std::make_shared<CloneTaskManager>(cloneCore_,
        cloneMetric_);

    cloneServiceManagerBackend_ =
            std::make_shared<CloneServiceManagerBackendImpl>(cloneCore_);
    cloneServiceManager_ = std::make_shared<CloneServiceManager>(
                cloneTaskMgr_,
                cloneCore_,
                cloneServiceManagerBackend_);
    if (cloneServiceManager_->Init(
            snapshotCloneServerOptions_.serverOption) < 0) {
        LOG(ERROR) << "CloneServiceManager init fail.";
        return false;
    }
    service_ = std::make_shared<SnapshotCloneServiceImpl>(
            snapshotServiceManager_,
            cloneServiceManager_);
    server_  = std::make_shared<brpc::Server>();
    if (server_->AddService(service_.get(),
                            brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        LOG(ERROR) << "Failed to add snapshot_service!\n";
        return false;
    }
    return true;
}

bool SnapShotCloneServer::Start(void) {
    // 先启动clone服务再启动snapshot服务，因为删除快照依赖是否有clone引用
    int ret = cloneServiceManager_->Start();
    if (ret < 0) {
        LOG(ERROR) << "cloneServiceManager start fail"
                   << ", ret = " << ret;
        return false;
    }
    ret = cloneServiceManager_->RecoverCloneTask();
    if (ret < 0) {
        LOG(ERROR) << "RecoverCloneTask fail"
                   << ", ret = " << ret;
        return false;
    }
    ret = snapshotServiceManager_->Start();
    if (ret < 0) {
        LOG(ERROR) << "snapshotServiceManager start fail"
                   << ", ret = " << ret;
        return false;
    }
    ret = snapshotServiceManager_->RecoverSnapshotTask();
    if (ret < 0) {
        LOG(ERROR) << "RecoverSnapshotTask fail"
                   << ", ret = " << ret;
        return false;
    }

    brpc::ServerOptions option;
    option.idle_timeout_sec = -1;
    if (server_->Start(snapshotCloneServerOptions_.serverOption.addr.c_str(),
        &option) != 0) {
        LOG(FATAL) << "snapshotclone rpc server start fail.";
    }
    LOG(INFO) << "snapshotclone service start ok ...";
    return true;
}

void SnapShotCloneServer::RunUntilQuit(void) {
    server_->RunUntilAskedToQuit();
}

void SnapShotCloneServer::Stop(void) {
    LOG(INFO) << "snapshotcloneserver stopping ...";
    server_->Stop(0);
    server_->Join();
    snapshotServiceManager_->Stop();
    cloneServiceManager_->Stop();
    LOG(INFO) << "snapshorcloneserver stopped";
}

}  // namespace snapshotcloneserver
}  // namespace curve
