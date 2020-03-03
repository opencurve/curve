/*
 * Project: nebd
 * Created Date: 2020-01-16
 * Author: lixiaocui
 * Copyright (c) 2020 netease
 */

#include <glog/logging.h>
#include <memory>
#include "src/common/file_lock.h"
#include "src/part2/nebd_server.h"
#include "src/part2/file_service.h"
#include "src/part2/heartbeat_service.h"

namespace nebd {
namespace server {
int NebdServer::Init(const std::string &confPath,
    std::shared_ptr<CurveClient> curveClient) {
    if (isRunning_) {
        LOG(WARNING) << "NebdServer is inited";
        return -1;
    }

    bool loadConf = LoadConfFromFile(confPath);
    if (false == loadConf) {
        LOG(ERROR) << "NebdServer load config from file fail";
        return -1;
    }
    LOG(INFO) << "NebdServer load config from file ok";

    bool initAddressOk = conf_.GetStringValue(LISTENADDRESS, &listenAddress_);
    if (false == initAddressOk) {
        LOG(ERROR) << "NebdServer init socket file address fail";
        return -1;
    }
    LOG(INFO) << "NebdServer init socket file address ok";

    curveClient_ = curveClient;
    bool initExecutorOk = InitCurveRequestExecutor();
    if (false == initExecutorOk) {
        LOG(ERROR) << "NebdServer init curveRequestExecutor fail";
        return -1;
    }
    LOG(INFO) << "NebdServer init curveRequestExecutor ok";

    bool initFileManagerOk = InitFileManager();
    if (false == initFileManagerOk) {
        LOG(ERROR) << "NebdServer init fileManager fail";
        return -1;
    }
    LOG(INFO) << "NebdServer init fileManager ok";

    bool initHeartbeatManagerOk = InitHeartbeatManager();
    if (false == initHeartbeatManagerOk) {
        LOG(ERROR) << "NebdServer init heartbeatManager fail";
        return -1;
    }
    LOG(INFO) << "NebdServer init heartbeatManager ok";

    LOG(INFO) << "NebdServer init ok";
    return 0;
}

int NebdServer::RunUntilAskedToQuit() {
    if (false == StartServer()) {
        LOG(INFO) << "start server fail";
        return -1;
    }

    return 0;
}

int NebdServer::Fini() {
    if (isRunning_) {
        brpc::AskToQuit();
    }

    if (fileManager_ != nullptr) {
        fileManager_->Fini();
    }

    if (curveClient_ != nullptr) {
        curveClient_ ->UnInit();
    }

    if (heartbeatManager_ != nullptr) {
        heartbeatManager_->Fini();
    }

    return 0;
}

bool NebdServer::LoadConfFromFile(const std::string &confPath) {
    conf_.SetConfigPath(confPath);
    return conf_.LoadConfig();
}

bool NebdServer::InitFileManager() {
    MetaFileManagerPtr metaFileManager = InitMetaFileManager();
    if (nullptr == metaFileManager) {
        LOG(ERROR) << "NebdServer init meta file manager fail";
        return false;
    }

    fileManager_ = std::make_shared<NebdFileManager>(metaFileManager);
    CHECK(fileManager_ != nullptr) << "Init file manager failed.";

    int runRes = fileManager_->Run();
    if (0 != runRes) {
        LOG(ERROR) << "nebd file manager run fail";
        return false;
    }

    return true;
}

bool NebdServer::InitCurveRequestExecutor() {
    std::string confPath;
    bool getOk = conf_.GetStringValue(CURVECLIENTCONFPATH, &confPath);
    if (!getOk) {
        LOG(ERROR) << "get " << CURVECLIENTCONFPATH << " fail";
        return false;
    }

    int initRes = curveClient_->Init(confPath);
    if (initRes < 0) {
        LOG(ERROR) << "Init curve client fail";
        return false;
    }

    CurveRequestExecutor::GetInstance().Init(curveClient_);
    return true;
}

MetaFileManagerPtr NebdServer::InitMetaFileManager() {
    NebdMetaFileManagerOption option;
    option.wrapper = std::make_shared<PosixWrapper>();
    option.parser = std::make_shared<NebdMetaFileParser>();
    bool getOk = conf_.GetStringValue(METAFILEPATH, &option.metaFilePath);
    if (false == getOk) {
        return nullptr;
    }

    MetaFileManagerPtr metaFileManager =
        std::make_shared<NebdMetaFileManager>();
    CHECK(metaFileManager != nullptr) << "meta file manager is nullptr";
    int ret = metaFileManager->Init(option);
    if (ret != 0) {
        LOG(ERROR) << "Init meta file manager failed.";
        return nullptr;
    }
    return metaFileManager;
}

bool NebdServer::InitHeartbeatManagerOption(HeartbeatManagerOption *opt) {
    bool getOk = conf_.GetUInt32Value(
        HEARTBEATTIMEOUTSEC, &opt->heartbeatTimeoutS);
    if (false == getOk) {
        LOG(ERROR) << "NebdServer get heartbeat.timeout.sec fail";
        return false;
    }

    getOk = conf_.GetUInt32Value(
        HEARTBEATCHECKINTERVALMS, &opt->checkTimeoutIntervalMs);
    if (false == getOk) {
        LOG(ERROR) << "NebdServer get heartbeat.check.interval.ms fail";
        return false;
    }

    opt->fileManager = fileManager_;
    return true;
}

bool NebdServer::InitHeartbeatManager() {
    HeartbeatManagerOption option;
    bool initOptionSuccess = InitHeartbeatManagerOption(&option);
    if (!initOptionSuccess) {
        LOG(ERROR) << "NebdServer init heartbeat manager option fail";
        return false;
    }
    heartbeatManager_ = std::make_shared<HeartbeatManager>(option);
    CHECK(heartbeatManager_ != nullptr) << "Init heartbeat manager failed.";

    int runRes = heartbeatManager_->Run();
    if (0 != runRes) {
        LOG(ERROR) << "nebd heartbeat manager run fail";
        return false;
    }
    return true;
}

bool NebdServer::StartServer() {
    // add service
    NebdFileServiceImpl fileService(fileManager_);
    int addFileServiceRes = server_.AddService(
        &fileService, brpc::SERVER_DOESNT_OWN_SERVICE);
    if (0 != addFileServiceRes) {
        LOG(ERROR) << "NebdServer add file service fail";
        return false;
    }

    NebdHeartbeatServiceImpl heartbeatService(heartbeatManager_);
    addFileServiceRes = server_.AddService(
        &heartbeatService, brpc::SERVER_DOESNT_OWN_SERVICE);
    if (0 != addFileServiceRes) {
        LOG(ERROR) << "NebdServer add heartbeat service fail";
        return false;
    }

    // start brcp server
    brpc::ServerOptions option;
    option.idle_timeout_sec = -1;
    // 获取文件锁
    common::FileLock fileLock(listenAddress_ + ".lock");
    if (fileLock.AcquireFileLock() != 0) {
        LOG(ERROR) << "Address already in use";
        return -1;
    }
    int startBrpcServerRes = server_.StartAtSockFile(
                                    listenAddress_.c_str(), &option);
    if (0 != startBrpcServerRes) {
        LOG(ERROR) << "NebdServer start brpc server fail, res="
            << startBrpcServerRes;
        return false;
    }

    isRunning_ = true;
    server_.RunUntilAskedToQuit();

    isRunning_ = false;
    fileLock.ReleaseFileLock();
    return true;
}

}  // namespace server
}  // namespace nebd

