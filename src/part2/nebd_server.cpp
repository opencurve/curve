/*
 * Project: nebd
 * Created Date: 2020-01-16
 * Author: lixiaocui
 * Copyright (c) 2020 netease
 */

#include <glog/logging.h>
#include <memory>
#include "src/part2/nebd_server.h"
#include "src/part2/file_service.h"
#include "src/part2/heartbeat_service.h"

namespace nebd {
namespace server {
int NebdServer::Init(const std::string &confPath) {
    if (isRunning_) {
        LOG(WARNING) << "NebdServer is inited";
        return -1;
    }

    bool loadConf = LoadConfFromFile(confPath);
    if (false == loadConf) {
        LOG(ERROR) << "NebdServer load config from file fail";
        return -1;
    }

    bool initAddressOk = conf_.GetStringValue(LISTENADDRESS, &listenAddress_);
    if (false == initAddressOk) {
        LOG(ERROR) << "NebdServer init socket file address fail";
        return -1;
    }

    bool initFileManagerOk = InitFileManager();
    if (false == initFileManagerOk) {
        LOG(ERROR) << "NebdServer init fileManager fail";
        return -1;
    }

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
    return 0;
}

bool NebdServer::LoadConfFromFile(const std::string &confPath) {
    conf_.SetConfigPath(confPath);
    return conf_.LoadConfig();
}

bool NebdServer::InitFileManager() {
    fileManager_ = std::make_shared<NebdFileManager>();

    NebdFileManagerOption opt;
    bool initOptionOk = InitNebdFileManagerOption(&opt);
    if (false == initOptionOk) {
        LOG(ERROR) << "NebdServer init NebdFileManagerOption fail";
        return false;
    }

    int initRes = fileManager_->Init(opt);
    if (0 != initRes) {
         LOG(ERROR) << "Init nebd file server fail";
         return false;
    }

    int runRes = fileManager_->Run();
    if (0 != runRes) {
        LOG(ERROR) << "nebd file manager run fail";
        return false;
    }

    return true;
}

bool NebdServer::InitNebdFileManagerOption(NebdFileManagerOption *opt) {
    bool getOk = conf_.GetUInt32Value(
        HEARTBEATTIMEOUTSEC, &opt->heartbeatTimeoutS);
    if (false == getOk) {
        LOG(ERROR) << "NebdServer get heartbeat.timeout.sec fail";
        return false;
    }

    opt->metaFileManager = InitMetaFileManager();
    if (nullptr == opt->metaFileManager) {
        LOG(ERROR) << "NebdServer init meta file manager fail";
        return false;
    }

    return true;
}

MetaFileManagerPtr NebdServer::InitMetaFileManager() {
    std::string metaFilePath;
    bool getOk = conf_.GetStringValue(METAFILEPATH, &metaFilePath);
    if (false == getOk) {
        return nullptr;
    }

    return std::make_shared<NebdMetaFileManager>(metaFilePath);
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

    NebdHeartbeatServiceImpl heartbeatService(fileManager_);
    addFileServiceRes = server_.AddService(
        &heartbeatService, brpc::SERVER_DOESNT_OWN_SERVICE);
    if (0 != addFileServiceRes) {
        LOG(ERROR) << "NebdServer add heartbeat service fail";
        return false;
    }

    // start brcp server
    brpc::ServerOptions option;
    option.idle_timeout_sec = -1;
    int startBrpcServerRes = server_.Start(listenAddress_.c_str(), &option);
    if (0 != startBrpcServerRes) {
        LOG(ERROR) << "NebdServer start brpc server fail, res="
            << startBrpcServerRes;
        return false;
    }

    isRunning_ = true;
    server_.RunUntilAskedToQuit();

    isRunning_ = false;
    return true;
}

}  // namespace server
}  // namespace nebd

