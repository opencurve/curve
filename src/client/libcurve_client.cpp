/**
 * Project: curve
 * File Created: 2020-02-04 15:37
 * Author: wuhanqing
 * Copyright (c) 2020 netease
 */

#include "include/client/libcurve.h"
#include "src/client/libcurve_file.h"

namespace curve {
namespace client {

CurveClient::CurveClient() : fileClient_(new FileClient()) {}

CurveClient::~CurveClient() {
    delete fileClient_;
    fileClient_ = nullptr;
}

int CurveClient::Init(const std::string& configPath) {
    return fileClient_->Init(configPath);
}

void CurveClient::UnInit() {
    return fileClient_->UnInit();
}

int CurveClient::Open(const std::string& filename,
                      std::string* sessionId) {
    curve::client::UserInfo userInfo;
    std::string realFileName;
    bool ret = curve::client::ServiceHelper::GetUserInfoFromFilename(
        filename, &realFileName, &userInfo.owner);

    if (!ret) {
        LOG(ERROR) << "Get User Info from filename failed!";
        return -LIBCURVE_ERROR::FAILED;
    }

    return fileClient_->Open(realFileName, userInfo, sessionId);
}

int CurveClient::ReOpen(const std::string& filename,
                        const std::string& sessionId,
                        std::string* newSessionId) {
    curve::client::UserInfo userInfo;
    std::string realFileName;
    bool ret = curve::client::ServiceHelper::GetUserInfoFromFilename(
        filename, &realFileName, &userInfo.owner);

    if (!ret) {
        LOG(ERROR) << "Get User Info from filename failed!";
        return -LIBCURVE_ERROR::FAILED;
    }

    return fileClient_->ReOpen(realFileName, sessionId,
                               userInfo, newSessionId);
}

int CurveClient::Close(int fd) {
    return fileClient_->Close(fd);
}

int CurveClient::Extend(const std::string& filename,
                        int64_t newsize) {
    curve::client::UserInfo userInfo;
    std::string realFileName;
    bool ret = curve::client::ServiceHelper::GetUserInfoFromFilename(
        filename, &realFileName, &userInfo.owner);

    if (!ret) {
        LOG(ERROR) << "Get User Info from filename failed!";
        return -LIBCURVE_ERROR::FAILED;
    }

    return fileClient_->Extend(realFileName, userInfo, newsize);
}

int64_t CurveClient::StatFile(const std::string& filename) {
    FileStatInfo fileStatInfo;
    curve::client::UserInfo userInfo;
    std::string realFileName;
    bool ret = curve::client::ServiceHelper::GetUserInfoFromFilename(
        filename, &realFileName, &userInfo.owner);

    if (!ret) {
        LOG(ERROR) << "Get User Info from filename failed!";
        return -LIBCURVE_ERROR::FAILED;
    }

    fileClient_->StatFile(realFileName, userInfo, &fileStatInfo);
    return fileStatInfo.length;
}

int CurveClient::AioRead(int fd, CurveAioContext* aioctx) {
    return fileClient_->AioRead(fd, aioctx);
}

int CurveClient::AioWrite(int fd, CurveAioContext* aioctx) {
    return fileClient_->AioWrite(fd, aioctx);
}

}  // namespace client
}  // namespace curve
