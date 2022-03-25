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

/**
 * Project: curve
 * File Created: 2020-02-04 15:37
 * Author: wuhanqing
 */

#include "include/client/libcurve.h"
#include "src/client/libcurve_file.h"
#include "src/client/source_reader.h"

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
                      const OpenFlags& openflags) {
    curve::client::UserInfo userInfo;
    std::string realFileName;
    bool ret = curve::client::ServiceHelper::GetUserInfoFromFilename(
        filename, &realFileName, &userInfo.owner);

    if (!ret) {
        LOG(ERROR) << "Get User Info from filename failed!";
        return -LIBCURVE_ERROR::FAILED;
    }

    return fileClient_->Open(realFileName, userInfo, openflags);
}

int CurveClient::ReOpen(const std::string& filename,
                        const OpenFlags& openflags) {
    return Open(filename, openflags);
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

    int rc = fileClient_->StatFile(realFileName, userInfo, &fileStatInfo);
    return rc == LIBCURVE_ERROR::OK ? fileStatInfo.length : rc;
}

int CurveClient::AioRead(int fd, CurveAioContext* aioctx,
                         UserDataType dataType) {
    return fileClient_->AioRead(fd, aioctx, dataType);
}

int CurveClient::AioWrite(int fd, CurveAioContext* aioctx,
                          UserDataType dataType) {
    return fileClient_->AioWrite(fd, aioctx, dataType);
}

int CurveClient::AioDiscard(int fd, CurveAioContext* aioctx) {
    return fileClient_->AioDiscard(fd, aioctx);
}

void CurveClient::SetFileClient(FileClient* client) {
    delete fileClient_;
    fileClient_ = client;
}

}  // namespace client
}  // namespace curve
