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
 * Date: Fri May 15 15:46:59 CST 2020
 * Author: wuhanqing
 */

#include "curvefs_python/cbd_client.h"

#include <vector>

#include "src/client/libcurve_file.h"

inline curve::client::UserInfo ToCurveClientUserInfo(UserInfo_t* userInfo) {
    return curve::client::UserInfo(userInfo->owner, userInfo->password);
}

CBDClient::CBDClient() : client_(new curve::client::FileClient()) {}

CBDClient::~CBDClient() {}

int CBDClient::Init(const char* configPath) {
    return client_->Init(configPath);
}

void CBDClient::UnInit() {
    return client_->UnInit();
}

int CBDClient::Open(const char* filename, UserInfo_t* userInfo) {
    return client_->Open(filename, ToCurveClientUserInfo(userInfo));
}

int CBDClient::Close(int fd) {
    return client_->Close(fd);
}

int CBDClient::Create(const char* filename, UserInfo_t* userInfo, size_t size) {
    return client_->Create(filename, ToCurveClientUserInfo(userInfo), size);
}

int CBDClient::Create2(const char* filename, UserInfo_t* userInfo, size_t size,
                                    uint64_t stripeUnit, uint64_t stripeCount) {
    return client_->Create2(filename, ToCurveClientUserInfo(userInfo),
                                                 size, stripeUnit, stripeCount);
}

int CBDClient::Unlink(const char* filename, UserInfo_t* userInfo) {
    return client_->Unlink(filename, ToCurveClientUserInfo(userInfo));
}

int CBDClient::DeleteForce(const char* filename, UserInfo_t* userInfo) {
    return client_->Unlink(filename, ToCurveClientUserInfo(userInfo), true);
}

int CBDClient::Rename(UserInfo_t* userInfo, const char* oldPath,
                      const char* newPath) {
    return client_->Rename(ToCurveClientUserInfo(userInfo), oldPath, newPath);
}

int CBDClient::Extend(const char* filename, UserInfo_t* userInfo,
                      uint64_t size) {
    return client_->Extend(filename, ToCurveClientUserInfo(userInfo), size);
}

int CBDClient::Read(int fd, char* buf, unsigned long offset, unsigned long length) {  // NOLINT
    return client_->Read(fd, buf, offset, length);
}

int CBDClient::Write(int fd, const char* buf, unsigned long offset, unsigned long length) {  // NOLINT
    return client_->Write(fd, buf, offset, length);
}

int CBDClient::AioRead(int fd, AioContext* aioctx) {
    return client_->AioRead(fd, reinterpret_cast<CurveAioContext*>(aioctx));
}

int CBDClient::AioWrite(int fd, AioContext* aioctx) {
    return client_->AioWrite(fd, reinterpret_cast<CurveAioContext*>(aioctx));
}

int CBDClient::StatFile(const char* filename, UserInfo_t* userInfo,
                        FileInfo_t* finfo) {
    return client_->StatFile(filename, ToCurveClientUserInfo(userInfo),
                             reinterpret_cast<FileStatInfo*>(finfo));
}

int CBDClient::ChangeOwner(const char* filename, const char* newOwner,
                           UserInfo_t* userInfo) {
    return client_->ChangeOwner(filename, newOwner,
                                ToCurveClientUserInfo(userInfo));
}

DirInfos_t* CBDClient::OpenDir(const char* dirpath, UserInfo_t* userInfo) {
    DirInfos_t* dirinfo = new (std::nothrow) DirInfos_t();
    dirinfo->dirpath = const_cast<char*>(dirpath);
    dirinfo->userinfo = userInfo;
    dirinfo->fileinfo = nullptr;

    return dirinfo;
}

int CBDClient::Listdir(DirInfos_t* dirinfo) {
    std::vector<FileStatInfo> fileInfos;
    int ret = client_->Listdir(
        dirinfo->dirpath, ToCurveClientUserInfo(dirinfo->userinfo), &fileInfos);

    if (ret != LIBCURVE_ERROR::OK) {
        return ret;
    }

    dirinfo->dirsize = fileInfos.size();
    dirinfo->fileinfo = new FileInfo[dirinfo->dirsize];

    for (uint64_t i = 0; i < dirinfo->dirsize; ++i) {
        dirinfo->fileinfo[i].id = fileInfos[i].id;
        dirinfo->fileinfo[i].parentid = fileInfos[i].parentid;
        dirinfo->fileinfo[i].filetype = fileInfos[i].filetype;
        dirinfo->fileinfo[i].length = fileInfos[i].length;
        dirinfo->fileinfo[i].ctime = fileInfos[i].ctime;

        memcpy(dirinfo->fileinfo[i].owner, fileInfos[i].owner, NAME_MAX_SIZE);
        memcpy(dirinfo->fileinfo[i].filename, fileInfos[i].filename,
               NAME_MAX_SIZE);
    }

    return ret;
}

void CBDClient::CloseDir(DirInfos_t* dirinfo) {
    if (dirinfo->fileinfo != nullptr) {
        delete[] dirinfo->fileinfo;
        dirinfo->fileinfo = nullptr;
    }

    delete dirinfo;
}

int CBDClient::Mkdir(const char* dirpath, UserInfo_t* userInfo) {
    return client_->Mkdir(dirpath, ToCurveClientUserInfo(userInfo));
}

int CBDClient::Rmdir(const char* dirpath, UserInfo_t* userInfo) {
    return client_->Rmdir(dirpath, ToCurveClientUserInfo(userInfo));
}

std::string CBDClient::GetClusterId() {
    return client_->GetClusterId();
}
