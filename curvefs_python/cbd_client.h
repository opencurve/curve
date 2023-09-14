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

#ifndef CURVEFS_PYTHON_CBD_CLIENT_H_
#define CURVEFS_PYTHON_CBD_CLIENT_H_

#include <memory>
#include <string>
#include <vector>

#include "curvefs_python/curve_type.h"

namespace curve {
namespace client {

class FileClient;

}  // namespace client
}  // namespace curve

class CBDClient {
 public:
    CBDClient();
    ~CBDClient();

    int Init(const char* configPath);
    void UnInit();

    int Open(const char* filename, UserInfo_t* userInfo);
    int Close(int fd);

    int Create(const char* filename, UserInfo_t* userInfo, size_t size);
    int Create2(const CreateContext* context);
    int Unlink(const char* filename, UserInfo_t* info);
    int DeleteForce(const char* filename, UserInfo_t* info);
    int Recover(const char* filename, UserInfo_t* info, uint64_t fileId);
    int Rename(UserInfo_t* info, const char* oldpath, const char* newpath);
    int Extend(const char* filename, UserInfo_t* info, uint64_t size);

    // Synchronous read and write
    int Read(int fd, char* buf, unsigned long offset, unsigned long length);         // NOLINT
    int Write(int fd, const char* buf, unsigned long offset, unsigned long length);  // NOLINT

    // Asynchronous read and write
    int AioRead(int fd, AioContext* aioctx);
    int AioWrite(int fd, AioContext* aioctx);

    // Obtain basic information about the file
    int StatFile(const char* filename, UserInfo_t* info, FileInfo_t* finfo);
    int ChangeOwner(const char* filename, const char* owner, UserInfo_t* info);

    DirInfos_t* OpenDir(const char* dirpath, UserInfo_t* userinfo);
    int Listdir(DirInfos_t* dirinfo);
    void CloseDir(DirInfos_t* dirinfo);
    int Mkdir(const char* dirpath, UserInfo_t* info);
    int Rmdir(const char* dirpath, UserInfo_t* info);

    std::string GetClusterId();

    std::vector<std::string> ListPoolset();

 private:
    std::unique_ptr<curve::client::FileClient> client_;
};

#endif  // CURVEFS_PYTHON_CBD_CLIENT_H_
