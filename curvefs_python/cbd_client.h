/**
 * Project: curve
 * Date: Fri May 15 15:46:59 CST 2020
 * Author: wuhanqing
 * Copyright (c) 2020 NetEase
 */

#ifndef CURVEFS_PYTHON_CBD_CLIENT_H_
#define CURVEFS_PYTHON_CBD_CLIENT_H_

#include <memory>
#include <string>

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
    int Unlink(const char* filename, UserInfo_t* info);
    int DeleteForce(const char* filename, UserInfo_t* info);
    int Rename(UserInfo_t* info, const char* oldpath, const char* newpath);
    int Extend(const char* filename, UserInfo_t* info, uint64_t size);

    // 同步读写
    int Read(int fd, char* buf, unsigned long offset, unsigned long length);         // NOLINT
    int Write(int fd, const char* buf, unsigned long offset, unsigned long length);  // NOLINT

    // 异步读写
    int AioRead(int fd, AioContext* aioctx);
    int AioWrite(int fd, AioContext* aioctx);

    // 获取文件的基本信息
    int StatFile(const char* filename, UserInfo_t* info, FileInfo_t* finfo);
    int ChangeOwner(const char* filename, const char* owner, UserInfo_t* info);

    DirInfos_t* OpenDir(const char* dirpath, UserInfo_t* userinfo);
    int Listdir(DirInfos_t* dirinfo);
    void CloseDir(DirInfos_t* dirinfo);
    int Mkdir(const char* dirpath, UserInfo_t* info);
    int Rmdir(const char* dirpath, UserInfo_t* info);

    std::string GetClusterId();

 private:
    std::unique_ptr<curve::client::FileClient> client_;
};

#endif  // CURVEFS_PYTHON_CBD_CLIENT_H_
