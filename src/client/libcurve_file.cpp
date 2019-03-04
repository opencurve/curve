/*
 * Project: curve
 * File Created: Monday, 18th February 2019 11:20:01 am
 * Author: tongguangxun
 * Copyright (c)￼ 2018 netease
 */

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <mutex>    // NOLINT
#include "src/client/libcurve_file.h"
#include "src/client/client_common.h"
#include "src/client/client_config.h"
#include "include/client/libcurve_qemu.h"
#include "src/client/file_instance.h"
#include "include/curve_compiler_specific.h"
#include "src/client/iomanager4file.h"
using curve::common::ReadLockGuard;
using curve::common::WriteLockGuard;

bool globalclientinited_ = false;
curve::client::FileClient* globalclient = nullptr;
namespace curve {
namespace client {
FileClient::FileClient(): fdcount_(0) {
    fileserviceMap_.clear();
}

LIBCURVE_ERROR FileClient::Init(const char* configpath) {
    if (-1 == clientconfig_.Init(configpath)) {
        LOG(ERROR) << "config init failed!";
        return LIBCURVE_ERROR::FAILED;
    }
    google::SetCommandLineOption("minloglevel",
        std::to_string(clientconfig_.GetFileServiceOption().loglevel).c_str());
    return LIBCURVE_ERROR::OK;
}

void FileClient::UnInit() {
    WriteLockGuard lk(rwlock_);
    for (auto iter : fileserviceMap_) {
        iter.second->UnInitialize();
        delete iter.second;
    }
    fileserviceMap_.clear();
}

LIBCURVE_ERROR FileClient::StatFs(int fd,
                                    std::string filename,
                                    FileStatInfo* finfo) {
    ReadLockGuard lk(rwlock_);
    if (CURVE_UNLIKELY(fileserviceMap_.find(fd) == fileserviceMap_.end())) {
        return LIBCURVE_ERROR::FAILED;
    }
    return fileserviceMap_[fd]->StatFs(filename, finfo);
}

int FileClient::Open(std::string filename, size_t size, bool create) {
    FileInstance* fileserv = new (std::nothrow) FileInstance();
    if (fileserv == nullptr ||
        !fileserv->Initialize(clientconfig_.GetFileServiceOption())) {
        LOG(ERROR) << "FileInstance initialize failed!";
        delete fileserv;
        return -1;
    }
    if (LIBCURVE_ERROR::FAILED ==
        fileserv->Open(filename, size, create)) {
        LOG(ERROR) << "open file failed!";
        delete fileserv;
        return -1;
    }
    int fd = fdcount_.fetch_add(1, std::memory_order_acq_rel);
    {
        WriteLockGuard lk(rwlock_);
        fileserviceMap_[fd] = fileserv;
    }
    return fd;
}

LIBCURVE_ERROR FileClient::Read(int fd, char* buf, off_t offset, size_t len) {
    ReadLockGuard lk(rwlock_);
    if (CURVE_UNLIKELY(fileserviceMap_.find(fd) == fileserviceMap_.end())) {
        LOG(ERROR) << "invalid fd!";
        return LIBCURVE_ERROR::FAILED;
    }
    return fileserviceMap_[fd]->Read(buf, offset, len);
}

LIBCURVE_ERROR FileClient::Write(int fd,
                                const char* buf,
                                off_t offset,
                                size_t len) {
    ReadLockGuard lk(rwlock_);
    if (CURVE_UNLIKELY(fileserviceMap_.find(fd) == fileserviceMap_.end())) {
        LOG(ERROR) << "invalid fd!";
        return LIBCURVE_ERROR::FAILED;
    }
    return fileserviceMap_[fd]->Write(buf, offset, len);
}

LIBCURVE_ERROR FileClient::AioRead(int fd, CurveAioContext* aioctx) {
    ReadLockGuard lk(rwlock_);
    if (CURVE_UNLIKELY(fileserviceMap_.find(fd) == fileserviceMap_.end())) {
        LOG(ERROR) << "invalid fd!";
        return  LIBCURVE_ERROR::FAILED;
    } else {
        fileserviceMap_[fd]->AioRead(aioctx);
    }
    return LIBCURVE_ERROR::OK;
}

LIBCURVE_ERROR FileClient::AioWrite(int fd, CurveAioContext* aioctx) {
    ReadLockGuard lk(rwlock_);
    if (CURVE_UNLIKELY(fileserviceMap_.find(fd) == fileserviceMap_.end())) {
        LOG(ERROR) << "invalid fd!";
        return LIBCURVE_ERROR::FAILED;
    } else {
        fileserviceMap_[fd]->AioWrite(aioctx);
    }
    return LIBCURVE_ERROR::OK;
}

void FileClient::Close(int fd) {
    WriteLockGuard lk(rwlock_);
    auto iter = fileserviceMap_.find(fd);
    if (iter == fileserviceMap_.end()) {
        return;
    }
    fileserviceMap_[fd]->Close();
    fileserviceMap_[fd]->UnInitialize();
    delete fileserviceMap_[fd];
    fileserviceMap_.erase(iter);
}
}   // namespace client
}   // namespace curve


// 全局初始化与反初始化
LIBCURVE_ERROR GlobalInit(const char* configpath);
void GlobalUnInit();

LIBCURVE_ERROR Init(const char* path) {
    return GlobalInit(path);
}

int Open(const char* filename, size_t size, bool create) {
    return globalclient->Open(filename, size, create);
}

LIBCURVE_ERROR Read(int fd, char* buf, off_t offset, size_t length) {
    return globalclient->Read(fd, buf, offset, length);
}

LIBCURVE_ERROR Write(int fd, const char* buf, off_t offset, size_t length) {
    return globalclient->Write(fd, buf, offset, length);
}

LIBCURVE_ERROR AioRead(int fd, CurveAioContext* aioctx) {
    DVLOG(9) << "offset: " << aioctx->offset
        << " length: " << aioctx->length
        << " op: " << aioctx->op;
    return globalclient->AioRead(fd, aioctx);
}

LIBCURVE_ERROR AioWrite(int fd, CurveAioContext* aioctx) {
    DVLOG(9) << "offset: " << aioctx->offset
        << " length: " << aioctx->length
        << " op: " << aioctx->op
        << " buf: " << *(unsigned int*)aioctx->buf;
    return globalclient->AioWrite(fd, aioctx);
}

void Close(int fd) {
    globalclient->Close(fd);
}

LIBCURVE_ERROR StatFs(int fd, const char* filename, FileStatInfo* finfo) {
    return globalclient->StatFs(fd, filename, finfo);
}

void UnInit() {
    GlobalUnInit();
}

LIBCURVE_ERROR GlobalInit(const char* path) {
    LIBCURVE_ERROR ret = LIBCURVE_ERROR::OK;
    if (globalclientinited_) {
        LOG(INFO) << "global cient already inited!";
        return LIBCURVE_ERROR::OK;
    }
    if (globalclient == nullptr) {
        globalclient = new (std::nothrow) curve::client::FileClient();
        if (globalclient != nullptr) {
            ret = globalclient->Init(path);
            LOG(INFO) << "create global client instance success!";
        } else {
            LOG(ERROR) << "create global client instance fail!";
        }
    }
    globalclientinited_ = ret == LIBCURVE_ERROR::OK;
    return ret;
}

void GlobalUnInit() {
    if (globalclient != nullptr) {
        globalclient->UnInit();
        delete globalclient;
        globalclient = nullptr;
        globalclientinited_ = false;
        LOG(INFO) << "destory global client instance success!";
    }
}
