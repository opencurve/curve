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
#include "src/client/service_helper.h"

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
    google::SetCommandLineOption("minloglevel", std::to_string(
        clientconfig_.GetFileServiceOption().loginfo.loglevel).c_str());
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

LIBCURVE_ERROR FileClient::StatFs(int fd, FileStatInfo* finfo) {
    ReadLockGuard lk(rwlock_);
    if (CURVE_UNLIKELY(fileserviceMap_.find(fd) == fileserviceMap_.end())) {
        return LIBCURVE_ERROR::FAILED;
    }
    return fileserviceMap_[fd]->StatFs(finfo);
}

int FileClient::Open(const std::string& filename,
                     UserInfo_t userinfo,
                     size_t size,
                     bool create) {
    FileInstance* fileserv = new (std::nothrow) FileInstance();
    if (fileserv == nullptr ||
        !fileserv->Initialize(userinfo, clientconfig_.GetFileServiceOption())) {
        LOG(ERROR) << "FileInstance initialize failed!";
        delete fileserv;
        return -1;
    }

    LIBCURVE_ERROR ret = fileserv->Open(filename, size, create);
    if (LIBCURVE_ERROR::FAILED == ret || LIBCURVE_ERROR::AUTHFAIL == ret) {
        LOG(ERROR) << ErrorNum2ErrorName(ret);
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
    if (CheckAligned(offset, len) == false) {
        return LIBCURVE_ERROR::NOT_ALIGNED;
    }

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
    if (CheckAligned(offset, len) == false) {
        return LIBCURVE_ERROR::NOT_ALIGNED;
    }

    ReadLockGuard lk(rwlock_);
    if (CURVE_UNLIKELY(fileserviceMap_.find(fd) == fileserviceMap_.end())) {
        LOG(ERROR) << "invalid fd!";
        return LIBCURVE_ERROR::FAILED;
    }
    return fileserviceMap_[fd]->Write(buf, offset, len);
}

LIBCURVE_ERROR FileClient::AioRead(int fd, CurveAioContext* aioctx) {
    if (CheckAligned(aioctx->offset, aioctx->length) == false) {
        return LIBCURVE_ERROR::NOT_ALIGNED;
    }

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
    if (CheckAligned(aioctx->offset, aioctx->length) == false) {
        return LIBCURVE_ERROR::NOT_ALIGNED;
    }

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

bool FileClient::CheckAligned(off_t offset, size_t length) {
    return (offset % IO_ALIGNED_BLOCK_SIZE == 0) &&
           (length % IO_ALIGNED_BLOCK_SIZE == 0);
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
    curve::client::UserInfo_t userinfo;
    std::string realname;
    if (!curve::client::ServiceHelper::GetUserInfoFromFilename(filename,
                                        &realname,
                                        &userinfo.owner)) {
        LOG(ERROR) << "get user info from filename failed!";
        return LIBCURVE_ERROR::FAILED;
    }
    return globalclient->Open(realname, userinfo, size, create);
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

LIBCURVE_ERROR StatFs(int fd, FileStatInfo* finfo) {
    return globalclient->StatFs(fd, finfo);
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
