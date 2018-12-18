/*
 * Project: curve
 * File Created: Tuesday, 25th September 2018 2:07:11 pm
 * Author: tongguangxun
 * Copyright (c) 2018 NetEase
 */

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "src/client/session.h"
#include "src/client/libcurve.hpp"
#include "include/client/libcurve.h"
#include "include/curve_compiler_specific.h"
#include "src/client/io_context_manager.h"
#include "src/client/client_common.h"

using curve::common::ReadLockGuard;
using curve::common::WriteLockGuard;

namespace curve {
namespace client {

    std::unordered_map<int, Session*> LibCurve::sessionMap_;
    std::atomic<uint64_t> LibCurve::fdcount_(1);
    RWLock  LibCurve::rwlock_;

    int LibCurve::Init(std::string configpath) {
        // TODO(tongguangxun): read config and init
        // set log level to ERROR to avoid verbose info output in qemu
        // change to setting appropriate level according to config file
        google::SetCommandLineOption("minloglevel", std::to_string(2).c_str());
        return 0;
    }

    void LibCurve::UnInit() {
        WriteLockGuard lk(rwlock_);
        for (auto iter : sessionMap_) {
            iter.second->UnInitialize();
            delete iter.second;
        }
        sessionMap_.clear();
    }

    CreateFileErrorType LibCurve::CreateFile(std::string name, size_t size) {
        return Session::CreateFile(name, size);
    }

    FInfo LibCurve::GetInfo(std::string name) {
        FInfo finfo;
        Session::GetFileInfo(name, &finfo);
        DVLOG(9) << "Filename: " << name
                 << "File size: " << finfo.length;
        return finfo;
    }

    int LibCurve::Open(std::string filename) {
        Session* newsession = new (std::nothrow) Session();
        if (!newsession->Initialize()) {
            LOG(ERROR) << "Session initialize failed!";
            delete newsession;
            return -1;
        }
        if (OpenFileErrorType::FILE_OPEN_FAILED
             == newsession->Open(filename)) {
            LOG(ERROR) << "open file failed!";
            delete newsession;
            return -1;
        }
        int fd = fdcount_.fetch_add(1, std::memory_order_acq_rel);
        WriteLockGuard lk(rwlock_);
        sessionMap_[fd] = newsession;
        return fd;
    }

    int LibCurve::Read(int fd, char* buf, off_t offset, size_t length) {
        ReadLockGuard lk(rwlock_);
        if (CURVE_UNLIKELY(sessionMap_.find(fd) == sessionMap_.end())) {
            return -1;
        }
        return sessionMap_[fd]->GetIOCtxManager()->Read(buf, offset, length);
    }

    int LibCurve::Write(int fd, const char* buf, off_t offset, size_t length) {
        ReadLockGuard lk(rwlock_);
        if (CURVE_UNLIKELY(sessionMap_.find(fd) == sessionMap_.end())) {
            return -1;
        }
        return sessionMap_[fd]->GetIOCtxManager()->Write(buf, offset, length);
    }

    int LibCurve::AioRead(int fd, CurveAioContext* aioctx) {
        ReadLockGuard lk(rwlock_);
        if (CURVE_UNLIKELY(sessionMap_.find(fd) == sessionMap_.end())) {
            return  -1;
        } else {
            sessionMap_[fd]->GetIOCtxManager()->AioRead(aioctx);
        }
        return 0;
    }

    int LibCurve::AioWrite(int fd, CurveAioContext* aioctx) {
        ReadLockGuard lk(rwlock_);
        if (CURVE_UNLIKELY(sessionMap_.find(fd) == sessionMap_.end())) {
            return -1;
        } else {
            sessionMap_[fd]->GetIOCtxManager()->AioWrite(aioctx);
        }
        return 0;
    }

    void LibCurve::Close(int fd) {
        WriteLockGuard lk(rwlock_);
        auto iter = sessionMap_.find(fd);
        if (iter == sessionMap_.end()) {
            return;
        }
        sessionMap_[fd]->UnInitialize();
        delete sessionMap_[fd];
        sessionMap_.erase(iter);
    }

    void LibCurve::Unlink(std::string filename) {
    }
}   // namespace client
}   // namespace curve

#ifdef __cplusplus
extern "C"
#endif
int Init(const char* configpath) {
    if (!configpath) {
        return -1;
    } else {
        return curve::client::LibCurve::Init(configpath);
    }
}

#ifdef __cplusplus
extern "C"
#endif
CreateFileErrorType CreateFile(const char* name, size_t size) {
    return curve::client::LibCurve::CreateFile(name, size);
}

#ifdef __cplusplus
extern "C"
#endif
int Open(const char* filename) {
    return curve::client::LibCurve::Open(filename);
}

#ifdef __cplusplus
extern "C"
#endif
int Read(int fd, char* buf, off_t offset, size_t length) {
    return curve::client::LibCurve::Read(fd, buf, offset, length);
}

#ifdef __cplusplus
extern "C"
#endif
int Write(int fd, const char* buf, off_t offset, size_t length) {
    return curve::client::LibCurve::Write(fd, buf, offset, length);
}

#ifdef __cplusplus
extern "C"
#endif
int AioRead(int fd, CurveAioContext* aioctx) {
    DVLOG(9) << "offset: " << aioctx->offset
        << " length: " << aioctx->length
        << " op: " << aioctx->op;
    return curve::client::LibCurve::AioRead(fd, aioctx);
    // TODO(tongguangxun) :we need specify return code.
}

#ifdef __cplusplus
extern "C"
#endif
int AioWrite(int fd, CurveAioContext* aioctx) {
    DVLOG(9) << "offset: " << aioctx->offset
        << " length: " << aioctx->length
        << " op: " << aioctx->op
        << " buf: " << *(unsigned int*)aioctx->buf;
    return curve::client::LibCurve::AioWrite(fd, aioctx);
}

#ifdef __cplusplus
extern "C"
#endif
void Close(int fd) {
    curve::client::LibCurve::Close(fd);
}

#ifdef __cplusplus
extern "C"
#endif
FInfo GetInfo(const char* filename) {
    return curve::client::LibCurve::GetInfo(filename);
}

#ifdef __cplusplus
extern "C"
#endif
void Unlink(const char* filename) {
    curve::client::LibCurve::Unlink(filename);
}

#ifdef __cplusplus
extern "C"
#endif
void UnInit() {
    curve::client::LibCurve::UnInit();
}
