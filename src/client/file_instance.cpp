/*
 * Project: curve
 * File Created: Tuesday, 25th September 2018 4:58:20 pm
 * Author: tongguangxun
 * Copyright (c) 2018 NetEase
 */

#include <glog/logging.h>
#include <gflags/gflags.h>
#include <butil/endpoint.h>

#include "proto/topology.pb.h"
#include "proto/nameserver2.pb.h"
#include "src/common/timeutility.h"
#include "src/client/file_instance.h"
#include "src/client/metacache.h"
#include "src/client/mds_client.h"
#include "src/client/iomanager4file.h"
#include "src/client/request_scheduler.h"
#include "src/client/request_sender_manager.h"

using curve::client::ClientConfig;
using curve::common::TimeUtility;
using curve::mds::SessionStatus;
namespace curve {
namespace client {

FileInstance::FileInstance() {
    leaseexcutor_     = nullptr;

    finfo_.chunksize   = 4 * 1024 * 1024;
    finfo_.segmentsize = 1 * 1024 * 1024 * 1024ul;
}

bool FileInstance::Initialize(UserInfo_t userinfo,
                              FileServiceOption_t fileservicopt) {
    fileopt_ = fileservicopt;
    bool ret = false;
    do {
        if (!userinfo.Valid()) {
            LOG(ERROR) << "userinfo not valid!";
            break;
        }

        if (mdsclient_.Initialize(userinfo, fileopt_.metaserveropt) != 0) {
            LOG(ERROR) << "MDSClient init failed!";
            break;
        }

        if (!iomanager4file_.Initialize(fileopt_.ioopt)) {
            LOG(ERROR) << "Init io context manager failed!";
            break;
        }
        leaseexcutor_ = new (std::nothrow) LeaseExcutor(fileopt_.leaseopt,
                                                        &mdsclient_,
                                                        &iomanager4file_);
        if (CURVE_UNLIKELY(leaseexcutor_ == nullptr)) {
            LOG(ERROR) << "allocate lease excutor failed!";
            break;
        }

        ret = true;
    } while (0);

    if (!ret) {
        delete leaseexcutor_;
    }
    return ret;
}

void FileInstance::UnInitialize() {
    iomanager4file_.UnInitialize();
    leaseexcutor_->Stop();
    mdsclient_.UnInitialize();

    delete leaseexcutor_;
    leaseexcutor_ = nullptr;
}

LIBCURVE_ERROR FileInstance::Read(char* buf, off_t offset, size_t length) {
    return iomanager4file_.Read(buf, offset, length, &mdsclient_);
}

LIBCURVE_ERROR FileInstance::Write(const char* buf, off_t offset, size_t len) {
    return iomanager4file_.Write(buf, offset, len, &mdsclient_);
}

void FileInstance::AioRead(CurveAioContext* aioctx) {
    iomanager4file_.AioRead(aioctx, &mdsclient_);
}

void FileInstance::AioWrite(CurveAioContext* aioctx) {
    iomanager4file_.AioWrite(aioctx, &mdsclient_);
}

LIBCURVE_ERROR FileInstance::StatFs(std::string filename, FileStatInfo* finfo) {
    FInfo_t fi;
    if (GetFileInfo(filename, &fi) == 0) {
        finfo->ctime    = fi.ctime;
        finfo->length   = fi.length;
        finfo->filetype = fi.filetype;
    } else {
        LOG(ERROR) << "Get file info failed!";
        return LIBCURVE_ERROR::FAILED;
    }
    return LIBCURVE_ERROR::OK;
}

LIBCURVE_ERROR FileInstance::CreateFile(std::string filename, size_t size) {
    return mdsclient_.CreateFile(filename, size);
}

LIBCURVE_ERROR FileInstance::Open(std::string fname, size_t size, bool create) {
    LeaseSession_t  lease;
    LIBCURVE_ERROR ret = LIBCURVE_ERROR::OK;
    if (create) {
        ret = CreateFile(fname, size);
    }

    if (LIBCURVE_ERROR::OK == ret || LIBCURVE_ERROR::EXISTS == ret) {
        ret = mdsclient_.OpenFile(fname, &finfo_, &lease);
        if (LIBCURVE_ERROR::OK == ret) {
            ret = leaseexcutor_->Start(finfo_, lease) ? LIBCURVE_ERROR::OK
                                                      : LIBCURVE_ERROR::FAILED;
        } else {
            LOG(ERROR) << "Open file failed!";
        }
    }
    return ret;
}

LIBCURVE_ERROR FileInstance::GetFileInfo(std::string filename, FInfo_t* fi) {
    return mdsclient_.GetFileInfo(filename, fi);
}

LIBCURVE_ERROR FileInstance::Close() {
    return mdsclient_.CloseFile(finfo_.filename,
                                leaseexcutor_->GetLeaseSessionID());
}
}   // namespace client
}   // namespace curve
