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

bool FileInstance::Initialize(MDSClient* mdsclient,
                              const UserInfo_t& userinfo,
                              FileServiceOption_t fileservicopt) {
    fileopt_ = fileservicopt;
    bool ret = false;
    do {
        if (!userinfo.Valid()) {
            LOG(ERROR) << "userinfo not valid!";
            break;
        }

        userinfo_ = userinfo;
        mdsclient_ = mdsclient;

        if (!iomanager4file_.Initialize(fileopt_.ioOpt)) {
            LOG(ERROR) << "Init io context manager failed!";
            break;
        }
        leaseexcutor_ = new (std::nothrow) LeaseExcutor(fileopt_.leaseOpt,
                                                        userinfo_,
                                                        mdsclient_,
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
    mdsclient_->UnInitialize();

    delete leaseexcutor_;
    leaseexcutor_ = nullptr;
}

int FileInstance::Read(char* buf, off_t offset, size_t length) {
    return iomanager4file_.Read(buf, offset, length, mdsclient_);
}

int FileInstance::Write(const char* buf, off_t offset, size_t len) {
    return iomanager4file_.Write(buf, offset, len, mdsclient_);
}

void FileInstance::AioRead(CurveAioContext* aioctx) {
    iomanager4file_.AioRead(aioctx, mdsclient_);
}

void FileInstance::AioWrite(CurveAioContext* aioctx) {
    iomanager4file_.AioWrite(aioctx, mdsclient_);
}

int FileInstance::Open(const std::string& filename, UserInfo_t userinfo) {
    LeaseSession_t  lease;
    int ret = -LIBCURVE_ERROR::FAILED;

    ret = mdsclient_->OpenFile(filename, userinfo_, &finfo_, &lease);
    if (LIBCURVE_ERROR::OK == ret) {
        finfo_.fullPathName = filename;
        ret = leaseexcutor_->Start(finfo_, lease) ? LIBCURVE_ERROR::OK
                                                    : LIBCURVE_ERROR::FAILED;
    } else {
        LOG(ERROR) << "Open file failed!";
    }

    return -ret;
}

int FileInstance::GetFileInfo(const std::string& filename, FInfo_t* fi) {
    return -mdsclient_->GetFileInfo(filename, userinfo_, fi);
}

int FileInstance::Close() {
    return -mdsclient_->CloseFile(finfo_.fullPathName, userinfo_,
                                leaseexcutor_->GetLeaseSessionID());
}
}   // namespace client
}   // namespace curve
