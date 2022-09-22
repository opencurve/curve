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

/*
 * Project: curve
 * File Created: Tuesday, 25th September 2018 4:58:20 pm
 * Author: tongguangxun
 */

#include "src/client/file_instance.h"

#include <butil/endpoint.h>
#include <glog/logging.h>
#include <utility>

#include "include/client/libcurve.h"
#include "src/client/iomanager4file.h"
#include "src/client/mds_client.h"
#include "src/common/timeutility.h"
#include "src/common/curve_define.h"
#include "src/common/uuid.h"

namespace curve {
namespace client {

using curve::client::ClientConfig;
using curve::common::TimeUtility;
using curve::mds::SessionStatus;

FileInstance::FileInstance()
    : finfo_(),
      fileopt_(),
      mdsclient_(nullptr),
      leaseExecutor_(),
      iomanager4file_(),
      readonly_(false) {}

bool FileInstance::Initialize(const std::string& filename,
                              std::shared_ptr<MDSClient> mdsclient,
                              const UserInfo_t& userinfo,
                              const int openflags,
                              const FileServiceOption& fileservicopt,
                              bool readonly) {
    readonly_ = readonly;
    fileopt_ = fileservicopt;
    bool ret = false;
    do {
        if (!userinfo.Valid()) {
            LOG(ERROR) << "userinfo not valid!";
            break;
        }

        if (mdsclient == nullptr) {
            LOG(ERROR) << "mdsclient pointer is null!";
            break;
        }

        finfo_.context.openflags = openflags;
        finfo_.userinfo = userinfo;
        mdsclient_ = std::move(mdsclient);

        finfo_.fullPathName = filename;

        if (!iomanager4file_.Initialize(filename, fileopt_.ioOpt,
                                        mdsclient_.get())) {
            LOG(ERROR) << "Init io context manager failed, filename = "
                       << filename;
            break;
        }

        iomanager4file_.UpdateFileInfo(finfo_);

        leaseExecutor_.reset(new (std::nothrow) LeaseExecutor(
            fileopt_.leaseOpt, finfo_.userinfo, mdsclient_.get(),
            &iomanager4file_));
        if (CURVE_UNLIKELY(leaseExecutor_ == nullptr)) {
            LOG(ERROR) << "Allocate LeaseExecutor failed, filename = "
                       << filename;
            break;
        }

        ret = true;
    } while (0);

    return ret;
}

void FileInstance::UnInitialize() {
    StopLease();

    iomanager4file_.UnInitialize();

    // release the ownership of mdsclient
    mdsclient_.reset();
}

int FileInstance::Read(char* buf, off_t offset, size_t length) {
    DLOG_EVERY_SECOND(INFO) << "begin Read "<< finfo_.fullPathName
                            << ", offset = " << offset
                            << ", len = " << length;
    return iomanager4file_.Read(buf, offset, length, mdsclient_.get());
}

int FileInstance::Write(const char* buf, off_t offset, size_t len) {
    if (readonly_) {
        DVLOG(9) << "open with read only, do not support write!";
        return -1;
    }
    if (!CanWrite()) {
        return LIBCURVE_ERROR::PERMISSION_DENY;
    }
    DLOG_EVERY_SECOND(INFO) << "begin write " << finfo_.fullPathName
                            << ", offset = " << offset
                            << ", len = " << len;
    return iomanager4file_.Write(buf, offset, len, mdsclient_.get());
}

int FileInstance::AioRead(CurveAioContext* aioctx, UserDataType dataType) {
    DLOG_EVERY_SECOND(INFO) << "begin AioRead " << finfo_.fullPathName
                            << ", offset = " << aioctx->offset
                            << ", len = " << aioctx->length;
    return iomanager4file_.AioRead(aioctx, mdsclient_.get(), dataType);
}

int FileInstance::AioWrite(CurveAioContext* aioctx, UserDataType dataType) {
    if (readonly_) {
        DVLOG(9) << "open with read only, do not support write!";
        return -1;
    }
    if (!CanWrite()) {
        return LIBCURVE_ERROR::PERMISSION_DENY;
    }
    DLOG_EVERY_SECOND(INFO) << "begin AioWrite " << finfo_.fullPathName
                            << ", offset = " << aioctx->offset
                            << ", len = " << aioctx->length;
    return iomanager4file_.AioWrite(aioctx, mdsclient_.get(), dataType);
}

int FileInstance::Discard(off_t offset, size_t length) {
    if (!readonly_) {
        return iomanager4file_.Discard(offset, length, mdsclient_.get());
    }

    LOG(ERROR) << "Open with read only, not support Discard";
    return -1;
}

int FileInstance::AioDiscard(CurveAioContext* aioctx) {
    if (!readonly_) {
        return iomanager4file_.AioDiscard(aioctx, mdsclient_.get());
    }

    LOG(ERROR) << "Open with read only, not support AioDiscard";
    return -1;
}

// 两种场景会造成在Open的时候返回LIBCURVE_ERROR::FILE_OCCUPIED
// 1. 强制重启qemu不会调用close逻辑，然后启动的时候原来的文件sessio还没过期.
//    导致再次去发起open的时候，返回被占用，这种情况可以通过load sessionmap
//    拿到已有的session，再去执行refresh。
// 2. 由于网络原因，导致open rpc超时，然后再去重试的时候就会返回FILE_OCCUPIED
//    这时候当前还没有成功打开，所以还没有存储该session信息，所以无法通过refresh
//    再去打开，所以这时候需要获取mds一侧session lease时长，然后在client这一侧
//    等待一段时间再去Open，如果依然失败，就向上层返回失败。
int FileInstance::Open(const std::string& filename,
                        const UserInfo& userinfo,
                        std::string* sessionId) {
    LeaseSession_t  lease;
    int ret = LIBCURVE_ERROR::FAILED;
    finfo_.context.uuid = curve::common::UUIDGenerator().GenerateUUID();
    FileEpoch_t fEpoch;
    ret = mdsclient_->OpenFile(filename, finfo_.userinfo,
        &finfo_, &fEpoch, &lease);
    if (ret == LIBCURVE_ERROR::OK) {
        iomanager4file_.UpdateFileThrottleParams(finfo_.throttleParams);
        ret = leaseExecutor_->Start(finfo_, lease) ? LIBCURVE_ERROR::OK
                                                   : LIBCURVE_ERROR::FAILED;
        if (nullptr != sessionId) {
            sessionId->assign(lease.sessionID);
        }
        iomanager4file_.UpdateFileEpoch(fEpoch);
    }
    return -ret;
}

int FileInstance::ReOpen(const std::string& filename,
                         const std::string& sessionId,
                         const UserInfo& userInfo,
                         std::string* newSessionId) {
    return Open(filename, userInfo, newSessionId);
}

int FileInstance::GetFileInfo(const std::string& filename,
    FInfo_t* fi, FileEpoch_t *fEpoch) {
    LIBCURVE_ERROR ret = mdsclient_->GetFileInfo(filename, finfo_.userinfo,
                                                 fi, fEpoch);
    return -ret;
}

int FileInstance::Close() {
    if (readonly_) {
        LOG(INFO) << "close read only file!" << finfo_.fullPathName;
        return 0;
    }

    StopLease();

    LIBCURVE_ERROR ret =
        mdsclient_->CloseFile(finfo_.fullPathName, finfo_.userinfo,
            "", finfo_.context);
    return -ret;
}

FileInstance* FileInstance::NewInitedFileInstance(
    const FileServiceOption& fileServiceOption,
    std::shared_ptr<MDSClient> mdsClient,
    const std::string& filename,
    const UserInfo& userInfo,
    const int openflags,  // TODO(all): maybe we can put userinfo and readonly into openflags  // NOLINT
    bool readonly) {
    FileInstance* instance = new (std::nothrow) FileInstance();
    if (instance == nullptr) {
        LOG(ERROR) << "Create FileInstance failed, filename: " << filename;
        return nullptr;
    }

    bool ret = instance->Initialize(filename, std::move(mdsClient), userInfo,
                                    openflags, fileServiceOption, readonly);
    if (!ret) {
        LOG(ERROR) << "FileInstance initialize failed"
                   << ", filename = " << filename
                   << ", owner = " << userInfo.owner
                   << ", readonly = " << readonly;
        delete instance;
        return nullptr;
    }

    return instance;
}

FileInstance* FileInstance::Open4Readonly(const FileServiceOption& opt,
                                          std::shared_ptr<MDSClient> mdsclient,
                                          const std::string& filename,
                                          const UserInfo& userInfo,
                                          const int openflags) {
    FileInstance* instance = FileInstance::NewInitedFileInstance(
        opt, std::move(mdsclient), filename, userInfo, openflags, true);
    if (instance == nullptr) {
        LOG(ERROR) << "NewInitedFileInstance failed, filename = " << filename;
        return nullptr;
    }

    FInfo fileInfo;
    FileEpoch_t fEpoch;
    int ret = instance->GetFileInfo(filename, &fileInfo, &fEpoch);
    if (ret != 0) {
        LOG(ERROR) << "Get file info failed!";
        instance->UnInitialize();
        delete instance;
        return nullptr;
    }

    fileInfo.context.openflags = openflags;
    fileInfo.userinfo = userInfo;
    fileInfo.fullPathName = filename;
    instance->GetIOManager4File()->UpdateFileInfo(fileInfo);
    instance->GetIOManager4File()->UpdateFileEpoch(fEpoch);

    return instance;
}

void FileInstance::StopLease() {
    if (leaseExecutor_) {
        leaseExecutor_->Stop();
        leaseExecutor_.reset();
    }
}

bool FileInstance::CanWrite() const {
    return finfo_.context.openflags & CurveOpenFlags::CURVE_FORCE_WRITE
        || finfo_.context.openflags & CurveOpenFlags::CURVE_RDWR;
}


}   // namespace client
}   // namespace curve
