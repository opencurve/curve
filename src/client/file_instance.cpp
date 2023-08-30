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

#include "src/client/iomanager4file.h"
#include "src/client/mds_client.h"
#include "src/common/timeutility.h"
#include "src/common/curve_define.h"
#include "src/common/fast_align.h"

namespace curve {
namespace client {

using curve::client::ClientConfig;
using curve::common::TimeUtility;
using curve::mds::SessionStatus;
using curve::common::is_aligned;

bool CheckAlign(off_t off, size_t length, size_t blocksize) {
    return is_aligned(off, blocksize) && is_aligned(length, blocksize);
}

bool FileInstance::Initialize(const std::string& filename,
                              const std::shared_ptr<MDSClient>& mdsclient,
                              const UserInfo& userinfo,
                              const OpenFlags& openflags,
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

        mdsclient_ = mdsclient;
        finfo_.openflags = openflags;
        finfo_.userinfo = userinfo;
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
        if (leaseExecutor_ == nullptr) {
            LOG(ERROR) << "Allocate LeaseExecutor failed, filename = "
                       << filename;
            break;
        }

        ret = true;
    } while (false);

    return ret;
}

void FileInstance::UnInitialize() {
    StopLease();

    iomanager4file_.UnInitialize();

    // release the ownership of mdsclient
    mdsclient_.reset();
}

int FileInstance::Read(char* buf, off_t offset, size_t length) {
    if (CURVE_UNLIKELY(!CheckAlign(offset, length, blocksize_))) {
        LOG(ERROR) << "IO not aligned, off: " << offset
                   << ", length: " << length
                   << ", block size: " << blocksize_;
        return -LIBCURVE_ERROR::NOT_ALIGNED;
    }

    DLOG_EVERY_SECOND(INFO) << "begin Read "<< finfo_.fullPathName
                            << ", offset = " << offset
                            << ", len = " << length;
    return iomanager4file_.Read(buf, offset, length, mdsclient_.get());
}

int FileInstance::Write(const char *buf, off_t offset, size_t len) {
    if (CURVE_UNLIKELY(readonly_)) {
        DVLOG(9) << "open with read only, do not support write!";
        return -1;
    }

    if (CURVE_UNLIKELY(!CheckAlign(offset, len, blocksize_))) {
        LOG(ERROR) << "IO not aligned, off: " << offset << ", length: " << len
                   << ", block size: " << blocksize_;
        return -LIBCURVE_ERROR::NOT_ALIGNED;
    }

    DLOG_EVERY_SECOND(INFO) << "begin write " << finfo_.fullPathName
                            << ", offset = " << offset << ", len = " << len;
    return iomanager4file_.Write(buf, offset, len, mdsclient_.get());
}

int FileInstance::AioRead(CurveAioContext* aioctx, UserDataType dataType) {
    if (CURVE_UNLIKELY(
                !CheckAlign(aioctx->offset, aioctx->length, blocksize_))) {
        LOG(ERROR) << "IO not aligned, off: " << aioctx->offset
                   << ", length: " << aioctx->length
                   << ", block size: " << blocksize_;
        aioctx->ret = -LIBCURVE_ERROR::NOT_ALIGNED;
        aioctx->cb(aioctx);
        return -LIBCURVE_ERROR::NOT_ALIGNED;
    }

    DLOG_EVERY_SECOND(INFO) << "begin AioRead " << finfo_.fullPathName
                            << ", offset = " << aioctx->offset
                            << ", len = " << aioctx->length;
    return iomanager4file_.AioRead(aioctx, mdsclient_.get(), dataType);
}

int FileInstance::AioWrite(CurveAioContext *aioctx, UserDataType dataType) {
    if (CURVE_UNLIKELY(readonly_)) {
        DVLOG(9) << "open with read only, do not support write!";
        return -1;
    }

    if (CURVE_UNLIKELY(
                !CheckAlign(aioctx->offset, aioctx->length, blocksize_))) {
        LOG(ERROR) << "IO not aligned, off: " << aioctx->offset
                   << ", length: " << aioctx->length
                   << ", block size: " << blocksize_;
        aioctx->ret = -LIBCURVE_ERROR::NOT_ALIGNED;
        aioctx->cb(aioctx);
        return -LIBCURVE_ERROR::NOT_ALIGNED;
    }

    DLOG_EVERY_SECOND(INFO) << "begin AioWrite " << finfo_.fullPathName
                            << ", offset = " << aioctx->offset
                            << ", len = " << aioctx->length;
    return iomanager4file_.AioWrite(aioctx, mdsclient_.get(), dataType);
}

int FileInstance::Discard(off_t offset, size_t length) {
    if (CURVE_LIKELY(!readonly_)) {
        return iomanager4file_.Discard(offset, length, mdsclient_.get());
    }

    LOG(ERROR) << "Open with read only, not support Discard";
    return -1;
}

int FileInstance::AioDiscard(CurveAioContext *aioctx) {
    if (CURVE_LIKELY(!readonly_)) {
        return iomanager4file_.AioDiscard(aioctx, mdsclient_.get());
    }

    LOG(ERROR) << "Open with read only, not support AioDiscard";
    return -1;
}

//Two scenarios can cause LIBCURVE_ERROR::FILE_OCCUPIED to be returned during opening
//1. Forcing a restart of QEMU will not call the close logic, and the original file session has not expired when starting
//When initiating open again, the return is occupied, which can be done through the load sessionmap
//Get the existing session and then perform a refresh.
//2. Due to network reasons, the open rpc timed out and when trying again, it will return FILE_OCCUPIED
//At this time, the session information has not been successfully opened yet, so it cannot be refreshed
//Open it again, so at this point, you need to obtain the session lease duration on the mds side, and then on the client side
//Wait for a period of time before going to Open, and if it still fails, return the failure to the upper level.
int FileInstance::Open(std::string* sessionId) {
    LeaseSession_t  lease;
    int ret = LIBCURVE_ERROR::FAILED;

    FileEpoch fEpoch;
    ret = mdsclient_->OpenFile(finfo_.fullPathName, finfo_.userinfo, &finfo_,
                               &fEpoch, &lease);
    if (ret == LIBCURVE_ERROR::OK) {
        iomanager4file_.UpdateFileThrottleParams(finfo_.throttleParams);
        ret = leaseExecutor_->Start(finfo_, lease) ? LIBCURVE_ERROR::OK
                                                   : LIBCURVE_ERROR::FAILED;
        if (nullptr != sessionId) {
            sessionId->assign(lease.sessionID);
        }
        iomanager4file_.UpdateFileEpoch(fEpoch);
        blocksize_ = finfo_.blocksize;
    }
    return -ret;
}

int FileInstance::GetFileInfo(const std::string &filename, FInfo_t *fi,
                              FileEpoch_t *fEpoch) {
    LIBCURVE_ERROR ret =
        mdsclient_->GetFileInfo(filename, finfo_.userinfo, fi, fEpoch);
    return -ret;
}

int FileInstance::Close() {
    if (readonly_) {
        LOG(INFO) << "close read only file!" << finfo_.fullPathName;
        return 0;
    }

    StopLease();

    LIBCURVE_ERROR ret =
        mdsclient_->CloseFile(finfo_.fullPathName, finfo_.userinfo, "");
    return -ret;
}

FileInstance* FileInstance::NewInitedFileInstance(
    const FileServiceOption& fileServiceOption,
    const std::shared_ptr<MDSClient>& mdsClient,
    const std::string& filename,
    const UserInfo& userInfo,
    const OpenFlags& openflags,  // TODO(all): maybe we can put userinfo and readonly into openflags  // NOLINT
    bool readonly) {
    FileInstance *instance = new (std::nothrow) FileInstance();
    if (instance == nullptr) {
        LOG(ERROR) << "Create FileInstance failed, filename: " << filename;
        return nullptr;
    }

    bool ret = instance->Initialize(filename, mdsClient, userInfo, openflags,
                                    fileServiceOption, readonly);
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

FileInstance* FileInstance::Open4Readonly(
    const FileServiceOption& opt,
    const std::shared_ptr<MDSClient>& mdsclient,
    const std::string& filename,
    const UserInfo& userInfo,
    const OpenFlags& openflags) {
    FileInstance* instance = FileInstance::NewInitedFileInstance(
        opt, mdsclient, filename, userInfo, openflags, true);
    if (instance == nullptr) {
        LOG(ERROR) << "NewInitedFileInstance failed, filename = " << filename;
        return nullptr;
    }

    FileEpoch_t fEpoch;
    int ret = mdsclient->GetFileInfo(filename, userInfo, &instance->finfo_,
                                     &fEpoch);
    if (ret != 0) {
        LOG(ERROR) << "Get file info failed!";
        instance->UnInitialize();
        delete instance;
        return nullptr;
    }

    instance->finfo_.openflags = openflags;
    instance->finfo_.userinfo = userInfo;
    instance->finfo_.fullPathName = filename;
    instance->blocksize_ = instance->finfo_.blocksize;
    LOG(INFO) << "block size is " << instance->blocksize_;
    instance->GetIOManager4File()->UpdateFileInfo(instance->finfo_);
    instance->GetIOManager4File()->UpdateFileEpoch(fEpoch);

    return instance;
}

void FileInstance::StopLease() {
    if (leaseExecutor_) {
        leaseExecutor_->Stop();
        leaseExecutor_.reset();
    }
}

}  // namespace client
}  // namespace curve
