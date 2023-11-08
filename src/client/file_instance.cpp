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

    return iomanager4file_.Read(buf, offset, length, mdsclient_.get());
}

int FileInstance::Write(const char *buf, off_t offset, size_t len) {
    if (CURVE_UNLIKELY(readonly_)) {
        DVLOG(9) << "open with read only, do not support write!";
        return -LIBCURVE_ERROR::READONLY;
    }

    if (finfo_.readonly) {
        DVLOG(9) << "file is readonly, can not write!";
        return -LIBCURVE_ERROR::READONLY;
    }

    if (CURVE_UNLIKELY(!CheckAlign(offset, len, blocksize_))) {
        LOG(ERROR) << "IO not aligned, off: " << offset << ", length: " << len
                   << ", block size: " << blocksize_;
        return -LIBCURVE_ERROR::NOT_ALIGNED;
    }

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

    if (GetReadSnapshotSn()) {
        return iomanager4file_.AioReadSnapshot(
            aioctx, mdsclient_.get(), dataType);
    }
    return iomanager4file_.AioRead(aioctx, mdsclient_.get(), dataType);
}

int FileInstance::AioWrite(CurveAioContext *aioctx, UserDataType dataType) {
    if (CURVE_UNLIKELY(readonly_)) {
        DVLOG(9) << "open with read only, do not support write!";
        return -1;
    }

    if (finfo_.readonly) {
        DVLOG(9) << "file is readonly, can not write!";
        return -LIBCURVE_ERROR::READONLY;
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

    return iomanager4file_.AioWrite(aioctx, mdsclient_.get(), dataType);
}

// 两种场景会造成在Open的时候返回LIBCURVE_ERROR::FILE_OCCUPIED
// 1. 强制重启qemu不会调用close逻辑，然后启动的时候原来的文件sessio还没过期.
//    导致再次去发起open的时候，返回被占用，这种情况可以通过load sessionmap
//    拿到已有的session，再去执行refresh。
// 2. 由于网络原因，导致open rpc超时，然后再去重试的时候就会返回FILE_OCCUPIED
//    这时候当前还没有成功打开，所以还没有存储该session信息，所以无法通过refresh
//    再去打开，所以这时候需要获取mds一侧session lease时长，然后在client这一侧
//    等待一段时间再去Open，如果依然失败，就向上层返回失败。
int FileInstance::Open(std::string* sessionId) {
    LeaseSession_t  lease;
    int ret = LIBCURVE_ERROR::FAILED;

    FileEpoch fEpoch;
    ret = mdsclient_->OpenFile(finfo_.fullPathName, finfo_.userinfo, &finfo_,
                               &fEpoch, &lease);
    if (ret == LIBCURVE_ERROR::OK) {
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
    FileInstance* instance = new (std::nothrow) FileInstance();
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

}   // namespace client
}   // namespace curve
