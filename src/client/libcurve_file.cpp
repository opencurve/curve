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
 * File Created: Monday, 18th February 2019 11:20:01 am
 * Author: tongguangxun
 */

#include "src/client/libcurve_file.h"

#include <brpc/server.h>
#include <glog/logging.h>

#include <algorithm>
#include <memory>
#include <mutex>   // NOLINT
#include <thread>  // NOLINT

#include "include/client/libcurve.h"
#include "include/curve_compiler_specific.h"
#include "proto/nameserver2.pb.h"
#include "src/client/client_common.h"
#include "src/client/client_config.h"
#include "src/client/file_instance.h"
#include "src/client/iomanager4file.h"
#include "src/client/service_helper.h"
#include "src/client/source_reader.h"
#include "src/common/curve_version.h"
#include "src/common/net_common.h"
#include "src/common/uuid.h"

#define PORT_LIMIT  65535

bool globalclientinited_ = false;
curve::client::FileClient* globalclient = nullptr;

using curve::client::UserInfo;

namespace brpc {
    DECLARE_int32(health_check_interval);
}  // namespace brpc

namespace curve {
namespace client {

using curve::common::ReadLockGuard;
using curve::common::WriteLockGuard;

static const int PROCESS_NAME_MAX = 32;
static char g_processname[PROCESS_NAME_MAX];

void LoggerGuard::InitInternal(const std::string& confPath) {
    curve::common::Configuration conf;
    conf.SetConfigPath(confPath);

    bool rc = conf.LoadConfig();
    if (!rc) {
        LOG(ERROR) << "Load config failed, config path = " << confPath
                   << ", not init glog";
        return;
    }

    FLAGS_minloglevel = 0;
    FLAGS_log_dir = "/tmp";

    // disable error log output to console
    FLAGS_stderrthreshold = 3;

    LOG_IF(WARNING, !conf.GetIntValue("global.logLevel", &FLAGS_minloglevel))
        << "config no loglevel info, using default value 0";
    LOG_IF(WARNING, !conf.GetStringValue("global.logPath", &FLAGS_log_dir))
        << "config no logpath info, using default dir '/tmp'";

    std::string processName = std::string("libcurve-").append(
        curve::common::UUIDGenerator().GenerateUUID().substr(0, 8));
    snprintf(g_processname, sizeof(g_processname),
            "%s", processName.c_str());
    google::InitGoogleLogging(g_processname);
}

void InitLogging(const std::string& confPath) {
    static LoggerGuard guard(confPath);
}

FileClient::FileClient(): fdcount_(0), openedFileNum_("opened_file_num") {
    inited_ = false;
    mdsClient_ = nullptr;
    fileserviceMap_.clear();
}

int FileClient::Init(const std::string& configpath) {
    if (inited_) {
        LOG(WARNING) << "already inited!";
        return 0;
    }

    curve::client::InitLogging(configpath);
    curve::common::ExposeCurveVersion();

    if (-1 == clientconfig_.Init(configpath.c_str())) {
        LOG(ERROR) << "config init failed!";
        return -LIBCURVE_ERROR::FAILED;
    }

    mdsClient_ = new (std::nothrow) MDSClient();
    if (mdsClient_ == nullptr) {
        return -LIBCURVE_ERROR::FAILED;
    }

    auto ret = mdsClient_->Initialize(clientconfig_.
               GetFileServiceOption().metaServerOpt);
    if (LIBCURVE_ERROR::OK != ret) {
        LOG(ERROR) << "Init global mds client failed!";
        delete mdsClient_;
        mdsClient_ = nullptr;
        return -LIBCURVE_ERROR::FAILED;
    }

    if (clientconfig_.GetFileServiceOption().commonOpt.turnOffHealthCheck) {
        brpc::FLAGS_health_check_interval = -1;
    }

    // set option for source reader
    SourceReader::GetInstance().SetOption(clientconfig_.GetFileServiceOption());

    bool rc = StartDummyServer();
    if (rc == false) {
        mdsClient_->UnInitialize();
        delete mdsClient_;
        mdsClient_ = nullptr;
        return -LIBCURVE_ERROR::FAILED;
    }

    inited_ = true;
    return LIBCURVE_ERROR::OK;
}

void FileClient::UnInit() {
    if (!inited_) {
        LOG(WARNING) << "not inited!";
        return;
    }
    WriteLockGuard lk(rwlock_);
    for (auto iter : fileserviceMap_) {
        iter.second->UnInitialize();
        delete iter.second;
    }
    fileserviceMap_.clear();

    if (mdsClient_ != nullptr) {
        mdsClient_->UnInitialize();
        delete mdsClient_;
        mdsClient_ = nullptr;
    }
    inited_ = false;
}

int FileClient::Open(const std::string& filename,
                     const UserInfo_t& userinfo,
                     std::string* sessionId) {
    FileInstance* fileserv = FileInstance::NewInitedFileInstance(
        clientconfig_.GetFileServiceOption(), mdsClient_, filename, userinfo,
        false);
    if (fileserv == nullptr) {
        LOG(ERROR) << "NewInitedFileInstance fail";
        return -1;
    }

    int ret = fileserv->Open(filename, userinfo, sessionId);
    if (ret != LIBCURVE_ERROR::OK) {
        LOG(ERROR) << "Open file failed, filename: " << filename
                   << ", retCode: " << ret;
        fileserv->UnInitialize();
        delete fileserv;
        return ret;
    }

    int fd = fdcount_.fetch_add(1, std::memory_order_relaxed);

    {
        WriteLockGuard lk(rwlock_);
        fileserviceMap_[fd] = fileserv;
    }

    LOG(INFO) << "Open success, filname = " << filename << ", fd = " << fd;
    openedFileNum_ << 1;

    return fd;
}

int FileClient::ReOpen(const std::string& filename,
                       const std::string& sessionId,
                       const UserInfo& userInfo,
                       std::string* newSessionId) {
    FileInstance* fileInstance = FileInstance::NewInitedFileInstance(
        clientconfig_.GetFileServiceOption(), mdsClient_, filename, userInfo,
        false);
    if (nullptr == fileInstance) {
        LOG(ERROR) << "NewInitedFileInstance fail";
        return -1;
    }

    int ret = fileInstance->ReOpen(filename, sessionId, userInfo, newSessionId);
    if (ret != LIBCURVE_ERROR::OK) {
        LOG(ERROR) << "ReOpen file fail, retCode = " << ret;
        fileInstance->UnInitialize();
        delete fileInstance;
        return ret;
    }

    int fd = fdcount_.fetch_add(1, std::memory_order_relaxed);

    {
        WriteLockGuard wlk(rwlock_);
        fileserviceMap_[fd] = fileInstance;
    }

    openedFileNum_ << 1;

    return fd;
}

int FileClient::Open4ReadOnly(const std::string& filename,
    const UserInfo_t& userinfo) {

    FileInstance* instance = FileInstance::Open4Readonly(
        clientconfig_.GetFileServiceOption(), mdsClient_, filename, userinfo);

    if (instance == nullptr) {
        LOG(ERROR) << "Open4Readonly failed, filename = " << filename;
        return -1;
    }

    int fd = fdcount_.fetch_add(1, std::memory_order_relaxed);

    {
        WriteLockGuard lk(rwlock_);
        fileserviceMap_[fd] = instance;
    }

    openedFileNum_ << 1;

    return fd;
}

int FileClient::Create(const std::string& filename,
    const UserInfo_t& userinfo, size_t size) {
    LIBCURVE_ERROR ret;
    if (mdsClient_ != nullptr) {
        ret = mdsClient_->CreateFile(filename, userinfo, size);
        LOG_IF(ERROR, ret != LIBCURVE_ERROR::OK)
            << "Create file failed, filename: " << filename << ", ret: " << ret;
    } else {
        LOG(ERROR) << "global mds client not inited!";
        return -LIBCURVE_ERROR::FAILED;
    }
    return -ret;
}

int FileClient::Create2(const std::string& filename,
    const UserInfo_t& userinfo, size_t size,
    uint64_t stripeUnit, uint64_t stripeCount) {
    LIBCURVE_ERROR ret;
    if (mdsClient_ != nullptr) {
        ret = mdsClient_->CreateFile(filename, userinfo, size, true,
                                     stripeUnit, stripeCount);
        LOG_IF(ERROR, ret != LIBCURVE_ERROR::OK)
            << "Create file failed, filename: " << filename << ", ret: " << ret;
    } else {
        LOG(ERROR) << "global mds client not inited!";
        return -LIBCURVE_ERROR::FAILED;
    }
    return -ret;
}

int FileClient::Read(int fd, char* buf, off_t offset, size_t len) {
    // 长度为0，直接返回，不做任何操作
    if (len == 0) {
        return -LIBCURVE_ERROR::OK;
    }

    if (CheckAligned(offset, len) == false) {
        LOG(ERROR) << "Read request not aligned, length = " << len
                   << ", offset = " << offset << ", fd = " << fd;
        return -LIBCURVE_ERROR::NOT_ALIGNED;
    }

    ReadLockGuard lk(rwlock_);
    if (CURVE_UNLIKELY(fileserviceMap_.find(fd) == fileserviceMap_.end())) {
        LOG(ERROR) << "invalid fd!";
        return -LIBCURVE_ERROR::BAD_FD;
    }

    return fileserviceMap_[fd]->Read(buf, offset, len);
}

int FileClient::Write(int fd, const char* buf, off_t offset, size_t len) {
    // 长度为0，直接返回，不做任何操作
    if (len == 0) {
        return -LIBCURVE_ERROR::OK;
    }

    if (CheckAligned(offset, len) == false) {
        LOG(ERROR) << "Write request not aligned, length = " << len
                   << ", offset = " << offset << ", fd = " << fd;
        return -LIBCURVE_ERROR::NOT_ALIGNED;
    }

    ReadLockGuard lk(rwlock_);
    if (CURVE_UNLIKELY(fileserviceMap_.find(fd) == fileserviceMap_.end())) {
        LOG(ERROR) << "invalid fd!";
        return -LIBCURVE_ERROR::BAD_FD;
    }

    return fileserviceMap_[fd]->Write(buf, offset, len);
}

int FileClient::AioRead(int fd, CurveAioContext* aioctx,
                        UserDataType dataType) {
    // 长度为0，直接返回，不做任何操作
    if (aioctx->length == 0) {
        return -LIBCURVE_ERROR::OK;
    }

    if (CheckAligned(aioctx->offset, aioctx->length) == false) {
        LOG(ERROR) << "AioRead request not aligned, length = " << aioctx->length
                   << ", offset = " << aioctx->offset << ", fd = " << fd;
        return -LIBCURVE_ERROR::NOT_ALIGNED;
    }

    int ret = -LIBCURVE_ERROR::FAILED;
    ReadLockGuard lk(rwlock_);
    if (CURVE_UNLIKELY(fileserviceMap_.find(fd) == fileserviceMap_.end())) {
        LOG(ERROR) << "invalid fd!";
        ret = -LIBCURVE_ERROR::BAD_FD;
    } else {
        ret = fileserviceMap_[fd]->AioRead(aioctx, dataType);
    }

    return ret;
}

int FileClient::AioWrite(int fd, CurveAioContext* aioctx,
                         UserDataType dataType) {
    // 长度为0，直接返回，不做任何操作
    if (aioctx->length == 0) {
        return -LIBCURVE_ERROR::OK;
    }

    if (CheckAligned(aioctx->offset, aioctx->length) == false) {
        LOG(ERROR) << "AioWrite request not aligned, length = "
                   << aioctx->length << ", offset = " << aioctx->offset
                   << ", fd = " << fd;
        return -LIBCURVE_ERROR::NOT_ALIGNED;
    }

    int ret = -LIBCURVE_ERROR::FAILED;
    ReadLockGuard lk(rwlock_);
    if (CURVE_UNLIKELY(fileserviceMap_.find(fd) == fileserviceMap_.end())) {
        LOG(ERROR) << "invalid fd!";
        ret = -LIBCURVE_ERROR::BAD_FD;
    } else {
        ret = fileserviceMap_[fd]->AioWrite(aioctx, dataType);
    }

    return ret;
}

int FileClient::Rename(const UserInfo_t& userinfo,
    const std::string& oldpath, const std::string& newpath) {
    LIBCURVE_ERROR ret;
    if (mdsClient_ != nullptr) {
        ret = mdsClient_->RenameFile(userinfo, oldpath, newpath);
        LOG_IF(ERROR, ret != LIBCURVE_ERROR::OK)
            << "Rename failed, OldPath: " << oldpath
            << ", NewPath: " << newpath
            << ", ret: " << ret;
    } else {
        LOG(ERROR) << "global mds client not inited!";
        return -LIBCURVE_ERROR::FAILED;
    }
    return -ret;
}

int FileClient::Extend(const std::string& filename,
    const UserInfo_t& userinfo, uint64_t newsize) {
    LIBCURVE_ERROR ret;
    if (mdsClient_ != nullptr) {
        ret = mdsClient_->Extend(filename, userinfo, newsize);
        LOG_IF(ERROR, ret != LIBCURVE_ERROR::OK)
            << "Extend failed, filename: " << filename
            << ", NewSize: " << newsize
            << ", ret: " << ret;
    } else {
        LOG(ERROR) << "global mds client not inited!";
        return -LIBCURVE_ERROR::FAILED;
    }
    return -ret;
}

int FileClient::Unlink(const std::string& filename,
    const UserInfo_t& userinfo, bool deleteforce) {
    LIBCURVE_ERROR ret;
    if (mdsClient_ != nullptr) {
        ret = mdsClient_->DeleteFile(filename, userinfo, deleteforce);
        LOG_IF(ERROR, ret != LIBCURVE_ERROR::OK)
            << "Unlink failed, filename: " << filename
            << ", force: " << deleteforce
            << ", ret: " << ret;
    } else {
        LOG(ERROR) << "global mds client not inited!";
        return -LIBCURVE_ERROR::FAILED;
    }
    return -ret;
}

int FileClient::Recover(const std::string& filename,
    const UserInfo_t& userinfo, uint64_t fileId) {
    LIBCURVE_ERROR ret;
    if (mdsClient_ != nullptr) {
        ret = mdsClient_->RecoverFile(filename, userinfo, fileId);
        LOG_IF(ERROR, ret != LIBCURVE_ERROR::OK)
            << "Recover failed, filename: " << filename
            << ", ret: " << ret;
    } else {
        LOG(ERROR) << "global mds client not inited!";
        return -LIBCURVE_ERROR::FAILED;
    }
    return -ret;
}

int FileClient::StatFile(const std::string& filename,
    const UserInfo_t& userinfo, FileStatInfo* finfo) {
    FInfo_t fi;
    int ret;
    if (mdsClient_ != nullptr) {
        ret = mdsClient_->GetFileInfo(filename, userinfo, &fi);
        LOG_IF(ERROR, ret != LIBCURVE_ERROR::OK)
            << "StatFile failed, filename: " << filename << ", ret" << ret;
    } else {
        LOG(ERROR) << "global mds client not inited!";
        return -LIBCURVE_ERROR::FAILED;
    }

    if (ret == LIBCURVE_ERROR::OK) {
        finfo->id       = fi.id;
        finfo->parentid = fi.parentid;
        finfo->ctime    = fi.ctime;
        finfo->length   = fi.length;
        finfo->filetype = fi.filetype;
        finfo->stripeUnit = fi.stripeUnit;
        finfo->stripeCount = fi.stripeCount;

        memcpy(finfo->filename, fi.filename.c_str(),
                std::min(sizeof(finfo->filename), fi.filename.size() + 1));
        memcpy(finfo->owner, fi.owner.c_str(),
                std::min(sizeof(finfo->owner), fi.owner.size() + 1));

        finfo->fileStatus = static_cast<int>(fi.filestatus);
    }

    return -ret;
}

int FileClient::Listdir(const std::string& dirpath,
    const UserInfo_t& userinfo, std::vector<FileStatInfo>* filestatVec) {
    LIBCURVE_ERROR ret;
    if (mdsClient_ != nullptr) {
        ret = mdsClient_->Listdir(dirpath, userinfo, filestatVec);
        LOG_IF(ERROR,
               ret != LIBCURVE_ERROR::OK && ret != LIBCURVE_ERROR::NOTEXIST)
            << "Listdir failed, Path: " << dirpath << ", ret: " << ret;
    } else {
        LOG(ERROR) << "global mds client not inited!";
        return -LIBCURVE_ERROR::FAILED;
    }
    return -ret;
}

int FileClient::Mkdir(const std::string& dirpath, const UserInfo_t& userinfo) {
    LIBCURVE_ERROR ret;
    if (mdsClient_ != nullptr) {
        ret = mdsClient_->CreateFile(dirpath, userinfo, 0, false);
        if (ret != LIBCURVE_ERROR::OK) {
            if (ret == LIBCURVE_ERROR::EXISTS) {
                LOG(WARNING) << "Create directory failed, " << dirpath
                             << " already exists";
            } else {
                LOG_IF(ERROR, ret != LIBCURVE_ERROR::OK)
                    << "Create directory failed, dir: " << dirpath
                    << ", ret: " << ret;
            }
        }
    } else {
        LOG(ERROR) << "global mds client not inited!";
        return -LIBCURVE_ERROR::FAILED;
    }
    return -ret;
}

int FileClient::Rmdir(const std::string& dirpath, const UserInfo_t& userinfo) {
    LIBCURVE_ERROR ret;
    if (mdsClient_ != nullptr) {
        ret = mdsClient_->DeleteFile(dirpath, userinfo);
        LOG_IF(ERROR, ret != LIBCURVE_ERROR::OK)
            << "Rmdir failed, Path: " << dirpath << ", ret: " << ret;
    } else {
        LOG(ERROR) << "global mds client not inited!";
        return -LIBCURVE_ERROR::FAILED;
    }
    return -ret;
}

int FileClient::ChangeOwner(const std::string& filename,
    const std::string& newOwner, const UserInfo_t& userinfo) {
    LIBCURVE_ERROR ret;
    if (mdsClient_ != nullptr) {
        ret = mdsClient_->ChangeOwner(filename, newOwner, userinfo);
        LOG_IF(ERROR, ret != LIBCURVE_ERROR::OK)
            << "ChangeOwner failed, filename: " << filename << ", ret: " << ret;
    } else {
        LOG(ERROR) << "global mds client not inited!";
        return -LIBCURVE_ERROR::FAILED;
    }
    return -ret;
}

int FileClient::Close(int fd) {
    {
        ReadLockGuard lk(rwlock_);
        auto iter = fileserviceMap_.find(fd);
        if (iter == fileserviceMap_.end()) {
            LOG(ERROR) << "CloseFile failed not found fd = " << fd;
            return -LIBCURVE_ERROR::FAILED;
        }
    }

    int ret = fileserviceMap_[fd]->Close();
    if (ret == LIBCURVE_ERROR::OK ||
        ret == -LIBCURVE_ERROR::SESSION_NOT_EXIST) {
        fileserviceMap_[fd]->UnInitialize();

        {
            WriteLockGuard lk(rwlock_);
            delete fileserviceMap_[fd];
            fileserviceMap_.erase(fd);
        }

        LOG(INFO) << "CloseFile ok, fd = " << fd;

        openedFileNum_ << -1;

        return LIBCURVE_ERROR::OK;
    }

    LOG(ERROR) << "CloseFile failed fd = " << fd;
    return -LIBCURVE_ERROR::FAILED;
}

int FileClient::GetClusterId(char* buf, int len) {
    std::string result = GetClusterId();

    if (result.empty()) {
        return -LIBCURVE_ERROR::FAILED;
    }

    if (len >= result.size() + 1) {
        snprintf(buf, len, "%s", result.c_str());
        return LIBCURVE_ERROR::OK;
    }

    LOG(ERROR) << "buffer length is too small, "
               << "cluster id length is " << result.size() + 1;

    return -LIBCURVE_ERROR::FAILED;
}

std::string FileClient::GetClusterId() {
    if (mdsClient_ == nullptr) {
        LOG(ERROR) << "global mds client not inited!";
        return {};
    }

    ClusterContext clsctx;
    int ret = mdsClient_->GetClusterInfo(&clsctx);
    if (ret == LIBCURVE_ERROR::OK) {
        return clsctx.clusterId;
    }

    LOG(ERROR) << "GetClusterId failed, ret: " << ret;
    return {};
}

int FileClient::GetFileInfo(int fd, FInfo* finfo) {
    int ret = -LIBCURVE_ERROR::FAILED;
    ReadLockGuard lk(rwlock_);

    auto iter = fileserviceMap_.find(fd);
    if (iter != fileserviceMap_.end()) {
        *finfo = iter->second->GetCurrentFileInfo();
        return 0;
    }

    return ret;
}

bool FileClient::StartDummyServer() {
    if (!clientconfig_.GetFileServiceOption().commonOpt.mdsRegisterToMDS) {
        LOG(INFO) << "No need register to MDS";
        ClientDummyServerInfo::GetInstance().SetRegister(false);
        return true;
    }

    static std::once_flag flag;
    uint16_t dummyServerStartPort = clientconfig_.GetDummyserverStartPort();
    std::call_once(flag, [&]() {
        while (dummyServerStartPort < PORT_LIMIT) {
            int ret = brpc::StartDummyServerAt(dummyServerStartPort);
            if (ret >= 0) {
                LOG(INFO) << "Start dummy server success, listen port = "
                          << dummyServerStartPort;
                break;
            }

            ++dummyServerStartPort;
        }
    });

    if (dummyServerStartPort >= PORT_LIMIT) {
        LOG(ERROR) << "Start dummy server failed!";
        return false;
    }

    // 获取本地ip
    std::string ip;
    if (!common::NetCommon::GetLocalIP(&ip)) {
        LOG(ERROR) << "Get local ip failed!";
        return false;
    }

    ClientDummyServerInfo::GetInstance().SetRegister(true);
    ClientDummyServerInfo::GetInstance().SetPort(dummyServerStartPort);
    ClientDummyServerInfo::GetInstance().SetIP(ip);

    return true;
}

}   // namespace client
}   // namespace curve


// 全局初始化与反初始化
int GlobalInit(const char* configpath);
void GlobalUnInit();

int Init(const char* path) {
    return GlobalInit(path);
}

int Open4Qemu(const char* filename) {
    curve::client::UserInfo_t userinfo;
    std::string realname;
    bool ret = curve::client::ServiceHelper::GetUserInfoFromFilename(filename,
               &realname, &userinfo.owner);
    if (!ret) {
        LOG(ERROR) << "get user info from filename failed!";
        return -LIBCURVE_ERROR::FAILED;
    }

    if (globalclient == nullptr) {
        LOG(ERROR) << "not inited!";
        return -LIBCURVE_ERROR::FAILED;
    }

    return globalclient->Open(realname, userinfo);
}

int Extend4Qemu(const char* filename, int64_t newsize) {
    curve::client::UserInfo_t userinfo;
    std::string realname;
    bool ret = curve::client::ServiceHelper::GetUserInfoFromFilename(filename,
               &realname, &userinfo.owner);
    if (!ret) {
        LOG(ERROR) << "get user info from filename failed!";
        return -LIBCURVE_ERROR::FAILED;
    }
    if (globalclient == nullptr) {
        LOG(ERROR) << "not inited!";
        return -LIBCURVE_ERROR::FAILED;
    }
    if (newsize <= 0) {
        LOG(ERROR) << "File size is wrong, " << newsize;
        return -LIBCURVE_ERROR::FAILED;
    }

    return globalclient->Extend(realname, userinfo,
            static_cast<uint64_t>(newsize));
}

int Open(const char* filename, const C_UserInfo_t* userinfo) {
    if (globalclient == nullptr) {
        LOG(ERROR) << "not inited!";
        return -LIBCURVE_ERROR::FAILED;
    }

    return globalclient->Open(filename,
            UserInfo(userinfo->owner, userinfo->password));
}

int Read(int fd, char* buf, off_t offset, size_t length) {
    if (globalclient == nullptr) {
        LOG(ERROR) << "not inited!";
        return -LIBCURVE_ERROR::FAILED;
    }

    return globalclient->Read(fd, buf, offset, length);
}

int Write(int fd, const char* buf, off_t offset, size_t length) {
    if (globalclient == nullptr) {
        LOG(ERROR) << "not inited!";
        return -LIBCURVE_ERROR::FAILED;
    }

    return globalclient->Write(fd, buf, offset, length);
}

int AioRead(int fd, CurveAioContext* aioctx) {
    if (globalclient == nullptr) {
        LOG(ERROR) << "not inited!";
        return -LIBCURVE_ERROR::FAILED;
    }

    DVLOG(9) << "offset: " << aioctx->offset
        << " length: " << aioctx->length
        << " op: " << aioctx->op;
    return globalclient->AioRead(fd, aioctx);
}

int AioWrite(int fd, CurveAioContext* aioctx) {
    if (globalclient == nullptr) {
        LOG(ERROR) << "not inited!";
        return -LIBCURVE_ERROR::FAILED;
    }

    DVLOG(9) << "offset: " << aioctx->offset
        << " length: " << aioctx->length
        << " op: " << aioctx->op
        << " buf: " << *(unsigned int*)aioctx->buf;
    return globalclient->AioWrite(fd, aioctx);
}

int Create(const char* filename, const C_UserInfo_t* userinfo, size_t size) {
    if (globalclient == nullptr) {
        LOG(ERROR) << "not inited!";
        return -LIBCURVE_ERROR::FAILED;
    }

    return globalclient->Create(filename,
            UserInfo(userinfo->owner, userinfo->password), size);
}

int Create2(const char* filename, const C_UserInfo_t* userinfo, size_t size,
                                uint64_t stripeUnit, uint64_t stripeCount) {
    if (globalclient == nullptr) {
        LOG(ERROR) << "not inited!";
        return -LIBCURVE_ERROR::FAILED;
    }

    return globalclient->Create2(filename,
            UserInfo(userinfo->owner, userinfo->password),
                       size, stripeUnit, stripeCount);
}

int Rename(const C_UserInfo_t* userinfo,
    const char* oldpath, const char* newpath) {
    if (globalclient == nullptr) {
        LOG(ERROR) << "not inited!";
        return -LIBCURVE_ERROR::FAILED;
    }

    return globalclient->Rename(UserInfo(userinfo->owner, userinfo->password),
            oldpath, newpath);
}

int Extend(const char* filename,
    const C_UserInfo_t* userinfo, uint64_t newsize) {
    if (globalclient == nullptr) {
        LOG(ERROR) << "not inited!";
        return -LIBCURVE_ERROR::FAILED;
    }

    return globalclient->Extend(filename,
            UserInfo(userinfo->owner, userinfo->password), newsize);
}

int Unlink(const char* filename, const C_UserInfo_t* userinfo) {
    if (globalclient == nullptr) {
        LOG(ERROR) << "not inited!";
        return -LIBCURVE_ERROR::FAILED;
    }

    return globalclient->Unlink(filename,
            UserInfo(userinfo->owner, userinfo->password));
}

int DeleteForce(const char* filename, const C_UserInfo_t* userinfo) {
    if (globalclient == nullptr) {
        LOG(ERROR) << "not inited!";
        return -LIBCURVE_ERROR::FAILED;
    }

    return globalclient->Unlink(filename,
            UserInfo(userinfo->owner, userinfo->password),
            true);
}

int Recover(const char* filename, const C_UserInfo_t* userinfo,
                                  uint64_t fileId) {
    if (globalclient == nullptr) {
        LOG(ERROR) << "not inited!";
        return -LIBCURVE_ERROR::FAILED;
    }

    return globalclient->Recover(filename,
            UserInfo(userinfo->owner, userinfo->password),
            fileId);
}

DirInfo_t* OpenDir(const char* dirpath, const C_UserInfo_t* userinfo) {
    if (globalclient == nullptr) {
        LOG(ERROR) << "not inited!";
        return nullptr;
    }

    DirInfo_t* dirinfo = new (std::nothrow) DirInfo_t;
    dirinfo->dirpath = const_cast<char*>(dirpath);
    dirinfo->userinfo = const_cast<C_UserInfo_t*>(userinfo);
    dirinfo->fileStat = nullptr;

    return dirinfo;
}

int Listdir(DirInfo_t* dirinfo) {
    if (globalclient == nullptr) {
        LOG(ERROR) << "not inited!";
        return -LIBCURVE_ERROR::FAILED;
    }

    if (dirinfo == nullptr) {
        LOG(ERROR) << "dir not opened!";
        return -LIBCURVE_ERROR::FAILED;
    }

    std::vector<FileStatInfo> fileStat;
    int ret = globalclient->Listdir(dirinfo->dirpath,
             UserInfo(dirinfo->userinfo->owner, dirinfo->userinfo->password),
             &fileStat);

    dirinfo->dirSize = fileStat.size();
    dirinfo->fileStat = new (std::nothrow) FileStatInfo_t[dirinfo->dirSize];

    if (dirinfo->fileStat == nullptr) {
        LOG(ERROR) << "allocate FileStatInfo memory failed!";
        return -LIBCURVE_ERROR::FAILED;
    }

    for (int i = 0; i < dirinfo->dirSize; i++) {
        dirinfo->fileStat[i].id = fileStat[i].id;
        dirinfo->fileStat[i].parentid = fileStat[i].parentid;
        dirinfo->fileStat[i].filetype = fileStat[i].filetype;
        dirinfo->fileStat[i].length = fileStat[i].length;
        dirinfo->fileStat[i].ctime = fileStat[i].ctime;
        memset(dirinfo->fileStat[i].owner, 0, NAME_MAX_SIZE);
        memcpy(dirinfo->fileStat[i].owner, fileStat[i].owner, NAME_MAX_SIZE);
        memset(dirinfo->fileStat[i].filename, 0, NAME_MAX_SIZE);
        memcpy(dirinfo->fileStat[i].filename, fileStat[i].filename,
                NAME_MAX_SIZE);
    }

    return ret;
}

void CloseDir(DirInfo_t* dirinfo) {
    if (dirinfo != nullptr) {
        if (dirinfo->fileStat != nullptr) {
            delete[] dirinfo->fileStat;
        }
        delete dirinfo;
        LOG(INFO) << "close dir";
    }
}

int Mkdir(const char* dirpath, const C_UserInfo_t* userinfo) {
    if (globalclient == nullptr) {
        LOG(ERROR) << "not inited!";
        return -LIBCURVE_ERROR::FAILED;
    }

    return globalclient->Mkdir(dirpath,
            UserInfo(userinfo->owner, userinfo->password));
}

int Rmdir(const char* dirpath, const C_UserInfo_t* userinfo) {
    if (globalclient == nullptr) {
        LOG(ERROR) << "not inited!";
        return -LIBCURVE_ERROR::FAILED;
    }

    return globalclient->Rmdir(dirpath,
            UserInfo(userinfo->owner, userinfo->password));
}

int Close(int fd) {
    if (globalclient == nullptr) {
        LOG(ERROR) << "not inited!";
        return -LIBCURVE_ERROR::FAILED;
    }

    return globalclient->Close(fd);
}

int StatFile4Qemu(const char* filename, FileStatInfo* finfo) {
    curve::client::UserInfo_t userinfo;
    std::string realname;
    bool ret = curve::client::ServiceHelper::GetUserInfoFromFilename(filename,
               &realname, &userinfo.owner);
    if (!ret) {
        LOG(ERROR) << "get user info from filename failed!";
        return -LIBCURVE_ERROR::FAILED;
    }

    if (globalclient == nullptr) {
        LOG(ERROR) << "not inited!";
        return -LIBCURVE_ERROR::FAILED;
    }

    return globalclient->StatFile(realname, userinfo, finfo);
}

int StatFile(const char* filename,
    const C_UserInfo_t* cuserinfo, FileStatInfo* finfo) {
    if (globalclient == nullptr) {
        LOG(ERROR) << "not inited!";
        return -LIBCURVE_ERROR::FAILED;
    }

    curve::client::UserInfo_t userinfo(cuserinfo->owner, cuserinfo->password);
    return globalclient->StatFile(filename, userinfo, finfo);
}

int ChangeOwner(const char* filename,
    const char* newOwner, const C_UserInfo_t* cuserinfo) {
    if (globalclient == nullptr) {
        LOG(ERROR) << "not inited!";
        return -LIBCURVE_ERROR::FAILED;
    }

    curve::client::UserInfo_t userinfo(cuserinfo->owner, cuserinfo->password);
    return globalclient->ChangeOwner(filename, newOwner, userinfo);
}

void UnInit() {
    GlobalUnInit();
}

int GetClusterId(char* buf, int len) {
    if (globalclient == nullptr) {
        LOG(ERROR) << "not inited!";
        return -LIBCURVE_ERROR::FAILED;
    }

    return globalclient->GetClusterId(buf, len);
}

int GlobalInit(const char* path) {
    int ret = 0;
    if (globalclientinited_) {
        LOG(INFO) << "global cient already inited!";
        return LIBCURVE_ERROR::OK;
    }

    if (globalclient == nullptr) {
        globalclient = new (std::nothrow) curve::client::FileClient();
        if (globalclient != nullptr) {
            ret = globalclient->Init(path);
            if (ret == LIBCURVE_ERROR::OK) {
                LOG(INFO) << "create global client instance success!";
            } else {
                delete globalclient;
                globalclient = nullptr;
                LOG(ERROR) << "init global client instance failed!";
            }
        } else {
            ret = -1;
            LOG(ERROR) << "create global client instance fail!";
        }
    }
    globalclientinited_ = ret == 0;
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
