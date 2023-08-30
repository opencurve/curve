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
#include <cstdio>
#include <memory>
#include <mutex>   // NOLINT
#include <thread>  // NOLINT
#include <utility>
#include <list>

#include "include/client/libcurve.h"
#include "include/client/libcurve_define.h"
#include "include/curve_compiler_specific.h"
#include "src/client/client_common.h"
#include "src/client/client_config.h"
#include "src/client/file_instance.h"
#include "src/client/iomanager4file.h"
#include "src/client/service_helper.h"
#include "src/client/source_reader.h"
#include "src/common/curve_version.h"
#include "src/common/net_common.h"
#include "src/common/uuid.h"
#include "src/common/string_util.h"

bool globalclientinited_ = false;
curve::client::FileClient *globalclient = nullptr;

using curve::client::UserInfo;

namespace brpc {
DECLARE_int32(health_check_interval);
}  // namespace brpc

namespace curve {
namespace client {

using curve::common::ReadLockGuard;
using curve::common::WriteLockGuard;

namespace {

constexpr int kDummyServerPortLimit = 65535;
constexpr int kProcessNameMax = 32;
char g_processname[kProcessNameMax];

class LoggerGuard {
 private:
    friend void InitLogging(const std::string &confPath);

    explicit LoggerGuard(const std::string &confpath) {
        InitInternal(confpath);
    }

    ~LoggerGuard() {
        if (needShutdown_) {
            google::ShutdownGoogleLogging();
        }
    }

    void InitInternal(const std::string &confpath);

 private:
    bool needShutdown_ = false;
};

void LoggerGuard::InitInternal(const std::string &confPath) {
    curve::common::Configuration conf;
    conf.SetConfigPath(confPath);

    bool rc = conf.LoadConfig();
    if (!rc) {
        LOG(ERROR) << "Load config failed, config path = " << confPath
                   << ", not init glog";
        return;
    }

    bool enable = true;
    LOG_IF(WARNING, !conf.GetBoolValue("global.logging.enable", &enable))
        << "config no `global.logging.enable`, enable logging by default";

    if (!enable) {
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

    std::string processName =
        std::string("libcurve-")
            .append(curve::common::UUIDGenerator().GenerateUUID().substr(0, 8));
    snprintf(g_processname, sizeof(g_processname), "%s", processName.c_str());
    google::InitGoogleLogging(g_processname);
    needShutdown_ = true;
}

void InitLogging(const std::string &confPath) {
    static LoggerGuard guard(confPath);
}

}  // namespace

FileClient::FileClient()
    : rwlock_(), fdcount_(0), fileserviceMap_(), clientconfig_(), mdsClient_(),
      csClient_(std::make_shared<ChunkServerClient>()),
      csBroadCaster_(std::make_shared<ChunkServerBroadCaster>(csClient_)),
      inited_(false),
      openedFileNum_("open_file_num_" + common::ToHexString(this)) {}

int FileClient::Init(const std::string& configpath) {
    if (inited_) {
        LOG(WARNING) << "already inited!";
        return 0;
    }

    InitLogging(configpath);
    curve::common::ExposeCurveVersion();

    if (-1 == clientconfig_.Init(configpath)) {
        LOG(ERROR) << "config init failed!";
        return -LIBCURVE_ERROR::FAILED;
    }

    auto tmpMdsClient = std::make_shared<MDSClient>();

    auto ret = tmpMdsClient->Initialize(
        clientconfig_.GetFileServiceOption().metaServerOpt);
    if (LIBCURVE_ERROR::OK != ret) {
        LOG(ERROR) << "Init global mds client failed!";
        return -LIBCURVE_ERROR::FAILED;
    }

    if (clientconfig_.GetFileServiceOption().commonOpt.turnOffHealthCheck) {
        brpc::FLAGS_health_check_interval = -1;
    }

    // set option for source reader
    SourceReader::GetInstance().SetOption(clientconfig_.GetFileServiceOption());

    bool rc = StartDummyServer();
    if (rc == false) {
        return -LIBCURVE_ERROR::FAILED;
    }

    mdsClient_ = std::move(tmpMdsClient);

    int rc2 = csClient_->Init(clientconfig_.GetFileServiceOption().csClientOpt);
    if (rc2 != 0) {
        LOG(ERROR) << "Init ChunkServer Client failed!";
        return -LIBCURVE_ERROR::FAILED;
    }
    rc2 = csBroadCaster_->Init(
        clientconfig_.GetFileServiceOption().csBroadCasterOpt);
    if (rc2 != 0) {
        LOG(ERROR) << "Init ChunkServer BroadCaster failed!";
        return -LIBCURVE_ERROR::FAILED;
    }
    inited_ = true;
    LOG(INFO) << "Init file client success";
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
    fileserviceFileNameMap_.clear();

    mdsClient_.reset();
    inited_ = false;
}

int FileClient::Open(const std::string &filename, const UserInfo_t &userinfo,
                     const OpenFlags &openflags) {
    LOG(INFO) << "Opening filename: " << filename << ", flags: " << openflags;
    ClientConfig clientConfig;
    if (openflags.confPath.empty()) {
        clientConfig = clientconfig_;
    } else {
        if (-1 == clientConfig.Init(openflags.confPath)) {
            LOG(ERROR) << "config init client failed!";
            return -LIBCURVE_ERROR::FAILED;
        }
    }

    auto mdsClient = std::make_shared<MDSClient>();
    auto res = mdsClient->Initialize(
        clientConfig.GetFileServiceOption().metaServerOpt);
    if (LIBCURVE_ERROR::OK != res) {
        LOG(ERROR) << "Init mds client failed!";
        return -LIBCURVE_ERROR::FAILED;
    }

    FileInstance *fileserv = FileInstance::NewInitedFileInstance(
        clientConfig.GetFileServiceOption(), mdsClient, filename, userinfo,
        openflags, false);
    if (fileserv == nullptr) {
        LOG(ERROR) << "NewInitedFileInstance fail";
        return -1;
    }

    int ret = fileserv->Open();
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
        fileserviceFileNameMap_[filename] = fileserv;
    }

    LOG(INFO) << "Open success, filname = " << filename << ", fd = " << fd;
    openedFileNum_ << 1;

    return fd;
}

int FileClient::Open4ReadOnly(const std::string &filename,
                              const UserInfo_t &userinfo, bool disableStripe) {
    FileInstance *instance = FileInstance::Open4Readonly(
        clientconfig_.GetFileServiceOption(), mdsClient_, filename, userinfo);

    if (instance == nullptr) {
        LOG(ERROR) << "Open4Readonly failed, filename = " << filename;
        return -1;
    }

    if (disableStripe) {
        instance->GetIOManager4File()->SetDisableStripe();
    }

    int fd = fdcount_.fetch_add(1, std::memory_order_relaxed);

    {
        WriteLockGuard lk(rwlock_);
        fileserviceMap_[fd] = instance;
        fileserviceFileNameMap_[filename] = instance;
    }

    openedFileNum_ << 1;

    return fd;
}

int FileClient::IncreaseEpoch(const std::string &filename,
                              const UserInfo_t &userinfo) {
    LOG(INFO) << "IncreaseEpoch, filename: " << filename;
    FInfo_t fi;
    FileEpoch_t fEpoch;
    std::list<CopysetPeerInfo<ChunkServerID>> csLocs;
    LIBCURVE_ERROR ret;
    if (mdsClient_ != nullptr) {
        ret = mdsClient_->IncreaseEpoch(filename, userinfo, &fi, &fEpoch,
                                        &csLocs);
        LOG_IF(ERROR, ret != LIBCURVE_ERROR::OK)
            << "IncreaseEpoch failed, filename: " << filename
            << ", ret: " << ret;
    } else {
        LOG(ERROR) << "global mds client not inited!";
        return -LIBCURVE_ERROR::FAILED;
    }

    int ret2 =
        csBroadCaster_->BroadCastFileEpoch(fEpoch.fileId, fEpoch.epoch, csLocs);
    LOG_IF(ERROR, ret2 != LIBCURVE_ERROR::OK)
        << "BroadCastEpoch failed, filename: " << filename << ", ret: " << ret2;

    // update epoch if file is already open
    auto it = fileserviceFileNameMap_.find(filename);
    if (it != fileserviceFileNameMap_.end()) {
        it->second->UpdateFileEpoch(fEpoch);
    }
    return ret2;
}

int FileClient::Create(const std::string& filename,
                       const UserInfo& userinfo,
                       size_t size) {
    LIBCURVE_ERROR ret;
    if (mdsClient_ != nullptr) {
        CreateFileContext ctx;
        ctx.pagefile = true;
        ctx.name = filename;
        ctx.user = userinfo;
        ctx.length = size;

        ret = mdsClient_->CreateFile(ctx);
        LOG_IF(ERROR, ret != LIBCURVE_ERROR::OK)
            << "Create file failed, filename: " << filename << ", ret: " << ret;
    } else {
        LOG(ERROR) << "global mds client not inited!";
        return -LIBCURVE_ERROR::FAILED;
    }
    return -ret;
}

int FileClient::Create2(const CreateFileContext& context) {
    LIBCURVE_ERROR ret;
    if (mdsClient_ != nullptr) {
        ret = mdsClient_->CreateFile(context);
        LOG_IF(ERROR, ret != LIBCURVE_ERROR::OK)
                << "Create file failed, filename: " << context.name
                << ", ret: " << ret;
    } else {
        LOG(ERROR) << "global mds client not inited!";
        return -LIBCURVE_ERROR::FAILED;
    }
    return -ret;
}

int FileClient::Read(int fd, char *buf, off_t offset, size_t len) {
    //Length is 0, returns directly without any operation
    if (len == 0) {
        return -LIBCURVE_ERROR::OK;
    }

    ReadLockGuard lk(rwlock_);
    if (CURVE_UNLIKELY(fileserviceMap_.find(fd) == fileserviceMap_.end())) {
        LOG(ERROR) << "invalid fd!";
        return -LIBCURVE_ERROR::BAD_FD;
    }

    return fileserviceMap_[fd]->Read(buf, offset, len);
}

int FileClient::Write(int fd, const char *buf, off_t offset, size_t len) {
    //Length is 0, returns directly without any operation
    if (len == 0) {
        return -LIBCURVE_ERROR::OK;
    }

    ReadLockGuard lk(rwlock_);
    if (CURVE_UNLIKELY(fileserviceMap_.find(fd) == fileserviceMap_.end())) {
        LOG(ERROR) << "invalid fd!";
        return -LIBCURVE_ERROR::BAD_FD;
    }

    return fileserviceMap_[fd]->Write(buf, offset, len);
}

int FileClient::Discard(int fd, off_t offset, size_t length) {
    ReadLockGuard lk(rwlock_);
    auto iter = fileserviceMap_.find(fd);
    if (CURVE_UNLIKELY(iter == fileserviceMap_.end())) {
        LOG(ERROR) << "invalid fd, fd = " << fd;
        return -LIBCURVE_ERROR::BAD_FD;
    }

    return iter->second->Discard(offset, length);
}

int FileClient::AioRead(int fd, CurveAioContext *aioctx,
                        UserDataType dataType) {
    //Length is 0, returns directly without any operation
    if (aioctx->length == 0) {
        return -LIBCURVE_ERROR::OK;
    }

    int ret = -LIBCURVE_ERROR::FAILED;
    ReadLockGuard lk(rwlock_);
    auto it = fileserviceMap_.find(fd);
    if (CURVE_UNLIKELY(it == fileserviceMap_.end())) {
        LOG(ERROR) << "invalid fd!";
        ret = -LIBCURVE_ERROR::BAD_FD;
    } else {
        ret = it->second->AioRead(aioctx, dataType);
    }

    return ret;
}

int FileClient::AioWrite(int fd, CurveAioContext *aioctx,
                         UserDataType dataType) {
    //Length is 0, returns directly without any operation

    if (aioctx->length == 0) {
        return -LIBCURVE_ERROR::OK;
    }

    int ret = -LIBCURVE_ERROR::FAILED;
    ReadLockGuard lk(rwlock_);
    auto it = fileserviceMap_.find(fd);
    if (CURVE_UNLIKELY(it == fileserviceMap_.end())) {
        LOG(ERROR) << "invalid fd!";
        ret = -LIBCURVE_ERROR::BAD_FD;
    } else {
        ret = it->second->AioWrite(aioctx, dataType);
    }

    return ret;
}

int FileClient::AioDiscard(int fd, CurveAioContext *aioctx) {
    ReadLockGuard lk(rwlock_);
    auto iter = fileserviceMap_.find(fd);
    if (CURVE_UNLIKELY(iter == fileserviceMap_.end())) {
        LOG(ERROR) << "invalid fd";
        return -LIBCURVE_ERROR::BAD_FD;
    } else {
        return iter->second->AioDiscard(aioctx);
    }
}

int FileClient::Rename(const UserInfo_t &userinfo, const std::string &oldpath,
                       const std::string &newpath) {
    LIBCURVE_ERROR ret;
    if (mdsClient_ != nullptr) {
        ret = mdsClient_->RenameFile(userinfo, oldpath, newpath);
        LOG_IF(ERROR, ret != LIBCURVE_ERROR::OK)
            << "Rename failed, OldPath: " << oldpath << ", NewPath: " << newpath
            << ", ret: " << ret;
    } else {
        LOG(ERROR) << "global mds client not inited!";
        return -LIBCURVE_ERROR::FAILED;
    }
    return -ret;
}

int FileClient::Extend(const std::string &filename, const UserInfo_t &userinfo,
                       uint64_t newsize) {
    LIBCURVE_ERROR ret;
    if (mdsClient_ != nullptr) {
        ret = mdsClient_->Extend(filename, userinfo, newsize);
        LOG_IF(ERROR, ret != LIBCURVE_ERROR::OK)
            << "Extend failed, filename: " << filename
            << ", NewSize: " << newsize << ", ret: " << ret;
    } else {
        LOG(ERROR) << "global mds client not inited!";
        return -LIBCURVE_ERROR::FAILED;
    }
    return -ret;
}

int FileClient::Unlink(const std::string &filename, const UserInfo_t &userinfo,
                       bool deleteforce) {
    LIBCURVE_ERROR ret;
    if (mdsClient_ != nullptr) {
        ret = mdsClient_->DeleteFile(filename, userinfo, deleteforce);
        LOG_IF(ERROR, ret != LIBCURVE_ERROR::OK)
            << "Unlink failed, filename: " << filename
            << ", force: " << deleteforce << ", ret: " << ret;
    } else {
        LOG(ERROR) << "global mds client not inited!";
        return -LIBCURVE_ERROR::FAILED;
    }
    return -ret;
}

int FileClient::Recover(const std::string &filename, const UserInfo_t &userinfo,
                        uint64_t fileId) {
    LIBCURVE_ERROR ret;
    if (mdsClient_ != nullptr) {
        ret = mdsClient_->RecoverFile(filename, userinfo, fileId);
        LOG_IF(ERROR, ret != LIBCURVE_ERROR::OK)
            << "Recover failed, filename: " << filename << ", ret: " << ret;
    } else {
        LOG(ERROR) << "global mds client not inited!";
        return -LIBCURVE_ERROR::FAILED;
    }
    return -ret;
}

int FileClient::StatFile(int fd, FileStatInfo *finfo) {
    FInfo_t fi;
    {
        ReadLockGuard lk(rwlock_);
        auto iter = fileserviceMap_.find(fd);
        if (iter == fileserviceMap_.end()) {
            LOG(ERROR) << "StatFile failed not found fd = " << fd;
            return -LIBCURVE_ERROR::FAILED;
        }
        FileInstance *instance = fileserviceMap_[fd];
        fi = instance->GetCurrentFileInfo();
    }
    BuildFileStatInfo(fi, finfo);

    return LIBCURVE_ERROR::OK;
}

int FileClient::StatFile(const std::string &filename,
                         const UserInfo_t &userinfo, FileStatInfo *finfo) {
    FInfo_t fi;
    FileEpoch_t fEpoch;
    int ret;
    if (mdsClient_ != nullptr) {
        ret = mdsClient_->GetFileInfo(filename, userinfo, &fi, &fEpoch);
        LOG_IF(ERROR, ret != LIBCURVE_ERROR::OK)
            << "StatFile failed, filename: " << filename << ", ret" << ret;
    } else {
        LOG(ERROR) << "global mds client not inited!";
        return -LIBCURVE_ERROR::FAILED;
    }

    if (ret == LIBCURVE_ERROR::OK) {
        BuildFileStatInfo(fi, finfo);
    }

    return -ret;
}

int FileClient::Listdir(const std::string &dirpath, const UserInfo_t &userinfo,
                        std::vector<FileStatInfo> *filestatVec) {
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

int FileClient::Mkdir(const std::string &dirpath, const UserInfo_t &userinfo) {
    LIBCURVE_ERROR ret;
    if (mdsClient_ != nullptr) {
        CreateFileContext context;
        context.pagefile = false;
        context.user = userinfo;
        context.name = dirpath;
        ret = mdsClient_->CreateFile(context);
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

int FileClient::Rmdir(const std::string &dirpath, const UserInfo_t &userinfo) {
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

int FileClient::ChangeOwner(const std::string &filename,
                            const std::string &newOwner,
                            const UserInfo_t &userinfo) {
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
        std::string filename =
            fileserviceMap_[fd]->GetCurrentFileInfo().fullPathName;
        fileserviceMap_[fd]->UnInitialize();

        {
            WriteLockGuard lk(rwlock_);
            delete fileserviceMap_[fd];
            fileserviceMap_.erase(fd);
            fileserviceFileNameMap_.erase(filename);
        }

        LOG(INFO) << "CloseFile ok, fd = " << fd;

        openedFileNum_ << -1;

        return LIBCURVE_ERROR::OK;
    }

    LOG(ERROR) << "CloseFile failed fd = " << fd;
    return -LIBCURVE_ERROR::FAILED;
}

int FileClient::GetClusterId(char *buf, int len) {
    std::string result = GetClusterId();

    if (result.empty()) {
        return -LIBCURVE_ERROR::FAILED;
    }

    if (static_cast<size_t>(len) >= result.size() + 1) {
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

int FileClient::GetFileInfo(int fd, FInfo *finfo) {
    int ret = -LIBCURVE_ERROR::FAILED;
    ReadLockGuard lk(rwlock_);

    auto iter = fileserviceMap_.find(fd);
    if (iter != fileserviceMap_.end()) {
        *finfo = iter->second->GetCurrentFileInfo();
        return 0;
    }

    return ret;
}

std::vector<std::string> FileClient::ListPoolset() {
    std::vector<std::string> out;
    if (CURVE_UNLIKELY(mdsClient_ == nullptr)) {
        LOG(WARNING) << "global mds client not inited!";
        return out;
    }

    const auto ret = mdsClient_->ListPoolset(&out);
    LOG_IF(WARNING, ret != LIBCURVE_ERROR::OK)
            << "Failed to list poolset, error: " << ret;
    return out;
}

void FileClient::BuildFileStatInfo(const FInfo_t &fi, FileStatInfo *finfo) {
    finfo->id = fi.id;
    finfo->parentid = fi.parentid;
    finfo->ctime = fi.ctime;
    finfo->length = fi.length;
    finfo->blocksize = fi.blocksize;
    finfo->filetype = fi.filetype;
    finfo->stripeUnit = fi.stripeUnit;
    finfo->stripeCount = fi.stripeCount;

    memcpy(finfo->filename, fi.filename.c_str(),
               std::min(sizeof(finfo->filename), fi.filename.size() + 1));
    memcpy(finfo->owner, fi.owner.c_str(),
            std::min(sizeof(finfo->owner), fi.owner.size() + 1));

    finfo->fileStatus = static_cast<int>(fi.filestatus);
}

bool FileClient::StartDummyServer() {
    if (!clientconfig_.GetFileServiceOption().commonOpt.mdsRegisterToMDS) {
        LOG(INFO) << "No need register to MDS";
        ClientDummyServerInfo::GetInstance().SetRegister(false);
        return true;
    }

    // FIXME(wuhanqing): if curve-fuse and curve-client both want to start
    //                   dummy server, curve-fuse mount will fail
    static std::once_flag flag;
    int dummyServerStartPort = clientconfig_.GetDummyserverStartPort();
    std::call_once(flag, [&]() {
        while (dummyServerStartPort < kDummyServerPortLimit) {
            int ret = brpc::StartDummyServerAt(dummyServerStartPort);
            if (ret >= 0) {
                LOG(INFO) << "Start dummy server success, listen port = "
                          << dummyServerStartPort;
                break;
            }

            ++dummyServerStartPort;
        }
    });

    if (dummyServerStartPort >= kDummyServerPortLimit) {
        LOG(ERROR) << "Start dummy server failed!";
        return false;
    }

    //Obtain local IP
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

}  // namespace client
}  // namespace curve


//Global initialization and deinitialization
int GlobalInit(const char *configpath);
void GlobalUnInit();

int Init(const char *path) { return GlobalInit(path); }

int Open4Qemu(const char *filename) {
    curve::client::UserInfo_t userinfo;
    std::string realname;
    bool ret = curve::client::ServiceHelper::GetUserInfoFromFilename(
        filename, &realname, &userinfo.owner);
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

int IncreaseEpoch(const char *filename) {
    curve::client::UserInfo_t userinfo;
    std::string realname;
    bool ret = curve::client::ServiceHelper::GetUserInfoFromFilename(
        filename, &realname, &userinfo.owner);
    if (!ret) {
        LOG(ERROR) << "get user info from filename failed!";
        return -LIBCURVE_ERROR::FAILED;
    }

    if (globalclient == nullptr) {
        LOG(ERROR) << "not inited!";
        return -LIBCURVE_ERROR::FAILED;
    }

    return globalclient->IncreaseEpoch(realname, userinfo);
}

int Extend4Qemu(const char *filename, int64_t newsize) {
    curve::client::UserInfo_t userinfo;
    std::string realname;
    bool ret = curve::client::ServiceHelper::GetUserInfoFromFilename(
        filename, &realname, &userinfo.owner);
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

int Open(const char *filename, const C_UserInfo_t *userinfo) {
    if (globalclient == nullptr) {
        LOG(ERROR) << "not inited!";
        return -LIBCURVE_ERROR::FAILED;
    }

    return globalclient->Open(filename,
                              UserInfo(userinfo->owner, userinfo->password));
}

int Read(int fd, char *buf, off_t offset, size_t length) {
    if (globalclient == nullptr) {
        LOG(ERROR) << "not inited!";
        return -LIBCURVE_ERROR::FAILED;
    }

    return globalclient->Read(fd, buf, offset, length);
}

int Write(int fd, const char *buf, off_t offset, size_t length) {
    if (globalclient == nullptr) {
        LOG(ERROR) << "not inited!";
        return -LIBCURVE_ERROR::FAILED;
    }

    return globalclient->Write(fd, buf, offset, length);
}

int Discard(int fd, off_t offset, size_t length) {
    if (globalclient == nullptr) {
        LOG(ERROR) << "Not inited!";
        return -LIBCURVE_ERROR::FAILED;
    }

    return globalclient->Discard(fd, offset, length);
}

int AioRead(int fd, CurveAioContext *aioctx) {
    if (globalclient == nullptr) {
        LOG(ERROR) << "not inited!";
        return -LIBCURVE_ERROR::FAILED;
    }

    DVLOG(9) << "offset: " << aioctx->offset << " length: " << aioctx->length
             << " op: " << aioctx->op;
    return globalclient->AioRead(fd, aioctx);
}

int AioWrite(int fd, CurveAioContext *aioctx) {
    if (globalclient == nullptr) {
        LOG(ERROR) << "not inited!";
        return -LIBCURVE_ERROR::FAILED;
    }

    DVLOG(9) << "offset: " << aioctx->offset << " length: " << aioctx->length
             << " op: " << aioctx->op
             << " buf: " << *(unsigned int *)aioctx->buf;
    return globalclient->AioWrite(fd, aioctx);
}

int AioDiscard(int fd, CurveAioContext *aioctx) {
    if (globalclient == nullptr) {
        LOG(ERROR) << "Not inited!";
        return -LIBCURVE_ERROR::FAILED;
    }

    return globalclient->AioDiscard(fd, aioctx);
}

int Create(const char *filename, const C_UserInfo_t *userinfo, size_t size) {
    if (globalclient == nullptr) {
        LOG(ERROR) << "not inited!";
        return -LIBCURVE_ERROR::FAILED;
    }

    return globalclient->Create(
        filename, UserInfo(userinfo->owner, userinfo->password), size);
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

int Extend(const char *filename, const C_UserInfo_t *userinfo,
           uint64_t newsize) {
    if (globalclient == nullptr) {
        LOG(ERROR) << "not inited!";
        return -LIBCURVE_ERROR::FAILED;
    }

    return globalclient->Extend(
        filename, UserInfo(userinfo->owner, userinfo->password), newsize);
}

int Unlink(const char *filename, const C_UserInfo_t *userinfo) {
    if (globalclient == nullptr) {
        LOG(ERROR) << "not inited!";
        return -LIBCURVE_ERROR::FAILED;
    }

    return globalclient->Unlink(filename,
                                UserInfo(userinfo->owner, userinfo->password));
}

int DeleteForce(const char *filename, const C_UserInfo_t *userinfo) {
    if (globalclient == nullptr) {
        LOG(ERROR) << "not inited!";
        return -LIBCURVE_ERROR::FAILED;
    }

    return globalclient->Unlink(
        filename, UserInfo(userinfo->owner, userinfo->password), true);
}

int Recover(const char *filename, const C_UserInfo_t *userinfo,
            uint64_t fileId) {
    if (globalclient == nullptr) {
        LOG(ERROR) << "not inited!";
        return -LIBCURVE_ERROR::FAILED;
    }

    return globalclient->Recover(
        filename, UserInfo(userinfo->owner, userinfo->password), fileId);
}

DirInfo_t *OpenDir(const char *dirpath, const C_UserInfo_t *userinfo) {
    if (globalclient == nullptr) {
        LOG(ERROR) << "not inited!";
        return nullptr;
    }

    DirInfo_t *dirinfo = new (std::nothrow) DirInfo_t;
    dirinfo->dirpath = const_cast<char *>(dirpath);
    dirinfo->userinfo = const_cast<C_UserInfo_t *>(userinfo);
    dirinfo->fileStat = nullptr;

    return dirinfo;
}

int Listdir(DirInfo_t *dirinfo) {
    if (globalclient == nullptr) {
        LOG(ERROR) << "not inited!";
        return -LIBCURVE_ERROR::FAILED;
    }

    if (dirinfo == nullptr) {
        LOG(ERROR) << "dir not opened!";
        return -LIBCURVE_ERROR::FAILED;
    }

    std::vector<FileStatInfo> fileStat;
    int ret = globalclient->Listdir(
        dirinfo->dirpath,
        UserInfo(dirinfo->userinfo->owner, dirinfo->userinfo->password),
        &fileStat);

    dirinfo->dirSize = fileStat.size();
    dirinfo->fileStat = new (std::nothrow) FileStatInfo_t[dirinfo->dirSize];

    if (dirinfo->fileStat == nullptr) {
        LOG(ERROR) << "allocate FileStatInfo memory failed!";
        return -LIBCURVE_ERROR::FAILED;
    }

    for (uint64_t i = 0; i < dirinfo->dirSize; i++) {
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

void CloseDir(DirInfo_t *dirinfo) {
    if (dirinfo != nullptr) {
        if (dirinfo->fileStat != nullptr) {
            delete[] dirinfo->fileStat;
        }
        delete dirinfo;
        LOG(INFO) << "close dir";
    }
}

int Mkdir(const char *dirpath, const C_UserInfo_t *userinfo) {
    if (globalclient == nullptr) {
        LOG(ERROR) << "not inited!";
        return -LIBCURVE_ERROR::FAILED;
    }

    return globalclient->Mkdir(dirpath,
                               UserInfo(userinfo->owner, userinfo->password));
}

int Rmdir(const char *dirpath, const C_UserInfo_t *userinfo) {
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

int StatFile4Qemu(const char *filename, FileStatInfo *finfo) {
    curve::client::UserInfo_t userinfo;
    std::string realname;
    bool ret = curve::client::ServiceHelper::GetUserInfoFromFilename(
        filename, &realname, &userinfo.owner);
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

int StatFile(const char *filename, const C_UserInfo_t *cuserinfo,
             FileStatInfo *finfo) {
    if (globalclient == nullptr) {
        LOG(ERROR) << "not inited!";
        return -LIBCURVE_ERROR::FAILED;
    }

    curve::client::UserInfo_t userinfo(cuserinfo->owner, cuserinfo->password);
    return globalclient->StatFile(filename, userinfo, finfo);
}

int ChangeOwner(const char *filename, const char *newOwner,
                const C_UserInfo_t *cuserinfo) {
    if (globalclient == nullptr) {
        LOG(ERROR) << "not inited!";
        return -LIBCURVE_ERROR::FAILED;
    }

    curve::client::UserInfo_t userinfo(cuserinfo->owner, cuserinfo->password);
    return globalclient->ChangeOwner(filename, newOwner, userinfo);
}

void UnInit() { GlobalUnInit(); }

int GetClusterId(char *buf, int len) {
    if (globalclient == nullptr) {
        LOG(ERROR) << "not inited!";
        return -LIBCURVE_ERROR::FAILED;
    }

    return globalclient->GetClusterId(buf, len);
}

int GlobalInit(const char *path) {
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

const char *LibCurveErrorName(LIBCURVE_ERROR err) {
    switch (err) {
    case LIBCURVE_ERROR::OK:
        return "OK";
    case LIBCURVE_ERROR::EXISTS:
        return "EXISTS";
    case LIBCURVE_ERROR::FAILED:
        return "FAILED";
    case LIBCURVE_ERROR::DISABLEIO:
        return "DISABLEIO";
    case LIBCURVE_ERROR::AUTHFAIL:
        return "AUTHFAIL";
    case LIBCURVE_ERROR::DELETING:
        return "DELETING";
    case LIBCURVE_ERROR::NOTEXIST:
        return "NOTEXIST";
    case LIBCURVE_ERROR::UNDER_SNAPSHOT:
        return "UNDER_SNAPSHOT";
    case LIBCURVE_ERROR::NOT_UNDERSNAPSHOT:
        return "NOT_UNDERSNAPSHOT";
    case LIBCURVE_ERROR::DELETE_ERROR:
        return "DELETE_ERROR";
    case LIBCURVE_ERROR::NOT_ALLOCATE:
        return "NOT_ALLOCATE";
    case LIBCURVE_ERROR::NOT_SUPPORT:
        return "NOT_SUPPORT";
    case LIBCURVE_ERROR::NOT_EMPTY:
        return "NOT_EMPTY";
    case LIBCURVE_ERROR::NO_SHRINK_BIGGER_FILE:
        return "NO_SHRINK_BIGGER_FILE";
    case LIBCURVE_ERROR::SESSION_NOTEXISTS:
        return "SESSION_NOTEXISTS";
    case LIBCURVE_ERROR::FILE_OCCUPIED:
        return "FILE_OCCUPIED";
    case LIBCURVE_ERROR::PARAM_ERROR:
        return "PARAM_ERROR";
    case LIBCURVE_ERROR::INTERNAL_ERROR:
        return "INTERNAL_ERROR";
    case LIBCURVE_ERROR::CRC_ERROR:
        return "CRC_ERROR";
    case LIBCURVE_ERROR::INVALID_REQUEST:
        return "INVALID_REQUEST";
    case LIBCURVE_ERROR::DISK_FAIL:
        return "DISK_FAIL";
    case LIBCURVE_ERROR::NO_SPACE:
        return "NO_SPACE";
    case LIBCURVE_ERROR::NOT_ALIGNED:
        return "NOT_ALIGNED";
    case LIBCURVE_ERROR::BAD_FD:
        return "BAD_FD";
    case LIBCURVE_ERROR::LENGTH_NOT_SUPPORT:
        return "LENGTH_NOT_SUPPORT";
    case LIBCURVE_ERROR::SESSION_NOT_EXIST:
        return "SESSION_NOT_EXIST";
    case LIBCURVE_ERROR::STATUS_NOT_MATCH:
        return "STATUS_NOT_MATCH";
    case LIBCURVE_ERROR::DELETE_BEING_CLONED:
        return "DELETE_BEING_CLONED";
    case LIBCURVE_ERROR::CLIENT_NOT_SUPPORT_SNAPSHOT:
        return "CLIENT_NOT_SUPPORT_SNAPSHOT";
    case LIBCURVE_ERROR::SNAPSTHO_FROZEN:
        return "SNAPSTHO_FROZEN";
    case LIBCURVE_ERROR::RETRY_UNTIL_SUCCESS:
        return "RETRY_UNTIL_SUCCESS";
    case LIBCURVE_ERROR::EPOCH_TOO_OLD:
        return "EPOCH_TOO_OLD";
    case LIBCURVE_ERROR::UNKNOWN:
        break;
    }

    static thread_local char message[64];
    snprintf(message, sizeof(message), "Unknown[%d]", static_cast<int>(err));
    return message;
}
