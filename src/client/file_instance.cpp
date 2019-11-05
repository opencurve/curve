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
#include "src/client/session_map.h"

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

bool FileInstance::Initialize(const std::string& filename,
                              MDSClient* mdsclient,
                              const UserInfo_t& userinfo,
                              FileServiceOption_t fileservicopt,
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
            return false;
        }

        finfo_.userinfo = userinfo;
        mdsclient_ = mdsclient;

        finfo_.fullPathName = filename;

        if (!iomanager4file_.Initialize(filename, fileopt_.ioOpt, mdsclient_)) {
            LOG(ERROR) << "Init io context manager failed!";
            break;
        }

        iomanager4file_.UpdataFileInfo(finfo_);

        leaseexcutor_ = new (std::nothrow) LeaseExcutor(fileopt_.leaseOpt,
                                finfo_.userinfo, mdsclient_, &iomanager4file_);
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
    // 文件在退出的时候需要先将io manager退出，再退出lease续约线程。
    // 因为如果后台集群重新部署了，需要通过lease续约来获取当前session状态
    // 这样在session过期后才能将inflight RPC正确回收掉。
    iomanager4file_.UnInitialize();
    if (leaseexcutor_ != nullptr) {
        leaseexcutor_->Stop();
        delete leaseexcutor_;
        leaseexcutor_ = nullptr;
    }
}

int FileInstance::Read(char* buf, off_t offset, size_t length) {
    return iomanager4file_.Read(buf, offset, length, mdsclient_);
}

int FileInstance::Write(const char* buf, off_t offset, size_t len) {
    if (readonly_) {
        DVLOG(9) << "open with read only, do not support write!";
        return -1;
    }
    return iomanager4file_.Write(buf, offset, len, mdsclient_);
}

int FileInstance::AioRead(CurveAioContext* aioctx) {
    return iomanager4file_.AioRead(aioctx, mdsclient_);
}

int FileInstance::AioWrite(CurveAioContext* aioctx) {
    if (readonly_) {
        DVLOG(9) << "open with read only, do not support write!";
        return -1;
    }
    return iomanager4file_.AioWrite(aioctx, mdsclient_);
}

// 两种场景会造成在Open的时候返回LIBCURVE_ERROR::FILE_OCCUPIED
// 1. 强制重启qemu不会调用close逻辑，然后启动的时候原来的文件sessio还没过期.
//    导致再次去发起open的时候，返回被占用，这种情况可以通过load sessionmap
//    拿到已有的session，再去执行refresh。
// 2. 由于网络原因，导致open rpc超时，然后再去重试的时候就会返回FILE_OCCUPIED
//    这时候当前还没有成功打开，所以还没有存储该session信息，所以无法通过refresh
//    再去打开，所以这时候需要获取mds一侧session lease时长，然后在client这一侧
//    等待一段时间再去Open，如果依然失败，就向上层返回失败。
int FileInstance::Open(const std::string& filename, UserInfo_t userinfo) {
    LeaseSession_t  lease;
    int ret = LIBCURVE_ERROR::FAILED;

    ret = mdsclient_->OpenFile(filename, finfo_.userinfo, &finfo_, &lease);
    if (LIBCURVE_ERROR::FILE_OCCUPIED == ret) {
        SessionMap sm;
        int r = 0;
        std::string sessionid = sm.GetFileSessionID(
            fileopt_.sessionmapOpt.sessionmap_path, filename);
        if (sessionid.empty()) {
            LOG(WARNING) << "get file session id failed!";
            return -ret;
        }

        leaseRefreshResult resp;
        r = mdsclient_->RefreshSession(filename, userinfo, sessionid, &resp);

        if (r != LIBCURVE_ERROR::OK) {
            return -ret;
        }

        r = GetFileInfo(filename, &finfo_);
        if (r != LIBCURVE_ERROR::OK) {
            return -ret;
        }

        ret = LIBCURVE_ERROR::OK;
    } else if (ret != LIBCURVE_ERROR::OK) {
        LOG(ERROR) << "Open file failed! filename = " << filename;
        return -ret;
    }

    if (ret == LIBCURVE_ERROR::OK) {
        SessionMap sm;
        int r = sm.PersistSessionMapWithLock(
        fileopt_.sessionmapOpt.sessionmap_path, filename, lease.sessionID);
        LOG_IF(WARNING, r != 0) << "persist file session id failed!";

        ret = leaseexcutor_->Start(finfo_, lease) ? LIBCURVE_ERROR::OK
                                                  : LIBCURVE_ERROR::FAILED;
    }

    return -ret;
}

int FileInstance::GetFileInfo(const std::string& filename, FInfo_t* fi) {
    LIBCURVE_ERROR ret = mdsclient_->GetFileInfo(filename, finfo_.userinfo, fi);
    return -ret;
}

int FileInstance::Close() {
    if (readonly_) {
        LOG(INFO) << "close read only file!" << finfo_.fullPathName;
        return 0;
    }

    LIBCURVE_ERROR ret = mdsclient_->CloseFile(finfo_.fullPathName,
                                finfo_.userinfo,
                                leaseexcutor_->GetLeaseSessionID());

    if (ret == LIBCURVE_ERROR::OK) {
        SessionMap sm;
        int r = sm.DelSessionID(fileopt_.sessionmapOpt.sessionmap_path,
                                finfo_.fullPathName);
        if (r != 0) {
            LOG(WARNING) << "session delete failed!";
        }
    }
    return -ret;
}
}   // namespace client
}   // namespace curve
