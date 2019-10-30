/*
 * Project: curve
 * File Created: Monday, 18th February 2019 6:25:25 pm
 * Author: tongguangxun
 * Copyright (c)￼ 2018 netease
 */
#include <glog/logging.h>
#include <bthread/bthread.h>

#include <thread>   // NOLINT
#include <chrono>   // NOLINT

#include "src/common/uuid.h"
#include "src/common/net_common.h"
#include "src/client/metacache.h"
#include "src/client/mds_client.h"
#include "src/common/timeutility.h"
#include "src/client/lease_excutor.h"

using curve::common::TimeUtility;
using curve::common::ReadLockGuard;
using curve::common::WriteLockGuard;

namespace curve {
namespace client {
MDSClient::MDSClient() {
    inited_   = false;
    channel_  = nullptr;
}

LIBCURVE_ERROR MDSClient::Initialize(const MetaServerOption_t& metaServerOpt) {
    if (inited_) {
        LOG(INFO) << "MDSClient already started!";
        return LIBCURVE_ERROR::OK;
    }

    lastWorkingMDSAddrIndex_ = -1;
    metaServerOpt_ = metaServerOpt;

    int rc = mdsClientBase_.Init(metaServerOpt_);
    if (rc !=0) {
        LOG(ERROR) << "mds client rpc base init failed!";
        return LIBCURVE_ERROR::FAILED;
    }

    confMetric_.rpcRetryTimes.set_value(metaServerOpt_.rpcRetryTimes);
    confMetric_.rpcTimeoutMs.set_value(metaServerOpt_.rpcTimeoutMs);
    std::string metaserverAddr;
    for (auto addr : metaServerOpt_.metaaddrvec) {
        metaserverAddr.append(addr).append("@");
    }
    confMetric_.metaserverAddr.set_value(metaserverAddr);

    LOG(INFO) << "MDS Client conf info: "
              << "rpcRetryTimes = " << metaServerOpt_.rpcRetryTimes
              << ", rpcTimeoutMs = " << metaServerOpt_.rpcTimeoutMs
              << ", retryIntervalUs = " << metaServerOpt_.retryIntervalUs;

    for (auto addr : metaServerOpt_.metaaddrvec) {
        lastWorkingMDSAddrIndex_++;

        channel_ = new (std::nothrow) brpc::Channel();
        if (channel_->Init(addr.c_str(), nullptr) != 0) {
            LOG(WARNING) << "Init channel failed!" << addr;
            continue;
        }
        mdsClientMetric_.metaserverAddr = addr;
        inited_ = true;
        return LIBCURVE_ERROR::OK;
    }

    LOG(ERROR) << "init failed!";
    return LIBCURVE_ERROR::FAILED;
}

void MDSClient::UnInitialize() {
    delete channel_;
    channel_ = nullptr;
    inited_ = false;
}

// 切换mds地址
// 如果mds集群编号分别为：0、1、2、3、4，且lastWorkingMDSAddrIndex_ = 2，那么
// ChangeMDServer尝试顺序是3，4，0，1.且只要有一个channel init成功就返回。
bool MDSClient::ChangeMDServer(int* mdsAddrleft) {
    bool createRet = false;
    int addrSize = metaServerOpt_.metaaddrvec.size();
    int nextMDSAddrIndex = (lastWorkingMDSAddrIndex_ + 1) % addrSize;

    while (1) {
        if (nextMDSAddrIndex == lastWorkingMDSAddrIndex_) {
            LOG_EVERY_N(ERROR, 10) << "have retried all mds address!";
            break;
        }

        (*mdsAddrleft)--;

        {
            // 加写锁，后续其他线程如果调用，则阻塞
            std::unique_lock<bthread::Mutex> lk(mutex_);

            // 重新创建之前需要将原来的channel析构掉，否则会造成第二次链接失败
            delete channel_;
            channel_ = new (std::nothrow) brpc::Channel();

            int ret = channel_->Init(metaServerOpt_.
                                metaaddrvec[nextMDSAddrIndex].c_str(), nullptr);
            if (ret != 0) {
                LOG(WARNING) << "Init channel failed!";
                continue;
            }

            createRet = true;

            mdsClientMetric_.mdsServerChangeTimes << 1;
            mdsClientMetric_.metaserverAddr = metaServerOpt_.
                                                metaaddrvec[nextMDSAddrIndex];
            break;
        }

        ++nextMDSAddrIndex;
        nextMDSAddrIndex %= addrSize;
    }

    lastWorkingMDSAddrIndex_ = nextMDSAddrIndex;
    return createRet;
}

bool MDSClient::UpdateRetryinfoOrChangeServer(int* retrycount,
                                              int* mdsAddrleft,
                                              bool sync) {
    uint64_t retryTime = sync ? metaServerOpt_.synchronizeRPCRetryTime
                              : metaServerOpt_.rpcRetryTimes;
    (*retrycount)++;
    if (*retrycount >= retryTime) {
        if (*mdsAddrleft > 0 && ChangeMDServer(mdsAddrleft)) {
            // 切换mds地址，重新在新的mds addr上重试
            *retrycount = 0;
        } else {
            LOG(WARNING) << "retry failed!";
            bthread_usleep(metaServerOpt_.retryIntervalUs);
            return false;
        }
    } else {
        bthread_usleep(metaServerOpt_.retryIntervalUs);
    }
    return true;
}

LIBCURVE_ERROR MDSClient::Register(const std::string& ip,
                                   uint16_t port) {
    // 记录当前mds重试次数
    int count = 0;
    // 记录还没重试的mds addr数量
    int mdsAddrleft = metaServerOpt_.metaaddrvec.size() - 1;

    while (count < metaServerOpt_.synchronizeRPCRetryTime) {
        brpc::Controller cntl;
        curve::mds::RegistClientResponse response;

        mdsClientMetric_.registerClient.qps.count << 1;
        {
            LatencyGuard lg(&mdsClientMetric_.registerClient.latency);
            std::unique_lock<bthread::Mutex> lk(mutex_);
            mdsClientBase_.Register(ip,
                                    port,
                                    &response,
                                    &cntl,
                                    channel_);
        }

        if (cntl.Failed()) {
            mdsClientMetric_.registerClient.eps.count << 1;
            LOG(WARNING) << "register client failed, errcorde = "
                        << response.statuscode()
                        << ", error content:"
                        << cntl.ErrorText()
                        << ", log id = " << cntl.log_id();

            if (!UpdateRetryinfoOrChangeServer(&count, &mdsAddrleft)) {
                break;
            }
            continue;
        }

        LIBCURVE_ERROR retcode;
        curve::mds::StatusCode stcode = response.statuscode();
        MDSStatusCode2LibcurveError(stcode, &retcode);

        LOG_IF(ERROR, retcode != LIBCURVE_ERROR::OK)
                << "Register failed, errocde = " << retcode
                << ", error message = " << curve::mds::StatusCode_Name(stcode)
                << ", log id = " << cntl.log_id();
        return retcode;
    }
    return LIBCURVE_ERROR::FAILED;
}

LIBCURVE_ERROR MDSClient::OpenFile(const std::string& filename,
                                    const UserInfo_t& userinfo,
                                    FInfo_t* fi,
                                    LeaseSession* lease) {
    // 记录当前mds重试次数
    int count = 0;
    // 记录还没重试的mds addr数量
    int mdsAddrleft = metaServerOpt_.metaaddrvec.size() - 1;

    while (count < metaServerOpt_.synchronizeRPCRetryTime) {
        bool infoComplete = false;

        brpc::Controller cntl;
        curve::mds::OpenFileResponse response;

        mdsClientMetric_.openFile.qps.count << 1;
        {
            LatencyGuard lg(&mdsClientMetric_.openFile.latency);
            std::unique_lock<bthread::Mutex> lk(mutex_);
            mdsClientBase_.OpenFile(filename,
                                    userinfo,
                                    &response,
                                    &cntl,
                                    channel_);
        }

        if (cntl.Failed()) {
            mdsClientMetric_.openFile.eps.count << 1;
            LOG(WARNING) << "open file failed, errcorde = "
                        << response.statuscode()
                        << ", error content:"
                        << cntl.ErrorText()
                        << ", log id = " << cntl.log_id();

            if (!UpdateRetryinfoOrChangeServer(&count, &mdsAddrleft)) {
                break;
            }
            continue;
        } else {
            if (response.has_protosession()) {
                infoComplete = true;
                ::curve::mds::ProtoSession leasesession = response.protosession();  // NOLINT
                lease->sessionID     = leasesession.sessionid();
                lease->leaseTime     = leasesession.leasetime();
                lease->createTime    = leasesession.createtime();
            }

            if (infoComplete && response.has_fileinfo()) {
                curve::mds::FileInfo finfo = response.fileinfo();
                ServiceHelper::ProtoFileInfo2Local(&finfo, fi);
            } else {
                infoComplete = false;
            }
        }

        LIBCURVE_ERROR retcode;
        curve::mds::StatusCode stcode = response.statuscode();
        MDSStatusCode2LibcurveError(stcode, &retcode);

        LOG_IF(ERROR, retcode != LIBCURVE_ERROR::OK)
                << "OpenFile: filename = " << filename.c_str()
                << ", owner = " << userinfo.owner
                << ", errocde = " << retcode
                << ", error message = " << curve::mds::StatusCode_Name(stcode)
                << ", log id = " << cntl.log_id();

        if (!infoComplete) {
            LOG(ERROR) << "mds response has no file info!";
            return LIBCURVE_ERROR::FAILED;
        }

        return retcode;
    }
    return LIBCURVE_ERROR::FAILED;
}

LIBCURVE_ERROR MDSClient::CreateFile(const std::string& filename,
                                    const UserInfo_t& userinfo,
                                    size_t size,
                                    bool normalFile) {
    // 记录当前mds重试次数
    int count = 0;
    // 记录还没重试的mds addr数量
    int mdsAddrleft = metaServerOpt_.metaaddrvec.size() - 1;

    while (count < metaServerOpt_.synchronizeRPCRetryTime) {
        brpc::Controller cntl;
        curve::mds::CreateFileResponse response;

        mdsClientMetric_.createFile.qps.count << 1;
        {
            LatencyGuard lg(&mdsClientMetric_.createFile.latency);
            std::unique_lock<bthread::Mutex> lk(mutex_);
            curve::mds::CurveFSService_Stub stub(channel_);
            mdsClientBase_.CreateFile(filename,
                                      userinfo,
                                      size,
                                      normalFile,
                                      &response,
                                      &cntl,
                                      channel_);
        }

        if (cntl.Failed()) {
            mdsClientMetric_.createFile.eps.count << 1;
            LOG(WARNING) << "Create file or directory failed, errcorde = "
                        << response.statuscode()
                        << ", error content:"
                        << cntl.ErrorText()
                        << ", retry CreateFile, retry times = "
                        << count
                        << ", log id = " << cntl.log_id();

            if (!UpdateRetryinfoOrChangeServer(&count, &mdsAddrleft)) {
                break;
            }
            continue;
        }

        LIBCURVE_ERROR retcode;
        curve::mds::StatusCode stcode = response.statuscode();
        MDSStatusCode2LibcurveError(stcode, &retcode);

        LOG_IF(ERROR, retcode != LIBCURVE_ERROR::OK)
                << "CreateFile: filename = " << filename.c_str()
                << ", owner = " << userinfo.owner
                << ", is nomalfile: " << normalFile
                << ", errocde = " << retcode
                << ", error message = " << curve::mds::StatusCode_Name(stcode)
                << ", log id = " << cntl.log_id();

        return retcode;
    }
    return LIBCURVE_ERROR::FAILED;
}

LIBCURVE_ERROR MDSClient::CloseFile(const std::string& filename,
                                    const UserInfo_t& userinfo,
                                    const std::string& sessionid) {
    // 记录当前mds重试次数
    int count = 0;
    // 记录还没重试的mds addr数量
    int mdsAddrleft = metaServerOpt_.metaaddrvec.size() - 1;

    while (count < metaServerOpt_.synchronizeRPCRetryTime) {
        brpc::Controller cntl;
        curve::mds::CloseFileResponse response;

        mdsClientMetric_.closeFile.qps.count << 1;
        {
            LatencyGuard lg(&mdsClientMetric_.closeFile.latency);
            std::unique_lock<bthread::Mutex> lk(mutex_);
            mdsClientBase_.CloseFile(filename,
                                    userinfo,
                                    sessionid,
                                    &response,
                                    &cntl,
                                    channel_);
        }

        if (cntl.Failed()) {
            mdsClientMetric_.closeFile.eps.count << 1;
            LOG(WARNING) << "close file failed, errcorde = "
                        << response.statuscode()
                        << ", error content:"
                        << cntl.ErrorText()
                        << ", retry CloseFile, retry times = "
                        << count
                        << ", log id = " << cntl.log_id();

            if (!UpdateRetryinfoOrChangeServer(&count, &mdsAddrleft)) {
                break;
            }
            continue;
        }

        LIBCURVE_ERROR retcode;
        curve::mds::StatusCode stcode = response.statuscode();
        MDSStatusCode2LibcurveError(stcode, &retcode);

        LOG_IF(ERROR, retcode != LIBCURVE_ERROR::OK)
                << "CloseFile: filename = " << filename.c_str()
                << ", owner = " << userinfo.owner
                << ", sessionid = " << sessionid
                << ", errocde = " << retcode
                << ", error message = " << curve::mds::StatusCode_Name(stcode)
                << ", log id = " << cntl.log_id();

        return retcode;
    }
    return LIBCURVE_ERROR::FAILED;
}

LIBCURVE_ERROR MDSClient::GetFileInfo(const std::string& filename,
                                    const UserInfo_t& uinfo,
                                    FInfo_t* fi) {
    // 记录当前mds重试次数
    int count = 0;
    // 记录还没重试的mds addr数量
    int mdsAddrleft = metaServerOpt_.metaaddrvec.size() - 1;

    while (count < metaServerOpt_.synchronizeRPCRetryTime) {
        brpc::Controller cntl;
        curve::mds::GetFileInfoResponse response;

        mdsClientMetric_.getFile.qps.count << 1;
        {
            LatencyGuard lg(&mdsClientMetric_.getFile.latency);
            std::unique_lock<bthread::Mutex> lk(mutex_);
            mdsClientBase_.GetFileInfo(filename,
                                       uinfo,
                                       &response,
                                       &cntl,
                                       channel_);
        }

        if (cntl.Failed()) {
            mdsClientMetric_.getFile.eps.count << 1;
            LOG(WARNING)  << "get file info failed, error content:"
                        << cntl.ErrorText()
                        << ", retry GetFileInfo, retry times = "
                        << count
                        << ", log id = " << cntl.log_id();

            if (!UpdateRetryinfoOrChangeServer(&count, &mdsAddrleft)) {
                break;
            }
            continue;
        }

        if (response.has_fileinfo()) {
            curve::mds::FileInfo finfo = response.fileinfo();
            ServiceHelper::ProtoFileInfo2Local(&finfo, fi);
        } else {
            LOG(ERROR) << "response has no file info!";
        }

        LIBCURVE_ERROR retcode;
        curve::mds::StatusCode stcode = response.statuscode();
        MDSStatusCode2LibcurveError(stcode, &retcode);

        LOG_IF(ERROR, retcode != LIBCURVE_ERROR::OK)
                << "GetFileInfo: filename = " << filename.c_str()
                << ", owner = " << uinfo.owner
                << ", errocde = " << retcode
                << ", error message = " << curve::mds::StatusCode_Name(stcode)
                << ", log id = " << cntl.log_id();

        return retcode;
    }

    return LIBCURVE_ERROR::FAILED;
}

LIBCURVE_ERROR MDSClient::CreateSnapShot(const std::string& filename,
                                        const UserInfo_t& userinfo,
                                        uint64_t* seq) {
    // 记录当前mds重试次数
    int count = 0;
    // 记录还没重试的mds addr数量
    int mdsAddrleft = metaServerOpt_.metaaddrvec.size() - 1;

    while (count < metaServerOpt_.synchronizeRPCRetryTime) {
        brpc::Controller cntl;
        ::curve::mds::CreateSnapShotResponse response;
        {
            std::unique_lock<bthread::Mutex> lk(mutex_);
            mdsClientBase_.CreateSnapShot(filename,
                                          userinfo,
                                          &response,
                                          &cntl,
                                          channel_);
        }

        if (cntl.Failed()) {
            LOG(WARNING) << "create snap file failed, errcorde = "
                        << response.statuscode()
                        << ", error content:"
                        << cntl.ErrorText()
                        << ", retry CreateSnapShot, retry times = "
                        << count
                        << ", log id = " << cntl.log_id();

            if (!UpdateRetryinfoOrChangeServer(&count, &mdsAddrleft)) {
                break;
            }
            continue;
        }

        ::curve::mds::StatusCode stcode = response.statuscode();

        if (stcode == ::curve::mds::StatusCode::kOK &&
            response.has_snapshotfileinfo()) {
            FInfo_t* fi = new (std::nothrow) FInfo_t;
            curve::mds::FileInfo finfo = response.snapshotfileinfo();
            ServiceHelper::ProtoFileInfo2Local(&finfo, fi);
            *seq = fi->seqnum;
            delete fi;
            return LIBCURVE_ERROR::OK;
        } else if (!response.has_snapshotfileinfo() &&
                   stcode == ::curve::mds::StatusCode::kOK) {
            LOG(WARNING) << "mds side response has no snapshot file info!";
            return LIBCURVE_ERROR::FAILED;
        }

        LIBCURVE_ERROR retcode;
        MDSStatusCode2LibcurveError(stcode, &retcode);

        LOG_IF(ERROR, retcode != LIBCURVE_ERROR::OK)
                << "CreateSnapShot: filename = " << filename.c_str()
                << ", owner = " << userinfo.owner
                << ", errocde = " << retcode
                << ", error message = " << curve::mds::StatusCode_Name(stcode)
                << ", log id = " << cntl.log_id();

        return retcode;
    }
    return LIBCURVE_ERROR::FAILED;
}

LIBCURVE_ERROR MDSClient::DeleteSnapShot(const std::string& filename,
                                         const UserInfo_t& userinfo,
                                         uint64_t seq) {
    // 记录当前mds重试次数
    int count = 0;
    // 记录还没重试的mds addr数量
    int mdsAddrleft = metaServerOpt_.metaaddrvec.size() - 1;

    LIBCURVE_ERROR ret = LIBCURVE_ERROR::FAILED;
    while (count < metaServerOpt_.synchronizeRPCRetryTime) {
        brpc::Controller cntl;
        ::curve::mds::DeleteSnapShotResponse response;

        {
            std::unique_lock<bthread::Mutex> lk(mutex_);
            mdsClientBase_.DeleteSnapShot(filename,
                                          userinfo,
                                          seq,
                                          &response,
                                          &cntl,
                                          channel_);
        }

        if (cntl.Failed()) {
            LOG(WARNING) << "delete snap file failed, errcorde = "
                        << response.statuscode()
                        << ", error content:"
                        << cntl.ErrorText()
                        << ", retry DeleteSnapShot, retry times = "
                        << count
                        << ", log id = " << cntl.log_id();

            if (!UpdateRetryinfoOrChangeServer(&count, &mdsAddrleft)) {
                break;
            }
            continue;
        }

        LIBCURVE_ERROR retcode;
        curve::mds::StatusCode stcode = response.statuscode();
        MDSStatusCode2LibcurveError(stcode, &retcode);

        LOG_IF(ERROR, retcode != LIBCURVE_ERROR::OK)
                << "DeleteSnapShot: filename = " << filename.c_str()
                << ", owner = " << userinfo.owner
                << ", seqnum = " << seq
                << ", errocde = " << retcode
                << ", error message = " << curve::mds::StatusCode_Name(stcode)
                << ", log id = " << cntl.log_id();

        return retcode;
    }
    return LIBCURVE_ERROR::FAILED;
}

LIBCURVE_ERROR MDSClient::GetSnapShot(const std::string& filename,
                                        const UserInfo_t& userinfo,
                                        uint64_t seq,
                                        FInfo* fi) {
    // 记录当前mds重试次数
    int count = 0;
    // 记录还没重试的mds addr数量
    int mdsAddrleft = metaServerOpt_.metaaddrvec.size() - 1;

    std::vector<uint64_t> seqVec;
    seqVec.push_back(seq);

    LIBCURVE_ERROR ret = LIBCURVE_ERROR::FAILED;
    while (count < metaServerOpt_.synchronizeRPCRetryTime) {
        brpc::Controller cntl;
        ::curve::mds::ListSnapShotFileInfoResponse response;

        {
            std::unique_lock<bthread::Mutex> lk(mutex_);
            mdsClientBase_.ListSnapShot(filename,
                                        userinfo,
                                        &seqVec,
                                        &response,
                                        &cntl,
                                        channel_);
        }

        if (cntl.Failed()) {
            LOG(ERROR) << "list snap file failed, errcorde = "
                        << response.statuscode()
                        << ", error content:"
                        << cntl.ErrorText()
                        << ", retry GetSnapShot, retry times = "
                        << count
                        << ", log id = " << cntl.log_id();

            if (!UpdateRetryinfoOrChangeServer(&count, &mdsAddrleft)) {
                break;
            }
            continue;
        }

        ::curve::mds::StatusCode stcode = response.statuscode();

        if (curve::mds::StatusCode::kOwnerAuthFail == stcode) {
            LOG(ERROR) << "auth failed!";
            return LIBCURVE_ERROR::AUTHFAIL;
        }

        LOG_IF(ERROR, stcode != ::curve::mds::StatusCode::kOK)
        << "list snap file failed, errcode = " << stcode;

        auto size = response.fileinfo_size();
        for (int i = 0; i < size; i++) {
            curve::mds::FileInfo finfo = response.fileinfo(i);
            ServiceHelper::ProtoFileInfo2Local(&finfo, fi);
        }

        LIBCURVE_ERROR retcode;
        MDSStatusCode2LibcurveError(stcode, &retcode);

        LOG_IF(ERROR, retcode != LIBCURVE_ERROR::OK)
                << "GetSnapShot: filename = " << filename.c_str()
                << ", owner = " << userinfo.owner
                << ", seqnum = " << seq
                << ", errocde = " << retcode
                << ", error message = " << curve::mds::StatusCode_Name(stcode)
                << ", log id = " << cntl.log_id();

        return retcode;
    }
    return LIBCURVE_ERROR::FAILED;
}

LIBCURVE_ERROR MDSClient::ListSnapShot(const std::string& filename,
                                        const UserInfo_t& userinfo,
                                        const std::vector<uint64_t>* seq,
                                        std::vector<FInfo*>* snapif) {
    // 记录当前mds重试次数
    int count = 0;
    // 记录还没重试的mds addr数量
    int mdsAddrleft = metaServerOpt_.metaaddrvec.size() - 1;

    LIBCURVE_ERROR ret = LIBCURVE_ERROR::FAILED;
    while (count < metaServerOpt_.synchronizeRPCRetryTime) {
        brpc::Controller cntl;
        ::curve::mds::ListSnapShotFileInfoResponse response;
        if ((*seq).size() > (*snapif).size()) {
            LOG(ERROR) << "resource not enough!";
            return LIBCURVE_ERROR::FAILED;
        }

        {
            std::unique_lock<bthread::Mutex> lk(mutex_);
            mdsClientBase_.ListSnapShot(filename,
                                        userinfo,
                                        seq,
                                        &response,
                                        &cntl,
                                        channel_);
        }

        if (cntl.Failed()) {
            LOG(WARNING) << "list snap file failed, errcorde = "
                        << response.statuscode()
                        << ", error content:"
                        << cntl.ErrorText()
                        << ", retry ListSnapShot, retry times = "
                        << count
                        << ", log id = " << cntl.log_id();

            if (!UpdateRetryinfoOrChangeServer(&count, &mdsAddrleft)) {
                break;
            }
            continue;
        }
        ::curve::mds::StatusCode stcode = response.statuscode();

        if (curve::mds::StatusCode::kOwnerAuthFail == stcode) {
            LOG(ERROR) << "auth failed!";
            return LIBCURVE_ERROR::AUTHFAIL;
        }

        LOG_IF(ERROR, stcode != ::curve::mds::StatusCode::kOK)
        << "list snap file failed, errcode = " << stcode;

        auto size = response.fileinfo_size();
        for (int i = 0; i < size; i++) {
            curve::mds::FileInfo finfo = response.fileinfo(i);
            ServiceHelper::ProtoFileInfo2Local(&finfo, (*snapif)[i]);
        }

        LIBCURVE_ERROR retcode;
        MDSStatusCode2LibcurveError(stcode, &retcode);

        LOG_IF(ERROR, retcode != LIBCURVE_ERROR::OK)
                << "ListSnapShot: filename = " << filename.c_str()
                << ", owner = " << userinfo.owner
                << ", errocde = " << retcode
                << ", error message = " << curve::mds::StatusCode_Name(stcode);

        return retcode;
    }
    return LIBCURVE_ERROR::FAILED;
}

LIBCURVE_ERROR MDSClient::GetSnapshotSegmentInfo(const std::string& filename,
                                        const UserInfo_t& userinfo,
                                        uint64_t seq,
                                        uint64_t offset,
                                        SegmentInfo *segInfo) {
    // 记录当前mds重试次数
    int count = 0;
    // 记录还没重试的mds addr数量
    int mdsAddrleft = metaServerOpt_.metaaddrvec.size() - 1;

    while (count < metaServerOpt_.synchronizeRPCRetryTime) {
        brpc::Controller cntl;
        ::curve::mds::GetOrAllocateSegmentResponse response;

        {
            std::unique_lock<bthread::Mutex> lk(mutex_);
            mdsClientBase_.GetSnapshotSegmentInfo(filename,
                                                  userinfo,
                                                  seq,
                                                  offset,
                                                  &response,
                                                  &cntl,
                                                  channel_);
        }

        if (cntl.Failed()) {
            LOG(WARNING) << "get snap file segment info failed, errcorde = "
                        << response.statuscode()
                        << ", error content:"
                        << cntl.ErrorText()
                        << ", retry GetSnapshotSegmentInfo, retry times = "
                        << count;

            if (!UpdateRetryinfoOrChangeServer(&count, &mdsAddrleft)) {
                break;
            }
            continue;
        }
        ::curve::mds::StatusCode stcode = response.statuscode();
        if (curve::mds::StatusCode::kOwnerAuthFail == stcode) {
            LOG(ERROR) << "auth failed!";
            return LIBCURVE_ERROR::AUTHFAIL;
        }

        if (stcode == ::curve::mds::StatusCode::kOK) {
            LogicPoolID logicpoolid = 0;
            curve::mds::PageFileSegment pfs;
            if (response.has_pagefilesegment()) {
                pfs = response.pagefilesegment();
                if (pfs.has_logicalpoolid()) {
                    logicpoolid = pfs.logicalpoolid();
                } else {
                    LOG(ERROR) << "page file segment has no logicpool info";
                    break;
                }

                if (pfs.has_segmentsize()) {
                    segInfo->segmentsize = pfs.segmentsize();
                } else {
                    LOG(ERROR) << "page file segment has no segmentsize info";
                    break;
                }

                if (pfs.has_chunksize()) {
                    segInfo->chunksize = pfs.chunksize();
                } else {
                    LOG(ERROR) << "page file segment has no chunksize info";
                    break;
                }

                if (pfs.has_startoffset()) {
                    segInfo->startoffset = pfs.startoffset();
                } else {
                    LOG(ERROR) << "pagefile segment has no offset info";
                    break;
                }

                segInfo->lpcpIDInfo.lpid = logicpoolid;

                int chunksNum = pfs.chunks_size();
                if (chunksNum == 0 &&
                    stcode == ::curve::mds::StatusCode::kOK) {
                    LOG(ERROR) << "mds allocate segment, but no chunk info!";
                    break;
                }

                for (int i = 0; i < chunksNum; i++) {
                    ChunkID chunkid = 0;
                    CopysetID copysetid = 0;
                    if (pfs.chunks(i).has_chunkid()) {
                        chunkid = pfs.chunks(i).chunkid();
                    } else {
                        LOG(ERROR) << "pagefile segment has no chunkid info";
                        break;
                    }

                    if (pfs.chunks(i).has_copysetid()) {
                        copysetid = pfs.chunks(i).copysetid();
                        segInfo->lpcpIDInfo.cpidVec.push_back(copysetid);
                    } else {
                        LOG(ERROR) << "pagefile segment has no copysetid info";
                        break;
                    }

                    segInfo->chunkvec.push_back(ChunkIDInfo(chunkid,
                                                            logicpoolid,
                                                            copysetid));

                    DVLOG(9) << "chunk id: " << chunkid
                            << " pool id: " << logicpoolid
                            << " copyset id: " << copysetid
                            << " chunk id: " << chunkid;
                }
            }
            return LIBCURVE_ERROR::OK;
        }

        LIBCURVE_ERROR retcode;
        MDSStatusCode2LibcurveError(stcode, &retcode);

        LOG_IF(ERROR, retcode != LIBCURVE_ERROR::OK)
                << "GetSnapshotSegmentInfo: filename = " << filename.c_str()
                << ", owner = " << userinfo.owner
                << ", offset = " << offset
                << ", seqnum = " << seq
                << ", errocde = " << retcode
                << ", error message = " << curve::mds::StatusCode_Name(stcode);

        return retcode;
    }
    return LIBCURVE_ERROR::FAILED;
}

LIBCURVE_ERROR MDSClient::RefreshSession(const std::string& filename,
                                const UserInfo_t& userinfo,
                                const std::string& sessionid,
                                leaseRefreshResult* resp) {
    // 记录当前mds重试次数
    int count = 0;
    // 记录还没重试的mds addr数量
    int mdsAddrleft = metaServerOpt_.metaaddrvec.size() - 1;

    while (count < metaServerOpt_.synchronizeRPCRetryTime) {
        brpc::Controller cntl;
        curve::mds::ReFreshSessionResponse response;
        mdsClientMetric_.refreshSession.qps.count << 1;

        {
            LatencyGuard lg(&mdsClientMetric_.refreshSession.latency);
            std::unique_lock<bthread::Mutex> lk(mutex_);
            mdsClientBase_.RefreshSession(filename,
                                        userinfo,
                                        sessionid,
                                        &response,
                                        &cntl,
                                        channel_);
        }

        if (cntl.Failed()) {
            mdsClientMetric_.refreshSession.eps.count << 1;
            LOG(WARNING) << "Fail to send ReFreshSessionRequest, "
                        << cntl.ErrorText()
                        << ", retry again!";
            if (!UpdateRetryinfoOrChangeServer(&count, &mdsAddrleft)) {
                break;
            }
            continue;
        }

        curve::mds::StatusCode stcode = response.statuscode();

        if (stcode != curve::mds::StatusCode::kOK) {
            LOG(WARNING) << "RefreshSession NOT OK: filename = "
                        << filename.c_str()
                        << ", owner = "
                        << userinfo.owner
                        << ", sessionid = "
                        << sessionid
                        << ", status code = "
                        << curve::mds::StatusCode_Name(stcode);
        }

        if (curve::mds::StatusCode::kOwnerAuthFail == stcode) {
            resp->status = leaseRefreshResult::Status::FAILED;
            return LIBCURVE_ERROR::AUTHFAIL;
        }

        LOG_EVERY_N(INFO, 10) << "RefreshSession returned: filename = "
                            << filename.c_str()
                            << ", owner = "
                            << userinfo.owner
                            << ", sessionid = "
                            << sessionid
                            << ", status code = "
                            << curve::mds::StatusCode_Name(stcode);

        if (stcode == curve::mds::StatusCode::kSessionNotExist
            || stcode == curve::mds::StatusCode::kFileNotExists) {
            resp->status = leaseRefreshResult::Status::NOT_EXIST;
        } else if (stcode != curve::mds::StatusCode::kOK) {
            resp->status = leaseRefreshResult::Status::FAILED;
            return LIBCURVE_ERROR::FAILED;
        } else {
            if (response.has_fileinfo()) {
                curve::mds::FileInfo finfo = response.fileinfo();
                ServiceHelper::ProtoFileInfo2Local(&finfo, &resp->finfo);
                resp->status = leaseRefreshResult::Status::OK;
            } else {
                LOG(WARNING) << "session response has no fileinfo!";
                return LIBCURVE_ERROR::FAILED;
            }
        }
        return LIBCURVE_ERROR::OK;
    }
    return LIBCURVE_ERROR::FAILED;
}

LIBCURVE_ERROR MDSClient::CheckSnapShotStatus(const std::string& filename,
                                              const UserInfo_t& userinfo,
                                              uint64_t seq,
                                              FileStatus* filestatus) {
    // 记录当前mds重试次数
    int count = 0;
    // 记录还没重试的mds addr数量
    int mdsAddrleft = metaServerOpt_.metaaddrvec.size() - 1;

    LIBCURVE_ERROR ret = LIBCURVE_ERROR::FAILED;
    while (count < metaServerOpt_.synchronizeRPCRetryTime) {
        brpc::Controller cntl;
        ::curve::mds::CheckSnapShotStatusResponse response;

        {
            std::unique_lock<bthread::Mutex> lk(mutex_);
            mdsClientBase_.CheckSnapShotStatus(filename,
                                               userinfo,
                                               seq,
                                               &response,
                                               &cntl,
                                               channel_);
        }

        if (cntl.Failed()) {
            LOG(WARNING) << "check snap file failed, errcorde = "
                        << response.statuscode()
                        << ", error content:"
                        << cntl.ErrorText();
            if (!UpdateRetryinfoOrChangeServer(&count, &mdsAddrleft)) {
                break;
            }
            continue;
        }

        if (response.has_filestatus() && filestatus != nullptr) {
            *filestatus = static_cast<curve::client::FileStatus>(
                                            response.filestatus());
        }

        LIBCURVE_ERROR retcode;
        curve::mds::StatusCode stcode = response.statuscode();
        MDSStatusCode2LibcurveError(stcode, &retcode);

        LOG_IF(ERROR, retcode != LIBCURVE_ERROR::OK)
                << "CheckSnapShotStatus: filename = " << filename.c_str()
                << ", owner = " << userinfo.owner
                << ", seqnum = " << seq
                << ", errocde = " << retcode
                << ", error message = " << curve::mds::StatusCode_Name(stcode);

        return retcode;
    }
    return ret;
}

LIBCURVE_ERROR MDSClient::GetServerList(const LogicPoolID& logicalpooid,
                            const std::vector<CopysetID>& copysetidvec,
                            std::vector<CopysetInfo_t>* cpinfoVec) {
    // 记录当前mds重试次数
    int count = 0;
    // 记录重试中timeOut次数
    int timeOutTimes = 0;
    // 记录还没重试的mds addr数量
    int mdsAddrleft = metaServerOpt_.metaaddrvec.size() - 1;

    while (count < metaServerOpt_.rpcRetryTimes) {
        brpc::Controller cntl;
        curve::mds::topology::GetChunkServerListInCopySetsResponse response;

        mdsClientMetric_.getServerList.qps.count << 1;
        {
            LatencyGuard lg(&mdsClientMetric_.getServerList.latency);   // NOLINT
            std::unique_lock<bthread::Mutex> lk(mutex_);
            mdsClientBase_.GetServerList(logicalpooid,
                                        copysetidvec,
                                        &response,
                                        &cntl,
                                        channel_);
        }

        if (cntl.Failed()) {
            mdsClientMetric_.getServerList.eps.count << 1;
            LOG(WARNING)  << "get server list from mds failed, status code = "
                        << response.statuscode()
                        << ", error content:"
                        << cntl.ErrorText()
                        << ", retry GetServerList, retry times = "
                        << count;

            // 1. 访问不存在的IP地址会报错：ETIMEDOUT
            // 2. 访问存在的IP地址，但无人监听：ECONNREFUSED
            // 3. 正常发送RPC情况下，对端进程挂掉了：EHOSTDOWN
            // 4. 链接建立，对端主机挂掉了：brpc::ERPCTIMEDOUT
            // 5. 对端server调用了Stop：ELOGOFF
            // 6. 对端链接已关闭：ECONNRESET
            // 在这几种场景下，主动切换mds。
            // GetServerList在主IO路劲上，所以即使切换mds server失败
            // 也不能直接向上返回，也需要重试到规定次数。
            // 因为返回失败就会导致qemu一侧磁盘IO错误，上层应用就crash了。
            // 所以这里的rpc超时次数要设置大一点
            if (cntl.ErrorCode() == brpc::ERPCTIMEDOUT ||
                cntl.ErrorCode() == ETIMEDOUT) {
                timeOutTimes++;
            }

            // rpc超时次数达到synchronizeRPCRetryTime次的时候就触发切换mds
            if (timeOutTimes > metaServerOpt_.synchronizeRPCRetryTime ||
                cntl.ErrorCode() == EHOSTDOWN ||
                cntl.ErrorCode() == ECONNRESET ||
                cntl.ErrorCode() == ECONNREFUSED ||
                cntl.ErrorCode() == brpc::ELOGOFF) {
                count++;
                if (!ChangeMDServer(&mdsAddrleft)) {
                    LOG(WARNING) << "change mds server failed!";
                    bthread_usleep(metaServerOpt_.retryIntervalUs);
                } else {
                    timeOutTimes = 0;
                }
            } else {
                if (!UpdateRetryinfoOrChangeServer(&count, &mdsAddrleft, false)) {  //  NOLINT
                    LOG(WARNING) << "UpdateRetryinfoOrChangeServer failed!";
                }
            }
            continue;
        }

        int csinfonum = response.csinfo_size();
        for (int i = 0; i < csinfonum; i++) {
            std::string copyset_peer;
            curve::mds::topology::CopySetServerInfo info = response.csinfo(i);
            CopysetInfo_t copysetseverl;
            copysetseverl.cpid_ = info.copysetid();

            int cslocsNum = info.cslocs_size();
            for (int j = 0; j < cslocsNum; j++) {
                curve::mds::topology::ChunkServerLocation csl = info.cslocs(j);

                CopysetPeerInfo_t csinfo;
                uint16_t port           = csl.port();
                std::string hostip      = csl.hostip();
                csinfo.chunkserverid_   = csl.chunkserverid();

                EndPoint ep;
                butil::str2endpoint(hostip.c_str(), port, &ep);
                ChunkServerAddr pd(ep);
                csinfo.csaddr_ = pd;

                copysetseverl.AddCopysetPeerInfo(csinfo);

                copyset_peer.append(hostip)
                            .append(":")
                            .append(std::to_string(port))
                            .append(", ");
            }
            DVLOG(9) << "copyset id : " << copysetseverl.cpid_
                      << ", peer info : " << copyset_peer.c_str();
            cpinfoVec->push_back(copysetseverl);
        }

        if (response.statuscode() == 0) {
            return LIBCURVE_ERROR::OK;
        }
        return LIBCURVE_ERROR::FAILED;
    }
    return LIBCURVE_ERROR::FAILED;
}

LIBCURVE_ERROR MDSClient::CreateCloneFile(const std::string &destination,
                                        const UserInfo_t& userinfo,
                                        uint64_t size,
                                        uint64_t sn,
                                        uint32_t chunksize,
                                        FInfo* fileinfo) {
    // 记录当前mds重试次数
    int count = 0;
    // 记录还没重试的mds addr数量
    int mdsAddrleft = metaServerOpt_.metaaddrvec.size() - 1;

    while (count < metaServerOpt_.synchronizeRPCRetryTime) {
        brpc::Controller cntl;
        curve::mds::CreateCloneFileResponse response;

        {
            std::unique_lock<bthread::Mutex> lk(mutex_);
            mdsClientBase_.CreateCloneFile(destination,
                                           userinfo,
                                           size,
                                           sn,
                                           chunksize,
                                           &response,
                                           &cntl,
                                           channel_);
        }

        if (cntl.Failed()) {
            LOG(WARNING) << "Create clone file failed, errcorde = "
                        << response.statuscode()
                        << ", error content:"
                        << cntl.ErrorText()
                        << ", retry CreateCloneFile, retry times = "
                        << count;

            if (!UpdateRetryinfoOrChangeServer(&count, &mdsAddrleft)) {
                break;
            }
            continue;
        }

        curve::mds::StatusCode stcode = response.statuscode();
        if (stcode == curve::mds::StatusCode::kOK) {
            curve::mds::FileInfo finfo = response.fileinfo();
            ServiceHelper::ProtoFileInfo2Local(&finfo, fileinfo);
            return LIBCURVE_ERROR::OK;
        }

        LIBCURVE_ERROR retcode;
        MDSStatusCode2LibcurveError(stcode, &retcode);

        LOG_IF(ERROR, retcode != LIBCURVE_ERROR::OK)
                << "CreateCloneFile: destination = " << destination
                << ", owner = " << userinfo.owner.c_str()
                << ", seqnum = " << sn
                << ", size = " << size
                << ", chunksize = " << chunksize
                << ", errocde = " << retcode
                << ", error message = " << curve::mds::StatusCode_Name(stcode)
                << ", log id = " << cntl.log_id();

        return retcode;
    }
    return LIBCURVE_ERROR::FAILED;
}


LIBCURVE_ERROR MDSClient::CompleteCloneMeta(const std::string &destination,
                                        const UserInfo_t& userinfo) {
    return SetCloneFileStatus(destination,
                              FileStatus::CloneMetaInstalled,
                              userinfo);
}

LIBCURVE_ERROR MDSClient::CompleteCloneFile(const std::string &destination,
                                        const UserInfo_t& userinfo) {
    return SetCloneFileStatus(destination, FileStatus::Cloned, userinfo);
}

LIBCURVE_ERROR MDSClient::SetCloneFileStatus(const std::string &filename,
                                            const FileStatus& filestatus,
                                            const UserInfo_t& userinfo,
                                            uint64_t fileID) {
    // 记录当前mds重试次数
    int count = 0;
    // 记录还没重试的mds addr数量
    int mdsAddrleft = metaServerOpt_.metaaddrvec.size() - 1;

    while (count < metaServerOpt_.synchronizeRPCRetryTime) {
        brpc::Controller cntl;
        curve::mds::SetCloneFileStatusResponse response;

        {
            std::unique_lock<bthread::Mutex> lk(mutex_);
            mdsClientBase_.SetCloneFileStatus(filename,
                                              filestatus,
                                              userinfo,
                                              fileID,
                                              &response,
                                              &cntl,
                                              channel_);
        }

        if (cntl.Failed()) {
            LOG(WARNING) << "SetCloneFileStatus invoke failed, errcorde = "
                        << response.statuscode()
                        << ", error content:"
                        << cntl.ErrorText()
                        << ", retry SetCloneFileStatus, retry times = "
                        << count
                        << ", log id = " << cntl.log_id();

            if (!UpdateRetryinfoOrChangeServer(&count, &mdsAddrleft)) {
                break;
            }
            continue;
        }

        LIBCURVE_ERROR retcode;
        curve::mds::StatusCode stcode = response.statuscode();
        MDSStatusCode2LibcurveError(stcode, &retcode);

        LOG_IF(ERROR, retcode != LIBCURVE_ERROR::OK)
                << "CreateCloneFile failed, filename = " << filename.c_str()
                << ", owner = " << userinfo.owner.c_str()
                << ", filestatus = " << static_cast<int>(filestatus)
                << ", fileID = " << fileID
                << ", errocde = " << retcode
                << ", error message = " << curve::mds::StatusCode_Name(stcode)
                << ", log id = " << cntl.log_id();

        return retcode;
    }

    return LIBCURVE_ERROR::FAILED;
}

LIBCURVE_ERROR MDSClient::GetOrAllocateSegment(bool allocate,
                                        const UserInfo_t& userinfo,
                                        uint64_t offset,
                                        const FInfo_t* fi,
                                        SegmentInfo *segInfo) {
    // 记录当前mds重试次数
    int count = 0;
    // 记录重试中timeOut次数
    int timeOutTimes = 0;
    // 记录还没重试的mds addr数量
    int mdsAddrleft = metaServerOpt_.metaaddrvec.size() - 1;

    while (count < metaServerOpt_.rpcRetryTimes) {
        brpc::Controller cntl;
        curve::mds::GetOrAllocateSegmentResponse response;

        mdsClientMetric_.getOrAllocateSegment.qps.count << 1;
        {
            LatencyGuard lg(&mdsClientMetric_.getOrAllocateSegment.latency);    // NOLINT
            std::unique_lock<bthread::Mutex> lk(mutex_);
            mdsClientBase_.GetOrAllocateSegment(allocate,
                                                userinfo,
                                                offset,
                                                fi,
                                                &response,
                                                &cntl,
                                                channel_);
        }

        if (cntl.Failed()) {
            mdsClientMetric_.getOrAllocateSegment.eps.count << 1;
            LOG(WARNING)  << "allocate segment failed, error code = "
                        << response.statuscode()
                        << ", error content:" << cntl.ErrorText()
                        << ", offset:" << offset
                        << ", retry allocate, retry times = "
                        << count;


            // 1. 访问不存在的IP地址会报错：ETIMEDOUT
            // 2. 访问存在的IP地址，但无人监听：ECONNREFUSED
            // 3. 正常发送RPC情况下，对端进程挂掉了：EHOSTDOWN
            // 4. 链接建立，对端主机挂掉了：brpc::ERPCTIMEDOUT
            // 5. 对端server调用了Stop：ELOGOFF
            // 6. 对端链接已关闭：ECONNRESET
            // 在这几种场景下，主动切换mds。
            // GetOrAllocateSegment在主IO路劲上，所以即使切换mds server失败
            // 也不能直接向上返回，也需要重试到规定次数。
            // 因为返回失败就会导致qemu一侧磁盘IO错误，上层应用就crash了。
            if (cntl.ErrorCode() == brpc::ERPCTIMEDOUT ||
                cntl.ErrorCode() == ETIMEDOUT) {
                timeOutTimes++;
            }

            // rpc超时次数达到synchronizeRPCRetryTime次的时候就触发切换mds
            if (timeOutTimes > metaServerOpt_.synchronizeRPCRetryTime ||
                cntl.ErrorCode() == EHOSTDOWN ||
                cntl.ErrorCode() == ECONNRESET ||
                cntl.ErrorCode() == ECONNREFUSED ||
                cntl.ErrorCode() == brpc::ELOGOFF) {
                count++;
                if (!ChangeMDServer(&mdsAddrleft)) {
                    LOG(WARNING) << "change mds server failed!";
                    bthread_usleep(metaServerOpt_.retryIntervalUs);
                } else {
                    timeOutTimes = 0;
                }
            } else {
                if (!UpdateRetryinfoOrChangeServer(&count, &mdsAddrleft, false)) {   //  NOLINT
                    LOG(WARNING) << "UpdateRetryinfoOrChangeServer failed!";
                }
            }
            continue;
        } else {
            if (curve::mds::StatusCode::kOwnerAuthFail
                                                    == response.statuscode()) {
                LOG(ERROR) << "auth failed!";
                return LIBCURVE_ERROR::AUTHFAIL;
            } else if (::curve::mds::StatusCode::kSegmentNotAllocated
                                                    == response.statuscode()) {
                LOG(WARNING) << "segment not allocated!";
                return LIBCURVE_ERROR::NOT_ALLOCATE;
            }

            uint64_t startoffset = 0;
            LogicPoolID logicpoolid = 0;
            curve::mds::PageFileSegment pfs;
            if (response.has_pagefilesegment()) {
                pfs = response.pagefilesegment();
                if (pfs.has_logicalpoolid()) {
                    logicpoolid = pfs.logicalpoolid();
                } else {
                    LOG(WARNING) << "page file segment has no logicpool info";
                    break;
                }

                if (pfs.has_segmentsize()) {
                    segInfo->segmentsize = pfs.segmentsize();
                } else {
                    LOG(WARNING) << "page file segment has no logicpool info";
                    break;
                }

                if (pfs.has_chunksize()) {
                    segInfo->chunksize = pfs.chunksize();
                } else {
                    LOG(WARNING) << "page file segment has no logicpool info";
                    break;
                }

                if (pfs.has_startoffset()) {
                    segInfo->startoffset = pfs.startoffset();
                } else {
                    LOG(WARNING) << "page file segment has no startoffset info";
                    break;
                }

                segInfo->lpcpIDInfo.lpid = logicpoolid;

                int chunksNum = pfs.chunks_size();
                if (allocate && chunksNum <= 0) {
                    LOG(WARNING) << "MDS allocate segment, but no chunkinfo!";
                    break;
                }

                for (int i = 0; i < chunksNum; i++) {
                    ChunkID chunkid = 0;
                    CopysetID copysetid = 0;
                    if (pfs.chunks(i).has_chunkid()) {
                        chunkid = pfs.chunks(i).chunkid();
                    } else {
                        LOG(WARNING) << "page file segment has no chunkid info";
                        break;
                    }

                    if (pfs.chunks(i).has_copysetid()) {
                        copysetid = pfs.chunks(i).copysetid();
                        segInfo->lpcpIDInfo.cpidVec.push_back(copysetid);
                    } else {
                        LOG(WARNING)
                        << "pagefile segment has no copysetid info";
                        break;
                    }

                    ChunkIDInfo chunkinfo(chunkid, logicpoolid, copysetid);
                    segInfo->chunkvec.push_back(chunkinfo);

                    DVLOG(9) << " pool id: " << logicpoolid
                            << " copyset id: " << copysetid
                            << " chunk id: " << chunkid;
                }
            }
            return LIBCURVE_ERROR::OK;
        }
        return LIBCURVE_ERROR::FAILED;
    }
    return LIBCURVE_ERROR::FAILED;
}

LIBCURVE_ERROR MDSClient::RenameFile(const UserInfo_t& userinfo,
                                        const std::string &origin,
                                        const std::string &destination,
                                        uint64_t originId,
                                        uint64_t destinationId) {
    // 记录当前mds重试次数
    int count = 0;
    // 记录还没重试的mds addr数量
    int mdsAddrleft = metaServerOpt_.metaaddrvec.size() - 1;

    while (count < metaServerOpt_.synchronizeRPCRetryTime) {
        brpc::Controller cntl;
        curve::mds::RenameFileResponse response;

        mdsClientMetric_.renameFile.qps.count << 1;
        {
            LatencyGuard lg(&mdsClientMetric_.renameFile.latency);
            std::unique_lock<bthread::Mutex> lk(mutex_);
            mdsClientBase_.RenameFile(userinfo,
                                      origin,
                                      destination,
                                      originId,
                                      destinationId,
                                      &response,
                                      &cntl,
                                      channel_);
        }

        if (cntl.Failed()) {
            mdsClientMetric_.renameFile.eps.count << 1;
            LOG(WARNING) << "RenameFile invoke failed, errcorde = "
                        << response.statuscode()
                        << ", error content:"
                        << cntl.ErrorText()
                        << ", retry RenameFile, retry times = "
                        << count
                        << ", log id = " << cntl.log_id();

            if (!UpdateRetryinfoOrChangeServer(&count, &mdsAddrleft)) {
                break;
            }
            continue;
        }

        LIBCURVE_ERROR retcode;
        curve::mds::StatusCode stcode = response.statuscode();
        MDSStatusCode2LibcurveError(stcode, &retcode);

        LOG_IF(ERROR, retcode != LIBCURVE_ERROR::OK)
                << "RenameFile: origin = " << origin.c_str()
                << ", destination = " << destination.c_str()
                << ", originId = " << originId
                << ", destinationId = " << destinationId
                << ", owner = " << userinfo.owner.c_str()
                << ", errocde = " << retcode
                << ", error message = " << curve::mds::StatusCode_Name(stcode)
                << ", log id = " << cntl.log_id();

        return retcode;
    }
    return LIBCURVE_ERROR::FAILED;
}

LIBCURVE_ERROR MDSClient::Extend(const std::string& filename,
                                 const UserInfo_t& userinfo,
                                 uint64_t newsize) {
    // 记录当前mds重试次数
    int count = 0;
    // 记录还没重试的mds addr数量
    int mdsAddrleft = metaServerOpt_.metaaddrvec.size() - 1;

    while (count < metaServerOpt_.synchronizeRPCRetryTime) {
        brpc::Controller cntl;
        curve::mds::ExtendFileResponse response;

        mdsClientMetric_.extendFile.qps.count << 1;
        {
            LatencyGuard lg(&mdsClientMetric_.extendFile.latency);
            std::unique_lock<bthread::Mutex> lk(mutex_);
            mdsClientBase_.Extend(filename,
                                userinfo,
                                newsize,
                                &response,
                                &cntl,
                                channel_);
        }

        if (cntl.Failed()) {
            mdsClientMetric_.extendFile.eps.count << 1;
            LOG(WARNING) << "ExtendFile invoke failed, errcorde = "
                        << response.statuscode()
                        << ", error content:"
                        << cntl.ErrorText()
                        << ", retry ExtendFile, retry times = "
                        << count
                        << ", log id = " << cntl.log_id();

            if (!UpdateRetryinfoOrChangeServer(&count, &mdsAddrleft)) {
                break;
            }
            continue;
        }

        LIBCURVE_ERROR retcode;
        curve::mds::StatusCode stcode = response.statuscode();
        MDSStatusCode2LibcurveError(stcode, &retcode);

        LOG_IF(ERROR, retcode != LIBCURVE_ERROR::OK)
                << "Extend: filename = " << filename.c_str()
                << ", owner = " << userinfo.owner.c_str()
                << ", newsize = " << newsize
                << ", errocde = " << retcode
                << ", error message = " << curve::mds::StatusCode_Name(stcode)
                << ", log id = " << cntl.log_id();

        return retcode;
    }
    return LIBCURVE_ERROR::FAILED;
}

LIBCURVE_ERROR MDSClient::DeleteFile(const std::string& filename,
                                     const UserInfo_t& userinfo,
                                     bool deleteforce,
                                     uint64_t fileid) {
    // 记录当前mds重试次数
    int count = 0;
    // 记录还没重试的mds addr数量
    int mdsAddrleft = metaServerOpt_.metaaddrvec.size() - 1;

    while (count < metaServerOpt_.synchronizeRPCRetryTime) {
        brpc::Controller cntl;
        curve::mds::DeleteFileResponse response;

        mdsClientMetric_.deleteFile.qps.count << 1;
        {
            LatencyGuard lg(&mdsClientMetric_.deleteFile.latency);
            std::unique_lock<bthread::Mutex> lk(mutex_);
            mdsClientBase_.DeleteFile(filename,
                                      userinfo,
                                      deleteforce,
                                      fileid,
                                      &response,
                                      &cntl,
                                      channel_);
        }

        if (cntl.Failed()) {
            mdsClientMetric_.deleteFile.eps.count << 1;
            LOG(WARNING) << "DeleteFile invoke failed, errcorde = "
                        << response.statuscode()
                        << ", error content:"
                        << cntl.ErrorText()
                        << ", retry DeleteFile, retry times = "
                        << count
                        << ", log id = " << cntl.log_id();

            if (!UpdateRetryinfoOrChangeServer(&count, &mdsAddrleft)) {
                break;
            }
            continue;
        }

        LIBCURVE_ERROR retcode;
        curve::mds::StatusCode stcode = response.statuscode();
        MDSStatusCode2LibcurveError(stcode, &retcode);

        LOG_IF(ERROR, retcode != LIBCURVE_ERROR::OK)
                << "DeleteFile: filename = " << filename.c_str()
                << ", owner = " << userinfo.owner.c_str()
                << ", errocde = " << retcode
                << ", error message = " << curve::mds::StatusCode_Name(stcode)
                << ", log id = " << cntl.log_id();

        return retcode;
    }
    return LIBCURVE_ERROR::FAILED;
}

LIBCURVE_ERROR MDSClient::ChangeOwner(const std::string& filename,
                                        const std::string& newOwner,
                                        const UserInfo_t& userinfo) {
    // 记录当前mds重试次数
    int count = 0;
    // 记录还没重试的mds addr数量
    int mdsAddrleft = metaServerOpt_.metaaddrvec.size() - 1;

    while (count < metaServerOpt_.synchronizeRPCRetryTime) {
        brpc::Controller cntl;
        curve::mds::ChangeOwnerResponse response;

        mdsClientMetric_.changeOwner.qps.count << 1;
        {
            LatencyGuard lg(&mdsClientMetric_.changeOwner.latency);
            std::unique_lock<bthread::Mutex> lk(mutex_);
            mdsClientBase_.ChangeOwner(filename,
                                       newOwner,
                                       userinfo,
                                       &response,
                                       &cntl,
                                       channel_);
        }

        if (cntl.Failed()) {
            mdsClientMetric_.changeOwner.eps.count << 1;
            LOG(WARNING) << "ChangeOwner invoke failed, errcorde = "
                        << response.statuscode()
                        << ", error content:"
                        << cntl.ErrorText()
                        << ", retry ChangeOwner, retry times = "
                        << count
                        << ", log id = " << cntl.log_id();

            if (!UpdateRetryinfoOrChangeServer(&count, &mdsAddrleft)) {
                break;
            }
            continue;
        }

        LIBCURVE_ERROR retcode;
        curve::mds::StatusCode stcode = response.statuscode();
        MDSStatusCode2LibcurveError(stcode, &retcode);

        LOG_IF(ERROR, retcode != LIBCURVE_ERROR::OK)
                << "ChangeOwner: filename = " << filename.c_str()
                << ", owner = " << userinfo.owner.c_str()
                << ", new owner = " << newOwner.c_str()
                << ", errocde = " << retcode
                << ", error message = " << curve::mds::StatusCode_Name(stcode)
                << ", log id = " << cntl.log_id();

        return retcode;
    }
    return LIBCURVE_ERROR::FAILED;
}

LIBCURVE_ERROR MDSClient::Listdir(const std::string& dirpath,
                        const UserInfo_t& userinfo,
                        std::vector<FileStatInfo>* filestatVec) {
    // 记录当前mds重试次数
    int count = 0;
    // 记录还没重试的mds addr数量
    int mdsAddrleft = metaServerOpt_.metaaddrvec.size() - 1;

    while (count < metaServerOpt_.synchronizeRPCRetryTime) {
        brpc::Controller cntl;
        curve::mds::ListDirResponse response;

        mdsClientMetric_.listDir.qps.count << 1;
        {
            LatencyGuard lg(&mdsClientMetric_.listDir.latency);
            std::unique_lock<bthread::Mutex> lk(mutex_);
            mdsClientBase_.Listdir(dirpath,
                                   userinfo,
                                   &response,
                                   &cntl,
                                   channel_);
        }

        if (cntl.Failed()) {
            mdsClientMetric_.listDir.eps.count << 1;
            LOG(WARNING) << "Listdir invoke failed, errcorde = "
                        << response.statuscode()
                        << ", error content:"
                        << cntl.ErrorText()
                        << ", retry Listdir, retry times = "
                        << count
                        << ", log id = " << cntl.log_id();

            if (!UpdateRetryinfoOrChangeServer(&count, &mdsAddrleft)) {
                break;
            }
            continue;
        }

        LIBCURVE_ERROR retcode;
        curve::mds::StatusCode stcode = response.statuscode();
        MDSStatusCode2LibcurveError(stcode, &retcode);

        LOG_IF(ERROR, retcode != LIBCURVE_ERROR::OK)
                << "Listdir: filename = " << dirpath.c_str()
                << ", owner = " << userinfo.owner.c_str()
                << ", errocde = " << retcode
                << ", error message = " << curve::mds::StatusCode_Name(stcode)
                << ", log id = " << cntl.log_id();

        if (retcode == LIBCURVE_ERROR::OK) {
            int fileinfoNum = response.fileinfo_size();
            for (int i = 0; i < fileinfoNum; i++) {
                curve::mds::FileInfo finfo = response.fileinfo(i);
                FileStatInfo_t filestat;
                filestat.id = finfo.id();
                filestat.length = finfo.length();
                filestat.parentid = finfo.parentid();
                filestat.filetype = static_cast<FileType>(finfo.filetype());
                filestat.ctime = finfo.ctime();
                memset(filestat.owner, 0, NAME_MAX_SIZE);
                memcpy(filestat.owner, finfo.owner().c_str(), NAME_MAX_SIZE);
                memset(filestat.filename, 0, NAME_MAX_SIZE);
                memcpy(filestat.filename, finfo.filename().c_str(),
                       NAME_MAX_SIZE);
                filestatVec->push_back(filestat);
            }
        }

        return retcode;
    }
    return LIBCURVE_ERROR::FAILED;
}

void MDSClient::MDSStatusCode2LibcurveError(const curve::mds::StatusCode& status
                                            , LIBCURVE_ERROR* errcode) {
    switch (status) {
        case ::curve::mds::StatusCode::kOK:
            *errcode = LIBCURVE_ERROR::OK;
            break;
        case ::curve::mds::StatusCode::kFileExists:
            *errcode = LIBCURVE_ERROR::EXISTS;
            break;
        case ::curve::mds::StatusCode::kSnapshotFileNotExists:
        case ::curve::mds::StatusCode::kFileNotExists:
        case ::curve::mds::StatusCode::kDirNotExist:
            *errcode = LIBCURVE_ERROR::NOTEXIST;
            break;
        case ::curve::mds::StatusCode::kSegmentNotAllocated:
            *errcode = LIBCURVE_ERROR::NOT_ALLOCATE;
            break;
        case ::curve::mds::StatusCode::kShrinkBiggerFile:
            *errcode = LIBCURVE_ERROR::NO_SHRINK_BIGGER_FILE;
            break;
        case ::curve::mds::StatusCode::kNotSupported:
            *errcode = LIBCURVE_ERROR::NOT_SUPPORT;
            break;
        case ::curve::mds::StatusCode::kOwnerAuthFail:
            *errcode = LIBCURVE_ERROR::AUTHFAIL;
            break;
        case ::curve::mds::StatusCode::kSnapshotFileDeleteError:
            *errcode = LIBCURVE_ERROR::DELETE_ERROR;
            break;
        case ::curve::mds::StatusCode::kFileUnderSnapShot:
            *errcode = LIBCURVE_ERROR::UNDER_SNAPSHOT;
            break;
        case ::curve::mds::StatusCode::kFileNotUnderSnapShot:
            *errcode = LIBCURVE_ERROR::NOT_UNDERSNAPSHOT;
            break;
        case ::curve::mds::StatusCode::kSnapshotDeleting:
            *errcode = LIBCURVE_ERROR::DELETING;
            break;
        case ::curve::mds::StatusCode::kDirNotEmpty:
            *errcode = LIBCURVE_ERROR::NOT_EMPTY;
            break;
        case ::curve::mds::StatusCode::kFileOccupied:
            *errcode = LIBCURVE_ERROR::FILE_OCCUPIED;
            break;
        case ::curve::mds::StatusCode::kSessionNotExist:
            *errcode = LIBCURVE_ERROR::SESSION_NOT_EXIST;
            break;
        case ::curve::mds::StatusCode::kParaError:
            *errcode = LIBCURVE_ERROR::INTERNAL_ERROR;
            break;
        case ::curve::mds::StatusCode::kStorageError:
            *errcode = LIBCURVE_ERROR::INTERNAL_ERROR;
            break;
        case ::curve::mds::StatusCode::kFileLengthNotSupported:
            *errcode = LIBCURVE_ERROR::LENGTH_NOT_SUPPORT;
            break;
        default:
            *errcode = LIBCURVE_ERROR::UNKNOWN;
            break;
    }
}

}   // namespace client
}   // namespace curve
