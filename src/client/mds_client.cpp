/*
 * Project: curve
 * File Created: Monday, 18th February 2019 6:25:25 pm
 * Author: tongguangxun
 * Copyright (c)￼ 2018 netease
 */
#include <glog/logging.h>

#include <thread>   // NOLINT
#include <chrono>   // NOLINT

#include "src/client/metacache.h"
#include "src/client/mds_client.h"
#include "src/common/timeutility.h"
#include "src/client/lease_excutor.h"

using curve::common::TimeUtility;
using curve::common::ReadLockGuard;
using curve::common::WriteLockGuard;

const char* kRootUserName = "root";
namespace curve {
namespace client {
MDSClient::MDSClient() : cntlID_(0) {
    inited_   = false;
    channel_  = nullptr;
}

LIBCURVE_ERROR MDSClient::Initialize(MetaServerOption_t metaServerOpt) {
    if (inited_) {
        LOG(INFO) << "MDSClient already started!";
        return LIBCURVE_ERROR::OK;
    }

    lastWorkingMDSAddrIndex_ = -1;
    metaServerOpt_ = metaServerOpt;

    for (auto addr : metaServerOpt_.metaaddrvec) {
        lastWorkingMDSAddrIndex_++;

        channel_ = new (std::nothrow) brpc::Channel();
        if (channel_->Init(addr.c_str(), nullptr) != 0) {
            LOG(ERROR) << "Init channel failed!";
            continue;
        }

        inited_ = true;
        return LIBCURVE_ERROR::OK;
    }

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
            LOG(ERROR) << "have retried all mds address!";
            break;
        }

        (*mdsAddrleft)--;

        {
            // 加写锁，后续其他线程如果调用，则阻塞
            WriteLockGuard wrguard(rwlock_);

            // 重新创建之前需要将原来的channel析构掉，否则会造成第二次链接失败
            delete channel_;
            channel_ = new (std::nothrow) brpc::Channel();

            int ret = channel_->Init(metaServerOpt_.
                                metaaddrvec[nextMDSAddrIndex].c_str(), nullptr);
            if (ret != 0) {
                LOG(ERROR) << "Init channel failed!";
                continue;
            }

            createRet = true;

            mdsClientMetric_.mdsServerChangeTimes << 1;
            break;
        }

        ++nextMDSAddrIndex;
        nextMDSAddrIndex %= addrSize;
    }

    lastWorkingMDSAddrIndex_ = nextMDSAddrIndex;
    return createRet;
}

bool MDSClient::UpdateRetryinfoOrChangeServer(int* retrycount,
                                              int* mdsAddrleft) {
    (*retrycount)++;
    if (*retrycount >= metaServerOpt_.rpcRetryTimes) {
        if (*mdsAddrleft > 0 && ChangeMDServer(mdsAddrleft)) {
            // 切换mds地址，重新在新的mds addr上重试
            *retrycount = 0;
        } else {
            LOG(ERROR) << "retry failed!";
            return false;
        }
    } else {
        std::this_thread::sleep_for(std::chrono::microseconds(
                                metaServerOpt_.retryIntervalUs));
    }
    return true;
}

LIBCURVE_ERROR MDSClient::OpenFile(const std::string& filename,
                                    const UserInfo_t& userinfo,
                                    FInfo_t* fi,
                                    LeaseSession* lease) {
    // 记录当前mds重试次数
    int count = 0;
    // 记录还没重试的mds addr数量
    int mdsAddrleft = metaServerOpt_.metaaddrvec.size() - 1;

    while (count < metaServerOpt_.rpcRetryTimes) {
        bool infoComplete = false;

        curve::mds::OpenFileRequest request;
        curve::mds::OpenFileResponse response;
        request.set_filename(filename);

        FillUserInfo<curve::mds::OpenFileRequest>(&request, userinfo);

        brpc::Controller cntl;
        cntl.set_timeout_ms(metaServerOpt_.rpcTimeoutMs);
        cntl.set_log_id(GetLogId());

        LOG(INFO) << "OpenFile: filename = " << filename.c_str()
                  << ", owner = " << userinfo.owner
                  << ", log id = " << cntl.log_id();

        {
            ReadLockGuard readGuard(rwlock_);
            curve::mds::CurveFSService_Stub stub(channel_);
            stub.OpenFile(&cntl, &request, &response, nullptr);
        }

        if (cntl.Failed()) {
            RecordMetricInfo(cntl.ErrorCode());
            LOG(ERROR) << "open file failed, errcorde = "
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

    while (count < metaServerOpt_.rpcRetryTimes) {
        brpc::Controller cntl;
        cntl.set_timeout_ms(metaServerOpt_.rpcTimeoutMs);
        cntl.set_log_id(GetLogId());

        curve::mds::CreateFileRequest request;
        curve::mds::CreateFileResponse response;
        request.set_filename(filename);
        if (normalFile) {
            request.set_filetype(curve::mds::FileType::INODE_PAGEFILE);
            request.set_filelength(size);
        } else {
            request.set_filetype(curve::mds::FileType::INODE_DIRECTORY);
        }

        FillUserInfo<curve::mds::CreateFileRequest>(&request, userinfo);

        LOG(INFO) << "CreateFile: filename = " << filename.c_str()
                  << ", owner = " << userinfo.owner
                  << ", is nomalfile: " << normalFile
                  << ", log id = " << cntl.log_id();

        {
            ReadLockGuard readGuard(rwlock_);
            curve::mds::CurveFSService_Stub stub(channel_);
            stub.CreateFile(&cntl, &request, &response, NULL);
        }

        if (cntl.Failed()) {
            RecordMetricInfo(cntl.ErrorCode());

            LOG(ERROR) << "Create file or directory failed, errcorde = "
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

    while (count < metaServerOpt_.rpcRetryTimes) {
        brpc::Controller cntl;
        cntl.set_timeout_ms(metaServerOpt_.rpcTimeoutMs);
        cntl.set_log_id(GetLogId());

        curve::mds::CloseFileRequest request;
        curve::mds::CloseFileResponse response;
        request.set_filename(filename);
        request.set_sessionid(sessionid);

        FillUserInfo<curve::mds::CloseFileRequest>(&request, userinfo);

        LOG(INFO) << "CloseFile: filename = " << filename.c_str()
                  << ", owner = " << userinfo.owner
                  << ", sessionid = " << sessionid
                  << ", log id = " << cntl.log_id();

        {
            ReadLockGuard readGuard(rwlock_);
            curve::mds::CurveFSService_Stub stub(channel_);
            stub.CloseFile(&cntl, &request, &response, nullptr);
        }

        if (cntl.Failed()) {
            RecordMetricInfo(cntl.ErrorCode());

            LOG(ERROR) << "close file failed, errcorde = "
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

    while (count < metaServerOpt_.rpcRetryTimes) {
        brpc::Controller cntl;
        cntl.set_timeout_ms(metaServerOpt_.rpcTimeoutMs);
        cntl.set_log_id(GetLogId());

        curve::mds::GetFileInfoRequest request;
        curve::mds::GetFileInfoResponse response;
        request.set_filename(filename);

        FillUserInfo<curve::mds::GetFileInfoRequest>(&request, uinfo);

        LOG(INFO) << "GetFileInfo: filename = " << filename.c_str()
                  << ", owner = " << uinfo.owner
                  << ", log id = " << cntl.log_id();

        {
            ReadLockGuard readGuard(rwlock_);
            curve::mds::CurveFSService_Stub stub(channel_);
            stub.GetFileInfo(&cntl, &request, &response, nullptr);
        }

        if (cntl.Failed()) {
            RecordMetricInfo(cntl.ErrorCode());

            LOG(ERROR)  << "get file info failed, error content:"
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

    while (count < metaServerOpt_.rpcRetryTimes) {
        brpc::Controller cntl;
        cntl.set_timeout_ms(metaServerOpt_.rpcTimeoutMs);
        cntl.set_log_id(GetLogId());

        ::curve::mds::CreateSnapShotRequest request;
        ::curve::mds::CreateSnapShotResponse response;

        request.set_filename(filename);

        FillUserInfo<::curve::mds::CreateSnapShotRequest>(&request, userinfo);

        LOG(INFO) << "CreateSnapShot: filename = " << filename.c_str()
                  << ", owner = " << userinfo.owner
                  << ", log id = " << cntl.log_id();

        {
            ReadLockGuard readGuard(rwlock_);
            curve::mds::CurveFSService_Stub stub(channel_);
            stub.CreateSnapShot(&cntl, &request, &response, nullptr);
        }

        if (cntl.Failed()) {
            RecordMetricInfo(cntl.ErrorCode());

            LOG(ERROR) << "create snap file failed, errcorde = "
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
        } else if (!response.has_snapshotfileinfo()) {
            LOG(ERROR) << "mds side response has no snapshot file info!";
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
    while (count < metaServerOpt_.rpcRetryTimes) {
        brpc::Controller cntl;
        cntl.set_timeout_ms(metaServerOpt_.rpcTimeoutMs);
        cntl.set_log_id(GetLogId());

        ::curve::mds::DeleteSnapShotRequest request;
        ::curve::mds::DeleteSnapShotResponse response;

        request.set_seq(seq);
        request.set_filename(filename);

        FillUserInfo<::curve::mds::DeleteSnapShotRequest>(&request, userinfo);

        LOG(INFO) << "DeleteSnapShot: filename = " << filename.c_str()
                  << ", owner = " << userinfo.owner
                  << ", seqnum = " << seq
                  << ", log id = " << cntl.log_id();

        {
            ReadLockGuard readGuard(rwlock_);
            curve::mds::CurveFSService_Stub stub(channel_);
            stub.DeleteSnapShot(&cntl, &request, &response, nullptr);
        }

        if (cntl.Failed()) {
            RecordMetricInfo(cntl.ErrorCode());

            LOG(ERROR) << "delete snap file failed, errcorde = "
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

    LIBCURVE_ERROR ret = LIBCURVE_ERROR::FAILED;
    while (count < metaServerOpt_.rpcRetryTimes) {
        brpc::Controller cntl;
        cntl.set_timeout_ms(metaServerOpt_.rpcTimeoutMs);
        cntl.set_log_id(GetLogId());

        ::curve::mds::ListSnapShotFileInfoRequest request;
        ::curve::mds::ListSnapShotFileInfoResponse response;

        request.set_filename(filename);
        request.add_seq(seq);

        FillUserInfo<::curve::mds::ListSnapShotFileInfoRequest>(
            &request, userinfo);

        LOG(INFO) << "GetSnapShot: filename = " << filename.c_str()
                  << ", owner = " << userinfo.owner
                  << ", seqnum = " << seq
                  << ", log id = " << cntl.log_id();

        {
            ReadLockGuard readGuard(rwlock_);
            curve::mds::CurveFSService_Stub stub(channel_);
            stub.ListSnapShot(&cntl, &request, &response, nullptr);
        }

        if (cntl.Failed()) {
            RecordMetricInfo(cntl.ErrorCode());

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
    while (count < metaServerOpt_.rpcRetryTimes) {
        brpc::Controller cntl;
        cntl.set_timeout_ms(metaServerOpt_.rpcTimeoutMs);
        cntl.set_log_id(GetLogId());

        ::curve::mds::ListSnapShotFileInfoRequest request;
        ::curve::mds::ListSnapShotFileInfoResponse response;

        request.set_filename(filename);

        FillUserInfo<::curve::mds::ListSnapShotFileInfoRequest>(
            &request, userinfo);

        if ((*seq).size() > (*snapif).size()) {
            LOG(ERROR) << "resource not enough!";
            return LIBCURVE_ERROR::FAILED;
        }
        for (unsigned int i = 0; i < (*seq).size(); i++) {
            request.add_seq((*seq)[i]);
        }

        LOG(INFO) << "ListSnapShot: filename = " << filename.c_str()
                  << ", owner = " << userinfo.owner
                  << ", log id = " << cntl.log_id();

        {
            ReadLockGuard readGuard(rwlock_);
            curve::mds::CurveFSService_Stub stub(channel_);
            stub.ListSnapShot(&cntl, &request, &response, nullptr);
        }

        if (cntl.Failed()) {
            RecordMetricInfo(cntl.ErrorCode());

            LOG(ERROR) << "list snap file failed, errcorde = "
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

    while (count < metaServerOpt_.rpcRetryTimes) {
        brpc::Controller cntl;
        cntl.set_timeout_ms(metaServerOpt_.rpcTimeoutMs);
        cntl.set_log_id(GetLogId());

        ::curve::mds::GetOrAllocateSegmentRequest request;
        ::curve::mds::GetOrAllocateSegmentResponse response;

        request.set_filename(filename);
        request.set_offset(offset);
        request.set_allocateifnotexist(false);
        request.set_seqnum(seq);

        FillUserInfo<::curve::mds::GetOrAllocateSegmentRequest>(
            &request, userinfo);

        LOG(INFO) << "GetSnapshotSegmentInfo: filename = " << filename.c_str()
                  << ", owner = " << userinfo.owner
                  << ", offset = " << offset
                  << ", seqnum = " << seq;

        {
            ReadLockGuard readGuard(rwlock_);
            curve::mds::CurveFSService_Stub stub(channel_);
            stub.GetSnapShotFileSegment(&cntl, &request, &response, nullptr);
        }

        if (cntl.Failed()) {
            RecordMetricInfo(cntl.ErrorCode());

            LOG(ERROR) << "get snap file segment info failed, errcorde = "
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
    brpc::Controller cntl;
    cntl.set_timeout_ms(metaServerOpt_.rpcTimeoutMs);
        cntl.set_log_id(GetLogId());

    curve::mds::CurveFSService_Stub stub(channel_);
    curve::mds::ReFreshSessionRequest request;
    curve::mds::ReFreshSessionResponse response;

    request.set_filename(filename);
    request.set_sessionid(sessionid);

    FillUserInfo<::curve::mds::ReFreshSessionRequest>(&request, userinfo);

    LOG_EVERY_N(INFO, 10) << "RefreshSession: filename = " << filename.c_str()
                  << ", owner = " << userinfo.owner
                  << ", sessionid = " << sessionid;

    {
        ReadLockGuard readGuard(rwlock_);
        curve::mds::CurveFSService_Stub stub(channel_);
        stub.RefreshSession(&cntl, &request, &response, nullptr);
    }

    if (cntl.Failed()) {
        RecordMetricInfo(cntl.ErrorCode());

        LOG(ERROR) << "Fail to send ReFreshSessionRequest, "
                    << cntl.ErrorText();
        return LIBCURVE_ERROR::FAILED;
    }

    curve::mds::StatusCode stcode = response.statuscode();

    if (stcode != curve::mds::StatusCode::kOK) {
        LOG(ERROR) << "RefreshSession NOT OK: filename = "
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
    } else {
        resp->status = leaseRefreshResult::Status::OK;
        if (response.has_fileinfo()) {
            curve::mds::FileInfo finfo = response.fileinfo();
            ServiceHelper::ProtoFileInfo2Local(&finfo, &resp->finfo);
        }
    }

    return LIBCURVE_ERROR::OK;
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
    while (count < metaServerOpt_.rpcRetryTimes) {
        brpc::Controller cntl;
        cntl.set_timeout_ms(metaServerOpt_.rpcTimeoutMs);
        cntl.set_log_id(GetLogId());

        ::curve::mds::CheckSnapShotStatusRequest request;
        ::curve::mds::CheckSnapShotStatusResponse response;

        request.set_seq(seq);
        request.set_filename(filename);

        FillUserInfo<::curve::mds::CheckSnapShotStatusRequest>(
            &request, userinfo);

        LOG(INFO) << "CheckSnapShotStatus: filename = " << filename.c_str()
                  << ", owner = " << userinfo.owner
                  << ", seqnum = " << seq;

        {
            ReadLockGuard readGuard(rwlock_);
            curve::mds::CurveFSService_Stub stub(channel_);
            stub.CheckSnapShotStatus(&cntl, &request, &response, nullptr);
        }

        if (cntl.Failed()) {
            RecordMetricInfo(cntl.ErrorCode());

            LOG(ERROR) << "check snap file failed, errcorde = "
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
    // 记录还没重试的mds addr数量
    int mdsAddrleft = metaServerOpt_.metaaddrvec.size() - 1;

    while (count < metaServerOpt_.rpcRetryTimes) {
        brpc::Controller cntl;
        cntl.set_timeout_ms(metaServerOpt_.rpcTimeoutMs);
        cntl.set_log_id(GetLogId());

        curve::mds::topology::GetChunkServerListInCopySetsRequest request;
        curve::mds::topology::GetChunkServerListInCopySetsResponse response;

        request.set_logicalpoolid(logicalpooid);
        for (auto copysetid : copysetidvec) {
            request.add_copysetid(copysetid);
            LOG(INFO) << "copyset id = " << copysetid;
        }

        LOG(INFO) << "GetServerList: logicalpooid = " << logicalpooid;

        {
            ReadLockGuard readGuard(rwlock_);
            curve::mds::topology::TopologyService_Stub stub(channel_);
            stub.GetChunkServerListInCopySets(&cntl, &request, &response, nullptr);     // NOLINT
        }

        if (cntl.Failed()) {
            RecordMetricInfo(cntl.ErrorCode());

            LOG(ERROR)  << "get server list from mds failed, status code = "
                        << response.statuscode()
                        << ", error content:"
                        << cntl.ErrorText()
                        << ", retry GetServerList, retry times = "
                        << count;

            if (!UpdateRetryinfoOrChangeServer(&count, &mdsAddrleft)) {
                break;
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
            LOG(INFO) << "copyset id : " << copysetseverl.cpid_
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

    while (count < metaServerOpt_.rpcRetryTimes) {
        brpc::Controller cntl;
        cntl.set_timeout_ms(metaServerOpt_.rpcTimeoutMs);
        cntl.set_log_id(GetLogId());

        curve::mds::CreateCloneFileRequest request;
        curve::mds::CreateCloneFileResponse response;

        request.set_filename(destination);
        request.set_filetype(curve::mds::FileType::INODE_PAGEFILE);
        request.set_filelength(size);
        request.set_chunksize(chunksize);
        request.set_seq(sn);

        FillUserInfo<::curve::mds::CreateCloneFileRequest>(
            &request, userinfo);

        LOG(INFO) << "CreateCloneFile: destination = " << destination
                  << ", owner = " << userinfo.owner.c_str()
                  << ", seqnum = " << sn
                  << ", size = " << size
                  << ", chunksize = " << chunksize;

        {
            ReadLockGuard readGuard(rwlock_);
            curve::mds::CurveFSService_Stub stub(channel_);
            stub.CreateCloneFile(&cntl, &request, &response, NULL);
        }

        if (cntl.Failed()) {
            RecordMetricInfo(cntl.ErrorCode());

            LOG(ERROR) << "Create clone file failed, errcorde = "
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

    while (count < metaServerOpt_.rpcRetryTimes) {
        brpc::Controller cntl;
        cntl.set_timeout_ms(metaServerOpt_.rpcTimeoutMs);
        cntl.set_log_id(GetLogId());

        curve::mds::SetCloneFileStatusRequest request;
        curve::mds::SetCloneFileStatusResponse response;

        request.set_filename(filename);
        request.set_filestatus(static_cast<curve::mds::FileStatus>(filestatus));
        if (fileID > 0) {
            request.set_fileid(fileID);
        }

        FillUserInfo<::curve::mds::SetCloneFileStatusRequest>(
            &request, userinfo);

        LOG(INFO) << "CreateCloneFile: filename = " << filename.c_str()
                  << ", owner = " << userinfo.owner.c_str()
                  << ", filestatus = " << static_cast<int>(filestatus)
                  << ", fileID = " << fileID
                  << ", log id = " << cntl.log_id();

        {
            ReadLockGuard readGuard(rwlock_);
            curve::mds::CurveFSService_Stub stub(channel_);
            stub.SetCloneFileStatus(&cntl, &request, &response, NULL);
        }

        if (cntl.Failed()) {
            RecordMetricInfo(cntl.ErrorCode());

            LOG(ERROR) << "SetCloneFileStatus invoke failed, errcorde = "
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
    // 记录还没重试的mds addr数量
    int mdsAddrleft = metaServerOpt_.metaaddrvec.size() - 1;

    while (count < metaServerOpt_.rpcRetryTimes) {
        brpc::Controller cntl;
        cntl.set_timeout_ms(metaServerOpt_.rpcTimeoutMs);
        cntl.set_log_id(GetLogId());

        curve::mds::GetOrAllocateSegmentRequest request;
        curve::mds::GetOrAllocateSegmentResponse response;

        // convert the user offset to seg  offset
        uint64_t segmentsize = fi->segmentsize;
        uint64_t chunksize = fi->chunksize;
        uint64_t seg_offset = (offset / segmentsize) * segmentsize;

        request.set_filename(fi->fullPathName);
        request.set_offset(seg_offset);
        request.set_allocateifnotexist(allocate);

        FillUserInfo<curve::mds::GetOrAllocateSegmentRequest>(&request,
                                                              userinfo);

        LOG(INFO) << "GetOrAllocateSegment: allocate = " << allocate
                  << ", owner = " << userinfo.owner.c_str()
                  << ", offset = " << offset
                  << ", log id = " << cntl.log_id();

        {
            ReadLockGuard readGuard(rwlock_);
            curve::mds::CurveFSService_Stub stub(channel_);
            stub.GetOrAllocateSegment(&cntl, &request, &response, NULL);
        }

        DVLOG(9) << "Get segment at offset: " << seg_offset
                << "Response status: " << response.statuscode();

        if (cntl.Failed()) {
            RecordMetricInfo(cntl.ErrorCode());

            LOG(ERROR)  << "allocate segment failed, error code = "
                        << response.statuscode()
                        << ", error content:" << cntl.ErrorText()
                        << ", segment offset:" << seg_offset
                        << ", retry allocate, retry times = "
                        << count;

            if (!UpdateRetryinfoOrChangeServer(&count, &mdsAddrleft)) {
                break;
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
                    LOG(ERROR) << "page file segment has no logicpool info";
                    break;
                }

                if (pfs.has_segmentsize()) {
                    segInfo->segmentsize = pfs.segmentsize();
                } else {
                    LOG(ERROR) << "page file segment has no logicpool info";
                    break;
                }

                if (pfs.has_chunksize()) {
                    segInfo->chunksize = pfs.chunksize();
                } else {
                    LOG(ERROR) << "page file segment has no logicpool info";
                    break;
                }

                if (pfs.has_startoffset()) {
                    segInfo->startoffset = pfs.startoffset();
                } else {
                    LOG(ERROR) << "page file segment has no startoffset info";
                    break;
                }

                segInfo->lpcpIDInfo.lpid = logicpoolid;

                int chunksNum = pfs.chunks_size();
                if (allocate && chunksNum <= 0) {
                    LOG(ERROR) << "MDS allocate segment, but no chunkinfo!";
                    break;
                }

                for (int i = 0; i < chunksNum; i++) {
                    ChunkID chunkid = 0;
                    CopysetID copysetid = 0;
                    if (pfs.chunks(i).has_chunkid()) {
                        chunkid = pfs.chunks(i).chunkid();
                    } else {
                        LOG(ERROR) << "page file segment has no chunkid info";
                        break;
                    }

                    if (pfs.chunks(i).has_copysetid()) {
                        copysetid = pfs.chunks(i).copysetid();
                        segInfo->lpcpIDInfo.cpidVec.push_back(copysetid);
                    } else {
                        LOG(ERROR) << "page file segment has no copysetid info";
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

    while (count < metaServerOpt_.rpcRetryTimes) {
        brpc::Controller cntl;
        cntl.set_timeout_ms(metaServerOpt_.rpcTimeoutMs);
        cntl.set_log_id(GetLogId());

        curve::mds::RenameFileRequest request;
        curve::mds::RenameFileResponse response;

        request.set_oldfilename(origin);
        request.set_newfilename(destination);

        if (originId > 0 && destinationId > 0) {
            request.set_oldfileid(originId);
            request.set_newfileid(destinationId);
        }

        FillUserInfo<::curve::mds::RenameFileRequest>(&request, userinfo);

        LOG(INFO) << "RenameFile: origin = " << origin.c_str()
                  << ", destination = " << destination.c_str()
                  << ", originId = " << originId
                  << ", destinationId = " << destinationId
                  << ", owner = " << userinfo.owner.c_str()
                  << ", log id = " << cntl.log_id();

        {
            ReadLockGuard readGuard(rwlock_);
            curve::mds::CurveFSService_Stub stub(channel_);
            stub.RenameFile(&cntl, &request, &response, NULL);
        }

        if (cntl.Failed()) {
            RecordMetricInfo(cntl.ErrorCode());

            LOG(ERROR) << "RenameFile invoke failed, errcorde = "
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

    while (count < metaServerOpt_.rpcRetryTimes) {
        brpc::Controller cntl;
        cntl.set_timeout_ms(metaServerOpt_.rpcTimeoutMs);
        cntl.set_log_id(GetLogId());

        curve::mds::ExtendFileRequest request;
        curve::mds::ExtendFileResponse response;

        request.set_filename(filename);
        request.set_newsize(newsize);

        FillUserInfo<::curve::mds::ExtendFileRequest>(&request, userinfo);

        LOG(INFO) << "Extend: filename = " << filename.c_str()
                  << ", owner = " << userinfo.owner.c_str()
                  << ", newsize = " << newsize
                  << ", log id = " << cntl.log_id();

        {
            ReadLockGuard readGuard(rwlock_);
            curve::mds::CurveFSService_Stub stub(channel_);
            stub.ExtendFile(&cntl, &request, &response, NULL);
        }

        if (cntl.Failed()) {
            RecordMetricInfo(cntl.ErrorCode());

            LOG(ERROR) << "ExtendFile invoke failed, errcorde = "
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

    while (count < metaServerOpt_.rpcRetryTimes) {
        brpc::Controller cntl;
        cntl.set_timeout_ms(metaServerOpt_.rpcTimeoutMs);
        cntl.set_log_id(GetLogId());

        curve::mds::DeleteFileRequest request;
        curve::mds::DeleteFileResponse response;

        request.set_filename(filename);
        if (fileid > 0) {
            request.set_fileid(fileid);
        }

        request.set_forcedelete(deleteforce);

        FillUserInfo<::curve::mds::DeleteFileRequest>(&request, userinfo);

        LOG(INFO) << "DeleteFile: filename = " << filename.c_str()
                  << ", owner = " << userinfo.owner.c_str()
                  << ", log id = " << cntl.log_id();

        {
            ReadLockGuard readGuard(rwlock_);
            curve::mds::CurveFSService_Stub stub(channel_);
            stub.DeleteFile(&cntl, &request, &response, NULL);
        }

        if (cntl.Failed()) {
            RecordMetricInfo(cntl.ErrorCode());

            LOG(ERROR) << "DeleteFile invoke failed, errcorde = "
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

    while (count < metaServerOpt_.rpcRetryTimes) {
        brpc::Controller cntl;
        cntl.set_timeout_ms(metaServerOpt_.rpcTimeoutMs);
        cntl.set_log_id(GetLogId());

        curve::mds::ChangeOwnerRequest request;
        curve::mds::ChangeOwnerResponse response;

        request.set_filename(filename);
        request.set_newowner(newOwner);

        uint64_t date = curve::common::TimeUtility::GetTimeofDayUs();
        request.set_rootowner(userinfo.owner);
        request.set_date(date);

        if (!userinfo.owner.compare(kRootUserName) &&
             userinfo.password.compare("")) {
            std::string str2sig = Authenticator::GetString2Signature(date,
                                                        userinfo.owner);
            std::string sig = Authenticator::CalcString2Signature(str2sig,
                                                         userinfo.password);
            request.set_signature(sig);
        } else {
            LOG(WARNING) << "change owner need root user!";
            return LIBCURVE_ERROR::AUTHFAIL;
        }

        LOG(INFO) << "ChangeOwner: filename = " << filename.c_str()
                  << ", operator owner = " << userinfo.owner.c_str()
                  << ", new owner = " << newOwner.c_str()
                  << ", log id = " << cntl.log_id();

        {
            ReadLockGuard readGuard(rwlock_);
            curve::mds::CurveFSService_Stub stub(channel_);
            stub.ChangeOwner(&cntl, &request, &response, NULL);
        }

        if (cntl.Failed()) {
            RecordMetricInfo(cntl.ErrorCode());

            LOG(ERROR) << "ChangeOwner invoke failed, errcorde = "
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

    while (count < metaServerOpt_.rpcRetryTimes) {
        brpc::Controller cntl;
        cntl.set_timeout_ms(metaServerOpt_.rpcTimeoutMs);
        cntl.set_log_id(GetLogId());

        curve::mds::ListDirRequest request;
        curve::mds::ListDirResponse response;

        request.set_filename(dirpath);

        FillUserInfo<::curve::mds::ListDirRequest>(&request, userinfo);

        LOG(INFO) << "Listdir: filename = " << dirpath.c_str()
                  << ", owner = " << userinfo.owner.c_str()
                  << ", log id = " << cntl.log_id();

        {
            ReadLockGuard readGuard(rwlock_);
            curve::mds::CurveFSService_Stub stub(channel_);
            stub.ListDir(&cntl, &request, &response, NULL);
        }

        if (cntl.Failed()) {
            RecordMetricInfo(cntl.ErrorCode());

            LOG(ERROR) << "Listdir invoke failed, errcorde = "
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
            *errcode = LIBCURVE_ERROR::INTERNAL_ERROR;
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
