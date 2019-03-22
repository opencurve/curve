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

namespace curve {
namespace client {
MDSClient::MDSClient() {
    inited_   = false;
    channel_  = nullptr;
}

LIBCURVE_ERROR MDSClient::Initialize(UserInfo_t userinfo,
                                    MetaServerOption_t metaServerOpt) {
    if (inited_) {
        LOG(INFO) << "MDSClient already started!";
        return LIBCURVE_ERROR::OK;
    }

    userinfo_ = userinfo;
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

LIBCURVE_ERROR MDSClient::OpenFile(std::string filename,
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
        request.set_owner(userinfo_.owner);
        if (userinfo_.password != "") {
            request.set_password(userinfo_.password);
        }

        brpc::Controller cntl;
        cntl.set_timeout_ms(metaServerOpt_.rpcTimeoutMs);
        cntl.set_max_retry(metaServerOpt_.rpcRetryTimes);

        {
            ReadLockGuard readGuard(rwlock_);
            curve::mds::CurveFSService_Stub stub(channel_);
            stub.OpenFile(&cntl, &request, &response, nullptr);
        }

        if (cntl.Failed()) {
            LOG(ERROR) << "open file failed, errcorde = "
                        << response.statuscode()
                        << ", error content:"
                        << cntl.ErrorText();
            if (!UpdateRetryinfoOrChangeServer(&count, &mdsAddrleft)) {
                break;
            }
            continue;
        } else {
            if (response.has_protosession()) {
                infoComplete = true;
                ::curve::mds::ProtoSession leasesession = response.protosession();  // NOLINT
                lease->sessionID     = leasesession.sessionid();
                lease->token         = leasesession.token();
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

        if (!infoComplete) {
            LOG(ERROR) << "file info not complete!";
            return LIBCURVE_ERROR::FAILED;
        }

        auto retcode = response.statuscode();
        if (curve::mds::StatusCode::kOK == retcode) {
            return LIBCURVE_ERROR::OK;
        } else if (curve::mds::StatusCode::kFileExists == retcode) {
            LOG(WARNING) << "file already exists!";
            return LIBCURVE_ERROR::EXISTS;
        } else if (curve::mds::StatusCode::kOwnerAuthFail
                                                    == response.statuscode()) {
            LOG(ERROR) << "auth failed!";
            return LIBCURVE_ERROR::AUTHFAIL;
        }

        return LIBCURVE_ERROR::FAILED;
    }
    return LIBCURVE_ERROR::FAILED;
}

LIBCURVE_ERROR MDSClient::CreateFile(std::string filename, size_t size) {
    // 记录当前mds重试次数
    int count = 0;
    // 记录还没重试的mds addr数量
    int mdsAddrleft = metaServerOpt_.metaaddrvec.size() - 1;

    while (count < metaServerOpt_.rpcRetryTimes) {
        brpc::Controller cntl;
        cntl.set_timeout_ms(metaServerOpt_.rpcTimeoutMs);
        cntl.set_max_retry(metaServerOpt_.rpcRetryTimes);

        curve::mds::CreateFileRequest request;
        curve::mds::CreateFileResponse response;
        request.set_filename(filename);
        request.set_filetype(curve::mds::FileType::INODE_PAGEFILE);
        request.set_filelength(size);
        request.set_owner(userinfo_.owner);
        if (userinfo_.password != "") {
            request.set_password(userinfo_.password);
        }

        {
            ReadLockGuard readGuard(rwlock_);
            curve::mds::CurveFSService_Stub stub(channel_);
            stub.CreateFile(&cntl, &request, &response, NULL);
        }

        if (cntl.Failed()) {
            LOG(ERROR) << "Create file failed, errcorde = "
                        << response.statuscode()
                        << ", error content:"
                        << cntl.ErrorText()
                        << ", retry CreateFile, retry times = "
                        << count;

            if (!UpdateRetryinfoOrChangeServer(&count, &mdsAddrleft)) {
                break;
            }
            continue;
        }

        curve::mds::StatusCode stcode = response.statuscode();
        if (stcode == curve::mds::StatusCode::kFileExists) {
            return LIBCURVE_ERROR::EXISTS;
        } else if (stcode == curve::mds::StatusCode::kOK) {
            return LIBCURVE_ERROR::OK;
        } else if (curve::mds::StatusCode::kOwnerAuthFail == stcode) {
            LOG(ERROR) << "auth failed!";
            return LIBCURVE_ERROR::AUTHFAIL;
        }

        return LIBCURVE_ERROR::FAILED;
    }
    return LIBCURVE_ERROR::FAILED;
}

LIBCURVE_ERROR MDSClient::CloseFile(std::string fname, std::string sessionid) {
    // 记录当前mds重试次数
    int count = 0;
    // 记录还没重试的mds addr数量
    int mdsAddrleft = metaServerOpt_.metaaddrvec.size() - 1;

    while (count < metaServerOpt_.rpcRetryTimes) {
        brpc::Controller cntl;
        cntl.set_timeout_ms(metaServerOpt_.rpcTimeoutMs);
        cntl.set_max_retry(metaServerOpt_.rpcRetryTimes);

        curve::mds::CloseFileRequest request;
        curve::mds::CloseFileResponse response;
        request.set_filename(fname);
        request.set_sessionid(sessionid);
        request.set_owner(userinfo_.owner);
        if (userinfo_.password != "") {
            request.set_password(userinfo_.password);
        }

        {
            ReadLockGuard readGuard(rwlock_);
            curve::mds::CurveFSService_Stub stub(channel_);
            stub.CloseFile(&cntl, &request, &response, nullptr);
        }

        if (cntl.Failed()) {
            LOG(ERROR) << "close file failed, errcorde = "
                        << response.statuscode()
                        << ", error content:"
                        << cntl.ErrorText()
                        << ", retry CloseFile, retry times = "
                        << count;

            if (!UpdateRetryinfoOrChangeServer(&count, &mdsAddrleft)) {
                break;
            }
            continue;
        }

        auto retcode = response.statuscode();
        if (retcode == ::curve::mds::StatusCode::kOK) {
            return LIBCURVE_ERROR::OK;
        } else if (retcode == ::curve::mds::StatusCode::kFileNotExists) {
            LOG(WARNING) << "file not exists!";
            return LIBCURVE_ERROR::NOTEXIST;
        } else if (curve::mds::StatusCode::kOwnerAuthFail == response.statuscode()) {   // NOLINT
            LOG(ERROR) << "auth failed!";
            return LIBCURVE_ERROR::AUTHFAIL;
        }

        return LIBCURVE_ERROR::FAILED;
    }
    return LIBCURVE_ERROR::FAILED;
}

LIBCURVE_ERROR MDSClient::GetOrAllocateSegment(bool allocate,
                                    LogicalPoolCopysetIDInfo* lpcsIDInfo,
                                    uint64_t offset,
                                    const FInfo_t* fi,
                                    MetaCache* mc) {
    // 记录当前mds重试次数
    int count = 0;
    // 记录还没重试的mds addr数量
    int mdsAddrleft = metaServerOpt_.metaaddrvec.size() - 1;

    while (count < metaServerOpt_.rpcRetryTimes) {
        brpc::Controller cntl;
        cntl.set_timeout_ms(metaServerOpt_.rpcTimeoutMs);
        cntl.set_max_retry(metaServerOpt_.rpcRetryTimes);

        curve::mds::GetOrAllocateSegmentRequest request;
        curve::mds::GetOrAllocateSegmentResponse response;

        // convert the user offset to seg  offset
        uint64_t segmentsize = fi->segmentsize;
        uint64_t chunksize = fi->chunksize;
        uint64_t seg_offset = (offset / segmentsize) * segmentsize;

        request.set_filename(fi->filename);
        request.set_offset(seg_offset);
        request.set_allocateifnotexist(allocate);
        request.set_owner(userinfo_.owner);
        if (userinfo_.password != "") {
            request.set_password(userinfo_.password);
        }

        {
            ReadLockGuard readGuard(rwlock_);
            curve::mds::CurveFSService_Stub stub(channel_);
            stub.GetOrAllocateSegment(&cntl, &request, &response, NULL);
        }

        DVLOG(9) << "Get segment at offset: " << seg_offset
                << "Response status: " << response.statuscode();

        if (cntl.Failed()) {
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
                if (pfs.has_startoffset()) {
                    startoffset = pfs.startoffset();
                } else {
                    LOG(ERROR) << "page file segment has no startoffset info";
                    break;
                }
                lpcsIDInfo->lpid = logicpoolid;

                int chunksNum = pfs.chunks_size();
                if (allocate && chunksNum <= 0) {
                    LOG(ERROR) << "MDS allocate segment, but no chunkinfo!";
                    break;
                }
                DVLOG(9) << "update meta cache of " << chunksNum
                        << " chunks, at offset: " << startoffset;
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
                        lpcsIDInfo->cpidVec.push_back(copysetid);
                    } else {
                        LOG(ERROR) << "page file segment has no copysetid info";
                        break;
                    }

                    ChunkIndex cindex = (startoffset + i*chunksize)/chunksize;

                    ChunkIDInfo_t chunkinfo;
                    chunkinfo.lpid_ = logicpoolid;
                    chunkinfo.cpid_ = copysetid;
                    chunkinfo.cid_  = chunkid;
                    mc->UpdateChunkInfoByIndex(cindex, chunkinfo);
                    DVLOG(9) << " pool id: " << logicpoolid
                            << " copyset id: " << copysetid
                            << " cindex: " << cindex
                            << " chunk id: " << chunkid;
                }
            }
            return LIBCURVE_ERROR::OK;
        }
        return LIBCURVE_ERROR::FAILED;
    }
    return LIBCURVE_ERROR::FAILED;
}

LIBCURVE_ERROR MDSClient::GetFileInfo(std::string filename, FInfo_t* fi) {
    // 记录当前mds重试次数
    int count = 0;
    // 记录还没重试的mds addr数量
    int mdsAddrleft = metaServerOpt_.metaaddrvec.size() - 1;

    while (count < metaServerOpt_.rpcRetryTimes) {
        brpc::Controller cntl;
        cntl.set_timeout_ms(metaServerOpt_.rpcTimeoutMs);
        cntl.set_max_retry(metaServerOpt_.rpcRetryTimes);

        curve::mds::GetFileInfoRequest request;
        curve::mds::GetFileInfoResponse response;
        request.set_filename(filename);
        request.set_owner(userinfo_.owner);
        if (userinfo_.password != "") {
            request.set_password(userinfo_.password);
        }

        {
            ReadLockGuard readGuard(rwlock_);
            curve::mds::CurveFSService_Stub stub(channel_);
            stub.GetFileInfo(&cntl, &request, &response, nullptr);
        }

        if (cntl.Failed()) {
            LOG(ERROR)  << "get file info failed, error content:"
                        << cntl.ErrorText()
                        << ", retry GetFileInfo, retry times = "
                        << count;

            if (!UpdateRetryinfoOrChangeServer(&count, &mdsAddrleft)) {
                break;
            }
            continue;
        }

        if (response.has_fileinfo()) {
            curve::mds::FileInfo finfo = response.fileinfo();
            ServiceHelper::ProtoFileInfo2Local(&finfo, fi);
        } else {
            LOG(ERROR) << "response has no file info, try again!";
            break;
        }

        auto retcode = response.statuscode();
        if (retcode == curve::mds::StatusCode::kOK) {
            return LIBCURVE_ERROR::OK;
        } else if (retcode == ::curve::mds::StatusCode::kFileNotExists) {
            return LIBCURVE_ERROR::NOTEXIST;
        } else if (curve::mds::StatusCode::kOwnerAuthFail== retcode) {
            LOG(ERROR) << "auth failed!";
            return LIBCURVE_ERROR::AUTHFAIL;
        }

        return LIBCURVE_ERROR::FAILED;
    }

    return LIBCURVE_ERROR::FAILED;
}

LIBCURVE_ERROR MDSClient::CreateSnapShot(std::string filename,
                                        UserInfo_t userinfo,
                                        uint64_t* seq) {
    // 记录当前mds重试次数
    int count = 0;
    // 记录还没重试的mds addr数量
    int mdsAddrleft = metaServerOpt_.metaaddrvec.size() - 1;

    while (count < metaServerOpt_.rpcRetryTimes) {
        brpc::Controller cntl;
        cntl.set_timeout_ms(metaServerOpt_.rpcTimeoutMs);
        cntl.set_max_retry(metaServerOpt_.rpcRetryTimes);

        ::curve::mds::CreateSnapShotRequest request;
        ::curve::mds::CreateSnapShotResponse response;

        request.set_filename(filename);
        request.set_owner(userinfo.owner);
        if (userinfo.password != "") {
            request.set_password(userinfo.password);
        }

        {
            ReadLockGuard readGuard(rwlock_);
            curve::mds::CurveFSService_Stub stub(channel_);
            stub.CreateSnapShot(&cntl, &request, &response, nullptr);
        }

        if (cntl.Failed()) {
            LOG(ERROR) << "create snap file failed, errcorde = "
                        << response.statuscode()
                        << ", error content:"
                        << cntl.ErrorText()
                        << ", retry CreateSnapShot, retry times = "
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
        } else if (curve::mds::StatusCode::kFileUnderSnapShot == stcode) {
            return LIBCURVE_ERROR::UNDER_SNAPSHOT;
        }

        LOG_IF(ERROR, stcode != ::curve::mds::StatusCode::kOK)
        << "cretae snap file failed, errcode = " << stcode;

        if (stcode == ::curve::mds::StatusCode::kOK &&
            response.has_snapshotfileinfo()) {
            FInfo_t* fi = new (std::nothrow) FInfo_t;
            curve::mds::FileInfo finfo = response.snapshotfileinfo();
            ServiceHelper::ProtoFileInfo2Local(&finfo, fi);
            *seq = fi->seqnum;
            delete fi;
            return LIBCURVE_ERROR::OK;
        }
        return LIBCURVE_ERROR::FAILED;
    }
    return LIBCURVE_ERROR::FAILED;
}

LIBCURVE_ERROR MDSClient::DeleteSnapShot(std::string filename,
                                         UserInfo_t userinfo,
                                         uint64_t seq) {
    // 记录当前mds重试次数
    int count = 0;
    // 记录还没重试的mds addr数量
    int mdsAddrleft = metaServerOpt_.metaaddrvec.size() - 1;

    LIBCURVE_ERROR ret = LIBCURVE_ERROR::FAILED;
    while (count < metaServerOpt_.rpcRetryTimes) {
        brpc::Controller cntl;
        cntl.set_timeout_ms(metaServerOpt_.rpcTimeoutMs);
        cntl.set_max_retry(metaServerOpt_.rpcRetryTimes);

        ::curve::mds::DeleteSnapShotRequest request;
        ::curve::mds::DeleteSnapShotResponse response;

        request.set_seq(seq);
        request.set_filename(filename);
        request.set_owner(userinfo.owner);
        if (userinfo.password != "") {
            request.set_password(userinfo.password);
        }

        {
            ReadLockGuard readGuard(rwlock_);
            curve::mds::CurveFSService_Stub stub(channel_);
            stub.DeleteSnapShot(&cntl, &request, &response, nullptr);
        }

        if (cntl.Failed()) {
            LOG(ERROR) << "delete snap file failed, errcorde = "
                        << response.statuscode()
                        << ", error content:"
                        << cntl.ErrorText()
                        << ", retry DeleteSnapShot, retry times = "
                        << count;

            if (!UpdateRetryinfoOrChangeServer(&count, &mdsAddrleft)) {
                break;
            }
            continue;
        }
        ::curve::mds::StatusCode stcode = response.statuscode();

        if (curve::mds::StatusCode::kOwnerAuthFail == stcode) {
            return LIBCURVE_ERROR::AUTHFAIL;
            LOG(ERROR) << "auth failed!";
        }

        LOG_IF(ERROR, stcode != ::curve::mds::StatusCode::kOK)
        << "delete snap file failed, errcode = " << stcode;

        if (stcode == ::curve::mds::StatusCode::kOK) {
            return LIBCURVE_ERROR::OK;
        } else if (stcode == ::curve::mds::StatusCode::kSnapshotFileNotExists) {
            return LIBCURVE_ERROR::NOTEXIST;
        }
        return LIBCURVE_ERROR::FAILED;
    }
    return LIBCURVE_ERROR::FAILED;
}

LIBCURVE_ERROR MDSClient::GetSnapShot(std::string filename,
                                        UserInfo_t userinfo,
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
        cntl.set_max_retry(metaServerOpt_.rpcRetryTimes);

        ::curve::mds::ListSnapShotFileInfoRequest request;
        ::curve::mds::ListSnapShotFileInfoResponse response;

        request.set_filename(filename);
        request.add_seq(seq);
        request.set_owner(userinfo.owner);
        if (userinfo.password != "") {
            request.set_password(userinfo.password);
        }

        {
            ReadLockGuard readGuard(rwlock_);
            curve::mds::CurveFSService_Stub stub(channel_);
            stub.ListSnapShot(&cntl, &request, &response, nullptr);
        }

        if (cntl.Failed()) {
            LOG(ERROR) << "list snap file failed, errcorde = "
                        << response.statuscode()
                        << ", error content:"
                        << cntl.ErrorText()
                        << ", retry GetSnapShot, retry times = "
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

        LOG_IF(ERROR, stcode != ::curve::mds::StatusCode::kOK)
        << "list snap file failed, errcode = " << stcode;

        auto size = response.fileinfo_size();
        for (int i = 0; i < size; i++) {
            curve::mds::FileInfo finfo = response.fileinfo(i);
            ServiceHelper::ProtoFileInfo2Local(&finfo, fi);
        }

        if (stcode == ::curve::mds::StatusCode::kOK) {
            return LIBCURVE_ERROR::OK;
        } else if (stcode == ::curve::mds::StatusCode::kSnapshotFileNotExists) {
            return LIBCURVE_ERROR::NOTEXIST;
        }
        return LIBCURVE_ERROR::FAILED;
    }
    return LIBCURVE_ERROR::FAILED;
}

LIBCURVE_ERROR MDSClient::ListSnapShot(std::string filename,
                                        UserInfo_t userinfo,
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
        cntl.set_max_retry(metaServerOpt_.rpcRetryTimes);

        ::curve::mds::ListSnapShotFileInfoRequest request;
        ::curve::mds::ListSnapShotFileInfoResponse response;

        request.set_filename(filename);
        request.set_owner(userinfo.owner);
        if (userinfo.password != "") {
            request.set_password(userinfo.password);
        }

        if ((*seq).size() > (*snapif).size()) {
            LOG(ERROR) << "resource not enough!";
            return LIBCURVE_ERROR::FAILED;
        }
        for (int i = 0; i < (*seq).size(); i++) {
            request.add_seq((*seq)[i]);
        }

        {
            ReadLockGuard readGuard(rwlock_);
            curve::mds::CurveFSService_Stub stub(channel_);
            stub.ListSnapShot(&cntl, &request, &response, nullptr);
        }

        if (cntl.Failed()) {
            LOG(ERROR) << "list snap file failed, errcorde = "
                        << response.statuscode()
                        << ", error content:"
                        << cntl.ErrorText()
                        << ", retry ListSnapShot, retry times = "
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

        LOG_IF(ERROR, stcode != ::curve::mds::StatusCode::kOK)
        << "list snap file failed, errcode = " << stcode;

        auto size = response.fileinfo_size();
        for (int i = 0; i < size; i++) {
            curve::mds::FileInfo finfo = response.fileinfo(i);
            ServiceHelper::ProtoFileInfo2Local(&finfo, (*snapif)[i]);
        }

        if (stcode == ::curve::mds::StatusCode::kOK) {
            return LIBCURVE_ERROR::OK;
        } else if (stcode == ::curve::mds::StatusCode::kSnapshotFileNotExists) {
            return LIBCURVE_ERROR::NOTEXIST;
        }

        return LIBCURVE_ERROR::FAILED;
    }
    return LIBCURVE_ERROR::FAILED;
}

LIBCURVE_ERROR MDSClient::GetSnapshotSegmentInfo(std::string filename,
                                        UserInfo_t userinfo,
                                        LogicalPoolCopysetIDInfo* lpcsIDInfo,
                                        uint64_t seq,
                                        uint64_t offset,
                                        SegmentInfo *segInfo,
                                        MetaCache* mc) {
    // 记录当前mds重试次数
    int count = 0;
    // 记录还没重试的mds addr数量
    int mdsAddrleft = metaServerOpt_.metaaddrvec.size() - 1;

    while (count < metaServerOpt_.rpcRetryTimes) {
        brpc::Controller cntl;
        cntl.set_timeout_ms(metaServerOpt_.rpcTimeoutMs);
        cntl.set_max_retry(metaServerOpt_.rpcRetryTimes);

        ::curve::mds::GetOrAllocateSegmentRequest request;
        ::curve::mds::GetOrAllocateSegmentResponse response;

        request.set_filename(filename);
        request.set_offset(offset);
        request.set_allocateifnotexist(false);
        request.set_seqnum(seq);
        request.set_owner(userinfo.owner);
        if (userinfo.password != "") {
            request.set_password(userinfo.password);
        }

        {
            ReadLockGuard readGuard(rwlock_);
            curve::mds::CurveFSService_Stub stub(channel_);
            stub.GetSnapShotFileSegment(&cntl, &request, &response, nullptr);
        }

        if (cntl.Failed()) {
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

        if (stcode == ::curve::mds::StatusCode::kOK ||
            stcode == ::curve::mds::StatusCode::kSegmentNotAllocated) {
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
                    LOG(ERROR) << "pagefile segment has no offset info";
                    break;
                }

                lpcsIDInfo->lpid = logicpoolid;

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
                        lpcsIDInfo->cpidVec.push_back(copysetid);
                    } else {
                        LOG(ERROR) << "pagefile segment has no copysetid info";
                        break;
                    }

                    segInfo->chunkvec.push_back(ChunkIDInfo(chunkid,
                                                            logicpoolid,
                                                            copysetid));
                    mc->UpdateChunkInfoByID(logicpoolid,
                                            copysetid,
                                            chunkid);

                    DVLOG(9) << "chunk id: " << chunkid
                            << " pool id: " << logicpoolid
                            << " copyset id: " << copysetid
                            << " chunk id: " << chunkid;
                }
            }
            return LIBCURVE_ERROR::OK;
        }

        if (stcode == ::curve::mds::StatusCode::kSnapshotFileNotExists) {
            return LIBCURVE_ERROR::NOTEXIST;
        }
        return LIBCURVE_ERROR::FAILED;
    }
    return LIBCURVE_ERROR::FAILED;
}

LIBCURVE_ERROR MDSClient::RefreshSession(std::string filename,
                                std::string sessionid,
                                uint64_t date,
                                std::string signature,
                                leaseRefreshResult* resp) {
    brpc::Controller cntl;
    cntl.set_timeout_ms(metaServerOpt_.rpcTimeoutMs);
    cntl.set_max_retry(metaServerOpt_.rpcRetryTimes);

    curve::mds::CurveFSService_Stub stub(channel_);
    curve::mds::ReFreshSessionRequest request;
    curve::mds::ReFreshSessionResponse response;

    request.set_signature("");
    request.set_filename(filename);
    request.set_sessionid(sessionid);
    request.set_date(TimeUtility::GetTimeofDayUs());
    request.set_owner(userinfo_.owner);
    if (userinfo_.password != "") {
        request.set_password(userinfo_.password);
    }

    {
        ReadLockGuard readGuard(rwlock_);
        curve::mds::CurveFSService_Stub stub(channel_);
        stub.RefreshSession(&cntl, &request, &response, nullptr);
    }

    if (cntl.Failed()) {
        LOG(ERROR) << "Fail to send ReFreshSessionRequest, "
                    << cntl.ErrorText();
        return LIBCURVE_ERROR::FAILED;
    }

    curve::mds::StatusCode stcode = response.statuscode();
    if (curve::mds::StatusCode::kOwnerAuthFail == stcode) {
        resp->status = leaseRefreshResult::Status::FAILED;
        LOG(ERROR) << "auth failed!";
        return LIBCURVE_ERROR::AUTHFAIL;
    }

    if (stcode == curve::mds::StatusCode::kSessionNotExist
        || stcode == curve::mds::StatusCode::kFileNotExists) {
        resp->status = leaseRefreshResult::Status::NOT_EXIST;
        LOG(ERROR) << "file not exist!";
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

LIBCURVE_ERROR MDSClient::CheckSnapShotStatus(std::string filename,
                                              UserInfo_t userinfo,
                                              uint64_t seq) {
    return LIBCURVE_ERROR::OK;
    /*
    LIBCURVE_ERROR ret = LIBCURVE_ERROR::FAILED;

    brpc::Controller cntl;
    cntl.set_timeout_ms(metaServerOpt_.rpc_timeout_ms);
    cntl.set_max_retry(metaServerOpt_.rpc_retry_times);

    curve::mds::CurveFSService_Stub stub(channel_);
    ::curve::mds::CheckSnapShotStatusRequest request;
    ::curve::mds::CheckSnapShotStatusResponse response;

    request.set_seq(seq);
    request.set_filename(filename);
    request.set_owner(userinfo.owner);
    if (userinfo.password != "") {
        request.set_password(userinfo.password);
    }

    stub.CheckSnapShotStatus(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        LOG(ERROR) << "check snap file failed, errcorde = "
                    << response.statuscode()
                    << ", error content:"
                    << cntl.ErrorText();
    } else {
        ::curve::mds::StatusCode stcode = response.statuscode();

        if (curve::mds::StatusCode::kSnapshotFileDeleteError == stcode) {
            LOG(ERROR) << "delete error!";
            ret = LIBCURVE_ERROR::DELETE_ERROR;
        } else if (curve::mds::StatusCode::kOwnerAuthFail == stcode) {
            LOG(ERROR) << "auth failed!";
            ret = LIBCURVE_ERROR::AUTHFAIL;
        } else if (curve::mds::StatusCode::kSnapshotDeleting == stcode) {
            ret = LIBCURVE_ERROR::DELETING;
        } else if (curve::mds::StatusCode::kSnapshotFileNotExists == stcode) {
            LOG(ERROR) << "snapshot file not exist!";
            ret = LIBCURVE_ERROR::NOTEXIST;
        } else if (stcode == curve::mds::StatusCode::kOK) {
            ret = LIBCURVE_ERROR::OK;
        }
    }
    return ret;
    */
}

LIBCURVE_ERROR MDSClient::GetServerList(const LogicPoolID& lpid,
                            const std::vector<CopysetID>& csid,
                            MetaCache* mc) {
    // 记录当前mds重试次数
    int count = 0;
    // 记录还没重试的mds addr数量
    int mdsAddrleft = metaServerOpt_.metaaddrvec.size() - 1;

    while (count < metaServerOpt_.rpcRetryTimes) {
        brpc::Controller cntl;
        cntl.set_timeout_ms(metaServerOpt_.rpcTimeoutMs);
        cntl.set_max_retry(metaServerOpt_.rpcRetryTimes);

        curve::mds::topology::GetChunkServerListInCopySetsRequest request;
        curve::mds::topology::GetChunkServerListInCopySetsResponse response;

        request.set_logicalpoolid(lpid);
        for (auto iter : csid) {
            request.add_copysetid(iter);
        }

        {
            ReadLockGuard readGuard(rwlock_);
            curve::mds::topology::TopologyService_Stub stub(channel_);
            stub.GetChunkServerListInCopySets(&cntl, &request, &response, nullptr);     // NOLINT
        }

        if (cntl.Failed()) {
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
            curve::mds::topology::CopySetServerInfo info = response.csinfo(i);
            CopysetID csid = info.copysetid();
            CopysetInfo_t copysetseverl;

            int cslocsNum = info.cslocs_size();
            for (int j = 0; j < cslocsNum; j++) {
                curve::mds::topology::ChunkServerLocation csl = info.cslocs(j);

                CopysetPeerInfo_t csinfo;
                uint16_t port           = csl.port();
                std::string hostip      = csl.hostip();
                csinfo.chunkserverid_   = csl.chunkserverid();

                EndPoint ep;
                butil::str2endpoint(hostip.c_str(), port, &ep);
                PeerId pd(ep);
                csinfo.peerid_ = pd;

                copysetseverl.AddCopysetPeerInfo(csinfo);
            }
            mc->UpdateCopysetInfo(lpid, csid, copysetseverl);
        }

        if (response.statuscode() == 0) {
            return LIBCURVE_ERROR::OK;
        }
        return LIBCURVE_ERROR::FAILED;
    }
    return LIBCURVE_ERROR::FAILED;
}
}   // namespace client
}   // namespace curve
