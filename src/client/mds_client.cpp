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
 * File Created: Monday, 18th February 2019 6:25:25 pm
 * Author: tongguangxun
 */
#include "src/client/mds_client.h"

#include <glog/logging.h>

#include <utility>
#include <algorithm>

#include "src/client/lease_executor.h"
#include "src/client/metacache.h"
#include "src/common/net_common.h"
#include "src/common/string_util.h"
#include "src/common/timeutility.h"

namespace curve {
namespace client {

using curve::common::NetCommon;
using curve::common::TimeUtility;
using curve::common::ReadLockGuard;
using curve::common::WriteLockGuard;
using curve::mds::StatusCode;
using curve::mds::FileInfo;
using curve::mds::PageFileChunkInfo;
using curve::mds::PageFileSegment;
using curve::mds::CreateFileResponse;
using curve::mds::DeleteFileResponse;
using curve::mds::RecoverFileResponse;
using curve::mds::GetFileInfoResponse;
using curve::mds::GetOrAllocateSegmentResponse;
using curve::mds::RenameFileResponse;
using curve::mds::ExtendFileResponse;
using curve::mds::ChangeOwnerResponse;
using curve::mds::ListDirResponse;
using curve::mds::CreateSnapShotResponse;
using curve::mds::ListSnapShotFileInfoResponse;
using curve::mds::DeleteSnapShotResponse;
using curve::mds::CheckSnapShotStatusResponse;
using curve::mds::SessionStatus;
using curve::mds::ProtoSession;
using curve::mds::OpenFileResponse;
using curve::mds::CloseFileResponse;
using curve::mds::ReFreshSessionResponse;
using curve::mds::CreateCloneFileResponse;
using curve::mds::SetCloneFileStatusResponse;
using curve::mds::topology::CopySetServerInfo;
using curve::mds::topology::ChunkServerLocation;
using curve::mds::topology::GetChunkServerListInCopySetsResponse;

MDSClient::MDSClient(const std::string& metricPrefix)
    : mdsClientMetric_(metricPrefix) {
    inited_   = false;
}

LIBCURVE_ERROR MDSClient::Initialize(const MetaServerOption& metaServerOpt) {
    if (inited_) {
        LOG(INFO) << "MDSClient already started!";
        return LIBCURVE_ERROR::OK;
    }

    metaServerOpt_ = metaServerOpt;

    int rc = mdsClientBase_.Init(metaServerOpt_);
    if (rc != 0) {
        LOG(ERROR) << "mds client rpc base init failed!";
        return LIBCURVE_ERROR::FAILED;
    }

    rpcExcutor.SetOption(metaServerOpt);

    inited_ = true;
    return LIBCURVE_ERROR::OK;
}

void MDSClient::UnInitialize() {
    inited_ = false;
}

// rpc发送和mds地址切换状态机
LIBCURVE_ERROR MDSClient::MDSRPCExcutor::DoRPCTask(RPCFunc rpctask,
                                                   uint64_t maxRetryTimeMS) {
    // 记录上一次正在服务的mds index
    int lastWorkingMDSIndex = currentWorkingMDSAddrIndex_;

    // 记录当前正在使用的mds index
    int curRetryMDSIndex = currentWorkingMDSAddrIndex_;

    // 记录当前mds重试的次数
    uint64_t currentMDSRetryCount = 0;

    // 执行起始时间点
    uint64_t startTime = TimeUtility::GetTimeofDayMs();

    // rpc超时时间
    uint64_t rpcTimeOutMS = metaServerOpt_.mdsRPCTimeoutMs;

    int retcode = -1;
    bool needChangeMDS = false;
    while (GoOnRetry(startTime, maxRetryTimeMS)) {
        // 1. 创建当前rpc需要使用的channel和controller，执行rpc任务
        retcode = ExcuteTask(curRetryMDSIndex, rpcTimeOutMS, rpctask);

        // 2. 根据rpc返回值进行预处理
        if (retcode < 0) {
            curRetryMDSIndex = PreProcessBeforeRetry(retcode,
                               &currentMDSRetryCount, curRetryMDSIndex,
                               &lastWorkingMDSIndex, &rpcTimeOutMS);
            continue;
        }

        // 3. 此时rpc是正常返回的，更新当前正在服务的mds地址index
        currentWorkingMDSAddrIndex_.store(curRetryMDSIndex);
        return static_cast<LIBCURVE_ERROR>(retcode);
    }

    // 4. 重试超限，向上返回
    switch (retcode) {
        case -LIBCURVE_ERROR::NOT_SUPPORT:
            return LIBCURVE_ERROR::NOT_SUPPORT;
        case -LIBCURVE_ERROR::FILE_OCCUPIED:
            return LIBCURVE_ERROR::FILE_OCCUPIED;
        default:
            return LIBCURVE_ERROR::FAILED;
    }
}

bool MDSClient::MDSRPCExcutor::GoOnRetry(uint64_t startTimeMS,
                                         uint64_t maxRetryTimeMS) {
    uint64_t currentTime = TimeUtility::GetTimeofDayMs();
    return currentTime - startTimeMS < maxRetryTimeMS;
}

int MDSClient::MDSRPCExcutor::PreProcessBeforeRetry(int status,
                                                    uint64_t* curMDSRetryCount,
                                                    int curRetryMDSIndex,
                                                    int* lastWorkingMDSIndex,
                                                    uint64_t* timeOutMS) {
    int nextMDSIndex = 0;
    bool rpcTimeout = false;
    bool needChangeMDS = false;
    // 1. 访问存在的IP地址，但无人监听：ECONNREFUSED
    // 2. 正常发送RPC情况下，对端进程挂掉了：EHOSTDOWN
    // 3. 对端server调用了Stop：ELOGOFF
    // 4. 对端链接已关闭：ECONNRESET
    // 5. 在一个mds节点上rpc失败超过限定次数
    // 在这几种场景下，主动切换mds。
    if (status == -EHOSTDOWN || status == -ECONNRESET ||
        status == -ECONNREFUSED || status == -brpc::ELOGOFF ||
        *curMDSRetryCount >= metaServerOpt_.mdsMaxFailedTimesBeforeChangeMDS) {
        needChangeMDS = true;

        // 在开启健康检查的情况下，在底层tcp连接失败时
        // rpc请求会本地直接返回 EHOSTSOWN
        // 这种情况下，增加一些睡眠时间，避免大量的重试请求占满bthread
        // TODO(wuhanqing): 关闭健康检查
        if (status == -EHOSTDOWN) {
            bthread_usleep(metaServerOpt_.mdsRPCRetryIntervalUS);
        }
    } else if (status == -brpc::ERPCTIMEDOUT || status == -ETIMEDOUT) {
        rpcTimeout = true;
        needChangeMDS = false;
        // 触发超时指数退避
        *timeOutMS *= 2;
        *timeOutMS = std::min(*timeOutMS, metaServerOpt_.mdsMaxRPCTimeoutMS);
        *timeOutMS = std::max(*timeOutMS, metaServerOpt_.mdsRPCTimeoutMs);
    }

    // 获取下一次需要重试的mds索引
    nextMDSIndex = GetNextMDSIndex(needChangeMDS, curRetryMDSIndex, lastWorkingMDSIndex);   // NOLINT

    // 更新curMDSRetryCount和rpctimeout
    if (nextMDSIndex != curRetryMDSIndex) {
        *curMDSRetryCount = 0;
        *timeOutMS = metaServerOpt_.mdsRPCTimeoutMs;
    } else {
        ++(*curMDSRetryCount);
        // 还是在当前mds上重试，且rpc不是超时错误，就进行睡眠，然后再重试
        if (!rpcTimeout) {
            bthread_usleep(metaServerOpt_.mdsRPCRetryIntervalUS);
        }
    }

    return nextMDSIndex;
}
/**
 * 根据输入状态获取下一次需要重试的mds索引，mds切换逻辑：
 * 记录三个状态：curRetryMDSIndex、lastWorkingMDSIndex、
 *             currentWorkingMDSIndex
 * 1. 开始的时候curRetryMDSIndex = currentWorkingMDSIndex
 *            lastWorkingMDSIndex = currentWorkingMDSIndex
 * 2. 如果rpc失败，会触发切换curRetryMDSIndex，如果这时候lastWorkingMDSIndex
 *    与currentWorkingMDSIndex相等，这时候会顺序切换到下一个mds索引，
 *    如果lastWorkingMDSIndex与currentWorkingMDSIndex不相等，那么
 *    说明有其他接口更新了currentWorkingMDSAddrIndex_，那么本次切换
 *    直接切换到currentWorkingMDSAddrIndex_
 */
int MDSClient::MDSRPCExcutor::GetNextMDSIndex(bool needChangeMDS,
                                              int currentRetryIndex,
                                              int* lastWorkingindex) {
    int nextMDSIndex = 0;
    if (std::atomic_compare_exchange_strong(&currentWorkingMDSAddrIndex_,
        lastWorkingindex, currentWorkingMDSAddrIndex_.load())) {
        int size = metaServerOpt_.mdsAddrs.size();
        nextMDSIndex = needChangeMDS ? (currentRetryIndex + 1) % size
                                     : currentRetryIndex;
    } else {
        nextMDSIndex = *lastWorkingindex;
    }

    return nextMDSIndex;
}

int MDSClient::MDSRPCExcutor::ExcuteTask(int mdsindex,
                                         uint64_t rpcTimeOutMS,
                                         RPCFunc task) {
    const std::string& mdsaddr = metaServerOpt_.mdsAddrs[mdsindex];

    brpc::Channel channel;
    int ret = channel.Init(mdsaddr.c_str(), nullptr);
    if (ret != 0) {
        LOG(WARNING) << "Init channel failed! addr = " << mdsaddr;
        // 返回EHOSTDOWN给上层调用者，促使其切换mds
        return -EHOSTDOWN;
    }

    brpc::Controller cntl;
    cntl.set_log_id(GetLogId());
    cntl.set_timeout_ms(rpcTimeOutMS);

    return task(mdsindex, rpcTimeOutMS, &channel, &cntl);
}

#define RPCTaskDefine                                                \
    [&](int mdsindex, uint64_t rpctimeoutMS, brpc::Channel* channel, \
        brpc::Controller* cntl) -> int

LIBCURVE_ERROR MDSClient::OpenFile(const std::string& filename,
                                   const UserInfo_t& userinfo,
                                   FInfo_t* fi,
                                   LeaseSession* lease) {
    auto task = RPCTaskDefine {
        OpenFileResponse response;
        mdsClientMetric_.openFile.qps.count << 1;
        LatencyGuard lg(&mdsClientMetric_.openFile.latency);
        mdsClientBase_.OpenFile(filename, userinfo, &response, cntl, channel);

        if (cntl->Failed()) {
            mdsClientMetric_.openFile.eps.count << 1;
            LOG(WARNING) << "open file failed, errcorde = "
                << cntl->ErrorCode() << ", error content:"
                << cntl->ErrorText() << ", log id = " << cntl->log_id();
            return -cntl->ErrorCode();
        }

        LIBCURVE_ERROR retcode = LIBCURVE_ERROR::FAILED;
        StatusCode stcode = response.statuscode();
        MDSStatusCode2LibcurveError(stcode, &retcode);
        LOG_IF(WARNING, retcode != LIBCURVE_ERROR::OK)
            << "OpenFile: filename = " << filename
            << ", owner = " << userinfo.owner
            << ", errocde = " << retcode
            << ", error msg = " << StatusCode_Name(stcode)
            << ", log id = " << cntl->log_id();

        bool flag = response.has_protosession() && response.has_fileinfo();
        if (flag) {
            ProtoSession leasesession = response.protosession();
            lease->sessionID     = leasesession.sessionid();
            lease->leaseTime     = leasesession.leasetime();
            lease->createTime    = leasesession.createtime();

            const curve::mds::FileInfo& protoFileInfo = response.fileinfo();
            ServiceHelper::ProtoFileInfo2Local(protoFileInfo, fi);

            if (protoFileInfo.has_clonesource() &&
                protoFileInfo.filestatus() ==
                    curve::mds::FileStatus::kFileCloneMetaInstalled) {
                if (!response.has_clonesourcesegment()) {
                    LOG(WARNING) << filename
                                 << " has clone source and status is "
                                    "CloneMetaInstalled, but response does not "
                                    "contains clone source segment";
                    return retcode;
                }

                ServiceHelper::ProtoCloneSourceInfo2Local(response,
                                                          &fi->sourceInfo);
            }
        } else {
            LOG(WARNING) << "mds response has no file info or session info!";
            return LIBCURVE_ERROR::FAILED;
        }

        return retcode;
    };
    return rpcExcutor.DoRPCTask(task, metaServerOpt_.mdsMaxRetryMS);
}

LIBCURVE_ERROR MDSClient::CreateFile(const std::string& filename,
                                     const UserInfo_t& userinfo,
                                     size_t size,
                                     bool normalFile,
                                     uint64_t stripeUnit,
                                     uint64_t stripeCount) {
    auto task = RPCTaskDefine {
        CreateFileResponse response;
        mdsClientMetric_.createFile.qps.count << 1;
        LatencyGuard lg(&mdsClientMetric_.createFile.latency);
        mdsClientBase_.CreateFile(filename, userinfo, size, normalFile,
        stripeUnit, stripeCount, &response, cntl, channel);

        if (cntl->Failed()) {
            mdsClientMetric_.createFile.eps.count << 1;
            LOG(WARNING) << "Create file or directory failed, errcorde = "
                << cntl->ErrorCode() << ", error content:"
                << cntl->ErrorText() << ", log id = " << cntl->log_id();
            return -cntl->ErrorCode();
        }

        LIBCURVE_ERROR retcode;
        StatusCode stcode = response.statuscode();
        MDSStatusCode2LibcurveError(stcode, &retcode);
        LOG_IF(WARNING, retcode != LIBCURVE_ERROR::OK)
            << "CreateFile: filename = " << filename
            << ", owner = " << userinfo.owner
            << ", is nomalfile: " << normalFile
            << ", errocde = " << retcode
            << ", error msg = " << StatusCode_Name(stcode)
            << ", log id = " << cntl->log_id();
        return retcode;
    };
    return rpcExcutor.DoRPCTask(task, metaServerOpt_.mdsMaxRetryMS);
}

LIBCURVE_ERROR MDSClient::CloseFile(const std::string& filename,
                                    const UserInfo_t& userinfo,
                                    const std::string& sessionid) {
    auto task = RPCTaskDefine {
        CloseFileResponse response;
        mdsClientMetric_.closeFile.qps.count << 1;
        LatencyGuard lg(&mdsClientMetric_.closeFile.latency);
        mdsClientBase_.CloseFile(filename, userinfo, sessionid,
                                &response, cntl, channel);

        if (cntl->Failed()) {
            mdsClientMetric_.closeFile.eps.count << 1;
            LOG(WARNING) << "close file failed, errcorde = "
                << cntl->ErrorCode() << ", error content:"
                << cntl->ErrorText() << ", log id = " << cntl->log_id();
            return -cntl->ErrorCode();
        }

        LIBCURVE_ERROR retcode;
        StatusCode stcode = response.statuscode();
        MDSStatusCode2LibcurveError(stcode, &retcode);
        LOG_IF(WARNING, retcode != LIBCURVE_ERROR::OK)
            << "CloseFile: filename = " << filename
            << ", owner = " << userinfo.owner
            << ", sessionid = " << sessionid
            << ", errocde = " << retcode
            << ", error msg = " << StatusCode_Name(stcode)
            << ", log id = " << cntl->log_id();
        return retcode;
    };
    return rpcExcutor.DoRPCTask(task, metaServerOpt_.mdsMaxRetryMS);
}

LIBCURVE_ERROR MDSClient::GetFileInfo(const std::string& filename,
                                      const UserInfo_t& uinfo,
                                      FInfo_t* fi) {
    auto task = RPCTaskDefine {
        GetFileInfoResponse response;
        mdsClientMetric_.getFile.qps.count << 1;
        LatencyGuard lg(&mdsClientMetric_.getFile.latency);
        mdsClientBase_.GetFileInfo(filename, uinfo, &response, cntl, channel);

        if (cntl->Failed()) {
            mdsClientMetric_.getFile.eps.count << 1;
            return -cntl->ErrorCode();
        }

        if (response.has_fileinfo()) {
            ServiceHelper::ProtoFileInfo2Local(response.fileinfo(), fi);
        }

        LIBCURVE_ERROR retcode;
        StatusCode stcode = response.statuscode();
        MDSStatusCode2LibcurveError(stcode, &retcode);
        LOG_IF(WARNING, retcode != LIBCURVE_ERROR::OK)
            << "GetFileInfo: filename = " << filename
            << ", owner = " << uinfo.owner
            << ", errocde = " << retcode
            << ", error msg = " << StatusCode_Name(stcode)
            << ", log id = " << cntl->log_id();
        return retcode;
    };
    return rpcExcutor.DoRPCTask(task, metaServerOpt_.mdsMaxRetryMS);
}

LIBCURVE_ERROR MDSClient::CreateSnapShot(const std::string& filename,
                                         const UserInfo_t& userinfo,
                                         uint64_t* seq) {
    auto task = RPCTaskDefine {
        CreateSnapShotResponse response;
        mdsClientBase_.CreateSnapShot(filename, userinfo,
                                      &response, cntl, channel);

        if (cntl->Failed()) {
            LOG(WARNING) << "create snap file failed, errcorde = "
                << cntl->ErrorCode() << ", error content:"
                << cntl->ErrorText() << ", log id = " << cntl->log_id();
            return -cntl->ErrorCode();
        }

        bool hasinfo = response.has_snapshotfileinfo();
        StatusCode stcode = response.statuscode();

        if ((stcode == StatusCode::kOK ||
             stcode == StatusCode::kFileUnderSnapShot) &&
            hasinfo) {
            FInfo_t* fi = new (std::nothrow) FInfo_t;
            ServiceHelper::ProtoFileInfo2Local(response.snapshotfileinfo(), fi);
            *seq = fi->seqnum;
            delete fi;
            if (stcode == StatusCode::kOK) {
                return LIBCURVE_ERROR::OK;
            } else {
                return LIBCURVE_ERROR::UNDER_SNAPSHOT;
            }
        } else if (!hasinfo && stcode == StatusCode::kOK) {
            LOG(WARNING) << "mds side response has no snapshot file info!";
            return LIBCURVE_ERROR::FAILED;
        }

        if (hasinfo) {
            FInfo_t fi;
            ServiceHelper::ProtoFileInfo2Local(response.snapshotfileinfo(), &fi);  // NOLINT
            *seq = fi.seqnum;
        }

        LIBCURVE_ERROR retcode;
        MDSStatusCode2LibcurveError(stcode, &retcode);
        LOG_IF(WARNING, retcode != LIBCURVE_ERROR::OK)
                << "CreateSnapShot: filename = " << filename
                << ", owner = " << userinfo.owner
                << ", errocde = " << retcode
                << ", error msg = " << StatusCode_Name(stcode)
                << ", log id = " << cntl->log_id();
        return retcode;
    };
    return rpcExcutor.DoRPCTask(task, metaServerOpt_.mdsMaxRetryMS);
}

LIBCURVE_ERROR MDSClient::DeleteSnapShot(const std::string& filename,
                                         const UserInfo_t& userinfo,
                                         uint64_t seq) {
    auto task = RPCTaskDefine {
        DeleteSnapShotResponse response;
        mdsClientBase_.DeleteSnapShot(filename, userinfo, seq,
                                     &response, cntl, channel);

        if (cntl->Failed()) {
            LOG(WARNING) << "delete snap file failed, errcorde = "
                << cntl->ErrorCode() << ", error content:"
                << cntl->ErrorText() << ", log id = " << cntl->log_id();
            return -cntl->ErrorCode();
        }

        LIBCURVE_ERROR retcode;
        StatusCode stcode = response.statuscode();
        MDSStatusCode2LibcurveError(stcode, &retcode);
        LOG_IF(WARNING, retcode != LIBCURVE_ERROR::OK)
                << "DeleteSnapShot: filename = " << filename
                << ", owner = " << userinfo.owner
                << ", seqnum = " << seq << ", errocde = " << retcode
                << ", error msg = " << StatusCode_Name(stcode)
                << ", log id = " << cntl->log_id();
        return retcode;
    };
    return rpcExcutor.DoRPCTask(task, metaServerOpt_.mdsMaxRetryMS);
}

LIBCURVE_ERROR MDSClient::ListSnapShot(const std::string& filename,
                                       const UserInfo_t& userinfo,
                                       const std::vector<uint64_t>* seq,
                                       std::map<uint64_t, FInfo>* snapif) {
    auto task = RPCTaskDefine {
        ListSnapShotFileInfoResponse response;
        mdsClientBase_.ListSnapShot(filename, userinfo, seq,
                                    &response, cntl, channel);

        if (cntl->Failed()) {
            LOG(WARNING) << "list snap file failed, errcorde = "
                << cntl->ErrorCode() << ", error content:"
                << cntl->ErrorText() << ", log id = " << cntl->log_id();
            return -cntl->ErrorCode();
        }

        LIBCURVE_ERROR retcode;
        StatusCode stcode = response.statuscode();
        MDSStatusCode2LibcurveError(stcode, &retcode);
        LOG_IF(WARNING, retcode != LIBCURVE_ERROR::OK)
                << "ListSnapShot: filename = " << filename
                << ", owner = " << userinfo.owner
                << ", errocde = " << retcode
                << ", error msg = " << StatusCode_Name(stcode);

        if (stcode == StatusCode::kOwnerAuthFail) {
            return LIBCURVE_ERROR::AUTHFAIL;
        }

        for (int i = 0; i < response.fileinfo_size(); i++) {
            FInfo_t tempInfo;
            ServiceHelper::ProtoFileInfo2Local(response.fileinfo(i), &tempInfo);
            snapif->insert(std::make_pair(tempInfo.seqnum, tempInfo));
        }

        if (response.fileinfo_size() != seq->size()) {
            LOG(WARNING) << "some snapshot info not found!";
            return LIBCURVE_ERROR::NOTEXIST;
        }

        return retcode;
    };
    return rpcExcutor.DoRPCTask(task, metaServerOpt_.mdsMaxRetryMS);
}

LIBCURVE_ERROR MDSClient::GetSnapshotSegmentInfo(const std::string& filename,
                                                 const UserInfo_t& userinfo,
                                                 uint64_t seq,
                                                 uint64_t offset,
                                                 SegmentInfo* segInfo) {
    auto task = RPCTaskDefine {
        GetOrAllocateSegmentResponse response;
        mdsClientBase_.GetSnapshotSegmentInfo(filename, userinfo, seq, offset,
                                              &response, cntl, channel);
        if (cntl->Failed()) {
            LOG(WARNING) << "get snap file segment info failed, errcorde = "
                << cntl->ErrorCode()
                << ", error content:" << cntl->ErrorText();
            return -cntl->ErrorCode();
        }

        LIBCURVE_ERROR retcode;
        StatusCode stcode = response.statuscode();
        MDSStatusCode2LibcurveError(stcode, &retcode);
        LOG_IF(WARNING, retcode != LIBCURVE_ERROR::OK)
                << "GetSnapshotSegmentInfo: filename = " << filename
                << ", owner = " << userinfo.owner
                << ", offset = " << offset
                << ", seqnum = " << seq
                << ", errocde = " << retcode
                << ", error msg = " << StatusCode_Name(stcode);

        if (stcode != StatusCode::kOK) {
            LOG(WARNING) << "GetSnapshotSegmentInfo return error, "
                       << ", filename = " << filename
                       << ", seq = " << seq;
            return retcode;
        }
        if (!response.has_pagefilesegment()) {
            LOG(WARNING) << "response has no pagesegment!";
            return LIBCURVE_ERROR::OK;
        }

        PageFileSegment pfs = response.pagefilesegment();
        LogicPoolID logicpoolid = pfs.logicalpoolid();
        segInfo->segmentsize = pfs.segmentsize();
        segInfo->chunksize = pfs.chunksize();
        segInfo->startoffset = pfs.startoffset();
        segInfo->lpcpIDInfo.lpid = logicpoolid;

        int chunksNum = pfs.chunks_size();
        if (chunksNum == 0) {
            LOG(WARNING) << "mds allocate segment, but no chunk!";
            return LIBCURVE_ERROR::FAILED;
        }

        for (int i = 0; i < chunksNum; i++) {
            ChunkID chunkid = pfs.chunks(i).chunkid();
            CopysetID copysetid = pfs.chunks(i).copysetid();
            segInfo->lpcpIDInfo.cpidVec.push_back(copysetid);
            segInfo->chunkvec.emplace_back(chunkid, logicpoolid, copysetid);
            DVLOG(9) << "chunk id: " << chunkid << " pool id: " << logicpoolid
                    << " copyset id: " << copysetid << " chunk id: " << chunkid;
        }
        return retcode;
    };
    return rpcExcutor.DoRPCTask(task, metaServerOpt_.mdsMaxRetryMS);
}

LIBCURVE_ERROR MDSClient::RefreshSession(const std::string& filename,
                                         const UserInfo_t& userinfo,
                                         const std::string& sessionid,
                                         LeaseRefreshResult* resp,
                                         LeaseSession* lease) {
    auto task = RPCTaskDefine {
        ReFreshSessionResponse response;
        mdsClientMetric_.refreshSession.qps.count << 1;
        LatencyGuard lg(&mdsClientMetric_.refreshSession.latency);
        mdsClientBase_.RefreshSession(filename, userinfo, sessionid,
                                      &response, cntl, channel);
        if (cntl->Failed()) {
            mdsClientMetric_.refreshSession.eps.count << 1;
            LOG(WARNING) << "Fail to send ReFreshSessionRequest, "
                << cntl->ErrorText()
                << ", filename = " << filename
                << ", sessionid = " << sessionid;
            return -cntl->ErrorCode();
        }

        StatusCode stcode = response.statuscode();
        if (stcode != StatusCode::kOK) {
            LOG(WARNING)
                << "RefreshSession NOT OK: filename = " << filename
                << ", owner = " << userinfo.owner
                << ", sessionid = " << sessionid
                << ", status code = " << StatusCode_Name(stcode);
        } else {
            LOG_EVERY_SECOND(INFO)
                << "RefreshSession returned: filename = " << filename
                << ", owner = " << userinfo.owner
                << ", sessionid = " << sessionid
                << ", status code = " << StatusCode_Name(stcode);
        }

        switch (stcode) {
            case StatusCode::kSessionNotExist:
            case StatusCode::kFileNotExists:
                resp->status = LeaseRefreshResult::Status::NOT_EXIST;
                break;
            case StatusCode::kOwnerAuthFail:
                resp->status = LeaseRefreshResult::Status::FAILED;
                return LIBCURVE_ERROR::AUTHFAIL;
                break;
            case StatusCode::kOK:
                if (response.has_fileinfo()) {
                    ServiceHelper::ProtoFileInfo2Local(response.fileinfo(),
                                                       &resp->finfo);
                    resp->status = LeaseRefreshResult::Status::OK;
                } else {
                    LOG(WARNING) << "session response has no fileinfo!";
                    return LIBCURVE_ERROR::FAILED;
                }
                if (nullptr != lease) {
                    if (!response.has_protosession()) {
                        LOG(WARNING) << "session response has no protosession";
                        return LIBCURVE_ERROR::FAILED;
                    }
                    ProtoSession leasesession = response.protosession();
                    lease->sessionID = leasesession.sessionid();
                    lease->leaseTime = leasesession.leasetime();
                    lease->createTime = leasesession.createtime();
                }
                break;
            default:
                resp->status = LeaseRefreshResult::Status::FAILED;
                return LIBCURVE_ERROR::FAILED;
                break;
        }
        return LIBCURVE_ERROR::OK;
    };
    return rpcExcutor.DoRPCTask(task, metaServerOpt_.mdsMaxRetryMS);
}

LIBCURVE_ERROR MDSClient::CheckSnapShotStatus(const std::string& filename,
                                              const UserInfo_t& userinfo,
                                              uint64_t seq,
                                              FileStatus* filestatus) {
    auto task = RPCTaskDefine {
        CheckSnapShotStatusResponse response;
        mdsClientBase_.CheckSnapShotStatus(filename, userinfo, seq,
                                           &response, cntl, channel);

        if (cntl->Failed()) {
            LOG(WARNING) << "check snap file failed, errcorde = "
                << cntl->ErrorCode() << ", error content:"
                << cntl->ErrorText();
            return -cntl->ErrorCode();
        }

        bool good = response.has_filestatus() && filestatus != nullptr;
        if (good) {
            *filestatus = static_cast<FileStatus>(response.filestatus());
        }

        LIBCURVE_ERROR retcode;
        StatusCode stcode = response.statuscode();
        MDSStatusCode2LibcurveError(stcode, &retcode);
        LOG_IF(WARNING, retcode != LIBCURVE_ERROR::OK)
            << "CheckSnapShotStatus: filename = " << filename
            << ", owner = " << userinfo.owner
            << ", seqnum = " << seq << ", errocde = " << retcode
            << ", error msg = " << StatusCode_Name(stcode);
        return retcode;
    };
    return rpcExcutor.DoRPCTask(task, metaServerOpt_.mdsMaxRetryMS);
}

#define IOPathMaxRetryMS UINT64_MAX

LIBCURVE_ERROR MDSClient::GetServerList(
    const LogicPoolID& logicalpooid,
    const std::vector<CopysetID>& copysetidvec,
    std::vector<CopysetInfo>* cpinfoVec) {
    auto task = RPCTaskDefine {
        GetChunkServerListInCopySetsResponse response;
        mdsClientMetric_.getServerList.qps.count << 1;
        LatencyGuard lg(&mdsClientMetric_.getServerList.latency);
        mdsClientBase_.GetServerList(logicalpooid, copysetidvec, &response,
                                     cntl, channel);
        if (cntl->Failed()) {
            mdsClientMetric_.getServerList.eps.count << 1;
            LOG_EVERY_SECOND(ERROR)
                << "get server list from mds failed, status code = "
                << cntl->ErrorCode() << ", error content:"
                << cntl->ErrorText();
            return -cntl->ErrorCode();
        }

        int csinfonum = response.csinfo_size();
        for (int i = 0; i < csinfonum; i++) {
            std::string copyset_peer;
            CopysetInfo copysetseverl;
            CopySetServerInfo info = response.csinfo(i);

            copysetseverl.cpid_ = info.copysetid();
            int cslocsNum = info.cslocs_size();
            for (int j = 0; j < cslocsNum; j++) {
                CopysetPeerInfo csinfo;
                ChunkServerLocation csl = info.cslocs(j);
                uint16_t port = csl.port();
                std::string internalIp = csl.hostip();
                csinfo.chunkserverID = csl.chunkserverid();
                std::string externalIp = internalIp;
                if (csl.has_externalip()) {
                    externalIp = csl.externalip();
                }

                EndPoint internal;
                butil::str2endpoint(internalIp.c_str(), port, &internal);
                EndPoint external;
                butil::str2endpoint(externalIp.c_str(), port, &external);
                csinfo.internalAddr = ChunkServerAddr(internal);
                csinfo.externalAddr = ChunkServerAddr(external);

                copysetseverl.AddCopysetPeerInfo(csinfo);
                copyset_peer.append(internalIp).append(":")
                            .append(std::to_string(port)).append(", ");
            }
            cpinfoVec->push_back(copysetseverl);
            DVLOG(9) << "copyset id : " << copysetseverl.cpid_
                     << ", peer info : " << copyset_peer;
        }

        LOG_IF(WARNING, response.statuscode() != 0)
            << "GetServerList failed"
            << ", errocde = " << response.statuscode()
            << ", log id = " << cntl->log_id();

        return response.statuscode() == 0 ? LIBCURVE_ERROR::OK :
               LIBCURVE_ERROR::FAILED;
    };
    return rpcExcutor.DoRPCTask(task, IOPathMaxRetryMS);
}

LIBCURVE_ERROR MDSClient::GetClusterInfo(ClusterContext* clsctx) {
    auto task = RPCTaskDefine {
        curve::mds::topology::GetClusterInfoResponse response;
        mdsClientBase_.GetClusterInfo(&response, cntl, channel);

        if (cntl->Failed()) {
            LOG(WARNING) << "get cluster info from mds failed, status code = "
                       << cntl->ErrorCode()
                       << ", error content: " << cntl->ErrorText();
            return -cntl->ErrorCode();
        }

        if (response.statuscode() == 0) {
            clsctx->clusterId = response.clusterid();
            return LIBCURVE_ERROR::OK;
        }
        return LIBCURVE_ERROR::FAILED;
    };
    return rpcExcutor.DoRPCTask(task, metaServerOpt_.mdsMaxRetryMS);
}

LIBCURVE_ERROR MDSClient::CreateCloneFile(const std::string& source,
                                          const std::string& destination,
                                          const UserInfo_t& userinfo,
                                          uint64_t size, uint64_t sn,
                                          uint32_t chunksize,
                                          uint64_t stripeUnit,
                                          uint64_t stripeCount,
                                          FInfo* fileinfo) {
    auto task = RPCTaskDefine {
        CreateCloneFileResponse response;
        mdsClientBase_.CreateCloneFile(source, destination, userinfo, size, sn,
                                       chunksize, stripeUnit, stripeCount,
                                       &response, cntl, channel);
        if (cntl->Failed()) {
            LOG(WARNING) << "Create clone file failed, errcorde = "
                         << cntl->ErrorCode()
                         << ", error content:" << cntl->ErrorText()
                         << ", source = " << source
                         << ", destination = " << destination;
            return -cntl->ErrorCode();
        }

        LIBCURVE_ERROR retcode;
        StatusCode stcode = response.statuscode();
        MDSStatusCode2LibcurveError(stcode, &retcode);
        LOG_IF(WARNING, retcode != LIBCURVE_ERROR::OK)
            << "CreateCloneFile: source = " << source
            << ", destination = " << destination
            << ", owner = " << userinfo.owner << ", seqnum = " << sn
            << ", size = " << size << ", chunksize = " << chunksize
            << ", errocde = " << retcode
            << ", error msg = " << StatusCode_Name(stcode)
            << ", log id = " << cntl->log_id();

        if (stcode == StatusCode::kOK) {
            ServiceHelper::ProtoFileInfo2Local(response.fileinfo(), fileinfo);
            fileinfo->sourceInfo.name = response.fileinfo().clonesource();
            fileinfo->sourceInfo.length = response.fileinfo().clonelength();
        }

        return retcode;
    };
    return rpcExcutor.DoRPCTask(task, metaServerOpt_.mdsMaxRetryMS);
}

LIBCURVE_ERROR MDSClient::CompleteCloneMeta(const std::string& destination,
                                            const UserInfo_t& userinfo) {
    return SetCloneFileStatus(destination, FileStatus::CloneMetaInstalled,
                              userinfo);
}

LIBCURVE_ERROR MDSClient::CompleteCloneFile(const std::string& destination,
                                            const UserInfo_t& userinfo) {
    return SetCloneFileStatus(destination, FileStatus::Cloned, userinfo);
}

LIBCURVE_ERROR MDSClient::SetCloneFileStatus(const std::string& filename,
                                             const FileStatus& filestatus,
                                             const UserInfo_t& userinfo,
                                             uint64_t fileID) {
    auto task = RPCTaskDefine {
        SetCloneFileStatusResponse response;
        mdsClientBase_.SetCloneFileStatus(filename, filestatus, userinfo,
                                          fileID, &response, cntl, channel);
        if (cntl->Failed()) {
            LOG(WARNING) << "SetCloneFileStatus invoke failed, errcorde = "
                << cntl->ErrorCode() << ", error content:"
                << cntl->ErrorText() << ", log id = " << cntl->log_id();
            return -cntl->ErrorCode();
        }

        LIBCURVE_ERROR retcode;
        StatusCode stcode = response.statuscode();
        MDSStatusCode2LibcurveError(stcode, &retcode);
        LOG_IF(WARNING, retcode != LIBCURVE_ERROR::OK)
                << "SetCloneFileStatus failed, filename = " << filename
                << ", owner = " << userinfo.owner
                << ", filestatus = " << static_cast<int>(filestatus)
                << ", fileID = " << fileID
                << ", errocde = " << retcode
                << ", error msg = " << StatusCode_Name(stcode)
                << ", log id = " << cntl->log_id();
        return retcode;
    };
    return rpcExcutor.DoRPCTask(task, metaServerOpt_.mdsMaxRetryMS);
}

LIBCURVE_ERROR MDSClient::GetOrAllocateSegment(bool allocate,
                                               uint64_t offset,
                                               const FInfo_t* fi,
                                               SegmentInfo* segInfo) {
    auto task = RPCTaskDefine {
        GetOrAllocateSegmentResponse response;
        mdsClientMetric_.getOrAllocateSegment.qps.count << 1;
        LatencyGuard lg(&mdsClientMetric_.getOrAllocateSegment.latency);
        mdsClientBase_.GetOrAllocateSegment(allocate, offset, fi,
                                            &response, cntl, channel);
        if (cntl->Failed()) {
            mdsClientMetric_.getOrAllocateSegment.eps.count << 1;
            LOG(WARNING)
                << "allocate segment failed, error code = "
                << cntl->ErrorCode()
                << ", error content:" << cntl->ErrorText()
                << ", offset:" << offset;
            return -cntl->ErrorCode();
        }

        auto statuscode = response.statuscode();
        switch (statuscode) {
            case StatusCode::kOwnerAuthFail:
                LOG(WARNING) << "GetOrAllocateSegment Auth failed!";
                return LIBCURVE_ERROR::AUTHFAIL;
                break;
            case StatusCode::kSegmentNotAllocated:
                LOG(WARNING) << "segment not allocated!";
                return LIBCURVE_ERROR::NOT_ALLOCATE;
                break;
            default: break;
        }

        PageFileSegment pfs = response.pagefilesegment();
        segInfo->chunksize = pfs.chunksize();
        segInfo->segmentsize = pfs.segmentsize();
        segInfo->startoffset = pfs.startoffset();
        LogicPoolID logicpoolid = pfs.logicalpoolid();
        segInfo->lpcpIDInfo.lpid = pfs.logicalpoolid();

        int chunksNum = pfs.chunks_size();
        if (allocate && chunksNum <= 0) {
            LOG(WARNING) << "MDS allocate segment, but no chunkinfo!";
            return LIBCURVE_ERROR::FAILED;
        }

        for (int i = 0; i < chunksNum; i++) {
            ChunkID chunkid = pfs.chunks(i).chunkid();
            CopysetID copysetid = pfs.chunks(i).copysetid();
            segInfo->lpcpIDInfo.cpidVec.push_back(copysetid);
            segInfo->chunkvec.emplace_back(chunkid, logicpoolid, copysetid);
        }
        return LIBCURVE_ERROR::OK;
    };
    return rpcExcutor.DoRPCTask(task, IOPathMaxRetryMS);
}

LIBCURVE_ERROR MDSClient::RenameFile(const UserInfo_t& userinfo,
                                     const std::string& origin,
                                     const std::string& destination,
                                     uint64_t originId,
                                     uint64_t destinationId) {
    auto task = RPCTaskDefine {
        RenameFileResponse response;
        mdsClientMetric_.renameFile.qps.count << 1;
        LatencyGuard lg(&mdsClientMetric_.renameFile.latency);
        mdsClientBase_.RenameFile(userinfo, origin, destination, originId,
                                    destinationId, &response, cntl, channel);
        if (cntl->Failed()) {
            mdsClientMetric_.renameFile.eps.count << 1;
            LOG(WARNING) << "RenameFile invoke failed, errcorde = "
                << cntl->ErrorCode()  << ", error content:"
                << cntl->ErrorText() << ", log id = " << cntl->log_id();
            return -cntl->ErrorCode();
        }

        LIBCURVE_ERROR retcode;
        StatusCode stcode = response.statuscode();
        MDSStatusCode2LibcurveError(stcode, &retcode);
        LOG_IF(WARNING, retcode != LIBCURVE_ERROR::OK)
                << "RenameFile: origin = " << origin
                << ", destination = " << destination
                << ", originId = " << originId
                << ", destinationId = " << destinationId
                << ", owner = " << userinfo.owner
                << ", errocde = " << retcode
                << ", error msg = "
                << StatusCode_Name(stcode)
                << ", log id = " << cntl->log_id();

        // MDS does not currently support rename file, retry again
        if (retcode == LIBCURVE_ERROR::NOT_SUPPORT) {
            return -retcode;
        }

        return retcode;
    };
    return rpcExcutor.DoRPCTask(task, metaServerOpt_.mdsMaxRetryMS);
}

LIBCURVE_ERROR MDSClient::Extend(const std::string& filename,
                                 const UserInfo_t& userinfo,
                                 uint64_t newsize) {
    auto task = RPCTaskDefine {
        ExtendFileResponse response;
        mdsClientMetric_.extendFile.qps.count << 1;
        LatencyGuard lg(&mdsClientMetric_.extendFile.latency);
        mdsClientBase_.Extend(filename, userinfo, newsize, &response,
                              cntl, channel);
        if (cntl->Failed()) {
            mdsClientMetric_.extendFile.eps.count << 1;
            LOG(WARNING) << "ExtendFile invoke failed, errcorde = "
                    << cntl->ErrorCode()
                    << ", error content:"
                    << cntl->ErrorText()
                    << ", log id = " << cntl->log_id();
            return -cntl->ErrorCode();
        }

        LIBCURVE_ERROR retcode;
        StatusCode stcode = response.statuscode();
        MDSStatusCode2LibcurveError(stcode, &retcode);
        LOG_IF(WARNING, retcode != LIBCURVE_ERROR::OK)
                    << "Extend: filename = " << filename
                    << ", owner = " << userinfo.owner
                    << ", newsize = " << newsize
                    << ", errocde = " << retcode
                    << ", error msg = " << StatusCode_Name(stcode)
                    << ", log id = " << cntl->log_id();
        return retcode;
    };
    return rpcExcutor.DoRPCTask(task, metaServerOpt_.mdsMaxRetryMS);
}

LIBCURVE_ERROR MDSClient::DeleteFile(const std::string& filename,
                                     const UserInfo_t& userinfo,
                                     bool deleteforce,
                                     uint64_t fileid) {
    auto task = RPCTaskDefine {
        DeleteFileResponse response;
        mdsClientMetric_.deleteFile.qps.count << 1;
        LatencyGuard lg(&mdsClientMetric_.deleteFile.latency);
        mdsClientBase_.DeleteFile(filename, userinfo, deleteforce,
                                    fileid, &response, cntl, channel);
        if (cntl->Failed()) {
            mdsClientMetric_.deleteFile.eps.count << 1;
            LOG(WARNING) << "DeleteFile invoke failed, errcorde = "
                << cntl->ErrorCode() << ", error content:"
                << cntl->ErrorText() << ", log id = " << cntl->log_id();
            return -cntl->ErrorCode();
        }

        LIBCURVE_ERROR retcode;
        StatusCode stcode = response.statuscode();
        MDSStatusCode2LibcurveError(stcode, &retcode);
        LOG_IF(WARNING, retcode != LIBCURVE_ERROR::OK)
                << "DeleteFile: filename = " << filename
                << ", owner = " << userinfo.owner
                << ", errocde = " << retcode
                << ", error msg = " << StatusCode_Name(stcode)
                << ", log id = " << cntl->log_id();

        // MDS does not currently support delete file, retry again
        if (retcode == LIBCURVE_ERROR::NOT_SUPPORT) {
            return -retcode;
        }

        return retcode;
    };
    return rpcExcutor.DoRPCTask(task, metaServerOpt_.mdsMaxRetryMS);
}

LIBCURVE_ERROR MDSClient::RecoverFile(const std::string& filename,
                                     const UserInfo_t& userinfo,
                                     uint64_t fileid) {
    auto task = RPCTaskDefine {
        RecoverFileResponse response;
        mdsClientMetric_.recoverFile.qps.count << 1;
        LatencyGuard lg(&mdsClientMetric_.recoverFile.latency);
        mdsClientBase_.RecoverFile(filename, userinfo, fileid,
                                   &response, cntl, channel);
        if (cntl->Failed()) {
            mdsClientMetric_.recoverFile.eps.count << 1;
            LOG(WARNING) << "RecoverFile invoke failed, errcorde = "
                << cntl->ErrorCode() << ", error content:"
                << cntl->ErrorText() << ", log id = " << cntl->log_id();
            return -cntl->ErrorCode();
        }

        LIBCURVE_ERROR retcode;
        StatusCode stcode = response.statuscode();
        MDSStatusCode2LibcurveError(stcode, &retcode);
        LOG_IF(WARNING, retcode != LIBCURVE_ERROR::OK)
                << "RecoverFile: filename = " << filename
                << ", owner = " << userinfo.owner
                << ", errocde = " << retcode
                << ", error msg = " << StatusCode_Name(stcode)
                << ", log id = " << cntl->log_id();
        return retcode;
    };
    return rpcExcutor.DoRPCTask(task, metaServerOpt_.mdsMaxRetryMS);
}

LIBCURVE_ERROR MDSClient::ChangeOwner(const std::string& filename,
                                      const std::string& newOwner,
                                      const UserInfo_t& userinfo) {
    auto task = RPCTaskDefine {
        ChangeOwnerResponse response;
        mdsClientMetric_.changeOwner.qps.count << 1;
        LatencyGuard lg(&mdsClientMetric_.changeOwner.latency);
        mdsClientBase_.ChangeOwner(filename, newOwner, userinfo,
                                   &response, cntl, channel);
        if (cntl->Failed()) {
            mdsClientMetric_.changeOwner.eps.count << 1;
            LOG(WARNING) << "ChangeOwner invoke failed, errcorde = "
                << cntl->ErrorCode() << ", error content:"
                << cntl->ErrorText() << ", log id = " << cntl->log_id();
            return -cntl->ErrorCode();
        }

        LIBCURVE_ERROR retcode;
        StatusCode stcode = response.statuscode();
        MDSStatusCode2LibcurveError(stcode, &retcode);
        LOG_IF(WARNING, retcode != LIBCURVE_ERROR::OK)
                << "ChangeOwner: filename = " << filename
                << ", owner = " << userinfo.owner
                << ", new owner = " << newOwner
                << ", errocde = " << retcode
                << ", error msg = " << StatusCode_Name(stcode)
                << ", log id = " << cntl->log_id();

        // MDS does not currently support change file owner, retry again
        if (retcode == LIBCURVE_ERROR::NOT_SUPPORT) {
            return -retcode;
        }

        return retcode;
    };
    return rpcExcutor.DoRPCTask(task, metaServerOpt_.mdsMaxRetryMS);
}

LIBCURVE_ERROR MDSClient::Listdir(const std::string& dirpath,
                                  const UserInfo_t& userinfo,
                                  std::vector<FileStatInfo>* filestatVec) {
    auto task = RPCTaskDefine {
        ListDirResponse response;
        mdsClientMetric_.listDir.qps.count << 1;
        LatencyGuard lg(&mdsClientMetric_.listDir.latency);
        mdsClientBase_.Listdir(dirpath, userinfo, &response, cntl, channel);

        if (cntl->Failed()) {
            mdsClientMetric_.listDir.eps.count << 1;
            LOG(WARNING) << "Listdir invoke failed, errcorde = "
                << cntl->ErrorCode() << ", error content:"
                << cntl->ErrorText() << ", log id = " << cntl->log_id();
            return -cntl->ErrorCode();
        }

        LIBCURVE_ERROR retcode;
        StatusCode stcode = response.statuscode();
        MDSStatusCode2LibcurveError(stcode, &retcode);
        LOG_IF(WARNING, retcode != LIBCURVE_ERROR::OK)
                << "Listdir: filename = " << dirpath
                << ", owner = " << userinfo.owner
                << ", errocde = " << retcode
                << ", error msg = " << StatusCode_Name(stcode)
                << ", log id = " << cntl->log_id();

        if (retcode == LIBCURVE_ERROR::OK) {
            int fileinfoNum = response.fileinfo_size();
            for (int i = 0; i < fileinfoNum; i++) {
                FileInfo finfo = response.fileinfo(i);
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
    };
    return rpcExcutor.DoRPCTask(task, metaServerOpt_.mdsMaxRetryMS);
}

LIBCURVE_ERROR MDSClient::GetChunkServerInfo(const ChunkServerAddr& csAddr,
                                             CopysetPeerInfo* chunkserverInfo) {
    if (!chunkserverInfo) {
        LOG(ERROR) << "chunkserverInfo pointer is null!";
        return LIBCURVE_ERROR::FAILED;
    }

    bool valid = NetCommon::CheckAddressValid(csAddr.ToString());
    if (!valid) {
        LOG(ERROR) << "chunkserver address "
                   << csAddr.ToString() << " invalid!";
        return LIBCURVE_ERROR::FAILED;
    }

    auto task = RPCTaskDefine {
        curve::mds::topology::GetChunkServerInfoResponse response;

        mdsClientMetric_.getChunkServerId.qps.count << 1;
        LatencyGuard guard(&mdsClientMetric_.getChunkServerId.latency);

        std::vector<std::string> strs;
        curve::common::SplitString(csAddr.ToString(), ":", &strs);
        const std::string& ip = strs[0];
        uint64_t port;
        curve::common::StringToUll(strs[1], &port);
        mdsClientBase_.GetChunkServerInfo(ip, port, &response, cntl, channel);

        if (cntl->Failed()) {
            LOG(WARNING) << "GetChunkServerInfo invoke failed, errcorde = "
                << cntl->ErrorCode() << ", error content:"
                << cntl->ErrorText() << ", log id = " << cntl->log_id();
            return -cntl->ErrorCode();
        }

        int statusCode = response.statuscode();
        LOG_IF(WARNING, statusCode != 0)
                << "GetChunkServerInfo: errocde = " << statusCode
                << ", log id = " << cntl->log_id();

        if (statusCode == 0) {
            const auto& csInfo = response.chunkserverinfo();
            ChunkServerID csId = csInfo.chunkserverid();
            std::string internalIp = csInfo.hostip();
            std::string externalIp = internalIp;
            if (csInfo.has_externalip()) {
                externalIp = csInfo.externalip();
            }
            uint32_t port = csInfo.port();
            EndPoint internal;
            butil::str2endpoint(internalIp.c_str(), port, &internal);
            EndPoint external;
            butil::str2endpoint(externalIp.c_str(), port, &external);
            *chunkserverInfo = CopysetPeerInfo(csId, ChunkServerAddr(internal),
                                               ChunkServerAddr(external));
            return LIBCURVE_ERROR::OK;
        } else {
            return LIBCURVE_ERROR::FAILED;
        }
    };
    return rpcExcutor.DoRPCTask(task, metaServerOpt_.mdsMaxRetryMS);
}

LIBCURVE_ERROR MDSClient::ListChunkServerInServer(
    const std::string& serverIp, std::vector<ChunkServerID>* csIds) {
    auto task = RPCTaskDefine {
        curve::mds::topology::ListChunkServerResponse response;

        mdsClientMetric_.listChunkserverInServer.qps.count << 1;
        LatencyGuard guard(&mdsClientMetric_.listChunkserverInServer.latency);

        mdsClientBase_.ListChunkServerInServer(
            serverIp, &response, cntl, channel);

        if (cntl->Failed()) {
            LOG(WARNING) << "ListChunkServerInServer failed, "
                << cntl->ErrorText()
                << ", log id = " << cntl->log_id();
            return -cntl->ErrorCode();
        }

        int statusCode = response.statuscode();
        LOG_IF(WARNING, statusCode != 0)
            << "ListChunkServerInServer failed, "
            << "errorcode = " << response.statuscode()
            << ", chunkserver ip = " << serverIp
            << ", log id = " << cntl->log_id();

        if (statusCode == 0) {
            csIds->reserve(response.chunkserverinfos_size());
            for (int i = 0; i < response.chunkserverinfos_size(); ++i) {
                csIds->emplace_back(
                    response.chunkserverinfos(i).chunkserverid());
            }

            return LIBCURVE_ERROR::OK;
        } else {
            return LIBCURVE_ERROR::FAILED;
        }
    };

    return rpcExcutor.DoRPCTask(task, metaServerOpt_.mdsMaxRetryMS);
}

void MDSClient::MDSStatusCode2LibcurveError(const StatusCode& status,
                                            LIBCURVE_ERROR* errcode) {
    switch (status) {
        case StatusCode::kOK:
            *errcode = LIBCURVE_ERROR::OK;
            break;
        case StatusCode::kFileExists:
            *errcode = LIBCURVE_ERROR::EXISTS;
            break;
        case StatusCode::kSnapshotFileNotExists:
        case StatusCode::kFileNotExists:
        case StatusCode::kDirNotExist:
            *errcode = LIBCURVE_ERROR::NOTEXIST;
            break;
        case StatusCode::kSegmentNotAllocated:
            *errcode = LIBCURVE_ERROR::NOT_ALLOCATE;
            break;
        case StatusCode::kShrinkBiggerFile:
            *errcode = LIBCURVE_ERROR::NO_SHRINK_BIGGER_FILE;
            break;
        case StatusCode::kNotSupported:
            *errcode = LIBCURVE_ERROR::NOT_SUPPORT;
            break;
        case StatusCode::kOwnerAuthFail:
            *errcode = LIBCURVE_ERROR::AUTHFAIL;
            break;
        case StatusCode::kSnapshotFileDeleteError:
            *errcode = LIBCURVE_ERROR::DELETE_ERROR;
            break;
        case StatusCode::kFileUnderSnapShot:
            *errcode = LIBCURVE_ERROR::UNDER_SNAPSHOT;
            break;
        case StatusCode::kFileNotUnderSnapShot:
            *errcode = LIBCURVE_ERROR::NOT_UNDERSNAPSHOT;
            break;
        case StatusCode::kSnapshotDeleting:
            *errcode = LIBCURVE_ERROR::DELETING;
            break;
        case StatusCode::kDirNotEmpty:
            *errcode = LIBCURVE_ERROR::NOT_EMPTY;
            break;
        case StatusCode::kFileOccupied:
            *errcode = LIBCURVE_ERROR::FILE_OCCUPIED;
            break;
        case StatusCode::kSessionNotExist:
            *errcode = LIBCURVE_ERROR::SESSION_NOT_EXIST;
            break;
        case StatusCode::kParaError:
            *errcode = LIBCURVE_ERROR::PARAM_ERROR;
            break;
        case StatusCode::kStorageError:
            *errcode = LIBCURVE_ERROR::INTERNAL_ERROR;
            break;
        case StatusCode::kFileLengthNotSupported:
            *errcode = LIBCURVE_ERROR::LENGTH_NOT_SUPPORT;
            break;
        case ::curve::mds::StatusCode::kCloneStatusNotMatch:
            *errcode = LIBCURVE_ERROR::STATUS_NOT_MATCH;
            break;
        case ::curve::mds::StatusCode::kDeleteFileBeingCloned:
            *errcode = LIBCURVE_ERROR::DELETE_BEING_CLONED;
            break;
        case ::curve::mds::StatusCode::kClientVersionNotMatch:
            *errcode = LIBCURVE_ERROR::CLIENT_NOT_SUPPORT_SNAPSHOT;
            break;
        case ::curve::mds::StatusCode::kSnapshotFrozen:
            *errcode = LIBCURVE_ERROR::SNAPSTHO_FROZEN;
            break;
        default:
            *errcode = LIBCURVE_ERROR::UNKNOWN;
            break;
    }
}

}   // namespace client
}   // namespace curve
