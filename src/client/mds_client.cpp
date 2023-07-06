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
#include <brpc/errno.pb.h>

#include <memory>
#include <utility>
#include <algorithm>

#include "include/client/libcurve_define.h"
#include "proto/auth.pb.h"
#include "proto/topology.pb.h"
#include "src/client/auth_client.h"
#include "src/client/lease_executor.h"
#include "src/common/net_common.h"
#include "src/common/string_util.h"
#include "src/common/timeutility.h"
#include "src/common/curve_version.h"
#include "src/common/curve_define.h"

namespace curve {
namespace client {

using curve::common::NetCommon;
using curve::common::TimeUtility;
using curve::mds::FileInfo;
using curve::mds::PageFileChunkInfo;
using curve::mds::PageFileSegment;
using curve::mds::ProtoSession;
using curve::mds::StatusCode;
using curve::common::ChunkServerLocation;
using curve::mds::topology::CopySetServerInfo;
using curve::common::MDS_ROLE;
using curve::mds::topology::TopologyService_Stub;
using curve::mds::CurveFSService_Stub;
using curve::common::Encryptor;

using curve::mds::OpenFileRequest;
using curve::mds::OpenFileResponse;
using curve::mds::CreateFileRequest;
using curve::mds::CreateFileResponse;
using curve::mds::CloseFileRequest;
using curve::mds::CloseFileResponse;
using curve::mds::RenameFileRequest;
using curve::mds::RenameFileResponse;
using curve::mds::ExtendFileRequest;
using curve::mds::ExtendFileResponse;
using curve::mds::DeleteFileRequest;
using curve::mds::DeleteFileResponse;
using curve::mds::RecoverFileRequest;
using curve::mds::RecoverFileResponse;
using curve::mds::GetFileInfoRequest;
using curve::mds::GetFileInfoResponse;
using curve::mds::IncreaseFileEpochResponse;
using curve::mds::DeleteSnapShotRequest;
using curve::mds::DeleteSnapShotResponse;
using curve::mds::ReFreshSessionRequest;
using curve::mds::ReFreshSessionResponse;
using curve::mds::ListDirRequest;
using curve::mds::ListDirResponse;
using curve::mds::ChangeOwnerRequest;
using curve::mds::ChangeOwnerResponse;
using curve::mds::CreateSnapShotRequest;
using curve::mds::CreateSnapShotResponse;
using curve::mds::CreateCloneFileRequest;
using curve::mds::CreateCloneFileResponse;
using curve::mds::SetCloneFileStatusRequest;
using curve::mds::SetCloneFileStatusResponse;
using curve::mds::GetOrAllocateSegmentRequest;
using curve::mds::GetOrAllocateSegmentResponse;
using curve::mds::DeAllocateSegmentRequest;
using curve::mds::DeAllocateSegmentResponse;
using curve::mds::CheckSnapShotStatusRequest;
using curve::mds::CheckSnapShotStatusResponse;
using curve::mds::ListSnapShotFileInfoRequest;
using curve::mds::ListSnapShotFileInfoResponse;
using curve::mds::GetOrAllocateSegmentRequest;
using curve::mds::GetOrAllocateSegmentResponse;
using curve::mds::topology::GetChunkServerListInCopySetsRequest;
using curve::mds::topology::GetChunkServerListInCopySetsResponse;
using curve::mds::topology::GetClusterInfoRequest;
using curve::mds::topology::GetClusterInfoResponse;
using curve::mds::topology::GetChunkServerInfoResponse;
using curve::mds::topology::ListChunkServerResponse;
using curve::mds::IncreaseFileEpochRequest;
using curve::mds::IncreaseFileEpochResponse;
using curve::mds::topology::ListPoolsetRequest;
using curve::mds::topology::ListPoolsetResponse;
using curve::mds::topology::GetChunkServerInfoRequest;
using curve::mds::topology::GetChunkServerInfoResponse;
using curve::mds::topology::ListChunkServerRequest;
using curve::mds::topology::ListChunkServerResponse;

const char* kRootUserName = "root";

MDSClient::MDSClient(const std::string &metricPrefix)
    : inited_(false), metaServerOpt_(), mdsClientMetric_(metricPrefix),
      rpcExcutor_(),
      authClient_(std::make_shared<AuthClient>()) {}

MDSClient::~MDSClient() { UnInitialize(); }

LIBCURVE_ERROR MDSClient::Initialize(const MetaServerOption &metaServerOpt,
    std::shared_ptr<AuthClient> authClient) {
    if (inited_) {
        LOG(INFO) << "MDSClient already started!";
        return LIBCURVE_ERROR::OK;
    }

    metaServerOpt_ = metaServerOpt;
    rpcExcutor_.SetOption(metaServerOpt.rpcRetryOpt);

    if (authClient != nullptr) {
        authClient_ = authClient;
    }

    std::ostringstream oss;
    for (const auto &addr : metaServerOpt_.rpcRetryOpt.addrs) {
        oss << " " << addr;
    }

    LOG(INFO) << "MDSClient init success, addresses:" << oss.str();
    inited_ = true;
    return LIBCURVE_ERROR::OK;
}


void MDSClient::UnInitialize() {
    inited_ = false;
}

std::string MDSClient::CalcSignature(const UserInfo& userinfo,
    uint64_t date) {
    std::string sig;
    if (IsRootUserAndHasPassword(userinfo)) {
        std::string str2sig = Encryptor::GetString2Signature(date,
            userinfo.owner);
        sig = Encryptor::CalcString2Signature(
            str2sig, userinfo.password);
    }
    return sig;
}

template <typename T>
void FillClienIpPortIfRegistered(T* request) {
    auto& clientinfo = ClientDummyServerInfo::GetInstance();
    if (clientinfo.GetRegister()) {
        request->set_clientip(clientinfo.GetIP());
        request->set_clientport(clientinfo.GetPort());
    }
}

#define RPCTaskDefine                                                   \
    [&](CURVE_UNUSED int addrindex, CURVE_UNUSED uint64_t rpctimeoutMS, \
        brpc::Channel* channel, brpc::Controller* cntl) -> int


#define GET_AUTH_TOKEN(authClient, request) \
    do { \
        bool ret = authClient->GetToken(MDS_ROLE, \
            request.mutable_authtoken()); \
        if (!ret) { \
            LOG(ERROR) << __func__ << " get token failed!"; \
            return LIBCURVE_ERROR::GET_AUTH_TOKEN_FAIL; \
        } \
    } while (0)


LIBCURVE_ERROR MDSClient::OpenFile(const std::string &filename,
                                   const UserInfo_t &userinfo, FInfo_t *fi,
                                   FileEpoch_t *fEpoch,
                                   LeaseSession *lease) {
    auto task = RPCTaskDefine {
        (void)addrindex;
        (void)rpctimeoutMS;
        LOG(INFO) << "OpenFile: filename = " << filename
                  << ", owner = " << userinfo.owner
                  << ", log id = " << cntl->log_id();
        mdsClientMetric_.openFile.qps.count << 1;
        LatencyGuard lg(&mdsClientMetric_.openFile.latency);

        OpenFileRequest request;
        OpenFileResponse response;
        request.set_filename(filename);
        request.set_clientversion(curve::common::CurveVersion());
        FillUserInfo(&request, userinfo);
        GET_AUTH_TOKEN(authClient_, request);

        CurveFSService_Stub stub(channel);
        stub.OpenFile(cntl, &request, &response, nullptr);
        if (cntl->Failed()) {
            mdsClientMetric_.openFile.eps.count << 1;
            LOG(WARNING) << "open file failed, errcorde = " << cntl->ErrorCode()
                         << ", error content:" << cntl->ErrorText()
                         << ", log id = " << cntl->log_id();
            return -cntl->ErrorCode();
        }

        LIBCURVE_ERROR retcode = LIBCURVE_ERROR::FAILED;
        StatusCode stcode = response.statuscode();
        MDSStatusCode2LibcurveError(stcode, &retcode);
        LOG_IF(WARNING, retcode != LIBCURVE_ERROR::OK)
            << "OpenFile: filename = " << filename
            << ", owner = " << userinfo.owner << ", errocde = " << retcode
            << ", error msg = " << StatusCode_Name(stcode)
            << ", log id = " << cntl->log_id();

        bool flag = response.has_protosession() && response.has_fileinfo();
        if (flag) {
            const ProtoSession &leasesession = response.protosession();
            lease->sessionID = leasesession.sessionid();
            lease->leaseTime = leasesession.leasetime();
            lease->createTime = leasesession.createtime();

            const curve::mds::FileInfo &protoFileInfo = response.fileinfo();
            LOG(INFO) << "OpenFile succeeded, filename: " << filename
                      << ", file info " << protoFileInfo.DebugString();
            ServiceHelper::ProtoFileInfo2Local(protoFileInfo, fi, fEpoch);

            const bool isLazyCloneFile =
                protoFileInfo.has_clonesource() &&
                (protoFileInfo.filestatus() ==
                 curve::mds::FileStatus::kFileCloneMetaInstalled);
            if (isLazyCloneFile) {
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
    return ReturnError(
        rpcExcutor_.DoRPCTask(task, metaServerOpt_.mdsMaxRetryMS));
}

LIBCURVE_ERROR MDSClient::CreateFile(const CreateFileContext& context) {
    auto task = RPCTaskDefine {
        (void)addrindex;
        (void)rpctimeoutMS;
        if (context.pagefile) {
            LOG(INFO) << "CreateFile, filename = `" << context.name << "`"
                      << ", owner = " << context.user.owner
                      << ", stripe unit = " << context.stripeUnit
                      << ", stripe count = " << context.stripeCount
                      << ", poolset = `" << context.poolset << "`"
                      << ", length = " << context.length / common::kGB << "GiB"
                      << ", log id = " << cntl->log_id();
        } else {
            LOG(INFO) << "CreateDirectory, dirname = `" << context.name << "`"
                      << ", owner = " << context.user.owner
                      << ", log id = " << cntl->log_id();
        }
        mdsClientMetric_.createFile.qps.count << 1;
        LatencyGuard lg(&mdsClientMetric_.createFile.latency);

        CreateFileRequest request;
        CreateFileResponse response;
        request.set_filename(context.name);
        if (context.pagefile) {
            request.set_filetype(curve::mds::FileType::INODE_PAGEFILE);
            request.set_filelength(context.length);
            request.set_stripeunit(context.stripeUnit);
            request.set_stripecount(context.stripeCount);
            request.set_poolset(context.poolset);
        } else {
            request.set_filetype(curve::mds::FileType::INODE_DIRECTORY);
        }
        FillUserInfo(&request, context.user);
        GET_AUTH_TOKEN(authClient_, request);

        CurveFSService_Stub stub(channel);
        stub.CreateFile(cntl, &request, &response, NULL);
        if (cntl->Failed()) {
            mdsClientMetric_.createFile.eps.count << 1;
            LOG(WARNING) << "Create file or directory failed, errcorde = "
                         << cntl->ErrorCode()
                         << ", error content:" << cntl->ErrorText()
                         << ", log id = " << cntl->log_id();
            return -cntl->ErrorCode();
        }

        LIBCURVE_ERROR retcode;
        StatusCode stcode = response.statuscode();
        MDSStatusCode2LibcurveError(stcode, &retcode);
        LOG_IF(WARNING, retcode != LIBCURVE_ERROR::OK)
            << "CreateFile: filename = " << context.name
            << ", owner = " << context.user.owner
            << ", is pagefile: " << context.pagefile
            << ", errcode = " << retcode
            << ", error msg = " << StatusCode_Name(stcode)
            << ", log id = " << cntl->log_id();
        return retcode;
    };
    return ReturnError(
        rpcExcutor_.DoRPCTask(task, metaServerOpt_.mdsMaxRetryMS));
}

LIBCURVE_ERROR MDSClient::CloseFile(const std::string &filename,
                                    const UserInfo_t &userinfo,
                                    const std::string &sessionid) {
    auto task = RPCTaskDefine {
        (void)addrindex;
        (void)rpctimeoutMS;
        LOG(INFO) << "CloseFile: filename = " << filename
                  << ", owner = " << userinfo.owner
                  << ", sessionid = " << sessionid
                  << ", log id = " << cntl->log_id();
        mdsClientMetric_.closeFile.qps.count << 1;
        LatencyGuard lg(&mdsClientMetric_.closeFile.latency);

        CloseFileRequest request;
        CloseFileResponse response;
        request.set_filename(filename);
        request.set_sessionid(sessionid);
        FillUserInfo(&request, userinfo);
        FillClienIpPortIfRegistered(&request);
        GET_AUTH_TOKEN(authClient_, request);

        CurveFSService_Stub stub(channel);
        stub.CloseFile(cntl, &request, &response, nullptr);
        if (cntl->Failed()) {
            mdsClientMetric_.closeFile.eps.count << 1;
            LOG(WARNING) << "close file failed, errcorde = "
                         << cntl->ErrorCode()
                         << ", error content:" << cntl->ErrorText()
                         << ", log id = " << cntl->log_id();
            return -cntl->ErrorCode();
        }

        LIBCURVE_ERROR retcode;
        StatusCode stcode = response.statuscode();
        MDSStatusCode2LibcurveError(stcode, &retcode);
        LOG_IF(WARNING, retcode != LIBCURVE_ERROR::OK)
            << "CloseFile: filename = " << filename
            << ", owner = " << userinfo.owner << ", sessionid = " << sessionid
            << ", errocde = " << retcode
            << ", error msg = " << StatusCode_Name(stcode)
            << ", log id = " << cntl->log_id();
        return retcode;
    };
    return ReturnError(
        rpcExcutor_.DoRPCTask(task, metaServerOpt_.mdsMaxRetryMS));
}

LIBCURVE_ERROR MDSClient::GetFileInfo(const std::string &filename,
                                      const UserInfo_t &userinfo, FInfo_t *fi,
                                      FileEpoch_t *fEpoch) {
    auto task = RPCTaskDefine {
        (void)addrindex;
        (void)rpctimeoutMS;
        LOG_EVERY_SECOND(INFO) << "GetFileInfo: filename = " << filename
                               << ", owner = " << userinfo.owner
                               << ", log id = " << cntl->log_id();
        mdsClientMetric_.getFile.qps.count << 1;
        LatencyGuard lg(&mdsClientMetric_.getFile.latency);

        GetFileInfoRequest request;
        GetFileInfoResponse response;
        request.set_filename(filename);
        FillUserInfo(&request, userinfo);
        GET_AUTH_TOKEN(authClient_, request);

        CurveFSService_Stub stub(channel);
        stub.GetFileInfo(cntl, &request, &response, nullptr);
        if (cntl->Failed()) {
            mdsClientMetric_.getFile.eps.count << 1;
            LOG(WARNING) << "Fail to GetFileInfo, filename: " << filename
                         << ", error: " << cntl->ErrorText();
            return -cntl->ErrorCode();
        }

        if (response.has_fileinfo()) {
            ServiceHelper::ProtoFileInfo2Local(response.fileinfo(), fi, fEpoch);
        }

        LIBCURVE_ERROR retcode;
        StatusCode stcode = response.statuscode();
        MDSStatusCode2LibcurveError(stcode, &retcode);
        LOG_IF(WARNING, retcode != LIBCURVE_ERROR::OK)
            << "GetFileInfo: filename = " << filename
            << ", owner = " << userinfo.owner << ", errocde = " << retcode
            << ", error msg = " << StatusCode_Name(stcode)
            << ", log id = " << cntl->log_id();
        return retcode;
    };
    return ReturnError(
        rpcExcutor_.DoRPCTask(task, metaServerOpt_.mdsMaxRetryMS));
}

LIBCURVE_ERROR MDSClient::IncreaseEpoch(const std::string& filename,
    const UserInfo_t& userinfo,
    FInfo_t* fi,
    FileEpoch_t *fEpoch,
    std::list<CopysetPeerInfo<ChunkServerID>> *csLocs) {
    auto task = RPCTaskDefine {
        (void)addrindex;
        (void)rpctimeoutMS;
        LOG(INFO) << "IncreaseEpoch, filename: " << filename
                  << ", user: " << userinfo.owner
                  << ", log id = " << cntl->log_id();
        mdsClientMetric_.increaseEpoch.qps.count << 1;
        LatencyGuard lg(&mdsClientMetric_.increaseEpoch.latency);

        IncreaseFileEpochRequest request;
        IncreaseFileEpochResponse response;
        request.set_filename(filename);
        FillUserInfo(&request, userinfo);
        GET_AUTH_TOKEN(authClient_, request);

        CurveFSService_Stub stub(channel);
        stub.IncreaseFileEpoch(cntl, &request, &response, nullptr);
        if (cntl->Failed()) {
            mdsClientMetric_.increaseEpoch.eps.count << 1;
            return -cntl->ErrorCode();
        }

        StatusCode stcode = response.statuscode();
        if (stcode != StatusCode::kOK) {
            LIBCURVE_ERROR retcode;
            MDSStatusCode2LibcurveError(stcode, &retcode);
            LOG(ERROR) << "IncreaseEpoch: filename = " << filename
                << ", owner = " << userinfo.owner
                << ", errocde = " << retcode
                << ", error msg = " << StatusCode_Name(stcode)
                << ", log id = " << cntl->log_id();
            return retcode;
        }

        if (response.has_fileinfo()) {
            ServiceHelper::ProtoFileInfo2Local(response.fileinfo(), fi, fEpoch);
        } else {
            LOG(ERROR) << "IncreaseEpoch response has no fileinfo!";
            return LIBCURVE_ERROR::FAILED;
        }

        csLocs->clear();
        int csLocSize = response.cslocs_size();
        for (int i = 0; i < csLocSize; i++) {
            CopysetPeerInfo<ChunkServerID> csinfo;
            csinfo.peerID = response.cslocs(i).chunkserverid();
            EndPoint internal;
            butil::str2endpoint(response.cslocs(i).hostip().c_str(),
                    response.cslocs(i).port(), &internal);
            EndPoint external;
            const bool hasExternalIp = response.cslocs(i).has_externalip();
            if (hasExternalIp) {
                butil::str2endpoint(response.cslocs(i).externalip().c_str(),
                    response.cslocs(i).port(), &external);
            }
            csinfo.internalAddr = PeerAddr(internal);
            csinfo.externalAddr = PeerAddr(external);

            csLocs->push_back(std::move(csinfo));
        }
        return LIBCURVE_ERROR::OK;
    };
    return ReturnError(
        rpcExcutor_.DoRPCTask(task, metaServerOpt_.mdsMaxRetryMS));
}

LIBCURVE_ERROR MDSClient::CreateSnapShot(const std::string& filename,
                                         const UserInfo_t& userinfo,
                                         uint64_t* seq) {
    auto task = RPCTaskDefine {
        (void)addrindex;
        (void)rpctimeoutMS;
        LOG(INFO) << "CreateSnapShot: filename = " << filename
                  << ", owner = " << userinfo.owner
                  << ", log id = " << cntl->log_id();
        mdsClientMetric_.createSnapShot.qps.count << 1;
        LatencyGuard lg(&mdsClientMetric_.createSnapShot.latency);

        CreateSnapShotRequest request;
        CreateSnapShotResponse response;
        request.set_filename(filename);
        FillUserInfo(&request, userinfo);
        GET_AUTH_TOKEN(authClient_, request);

        CurveFSService_Stub stub(channel);
        stub.CreateSnapShot(cntl, &request, &response, nullptr);
        if (cntl->Failed()) {
            mdsClientMetric_.createSnapShot.eps.count << 1;
            LOG(WARNING) << "create snap file failed, errcorde = "
                         << cntl->ErrorCode()
                         << ", error content:" << cntl->ErrorText()
                         << ", log id = " << cntl->log_id();
            return -cntl->ErrorCode();
        }

        bool hasinfo = response.has_snapshotfileinfo();
        StatusCode stcode = response.statuscode();
        if ((stcode == StatusCode::kOK ||
             stcode == StatusCode::kFileUnderSnapShot) &&
            hasinfo) {
            FInfo_t *fi = new (std::nothrow) FInfo_t;
            FileEpoch_t fEpoch;
            ServiceHelper::ProtoFileInfo2Local(response.snapshotfileinfo(),
                                               fi, &fEpoch);
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
            FileEpoch_t fEpoch;
            ServiceHelper::ProtoFileInfo2Local(response.snapshotfileinfo(),
                                               &fi, &fEpoch);  // NOLINT
            *seq = fi.seqnum;
        }

        LIBCURVE_ERROR retcode;
        MDSStatusCode2LibcurveError(stcode, &retcode);
        LOG_IF(WARNING, retcode != LIBCURVE_ERROR::OK)
            << "CreateSnapShot: filename = " << filename
            << ", owner = " << userinfo.owner << ", errocde = " << retcode
            << ", error msg = " << StatusCode_Name(stcode)
            << ", log id = " << cntl->log_id();
        return retcode;
    };
    return ReturnError(
        rpcExcutor_.DoRPCTask(task, metaServerOpt_.mdsMaxRetryMS));
}

LIBCURVE_ERROR MDSClient::DeleteSnapShot(const std::string &filename,
                                         const UserInfo_t &userinfo,
                                         uint64_t seq) {
    auto task = RPCTaskDefine {
        (void)addrindex;
        (void)rpctimeoutMS;
        LOG(INFO) << "DeleteSnapShot: filename = " << filename
                  << ", owner = " << userinfo.owner
                  << ", seqnum = " << seq
                  << ", log id = " << cntl->log_id();
        mdsClientMetric_.deleteSnapShot.qps.count << 1;
        LatencyGuard lg(&mdsClientMetric_.deleteSnapShot.latency);

        DeleteSnapShotRequest request;
        DeleteSnapShotResponse response;
        request.set_seq(seq);
        request.set_filename(filename);
        FillUserInfo(&request, userinfo);
        GET_AUTH_TOKEN(authClient_, request);

        CurveFSService_Stub stub(channel);
        stub.DeleteSnapShot(cntl, &request, &response, nullptr);
        if (cntl->Failed()) {
            mdsClientMetric_.deleteSnapShot.eps.count << 1;
            LOG(WARNING) << "delete snap file failed, errcorde = "
                         << cntl->ErrorCode()
                         << ", error content:" << cntl->ErrorText()
                         << ", log id = " << cntl->log_id();
            return -cntl->ErrorCode();
        }

        LIBCURVE_ERROR retcode;
        StatusCode stcode = response.statuscode();
        MDSStatusCode2LibcurveError(stcode, &retcode);
        LOG_IF(WARNING, retcode != LIBCURVE_ERROR::OK)
            << "DeleteSnapShot: filename = " << filename
            << ", owner = " << userinfo.owner << ", seqnum = " << seq
            << ", errocde = " << retcode
            << ", error msg = " << StatusCode_Name(stcode)
            << ", log id = " << cntl->log_id();
        return retcode;
    };
    return ReturnError(
        rpcExcutor_.DoRPCTask(task, metaServerOpt_.mdsMaxRetryMS));
}

LIBCURVE_ERROR MDSClient::ListSnapShot(const std::string &filename,
                                       const UserInfo_t &userinfo,
                                       const std::vector<uint64_t> *seq,
                                       std::map<uint64_t, FInfo> *snapif) {
    auto task = RPCTaskDefine {
        (void)addrindex;
        (void)rpctimeoutMS;
        LOG(INFO) << "ListSnapShot: filename = " << filename
                  << ", owner = " << userinfo.owner
                  << ", seqnum = " << [seq] () {
                    std::string data("[ ");
                    for (uint64_t v : *seq) {
                        data += std::to_string(v);
                        data += " ";
                    }
                    data += "]";
                    return data;
                } ()
                  << ", log id = " << cntl->log_id();
        mdsClientMetric_.listSnapShot.qps.count << 1;
        LatencyGuard lg(&mdsClientMetric_.listSnapShot.latency);

        ListSnapShotFileInfoRequest request;
        ListSnapShotFileInfoResponse response;
        for (unsigned int i = 0; i < (*seq).size(); i++) {
            request.add_seq((*seq)[i]);
        }
        request.set_filename(filename);
        FillUserInfo(&request, userinfo);
        GET_AUTH_TOKEN(authClient_, request);

        CurveFSService_Stub stub(channel);
        stub.ListSnapShot(cntl, &request, &response, nullptr);
        if (cntl->Failed()) {
            mdsClientMetric_.listSnapShot.eps.count << 1;
            LOG(WARNING) << "list snap file failed, errcorde = "
                         << cntl->ErrorCode()
                         << ", error content:" << cntl->ErrorText()
                         << ", log id = " << cntl->log_id();
            return -cntl->ErrorCode();
        }

        LIBCURVE_ERROR retcode;
        StatusCode stcode = response.statuscode();
        MDSStatusCode2LibcurveError(stcode, &retcode);
        LOG_IF(WARNING, retcode != LIBCURVE_ERROR::OK)
            << "ListSnapShot: filename = " << filename
            << ", owner = " << userinfo.owner << ", errocde = " << retcode
            << ", error msg = " << StatusCode_Name(stcode);

        if (stcode == StatusCode::kOwnerAuthFail) {
            return LIBCURVE_ERROR::AUTHFAIL;
        }

        for (int i = 0; i < response.fileinfo_size(); i++) {
            FInfo_t tempInfo;
            FileEpoch_t fEpoch;
            ServiceHelper::ProtoFileInfo2Local(response.fileinfo(i),
                                               &tempInfo, &fEpoch);
            snapif->insert(std::make_pair(tempInfo.seqnum, tempInfo));
        }

        if (response.fileinfo_size() != static_cast<int>(seq->size())) {
            LOG(WARNING) << "some snapshot info not found!";
            return LIBCURVE_ERROR::NOTEXIST;
        }

        return retcode;
    };
    return ReturnError(
        rpcExcutor_.DoRPCTask(task, metaServerOpt_.mdsMaxRetryMS));
}

LIBCURVE_ERROR MDSClient::GetSnapshotSegmentInfo(const std::string &filename,
                                                 const UserInfo_t &userinfo,
                                                 uint64_t seq, uint64_t offset,
                                                 SegmentInfo *segInfo) {
    auto task = RPCTaskDefine {
        (void)addrindex;
        (void)rpctimeoutMS;
        LOG(INFO) << "GetSnapshotSegmentInfo: filename = " << filename
                  << ", owner = " << userinfo.owner
                  << ", offset = " << offset
                  << ", seqnum = " << seq
                  << ", log id = " << cntl->log_id();
        mdsClientMetric_.getSnapshotSegmentInfo.qps.count << 1;
        LatencyGuard lg(&mdsClientMetric_.getSnapshotSegmentInfo.latency);

        GetOrAllocateSegmentRequest request;
        GetOrAllocateSegmentResponse response;
        request.set_filename(filename);
        request.set_offset(offset);
        request.set_allocateifnotexist(false);
        request.set_seqnum(seq);
        FillUserInfo(&request, userinfo);
        GET_AUTH_TOKEN(authClient_, request);

        CurveFSService_Stub stub(channel);
        stub.GetSnapShotFileSegment(cntl, &request, &response, nullptr);
        if (cntl->Failed()) {
            mdsClientMetric_.getSnapshotSegmentInfo.eps.count << 1;
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
            << ", owner = " << userinfo.owner << ", offset = " << offset
            << ", seqnum = " << seq << ", errocde = " << retcode
            << ", error msg = " << StatusCode_Name(stcode);

        if (stcode != StatusCode::kOK) {
            LOG(WARNING) << "GetSnapshotSegmentInfo return error, "
                         << ", filename = " << filename << ", seq = " << seq;
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
                     << " copyset id: " << copysetid
                     << " chunk id: " << chunkid;
        }
        return retcode;
    };
    return ReturnError(
        rpcExcutor_.DoRPCTask(task, metaServerOpt_.mdsMaxRetryMS));
}

LIBCURVE_ERROR MDSClient::RefreshSession(const std::string &filename,
                                         const UserInfo_t &userinfo,
                                         const std::string &sessionid,
                                         LeaseRefreshResult *resp,
                                         LeaseSession *lease) {
    auto task = RPCTaskDefine {
        (void)addrindex;
        (void)rpctimeoutMS;
        LOG_EVERY_N(INFO, 10) << "RefreshSession: filename = " << filename
                              << ", owner = " << userinfo.owner
                              << ", sessionid = " << sessionid
                              << ", log id = " << cntl->log_id();
        mdsClientMetric_.refreshSession.qps.count << 1;
        LatencyGuard lg(&mdsClientMetric_.refreshSession.latency);

        ReFreshSessionRequest request;
        ReFreshSessionResponse response;
        request.set_filename(filename);
        request.set_sessionid(sessionid);
        request.set_clientversion(curve::common::CurveVersion());
        FillUserInfo(&request, userinfo);
        FillClienIpPortIfRegistered(&request);
        GET_AUTH_TOKEN(authClient_, request);

        CurveFSService_Stub stub(channel);
        stub.RefreshSession(cntl, &request, &response, nullptr);
        if (cntl->Failed()) {
            mdsClientMetric_.refreshSession.eps.count << 1;
            LOG(WARNING) << "Fail to send ReFreshSessionRequest, "
                         << cntl->ErrorText() << ", filename = " << filename
                         << ", sessionid = " << sessionid;
            return -cntl->ErrorCode();
        }

        StatusCode stcode = response.statuscode();
        if (stcode != StatusCode::kOK) {
            LOG(WARNING) << "RefreshSession NOT OK: filename = " << filename
                         << ", owner = " << userinfo.owner
                         << ", sessionid = " << sessionid
                         << ", status code = " << StatusCode_Name(stcode);
        } else {
            LOG_EVERY_N(INFO, 100)
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
                FileEpoch_t fEpoch;
                ServiceHelper::ProtoFileInfo2Local(response.fileinfo(),
                                                   &resp->finfo,
                                                   &fEpoch);
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
    return ReturnError(
        rpcExcutor_.DoRPCTask(task, metaServerOpt_.mdsMaxRetryMS));
}

LIBCURVE_ERROR MDSClient::CheckSnapShotStatus(const std::string &filename,
                                              const UserInfo_t &userinfo,
                                              uint64_t seq,
                                              FileStatus *filestatus) {
    auto task = RPCTaskDefine {
        (void)addrindex;
        (void)rpctimeoutMS;
        LOG(INFO) << "CheckSnapShotStatus: filename = " << filename
                  << ", owner = " << userinfo.owner
                  << ", seqnum = " << seq
                  << ", log id = " << cntl->log_id();
        mdsClientMetric_.checkSnapShotStatus.qps.count << 1;
        LatencyGuard lg(&mdsClientMetric_.checkSnapShotStatus.latency);

        CheckSnapShotStatusRequest request;
        CheckSnapShotStatusResponse response;
        request.set_seq(seq);
        request.set_filename(filename);
        FillUserInfo(&request, userinfo);
        GET_AUTH_TOKEN(authClient_, request);

        CurveFSService_Stub stub(channel);
        stub.CheckSnapShotStatus(cntl, &request, &response, nullptr);
        if (cntl->Failed()) {
            mdsClientMetric_.checkSnapShotStatus.eps.count << 1;
            LOG(WARNING) << "check snap file failed, errcorde = "
                         << cntl->ErrorCode()
                         << ", error content:" << cntl->ErrorText();
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
            << ", owner = " << userinfo.owner << ", seqnum = " << seq
            << ", errocde = " << retcode
            << ", error msg = " << StatusCode_Name(stcode);
        return retcode;
    };
    return ReturnError(
        rpcExcutor_.DoRPCTask(task, metaServerOpt_.mdsMaxRetryMS));
}

LIBCURVE_ERROR
MDSClient::GetServerList(const LogicPoolID &logicalpooid,
                         const std::vector<CopysetID> &copysetidvec,
                         std::vector<CopysetInfo<ChunkServerID>> *cpinfoVec) {
    auto task = RPCTaskDefine {
        (void)addrindex;
        (void)rpctimeoutMS;
        mdsClientMetric_.getServerList.qps.count << 1;
        LatencyGuard lg(&mdsClientMetric_.getServerList.latency);

        GetChunkServerListInCopySetsRequest request;
        GetChunkServerListInCopySetsResponse response;
        request.set_logicalpoolid(logicalpooid);
        std::string requestCopysets;
        for (auto copysetid : copysetidvec) {
            request.add_copysetid(copysetid);
            requestCopysets.append(std::to_string(copysetid)).append(" ");
        }
        GET_AUTH_TOKEN(authClient_, request);

        TopologyService_Stub stub(channel);
        stub.GetChunkServerListInCopySets(cntl, &request, &response, nullptr);
        if (cntl->Failed()) {
            mdsClientMetric_.getServerList.eps.count << 1;
            LOG(WARNING) << "get server list from mds failed, error is "
                         << cntl->ErrorText()
                         << ", log id = " << cntl->log_id();
            return -cntl->ErrorCode();
        }

        int csinfonum = response.csinfo_size();
        cpinfoVec->reserve(csinfonum);
        for (int i = 0; i < csinfonum; i++) {
            std::string copyset_peer;
            CopysetInfo<ChunkServerID> copysetseverl;
            CopySetServerInfo info = response.csinfo(i);

            copysetseverl.cpid_ = info.copysetid();
            int cslocsNum = info.cslocs_size();
            for (int j = 0; j < cslocsNum; j++) {
                CopysetPeerInfo<ChunkServerID> csinfo;
                ChunkServerLocation csl = info.cslocs(j);
                uint16_t port = csl.port();
                std::string internalIp = csl.hostip();
                csinfo.peerID = csl.chunkserverid();
                std::string externalIp = internalIp;
                if (csl.has_externalip()) {
                    externalIp = csl.externalip();
                }

                EndPoint internal;
                butil::str2endpoint(internalIp.c_str(), port, &internal);
                EndPoint external;
                butil::str2endpoint(externalIp.c_str(), port, &external);
                csinfo.internalAddr = PeerAddr(internal);
                csinfo.externalAddr = PeerAddr(external);

                copysetseverl.AddCopysetPeerInfo(csinfo);
                copyset_peer.append(internalIp)
                    .append(":")
                    .append(std::to_string(port))
                    .append(", ");
            }
            cpinfoVec->push_back(copysetseverl);
            DVLOG(9) << "copyset id : " << copysetseverl.cpid_
                     << ", peer info : " << copyset_peer;
        }

        LOG_IF(WARNING, response.statuscode() != 0)
            << "GetServerList failed"
            << ", errocde = " << response.statuscode()
            << ", log id = " << cntl->log_id();

        return response.statuscode() == 0 ? LIBCURVE_ERROR::OK
                                          : LIBCURVE_ERROR::FAILED;
    };
    return ReturnError(rpcExcutor_.DoRPCTask(task, 0));
}

LIBCURVE_ERROR MDSClient::GetClusterInfo(ClusterContext *clsctx) {
    auto task = RPCTaskDefine {
        (void)addrindex;
        (void)rpctimeoutMS;
        mdsClientMetric_.getClusterInfo.qps.count << 1;
        LatencyGuard lg(&mdsClientMetric_.getClusterInfo.latency);

        GetClusterInfoRequest request;
        GetClusterInfoResponse response;
        GET_AUTH_TOKEN(authClient_, request);

        TopologyService_Stub stub(channel);
        stub.GetClusterInfo(cntl, &request, &response, nullptr);
        if (cntl->Failed()) {
            mdsClientMetric_.getClusterInfo.eps.count << 1;
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
    return ReturnError(
        rpcExcutor_.DoRPCTask(task, metaServerOpt_.mdsMaxRetryMS));
}

LIBCURVE_ERROR MDSClient::ListPoolset(std::vector<std::string>* out) {
    assert(out != nullptr);
    auto task = RPCTaskDefine {
        (void)addrindex;
        (void)rpctimeoutMS;
        mdsClientMetric_.listPoolset.qps.count << 1;
        LatencyGuard lg(&mdsClientMetric_.listPoolset.latency);

        ListPoolsetRequest request;
        ListPoolsetResponse response;
        GET_AUTH_TOKEN(authClient_, request);

        TopologyService_Stub stub(channel);
        stub.ListPoolset(cntl, &request, &response, nullptr);
        if (cntl->Failed()) {
            mdsClientMetric_.listPoolset.eps.count << 1;
            LOG(WARNING) << "Failed to list poolset, error: "
                         << cntl->ErrorText();
            return -cntl->ErrorCode();
        }

        const bool succ = (response.statuscode() == 0);
        if (!succ) {
            LOG(WARNING) << "Failed to list poolset, response error: "
                         << response.statuscode();
            return LIBCURVE_ERROR::FAILED;
        }

        for (const auto& p : response.poolsetinfos()) {
            out->emplace_back(p.poolsetname());
        }
        return LIBCURVE_ERROR::OK;
    };

    return ReturnError(
            rpcExcutor_.DoRPCTask(task, metaServerOpt_.mdsMaxRetryMS));
}

LIBCURVE_ERROR MDSClient::CreateCloneFile(const std::string& source,
                                          const std::string& destination,
                                          const UserInfo_t& userinfo,
                                          uint64_t size,
                                          uint64_t sn,
                                          uint32_t chunksize,
                                          uint64_t stripeUnit,
                                          uint64_t stripeCount,
                                          const std::string& poolset,
                                          FInfo* fileinfo) {
    auto task = RPCTaskDefine {
        (void)addrindex;
        (void)rpctimeoutMS;
        mdsClientMetric_.createCloneFile.qps.count << 1;
        LatencyGuard lg(&mdsClientMetric_.createCloneFile.latency);

        CreateCloneFileRequest request;
        CreateCloneFileResponse response;
        request.set_seq(sn);
        request.set_filelength(size);
        request.set_filename(destination);
        request.set_chunksize(chunksize);
        request.set_filetype(curve::mds::FileType::INODE_PAGEFILE);
        request.set_clonesource(source);
        request.set_stripeunit(stripeUnit);
        request.set_stripecount(stripeCount);
        request.set_poolset(poolset);
        FillUserInfo(&request, userinfo);
        GET_AUTH_TOKEN(authClient_, request);
        LOG(INFO) << "CreateCloneFile: " << request.ShortDebugString()
                << ", log id = " << cntl->log_id();

        CurveFSService_Stub stub(channel);
        stub.CreateCloneFile(cntl, &request, &response, NULL);
        if (cntl->Failed()) {
            mdsClientMetric_.createCloneFile.eps.count << 1;
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
            FileEpoch_t fEpoch;
            ServiceHelper::ProtoFileInfo2Local(response.fileinfo(),
                                               fileinfo, &fEpoch);
            fileinfo->sourceInfo.name = response.fileinfo().clonesource();
            fileinfo->sourceInfo.length = response.fileinfo().clonelength();
        }

        return retcode;
    };
    return ReturnError(
        rpcExcutor_.DoRPCTask(task, metaServerOpt_.mdsMaxRetryMS));
}

LIBCURVE_ERROR MDSClient::CompleteCloneMeta(const std::string &destination,
                                            const UserInfo_t &userinfo) {
    return SetCloneFileStatus(destination, FileStatus::CloneMetaInstalled,
                              userinfo);
}

LIBCURVE_ERROR MDSClient::CompleteCloneFile(const std::string &destination,
                                            const UserInfo_t &userinfo) {
    return SetCloneFileStatus(destination, FileStatus::Cloned, userinfo);
}

LIBCURVE_ERROR MDSClient::SetCloneFileStatus(const std::string &filename,
                                             const FileStatus &filestatus,
                                             const UserInfo_t &userinfo,
                                             uint64_t fileID) {
    auto task = RPCTaskDefine {
        (void)addrindex;
        (void)rpctimeoutMS;
        LOG(INFO) << "SetCloneFileStatus: filename = " << filename
                  << ", owner = " << userinfo.owner
                  << ", filestatus = " << static_cast<int>(filestatus)
                  << ", fileID = " << fileID
                  << ", log id = " << cntl->log_id();
        mdsClientMetric_.setCloneFileStatus.qps.count << 1;
        LatencyGuard lg(&mdsClientMetric_.setCloneFileStatus.latency);

        SetCloneFileStatusRequest request;
        SetCloneFileStatusResponse response;
        request.set_filename(filename);
        request.set_filestatus(static_cast<curve::mds::FileStatus>(filestatus));
        if (fileID > 0) {
            request.set_fileid(fileID);
        }
        FillUserInfo(&request, userinfo);
        GET_AUTH_TOKEN(authClient_, request);

        CurveFSService_Stub stub(channel);
        stub.SetCloneFileStatus(cntl, &request, &response, NULL);
        if (cntl->Failed()) {
            mdsClientMetric_.setCloneFileStatus.eps.count << 1;
            LOG(WARNING) << "SetCloneFileStatus invoke failed, errcorde = "
                         << cntl->ErrorCode()
                         << ", error content:" << cntl->ErrorText()
                         << ", log id = " << cntl->log_id();
            return -cntl->ErrorCode();
        }

        LIBCURVE_ERROR retcode;
        StatusCode stcode = response.statuscode();
        MDSStatusCode2LibcurveError(stcode, &retcode);
        LOG_IF(WARNING, retcode != LIBCURVE_ERROR::OK)
            << "SetCloneFileStatus failed, filename = " << filename
            << ", owner = " << userinfo.owner
            << ", filestatus = " << static_cast<int>(filestatus)
            << ", fileID = " << fileID << ", errocde = " << retcode
            << ", error msg = " << StatusCode_Name(stcode)
            << ", log id = " << cntl->log_id();
        return retcode;
    };
    return ReturnError(
        rpcExcutor_.DoRPCTask(task, metaServerOpt_.mdsMaxRetryMS));
}

LIBCURVE_ERROR MDSClient::GetOrAllocateSegment(bool allocate, uint64_t offset,
                                               const FInfo_t *fi,
                                               const FileEpoch_t *fEpoch,
                                               SegmentInfo *segInfo) {
    auto task = RPCTaskDefine {
        (void)addrindex;
        (void)rpctimeoutMS;
        // convert the user offset to seg  offset
        uint64_t segmentsize = fi->segmentsize;
        uint64_t seg_offset = (offset / segmentsize) * segmentsize;
        LOG(INFO) << "GetOrAllocateSegment: filename = " << fi->fullPathName
                  << ", allocate = " << allocate << ", owner = " << fi->owner
                  << ", offset = " << offset
                  << ", segment offset = " << seg_offset
                  << ", log id = " << cntl->log_id();
        mdsClientMetric_.getOrAllocateSegment.qps.count << 1;
        LatencyGuard lg(&mdsClientMetric_.getOrAllocateSegment.latency);

        GetOrAllocateSegmentRequest request;
        GetOrAllocateSegmentResponse response;
        request.set_filename(fi->fullPathName);
        request.set_offset(seg_offset);
        request.set_allocateifnotexist(allocate);
        if (allocate && fEpoch != nullptr && fEpoch->epoch != 0) {
            request.set_epoch(fEpoch->epoch);
        }
        FillUserInfo(&request, fi->userinfo);
        GET_AUTH_TOKEN(authClient_, request);

        CurveFSService_Stub stub(channel);
        stub.GetOrAllocateSegment(cntl, &request, &response, NULL);
        if (cntl->Failed()) {
            mdsClientMetric_.getOrAllocateSegment.eps.count << 1;
            LOG(WARNING) << "allocate segment failed, error code = "
                         << cntl->ErrorCode()
                         << ", error content:" << cntl->ErrorText()
                         << ", offset:" << offset;
            return -cntl->ErrorCode();
        }

        auto statuscode = response.statuscode();
        switch (statuscode) {
        case StatusCode::kParaError:
            LOG(WARNING) << "GetOrAllocateSegment: error param!";
            return LIBCURVE_ERROR::FAILED;
        case StatusCode::kOwnerAuthFail:
            LOG(WARNING) << "GetOrAllocateSegment: auth failed!";
            return LIBCURVE_ERROR::AUTHFAIL;
        case StatusCode::kFileNotExists:
            LOG(WARNING) << "GetOrAllocateSegment: file not exists!";
            return LIBCURVE_ERROR::FAILED;
        case StatusCode::kSegmentNotAllocated:
            LOG(WARNING) << "GetOrAllocateSegment: segment not allocated!";
            return LIBCURVE_ERROR::NOT_ALLOCATE;
        case StatusCode::kEpochTooOld:
            LOG(WARNING) << "GetOrAllocateSegment return epoch too old!";
            return LIBCURVE_ERROR::EPOCH_TOO_OLD;
        default:
            break;
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
            // Now, we will retry until allocate segment success
            return -LIBCURVE_ERROR::RETRY_UNTIL_SUCCESS;
        }

        for (int i = 0; i < chunksNum; i++) {
            ChunkID chunkid = pfs.chunks(i).chunkid();
            CopysetID copysetid = pfs.chunks(i).copysetid();
            segInfo->lpcpIDInfo.cpidVec.push_back(copysetid);
            segInfo->chunkvec.emplace_back(chunkid, logicpoolid, copysetid);
        }
        return LIBCURVE_ERROR::OK;
    };
    return ReturnError(rpcExcutor_.DoRPCTask(task, 0));
}

LIBCURVE_ERROR MDSClient::DeAllocateSegment(const FInfo *fileInfo,
                                            uint64_t offset) {
    auto task = RPCTaskDefine {
        (void)addrindex;
        (void)rpctimeoutMS;
        LOG(INFO) << "DeAllocateSegment: filename = " << fileInfo->fullPathName
                  << ", offset = " << offset
                  << ", logid = " << cntl->log_id();
        mdsClientMetric_.deAllocateSegment.qps.count << 1;
        LatencyGuard lg(&mdsClientMetric_.deAllocateSegment.latency);

        DeAllocateSegmentRequest request;
        DeAllocateSegmentResponse response;
        request.set_filename(fileInfo->fullPathName);
        request.set_offset(offset);
        FillUserInfo(&request, fileInfo->userinfo);
        GET_AUTH_TOKEN(authClient_, request);

        CurveFSService_Stub stub(channel);
        stub.DeAllocateSegment(cntl, &request, &response, nullptr);
        if (cntl->Failed()) {
            mdsClientMetric_.deAllocateSegment.eps.count << 1;
            LOG(WARNING) << "DeAllocateSegment failed, error = "
                         << cntl->ErrorText()
                         << ", filename = " << fileInfo->fullPathName
                         << ", offset = " << offset;
            return -cntl->ErrorCode();
        }

        auto statusCode = response.statuscode();
        if (statusCode == StatusCode::kOK ||
            statusCode == StatusCode::kSegmentNotAllocated) {
            return LIBCURVE_ERROR::OK;
        } else {
            LOG(WARNING) << "DeAllocateSegment mds return failed, error = "
                         << mds::StatusCode_Name(statusCode)
                         << ", filename = " << fileInfo->fullPathName
                         << ", offset = " << offset;
            LIBCURVE_ERROR errCode;
            MDSStatusCode2LibcurveError(statusCode, &errCode);
            return errCode;
        }
    };

    return ReturnError(
        rpcExcutor_.DoRPCTask(task, metaServerOpt_.mdsMaxRetryMS));
}

LIBCURVE_ERROR MDSClient::RenameFile(const UserInfo_t &userinfo,
                                     const std::string &origin,
                                     const std::string &destination,
                                     uint64_t originId,
                                     uint64_t destinationId) {
    auto task = RPCTaskDefine {
        (void)addrindex;
        (void)rpctimeoutMS;
        LOG(INFO) << "RenameFile: origin = " << origin
                  << ", destination = " << destination
                  << ", originId = " << originId
                  << ", destinationId = " << destinationId
                  << ", owner = " << userinfo.owner
                  << ", log id = " << cntl->log_id();
        mdsClientMetric_.renameFile.qps.count << 1;
        LatencyGuard lg(&mdsClientMetric_.renameFile.latency);

        RenameFileRequest request;
        RenameFileResponse response;
        request.set_oldfilename(origin);
        request.set_newfilename(destination);
        if (originId > 0 && destinationId > 0) {
            request.set_oldfileid(originId);
            request.set_newfileid(destinationId);
        }
        FillUserInfo(&request, userinfo);
        GET_AUTH_TOKEN(authClient_, request);

        CurveFSService_Stub stub(channel);
        stub.RenameFile(cntl, &request, &response, NULL);
        if (cntl->Failed()) {
            mdsClientMetric_.renameFile.eps.count << 1;
            LOG(WARNING) << "RenameFile invoke failed, errcorde = "
                         << cntl->ErrorCode()
                         << ", error content:" << cntl->ErrorText()
                         << ", log id = " << cntl->log_id();
            return -cntl->ErrorCode();
        }

        LIBCURVE_ERROR retcode;
        StatusCode stcode = response.statuscode();
        MDSStatusCode2LibcurveError(stcode, &retcode);
        LOG_IF(WARNING, retcode != LIBCURVE_ERROR::OK)
            << "RenameFile: origin = " << origin
            << ", destination = " << destination << ", originId = " << originId
            << ", destinationId = " << destinationId
            << ", owner = " << userinfo.owner << ", errocde = " << retcode
            << ", error msg = " << StatusCode_Name(stcode)
            << ", log id = " << cntl->log_id();

        // MDS does not currently support rename file, retry again
        if (retcode == LIBCURVE_ERROR::NOT_SUPPORT) {
            return -retcode;
        }

        return retcode;
    };
    return ReturnError(
        rpcExcutor_.DoRPCTask(task, metaServerOpt_.mdsMaxRetryMS));
}

LIBCURVE_ERROR MDSClient::Extend(const std::string &filename,
                                 const UserInfo_t &userinfo, uint64_t newsize) {
    auto task = RPCTaskDefine {
        (void)addrindex;
        (void)rpctimeoutMS;
        LOG(INFO) << "Extend: filename = " << filename
                  << ", owner = " << userinfo.owner
                  << ", newsize = " << newsize
                  << ", log id = " << cntl->log_id();
        mdsClientMetric_.extendFile.qps.count << 1;
        LatencyGuard lg(&mdsClientMetric_.extendFile.latency);

        ExtendFileRequest request;
        ExtendFileResponse response;
        request.set_filename(filename);
        request.set_newsize(newsize);
        FillUserInfo(&request, userinfo);
        GET_AUTH_TOKEN(authClient_, request);

        CurveFSService_Stub stub(channel);
        stub.ExtendFile(cntl, &request, &response, NULL);
        if (cntl->Failed()) {
            mdsClientMetric_.extendFile.eps.count << 1;
            LOG(WARNING) << "ExtendFile invoke failed, errcorde = "
                         << cntl->ErrorCode()
                         << ", error content:" << cntl->ErrorText()
                         << ", log id = " << cntl->log_id();
            return -cntl->ErrorCode();
        }

        LIBCURVE_ERROR retcode;
        StatusCode stcode = response.statuscode();
        MDSStatusCode2LibcurveError(stcode, &retcode);
        LOG_IF(WARNING, retcode != LIBCURVE_ERROR::OK)
            << "Extend: filename = " << filename
            << ", owner = " << userinfo.owner << ", newsize = " << newsize
            << ", errocde = " << retcode
            << ", error msg = " << StatusCode_Name(stcode)
            << ", log id = " << cntl->log_id();
        return retcode;
    };
    return ReturnError(
        rpcExcutor_.DoRPCTask(task, metaServerOpt_.mdsMaxRetryMS));
}

LIBCURVE_ERROR MDSClient::DeleteFile(const std::string &filename,
                                     const UserInfo_t &userinfo,
                                     bool deleteforce, uint64_t fileid) {
    auto task = RPCTaskDefine {
        (void)addrindex;
        (void)rpctimeoutMS;
        LOG(INFO) << "DeleteFile: filename = " << filename
                  << ", owner = " << userinfo.owner
                  << ", log id = " << cntl->log_id();
        mdsClientMetric_.deleteFile.qps.count << 1;
        LatencyGuard lg(&mdsClientMetric_.deleteFile.latency);

        DeleteFileRequest request;
        DeleteFileResponse response;
        request.set_filename(filename);
        request.set_forcedelete(deleteforce);
        if (fileid > 0) {
            request.set_fileid(fileid);
        }
        FillUserInfo(&request, userinfo);
        GET_AUTH_TOKEN(authClient_, request);

        CurveFSService_Stub stub(channel);
        stub.DeleteFile(cntl, &request, &response, NULL);
        if (cntl->Failed()) {
            mdsClientMetric_.deleteFile.eps.count << 1;
            LOG(WARNING) << "DeleteFile invoke failed, errcorde = "
                         << cntl->ErrorCode()
                         << ", error content:" << cntl->ErrorText()
                         << ", log id = " << cntl->log_id();
            return -cntl->ErrorCode();
        }


        LIBCURVE_ERROR retcode;
        StatusCode stcode = response.statuscode();
        MDSStatusCode2LibcurveError(stcode, &retcode);
        LOG_IF(WARNING, retcode != LIBCURVE_ERROR::OK)
            << "DeleteFile: filename = " << filename
            << ", owner = " << userinfo.owner << ", errocde = " << retcode
            << ", error msg = " << StatusCode_Name(stcode)
            << ", log id = " << cntl->log_id();

        // MDS does not currently support delete file, retry again
        if (retcode == LIBCURVE_ERROR::NOT_SUPPORT) {
            return -retcode;
        }

        return retcode;
    };
    return ReturnError(
        rpcExcutor_.DoRPCTask(task, metaServerOpt_.mdsMaxRetryMS));
}

LIBCURVE_ERROR MDSClient::RecoverFile(const std::string &filename,
                                      const UserInfo_t &userinfo,
                                      uint64_t fileid) {
    auto task = RPCTaskDefine {
        (void)addrindex;
        (void)rpctimeoutMS;
        LOG(INFO) << "RecoverFile: filename = " << filename
                  << ", owner = " << userinfo.owner
                  << ", log id = " << cntl->log_id();
        mdsClientMetric_.recoverFile.qps.count << 1;
        LatencyGuard lg(&mdsClientMetric_.recoverFile.latency);

        RecoverFileRequest request;
        RecoverFileResponse response;
        request.set_filename(filename);
        request.set_fileid(fileid);
        FillUserInfo(&request, userinfo);
        GET_AUTH_TOKEN(authClient_, request);

        CurveFSService_Stub stub(channel);
        stub.RecoverFile(cntl, &request, &response, NULL);
        if (cntl->Failed()) {
            mdsClientMetric_.recoverFile.eps.count << 1;
            LOG(WARNING) << "RecoverFile invoke failed, errcorde = "
                         << cntl->ErrorCode()
                         << ", error content:" << cntl->ErrorText()
                         << ", log id = " << cntl->log_id();
            return -cntl->ErrorCode();
        }

        LIBCURVE_ERROR retcode;
        StatusCode stcode = response.statuscode();
        MDSStatusCode2LibcurveError(stcode, &retcode);
        LOG_IF(WARNING, retcode != LIBCURVE_ERROR::OK)
            << "RecoverFile: filename = " << filename
            << ", owner = " << userinfo.owner << ", errocde = " << retcode
            << ", error msg = " << StatusCode_Name(stcode)
            << ", log id = " << cntl->log_id();
        return retcode;
    };
    return ReturnError(
        rpcExcutor_.DoRPCTask(task, metaServerOpt_.mdsMaxRetryMS));
}

LIBCURVE_ERROR MDSClient::ChangeOwner(const std::string &filename,
                                      const std::string &newOwner,
                                      const UserInfo_t &userinfo) {
    auto task = RPCTaskDefine {
        (void)addrindex;
        (void)rpctimeoutMS;
        LOG(INFO) << "ChangeOwner: filename = " << filename
                  << ", operator owner = " << userinfo.owner
                  << ", new owner = " << newOwner
                  << ", log id = " << cntl->log_id();
        mdsClientMetric_.changeOwner.qps.count << 1;
        LatencyGuard lg(&mdsClientMetric_.changeOwner.latency);

        ChangeOwnerRequest request;
        ChangeOwnerResponse response;
        uint64_t date = curve::common::TimeUtility::GetTimeofDayUs();
        request.set_date(date);
        request.set_filename(filename);
        request.set_newowner(newOwner);
        request.set_rootowner(userinfo.owner);
        request.set_signature(CalcSignature(userinfo, date));
        GET_AUTH_TOKEN(authClient_, request);

        CurveFSService_Stub stub(channel);
        stub.ChangeOwner(cntl, &request, &response, NULL);
        if (cntl->Failed()) {
            mdsClientMetric_.changeOwner.eps.count << 1;
            LOG(WARNING) << "ChangeOwner invoke failed, errcorde = "
                         << cntl->ErrorCode()
                         << ", error content:" << cntl->ErrorText()
                         << ", log id = " << cntl->log_id();
            return -cntl->ErrorCode();
        }

        LIBCURVE_ERROR retcode;
        StatusCode stcode = response.statuscode();
        MDSStatusCode2LibcurveError(stcode, &retcode);
        LOG_IF(WARNING, retcode != LIBCURVE_ERROR::OK)
            << "ChangeOwner: filename = " << filename
            << ", owner = " << userinfo.owner << ", new owner = " << newOwner
            << ", errocde = " << retcode
            << ", error msg = " << StatusCode_Name(stcode)
            << ", log id = " << cntl->log_id();

        // MDS does not currently support change file owner, retry again
        if (retcode == LIBCURVE_ERROR::NOT_SUPPORT) {
            return -retcode;
        }

        return retcode;
    };
    return ReturnError(
        rpcExcutor_.DoRPCTask(task, metaServerOpt_.mdsMaxRetryMS));
}

LIBCURVE_ERROR MDSClient::ListDir(const std::string &dirpath,
                                  const UserInfo_t &userinfo,
                                  std::vector<FileStatInfo> *filestatVec) {
    auto task = RPCTaskDefine {
        (void)addrindex;
        (void)rpctimeoutMS;
        LOG(INFO) << "Listdir: filename = " << dirpath
                  << ", owner = " << userinfo.owner
                  << ", log id = " << cntl->log_id();
        mdsClientMetric_.listDir.qps.count << 1;
        LatencyGuard lg(&mdsClientMetric_.listDir.latency);

        curve::mds::ListDirRequest request;
        ListDirResponse response;
        request.set_filename(dirpath);
        FillUserInfo(&request, userinfo);
        GET_AUTH_TOKEN(authClient_, request);

        CurveFSService_Stub stub(channel);
        stub.ListDir(cntl, &request, &response, NULL);
        if (cntl->Failed()) {
            mdsClientMetric_.listDir.eps.count << 1;
            LOG(WARNING) << "Listdir invoke failed, errcorde = "
                         << cntl->ErrorCode()
                         << ", error content:" << cntl->ErrorText()
                         << ", log id = " << cntl->log_id();
            return -cntl->ErrorCode();
        }

        LIBCURVE_ERROR retcode;
        StatusCode stcode = response.statuscode();
        MDSStatusCode2LibcurveError(stcode, &retcode);
        LOG_IF(WARNING, retcode != LIBCURVE_ERROR::OK)
            << "Listdir: filename = " << dirpath
            << ", owner = " << userinfo.owner << ", errocde = " << retcode
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
    return ReturnError(
        rpcExcutor_.DoRPCTask(task, metaServerOpt_.mdsMaxRetryMS));
}

LIBCURVE_ERROR MDSClient::GetChunkServerInfo(const PeerAddr &csAddr,
                             CopysetPeerInfo<ChunkServerID> *chunkserverInfo) {
    if (!chunkserverInfo) {
        LOG(ERROR) << "chunkserverInfo pointer is null!";
        return LIBCURVE_ERROR::FAILED;
    }

    bool valid = NetCommon::CheckAddressValid(csAddr.ToString());
    if (!valid) {
        LOG(ERROR) << "chunkserver address " << csAddr.ToString()
                   << " invalid!";
        return LIBCURVE_ERROR::FAILED;
    }

    auto task = RPCTaskDefine {
        (void)addrindex;
        (void)rpctimeoutMS;
        mdsClientMetric_.getChunkServerId.qps.count << 1;
        LatencyGuard guard(&mdsClientMetric_.getChunkServerId.latency);

        std::vector<std::string> strs;
        curve::common::SplitString(csAddr.ToString(), ":", &strs);
        const std::string &ip = strs[0];

        uint64_t port;
        bool succ = curve::common::StringToUll(strs[1], &port);
        if (!succ) {
            LOG(ERROR) << "convert " << strs[1] << " to port failed";
            return LIBCURVE_ERROR::FAILED;
        }
        LOG(INFO) << "GetChunkServerInfo from mds: "
                  << "ip = " << ip
                  << ", port = " << port
                  << ", log id = " << cntl->log_id();

        GetChunkServerInfoRequest request;
        GetChunkServerInfoResponse response;
        request.set_hostip(ip);
        request.set_port(port);
        GET_AUTH_TOKEN(authClient_, request);

        TopologyService_Stub stub(channel);
        stub.GetChunkServer(cntl, &request, &response, NULL);
        if (cntl->Failed()) {
            LOG(WARNING) << "GetChunkServerInfo invoke failed, errcorde = "
                         << cntl->ErrorCode()
                         << ", error content:" << cntl->ErrorText()
                         << ", log id = " << cntl->log_id();
            return -cntl->ErrorCode();
        }

        int statusCode = response.statuscode();
        LOG_IF(WARNING, statusCode != 0)
            << "GetChunkServerInfo: errocde = " << statusCode
            << ", log id = " << cntl->log_id();

        if (statusCode == 0) {
            const auto &csInfo = response.chunkserverinfo();
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
            *chunkserverInfo =
                CopysetPeerInfo<ChunkServerID>(csId, PeerAddr(internal),
                                               PeerAddr(external));
            return LIBCURVE_ERROR::OK;
        } else {
            return LIBCURVE_ERROR::FAILED;
        }
    };
    return ReturnError(
        rpcExcutor_.DoRPCTask(task, metaServerOpt_.mdsMaxRetryMS));
}

LIBCURVE_ERROR
MDSClient::ListChunkServerInServer(const std::string &serverIp,
                                   std::vector<ChunkServerID> *csIds) {
    auto task = RPCTaskDefine {
        (void)addrindex;
        (void)rpctimeoutMS;
        LOG(INFO) << "ListChunkServerInServer from mds: "
                  << "ip = " << serverIp
                  << ", log id = " << cntl->log_id();
        mdsClientMetric_.listChunkserverInServer.qps.count << 1;
        LatencyGuard guard(&mdsClientMetric_.listChunkserverInServer.latency);

        ListChunkServerRequest request;
        ListChunkServerResponse response;
        request.set_ip(serverIp);
        GET_AUTH_TOKEN(authClient_, request);

        TopologyService_Stub stub(channel);
        stub.ListChunkServer(cntl, &request, &response, NULL);
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

    return ReturnError(
        rpcExcutor_.DoRPCTask(task, metaServerOpt_.mdsMaxRetryMS));
}

void MDSClient::MDSStatusCode2LibcurveError(const StatusCode &status,
                                            LIBCURVE_ERROR *errcode) {
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
    case StatusCode::kPoolsetNotExist:
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
    case ::curve::mds::StatusCode::kAuthFailed:
        *errcode = LIBCURVE_ERROR::AUTH_FAILED;
        break;
    default:
        *errcode = LIBCURVE_ERROR::UNKNOWN;
        break;
    }
}


LIBCURVE_ERROR MDSClient::ReturnError(int retcode) {
    // logic error
    if (retcode >= 0) {
        return static_cast<LIBCURVE_ERROR>(retcode);
    }

    // rpc error or special defined error
    switch (retcode) {
    case -LIBCURVE_ERROR::NOT_SUPPORT:
        return LIBCURVE_ERROR::NOT_SUPPORT;
    case -LIBCURVE_ERROR::FILE_OCCUPIED:
        return LIBCURVE_ERROR::FILE_OCCUPIED;
    default:
        return LIBCURVE_ERROR::FAILED;
    }
}

}  // namespace client
}  // namespace curve
