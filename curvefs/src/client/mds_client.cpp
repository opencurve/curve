/*
 *  Copyright (c) 2021 NetEase Inc.
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
 * Created Date: Thur Jun 15 2021
 * Author: lixiaocui
 */

#include "curvefs/src/client/mds_client.h"

namespace curvefs {
namespace client {

CURVEFS_ERROR MdsClientImpl::MDSRPCExcutor::DoRPCTask(RPCFunc task) {
    // TODO(lixiaocui): may be need reuse channel
    brpc::Channel channel;
    std::string mdsaddr = opt_.mdsaddr;
    int ret = channel.Init(mdsaddr.c_str(), nullptr);
    if (ret != 0) {
        LOG(WARNING) << "Init channel failed, addr = " << mdsaddr;
        return static_cast<CURVEFS_ERROR>(-EHOSTDOWN);
    }

    brpc::Controller cntl;
    cntl.set_timeout_ms(opt_.rpcTimeoutMs);

    return task(&channel, &cntl);
}


CURVEFS_ERROR
MdsClientImpl::Init(const MdsOption &mdsOpt, MDSBaseClient *baseclient) {
    excutor_.SetOption(mdsOpt);
    mdsbasecli_ = baseclient;
    return CURVEFS_ERROR::OK;
}

#define RPCTask                                                                \
    [&](brpc::Channel * channel, brpc::Controller * cntl) -> CURVEFS_ERROR

CURVEFS_ERROR MdsClientImpl::CreateFs(const std::string &fsName,
                                      uint64_t blockSize,
                                      const Volume &volume) {
    auto task = RPCTask {
        CreateFsResponse response;
        mdsbasecli_->CreateFs(fsName, blockSize, volume, &response, cntl,
                              channel);
        if (cntl->Failed()) {
            LOG(WARNING) << "CreateFs Failed, errorcode = " << cntl->ErrorCode()
                         << ", error content:" << cntl->ErrorText()
                         << ", log id = " << cntl->log_id();
            return static_cast<CURVEFS_ERROR>(-cntl->ErrorCode());
        }

        CURVEFS_ERROR retcode = CURVEFS_ERROR::FAILED;
        FSStatusCode stcode = response.statuscode();
        FSStatusCode2CurveFSErr(stcode, &retcode);
        LOG_IF(WARNING, retcode != CURVEFS_ERROR::OK)
            << "CreateFs: fsname = " << fsName << ", blocksize = " << blockSize
            << ", volume = " << volume.DebugString()
            << ", errcode = " << retcode
            << ", errmsg = " << FSStatusCode_Name(stcode);
        // TDOD(lixiaocui): exception handling
        return retcode;
    };
    return excutor_.DoRPCTask(task);
}

CURVEFS_ERROR MdsClientImpl::CreateFsS3(const std::string &fsName,
                                      uint64_t blockSize,
                                      const S3Info &s3Info) {
    auto task = RPCTask {
        CreateFsResponse response;
        mdsbasecli_->CreateFsS3(fsName, blockSize, s3Info, &response, cntl,
                              channel);
        if (cntl->Failed()) {
            LOG(WARNING) << "CreateFs Failed, errorcode = " << cntl->ErrorCode()
                         << ", error content:" << cntl->ErrorText()
                         << ", log id = " << cntl->log_id();
            return static_cast<CURVEFS_ERROR>(-cntl->ErrorCode());
        }

        CURVEFS_ERROR retcode = CURVEFS_ERROR::FAILED;
        FSStatusCode stcode = response.statuscode();
        FSStatusCode2CurveFSErr(stcode, &retcode);
        LOG_IF(WARNING, retcode != CURVEFS_ERROR::OK)
            << "CreateFs: fsname = " << fsName << ", blocksize = " << blockSize
            << ", errcode = " << retcode
            << ", errmsg = " << FSStatusCode_Name(stcode);

        // TDOD(lixiaocui): exception handling
        return retcode;
    };
    return excutor_.DoRPCTask(task);
}

CURVEFS_ERROR MdsClientImpl::DeleteFs(const std::string &fsName) {
    auto task = RPCTask {
        DeleteFsResponse response;
        mdsbasecli_->DeleteFs(fsName, &response, cntl, channel);
        if (cntl->Failed()) {
            LOG(WARNING) << "DeleteFs Failed, errorcode = " << cntl->ErrorCode()
                         << ", error content:" << cntl->ErrorText()
                         << ", log id = " << cntl->log_id();
            return static_cast<CURVEFS_ERROR>(-cntl->ErrorCode());
        }

        CURVEFS_ERROR retcode = CURVEFS_ERROR::FAILED;
        FSStatusCode stcode = response.statuscode();
        FSStatusCode2CurveFSErr(stcode, &retcode);
        LOG_IF(WARNING, retcode != CURVEFS_ERROR::OK)
            << "DeleteFs: fsname = " << fsName << ", errcode = " << retcode
            << ", errmsg = " << FSStatusCode_Name(stcode);
        // TDOD(lixiaocui): exception handling
        return retcode;
    };
    return excutor_.DoRPCTask(task);
}

CURVEFS_ERROR MdsClientImpl::MountFs(const std::string &fsName,
                                     const std::string &mountPt,
                                     FsInfo *fsInfo) {
    auto task = RPCTask {
        MountFsResponse response;
        mdsbasecli_->MountFs(fsName, mountPt, &response, cntl, channel);
        if (cntl->Failed()) {
            LOG(WARNING) << "MountFs Failed, errorcode = " << cntl->ErrorCode()
                         << ", error content:" << cntl->ErrorText()
                         << ", log id = " << cntl->log_id();
            return static_cast<CURVEFS_ERROR>(-cntl->ErrorCode());
        }

        CURVEFS_ERROR retcode = CURVEFS_ERROR::FAILED;
        FSStatusCode stcode = response.statuscode();
        FSStatusCode2CurveFSErr(stcode, &retcode);
        LOG_IF(WARNING, retcode != CURVEFS_ERROR::OK)
            << "MountFs: fsname = " << fsName
            << ", mountPt = " << mountPt
            << ", errcode = " << retcode
            << ", errmsg = " << FSStatusCode_Name(stcode);

        if (response.has_fsinfo()) {
            fsInfo->CopyFrom(response.fsinfo());
        }
        // TDOD(lixiaocui): exception handling
        return retcode;
    };
    return excutor_.DoRPCTask(task);
}

CURVEFS_ERROR MdsClientImpl::UmountFs(const std::string &fsName,
                                      const std::string &mountPt) {
    auto task = RPCTask {
        UmountFsResponse response;
        mdsbasecli_->UmountFs(fsName, mountPt, &response, cntl, channel);
        if (cntl->Failed()) {
            LOG(WARNING) << "UmountFs Failed, errorcode = " << cntl->ErrorCode()
                         << ", error content:" << cntl->ErrorText()
                         << ", log id = " << cntl->log_id();
            return static_cast<CURVEFS_ERROR>(-cntl->ErrorCode());
        }

        CURVEFS_ERROR retcode = CURVEFS_ERROR::FAILED;
        FSStatusCode stcode = response.statuscode();
        FSStatusCode2CurveFSErr(stcode, &retcode);
        LOG_IF(WARNING, retcode != CURVEFS_ERROR::OK)
            << "UmountFs: fsname = " << fsName
            << ", mountPt = " << mountPt
            << ", errcode = " << retcode
            << ", errmsg = " << FSStatusCode_Name(stcode);
        // TDOD(lixiaocui): exception handling
        return retcode;
    };
    return excutor_.DoRPCTask(task);
}

CURVEFS_ERROR MdsClientImpl::GetFsInfo(const std::string &fsName,
                                       FsInfo *fsInfo) {
    auto task = RPCTask {
        GetFsInfoResponse response;
        mdsbasecli_->GetFsInfo(fsName, &response, cntl, channel);
        if (cntl->Failed()) {
            LOG(WARNING) << "GetFsInfo Failed, errorcode = "
                         << cntl->ErrorCode()
                         << ", error content:" << cntl->ErrorText()
                         << ", log id = " << cntl->log_id();
            return static_cast<CURVEFS_ERROR>(-cntl->ErrorCode());
        }

        CURVEFS_ERROR retcode = CURVEFS_ERROR::FAILED;
        FSStatusCode stcode = response.statuscode();
        FSStatusCode2CurveFSErr(stcode, &retcode);
        LOG_IF(WARNING, retcode != CURVEFS_ERROR::OK)
            << "GetFsInfo: fsname = " << fsName << ", errcode = " << retcode
            << ", errmsg = " << FSStatusCode_Name(stcode);

        if (response.has_fsinfo()) {
            fsInfo->CopyFrom(response.fsinfo());
        }
        // TDOD(lixiaocui): exception handling
        return retcode;
    };
    return excutor_.DoRPCTask(task);
}

CURVEFS_ERROR MdsClientImpl::GetFsInfo(uint32_t fsId, FsInfo *fsInfo) {
    auto task = RPCTask {
        GetFsInfoResponse response;
        mdsbasecli_->GetFsInfo(fsId, &response, cntl, channel);
        if (cntl->Failed()) {
            LOG(WARNING) << "GetFsInfo Failed, errorcode = "
                         << cntl->ErrorCode()
                         << ", error content:" << cntl->ErrorText()
                         << ", log id = " << cntl->log_id();
            return static_cast<CURVEFS_ERROR>(-cntl->ErrorCode());
        }

        CURVEFS_ERROR retcode = CURVEFS_ERROR::FAILED;
        FSStatusCode stcode = response.statuscode();
        FSStatusCode2CurveFSErr(stcode, &retcode);
        LOG_IF(WARNING, retcode != CURVEFS_ERROR::OK)
            << "GetFsInfo: fsid = " << fsId << ", errcode = " << retcode
            << ", errmsg = " << FSStatusCode_Name(stcode);

        if (response.has_fsinfo()) {
            fsInfo->CopyFrom(response.fsinfo());
        }
        // TDOD(lixiaocui): exception handling
        return retcode;
    };
    return excutor_.DoRPCTask(task);
}

CURVEFS_ERROR MdsClientImpl::CommitTx(uint32_t fsId,
                                      const std::vector<PartitionTxId>& txIds) {
    auto task = RPCTask {
        CommitTxResponse response;
        mdsbasecli_->CommitTx(fsId, txIds, &response, cntl, channel);

        if (cntl->Failed()) {
            LOG(WARNING) << "CommitTx failed, errorCode = " << cntl->ErrorCode()
                         << ", errorText = " << cntl->ErrorText()
                         << ", logId = " << cntl->log_id();
            return static_cast<CURVEFS_ERROR>(-cntl->ErrorCode());
        }

        CURVEFS_ERROR rc = CURVEFS_ERROR::FAILED;
        FSStatusCode statusCode = response.statuscode();
        FSStatusCode2CurveFSErr(statusCode, &rc);
        LOG_IF(WARNING, rc != CURVEFS_ERROR::OK)
            << "CommitTx: fsid = " << fsId << ", retCode = " << rc
            << ", errmsg = " << FSStatusCode_Name(statusCode);

        return rc;
    };

    // TODO(Wine93): retry until success
    return excutor_.DoRPCTask(task);
}

void MdsClientImpl::FSStatusCode2CurveFSErr(FSStatusCode stcode,
                                            CURVEFS_ERROR *retcode) {
    switch (stcode) {
    case FSStatusCode::OK:
        *retcode = CURVEFS_ERROR::OK;
        break;

    case FSStatusCode::NOT_FOUND:
        *retcode = CURVEFS_ERROR::NOTEXIST;
        break;

    case FSStatusCode::PARAM_ERROR:
        *retcode = CURVEFS_ERROR::INVALIDPARAM;
        break;

    default:
        *retcode = CURVEFS_ERROR::UNKNOWN;
        break;
    }
}

}  // namespace client
}  // namespace curvefs
