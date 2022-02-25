/*
 *  Copyright (c) 2022 NetEase Inc.
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
 * Author: chengyi01
 */

#include "curvefs/src/metaserver/mdsclient/mds_client.h"

#include <map>
#include <vector>
namespace curvefs {
namespace metaserver {
namespace mdsclient {

FSStatusCode
MdsClientImpl::Init(const ::curve::client::MetaServerOption &mdsOpt,
                    MDSBaseClient *baseclient) {
    mdsOpt_ = mdsOpt;
    rpcexcutor_.SetOption(mdsOpt_.rpcRetryOpt);
    mdsbasecli_ = baseclient;

    std::ostringstream oss;
    std::for_each(mdsOpt_.rpcRetryOpt.addrs.begin(),
                  mdsOpt_.rpcRetryOpt.addrs.end(),
                  [&](const std::string &addr) { oss << " " << addr; });

    LOG(INFO) << "MDSClient init success, addresses:" << oss.str();
    return FSStatusCode::OK;
}

#define RPCTask                                                                \
    [&](int addrindex, uint64_t rpctimeoutMS, brpc::Channel *channel,          \
        brpc::Controller *cntl) -> int


FSStatusCode MdsClientImpl::GetFsInfo(const std::string &fsName,
                                      FsInfo *fsInfo) {
    auto task = RPCTask {
        mdsClientMetric_.getFsInfo.qps.count << 1;
        GetFsInfoResponse response;
        mdsbasecli_->GetFsInfo(fsName, &response, cntl, channel);

        if (cntl->Failed()) {
            mdsClientMetric_.getFsInfo.eps.count << 1;
            LOG(WARNING) << "GetFsInfo Failed, errorcode = "
                         << cntl->ErrorCode()
                         << ", error content:" << cntl->ErrorText()
                         << ", log id = " << cntl->log_id();
            return -cntl->ErrorCode();
        }

        FSStatusCode ret = response.statuscode();
        if (ret != FSStatusCode::OK) {
            LOG(WARNING) << "GetFsInfo: fsname = " << fsName
                         << ", errcode = " << ret
                         << ", errmsg = " << FSStatusCode_Name(ret);
        } else if (response.has_fsinfo()) {
            fsInfo->CopyFrom(response.fsinfo());
        }

        return ret;
    };
    return ReturnError(rpcexcutor_.DoRPCTask(task, mdsOpt_.mdsMaxRetryMS));
}

FSStatusCode MdsClientImpl::GetFsInfo(uint32_t fsId, FsInfo *fsInfo) {
    auto task = RPCTask {
        mdsClientMetric_.getFsInfo.qps.count << 1;
        GetFsInfoResponse response;
        mdsbasecli_->GetFsInfo(fsId, &response, cntl, channel);
        if (cntl->Failed()) {
            mdsClientMetric_.getFsInfo.eps.count << 1;
            LOG(WARNING) << "GetFsInfo Failed, errorcode = "
                         << cntl->ErrorCode()
                         << ", error content:" << cntl->ErrorText()
                         << ", log id = " << cntl->log_id();
            return -cntl->ErrorCode();
        }

        FSStatusCode ret = response.statuscode();
        if (ret != FSStatusCode::OK) {
            LOG(WARNING) << "GetFsInfo: fsid = " << fsId
                         << ", errcode = " << ret
                         << ", errmsg = " << FSStatusCode_Name(ret);
        } else if (response.has_fsinfo()) {
            fsInfo->CopyFrom(response.fsinfo());
        }
        return ret;
    };
    return ReturnError(rpcexcutor_.DoRPCTask(task, mdsOpt_.mdsMaxRetryMS));
}

FSStatusCode MdsClientImpl::ReturnError(int retcode) {
    // rpc error convert to FSStatusCode::RPC_ERROR
    if (retcode < 0) {
        return FSStatusCode::RPC_ERROR;
    }

    // logic error
    return static_cast<FSStatusCode>(retcode);
}

}  // namespace mdsclient
}  // namespace metaserver
}  // namespace curvefs
