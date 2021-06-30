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

#include <algorithm>
#include "curvefs/src/client/metaserver_client.h"

namespace curvefs {
namespace client {

CURVEFS_ERROR
MetaServerClientImpl::MetaServerRPCExcutor::DoRPCTask(RPCFunc task) {
    brpc::Channel channel;
    int ret = channel.Init(opt_.msaddr.c_str(), nullptr);
    if (ret != 0) {
        LOG(WARNING) << "Init channel failed, addr = " << opt_.msaddr;
        return static_cast<CURVEFS_ERROR>(-EHOSTDOWN);
    }

    brpc::Controller cntl;
    cntl.set_timeout_ms(opt_.rpcTimeoutMs);

    return task(&channel, &cntl);
}

CURVEFS_ERROR
MetaServerClientImpl::Init(const MetaServerOption &metaopt,
                           MetaServerBaseClient *baseclient) {
    basecli_ = baseclient;
    excutor_.SetOption(metaopt);
    return CURVEFS_ERROR::OK;
}


#define RPCTaskDefine                                                          \
    [&](brpc::Channel * channel, brpc::Controller * cntl) -> CURVEFS_ERROR

CURVEFS_ERROR MetaServerClientImpl::GetDentry(uint32_t fsId, uint64_t inodeid,
                                              const std::string &name,
                                              Dentry *out) {
    auto task = RPCTaskDefine {
        GetDentryResponse response;
        basecli_->GetDentry(fsId, inodeid, name, &response, cntl, channel);

        if (cntl->Failed()) {
            LOG(WARNING) << "GetDentry Failed, errorcode = "
                         << cntl->ErrorCode()
                         << ", error content:" << cntl->ErrorText()
                         << ", log id = " << cntl->log_id();
            return static_cast<CURVEFS_ERROR>(-cntl->ErrorCode());
        }

        CURVEFS_ERROR retcode = CURVEFS_ERROR::FAILED;
        MetaStatusCode stcode = response.statuscode();
        MetaServerStatusCode2CurveFSErr(stcode, &retcode);
        if (retcode != CURVEFS_ERROR::OK) {
            LOG(WARNING) << "GetDentry: fsId = " << fsId
                         << ", inodeid = " << inodeid << ", name = " << name
                         << ", errcode = " << retcode
                         << ", errmsg = " << MetaStatusCode_Name(stcode);
        } else if (response.has_dentry()) {
            *out = response.dentry();
        }

        // TDOD(lixiaocui): exception handling
        return retcode;
    };

    return excutor_.DoRPCTask(task);
}

CURVEFS_ERROR MetaServerClientImpl::ListDentry(uint32_t fsId, uint64_t inodeid,
                                               const std::string &last,
                                               uint32_t count,
                                               std::list<Dentry> *dentryList) {
    auto task = RPCTaskDefine {
        ListDentryResponse response;
        basecli_->ListDentry(fsId, inodeid, last, count, &response, cntl,
                             channel);
        if (cntl->Failed()) {
            LOG(WARNING) << "ListDentry Failed, errorcode = "
                         << cntl->ErrorCode()
                         << ", error content:" << cntl->ErrorText()
                         << ", log id = " << cntl->log_id();
            return static_cast<CURVEFS_ERROR>(-cntl->ErrorCode());
        }

        CURVEFS_ERROR retcode = CURVEFS_ERROR::FAILED;
        MetaStatusCode stcode = response.statuscode();
        MetaServerStatusCode2CurveFSErr(stcode, &retcode);
        LOG_IF(WARNING, retcode != CURVEFS_ERROR::OK)
            << "ListDentry: fsId = " << fsId << ", inodeid = " << inodeid
            << ", last = " << last << ",count = " << count
            << ", errcode = " << retcode
            << ", errmsg = " << MetaStatusCode_Name(stcode);
        if (retcode != CURVEFS_ERROR::OK) {
            LOG(WARNING) << "ListDentry: fsId = " << fsId
                         << ", inodeid = " << inodeid << ", last = " << last
                         << ",count = " << count << ", errcode = " << retcode
                         << ", errmsg = " << MetaStatusCode_Name(stcode);
        } else {
            auto dentrys = response.dentrys();
            for_each(dentrys.begin(), dentrys.end(),
                     [&](Dentry &d) { dentryList->push_back(d); });
        }

        // TDOD(lixiaocui): exception handling
        return retcode;
    };

    return excutor_.DoRPCTask(task);
}

CURVEFS_ERROR MetaServerClientImpl::CreateDentry(const Dentry &dentry) {
    auto task = RPCTaskDefine {
        CreateDentryResponse response;
        basecli_->CreateDentry(dentry, &response, cntl, channel);

        if (cntl->Failed()) {
            LOG(WARNING) << "CreateDentry Failed, errorcode = "
                         << cntl->ErrorCode()
                         << ", error content:" << cntl->ErrorText()
                         << ", log id = " << cntl->log_id();
            return static_cast<CURVEFS_ERROR>(-cntl->ErrorCode());
        }

        CURVEFS_ERROR retcode = CURVEFS_ERROR::FAILED;
        MetaStatusCode stcode = response.statuscode();
        MetaServerStatusCode2CurveFSErr(stcode, &retcode);
        LOG_IF(WARNING, retcode != CURVEFS_ERROR::OK)
            << "CreateDentry:  dentry = " << dentry.DebugString()
            << ", errcode = " << retcode
            << ", errmsg = " << MetaStatusCode_Name(stcode);
        // TDOD(lixiaocui): exception handling
        return retcode;
    };
    return excutor_.DoRPCTask(task);
}

CURVEFS_ERROR MetaServerClientImpl::DeleteDentry(uint32_t fsId,
                                                 uint64_t inodeid,
                                                 const std::string &name) {
    auto task = RPCTaskDefine {
        DeleteDentryResponse response;
        basecli_->DeleteDentry(fsId, inodeid, name, &response, cntl, channel);

        if (cntl->Failed()) {
            LOG(WARNING) << "DeleteDentry Failed, errorcode = "
                         << cntl->ErrorCode()
                         << ", error content:" << cntl->ErrorText()
                         << ", log id = " << cntl->log_id();
            return static_cast<CURVEFS_ERROR>(-cntl->ErrorCode());
        }

        CURVEFS_ERROR retcode = CURVEFS_ERROR::FAILED;
        MetaStatusCode stcode = response.statuscode();
        MetaServerStatusCode2CurveFSErr(stcode, &retcode);
        LOG_IF(WARNING, retcode != CURVEFS_ERROR::OK)
            << "DeleteDentry:  fsid = " << fsId << ", inodeid = " << inodeid
            << ", name = " << name << ", errcode = " << retcode
            << ", errmsg = " << MetaStatusCode_Name(stcode);
        // TDOD(lixiaocui): exception handling
        return retcode;
    };
    return excutor_.DoRPCTask(task);
}

CURVEFS_ERROR MetaServerClientImpl::GetInode(uint32_t fsId, uint64_t inodeid,
                                             Inode *out) {
    auto task = RPCTaskDefine {
        GetInodeResponse response;
        basecli_->GetInode(fsId, inodeid, &response, cntl, channel);

        if (cntl->Failed()) {
            LOG(WARNING) << "GetInode Failed, errorcode = " << cntl->ErrorCode()
                         << ", error content:" << cntl->ErrorText()
                         << ", log id = " << cntl->log_id();
            return static_cast<CURVEFS_ERROR>(-cntl->ErrorCode());
        }

        CURVEFS_ERROR retcode = CURVEFS_ERROR::FAILED;
        MetaStatusCode stcode = response.statuscode();
        MetaServerStatusCode2CurveFSErr(stcode, &retcode);
        LOG_IF(WARNING, retcode != CURVEFS_ERROR::OK)
            << "GetInode:  inodeid = " << inodeid << ", errcode = " << retcode
            << ", errmsg = " << MetaStatusCode_Name(stcode);

        if (response.has_inode()) {
            out->CopyFrom(response.inode());
        }
        // TDOD(lixiaocui): exception handling
        return retcode;
    };
    return excutor_.DoRPCTask(task);
}

CURVEFS_ERROR MetaServerClientImpl::UpdateInode(const Inode &inode) {
    auto task = RPCTaskDefine {
        UpdateInodeResponse response;
        basecli_->UpdateInode(inode, &response, cntl, channel);

        if (cntl->Failed()) {
            LOG(WARNING) << "UpdateInode Failed, errorcode = "
                         << cntl->ErrorCode()
                         << ", error content:" << cntl->ErrorText()
                         << ", log id = " << cntl->log_id();
            return static_cast<CURVEFS_ERROR>(-cntl->ErrorCode());
        }

        CURVEFS_ERROR retcode = CURVEFS_ERROR::FAILED;
        MetaStatusCode stcode = response.statuscode();
        MetaServerStatusCode2CurveFSErr(stcode, &retcode);
        LOG_IF(WARNING, retcode != CURVEFS_ERROR::OK)
            << "UpdateInode:  inodeid = " << inode.DebugString()
            << ", errcode = " << retcode
            << ", errmsg = " << MetaStatusCode_Name(stcode);
        // TDOD(lixiaocui): exception handling
        return retcode;
    };
    return excutor_.DoRPCTask(task);
}

CURVEFS_ERROR MetaServerClientImpl::CreateInode(const InodeParam &param,
                                                Inode *out) {
    auto task = RPCTaskDefine {
        CreateInodeResponse response;
        basecli_->CreateInode(param, &response, cntl, channel);

        if (cntl->Failed()) {
            LOG(WARNING) << "CreateInode Failed, errorcode = "
                         << cntl->ErrorCode()
                         << ", error content:" << cntl->ErrorText()
                         << ", log id = " << cntl->log_id();
            return static_cast<CURVEFS_ERROR>(-cntl->ErrorCode());
        }

        CURVEFS_ERROR retcode = CURVEFS_ERROR::FAILED;
        MetaStatusCode stcode = response.statuscode();
        MetaServerStatusCode2CurveFSErr(stcode, &retcode);
        LOG_IF(WARNING, retcode != CURVEFS_ERROR::OK)
            << "CreateInode:  param = "
            << ", errcode = " << retcode
            << ", errmsg = " << MetaStatusCode_Name(stcode);

        if (response.has_inode()) {
            *out = response.inode();
        }
        // TDOD(lixiaocui): exception handling
        return retcode;
    };
    return excutor_.DoRPCTask(task);
}

CURVEFS_ERROR MetaServerClientImpl::DeleteInode(uint32_t fsId,
                                                uint64_t inodeid) {
    auto task = RPCTaskDefine {
        DeleteInodeResponse response;
        basecli_->DeleteInode(fsId, inodeid, &response, cntl, channel);

        if (cntl->Failed()) {
            LOG(WARNING) << "DeleteInode Failed, errorcode = "
                         << cntl->ErrorCode()
                         << ", error content:" << cntl->ErrorText()
                         << ", log id = " << cntl->log_id();
            return static_cast<CURVEFS_ERROR>(-cntl->ErrorCode());
        }

        CURVEFS_ERROR retcode = CURVEFS_ERROR::FAILED;
        MetaStatusCode stcode = response.statuscode();
        MetaServerStatusCode2CurveFSErr(stcode, &retcode);
        LOG_IF(WARNING, retcode != CURVEFS_ERROR::OK)
            << "DeleteInode:  fsid = " << fsId << ", inodeid = " << inodeid
            << ", errcode = " << retcode
            << ", errmsg = " << MetaStatusCode_Name(stcode);
        // TDOD(lixiaocui): exception handling
        return retcode;
    };
    return excutor_.DoRPCTask(task);
}

void MetaServerClientImpl::MetaServerStatusCode2CurveFSErr(
    const MetaStatusCode &statcode, CURVEFS_ERROR *errcode) {
    switch (statcode) {
    case MetaStatusCode::OK:
        *errcode = CURVEFS_ERROR::OK;
        break;
    case MetaStatusCode::PARAM_ERROR:
        *errcode = CURVEFS_ERROR::INVALIDPARAM;
        break;
    case MetaStatusCode::NOT_FOUND:
        *errcode = CURVEFS_ERROR::NOTEXIST;
        break;
    default:
        *errcode = CURVEFS_ERROR::UNKNOWN;
        break;
    }
}

}  // namespace client
}  // namespace curvefs
