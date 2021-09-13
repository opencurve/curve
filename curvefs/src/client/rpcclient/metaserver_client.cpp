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
 * Created Date: Mon Sept 1 2021
 * Author: lixiaocui
 */

#include <vector>
#include <algorithm>
#include "curvefs/src/client/rpcclient/metaserver_client.h"

namespace curvefs {
namespace client {
namespace rpcclient {
using CreateDentryExcutor = TaskExecutor;
using GetDentryExcutor = TaskExecutor;
using ListDentryExcutor = TaskExecutor;
using DeleteDentryExcutor = TaskExecutor;
using PrepareRenameTxExcutor = TaskExecutor;
using DeleteInodeExcutor = TaskExecutor;
using UpdateInodeExcutor = TaskExecutor;
using GetInodeExcutor = TaskExecutor;

MetaStatusCode MetaServerClientImpl::Init(
    const ExcutorOpt &excutorOpt, std::shared_ptr<MetaCache> metaCache,
    std::shared_ptr<ChannelManager<MetaserverID>> channelManager) {
    opt_ = excutorOpt;
    metaCache_ = metaCache;
    channelManager_ = channelManager;
    return MetaStatusCode::OK;
}

#define RPCTask                                                                \
    [&](LogicPoolID poolID, CopysetID copysetID, PartitionID partitionID,      \
        uint64_t txId, uint64_t applyIndex, brpc::Channel * channel,           \
        brpc::Controller * cntl) -> int

MetaStatusCode MetaServerClientImpl::GetTxId(uint32_t fsId,
                                             uint64_t inodeId,
                                             uint32_t* partitionId,
                                             uint64_t* txId) {
    if (!metaCache_->GetTxId(fsId, inodeId, partitionId, txId)) {
        return MetaStatusCode::NOT_FOUND;
    }
    return MetaStatusCode::OK;
}

void MetaServerClientImpl::SetTxId(uint32_t partitionId, uint64_t txId) {
    metaCache_->SetTxId(partitionId, txId);
}

MetaStatusCode MetaServerClientImpl::GetDentry(uint32_t fsId, uint64_t inodeid,
                                               const std::string &name,
                                               Dentry *out) {
    auto task = RPCTask {
        GetDentryResponse response;
        GetDentryRequest request;
        request.set_poolid(poolID);
        request.set_copysetid(copysetID);
        request.set_partitionid(partitionID);
        request.set_fsid(fsId);
        request.set_parentinodeid(inodeid);
        request.set_name(name);
        request.set_txid(txId);

        curvefs::metaserver::MetaServerService_Stub stub(channel);
        stub.GetDentry(cntl, &request, &response, nullptr);

        if (cntl->Failed()) {
            LOG(WARNING) << "GetDentry Failed, errorcode = "
                         << cntl->ErrorCode()
                         << ", error content:" << cntl->ErrorText()
                         << ", log id = " << cntl->log_id();
            return -cntl->ErrorCode();
        }
        MetaStatusCode ret = response.statuscode();

        if (ret != MetaStatusCode::OK) {
            LOG(WARNING) << "GetDentry: fsId = " << fsId
                         << ", inodeid = " << inodeid << ", name = " << name
                         << ", errcode = " << ret
                         << ", errmsg = " << MetaStatusCode_Name(ret);
        } else if (response.has_dentry() && response.has_appliedindex()) {
            *out = response.dentry();

            metaCache_->UpdateApplyIndex(CopysetGroupID(poolID, copysetID),
                                         response.appliedindex());
        } else {
            LOG(WARNING) << "GetDentry: fsId = " << fsId
                         << ", inodeid = " << inodeid << ", name = " << name
                         << " ok, but dentry or applyIndex not set in response:"
                         << response.DebugString();
            return -1;
        }
        return ret;
    };

    auto taskCtx = std::make_shared<TaskContext>(MetaServerOpType::GetDentry,
                                                 task, fsId, inodeid);
    GetDentryExcutor excutor(opt_, metaCache_, channelManager_);
    return ReturnError(excutor.DoRPCTask(taskCtx));
}

MetaStatusCode MetaServerClientImpl::ListDentry(uint32_t fsId, uint64_t inodeid,
                                                const std::string &last,
                                                uint32_t count,
                                                std::list<Dentry> *dentryList) {
    auto task = RPCTask {
        ListDentryRequest request;
        ListDentryResponse response;
        request.set_poolid(poolID);
        request.set_copysetid(copysetID);
        request.set_partitionid(partitionID);
        request.set_fsid(fsId);
        request.set_dirinodeid(inodeid);
        request.set_txid(txId);
        request.set_last(last);
        request.set_count(count);

        curvefs::metaserver::MetaServerService_Stub stub(channel);
        stub.ListDentry(cntl, &request, &response, nullptr);

        if (cntl->Failed()) {
            LOG(WARNING) << "ListDentry Failed, errorcode = "
                         << cntl->ErrorCode()
                         << ", error content:" << cntl->ErrorText()
                         << ", log id = " << cntl->log_id();
            return -cntl->ErrorCode();
        }

        MetaStatusCode ret = response.statuscode();
        if (ret != MetaStatusCode::OK) {
            LOG(WARNING) << "ListDentry: fsId = " << fsId
                         << ", inodeid = " << inodeid << ", last = " << last
                         << ", count = " << count << ", errcode = " << ret
                         << ", errmsg = " << MetaStatusCode_Name(ret);
        } else if (response.has_appliedindex() && response.dentrys_size() > 0) {
            metaCache_->UpdateApplyIndex(CopysetGroupID(poolID, copysetID),
                                         response.appliedindex());

            auto dentrys = response.dentrys();
            for_each(dentrys.begin(), dentrys.end(),
                     [&](Dentry &d) { dentryList->push_back(d); });
        } else {
            LOG(WARNING)
                << "ListDentry: fsId = " << fsId << ", inodeid = " << inodeid
                << ", last = " << last << ", count = " << count
                << " ok, but dentry and applyIndex not set in response:"
                << response.DebugString();
            return -1;
        }

        return ret;
    };

    auto taskCtx = std::make_shared<TaskContext>(MetaServerOpType::ListDentry,
                                                 task, fsId, inodeid);
    ListDentryExcutor excutor(opt_, metaCache_, channelManager_);
    return ReturnError(excutor.DoRPCTask(taskCtx));
}

MetaStatusCode MetaServerClientImpl::CreateDentry(const Dentry &dentry) {
    auto task = RPCTask {
        CreateDentryResponse response;
        CreateDentryRequest request;
        request.set_poolid(poolID);
        request.set_copysetid(copysetID);
        request.set_partitionid(partitionID);
        Dentry *d = new Dentry;
        d->set_fsid(dentry.fsid());
        d->set_inodeid(dentry.inodeid());
        d->set_parentinodeid(dentry.parentinodeid());
        d->set_name(dentry.name());
        d->set_txid(txId);
        request.set_allocated_dentry(d);
        curvefs::metaserver::MetaServerService_Stub stub(channel);
        stub.CreateDentry(cntl, &request, &response, nullptr);

        if (cntl->Failed()) {
            LOG(WARNING) << "CreateDentry Failed, errorcode = "
                         << cntl->ErrorCode()
                         << ", error content:" << cntl->ErrorText()
                         << ", log id = " << cntl->log_id();
            return -cntl->ErrorCode();
        }

        MetaStatusCode ret = response.statuscode();
        if (ret != MetaStatusCode::OK) {
            LOG(WARNING) << "CreateDentry:  dentry = " << dentry.DebugString()
                         << ", errcode = " << ret
                         << ", errmsg = " << MetaStatusCode_Name(ret);
        } else if (response.has_appliedindex()) {
            metaCache_->UpdateApplyIndex(CopysetGroupID(poolID, copysetID),
                                         response.appliedindex());
        } else {
            LOG(WARNING) << "CreateDentry:  dentry = " << dentry.DebugString()
                         << " ok, but applyIndex not set in response:"
                         << response.DebugString();
            return -1;
        }
        return ret;
    };

    auto taskCtx = std::make_shared<TaskContext>(
        MetaServerOpType::CreateDentry, task, dentry.fsid(), dentry.inodeid());
    CreateDentryExcutor excutor(opt_, metaCache_, channelManager_);
    return ReturnError(excutor.DoRPCTask(taskCtx));
}

MetaStatusCode MetaServerClientImpl::DeleteDentry(uint32_t fsId,
                                                  uint64_t inodeid,
                                                  const std::string &name) {
    auto task = RPCTask {
        DeleteDentryResponse response;
        DeleteDentryRequest request;
        request.set_poolid(poolID);
        request.set_copysetid(copysetID);
        request.set_partitionid(partitionID);
        request.set_fsid(fsId);
        request.set_parentinodeid(inodeid);
        request.set_name(name);
        request.set_txid(txId);
        request.set_txid(1);

        curvefs::metaserver::MetaServerService_Stub stub(channel);
        stub.DeleteDentry(cntl, &request, &response, nullptr);

        if (cntl->Failed()) {
            LOG(WARNING) << "DeleteDentry Failed, errorcode = "
                         << cntl->ErrorCode()
                         << ", error content:" << cntl->ErrorText()
                         << ", log id = " << cntl->log_id();
            return -cntl->ErrorCode();
        }

        MetaStatusCode ret = response.statuscode();
        if (ret != MetaStatusCode::OK) {
            LOG(WARNING) << "DeleteDentry:  fsid = " << fsId
                         << ", inodeid = " << inodeid << ", name = " << name
                         << ", errcode = " << ret
                         << ", errmsg = " << MetaStatusCode_Name(ret);
        } else if (response.has_appliedindex()) {
            metaCache_->UpdateApplyIndex(CopysetGroupID(poolID, copysetID),
                                         response.appliedindex());
        } else {
            LOG(WARNING) << "DeleteDentry:  fsid = " << fsId
                         << ", inodeid = " << inodeid << ", name = " << name
                         << " ok, but applyIndex not set in response:"
                         << response.DebugString();
            return -1;
        }
        return ret;
    };

    auto taskCtx = std::make_shared<TaskContext>(MetaServerOpType::DeleteDentry,
                                                 task, fsId, inodeid);
    DeleteDentryExcutor excutor(opt_, metaCache_, channelManager_);
    return ReturnError(excutor.DoRPCTask(taskCtx));
}

MetaStatusCode MetaServerClientImpl::PrepareRenameTx(
    const std::vector<Dentry>& dentrys) {
    auto task = RPCTask {
        PrepareRenameTxRequest request;
        PrepareRenameTxResponse response;

        request.set_poolid(poolID);
        request.set_copysetid(copysetID);
        request.set_partitionid(partitionID);
        *request.mutable_dentrys() = { dentrys.begin(), dentrys.end() };

        curvefs::metaserver::MetaServerService_Stub stub(channel);
        stub.PrepareRenameTx(cntl, &request, &response, nullptr);

        if (cntl->Failed()) {
            LOG(WARNING) << "PrepareRenameTx failed"
                         << ", errorCode = " << cntl->ErrorCode()
                         << ", errorText = " << cntl->ErrorText()
                         << ", logId = " << cntl->log_id();
            return -cntl->ErrorCode();
        }

        auto rc = response.statuscode();
        if (rc != MetaStatusCode::OK) {
            LOG(WARNING) << "PrepareRenameTx: retCode = " << rc
                         << ", message = " << MetaStatusCode_Name(rc);
        } else if (response.has_appliedindex()) {
            metaCache_->UpdateApplyIndex(CopysetGroupID(poolID, copysetID),
                                         response.appliedindex());
        } else {
            LOG(WARNING) << "PrepareRenameTx OK"
                         << ", but applyIndex not set in response:"
                         << response.DebugString();
            return -1;
        }
        return rc;
    };

    auto fsId = dentrys[0].fsid();
    auto inodeId = dentrys[0].parentinodeid();
    auto taskCtx = std::make_shared<TaskContext>(
        MetaServerOpType::PrepareRenameTx, task, fsId, inodeId);
    PrepareRenameTxExcutor excutor(opt_, metaCache_, channelManager_);
    return ReturnError(excutor.DoRPCTask(taskCtx));
}

MetaStatusCode MetaServerClientImpl::GetInode(uint32_t fsId, uint64_t inodeid,
                                              Inode *out) {
    auto task = RPCTask {
        GetInodeRequest request;
        GetInodeResponse response;
        request.set_poolid(poolID);
        request.set_copysetid(copysetID);
        request.set_partitionid(partitionID);
        request.set_fsid(fsId);
        request.set_inodeid(inodeid);

        curvefs::metaserver::MetaServerService_Stub stub(channel);
        stub.GetInode(cntl, &request, &response, nullptr);

        if (cntl->Failed()) {
            LOG(WARNING) << "GetInode Failed, errorcode = " << cntl->ErrorCode()
                         << ", error content:" << cntl->ErrorText()
                         << ", log id = " << cntl->log_id();
            return -cntl->ErrorCode();
        }

        MetaStatusCode ret = response.statuscode();
        if (ret != MetaStatusCode::OK) {
            LOG(WARNING) << "GetInode:  inodeid = " << inodeid
                         << ", errcode = " << ret
                         << ", errmsg = " << MetaStatusCode_Name(ret);
        } else if (response.has_inode()) {
            out->CopyFrom(response.inode());
        } else if (response.has_appliedindex()) {
            metaCache_->UpdateApplyIndex(CopysetGroupID(poolID, copysetID),
                                         response.appliedindex());
        } else {
            LOG(WARNING) << "GetInode:  inodeid = " << inodeid
                         << " ok, but applyIndex not set in response: "
                         << response.DebugString();
            return -1;
        }
        return ret;
    };

    auto taskCtx = std::make_shared<TaskContext>(MetaServerOpType::GetInode,
                                                 task, fsId, inodeid);
    GetInodeExcutor excutor(opt_, metaCache_, channelManager_);
    return ReturnError(excutor.DoRPCTask(taskCtx));
}

MetaStatusCode MetaServerClientImpl::UpdateInode(const Inode &inode) {
    auto task = RPCTask {
        UpdateInodeResponse response;
        UpdateInodeRequest request;
        request.set_poolid(poolID);
        request.set_copysetid(copysetID);
        request.set_partitionid(partitionID);
        request.set_inodeid(inode.inodeid());
        request.set_fsid(inode.fsid());
        request.set_length(inode.length());
        request.set_ctime(inode.ctime());
        request.set_mtime(inode.mtime());
        request.set_atime(inode.atime());
        request.set_uid(inode.uid());
        request.set_gid(inode.gid());
        request.set_mode(inode.mode());
        request.set_nlink(inode.nlink());
        request.set_openflag(inode.openflag());
        if (inode.has_volumeextentlist()) {
            curvefs::metaserver::VolumeExtentList *vlist =
                new curvefs::metaserver::VolumeExtentList;
            vlist->CopyFrom(inode.volumeextentlist());
            request.set_allocated_volumeextentlist(vlist);
        }
        curvefs::metaserver::MetaServerService_Stub stub(channel);
        stub.UpdateInode(cntl, &request, &response, nullptr);

        if (cntl->Failed()) {
            LOG(WARNING) << "UpdateInode Failed, errorcode = "
                         << cntl->ErrorCode()
                         << ", error content:" << cntl->ErrorText()
                         << ", log id = " << cntl->log_id();
            return -cntl->ErrorCode();
        }

        MetaStatusCode ret = response.statuscode();
        if (ret != MetaStatusCode::OK) {
            LOG(WARNING) << "UpdateInode:  inodeid = " << inode.DebugString()
                         << ", errcode = " << ret
                         << ", errmsg = " << MetaStatusCode_Name(ret);
        } else if (response.has_appliedindex()) {
            metaCache_->UpdateApplyIndex(CopysetGroupID(poolID, copysetID),
                                         response.appliedindex());
        } else {
            LOG(WARNING) << "UpdateInode:  inodeid = " << inode.DebugString()
                         << "ok, but applyIndex not set in response:"
                         << response.DebugString();
            return -1;
        }
        return ret;
    };

    auto taskCtx = std::make_shared<TaskContext>(
        MetaServerOpType::UpdateInode, task, inode.fsid(), inode.inodeid());
    UpdateInodeExcutor excutor(opt_, metaCache_, channelManager_);
    return ReturnError(excutor.DoRPCTask(taskCtx));
}

MetaStatusCode MetaServerClientImpl::CreateInode(const InodeParam &param,
                                                 Inode *out) {
    auto task = RPCTask {
        CreateInodeResponse response;
        CreateInodeRequest request;
        request.set_poolid(poolID);
        request.set_copysetid(copysetID);
        request.set_partitionid(partitionID);
        request.set_fsid(param.fsId);
        request.set_length(param.length);
        request.set_uid(param.uid);
        request.set_gid(param.gid);
        request.set_mode(param.mode);
        request.set_type(param.type);
        request.set_symlink(param.symlink);
        curvefs::metaserver::MetaServerService_Stub stub(channel);
        stub.CreateInode(cntl, &request, &response, nullptr);

        if (cntl->Failed()) {
            LOG(WARNING) << "CreateInode Failed, errorcode = "
                         << cntl->ErrorCode()
                         << ", error content:" << cntl->ErrorText()
                         << ", log id = " << cntl->log_id();
            return -cntl->ErrorCode();
        }

        MetaStatusCode ret = response.statuscode();
        if (ret != MetaStatusCode::OK) {
            LOG(WARNING) << "CreateInode:  param = "
                         << ", errcode = " << ret
                         << ", errmsg = " << MetaStatusCode_Name(ret);
        } else if (response.has_inode()) {
            *out = response.inode();
        } else if (response.has_appliedindex()) {
            metaCache_->UpdateApplyIndex(CopysetGroupID(poolID, copysetID),
                                         response.appliedindex());
        } else {
            LOG(WARNING) << "CreateInode:  param = "
                         << " ok, but applyIndex not set in response:"
                         << response.DebugString();
            return -1;
        }
        return ret;
    };

    auto taskCtx = std::make_shared<TaskContext>(MetaServerOpType::CreateInode,
                                                 task, param.fsId, 0);
    CreateInodeExcutor excutor(opt_, metaCache_, channelManager_);
    return ReturnError(excutor.DoRPCTask(taskCtx));
}

MetaStatusCode MetaServerClientImpl::DeleteInode(uint32_t fsId,
                                                 uint64_t inodeid) {
    auto task = RPCTask {
        DeleteInodeResponse response;
        DeleteInodeRequest request;
        request.set_poolid(poolID);
        request.set_copysetid(copysetID);
        request.set_partitionid(partitionID);
        request.set_fsid(fsId);
        request.set_inodeid(inodeid);
        curvefs::metaserver::MetaServerService_Stub stub(channel);
        stub.DeleteInode(cntl, &request, &response, nullptr);

        if (cntl->Failed()) {
            LOG(WARNING) << "DeleteInode Failed, errorcode = "
                         << cntl->ErrorCode()
                         << ", error content:" << cntl->ErrorText()
                         << ", log id = " << cntl->log_id();
            return -cntl->ErrorCode();
        }

        MetaStatusCode ret = response.statuscode();
        if (ret != MetaStatusCode::OK) {
            LOG(WARNING) << "DeteInode:  fsid = " << fsId
                         << ", inodeid = " << inodeid << ", errcode = " << ret
                         << ", errmsg = " << MetaStatusCode_Name(ret);
        } else if (response.has_appliedindex()) {
            metaCache_->UpdateApplyIndex(CopysetGroupID(poolID, copysetID),
                                         response.appliedindex());
        } else {
            LOG(WARNING) << "DeteInode:  fsid = " << fsId
                         << ", inodeid = " << inodeid
                         << " ok, but applyIndex not set in response:"
                         << response.DebugString();
            return -1;
        }
        return ret;
    };

    auto taskCtx = std::make_shared<TaskContext>(MetaServerOpType::DeleteInode,
                                                 task, fsId, inodeid);
    DeleteInodeExcutor excutor(opt_, metaCache_, channelManager_);
    return ReturnError(excutor.DoRPCTask(taskCtx));
}

MetaStatusCode MetaServerClientImpl::ReturnError(int retcode) {
    if (retcode < 0) {
        return MetaStatusCode::RPC_ERROR;
    }
    return static_cast<MetaStatusCode>(retcode);
}

}  // namespace rpcclient
}  // namespace client
}  // namespace curvefs
