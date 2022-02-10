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

#include "curvefs/src/client/rpcclient/metaserver_client.h"

#include <algorithm>
#include <vector>

using curvefs::metaserver::GetOrModifyS3ChunkInfoRequest;
using curvefs::metaserver::GetOrModifyS3ChunkInfoResponse;

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
using GetOrModifyS3ChunkInfoExcutor = TaskExecutor;

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
        brpc::Controller * cntl, TaskExecutorDone *taskExecutorDone) -> int

#define AsyncRPCTask                                                           \
    [=](LogicPoolID poolID, CopysetID copysetID, PartitionID partitionID,      \
        uint64_t txId, uint64_t applyIndex, brpc::Channel * channel,           \
        brpc::Controller * cntl, TaskExecutorDone *taskExecutorDone) -> int

class MetaServerClientRpcDoneBase : public google::protobuf::Closure {
 public:
    MetaServerClientRpcDoneBase(TaskExecutorDone *done,
        const std::shared_ptr<MetaServerClientMetric> &metaserverClientMetric):
        done_(done),
        metaserverClientMetric_(metaserverClientMetric) {}

    virtual ~MetaServerClientRpcDoneBase() {}

 protected:
    TaskExecutorDone *done_;
    std::shared_ptr<MetaServerClientMetric> metaserverClientMetric_;
};

MetaStatusCode MetaServerClientImpl::GetTxId(uint32_t fsId, uint64_t inodeId,
                                             uint32_t *partitionId,
                                             uint64_t *txId) {
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
        metaserverClientMetric_->getDentry.qps.count << 1;
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
            metaserverClientMetric_->getDentry.eps.count << 1;
            LOG(WARNING) << "GetDentry Failed, errorcode = "
                         << cntl->ErrorCode()
                         << ", error content:" << cntl->ErrorText()
                         << ", log id = " << cntl->log_id();
            return -cntl->ErrorCode();
        }
        MetaStatusCode ret = response.statuscode();

        if (ret != MetaStatusCode::OK) {
            LOG_IF(WARNING, ret != MetaStatusCode::NOT_FOUND)
                << "GetDentry: fsId = " << fsId << ", inodeid = " << inodeid
                << ", name = " << name << ", errcode = " << ret
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

        VLOG(6) << "GetDentry success, request: " << request.DebugString()
                << "response: " << response.ShortDebugString();
        return ret;
    };

    auto taskCtx = std::make_shared<TaskContext>(MetaServerOpType::GetDentry,
                                                 task, fsId, inodeid);
    GetDentryExcutor excutor(opt_, metaCache_, channelManager_, taskCtx);
    return ConvertToMetaStatusCode(excutor.DoRPCTask());
}

MetaStatusCode MetaServerClientImpl::ListDentry(uint32_t fsId, uint64_t inodeid,
                                                const std::string &last,
                                                uint32_t count,
                                                std::list<Dentry> *dentryList) {
    auto task = RPCTask {
        metaserverClientMetric_->listDentry.qps.count << 1;

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
            metaserverClientMetric_->listDentry.eps.count << 1;
            LOG(WARNING) << "ListDentry Failed, errorcode = "
                         << cntl->ErrorCode()
                         << ", error content:" << cntl->ErrorText()
                         << ", log id = " << cntl->log_id();
            return -cntl->ErrorCode();
        }

        MetaStatusCode ret = response.statuscode();
        if (ret != MetaStatusCode::OK) {
            LOG_IF(WARNING, ret != MetaStatusCode::NOT_FOUND)
                << "ListDentry: fsId = " << fsId << ", inodeid = " << inodeid
                << ", last = " << last << ", count = " << count
                << ", errcode = " << ret
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

        VLOG(6) << "ListDentry success, request: " << request.DebugString()
                << "response: " << response.DebugString();
        return ret;
    };

    auto taskCtx = std::make_shared<TaskContext>(MetaServerOpType::ListDentry,
                                                 task, fsId, inodeid);
    ListDentryExcutor excutor(opt_, metaCache_, channelManager_, taskCtx);
    return ConvertToMetaStatusCode(excutor.DoRPCTask());
}

MetaStatusCode MetaServerClientImpl::CreateDentry(const Dentry &dentry) {
    auto task = RPCTask {
        metaserverClientMetric_->createDentry.qps.count << 1;
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

        std::ostringstream oss;
        channel->Describe(oss, {});

        VLOG(6) << "CreateDentry " << request.ShortDebugString() << " to "
                << oss.str();

        if (cntl->Failed()) {
            metaserverClientMetric_->createDentry.eps.count << 1;
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

        VLOG(6) << "CreateDentry "
                << (ret == MetaStatusCode::OK ? "success" : "failure")
                << ", request: " << request.DebugString()
                << "response: " << response.DebugString();
        return ret;
    };

    auto taskCtx = std::make_shared<TaskContext>(
        MetaServerOpType::CreateDentry, task, dentry.fsid(),
        // TODO(@lixiaocui): may be taskContext need diffrent according to
        // different operatrion
        dentry.parentinodeid());
    CreateDentryExcutor excutor(opt_, metaCache_, channelManager_, taskCtx);
    return ConvertToMetaStatusCode(excutor.DoRPCTask());
}

MetaStatusCode MetaServerClientImpl::DeleteDentry(uint32_t fsId,
                                                  uint64_t inodeid,
                                                  const std::string &name) {
    auto task = RPCTask {
        metaserverClientMetric_->deleteDentry.qps.count << 1;
        DeleteDentryResponse response;
        DeleteDentryRequest request;
        request.set_poolid(poolID);
        request.set_copysetid(copysetID);
        request.set_partitionid(partitionID);
        request.set_fsid(fsId);
        request.set_parentinodeid(inodeid);
        request.set_name(name);
        request.set_txid(txId);

        curvefs::metaserver::MetaServerService_Stub stub(channel);
        stub.DeleteDentry(cntl, &request, &response, nullptr);

        if (cntl->Failed()) {
            metaserverClientMetric_->deleteDentry.eps.count << 1;
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

        VLOG(6) << "DeleteDentry success, request: " << request.DebugString()
                << "response: " << response.DebugString();
        return ret;
    };

    auto taskCtx = std::make_shared<TaskContext>(MetaServerOpType::DeleteDentry,
                                                 task, fsId, inodeid);
    DeleteDentryExcutor excutor(opt_, metaCache_, channelManager_, taskCtx);
    return ConvertToMetaStatusCode(excutor.DoRPCTask());
}

MetaStatusCode
MetaServerClientImpl::PrepareRenameTx(const std::vector<Dentry> &dentrys) {
    auto task = RPCTask {
        metaserverClientMetric_->prepareRenameTx.qps.count << 1;

        PrepareRenameTxRequest request;
        PrepareRenameTxResponse response;
        request.set_poolid(poolID);
        request.set_copysetid(copysetID);
        request.set_partitionid(partitionID);
        *request.mutable_dentrys() = {dentrys.begin(), dentrys.end()};

        curvefs::metaserver::MetaServerService_Stub stub(channel);
        stub.PrepareRenameTx(cntl, &request, &response, nullptr);

        if (cntl->Failed()) {
            metaserverClientMetric_->prepareRenameTx.eps.count << 1;
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

        VLOG(6) << "PrepareRenameTx success, request: " << request.DebugString()
                << "response: " << response.DebugString();
        return rc;
    };

    auto fsId = dentrys[0].fsid();
    auto inodeId = dentrys[0].parentinodeid();
    auto taskCtx = std::make_shared<TaskContext>(
        MetaServerOpType::PrepareRenameTx, task, fsId, inodeId);
    PrepareRenameTxExcutor excutor(opt_, metaCache_, channelManager_, taskCtx);
    return ConvertToMetaStatusCode(excutor.DoRPCTask());
}

MetaStatusCode MetaServerClientImpl::GetInode(uint32_t fsId, uint64_t inodeid,
                                              Inode *out) {
    auto task = RPCTask {
        metaserverClientMetric_->getInode.qps.count << 1;
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
            metaserverClientMetric_->getInode.eps.count << 1;
            LOG(WARNING) << "GetInode Failed, errorcode = " << cntl->ErrorCode()
                         << ", error content:" << cntl->ErrorText()
                         << ", log id = " << cntl->log_id();
            return -cntl->ErrorCode();
        }

        MetaStatusCode ret = response.statuscode();
        if (ret != MetaStatusCode::OK) {
            LOG_IF(WARNING, ret != MetaStatusCode::NOT_FOUND)
                << "GetInode: inodeid:" << inodeid << ", errcode = " << ret
                << ", errmsg = " << MetaStatusCode_Name(ret);
        } else if (response.has_inode() && response.has_appliedindex()) {
            out->CopyFrom(response.inode());

            metaCache_->UpdateApplyIndex(CopysetGroupID(poolID, copysetID),
                                         response.appliedindex());
        } else {
            LOG(WARNING) << "GetInode: inodeid:" << inodeid
                         << " ok, but applyIndex or inode not set in response: "
                         << response.DebugString();
            return -1;
        }

        auto &s3chunkinfoMap = response.inode().s3chunkinfomap();
        for (auto &item : s3chunkinfoMap) {
            VLOG(9) << "inodeInfo, inodeId:" << inodeid
                    << ",s3chunkinfo item key:" << item.first
                    << ", value:" << item.second.DebugString();
        }
        return ret;
    };

    auto taskCtx = std::make_shared<TaskContext>(MetaServerOpType::GetInode,
                                                 task, fsId, inodeid);
    GetInodeExcutor excutor(opt_, metaCache_, channelManager_, taskCtx);
    return ConvertToMetaStatusCode(excutor.DoRPCTask());
}

MetaStatusCode MetaServerClientImpl::UpdateInode(const Inode &inode) {
    auto task = RPCTask {
        metaserverClientMetric_->updateInode.qps.count << 1;
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
            metaserverClientMetric_->updateInode.eps.count << 1;
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

        VLOG(6) << "UpdateInode success, request: " << request.DebugString()
                << "response: " << response.DebugString();
        return ret;
    };

    auto taskCtx = std::make_shared<TaskContext>(
        MetaServerOpType::UpdateInode, task, inode.fsid(), inode.inodeid());
    UpdateInodeExcutor excutor(opt_, metaCache_, channelManager_, taskCtx);
    return ConvertToMetaStatusCode(excutor.DoRPCTask());
}

class UpdateInodeRpcDone : public MetaServerClientRpcDoneBase {
 public:
    UpdateInodeRpcDone(TaskExecutorDone *done,
        const std::shared_ptr<MetaServerClientMetric> &metaserverClientMetric):
        MetaServerClientRpcDoneBase(done, metaserverClientMetric) {}
    virtual ~UpdateInodeRpcDone() {}
    void Run() override;
    UpdateInodeResponse response;
};

void UpdateInodeRpcDone::Run() {
    std::unique_ptr<UpdateInodeRpcDone> self_guard(this);
    brpc::ClosureGuard done_guard(done_);
    auto taskCtx = done_->GetTaskExcutor()->GetTaskCxt();
    auto& cntl = taskCtx->cntl_;
    auto metaCache = done_->GetTaskExcutor()->GetMetaCache();
    if (cntl.Failed()) {
        metaserverClientMetric_->updateInode.eps.count << 1;
        LOG(WARNING) << "UpdateInode Failed, errorcode = "
                     << cntl.ErrorCode()
                     << ", error content: " << cntl.ErrorText()
                     << ", log id: " << cntl.log_id();
         done_->SetRetCode(-cntl.ErrorCode());
        return;
    }

    MetaStatusCode ret = response.statuscode();
    if (ret != MetaStatusCode::OK) {
        LOG(WARNING) << "UpdateInode:  inodeid = "
                     << taskCtx->inodeID
                     << ", errcode = " << ret
                     << ", errmsg = " << MetaStatusCode_Name(ret);
    } else if (response.has_appliedindex()) {
        metaCache->UpdateApplyIndex(taskCtx->target.groupID,
                                     response.appliedindex());
    } else {
        LOG(WARNING) << "UpdateInode:  inodeid = "
                     << taskCtx->inodeID
                     << "ok, but applyIndex not set in response:"
                     << response.DebugString();
        done_->SetRetCode(-1);
        return;
    }

    VLOG(6) << "UpdateInode success, "
            << "response: " << response.DebugString();
    done_->SetRetCode(ret);
    return;
}

void MetaServerClientImpl::UpdateInodeAsync(const Inode &inode,
    MetaServerClientDone *done) {
    auto task = AsyncRPCTask {
        metaserverClientMetric_->updateInode.qps.count << 1;

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

        auto *rpcDone = new UpdateInodeRpcDone(taskExecutorDone,
            metaserverClientMetric_);

        curvefs::metaserver::MetaServerService_Stub stub(channel);
        stub.UpdateInode(cntl, &request, &rpcDone->response, rpcDone);
        return MetaStatusCode::OK;
    };

    auto taskCtx = std::make_shared<TaskContext>(
        MetaServerOpType::UpdateInode, task, inode.fsid(), inode.inodeid());
    auto excutor = std::make_shared<UpdateInodeExcutor>(opt_,
        metaCache_, channelManager_, taskCtx);
    TaskExecutorDone *taskDone = new TaskExecutorDone(
        excutor, done);
    brpc::ClosureGuard taskDone_guard(taskDone);
    int ret = excutor->DoAsyncRPCTask(taskDone);
    if (ret < 0) {
        taskDone->SetRetCode(ret);
        return;
    }
    taskDone_guard.release();
}

MetaStatusCode MetaServerClientImpl::GetOrModifyS3ChunkInfo(
    uint32_t fsId, uint64_t inodeId,
    const google::protobuf::Map<
        uint64_t, S3ChunkInfoList> &s3ChunkInfos,
    bool returnS3ChunkInfoMap,
    google::protobuf::Map<
            uint64_t, S3ChunkInfoList> *out) {
    auto task = RPCTask {
        metaserverClientMetric_->appendS3ChunkInfo.qps.count << 1;

        GetOrModifyS3ChunkInfoRequest request;
        GetOrModifyS3ChunkInfoResponse response;
        request.set_poolid(poolID);
        request.set_copysetid(copysetID);
        request.set_partitionid(partitionID);
        request.set_fsid(fsId);
        request.set_inodeid(inodeId);
        request.set_returns3chunkinfomap(returnS3ChunkInfoMap);
        *(request.mutable_s3chunkinfoadd()) = s3ChunkInfos;

        curvefs::metaserver::MetaServerService_Stub stub(channel);
        stub.GetOrModifyS3ChunkInfo(cntl, &request, &response, nullptr);

        if (cntl->Failed()) {
            metaserverClientMetric_->appendS3ChunkInfo.eps.count << 1;
            LOG(WARNING) << "GetOrModifyS3ChunkInfo Failed, errorcode: "
                         << cntl->ErrorCode()
                         << ", error content: " << cntl->ErrorText()
                         << ", log id: " << cntl->log_id();
            return -cntl->ErrorCode();
        }

        MetaStatusCode ret = response.statuscode();
        if (ret != MetaStatusCode::OK) {
            LOG(WARNING) << "GetOrModifyS3ChunkInfo, inodeId: " << inodeId
                         << ", fsId: " << fsId
                         << ", errorcode: " << ret
                         << ", errmsg: " << MetaStatusCode_Name(ret);
            return ret;
        } else if (response.has_appliedindex()) {
            metaCache_->UpdateApplyIndex(CopysetGroupID(poolID, copysetID),
                                         response.appliedindex());
            if (returnS3ChunkInfoMap) {
                CHECK(out != nullptr) << "out ptr should be set.";
                out->swap(*response.mutable_s3chunkinfomap());
            }
        } else {
            LOG(WARNING) << "GetOrModifyS3ChunkInfo,  inodeId: " << inodeId
                         << ", fsId: " << fsId
                         << "ok, but applyIndex or inode not set in response: "
                         << response.DebugString();
            return -1;
        }
        VLOG(6) << "GetOrModifyS3ChunkInfo success, request: "
                << request.DebugString()
                << "response: " << response.DebugString();
        return ret;
    };

    auto taskCtx = std::make_shared<TaskContext>(
        MetaServerOpType::GetOrModifyS3ChunkInfo, task, fsId, inodeId);
    GetOrModifyS3ChunkInfoExcutor excutor(
        opt_, metaCache_, channelManager_, taskCtx);
    return ConvertToMetaStatusCode(excutor.DoRPCTask());
}

class GetOrModifyS3ChunkInfoRpcDone : public MetaServerClientRpcDoneBase {
 public:
    GetOrModifyS3ChunkInfoRpcDone(TaskExecutorDone *done,
        const std::shared_ptr<MetaServerClientMetric> &metaserverClientMetric):
        MetaServerClientRpcDoneBase(done, metaserverClientMetric) {}

    virtual ~GetOrModifyS3ChunkInfoRpcDone() {}
    void Run() override;
    GetOrModifyS3ChunkInfoResponse response;
};

void GetOrModifyS3ChunkInfoRpcDone::Run() {
    std::unique_ptr<GetOrModifyS3ChunkInfoRpcDone> self_guard(this);
    brpc::ClosureGuard done_guard(done_);
    auto taskCtx = done_->GetTaskExcutor()->GetTaskCxt();
    auto& cntl = taskCtx->cntl_;
    auto metaCache = done_->GetTaskExcutor()->GetMetaCache();
    if (cntl.Failed()) {
        metaserverClientMetric_->appendS3ChunkInfo.eps.count << 1;
        LOG(WARNING) << "GetOrModifyS3ChunkInfo Failed, errorcode: "
                     << cntl.ErrorCode()
                     << ", error content: " << cntl.ErrorText()
                     << ", log id: " << cntl.log_id();
        done_->SetRetCode(-cntl.ErrorCode());
        return;
    }

    MetaStatusCode ret = response.statuscode();
    if (ret != MetaStatusCode::OK) {
        LOG(WARNING) << "GetOrModifyS3ChunkInfo, inodeId: "
                     << taskCtx->inodeID
                     << ", fsId: " << taskCtx->fsID
                     << ", errorcode: " << ret
                     << ", errmsg: " << MetaStatusCode_Name(ret);
        done_->SetRetCode(ret);
        return;
    } else if (response.has_appliedindex()) {
        metaCache->UpdateApplyIndex(taskCtx->target.groupID,
                                     response.appliedindex());
    } else {
        LOG(WARNING) << "GetOrModifyS3ChunkInfo,  inodeId: "
                     << taskCtx->inodeID
                     << ", fsId: "
                     << taskCtx->fsID
                     << "ok, but applyIndex or inode not set in response: "
                     << response.DebugString();
        done_->SetRetCode(-1);
        return;
    }
    VLOG(6) << "GetOrModifyS3ChunkInfo success, "
            << "response: " << response.DebugString();
    done_->SetRetCode(ret);
    return;
}

void MetaServerClientImpl::GetOrModifyS3ChunkInfoAsync(
    uint32_t fsId, uint64_t inodeId,
    const google::protobuf::Map<
        uint64_t, S3ChunkInfoList> &s3ChunkInfos,
    MetaServerClientDone *done) {
    auto task = AsyncRPCTask {
        metaserverClientMetric_->appendS3ChunkInfo.qps.count << 1;

        GetOrModifyS3ChunkInfoRequest request;
        request.set_poolid(poolID);
        request.set_copysetid(copysetID);
        request.set_partitionid(partitionID);
        request.set_fsid(fsId);
        request.set_inodeid(inodeId);
        request.set_returns3chunkinfomap(false);
        *(request.mutable_s3chunkinfoadd()) = s3ChunkInfos;

        auto *rpcDone = new GetOrModifyS3ChunkInfoRpcDone(taskExecutorDone,
            metaserverClientMetric_);

        curvefs::metaserver::MetaServerService_Stub stub(channel);
        stub.GetOrModifyS3ChunkInfo(
            cntl, &request, &rpcDone->response, rpcDone);
        return MetaStatusCode::OK;
    };

    auto taskCtx = std::make_shared<TaskContext>(
        MetaServerOpType::GetOrModifyS3ChunkInfo, task, fsId, inodeId);
    auto excutor = std::make_shared<GetOrModifyS3ChunkInfoExcutor>(opt_,
        metaCache_, channelManager_, taskCtx);
    TaskExecutorDone *taskDone = new TaskExecutorDone(
        excutor, done);
    brpc::ClosureGuard taskDone_guard(taskDone);
    int ret = excutor->DoAsyncRPCTask(taskDone);
    if (ret < 0) {
        taskDone->SetRetCode(ret);
        return;
    }
    taskDone_guard.release();
}

MetaStatusCode MetaServerClientImpl::CreateInode(const InodeParam &param,
                                                 Inode *out) {
    auto task = RPCTask {
        metaserverClientMetric_->createInode.qps.count << 1;
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
        request.set_rdev(param.rdev);
        request.set_symlink(param.symlink);
        curvefs::metaserver::MetaServerService_Stub stub(channel);
        stub.CreateInode(cntl, &request, &response, nullptr);

        if (cntl->Failed()) {
            metaserverClientMetric_->createInode.eps.count << 1;
            LOG(WARNING) << "CreateInode Failed, errorcode = "
                         << cntl->ErrorCode()
                         << ", error content:" << cntl->ErrorText()
                         << ", log id = " << cntl->log_id();
            return -cntl->ErrorCode();
        }

        MetaStatusCode ret = response.statuscode();
        if (ret != MetaStatusCode::OK) {
            LOG(WARNING) << "CreateInode:  param = " << param
                         << ", errcode = " << ret
                         << ", errmsg = " << MetaStatusCode_Name(ret)
                         << ", remote side = "
                         << butil::endpoint2str(cntl->remote_side()).c_str()
                         << ", request: " << request.ShortDebugString()
                         << ", pool: " << poolID << ", copyset: " << copysetID
                         << ", partition: " << partitionID;
        } else if (response.has_inode() && response.has_appliedindex()) {
            *out = response.inode();

            metaCache_->UpdateApplyIndex(CopysetGroupID(poolID, copysetID),
                                         response.appliedindex());
        } else {
            LOG(WARNING) << "CreateInode:  param = " << param
                         << " ok, but applyIndex or inode not set in response:"
                         << response.DebugString();
            return -1;
        }

        VLOG(6) << "CreateInode success, request: " << request.DebugString()
                << "response: " << response.DebugString();
        return ret;
    };

    auto taskCtx = std::make_shared<TaskContext>(MetaServerOpType::CreateInode,
                                                 task, param.fsId, 0);
    CreateInodeExcutor excutor(opt_, metaCache_, channelManager_, taskCtx);
    return ConvertToMetaStatusCode(excutor.DoRPCTask());
}

MetaStatusCode MetaServerClientImpl::DeleteInode(uint32_t fsId,
                                                 uint64_t inodeid) {
    auto task = RPCTask {
        metaserverClientMetric_->deleteInode.qps.count << 1;
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
            metaserverClientMetric_->deleteInode.eps.count << 1;
            LOG(WARNING) << "DeleteInode Failed, errorcode = "
                         << cntl->ErrorCode()
                         << ", error content:" << cntl->ErrorText()
                         << ", log id = " << cntl->log_id();
            return -cntl->ErrorCode();
        }

        MetaStatusCode ret = response.statuscode();
        if (ret != MetaStatusCode::OK) {
            LOG(WARNING) << "DeleteInode:  fsid = " << fsId
                         << ", inodeid = " << inodeid << ", errcode = " << ret
                         << ", errmsg = " << MetaStatusCode_Name(ret);
        } else if (response.has_appliedindex()) {
            metaCache_->UpdateApplyIndex(CopysetGroupID(poolID, copysetID),
                                         response.appliedindex());
        } else {
            LOG(WARNING) << "DeleteInode:  fsid = " << fsId
                         << ", inodeid = " << inodeid
                         << " ok, but applyIndex not set in response:"
                         << response.DebugString();
            return -1;
        }

        VLOG(6) << "DeleteInode success, request: " << request.DebugString()
                << "response: " << response.DebugString();
        return ret;
    };

    auto taskCtx = std::make_shared<TaskContext>(MetaServerOpType::DeleteInode,
                                                 task, fsId, inodeid);
    DeleteInodeExcutor excutor(opt_, metaCache_, channelManager_, taskCtx);
    return ConvertToMetaStatusCode(excutor.DoRPCTask());
}

}  // namespace rpcclient
}  // namespace client
}  // namespace curvefs
