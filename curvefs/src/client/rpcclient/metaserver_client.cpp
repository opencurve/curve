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
#include <brpc/closure_guard.h>
#include <butil/iobuf.h>
#include <glog/logging.h>
#include <time.h>

#include <cstddef>
#include <memory>
#include <vector>
#include <utility>
#include <algorithm>

#include "absl/cleanup/cleanup.h"
#include "curvefs/proto/metaserver.pb.h"
#include "curvefs/src/client/rpcclient/metacache.h"
#include "curvefs/src/client/rpcclient/task_excutor.h"
#include "src/common/string_util.h"
#include "curvefs/src/common/rpc_stream.h"
#include "curvefs/src/common/metric_utils.h"

using ::curve::common::StringToUl;
using ::curve::common::StringToUll;
using curvefs::metaserver::GetOrModifyS3ChunkInfoRequest;
using curvefs::metaserver::GetOrModifyS3ChunkInfoResponse;
using curvefs::metaserver::BatchGetInodeAttrRequest;
using curvefs::metaserver::BatchGetInodeAttrResponse;
using curvefs::metaserver::BatchGetXAttrRequest;
using curvefs::metaserver::BatchGetXAttrResponse;

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
using BatchGetInodeAttrExcutor = TaskExecutor;
using BatchGetXAttrExcutor = TaskExecutor;
using GetOrModifyS3ChunkInfoExcutor = TaskExecutor;
using UpdateVolumeExtentExecutor = TaskExecutor;
using GetVolumeExtentExecutor = TaskExecutor;

using ::curvefs::common::LatencyUpdater;
using ::curvefs::common::StreamOptions;
using ::curvefs::common::StreamConnection;
using ::curvefs::metaserver::S3ChunkInfo;
using ::curvefs::metaserver::MetaServerService_Stub;

MetaStatusCode MetaServerClientImpl::Init(
    const ExcutorOpt &excutorOpt, const ExcutorOpt &excutorInternalOpt,
    std::shared_ptr<MetaCache> metaCache,
    std::shared_ptr<ChannelManager<MetaserverID>> channelManager) {
    opt_ = excutorOpt;
    optInternal_ = excutorInternalOpt;
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
                                MetaServerClientMetric *metric)
        : done_(done), metric_(metric) {}

    ~MetaServerClientRpcDoneBase() override = default;

 protected:
    TaskExecutorDone *done_;
    MetaServerClientMetric *metric_;
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
        metric_.getDentry.qps.count << 1;
        LatencyUpdater updater(&metric_.getDentry.latency);
        GetDentryResponse response;
        GetDentryRequest request;
        request.set_poolid(poolID);
        request.set_copysetid(copysetID);
        request.set_partitionid(partitionID);
        request.set_fsid(fsId);
        request.set_parentinodeid(inodeid);
        request.set_name(name);
        request.set_txid(txId);
        request.set_appliedindex(applyIndex);

        curvefs::metaserver::MetaServerService_Stub stub(channel);
        stub.GetDentry(cntl, &request, &response, nullptr);

        if (cntl->Failed()) {
            metric_.getDentry.eps.count << 1;
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

        VLOG(6) << "GetDentry done, request: " << request.DebugString()
                << "response: " << response.ShortDebugString();
        return ret;
    };

    auto taskCtx = std::make_shared<TaskContext>(MetaServerOpType::GetDentry,
                                                 task, fsId, inodeid, false,
                                                 opt_.enableRenameParallel);
    GetDentryExcutor excutor(opt_, metaCache_, channelManager_,
                             std::move(taskCtx));
    return ConvertToMetaStatusCode(excutor.DoRPCTask());
}

MetaStatusCode MetaServerClientImpl::ListDentry(uint32_t fsId, uint64_t inodeid,
                                                const std::string &last,
                                                uint32_t count,
                                                bool onlyDir,
                                                std::list<Dentry> *dentryList) {
    auto task = RPCTask {
        metric_.listDentry.qps.count << 1;
        LatencyUpdater updater(&metric_.listDentry.latency);
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
        request.set_onlydir(onlyDir);
        request.set_appliedindex(applyIndex);

        curvefs::metaserver::MetaServerService_Stub stub(channel);
        stub.ListDentry(cntl, &request, &response, nullptr);

        if (cntl->Failed()) {
            metric_.listDentry.eps.count << 1;
            LOG(WARNING) << "ListDentry Failed, errorcode = "
                         << cntl->ErrorCode()
                         << ", error content:" << cntl->ErrorText()
                         << ", log id = " << cntl->log_id();
            return -cntl->ErrorCode();
        }

        MetaStatusCode ret = response.statuscode();
        if (ret != MetaStatusCode::OK) {
            LOG(WARNING) << "ListDentry: fsId = " << fsId
                         << ", inodeid = " << inodeid
                         << ", last = " << last << ", count = " << count
                         << ", onlyDir = " << onlyDir
                         << ", errcode = " << ret
                         << ", errmsg = " << MetaStatusCode_Name(ret);
        } else if (response.has_appliedindex()) {
            metaCache_->UpdateApplyIndex(CopysetGroupID(poolID, copysetID),
                                         response.appliedindex());

            auto dentrys = response.dentrys();
            for_each(dentrys.begin(), dentrys.end(),
                     [&](Dentry &d) { dentryList->push_back(d); });
        } else {
            LOG(WARNING)
                << "ListDentry: fsId = " << fsId << ", inodeid = " << inodeid
                << ", last = " << last << ", count = " << count
                << ", onlyDir = " << onlyDir
                << " ok, but applyIndex not set in response:"
                << response.DebugString();
            return -1;
        }

        VLOG(6) << "ListDentry done, request: " << request.DebugString()
                << "response: " << response.DebugString();
        return ret;
    };

    auto taskCtx = std::make_shared<TaskContext>(MetaServerOpType::ListDentry,
                                                 task, fsId, inodeid, false,
                                                 opt_.enableRenameParallel);
    ListDentryExcutor excutor(opt_, metaCache_, channelManager_,
                              std::move(taskCtx));
    return ConvertToMetaStatusCode(excutor.DoRPCTask());
}

MetaStatusCode MetaServerClientImpl::CreateDentry(const Dentry &dentry) {
    auto task = RPCTask {
        metric_.createDentry.qps.count << 1;
        LatencyUpdater updater(&metric_.createDentry.latency);
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
        d->set_type(dentry.type());
        request.set_allocated_dentry(d);
        curvefs::metaserver::MetaServerService_Stub stub(channel);
        stub.CreateDentry(cntl, &request, &response, nullptr);

        std::ostringstream oss;
        channel->Describe(oss, {});

        VLOG(6) << "CreateDentry " << request.ShortDebugString() << " to "
                << oss.str();

        if (cntl->Failed()) {
            metric_.createDentry.eps.count << 1;
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
        dentry.parentinodeid(), false, opt_.enableRenameParallel);
    CreateDentryExcutor excutor(opt_, metaCache_, channelManager_,
                                std::move(taskCtx));
    return ConvertToMetaStatusCode(excutor.DoRPCTask());
}

MetaStatusCode MetaServerClientImpl::DeleteDentry(uint32_t fsId,
                                                  uint64_t inodeid,
                                                  const std::string &name,
                                                  FsFileType type) {
    auto task = RPCTask {
        metric_.deleteDentry.qps.count << 1;
        LatencyUpdater updater(&metric_.deleteDentry.latency);
        DeleteDentryResponse response;
        DeleteDentryRequest request;
        request.set_poolid(poolID);
        request.set_copysetid(copysetID);
        request.set_partitionid(partitionID);
        request.set_fsid(fsId);
        request.set_parentinodeid(inodeid);
        request.set_name(name);
        request.set_txid(txId);
        request.set_type(type);

        curvefs::metaserver::MetaServerService_Stub stub(channel);
        stub.DeleteDentry(cntl, &request, &response, nullptr);

        if (cntl->Failed()) {
            metric_.deleteDentry.eps.count << 1;
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

        VLOG(6) << "DeleteDentry done, request: " << request.DebugString()
                << "response: " << response.DebugString();
        return ret;
    };

    auto taskCtx = std::make_shared<TaskContext>(MetaServerOpType::DeleteDentry,
                                                 task, fsId, inodeid, false,
                                                 opt_.enableRenameParallel);
    DeleteDentryExcutor excutor(opt_, metaCache_, channelManager_,
                                std::move(taskCtx));
    return ConvertToMetaStatusCode(excutor.DoRPCTask());
}

MetaStatusCode
MetaServerClientImpl::PrepareRenameTx(const std::vector<Dentry> &dentrys) {
    auto task = RPCTask {
        metric_.prepareRenameTx.qps.count << 1;
        LatencyUpdater updater(
            &metric_.prepareRenameTx.latency);
        PrepareRenameTxRequest request;
        PrepareRenameTxResponse response;
        request.set_poolid(poolID);
        request.set_copysetid(copysetID);
        request.set_partitionid(partitionID);
        *request.mutable_dentrys() = {dentrys.begin(), dentrys.end()};

        curvefs::metaserver::MetaServerService_Stub stub(channel);
        stub.PrepareRenameTx(cntl, &request, &response, nullptr);

        if (cntl->Failed()) {
            metric_.prepareRenameTx.eps.count << 1;
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

        VLOG(6) << "PrepareRenameTx done, request: " << request.DebugString()
                << "response: " << response.DebugString();
        return rc;
    };

    auto fsId = dentrys[0].fsid();
    auto inodeId = dentrys[0].parentinodeid();
    auto taskCtx = std::make_shared<TaskContext>(
        MetaServerOpType::PrepareRenameTx, task, fsId, inodeId);
    PrepareRenameTxExcutor excutor(opt_, metaCache_, channelManager_,
                                   std::move(taskCtx));
    return ConvertToMetaStatusCode(excutor.DoRPCTask());
}

MetaStatusCode MetaServerClientImpl::GetInode(uint32_t fsId, uint64_t inodeid,
                                              Inode *out, bool* streaming) {
    auto task = RPCTask {
        metric_.getInode.qps.count << 1;
        LatencyUpdater updater(&metric_.getInode.latency);
        GetInodeRequest request;
        GetInodeResponse response;
        request.set_poolid(poolID);
        request.set_copysetid(copysetID);
        request.set_partitionid(partitionID);
        request.set_fsid(fsId);
        request.set_inodeid(inodeid);
        request.set_appliedindex(applyIndex);
        request.set_supportstreaming(true);

        curvefs::metaserver::MetaServerService_Stub stub(channel);
        stub.GetInode(cntl, &request, &response, nullptr);

        if (cntl->Failed()) {
            metric_.getInode.eps.count << 1;
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

        *streaming = response.has_streaming() ? response.streaming() : false;
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
    GetInodeExcutor excutor(opt_, metaCache_, channelManager_,
                            std::move(taskCtx));
    return ConvertToMetaStatusCode(excutor.DoRPCTask());
}

bool GroupInodeIdByPartition(
    uint32_t fsId,
    std::shared_ptr<MetaCache> metaCache,
    const std::set<uint64_t> &inodeIds,
    std::unordered_map<uint32_t, std::vector<uint64_t>> *inodeGroups) {
    for (const auto &it : inodeIds) {
        uint32_t pId = 0;
        if (metaCache->GetPartitionIdByInodeId(fsId, it, &pId)) {
            auto iter = inodeGroups->find(pId);
            if (iter == inodeGroups->end()) {
                inodeGroups->emplace(pId, std::vector<uint64_t>({it}));
            } else {
                iter->second.push_back(it);
            }
        } else {
            LOG(ERROR) << "Group inodeId fialed when get partitionId by"
                       << "inodeId, fsId = " << fsId << ", inodeId = " << it;
            return false;
        }
    }
    return true;
}

bool MetaServerClientImpl::SplitRequestInodes(
    uint32_t fsId,
    const std::set<uint64_t> &inodeIds,
    std::vector<std::vector<uint64_t>> *inodeGroups) {
    std::unordered_map<uint32_t, std::vector<uint64_t>> groups;
    bool ret = GroupInodeIdByPartition(fsId, metaCache_, inodeIds, &groups);
    if (!ret) {
        return false;
    }
    for (const auto &it : groups) {
        auto iter = it.second.begin();
        while (iter != it.second.end()) {
            std::vector<uint64_t> tmp;
            uint32_t batchLimit = opt_.batchInodeAttrLimit;
            while (iter != it.second.end() && batchLimit > 0) {
                tmp.emplace_back(*iter);
                iter++;
                batchLimit--;
            }
            inodeGroups->emplace_back(std::move(tmp));
        }
    }
    return true;
}

class BatchGetInodeAttrRpcDone : public MetaServerClientRpcDoneBase {
 public:
    using MetaServerClientRpcDoneBase::MetaServerClientRpcDoneBase;

    void Run() override;
    BatchGetInodeAttrResponse response;
};

void BatchGetInodeAttrRpcDone::Run() {
    std::unique_ptr<BatchGetInodeAttrRpcDone> self_guard(this);
    brpc::ClosureGuard done_guard(done_);
    auto taskCtx = done_->GetTaskExcutor()->GetTaskCxt();
    auto& cntl = taskCtx->cntl_;
    auto metaCache = done_->GetTaskExcutor()->GetMetaCache();
    if (cntl.Failed()) {
        metric_->batchGetInodeAttr.eps.count << 1;
        LOG(WARNING) << "batchGetInodeAttr Failed, errorcode = "
                     << cntl.ErrorCode()
                     << ", error content: " << cntl.ErrorText()
                     << ", log id: " << cntl.log_id();
         done_->SetRetCode(-cntl.ErrorCode());
        return;
    }

    MetaStatusCode ret = response.statuscode();
    if (ret != MetaStatusCode::OK) {
        LOG(WARNING) << "batchGetInodeAttr failed"
                     << ", errcode = " << ret
                     << ", errmsg = " << MetaStatusCode_Name(ret);
    } else if (response.has_appliedindex()) {
        metaCache->UpdateApplyIndex(taskCtx->target.groupID,
                                    response.appliedindex());
    } else {
        LOG(WARNING) << "batchGetInodeAttr ok,"
                     << " but applyIndex not set in response:"
                     << response.DebugString();
        done_->SetRetCode(-1);
        return;
    }

    VLOG(6) << "batchGetInodeAttr done, "
            << "response: " << response.DebugString();
    done_->SetRetCode(ret);
    dynamic_cast<BatchGetInodeAttrTaskExecutorDone*>(done_)
        ->SetInodeAttrs(response.attr());
    return;
}

MetaStatusCode MetaServerClientImpl::BatchGetInodeAttr(uint32_t fsId,
    const std::set<uint64_t> &inodeIds,
    std::list<InodeAttr> *attr) {
    // group inodeid by partition and batchlimit
    std::vector<std::vector<uint64_t>> inodeGroups;
    if (!SplitRequestInodes(fsId, inodeIds, &inodeGroups)) {
        return MetaStatusCode::NOT_FOUND;
    }

    // TDOD(wanghai): send rpc parallelly
    for (const auto &it : inodeGroups) {
        if (it.empty()) {
            LOG(WARNING) << "BatchGetInodeAttr request empty.";
            return MetaStatusCode::PARAM_ERROR;
        }
        uint64_t inodeId = *it.begin();
        auto task = RPCTask {
            metric_.batchGetInodeAttr.qps.count << 1;
            LatencyUpdater updater(&metric_.batchGetInodeAttr.latency);
            BatchGetInodeAttrRequest request;
            BatchGetInodeAttrResponse response;
            request.set_poolid(poolID);
            request.set_copysetid(copysetID);
            request.set_partitionid(partitionID);
            request.set_fsid(fsId);
            request.set_appliedindex(applyIndex);
            *request.mutable_inodeid() = { it.begin(), it.end() };

            curvefs::metaserver::MetaServerService_Stub stub(channel);
            stub.BatchGetInodeAttr(cntl, &request, &response, nullptr);

            if (cntl->Failed()) {
                metric_.batchGetInodeAttr.eps.count << 1;
                LOG(WARNING) << "BatchGetInodeAttr Failed, errorcode = "
                             << cntl->ErrorCode()
                             << ", error content:" << cntl->ErrorText()
                             << ", log id = " << cntl->log_id();
                return -cntl->ErrorCode();
            }

            MetaStatusCode ret = response.statuscode();
            if (ret != MetaStatusCode::OK) {
                LOG(WARNING) << "BatchGetInodeAttr failed, errcode = " << ret
                             << ", errmsg = " << MetaStatusCode_Name(ret);
            } else if (response.attr_size() > 0 &&
                        response.has_appliedindex()) {
                auto *attrs = response.mutable_attr();
                attr->insert(attr->end(),
                             std::make_move_iterator(attrs->begin()),
                             std::make_move_iterator(attrs->end()));
                metaCache_->UpdateApplyIndex(
                    CopysetGroupID(poolID, copysetID),
                    response.appliedindex());
            } else {
                LOG(WARNING) << "BatchGetInodeAttr ok, but"
                             << " applyIndex or attr not set in response: "
                             << response.DebugString();
                return -1;
            }
            return ret;
        };
        auto taskCtx = std::make_shared<TaskContext>(
            MetaServerOpType::BatchGetInodeAttr, task, fsId, inodeId);
        BatchGetInodeAttrExcutor excutor(
            opt_, metaCache_, channelManager_, std::move(taskCtx));
        auto ret = ConvertToMetaStatusCode(excutor.DoRPCTask());
        if (ret != MetaStatusCode::OK) {
            attr->clear();
            return ret;
        }
    }
    return MetaStatusCode::OK;
}

MetaStatusCode MetaServerClientImpl::BatchGetInodeAttrAsync(uint32_t fsId,
    const std::vector<uint64_t> &inodeIds, MetaServerClientDone *done) {
    if (inodeIds.empty()) {
        done->Run();
        return MetaStatusCode::OK;
    }

    auto task = AsyncRPCTask {
        metric_.batchGetInodeAttr.qps.count << 1;
        LatencyUpdater updater(&metric_.batchGetInodeAttr.latency);
        BatchGetInodeAttrRequest request;
        BatchGetInodeAttrResponse response;
        request.set_poolid(poolID);
        request.set_copysetid(copysetID);
        request.set_partitionid(partitionID);
        request.set_fsid(fsId);
        request.set_appliedindex(applyIndex);
        *request.mutable_inodeid() = { inodeIds.begin(), inodeIds.end() };
        auto *rpcDone = new BatchGetInodeAttrRpcDone(taskExecutorDone,
                                                     &metric_);
        curvefs::metaserver::MetaServerService_Stub stub(channel);
        stub.BatchGetInodeAttr(cntl, &request, &rpcDone->response, rpcDone);
        return MetaStatusCode::OK;
    };
    auto taskCtx = std::make_shared<TaskContext>(
        MetaServerOpType::BatchGetInodeAttr, task, fsId, *inodeIds.begin());
    auto excutor = std::make_shared<BatchGetInodeAttrExcutor>(opt_,
        metaCache_, channelManager_, std::move(taskCtx));
    TaskExecutorDone *taskDone = new BatchGetInodeAttrTaskExecutorDone(
        excutor, done);
    excutor->DoAsyncRPCTask(taskDone);
    return MetaStatusCode::OK;
}

MetaStatusCode MetaServerClientImpl::BatchGetXAttr(uint32_t fsId,
    const std::set<uint64_t> &inodeIds,
    std::list<XAttr> *xattr) {
    // group inodeid by partition and batchlimit
    std::vector<std::vector<uint64_t>> inodeGroups;
    if (!SplitRequestInodes(fsId, inodeIds, &inodeGroups)) {
        return MetaStatusCode::NOT_FOUND;
    }

    // TDOD(wanghai): send rpc parallelly
    for (const auto &it : inodeGroups) {
        if (it.empty()) {
            LOG(WARNING) << "BatchGetInodeXAttr request empty.";
            return MetaStatusCode::PARAM_ERROR;
        }

        uint64_t inodeId = *it.begin();
        auto task = RPCTask {
            metric_.batchGetXattr.qps.count << 1;
            LatencyUpdater updater(&metric_.batchGetXattr.latency);
            BatchGetXAttrRequest request;
            BatchGetXAttrResponse response;
            request.set_poolid(poolID);
            request.set_copysetid(copysetID);
            request.set_partitionid(partitionID);
            request.set_fsid(fsId);
            request.set_appliedindex(applyIndex);
            *request.mutable_inodeid() = { it.begin(), it.end() };

            curvefs::metaserver::MetaServerService_Stub stub(channel);
            stub.BatchGetXAttr(cntl, &request, &response, nullptr);

            if (cntl->Failed()) {
                metric_.batchGetXattr.eps.count << 1;
                LOG(WARNING) << "BatchGetXAttr Failed, errorcode = "
                             << cntl->ErrorCode()
                             << ", error content:" << cntl->ErrorText()
                             << ", log id = " << cntl->log_id();
                return -cntl->ErrorCode();
            }

            MetaStatusCode ret = response.statuscode();
            if (ret != MetaStatusCode::OK) {
                LOG(WARNING) << "BatchGetXAttr failed, errcode = " << ret
                             << ", errmsg = " << MetaStatusCode_Name(ret);
            } else if (response.xattr_size() > 0 &&
                        response.has_appliedindex()) {
                auto *xattrs = response.mutable_xattr();
                xattr->insert(xattr->end(),
                              std::make_move_iterator(xattrs->begin()),
                              std::make_move_iterator(xattrs->end()));
                metaCache_->UpdateApplyIndex(
                    CopysetGroupID(poolID, copysetID),
                        response.appliedindex());
            } else {
                LOG(WARNING) << "BatchGetXAttr ok, but"
                             << " applyIndex or attr not set in response: "
                             << response.DebugString();
                return -1;
            }
            return ret;
        };
        auto taskCtx = std::make_shared<TaskContext>(
            MetaServerOpType::BatchGetInodeAttr, task, fsId, inodeId);
        BatchGetInodeAttrExcutor excutor(
            opt_, metaCache_, channelManager_, std::move(taskCtx));
        auto ret = ConvertToMetaStatusCode(excutor.DoRPCTask());
        if (ret != MetaStatusCode::OK) {
            xattr->clear();
            return ret;
        }
    }
    return MetaStatusCode::OK;
}

MetaStatusCode
MetaServerClientImpl::UpdateInode(const UpdateInodeRequest &request,
                                  bool internal) {
    auto task = RPCTask {
        metric_.updateInode.qps.count << 1;
        LatencyUpdater updater(&metric_.updateInode.latency);

        UpdateInodeRequest req = request;
        req.set_poolid(poolID);
        req.set_copysetid(copysetID);
        req.set_partitionid(partitionID);

        UpdateInodeResponse response;
        curvefs::metaserver::MetaServerService_Stub stub(channel);
        stub.UpdateInode(cntl, &req, &response, nullptr);

        if (cntl->Failed()) {
            metric_.updateInode.eps.count << 1;
            LOG(WARNING) << "UpdateInode Failed, errorcode = "
                         << cntl->ErrorCode()
                         << ", error content:" << cntl->ErrorText()
                         << ", log id = " << cntl->log_id();
            return -cntl->ErrorCode();
        }

        MetaStatusCode ret = response.statuscode();
        if (ret != MetaStatusCode::OK) {
            LOG(WARNING) << "UpdateInode:  request: " << request.DebugString()
                         << ", errcode = " << ret
                         << ", errmsg = " << MetaStatusCode_Name(ret);
        } else if (response.has_appliedindex()) {
            metaCache_->UpdateApplyIndex(CopysetGroupID(poolID, copysetID),
                                         response.appliedindex());
        } else {
            LOG(WARNING) << "UpdateInode:  request: " << request.DebugString()
                         << "ok, but applyIndex not set in response:"
                         << response.DebugString();
            return -1;
        }

        VLOG(6) << "UpdateInode done, request: " << request.DebugString()
                << "response: " << response.DebugString();
        return ret;
    };

    auto taskCtx = std::make_shared<TaskContext>(
        MetaServerOpType::UpdateInode, task, request.fsid(), request.inodeid());
    ExcutorOpt opt;
    if (internal) {
        opt = optInternal_;
    } else {
        opt = opt_;
    }
    UpdateInodeExcutor excutor(opt, metaCache_, channelManager_,
                               std::move(taskCtx));
    return ConvertToMetaStatusCode(excutor.DoRPCTask());
}

namespace {

void FillInodeAttr(const Inode& inode,
                  InodeOpenStatusChange statusChange,
                  bool nlink,
                  UpdateInodeRequest* request) {
    request->set_inodeid(inode.inodeid());
    request->set_fsid(inode.fsid());
    request->set_length(inode.length());
    request->set_ctime(inode.ctime());
    request->set_mtime(inode.mtime());
    request->set_atime(inode.atime());
    request->set_uid(inode.uid());
    request->set_gid(inode.gid());
    request->set_mode(inode.mode());
    request->set_inodeopenstatuschange(statusChange);
    *request->mutable_parent() = inode.parent();
    if (inode.xattr_size() > 0) {
        *request->mutable_xattr() = inode.xattr();
    }

    if (nlink) {
        request->set_nlink(inode.nlink());
    }
}

void FillDataIndices(DataIndices&& indices, UpdateInodeRequest* request) {
    if (indices.s3ChunkInfoMap && !indices.s3ChunkInfoMap->empty()) {
        *request->mutable_s3chunkinfoadd() =
            std::move(indices.s3ChunkInfoMap.value());
    }

    if (indices.volumeExtents && indices.volumeExtents->slices_size() > 0) {
        *request->mutable_volumeextents() =
            std::move(indices.volumeExtents.value());
    }
}

}  // namespace

MetaStatusCode
MetaServerClientImpl::UpdateInodeAttr(const Inode &inode,
                                      InodeOpenStatusChange statusChange) {
    UpdateInodeRequest request;
    FillInodeAttr(inode, statusChange, /*nlink=*/true, &request);
    return UpdateInode(request);
}

MetaStatusCode MetaServerClientImpl::UpdateInodeAttrWithOutNlink(
    const Inode &inode, InodeOpenStatusChange statusChange,
    S3ChunkInfoMap *s3ChunkInfoAdd, bool internal) {
    UpdateInodeRequest request;
    FillInodeAttr(inode, statusChange, /*nlink=*/false, &request);
    if (s3ChunkInfoAdd != nullptr) {
        DataIndices indices;
        indices.s3ChunkInfoMap = *s3ChunkInfoAdd;
        FillDataIndices(std::move(indices), &request);
    }
    return UpdateInode(request, internal);
}

class UpdateInodeRpcDone : public MetaServerClientRpcDoneBase {
 public:
    using MetaServerClientRpcDoneBase::MetaServerClientRpcDoneBase;

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
        metric_->updateInode.eps.count << 1;
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

    VLOG(6) << "UpdateInode done, "
            << "response: " << response.DebugString();
    done_->SetRetCode(ret);
    return;
}

void MetaServerClientImpl::UpdateInodeAsync(const UpdateInodeRequest &request,
    MetaServerClientDone *done) {
    auto task = AsyncRPCTask {
        metric_.updateInode.qps.count << 1;

        UpdateInodeRequest req = request;
        req.set_poolid(poolID);
        req.set_copysetid(copysetID);
        req.set_partitionid(partitionID);

        auto *rpcDone = new UpdateInodeRpcDone(taskExecutorDone, &metric_);
        curvefs::metaserver::MetaServerService_Stub stub(channel);
        stub.UpdateInode(cntl, &req, &rpcDone->response, rpcDone);
        return MetaStatusCode::OK;
    };

    auto taskCtx = std::make_shared<TaskContext>(
        MetaServerOpType::UpdateInode, task, request.fsid(), request.inodeid());
    auto excutor = std::make_shared<UpdateInodeExcutor>(opt_,
        metaCache_, channelManager_, std::move(taskCtx));
    TaskExecutorDone *taskDone = new TaskExecutorDone(
        excutor, done);
    excutor->DoAsyncRPCTask(taskDone);
}

void MetaServerClientImpl::UpdateInodeAttrAsync(
    const Inode &inode, MetaServerClientDone *done,
    InodeOpenStatusChange statusChange) {
    UpdateInodeRequest request;
    FillInodeAttr(inode, statusChange, /*nlink=*/true, &request);
    UpdateInodeAsync(request, done);
}

void MetaServerClientImpl::UpdateInodeWithOutNlinkAsync(
    const Inode &inode,
    MetaServerClientDone *done,
    InodeOpenStatusChange statusChange,
    DataIndices &&indices) {
    UpdateInodeRequest request;
    FillInodeAttr(inode, statusChange, /*nlink=*/false, &request);
    FillDataIndices(std::move(indices), &request);
    UpdateInodeAsync(request, done);
}

bool MetaServerClientImpl::ParseS3MetaStreamBuffer(butil::IOBuf* buffer,
                                                   uint64_t* chunkIndex,
                                                   S3ChunkInfoList* list) {
    butil::IOBuf out;
    std::string delim = ":";
    if (buffer->cut_until(&out, delim) != 0) {
        LOG(ERROR) << "invalid stream buffer: no delimiter";
        return false;
    } else if (!StringToUll(out.to_string(), chunkIndex)) {
        LOG(ERROR) << "invalid stream buffer: invalid chunkIndex";
        return false;
    } else if (!brpc::ParsePbFromIOBuf(list, *buffer)) {
        LOG(ERROR) << "invalid stream buffer: invalid s3chunkinfo list";
        return false;
    }

    return true;
}

bool MetaServerClientImpl::HandleS3MetaStreamBuffer(butil::IOBuf* buffer,
                                                    S3ChunkInfoMap* out) {
    uint64_t chunkIndex;
    S3ChunkInfoList list;
    if (!ParseS3MetaStreamBuffer(buffer, &chunkIndex, &list)) {
        return false;
    }

    auto merge = [](S3ChunkInfoList* from, S3ChunkInfoList* to) {
        for (size_t i = 0; i < from->s3chunks_size(); i++) {
            auto chunkinfo = to->add_s3chunks();
            *chunkinfo = std::move(*from->mutable_s3chunks(i));
        }
    };

    auto iter = out->find(chunkIndex);
    if (iter == out->end()) {
        out->insert({chunkIndex, std::move(list)});
    } else {
        merge(&list, &iter->second);
    }
    return true;
}

MetaStatusCode MetaServerClientImpl::GetOrModifyS3ChunkInfo(
    uint32_t fsId, uint64_t inodeId,
    const google::protobuf::Map<uint64_t, S3ChunkInfoList> &s3ChunkInfos,
    bool returnS3ChunkInfoMap,
    google::protobuf::Map<uint64_t, S3ChunkInfoList> *out, bool internal) {
    auto task = RPCTask {
        metric_.appendS3ChunkInfo.qps.count << 1;
        LatencyUpdater updater(&metric_.appendS3ChunkInfo.latency);
        GetOrModifyS3ChunkInfoRequest request;
        GetOrModifyS3ChunkInfoResponse response;
        request.set_poolid(poolID);
        request.set_copysetid(copysetID);
        request.set_partitionid(partitionID);
        request.set_fsid(fsId);
        request.set_inodeid(inodeId);
        request.set_returns3chunkinfomap(returnS3ChunkInfoMap);
        *(request.mutable_s3chunkinfoadd()) = s3ChunkInfos;
        request.set_supportstreaming(true);

        curvefs::metaserver::MetaServerService_Stub stub(channel);

        // stream connection for s3chunkinfo list
        std::shared_ptr<StreamConnection> connection;
        auto defer = absl::MakeCleanup([&]() {
            if (connection != nullptr) {
                streamClient_.Close(connection);
            }
        });
        auto receiveCallback = [&](butil::IOBuf* buffer) {
            return HandleS3MetaStreamBuffer(buffer, out);
        };
        if (returnS3ChunkInfoMap) {
            StreamOptions options(opt_.rpcStreamIdleTimeoutMS);
            connection = streamClient_.Connect(cntl, receiveCallback, options);
            if (nullptr == connection) {
                LOG(ERROR) << "Stream connect failed in client-side";
                return MetaStatusCode::RPC_STREAM_ERROR;
            }
        }

        stub.GetOrModifyS3ChunkInfo(cntl, &request, &response, nullptr);

        if (cntl->Failed()) {
            metric_.appendS3ChunkInfo.eps.count << 1;
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
                auto status = connection->WaitAllDataReceived();
                if (status != StreamStatus::STREAM_OK) {
                    LOG(ERROR) << "Receive stream data failed"
                               << ", status=" << status;
                    return MetaStatusCode::RPC_STREAM_ERROR;
                }
            }
        } else {
            LOG(WARNING) << "GetOrModifyS3ChunkInfo,  inodeId: " << inodeId
                         << ", fsId: " << fsId
                         << " ok, but applyIndex or inode not set in response: "
                         << response.DebugString();
            return -1;
        }
        VLOG(6) << "GetOrModifyS3ChunkInfo done, request: "
                << request.DebugString()
                << "response: " << response.DebugString();
        return ret;
    };

    bool streaming = returnS3ChunkInfoMap;
    auto taskCtx = std::make_shared<TaskContext>(
        MetaServerOpType::GetOrModifyS3ChunkInfo,
        task, fsId, inodeId, streaming);
    ExcutorOpt opt;
    if (internal) {
        opt = optInternal_;
    } else {
        opt = opt_;
    }
    GetOrModifyS3ChunkInfoExcutor excutor(
        opt, metaCache_, channelManager_, std::move(taskCtx));
    return ConvertToMetaStatusCode(excutor.DoRPCTask());
}

class GetOrModifyS3ChunkInfoRpcDone : public MetaServerClientRpcDoneBase {
 public:
    using MetaServerClientRpcDoneBase::MetaServerClientRpcDoneBase;

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
        metric_->appendS3ChunkInfo.eps.count << 1;
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
    VLOG(6) << "GetOrModifyS3ChunkInfo done, response: "
            << response.DebugString();
    done_->SetRetCode(ret);
    return;
}

void MetaServerClientImpl::GetOrModifyS3ChunkInfoAsync(
    uint32_t fsId, uint64_t inodeId,
    const google::protobuf::Map<
        uint64_t, S3ChunkInfoList> &s3ChunkInfos,
    MetaServerClientDone *done) {
    auto task = AsyncRPCTask {
        metric_.appendS3ChunkInfo.qps.count << 1;

        GetOrModifyS3ChunkInfoRequest request;
        request.set_poolid(poolID);
        request.set_copysetid(copysetID);
        request.set_partitionid(partitionID);
        request.set_fsid(fsId);
        request.set_inodeid(inodeId);
        request.set_returns3chunkinfomap(false);
        *(request.mutable_s3chunkinfoadd()) = s3ChunkInfos;

        auto *rpcDone =
            new GetOrModifyS3ChunkInfoRpcDone(taskExecutorDone, &metric_);

        curvefs::metaserver::MetaServerService_Stub stub(channel);
        stub.GetOrModifyS3ChunkInfo(
            cntl, &request, &rpcDone->response, rpcDone);
        return MetaStatusCode::OK;
    };

    auto taskCtx = std::make_shared<TaskContext>(
        MetaServerOpType::GetOrModifyS3ChunkInfo, task, fsId, inodeId);
    auto excutor = std::make_shared<GetOrModifyS3ChunkInfoExcutor>(opt_,
        metaCache_, channelManager_, std::move(taskCtx));
    TaskExecutorDone *taskDone = new TaskExecutorDone(
        excutor, done);
    excutor->DoAsyncRPCTask(taskDone);
}

MetaStatusCode MetaServerClientImpl::CreateInode(const InodeParam &param,
                                                 Inode *out) {
    auto task = RPCTask {
        metric_.createInode.qps.count << 1;
        LatencyUpdater updater(&metric_.createInode.latency);
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
        request.set_parent(param.parent);
        struct timespec now;
        clock_gettime(CLOCK_REALTIME, &now);
        Time* tm = new Time();
        tm->set_sec(now.tv_sec);
        tm->set_nsec(now.tv_nsec);
        request.set_allocated_create(tm);
        curvefs::metaserver::MetaServerService_Stub stub(channel);
        stub.CreateInode(cntl, &request, &response, nullptr);

        if (cntl->Failed()) {
            metric_.createInode.eps.count << 1;
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

        VLOG(6) << "CreateInode done, request: " << request.DebugString()
                << "response: " << response.DebugString();
        return ret;
    };

    auto taskCtx = std::make_shared<TaskContext>(MetaServerOpType::CreateInode,
                                                 task, param.fsId, 0);
    CreateInodeExcutor excutor(opt_, metaCache_, channelManager_,
                               std::move(taskCtx));
    return ConvertToMetaStatusCode(excutor.DoRPCTask());
}

MetaStatusCode MetaServerClientImpl::DeleteInode(uint32_t fsId,
                                                 uint64_t inodeid) {
    auto task = RPCTask {
        metric_.deleteInode.qps.count << 1;
        LatencyUpdater updater(&metric_.deleteInode.latency);
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
            metric_.deleteInode.eps.count << 1;
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

        VLOG(6) << "DeleteInode done, request: " << request.DebugString()
                << "response: " << response.DebugString();
        return ret;
    };

    auto taskCtx = std::make_shared<TaskContext>(MetaServerOpType::DeleteInode,
                                                 task, fsId, inodeid);
    DeleteInodeExcutor excutor(opt_, metaCache_, channelManager_,
                               std::move(taskCtx));
    return ConvertToMetaStatusCode(excutor.DoRPCTask());
}

struct UpdateVolumeExtentRpcDone : MetaServerClientRpcDoneBase {
    using MetaServerClientRpcDoneBase::MetaServerClientRpcDoneBase;

    void Run() override;

    metaserver::UpdateVolumeExtentResponse response;
};

void UpdateVolumeExtentRpcDone::Run() {
    std::unique_ptr<UpdateVolumeExtentRpcDone> guard(this);
    brpc::ClosureGuard doneGuard(done_);

    auto taskCtx = done_->GetTaskExcutor()->GetTaskCxt();
    auto metaCache = done_->GetTaskExcutor()->GetMetaCache();
    auto& cntl = taskCtx->cntl_;

    if (cntl.Failed()) {
        metric_->updateVolumeExtent.eps.count << 1;
        LOG(WARNING) << "UpdateVolumeExtent failed, error: " << cntl.ErrorText()
                     << ", log id: " << cntl.log_id();
        done_->SetRetCode(-cntl.ErrorCode());
        return;
    }

    auto st = response.statuscode();
    if (st != MetaStatusCode::OK) {
        metric_->updateVolumeExtent.eps.count << 1;
        LOG(WARNING) << "UpdateVolumeExtent failed, error: "
                     << MetaStatusCode_Name(st)
                     << ", inode: " << taskCtx->inodeID;
    } else if (response.has_appliedindex()) {
        metaCache->UpdateApplyIndex(taskCtx->target.groupID,
                                    response.appliedindex());
    }

    VLOG(6) << "UpdateVolumeExtent done, response: "
            << response.ShortDebugString();
    done_->SetRetCode(st);
}

#define SET_COMMON_FIELDS                     \
    do {                                      \
        request.set_poolid(poolID);           \
        request.set_copysetid(copysetID);     \
        request.set_partitionid(partitionID); \
        request.set_fsid(fsId);               \
        request.set_inodeid(inodeId);         \
    } while (0)

void MetaServerClientImpl::AsyncUpdateVolumeExtent(
    uint32_t fsId,
    uint64_t inodeId,
    const VolumeExtentList &extents,
    MetaServerClientDone *done) {
    auto task = AsyncRPCTask {
        metric_.updateVolumeExtent.qps.count << 1;
        metaserver::UpdateVolumeExtentRequest request;
        SET_COMMON_FIELDS;
        request.set_allocated_extents(new VolumeExtentList{extents});

        auto *rpcDone =
            new UpdateVolumeExtentRpcDone(taskExecutorDone, &metric_);
        MetaServerService_Stub stub(channel);
        stub.UpdateVolumeExtent(cntl, &request, &rpcDone->response, rpcDone);
        return MetaStatusCode::OK;
    };

    auto taskCtx = std::make_shared<TaskContext>(
        MetaServerOpType::UpdateVolumeExtent, task, fsId, inodeId);
    auto executor = std::make_shared<UpdateVolumeExtentExecutor>(
        opt_, metaCache_, channelManager_, std::move(taskCtx));
    auto *taskDone = new TaskExecutorDone(executor, done);
    executor->DoAsyncRPCTask(taskDone);
}

namespace {

struct ParseVolumeExtentCallBack {
    explicit ParseVolumeExtentCallBack(VolumeExtentList *ext) : extents(ext) {}

    bool operator()(butil::IOBuf *data) const {
        metaserver::VolumeExtentSlice slice;
        if (!brpc::ParsePbFromIOBuf(&slice, *data)) {
            LOG(ERROR) << "Failed to parse volume extent slice failed";
            return false;
        }

        *extents->add_slices() = std::move(slice);
        return true;
    }

    VolumeExtentList *extents;
};

}  // namespace

MetaStatusCode MetaServerClientImpl::GetVolumeExtent(
    uint32_t fsId,
    uint64_t inodeId,
    bool streaming,
    VolumeExtentList *extents) {
    auto task = RPCTask {
        (void)txId;
        (void)applyIndex;
        (void)taskExecutorDone;
        metric_.getVolumeExtent.qps.count << 1;
        LatencyUpdater updater(&metric_.getVolumeExtent.latency);
        metaserver::GetVolumeExtentRequest request;
        metaserver::GetVolumeExtentResponse response;

        SET_COMMON_FIELDS;

        request.set_streaming(streaming);
        request.set_appliedindex(applyIndex);

        VLOG(9) << "GetVolumeExtent request, " << request.ShortDebugString();

        // for streaming
        std::shared_ptr<StreamConnection> connection;
        auto closeConn = absl::MakeCleanup([this, &connection]() {
            if (connection != nullptr) {
                streamClient_.Close(connection);
            }
        });

        if (streaming) {
            StreamOptions opts(opt_.rpcStreamIdleTimeoutMS);
            connection = streamClient_.Connect(
                cntl, ParseVolumeExtentCallBack{extents}, opts);
            if (connection == nullptr) {
                LOG(ERROR) << "Failed to connection remote side, ino: "
                           << inodeId << ", poolid: " << poolID
                           << ", copysetid: " << copysetID
                           << ", remote side: " << cntl->remote_side();
                return MetaStatusCode::RPC_STREAM_ERROR;
            }
        }

        MetaServerService_Stub stub(channel);
        stub.GetVolumeExtent(cntl, &request, &response, nullptr);

        if (cntl->Failed()) {
            metric_.getVolumeExtent.eps.count << 1;
            LOG(WARNING) << "GetVolumeExtent failed, error: "
                         << cntl->ErrorText() << ", log id: " << cntl->log_id();
            return -cntl->ErrorCode();
        }

        auto st = response.statuscode();
        if (st != MetaStatusCode::OK) {
            metric_.getVolumeExtent.eps.count << 1;
            LOG(WARNING) << "GetVolumeExtent failed, inodeid: " << inodeId
                         << ", error: " << MetaStatusCode_Name(st);
            return st;
        } else if (response.has_appliedindex()) {
            metaCache_->UpdateApplyIndex(CopysetGroupID(poolID, copysetID),
                                         response.appliedindex());
        }

        if (!streaming) {
            *extents = std::move(*response.mutable_slices());
            return st;
        }

        auto status = connection->WaitAllDataReceived();
        if (status != StreamStatus::STREAM_OK) {
            LOG(ERROR) << "Failed to receive data, status: " << status;
            return MetaStatusCode::RPC_STREAM_ERROR;
        }

        VLOG(9) << "GetVolumeExtent success, inode: " << inodeId
                << ", extents: " << extents->ShortDebugString();
        return st;
    };

    auto taskCtx = std::make_shared<TaskContext>(
        MetaServerOpType::GetInode, task, fsId, inodeId, streaming);
    GetVolumeExtentExecutor executor(opt_, metaCache_, channelManager_,
                                     std::move(taskCtx));
    return ConvertToMetaStatusCode(executor.DoRPCTask());
}

MetaStatusCode MetaServerClientImpl::GetInodeAttr(
    uint32_t fsId, uint64_t inodeid, InodeAttr *attr) {
    std::set<uint64_t> inodeIds;
    inodeIds.insert(inodeid);
    std::list<InodeAttr> attrs;
    MetaStatusCode ret = BatchGetInodeAttr(fsId, inodeIds, &attrs);
    if (ret != MetaStatusCode::OK) {
        return ret;
    }
    if (attrs.size() != 1) {
        LOG(ERROR) << "GetInodeAttr return attrs.size() != 1, which is "
                   << attrs.size();
        return MetaStatusCode::UNKNOWN_ERROR;
    }
    *attr = attrs.front();
    return MetaStatusCode::OK;
}

}  // namespace rpcclient
}  // namespace client
}  // namespace curvefs
