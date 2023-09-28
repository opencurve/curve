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
 * Created Date: 2021-05-19
 * Author: chenwei
 */

#include <list>
#include <string>

#include "curvefs/src/metaserver/metaserver_service.h"
#include "curvefs/src/metaserver/copyset/meta_operator.h"
#include "curvefs/src/metaserver/metaservice_closure.h"

static bvar::LatencyRecorder g_oprequest_in_service_before_propose_latency(
                                    "oprequest_in_service_before_propose");

namespace curvefs {
namespace metaserver {

using ::curvefs::metaserver::copyset::BatchGetInodeAttrOperator;
using ::curvefs::metaserver::copyset::BatchGetXAttrOperator;
using ::curvefs::metaserver::copyset::CreateDentryOperator;
using ::curvefs::metaserver::copyset::CreateInodeOperator;
using ::curvefs::metaserver::copyset::CreateManageInodeOperator;
using ::curvefs::metaserver::copyset::CreatePartitionOperator;
using ::curvefs::metaserver::copyset::CreateRootInodeOperator;
using ::curvefs::metaserver::copyset::DeleteDentryOperator;
using ::curvefs::metaserver::copyset::DeleteInodeOperator;
using ::curvefs::metaserver::copyset::DeletePartitionOperator;
using ::curvefs::metaserver::copyset::GetDentryOperator;
using ::curvefs::metaserver::copyset::GetInodeOperator;
using ::curvefs::metaserver::copyset::GetOrModifyS3ChunkInfoOperator;
using ::curvefs::metaserver::copyset::GetVolumeExtentOperator;
using ::curvefs::metaserver::copyset::ListDentryOperator;
using ::curvefs::metaserver::copyset::PrepareRenameTxOperator;
using ::curvefs::metaserver::copyset::UpdateDeallocatableBlockGroupOperator;
using ::curvefs::metaserver::copyset::UpdateFsUsedOperator;
using ::curvefs::metaserver::copyset::UpdateInodeOperator;
using ::curvefs::metaserver::copyset::UpdateInodeS3VersionOperator;
using ::curvefs::metaserver::copyset::UpdateVolumeExtentOperator;

namespace {

struct OperatorHelper {
    OperatorHelper(CopysetNodeManager* manager, InflightThrottle* throttle)
        : manager(manager), throttle(throttle) {}

    template <typename OperatorT, typename RequestT, typename ResponseT>
    void operator()(google::protobuf::RpcController* cntl,
                    const RequestT* request, ResponseT* response,
                    google::protobuf::Closure* done, PoolId poolId,
                    CopysetId copysetId) {
        butil::Timer timer;
        timer.start();
        // check if overloaded
        brpc::ClosureGuard doneGuard(done);
        if (throttle->IsOverLoad()) {
            LOG_EVERY_N(WARNING, 100)
                << "service overload, request: " << request->ShortDebugString();
            response->set_statuscode(MetaStatusCode::OVERLOAD);
            return;
        }

        auto node = manager->GetCopysetNode(poolId, copysetId);

        if (!node) {
            LOG(WARNING) << "Copyset not found, request: "
                         << request->ShortDebugString();
            response->set_statuscode(MetaStatusCode::COPYSET_NOTEXIST);
            return;
        }

        auto* op = new OperatorT(
            node, cntl, request, response,
            new MetaServiceClosure(throttle, doneGuard.release()));
        timer.stop();
        g_oprequest_in_service_before_propose_latency << timer.u_elapsed();
        node->GetMetric()->NewArrival(op->GetOperatorType());
        op->Propose();
    }

    CopysetNodeManager* manager;
    InflightThrottle* throttle;
};

}  // namespace

void MetaServerServiceImpl::GetDentry(
    ::google::protobuf::RpcController* controller,
    const ::curvefs::metaserver::GetDentryRequest* request,
    ::curvefs::metaserver::GetDentryResponse* response,
    ::google::protobuf::Closure* done) {
    OperatorHelper helper(copysetNodeManager_, inflightThrottle_);
    helper.operator()<GetDentryOperator>(controller, request, response, done,
                                         request->poolid(),
                                         request->copysetid());
}

void MetaServerServiceImpl::ListDentry(
    ::google::protobuf::RpcController* controller,
    const ::curvefs::metaserver::ListDentryRequest* request,
    ::curvefs::metaserver::ListDentryResponse* response,
    ::google::protobuf::Closure* done) {
    OperatorHelper helper(copysetNodeManager_, inflightThrottle_);

    helper.operator()<ListDentryOperator>(controller, request, response, done,
                                          request->poolid(),
                                          request->copysetid());
}

void MetaServerServiceImpl::CreateDentry(
    ::google::protobuf::RpcController* controller,
    const ::curvefs::metaserver::CreateDentryRequest* request,
    ::curvefs::metaserver::CreateDentryResponse* response,
    ::google::protobuf::Closure* done) {
    OperatorHelper helper(copysetNodeManager_, inflightThrottle_);
    helper.operator()<CreateDentryOperator>(controller, request, response, done,
                                            request->poolid(),
                                            request->copysetid());
}

void MetaServerServiceImpl::DeleteDentry(
    ::google::protobuf::RpcController* controller,
    const ::curvefs::metaserver::DeleteDentryRequest* request,
    ::curvefs::metaserver::DeleteDentryResponse* response,
    ::google::protobuf::Closure* done) {
    OperatorHelper helper(copysetNodeManager_, inflightThrottle_);
    helper.operator()<DeleteDentryOperator>(controller, request, response, done,
                                            request->poolid(),
                                            request->copysetid());
}

void MetaServerServiceImpl::GetInode(
    ::google::protobuf::RpcController* controller,
    const ::curvefs::metaserver::GetInodeRequest* request,
    ::curvefs::metaserver::GetInodeResponse* response,
    ::google::protobuf::Closure* done) {
    OperatorHelper helper(copysetNodeManager_, inflightThrottle_);
    helper.operator()<GetInodeOperator>(controller, request, response, done,
                                        request->poolid(),
                                        request->copysetid());
}

void MetaServerServiceImpl::BatchGetInodeAttr(
    ::google::protobuf::RpcController* controller,
    const ::curvefs::metaserver::BatchGetInodeAttrRequest* request,
    ::curvefs::metaserver::BatchGetInodeAttrResponse* response,
    ::google::protobuf::Closure* done) {
    OperatorHelper helper(copysetNodeManager_, inflightThrottle_);
    helper.operator()<BatchGetInodeAttrOperator>(controller, request, response,
                                                 done,
                                                 request->poolid(),
                                                 request->copysetid());
}

void MetaServerServiceImpl::BatchGetXAttr(
    ::google::protobuf::RpcController* controller,
    const ::curvefs::metaserver::BatchGetXAttrRequest* request,
    ::curvefs::metaserver::BatchGetXAttrResponse* response,
    ::google::protobuf::Closure* done) {
    OperatorHelper helper(copysetNodeManager_, inflightThrottle_);
    helper.operator()<BatchGetXAttrOperator>(controller, request, response,
                                             done,
                                             request->poolid(),
                                             request->copysetid());
}

void MetaServerServiceImpl::CreateInode(
    ::google::protobuf::RpcController* controller,
    const ::curvefs::metaserver::CreateInodeRequest* request,
    ::curvefs::metaserver::CreateInodeResponse* response,
    ::google::protobuf::Closure* done) {
    OperatorHelper helper(copysetNodeManager_, inflightThrottle_);
    helper.operator()<CreateInodeOperator>(controller, request, response, done,
                                           request->poolid(),
                                           request->copysetid());
}

void MetaServerServiceImpl::CreateRootInode(
    ::google::protobuf::RpcController* controller,
    const ::curvefs::metaserver::CreateRootInodeRequest* request,
    ::curvefs::metaserver::CreateRootInodeResponse* response,
    ::google::protobuf::Closure* done) {
    OperatorHelper helper(copysetNodeManager_, inflightThrottle_);
    helper.operator()<CreateRootInodeOperator>(controller, request, response,
                                               done, request->poolid(),
                                               request->copysetid());
}

void MetaServerServiceImpl::CreateManageInode(
    ::google::protobuf::RpcController* controller,
    const ::curvefs::metaserver::CreateManageInodeRequest* request,
    ::curvefs::metaserver::CreateManageInodeResponse* response,
    ::google::protobuf::Closure* done) {
    OperatorHelper helper(copysetNodeManager_, inflightThrottle_);
    helper.operator()<CreateManageInodeOperator>(controller, request, response,
                                               done, request->poolid(),
                                               request->copysetid());
}

void MetaServerServiceImpl::UpdateInode(
    ::google::protobuf::RpcController* controller,
    const ::curvefs::metaserver::UpdateInodeRequest* request,
    ::curvefs::metaserver::UpdateInodeResponse* response,
    ::google::protobuf::Closure* done) {
    OperatorHelper helper(copysetNodeManager_, inflightThrottle_);
    helper.operator()<UpdateInodeOperator>(controller, request, response, done,
                                           request->poolid(),
                                           request->copysetid());
}

void MetaServerServiceImpl::GetOrModifyS3ChunkInfo(
    ::google::protobuf::RpcController* controller,
    const ::curvefs::metaserver::GetOrModifyS3ChunkInfoRequest* request,
    ::curvefs::metaserver::GetOrModifyS3ChunkInfoResponse* response,
    ::google::protobuf::Closure* done) {
    OperatorHelper helper(copysetNodeManager_, inflightThrottle_);
    helper.operator()<GetOrModifyS3ChunkInfoOperator>(
                                                 controller, request, response,
                                                 done, request->poolid(),
                                                 request->copysetid());
}

void MetaServerServiceImpl::DeleteInode(
    ::google::protobuf::RpcController* controller,
    const ::curvefs::metaserver::DeleteInodeRequest* request,
    ::curvefs::metaserver::DeleteInodeResponse* response,
    ::google::protobuf::Closure* done) {
    OperatorHelper helper(copysetNodeManager_, inflightThrottle_);
    helper.operator()<DeleteInodeOperator>(controller, request, response, done,
                                           request->poolid(),
                                           request->copysetid());
}

void MetaServerServiceImpl::CreatePartition(
    google::protobuf::RpcController* controller,
    const CreatePartitionRequest* request, CreatePartitionResponse* response,
    google::protobuf::Closure* done) {
    OperatorHelper helper(copysetNodeManager_, inflightThrottle_);
    helper.operator()<CreatePartitionOperator>(
        controller, request, response, done, request->partition().poolid(),
        request->partition().copysetid());
}

void MetaServerServiceImpl::DeletePartition(
    google::protobuf::RpcController* controller,
    const DeletePartitionRequest* request, DeletePartitionResponse* response,
    google::protobuf::Closure* done) {
    OperatorHelper helper(copysetNodeManager_, inflightThrottle_);
    helper.operator()<DeletePartitionOperator>(
        controller, request, response, done, request->poolid(),
        request->copysetid());
}

void MetaServerServiceImpl::PrepareRenameTx(
    google::protobuf::RpcController* controller,
    const PrepareRenameTxRequest* request, PrepareRenameTxResponse* response,
    google::protobuf::Closure* done) {
    OperatorHelper helper(copysetNodeManager_, inflightThrottle_);
    helper.operator()<PrepareRenameTxOperator>(controller, request, response,
                                               done, request->poolid(),
                                               request->copysetid());
}

void MetaServerServiceImpl::GetVolumeExtent(
    ::google::protobuf::RpcController* controller,
    const GetVolumeExtentRequest* request,
    GetVolumeExtentResponse* response,
    ::google::protobuf::Closure* done) {
    OperatorHelper helper(copysetNodeManager_, inflightThrottle_);
    helper.operator()<GetVolumeExtentOperator>(controller, request, response,
                                               done, request->poolid(),
                                               request->copysetid());
}

void MetaServerServiceImpl::UpdateVolumeExtent(
    ::google::protobuf::RpcController* controller,
    const UpdateVolumeExtentRequest* request,
    UpdateVolumeExtentResponse* response,
    ::google::protobuf::Closure* done) {
    OperatorHelper helper(copysetNodeManager_, inflightThrottle_);
    helper.operator()<UpdateVolumeExtentOperator>(controller, request, response,
                                                  done, request->poolid(),
                                                  request->copysetid());
}

void MetaServerServiceImpl::UpdateDeallocatableBlockGroup(
    ::google::protobuf::RpcController *controller,
    const UpdateDeallocatableBlockGroupRequest *request,
    UpdateDeallocatableBlockGroupResponse *response,
    ::google::protobuf::Closure *done) {
    OperatorHelper helper(copysetNodeManager_, inflightThrottle_);
    helper.operator()<UpdateDeallocatableBlockGroupOperator>(
        controller, request, response, done, request->poolid(),
        request->copysetid());
}

void MetaServerServiceImpl::UpdateFsUsed(
    ::google::protobuf::RpcController* controller,
    const UpdateFsUsedRequest* request, UpdateFsUsedResponse* response,
    ::google::protobuf::Closure* done) {
    if (!request->has_delta() || !request->has_fromclient()) {
        response->set_statuscode(MetaStatusCode::PARAM_ERROR);
        done->Run();
        return;
    }

    if (request->fromclient() == true) {
        auto delta = request->delta();
        fsUsedManager_->AddFsUsedDelta(std::move(delta));
        response->set_statuscode(MetaStatusCode::OK);
        done->Run();
        return;
    }

    OperatorHelper helper(copysetNodeManager_, inflightThrottle_);
    helper.operator()<UpdateFsUsedOperator>(controller, request, response, done,
                                            request->poolid(),
                                            request->copysetid());
}

}  // namespace metaserver
}  // namespace curvefs
