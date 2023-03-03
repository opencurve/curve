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
 * Created Date: 2021-08-26
 * Author: wanghai01
 */

#include "curvefs/src/mds/topology/topology_service.h"


namespace curvefs {
namespace mds {
namespace topology {

// RPC encapsulation for corresponding methods in topology_service manager
void TopologyServiceImpl::RegistMetaServer(
    google::protobuf::RpcController* cntl_base,
    const MetaServerRegistRequest* request,
    MetaServerRegistResponse* response,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);

    brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);

    LOG(INFO) << "Received request[log_id=" << cntl->log_id()
              << "] from " << cntl->remote_side()
              << " to " << cntl->local_side()
              << ". [MetaServerRegistRequest] "
              << request->DebugString();

    topologyManager_->RegistMetaServer(request, response);

    if (TopoStatusCode::TOPO_OK != response->statuscode()) {
        LOG(ERROR) << "Send response[log_id=" << cntl->log_id()
                   << "] from " << cntl->local_side()
                   << " to " << cntl->remote_side()
                   << ". [MetaServerRegistResponse] "
                   << response->DebugString();
    } else {
        LOG(INFO) << "Send response[log_id=" << cntl->log_id()
                  << "] from " << cntl->local_side()
                  << " to " << cntl->remote_side()
                  << ". [MetaServerRegistResponse] "
                  << response->DebugString();
    }
}

void TopologyServiceImpl::ListMetaServer(
    google::protobuf::RpcController* cntl_base,
    const ListMetaServerRequest* request,
    ListMetaServerResponse* response,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);

    brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);

    LOG(INFO) << "Received request[log_id=" << cntl->log_id()
              << "] from " << cntl->remote_side()
              << " to " << cntl->local_side()
              << ". [ListMetaServerRequest] "
              << request->DebugString();

    topologyManager_->ListMetaServer(request, response);

    if (TopoStatusCode::TOPO_OK != response->statuscode()) {
        LOG(ERROR) << "Send response[log_id=" << cntl->log_id()
                   << "] from " << cntl->local_side()
                   << " to " << cntl->remote_side()
                   << ". [ListMetaServerResponse] "
                   << response->DebugString();
    } else {
        LOG(INFO) << "Send response[log_id=" << cntl->log_id()
                  << "] from " << cntl->local_side()
                  << " to " << cntl->remote_side()
                  << ". [ListMetaServerResponse] "
                  << response->DebugString();
    }
}

void TopologyServiceImpl::GetMetaServer(
    google::protobuf::RpcController* cntl_base,
    const GetMetaServerInfoRequest* request,
    GetMetaServerInfoResponse* response,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);

    brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);

    LOG(INFO) << "Received request[log_id=" << cntl->log_id()
              << "] from " << cntl->remote_side()
              << " to " << cntl->local_side()
              << ". [GetMetaServerInfoRequest] "
              << request->DebugString();

    topologyManager_->GetMetaServer(request, response);

    if (TopoStatusCode::TOPO_OK != response->statuscode()) {
        LOG(ERROR) << "Send response[log_id=" << cntl->log_id()
                   << "] from " << cntl->local_side()
                   << " to " << cntl->remote_side()
                   << ". [GetMetaServerInfoResponse] "
                   << response->DebugString();
    } else {
        LOG(INFO) << "Send response[log_id=" << cntl->log_id()
                  << "] from " << cntl->local_side()
                  << " to " << cntl->remote_side()
                  << ". [GetMetaServerInfoResponse] "
                  << response->DebugString();
    }
}

void TopologyServiceImpl::DeleteMetaServer(
    google::protobuf::RpcController* cntl_base,
    const DeleteMetaServerRequest* request,
    DeleteMetaServerResponse* response,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);

    brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);

    LOG(INFO) << "Received request[log_id=" << cntl->log_id()
              << "] from " << cntl->remote_side()
              << " to " << cntl->local_side()
              << ". [DeleteMetaServerRequest] "
              << request->DebugString();

    topologyManager_->DeleteMetaServer(request, response);

    if (TopoStatusCode::TOPO_OK != response->statuscode()) {
        LOG(ERROR) << "Send response[log_id=" << cntl->log_id()
                   << "] from " << cntl->local_side()
                   << " to " << cntl->remote_side()
                   << ". [DeleteMetaServerResponse] "
                   << response->DebugString();
    } else {
        LOG(INFO) << "Send response[log_id=" << cntl->log_id()
                  << "] from " << cntl->local_side()
                  << " to " << cntl->remote_side()
                  << ". [DeleteMetaServerResponse] "
                  << response->DebugString();
    }
}

void TopologyServiceImpl::RegistServer(
    google::protobuf::RpcController* cntl_base,
    const ServerRegistRequest* request,
    ServerRegistResponse* response,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);

    brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);

    LOG(INFO) << "Received request[log_id=" << cntl->log_id()
              << "] from " << cntl->remote_side()
              << " to " << cntl->local_side()
              << ". [ServerRegistRequest] "
              << request->DebugString();

    topologyManager_->RegistServer(request, response);

    if (TopoStatusCode::TOPO_OK != response->statuscode()) {
        LOG(ERROR) << "Send response[log_id=" << cntl->log_id()
                   << "] from " << cntl->local_side()
                   << " to " << cntl->remote_side()
                   << ". [ServerRegistResponse] "
                   << response->DebugString();
    } else {
        LOG(INFO) << "Send response[log_id=" << cntl->log_id()
                  << "] from " << cntl->local_side()
                  << " to " << cntl->remote_side()
                  << ". [ServerRegistResponse] "
                  << response->DebugString();
    }
}

void TopologyServiceImpl::GetServer(
    google::protobuf::RpcController* cntl_base,
    const GetServerRequest* request,
    GetServerResponse* response,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);

    brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);

    LOG(INFO) << "Received request[log_id=" << cntl->log_id()
              << "] from " << cntl->remote_side()
              << " to " << cntl->local_side()
              << ". [GetServerRequest] "
              << request->DebugString();

    topologyManager_->GetServer(request, response);

    if (TopoStatusCode::TOPO_OK != response->statuscode()) {
        LOG(ERROR) << "Send response[log_id=" << cntl->log_id()
                   << "] from " << cntl->local_side()
                   << " to " << cntl->remote_side()
                   << ". [GetServerResponse] "
                   << response->DebugString();
    } else {
        LOG(INFO) << "Send response[log_id=" << cntl->log_id()
                  << "] from " << cntl->local_side()
                  << " to " << cntl->remote_side()
                  << ". [GetServerResponse] "
                  << response->DebugString();
    }
}

void TopologyServiceImpl::DeleteServer(
    google::protobuf::RpcController* cntl_base,
    const DeleteServerRequest* request,
    DeleteServerResponse* response,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);

    brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);

    LOG(INFO) << "Received request[log_id=" << cntl->log_id()
              << "] from " << cntl->remote_side()
              << " to " << cntl->local_side()
              << ". [DeleteServerRequest] "
              << request->DebugString();

    topologyManager_->DeleteServer(request, response);

    if (TopoStatusCode::TOPO_OK != response->statuscode()) {
        LOG(ERROR) << "Send response[log_id=" << cntl->log_id()
                   << "] from " << cntl->local_side()
                   << " to " << cntl->remote_side()
                   << ". [DeleteServerResponse] "
                   << response->DebugString();
    } else {
        LOG(INFO) << "Send response[log_id=" << cntl->log_id()
                  << "] from " << cntl->local_side()
                  << " to " << cntl->remote_side()
                  << ". [DeleteServerResponse] "
                  << response->DebugString();
    }
}

void TopologyServiceImpl::ListZoneServer(
    google::protobuf::RpcController* cntl_base,
    const ListZoneServerRequest* request,
    ListZoneServerResponse* response,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);

    brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);

    LOG(INFO) << "Received request[log_id=" << cntl->log_id()
              << "] from " << cntl->remote_side()
              << " to " << cntl->local_side()
              << ". [ListZoneServerRequest] "
              << request->DebugString();

    topologyManager_->ListZoneServer(request, response);

    if (TopoStatusCode::TOPO_OK != response->statuscode()) {
        LOG(ERROR) << "Send response[log_id=" << cntl->log_id()
                   << "] from " << cntl->local_side()
                   << " to " << cntl->remote_side()
                   << ". [ListZoneServerResponse] "
                   << response->DebugString();
    } else {
        LOG(INFO) << "Send response[log_id=" << cntl->log_id()
                  << "] from " << cntl->local_side()
                  << " to " << cntl->remote_side()
                  << ". [ListZoneServerResponse] "
                  << response->DebugString();
    }
}

void TopologyServiceImpl::CreateZone(
    google::protobuf::RpcController* cntl_base,
    const CreateZoneRequest* request,
    CreateZoneResponse* response,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);

    brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);

    LOG(INFO) << "Received request[log_id=" << cntl->log_id()
              << "] from " << cntl->remote_side()
              << " to " << cntl->local_side()
              << ". [CreateZone_ZoneRequest] "
              << request->DebugString();

    topologyManager_->CreateZone(request, response);

    if (TopoStatusCode::TOPO_OK != response->statuscode()) {
        LOG(ERROR) << "Send response[log_id=" << cntl->log_id()
                   << "] from " << cntl->local_side()
                   << " to " << cntl->remote_side()
                   << ". [CreateZone_ZoneResponse] "
                   << response->DebugString();
    } else {
        LOG(INFO) << "Send response[log_id=" << cntl->log_id()
                  << "] from " << cntl->local_side()
                  << " to " << cntl->remote_side()
                  << ". [CreateZone_ZoneResponse] "
                  << response->DebugString();
    }
}

void TopologyServiceImpl::DeleteZone(
    google::protobuf::RpcController* cntl_base,
    const DeleteZoneRequest* request,
    DeleteZoneResponse* response,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);

    brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);

    LOG(INFO) << "Received request[log_id=" << cntl->log_id()
              << "] from " << cntl->remote_side()
              << " to " << cntl->local_side()
              << ". [DeleteZone_ZoneRequest] "
              << request->DebugString();

    topologyManager_->DeleteZone(request, response);

    if (TopoStatusCode::TOPO_OK != response->statuscode()) {
        LOG(ERROR) << "Send response[log_id=" << cntl->log_id()
                   << "] from " << cntl->local_side()
                   << " to " << cntl->remote_side()
                   << ". [DeleteZone_ZoneResponse] "
                   << response->DebugString();
    } else {
        LOG(INFO) << "Send response[log_id=" << cntl->log_id()
                  << "] from " << cntl->local_side()
                  << " to " << cntl->remote_side()
                  << ". [DeleteZone_ZoneResponse] "
                  << response->DebugString();
    }
}

void TopologyServiceImpl::GetZone(
    google::protobuf::RpcController* cntl_base,
    const GetZoneRequest* request,
    GetZoneResponse* response,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);

    brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);

    LOG(INFO) << "Received request[log_id=" << cntl->log_id()
              << "] from " << cntl->remote_side()
              << " to " << cntl->local_side()
              << ". [GetZone_ZoneRequest] "
              << request->DebugString();

    topologyManager_->GetZone(request, response);

    if (TopoStatusCode::TOPO_OK != response->statuscode()) {
        LOG(ERROR) << "Send response[log_id=" << cntl->log_id()
                   << "] from " << cntl->local_side()
                   << " to " << cntl->remote_side()
                   << ". [GetZone_ZoneResponse] "
                   << response->DebugString();
    } else {
        LOG(INFO) << "Send response[log_id=" << cntl->log_id()
                  << "] from " << cntl->local_side()
                  << " to " << cntl->remote_side()
                  << ". [GetZone_ZoneResponse] "
                  << response->DebugString();
    }
}

void TopologyServiceImpl::ListPoolZone(
    google::protobuf::RpcController* cntl_base,
    const ListPoolZoneRequest* request,
    ListPoolZoneResponse* response,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);

    brpc::Controller* cntl =
        static_cast<brpc::Controller*>(cntl_base);

    LOG(INFO) << "Received request[log_id=" << cntl->log_id()
              << "] from " << cntl->remote_side()
              << " to " << cntl->local_side()
              << ". [ListPoolZoneRequest] "
              << request->DebugString();

    topologyManager_->ListPoolZone(request, response);

    if (TopoStatusCode::TOPO_OK != response->statuscode()) {
        LOG(ERROR) << "Send response[log_id=" << cntl->log_id()
                   << "] from " << cntl->local_side()
                   << " to " << cntl->remote_side()
                   << ". [ListPoolZoneResponse] "
                   << response->DebugString();
    } else {
        LOG(INFO) << "Send response[log_id=" << cntl->log_id()
                  << "] from " << cntl->local_side()
                  << " to " << cntl->remote_side()
                  << ". [ListPoolZoneResponse] "
                  << response->DebugString();
    }
}

void TopologyServiceImpl::CreatePool(
    google::protobuf::RpcController* cntl_base,
    const CreatePoolRequest* request,
    CreatePoolResponse* response,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);

    brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);

    LOG(INFO) << "Received request[log_id=" << cntl->log_id()
              << "] from " << cntl->remote_side()
              << " to " << cntl->local_side()
              << ". [CreatePool_PoolRequest] "
              << request->DebugString();

    topologyManager_->CreatePool(request, response);

    if (TopoStatusCode::TOPO_OK != response->statuscode()) {
        LOG(ERROR) << "Send response[log_id=" << cntl->log_id()
                   << "] from " << cntl->local_side()
                   << " to " << cntl->remote_side()
                   << ". [CreatePool_PoolResponse] "
                   << response->DebugString();
    } else {
        LOG(INFO) << "Send response[log_id=" << cntl->log_id()
                  << "] from " << cntl->local_side()
                  << " to " << cntl->remote_side()
                  << ". [CreatePool_PoolResponse] "
                  << response->DebugString();
    }
}

void TopologyServiceImpl::DeletePool(
    google::protobuf::RpcController* cntl_base,
    const DeletePoolRequest* request,
    DeletePoolResponse* response,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);

    brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);

    LOG(INFO) << "Received request[log_id=" << cntl->log_id()
              << "] from " << cntl->remote_side()
              << " to " << cntl->local_side()
              << ". [DeletePool_PoolRequest] "
              << request->DebugString();

    topologyManager_->DeletePool(request, response);

    if (TopoStatusCode::TOPO_OK != response->statuscode()) {
        LOG(ERROR) << "Send response[log_id=" << cntl->log_id()
                   << "] from " << cntl->local_side()
                   << " to " << cntl->remote_side()
                   << ". [DeletePool_PoolResponse] "
                   << response->DebugString();
    } else {
        LOG(INFO) << "Send response[log_id=" << cntl->log_id()
                  << "] from " << cntl->local_side()
                  << " to " << cntl->remote_side()
                  << ". [DeletePool_PoolResponse] "
                  << response->DebugString();
    }
}

void TopologyServiceImpl::GetPool(
    google::protobuf::RpcController* cntl_base,
    const GetPoolRequest* request,
    GetPoolResponse* response,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);

    brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);

    LOG(INFO) << "Received request[log_id=" << cntl->log_id()
              << "] from " << cntl->remote_side()
              << " to " << cntl->local_side()
              << ". [GetPool_PoolRequest] "
              << request->DebugString();

    topologyManager_->GetPool(request, response);

    if (TopoStatusCode::TOPO_OK != response->statuscode()) {
        LOG(ERROR) << "Send response[log_id=" << cntl->log_id()
                   << "] from " << cntl->local_side()
                   << " to " << cntl->remote_side()
                   << ". [GetPool_PoolResponse] "
                   << response->DebugString();
    } else {
        LOG(INFO) << "Send response[log_id=" << cntl->log_id()
                  << "] from " << cntl->local_side()
                  << " to " << cntl->remote_side()
                  << ". [GetPool_PoolResponse] "
                  << response->DebugString();
    }
}

void TopologyServiceImpl::ListPool(
    google::protobuf::RpcController* cntl_base,
    const ListPoolRequest* request,
    ListPoolResponse* response,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);

    brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);

    LOG(INFO) << "Received request[log_id=" << cntl->log_id()
              << "] from " << cntl->remote_side()
              << " to " << cntl->local_side()
              << ". [ListPoolRequest] "
              << request->DebugString();

    topologyManager_->ListPool(request, response);

    if (TopoStatusCode::TOPO_OK != response->statuscode()) {
        LOG(ERROR) << "Send response[log_id=" << cntl->log_id()
                   << "] from " << cntl->local_side()
                   << " to " << cntl->remote_side()
                   << ". [ListPoolResponse] "
                   << response->DebugString();
    } else {
        LOG(INFO) << "Send response[log_id=" << cntl->log_id()
                  << "] from " << cntl->local_side()
                  << " to " << cntl->remote_side()
                  << ". [ListPoolResponse] "
                  << response->DebugString();
    }
}

void TopologyServiceImpl::GetMetaServerListInCopysets(
        google::protobuf::RpcController* cntl_base,
        const GetMetaServerListInCopySetsRequest* request,
        GetMetaServerListInCopySetsResponse* response,
        google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);
    LOG(INFO) << "Received request[log_id=" << cntl->log_id()
              << "] from " << cntl->remote_side()
              << " to " << cntl->local_side()
              << ". [GetMetaServerListInCopySetsRequest] "
              << request->DebugString();

    topologyManager_->GetMetaServerListInCopysets(request, response);
    if (TopoStatusCode::TOPO_OK != response->statuscode()) {
        LOG(ERROR) << "Send response[log_id=" << cntl->log_id()
                   << "] from " << cntl->local_side()
                   << " to " << cntl->remote_side()
                   << ". [GetMetaServerListInCopySetsResponse] "
                   << response->DebugString();
    } else {
        LOG(INFO) << "Send response[log_id=" << cntl->log_id()
                  << "] from " << cntl->local_side()
                  << " to " << cntl->remote_side()
                  << ". [GetMetaServerListInCopySetsResponse] "
                  << response->DebugString();
    }
}

void TopologyServiceImpl::CreatePartition(
        ::google::protobuf::RpcController* cntl_base,
        const CreatePartitionRequest* request,
        CreatePartitionResponse* response,
        ::google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);
    LOG(INFO) << "Received request[log_id=" << cntl->log_id()
              << "] from " << cntl->remote_side()
              << " to " << cntl->local_side()
              << ". [CreatePartitionRequest] "
              << request->DebugString();

    topologyManager_->CreatePartitions(request, response);
    if (TopoStatusCode::TOPO_OK != response->statuscode()) {
        LOG(ERROR) << "Send response[log_id=" << cntl->log_id()
                   << "] from " << cntl->local_side()
                   << " to " << cntl->remote_side()
                   << ". [CreatePartitionResponse] "
                   << response->DebugString();
    } else {
        LOG(INFO) << "Send response[log_id=" << cntl->log_id()
                  << "] from " << cntl->local_side()
                  << " to " << cntl->remote_side()
                  << ". [CreatePartitionResponse] "
                  << response->DebugString();
    }
}

void TopologyServiceImpl::DeletePartition(
    ::google::protobuf::RpcController* cntl_base,
    const DeletePartitionRequest* request,
    DeletePartitionResponse* response,
    ::google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);
    LOG(INFO) << "Received request[log_id=" << cntl->log_id()
              << "] from " << cntl->remote_side()
              << " to " << cntl->local_side()
              << ". [DeletePartitionRequest] "
              << request->DebugString();

    topologyManager_->DeletePartition(request, response);
    if (TopoStatusCode::TOPO_OK != response->statuscode()) {
        LOG(ERROR) << "Send response[log_id=" << cntl->log_id()
                   << "] from " << cntl->local_side()
                   << " to " << cntl->remote_side()
                   << ". [DeletePartitionResponse] "
                   << response->DebugString();
    } else {
        LOG(INFO) << "Send response[log_id=" << cntl->log_id()
                  << "] from " << cntl->local_side()
                  << " to " << cntl->remote_side()
                  << ". [DeletePartitionResponse] "
                  << response->DebugString();
    }
}


void TopologyServiceImpl::CommitTx(
        ::google::protobuf::RpcController* cntl_base,
        const CommitTxRequest* request,
        CommitTxResponse* response,
        ::google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);
    LOG(INFO) << "Received request[log_id=" << cntl->log_id()
              << "] from " << cntl->remote_side()
              << " to " << cntl->local_side()
              << ". [CommitTxRequest] "
              << request->DebugString();

    topologyManager_->CommitTx(request, response);
    if (TopoStatusCode::TOPO_OK != response->statuscode()) {
        LOG(ERROR) << "Send response[log_id=" << cntl->log_id()
                   << "] from " << cntl->local_side()
                   << " to " << cntl->remote_side()
                   << ". [CommitTxResponse] "
                   << response->DebugString();
    } else {
        LOG(INFO) << "Send response[log_id=" << cntl->log_id()
                  << "] from " << cntl->local_side()
                  << " to " << cntl->remote_side()
                  << ". [CommitTxResponse] "
                  << response->DebugString();
    }
}

void TopologyServiceImpl::ListPartition(
        ::google::protobuf::RpcController* cntl_base,
        const ListPartitionRequest* request,
        ListPartitionResponse* response,
        ::google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);
    LOG(INFO) << "Received request[log_id=" << cntl->log_id()
              << "] from " << cntl->remote_side()
              << " to " << cntl->local_side()
              << ". [ListPartitionRequest] "
              << request->DebugString();

    topologyManager_->ListPartition(request, response);
    if (TopoStatusCode::TOPO_OK != response->statuscode()) {
        LOG(ERROR) << "Send response[log_id=" << cntl->log_id()
                   << "] from " << cntl->local_side()
                   << " to " << cntl->remote_side()
                   << ". [ListPartitionResponse] "
                   << response->DebugString();
    } else {
        LOG(INFO) << "Send response[log_id=" << cntl->log_id()
                  << "] from " << cntl->local_side()
                  << " to " << cntl->remote_side()
                  << ". [ListPartitionResponse] "
                  << response->DebugString();
    }
}

void TopologyServiceImpl::GetCopysetOfPartition(
        ::google::protobuf::RpcController* cntl_base,
        const GetCopysetOfPartitionRequest* request,
        GetCopysetOfPartitionResponse* response,
        ::google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);
    LOG(INFO) << "Received request[log_id=" << cntl->log_id()
              << "] from " << cntl->remote_side()
              << " to " << cntl->local_side()
              << ". [GetCopysetOfPartitionRequest] "
              << request->DebugString();

    topologyManager_->GetCopysetOfPartition(request, response);
    if (TopoStatusCode::TOPO_OK != response->statuscode()) {
        LOG(ERROR) << "Send response[log_id=" << cntl->log_id()
                   << "] from " << cntl->local_side()
                   << " to " << cntl->remote_side()
                   << ". [GetCopysetOfPartitionRequest] "
                   << response->DebugString();
    } else {
        LOG(INFO) << "Send response[log_id=" << cntl->log_id()
                  << "] from " << cntl->local_side()
                  << " to " << cntl->remote_side()
                  << ". [GetCopysetOfPartitionRequest] "
                  << response->DebugString();
    }
}

void TopologyServiceImpl::GetCopysetsInfo(
    ::google::protobuf::RpcController* cntl_base,
    const GetCopysetsInfoRequest* request, GetCopysetsInfoResponse* response,
    ::google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);
    LOG(INFO) << "Received request[log_id=" << cntl->log_id() << "] from "
              << cntl->remote_side() << " to " << cntl->local_side()
              << ". [GetCopysetsInfoRequest] " << request->DebugString();

    topologyManager_->GetCopysetsInfo(request, response);

    LOG(INFO) << "Send response[log_id=" << cntl->log_id() << "] from "
              << cntl->local_side() << " to " << cntl->remote_side()
              << ". [GetCopysetsInfoResponse] " << response->DebugString();
}

void TopologyServiceImpl::ListCopysetInfo(
    ::google::protobuf::RpcController* cntl_base,
    const ListCopysetInfoRequest* request, ListCopysetInfoResponse* response,
    ::google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);
    LOG(INFO) << "Received request[log_id=" << cntl->log_id() << "] from "
              << cntl->remote_side() << " to " << cntl->local_side()
              << ". [ListCopysetInfoRequest] " << request->DebugString();

    topologyManager_->ListCopysetsInfo(response);

    LOG(INFO) << "Send response[log_id=" << cntl->log_id() << "] from "
              << cntl->local_side() << " to " << cntl->remote_side()
              << ". [ListCopysetInfoRequest] " << response->DebugString();
}

void TopologyServiceImpl::StatMetadataUsage(
    ::google::protobuf::RpcController* controller,
    const ::curvefs::mds::topology::StatMetadataUsageRequest* request,
    ::curvefs::mds::topology::StatMetadataUsageResponse* response,
    ::google::protobuf::Closure* done) {
    brpc::ClosureGuard guard(done);
    LOG(INFO) << "start to state metadata usage.";
    topologyManager_->GetMetaServersSpace(response->mutable_metadatausages());
    LOG(INFO) << "state metadata usage end.";
    return;
}

void TopologyServiceImpl::ListTopology(
    ::google::protobuf::RpcController* controller,
    const ListTopologyRequest* request, ListTopologyResponse* response,
    ::google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);

    LOG(INFO) << "Received request[log_id=" << cntl->log_id() << "] from "
              << cntl->remote_side() << " to " << cntl->local_side()
              << ". [ListTopologyRequest] " << request->DebugString();

    topologyManager_->GetTopology(response);

    LOG(INFO) << "Send response[log_id=" << cntl->log_id() << "] from "
              << cntl->local_side() << " to " << cntl->remote_side()
              << ". [ListTopologyRequest] " << response->DebugString();
}

void TopologyServiceImpl::RegistMemcacheCluster(
    ::google::protobuf::RpcController* controller,
    const RegistMemcacheClusterRequest* request,
    RegistMemcacheClusterResponse* response,
    ::google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    auto* cntl = static_cast<brpc::Controller*>(controller);

    LOG(INFO) << "Received request[log_id=" << cntl->log_id() << "] from "
              << cntl->remote_side() << " to " << cntl->local_side()
              << ". [RegistMemcacheCluster] " << request->DebugString();
    if (request->servers_size() == 0) {
        // no server
        response->set_statuscode(TOPO_INVALID_PARAM);
    } else {
        topologyManager_->RegistMemcacheCluster(request, response);
    }

    LOG(INFO) << "Send response[log_id=" << cntl->log_id() << "] from "
              << cntl->local_side() << " to " << cntl->remote_side()
              << ". [RegistMemcacheCluster] " << response->DebugString();
}

void TopologyServiceImpl::ListMemcacheCluster(
    ::google::protobuf::RpcController* controller,
    const ListMemcacheClusterRequest* request,
    ListMemcacheClusterResponse* response, ::google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    auto* cntl = static_cast<brpc::Controller*>(controller);

    LOG(INFO) << "Received request[log_id=" << cntl->log_id() << "] from "
              << cntl->remote_side() << " to " << cntl->local_side()
              << ". [ListMemcacheCluster] " << request->DebugString();

    topologyManager_->ListMemcacheCluster(response);

    LOG(INFO) << "Send response[log_id=" << cntl->log_id() << "] from "
              << cntl->local_side() << " to " << cntl->remote_side()
              << ". [ListMemcacheCluster] " << response->DebugString();
}

void TopologyServiceImpl::AllocOrGetMemcacheCluster(
    ::google::protobuf::RpcController* controller,
    const AllocOrGetMemcacheClusterRequest* request,
    AllocOrGetMemcacheClusterResponse* response,
    ::google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    auto* cntl = static_cast<brpc::Controller*>(controller);

    LOG(INFO) << "Received request[log_id=" << cntl->log_id() << "] from "
              << cntl->remote_side() << " to " << cntl->local_side()
              << ". [AllocOrGetMemcacheCluster] " << request->DebugString();

    topologyManager_->AllocOrGetMemcacheCluster(request, response);

    LOG(INFO) << "Send response[log_id=" << cntl->log_id() << "] from "
              << cntl->local_side() << " to " << cntl->remote_side()
              << ". [AllocOrGetMemcacheCluster] " << response->DebugString();
}

}  // namespace topology
}  // namespace mds
}  // namespace curvefs
