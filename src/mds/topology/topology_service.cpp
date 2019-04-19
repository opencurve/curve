/*
 * Project: curve
 * Created Date: Mon Aug 20 2018
 * Author: xuchaojie
 * Copyright (c) 2018 netease
 */

#include "src/mds/topology/topology_service.h"


namespace curve {
namespace mds {
namespace topology {

void TopologyServiceImpl::RegistChunkServer(
    google::protobuf::RpcController* cntl_base,
    const ChunkServerRegistRequest* request,
    ChunkServerRegistResponse* response,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);

    brpc::Controller* cntl =
        static_cast<brpc::Controller*>(cntl_base);

    LOG(INFO) << "Received request[log_id=" << cntl->log_id()
              << "] from " << cntl->remote_side()
              << " to " << cntl->local_side()
              << ". [ChunkServerRegistRequest] "
              << request->DebugString();

    topology_->RegistChunkServer(request, response);

    if (kTopoErrCodeSuccess != response->statuscode()) {
        LOG(ERROR) << "Send response[log_id=" << cntl->log_id()
                   << "] from " << cntl->local_side()
                   << " to " << cntl->remote_side()
                   << ". [ChunkServerRegistResponse] "
                   << response->DebugString();
    } else {
        LOG(INFO) << "Send response[log_id=" << cntl->log_id()
                  << "] from " << cntl->local_side()
                  << " to " << cntl->remote_side()
                  << ". [ChunkServerRegistResponse] "
                  << response->DebugString();
    }
}

void TopologyServiceImpl::ListChunkServer(
    google::protobuf::RpcController* cntl_base,
    const ListChunkServerRequest* request,
    ListChunkServerResponse* response,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);

    brpc::Controller* cntl =
        static_cast<brpc::Controller*>(cntl_base);

    LOG(INFO) << "Received request[log_id=" << cntl->log_id()
              << "] from " << cntl->remote_side()
              << " to " << cntl->local_side()
              << ". [ListChunkServerRequest] "
              << request->DebugString();

    topology_->ListChunkServer(request, response);

    if (kTopoErrCodeSuccess != response->statuscode()) {
        LOG(ERROR) << "Send response[log_id=" << cntl->log_id()
                   << "] from " << cntl->local_side()
                   << " to " << cntl->remote_side()
                   << ". [ListChunkServerResponse] "
                   << response->DebugString();
    } else {
        LOG(INFO) << "Send response[log_id=" << cntl->log_id()
                  << "] from " << cntl->local_side()
                  << " to " << cntl->remote_side()
                  << ". [ListChunkServerResponse] "
                  << response->DebugString();
    }
}

void TopologyServiceImpl::GetChunkServer(
    google::protobuf::RpcController* cntl_base,
    const GetChunkServerInfoRequest* request,
    GetChunkServerInfoResponse* response,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);

    brpc::Controller* cntl =
        static_cast<brpc::Controller*>(cntl_base);

    LOG(INFO) << "Received request[log_id=" << cntl->log_id()
              << "] from " << cntl->remote_side()
              << " to " << cntl->local_side()
              << ". [GetChunkServerInfoRequest] "
              << request->DebugString();

    topology_->GetChunkServer(request, response);

    if (kTopoErrCodeSuccess != response->statuscode()) {
        LOG(ERROR) << "Send response[log_id=" << cntl->log_id()
                   << "] from " << cntl->local_side()
                   << " to " << cntl->remote_side()
                   << ". [GetChunkServerInfoResponse] "
                   << response->DebugString();
    } else {
        LOG(INFO) << "Send response[log_id=" << cntl->log_id()
                  << "] from " << cntl->local_side()
                  << " to " << cntl->remote_side()
                  << ". [GetChunkServerInfoResponse] "
                  << response->DebugString();
    }
}

void TopologyServiceImpl::DeleteChunkServer(
    google::protobuf::RpcController* cntl_base,
    const DeleteChunkServerRequest* request,
    DeleteChunkServerResponse* response,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);

    brpc::Controller* cntl =
        static_cast<brpc::Controller*>(cntl_base);

    LOG(INFO) << "Received request[log_id=" << cntl->log_id()
              << "] from " << cntl->remote_side()
              << " to " << cntl->local_side()
              << ". [DeleteChunkServerRequest] "
              << request->DebugString();

    topology_->DeleteChunkServer(request, response);

    if (kTopoErrCodeSuccess != response->statuscode()) {
        LOG(ERROR) << "Send response[log_id=" << cntl->log_id()
                   << "] from " << cntl->local_side()
                   << " to " << cntl->remote_side()
                   << ". [DeleteChunkServerResponse] "
                   << response->DebugString();
    } else {
        LOG(INFO) << "Send response[log_id=" << cntl->log_id()
                  << "] from " << cntl->local_side()
                  << " to " << cntl->remote_side()
                  << ". [DeleteChunkServerResponse] "
                  << response->DebugString();
    }
}

void TopologyServiceImpl::SetChunkServer(
    google::protobuf::RpcController* cntl_base,
    const SetChunkServerStatusRequest* request,
    SetChunkServerStatusResponse* response,
    google::protobuf::Closure* done) {

    brpc::ClosureGuard done_guard(done);

    brpc::Controller* cntl =
        static_cast<brpc::Controller*>(cntl_base);

    LOG(INFO) << "Received request[log_id=" << cntl->log_id()
              << "] from " << cntl->remote_side()
              << " to " << cntl->local_side()
              << ". [SetChunkServerStatusRequest] "
              << request->DebugString();

    topology_->SetChunkServer(request, response);

    if (kTopoErrCodeSuccess != response->statuscode()) {
        LOG(ERROR) << "Send response[log_id=" << cntl->log_id()
                   << "] from " << cntl->local_side()
                   << " to " << cntl->remote_side()
                   << ". [SetChunkServerStatusResponse] "
                   << response->DebugString();
    } else {
        LOG(INFO) << "Send response[log_id=" << cntl->log_id()
                  << "] from " << cntl->local_side()
                  << " to " << cntl->remote_side()
                  << ". [SetChunkServerStatusResponse] "
                  << response->DebugString();
    }
}

void TopologyServiceImpl::RegistServer(
    google::protobuf::RpcController* cntl_base,
    const ServerRegistRequest* request,
    ServerRegistResponse* response,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);

    brpc::Controller* cntl =
        static_cast<brpc::Controller*>(cntl_base);

    LOG(INFO) << "Received request[log_id=" << cntl->log_id()
              << "] from " << cntl->remote_side()
              << " to " << cntl->local_side()
              << ". [ServerRegistRequest] "
              << request->DebugString();

    topology_->RegistServer(request, response);

    if (kTopoErrCodeSuccess != response->statuscode()) {
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

    brpc::Controller* cntl =
        static_cast<brpc::Controller*>(cntl_base);

    LOG(INFO) << "Received request[log_id=" << cntl->log_id()
              << "] from " << cntl->remote_side()
              << " to " << cntl->local_side()
              << ". [GetServerRequest] "
              << request->DebugString();

    topology_->GetServer(request, response);

    if (kTopoErrCodeSuccess != response->statuscode()) {
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

    brpc::Controller* cntl =
        static_cast<brpc::Controller*>(cntl_base);

    LOG(INFO) << "Received request[log_id=" << cntl->log_id()
              << "] from " << cntl->remote_side()
              << " to " << cntl->local_side()
              << ". [DeleteServerRequest] "
              << request->DebugString();

    topology_->DeleteServer(request, response);

    if (kTopoErrCodeSuccess != response->statuscode()) {
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

    brpc::Controller* cntl =
        static_cast<brpc::Controller*>(cntl_base);

    LOG(INFO) << "Received request[log_id=" << cntl->log_id()
              << "] from " << cntl->remote_side()
              << " to " << cntl->local_side()
              << ". [ListZoneServerRequest] "
              << request->DebugString();

    topology_->ListZoneServer(request, response);

    if (kTopoErrCodeSuccess != response->statuscode()) {
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
    const ZoneRequest* request,
    ZoneResponse* response,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);

    brpc::Controller* cntl =
        static_cast<brpc::Controller*>(cntl_base);

    LOG(INFO) << "Received request[log_id=" << cntl->log_id()
              << "] from " << cntl->remote_side()
              << " to " << cntl->local_side()
              << ". [CreateZone_ZoneRequest] "
              << request->DebugString();

    topology_->CreateZone(request, response);

    if (kTopoErrCodeSuccess != response->statuscode()) {
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
    const ZoneRequest* request,
    ZoneResponse* response,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);

    brpc::Controller* cntl =
        static_cast<brpc::Controller*>(cntl_base);

    LOG(INFO) << "Received request[log_id=" << cntl->log_id()
              << "] from " << cntl->remote_side()
              << " to " << cntl->local_side()
              << ". [DeleteZone_ZoneRequest] "
              << request->DebugString();

    topology_->DeleteZone(request, response);

    if (kTopoErrCodeSuccess != response->statuscode()) {
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

void TopologyServiceImpl::GetZone(google::protobuf::RpcController* cntl_base,
    const ZoneRequest* request,
    ZoneResponse* response,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);

    brpc::Controller* cntl =
        static_cast<brpc::Controller*>(cntl_base);

    LOG(INFO) << "Received request[log_id=" << cntl->log_id()
              << "] from " << cntl->remote_side()
              << " to " << cntl->local_side()
              << ". [GetZone_ZoneRequest] "
              << request->DebugString();

    topology_->GetZone(request, response);

    if (kTopoErrCodeSuccess != response->statuscode()) {
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

    topology_->ListPoolZone(request, response);

    if (kTopoErrCodeSuccess != response->statuscode()) {
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

void TopologyServiceImpl::CreatePhysicalPool(
    google::protobuf::RpcController* cntl_base,
    const PhysicalPoolRequest* request,
    PhysicalPoolResponse* response,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);

    brpc::Controller* cntl =
        static_cast<brpc::Controller*>(cntl_base);

    LOG(INFO) << "Received request[log_id=" << cntl->log_id()
              << "] from " << cntl->remote_side()
              << " to " << cntl->local_side()
              << ". [CreatePhysicalPool_PhysicalPoolRequest] "
              << request->DebugString();

    topology_->CreatePhysicalPool(request, response);

    if (kTopoErrCodeSuccess != response->statuscode()) {
        LOG(ERROR) << "Send response[log_id=" << cntl->log_id()
                   << "] from " << cntl->local_side()
                   << " to " << cntl->remote_side()
                   << ". [CreatePhysicalPool_PhysicalPoolResponse] "
                   << response->DebugString();
    } else {
        LOG(INFO) << "Send response[log_id=" << cntl->log_id()
                  << "] from " << cntl->local_side()
                  << " to " << cntl->remote_side()
                  << ". [CreatePhysicalPool_PhysicalPoolResponse] "
                  << response->DebugString();
    }
}

void TopologyServiceImpl::DeletePhysicalPool(
    google::protobuf::RpcController* cntl_base,
    const PhysicalPoolRequest* request,
    PhysicalPoolResponse* response,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);

    brpc::Controller* cntl =
        static_cast<brpc::Controller*>(cntl_base);

    LOG(INFO) << "Received request[log_id=" << cntl->log_id()
              << "] from " << cntl->remote_side()
              << " to " << cntl->local_side()
              << ". [DeletePhysicalPool_PhysicalPoolRequest] "
              << request->DebugString();

    topology_->DeletePhysicalPool(request, response);

    if (kTopoErrCodeSuccess != response->statuscode()) {
        LOG(ERROR) << "Send response[log_id=" << cntl->log_id()
                   << "] from " << cntl->local_side()
                   << " to " << cntl->remote_side()
                   << ". [DeletePhysicalPool_PhysicalPoolResponse] "
                   << response->DebugString();
    } else {
        LOG(INFO) << "Send response[log_id=" << cntl->log_id()
                  << "] from " << cntl->local_side()
                  << " to " << cntl->remote_side()
                  << ". [DeletePhysicalPool_PhysicalPoolResponse] "
                  << response->DebugString();
    }
}

void TopologyServiceImpl::GetPhysicalPool(
    google::protobuf::RpcController* cntl_base,
    const PhysicalPoolRequest* request,
    PhysicalPoolResponse* response,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);

    brpc::Controller* cntl =
        static_cast<brpc::Controller*>(cntl_base);

    LOG(INFO) << "Received request[log_id=" << cntl->log_id()
              << "] from " << cntl->remote_side()
              << " to " << cntl->local_side()
              << ". [GetPhysicalPool_PhysicalPoolRequest] "
              << request->DebugString();

    topology_->GetPhysicalPool(request, response);

    if (kTopoErrCodeSuccess != response->statuscode()) {
        LOG(ERROR) << "Send response[log_id=" << cntl->log_id()
                   << "] from " << cntl->local_side()
                   << " to " << cntl->remote_side()
                   << ". [GetPhysicalPool_PhysicalPoolResponse] "
                   << response->DebugString();
    } else {
        LOG(INFO) << "Send response[log_id=" << cntl->log_id()
                  << "] from " << cntl->local_side()
                  << " to " << cntl->remote_side()
                  << ". [GetPhysicalPool_PhysicalPoolResponse] "
                  << response->DebugString();
    }
}

void TopologyServiceImpl::ListPhysicalPool(
    google::protobuf::RpcController* cntl_base,
    const ListPhysicalPoolRequest* request,
    ListPhysicalPoolResponse* response,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);

    brpc::Controller* cntl =
        static_cast<brpc::Controller*>(cntl_base);

    LOG(INFO) << "Received request[log_id=" << cntl->log_id()
              << "] from " << cntl->remote_side()
              << " to " << cntl->local_side()
              << ". [ListPhysicalPoolRequest] "
              << request->DebugString();

    topology_->ListPhysicalPool(request, response);

    if (kTopoErrCodeSuccess != response->statuscode()) {
        LOG(ERROR) << "Send response[log_id=" << cntl->log_id()
                   << "] from " << cntl->local_side()
                   << " to " << cntl->remote_side()
                   << ". [ListPhysicalPoolResponse] "
                   << response->DebugString();
    } else {
        LOG(INFO) << "Send response[log_id=" << cntl->log_id()
                  << "] from " << cntl->local_side()
                  << " to " << cntl->remote_side()
                  << ". [ListPhysicalPoolResponse] "
                  << response->DebugString();
    }
}

void TopologyServiceImpl::CreateLogicalPool(
    google::protobuf::RpcController* cntl_base,
    const CreateLogicalPoolRequest* request,
    CreateLogicalPoolResponse* response,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);

    brpc::Controller* cntl =
        static_cast<brpc::Controller*>(cntl_base);

    LOG(INFO) << "Received request[log_id=" << cntl->log_id()
              << "] from " << cntl->remote_side()
              << " to " << cntl->local_side()
              << ". [CreateLogicalPoolRequest] "
              << request->DebugString();

    topology_->CreateLogicalPool(request, response);

    if (kTopoErrCodeSuccess != response->statuscode()) {
        LOG(ERROR) << "Send response[log_id=" << cntl->log_id()
                   << "] from " << cntl->local_side()
                   << " to " << cntl->remote_side()
                   << ". [CreateLogicalPoolResponse] "
                   << response->DebugString();
    } else {
        LOG(INFO) << "Send response[log_id=" << cntl->log_id()
                  << "] from " << cntl->local_side()
                  << " to " << cntl->remote_side()
                  << ". [CreateLogicalPoolResponse] "
                  << response->DebugString();
    }
}

void TopologyServiceImpl::DeleteLogicalPool(
    google::protobuf::RpcController* cntl_base,
    const DeleteLogicalPoolRequest* request,
    DeleteLogicalPoolResponse* response,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);

    brpc::Controller* cntl =
        static_cast<brpc::Controller*>(cntl_base);

    LOG(INFO) << "Received request[log_id=" << cntl->log_id()
              << "] from " << cntl->remote_side()
              << " to " << cntl->local_side()
              << ". [DeleteLogicalPoolRequest] "
              << request->DebugString();

    topology_->DeleteLogicalPool(request, response);

    if (kTopoErrCodeSuccess != response->statuscode()) {
        LOG(ERROR) << "Send response[log_id=" << cntl->log_id()
                   << "] from " << cntl->local_side()
                   << " to " << cntl->remote_side()
                   << ". [DeleteLogicalPoolResponse] "
                   << response->DebugString();
    } else {
        LOG(INFO) << "Send response[log_id=" << cntl->log_id()
                  << "] from " << cntl->local_side()
                  << " to " << cntl->remote_side()
                  << ". [DeleteLogicalPoolResponse] "
                  << response->DebugString();
    }
}

void TopologyServiceImpl::GetLogicalPool(
    google::protobuf::RpcController* cntl_base,
    const GetLogicalPoolRequest* request,
    GetLogicalPoolResponse* response,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);

    brpc::Controller* cntl =
        static_cast<brpc::Controller*>(cntl_base);

    LOG(INFO) << "Received request[log_id=" << cntl->log_id()
              << "] from " << cntl->remote_side()
              << " to " << cntl->local_side()
              << ". [GetLogicalPoolRequest] "
              << request->DebugString();

    topology_->GetLogicalPool(request, response);

    if (kTopoErrCodeSuccess != response->statuscode()) {
        LOG(ERROR) << "Send response[log_id=" << cntl->log_id()
                   << "] from " << cntl->local_side()
                   << " to " << cntl->remote_side()
                   << ". [GetLogicalPoolResponse] "
                   << response->DebugString();
    } else {
        LOG(INFO) << "Send response[log_id=" << cntl->log_id()
                  << "] from " << cntl->local_side()
                  << " to " << cntl->remote_side()
                  << ". [GetLogicalPoolResponse] "
                  << response->DebugString();
    }
}

void TopologyServiceImpl::ListLogicalPool(
    google::protobuf::RpcController* cntl_base,
    const ListLogicalPoolRequest* request,
    ListLogicalPoolResponse* response,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);

    brpc::Controller* cntl =
        static_cast<brpc::Controller*>(cntl_base);

    LOG(INFO) << "Received request[log_id=" << cntl->log_id()
              << "] from " << cntl->remote_side()
              << " to " << cntl->local_side()
              << ". [ListLogicalPoolRequest] "
              << request->DebugString();

    topology_->ListLogicalPool(request, response);

    if (kTopoErrCodeSuccess != response->statuscode()) {
        LOG(ERROR) << "Send response[log_id=" << cntl->log_id()
                   << "] from " << cntl->local_side()
                   << " to " << cntl->remote_side()
                   << ". [ListLogicalPoolResponse] "
                   << response->DebugString();
    } else {
        LOG(INFO) << "Send response[log_id=" << cntl->log_id()
                  << "] from " << cntl->local_side()
                  << " to " << cntl->remote_side()
                  << ". [ListLogicalPoolResponse] "
                  << response->DebugString();
    }
}

void TopologyServiceImpl::GetChunkServerListInCopySets(
    google::protobuf::RpcController* cntl_base,
    const GetChunkServerListInCopySetsRequest* request,
    GetChunkServerListInCopySetsResponse* response,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);

    brpc::Controller* cntl =
        static_cast<brpc::Controller*>(cntl_base);

    LOG(INFO) << "Received request[log_id=" << cntl->log_id()
              << "] from " << cntl->remote_side()
              << " to " << cntl->local_side()
              << ". [GetChunkServerListInCopySetsRequest] "
              << request->DebugString();

    topology_->GetChunkServerListInCopySets(request, response);

    if (kTopoErrCodeSuccess != response->statuscode()) {
        LOG(ERROR) << "Send response[log_id=" << cntl->log_id()
                   << "] from " << cntl->local_side()
                   << " to " << cntl->remote_side()
                   << ". [GetChunkServerListInCopySetsResponse] "
                   << response->DebugString();
    }
}

}  // namespace topology
}  // namespace mds
}  // namespace curve


















