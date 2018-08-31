/*
 * Project: curve
 * Created Date: 18-8-23
 * Author: wudemiao
 * Copyright (c) 2018 netease
 */

#include "src/chunkserver/chunk_service.h"


#include <brpc/closure_guard.h>
#include <brpc/controller.h>

#include <memory>
#include <cerrno>

#include "src/chunkserver/copyset_node.h"
#include "src/chunkserver/copyset_node_manager.h"
#include "src/chunkserver/op_request.h"

namespace curve {
namespace chunkserver {

void ChunkServiceImpl::DeleteChunk(::google::protobuf::RpcController *controller,
                                   const ::curve::chunkserver::ChunkRequest *request,
                                   ::curve::chunkserver::ChunkResponse *response,
                                   google::protobuf::Closure *done) {
    brpc::ClosureGuard doneGuard(done);
    // 判断 copyset 是否存在
    if (false == copysetNodeManager_->IsExist(request->logicpoolid(), request->copysetid())) {
        response->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_COPYSET_NOTEXIST);
        LOG(ERROR) << "failed found copyset node (" << request->logicpoolid() << "," << request->copysetid() << ")";
        return;
    }

    std::shared_ptr<OpRequest>
        req = std::make_shared<ChunkOpRequest>(copysetNodeManager_, controller, request, response, doneGuard.release());
    // 分发给 QoS，或者直接给后端线程池处理
    if (false) {
        // 通过 Qos server 将 request push 到 QoS 队列
    } else {
        copysetNodeManager_->ScheduleRequest(req);
    }
}

void ChunkServiceImpl::WriteChunk(::google::protobuf::RpcController *controller,
                                  const ::curve::chunkserver::ChunkRequest *request,
                                  ::curve::chunkserver::ChunkResponse *response,
                                  google::protobuf::Closure *done) {
    brpc::ClosureGuard doneGuard(done);

    // 判断 request 参数是否合法
    if (request->offset() + request->size() > copysetNodeManager_->GetCopysetNodeOptions().maxChunkSize) {
        response->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_INVALID_REQUEST);
        return;
    }
    // 判断 copyset 是否存在
    if (false == copysetNodeManager_->IsExist(request->logicpoolid(), request->copysetid())) {
        response->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_COPYSET_NOTEXIST);
        LOG(ERROR) << "failed found copyset node (" << request->logicpoolid() << "," << request->copysetid() << ")";
        return;
    }

    std::shared_ptr<OpRequest>
        req = std::make_shared<ChunkOpRequest>(copysetNodeManager_, controller, request, response,
                                               doneGuard.release());
    // 分发给 QoS，或者直接给后端线程池处理
    if (false) {
        // 通过 Qos server 将 request push 到 QoS 队列
    } else {
        copysetNodeManager_->ScheduleRequest(req);
    }
}

void ChunkServiceImpl::ReadChunk(::google::protobuf::RpcController *controller,
                                 const ::curve::chunkserver::ChunkRequest *request,
                                 ::curve::chunkserver::ChunkResponse *response,
                                 google::protobuf::Closure *done) {
    brpc::ClosureGuard doneGuard(done);

    // 判断 request 参数是否合法
    if (request->offset() + request->size() > copysetNodeManager_->GetCopysetNodeOptions().maxChunkSize) {
        response->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_INVALID_REQUEST);
        return;
    }
    // 判断 copyset 是否存在
    if (false == copysetNodeManager_->IsExist(request->logicpoolid(), request->copysetid())) {
        response->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_COPYSET_NOTEXIST);
        LOG(ERROR) << "failed found copyset node (" << request->logicpoolid() << "," << request->copysetid() << ")";
        return;
    }

    std::shared_ptr<OpRequest>
        req = std::make_shared<ChunkOpRequest>(copysetNodeManager_, controller, request, response, doneGuard.release());
    // 分发给 QoS，或者直接给后端线程池处理
    if (false) {
        // 通过 Qos server 将 request push 到 QoS 队列
    } else {
        copysetNodeManager_->ScheduleRequest(req);
    }
}

void ChunkServiceImpl::CreateChunkSnapshot(::google::protobuf::RpcController *controller,
                                           const ::curve::chunkserver::ChunkSnapshotRequest *request,
                                           ::curve::chunkserver::ChunkSnapshotResponse *response,
                                           google::protobuf::Closure *done) {
    brpc::ClosureGuard doneGuard(done);
    // 判断 copyset 是否存在
    if (false == copysetNodeManager_->IsExist(request->logicpoolid(), request->copysetid())) {
        response->set_status(CHUNK_SNAPSHOT_OP_STATUS::CHUNK_SNAPSHOT_OP_STATUS_COPYSET_NOTEXIST);
        LOG(ERROR) << "failed found copyset node (" << request->logicpoolid() << "," << request->copysetid() << ")";
        return;
    }

    std::shared_ptr<OpRequest>
        req = std::make_shared<ChunkSnapshotOpRequest>(copysetNodeManager_, controller, request, response,
                                                       doneGuard.release());
    // 分发给 QoS，或者直接给后端线程池处理
    if (false) {
        // 通过 Qos server 将 request push 到 QoS 队列
    } else {
        copysetNodeManager_->ScheduleRequest(req);
    }
}

void ChunkServiceImpl::DeleteChunkSnapshot(::google::protobuf::RpcController *controller,
                                           const ::curve::chunkserver::ChunkSnapshotRequest *request,
                                           ::curve::chunkserver::ChunkSnapshotResponse *response,
                                           google::protobuf::Closure *done) {
    brpc::ClosureGuard doneGuard(done);
    // 判断 copyset 是否存在
    if (false == copysetNodeManager_->IsExist(request->logicpoolid(), request->copysetid())) {
        response->set_status(CHUNK_SNAPSHOT_OP_STATUS::CHUNK_SNAPSHOT_OP_STATUS_COPYSET_NOTEXIST);
        LOG(ERROR) << "failed found copyset node (" << request->logicpoolid() << "," << request->copysetid() << ")";
        return;
    }

    std::shared_ptr<OpRequest>
        req = std::make_shared<ChunkSnapshotOpRequest>(copysetNodeManager_, controller, request, response,
                                                       doneGuard.release());
    // 分发给 QoS，或者直接给后端线程池处理
    if (false) {
        // 通过 Qos server 将 request push 到 QoS 队列
    } else {
        copysetNodeManager_->ScheduleRequest(req);
    }
}

void ChunkServiceImpl::ReadChunkSnapshot(::google::protobuf::RpcController *controller,
                                         const ::curve::chunkserver::ChunkSnapshotRequest *request,
                                         ::curve::chunkserver::ChunkSnapshotResponse *response,
                                         google::protobuf::Closure *done) {
    brpc::ClosureGuard doneGuard(done);
    // 判断 copyset 是否存在
    if (false == copysetNodeManager_->IsExist(request->logicpoolid(), request->copysetid())) {
        response->set_status(CHUNK_SNAPSHOT_OP_STATUS::CHUNK_SNAPSHOT_OP_STATUS_COPYSET_NOTEXIST);
        LOG(ERROR) << "failed found copyset node (" << request->logicpoolid() << "," << request->copysetid() << ")";
        return;
    }

    std::shared_ptr<OpRequest>
        req = std::make_shared<ChunkSnapshotOpRequest>(copysetNodeManager_, controller, request, response,
                                                       doneGuard.release());
    // 分发给 QoS，或者直接给后端线程池处理
    if (false) {
        // 通过 Qos server 将 request push 到 QoS 队列
    } else {
        copysetNodeManager_->ScheduleRequest(req);
    }
}

}  // namespace chunkserver
}  // namespace curve
