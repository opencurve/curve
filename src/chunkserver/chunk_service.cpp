/*
 * Project: curve
 * Created Date: 18-8-23
 * Author: wudemiao
 * Copyright (c) 2018 netease
 */

#include "src/chunkserver/chunk_service.h"

#include <glog/logging.h>
#include <brpc/closure_guard.h>
#include <brpc/controller.h>

#include <memory>
#include <cerrno>

#include "src/chunkserver/copyset_node.h"
#include "src/chunkserver/copyset_node_manager.h"
#include "src/chunkserver/op_request.h"

namespace curve {
namespace chunkserver {

void ChunkServiceImpl::DeleteChunk(RpcController *controller,
                                   const ChunkRequest *request,
                                   ChunkResponse *response,
                                   Closure *done) {
    brpc::ClosureGuard doneGuard(done);
    /* 判断 copyset 是否存在 */
    bool isExist = copysetNodeManager_->IsExist(request->logicpoolid(),
                                                request->copysetid());
    if (false == isExist) {
        response->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_COPYSET_NOTEXIST);
        LOG(ERROR)
            << "failed found copyset node " << request->logicpoolid() << ","
            << request->copysetid() << ")";
        return;
    }

    std::shared_ptr<ChunkOpRequest>
        req = std::make_shared<ChunkOpRequest>(copysetNodeManager_,
                                               controller,
                                               request,
                                               response,
                                               doneGuard.release());
    /* 分发给 QoS，或者直接给后端线程池处理 */
    if (false) {
        /* TODO(wudemiao): 通过 Qos server 将 request push 到 QoS 队列 */
    } else {
        copysetNodeManager_->ScheduleRequest(req);
    }
}

void ChunkServiceImpl::WriteChunk(RpcController *controller,
                                  const ChunkRequest *request,
                                  ChunkResponse *response,
                                  Closure *done) {
    brpc::ClosureGuard doneGuard(done);
    brpc::Controller *cntl = dynamic_cast<brpc::Controller *>(controller);
    DVLOG(9) << "Get write I/O request, op: " << request->optype()
             << " offset: " << request->offset()
             << " size: " << request->size() << " buf header: "
             << *(unsigned int *) cntl->request_attachment().to_string().c_str()
             << " attachement size " << cntl->request_attachment().size();

    /* 判断 request 参数是否合法 */
    auto maxSize = copysetNodeManager_->GetCopysetNodeOptions().maxChunkSize;
    if (request->offset() + request->size() > maxSize) {
        response->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_INVALID_REQUEST);
        DVLOG(9) << "I/O request, op: " << request->optype()
                 << " offset: " << request->offset()
                 << " size: " << request->size()
                 << " max size: " << maxSize;
        return;
    }
    /* 判断 copyset 是否存在 */
    bool isExist = copysetNodeManager_->IsExist(request->logicpoolid(),
                                                request->copysetid());
    if (false == isExist) {
        response->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_COPYSET_NOTEXIST);
        LOG(ERROR) << "failed found copyset node (" << request->logicpoolid()
                   << "," << request->copysetid() << ")";
        return;
    }

    std::shared_ptr<ChunkOpRequest>
        req = std::make_shared<ChunkOpRequest>(copysetNodeManager_,
                                               controller,
                                               request,
                                               response,
                                               doneGuard.release());
    /* 分发给 QoS，或者直接给后端线程池处理 */
    if (false) {
        /* 通过 Qos server 将 request push 到 QoS 队列 */
    } else {
        copysetNodeManager_->ScheduleRequest(req);
    }
}

void ChunkServiceImpl::ReadChunk(RpcController *controller,
                                 const ChunkRequest *request,
                                 ChunkResponse *response,
                                 Closure *done) {
    brpc::ClosureGuard doneGuard(done);

    auto maxSize = copysetNodeManager_->GetCopysetNodeOptions().maxChunkSize;
    if (request->offset() + request->size() > maxSize) {
        response->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_INVALID_REQUEST);
        DVLOG(9) << "I/O request, op: " << request->optype()
                 << " offset: " << request->offset()
                 << " size: " << request->size()
                 << " max size: " << maxSize;
        return;
    }
    bool isExist = copysetNodeManager_->IsExist(request->logicpoolid(),
                                                request->copysetid());
    if (false == isExist) {
        response->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_COPYSET_NOTEXIST);
        LOG(ERROR)
            << "failed found copyset node (" << request->logicpoolid() << ","
            << request->copysetid() << ")";
        return;
    }

    std::shared_ptr<ChunkOpRequest>
        req = std::make_shared<ChunkOpRequest>(copysetNodeManager_,
                                               controller,
                                               request,
                                               response,
                                               doneGuard.release());
    if (false) {
        /* 通过 Qos server 将 request push 到 QoS 队列 */
    } else {
        copysetNodeManager_->ScheduleRequest(req);
    }
}

}  // namespace chunkserver
}  // namespace curve
