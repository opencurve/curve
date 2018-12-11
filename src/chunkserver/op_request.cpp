/*
 * Project: curve
 * Created Date: 18-8-23
 * Author: wudemiao
 * Copyright (c) 2018 netease
 */

#include "src/chunkserver/op_request.h"

#include <glog/logging.h>
#include <brpc/controller.h>
#include <butil/sys_byteorder.h>
#include <brpc/closure_guard.h>

#include <memory>

#include "src/chunkserver/copyset_node.h"
#include "src/chunkserver/copyset_node_manager.h"
#include "src/chunkserver/chunk_closure.h"

namespace curve {
namespace chunkserver {

/*
 * Chunk Op 具体的调度逻辑还是由 copyset node manager 来决定，
 * 因为后期的 thread pool 归 copyset manager 管理
 */
void ChunkOpRequest::Schedule() {
    copysetNodeManager_->ScheduleRequest(shared_from_this());
}

void ChunkOpRequest::Process() {
    brpc::ClosureGuard doneGuard(done_);

    std::shared_ptr<CopysetNode>
        nodePtr = copysetNodeManager_->GetCopysetNode(request_->logicpoolid(),
                                                      request_->copysetid());
    CHECK(nullptr != nodePtr);
    switch (request_->optype()) {
        case CHUNK_OP_TYPE::CHUNK_OP_READ:
            nodePtr->ReadChunk(cntl_, request_, response_, doneGuard.release());
            break;
        case CHUNK_OP_TYPE::CHUNK_OP_WRITE:
            nodePtr->WriteChunk(cntl_,
                                request_,
                                response_,
                                doneGuard.release());
            break;
        case CHUNK_OP_TYPE::CHUNK_OP_DELETE:
            nodePtr->DeleteChunk(cntl_,
                                 request_,
                                 response_,
                                 doneGuard.release());
            break;
        default:
            response_->set_status(
                CHUNK_OP_STATUS::CHUNK_OP_STATUS_INVALID_REQUEST);
            LOG(ERROR) << "UNKNOWN Chunk Op";
    }
}

}  // namespace chunkserver
}  // namespace curve
