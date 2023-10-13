/*
 *  Copyright (c) 2020 NetEase Inc.
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
 * Created Date: 18-8-23
 * Author: wudemiao
 */

#include "src/chunkserver/chunk_service.h"

#include <brpc/closure_guard.h>
#include <brpc/controller.h>
#include <glog/logging.h>

#include <cerrno>
#include <memory>
#include <vector>

#include "include/curve_compiler_specific.h"
#include "src/chunkserver/chunk_service_closure.h"
#include "src/chunkserver/chunkserver_metrics.h"
#include "src/chunkserver/copyset_node.h"
#include "src/chunkserver/copyset_node_manager.h"
#include "src/chunkserver/op_request.h"
#include "src/common/fast_align.h"

namespace curve {
namespace chunkserver {

using ::curve::common::is_aligned;

ChunkServiceImpl::ChunkServiceImpl(
    const ChunkServiceOptions& chunkServiceOptions,
    const std::shared_ptr<EpochMap>& epochMap)
    : chunkServiceOptions_(chunkServiceOptions),
      copysetNodeManager_(chunkServiceOptions.copysetNodeManager),
      inflightThrottle_(chunkServiceOptions.inflightThrottle),
      epochMap_(epochMap),
      blockSize_(copysetNodeManager_->GetCopysetNodeOptions().blockSize) {
    maxChunkSize_ = copysetNodeManager_->GetCopysetNodeOptions().maxChunkSize;
}

void ChunkServiceImpl::DeleteChunk(RpcController* controller,
                                   const ChunkRequest* request,
                                   ChunkResponse* response, Closure* done) {
    ChunkServiceClosure* closure = new (std::nothrow)
        ChunkServiceClosure(inflightThrottle_, request, response, done);
    CHECK(nullptr != closure) << "new chunk service closure failed";

    brpc::ClosureGuard doneGuard(closure);

    if (inflightThrottle_->IsOverLoad()) {
        response->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_OVERLOAD);
        LOG_EVERY_N(WARNING, 100)
            << "DeleteChunk: "
            << "too many inflight requests to process in chunkserver";
        return;
    }

    // Determine if the copyset exists
    auto nodePtr = copysetNodeManager_->GetCopysetNode(request->logicpoolid(),
                                                       request->copysetid());
    if (nullptr == nodePtr) {
        response->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_COPYSET_NOTEXIST);
        LOG(WARNING) << "delete chunk failed, copyset node is not found:"
                     << request->logicpoolid() << "," << request->copysetid();
        return;
    }

    std::shared_ptr<DeleteChunkRequest> req =
        std::make_shared<DeleteChunkRequest>(nodePtr, controller, request,
                                             response, doneGuard.release());
    req->Process();
}

void ChunkServiceImpl::WriteChunk(RpcController* controller,
                                  const ChunkRequest* request,
                                  ChunkResponse* response, Closure* done) {
    ChunkServiceClosure* closure = new (std::nothrow)
        ChunkServiceClosure(inflightThrottle_, request, response, done);
    CHECK(nullptr != closure) << "new chunk service closure failed";

    brpc::ClosureGuard doneGuard(closure);

    if (inflightThrottle_->IsOverLoad()) {
        response->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_OVERLOAD);
        LOG_EVERY_N(WARNING, 100)
            << "WriteChunk: "
            << "too many inflight requests to process in chunkserver";
        return;
    }

    brpc::Controller* cntl = dynamic_cast<brpc::Controller*>(controller);
    DVLOG(9) << "Get write I/O request, op: " << request->optype()
             << " offset: " << request->offset() << " size: " << request->size()
             << " buf header: "
             << *(unsigned int*)cntl->request_attachment().to_string().c_str()
             << " attachement size " << cntl->request_attachment().size();

    if (request->has_epoch()) {
        if (!epochMap_->CheckEpoch(request->fileid(), request->epoch())) {
            LOG(WARNING) << "I/O request, op: " << request->optype()
                         << ", CheckEpoch failed, ChunkRequest: "
                         << request->ShortDebugString();
            response->set_status(
                CHUNK_OP_STATUS::CHUNK_OP_STATUS_EPOCH_TOO_OLD);
            return;
        }
    }

    // Determine whether the request parameter is legal
    if (!CheckRequestOffsetAndLength(request->offset(), request->size())) {
        response->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_INVALID_REQUEST);
        DVLOG(9) << "I/O request, op: " << request->optype()
                 << " offset: " << request->offset()
                 << " size: " << request->size()
                 << " max size: " << maxChunkSize_;
        return;
    }

    // Determine if the copyset exists
    auto nodePtr = copysetNodeManager_->GetCopysetNode(request->logicpoolid(),
                                                       request->copysetid());
    if (nullptr == nodePtr) {
        response->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_COPYSET_NOTEXIST);
        LOG(WARNING) << "write chunk failed, copyset node is not found:"
                     << request->logicpoolid() << "," << request->copysetid();
        return;
    }

    std::shared_ptr<WriteChunkRequest> req =
        std::make_shared<WriteChunkRequest>(nodePtr, controller, request,
                                            response, doneGuard.release());
    req->Process();
}

void ChunkServiceImpl::CreateCloneChunk(RpcController* controller,
                                        const ChunkRequest* request,
                                        ChunkResponse* response,
                                        Closure* done) {
    ChunkServiceClosure* closure = new (std::nothrow)
        ChunkServiceClosure(inflightThrottle_, request, response, done);
    CHECK(nullptr != closure) << "new chunk service closure failed";

    brpc::ClosureGuard doneGuard(closure);

    if (inflightThrottle_->IsOverLoad()) {
        response->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_OVERLOAD);
        LOG_EVERY_N(WARNING, 100)
            << "CreateCloneChunk: "
            << "too many inflight requests to process in chunkserver";
        return;
    }

    // The chunk size requested for creation does not match the size configured
    // for copyset
    if (request->size() != maxChunkSize_) {
        response->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_INVALID_REQUEST);
        DVLOG(9) << "Invalid chunk size: " << request->optype()
                 << " request size: " << request->size()
                 << " copyset size: " << maxChunkSize_;
        return;
    }

    // Determine if the copyset exists
    auto nodePtr = copysetNodeManager_->GetCopysetNode(request->logicpoolid(),
                                                       request->copysetid());
    if (nullptr == nodePtr) {
        response->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_COPYSET_NOTEXIST);
        LOG(WARNING) << "write chunk failed, copyset node is not found:"
                     << request->logicpoolid() << "," << request->copysetid();
        return;
    }

    std::shared_ptr<CreateCloneChunkRequest> req =
        std::make_shared<CreateCloneChunkRequest>(
            nodePtr, controller, request, response, doneGuard.release());
    req->Process();
}

void ChunkServiceImpl::CreateS3CloneChunk(
    RpcController* controller, const CreateS3CloneChunkRequest* request,
    CreateS3CloneChunkResponse* response, Closure* done) {
    (void)controller;
    (void)request;
    brpc::ClosureGuard doneGuard(done);
    response->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_INVALID_REQUEST);
    LOG(INFO) << "Invalid request, serverSide Not implement yet";
}

void ChunkServiceImpl::ReadChunk(RpcController* controller,
                                 const ChunkRequest* request,
                                 ChunkResponse* response, Closure* done) {
    ChunkServiceClosure* closure = new (std::nothrow)
        ChunkServiceClosure(inflightThrottle_, request, response, done);
    CHECK(nullptr != closure) << "new chunk service closure failed";

    brpc::ClosureGuard doneGuard(closure);

    if (inflightThrottle_->IsOverLoad()) {
        response->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_OVERLOAD);
        LOG_EVERY_N(WARNING, 100)
            << "ReadChunk: "
            << "too many inflight requests to process in chunkserver";
        return;
    }

    // Determine whether the request parameter is legal
    if (!CheckRequestOffsetAndLength(request->offset(), request->size())) {
        response->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_INVALID_REQUEST);
        LOG(ERROR) << "I/O request, op: " << request->optype()
                   << " offset: " << request->offset()
                   << " size: " << request->size()
                   << " max size: " << maxChunkSize_;
        return;
    }

    // Determine if the copyset exists
    auto nodePtr = copysetNodeManager_->GetCopysetNode(request->logicpoolid(),
                                                       request->copysetid());
    if (nullptr == nodePtr) {
        response->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_COPYSET_NOTEXIST);
        LOG(WARNING) << "read chunk failed, copyset node is not found:"
                     << request->logicpoolid() << "," << request->copysetid();
        return;
    }

    std::shared_ptr<ReadChunkRequest> req = std::make_shared<ReadChunkRequest>(
        nodePtr, chunkServiceOptions_.cloneManager, controller, request,
        response, doneGuard.release());
    req->Process();
}

void ChunkServiceImpl::RecoverChunk(RpcController* controller,
                                    const ChunkRequest* request,
                                    ChunkResponse* response, Closure* done) {
    ChunkServiceClosure* closure = new (std::nothrow)
        ChunkServiceClosure(inflightThrottle_, request, response, done);
    CHECK(nullptr != closure) << "new chunk service closure failed";

    brpc::ClosureGuard doneGuard(closure);

    if (inflightThrottle_->IsOverLoad()) {
        response->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_OVERLOAD);
        LOG_EVERY_N(WARNING, 100)
            << "RecoverChunk: "
            << "too many inflight requests to process in chunkserver";
        return;
    }

    // Determine whether the request parameter is legal
    if (!CheckRequestOffsetAndLength(request->offset(), request->size())) {
        response->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_INVALID_REQUEST);
        LOG(ERROR) << "I/O request, op: " << request->optype()
                   << " offset: " << request->offset()
                   << " size: " << request->size()
                   << " max size: " << maxChunkSize_;
        return;
    }

    // Determine if the copyset exists
    auto nodePtr = copysetNodeManager_->GetCopysetNode(request->logicpoolid(),
                                                       request->copysetid());
    if (nullptr == nodePtr) {
        response->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_COPYSET_NOTEXIST);
        LOG(WARNING) << "recover chunk failed, copyset node is not found:"
                     << request->logicpoolid() << "," << request->copysetid();
        return;
    }

    // RecoverChunk request and ReadChunk request share ReadChunkRequest
    std::shared_ptr<ReadChunkRequest> req = std::make_shared<ReadChunkRequest>(
        nodePtr, chunkServiceOptions_.cloneManager, controller, request,
        response, doneGuard.release());
    req->Process();
}

void ChunkServiceImpl::ReadChunkSnapshot(RpcController* controller,
                                         const ChunkRequest* request,
                                         ChunkResponse* response,
                                         Closure* done) {
    ChunkServiceClosure* closure = new (std::nothrow)
        ChunkServiceClosure(inflightThrottle_, request, response, done);
    CHECK(nullptr != closure) << "new chunk service closure failed";

    brpc::ClosureGuard doneGuard(closure);

    if (inflightThrottle_->IsOverLoad()) {
        response->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_OVERLOAD);
        LOG_EVERY_N(WARNING, 100)
            << "ReadChunkSnapshot: "
            << "too many inflight requests to process in chunkserver";
        return;
    }

    // Determine whether the request parameter is legal
    if (!CheckRequestOffsetAndLength(request->offset(), request->size())) {
        response->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_INVALID_REQUEST);
        return;
    }

    // Determine if the copyset exists
    auto nodePtr = copysetNodeManager_->GetCopysetNode(request->logicpoolid(),
                                                       request->copysetid());
    if (nullptr == nodePtr) {
        response->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_COPYSET_NOTEXIST);
        LOG(WARNING) << "read chunk snapshot failed, copyset node is not found:"
                     << request->logicpoolid() << "," << request->copysetid();
        return;
    }

    std::shared_ptr<ReadSnapshotRequest> req =
        std::make_shared<ReadSnapshotRequest>(nodePtr, controller, request,
                                              response, doneGuard.release());
    req->Process();
}

void ChunkServiceImpl::DeleteChunkSnapshotOrCorrectSn(
    RpcController* controller, const ChunkRequest* request,
    ChunkResponse* response, Closure* done) {
    ChunkServiceClosure* closure = new (std::nothrow)
        ChunkServiceClosure(inflightThrottle_, request, response, done);
    CHECK(nullptr != closure) << "new chunk service closure failed";

    brpc::ClosureGuard doneGuard(closure);

    if (inflightThrottle_->IsOverLoad()) {
        response->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_OVERLOAD);
        LOG_EVERY_N(WARNING, 100)
            << "DeleteChunkSnapshotOrCorrectSn: "
            << "too many inflight requests to process in chunkserver";
        return;
    }

    if (false == request->has_correctedsn()) {
        response->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_INVALID_REQUEST);
        LOG(ERROR) << "delete chunk snapshot failed, no corrected sn:"
                   << request->logicpoolid() << "," << request->copysetid();
        return;
    }

    // Determine if the copyset exists
    auto nodePtr = copysetNodeManager_->GetCopysetNode(request->logicpoolid(),
                                                       request->copysetid());
    if (nullptr == nodePtr) {
        response->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_COPYSET_NOTEXIST);
        LOG(WARNING) << "delete chunk snapshot failed, "
                     << "since copyset node is not found:"
                     << request->logicpoolid() << "," << request->copysetid();
        return;
    }

    std::shared_ptr<DeleteSnapshotRequest> req =
        std::make_shared<DeleteSnapshotRequest>(nodePtr, controller, request,
                                                response, doneGuard.release());

    req->Process();
}

/**
 * Currently, GetChunkInfo is defined in the rpc service layer and separated
 * from Chunk Service, And it does not go through QoS or raft consistency
 * protocols, so it is not allowed to inherit here OpRequest or QoSRequest to be
 * re encapsulated, but directly processed in place
 */
void ChunkServiceImpl::GetChunkInfo(RpcController* controller,
                                    const GetChunkInfoRequest* request,
                                    GetChunkInfoResponse* response,
                                    Closure* done) {
    (void)controller;
    ChunkServiceClosure* closure = new (std::nothrow)
        ChunkServiceClosure(inflightThrottle_, nullptr, nullptr, done);
    CHECK(nullptr != closure) << "new chunk service closure failed";

    brpc::ClosureGuard doneGuard(closure);

    if (inflightThrottle_->IsOverLoad()) {
        response->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_OVERLOAD);
        LOG_EVERY_N(WARNING, 100)
            << "GetChunkInfo: "
            << "too many inflight requests to process in chunkserver";
        return;
    }

    // Determine if the copyset exists
    auto nodePtr = copysetNodeManager_->GetCopysetNode(request->logicpoolid(),
                                                       request->copysetid());
    if (nullptr == nodePtr) {
        response->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_COPYSET_NOTEXIST);
        LOG(WARNING) << "GetChunkInfo failed, copyset node is not found: "
                     << request->logicpoolid() << "," << request->copysetid();
        return;
    }

    // Check tenure and whether you are a leader
    if (!nodePtr->IsLeaderTerm()) {
        PeerId leader = nodePtr->GetLeaderId();
        if (!leader.is_empty()) {
            response->set_redirect(leader.to_string());
        }
        response->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED);
        return;
    }

    CSErrorCode ret;
    CSChunkInfo chunkInfo;

    ret = nodePtr->GetDataStore()->GetChunkInfo(request->chunkid(), &chunkInfo);

    if (CSErrorCode::Success == ret) {
        // 1. Success, the chunk file must exist at this time
        response->add_chunksn(chunkInfo.curSn);
        if (chunkInfo.snapSn > 0) response->add_chunksn(chunkInfo.snapSn);
        response->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
    } else if (CSErrorCode::ChunkNotExistError == ret) {
        // 2. Chunk file does not exist, returned version set is empty
        response->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
    } else {
        // 3. Other errors
        LOG(ERROR) << "get chunk info failed, "
                   << " logic pool id: " << request->logicpoolid()
                   << " copyset id: " << request->copysetid()
                   << " chunk id: " << request->chunkid() << ", "
                   << " errno: " << errno << ", "
                   << " error message: " << strerror(errno)
                   << " data store return: " << ret;
        response->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN);
    }
}

void ChunkServiceImpl::GetChunkHash(RpcController* controller,
                                    const GetChunkHashRequest* request,
                                    GetChunkHashResponse* response,
                                    Closure* done) {
    (void)controller;
    brpc::ClosureGuard doneGuard(done);

    // Determine whether the request parameter is legal
    if (!CheckRequestOffsetAndLength(request->offset(), request->length())) {
        response->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_INVALID_REQUEST);
        LOG(ERROR) << "GetChunkHash illegal parameter:"
                   << " logic pool id: " << request->logicpoolid()
                   << " copyset id: " << request->copysetid()
                   << " chunk id: " << request->chunkid() << ", "
                   << " offset: " << request->offset()
                   << " length: " << request->length()
                   << " max size: " << maxChunkSize_;
        return;
    }

    // Determine if the copyset exists
    auto nodePtr = copysetNodeManager_->GetCopysetNode(request->logicpoolid(),
                                                       request->copysetid());
    if (nullptr == nodePtr) {
        response->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_COPYSET_NOTEXIST);
        LOG(WARNING) << "GetChunkHash failed, copyset node is not found: "
                     << request->logicpoolid() << "," << request->copysetid();
        return;
    }

    CSErrorCode ret;
    std::string hash;

    ret = nodePtr->GetDataStore()->GetChunkHash(
        request->chunkid(), request->offset(), request->length(), &hash);

    if (CSErrorCode::Success == ret) {
        // 1. Success
        response->set_hash(hash);
        response->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
    } else if (CSErrorCode::ChunkNotExistError == ret) {
        // 2. Chunk file does not exist, return a hash value of 0
        response->set_hash("0");
        response->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
    } else {
        // 3. Other errors
        LOG(ERROR) << "get chunk hash failed, "
                   << " logic pool id: " << request->logicpoolid()
                   << " copyset id: " << request->copysetid()
                   << " chunk id: " << request->chunkid() << ", "
                   << " errno: " << errno << ", "
                   << " error message: " << strerror(errno)
                   << " data store return: " << ret;
        response->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN);
    }
}

void ChunkServiceImpl::UpdateEpoch(RpcController* controller,
                                   const UpdateEpochRequest* request,
                                   UpdateEpochResponse* response,
                                   Closure* done) {
    (void)controller;
    brpc::ClosureGuard doneGuard(done);
    bool success = epochMap_->UpdateEpoch(request->fileid(), request->epoch());
    if (success) {
        response->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        LOG(INFO) << "Update fileId: " << request->fileid()
                  << " to epoch: " << request->epoch() << " success.";
    } else {
        response->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_EPOCH_TOO_OLD);
        LOG(WARNING) << "Update fileId: " << request->fileid()
                     << " to epoch: " << request->epoch()
                     << " failed, epoch too old.";
    }
}

bool ChunkServiceImpl::CheckRequestOffsetAndLength(uint32_t offset,
                                                   uint32_t len) const {
    // Check if offset+len is out of range
    if (CURVE_UNLIKELY(offset + len > maxChunkSize_)) {
        return false;
    }

    return is_aligned(offset, blockSize_) && is_aligned(len, blockSize_);
}

}  // namespace chunkserver
}  // namespace curve
