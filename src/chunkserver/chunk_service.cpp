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
#include <vector>

#include "src/chunkserver/copyset_node.h"
#include "src/chunkserver/copyset_node_manager.h"
#include "src/chunkserver/op_request.h"

namespace curve {
namespace chunkserver {

ChunkServiceImpl::ChunkServiceImpl(ChunkServiceOptions chunkServiceOptions) :
    chunkServiceOptions_(chunkServiceOptions),
    copysetNodeManager_(chunkServiceOptions.copysetNodeManager) {
    maxChunkSize_ = copysetNodeManager_->GetCopysetNodeOptions().maxChunkSize;
}

void ChunkServiceImpl::DeleteChunk(RpcController *controller,
                                   const ChunkRequest *request,
                                   ChunkResponse *response,
                                   Closure *done) {
    brpc::ClosureGuard doneGuard(done);

    // 判断copyset是否存在
    auto nodePtr = copysetNodeManager_->GetCopysetNode(request->logicpoolid(),
                                                       request->copysetid());
    if (nullptr == nodePtr) {
        response->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_COPYSET_NOTEXIST);
        LOG(ERROR) << "delete chunk failed, copyset node is not found:"
                   << request->logicpoolid() << "," << request->copysetid();
        return;
    }

    std::shared_ptr<DeleteChunkRequest>
        req = std::make_shared<DeleteChunkRequest>(nodePtr,
                                                   controller,
                                                   request,
                                                   response,
                                                   doneGuard.release());
    req->Process();
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

    // 判断request参数是否合法
    if (!CheckRequestOffsetAndLength(request->offset(), request->size())) {
        response->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_INVALID_REQUEST);
        DVLOG(9) << "I/O request, op: " << request->optype()
                 << " offset: " << request->offset()
                 << " size: " << request->size()
                 << " max size: " << maxChunkSize_;
        return;
    }

    // 判断copyset是否存在
    auto nodePtr = copysetNodeManager_->GetCopysetNode(request->logicpoolid(),
                                                       request->copysetid());
    if (nullptr == nodePtr) {
        response->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_COPYSET_NOTEXIST);
        LOG(ERROR) << "write chunk failed, copyset node is not found:"
                   << request->logicpoolid() << "," << request->copysetid();
        return;
    }

    std::shared_ptr<WriteChunkRequest>
        req = std::make_shared<WriteChunkRequest>(nodePtr,
                                                  controller,
                                                  request,
                                                  response,
                                                  doneGuard.release());
    req->Process();
}

void ChunkServiceImpl::CreateCloneChunk(RpcController *controller,
                                        const ChunkRequest *request,
                                        ChunkResponse *response,
                                        Closure *done) {
    brpc::ClosureGuard doneGuard(done);

    // 请求创建的chunk大小和copyset配置的大小不一致
    if (request->size() != maxChunkSize_) {
        response->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_INVALID_REQUEST);
        DVLOG(9) << "Invalid chunk size: " << request->optype()
                 << " request size: " << request->size()
                 << " copyset size: " << maxChunkSize_;
        return;
    }

    // 判断copyset是否存在
    auto nodePtr = copysetNodeManager_->GetCopysetNode(request->logicpoolid(),
                                                       request->copysetid());
    if (nullptr == nodePtr) {
        response->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_COPYSET_NOTEXIST);
        LOG(ERROR) << "write chunk failed, copyset node is not found:"
                   << request->logicpoolid() << "," << request->copysetid();
        return;
    }

    std::shared_ptr<CreateCloneChunkRequest>
        req = std::make_shared<CreateCloneChunkRequest>(nodePtr,
                                                        controller,
                                                        request,
                                                        response,
                                                        doneGuard.release());
    req->Process();
}

void ChunkServiceImpl::ReadChunk(RpcController *controller,
                                 const ChunkRequest *request,
                                 ChunkResponse *response,
                                 Closure *done) {
    brpc::ClosureGuard doneGuard(done);

    // 判断request参数是否合法
    auto maxSize = copysetNodeManager_->GetCopysetNodeOptions().maxChunkSize;
    if (!CheckRequestOffsetAndLength(request->offset(), request->size())) {
        response->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_INVALID_REQUEST);
        LOG(ERROR) << "I/O request, op: " << request->optype()
                   << " offset: " << request->offset()
                   << " size: " << request->size()
                   << " max size: " << maxChunkSize_;
        return;
    }

    // 判断copyset是否存在
    auto nodePtr = copysetNodeManager_->GetCopysetNode(request->logicpoolid(),
                                                       request->copysetid());
    if (nullptr == nodePtr) {
        response->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_COPYSET_NOTEXIST);
        LOG(ERROR) << "read chunk failed, copyset node is not found:"
                   << request->logicpoolid() << "," << request->copysetid();
        return;
    }

    std::shared_ptr<ReadChunkRequest> req =
        std::make_shared<ReadChunkRequest>(nodePtr,
                                           chunkServiceOptions_.cloneManager,
                                           controller,
                                           request,
                                           response,
                                           doneGuard.release());
    req->Process();
}

void ChunkServiceImpl::RecoverChunk(RpcController *controller,
                                    const ChunkRequest *request,
                                    ChunkResponse *response,
                                    Closure *done) {
    brpc::ClosureGuard doneGuard(done);

    // 判断request参数是否合法
    if (!CheckRequestOffsetAndLength(request->offset(), request->size())) {
        response->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_INVALID_REQUEST);
        LOG(ERROR) << "I/O request, op: " << request->optype()
                   << " offset: " << request->offset()
                   << " size: " << request->size()
                   << " max size: " << maxChunkSize_;
        return;
    }

    // 判断copyset是否存在
    auto nodePtr = copysetNodeManager_->GetCopysetNode(request->logicpoolid(),
                                                       request->copysetid());
    if (nullptr == nodePtr) {
        response->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_COPYSET_NOTEXIST);
        LOG(ERROR) << "recover chunk failed, copyset node is not found:"
                   << request->logicpoolid() << "," << request->copysetid();
        return;
    }

    // RecoverChunk请求和ReadChunk请求共用ReadChunkRequest
    std::shared_ptr<ReadChunkRequest> req =
        std::make_shared<ReadChunkRequest>(nodePtr,
                                           chunkServiceOptions_.cloneManager,
                                           controller,
                                           request,
                                           response,
                                           doneGuard.release());
    req->Process();
}

void ChunkServiceImpl::ReadChunkSnapshot(RpcController *controller,
                                         const ChunkRequest *request,
                                         ChunkResponse *response,
                                         Closure *done) {
    brpc::ClosureGuard doneGuard(done);

    // 判断request参数是否合法
    if (!CheckRequestOffsetAndLength(request->offset(), request->size())) {
        response->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_INVALID_REQUEST);
        return;
    }

    // 判断copyset是否存在
    auto nodePtr = copysetNodeManager_->GetCopysetNode(request->logicpoolid(),
                                                       request->copysetid());
    if (nullptr == nodePtr) {
        response->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_COPYSET_NOTEXIST);
        LOG(ERROR) << "read chunk snapshot failed, copyset node is not found:"
                   << request->logicpoolid() << "," << request->copysetid();
        return;
    }

    std::shared_ptr<ReadSnapshotRequest>
        req = std::make_shared<ReadSnapshotRequest>(nodePtr,
                                                    controller,
                                                    request,
                                                    response,
                                                    doneGuard.release());
    req->Process();
}

void ChunkServiceImpl::DeleteChunkSnapshotOrCorrectSn(
    RpcController *controller,
    const ChunkRequest *request,
    ChunkResponse *response,
    Closure *done) {
    brpc::ClosureGuard doneGuard(done);

    if (false == request->has_correctedsn()) {
        response->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_INVALID_REQUEST);
        LOG(ERROR) << "delete chunk snapshot failed, no corrected sn:"
                   << request->logicpoolid() << "," << request->copysetid();
        return;
    }

    // 判断copyset是否存在
    auto nodePtr = copysetNodeManager_->GetCopysetNode(request->logicpoolid(),
                                                       request->copysetid());
    if (nullptr == nodePtr) {
        response->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_COPYSET_NOTEXIST);
        LOG(ERROR) << "delete chunk snapshot failed, copyset node is not found:"
                   << request->logicpoolid() << "," << request->copysetid();
        return;
    }

    std::shared_ptr<DeleteSnapshotRequest>
        req = std::make_shared<DeleteSnapshotRequest>(nodePtr,
                                                      controller,
                                                      request,
                                                      response,
                                                      doneGuard.release());

    req->Process();
}

/**
 * 当前GetChunkInfo在rpc service层定义和Chunk Service分离的，
 * 且其并不经过QoS或者raft一致性协议，所以这里没有让其继承
 * OpRequest或者QoSRequest来重新封装，而是直接原地处理掉了
 */
void ChunkServiceImpl::GetChunkInfo(RpcController *controller,
                                    const GetChunkInfoRequest *request,
                                    GetChunkInfoResponse *response,
                                    Closure *done) {
    brpc::ClosureGuard doneGuard(done);

    // 判断copyset是否存在
    auto nodePtr =
        copysetNodeManager_->GetCopysetNode(request->logicpoolid(),
                                            request->copysetid());
    if (nullptr == nodePtr) {
        response->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_COPYSET_NOTEXIST);
        LOG(ERROR) << "GetChunkInfo failed, copyset node is not found: "
                   << request->logicpoolid() << "," << request->copysetid();
        return;
    }

    // 检查任期和自己是不是Leader
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
        // 1.成功，此时chunk文件肯定存在
        response->add_chunksn(chunkInfo.curSn);
        if (chunkInfo.snapSn > 0)
            response->add_chunksn(chunkInfo.snapSn);
        response->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
    } else if (CSErrorCode::ChunkNotExistError == ret) {
        // 2.chunk文件不存在，返回的版本集合为空
        response->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
    } else {
        // 3.其他错误
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

bool ChunkServiceImpl::CheckRequestOffsetAndLength(uint32_t offset,
                                                   uint32_t len) {
    // 检查offset+len是否越界
    if (offset + len > maxChunkSize_) {
        return false;
    }

    // 检查offset是否对齐
    if (offset % kOpRequestAlignSize != 0) {
        return false;
    }

    // 检查len是否对齐
    if (len % kOpRequestAlignSize != 0) {
        return false;
    }

    return true;
}

}  // namespace chunkserver
}  // namespace curve
