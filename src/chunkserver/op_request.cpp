/*
 * Project: curve
 * Created Date: 18-8-23
 * Author: wudemiao
 * Copyright (c) 2018 netease
 */

#include "src/chunkserver/op_request.h"

#include <brpc/controller.h>
#include <butil/sys_byteorder.h>
#include <brpc/closure_guard.h>

#include <memory>

#include "src/chunkserver/copyset_node.h"
#include "src/chunkserver/copyset_node_manager.h"
#include "src/chunkserver/chunk_closure.h"

namespace curve {
namespace chunkserver {

// Chunk Op 具体的调度逻辑还是由 copyset node manager 来决定，
// 因为后期的 thread pool 归 copyset manager 管理
void ChunkOpRequest::Schedule() {
    copysetNodeManager_->ScheduleRequest(shared_from_this());
}

int ChunkOpRequest::Encode(butil::IOBuf *data) {
    if (nullptr == data) {
        return -1;
    }

    // 1. append RequestType
    data->push_back((uint8_t) RequestType::CHUNK_OP);

    // 2. append op request
    const uint32_t metaSize = butil::HostToNet32(request_->ByteSize());
    data->append(&metaSize, sizeof(uint32_t));

    butil::IOBufAsZeroCopyOutputStream wrapper(data);
    if (!request_->SerializeToZeroCopyStream(&wrapper)) {
        LOG(ERROR) << "Fail to serialize request";
        response_->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN);
        return -1;
    }

    // 3. append op data
    brpc::Controller *cntl = dynamic_cast<brpc::Controller *>(cntl_);
    if (CHUNK_OP_TYPE::CHUNK_OP_WRITE == request_->optype()) {
        data->append(cntl->request_attachment());
    }

    return 0;
}

void ChunkOpRequest::Process() {
    brpc::ClosureGuard doneGuard(done_);

    LOG(ERROR) << "before get";
    std::shared_ptr<CopysetNode>
        nodePtr = copysetNodeManager_->GetCopysetNode(request_->logicpoolid(), request_->copysetid());
    LOG(ERROR) << "after get";
    CHECK(nullptr != nodePtr);

    switch (request_->optype()) {
        case CHUNK_OP_TYPE::CHUNK_OP_READ:
            nodePtr->ReadChunk(cntl_, request_, response_, doneGuard.release());
            break;
        case CHUNK_OP_TYPE::CHUNK_OP_WRITE:
            nodePtr->WriteChunk(cntl_, request_, response_, doneGuard.release());
            break;
        case CHUNK_OP_TYPE::CHUNK_OP_DELETE:
            nodePtr->DeleteChunk(cntl_, request_, response_, doneGuard.release());
            break;
        default:
            response_->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_INVALID_REQUEST);
            LOG(ERROR) << "UNKNOWN Chunk Op";
    }
}

int ChunkOpRequest::OnApply(std::shared_ptr<CopysetNode> copysetNode) {
    int ret = 0;
    size_t size;
    brpc::Controller *cntl = dynamic_cast<brpc::Controller *>(cntl_);
    switch (request_->optype()) {
        case CHUNK_OP_TYPE::CHUNK_OP_READ: {
            char *readBuffer = nullptr;
            size = request_->size();
            readBuffer = new(std::nothrow)char[size];  // FixMe(wudemiao) 是否有更好的方式
            memset(readBuffer, 0, size);
            if (nullptr == readBuffer) {
                LOG(FATAL) << "new readBuffer failed, error: " << strerror(errno);
                ret = -1;
                break;
            }
            if (true == copysetNode->dataStore_->ReadChunk(request_->chunkid(),
                                                           readBuffer,
                                                           request_->offset(),
                                                           &size)) {
                cntl->response_attachment().append(readBuffer, size);
                response_->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
            } else {
                LOG(ERROR) << "read: (" << request_->logicpoolid() << ", " << request_->copysetid()
                           << ") chunkid: " << request_->chunkid() << " data size: " << request_->size()
                           << " error, read len :" << size << ", error: " << strerror(errno);
                response_->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN);
                ret = -1;
            }
            if (nullptr != readBuffer) {
                delete[] readBuffer;
            }
            break;
        }
        case CHUNK_OP_TYPE::CHUNK_OP_WRITE:
            if (true == copysetNode->dataStore_->WriteChunk(request_->chunkid(),
                                                            cntl->request_attachment().to_string().c_str(),
                                                            request_->offset(),
                                                            request_->size())) {
                response_->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
            } else {
                LOG(FATAL) << "write: (" << request_->logicpoolid() << ", " << request_->copysetid()
                           << ") chunkid: " << request_->chunkid() << " data size: " << request_->size()
                           << ", error: " << strerror(errno);
                response_->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN);
                ret = -1;
            }
            break;
        case CHUNK_OP_TYPE::CHUNK_OP_DELETE:
            if (true == copysetNode->dataStore_->DeleteChunk(request_->chunkid())) {
                response_->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
            } else {
                LOG(ERROR) << "delete: (" << request_->logicpoolid() << ", " << request_->copysetid()
                           << ") chunkid: " << request_->chunkid() << ", error: " << strerror(errno);
                response_->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN);
                ret = -1;
            }
            break;
        default:
            response_->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_INVALID_REQUEST);
            LOG(ERROR) << "Unknown chunk op, ";
            ret = -1;
    }

    return ret;
}

void ChunkOpRequest::OnApply(std::shared_ptr<CopysetNode> copysetNode, butil::IOBuf *log) {
    // 反序列化
    uint32_t metaSize = 0;
    log->cutn(&metaSize, sizeof(uint32_t));
    metaSize = butil::NetToHost32(metaSize);

    butil::IOBuf meta;
    log->cutn(&meta, metaSize);
    butil::IOBufAsZeroCopyInputStream wrapper(meta);
    ChunkRequest request;
    CHECK(request.ParseFromZeroCopyStream(&wrapper));   // FixMe: 这样处理够健壮吗？
    butil::IOBuf data;
    data.swap(*log);

    switch (request.optype()) {
        case CHUNK_OP_TYPE::CHUNK_OP_READ:
            // read 直接忽略不处理
            break;
        case CHUNK_OP_TYPE::CHUNK_OP_WRITE:
            if (false == copysetNode->dataStore_->WriteChunk(request.chunkid(),
                                                             data.to_string().c_str(),
                                                             request.offset(),
                                                             request.size())) {
                LOG(ERROR) << "write: (" << request.logicpoolid() << ", " << request.copysetid()
                           << ") chunkid: " << request.chunkid() << " data size: " << request.size()
                           << ", error: " << strerror(errno);
            }
            break;
        case CHUNK_OP_TYPE::CHUNK_OP_DELETE:
            // 日志回放存在重复删除的可能性
            if (false == copysetNode->dataStore_->DeleteChunk(request.chunkid())) {
                LOG(ERROR) << "delete: (" << request.logicpoolid() << ", " << request.copysetid()
                           << ") chunkid: " << request.chunkid() << ", error: " << strerror(errno);
            }
            break;
        default:
            LOG(ERROR) << "Unknown chunk op";
            return;
    }
}

void ChunkSnapshotOpRequest::Schedule() {
    copysetNodeManager_->ScheduleRequest(shared_from_this());
}

void ChunkSnapshotOpRequest::Process() {
    brpc::ClosureGuard doneGuard(done_);

    LogicPoolID logicPoolId = request_->logicpoolid();
    CopysetID copysetId = request_->copysetid();
    std::shared_ptr<CopysetNode> nodePtr = copysetNodeManager_->GetCopysetNode(logicPoolId, copysetId);
    if (nullptr == nodePtr) {
        response_->set_status(CHUNK_SNAPSHOT_OP_STATUS::CHUNK_SNAPSHOT_OP_STATUS_INVALID_REQUEST);
        LOG(ERROR) << "通过<" << logicPoolId << "," << copysetId << ">, 找不到相应的 copyset node";
        return;
    }

    switch (request_->optype()) {
        case CHUNK_SNAPSHOT_OP_TYPE::CHUNK_SNAPSHOT_OP_READ:
            nodePtr->ReadChunkSnapshot(cntl_,
                                       request_,
                                       response_,
                                       doneGuard.release());
            break;
        case CHUNK_SNAPSHOT_OP_TYPE::CHUNK_SNAPSHOT_OP_CREATE:
            nodePtr->CreateChunkSnapshot(cntl_,
                                         request_,
                                         response_,
                                         doneGuard.release());
            break;
        case CHUNK_SNAPSHOT_OP_TYPE::CHUNK_SNAPSHOT_OP_DELETE:
            nodePtr->DeleteChunkSnapshot(cntl_,
                                         request_,
                                         response_,
                                         doneGuard.release());
            break;
        default:
            response_->set_status(CHUNK_SNAPSHOT_OP_STATUS::CHUNK_SNAPSHOT_OP_STATUS_INVALID_REQUEST);
            LOG(ERROR) << "UNKNOWN Chunk snapshot Op";
    }
}

int ChunkSnapshotOpRequest::OnApply(std::shared_ptr<CopysetNode> copysetNode) {
    switch (request_->optype()) {
        case CHUNK_SNAPSHOT_OP_TYPE::CHUNK_SNAPSHOT_OP_READ:
            // 读
            break;
        case CHUNK_SNAPSHOT_OP_TYPE::CHUNK_SNAPSHOT_OP_CREATE:
            // 创建
            break;
        case CHUNK_SNAPSHOT_OP_TYPE::CHUNK_SNAPSHOT_OP_DELETE:
            // 删除
            break;
        default:
            LOG(FATAL) << "Unknown chunk op";
            return -1;
    }
    return 0;
}

int ChunkSnapshotOpRequest::Encode(butil::IOBuf *data) {
    if (nullptr == data) {
        return -1;
    }

    // 1. append RequestType
    data->push_back((uint8_t) RequestType::CHUNK_SNAPSHOT_OP);

    // 2. append op request
    const uint32_t metaSize = butil::HostToNet32(request_->ByteSize());
    data->append(&metaSize, sizeof(uint32_t));
    butil::IOBufAsZeroCopyOutputStream wrapper(data);
    if (!request_->SerializeToZeroCopyStream(&wrapper)) {
        LOG(ERROR) << "Fail to serialize request";
        response_->set_status(CHUNK_SNAPSHOT_OP_STATUS::CHUNK_SNAPSHOT_OP_STATUS_FAILURE_UNKNOWN);
        return -1;
    }

    return 0;
}

void ChunkSnapshotOpRequest::OnApply(std::shared_ptr<CopysetNode> copysetNode, butil::IOBuf *log) {
    uint32_t metaSize = 0;
    log->cutn(&metaSize, sizeof(uint32_t));
    metaSize = butil::NetToHost32(metaSize);

    butil::IOBuf meta;
    log->cutn(&meta, metaSize);
    butil::IOBufAsZeroCopyInputStream wrapper(meta);
    ChunkSnapshotRequest request;
    CHECK(request.ParseFromZeroCopyStream(&wrapper));   // FixMe: 这样处理够健壮吗？
    butil::IOBuf data;
    data.swap(*log);

    switch (request.optype()) {
        case CHUNK_SNAPSHOT_OP_TYPE::CHUNK_SNAPSHOT_OP_READ:
            // 读，如果发现 crc 不一致，需要返回 client，
            LOG(INFO) << "read: (" << request.logicpoolid() << ", " << request.copysetid()
                      << ") chunkid: " << request.chunkid() << " snapshot id: " << request.snapshotid()
                      << " data size: " << request.size();
            break;
        case CHUNK_SNAPSHOT_OP_TYPE::CHUNK_SNAPSHOT_OP_CREATE:
            // 创建
            LOG(INFO) << "create: (" << request.logicpoolid() << ", " << request.copysetid()
                      << ") chunkid: " << request.chunkid() << " snapshot id: " << request.snapshotid();
            break;
        case CHUNK_SNAPSHOT_OP_TYPE::CHUNK_SNAPSHOT_OP_DELETE:
            // 删除
            LOG(INFO) << "delete: (" << request.logicpoolid() << ", " << request.copysetid()
                      << ") chunkid: " << request.chunkid() << " snapshot id: " << request.snapshotid();
            break;
        default:
            LOG(ERROR) << "Unknown chunk op";
            return;
    }
}

}  // namespace chunkserver
}  // namespace curve
