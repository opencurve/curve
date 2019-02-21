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
#include "src/chunkserver/chunk_closure.h"

namespace curve {
namespace chunkserver {

ChunkOpRequest::ChunkOpRequest() :
    datastore_(nullptr),
    node_(nullptr),
    cntl_(nullptr),
    request_(nullptr),
    response_(nullptr),
    done_(nullptr) {}

ChunkOpRequest::ChunkOpRequest(std::shared_ptr<CopysetNode> nodePtr,
                               RpcController *cntl,
                               const ChunkRequest *request,
                               ChunkResponse *response,
                               ::google::protobuf::Closure *done) :
    datastore_(nodePtr->GetDataStore()),
    node_(nodePtr),
    cntl_(dynamic_cast<brpc::Controller *>(cntl)),
    request_(request),
    response_(response),
    done_(done) {}

void ChunkOpRequest::Process() {
    brpc::ClosureGuard doneGuard(done_);
    /**
     * 如果propose成功，说明request成功交给了raft处理，
     * 那么done_就不能被调用，只有propose失败了才需要提前返回
     */
    if (0 == Propose()) {
        doneGuard.release();
    }
}

int ChunkOpRequest::Propose() {
    // 检查任期和自己是不是Leader
    if (!node_->IsLeaderTerm()) {
        RedirectChunkRequest();
        return -1;
    }
    // 打包op request为task
    braft::Task task;
    butil::IOBuf log;

    if (0 != Encode(cntl_, request_, &log)) {
        LOG(ERROR) << "chunk op request encode failure";
        response_->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN);
        return -1;
    }
    task.data = &log;
    task.done = new ChunkClosure(shared_from_this());

    node_->Propose(task);

    return 0;
}

void ChunkOpRequest::RedirectChunkRequest() {
    PeerId leader = node_->GetLeaderId();
    if (!leader.is_empty()) {
        response_->set_redirect(leader.to_string());
    }
    response_->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED);
}

int ChunkOpRequest::Encode(brpc::Controller *cntl,
                           const ChunkRequest *request,
                           butil::IOBuf *data) {
    // 1.append request length
    const uint32_t metaSize = butil::HostToNet32(request->ByteSize());
    data->append(&metaSize, sizeof(uint32_t));

    // 2.append op request
    butil::IOBufAsZeroCopyOutputStream wrapper(data);
    if (!request->SerializeToZeroCopyStream(&wrapper)) {
        LOG(ERROR) << "Fail to serialize request";
        return -1;
    }

    // 3.append op data
    if (CHUNK_OP_TYPE::CHUNK_OP_WRITE == request->optype()) {
        data->append(cntl->request_attachment());
    }

    return 0;
}

std::shared_ptr<ChunkOpRequest> ChunkOpRequest::Decode(butil::IOBuf log,
                                                       ChunkRequest *request,
                                                       butil::IOBuf *data) {
    uint32_t metaSize = 0;
    log.cutn(&metaSize, sizeof(uint32_t));
    metaSize = butil::NetToHost32(metaSize);

    butil::IOBuf meta;
    log.cutn(&meta, metaSize);
    butil::IOBufAsZeroCopyInputStream wrapper(meta);
    bool ret = request->ParseFromZeroCopyStream(&wrapper);
    if (false == ret) {
        LOG(ERROR) << "failed deserialize";
        return nullptr;
    }
    data->swap(log);

    switch (request->optype()) {
        case CHUNK_OP_TYPE::CHUNK_OP_READ:
            return std::make_shared<ReadChunkRequest>();
        case CHUNK_OP_TYPE::CHUNK_OP_WRITE:
            return std::make_shared<WriteChunkRequest>();
        case CHUNK_OP_TYPE::CHUNK_OP_DELETE:
            return std::make_shared<DeleteChunkRequest>();
        case CHUNK_OP_TYPE::CHUNK_OP_READ_SNAP:
            return std::make_shared<ReadSnapshotRequest>();
        case CHUNK_OP_TYPE::CHUNK_OP_DELETE_SNAP:
            return std::make_shared<DeleteChunkRequest>();
        default:LOG(ERROR) << "Unknown chunk op";
            return nullptr;
    }
}

void ReadChunkRequest::Process() {
    brpc::ClosureGuard doneGuard(done_);

    if (!node_->IsLeaderTerm()) {
        RedirectChunkRequest();
        return;
    }
    /**
     * 如果携带了applied index，且小于当前copyset node
     * 的最新applied index，那么read
     */
    if (request_->has_appliedindex() &&
        node_->GetAppliedIndex() >= request_->appliedindex()) {
        char *readBuffer = nullptr;
        size_t size = request_->size();

        readBuffer = new(std::nothrow)char[size];
        CHECK(nullptr != readBuffer)
        << "new readBuffer failed " << strerror(errno);

        auto ret = datastore_->ReadChunk(request_->chunkid(),
                                         request_->sn(),
                                         readBuffer,
                                         request_->offset(),
                                         size);
        do {
            /**
             * 1.成功
             */
            if (CSErrorCode::Success == ret) {
                cntl_->response_attachment().append(readBuffer, size);
                response_->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
                DVLOG(9) << "read success: "
                         << " logic pool id: " << request_->logicpoolid()
                         << " copyset id: " << request_->copysetid()
                         << " chunkid: " << request_->chunkid()
                         << " data size: " << request_->size()
                         << " read len :" << size;
                break;
            }

            /**
             * 2.chunk not exist
             */
            if (CSErrorCode::ChunkNotExistError == ret) {
                response_->set_status(
                    CHUNK_OP_STATUS::CHUNK_OP_STATUS_CHUNK_NOTEXIST);
                break;
            }

            /**
             * 3.其他错误
             */
            LOG(ERROR) << "read failed: "
                       << " logic pool id: " << request_->logicpoolid()
                       << " copyset id: " << request_->copysetid()
                       << " chunkid: " << request_->chunkid()
                       << " data size: " << request_->size()
                       << " read len :" << size
                       << " error: " << strerror(errno)
                       << " data store return: " << ret;
            response_->set_status(
                CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN);
        } while (0);

        delete[] readBuffer;
        response_->set_appliedindex(node_->GetAppliedIndex());
        return;
    }

    /**
     * 如果没有携带applied index，那么走raft一致性协议read
     */
    if (0 == Propose()) {
        doneGuard.release();
    }
}

void DeleteChunkRequest::OnApply(uint64_t index,
                                 ::google::protobuf::Closure *done) {
    brpc::ClosureGuard doneGuard(done);

    auto ret = datastore_->DeleteChunk(request_->chunkid(),
                                       request_->sn());
    if (CSErrorCode::Success == ret) {
        response_->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        node_->UpdateAppliedIndex(index);
    } else {
        LOG(ERROR) << "delete chunk failed: "
                   << " logic pool id: " << request_->logicpoolid()
                   << " copyset id: " << request_->copysetid()
                   << " chunkid: " << request_->chunkid()
                   << " error: " << strerror(errno)
                   << " data store return: " << ret;
        response_->set_status(
            CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN);
    }
    auto maxIndex =
        (index > node_->GetAppliedIndex() ? index : node_->GetAppliedIndex());
    response_->set_appliedindex(maxIndex);
}

void DeleteChunkRequest::OnApplyFromLog(std::shared_ptr<CSDataStore> datastore,
                                        const ChunkRequest &request,
                                        const butil::IOBuf &data) {
    auto ret = datastore->DeleteChunk(request.chunkid(),
                                      request.sn());
    if (CSErrorCode::Success != ret) {
        LOG(WARNING) << "delete failed: "
                     << request.logicpoolid() << ", "
                     << request.copysetid()
                     << " chunkid: " << request.chunkid()
                     << " error: " << strerror(errno)
                     << " data store return: " << ret;
    }
}

void ReadChunkRequest::OnApply(uint64_t index,
                               ::google::protobuf::Closure *done) {
    brpc::ClosureGuard doneGuard(done);

    char *readBuffer = nullptr;
    uint32_t size = request_->size();
    readBuffer = new(std::nothrow)char[size];
    CHECK(nullptr != readBuffer) << "new readBuffer failed, "
                                 << errno << ":" << strerror(errno);
    auto ret = datastore_->ReadChunk(request_->chunkid(),
                                     request_->sn(),
                                     readBuffer,
                                     request_->offset(),
                                     size);
    do {
        /**
         * 1.成功
         */
        if (CSErrorCode::Success == ret) {
            cntl_->response_attachment().append(readBuffer, size);
            response_->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
            DVLOG(9) << "read success: "
                     << " logic pool id: " << request_->logicpoolid()
                     << " copyset id: " << request_->copysetid()
                     << " chunkid: " << request_->chunkid()
                     << " data size: " << request_->size()
                     << " read len :" << size;
            node_->UpdateAppliedIndex(index);
            break;
        }

        /**
         * 2.chunk not exist
         */
        if (CSErrorCode::ChunkNotExistError == ret) {
            response_->set_status(
                CHUNK_OP_STATUS::CHUNK_OP_STATUS_CHUNK_NOTEXIST);
            break;
        }

        /**
         * 3.其他错误
         */
        LOG(ERROR) << "read failed failed: "
                   << " logic pool id: " << request_->logicpoolid()
                   << " copyset id: " << request_->copysetid()
                   << " chunkid: " << request_->chunkid()
                   << " data size: " << request_->size()
                   << " read len :" << size
                   << " error: " << strerror(errno)
                   << " data store return: " << ret;
        response_->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN);
    } while (0);

    auto maxIndex =
        (index > node_->GetAppliedIndex() ? index : node_->GetAppliedIndex());
    response_->set_appliedindex(maxIndex);
    delete[] readBuffer;
}

void ReadChunkRequest::OnApplyFromLog(std::shared_ptr<CSDataStore> datastore,
                                      const ChunkRequest &request,
                                      const butil::IOBuf &data) {
    // read什么都不用做
}

void WriteChunkRequest::OnApply(uint64_t index,
                                ::google::protobuf::Closure *done) {
    brpc::ClosureGuard doneGuard(done);
    uint32_t cost;

    auto ret = datastore_->WriteChunk(request_->chunkid(),
                                      request_->sn(),
                                      cntl_->request_attachment().to_string().c_str(),  //NOLINT
                                      request_->offset(),
                                      request_->size(),
                                      &cost);
    /**
     * 1.成功
     */
    if (CSErrorCode::Success == ret) {
        response_->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        DVLOG(9) << "write success : "
                 << " logic pool id: " << request_->logicpoolid()
                 << " copyset id: " << request_->copysetid()
                 << " chunkid: " << request_->chunkid()
                 << " data size: " << request_->size();
        node_->UpdateAppliedIndex(index);
    } else {
        /**
         * 2.其他错误
         * TODO(wudemiao): 当前遇到write错误直接fatal退出整个
         * ChunkServer后期考虑仅仅标坏这个copyset，保证较好的可用性
        */
        LOG(FATAL) << "write failed: "
                   << " logic pool id: " << request_->logicpoolid()
                   << " copyset id: " << request_->copysetid()
                   << " chunkid: " << request_->chunkid()
                   << " data size: " << request_->size()
                   << " error: " << strerror(errno)
                   << " data store return: " << ret;
        response_->set_status(
            CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN);
    }
    auto maxIndex =
        (index > node_->GetAppliedIndex() ? index : node_->GetAppliedIndex());
    response_->set_appliedindex(maxIndex);
}

void WriteChunkRequest::OnApplyFromLog(std::shared_ptr<CSDataStore> datastore,
                                       const ChunkRequest &request,
                                       const butil::IOBuf &data) {
    uint32_t cost;
    auto ret = datastore->WriteChunk(request.chunkid(),
                                     request.sn(),
                                     data.to_string().c_str(),
                                     request.offset(),
                                     request.size(),
                                     &cost);
    if (CSErrorCode::Success != ret) {
        LOG(FATAL) << "write failed: " <<
                   request.logicpoolid() << ", " << request.copysetid()
                   << " chunkid: " << request.chunkid()
                   << " data size: " << request.size()
                   << " error: " << strerror(errno);
    }
}

void ReadSnapshotRequest::OnApply(uint64_t index,
                                  ::google::protobuf::Closure *done) {
    brpc::ClosureGuard doneGuard(done);
    char *readBuffer = nullptr;
    uint32_t size = request_->size();
    readBuffer = new(std::nothrow)char[size];
    CHECK(nullptr != readBuffer) << "new readBuffer failed, "
                                 << errno << ":" << strerror(errno);
    auto ret = datastore_->ReadSnapshotChunk(request_->chunkid(),
                                             request_->sn(),
                                             readBuffer,
                                             request_->offset(),
                                             request_->size());

    do {
        /**
         * 1.成功
         */
        if (CSErrorCode::Success == ret) {
            cntl_->response_attachment().append(readBuffer, size);
            response_->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
            DVLOG(9) << "read snapshot success: "
                     << " logic pool id: " << request_->logicpoolid()
                     << " copyset id: " << request_->copysetid()
                     << " chunkid: " << request_->chunkid()
                     << " sn: " << request_->sn()
                     << " data size: " << request_->size()
                     << " read len :" << size;
            node_->UpdateAppliedIndex(index);
            break;
        }
        /**
         * 2.chunk not exist
         */
        if (CSErrorCode::ChunkNotExistError == ret) {
            response_->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_CHUNK_NOTEXIST); //NOLINT
            break;
        }
        /**
         * 3.其他错误
         */
        LOG(ERROR) << "read snapshot failed: "
                   << " logic pool id: " << request_->logicpoolid()
                   << " copyset id: " << request_->copysetid()
                   << " chunkid: " << request_->chunkid()
                   << " sn: " << request_->sn()
                   << " data size: " << request_->size()
                   << " read len :" << size
                   << " offset: " << request_->offset()
                   << " error: " << strerror(errno)
                   << " data store return: " << ret;
        response_->set_status(
            CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN);
    } while (0);

    auto maxIndex =
        (index > node_->GetAppliedIndex() ? index : node_->GetAppliedIndex());
    response_->set_appliedindex(maxIndex);
    delete[] readBuffer;
}

void ReadSnapshotRequest::OnApplyFromLog(std::shared_ptr<CSDataStore> datastore,
                                         const ChunkRequest &request,
                                         const butil::IOBuf &data) {
    // read什么都不用做
}

void DeleteSnapshotRequest::OnApply(uint64_t index,
                                    ::google::protobuf::Closure *done) {
    brpc::ClosureGuard doneGuard(done);
    CSErrorCode ret = datastore_->DeleteSnapshotChunk(request_->chunkid(),
                                                      request_->sn());
    if (CSErrorCode::Success == ret) {
        response_->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        node_->UpdateAppliedIndex(index);
    } else {
        LOG(ERROR) << "delete snapshot failed: "
                   << " logic pool id: " << request_->logicpoolid()
                   << " copyset id: " << request_->copysetid()
                   << " chunkid: " << request_->chunkid()
                   << " sn: " << request_->sn()
                   << " error: " << strerror(errno)
                   << " data store return: " << ret;
        response_->set_status(
            CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN);
    }
    auto maxIndex =
        (index > node_->GetAppliedIndex() ? index : node_->GetAppliedIndex());
    response_->set_appliedindex(maxIndex);
}

void DeleteSnapshotRequest::OnApplyFromLog(std::shared_ptr<CSDataStore> datastore,  //NOLINT
                                           const ChunkRequest &request,
                                           const butil::IOBuf &data) {
    auto ret = datastore->DeleteSnapshotChunk(request.chunkid(),
                                              request.sn());
    if (CSErrorCode::Success != ret) {
        LOG(WARNING) << "delete snapshot failed: "
                     << request.logicpoolid() << ", "
                     << request.copysetid()
                     << " chunkid: " << request.chunkid()
                     << " sn: " << request.sn()
                     << " error: " << strerror(errno)
                     << " data store return: " << ret;
    }
}

}  // namespace chunkserver
}  // namespace curve
