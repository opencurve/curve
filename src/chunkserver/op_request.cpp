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

#include "src/chunkserver/op_request.h"

#include <glog/logging.h>
#include <brpc/controller.h>
#include <butil/sys_byteorder.h>
#include <brpc/closure_guard.h>

#include <memory>
#include <string>

#include "src/chunkserver/copyset_node.h"
#include "src/chunkserver/chunk_closure.h"
#include "src/chunkserver/clone_manager.h"
#include "src/chunkserver/clone_task.h"

namespace curve {
namespace chunkserver {

ChunkOpRequest::ChunkOpRequest() :
    datastore_(nullptr),
    node_(nullptr),
    cntl_(nullptr),
    request_(nullptr),
    response_(nullptr),
    done_(nullptr) {
}

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
    done_(done) {
}

void ChunkOpRequest::Process() {
    brpc::ClosureGuard doneGuard(done_);
    /**
     * If propose succeeds, it means that the request was successfully handed over to raft for processing.
     * Then done_ cannot be called, and only if the propose fails does it need to return early
     */
    const butil::IOBuf& data = cntl_->request_attachment();
    if (0 == Propose(request_, &data)) {
        doneGuard.release();
    }
}

int ChunkOpRequest::Propose(const ChunkRequest *request,
                            const butil::IOBuf *data) {
    // Check term and whether it is leader
    if (!node_->IsLeaderTerm()) {
        RedirectChunkRequest();
        return -1;
    }
    // Pack op request as task
    braft::Task task;
    butil::IOBuf log;
    if (0 != Encode(request, data, &log)) {
        LOG(ERROR) << "chunk op request encode failure";
        response_->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN);
        return -1;
    }
    task.data = &log;
    task.done = new ChunkClosure(shared_from_this());
    /**
     * Since apply is asynchronous, it is possible that a node is the leader at term1 and applies a log, but then
     * a master-slave switch occurs and the node becomes the leader at term3 again after a short period
     * of time, at which point the previously applied logs start to be processed. To achieve a strictly replicated
     * state machine in this case, this ABA problem needs to be solved by setting the leader's term at the time of apply
     */
    task.expected_term = node_->LeaderTerm();

    node_->Propose(task);

    return 0;
}

void ChunkOpRequest::RedirectChunkRequest() {
    // Compile with --copt -DUSE_BTHREAD_MUTEX
    // Otherwise a deadlock may occur: CLDCFS-1120
    // PeerId leader = node_->GetLeaderId();
    // if (!leader.is_empty()) {
    //     response_->set_redirect(leader.to_string());
    // }
    response_->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED);
}

int ChunkOpRequest::Encode(const ChunkRequest *request,
                           const butil::IOBuf *data,
                           butil::IOBuf *log) {
    // 1.append request length
    const uint32_t metaSize = butil::HostToNet32(request->ByteSize());
    log->append(&metaSize, sizeof(uint32_t));

    // 2.append op request
    butil::IOBufAsZeroCopyOutputStream wrapper(log);
    if (!request->SerializeToZeroCopyStream(&wrapper)) {
        LOG(ERROR) << "Fail to serialize request";
        return -1;
    }

    // 3.append op data
    if (data != nullptr) {
        log->append(*data);
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
        case CHUNK_OP_TYPE::CHUNK_OP_RECOVER:
            return std::make_shared<ReadChunkRequest>();
        case CHUNK_OP_TYPE::CHUNK_OP_WRITE:
            return std::make_shared<WriteChunkRequest>();
        case CHUNK_OP_TYPE::CHUNK_OP_DELETE:
            return std::make_shared<DeleteChunkRequest>();
        case CHUNK_OP_TYPE::CHUNK_OP_READ_SNAP:
            return std::make_shared<ReadSnapshotRequest>();
        case CHUNK_OP_TYPE::CHUNK_OP_DELETE_SNAP:
            return std::make_shared<DeleteSnapshotRequest>();
        case CHUNK_OP_TYPE::CHUNK_OP_PASTE:
            return std::make_shared<PasteChunkInternalRequest>();
        case CHUNK_OP_TYPE::CHUNK_OP_CREATE_CLONE:
            return std::make_shared<CreateCloneChunkRequest>();
        default:LOG(ERROR) << "Unknown chunk op";
            return nullptr;
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
    } else if (CSErrorCode::InternalError == ret) {
        LOG(FATAL) << "delete chunk failed: "
                   << " logic pool id: " << request_->logicpoolid()
                   << " copyset id: " << request_->copysetid()
                   << " chunkid: " << request_->chunkid()
                   << " data store return: " << ret;
    } else {
        LOG(ERROR) << "delete chunk failed: "
                   << " logic pool id: " << request_->logicpoolid()
                   << " copyset id: " << request_->copysetid()
                   << " chunkid: " << request_->chunkid()
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
    // datastore/request passed in as a parameter is preferred in the process
    auto ret = datastore->DeleteChunk(request.chunkid(),
                                      request.sn());
    if (CSErrorCode::Success == ret)
        return;

    if (CSErrorCode::InternalError == ret) {
        LOG(FATAL) << "delete failed: "
                   << request.logicpoolid() << ", "
                   << request.copysetid()
                   << " chunkid: " << request.chunkid()
                   << " data store return: " << ret;
    } else {
        LOG(ERROR) << "delete failed: "
                   << request.logicpoolid() << ", "
                   << request.copysetid()
                   << " chunkid: " << request.chunkid()
                   << " data store return: " << ret;
    }
}

ReadChunkRequest::ReadChunkRequest(std::shared_ptr<CopysetNode> nodePtr,
                                   CloneManager* cloneMgr,
                                   RpcController *cntl,
                                   const ChunkRequest *request,
                                   ChunkResponse *response,
                                   ::google::protobuf::Closure *done) :
    ChunkOpRequest(nodePtr, cntl, request, response, done),
    cloneMgr_(cloneMgr),
    concurrentApplyModule_(nodePtr->GetConcurrentApplyModule()),
    applyIndex(0) {
}

void ReadChunkRequest::Process() {
    brpc::ClosureGuard doneGuard(done_);

    if (!node_->IsLeaderTerm()) {
        RedirectChunkRequest();
        return;
    }

    /**
     * If the applied index is carried and is less than the latest applied index of
     * the current copyset node, or if the op type is CHUNK_OP_RECOVER, then the
     * consistency protocol does not need to be followed
     */
    if ((request_->has_appliedindex()
        && node_->GetAppliedIndex() >= request_->appliedindex())
        || request_->optype() == CHUNK_OP_TYPE::CHUNK_OP_RECOVER) {
        /**
         * Build shared_ptr<ReadChunkRequest>.
         * Since only std::: enable_shared_from_this<ChunkOpRequest> is provided in ChunkOpRequest,
         * shared_from_this() returns shared_ptr<ChunkOpRequest>
         */
        auto thisPtr
            = std::dynamic_pointer_cast<ReadChunkRequest>(shared_from_this());
        /*
         *  read is thrown to the concurrency layer for two reasons：
         *  (1). Place read I/O operations and other I/O operations such as write in the
         *  concurrency layer to isolate disk I/O from other logic
         *  (2). This is to ensure the semantics of a linearly consistent read. Since the current apply is
         *  concurrent, the applied index update is also concurrent. Although applied index updates are
         *  guaranteed to be monotonic, there may be jumps in updates. For example, if two op's with index=6,7
         *  enter the concurrent module at the same time and both execute successfully and return, but then the leader
         *  dies.  After the new leader is elected, the new leader has the logs of the two op's with index=6,7,
         *  but not applied yet, so the new leader must play back these two logs. Because apply is concurrent,
         *  the op log with index=7 may be applied before the one with index=6, and then the new
         *  leader's applied index will be updated to 7. This time the client comes to read the data
         *  written by the op with index=6, carrying the applied index=7. ChunkServer compares the applied index
         *  carried with the Chunkserver's applied index, then it decides to go through with the direct read,
         *  but ChunkServer actually has not yet dropped the data on disk with index=6. Then a stale read will occur.
         *  The solution is that the read is also queued up at the concurrency layer, so that the read request
         *  with index=6 must be queued up after the op with index=6, i.e. they are operating on the same chunk.
         *  The concurrency layer will put them in the same queue to ensure that the read is executed after the op
         *  apply with index=6, so that there is no stale read and the linear consistency of the read is guaranteed
         */
        auto task = std::bind(&ReadChunkRequest::OnApply,
                              thisPtr,
                              node_->GetAppliedIndex(),
                              doneGuard.release());
        concurrentApplyModule_->Push(
            request_->chunkid(), request_->optype(), task);
        return;
    }

    /**
     * If it does not carry an applied index, then read via the raft consistency protocol
     */
    if (0 == Propose(request_, nullptr)) {
        doneGuard.release();
    }
}

void ReadChunkRequest::OnApply(uint64_t index,
                               ::google::protobuf::Closure *done) {
    // Clear the status in the response first to ensure the correctness of the judgement after CheckForward
    response_->clear_status();

    CSChunkInfo chunkInfo;
    CSErrorCode errorCode = datastore_->GetChunkInfo(request_->chunkid(),
                                                     &chunkInfo);
    do {
        bool needLazyClone = false;
        // If the chunk to be Read does not exist, but the request contains information
        // about the Clone source, try to read the data from the Clone source
        if (CSErrorCode::ChunkNotExistError == errorCode) {
            if (existCloneInfo(request_)) {
                needLazyClone = true;
            } else {
                response_->set_status(
                    CHUNK_OP_STATUS::CHUNK_OP_STATUS_CHUNK_NOTEXIST);
                break;
            }
        } else if (CSErrorCode::Success != errorCode) {
            LOG(ERROR) << "get chunkinfo failed: "
                       << " logic pool id: " << request_->logicpoolid()
                       << " copyset id: " << request_->copysetid()
                       << " chunkid: " << request_->chunkid();
            response_->set_status(
                CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN);
            break;
        }
        // If you need to copy data from the source, you need to redirect
        // the request to the clone manager for processing
        if ( needLazyClone || NeedClone(chunkInfo) ) {
            applyIndex = index;
            std::shared_ptr<CloneTask> cloneTask =
            cloneMgr_->GenerateCloneTask(
                std::dynamic_pointer_cast<ReadChunkRequest>(shared_from_this()),
                done);
            // TODO(yyk) Try not to block the queue, specific considerations to be made later
            bool result = cloneMgr_->IssueCloneTask(cloneTask);
            if (!result) {
                LOG(ERROR) << "issue clone task failed: "
                           << " logic pool id: " << request_->logicpoolid()
                           << " copyset id: " << request_->copysetid()
                           << " chunkid: " << request_->chunkid();
                response_->set_status(
                    CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN);
                break;
            }
            // If the request is successfully redirected to the clone manager, it can be returned directly
            return;
        }
        // If it is a ReadChunk request it also needs to read the data locally
        if (request_->optype() == CHUNK_OP_TYPE::CHUNK_OP_READ) {
            ReadChunk();
        }
        // If the request is a recover request, the request area has already been written and can be returned as success
        if (request_->optype() == CHUNK_OP_TYPE::CHUNK_OP_RECOVER) {
            response_->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        }
    } while (false);

    if (response_->status() == CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS) {
        node_->UpdateAppliedIndex(index);
    }

    brpc::ClosureGuard doneGuard(done);
    auto maxIndex =
        (index > node_->GetAppliedIndex() ? index : node_->GetAppliedIndex());
    response_->set_appliedindex(maxIndex);
}

void ReadChunkRequest::OnApplyFromLog(std::shared_ptr<CSDataStore> datastore,
                                      const ChunkRequest &request,
                                      const butil::IOBuf &data) {
    // NOTE: datastore/request passed in as a parameter is preferred in the process
    // read needs to do nothing
}

bool ReadChunkRequest::NeedClone(const CSChunkInfo& chunkInfo) {
    // If it is not a clone chunk, there is no need to copy
    if (chunkInfo.isClone) {
        off_t offset = request_->offset();
        size_t length = request_->size();
        uint32_t pageSize = chunkInfo.pageSize;
        uint32_t beginIndex = offset / pageSize;
        uint32_t endIndex = (offset + length - 1) / pageSize;
        // If it is a clone chunk and there is an unwritten page, it needs to be copied
        if (chunkInfo.bitmap->NextClearBit(beginIndex, endIndex)
            != Bitmap::NO_POS) {
            return true;
        }
    }
    return false;
}

static void ReadBufferDeleter(void* ptr) {
    delete[] static_cast<char*>(ptr);
}

void ReadChunkRequest::ReadChunk() {
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
    butil::IOBuf wrapper;
    wrapper.append_user_data(readBuffer, size, ReadBufferDeleter);
    if (CSErrorCode::Success == ret) {
        cntl_->response_attachment().append(wrapper);
        response_->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
    } else if (CSErrorCode::ChunkNotExistError == ret) {
        response_->set_status(
            CHUNK_OP_STATUS::CHUNK_OP_STATUS_CHUNK_NOTEXIST);
    } else if (CSErrorCode::InternalError == ret) {
        LOG(FATAL) << "read failed: "
                   << " logic pool id: " << request_->logicpoolid()
                   << " copyset id: " << request_->copysetid()
                   << " chunkid: " << request_->chunkid()
                   << " data size: " << request_->size()
                   << " read len :" << size
                   << " data store return: " << ret;
    } else {
        LOG(ERROR) << "read failed: "
                   << " logic pool id: " << request_->logicpoolid()
                   << " copyset id: " << request_->copysetid()
                   << " chunkid: " << request_->chunkid()
                   << " data size: " << request_->size()
                   << " read len :" << size
                   << " data store return: " << ret;
        response_->set_status(
            CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN);
    }
}

void WriteChunkRequest::OnApply(uint64_t index,
                                ::google::protobuf::Closure *done) {
    brpc::ClosureGuard doneGuard(done);
    uint32_t cost;

    std::string  cloneSourceLocation;
    if (existCloneInfo(request_)) {
        auto func = ::curve::common::LocationOperator::GenerateCurveLocation;
        cloneSourceLocation =  func(request_->clonefilesource(),
                            request_->clonefileoffset());
    }

    auto ret = datastore_->WriteChunk(request_->chunkid(),
                                      request_->sn(),
                                      cntl_->request_attachment(),
                                      request_->offset(),
                                      request_->size(),
                                      &cost,
                                      cloneSourceLocation);

    if (CSErrorCode::Success == ret) {
        response_->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        node_->UpdateAppliedIndex(index);
    } else if (CSErrorCode::BackwardRequestError == ret) {
        // There is a possibility that an old version of the request will appear at the moment the snapshot is taken
        // Return an error to the client and ask the client to retry with a new version
        LOG(WARNING) << "write failed: "
                     << " logic pool id: " << request_->logicpoolid()
                     << " copyset id: " << request_->copysetid()
                     << " chunkid: " << request_->chunkid()
                     << " data size: " << request_->size()
                     << " data store return: " << ret;
        response_->set_status(
            CHUNK_OP_STATUS::CHUNK_OP_STATUS_BACKWARD);
    } else if (CSErrorCode::InternalError == ret ||
               CSErrorCode::CrcCheckError == ret ||
               CSErrorCode::FileFormatError == ret) {
        /**
         * internalerror is usually a disk error, to prevent inconsistent copies and to make the process exit
         * TODO(yyk): When a write error is encountered, simply exit fatal
         * ChunkServer later considers just marking this copyset bad to ensure better availability
        */
        LOG(FATAL) << "write failed: "
                   << " logic pool id: " << request_->logicpoolid()
                   << " copyset id: " << request_->copysetid()
                   << " chunkid: " << request_->chunkid()
                   << " data size: " << request_->size()
                   << " data store return: " << ret;
    } else {
        LOG(ERROR) << "write failed: "
                   << " logic pool id: " << request_->logicpoolid()
                   << " copyset id: " << request_->copysetid()
                   << " chunkid: " << request_->chunkid()
                   << " data size: " << request_->size()
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
    // NOTE: datastore/request passed in as a parameter is preferred in the process
    uint32_t cost;
    std::string  cloneSourceLocation;
    if (existCloneInfo(&request)) {
        auto func = ::curve::common::LocationOperator::GenerateCurveLocation;
        cloneSourceLocation =  func(request.clonefilesource(),
                            request.clonefileoffset());
    }


    auto ret = datastore->WriteChunk(request.chunkid(),
                                     request.sn(),
                                     data,
                                     request.offset(),
                                     request.size(),
                                     &cost,
                                     cloneSourceLocation);
     if (CSErrorCode::Success == ret) {
         return;
     } else if (CSErrorCode::BackwardRequestError == ret) {
        LOG(WARNING) << "write failed: "
                     << " logic pool id: " << request.logicpoolid()
                     << " copyset id: " << request.copysetid()
                     << " chunkid: " << request.chunkid()
                     << " data size: " << request.size()
                     << " data store return: " << ret;
    } else if (CSErrorCode::InternalError == ret ||
               CSErrorCode::CrcCheckError == ret ||
               CSErrorCode::FileFormatError == ret) {
        LOG(FATAL) << "write failed: "
                   << " logic pool id: " << request.logicpoolid()
                   << " copyset id: " << request.copysetid()
                   << " chunkid: " << request.chunkid()
                   << " data size: " << request.size()
                   << " data store return: " << ret;
    } else {
        LOG(ERROR) << "write failed: "
                   << " logic pool id: " << request.logicpoolid()
                   << " copyset id: " << request.copysetid()
                   << " chunkid: " << request.chunkid()
                   << " data size: " << request.size()
                   << " data store return: " << ret;
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
    butil::IOBuf wrapper;
    wrapper.append_user_data(readBuffer, size, ReadBufferDeleter);

    do {
        /**
         * 1.success
         */
        if (CSErrorCode::Success == ret) {
            cntl_->response_attachment().append(wrapper);
            response_->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
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
         * 3.internal error
         */
        if (CSErrorCode::InternalError == ret) {
            LOG(FATAL) << "read snapshot failed: "
                       << " logic pool id: " << request_->logicpoolid()
                       << " copyset id: " << request_->copysetid()
                       << " chunkid: " << request_->chunkid()
                       << " sn: " << request_->sn()
                       << " data size: " << request_->size()
                       << " read len :" << size
                       << " offset: " << request_->offset()
                       << " data store return: " << ret;
        }
        /**
         * 4.other error
         */
        LOG(ERROR) << "read snapshot failed: "
                   << " logic pool id: " << request_->logicpoolid()
                   << " copyset id: " << request_->copysetid()
                   << " chunkid: " << request_->chunkid()
                   << " sn: " << request_->sn()
                   << " data size: " << request_->size()
                   << " read len :" << size
                   << " offset: " << request_->offset()
                   << " data store return: " << ret;
        response_->set_status(
            CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN);
    } while (0);

    auto maxIndex =
        (index > node_->GetAppliedIndex() ? index : node_->GetAppliedIndex());
    response_->set_appliedindex(maxIndex);
}

void ReadSnapshotRequest::OnApplyFromLog(std::shared_ptr<CSDataStore> datastore,
                                         const ChunkRequest &request,
                                         const butil::IOBuf &data) {
    // NOTE: datastore/request passed in as a parameter is preferred in the process
    // read needs to do nothing
}

void DeleteSnapshotRequest::OnApply(uint64_t index,
                                    ::google::protobuf::Closure *done) {
    brpc::ClosureGuard doneGuard(done);
    CSErrorCode ret = datastore_->DeleteSnapshotChunkOrCorrectSn(
        request_->chunkid(), request_->correctedsn());
    if (CSErrorCode::Success == ret) {
        response_->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        node_->UpdateAppliedIndex(index);
    } else if (CSErrorCode::BackwardRequestError == ret) {
        LOG(WARNING) << "delete snapshot or correct sn failed: "
                     << " logic pool id: " << request_->logicpoolid()
                     << " copyset id: " << request_->copysetid()
                     << " chunkid: " << request_->chunkid()
                     << " correctedSn: " << request_->correctedsn()
                     << " data store return: " << ret;
        response_->set_status(
            CHUNK_OP_STATUS::CHUNK_OP_STATUS_BACKWARD);
    } else if (CSErrorCode::InternalError == ret) {
        LOG(FATAL) << "delete snapshot or correct sn failed: "
                   << " logic pool id: " << request_->logicpoolid()
                   << " copyset id: " << request_->copysetid()
                   << " chunkid: " << request_->chunkid()
                   << " correctedSn: " << request_->correctedsn()
                   << " data store return: " << ret;
    } else {
        LOG(ERROR) << "delete snapshot or correct sn failed: "
                   << " logic pool id: " << request_->logicpoolid()
                   << " copyset id: " << request_->copysetid()
                   << " chunkid: " << request_->chunkid()
                   << " correctedSn: " << request_->correctedsn()
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
    // NOTE: datastore/request passed in as a parameter is preferred in the process
    auto ret = datastore->DeleteSnapshotChunkOrCorrectSn(
        request.chunkid(), request.correctedsn());
    if (CSErrorCode::Success == ret) {
        return;
    } else if (CSErrorCode::BackwardRequestError == ret) {
        LOG(WARNING) << "delete snapshot or correct sn failed: "
                     << request.logicpoolid() << ", "
                     << request.copysetid()
                     << " chunkid: " << request.chunkid()
                     << " correctedSn: " << request.correctedsn()
                     << " data store return: " << ret;
    } else if (CSErrorCode::InternalError == ret) {
        LOG(FATAL) << "delete snapshot or correct sn failed: "
                   << request.logicpoolid() << ", "
                   << request.copysetid()
                   << " chunkid: " << request.chunkid()
                   << " correctedSn: " << request.correctedsn()
                   << " data store return: " << ret;
    } else {
        LOG(ERROR) << "delete snapshot or correct sn failed: "
                   << request.logicpoolid() << ", "
                   << request.copysetid()
                   << " chunkid: " << request.chunkid()
                   << " correctedSn: " << request.correctedsn()
                   << " data store return: " << ret;
    }
}

void CreateCloneChunkRequest::OnApply(uint64_t index,
                                      ::google::protobuf::Closure *done) {
    brpc::ClosureGuard doneGuard(done);

    auto ret = datastore_->CreateCloneChunk(request_->chunkid(),
                                            request_->sn(),
                                            request_->correctedsn(),
                                            request_->size(),
                                            request_->location());

    if (CSErrorCode::Success == ret) {
        response_->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        node_->UpdateAppliedIndex(index);
    } else if (CSErrorCode::InternalError == ret ||
               CSErrorCode::CrcCheckError == ret ||
               CSErrorCode::FileFormatError == ret) {
        /**
         * TODO(yyk): When a write error is encountered, simply exit fatal
         * ChunkServer later considers just marking this copyset bad to ensure better availability
         */
        LOG(FATAL) << "create clone failed: "
                   << " logic pool id: " << request_->logicpoolid()
                   << " copyset id: " << request_->copysetid()
                   << " chunkid: " << request_->chunkid()
                   << " sn " << request_->sn()
                   << " correctedSn: " << request_->correctedsn()
                   << " location: " << request_->location();
        response_->set_status(
            CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN);
    } else if (CSErrorCode::ChunkConflictError == ret) {
        LOG(WARNING) << "create clone chunk exist: "
                   << " logic pool id: " << request_->logicpoolid()
                   << " copyset id: " << request_->copysetid()
                   << " chunkid: " << request_->chunkid()
                   << " sn " << request_->sn()
                   << " correctedSn: " << request_->correctedsn()
                   << " location: " << request_->location();
        response_->set_status(
            CHUNK_OP_STATUS::CHUNK_OP_STATUS_CHUNK_EXIST);
    } else {
        LOG(ERROR) << "create clone failed: "
                   << " logic pool id: " << request_->logicpoolid()
                   << " copyset id: " << request_->copysetid()
                   << " chunkid: " << request_->chunkid()
                   << " sn " << request_->sn()
                   << " correctedSn: " << request_->correctedsn()
                   << " location: " << request_->location();
        response_->set_status(
            CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN);
    }
    auto maxIndex =
        (index > node_->GetAppliedIndex() ? index : node_->GetAppliedIndex());
    response_->set_appliedindex(maxIndex);
}

void CreateCloneChunkRequest::OnApplyFromLog(std::shared_ptr<CSDataStore> datastore,  //NOLINT
                                             const ChunkRequest &request,
                                             const butil::IOBuf &data) {
    // NOTE: datastore/request passed in as a parameter is preferred in the process
    auto ret = datastore->CreateCloneChunk(request.chunkid(),
                                           request.sn(),
                                           request.correctedsn(),
                                           request.size(),
                                           request.location());
    if (CSErrorCode::Success == ret)
        return;

    if (CSErrorCode::ChunkConflictError == ret) {
        LOG(WARNING) << "create clone chunk exist: "
                << " logic pool id: " << request.logicpoolid()
                << " copyset id: " << request.copysetid()
                << " chunkid: " << request.chunkid()
                << " sn " << request.sn()
                << " correctedSn: " << request.correctedsn()
                << " location: " << request.location();
        return;
    }

    if (CSErrorCode::InternalError == ret ||
        CSErrorCode::CrcCheckError == ret ||
        CSErrorCode::FileFormatError == ret) {
        LOG(FATAL) << "create clone failed:"
                   << " logic pool id: " << request.logicpoolid()
                   << " copyset id: " << request.copysetid()
                   << " chunkid: " << request.chunkid()
                   << " sn " << request.sn()
                   << " correctedSn: " << request.correctedsn()
                   << " location: " << request.location();
    } else {
        LOG(ERROR) << "create clone failed: "
                   << " logic pool id: " << request.logicpoolid()
                   << " copyset id: " << request.copysetid()
                   << " chunkid: " << request.chunkid()
                   << " sn " << request.sn()
                   << " correctedSn: " << request.correctedsn()
                   << " location: " << request.location();
    }
}

void PasteChunkInternalRequest::Process() {
    brpc::ClosureGuard doneGuard(done_);
    /**
     * If propose succeeds, the request is successfully handed over to raft, then
     * done_ cannot be called, and only if propose fails does it need to return early
     */
    if (0 == Propose(request_, &data_)) {
        doneGuard.release();
    }
}

void PasteChunkInternalRequest::OnApply(uint64_t index,
                                        ::google::protobuf::Closure *done) {
    brpc::ClosureGuard doneGuard(done);

    auto ret = datastore_->PasteChunk(request_->chunkid(),
                                      data_.to_string().c_str(),  //NOLINT
                                      request_->offset(),
                                      request_->size());

    if (CSErrorCode::Success == ret) {
        response_->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        node_->UpdateAppliedIndex(index);
    } else if (CSErrorCode::InternalError == ret) {
        LOG(FATAL) << "paste chunk failed: "
                   << " logic pool id: " << request_->logicpoolid()
                   << " copyset id: " << request_->copysetid()
                   << " chunkid: " << request_->chunkid()
                   << " offset: " << request_->offset()
                   << " length: " << request_->size();
    } else {
        LOG(ERROR) << "paste chunk failed: "
                   << " logic pool id: " << request_->logicpoolid()
                   << " copyset id: " << request_->copysetid()
                   << " chunkid: " << request_->chunkid()
                   << " offset: " << request_->offset()
                   << " length: " << request_->size();
        response_->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN);
    }

    auto maxIndex = (index > node_->GetAppliedIndex()
                    ? index
                    : node_->GetAppliedIndex());
    response_->set_appliedindex(maxIndex);
}

void PasteChunkInternalRequest::OnApplyFromLog(std::shared_ptr<CSDataStore> datastore,  //NOLINT
                                               const ChunkRequest &request,
                                               const butil::IOBuf &data) {
    // NOTE: datastore/request passed in as a parameter is preferred in the process
    auto ret = datastore->PasteChunk(request.chunkid(),
                                     data.to_string().c_str(),
                                     request.offset(),
                                     request.size());
    if (CSErrorCode::Success == ret)
        return;

    if (CSErrorCode::InternalError == ret) {
        LOG(FATAL) << "paste chunk failed: "
                   << " logic pool id: " << request.logicpoolid()
                   << " copyset id: " << request.copysetid()
                   << " chunkid: " << request.chunkid()
                   << " offset: " << request.offset()
                   << " length: " << request.size();
    } else {
        LOG(ERROR) << "paste chunk failed: "
                   << " logic pool id: " << request.logicpoolid()
                   << " copyset id: " << request.copysetid()
                   << " chunkid: " << request.chunkid()
                   << " offset: " << request.offset()
                   << " length: " << request.size();
    }
}

}  // namespace chunkserver
}  // namespace curve
