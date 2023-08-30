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

#include <algorithm>
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

    // check if current node is leader
    if (!node_->IsLeaderTerm()) {
        RedirectChunkRequest();
        return;
    }

    /**
     *If the proposal is successful, it indicates that the request has been successfully handed over to the raft for processing,
     *So, done_ cannot be called, only if the proposal fails, it needs to be returned in advance
     */
    if (0 == Propose(request_, cntl_ ? &cntl_->request_attachment() :
                     nullptr)) {
        doneGuard.release();
    }
}

int ChunkOpRequest::Propose(const ChunkRequest *request,
                            const butil::IOBuf *data) {
    //Pack op request as task
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
     *Due to the asynchronous nature of the application, it is possible that a node in term1 is a leader and has applied a log,
     *But there was a master-slave switch in the middle, and in a short period of time, this node became the leader of term3 again,
     *Previously applied logs were only processed, in which case strict replication status needs to be implemented
     *To solve this ABA problem, you can set the term of the leader at the time of application
     */
    task.expected_term = node_->LeaderTerm();

    node_->Propose(task);

    return 0;
}

void ChunkOpRequest::RedirectChunkRequest() {
    //Compile with --copt -DUSE_BTHREAD_MUTEX
    //Otherwise, a deadlock may occur: CLDCFS-1120
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
                                                       butil::IOBuf *data,
                                                       uint64_t index,
                                                       PeerId leaderId) {
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
        case CHUNK_OP_TYPE::CHUNK_OP_SCAN:
            return std::make_shared<ScanChunkRequest>(index, leaderId);
        default:LOG(ERROR) << "Unknown chunk op";
            return nullptr;
    }
}

ApplyTaskType ChunkOpRequest::Schedule(CHUNK_OP_TYPE opType) {
    switch (opType) {
    case CHUNK_OP_READ:
    case CHUNK_OP_RECOVER:
        return ApplyTaskType::READ;
    default:
        return ApplyTaskType::WRITE;
    }
}

namespace {
uint64_t MaxAppliedIndex(
        const std::shared_ptr<curve::chunkserver::CopysetNode>& node,
        uint64_t current) {
    return std::max(current, node->GetAppliedIndex());
}
}  // namespace

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
                   << " data store return: " << ret
                   << ", request: " << request_->ShortDebugString();
    } else {
        LOG(ERROR) << "delete chunk failed: "
                   << " data store return: " << ret
                   << ", request: " << request_->ShortDebugString();
        response_->set_status(
            CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN);
    }
    response_->set_appliedindex(MaxAppliedIndex(node_, index));
}

void DeleteChunkRequest::OnApplyFromLog(std::shared_ptr<CSDataStore> datastore,
                                        const ChunkRequest &request,
                                        const butil::IOBuf &data) {
    (void)data;
    //NOTE: Prioritize the use of datastore/request passed in as parameters during processing
    auto ret = datastore->DeleteChunk(request.chunkid(),
                                      request.sn());
    if (CSErrorCode::Success == ret)
        return;

    if (CSErrorCode::InternalError == ret) {
        LOG(FATAL) << "delete failed: "
                   << " data store return: " << ret
                   << ", request: " << request.ShortDebugString();
    } else {
        LOG(ERROR) << "delete failed: "
                   << " data store return: " << ret
                   << ", request: " << request.ShortDebugString();
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

    braft::LeaderLeaseStatus lease_status;
    node_->GetLeaderLeaseStatus(&lease_status);

    if (node_->IsLeaseLeader(lease_status)) {  // local read
        /*
         * constrcut shared_ptr<ReadChunkRequest>, because ChunkOpRequest
         * extend from std::enable_shared_from_this<ChunkOpRequest>,
         * use shared_from_this() to return a shared_ptr<ChunkOpRequest>
         */
        auto thisPtr
            = std::dynamic_pointer_cast<ReadChunkRequest>(shared_from_this());
        /*
         * why push read requests to concurrent layer:
         *  1. all I/O operators including read and write requests are executed
         *     in concurrent layer, we can separate disk I/O from other logic.
         *  2. ensure linear consistency of read semantics.
         */
        auto task = std::bind(&ReadChunkRequest::OnApply,
                              thisPtr,
                              node_->GetAppliedIndex(),
                              doneGuard.release());
        concurrentApplyModule_->Push(request_->chunkid(),
                                     ChunkOpRequest::Schedule(request_->optype()),  // NOLINT
                                     task);
        return;
    }

    if (node_->IsLeaseExpired(lease_status)) {
        RedirectChunkRequest();
        return;
    }

    // braft::LEASE_NOT_READY || braft::LEASE_DISABLED
    // => log read, propose to raft
    if (0 == Propose(request_, nullptr)) {
        doneGuard.release();
    }
}

void ReadChunkRequest::OnApply(uint64_t index,
                               ::google::protobuf::Closure *done) {
    //Clear the status in the response first to ensure the correctness of the judgment after CheckForward
    response_->clear_status();

    CSChunkInfo chunkInfo;
    CSErrorCode errorCode = datastore_->GetChunkInfo(request_->chunkid(),
                                                     &chunkInfo);
    do {
        bool needLazyClone = false;
        //If the chunk that needs to be read does not exist, but the request contains Clone source information, try reading data from the Clone source
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
                       << " data store return: " << errorCode
                       << ", request: " << request_->ShortDebugString();
            response_->set_status(
                CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN);
            break;
        }
        //If you need to copy data from the source, you need to forward the request to the clone manager for processing
        if ( needLazyClone || NeedClone(chunkInfo) ) {
            applyIndex = index;
            std::shared_ptr<CloneTask> cloneTask =
            cloneMgr_->GenerateCloneTask(
                std::dynamic_pointer_cast<ReadChunkRequest>(shared_from_this()),
                done);
            //TODO (yyk) should try not to block the queue, and specific considerations should be taken later
            bool result = cloneMgr_->IssueCloneTask(cloneTask);
            if (!result) {
                LOG(ERROR) << "issue clone task failed: "
                           << ", request: " << request_->ShortDebugString();
                response_->set_status(
                    CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN);
                break;
            }
            //If the request is successfully forwarded to the clone manager, it can be returned directly
            return;
        }
        //If it is a ReadChunk request, data needs to be read locally
        if (request_->optype() == CHUNK_OP_TYPE::CHUNK_OP_READ) {
            ReadChunk();
        }
        //If it is a recover request, it indicates that the request area has been written and can directly return success
        if (request_->optype() == CHUNK_OP_TYPE::CHUNK_OP_RECOVER) {
            response_->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        }
    } while (false);

    if (response_->status() == CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS) {
        node_->UpdateAppliedIndex(index);
    }

    brpc::ClosureGuard doneGuard(done);
    response_->set_appliedindex(MaxAppliedIndex(node_, index));
}

void ReadChunkRequest::OnApplyFromLog(std::shared_ptr<CSDataStore> datastore,
                                      const ChunkRequest &request,
                                      const butil::IOBuf &data) {
    (void)datastore;
    (void)request;
    (void)data;
    //NOTE: Prioritize the use of datastore/request passed in as parameters during processing
    //Read doesn't need to do anything
}

bool ReadChunkRequest::NeedClone(const CSChunkInfo& chunkInfo) {
    //If it's not a clone chunk, there's no need to copy it
    if (chunkInfo.isClone) {
        off_t offset = request_->offset();
        size_t length = request_->size();
        uint32_t blockSize = chunkInfo.blockSize;
        uint32_t beginIndex = offset / blockSize;
        uint32_t endIndex = (offset + length - 1) / blockSize;
        //If it is a clone chunk and there are unwritten pages, it needs to be copied
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
                   << " data store return: " << ret
                   << ", request: " << request_->ShortDebugString();
    } else {
        LOG(ERROR) << "read failed: "
                   << " data store return: " << ret
                   << ", request: " << request_->ShortDebugString();
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
        //At the moment of taking a snapshot, there may be requests for older versions
        //Return an error to the client and ask them to try again with the new version of the original
        LOG(WARNING) << "write failed: "
                     << " data store return: " << ret
                     << ", request: " << request_->ShortDebugString();
        response_->set_status(
            CHUNK_OP_STATUS::CHUNK_OP_STATUS_BACKWARD);
    } else if (CSErrorCode::InternalError == ret ||
               CSErrorCode::CrcCheckError == ret ||
               CSErrorCode::FileFormatError == ret) {
        /**
         *An internal error is usually a disk error. To prevent inconsistent replicas, the process is forced to exit
         *TODO (yyk): Currently encountering a write error, directly fatally exit the entire process
         *ChunkServer will consider only flagging this copyset in the later stage to ensure good availability
        */
        LOG(FATAL) << "write failed: "
                   << " data store return: " << ret
                   << ", request: " << request_->ShortDebugString();
    } else {
        LOG(ERROR) << "write failed: "
                   << " data store return: " << ret
                   << ", request: " << request_->ShortDebugString();
        response_->set_status(
            CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN);
    }

    response_->set_appliedindex(MaxAppliedIndex(node_, index));
    node_->ShipToSync(request_->chunkid());
}

void WriteChunkRequest::OnApplyFromLog(std::shared_ptr<CSDataStore> datastore,
                                       const ChunkRequest &request,
                                       const butil::IOBuf &data) {
    //NOTE: Prioritize the use of datastore/request passed in as parameters during processing
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
                     << " data store return: " << ret
                     << ", request: " << request.ShortDebugString();
    } else if (CSErrorCode::InternalError == ret ||
               CSErrorCode::CrcCheckError == ret ||
               CSErrorCode::FileFormatError == ret) {
        LOG(FATAL) << "write failed: "
                   << " data store return: " << ret
                   << ", request: " << request.ShortDebugString();
    } else {
        LOG(ERROR) << "write failed: "
                   << " data store return: " << ret
                   << ", request: " << request.ShortDebugString();
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
         *1. Success
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
                       << " data store return: " << ret
                       << ", request: " << request_->ShortDebugString();
        }
        /**
         *4. Other errors
         */
        LOG(ERROR) << "read snapshot failed: "
                   << " data store return: " << ret
                   << ", request: " << request_->ShortDebugString();
        response_->set_status(
            CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN);
    } while (0);

    response_->set_appliedindex(MaxAppliedIndex(node_, index));
}

void ReadSnapshotRequest::OnApplyFromLog(std::shared_ptr<CSDataStore> datastore,
                                         const ChunkRequest &request,
                                         const butil::IOBuf &data) {
    (void)datastore;
    (void)request;
    (void)data;
    //NOTE: Prioritize the use of datastore/request passed in as parameters during processing
    //Read doesn't need to do anything
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
                     << " data store return: " << ret
                     << ", request: " << request_->ShortDebugString();
        response_->set_status(
            CHUNK_OP_STATUS::CHUNK_OP_STATUS_BACKWARD);
    } else if (CSErrorCode::InternalError == ret) {
        LOG(FATAL) << "delete snapshot or correct sn failed: "
                   << " data store return: " << ret
                   << ", request: " << request_->ShortDebugString();
    } else {
        LOG(ERROR) << "delete snapshot or correct sn failed: "
                   << " data store return: " << ret
                   << ", request: " << request_->ShortDebugString();
        response_->set_status(
            CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN);
    }

    response_->set_appliedindex(MaxAppliedIndex(node_, index));
}

void DeleteSnapshotRequest::OnApplyFromLog(std::shared_ptr<CSDataStore> datastore,  //NOLINT
                                           const ChunkRequest &request,
                                           const butil::IOBuf &data) {
    (void)data;
    //NOTE: Prioritize the use of datastore/request passed in as parameters during processing
    auto ret = datastore->DeleteSnapshotChunkOrCorrectSn(
        request.chunkid(), request.correctedsn());
    if (CSErrorCode::Success == ret) {
        return;
    } else if (CSErrorCode::BackwardRequestError == ret) {
        LOG(WARNING) << "delete snapshot or correct sn failed: "
                     << " data store return: " << ret
                     << ", request: " << request.ShortDebugString();
    } else if (CSErrorCode::InternalError == ret) {
        LOG(FATAL) << "delete snapshot or correct sn failed: "
                   << " data store return: " << ret
                   << ", request: " << request.ShortDebugString();
    } else {
        LOG(ERROR) << "delete snapshot or correct sn failed: "
                   << " data store return: " << ret
                   << ", request: " << request.ShortDebugString();
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
         *TODO (yyk): Currently encountering the createclonechunk error, directly fatally exit the entire process
         *ChunkServer will consider only flagging this copyset in the later stage to ensure good availability
         */
        LOG(FATAL) << "create clone failed: "
                   << ", request: " << request_->ShortDebugString();
        response_->set_status(
            CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN);
    } else if (CSErrorCode::ChunkConflictError == ret) {
        LOG(WARNING) << "create clone chunk exist: "
                   << ", request: " << request_->ShortDebugString();
        response_->set_status(
            CHUNK_OP_STATUS::CHUNK_OP_STATUS_CHUNK_EXIST);
    } else {
        LOG(ERROR) << "create clone failed: "
                   << ", request: " << request_->ShortDebugString();
        response_->set_status(
            CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN);
    }

    response_->set_appliedindex(MaxAppliedIndex(node_, index));
}

void CreateCloneChunkRequest::OnApplyFromLog(std::shared_ptr<CSDataStore> datastore,  //NOLINT
                                             const ChunkRequest &request,
                                             const butil::IOBuf &data) {
    (void)data;
    //NOTE: Prioritize the use of datastore/request passed in as parameters during processing
    auto ret = datastore->CreateCloneChunk(request.chunkid(),
                                           request.sn(),
                                           request.correctedsn(),
                                           request.size(),
                                           request.location());
    if (CSErrorCode::Success == ret)
        return;

    if (CSErrorCode::ChunkConflictError == ret) {
        LOG(WARNING) << "create clone chunk exist: "
                << ", request: " << request.ShortDebugString();
        return;
    }

    if (CSErrorCode::InternalError == ret ||
        CSErrorCode::CrcCheckError == ret ||
        CSErrorCode::FileFormatError == ret) {
        LOG(FATAL) << "create clone failed:"
                   << ", request: " << request.ShortDebugString();
    } else {
        LOG(ERROR) << "create clone failed: "
                   << ", request: " << request.ShortDebugString();
    }
}

void PasteChunkInternalRequest::Process() {
    brpc::ClosureGuard doneGuard(done_);

    // check if current node is leader
    if (!node_->IsLeaderTerm()) {
        RedirectChunkRequest();
        return;
    }

    /**
     *If the proposal is successful, it indicates that the request has been successfully handed over to the raft for processing,
     *So, done_ cannot be called, only if the proposal fails, it needs to be returned in advance
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
                   << ", request: " << request_->ShortDebugString();
    } else {
        LOG(ERROR) << "paste chunk failed: "
                   << ", request: " << request_->ShortDebugString();
        response_->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN);
    }

    response_->set_appliedindex(MaxAppliedIndex(node_, index));
}

void PasteChunkInternalRequest::OnApplyFromLog(std::shared_ptr<CSDataStore> datastore,  //NOLINT
                                               const ChunkRequest &request,
                                               const butil::IOBuf &data) {
    //NOTE: Prioritize the use of datastore/request passed in as parameters during processing

    auto ret = datastore->PasteChunk(request.chunkid(),
                                     data.to_string().c_str(),
                                     request.offset(),
                                     request.size());
    if (CSErrorCode::Success == ret)
        return;

    if (CSErrorCode::InternalError == ret) {
        LOG(FATAL) << "paste chunk failed: "
                   << ", request: " << request.ShortDebugString();
    } else {
        LOG(ERROR) << "paste chunk failed: "
                   << ", request: " << request.ShortDebugString();
    }
}

void ScanChunkRequest::OnApply(uint64_t index,
                               ::google::protobuf::Closure *done) {
    brpc::ClosureGuard doneGuard(done);

    // read and calculate crc, build scanmap
    uint32_t crc = 0;
    size_t size = request_->size();
    std::unique_ptr<char[]> readBuffer(new(std::nothrow)char[size]);
    CHECK(nullptr != readBuffer)
        << "new readBuffer failed " << strerror(errno);
    // scan chunk metapage or user data
    auto ret = 0;
    if (request_->has_readmetapage() && request_->readmetapage()) {
        ret = datastore_->ReadChunkMetaPage(request_->chunkid(),
                                            request_->sn(),
                                            readBuffer.get());
    } else {
        ret = datastore_->ReadChunk(request_->chunkid(),
                                    request_->sn(),
                                    readBuffer.get(),
                                    request_->offset(),
                                    size);
    }

    if (CSErrorCode::Success == ret) {
        crc = ::curve::common::CRC32(readBuffer.get(), size);
        // build scanmap
        ScanMap scanMap;
        scanMap.set_logicalpoolid(request_->logicpoolid());
        scanMap.set_copysetid(request_->copysetid());
        scanMap.set_chunkid(request_->chunkid());
        scanMap.set_index(index);
        scanMap.set_crc(crc);
        scanMap.set_offset(request_->offset());
        scanMap.set_len(size);

        ScanKey jobKey(request_->logicpoolid(), request_->copysetid());
        scanManager_->SetLocalScanMap(jobKey, scanMap);
        scanManager_->SetScanJobType(jobKey, ScanType::WaitMap);
        scanManager_->GenScanJobs(jobKey);
        response_->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
    } else if (CSErrorCode::ChunkNotExistError == ret) {
        response_->set_status(
            CHUNK_OP_STATUS::CHUNK_OP_STATUS_CHUNK_NOTEXIST);
    } else if (CSErrorCode::InternalError == ret) {
        LOG(FATAL) << "scan chunk failed, read chunk internal error"
                   << ", request: " << request_->ShortDebugString();
    } else {
        response_->set_status(
            CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN);
    }
}

void ScanChunkRequest::OnApplyFromLog(std::shared_ptr<CSDataStore> datastore,  //NOLINT
                                               const ChunkRequest &request,
                                               const butil::IOBuf &data) {
    (void)data;
    uint32_t crc = 0;
    size_t size = request.size();
    std::unique_ptr<char[]> readBuffer(new(std::nothrow)char[size]);
    CHECK(nullptr != readBuffer)
        << "new readBuffer failed " << strerror(errno);

    // scan chunk metapage or user data
    auto ret = 0;
    if (request.has_readmetapage() && request.readmetapage()) {
        ret = datastore->ReadChunkMetaPage(request.chunkid(),
                                            request.sn(),
                                            readBuffer.get());
    } else {
        ret = datastore->ReadChunk(request.chunkid(),
                                    request.sn(),
                                    readBuffer.get(),
                                    request.offset(),
                                    size);
    }

    if (CSErrorCode::Success == ret) {
        crc = ::curve::common::CRC32(readBuffer.get(), size);
        BuildAndSendScanMap(request, index_, crc);
    } else if (CSErrorCode::ChunkNotExistError == ret) {
        LOG(ERROR) << "scan failed: chunk not exist, "
                   << " datastore return: " << ret
                   << ", request: " << request.ShortDebugString();
    } else if (CSErrorCode::InternalError == ret) {
        LOG(FATAL) << "scan failed: "
                   << " datastore return: " << ret
                   << ", request: " << request.ShortDebugString();
    } else {
        LOG(ERROR) << "scan failed: "
                   << " datastore return: " << ret
                   << ", request: " << request.ShortDebugString();
    }
}

void ScanChunkRequest::BuildAndSendScanMap(const ChunkRequest &request,
                                           uint64_t index, uint32_t crc) {
    // send rpc to leader
    brpc::Channel *channel = new brpc::Channel();
    if (channel->Init(peer_.addr, NULL) != 0) {
        LOG(ERROR) << "Fail to init channel to chunkserver for send scanmap: "
                   << peer_;
        delete channel;
        return;
    }

    // build scanmap
    ScanMap *scanMap = new ScanMap();
    scanMap->set_logicalpoolid(request.logicpoolid());
    scanMap->set_copysetid(request.copysetid());
    scanMap->set_chunkid(request.chunkid());
    scanMap->set_index(index);
    scanMap->set_crc(crc);
    scanMap->set_offset(request.offset());
    scanMap->set_len(request.size());

    FollowScanMapRequest *scanMapRequest = new FollowScanMapRequest();
    scanMapRequest->set_allocated_scanmap(scanMap);

    ScanService_Stub stub(channel);
    brpc::Controller* cntl = new brpc::Controller();
    cntl->set_timeout_ms(request.sendscanmaptimeoutms());
    FollowScanMapResponse *scanMapResponse = new FollowScanMapResponse();
    SendScanMapClosure *done = new SendScanMapClosure(
                                   scanMapRequest,
                                   scanMapResponse,
                                   request.sendscanmaptimeoutms(),
                                   request.sendscanmapretrytimes(),
                                   request.sendscanmapretryintervalus(),
                                   cntl, channel);
    LOG(INFO) << "logid = " << cntl->log_id()
              << " Sending scanmap: " << scanMap->ShortDebugString()
              << " to leader: " << peer_.addr;
    stub.FollowScanMap(cntl, scanMapRequest, scanMapResponse, done);
}

}  // namespace chunkserver
}  // namespace curve
