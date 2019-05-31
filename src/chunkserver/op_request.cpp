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
     * 如果propose成功，说明request成功交给了raft处理，
     * 那么done_就不能被调用，只有propose失败了才需要提前返回
     */
    const butil::IOBuf& data = cntl_->request_attachment();
    if (0 == Propose(request_, &data)) {
        doneGuard.release();
    }
}

int ChunkOpRequest::Propose(const ChunkRequest *request,
                            const butil::IOBuf *data) {
    // 检查任期和自己是不是Leader
    if (!node_->IsLeaderTerm()) {
        RedirectChunkRequest();
        return -1;
    }
    // 打包op request为task
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
     * 由于apply是异步的，有可能某个节点在term1是leader，apply了一条log，
     * 但是中间发生了主从切换，在很短的时间内这个节点又变为term3的leader，
     * 之前apply的日志才开始进行处理，这种情况下要实现严格意义上的复制状态
     * 机，需要解决这种ABA问题，可以在apply的时候设置leader当时的term
     */
    task.expected_term = node_->LeaderTerm();

    node_->Propose(task);

    return 0;
}

void ChunkOpRequest::RedirectChunkRequest() {
    // Fix bthread deadlock issue: CLDCFS-1120
//    PeerId leader = node_->GetLeaderId();
//    if (!leader.is_empty()) {
//        response_->set_redirect(leader.to_string());
//    }
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
    } else {
        // TODO(wudemiao) 考虑用LOG(FATAL)
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
     * 如果携带了applied index，且小于当前copyset node
     * 的最新applied index，或者 op类型为CHUNK_OP_RECOVER
     * 那么不需要走一致性协议
     */
    if ((request_->has_appliedindex()
        && node_->GetAppliedIndex() >= request_->appliedindex())
        || request_->optype() == CHUNK_OP_TYPE::CHUNK_OP_RECOVER) {
        /**
         * 构造shared_ptr<ReadChunkRequest>，因为在ChunkOpRequest只指定了
         * std::enable_shared_from_this<ChunkOpRequest>，所以
         * shared_from_this()返回的是shared_ptr<ChunkOpRequest>
         */
        auto thisPtr
            = std::dynamic_pointer_cast<ReadChunkRequest>(shared_from_this());
        /*
         * 将read扔给并发层出于两个原因：
         *  (1). 将read I/O操作和write等其它I/O操作都放在并发层处理，以便隔离
         *  disk I/O和其他逻辑
         *  (2). 为了保证线性一致性read的语义。因为当前apply是并发的，所以applied
         *  index更新也是并发的，尽管applied index更新能够保证单调的，但是可能会存
         *  在更新跳跃的情况，例如，index=6,7的2个op同时进入并发模块，并且都执行成
         *  功返回了，这个时候leader挂了，new leader选出来，new leader上面有
         *  index=6,7两个op的日志，但是没有apply，那么new leader必然需要回放这两
         *  条日志，因为是并发的，所以index=7的op log可能先于index=6的被apply，然后
         *  new leader的applied index会被更新为7，这个时候client来了一个想读index=6
         *  的op写下的数据，携带的是applied index=7，这个时候ChunkServer比较携带的
         *  applied index和Chunkserver的applied index，那么会判定通过走直接读，但是
         *  ChunkServer实际上index=6的数据还没落盘。那么就会出现stale read。解决方法
         *  就是read也进并发层排队，那么需要read index=6的read request，必定会排在
         *  index=6的op的后面，也就是它们操作的是同一个chunk，并发层会将它们放在同一个
         *  队列中，这样就能保证index=6的op apply之后，read才会被执行，这样就不会出现
         *  stale read，保证了read的线性一致性
         */
        auto task = std::bind(&ReadChunkRequest::OnApply,
                              thisPtr,
                              node_->GetAppliedIndex(),
                              doneGuard.release());
        concurrentApplyModule_->Push(request_->chunkid(), task);
        return;
    }

    /**
     * 如果没有携带applied index，那么走raft一致性协议read
     */
    if (0 == Propose(request_, nullptr)) {
        doneGuard.release();
    }
}

void ReadChunkRequest::OnApply(uint64_t index,
                               ::google::protobuf::Closure *done) {
    // 先清除response中的status，以保证CheckForward后的判断的正确性
    response_->clear_status();

    CSChunkInfo chunkInfo;
    CSErrorCode errorCode = datastore_->GetChunkInfo(request_->chunkid(),
                                                     &chunkInfo);
    do {
        if (CSErrorCode::ChunkNotExistError == errorCode) {
            response_->set_status(
                CHUNK_OP_STATUS::CHUNK_OP_STATUS_CHUNK_NOTEXIST);
            DVLOG(9) << "chunk not exist: "
                     << " logic pool id: " << request_->logicpoolid()
                     << " copyset id: " << request_->copysetid()
                     << " chunkid: " << request_->chunkid();
            break;
        } else if (CSErrorCode::Success != errorCode) {
            LOG(ERROR) << "get chunkinfo failed: "
                       << " logic pool id: " << request_->logicpoolid()
                       << " copyset id: " << request_->copysetid()
                       << " chunkid: " << request_->chunkid();
            response_->set_status(
                CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN);
            break;
        }
        // 如果需要从源端拷贝数据，需要将请求转发给clone manager处理
        if (NeedClone(chunkInfo)) {
            applyIndex = index;
            std::shared_ptr<CloneTask> cloneTask =
            cloneMgr_->GenerateCloneTask(
                std::dynamic_pointer_cast<ReadChunkRequest>(shared_from_this()),
                done);
            // TODO(yyk) 尽量不能阻塞队列，后面要具体考虑
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
            // 如果请求成功转发给了clone manager就可以直接返回了
            return;
        }
        // 如果是ReadChunk请求还需要从本地读取数据
        if (request_->optype() == CHUNK_OP_TYPE::CHUNK_OP_READ)
            ReadChunk();
    } while (false);

    if (response_->status() == CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS)
        node_->UpdateAppliedIndex(index);

    brpc::ClosureGuard doneGuard(done);
    auto maxIndex =
        (index > node_->GetAppliedIndex() ? index : node_->GetAppliedIndex());
    response_->set_appliedindex(maxIndex);
}

void ReadChunkRequest::OnApplyFromLog(std::shared_ptr<CSDataStore> datastore,
                                      const ChunkRequest &request,
                                      const butil::IOBuf &data) {
    // read什么都不用做
}

bool ReadChunkRequest::NeedClone(const CSChunkInfo& chunkInfo) {
    // 如果不是 clone chunk，就不需要拷贝
    if (chunkInfo.isClone) {
        off_t offset = request_->offset();
        size_t length = request_->size();
        uint32_t pageSize = chunkInfo.pageSize;
        uint32_t beginIndex = offset / pageSize;
        uint32_t endIndex = (offset + length - 1) / pageSize;
        // 如果是clone chunk，且存在未被写过的page，就需要拷贝
        if (chunkInfo.bitmap->NextClearBit(beginIndex, endIndex)
            != Bitmap::NO_POS) {
            return true;
        }
    }
    return false;
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

    if (CSErrorCode::Success == ret) {
        cntl_->response_attachment().append(readBuffer, size);
        response_->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        DVLOG(9) << "read success: "
                 << " logic pool id: " << request_->logicpoolid()
                 << " copyset id: " << request_->copysetid()
                 << " chunkid: " << request_->chunkid()
                 << " data size: " << request_->size()
                 << " read len :" << size;
    } else if (CSErrorCode::ChunkNotExistError == ret) {
        response_->set_status(
            CHUNK_OP_STATUS::CHUNK_OP_STATUS_CHUNK_NOTEXIST);
        DVLOG(9) << "chunk not exist: "
                 << " logic pool id: " << request_->logicpoolid()
                 << " copyset id: " << request_->copysetid()
                 << " chunkid: " << request_->chunkid();
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

    delete[] readBuffer;
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
    CSErrorCode ret = datastore_->DeleteSnapshotChunkOrCorrectSn(
        request_->chunkid(), request_->correctedsn());
    if (CSErrorCode::Success == ret) {
        response_->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        node_->UpdateAppliedIndex(index);
    } else {
        // TODO(wudemiao) 考虑用LOG(FATAL)
        LOG(ERROR) << "delete snapshot or correct sn failed: "
                   << " logic pool id: " << request_->logicpoolid()
                   << " copyset id: " << request_->copysetid()
                   << " chunkid: " << request_->chunkid()
                   << " correctedSn: " << request_->correctedsn()
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
    auto ret = datastore->DeleteSnapshotChunkOrCorrectSn(
        request.chunkid(), request.correctedsn());
    if (CSErrorCode::Success != ret) {
        LOG(WARNING) << "delete snapshot or correct sn failed: "
                     << request.logicpoolid() << ", "
                     << request.copysetid()
                     << " chunkid: " << request.chunkid()
                     << " correctedSn: " << request.correctedsn()
                     << " error: " << strerror(errno)
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
        DVLOG(9) << "create clone chunk success : "
                 << " logic pool id: " << request_->logicpoolid()
                 << " copyset id: " << request_->copysetid()
                 << " chunkid: " << request_->chunkid()
                 << " sn " << request_->sn()
                 << " correctedSn: " << request_->correctedsn()
                 << " location: " << request_->location();
        node_->UpdateAppliedIndex(index);
    } else {
        /**
         * 其他错误
         * TODO(wudemiao): 当前遇到createclonechunk错误直接fatal退出整个
         * ChunkServer后期考虑仅仅标坏这个copyset，保证较好的可用性
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
    }
    auto maxIndex =
        (index > node_->GetAppliedIndex() ? index : node_->GetAppliedIndex());
    response_->set_appliedindex(maxIndex);
}

void CreateCloneChunkRequest::OnApplyFromLog(std::shared_ptr<CSDataStore> datastore,  //NOLINT
                                             const ChunkRequest &request,
                                             const butil::IOBuf &data) {
    auto ret = datastore->CreateCloneChunk(request.chunkid(),
                                           request.sn(),
                                           request.correctedsn(),
                                           request.size(),
                                           request.location());
    if (CSErrorCode::Success != ret) {
        LOG(FATAL) << "create clone failed: "
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
     * 如果propose成功，说明request成功交给了raft处理，
     * 那么done_就不能被调用，只有propose失败了才需要提前返回
     */
    if (0 == Propose(request_, &data_)) {
        doneGuard.release();
    }
}

void PasteChunkInternalRequest::RedirectChunkRequest() {
    if (readRequest_ == nullptr)
        return;

    PeerId leader = node_->GetLeaderId();
    if (!leader.is_empty()) {
        readRequest_->response_->set_redirect(leader.to_string());
    }
    readRequest_->response_->set_status(
        CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED);
}

void PasteChunkInternalRequest::OnApply(uint64_t index,
                                        ::google::protobuf::Closure *done) {
    brpc::ClosureGuard doneGuard(done);

    auto ret = datastore_->PasteChunk(request_->chunkid(),
                                      data_.to_string().c_str(),  //NOLINT
                                      request_->offset(),
                                      request_->size());

    if (CSErrorCode::Success == ret) {
        if (readRequest_ != nullptr) {
            readRequest_->response_->set_status(
                CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        }
        DVLOG(9) << "paste chunk success : "
                 << " logic pool id: " << request_->logicpoolid()
                 << " copyset id: " << request_->copysetid()
                 << " chunkid: " << request_->chunkid()
                 << " offset: " << request_->offset()
                 << " length: " << request_->size();
        node_->UpdateAppliedIndex(index);
    } else {
        LOG(FATAL) << "paste chunk failed: "
                   << " logic pool id: " << request_->logicpoolid()
                   << " copyset id: " << request_->copysetid()
                   << " chunkid: " << request_->chunkid()
                   << " offset: " << request_->offset()
                   << " length: " << request_->size();
        if (readRequest_ != nullptr) {
            readRequest_->response_->set_status(
                CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN);
        }
    }
    if (readRequest_ != nullptr) {
        auto maxIndex = (index > node_->GetAppliedIndex()
                        ? index
                        : node_->GetAppliedIndex());
        readRequest_->response_->set_appliedindex(maxIndex);
    }
}

void PasteChunkInternalRequest::OnApplyFromLog(std::shared_ptr<CSDataStore> datastore,  //NOLINT
                                               const ChunkRequest &request,
                                               const butil::IOBuf &data) {
    auto ret = datastore->PasteChunk(request.chunkid(),
                                     data.to_string().c_str(),
                                     request.offset(),
                                     request.size());
    if (CSErrorCode::Success != ret) {
        LOG(FATAL) << "paste chunk failed: "
                   << " logic pool id: " << request.logicpoolid()
                   << " copyset id: " << request.copysetid()
                   << " chunkid: " << request.chunkid()
                   << " offset: " << request.offset()
                   << " length: " << request.size();
    }
}

}  // namespace chunkserver
}  // namespace curve
