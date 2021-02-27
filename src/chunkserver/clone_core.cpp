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
 * Created Date: Wednesday March 20th 2019
 * Author: yangyaokai
 */

#include <vector>
#include <string>

#include "src/common/bitmap.h"
#include "src/chunkserver/clone_core.h"
#include "src/chunkserver/op_request.h"
#include "src/chunkserver/copyset_node.h"
#include "src/chunkserver/chunk_service_closure.h"
#include "src/chunkserver/datastore/chunkserver_datastore.h"
#include "src/common/timeutility.h"

namespace curve {
namespace chunkserver {

using curve::common::Bitmap;
using curve::common::TimeUtility;

static void ReadBufferDeleter(void* ptr) {
    delete[] static_cast<char*>(ptr);
}

DownloadClosure::DownloadClosure(std::shared_ptr<ReadChunkRequest> readRequest,
                                 std::shared_ptr<CloneCore> cloneCore,
                                 AsyncDownloadContext* downloadCtx,
                                 Closure* done)
    : isFailed_(false)
    , beginTime_(TimeUtility::GetTimeofDayUs())
    , readRequest_(readRequest)
    , cloneCore_(cloneCore)
    , downloadCtx_(downloadCtx)
    , done_(done) {
    // record initial metric
    if (readRequest_ != nullptr) {
        const ChunkRequest* request = readRequest_->GetChunkRequest();
        ChunkServerMetric* csMetric = ChunkServerMetric::GetInstance();
        csMetric->OnRequest(request->logicpoolid(),
                            request->copysetid(),
                            CSIOMetricType::DOWNLOAD);
    }
}

void DownloadClosure::Run() {
    std::unique_ptr<DownloadClosure> selfGuard(this);
    std::unique_ptr<AsyncDownloadContext> contextGuard(downloadCtx_);
    brpc::ClosureGuard doneGuard(done_);
    butil::IOBuf copyData;
    copyData.append_user_data(
        downloadCtx_->buf, downloadCtx_->size, ReadBufferDeleter);

    CHECK(readRequest_ != nullptr) << "read request is nullptr.";
    // Record ending metric
    const ChunkRequest* request = readRequest_->GetChunkRequest();
    ChunkServerMetric* csMetric = ChunkServerMetric::GetInstance();
    uint64_t latencyUs = TimeUtility::GetTimeofDayUs() - beginTime_;
    csMetric->OnResponse(request->logicpoolid(),
                         request->copysetid(),
                         CSIOMetricType::DOWNLOAD,
                         downloadCtx_->size,
                         latencyUs,
                         isFailed_);

    // Fail to copy data from source
    if (isFailed_) {
        LOG(ERROR) << "download origin data failed: "
                    << " logic pool id: " << request->logicpoolid()
                    << " copyset id: " << request->copysetid()
                    << " chunkid: " << request->chunkid()
                    << " AsyncDownloadContext: " << *downloadCtx_;
        cloneCore_->SetResponse(
            readRequest_, CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN);
        return;
    }

    if (CHUNK_OP_TYPE::CHUNK_OP_RECOVER == request->optype()) {
        // release doneGuard，hand closure to paste request
        cloneCore_->PasteCloneData(readRequest_,
                                   &copyData,
                                   downloadCtx_->offset,
                                   downloadCtx_->size,
                                   doneGuard.release());
    } else if (CHUNK_OP_TYPE::CHUNK_OP_READ == request->optype()) {
        // If an error occurs or processing is completed, the closure is returned to the user
        cloneCore_->SetReadChunkResponse(readRequest_, &copyData);

        // paste clone data is an asynchronous operation and can be processed very quickly
        cloneCore_->PasteCloneData(readRequest_,
                                   &copyData,
                                   downloadCtx_->offset,
                                   downloadCtx_->size,
                                   nullptr);
    }
}

void CloneClosure::Run() {
    // free resources
    std::unique_ptr<CloneClosure> selfGuard(this);
    std::unique_ptr<ChunkRequest> requestGuard(request_);
    std::unique_ptr<ChunkResponse> responseGuard(response_);
    brpc::ClosureGuard doneGuard(done_);
    // If userResponse is not empty, you need to assign the relevant content in response_ to userResponse
    if (userResponse_ != nullptr) {
        if (response_->has_status()) {
            userResponse_->set_status(response_->status());
        }
        if (response_->has_redirect()) {
            userResponse_->set_redirect(response_->redirect());
        }
        if (response_->has_appliedindex()) {
            userResponse_->set_appliedindex(response_->appliedindex());
        }
    }
}

int CloneCore::CloneReadByLocalInfo(
    std::shared_ptr<ReadChunkRequest> readRequest,
    const CSChunkInfo &chunkInfo, Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    const ChunkRequest* request = readRequest->request_;
    off_t offset = request->offset();
    size_t length = request->size();
    uint32_t pageSize = chunkInfo.pageSize;

    // offset and length must be aligned with pageSize
    if (offset % pageSize != 0 || length % pageSize != 0) {
        LOG(ERROR) << "Invalid offset or length: "
                   << " logic pool id: " << request->logicpoolid()
                   << " copyset id: " << request->copysetid()
                   << " chunkid: " << request->chunkid()
                   << " offset: " << offset
                   << " length: " << length
                   << " page size: " << pageSize;
        SetResponse(readRequest,
                    CHUNK_OP_STATUS::CHUNK_OP_STATUS_INVALID_REQUEST);
        return -1;
    }

    uint32_t beginIndex = offset / pageSize;
    uint32_t endIndex = (offset + length - 1) / pageSize;

    // When the request is submitted to the CloneManager, the chunk must be a clone chunk,
    // but it is possible that the chunk has already been traversed at this point because
    // there are other requests that operate on the same chunk. So here we have to check
    // if the chunk is a clone chunk, and if so, whether we want to copy the data.
    bool needClone = chunkInfo.isClone &&
                    (chunkInfo.bitmap->NextClearBit(beginIndex, endIndex)
                     != Bitmap::NO_POS);
    if (needClone) {
        // TODO(yyk) This place can be optimised, but the optimisation method may be complicated to check the conditions
        // Currently only the presence of unwritten pages determines whether a copy should be triggered
        // If there is a page within the requested read range in the chunk that has not been written,
        // the data needs to be copied from the source
        AsyncDownloadContext* downloadCtx =
            new (std::nothrow) AsyncDownloadContext;
        downloadCtx->location = chunkInfo.location;
        downloadCtx->offset = offset;
        downloadCtx->size = length;
        downloadCtx->buf = new (std::nothrow) char[length];
        DownloadClosure* downloadClosure =
            new (std::nothrow) DownloadClosure(readRequest,
                                               shared_from_this(),
                                               downloadCtx,
                                               doneGuard.release());
        copyer_->DownloadAsync(downloadClosure);
        return 0;
    }

    // Running at this steo means that there is no need to copy the data, if it is a recover request it can just return success
    // If it is a ReadChunk request, the chunk is read directly and returned
    if (CHUNK_OP_TYPE::CHUNK_OP_RECOVER == request->optype()) {
        SetResponse(readRequest, CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
    } else if (CHUNK_OP_TYPE::CHUNK_OP_READ == request->optype()) {
        // Call closure to return to the user when an error occurs or when processing is completed
        return ReadChunk(readRequest);
    }
    return 0;
}

void CloneCore::CloneReadByRequestInfo(std::shared_ptr<ReadChunkRequest>
    readRequest, Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    const ChunkRequest*  chunkRequest = readRequest->request_;

    auto func = ::curve::common::LocationOperator::GenerateCurveLocation;
    std::string location = func(chunkRequest->clonefilesource(),
        chunkRequest->clonefileoffset());

    AsyncDownloadContext* downloadCtx =
        new (std::nothrow) AsyncDownloadContext;
    downloadCtx->location = location;
    downloadCtx->offset = chunkRequest->offset();
    downloadCtx->size = chunkRequest->size();
    downloadCtx->buf = new (std::nothrow) char[chunkRequest->size()];
    DownloadClosure* downloadClosure =
    new (std::nothrow) DownloadClosure(readRequest,
                                    shared_from_this(),
                                    downloadCtx,
                                    doneGuard.release());
    copyer_->DownloadAsync(downloadClosure);
    return;
}

int CloneCore::HandleReadRequest(
    std::shared_ptr<ReadChunkRequest> readRequest,
    Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    const ChunkRequest* request = readRequest->request_;

    // get chunk info
    CSChunkInfo chunkInfo;
    ChunkID id = readRequest->ChunkId();
    std::shared_ptr<CSDataStore> dataStore = readRequest->datastore_;
    CSErrorCode errorCode = dataStore->GetChunkInfo(id, &chunkInfo);

    /*
    * The chunk exists: check the analysis bitmap to see if it can be read locally.
    * The chunk not exists:Read from clonesource if it contains clone
    *            information, otherwise return an error because the upper level ReadChunkRequest::OnApply
    *            has already handled the NoExist and cloneinfo not exist case
    */
    switch (errorCode) {
    case CSErrorCode::Success:
        return CloneReadByLocalInfo(readRequest, chunkInfo,
                                    doneGuard.release());
    case CSErrorCode::ChunkNotExistError:
        if (existCloneInfo(request)) {
            CloneReadByRequestInfo(readRequest, doneGuard.release());
            return 0;
        }
        // Otherwise fallthrough returns an error directly
    default:
        LOG(ERROR) << "get chunkinfo failed: "
                   << " logic pool id: " << request->logicpoolid()
                   << " copyset id: " << request->copysetid()
                   << " chunkid: " << request->chunkid()
                   << " error code: " << errorCode;
        SetResponse(readRequest,
                    CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN);
        return -1;
    }
}

int CloneCore::ReadChunk(std::shared_ptr<ReadChunkRequest> readRequest) {
    const ChunkRequest* request = readRequest->request_;
    off_t offset = request->offset();
    size_t length = request->size();
    std::unique_ptr<char[]> chunkData(new char[length]);
    std::shared_ptr<CSDataStore> dataStore = readRequest->datastore_;
    CSErrorCode errorCode;
    errorCode = dataStore->ReadChunk(request->chunkid(),
                                     request->sn(),
                                     chunkData.get(),
                                     offset,
                                     length);
    if (CSErrorCode::Success != errorCode) {
        SetResponse(readRequest,
                    CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN);
        LOG(ERROR) << "read chunk failed: "
                    << " logic pool id: " << request->logicpoolid()
                    << " copyset id: " << request->copysetid()
                    << " chunkid: " << request->chunkid()
                    << " read offset: " << offset
                    << " read length: " << length
                    << " error code: " << errorCode;
        return -1;
    }

    // You need to update the apply index after a successful read.
    readRequest->node_->UpdateAppliedIndex(readRequest->applyIndex);
    // Return the results to the user once the data has been read
    readRequest->cntl_->response_attachment().append(
        chunkData.get(), length);
    SetResponse(readRequest, CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
    return 0;
}

int CloneCore::SetReadChunkResponse(
    std::shared_ptr<ReadChunkRequest> readRequest,
    const butil::IOBuf* cloneData) {
    const ChunkRequest* request = readRequest->request_;
    CSChunkInfo chunkInfo;
    ChunkID id = readRequest->ChunkId();
    std::shared_ptr<CSDataStore> dataStore = readRequest->datastore_;
    CSErrorCode errorCode = dataStore->GetChunkInfo(id, &chunkInfo);

    // If the chunk does not exist, you need to check if the request has information about the source chunk
    // If the source chunk information is provided, it means that the lazy chunk allocation mechanism is used and the clone data can be returned directly.
    // There is a case when the requested chunk is lazy allocate and the chunk exists locally at the time of the request,
    // and part of the area requested to be read has already been written, and the chunk is deleted again when the data is copied from the source
    // In this case it will be returned as a normal request, but the data returned is not as expected
    // Since our current curve files are deleted on a delayed basis, no user IO can be ensured when the file is actually deleted.
    // If subsequent changes are added that trigger this issue, a fix will be required
    // TODO(yyk) fix it
    bool expect = errorCode == CSErrorCode::Success ||
                  (errorCode == CSErrorCode::ChunkNotExistError &&
                   existCloneInfo(request));
    if (!expect) {
        LOG(ERROR) << "get chunkinfo failed: "
                   << " logic pool id: " << request->logicpoolid()
                   << " copyset id: " << request->copysetid()
                   << " chunkid: " << request->chunkid()
                   << " error code: " << errorCode;
        SetResponse(readRequest,
                    CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN);
        return -1;
    }

    size_t length = request->size();
    butil::IOBuf responseData;
    // If the chunk exists, read the already written area from the chunk and merge it back
    if (errorCode == CSErrorCode::Success) {
        char* chunkData = new (std::nothrow) char[length];
        int ret = ReadThenMerge(
            readRequest, chunkInfo, cloneData, chunkData);
        responseData.append_user_data(chunkData, length, ReadBufferDeleter);
        if (ret < 0) {
            SetResponse(readRequest,
                        CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN);
            return ret;
        }
    } else {
        responseData = *cloneData;
    }
    readRequest->cntl_->response_attachment().append(responseData);

    // You need to update the apply index after a successful read.
    readRequest->node_->UpdateAppliedIndex(readRequest->applyIndex);
    SetResponse(readRequest, CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
    return 0;
}

int CloneCore::ReadThenMerge(std::shared_ptr<ReadChunkRequest> readRequest,
                             const CSChunkInfo& chunkInfo,
                             const butil::IOBuf* cloneData,
                             char* chunkData) {
    const ChunkRequest* request = readRequest->request_;
    ChunkID id = readRequest->ChunkId();
    std::shared_ptr<CSDataStore> dataStore = readRequest->datastore_;

    off_t offset = request->offset();
    size_t length = request->size();
    uint32_t pageSize = chunkInfo.pageSize;
    uint32_t beginIndex = offset / pageSize;
    uint32_t endIndex = (offset + length - 1) / pageSize;
    // Get the areas of the chunk file that have been written to and not written to
    std::vector<BitRange> copiedRanges;
    std::vector<BitRange> uncopiedRanges;
    if (chunkInfo.isClone) {
        chunkInfo.bitmap->Divide(beginIndex,
                                 endIndex,
                                 &uncopiedRanges,
                                 &copiedRanges);
    } else {
        BitRange range;
        range.beginIndex = beginIndex;
        range.endIndex = endIndex;
        copiedRanges.push_back(range);
    }

    // Offset of the starting position in the chunk to be read
    off_t readOff;
    // Relative offset of the read data to be copied into the buffer
    off_t relativeOff;
    // Length of data read from chunk at a time
    size_t readSize;
    // 1.Read from the chunk file for areas that have been written
    CSErrorCode errorCode;
    for (auto& range : copiedRanges) {
        readOff = range.beginIndex * pageSize;
        readSize = (range.endIndex - range.beginIndex + 1) * pageSize;
        relativeOff = readOff - offset;
        errorCode = dataStore->ReadChunk(request->chunkid(),
                                         request->sn(),
                                         chunkData + relativeOff,
                                         readOff,
                                         readSize);
        if (CSErrorCode::Success != errorCode) {
            LOG(ERROR) << "read chunk failed: "
                       << " logic pool id: " << request->logicpoolid()
                       << " copyset id: " << request->copysetid()
                       << " chunkid: " << request->chunkid()
                       << " read offset: " << readOff
                       << " read length: " << readSize
                       << " error code: " << errorCode;
            return -1;
        }
    }

    // 2.Copy and merge from the source download for unwritten regions
    for (auto& range : uncopiedRanges) {
        readOff = range.beginIndex * pageSize;
        readSize = (range.endIndex - range.beginIndex + 1) * pageSize;
        relativeOff = readOff - offset;
        cloneData->copy_to(chunkData + relativeOff, readSize, relativeOff);
    }
    return 0;
}

void CloneCore::PasteCloneData(std::shared_ptr<ReadChunkRequest> readRequest,
                               const butil::IOBuf* cloneData,
                               off_t offset,
                               size_t cloneDataSize,
                               Closure* done) {
    const ChunkRequest* request = readRequest->request_;
    bool dontPaste = CHUNK_OP_TYPE::CHUNK_OP_READ == request->optype()
                     && !enablePaste_;
    if (dontPaste) return;

    // Once the data has been copied, a PaseChunkRequest needs to be generated to paste the data into the chunk file
    ChunkRequest* pasteRequest = new ChunkRequest();
    pasteRequest->set_optype(curve::chunkserver::CHUNK_OP_TYPE::CHUNK_OP_PASTE);
    pasteRequest->set_logicpoolid(request->logicpoolid());
    pasteRequest->set_copysetid(request->copysetid());
    pasteRequest->set_chunkid(request->chunkid());
    pasteRequest->set_offset(offset);
    pasteRequest->set_size(cloneDataSize);
    std::shared_ptr<PasteChunkInternalRequest> req = nullptr;

    ChunkResponse* pasteResponse = new ChunkResponse();
    CloneClosure* closure = new CloneClosure();
    closure->SetRequest(pasteRequest);
    closure->SetResponse(pasteResponse);
    closure->SetClosure(done);
    // In the case of a recover chunk request, the result of the paste should be returned via rpc
    if (CHUNK_OP_TYPE::CHUNK_OP_RECOVER == request->optype()) {
        closure->SetUserResponse(readRequest->response_);
    }

    ChunkServiceClosure* pasteClosure =
        new (std::nothrow) ChunkServiceClosure(nullptr,
                                               pasteRequest,
                                               pasteResponse,
                                               closure);

    req = std::make_shared<PasteChunkInternalRequest>(readRequest->node_,
                                                      pasteRequest,
                                                      pasteResponse,
                                                      cloneData,
                                                      pasteClosure);
    req->Process();
}

inline void CloneCore::SetResponse(
    std::shared_ptr<ReadChunkRequest> readRequest, CHUNK_OP_STATUS status) {
    auto applyIndex = readRequest->node_->GetAppliedIndex();
    readRequest->response_->set_appliedindex(applyIndex);
    readRequest->response_->set_status(status);
}

}  // namespace chunkserver
}  // namespace curve
