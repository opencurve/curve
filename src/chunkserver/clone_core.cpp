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
    , downloadCtx_(downloadCtx)
    , cloneCore_(cloneCore)
    , readRequest_(readRequest)
    , done_(done) {
    // 记录初始metric
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
    // 记录结束metric
    const ChunkRequest* request = readRequest_->GetChunkRequest();
    ChunkServerMetric* csMetric = ChunkServerMetric::GetInstance();
    uint64_t latencyUs = TimeUtility::GetTimeofDayUs() - beginTime_;
    csMetric->OnResponse(request->logicpoolid(),
                         request->copysetid(),
                         CSIOMetricType::DOWNLOAD,
                         downloadCtx_->size,
                         latencyUs,
                         isFailed_);

    // 从源端拷贝数据失败
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
        // release doneGuard，将closure交给paste请求处理
        cloneCore_->PasteCloneData(readRequest_,
                                   &copyData,
                                   downloadCtx_->offset,
                                   downloadCtx_->size,
                                   doneGuard.release());
    } else if (CHUNK_OP_TYPE::CHUNK_OP_READ == request->optype()) {
        // 出错或处理结束调用closure返回给用户
        cloneCore_->SetReadChunkResponse(readRequest_, &copyData);

        // paste clone data是异步操作，很快就能处理完
        cloneCore_->PasteCloneData(readRequest_,
                                   &copyData,
                                   downloadCtx_->offset,
                                   downloadCtx_->size,
                                   nullptr);
    }
}

void CloneClosure::Run() {
    // 释放资源
    std::unique_ptr<CloneClosure> selfGuard(this);
    std::unique_ptr<ChunkRequest> requestGuard(request_);
    std::unique_ptr<ChunkResponse> responseGuard(response_);
    brpc::ClosureGuard doneGuard(done_);
    // 如果userResponse不为空，需要将response_中的相关内容赋值给userResponse
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

    // offset 和 length 必须与 pageSize 对齐
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

    // 请求提交到CloneManager的时候，chunk一定是clone chunk
    // 但是由于有其他请求操作相同的chunk，此时chunk有可能已经被遍写过了
    // 所以此处要先判断chunk是否是clone chunk，如果是再判断是否要拷贝数据
    bool needClone = chunkInfo.isClone &&
                    (chunkInfo.bitmap->NextClearBit(beginIndex, endIndex)
                     != Bitmap::NO_POS);
    if (needClone) {
        // TODO(yyk) 这一块可以优化，但是优化方法判断条件可能比较复杂
        // 目前只根据是否存在未写过的page来决定是否要触发拷贝
        // chunk中请求读取范围内的数据存在page未被写过，则需要从源端拷贝数据
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

    // 执行到这一步说明不需要拷贝数据，如果是recover请求可以直接返回成功
    // 如果是ReadChunk请求，则直接读chunk并返回
    if (CHUNK_OP_TYPE::CHUNK_OP_RECOVER == request->optype()) {
        SetResponse(readRequest, CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
    } else if (CHUNK_OP_TYPE::CHUNK_OP_READ == request->optype()) {
        // 出错或处理结束调用closure返回给用户
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

    // 获取chunk信息
    CSChunkInfo chunkInfo;
    ChunkID id = readRequest->ChunkId();
    std::shared_ptr<CSDataStore> dataStore = readRequest->datastore_;
    CSErrorCode errorCode = dataStore->GetChunkInfo(id, &chunkInfo);

    /*
    * chunk存在:按照查看分析bitmap判断是否可以本地读
    * chunk不存在:如包含clone信息则从clonesource读，否则返回错误
    *            因为上层ReadChunkRequest::OnApply已经处理了NoExist
    *            并且cloneinfo不存在的情况
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
        // 否则fallthrough直接返回错误
        FALLTHROUGH_INTENDED;
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

    // 读成功后需要更新 apply index
    readRequest->node_->UpdateAppliedIndex(readRequest->applyIndex);
    // Return 完成数据读取后可以将结果返回给用户
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

    // 如果chunk不存在，需要判断请求是否带源chunk的信息
    // 如果带了源chunk信息，说明用了lazy分配chunk机制，可以直接返回clone data
    // 有一种情况，当请求的chunk是lazy allocate的，请求时chunk在本地是存在的，
    // 并且请求读取的部分区域已经被写过，在从源端拷贝数据的时候，chunk又被删除了
    // 这种情况下会被当成正常请求返回，但是返回的数据不符合预期
    // 由于当前我们的curve file都是延迟删除的，文件真正删除时能够确保没有用户IO
    // 如果后续添加了一些改动触发到这个问题，则需要进行修复
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
    // 如果chunk存在，则要从chunk中读取已经写过的区域合并后返回
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

    // 读成功后需要更新 apply index
    readRequest->node_->UpdateAppliedIndex(readRequest->applyIndex);
    SetResponse(readRequest, CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
    return 0;
}

int CloneCore::ReadThenMerge(std::shared_ptr<ReadChunkRequest> readRequest,
                             const CSChunkInfo& chunkInfo,
                             const butil::IOBuf* cloneData,
                             char* chunkData) {
    const ChunkRequest* request = readRequest->request_;
    std::shared_ptr<CSDataStore> dataStore = readRequest->datastore_;

    off_t offset = request->offset();
    size_t length = request->size();
    uint32_t pageSize = chunkInfo.pageSize;
    uint32_t beginIndex = offset / pageSize;
    uint32_t endIndex = (offset + length - 1) / pageSize;
    // 获取chunk文件已经写过和未被写过的区域
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

    // 需要读取的起始位置在chunk中的偏移
    off_t readOff;
    // 读取到的数据要拷贝到缓冲区中的相对偏移
    off_t relativeOff;
    // 每次从chunk读取的数据长度
    size_t readSize;
    // 1.Read 对于已写过的区域，从chunk文件中读取
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

    // 2.Merge 对于未写过的区域，从源端下载的区域中拷贝出来进行merge
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

    // 数据拷贝完成以后，需要将产生PaseChunkRequest将数据Paste到chunk文件
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
    // 如果是recover chunk的请求，需要将paste的结果通过rpc返回
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
