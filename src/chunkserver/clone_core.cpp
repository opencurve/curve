/*
 * Project: curve
 * Created Date: Wednesday March 20th 2019
 * Author: yangyaokai
 * Copyright (c) 2018 netease
 */

#include <vector>

#include "src/common/bitmap.h"
#include "src/chunkserver/clone_core.h"
#include "src/chunkserver/op_request.h"
#include "src/chunkserver/copyset_node.h"
#include "src/chunkserver/datastore/chunkserver_datastore.h"

namespace curve {
namespace chunkserver {

using curve::common::Bitmap;

void CloneClosure::Run() {
    // 释放资源
    std::unique_ptr<CloneClosure> selfGuard(this);
    std::unique_ptr<ChunkRequest> requestGuard(request_);
    brpc::ClosureGuard doneGuard(done_);
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

    // 理论上到这里chunk肯定是存在的，如果返回ChunkNotExistError也当做错误处理
    if (errorCode != CSErrorCode::Success) {
        LOG(ERROR) << "get chunkinfo failed: "
                   << " logic pool id: " << request->logicpoolid()
                   << " copyset id: " << request->copysetid()
                   << " chunkid: " << request->chunkid()
                   << " error code: " << errorCode;
        SetResponse(readRequest,
                    CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN);
        return -1;
    }

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
    size_t cloneDataSize = length < sliceSize_ ? sliceSize_ : length;
    std::unique_ptr<char[]> cloneData = nullptr;

    // 请求提交到CloneManager的时候，chunk一定是clone chunk
    // 但是由于有其他请求操作相同的chunk，此时chunk有可能已经被遍写过了
    // 所以此处要先判断chunk是否是clone chunk，如果是再判断是否要拷贝数据
    if (chunkInfo.isClone) {
        // TODO(yyk) 这一块可以优化，但是优化方法判断条件可能比较复杂
        // 目前只根据是否存在未写过的page来决定是否要触发拷贝
        // chunk中请求读取范围内的数据存在page未被写过，则需要从源端拷贝数据
        if (chunkInfo.bitmap->NextClearBit(beginIndex, endIndex)
            != Bitmap::NO_POS) {
            cloneData = std::unique_ptr<char[]>(new char[cloneDataSize]);
            int ret = copyer_->Download(chunkInfo.location,
                                        offset,
                                        cloneDataSize,
                                        cloneData.get());
            // 从源端拷贝数据失败
            if (ret < 0) {
                LOG(ERROR) << "download origin data failed: "
                           << " logic pool id: " << request->logicpoolid()
                           << " copyset id: " << request->copysetid()
                           << " chunkid: " << request->chunkid()
                           << " location: " << chunkInfo.location
                           << " return: " << ret;
                SetResponse(readRequest,
                            CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN);
                return -1;
            }
        }
    }

    if (CHUNK_OP_TYPE::CHUNK_OP_RECOVER == request->optype()) {
        // 如果是CHUNK_OP_RECOVER，且不需要拷贝数据，直接返回成功
        if (cloneData == nullptr) {
            SetResponse(readRequest, CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        } else {
            PasteCloneData(readRequest,
                           cloneData.get(),
                           offset,
                           cloneDataSize,
                           doneGuard.release());
        }
    } else if (CHUNK_OP_TYPE::CHUNK_OP_READ == request->optype()) {
        // 释放doneGuard，回调交给ReadMergeReturn处理
        int ret = ReadMergeResponse(readRequest,
                                    chunkInfo,
                                    cloneData.get(),
                                    doneGuard.release());
        if (ret < 0)
            return ret;

        // cloneData不为nullptr，说明有拷贝数据，需要将数据paste到chunk
        if (cloneData != nullptr) {
            PasteCloneData(readRequest,
                           cloneData.get(),
                           offset,
                           cloneDataSize,
                           nullptr);
        }
    }

    return 0;
}

int CloneCore::ReadMergeResponse(std::shared_ptr<ReadChunkRequest> readRequest,
                                 const CSChunkInfo& chunkInfo,
                                 const char* cloneData,
                                 Closure* done) {
    brpc::ClosureGuard doneGuard(done);

    const ChunkRequest* request = readRequest->request_;
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
    std::unique_ptr<char[]> chunkData(new char[length]);
    std::shared_ptr<CSDataStore> dataStore = readRequest->datastore_;
    CSErrorCode errorCode;
    // 1.Read 对于已写过的区域，从chunk文件中读取
    for (auto& range : copiedRanges) {
        readOff = range.beginIndex * pageSize;
        readSize = (range.endIndex - range.beginIndex + 1) * pageSize;
        relativeOff = readOff - offset;
        errorCode = dataStore->ReadChunk(request->chunkid(),
                                         request->sn(),
                                         chunkData.get() + relativeOff,
                                         readOff,
                                         readSize);
        if (CSErrorCode::Success != errorCode) {
            SetResponse(readRequest,
                        CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN);
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
        memcpy(chunkData.get() + relativeOff,
               cloneData + relativeOff,
               readSize);
    }

    // 读成功后需要更新 apply index
    readRequest->node_->UpdateAppliedIndex(readRequest->applyIndex);
    // 3.Return 完成数据读取后可以将结果返回给用户
    readRequest->cntl_->response_attachment().append(
        chunkData.get(), length);
    SetResponse(readRequest, CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
    return 0;
}

void CloneCore::PasteCloneData(std::shared_ptr<ReadChunkRequest> readRequest,
                               const char* cloneData,
                               off_t offset,
                               size_t cloneDataSize,
                               Closure* done) {
    const ChunkRequest* request = readRequest->request_;
    // 数据拷贝完成以后，需要将产生PaseChunkRequest将数据Paste到chunk文件
    ChunkRequest* pasteRequest = new ChunkRequest();
    pasteRequest->set_optype(curve::chunkserver::CHUNK_OP_TYPE::CHUNK_OP_PASTE);
    pasteRequest->set_logicpoolid(request->logicpoolid());
    pasteRequest->set_copysetid(request->copysetid());
    pasteRequest->set_chunkid(request->chunkid());
    pasteRequest->set_offset(offset);
    pasteRequest->set_size(cloneDataSize);
    std::shared_ptr<PasteChunkInternalRequest> req = nullptr;

    CloneClosure* closure = new CloneClosure();
    closure->SetRequest(pasteRequest);
    closure->SetClosure(done);

    if (CHUNK_OP_TYPE::CHUNK_OP_RECOVER == request->optype()) {
        // 这里需要把ReadChunkRequest传给PasteChunkInternalRequest
        // 主要在里面设置ReadChunkRequest的response
        req =
            std::make_shared<PasteChunkInternalRequest>(readRequest,
                                                        readRequest->node_,
                                                        pasteRequest,
                                                        cloneData,
                                                        closure);
    } else {
        // 如果是CHUNK_OP_READ类型请求，rpc在前面已经返回给用户了
        // 所以这里不需要再把ReadChunkRequest传给PasteChunkInternalRequest
        req =
            std::make_shared<PasteChunkInternalRequest>(nullptr,
                                                        readRequest->node_,
                                                        pasteRequest,
                                                        cloneData,
                                                        closure);
    }
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
