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

#include "src/chunkserver/clone_core.h"

#include <string>
#include <vector>

#include "src/chunkserver/chunk_service_closure.h"
#include "src/chunkserver/copyset_node.h"
#include "src/chunkserver/datastore/chunkserver_datastore.h"
#include "src/chunkserver/op_request.h"
#include "src/common/bitmap.h"
#include "src/common/timeutility.h"

namespace curve
{
    namespace chunkserver
    {

        using curve::common::Bitmap;
        using curve::common::TimeUtility;

        static void ReadBufferDeleter(void *ptr) { delete[] static_cast<char *>(ptr); }

        DownloadClosure::DownloadClosure(std::shared_ptr<ReadChunkRequest> readRequest,
                                         std::shared_ptr<CloneCore> cloneCore,
                                         AsyncDownloadContext *downloadCtx,
                                         Closure *done)
            : isFailed_(false),
              beginTime_(TimeUtility::GetTimeofDayUs()),
              downloadCtx_(downloadCtx),
              cloneCore_(cloneCore),
              readRequest_(readRequest),
              done_(done)
        {
            // Record initial metric
            if (readRequest_ != nullptr)
            {
                const ChunkRequest *request = readRequest_->GetChunkRequest();
                ChunkServerMetric *csMetric = ChunkServerMetric::GetInstance();
                csMetric->OnRequest(request->logicpoolid(), request->copysetid(),
                                    CSIOMetricType::DOWNLOAD);
            }
        }

        void DownloadClosure::Run()
        {
            std::unique_ptr<DownloadClosure> selfGuard(this);
            std::unique_ptr<AsyncDownloadContext> contextGuard(downloadCtx_);
            brpc::ClosureGuard doneGuard(done_);
            butil::IOBuf copyData;
            copyData.append_user_data(downloadCtx_->buf, downloadCtx_->size,
                                      ReadBufferDeleter);

            CHECK(readRequest_ != nullptr) << "read request is nullptr.";
            // Record End Metric
            const ChunkRequest *request = readRequest_->GetChunkRequest();
            ChunkServerMetric *csMetric = ChunkServerMetric::GetInstance();
            uint64_t latencyUs = TimeUtility::GetTimeofDayUs() - beginTime_;
            csMetric->OnResponse(request->logicpoolid(), request->copysetid(),
                                 CSIOMetricType::DOWNLOAD, downloadCtx_->size,
                                 latencyUs, isFailed_);

            // Copying data from the source failed
            if (isFailed_)
            {
                LOG(ERROR) << "download origin data failed: "
                           << " logic pool id: " << request->logicpoolid()
                           << " copyset id: " << request->copysetid()
                           << " chunkid: " << request->chunkid()
                           << " AsyncDownloadContext: " << *downloadCtx_;
                cloneCore_->SetResponse(
                    readRequest_, CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN);
                return;
            }

            if (CHUNK_OP_TYPE::CHUNK_OP_RECOVER == request->optype())
            {
                // Release doneGuard, hand over the closure to the pass request for
                // processing
                cloneCore_->PasteCloneData(readRequest_, &copyData,
                                           downloadCtx_->offset, downloadCtx_->size,
                                           doneGuard.release());
            }
            else if (CHUNK_OP_TYPE::CHUNK_OP_READ == request->optype())
            {
                // Error or end of processing call closure returned to user
                cloneCore_->SetReadChunkResponse(readRequest_, &copyData);

                // Paste clone data is an asynchronous operation that can be processed
                // quickly
                cloneCore_->PasteCloneData(readRequest_, &copyData,
                                           downloadCtx_->offset, downloadCtx_->size,
                                           nullptr);
            }
        }

        void CloneClosure::Run()
        {
            // Release resources
            std::unique_ptr<CloneClosure> selfGuard(this);
            std::unique_ptr<ChunkRequest> requestGuard(request_);
            std::unique_ptr<ChunkResponse> responseGuard(response_);
            brpc::ClosureGuard doneGuard(done_);
            // If userResponse is not empty, you need to set the response_ Assign the
            // relevant content in to userResponse
            if (userResponse_ != nullptr)
            {
                if (response_->has_status())
                {
                    userResponse_->set_status(response_->status());
                }
                if (response_->has_redirect())
                {
                    userResponse_->set_redirect(response_->redirect());
                }
                if (response_->has_appliedindex())
                {
                    userResponse_->set_appliedindex(response_->appliedindex());
                }
            }
        }

        int CloneCore::CloneReadByLocalInfo(
            std::shared_ptr<ReadChunkRequest> readRequest, const CSChunkInfo &chunkInfo,
            Closure *done)
        {
            brpc::ClosureGuard doneGuard(done);
            const ChunkRequest *request = readRequest->request_;
            off_t offset = request->offset();
            size_t length = request->size();
            const uint32_t blockSize = chunkInfo.blockSize;

            // offset and length must be aligned with blockSize
            if (offset % blockSize != 0 || length % blockSize != 0)
            {
                LOG(ERROR) << "Invalid offset or length: "
                           << " logic pool id: " << request->logicpoolid()
                           << " copyset id: " << request->copysetid()
                           << " chunkid: " << request->chunkid()
                           << " offset: " << offset << " length: " << length
                           << " block size: " << blockSize;
                SetResponse(readRequest,
                            CHUNK_OP_STATUS::CHUNK_OP_STATUS_INVALID_REQUEST);
                return -1;
            }

            uint32_t beginIndex = offset / blockSize;
            uint32_t endIndex = (offset + length - 1) / blockSize;

            // When submitting a request to CloneManager, the chunk must be a clone
            // chunk However, due to other requests for the same chunk, it is possible
            // that the chunk has already been overwritten at this time So here we need
            // to first determine whether the chunk is a clone chunk, and then determine
            // whether to copy the data if so
            bool needClone = chunkInfo.isClone &&
                             (chunkInfo.bitmap->NextClearBit(beginIndex, endIndex) !=
                              Bitmap::NO_POS);
            if (needClone)
            {
                // The TODO(yyk) block can be optimized, but the optimization method may
                // determine complex conditions Currently, the decision to trigger
                // copying is only based on whether there are unwritten pages If the
                // data within the requested read range in the chunk has a page that has
                // not been written, it is necessary to copy the data from the source
                // side
                AsyncDownloadContext *downloadCtx =
                    new (std::nothrow) AsyncDownloadContext;
                downloadCtx->location = chunkInfo.location;
                downloadCtx->offset = offset;
                downloadCtx->size = length;
                downloadCtx->buf = new (std::nothrow) char[length];
                DownloadClosure *downloadClosure = new (std::nothrow) DownloadClosure(
                    readRequest, shared_from_this(), downloadCtx, doneGuard.release());
                copyer_->DownloadAsync(downloadClosure);
                return 0;
            }

            // Performing this step indicates that there is no need to copy data. If it
            // is a recover request, it can directly return success If it is a ReadChunk
            // request, read the chunk directly and return
            if (CHUNK_OP_TYPE::CHUNK_OP_RECOVER == request->optype())
            {
                SetResponse(readRequest, CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
            }
            else if (CHUNK_OP_TYPE::CHUNK_OP_READ == request->optype())
            {
                // Error or end of processing call closure returned to user
                return ReadChunk(readRequest);
            }
            return 0;
        }

        void CloneCore::CloneReadByRequestInfo(
            std::shared_ptr<ReadChunkRequest> readRequest, Closure *done)
        {
            brpc::ClosureGuard doneGuard(done);
            const ChunkRequest *chunkRequest = readRequest->request_;

            auto func = ::curve::common::LocationOperator::GenerateCurveLocation;
            std::string location =
                func(chunkRequest->clonefilesource(), chunkRequest->clonefileoffset());

            AsyncDownloadContext *downloadCtx = new (std::nothrow) AsyncDownloadContext;
            downloadCtx->location = location;
            downloadCtx->offset = chunkRequest->offset();
            downloadCtx->size = chunkRequest->size();
            downloadCtx->buf = new (std::nothrow) char[chunkRequest->size()];
            DownloadClosure *downloadClosure = new (std::nothrow) DownloadClosure(
                readRequest, shared_from_this(), downloadCtx, doneGuard.release());
            copyer_->DownloadAsync(downloadClosure);
            return;
        }

        int CloneCore::HandleReadRequest(std::shared_ptr<ReadChunkRequest> readRequest,
                                         Closure *done)
        {
            brpc::ClosureGuard doneGuard(done);
            const ChunkRequest *request = readRequest->request_;

            // Obtain chunk information
            CSChunkInfo chunkInfo;
            ChunkID id = readRequest->ChunkId();
            std::shared_ptr<CSDataStore> dataStore = readRequest->datastore_;
            CSErrorCode errorCode = dataStore->GetChunkInfo(id, &chunkInfo);

            /*
             * Chunk exists: Check and analyze Bitmap to determine if it can be read
             * locally Chunk does not exist: if it contains clone information, it will be
             * read from clonesource, otherwise an error will be returned Because the
             * upper level ReadChunkRequest::OnApply has already processed NoExist And
             * the situation where cloneinfo does not exist
             */
            switch (errorCode)
            {
            case CSErrorCode::Success:
                return CloneReadByLocalInfo(readRequest, chunkInfo,
                                            doneGuard.release());
            case CSErrorCode::ChunkNotExistError:
                if (existCloneInfo(request))
                {
                    CloneReadByRequestInfo(readRequest, doneGuard.release());
                    return 0;
                }
                // Otherwise, fallthrough will directly return an error
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

        int CloneCore::ReadChunk(std::shared_ptr<ReadChunkRequest> readRequest)
        {
            const ChunkRequest *request = readRequest->request_;
            off_t offset = request->offset();
            size_t length = request->size();
            std::unique_ptr<char[]> chunkData(new char[length]);
            std::shared_ptr<CSDataStore> dataStore = readRequest->datastore_;
            CSErrorCode errorCode;
            errorCode = dataStore->ReadChunk(request->chunkid(), request->sn(),
                                             chunkData.get(), offset, length);
            if (CSErrorCode::Success != errorCode)
            {
                SetResponse(readRequest,
                            CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN);
                LOG(ERROR) << "read chunk failed: "
                           << " logic pool id: " << request->logicpoolid()
                           << " copyset id: " << request->copysetid()
                           << " chunkid: " << request->chunkid()
                           << " read offset: " << offset << " read length: " << length
                           << " error code: " << errorCode;
                return -1;
            }

            // After successful reading, update the apply index
            readRequest->node_->UpdateAppliedIndex(readRequest->applyIndex);
            // After completing the data reading, Return can return the results to the
            // user
            readRequest->cntl_->response_attachment().append(chunkData.get(), length);
            SetResponse(readRequest, CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
            return 0;
        }

        int CloneCore::SetReadChunkResponse(
            std::shared_ptr<ReadChunkRequest> readRequest,
            const butil::IOBuf *cloneData)
        {
            const ChunkRequest *request = readRequest->request_;
            CSChunkInfo chunkInfo;
            ChunkID id = readRequest->ChunkId();
            std::shared_ptr<CSDataStore> dataStore = readRequest->datastore_;
            CSErrorCode errorCode = dataStore->GetChunkInfo(id, &chunkInfo);

            // If the chunk does not exist, it is necessary to determine whether the
            // request contains information about the source chunk If the source chunk
            // information is provided, it indicates that the lazy allocation chunk
            // mechanism is used, and clone data can be directly returned There is a
            // situation where the requested chunk is lazily allocated and the requested
            // chunk exists locally, And the requested read area has already been
            // written, and when copying data from the source, the chunk has been
            // deleted again In this case, it will be returned as a normal request, but
            // the returned data does not meet expectations Due to the current delayed
            // deletion of our curve files, it is ensured that there is no user IO when
            // the files are truly deleted If some changes are added later that trigger
            // this issue, it needs to be fixed
            //  TODO(yyk) fix it
            bool expect = errorCode == CSErrorCode::Success ||
                          (errorCode == CSErrorCode::ChunkNotExistError &&
                           existCloneInfo(request));
            if (!expect)
            {
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
            // If a chunk exists, read the regions that have already been written from
            // the chunk and merge them back
            if (errorCode == CSErrorCode::Success)
            {
                char *chunkData = new (std::nothrow) char[length];
                int ret = ReadThenMerge(readRequest, chunkInfo, cloneData, chunkData);
                responseData.append_user_data(chunkData, length, ReadBufferDeleter);
                if (ret < 0)
                {
                    SetResponse(readRequest,
                                CHUNK_OP_STATUS::CHUNK_OP_STATUS_FAILURE_UNKNOWN);
                    return ret;
                }
            }
            else
            {
                responseData = *cloneData;
            }
            readRequest->cntl_->response_attachment().append(responseData);

            // After successful reading, update the apply index
            readRequest->node_->UpdateAppliedIndex(readRequest->applyIndex);
            SetResponse(readRequest, CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
            return 0;
        }

        int CloneCore::ReadThenMerge(std::shared_ptr<ReadChunkRequest> readRequest,
                                     const CSChunkInfo &chunkInfo,
                                     const butil::IOBuf *cloneData, char *chunkData)
        {
            const ChunkRequest *request = readRequest->request_;
            std::shared_ptr<CSDataStore> dataStore = readRequest->datastore_;

            off_t offset = request->offset();
            size_t length = request->size();
            uint32_t blockSize = chunkInfo.blockSize;
            uint32_t beginIndex = offset / blockSize;
            uint32_t endIndex = (offset + length - 1) / blockSize;
            // Obtain the regions where the chunk file has been written and not written
            std::vector<BitRange> copiedRanges;
            std::vector<BitRange> uncopiedRanges;
            if (chunkInfo.isClone)
            {
                chunkInfo.bitmap->Divide(beginIndex, endIndex, &uncopiedRanges,
                                         &copiedRanges);
            }
            else
            {
                BitRange range;
                range.beginIndex = beginIndex;
                range.endIndex = endIndex;
                copiedRanges.push_back(range);
            }

            // The offset of the starting position to be read in the chunk
            off_t readOff;
            // The relative offset of the read data to be copied into the buffer
            off_t relativeOff;
            // The length of data read from chunk each time
            size_t readSize;
            // 1. Read for regions that have already been written, read from the chunk
            // file
            CSErrorCode errorCode;
            for (auto &range : copiedRanges)
            {
                readOff = range.beginIndex * blockSize;
                readSize = (range.endIndex - range.beginIndex + 1) * blockSize;
                relativeOff = readOff - offset;
                errorCode =
                    dataStore->ReadChunk(request->chunkid(), request->sn(),
                                         chunkData + relativeOff, readOff, readSize);
                if (CSErrorCode::Success != errorCode)
                {
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

            // 2. Merge: For areas that have not been written before, copy them from the
            // downloaded area on the source side for merging
            for (auto &range : uncopiedRanges)
            {
                readOff = range.beginIndex * blockSize;
                readSize = (range.endIndex - range.beginIndex + 1) * blockSize;
                relativeOff = readOff - offset;
                cloneData->copy_to(chunkData + relativeOff, readSize, relativeOff);
            }
            return 0;
        }

        void CloneCore::PasteCloneData(std::shared_ptr<ReadChunkRequest> readRequest,
                                       const butil::IOBuf *cloneData, off_t offset,
                                       size_t cloneDataSize, Closure *done)
        {
            const ChunkRequest *request = readRequest->request_;
            bool dontPaste =
                CHUNK_OP_TYPE::CHUNK_OP_READ == request->optype() && !enablePaste_;
            if (dontPaste)
                return;

            // After the data copy is completed, it is necessary to generate a
            // PaseChunkRequest and paste the data to the chunk file
            ChunkRequest *pasteRequest = new ChunkRequest();
            pasteRequest->set_optype(curve::chunkserver::CHUNK_OP_TYPE::CHUNK_OP_PASTE);
            pasteRequest->set_logicpoolid(request->logicpoolid());
            pasteRequest->set_copysetid(request->copysetid());
            pasteRequest->set_chunkid(request->chunkid());
            pasteRequest->set_offset(offset);
            pasteRequest->set_size(cloneDataSize);
            std::shared_ptr<PasteChunkInternalRequest> req = nullptr;

            ChunkResponse *pasteResponse = new ChunkResponse();
            CloneClosure *closure = new CloneClosure();
            closure->SetRequest(pasteRequest);
            closure->SetResponse(pasteResponse);
            closure->SetClosure(done);
            // If it is a request for a recover chunk, the result of the pass needs to
            // be returned through rpc
            if (CHUNK_OP_TYPE::CHUNK_OP_RECOVER == request->optype())
            {
                closure->SetUserResponse(readRequest->response_);
            }

            ChunkServiceClosure *pasteClosure = new (std::nothrow)
                ChunkServiceClosure(nullptr, pasteRequest, pasteResponse, closure);

            req = std::make_shared<PasteChunkInternalRequest>(
                readRequest->node_, pasteRequest, pasteResponse, cloneData,
                pasteClosure);
            req->Process();
        }

        inline void CloneCore::SetResponse(
            std::shared_ptr<ReadChunkRequest> readRequest, CHUNK_OP_STATUS status)
        {
            auto applyIndex = readRequest->node_->GetAppliedIndex();
            readRequest->response_->set_appliedindex(applyIndex);
            readRequest->response_->set_status(status);
        }

    } // namespace chunkserver
} // namespace curve
