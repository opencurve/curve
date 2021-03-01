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
 * Created Date: Monday March 11th 2019
 * Author: yangyaokai
 */

#ifndef SRC_CHUNKSERVER_CLONE_CORE_H_
#define SRC_CHUNKSERVER_CLONE_CORE_H_

#include <glog/logging.h>
#include <google/protobuf/message.h>
#include <google/protobuf/stubs/callback.h>
#include <brpc/controller.h>
#include <memory>

#include "proto/chunk.pb.h"
#include "include/chunkserver/chunkserver_common.h"
#include "src/common/timeutility.h"
#include "src/chunkserver/clone_copyer.h"
#include "src/chunkserver/datastore/define.h"

namespace curve {
namespace chunkserver {

using ::google::protobuf::Closure;
using ::google::protobuf::Message;
using curve::chunkserver::CSChunkInfo;
using common::TimeUtility;

class ReadChunkRequest;
class PasteChunkInternalRequest;
class CloneCore;

class DownloadClosure : public Closure {
 public:
    DownloadClosure(std::shared_ptr<ReadChunkRequest> readRequest,
                    std::shared_ptr<CloneCore> cloneCore,
                    AsyncDownloadContext* downloadCtx,
                    Closure *done);

    void Run();

    void SetFailed() {
        isFailed_ = true;
    }

    AsyncDownloadContext* GetDownloadContext() {
        return downloadCtx_;
    }

 protected:
    // Whether download is failed
    bool isFailed_;
    // Begin time of request
    uint64_t beginTime_;
    // Contexts of download request
    AsyncDownloadContext* downloadCtx_;
    // clone core object
    std::shared_ptr<CloneCore> cloneCore_;
    // read chunk request object
    std::shared_ptr<ReadChunkRequest> readRequest_;
    // Callbacks to be executed after the DownloadClosure life cycle
    Closure* done_;
};

class CloneClosure : public Closure {
 public:
    CloneClosure() : request_(nullptr)
                   , response_(nullptr)
                   , userResponse_(nullptr)
                   , done_(nullptr) {}

    void Run();
    void SetClosure(Closure *done) {
        done_ = done;
    }
    void SetRequest(Message* request) {
        request_ = dynamic_cast<ChunkRequest *>(request);
    }
    void SetResponse(Message* response) {
        response_ = dynamic_cast<ChunkResponse *>(response);
    }
    void SetUserResponse(Message* response) {
        userResponse_ = dynamic_cast<ChunkResponse *>(response);
    }

 private:
    // Paste chunk request structure
    ChunkRequest        *request_;
    // Paste chunk response structure
    ChunkResponse       *response_;
    // The actual response structure to be returned to the user
    ChunkResponse       *userResponse_;
    // Callbacks to be executed after the CloneClosure life cycle
    Closure             *done_;
};

class CloneCore : public std::enable_shared_from_this<CloneCore> {
    friend class DownloadClosure;
 public:
    CloneCore(uint32_t sliceSize, bool enablePaste,
              std::shared_ptr<OriginCopyer> copyer)
        : sliceSize_(sliceSize)
        , enablePaste_(enablePaste)
        , copyer_(copyer) {}
    virtual ~CloneCore() {}

    /**
     * Logic for handling read requests
     * @param readRequest[in]:Read request information
     * @param done[in]:Closures to be executed after the task is completed
     * @return: Return 0 for success, -1 for failure
     */
    int HandleReadRequest(std::shared_ptr<ReadChunkRequest> readRequest,
                          Closure* done);

 protected:
    /**
     * If the local chunk file exists, the data is read according to the
     * clone and bitmap information recorded locally, which involves reading the
     * remote file and merging the local file to return the result.
     * @param[in/out] readRequest: User request & response contexts
     * @param[in] chunkInfo: Local chunkinfo
     * @return Return 0 for success, a negative number for failure
     */
    int CloneReadByLocalInfo(std::shared_ptr<ReadChunkRequest> readRequest,
        const CSChunkInfo &chunkInfo, Closure* done);

    /**
     * If the local chunk file does not exist, the data is read according to
     * the clonesource information carried in the user request context
     * No merge local results involved
     * @param[in/out] readRequest: User request & response contexts
     */
    void CloneReadByRequestInfo(std::shared_ptr<ReadChunkRequest> readRequest,
        Closure* done);

    /**
     * Read the requested area from the local chunk and set the response
     * @param readRequest: User's ReadRequest
     * @return: Return 0 for success, -1 for failure
     */
    int ReadChunk(std::shared_ptr<ReadChunkRequest> readRequest);

    /**
     * Set the response for the read chunk type, including the data returned
     * and other return parameters
     * Read written areas from local chunk, unwritten areas from cloned data
     * then merge data in memory.
     * @param readRequest: User's ReadRequest
     * @param cloneData: Data copied from the source, starting at the same
     * offset as in the request
     * @return: Return 0 for success, -1 for failure
     */
    int SetReadChunkResponse(std::shared_ptr<ReadChunkRequest> readRequest,
                             const butil::IOBuf* cloneData);

    // Read written areas from the local chunk and merge them into the clone
    // data
    int ReadThenMerge(std::shared_ptr<ReadChunkRequest> readRequest,
                      const CSChunkInfo& chunkInfo,
                      const butil::IOBuf* cloneData,
                      char* chunkData);

    /**
     * Paste the data downloaded from the source into a local chunk file
     * @param readRequest: User's ReadRequest
     * @param cloneData: Downloaded data from source
     * @param offset: Offset of the downloaded data in the chunk file
     * @param cloneDataSize: Length of the downloaded file
     * @param done:Closures to be executed after the task is completed
     */
    void PasteCloneData(std::shared_ptr<ReadChunkRequest> readRequest,
                        const butil::IOBuf* cloneData,
                        off_t offset,
                        size_t cloneDataSize,
                        Closure* done);

    inline void SetResponse(std::shared_ptr<ReadChunkRequest> readRequest,
                            CHUNK_OP_STATUS status);

 private:
    // Size of slice per copy
    uint32_t sliceSize_;
    // Check if a read chunk request requires paste, true means yes, false
    // means no
    bool enablePaste_;
    // Responsible for downloading data from the source
    std::shared_ptr<OriginCopyer> copyer_;
};

}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_CLONE_CORE_H_
