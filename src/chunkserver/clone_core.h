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
    //Is there an error in downloading
    bool isFailed_;
    //Request start time
    uint64_t beginTime_;
    //Download request context information
    AsyncDownloadContext* downloadCtx_;
    //Clone core object
    std::shared_ptr<CloneCore> cloneCore_;
    //Read chunk request object
    std::shared_ptr<ReadChunkRequest> readRequest_;
    //Callbacks to be executed after the end of the DownloadClosure lifecycle
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
    //Request structure for paste chunk
    ChunkRequest        *request_;
    //Response structure of paste chunk
    ChunkResponse       *response_;
    //The response structure that truly needs to be returned to the user
    ChunkResponse       *userResponse_;
    //Callbacks to be executed after the end of the CloneClosure lifecycle
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
     *Logic for processing read requests
     * @param readRequest[in]: Read request information
     * @param done[in]: The closure to be executed after the task is completed
     * @return: Success returns 0, failure returns -1
     */
    int HandleReadRequest(std::shared_ptr<ReadChunkRequest> readRequest,
                          Closure* done);

 protected:
    /**
     *When a local chunk file exists, read data based on the locally recorded clone and bitmap information
     *Will involve reading remote files and merging with local files to return results
     * @param[in/out] readRequest: User Request&Response Context
     * @param[in] chunkInfo: corresponds to the local chunkinfo
     * @return Success returns 0, failure returns a negative number
     */
    int CloneReadByLocalInfo(std::shared_ptr<ReadChunkRequest> readRequest,
        const CSChunkInfo &chunkInfo, Closure* done);

    /**
     *When the local chunk file does not exist, read the data according to the clonesource information in the user request context
     *Not involving merge local results
     * @param[in/out] readRequest: User Request&Response Context
     */
    void CloneReadByRequestInfo(std::shared_ptr<ReadChunkRequest> readRequest,
        Closure* done);

    /**
     *Read the requested area from the local chunk and set the response
     * @param readRequest: User's ReadRequest
     * @return: Success returns 0, failure returns -1
     */
    int ReadChunk(std::shared_ptr<ReadChunkRequest> readRequest);

    /**
     *Set the response of the read chunk type, including the returned data and other return parameters
     *Read the written area from the local chunk, and obtain the unwritten area from the cloned data
     *Then merge the data into memory
     * @param readRequest: User's ReadRequest
     * @param cloneData: The data copied from the source has the same starting offset as the offset in the request
     * @return: Success returns 0, failure returns -1
     */
    int SetReadChunkResponse(std::shared_ptr<ReadChunkRequest> readRequest,
                             const butil::IOBuf* cloneData);

    //Read the previously written regions from the local chunk and merge them into clone data
    int ReadThenMerge(std::shared_ptr<ReadChunkRequest> readRequest,
                      const CSChunkInfo& chunkInfo,
                      const butil::IOBuf* cloneData,
                      char* chunkData);

    /**
     *Paste the downloaded data from the source into the local chunk file
     * @param readRequest: User's ReadRequest
     * @param cloneData: Data downloaded from the source
     * @param offset: The offset of the downloaded data in the chunk file
     * @param cloneDataSize: Download data length
     * @param done: The closure to be executed after the task is completed
     */
    void PasteCloneData(std::shared_ptr<ReadChunkRequest> readRequest,
                        const butil::IOBuf* cloneData,
                        off_t offset,
                        size_t cloneDataSize,
                        Closure* done);

    inline void SetResponse(std::shared_ptr<ReadChunkRequest> readRequest,
                            CHUNK_OP_STATUS status);

 private:
    //The size of each copied slice
    uint32_t sliceSize_;
    //Determine whether a read chunk type request requires paste. True requires paste, while false indicates no need
    bool enablePaste_;
    //Responsible for downloading data from the source
    std::shared_ptr<OriginCopyer> copyer_;
};

}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_CLONE_CORE_H_
