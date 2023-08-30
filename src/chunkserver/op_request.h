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

#ifndef SRC_CHUNKSERVER_OP_REQUEST_H_
#define SRC_CHUNKSERVER_OP_REQUEST_H_

#include <google/protobuf/message.h>
#include <butil/iobuf.h>
#include <brpc/controller.h>

#include <memory>

#include "proto/chunk.pb.h"
#include "include/chunkserver/chunkserver_common.h"
#include "src/chunkserver/concurrent_apply/concurrent_apply.h"
#include "src/chunkserver/datastore/define.h"
#include "src/chunkserver/scan_manager.h"

using ::google::protobuf::RpcController;
using ::curve::chunkserver::concurrent::ConcurrentApplyModule;
using ::curve::chunkserver::concurrent::ApplyTaskType;

namespace curve {
namespace chunkserver {

class CopysetNode;
class CSDataStore;
class CloneManager;
class CloneCore;
class CloneTask;
class ScanManager;


inline bool existCloneInfo(const ChunkRequest *request) {
    if (request != nullptr) {
        if (request->has_clonefilesource() &&
            request->has_clonefileoffset()) {
                return true;
        }
    }
    return false;
}

class ChunkOpRequest : public std::enable_shared_from_this<ChunkOpRequest> {
 public:
    ChunkOpRequest();
    ChunkOpRequest(std::shared_ptr<CopysetNode> nodePtr,
                   RpcController *cntl,
                   const ChunkRequest *request,
                   ChunkResponse *response,
                   ::google::protobuf::Closure *done);

    virtual ~ChunkOpRequest() = default;

    /**
     *Processing a request actually involves proposing to the corresponding copyset
     */
    virtual void Process();

    /**
     *Request normally obtains context on apply logic from memory
     * @param index: The index of this op log entry
     * @param done: corresponding ChunkClosure
     */
    virtual void OnApply(uint64_t index,
                         ::google::protobuf::Closure *done) = 0;

    /**
     *NOTE: During subclass implementation, prioritize the use of datastore/request passed in as parameters
     *Obtain detailed request information from the reverse sequence of the log entry for processing, request
     *The relevant context and dependent data store are passed in from parameters
     *1. Restart the replay log, read the op log entry from the disk, and then execute the on apply logic
     *2. Follower execute the logic of on apply
     * @param datastore: chunk data persistence layer
     * @param request: The detailed request information obtained after deserialization
     * @param data: The data to be processed by the request obtained after deserialization
     */
    virtual void OnApplyFromLog(std::shared_ptr<CSDataStore> datastore,
                                const ChunkRequest &request,
                                const butil::IOBuf &data) = 0;

    /**
     *Return the done member of the request
     */
    ::google::protobuf::Closure *Closure() { return done_; }

    /**
     *Return chunk id
     */
    ChunkID ChunkId() { return request_->chunkid(); }

    /**
     *Return request type
     */
    CHUNK_OP_TYPE OpType() { return request_->optype(); }

    /**
     *Return request size
     */
    uint32_t RequestSize() { return request_->size(); }

    /**
     *Forward request to leader
     */
    virtual void RedirectChunkRequest();

 public:
    /**
     *Op Serialization Tool Function
     * |            data                 |
     * |      op meta       |   op data  |
     * | op request length  | op request |
     * |     32 bit         |  ....      |
     *The explanation of each field is as follows:
     *Data: The data after encoding is actually the data of an op log entry
     *op meta: refers to the metadata of op, where is the length of the op request section
     *op data: refers to the data serialized by the request through protobuf
     * @param request: Chunk Request
     * @param data: The data content contained in the request
     * @param log: Provide parameters, store serialized data, and ensure the data by the user= Nullptr
     * @return 0 succeeded, -1 failed
     */
    static int Encode(const ChunkRequest *request,
                      const butil::IOBuf *data,
                      butil::IOBuf *log);

    /**
     *Deserialize, obtain ChunkOpRequest from log entry, and deserialize the current ChunkRequest and data
     *Will be passed out from the output parameter, rather than being placed in the member variable of ChunkOpRequest
     * @param log: op log entry
     * @param request: Provide parameters to store the reverse sequence context
     * @param data: Output parameters, op operation data
     * @return nullptr, failed, otherwise return the corresponding ChunkOpRequest
     */
    static std::shared_ptr<ChunkOpRequest> Decode(butil::IOBuf log,
                                                  ChunkRequest *request,
                                                  butil::IOBuf *data,
                                                  uint64_t index,
                                                  PeerId leaderId);

    static ApplyTaskType Schedule(CHUNK_OP_TYPE opType);

 protected:
    /**
     *Package the request as brand:: task and propose it to the corresponding replication group
     * @param request: Chunk Request
     * @param data: The data content contained in the request
     * @return 0 succeeded, -1 failed
     */
    int Propose(const ChunkRequest *request,
                const butil::IOBuf *data);

 protected:
    //Chunk Persistence Interface
    std::shared_ptr<CSDataStore> datastore_;
    //Copy Group
    std::shared_ptr<CopysetNode> node_;
    // rpc controller
    brpc::Controller *cntl_;
    //Rpc request
    const ChunkRequest *request_;
    //Rpc return
    ChunkResponse *response_;
    // rpc done closure
    ::google::protobuf::Closure *done_;
};

class DeleteChunkRequest : public ChunkOpRequest {
 public:
    DeleteChunkRequest() :
        ChunkOpRequest() {}
    DeleteChunkRequest(std::shared_ptr<CopysetNode> nodePtr,
                       RpcController *cntl,
                       const ChunkRequest *request,
                       ChunkResponse *response,
                       ::google::protobuf::Closure *done) :
        ChunkOpRequest(nodePtr,
                       cntl,
                       request,
                       response,
                       done) {}
    virtual ~DeleteChunkRequest() = default;

    void OnApply(uint64_t index, ::google::protobuf::Closure *done) override;
    void OnApplyFromLog(std::shared_ptr<CSDataStore> datastore,
                        const ChunkRequest &request,
                        const butil::IOBuf &data) override;
};

class ReadChunkRequest : public ChunkOpRequest {
    friend class CloneCore;
    friend class PasteChunkInternalRequest;

 public:
    ReadChunkRequest() :
        ChunkOpRequest() {}
    ReadChunkRequest(std::shared_ptr<CopysetNode> nodePtr,
                     CloneManager* cloneMgr,
                     RpcController *cntl,
                     const ChunkRequest *request,
                     ChunkResponse *response,
                     ::google::protobuf::Closure *done);

    virtual ~ReadChunkRequest() = default;

    void Process() override;
    void OnApply(uint64_t index, ::google::protobuf::Closure *done) override;
    void OnApplyFromLog(std::shared_ptr<CSDataStore> datastore,
                        const ChunkRequest &request,
                        const butil::IOBuf &data) override;

    const ChunkRequest* GetChunkRequest() {
        return request_;
    }

 private:
    //Determine whether to copy data based on chunk information
    bool NeedClone(const CSChunkInfo& chunkInfo);
    //Reading data from chunk file
    void ReadChunk();

 private:
    CloneManager* cloneMgr_;
    //Concurrent module
    ConcurrentApplyModule* concurrentApplyModule_;
    //Save the apply index
    uint64_t applyIndex;
};

class WriteChunkRequest : public ChunkOpRequest {
 public:
    WriteChunkRequest() :
        ChunkOpRequest() {}
    WriteChunkRequest(std::shared_ptr<CopysetNode> nodePtr,
                      RpcController *cntl,
                      const ChunkRequest *request,
                      ChunkResponse *response,
                      ::google::protobuf::Closure *done) :
        ChunkOpRequest(nodePtr,
                       cntl,
                       request,
                       response,
                       done) {}
    virtual ~WriteChunkRequest() = default;

    void OnApply(uint64_t index, ::google::protobuf::Closure *done);
    void OnApplyFromLog(std::shared_ptr<CSDataStore> datastore,
                        const ChunkRequest &request,
                        const butil::IOBuf &data) override;
};

class ReadSnapshotRequest : public ChunkOpRequest {
 public:
    ReadSnapshotRequest() :
        ChunkOpRequest() {}
    ReadSnapshotRequest(std::shared_ptr<CopysetNode> nodePtr,
                        RpcController *cntl,
                        const ChunkRequest *request,
                        ChunkResponse *response,
                        ::google::protobuf::Closure *done) :
        ChunkOpRequest(nodePtr,
                       cntl,
                       request,
                       response,
                       done) {}
    virtual ~ReadSnapshotRequest() = default;

    void OnApply(uint64_t index, ::google::protobuf::Closure *done) override;
    void OnApplyFromLog(std::shared_ptr<CSDataStore> datastore,
                        const ChunkRequest &request,
                        const butil::IOBuf &data) override;
};

class DeleteSnapshotRequest : public ChunkOpRequest {
 public:
    DeleteSnapshotRequest() :
        ChunkOpRequest() {}
    DeleteSnapshotRequest(std::shared_ptr<CopysetNode> nodePtr,
                          RpcController *cntl,
                          const ChunkRequest *request,
                          ChunkResponse *response,
                          ::google::protobuf::Closure *done) :
        ChunkOpRequest(nodePtr,
                       cntl,
                       request,
                       response,
                       done) {}
    virtual ~DeleteSnapshotRequest() = default;

    void OnApply(uint64_t index, ::google::protobuf::Closure *done) override;
    void OnApplyFromLog(std::shared_ptr<CSDataStore> datastore,
                        const ChunkRequest &request,
                        const butil::IOBuf &data) override;
};

class CreateCloneChunkRequest : public ChunkOpRequest {
 public:
    CreateCloneChunkRequest() :
        ChunkOpRequest() {}
    CreateCloneChunkRequest(std::shared_ptr<CopysetNode> nodePtr,
                            RpcController *cntl,
                            const ChunkRequest *request,
                            ChunkResponse *response,
                            ::google::protobuf::Closure *done) :
        ChunkOpRequest(nodePtr,
                       cntl,
                       request,
                       response,
                       done) {}
    virtual ~CreateCloneChunkRequest() = default;

    void OnApply(uint64_t index, ::google::protobuf::Closure *done) override;
    void OnApplyFromLog(std::shared_ptr<CSDataStore> datastore,
                        const ChunkRequest &request,
                        const butil::IOBuf &data) override;
};

class PasteChunkInternalRequest : public ChunkOpRequest {
 public:
    PasteChunkInternalRequest() :
        ChunkOpRequest() {}
    PasteChunkInternalRequest(std::shared_ptr<CopysetNode> nodePtr,
                              const ChunkRequest *request,
                              ChunkResponse *response,
                              const butil::IOBuf* data,
                              ::google::protobuf::Closure *done) :
        ChunkOpRequest(nodePtr,
                       nullptr,
                       request,
                       response,
                       done) {
            if (data != nullptr) {
                data_ = *data;
            }
        }
    virtual ~PasteChunkInternalRequest() = default;

    void Process() override;
    void OnApply(uint64_t index, ::google::protobuf::Closure *done) override;
    void OnApplyFromLog(std::shared_ptr<CSDataStore> datastore,
                        const ChunkRequest &request,
                        const butil::IOBuf &data) override;

 private:
    butil::IOBuf data_;
};

class ScanChunkRequest : public ChunkOpRequest {
 public:
    ScanChunkRequest(uint64_t index, PeerId peer) :
       ChunkOpRequest(), index_(index), peer_(peer) {}
    ScanChunkRequest(std::shared_ptr<CopysetNode> nodePtr,
                     ScanManager* scanManager,
                     const ChunkRequest *request,
                     ChunkResponse *response,
                     ::google::protobuf::Closure *done) :
        ChunkOpRequest(nodePtr,
                      nullptr,
                      request,
                      response,
                      done),
                      scanManager_(scanManager) {}
    virtual ~ScanChunkRequest() = default;

    void OnApply(uint64_t index, ::google::protobuf::Closure *done) override;
    void OnApplyFromLog(std::shared_ptr<CSDataStore> datastore,
                        const ChunkRequest &request,
                        const butil::IOBuf &data) override;

 private:
    void BuildAndSendScanMap(const ChunkRequest &request, uint64_t index,
                             uint32_t crc);
    ScanManager* scanManager_;
    uint64_t index_;
    PeerId peer_;
};

}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_OP_REQUEST_H_
