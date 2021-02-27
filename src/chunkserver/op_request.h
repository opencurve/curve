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

using ::google::protobuf::RpcController;
using ::curve::chunkserver::concurrent::ConcurrentApplyModule;

namespace curve {
namespace chunkserver {

class CopysetNode;
class CSDataStore;
class CloneManager;
class CloneCore;
class CloneTask;


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
     * Process the request and actually propose it to the corresponding copyset
     */
    virtual void Process();

    /**
     * request normally fetches the on apply logic from memory
     * @param index:op log entry's index
     * @param done:corresponding ChunkClosure
     */
    virtual void OnApply(uint64_t index,
                         ::google::protobuf::Closure *done) = 0;

    /**
     * NOTE: The subclass implementation prefers to use the datastore/request passed in as a
     * parameter, getting the request details from the log entry after deserialisation.
     * The request-related context and the dependent data store are passed in from the parameters.
     * 1. Restart the replay log, read the op log entry from disk and then execute the on apply logic
     * 2. follower performs the logic of on apply
     * @param datastore:chunk data persistence layer
     * @param request:The request details after deserialization
     * @param data:The data to be processed by the request after deserialization
     */
    virtual void OnApplyFromLog(std::shared_ptr<CSDataStore> datastore,
                                const ChunkRequest &request,
                                const butil::IOBuf &data) = 0;

    /**
     * Return the request's done member
     */
    ::google::protobuf::Closure *Closure() { return done_; }

    /**
     * Return chunk id
     */
    ChunkID ChunkId() { return request_->chunkid(); }

    /**
     * Return request type
     */
    CHUNK_OP_TYPE OpType() { return request_->optype(); }

    /**
     * Return the request size
     */
    uint32_t RequestSize() { return request_->size(); }

    /**
     * Redirect request to leader
     */
    virtual void RedirectChunkRequest();

 public:
    /**
     * Op Serialisation tool function
     * |            data                 |
     * |      op meta       |   op data  |
     * | op request length  | op request |
     * |     32 bit         |  ....      |
     * The fields are explained below：
     * data: The data after encoding, it is actually the data of an op log entry
     * op meta: The metadata of the op, here the length of the op request section
     * op data: Data after request serialization via protobuf
     * @param request:Chunk Request
     * @param data:Content of the data contained in the request
     * @param log:Store the serialized data, the user himself ensures that data!=nullptr
     * @return 0 for success, -1 for failure
     */
    static int Encode(const ChunkRequest *request,
                      const butil::IOBuf *data,
                      butil::IOBuf *log);

    /**
     * Deserialize, get ChunkOpRequest from log entry, the current deserialized ChunkRequest
     * and data will be passed out, not in the ChunkOpRequest member variable
     * @param log:op log entry
     * @param request: out parameter，Store the deserial context
     * @param data:out parameter，data for op operations
     * @return nullptr,fail, otherwise return the corresponding ChunkOpRequest
     */
    static std::shared_ptr<ChunkOpRequest> Decode(butil::IOBuf log,
                                                  ChunkRequest *request,
                                                  butil::IOBuf *data);

 protected:
    /**
     * Package the request as braft::task and propose it to the corresponding copyset
     * @param request:Chunk Request
     * @param data:Content of the data contained in the request
     * @return 0 for success,-1 for failure
     */
    int Propose(const ChunkRequest *request,
                const butil::IOBuf *data);

 protected:
    // chunk persistence interface
    std::shared_ptr<CSDataStore> datastore_;
    // copyset
    std::shared_ptr<CopysetNode> node_;
    // rpc controller
    brpc::Controller *cntl_;
    // rpc request
    const ChunkRequest *request_;
    // rpc response
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
    // Check if data needs to be copied based on chunk information
    bool NeedClone(const CSChunkInfo& chunkInfo);
    // read file from chunk
    void ReadChunk();

 private:
    CloneManager* cloneMgr_;
    // concurrent module
    ConcurrentApplyModule* concurrentApplyModule_;
    // save apply index
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

}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_OP_REQUEST_H_
