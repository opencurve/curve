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
 * Created Date: 18-9-25
 * Author: wudemiao
 */

#ifndef SRC_CLIENT_COPYSET_CLIENT_H_
#define SRC_CLIENT_COPYSET_CLIENT_H_

#include <butil/iobuf.h>
#include <google/protobuf/stubs/callback.h>

#include <memory>
#include <string>

#include "include/curve_compiler_specific.h"
#include "src/client/client_common.h"
#include "src/client/client_metric.h"
#include "src/client/config_info.h"
#include "src/client/request_context.h"
#include "src/client/request_sender_manager.h"
#include "src/common/concurrent/concurrent.h"

namespace curve {
namespace client {

using curve::common::Uncopyable;
using ::google::protobuf::Closure;

// TODO(tongguangxun): In addition to the read and write interfaces, the retry
// logic needs to be adjusted in the future
class MetaCache;
class RequestScheduler;
/**
 * Responsible for managing connections to ChunkServers and providing
 * upper-layer access to read/write interfaces for specific chunks within a
 * copyset.
 */
class CopysetClient {
 public:
    CopysetClient()
        : metaCache_(nullptr),
          senderManager_(nullptr),
          sessionNotValid_(false),
          scheduler_(nullptr),
          fileMetric_(nullptr),
          exitFlag_(false) {}

    CopysetClient(const CopysetClient&) = delete;
    CopysetClient& operator=(const CopysetClient&) = delete;

    virtual ~CopysetClient() {
        delete senderManager_;
        senderManager_ = nullptr;
    }

    int Init(MetaCache* metaCache, const IOSenderOption& ioSenderOpt,
             RequestScheduler* scheduler = nullptr,
             FileMetric* fileMetic = nullptr);
    /**
     * Return dependent Meta Cache
     */
    MetaCache* GetMetaCache() { return metaCache_; }

    /**
     * Reading Chunk
     * @param idinfo is the ID information related to chunk
     * @param sn: File version number
     * @param offset: Read offset
     * @param length: Read length
     * @param sourceInfo chunk Clone source information
     * @param done: closure of asynchronous callback on the previous layer
     */
    int ReadChunk(const ChunkIDInfo& idinfo, uint64_t sn, off_t offset,
                  size_t length, const RequestSourceInfo& sourceInfo,
                  google::protobuf::Closure* done);

    /**
     * Write Chunk
     * @param idinfo is the ID information related to chunk
     * @param fileId: file id
     * @param epoch: file epoch
     * @param sn: File version number
     * @param writeData: The data to be written
     *@param offset: write offset
     * @param length: The length written
     * @param sourceInfo: chunk Clone source information
     * @param done: closure of asynchronous callback on the previous layer
     */
    int WriteChunk(const ChunkIDInfo& idinfo, uint64_t fileId, uint64_t epoch,
                   uint64_t sn, const butil::IOBuf& writeData, off_t offset,
                   size_t length, const RequestSourceInfo& sourceInfo,
                   Closure* done);

    /**
     *Reading Chunk snapshot files
     * @param idinfo: the ID information related to chunk
     * @param sn: File version number
     * @param offset: Read offset
     * @param length: Read length
     * @param done: closure of asynchronous callback on the previous layer
     */
    int ReadChunkSnapshot(const ChunkIDInfo& idinfo, uint64_t sn, off_t offset,
                          size_t length, Closure* done);

    /**
     * Delete snapshots generated during this dump or left over from history
     * If no snapshot is generated during the dump process, modify the
     * correctedSn of the chunk
     * @param idinfo is the ID information related to chunk
     * @param correctedSn: Version number that needs to be corrected
     * @param done: closure of asynchronous callback on the previous layer
     */
    int DeleteChunkSnapshotOrCorrectSn(const ChunkIDInfo& idinfo,
                                       uint64_t correctedSn, Closure* done);

    /**
     * Obtain information about chunk files
     * @param idinfo: the ID information related to chunk
     * @param done: closure of asynchronous callback on the previous layer
     */
    int GetChunkInfo(const ChunkIDInfo& idinfo, Closure* done);

    /**
     * @brief lazy Create clone chunk
     * @param idinfo: the ID information related to chunk
     * @param location: URL of the data source
     * @param sn: chunk's serial number
     * @param correntSn: used to modify the chunk when creating CloneChunk
     * @param chunkSize: Chunk size
     * @param done: closure of asynchronous callback on the previous layer
     * @return error code
     */
    int CreateCloneChunk(const ChunkIDInfo& idinfo, const std::string& location,
                         uint64_t sn, uint64_t correntSn, uint64_t chunkSize,
                         Closure* done);

    /**
     * @brief Actual recovery chunk data
     * @param idinfo is the ID information related to chunk
     * @param offset: offset
     * @param len: length
     * @param done: closure of asynchronous callback on the previous layer
     * @return error code
     */
    int RecoverChunk(const ChunkIDInfo& idinfo, uint64_t offset, uint64_t len,
                     Closure* done);

    /**
     * @brief If the RequestSender corresponding to csId is not healthy, reset
     * it
     * @param csId chunkserver id
     */
    void ResetSenderIfNotHealth(const ChunkServerID& csId) {
        senderManager_->ResetSenderIfNotHealth(csId);
    }

    /**
     * session expired, retry RPC needs to be stopped
     */
    void StartRecycleRetryRPC() { sessionNotValid_ = true; }

    /**
     * session recovery notification no longer recycles retried RPCs
     */
    void ResumeRPCRetry() { sessionNotValid_ = false; }

    /**
     * Receive upper-layer closure notification when the file is closed.
     * Set the exitFlag based on the session's validity status. If there are RPC
     * timeouts under an invalid session state, these RPCs will return errors
     * directly. If the session is valid, RPCs will continue to be issued until
     * the retry limit is reached or they return successfully.
     */
    void ResetExitFlag() {
        if (sessionNotValid_) {
            exitFlag_ = true;
        }
    }

 private:
    friend class WriteChunkClosure;
    friend class ReadChunkClosure;

    // Pull new leader information
    bool FetchLeader(LogicPoolID lpid, CopysetID cpid, ChunkServerID* leaderid,
                     butil::EndPoint* leaderaddr);

    /**
     * Execute the send rpc task and retry with an error
     * @param[in]: idinfo is the ID information of the current rpc task
     * @param[in]: task is the rpc task executed this time
     * @param[in]: done is the asynchronous callback for this RPC task
     * @return: Successfully returns 0, otherwise -1
     */
    int DoRPCTask(
        const ChunkIDInfo& idinfo,
        std::function<void(Closure*, std::shared_ptr<RequestSender>)> task,
        Closure* done);

 private:
    // Metadata cache
    MetaCache* metaCache_;
    // Link managers for all ChunkServers
    RequestSenderManager* senderManager_;
    // Configuration
    IOSenderOption iosenderopt_;

    // Check if the session is valid. If the session is invalid, it's necessary
    // to pause the retry RPCs by re-pushing this RPC into the request scheduler
    // queue. This ensures that it doesn't block the internal threads of BRPC
    // and prevents operations on one file from affecting other files.
    bool sessionNotValid_;

    // request scheduler to push RPC back to the scheduling queue when the
    // session expires
    RequestScheduler* scheduler_;

    // The file metric corresponding to the current copyset client
    FileMetric* fileMetric_;

    // Is it in a stopped state? If it is during the shutdown process and the
    // session fails, it is necessary to directly return rpc without issuing it
    bool exitFlag_;
};

}  // namespace client
}  // namespace curve

#endif  // SRC_CLIENT_COPYSET_CLIENT_H_
