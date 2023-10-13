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

#ifndef SRC_CLIENT_REQUEST_SENDER_H_
#define SRC_CLIENT_REQUEST_SENDER_H_

#include <brpc/channel.h>
#include <butil/endpoint.h>
#include <butil/iobuf.h>

#include <string>

#include "include/curve_compiler_specific.h"
#include "src/client/chunk_closure.h"
#include "src/client/client_common.h"
#include "src/client/client_config.h"
#include "src/client/request_context.h"

namespace curve {
namespace client {

/**
 * A RequestSender is responsible for managing all aspects of a ChunkServer
 * Connection, currently there is only one connection for a ChunkServer
 */
class RequestSender {
 public:
    RequestSender(ChunkServerID chunkServerId, butil::EndPoint serverEndPoint)
        : chunkServerId_(chunkServerId),
          serverEndPoint_(serverEndPoint),
          channel_() {}
    virtual ~RequestSender() {}

    int Init(const IOSenderOption& ioSenderOpt);

    /**
     * Reading Chunk
     * @param IDInfo is the ID information related to chunk
     * @param sn: File version number
     * @param offset: Read offset
     * @param length: Read length
     * @param sourceInfo Data source information
     * @param done: closure of asynchronous callback on the previous layer
     */
    int ReadChunk(const ChunkIDInfo& idinfo, uint64_t sn, off_t offset,
                  size_t length, const RequestSourceInfo& sourceInfo,
                  ClientClosure* done);

    /**
     * Write Chunk
     * @param IDInfo is the ID information related to chunk
     * @param fileId: file id
     * @param epoch: file epoch
     * @param sn: File version number
     * @param data The data to be written
     * @param offset: write offset
     * @param length: The length written
     * @param sourceInfo Data source information
     * @param done: closure of asynchronous callback on the previous layer
     */
    int WriteChunk(const ChunkIDInfo& idinfo, uint64_t fileId, uint64_t epoch,
                   uint64_t sn, const butil::IOBuf& data, off_t offset,
                   size_t length, const RequestSourceInfo& sourceInfo,
                   ClientClosure* done);

    /**
     * Reading Chunk snapshot files
     * @param IDInfo is the ID information related to chunk
     * @param sn: File version number
     * @param offset: Read offset
     * @param length: Read length
     * @param done: closure of asynchronous callback on the previous layer
     */
    int ReadChunkSnapshot(const ChunkIDInfo& idinfo, uint64_t sn, off_t offset,
                          size_t length, ClientClosure* done);

    /**
     * Delete snapshots generated during this dump or left over from history
     * If no snapshot is generated during the dump process, modify the
     * correctedSn of the chunk
     * @param IDInfo is the ID information related to chunk
     * @param correctedSn: Chunk The version number that needs to be corrected
     * @param done: closure of asynchronous callback on the previous layer
     */
    int DeleteChunkSnapshotOrCorrectSn(const ChunkIDInfo& idinfo,
                                       uint64_t correctedSn,
                                       ClientClosure* done);

    /**
     * Obtain information about chunk files
     * @param IDInfo is the ID information related to chunk
     * @param done: closure of asynchronous callback on the previous layer
     * @param retriedTimes: Number of retries
     */
    int GetChunkInfo(const ChunkIDInfo& idinfo, ClientClosure* done);

    /**
     * @brief lazy Create clone chunk
     * @detail
     * - The format definition of a location is A@B The form of.
     * - If the source data is on s3, the location format is uri@s3 Uri is the
     * address of the actual chunk object;
     * - If the source data is on curves, the location format
     * is/filename/chunkindex@cs
     *
     * @param IDInfo is the ID information related to chunk
     * @param done: closure of asynchronous callback on the previous layer
     * @param: location, URL of the data source
     * @param: sn chunk's serial number
     * @param: correntSn used to modify the chunk when creating CloneChunk
     * @param: chunkSize Chunk size
     * @param retriedTimes: Number of retries
     *
     * @return error code
     */
    int CreateCloneChunk(const ChunkIDInfo& idinfo, ClientClosure* done,
                         const std::string& location, uint64_t sn,
                         uint64_t correntSn, uint64_t chunkSize);

    /**
     * @brief Actual recovery chunk data
     * @param IDInfo is the ID information related to chunk
     * @param done: closure of asynchronous callback on the previous layer
     * @param: offset: offset
     * @param: len: length
     * @param retriedTimes: Number of retries
     *
     * @return error code
     */
    int RecoverChunk(const ChunkIDInfo& idinfo, ClientClosure* done,
                     uint64_t offset, uint64_t len);
    /**
     * Reset Link to Chunk Server
     * @param chunkServerId: Chunk Server unique identifier
     * @param serverEndPoint: Chunk Server
     * @return 0 succeeded, -1 failed
     */
    int ResetSender(ChunkServerID chunkServerId,
                    butil::EndPoint serverEndPoint);

    bool IsSocketHealth() { return channel_.CheckHealth() == 0; }

 private:
    void UpdateRpcRPS(ClientClosure* done, OpType type) const;

    void SetRpcStuff(ClientClosure* done, brpc::Controller* cntl,
                     google::protobuf::Message* rpcResponse) const;

 private:
    // Rpc stub configuration
    IOSenderOption iosenderopt_;
    // The unique identification ID of ChunkServer
    ChunkServerID chunkServerId_;
    // Address of ChunkServer
    butil::EndPoint serverEndPoint_;
    brpc::Channel channel_; /* TODO(wudemiao): Multiple channels will be
                               maintained in the later stage */
};

}  // namespace client
}  // namespace curve

#endif  // SRC_CLIENT_REQUEST_SENDER_H_
