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
 * Created Date: 2020-03-10
 * Author: qinyi
 */

#ifndef TEST_INTEGRATION_COMMON_CHUNKSERVICE_OP_H_
#define TEST_INTEGRATION_COMMON_CHUNKSERVICE_OP_H_

#include <brpc/channel.h>
#include <string>
#include <set>
#include <memory>
#include "include/chunkserver/chunkserver_common.h"
#include "proto/common.pb.h"

namespace curve {
namespace chunkserver {

using curve::common::Peer;
using std::shared_ptr;
using std::string;

#define NULL_SN -1

struct ChunkServiceOpConf {
    Peer *leaderPeer;
    LogicPoolID logicPoolId;
    CopysetID copysetId;
    uint32_t rpcTimeout;
};

class ChunkServiceOp {
 public:
    /**
     * @brief Write a chunk through chunkService
     * @param opConf Common configuration parameters such as, leaderPeer/copyset, etc
     * @param chunkId
     * @param sn chunk version
     * @param offset
     * @param len
     * @param data to be written
     * @param cloneFileSource The file path of the clone source
     * @param cloneFileOffset Relative offset of clone chunk in clone source
     * @return  If the request fails to execute, -1 will be returned, otherwise an error code will be returned
     */
    static int WriteChunk(struct ChunkServiceOpConf *opConf, ChunkID chunkId,
                          SequenceNum sn, off_t offset, size_t len,
                          const char *data,
                          const std::string& cloneFileSource = "",
                          off_t cloneFileOffset = 0);

    /**
     * @brief Read chunk through chunkService
     * @param opConf Common configuration parameters such as, leaderPeer/copyset, etc
     * @param chunkId
     * @param sn chunk version
     * @param offset
     * @param len
     * @param data reading content
     * @param cloneFileSource The file path of the clone source
     * @param cloneFileOffset Relative offset of clone chunk in clone source
     * @return If the request fails to execute, -1 will be returned, otherwise an error code will be returned
     */
    static int ReadChunk(struct ChunkServiceOpConf *opConf, ChunkID chunkId,
                         SequenceNum sn, off_t offset, size_t len,
                         string *data,
                         const std::string& cloneFileSource = "",
                         off_t cloneFileOffset = 0);

    /**
     * @brief Read chunk snapshot through chunkService
     * @param opConf Common configuration parameters such as, leaderPeer/copyset, etc
     * @param chunkId
     * @param sn chunk version
     * @param offset
     * @param len
     * @param data reading content
     * @return If the request fails to execute, -1 will be returned, otherwise an error code will be returned
     */
    static int ReadChunkSnapshot(struct ChunkServiceOpConf *opConf,
                                 ChunkID chunkId, SequenceNum sn, off_t offset,
                                 size_t len, std::string *data);

    /**
     * @brief Delete chunk through chunkService
     * @param opConf Common configuration parameters such as, leaderPeer/copyset, etc
     * @param chunkId
     * @param sn chunk version
     * @return  If the request fails to execute, -1 will be returned, otherwise an error code will be returned
     */
    static int DeleteChunk(struct ChunkServiceOpConf *opConf, ChunkID chunkId,
                           SequenceNum sn);

    /**
     * @brief: Delete the snapshot generated during this dump or historical legacy through chunkService
     *         If no snapshot is generated during the dump process, modify the correctedSn of the chunk
     * @param opConf Common configuration parameters such as, leaderPeer/copyset, etc
     * @param chunkId
     * @param correctedSn
     * @return  If the request fails to execute, -1 will be returned, otherwise an error code will be returned
     */
    static int DeleteChunkSnapshotOrCorrectSn(struct ChunkServiceOpConf *opConf,
                                              ChunkID chunkId,
                                              SequenceNum correctedSn);

    /**
     * @brief Create a clone chunk through chunkService
     * @param opConf Common configuration parameters such as, leaderPeer/copyset, etc
     * @param chunkId
     * @param location The location of the source chunk on the source side, possibly on curve or S3
     * @param correctedSn
     * @param sn
     * @param chunkSize
     * @return If the request fails to execute, -1 will be returned, otherwise an error code will be returned
     */
    static int CreateCloneChunk(struct ChunkServiceOpConf *opConf,
                                ChunkID chunkId, const std::string &location,
                                uint64_t correctedSn, uint64_t sn,
                                uint64_t chunkSize);

    /**
     * @brief Restore Chunk through ChunkService
     * @param opConf Common configuration parameters such as, leaderPeer/copyset, etc
     * @param chunkId
     * @param offset
     * @param len
     * @return If the request fails to execute, -1 will be returned, otherwise an error code will be returned
     */
    static int RecoverChunk(struct ChunkServiceOpConf *opConf, ChunkID chunkId,
                            off_t offset, size_t len);

    /**
     * @brief: Obtain chunk metadata through chunkService
     * @param opConf Common configuration parameters such as, leaderPeer/copyset, etc
     * @param chunkId
     * @param curSn returns the current chunk version
     * @param snapSn returns the snapshot chunk version
     * @param redirectedLeader returns the redirected master node
     * @return If the request fails to execute, -1 will be returned, otherwise an error code will be returned
     */
    static int GetChunkInfo(struct ChunkServiceOpConf *opConf, ChunkID chunkId,
                            SequenceNum *curSn, SequenceNum *snapSn,
                            string *redirectedLeader);
};

class ChunkServiceVerify {
 public:
    explicit ChunkServiceVerify(struct ChunkServiceOpConf *opConf)
        : opConf_(opConf) {}

    /**
     * @brief executes the write chunk and writes the data to the corresponding area of chunkdata for subsequent data validation.
     * @param chunkId
     * @param sn chunk version
     * @param offset
     * @param len
     * @param data to be written
     * @param chunkData Expected data for the entire chunk
     * @param cloneFileSource The file path of the clone source
     * @param cloneFileOffset Relative offset of clone chunk in clone source
     * @return returns the error code for the write operation
     */
    int VerifyWriteChunk(ChunkID chunkId, SequenceNum sn, off_t offset,
                         size_t len, const char *data, string *chunkData,
                         const std::string& cloneFileSource = "",
                         off_t cloneFileOffset = 0);

    /**
     * @brief executes the read chunk and verifies whether the read content matches the expected data in the corresponding region of the chunkdata.
     * @param chunkId
     * @param sn chunk version
     * @param offset
     * @param len
     * @param chunkData Expected data for the entire chunk
     * @param cloneFileSource The file path of the clone source
     * @param cloneFileOffset Relative offset of clone chunk in clone source
     * @return  The read request result meets the expected return of 0, otherwise it returns -1
     */
    int VerifyReadChunk(ChunkID chunkId, SequenceNum sn, off_t offset,
                        size_t len, string *chunkData,
                        const std::string& cloneFileSource = "",
                        off_t cloneFileOffset = 0);

    /**
     * @brief Execute read chunk snapshot,
     *And verify whether the read content matches the expected data in the corresponding area of chunkdata.
     * @param chunkId
     * @param sn chunk version
     * @param offset
     * @param len
     * @param chunkData Expected data for the entire chunk
     * @return  The read request result meets the expected return of 0, otherwise it returns -1
     */
    int VerifyReadChunkSnapshot(ChunkID chunkId, SequenceNum sn, off_t offset,
                                size_t len, string *chunkData);

    /**
     * @brief delete chunk
     * @param chunkId
     * @param sn chunk version
     * @return returns the error code for the delete operation
     */
    int VerifyDeleteChunk(ChunkID chunkId, SequenceNum sn);

    /**
     * @brief Delete the snapshot of the chunk
     * @param chunkId
     * @param correctedSn
     * @return returns the error code for the delete operation
     */
    int VerifyDeleteChunkSnapshotOrCorrectSn(ChunkID chunkId,
                                             SequenceNum correctedSn);

    /**
     * @brief Create clone chunk
     * @param chunkId
     * @param location source address
     * @param correctedSn
     * @param sn
     * @param chunkSize
     * @return returns the error code for the creation operation
     */
    int VerifyCreateCloneChunk(ChunkID chunkId, const std::string &location,
                               uint64_t correctedSn, uint64_t sn,
                               uint64_t chunkSize);

    /**
     * @brief restore chunk
     * @param chunkId
     * @param offset
     * @param len
     * @return If the request fails to execute, -1 will be returned, otherwise an error code will be returned
     */
    int VerifyRecoverChunk(ChunkID chunkId, off_t offset, size_t len);

    /**
     * @brief to obtain chunk metadata and verify if the results meet expectations
     * @param chunkId
     * @param expCurSn Expected chunk version, -1 indicates non-existent
     * @param expSanpSn Expected snapshot version, -1 indicates non-existent
     * @param expLeader Expected redirectedLeader
     * @return returns 0 after successful verification, otherwise returns -1
     */
    int VerifyGetChunkInfo(ChunkID chunkId, SequenceNum expCurSn,
                           SequenceNum expSnapSn, string expLeader);

 private:
    struct ChunkServiceOpConf *opConf_;
    //Record the chunkId (expected existence) that has been written, used to determine whether the return value of the request meets expectations
    std::set<ChunkID> existChunks_;
};

}  // namespace chunkserver

}  // namespace curve

#endif  // TEST_INTEGRATION_COMMON_CHUNKSERVICE_OP_H_
