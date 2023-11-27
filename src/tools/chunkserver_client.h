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
 * Created Date: 2019-11-28
 * Author: charisu
 */

#ifndef SRC_TOOLS_CHUNKSERVER_CLIENT_H_
#define SRC_TOOLS_CHUNKSERVER_CLIENT_H_

#include <braft/builtin_service.pb.h>
#include <brpc/channel.h>
#include <gflags/gflags.h>

#include <iostream>
#include <string>

#include "proto/chunk.pb.h"
#include "proto/copyset.pb.h"
#include "src/tools/curve_tool_define.h"

using curve::chunkserver::CHUNK_OP_STATUS;
using curve::chunkserver::COPYSET_OP_STATUS;
using curve::chunkserver::CopysetStatusRequest;
using curve::chunkserver::CopysetStatusResponse;
using curve::chunkserver::GetChunkHashRequest;
using curve::chunkserver::GetChunkHashResponse;

namespace curve {
namespace tool {

struct Chunk {
    Chunk(uint32_t poolId, uint32_t csId, uint64_t chunkId2)
        : logicPoolId(poolId), copysetId(csId), chunkId(chunkId2) {}
    uint32_t logicPoolId;
    uint32_t copysetId;
    uint64_t chunkId;
};

std::ostream& operator<<(std::ostream& os, const Chunk& chunk);

class ChunkServerClient {
 public:
    virtual ~ChunkServerClient() = default;
    /**
     *  @brief initializes the channel. For an address, just initialize it once
     *  @param csAddr chunkserver address
     *  @return returns 0 for success, -1 for failure
     */
    virtual int Init(const std::string& csAddr);

    /**
     * @brief Invoke the RaftStat interface of braft to retrieve detailed
     * information about the replication group and store it in the 'iobuf'.
     * @param iobuf: Replication group details; valid when the return value is
     * 0.
     * @return 0 on success, -1 on failure.
     */
    virtual int GetRaftStatus(butil::IOBuf* iobuf);

    /**
     * @brief Check if the chunkserver is online, only check the controller, not
     * the response.
     * @return true if online, false if offline.
     */
    virtual bool CheckChunkServerOnline();

    /**
     * @brief Invoke the GetCopysetStatus interface of the chunkserver.
     * @param request: The request to query the copyset.
     * @param[out] response: The response containing detailed information about
     * the replication group; valid when the return value is 0.
     * @return 0 on success, -1 on failure.
     */
    virtual int GetCopysetStatus(const CopysetStatusRequest& request,
                                 CopysetStatusResponse* response);

    /**
     * @brief Obtain the hash value of a chunk from the chunkserver.
     * @param chunk: The chunk to be queried.
     * @param[out] chunkHash: The hash value of the chunk; valid when the return
     * value is 0.
     * @return 0 on success, -1 on failure.
     */
    virtual int GetChunkHash(const Chunk& chunk, std::string* chunkHash);

 private:
    brpc::Channel channel_;
    std::string csAddr_;
};

}  // namespace tool
}  // namespace curve

#endif  // SRC_TOOLS_CHUNKSERVER_CLIENT_H_
