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

#include <gflags/gflags.h>
#include <brpc/channel.h>
#include <braft/builtin_service.pb.h>

#include <string>
#include <iostream>

#include "proto/chunk.pb.h"
#include "proto/copyset.pb.h"
#include "src/tools/curve_tool_define.h"

using curve::chunkserver::CopysetStatusRequest;
using curve::chunkserver::CopysetStatusResponse;
using curve::chunkserver::COPYSET_OP_STATUS;
using curve::chunkserver::GetChunkHashRequest;
using curve::chunkserver::GetChunkHashResponse;
using curve::chunkserver::CHUNK_OP_STATUS;

namespace curve {
namespace tool {

struct Chunk {
    Chunk(uint32_t poolId, uint32_t csId, uint64_t chunkId2) :
        logicPoolId(poolId), copysetId(csId), chunkId(chunkId2) {}
    uint32_t logicPoolId;
    uint32_t copysetId;
    uint64_t chunkId;
};

std::ostream& operator<<(std::ostream& os, const Chunk& chunk);

class ChunkServerClient {
 public:
    virtual ~ChunkServerClient() = default;
    /**
    * @brief initializes the channel. For an address, just initialize it once
    * @param csAddr chunkserver address
    * @return returns 0 for success, -1 for failure
    */
    virtual int Init(const std::string& csAddr);

    /**
    * @brief: Call the RaftStat interface of Braft to obtain detailed information about the replication group, and place it in iobuf
    * @param iobuf replication group details, valid when the return value is 0
    * @return returns 0 for success, -1 for failure
    */
    virtual int GetRaftStatus(butil::IOBuf* iobuf);

    /**
    * @brief: Check if the chunkserver is online, only check the controller, not the response
    * @return returns true online and false offline
    */
    virtual bool CheckChunkServerOnline();

    /**
    * @brief calls the GetCopysetStatus interface of chunkserver
    & @param request Query the request for the copyset
    * @param response The response returned contains detailed information about the replication group, which is valid when the return value is 0
    * @return returns 0 for success, -1 for failure
    */
    virtual int GetCopysetStatus(const CopysetStatusRequest& request,
                                 CopysetStatusResponse* response);

    /**
    * @brief Get the hash value of chunks from chunkserver
    & @param chunk The chunk to query
    * @param[out] The hash value chunkHash chunk, valid when the return value is 0
    * @return returns 0 for success, -1 for failure
    */
    virtual int GetChunkHash(const Chunk& chunk, std::string* chunkHash);

 private:
    brpc::Channel channel_;
    std::string csAddr_;
};

}  // namespace tool
}  // namespace curve

#endif  // SRC_TOOLS_CHUNKSERVER_CLIENT_H_
