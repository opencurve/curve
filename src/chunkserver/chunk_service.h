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

#ifndef SRC_CHUNKSERVER_CHUNK_SERVICE_H_
#define SRC_CHUNKSERVER_CHUNK_SERVICE_H_

#include <vector>
#include <memory>
#include <string>

#include "proto/chunk.pb.h"
#include "src/chunkserver/config_info.h"

namespace curve {
namespace chunkserver {

using ::google::protobuf::RpcController;
using ::google::protobuf::Closure;

class CopysetNodeManager;

class ChunkServiceImpl : public ChunkService {
 public:
    explicit ChunkServiceImpl(ChunkServiceOptions chunkServiceOptions);
    ~ChunkServiceImpl() {}

    void DeleteChunk(RpcController *controller,
                     const ChunkRequest *request,
                     ChunkResponse *response,
                     Closure *done);

    void ReadChunk(RpcController *controller,
                   const ChunkRequest *request,
                   ChunkResponse *response,
                   Closure *done);

    void WriteChunk(RpcController *controller,
                    const ChunkRequest *request,
                    ChunkResponse *response,
                    Closure *done);

    void ReadChunkSnapshot(RpcController *controller,
                           const ChunkRequest *request,
                           ChunkResponse *response,
                           Closure *done);

    void DeleteChunkSnapshotOrCorrectSn(RpcController *controller,
                                        const ChunkRequest *request,
                                        ChunkResponse *response,
                                        Closure *done);

    void CreateCloneChunk(RpcController *controller,
                          const ChunkRequest *request,
                          ChunkResponse *response,
                          Closure *done);
    void CreateS3CloneChunk(RpcController* controller,
                       const CreateS3CloneChunkRequest* request,
                       CreateS3CloneChunkResponse* response,
                       Closure* done);
    void RecoverChunk(RpcController *controller,
                      const ChunkRequest *request,
                      ChunkResponse *response,
                      Closure *done);

    void GetChunkInfo(RpcController *controller,
                      const GetChunkInfoRequest *request,
                      GetChunkInfoResponse *response,
                      Closure *done);

    void GetChunkHash(RpcController *controller,
                      const GetChunkHashRequest *request,
                      GetChunkHashResponse *response,
                      Closure *done);

 private:
    /**
     * 验证op request的offset和length是否越界和对齐
     * @param offset[in]: op request' offset
     * @param len[in]: op request' length
     * @return true，说明合法，否则返回false
     */
    bool CheckRequestOffsetAndLength(uint32_t offset, uint32_t len);

 private:
    ChunkServiceOptions chunkServiceOptions_;
    CopysetNodeManager  *copysetNodeManager_;
    std::shared_ptr<InflightThrottle> inflightThrottle_;
    uint32_t            maxChunkSize_;
};

}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_CHUNK_SERVICE_H_
