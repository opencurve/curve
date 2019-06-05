/*
 * Project: curve
 * Created Date: 18-8-23
 * Author: wudemiao
 * Copyright (c) 2018 netease
 */

#ifndef SRC_CHUNKSERVER_CHUNK_SERVICE_H_
#define SRC_CHUNKSERVER_CHUNK_SERVICE_H_

#include <vector>

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

    void RecoverChunk(RpcController *controller,
                      const ChunkRequest *request,
                      ChunkResponse *response,
                      Closure *done);

    void GetChunkInfo(RpcController *controller,
                      const GetChunkInfoRequest *request,
                      GetChunkInfoResponse *response,
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
    uint32_t            maxChunkSize_;
};

}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_CHUNK_SERVICE_H_
