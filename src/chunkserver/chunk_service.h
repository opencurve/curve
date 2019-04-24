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
    explicit ChunkServiceImpl(ChunkServiceOptions chunkServiceOptions) :
        chunkServiceOptions_(chunkServiceOptions),
        copysetNodeManager_(chunkServiceOptions.copysetNodeManager) {}
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
    ChunkServiceOptions chunkServiceOptions_;
    CopysetNodeManager  *copysetNodeManager_;
};

}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_CHUNK_SERVICE_H_
