/*
 * Project: curve
 * Created Date: 18-8-23
 * Author: wudemiao
 * Copyright (c) 2018 netease
 */

#ifndef CURVE_CHUNKSERVER_CHUNK_SERVICE_H
#define CURVE_CHUNKSERVER_CHUNK_SERVICE_H

#include "proto/chunk.pb.h"

namespace curve {
namespace chunkserver {

using ::google::protobuf::RpcController;
using ::google::protobuf::Closure;

class CopysetNodeManager;

struct ChunkServiceOptions {
    CopysetNodeManager *copysetNodeManager;
};

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

 private:
    ChunkServiceOptions chunkServiceOptions_;
    CopysetNodeManager  *copysetNodeManager_;
};

}  // namespace chunkserver
}  // namespace curve

#endif  // CURVE_CHUNKSERVER_CHUNK_SERVICE_H
