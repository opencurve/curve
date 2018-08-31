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
    void DeleteChunk(::google::protobuf::RpcController *controller,
                     const ::curve::chunkserver::ChunkRequest *request,
                     ::curve::chunkserver::ChunkResponse *response,
                     google::protobuf::Closure *done);

    void ReadChunk(::google::protobuf::RpcController *controller,
                   const ::curve::chunkserver::ChunkRequest *request,
                   ::curve::chunkserver::ChunkResponse *response,
                   google::protobuf::Closure *done);

    void WriteChunk(::google::protobuf::RpcController *controller,
                    const ::curve::chunkserver::ChunkRequest *request,
                    ::curve::chunkserver::ChunkResponse *response,
                    google::protobuf::Closure *done);

    void CreateChunkSnapshot(::google::protobuf::RpcController *controller,
                             const ::curve::chunkserver::ChunkSnapshotRequest *request,
                             ::curve::chunkserver::ChunkSnapshotResponse *response,
                             google::protobuf::Closure *done);

    void DeleteChunkSnapshot(::google::protobuf::RpcController *controller,
                             const ::curve::chunkserver::ChunkSnapshotRequest *request,
                             ::curve::chunkserver::ChunkSnapshotResponse *response,
                             google::protobuf::Closure *done);

    void ReadChunkSnapshot(::google::protobuf::RpcController *controller,
                           const ::curve::chunkserver::ChunkSnapshotRequest *request,
                           ::curve::chunkserver::ChunkSnapshotResponse *response,
                           google::protobuf::Closure *done);

 private:
    ChunkServiceOptions chunkServiceOptions_;
    CopysetNodeManager *copysetNodeManager_;
};

}  // namespace chunkserver
}  // namespace curve

#endif  // CURVE_CHUNKSERVER_CHUNK_SERVICE_H
