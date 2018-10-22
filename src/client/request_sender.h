/*
 * Project: curve
 * Created Date: 18-9-25
 * Author: wudemiao
 * Copyright (c) 2018 netease
 */

#ifndef CURVE_CLIENT_CHUNK_REQUEST_SENDER_H
#define CURVE_CLIENT_CHUNK_REQUEST_SENDER_H

#include <brpc/channel.h>
#include <butil/endpoint.h>

#include "src/client/client_common.h"
#include "src/client/chunk_closure.h"

namespace curve {
namespace client {

using curve::chunkserver::ChunkRequest;
using curve::chunkserver::ChunkResponse;

struct SenderOptions {
    SenderOptions() : maxRetry(3), timeoutMs(5000) {}
    int maxRetry;
    int timeoutMs;
};

class RequestSender {
 public:
    RequestSender(ChunkServerID chunkServerId,
                       butil::EndPoint serverEndPoint,
                       SenderOptions senderOptions = SenderOptions())
        : chunkServerId_(chunkServerId),
          serverEndPoint_(serverEndPoint),
          channel_(),
          senderOptions_(senderOptions) {}
    virtual ~RequestSender() { }

    CURVE_MOCK int Init();

    CURVE_MOCK int ReadChunk(LogicPoolID logicPoolId,
                  CopysetID copysetId,
                  ChunkID chunkId,
                  off_t offset,
                  size_t length,
                  ReadChunkClosure *done);
    CURVE_MOCK int WriteChunk(LogicPoolID logicPoolId,
                   CopysetID copysetId,
                   ChunkID chunkId,
                   const char *buf,
                   off_t offset,
                   size_t length,
                   WriteChunkClosure *done);

    CURVE_MOCK int ResetSender(ChunkServerID chunkServerId,
                            butil::EndPoint serverEndPoint);

 private:
    ChunkServerID   chunkServerId_;
    butil::EndPoint serverEndPoint_;
    brpc::Channel   channel_; /* TODO(wudemiao): 后期会维护多个 channel */
    SenderOptions   senderOptions_;
};

}   // namespace client
}   // namespace curve

#endif  // CURVE_CLIENT_CHUNK_REQUEST_SENDER_H
