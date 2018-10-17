/*
 * Copyright (C) 2018 NetEase Inc. All rights reserved.
 * Project: Curve
 * 
 * History: 
 *          2018/08/30  Wenyu Zhou   Initial version
 */

#include "proto/chunkserver.pb.h"

#ifndef SRC_CHUNKSERVER_CHUNKSERVER_SERVICE_H__
#define SRC_CHUNKSERVER_CHUNKSERVER_SERVICE_H__

namespace curve {
namespace chunkserver {

class ChunkServer;

class ChunkServerServiceImpl : public ChunkServerService {
 public:
    explicit ChunkServerServiceImpl(ChunkServer *chunkserver) :
        chunkserver_(chunkserver) {}
    void GetState(::google::protobuf::RpcController *controller,
                  const ChunkServerRequest *request,
                  ChunkServerResponse *response,
                  ::google::protobuf::Closure *done) {
        // TODO(wenyu): to be implement
        return;
    }
    void GetInfo(::google::protobuf::RpcController *controller,
                 const ChunkServerRequest *request,
                 ChunkServerResponse *response,
                 ::google::protobuf::Closure *done) {
        // TODO(wenyu): to be implement
        return;
    }
 private:
    ChunkServer *chunkserver_;
};

}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_CHUNKSERVER_SERVICE_H__
