/*
 * Project: curve
 * Created Date: 2020-01-13
 * Author: lixiaocui1
 * Copyright (c) 2018 netease
 */

#ifndef SRC_CHUNKSERVER_CHUNKSERVER_SERVICE_H_
#define SRC_CHUNKSERVER_CHUNKSERVER_SERVICE_H_

#include <memory>
#include "proto/chunkserver.pb.h"
#include "src/chunkserver/copyset_node_manager.h"

namespace curve {
namespace chunkserver {

class ChunkServerServiceImpl : public ChunkServerService {
 public:
    explicit ChunkServerServiceImpl(CopysetNodeManager* copysetNodeManager)
        : copysetNodeManager_(copysetNodeManager) {}

    virtual void ChunkServerStatus(
        RpcController *controller,
        const ChunkServerStatusRequest *request,
        ChunkServerStatusResponse *response,
        Closure *done);

 private:
    CopysetNodeManager *copysetNodeManager_;
};

}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_CHUNKSERVER_SERVICE_H_

