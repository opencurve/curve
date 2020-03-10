/*
 * Project: curve
 * Created Date: 2020-01-13
 * Author: lixiaocui1
 * Copyright (c) 2018 netease
 */

#include <brpc/channel.h>
#include <brpc/controller.h>
#include "src/chunkserver/chunkserver_service.h"

namespace curve {
namespace chunkserver {
void ChunkServerServiceImpl::ChunkServerStatus(
    RpcController *controller,
    const ChunkServerStatusRequest *request,
    ChunkServerStatusResponse *response,
    Closure *done) {
    brpc::ClosureGuard done_guard(done);

    brpc::Controller* cntl =
        static_cast<brpc::Controller*>(controller);
    LOG(INFO) << "Received request[log_id=" << cntl->log_id()
        << "] from " << cntl->remote_side() << " to " << cntl->local_side()
        << ". [ChunkServerStatusRequest] " << request->DebugString();

    bool loadFinished = copysetNodeManager_->LoadFinished();
    response->set_copysetloadfin(loadFinished);
    LOG(INFO) << "Send response[log_id=" << cntl->log_id()
        << "] from " << cntl->local_side() << " to " << cntl->remote_side()
        << ". [ChunkServerStatusResponse] " << response->DebugString();
}

}  // namespace chunkserver
}  // namespace curve

