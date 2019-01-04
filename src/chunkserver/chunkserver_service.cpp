/*
 * Copyright (C) 2018 NetEase Inc. All rights reserved.
 * Project: Curve
 *
 * History:
 *          2018/08/30  Wenyu Zhou   Initial version
 */

#include "src/chunkserver/chunkserver_service.h"

#include <glog/logging.h>
#include <brpc/closure_guard.h>
#include <brpc/controller.h>

#include "src/chunkserver/chunkserver.h"

namespace curve {
namespace chunkserver {

void ChunkServerServiceImpl::Stop(
            ::google::protobuf::RpcController *controller,
            const ChunkServerStopRequest *request,
            ChunkServerStopResponse *response,
            ::google::protobuf::Closure *done) {
    brpc::ClosureGuard doneGuard(done);

    LOG(WARNING) << "Received shutdown request, going to shutdown chunkserver.";
    chunkserver_->Stop();

    response->set_status(CHUNKSERVER_OP_STATUS_SUCCESS);

    return;
}

}  // namespace chunkserver
}  // namespace curve
