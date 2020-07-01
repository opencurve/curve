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
 * Created Date: 2020-01-13
 * Author: lixiaocui1
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

