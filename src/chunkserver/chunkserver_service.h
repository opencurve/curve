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

