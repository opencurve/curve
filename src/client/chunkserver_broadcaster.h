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
 * File Created: 2022-06-30
 * Author: xuchaojie
 */

#ifndef SRC_CLIENT_CHUNKSERVER_BROADCASTER_H_
#define SRC_CLIENT_CHUNKSERVER_BROADCASTER_H_

#include <list>

#include "include/client/libcurve_define.h"
#include "src/client/client_common.h"
#include "src/client/metacache_struct.h"

#include "src/client/chunkserver_client.h"

namespace curve {
namespace client {

struct ChunkServerBroadCasterOption {
    uint32_t BroadCastMaxNum;

    ChunkServerBroadCasterOption()
      : BroadCastMaxNum(200) {}
};

class ChunkServerBroadCaster {
 public:
    ChunkServerBroadCaster()
        : csClient_(std::make_shared<ChunkServerClient>()) {}

    ChunkServerBroadCaster(const ChunkServerBroadCasterOption &option,
        const std::shared_ptr<ChunkServerClient> &csClient)
        : option_(option), csClient_(csClient) {}

    ~ChunkServerBroadCaster() {}

    int BroadCastFileEpoch(uint64_t fileId, uint64_t epoch,
        const std::list<CopysetPeerInfo> &csLocs);

 private:
    ChunkServerBroadCasterOption option_;
    std::shared_ptr<ChunkServerClient> csClient_;
};

}   // namespace client
}   // namespace curve


#endif  // SRC_CLIENT_CHUNKSERVER_BROADCASTER_H_
