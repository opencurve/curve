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
 * Created Date: Sat Jan 05 2019
 * Author: lixiaocui
 */
#include <glog/logging.h>
#include <string>
#include "test/mds/heartbeat/common.h"

namespace curve {
namespace mds {
namespace heartbeat {
ChunkServerHeartbeatRequest GetChunkServerHeartbeatRequestForTest() {
    ChunkServerHeartbeatRequest request;
    request.set_chunkserverid(1);
    request.set_token("hello");
    request.set_diskcapacity(100);
    request.set_ip("192.168.10.1");
    request.set_port(9000);
    request.set_diskused(10);
    request.set_leadercount(10);
    request.set_copysetcount(100);

    DiskState *state = new DiskState();
    state->set_errtype(1);
    std::string *errMsg = new std::string("healthy");
    state->set_allocated_errmsg(errMsg);
    request.set_allocated_diskstate(state);

    auto info = request.add_copysetinfos();
    info->set_logicalpoolid(1);
    info->set_copysetid(1);
    info->set_epoch(10);
    for (int i = 1; i <= 3; i++) {
        std::string ip = "192.168.10." + std::to_string(i) + ":9000:0";
        auto peer = info->add_peers();
        peer->set_address(ip);
        if (i == 1) {
            auto peer = new ::curve::common::Peer();
            peer->set_address(ip);
            info->set_allocated_leaderpeer(peer);
        }
    }

    auto *stats = new ChunkServerStatisticInfo();
    stats->set_readiops(1);
    stats->set_readrate(1);
    stats->set_writeiops(1);
    stats->set_writerate(1);
    stats->set_chunksizeusedbytes(100);
    stats->set_chunksizeleftbytes(100);
    stats->set_chunksizetrashedbytes(100);
    request.set_allocated_stats(stats);

    return request;
}
}  // namespace heartbeat
}  // namespace mds
}  // namespace curve
