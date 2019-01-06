/*
 * Project: curve
 * Created Date: Sat Jan 05 2019
 * Author: lixiaocui
 * Copyright (c) 2018 netease
 */

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

    CopysetInfo info;
    info.set_logicalpoolid(1);
    info.set_copysetid(1);
    info.set_epoch(10);
    info.set_leaderpeer("192.168.10.1:9000:0");
    info.add_peers("192.168.10.1:9000:0");
    info.add_peers("192.168.10.2:9000:0");
    info.add_peers("192.168.10.3:9000:0");
    auto addInfo = request.add_copysetinfos();
    *addInfo = info;

    auto *stats = new ChunkServerStatisticInfo();
    stats->set_readiops(1);
    stats->set_readrate(1);
    stats->set_writeiops(1);
    stats->set_writerate(1);
    request.set_allocated_stats(stats);

    return request;
}
}  // namespace heartbeat
}  // namespace mds
}  // namespace curve
