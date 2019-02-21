/*
 * Project: curve
 * Created Date: 19-2-28
 * Author: wudemiao
 * Copyright (c) 2018 netease
 */

#include "src/chunkserver/config_info.h"

#include "src/chunkserver/copyset_node_manager.h"
#include "src/chunkserver/datastore/chunkfile_pool.h"
#include "src/chunkserver/concurrent_apply.h"

namespace curve {
namespace chunkserver {

CopysetNodeOptions::CopysetNodeOptions()
    : electionTimeoutMs(1000),
      snapshotIntervalS(3600),
      catchupMargin(1000),
      usercodeInPthread(false),
      logUri("/log"),
      raftMetaUri("/raft_meta"),
      raftSnapshotUri("/raft_snapshot"),
      chunkDataUri("/data"),
      chunkSnapshotUri("/snapshot"),
      port(8200),
      maxChunkSize(16 * 1024 * 1024),
      pageSize(4096),
      concurrentapply(nullptr),
      chunkfilePool(nullptr),
      localFileSystem(nullptr) {
}

}  // namespace chunkserver
}  // namespace curve
