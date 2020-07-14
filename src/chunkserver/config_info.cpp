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
 * Created Date: 19-2-28
 * Author: wudemiao
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
      recyclerUri("/recycler"),
      port(8200),
      maxChunkSize(16 * 1024 * 1024),
      pageSize(4096),
      concurrentapply(nullptr),
      chunkfilePool(nullptr),
      localFileSystem(nullptr),
      snapshotThrottle(nullptr) {
}

}  // namespace chunkserver
}  // namespace curve
