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

#ifndef SRC_CHUNKSERVER_CONFIG_INFO_H_
#define SRC_CHUNKSERVER_CONFIG_INFO_H_

#include <string>
#include <memory>

#include "src/fs/local_filesystem.h"
#include "src/chunkserver/trash.h"
#include "src/chunkserver/inflight_throttle.h"
#include "src/chunkserver/concurrent_apply/concurrent_apply.h"
#include "include/chunkserver/chunkserver_common.h"

namespace curve {
namespace chunkserver {

using curve::fs::LocalFileSystem;
using curve::chunkserver::concurrent::ConcurrentApplyModule;

class FilePool;
class CopysetNodeManager;
class CloneManager;

/**
 * copyset node configuration options
 */
struct CopysetNodeOptions {
    // follower to candidate timeout, in ms, default is 1000ms
    int electionTimeoutMs;

    // Regular snapshot interval, default 3600s, i.e. 1 hour
    int snapshotIntervalS;

    // If the difference between follower and leader logs exceeds the
    // catchupMargin, an install snapshot will be executed for recovery,
    // default: 1000
    int catchupMargin;

    // Whether to enable pthread execution of user code, default false
    bool usercodeInPthread;

    // All uri formats: ${protocol}://${Absolute or relative path}
    // eg:
    // posix: local
    // bluestore: bluestore

    // raft log uri, default raft_log
    std::string logUri;

    // raft meta uri, default raft_meta
    std::string raftMetaUri;

    // raft snapshot uri，default raft_snpashot
    std::string raftSnapshotUri;

    // chunk data uri，default data
    std::string chunkDataUri;

    // chunk snapshot uri，default snapshot
    std::string chunkSnapshotUri;

    // copyset data recycling uri，default recycler
    std::string recyclerUri;

    std::string ip;
    uint32_t port;
    // Size of chunk file
    uint32_t maxChunkSize;
    // Page size of chunk file
    uint32_t pageSize;
    // Location length limit for clone chunk
    uint32_t locationLimit;

    // Concurrency module
    ConcurrentApplyModule *concurrentapply;
    // Chunk file pool
    std::shared_ptr<FilePool> chunkFilePool;
    // File System Adaptation Layer
    std::shared_ptr<LocalFileSystem> localFileSystem;
    // When the trash and heartbeat module determines that the chunkserver is
    // not in the copyset configuration group, it notifies the copysetManager
    // to move the copyset directory to the trash
    // Actual recovery of physical space after a period of time
    std::shared_ptr<Trash> trash;

    // Snapshot Throttle
    scoped_refptr<SnapshotThrottle> *snapshotThrottle;

    // Limit the number of copyset concurrent recovery when the chunkserver
    // starts, 0 means no limit
    uint32_t loadConcurrency = 0;
    // Maximum number of retries when checking if copyset is loaded
    // Possible exceptions: 1. Most copies are not available at the moment;
    // 2. Network problems etc. prevent access to the leader
    // 3. Other reasons for not getting the leader's committed index
    uint32_t checkRetryTimes = 3;
    // If the difference between the applied_index of the current peer and
    // the committed_index on the leader is less than this value, then we can
    // say the copyset is loaded
    uint32_t finishLoadMargin = 2000;
    // Internal sleep time to loop to check if copyset is loaded
    uint32_t checkLoadMarginIntervalMs = 1000;

    CopysetNodeOptions();
};

/**
 * ChunkServiceManager dependencies
 */
struct ChunkServiceOptions {
    CopysetNodeManager *copysetNodeManager;
    CloneManager *cloneManager;
    std::shared_ptr<InflightThrottle> inflightThrottle;
};

}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_CONFIG_INFO_H_
