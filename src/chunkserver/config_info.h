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

#include <memory>
#include <string>

#include "include/chunkserver/chunkserver_common.h"
#include "src/chunkserver/concurrent_apply/concurrent_apply.h"
#include "src/chunkserver/inflight_throttle.h"
#include "src/chunkserver/trash.h"
#include "src/fs/local_filesystem.h"

namespace curve {
namespace chunkserver {

using curve::chunkserver::concurrent::ConcurrentApplyModule;
using curve::fs::LocalFileSystem;

class FilePool;
class CopysetNodeManager;
class CloneManager;

/**
 * Configuration options for copyset node
 */
struct CopysetNodeOptions {
    // follower to candidate timeout, in ms, defaults to 1000ms
    int electionTimeoutMs;

    // The time interval for taking regular snapshots is 3600s by default, which
    // is 1 hour
    int snapshotIntervalS;

    // If true, read requests will be invoked in current lease leader node.
    // If false, all requests will propose to raft (log read).
    // Default: true
    bool enbaleLeaseRead;

    // If the difference between the follower and leader logs exceeds
    // catchupMargin, Will execute install snapshot for recovery, default: 1000
    int catchupMargin;

    // Enable pthread to execute user code, default to false
    bool usercodeInPthread;

    // All uri formats: ${protocol}://${absolute or relative path}
    //  eg:
    //  posix: local
    //  bluestore: bluestore

    // Raft log uri, default raft_log
    std::string logUri;

    // Raft meta uri, default raft_meta
    std::string raftMetaUri;

    // Raft snapshot uri, default raft_snpashot
    std::string raftSnapshotUri;

    // Chunk data uri, default data
    std::string chunkDataUri;

    // Chunk snapshot uri, default snapshot
    std::string chunkSnapshotUri;

    // Copyset data recycling uri, default recycler
    std::string recyclerUri;

    std::string ip;
    uint32_t port;
    // Chunk file size
    uint32_t maxChunkSize;
    // WAL segment file size
    uint32_t maxWalSegmentSize;
    // The page size of the chunk file
    uint32_t metaPageSize;
    // alignment for I/O request
    uint32_t blockSize;
    // Location length limit for clone chunks
    uint32_t locationLimit;

    // Concurrent module
    ConcurrentApplyModule* concurrentapply;
    // Chunk file pool
    std::shared_ptr<FilePool> chunkFilePool;
    // WAL file pool
    std::shared_ptr<FilePool> walFilePool;
    // File System Adaptation Layer
    std::shared_ptr<LocalFileSystem> localFileSystem;
    // When the recycle bin and heartbeat module determine that the chunkserver
    // is not in the copyset configuration group, Notify the copysetManager to
    // move the copyset directory to the recycle bin Actual recovery of physical
    // space after a period of time
    std::shared_ptr<Trash> trash;

    // Snapshot flow control
    scoped_refptr<SnapshotThrottle>* snapshotThrottle;

    // Limit the number of copyset concurrent recovery loads during chunkserver
    // startup, with a value of 0 indicating no limit
    uint32_t loadConcurrency = 0;
    // chunkserver sync_thread_pool number of threads.
    uint32_t syncConcurrency = 20;
    // copyset trigger sync timeout
    uint32_t syncTriggerSeconds = 25;
    // Check if the copyset has completed loading and the maximum number of
    // retries when an exception occurs Possible exceptions: 1. Currently, most
    // replicas have not yet been restored; 2. Network issues and other issues
    // preventing the acquisition of leaders
    // 3. Due to other reasons, it is not possible to obtain the committed index
    // of the leader
    uint32_t checkRetryTimes = 3;
    // the difference bewteen the current peer's applied_index and leader's
    // committed_index is less than this value Then it is determined that the
    // copyset has been loaded successfully
    uint32_t finishLoadMargin = 2000;
    // Internal sleep time for loop determination of whether copyset has been
    // loaded and completed
    uint32_t checkLoadMarginIntervalMs = 1000;

    // enable O_DSYNC when open chunkfile
    bool enableOdsyncWhenOpenChunkFile = false;
    // syncChunkLimit default limit
    uint64_t syncChunkLimit = 2 * 1024 * 1024;
    // syncHighChunkLimit default limit = 64k
    uint64_t syncThreshold = 64 * 1024;
    // check syncing interval
    uint32_t checkSyncingIntervalMs = 500u;

    CopysetNodeOptions();
};

/**
 *Dependencies for ChunkServiceManager
 */
struct ChunkServiceOptions {
    CopysetNodeManager* copysetNodeManager;
    CloneManager* cloneManager;
    std::shared_ptr<InflightThrottle> inflightThrottle;
};

inline CopysetNodeOptions::CopysetNodeOptions()
    : electionTimeoutMs(1000),
      snapshotIntervalS(3600),
      enbaleLeaseRead(true),
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
      metaPageSize(4096),
      blockSize(4096),
      concurrentapply(nullptr),
      chunkFilePool(nullptr),
      walFilePool(nullptr),
      localFileSystem(nullptr),
      snapshotThrottle(nullptr) {}

}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_CONFIG_INFO_H_
