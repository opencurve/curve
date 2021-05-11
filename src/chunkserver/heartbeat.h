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
 * Project: Curve
 *
 * History:
 *          2018/12/20  Wenyu Zhou   Initial version
 */

#ifndef SRC_CHUNKSERVER_HEARTBEAT_H_
#define SRC_CHUNKSERVER_HEARTBEAT_H_

#include <braft/node_manager.h>
#include <braft/node.h>                  // NodeImpl

#include <map>
#include <vector>
#include <string>
#include <atomic>
#include <memory>
#include <thread>  //NOLINT

#include "include/chunkserver/chunkserver_common.h"
#include "src/chunkserver/copyset_node_manager.h"
#include "src/common/wait_interval.h"
#include "src/common/concurrent/concurrent.h"
#include "proto/heartbeat.pb.h"

using ::curve::common::Thread;

namespace curve {
namespace chunkserver {

using HeartbeatRequest  = curve::mds::heartbeat::ChunkServerHeartbeatRequest;
using HeartbeatResponse = curve::mds::heartbeat::ChunkServerHeartbeatResponse;
using ConfigChangeInfo  = curve::mds::heartbeat::ConfigChangeInfo;
using CopySetConf       = curve::mds::heartbeat::CopySetConf;
using CandidateError    = curve::mds::heartbeat::CandidateError;
using TaskStatus        = butil::Status;
using CopysetNodePtr    = std::shared_ptr<CopysetNode>;

static uint64_t GetAtomicUint64(void* arg) {
    std::atomic<uint64_t>* v = (std::atomic<uint64_t> *)arg;
    return v->load(std::memory_order_acquire);
}

/**
 * Heatbeat subsystem options
 */
struct HeartbeatOptions {
    ChunkServerID           chunkserverId;
    std::string             chunkserverToken;
    std::string             storeUri;
    std::string             mdsListenAddr;
    std::string             ip;
    uint32_t                port;
    uint32_t                intervalSec;
    uint32_t                timeout;
    CopysetNodeManager*     copysetNodeManager;

    std::shared_ptr<LocalFileSystem> fs;
};

/**
 * Heartbeat subsystem processing module
 */
class Heartbeat {
 public:
    Heartbeat() {}
    ~Heartbeat() {}

    /**
     * @brief Initialize the heartbeat subsystem
     * @param[in] options Heatbeat subsystem options
     * @return 0 for success, non 0 for failure
     */
    int Init(const HeartbeatOptions& options);

    /**
     * @brief Clear heartbeat subsystem
     * @return 0 for success, non 0 for failure
     */
    int Fini();

    /**
     * @brief Run heartbeat subsystem
     * @return 0 for success, non 0 for failure
     */
    int Run();

 private:
    /**
     * @brief Stop heartbeat subsystem
     * @return 0 for success, non 0 for failure
     */
    int Stop();

    /*
     * Heartbeat Worker Threads
     */
    void HeartbeatWorker();

    /*
     * Get information about Chunkserver file system space
     */
    int GetFileSystemSpaces(size_t* capacity, size_t* free);

    /*
     * Build dopyset information for heartbeat messages
     */
    int BuildCopysetInfo(curve::mds::heartbeat::CopySetInfo* info,
                         CopysetNodePtr copyset);

    /*
     * Build heartbeat request
     */
    int BuildRequest(HeartbeatRequest* request);

    /*
     * Send heartbeat request
     */
    int SendHeartbeat(const HeartbeatRequest& request,
                      HeartbeatResponse* response);

    /*
     * Execute heartbeat tasks
     */
    int ExecTask(const HeartbeatResponse& response);

    /*
     * Dump heatbeat request information
     */
    void DumpHeartbeatRequest(const HeartbeatRequest& request);

    /*
     * Dump heartbeat response information
     */
    void DumpHeartbeatResponse(const HeartbeatResponse& response);

    /*
     * Purge copyset instances and persistent data
     */
    TaskStatus PurgeCopyset(LogicPoolID poolId, CopysetID copysetId);

 private:
    // Heartbeat thread
    Thread hbThread_;

    // Control the heartbeat module to run or stop
    std::atomic<bool> toStop_;

    // Use timers
    ::curve::common::WaitInterval waitInterval_;

    // Copyset management module
    CopysetNodeManager* copysetMan_;

    // ChunkServer directory
    std::string storePath_;

    // heartbeat options
    HeartbeatOptions options_;

    // MDS address
    std::vector<std::string> mdsEps_;

    // Current mds providing services
    int inServiceIndex_;

    // ChunkServer address
    butil::EndPoint csEp_;

    // Module initialization time, unix time
    uint64_t startUpTime_;
};

}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_HEARTBEAT_H_

