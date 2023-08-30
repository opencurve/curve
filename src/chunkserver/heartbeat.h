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
#include "src/chunkserver/scan_manager.h"
#include "proto/heartbeat.pb.h"
#include "proto/scan.pb.h"

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

/**
 *Heartbeat subsystem options
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
    ScanManager*            scanManager;

    std::shared_ptr<LocalFileSystem> fs;
    std::shared_ptr<FilePool> chunkFilePool;
};

/**
 *Heartbeat subsystem processing module
 */
class Heartbeat {
 public:
    Heartbeat() {}
    ~Heartbeat() {}

    /**
     * @brief Initialize heartbeat subsystem
     * @param[in] options Heartbeat subsystem options
     * @return 0: Success, non 0 failure
     */
    int Init(const HeartbeatOptions& options);

    /**
     * @brief Clean heartbeat subsystem
     * @return 0: Success, non 0 failure
     */
    int Fini();

    /**
     * @brief: Start the heartbeat subsystem
     * @return 0: Success, non 0 failure
     */
    int Run();

 private:
    /**
     * @brief Stop heartbeat subsystem
     * @return 0: Success, non 0 failure
     */
    int Stop();

    /*
     *Heartbeat Worker Thread
     */
    void HeartbeatWorker();

    /*
     *Obtain Chunkserver storage space information
     */
    int GetFileSystemSpaces(size_t* capacity, size_t* free);

    /*
     *Building a Copyset information item for heartbeat messages
     */
    int BuildCopysetInfo(curve::mds::heartbeat::CopySetInfo* info,
                         CopysetNodePtr copyset);

    /*
     *Build Heartbeat Request
     */
    int BuildRequest(HeartbeatRequest* request);

    /*
     *Send heartbeat message
     */
    int SendHeartbeat(const HeartbeatRequest& request,
                      HeartbeatResponse* response);

    /*
     *Perform Heartbeat Tasks
     */
    int ExecTask(const HeartbeatResponse& response);

    /*
     *Output heartbeat request information
     */
    void DumpHeartbeatRequest(const HeartbeatRequest& request);

    /*
     *Output heartbeat response information
     */
    void DumpHeartbeatResponse(const HeartbeatResponse& response);

    /*
     *Clean up replication group instances and persist data
     */
    TaskStatus PurgeCopyset(LogicPoolID poolId, CopysetID copysetId);

 private:
    //Heartbeat Thread
    Thread hbThread_;

    //Control the heartbeat module to run or stop
    std::atomic<bool> toStop_;

    //Using a timer
    ::curve::common::WaitInterval waitInterval_;

    //Copyset Management Module
    CopysetNodeManager* copysetMan_;

    //ChunkServer directory
    std::string storePath_;

    //Heartbeat Options
    HeartbeatOptions options_;

    //MDS address
    std::vector<std::string> mdsEps_;

    //Current mds for service
    int inServiceIndex_;

    //ChunkServer's own address
    butil::EndPoint csEp_;

    //Module initialization time, unix time
    uint64_t startUpTime_;

    ScanManager *scanMan_;
};

}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_HEARTBEAT_H_
