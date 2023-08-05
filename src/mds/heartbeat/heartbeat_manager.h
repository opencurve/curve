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
 * Created Date: Mon Nov 19 2018
 * Author: xuchaojie
 */

#ifndef SRC_MDS_HEARTBEAT_HEARTBEAT_MANAGER_H_
#define SRC_MDS_HEARTBEAT_HEARTBEAT_MANAGER_H_

#include <vector>
#include <map>
#include <atomic>
#include <string>
#include <memory>

#include "src/mds/topology/topology.h"
#include "src/mds/common/mds_define.h"
#include "src/mds/heartbeat/topo_updater.h"
#include "src/mds/heartbeat/copyset_conf_generator.h"
#include "src/mds/heartbeat/chunkserver_healthy_checker.h"
#include "src/mds/schedule/coordinator.h"
#include "src/common/concurrent/concurrent.h"
#include "src/common/interruptible_sleeper.h"
#include "proto/heartbeat.pb.h"
#include "src/mds/topology/topology_stat.h"

using ::curve::mds::topology::CopySetInfo;
using ::curve::mds::topology::PoolIdType;
using ::curve::mds::topology::CopySetIdType;
using ::curve::mds::topology::Topology;
using ::curve::mds::topology::TopologyStat;
using ::curve::mds::schedule::Coordinator;

using ::curve::common::Thread;
using ::curve::common::Atomic;
using ::curve::common::RWLock;
using ::curve::common::InterruptibleSleeper;

namespace curve {
namespace mds {
namespace heartbeat {

// the responsibilities of heartbeat manager including:
// 1. background threads inspection
//    - update lastest heartbeat timestamp of chunkserver
//    - regular chunkserver status inspection
// 2. distribute copyset instruction, in cases like:
//    - copyset reported by a chunkserver doesn't exist in mds, send 'empty'
//      config to certain chunkserver for cleaning corresponding copyset
//    - passing copyset information to scheduler (see UpdateChunkServerDiskStatus),   //NOLINT
//      check for any possible configuration changes
//    - chunkserver not included in follower copyset configuration,
//      send 'empty' config to certain chunkservers for cleaning
// 3. update topology information
//    - update epoch, copy relationship and other statistical data of topology
//      according to the copyset information reported by the chunkserver

class HeartbeatManager {
 public:
    HeartbeatManager(HeartbeatOption option,
        std::shared_ptr<Topology> topology,
        std::shared_ptr<TopologyStat> topologyStat,
        std::shared_ptr<Coordinator> coordinator);

    ~HeartbeatManager() {
        Stop();
    }

    /**
     * @brief Init Used by mds to initialize heartbeat module.
     *             It registers all chunkservers to chunkserver health
     *             checking module (class ChunkserverHealthyChecker),
     *             and initializes them to online status
     */
    void Init();

    /**
     * @brief Run Create a child thread for health checking module, which
     *            inspect missing heartbeat of the chunkserver
     */
    void Run();

    /*
    * @brief Stop Stop background thread of heartbeat module
    */
    void Stop();

    /**
     * @brief ChunkServerHeartbeat Manage heartbeat request
     *
     * @param[in] request RPC heartbeat request
     * @param[out] response Response of heartbeat request
     */
    void ChunkServerHeartbeat(const ChunkServerHeartbeatRequest &request,
                                ChunkServerHeartbeatResponse *response);

 private:
    /**
     * @brief Update disk status data of chunkserver
     *
     * @param request Heartbeat request
     */
    void UpdateChunkServerDiskStatus(
        const ChunkServerHeartbeatRequest &request);

    /**
     * @brief Update statistical data of chunkserver
     *
     * @param request Heartbeat request
     */
    void UpdateChunkServerStatistics(
        const ChunkServerHeartbeatRequest &request);

    /**
     * @brief Update version of chunkserver
     *
     * @param request Heartbeat request
     */
    void UpdateChunkServerVersion(const ChunkServerHeartbeatRequest &request);

    /**
     * @brief Background thread for heartbeat timeout inspection
     */
    void ChunkServerHealthyChecker();

    /**
     * @brief CheckRequest Check the validity of a heartbeat request
     *
     * @return Return HeartbeatStatusCode::hbOK when valid, otherwise return
     *         corresponding error code
     */
    HeartbeatStatusCode CheckRequest(const ChunkServerHeartbeatRequest &request);  // NOLINT

    // TODO(lixiaocui): optimize, unify the names of the two CopySetInfo in heartbeat and topology // NOLINT
    /**
     * @brief Convert copyset data structure from heartbeat format
     *        to topology format
     *
     * @param[in] info Copyset data reported by heartbeat
     * @param[out] out Copyset data structure of topology module
     *
     * @return Return true if succeeded, false if failed
     */
    bool FromHeartbeatCopySetInfoToTopologyOne(
        const ::curve::mds::heartbeat::CopySetInfo &info,
        ::curve::mds::topology::CopySetInfo *out);

    /**
     * @brief Extract ip address and port number from string, and fetch
     *        corresponding chunkserverID from topology. This is for receiving
     *        heartbeat message since it's a string in format of 'ip:port:id'
     *
     * @param[in] peer Chunkserver info in form of string 'ip:port:id'
     *
     * @return chunkserverId fetch by ip address and port number
     */
    ChunkServerIdType GetChunkserverIdByPeerStr(std::string peer);

 private:
    // Dependencies of heartbeat
    std::shared_ptr<Topology> topology_;
    std::shared_ptr<TopologyStat> topologyStat_;
    std::shared_ptr<Coordinator> coordinator_;

    // healthyChecker_ health checker running in background thread
    std::shared_ptr<ChunkserverHealthyChecker> healthyChecker_;
    // topoUpdater_ update epoch, copyset relationship of topology
    std::shared_ptr<TopoUpdater> topoUpdater_;
    // Decides whether an instruction to chunkserver is necessary according to old and new copyset info //NOLINT
    // It can deal with cases below:
    // 1. copyset reported by chunkserver doesn't exist in mds
    // 2. leader copyset
    // 3. chunkserver in not included in latest copyset
    std::shared_ptr<CopysetConfGenerator> copysetConfGenerator_;

    // Manage chunkserverHealthyChecker threads
    Thread backEndThread_;

    Atomic<bool> isStop_;
    InterruptibleSleeper sleeper_;
    int chunkserverHealthyCheckerRunInter_;
};

}  // namespace heartbeat
}  // namespace mds
}  // namespace curve

#endif  // SRC_MDS_HEARTBEAT_HEARTBEAT_MANAGER_H_
