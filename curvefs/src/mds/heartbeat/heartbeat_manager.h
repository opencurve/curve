/*
 *  Copyright (c) 2021 NetEase Inc.
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
 * Created Date: 2021-09-16
 * Author: chenwei
 */

#ifndef CURVEFS_SRC_MDS_HEARTBEAT_HEARTBEAT_MANAGER_H_
#define CURVEFS_SRC_MDS_HEARTBEAT_HEARTBEAT_MANAGER_H_

#include <atomic>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "curvefs/proto/heartbeat.pb.h"
#include "curvefs/src/mds/common/mds_define.h"
#include "curvefs/src/mds/heartbeat/copyset_conf_generator.h"
#include "curvefs/src/mds/heartbeat/metaserver_healthy_checker.h"
#include "curvefs/src/mds/heartbeat/topo_updater.h"
#include "curvefs/src/mds/schedule/coordinator.h"
#include "curvefs/src/mds/topology/topology.h"
#include "src/common/concurrent/concurrent.h"
#include "src/common/interruptible_sleeper.h"

using ::curvefs::mds::topology::PoolIdType;
using ::curvefs::mds::topology::CopySetIdType;
using ::curvefs::mds::topology::Topology;
using ::curvefs::mds::schedule::Coordinator;

using ::curve::common::Thread;
using ::curve::common::Atomic;
using ::curve::common::RWLock;
using ::curve::common::InterruptibleSleeper;

namespace curvefs {
namespace mds {
namespace heartbeat {

// the responsibilities of heartbeat manager including:
// 1. background threads inspection
//    - update lastest heartbeat timestamp of metaserver
//    - regular metaserver status inspection
// 2. update topology information
//    - update epoch, replicas relationship and partition info of topology
//      according to the copyset information reported by the metaserver

class HeartbeatManager {
 public:
    HeartbeatManager(const HeartbeatOption &option,
                     const std::shared_ptr<Topology> &topology,
                     const std::shared_ptr<Coordinator> &coordinator);

    ~HeartbeatManager() { Stop(); }

    /**
     * @brief Init Used by mds to initialize heartbeat module.
     *             It registers all metaservers to metaserver health
     *             checking module (class MetaserverHealthyChecker),
     *             and initializes them to online status
     */
    void Init();

    /**
     * @brief Run Create a child thread for health checking module, which
     *            inspect missing heartbeat of the metaserver
     */
    void Run();

    /*
    * @brief Stop Stop background thread of heartbeat module
    */
    void Stop();

    /**
     * @brief MetaServerHeartbeat Manage heartbeat request
     *
     * @param[in] request RPC heartbeat request
     * @param[out] response Response of heartbeat request
     */
    void MetaServerHeartbeat(const MetaServerHeartbeatRequest &request,
                             MetaServerHeartbeatResponse *response);

 private:
    /**
     * @brief Background thread for heartbeat timeout inspection
     */
    void MetaServerHealthyChecker();

    /**
     * @brief CheckRequest Check the validity of a heartbeat request
     *
     * @return Return HeartbeatStatusCode::hbOK when valid, otherwise return
     *         corresponding error code
     */
    HeartbeatStatusCode CheckRequest(
        const MetaServerHeartbeatRequest &request);  // NOLINT

    // TODO(lixiaocui): optimize, unify the names of the two CopySetInfo in
    // heartbeat and topology // NOLINT
    /**
     * @brief Convert copyset data structure from heartbeat format
     *        to topology format
     *
     * @param[in] info Copyset data reported by heartbeat
     * @param[out] out Copyset data structure of topology module
     *
     * @return Return true if succeeded, false if failed
     */
    bool TransformHeartbeatCopySetInfoToTopologyOne(
        const ::curvefs::mds::heartbeat::CopySetInfo &info,
        ::curvefs::mds::topology::CopySetInfo *out);

    /**
     * @brief Extract ip address and port number from string, and fetch
     *        corresponding metaserverID from topology. This is for receiving
     *        heartbeat message since it's a string in format of 'ip:port:id'
     *
     * @param[in] peer Metaserver info in form of string 'ip:port:id'
     *
     * @return metaserverId fetch by ip address and port number
     */
    MetaServerIdType GetMetaserverIdByPeerStr(const std::string &peer);

    void UpdateMetaServerSpace(const MetaServerHeartbeatRequest &request);

 private:
    // Dependencies of heartbeat
    std::shared_ptr<Topology> topology_;
    std::shared_ptr<Coordinator> coordinator_;

    // healthyChecker_ health checker running in background thread
    std::shared_ptr<MetaserverHealthyChecker> healthyChecker_;

    // topoUpdater_ update epoch, copyset relationship of topology
    std::shared_ptr<TopoUpdater> topoUpdater_;

    std::shared_ptr<CopysetConfGenerator> copysetConfGenerator_;

    // Manage metaserverHealthyChecker threads
    Thread backEndThread_;

    Atomic<bool> isStop_;
    InterruptibleSleeper sleeper_;
    int metaserverHealthyCheckerRunInter_;
};

}  // namespace heartbeat
}  // namespace mds
}  // namespace curvefs

#endif  // CURVEFS_SRC_MDS_HEARTBEAT_HEARTBEAT_MANAGER_H_
