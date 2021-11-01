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

#ifndef CURVEFS_SRC_MDS_HEARTBEAT_METASERVER_HEALTHY_CHECKER_H_
#define CURVEFS_SRC_MDS_HEARTBEAT_METASERVER_HEALTHY_CHECKER_H_

#include <chrono> //NOLINT
#include <memory>
#include <map>
#include "curvefs/src/mds/common/types.h"
#include "curvefs/src/mds/common/mds_define.h"
#include "curvefs/src/mds/topology/topology.h"
#include "curvefs/proto/topology.pb.h"

using ::std::chrono::steady_clock;
using ::curvefs::mds::topology::MetaServerIdType;
using ::curvefs::mds::topology::Topology;
using ::curvefs::mds::topology::OnlineState;

namespace curvefs {
namespace mds {
namespace heartbeat {
struct HeartbeatOption {
    HeartbeatOption() : HeartbeatOption(0, 0, 0) {}
    HeartbeatOption(uint64_t heartbeatInterval,
                    uint64_t heartbeatMissTimeout,
                    uint64_t offLineTimeout) {
        this->heartbeatIntervalMs = heartbeatInterval;
        this->heartbeatMissTimeOutMs = heartbeatMissTimeout;
        this->offLineTimeOutMs = offLineTimeout;
    }

    // heartbeatIntervalMs: normal heartbeat interval.
    // heartbeat sent to mds by metaservers in every heartbeatInterval
    uint64_t heartbeatIntervalMs;

    // network jitter is unavoidable, and for this reason
    // background process will alarm during the inspection once it
    // finds out that heartbeat is missed after heartbeatMissTimeOut peroid
    uint64_t heartbeatMissTimeOutMs;

    // offLineTimeOutMs:
    // the maximun peroid that heartbeat is missed without
    // setting the metaserver to offline status and alarm.
    // scheduling will depend on this status of metaserver
    uint64_t offLineTimeOutMs;

    // start cleaning copysets on chunkservers after
    // starting mds for this peroid of time
    uint64_t cleanFollowerAfterMs;

    // the time when the mds start (fetch from system)
    steady_clock::time_point mdsStartTime;
};

struct HeartbeatInfo {
    HeartbeatInfo() : HeartbeatInfo(
        0, steady_clock::time_point(), OnlineState::UNSTABLE) {}
    HeartbeatInfo(
        MetaServerIdType id,
        const steady_clock::time_point& time,
        const OnlineState& state) {
        this->msId = id;
        this->lastReceivedTime = time;
        this->state = state;
    }
    MetaServerIdType msId;
    steady_clock::time_point lastReceivedTime;
    OnlineState state;
};

class MetaserverHealthyChecker {
 public:
    MetaserverHealthyChecker(
        HeartbeatOption option, const std::shared_ptr<Topology> &topo) :
        option_(option), topo_(topo) {}
    ~MetaserverHealthyChecker() {}

    /**
     * @brief Update last time to receive a heartbeat
     *
     * @param[in] msId metaserver ID
     * @param[in] time time that heartbeat received
     *
     */
    void UpdateLastReceivedHeartbeatTime(MetaServerIdType msId,
                                        const steady_clock::time_point &time);

    /**
     * @brief CheckHeartBeatInterval: For heartbeat timeout and offline
     * detection
     * This function uses a timer and executes inspections below:
     * 1. default value of OnlineFlag is false
     * 2. When OnlineFlag has a false value:
     *     If current-time - last-heartbeat-received-time <=
     *     heartbeatMissTimeOut_:
     *         Set OnlineFlag to true, and update OnlineState to ONLINE
     *         in topology
     * 3. When OnlineFlag has a true value:
     *     If current-time - last-heartbeat-received-time >
     *     heartbeatMissTimeOut_:
     *         Alarm for missing heartbeat
     *     If current-time - last-heartbeat-received-time > offLineTimeOut_:
     *         Set OnlineFlag to false and update OnlineState to OFFLINE in
     *         topology, then alarm
     */
    void CheckHeartBeatInterval();

    // for test
    bool GetHeartBeatInfo(MetaServerIdType id, HeartbeatInfo *info);

 private:
    bool MetaServerStateNeedUpdate(
        const HeartbeatInfo &info, OnlineState *newState);

    void UpdateMetaServerOnlineState(
        MetaServerIdType id, const OnlineState &newState);

 private:
    HeartbeatOption option_;
    std::shared_ptr<Topology> topo_;

    mutable Mutex hbinfoLock_;
    std::map<MetaServerIdType, HeartbeatInfo> heartbeatInfos_;
};

}  // namespace heartbeat
}  // namespace mds
}  // namespace curvefs

#endif  // CURVEFS_SRC_MDS_HEARTBEAT_METASERVER_HEALTHY_CHECKER_H_

