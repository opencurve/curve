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
 * Created Date: Mon Mar 25 2019
 * Author: lixiaocui
 */

#ifndef SRC_MDS_HEARTBEAT_CHUNKSERVER_HEALTHY_CHECKER_H_
#define SRC_MDS_HEARTBEAT_CHUNKSERVER_HEALTHY_CHECKER_H_

#include <chrono> //NOLINT
#include <memory>
#include <map>
#include "src/mds/common/mds_define.h"
#include "src/mds/topology/topology.h"
#include "proto/topology.pb.h"

using ::std::chrono::steady_clock;
using ::curve::mds::topology::ChunkServerIdType;
using ::curve::mds::topology::Topology;
using ::curve::mds::topology::OnlineState;

namespace curve {
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
    // heartbeat sent to mds by chunkservers in every heartbeatInterval
    uint64_t heartbeatIntervalMs;

    // network jitter is unavoidable, and for this reason
    // background process will alarm during the inspection once it
    // finds out that heartbeat is missed after heartbeatMissTimeOut peroid
    uint64_t heartbeatMissTimeOutMs;

    // offLineTimeOutMs: 
    // the maximun peroid that heartbeat is missed without
    // setting the chunkserver to offline status and alarm.
    // scheduling will depend on the status of chunkserver
    uint64_t offLineTimeOutMs;

    // start cleaning copysets on chunkservers after
    // running mds for this peroid of time
    uint64_t cleanFollowerAfterMs;

    // the time when the mds start (fetch from system)
    steady_clock::time_point mdsStartTime;
};

struct HeartbeatInfo {
    HeartbeatInfo() : HeartbeatInfo(
        0, steady_clock::time_point(), OnlineState::UNSTABLE) {}
    HeartbeatInfo(
        ChunkServerIdType id,
        const steady_clock::time_point& time,
        const OnlineState& state) {
        this->csId = id;
        this->lastReceivedTime = time;
        this->state = state;
    }
    ChunkServerIdType csId;
    steady_clock::time_point lastReceivedTime;
    OnlineState state;
};

class ChunkserverHealthyChecker {
 public:
    explicit ChunkserverHealthyChecker(
        HeartbeatOption option, std::shared_ptr<Topology> topo) :
        option_(option), topo_(topo) {}
    ~ChunkserverHealthyChecker() {}

    /**
     * @brief Update last time to receive a heartbeat
     *
     * @param[in] csId chunkserver ID
     * @param[in] time time that heartbeat received
     *
     */
    void UpdateLastReceivedHeartbeatTime(ChunkServerIdType csId,
                                        const steady_clock::time_point &time);

    /**
     * @brief CheckHeartBeatInterval: For heartbeat timeout and offline detection
     * This function uses a timer and executes inspections below: 
     * (default value of OnlineFlag is false)
     * When OnlineFlag has a false value:
     *     If current-time - last-heartbeat-received-time <= heartbeatMissTimeOut_:
     *         Set OnlineFlag to true, and update OnlineState to ONLINE in topology
     * When OnlineFlag has a true value:
     *     If current-time - last-heartbeat-received-time > heartbeatMissTimeOut_:
     *         Alarm for missing heartbeat
     *     If current-time - last-heartbeat-received-time > offLineTimeOut_:
     *         Set OnlineFlag to false and update OnlineState to OFFLINE in topology, then alarm
     */
    void CheckHeartBeatInterval();

    // for test
    bool GetHeartBeatInfo(ChunkServerIdType id, HeartbeatInfo *info);

 private:
    // check whether the state of a chunkserver need to be updated, 
    // new state pass by parameter newState if update needed
    bool ChunkServerStateNeedUpdate(
        const HeartbeatInfo &info, OnlineState *newState);

    void UpdateChunkServerOnlineState(
        ChunkServerIdType id, const OnlineState &newState);

    bool TrySetChunkServerRetiredIfNeed(const HeartbeatInfo &info);

 private:
    HeartbeatOption option_;
    std::shared_ptr<Topology> topo_;

    mutable RWLock hbinfoLock_;
    std::map<ChunkServerIdType, HeartbeatInfo> heartbeatInfos_;
};

}  // namespace heartbeat
}  // namespace mds
}  // namespace curve

#endif  // SRC_MDS_HEARTBEAT_CHUNKSERVER_HEALTHY_CHECKER_H_

