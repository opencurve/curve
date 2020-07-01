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

    // heartbeatIntervalMs: 正常心跳间隔.
    // chunkServer每隔heartbeatInterval的时间会给mds发送心跳
    uint64_t heartbeatIntervalMs;

    // heartbeatMissTimeoutMs: 心跳超时时间
    // 网络抖动总是存在的，后台线程在检测过程如果发现chunkserver
    // 在heartbeatMissTimeOut的时间内没有心跳, 做报警处理
    uint64_t heartbeatMissTimeOutMs;

    // offLineTimeOutMs: 在offlineTimeOut的时间内如果没有收到chunkserver上报的心跳，//NOLINT
    // 把chunkserver的状态置为offline, 并报警。
    // schedule根据chunkserver的状态进行调度。
    uint64_t offLineTimeOutMs;

    // cleanFollowerAfterMs： mds启动cleanFollowerAfterMs时间后
    // 开启清理chunkserver上copyset的功能
    uint64_t cleanFollowerAfterMs;

    // mdsStartTime: mds启动时间
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
     * @brief UpdateLastReceivedHeartbeatTime 更新最近一次心跳到达时间
     *
     * @param[in] csId chunkserver ID
     * @param[in] time 收到心跳的时间
     *
     */
    void UpdateLastReceivedHeartbeatTime(ChunkServerIdType csId,
                                        const steady_clock::time_point &time);

    /**
     * @brief CheckHeartBeatInterval 执行心跳超时检查，offline检查
     * 使用一定时器，每隔heartbeatMissTimeOut_周期时间执行如下检查：
     * 1. OnlineFlag 初始值为false
     * 2. 当OnlineFlag 值为false时，
     *   若当前时间 - 上次心跳到达时间 <= heartbeatMissTimeOut_,
     *   则置OnlineFlag为true,并更新topology中OnlineState为ONLINE
     * 3. 当OnlineFlag 值为true时，
     *   若当前时间 - 上次心跳到达时间 > heartbeatMissTimeOut_，
     *   则报心跳miss报警
     * 4. 若当前时间 - 上次心跳到达时间 > offLineTimeOut_,
     *   则置OnlineFlag为false, 并更新topology中OnlineState为OFFLINE, 并报警
     */
    void CheckHeartBeatInterval();

    // for test
    bool GetHeartBeatInfo(ChunkServerIdType id, HeartbeatInfo *info);

 private:
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

