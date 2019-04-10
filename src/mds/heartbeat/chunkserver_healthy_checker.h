/*
 * Project: curve
 * Created Date: Mon Mar 25 2019
 * Author: lixiaocui
 * Copyright (c) 2018 netease
 */

#ifndef SRC_MDS_HEARTBEAT_CHUNKSERVER_HEALTHY_CHECKER_H_
#define SRC_MDS_HEARTBEAT_CHUNKSERVER_HEALTHY_CHECKER_H_

#include <chrono> //NOLINT
#include <memory>
#include <map>
#include "src/mds/common/mds_define.h"
#include "src/mds/topology/topology.h"

using ::std::chrono::steady_clock;
using ::curve::mds::topology::ChunkServerIdType;
using ::curve::mds::topology::Topology;

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
};

struct HeartbeatInfo {
    HeartbeatInfo() : HeartbeatInfo(0, steady_clock::time_point(), true) {}
    HeartbeatInfo(
        ChunkServerIdType id, const steady_clock::time_point &time, bool flag) {
        this->csId = id;
        this->lastReceivedTime = time;
        this->OnlineFlag = flag;
    }
    ChunkServerIdType csId;
    steady_clock::time_point lastReceivedTime;
    bool OnlineFlag;
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
    HeartbeatOption option_;
    std::shared_ptr<Topology> topo_;

    mutable RWLock hbinfoLock_;
    std::map<ChunkServerIdType, HeartbeatInfo> heartbeatInfos_;
};

}  // namespace heartbeat
}  // namespace mds
}  // namespace curve

#endif  // SRC_MDS_HEARTBEAT_CHUNKSERVER_HEALTHY_CHECKER_H_

