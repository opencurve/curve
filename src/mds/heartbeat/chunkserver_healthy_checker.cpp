/*
 * Project: curve
 * Created Date: Mon Mar 25 2019
 * Author: lixiaocui
 * Copyright (c) 2018 netease
 */

#include <glog/logging.h>
#include "src/mds/heartbeat/chunkserver_healthy_checker.h"
#include "proto/topology.pb.h"

using ::curve::mds::topology::ChunkServerState;
using ::curve::mds::topology::OnlineState;

using std::chrono::milliseconds;

namespace curve {
namespace mds {
namespace heartbeat {
void ChunkserverHealthyChecker::CheckHeartBeatInterval() {
    // 后台线程检查心跳是否miss
    for (auto &value : heartbeatInfos_) {
        bool needUpdate = false;
        steady_clock::duration timePass =
            steady_clock::now() - value.second.lastReceivedTime;
        // 当前时间 - 上次心跳到达时间 < miss, chunkserver状态应该是online
        if (timePass < milliseconds(option_.heartbeatMissTimeOutMs)) {
            if (!value.second.OnlineFlag) {
                LOG(INFO) << "chunkServer " << value.first << "is online";
                needUpdate = true;
            }
        } else {
            // miss < 若当前时间 - 上次心跳到达时间 < offline, 报警
            if (timePass < milliseconds(option_.offLineTimeOutMs)) {
                LOG_EVERY_N(WARNING, 10)
                             << "heartbeatManager find chunkServer: "
                             << value.first << " heartbeat miss, "
                             << timePass / milliseconds(1)
                             << " milliseconds from last heartbeat";
            } else {
                // 若当前时间 - 上次心跳到达时间 > offline, 报警, 设为offline
                // bug-fix[CLDCFS-904] offline状态应该一直报警
                LOG_EVERY_N(ERROR, 10)
                               << "heartbeatManager find chunkServer: "
                               << value.first << " offline, "
                               << timePass / milliseconds(1)
                               << " milliseconds from last heartbeat";
                if (value.second.OnlineFlag) {
                    needUpdate = true;
                }
            }
        }

        // 如果状态有更新，包括
        // oneline -> offline     offline -> online
        // 更新到topology模块
        if (needUpdate) {
            value.second.OnlineFlag = !value.second.OnlineFlag;
            int updateErrCode = ::curve::mds::topology::kTopoErrCodeSuccess;
            if (value.second.OnlineFlag) {
                updateErrCode =
                    topo_->UpdateOnlineState(OnlineState::ONLINE, value.first);
            } else {
                updateErrCode =
                    topo_->UpdateOnlineState(OnlineState::OFFLINE, value.first);
            }

            if (::curve::mds::topology::kTopoErrCodeSuccess != updateErrCode) {
                LOG(ERROR) << "heartbeatManager update chunkserver get "
                           "error code: " << updateErrCode;
            }
        }
    }
}

void ChunkserverHealthyChecker::UpdateLastReceivedHeartbeatTime(
    ChunkServerIdType csId, const steady_clock::time_point &time) {
    ::curve::common::WriteLockGuard lk(hbinfoLock_);
    if (heartbeatInfos_.find(csId) == heartbeatInfos_.end()) {
        heartbeatInfos_.emplace(csId, HeartbeatInfo(csId, time, true));
        return;
    }
    heartbeatInfos_[csId].lastReceivedTime = time;
}

bool ChunkserverHealthyChecker::GetHeartBeatInfo(
    ChunkServerIdType id, HeartbeatInfo *info) {
    if (heartbeatInfos_.find(id) == heartbeatInfos_.end()) {
        return false;
    }

    *info = heartbeatInfos_[id];
    return true;
}
}  // namespace heartbeat
}  // namespace mds
}  // namespace curve
