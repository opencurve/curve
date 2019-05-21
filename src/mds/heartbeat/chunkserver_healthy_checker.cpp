/*
 * Project: curve
 * Created Date: Mon Mar 25 2019
 * Author: lixiaocui
 * Copyright (c) 2018 netease
 */

#include <glog/logging.h>
#include "src/mds/heartbeat/chunkserver_healthy_checker.h"
#include "src/mds/topology/topology_item.h"
#include "proto/topology.pb.h"

using ::curve::mds::topology::ChunkServerState;
using ::curve::mds::topology::ChunkServerStatus;
using ::curve::mds::topology::OnlineState;
using ::curve::mds::topology::ChunkServer;
using ::curve::mds::topology::kTopoErrCodeSuccess;

using std::chrono::milliseconds;

namespace curve {
namespace mds {
namespace heartbeat {
void ChunkserverHealthyChecker::CheckHeartBeatInterval() {
    ::curve::common::WriteLockGuard lk(hbinfoLock_);
    auto iter = heartbeatInfos_.begin();
    // 后台线程检查心跳是否miss
    while (iter != heartbeatInfos_.end()) {
        bool needUpdateOnlineState = false;
        steady_clock::duration timePass =
            steady_clock::now() - iter->second.lastReceivedTime;
        // 当前时间 - 上次心跳到达时间 < miss, chunkserver状态应该是online
        if (timePass < milliseconds(option_.heartbeatMissTimeOutMs)) {
            if (!iter->second.OnlineFlag) {
                LOG(INFO) << "chunkServer " << iter->first << " is online";
                needUpdateOnlineState = true;
            }
        } else {
            // miss < 若当前时间 - 上次心跳到达时间 < offline, 报警
            if (timePass < milliseconds(option_.offLineTimeOutMs)) {
                LOG_EVERY_N(WARNING, 10)
                             << "heartbeatManager find chunkServer: "
                             << iter->first << " heartbeat miss, "
                             << timePass / milliseconds(1)
                             << " milliseconds from last heartbeat";
            } else {
                // 若当前时间 - 上次心跳到达时间 > offline, 报警, 设为offline
                // bug-fix[CLDCFS-904] offline状态应该一直报警
                LOG_EVERY_N(ERROR, 10)
                               << "heartbeatManager find chunkServer: "
                               << iter->first << " offline, "
                               << timePass / milliseconds(1)
                               << " milliseconds from last heartbeat";
                if (iter->second.OnlineFlag) {
                    needUpdateOnlineState = true;
                }
            }
        }

        // 如果状态有更新，包括
        // oneline -> offline     offline -> online
        // 更新到topology模块
        if (needUpdateOnlineState) {
            iter->second.OnlineFlag = !iter->second.OnlineFlag;
            UpdateChunkServerOnlineState(iter->first, iter->second.OnlineFlag);
        }

        // 如果是offline状态，并且chunkserver上没有copyset，设置为retired状态
        // 一般换盘的场景会出现
        bool iterNeedMove = true;
        if (false == iter->second.OnlineFlag) {
            // 设置retired成功, 将该chunkserver从heartbeatInfo列表中移除
            if (SetChunkServerRetired(iter->first)) {
                heartbeatInfos_.erase(iter++);
                iterNeedMove = false;
            }
        }

        if (iterNeedMove) {
            iter++;
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

void ChunkserverHealthyChecker::UpdateChunkServerOnlineState(
    ChunkServerIdType id, bool onlineFlag) {
    int updateErrCode = kTopoErrCodeSuccess;
    if (onlineFlag) {
        updateErrCode =
            topo_->UpdateOnlineState(OnlineState::ONLINE, id);
    } else {
        updateErrCode =
            topo_->UpdateOnlineState(OnlineState::OFFLINE, id);
    }

    if (kTopoErrCodeSuccess != updateErrCode) {
        LOG(ERROR) << "heartbeatManager update chunkserver get "
                        "error code: " << updateErrCode;
    }
}

bool ChunkserverHealthyChecker::SetChunkServerRetired(ChunkServerIdType id) {
    ChunkServer cs;
    if (!topo_->GetChunkServer(id, &cs)) {
        LOG(ERROR) << "heartbeatManager can not get chunkserver "
                    << id << " from topo";
        return false;
    }

    if (cs.GetStatus() == ChunkServerStatus::RETIRED) {
        LOG(INFO) << "chunkserver " << id << " is already in retired state";
        return true;
    }

    cs.SetStatus(ChunkServerStatus::RETIRED);
    // chunkserver上没有copyset
    if (topo_->GetCopySetsInChunkServer(id).empty()) {
        int updateErrCode = topo_->UpdateChunkServer(cs);
        // 更新失败
        if (kTopoErrCodeSuccess != updateErrCode) {
            LOG(ERROR) << "heartbeatManager update chunkserver get "
                    "error code: " << updateErrCode;
            return false;
        }
        // 更新成功
        LOG(INFO) << "heartbeatManager success update chunkserver "
                    << id << " to retired state";
        return true;
    // chunkserver上还有copyset
    } else {
        return false;
    }
}

}  // namespace heartbeat
}  // namespace mds
}  // namespace curve
