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
using ::curve::mds::topology::ChunkServer;
using ::curve::mds::topology::kTopoErrCodeSuccess;

using std::chrono::milliseconds;

namespace curve {
namespace mds {
namespace heartbeat {
void ChunkserverHealthyChecker::CheckHeartBeatInterval() {
    ::curve::common::WriteLockGuard lk(hbinfoLock_);
    auto iter = heartbeatInfos_.begin();
    while (iter != heartbeatInfos_.end()) {
        // 检测状态是否需要更新
        OnlineState newState;
        bool needUpdate = ChunkServerStateNeedUpdate(iter->second, &newState);

        // 将chunkserver的状态更新到topology中
        if (needUpdate) {
            UpdateChunkServerOnlineState(iter->first, newState);
            iter->second.state = newState;
        }

        // 如果是offline状态，并且chunkserver上没有copyset，设置为retired状态
        // 一般换盘的场景会出现
        bool iterNeedMove = TrySetChunkServerRetiredIfNeed(iter->second);
        if (iterNeedMove) {
            iter = heartbeatInfos_.erase(iter);
        } else {
            iter++;
        }
    }
}

bool ChunkserverHealthyChecker::ChunkServerStateNeedUpdate(
    const HeartbeatInfo &info, OnlineState *newState) {
    // 当前距离上次心跳的时间
    steady_clock::duration timePass =
        steady_clock::now() - info.lastReceivedTime;

    bool shouldOnline =
        (timePass < milliseconds(option_.heartbeatMissTimeOutMs));
    if (shouldOnline) {
        if (OnlineState::ONLINE != info.state) {
            LOG(INFO) << "chunkServer " << info.csId << " is online";
            *newState = OnlineState::ONLINE;
            return true;
        }

        return false;
    }

    bool shouldUnstable = (timePass < milliseconds(option_.offLineTimeOutMs));
    if (shouldUnstable) {
        if (OnlineState::UNSTABLE != info.state) {
            LOG(WARNING) << "chunkserver " << info.csId << " is unstable. "
                << timePass / milliseconds(1) << "ms from last heartbeat";
            *newState = OnlineState::UNSTABLE;
            return true;
        }

        return false;
    }

    bool shouldOffline = true;
    if (OnlineState::OFFLINE != info.state) {
        LOG(WARNING) << "chunkserver " << info.csId << " is offline. "
                << timePass / milliseconds(1) << "ms from last heartbeat";
        *newState = OnlineState::OFFLINE;
        return true;
    }

    return false;
}

void ChunkserverHealthyChecker::UpdateLastReceivedHeartbeatTime(
    ChunkServerIdType csId, const steady_clock::time_point &time) {
    ::curve::common::WriteLockGuard lk(hbinfoLock_);
    if (heartbeatInfos_.find(csId) == heartbeatInfos_.end()) {
        heartbeatInfos_.emplace(
            csId, HeartbeatInfo(csId, time, OnlineState::UNSTABLE));
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
    ChunkServerIdType id, const OnlineState &newState) {
    int errCode = topo_->UpdateChunkServerOnlineState(newState, id);

    if (kTopoErrCodeSuccess != errCode) {
        LOG(WARNING) << "heartbeatManager update chunkserver get error code: "
            << errCode;
    }
}

bool ChunkserverHealthyChecker::TrySetChunkServerRetiredIfNeed(
    const HeartbeatInfo &info) {
    // 非offline状态不考虑
    if (OnlineState::OFFLINE != info.state) {
        return false;
    }

    // 获取chunkserver出错
    ChunkServer cs;
    if (!topo_->GetChunkServer(info.csId, &cs)) {
        LOG(ERROR) << "heartbeatManager can not get chunkserver "
            << info.csId << " from topo";
        return false;
    }

    // chunkserver的状态已经是retired
    if (cs.GetStatus() == ChunkServerStatus::RETIRED) {
        LOG(INFO) << "chunkserver " << info.csId
            << " is already in retired state";
        return true;
    }

    // 查看chunkserver上是否还有copyset
    bool noCopyset = topo_->GetCopySetsInChunkServer(info.csId).empty();
    if (!noCopyset) {
        return false;
    }
    // 如果没有copyset, 设置为retired状态
    int updateErrCode = topo_->UpdateChunkServerRwState(
        ChunkServerStatus::RETIRED, info.csId);
    if (kTopoErrCodeSuccess != updateErrCode) {
        LOG(WARNING) << "heartbeatManager update chunkserver get error code: "
            << updateErrCode;
        return false;
    }

    LOG(INFO) << "heartbeatManager success update chunkserver " << info.csId
            << " to retired state";
    return true;
}

}  // namespace heartbeat
}  // namespace mds
}  // namespace curve
