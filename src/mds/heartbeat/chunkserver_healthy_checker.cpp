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
        // Check whether status need to be updated
        OnlineState newState;
        bool needUpdate = ChunkServerStateNeedUpdate(iter->second, &newState);

        // update chunkserver status to topology
        if (needUpdate) {
            UpdateChunkServerOnlineState(iter->first, newState);
            iter->second.state = newState;
        }
        // If a chunkserver is offline and contain no copyset, it will be set
        // to retired status
        // Function usually called when a disk need to be switched
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
    // time interval since last heartbeat arrived
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
    // Disregard when chunkserver is in ONLINE status
    if (OnlineState::ONLINE == info.state) {
        return false;
    }

    // Error when try fetching a chunkserver by its id
    ChunkServer cs;
    if (!topo_->GetChunkServer(info.csId, &cs)) {
        LOG(ERROR) << "heartbeatManager can not get chunkserver "
            << info.csId << " from topo";
        return false;
    }

    // chunkserver already retired
    if (cs.GetStatus() == ChunkServerStatus::RETIRED) {
        LOG(INFO) << "chunkserver " << info.csId
            << " is already in retired state";
        return true;
    }

    if (OnlineState::OFFLINE != info.state
            && cs.GetStatus() != ChunkServerStatus::PENDDING) {
        return false;
    }

    // Check for any remaining copyset on a chunkserver
    bool noCopyset = topo_->GetCopySetsInChunkServer(info.csId).empty();
    if (!noCopyset) {
        return false;
    }
    // Set chunkserver to retired status if there's no copyset on it
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
