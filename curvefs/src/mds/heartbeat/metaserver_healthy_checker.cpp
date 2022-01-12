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

#include <glog/logging.h>
#include "curvefs/src/mds/heartbeat/metaserver_healthy_checker.h"
#include "curvefs/src/mds/topology/topology_item.h"
#include "curvefs/proto/topology.pb.h"

using ::curvefs::mds::topology::MetaServer;
using ::curvefs::mds::topology::TopoStatusCode;
using ::curve::common::WriteLockGuard;
using std::chrono::milliseconds;

namespace curvefs {
namespace mds {
namespace heartbeat {
void MetaserverHealthyChecker::CheckHeartBeatInterval() {
    std::lock_guard<Mutex> lockguard(hbinfoLock_);
    auto iter = heartbeatInfos_.begin();
    while (iter != heartbeatInfos_.end()) {
        // Check whether status need to be updated
        OnlineState newState;
        bool needUpdate = MetaServerStateNeedUpdate(iter->second, &newState);

        // update metaserver status to topology
        if (needUpdate) {
            UpdateMetaServerOnlineState(iter->first, newState);
            iter->second.state = newState;
        }
        iter++;
    }
}

bool MetaserverHealthyChecker::MetaServerStateNeedUpdate(
    const HeartbeatInfo &info, OnlineState *newState) {
    // time interval since last heartbeat arrived
    steady_clock::duration timePass =
        steady_clock::now() - info.lastReceivedTime;

    bool shouldOnline =
        (timePass < milliseconds(option_.heartbeatMissTimeOutMs));
    if (shouldOnline) {
        if (OnlineState::ONLINE != info.state) {
            LOG(INFO) << "metaServer " << info.msId << " is online";
            *newState = OnlineState::ONLINE;
            return true;
        }

        return false;
    }

    bool shouldUnstable = (timePass < milliseconds(option_.offLineTimeOutMs));
    if (shouldUnstable) {
        if (OnlineState::UNSTABLE != info.state) {
            LOG(WARNING) << "metaserver " << info.msId << " is unstable. "
                << timePass / milliseconds(1) << "ms from last heartbeat";
            *newState = OnlineState::UNSTABLE;
            return true;
        }

        return false;
    }

    if (OnlineState::OFFLINE != info.state) {
        LOG(WARNING) << "metaserver " << info.msId << " is offline. "
                << timePass / milliseconds(1) << "ms from last heartbeat";
        *newState = OnlineState::OFFLINE;
        return true;
    }

    return false;
}

void MetaserverHealthyChecker::UpdateLastReceivedHeartbeatTime(
    MetaServerIdType msId, const steady_clock::time_point &time) {
    std::lock_guard<Mutex> lockguard(hbinfoLock_);
    if (heartbeatInfos_.find(msId) == heartbeatInfos_.end()) {
        heartbeatInfos_.emplace(
            msId, HeartbeatInfo(msId, time, OnlineState::UNSTABLE));
        return;
    }
    heartbeatInfos_[msId].lastReceivedTime = time;
}

bool MetaserverHealthyChecker::GetHeartBeatInfo(
    MetaServerIdType id, HeartbeatInfo *info) {
    if (heartbeatInfos_.find(id) == heartbeatInfos_.end()) {
        return false;
    }

    *info = heartbeatInfos_[id];
    return true;
}

void MetaserverHealthyChecker::UpdateMetaServerOnlineState(
    MetaServerIdType id, const OnlineState &newState) {
    TopoStatusCode errCode = topo_->UpdateMetaServerOnlineState(newState, id);

    if (TopoStatusCode::TOPO_OK != errCode) {
        LOG(WARNING) << "heartbeatManager update metaserver get error code: "
            << topology::TopoStatusCode_Name(errCode);
    }
}

}  // namespace heartbeat
}  // namespace mds
}  // namespace curvefs
