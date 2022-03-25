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
 * @Project: curve
 * @Date: 2021-11-8 11:01:48
 * @Author: chenwei
 */

#include "curvefs/src/mds/schedule/scheduler.h"
#include <glog/logging.h>
#include <algorithm>
#include <map>
#include <memory>
#include <utility>

namespace curvefs {
namespace mds {
namespace schedule {
// for test use, need a default implementation
int Scheduler::Schedule() { return 0; }

int64_t Scheduler::GetRunningInterval() { return 0; }

/**
 * process for SelectBestPlacementMetaServer process description:
 * 1. Select excludeZones/excludeMetaservers according to the peer of copyset
 * 2. Random select a zone in the same pool, that not in excludeZones
 * 3. Random select a metaserver in the selected zone,
 *    that not in excludeMetaservers
 * 4. If can not select a metaserver that meets the requirements,
 *    repeat step 2 and step 3 until step 2 can not find a avaliable zone
 */
MetaServerIdType Scheduler::SelectBestPlacementMetaServer(
    const CopySetInfo &copySetInfo, MetaServerIdType oldPeer) {
    // all the metaservers in the same pysical pool
    MetaServerInfo oldPeerInfo;
    if (oldPeer != UNINITIALIZE_ID) {
        if (!topo_->GetMetaServerInfo(oldPeer, &oldPeerInfo)) {
            LOG(ERROR) << "TopoAdapter cannot get info of metaserver "
                       << oldPeer;
            return UNINITIALIZE_ID;
        }
    }

    PoolIdType poolId = copySetInfo.id.first;
    std::vector<MetaServerInfo> metaServers =
        topo_->GetMetaServersInPool(poolId);
    if (metaServers.size() <= copySetInfo.peers.size()) {
        LOG(ERROR) << "pool " << poolId << " has " << metaServers.size()
                   << " metaservers, "
                      "not bigger than copysetInfo peers size: "
                   << copySetInfo.peers.size();
        return UNINITIALIZE_ID;
    }

    // restriction on zone and server
    std::set<ZoneIdType> excludeZones;
    std::set<MetaServerIdType> excludeMetaservers;
    for (auto &peer : copySetInfo.peers) {
        excludeMetaservers.emplace(peer.id);
        if (peer.id == oldPeer) {
            continue;
        }
        excludeZones.emplace(peer.zoneId);
    }

    uint16_t standardZoneNum = topo_->GetStandardZoneNumInPool(poolId);
    if (standardZoneNum <= 0) {
        LOG(ERROR) << "topoAdapter find pool " << poolId
                   << " standard zone num: " << standardZoneNum << " invalid";
        return UNINITIALIZE_ID;
    }

    MetaServerIdType target = UNINITIALIZE_ID;
    bool ret = topo_->ChooseNewMetaServerForCopyset(
        poolId, excludeZones, excludeMetaservers, &target);
    if (ret) {
        return target;
    }

    LOG(WARNING) << "can not find new metaserver for copyset:"
                 << copySetInfo.CopySetInfoStr() << ", oldPeer = " << oldPeer;
    return UNINITIALIZE_ID;
}

bool Scheduler::CopysetAllPeersOnline(const CopySetInfo &copySetInfo) {
    for (auto &peer : copySetInfo.peers) {
        MetaServerInfo out;
        if (!topo_->GetMetaServerInfo(peer.id, &out)) {
            return false;
        } else if (out.IsOffline()) {
            return false;
        }
    }

    return true;
}

}  // namespace schedule
}  // namespace mds
}  // namespace curvefs
