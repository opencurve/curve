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
 * Created Date: Thu Jan 03 2019
 * Author: lixiaocui
 */

#include <glog/logging.h>
#include <map>
#include <algorithm>
#include <utility>
#include <memory>
#include "src/mds/schedule/scheduler.h"
#include "src/mds/schedule/scheduler_helper.h"

namespace curve {
namespace mds {
namespace schedule {
int Scheduler::Schedule() {
    return 0;
}

int64_t Scheduler::GetRunningInterval() {
    return 0;
}

/**
 * process for SelectBestPlacementChunkServer process description:
 * Purpose: For copyset-m(1, 2, 3), select a chunkserver-n in chunkserverList{1,
 *          2, 3, 4, 5, 6, 7...} to replace copy 1, and generate Operator (copyset-m, +n, -1) //NOLINT
 *
 * Steps:
 * 1: Calculate two lists of zone and server that the new Chunkserver cannot
 *    locate in.
 * 2: for chunkserver-n in {1, 2, 3, 4, 5, 6, 7...} [traverse chunkserverList]
 *     1. determine whether chunkserver-n meets the limits of zone and server,
 *        and continue if not
 *     2. determine whether the chunkserver is healthy, continue if not
 *     3. determine whether the impact of operation copyset-m(1, 2, 3), +n, -1
 *        on the scatter-width of {1, 2, 3, n} meets the conditions
 *        some definitions:
 *            - the definition of scatter-width_map is std::map<chunkserverIdType, int> //NOLINT
 *            - scatter-width here means len(scatter-width_map)
 *            - scatter-width_map of chunkserver-a:
 *              key: id of chunkserver that the other copies of copyset on chunkserver-a located //NOLINT
 *              value: the number of copyset that own by both the chunkserver 'key' and chunkserver-a //NOLINT
 *        example: chunkserver-a has copyset-1{a, 1, 2}, copyset-2{a, 2, 3},
 *                 and now we want to migrate the replica on chunkserver 1 to
 *                 chunkserver n, which is operator (copyset-1, +n, -1)
 *            ① for chunkserver n(mark as target, to which the replica migrate), value of {2,3} in scatter-width_map will increase 1 //NOLINT
 *            ② for chunkserver 1(mark as source, from which the replica migrate out), value of {2,3} in scatter-width_map will decrease 1 //NOLINT
 *            ③for chunkserver 2 and 3 (mark as other), value of {n} will increase 1, and value of {1} will decrease 1 //NOLINT
 *     4. if the impact of the execution of the operator on the scatter-width of
 *        {1, 2, 3, n} meets the conditions, selects the chunkserver-n as the
 *        target node and break
 * then we're done!
 * if there isn't any chunkserver n selected that makes the scatter-width of
 * {1, 2, 3, n} meet the condition after the execution of (+n, -1), choose the
 * chunkserver that makes the scatter-width decrease the least.
 */
ChunkServerIdType Scheduler::SelectBestPlacementChunkServer(
    const CopySetInfo &copySetInfo, ChunkServerIdType oldPeer) {
    ChunkServerInfo oldPeerInfo;
    if (oldPeer != UNINTIALIZE_ID) {
        if (!topo_->GetChunkServerInfo(oldPeer, &oldPeerInfo)) {
            LOG(ERROR) << "TopoAdapter cannot get info of chunkserver "
                << oldPeer;
            return UNINTIALIZE_ID;
        }
    }
    std::vector<ChunkServerInfo> chunkServers =
        topo_->GetChunkServersInLogicalPool(copySetInfo.id.first);
    if (chunkServers.size() <= copySetInfo.peers.size()) {
        LOG(ERROR) << "logicalPool " << copySetInfo.id.first
                   << " has " << chunkServers.size() << " chunkservers, "
                   "not bigger than copysetInfo peers size: "
                   << copySetInfo.peers.size();
        return UNINTIALIZE_ID;
    }

    // restriction on zone and server
    std::map<ZoneIdType, bool> excludeZones;
    std::map<ServerIdType, bool> excludeServers;
    std::vector<ChunkServerIdType> otherList;
    for (auto &peer : copySetInfo.peers) {
        if (peer.id == oldPeer) {
            continue;
        }
        excludeZones[peer.zoneId] = true;
        excludeServers[peer.serverId] = true;
        otherList.emplace_back(peer.id);
    }

    int standardZoneNum =
        topo_->GetStandardZoneNumInLogicalPool(copySetInfo.id.first);
    if (standardZoneNum <= 0) {
        LOG(ERROR) << "topoAdapter find logicalPool " << copySetInfo.id.first
                   << " standard zone num: " << standardZoneNum
                   << " invalid";
        return UNINTIALIZE_ID;
    }
    if (excludeZones.size() >= standardZoneNum) {
        excludeZones.clear();
    }

    std::vector<std::pair<ChunkServerIdType, int>> candidates;
    // sort chunkserver according to copyset it has
    SchedulerHelper::SortChunkServerByCopySetNumAsc(&chunkServers, topo_);
    for (auto &cs : chunkServers) {
        if (excludeZones.find(cs.info.zoneId) != excludeZones.end() ||
            excludeServers.find(cs.info.serverId) != excludeServers.end()) {
            continue;
        }
        // judge by online, disk and capacity status
        if (!cs.IsHealthy()) {
            // TODO(lixiaocui): consider capacity
            //  || !IsChunkServerCapacitySaturated(chunkServer)) {
            LOG(WARNING) << "topoAdapter find chunkServer " << cs.info.id
                         << " abnormal, diskState： " << cs.diskState
                         << ", onlineState: " << cs.state
                         << ", capacity： " << cs.diskCapacity
                         << ", used: " << cs.diskUsed;
            continue;
        }

        // exclude the chunkserver exceeding the concurrent limit
        if (opController_->ChunkServerExceed(cs.info.id)) {
            continue;
        }

        // calculate the influence on scatter-width of other replicas
        std::map<ChunkServerIdType, std::pair<int, int>> out;
        int source = UNINTIALIZE_ID;
        int target = cs.info.id;
        int affected = 0;
        int minScatterWidth = GetMinScatterWidth(copySetInfo.id.first);
        if (minScatterWidth <= 0) {
            LOG(WARNING) << "minScatterWith in logical pool "
                       << copySetInfo.id.first << " is not initialized";
            return UNINTIALIZE_ID;
        }
        if (SchedulerHelper::InvovledReplicasSatisfyScatterWidthAfterMigration(
                copySetInfo, source, target, oldPeer, topo_,
                minScatterWidth, scatterWidthRangePerent_, &affected)) {
            LOG(INFO) << "SelectBestPlacementChunkServer select " << target
                      << " for " << copySetInfo.CopySetInfoStr()
                      << " to replace " << oldPeer
                      << ", all replica satisfy scatter width of migration";
            return target;
        } else {
            candidates.emplace_back(
                std::pair<ChunkServerIdType, int>(target, affected));
        }
    }

    if (candidates.empty()) {
        return UNINTIALIZE_ID;
    }
    SchedulerHelper::SortScatterWitAffected(&candidates);
    LOG(INFO) << "SelectBestPlacementChunkServer select "
              << candidates[candidates.size() - 1].first
              << " for " << copySetInfo.CopySetInfoStr()
              << " to replace " << oldPeer
              << ", target has least influence for migration";
    return candidates[candidates.size() - 1].first;
}

/**
 * process of SelectRedundantReplicaToRemove
 * purpose: for copyset-m(1, 2, 3, 4), select a chunkserver to remove in
 *          chunkserver list {1, 2, 3, 4}, which can be represented in
 *          operator(copyset-m, -n)
 * steps:
 * 1. the replica cannot be removed if the following conditions are satisfied:
 *     - the replica number of copyset-m is less than the standard
 *     - the number of zones that copyset-m covered doesn't satisfy the standard
 *     alarm when cases above occur
 * 2. for chunkserver-n in {1, 2, 3, 4, 5, 6, 7...} [traverse chunkserverList]
 *     1. determine whether chunkserver-n meets the limits of zone and server,
 *        and continue if not
 *     2. determine whether the chunkserver is healthy, continue if not
 *     3. determine whether the impact of operation copyset-m(1, 2, 3), +n, -1
 *        on the scatter-width of {1, 2, 3, n} meets the conditions
 *        some definitions:
 *            - the definition of scatter-width_map is std::map<chunkserverIdType, int> //NOLINT
 *            - scatter-width here means len(scatter-width_map)
 *            - scatter-width_map of chunkserver-a:
 *              key: id of chunkserver that the other copies of copyset on chunkserver-a located //NOLINT
 *              value: the number of copyset that own by both the chunkserver 'key' and chunkserver-a //NOLINT
 *        example: chunkserver-a has copyset-1{a, 1, 2}, copyset-2{a, 2, 3},
 *                 and now we want to migrate the replica on chunkserver 1 to
 *                 chunkserver n, which is operator (copyset-1, +n, -1)
 *            ① for chunkserver n(mark as target, to which the replica migrate), value of {2,3} in scatter-width_map will increase 1 //NOLINT
 *            ② for chunkserver 1(mark as source, from which the replica migrate out), value of {2,3} in scatter-width_map will decrease 1 //NOLINT
 *            ③for chunkserver 2 and 3 (mark as other), value of {n} will increase 1, and value of {1} will decrease 1 //NOLINT
 *     4. if the impact of the execution of the operator on the scatter-width of
 *        {1, 2, 3, n} meets the conditions, selects the chunkserver-n as the
 *        target node and break
 * then we're done!
 * if there isn't any chunkserver n selected that makes the scatter-width of
 * {1, 2, 3, n} meet the condition after the execution of (+n, -1), choose the
 * chunkserver that makes the scatter-width decrease the least.
 */
ChunkServerIdType Scheduler::SelectRedundantReplicaToRemove(
    const CopySetInfo &copySetInfo) {
    // if the replica number is smaller or equal to the standard, stop removing
    // and alarm
    int standardReplicaNum =
        topo_->GetStandardReplicaNumInLogicalPool(copySetInfo.id.first);
    if (standardReplicaNum <= 0) {
        LOG(ERROR) << "topoAdapter get standard replicaNum "
                   << standardReplicaNum << " in logicalPool,"
                   " replicaNum must >=0, please check";
        return UNINTIALIZE_ID;
    }
    if (copySetInfo.peers.size() <= standardReplicaNum) {
        LOG(ERROR) << "topoAdapter cannot select redundent replica for "
                   << copySetInfo.CopySetInfoStr() << ", beacuse replicaNum "
                   << copySetInfo.peers.size()
                   << " not bigger than standard num "
                   << standardReplicaNum;
        return UNINTIALIZE_ID;
    }

    // determine whether the zone condition is satisfied
    std::map<ZoneIdType, std::vector<ChunkServerIdType>> zoneList;
    int standardZoneNum =
        topo_->GetStandardZoneNumInLogicalPool(copySetInfo.id.first);
    if (standardZoneNum <= 0) {
        LOG(ERROR) << "topoAdapter get standard zoneNum "
                   << standardZoneNum << " in logicalPool "
                   << copySetInfo.id.first
                   << ", zoneNum must >=0, please check";
        return UNINTIALIZE_ID;
    }
    for (auto &peer : copySetInfo.peers) {
        auto item = zoneList.find(peer.zoneId);
        if (item == zoneList.end()) {
            zoneList.emplace(std::make_pair(peer.zoneId,
                std::vector<ChunkServerIdType>({peer.id})));
        } else {
            item->second.emplace_back(peer.id);
        }
    }

    // 1. alarm if the zone number is lass than the standard
    // TODO(lixiaocui): adjust by adding or deleting replica in this case
    if (zoneList.size() < standardZoneNum) {
        LOG(ERROR) << "topoAdapter find " << copySetInfo.CopySetInfoStr()
                   << " replicas distribute in "
                   << zoneList.size() << " zones, less than standard zoneNum "
                   << standardZoneNum << ", please check";
        return UNINTIALIZE_ID;
    }
    // 2. if greater or equal to the standard number
    // 2.1 equal
    // to satisfy the restriction of zone condition, should choose      这里需要修改         为了满足zone条件的限制, 应该要从zone下包含多个ps中选取
    // for example, if the replica are A(zone1) B(zone2) C(zone3) D(zone3) E(zone2) //NOLINT
    // the one to remove should be selected from BCDE
    // 2.2 greater
    // any chunkserver to remove will satisfy the requirement, thus we first
    // consider the one not online, then consider the scatter-width
    // for example, if the replicas are:
    // A(zone1) B(zone2) C(zone3) D(zone4) E(zone4)
    // we can remove anyone of it
    std::vector<ChunkServerIdType> candidateChunkServer;
    for (auto item : zoneList) {
        if (item.second.size() == 1) {
            if (zoneList.size() == standardZoneNum) {
                continue;
            }
        }

        for (auto csId : item.second) {
            candidateChunkServer.emplace_back(csId);
        }
    }

    // consider offline chunkservers first
    for (auto cs : candidateChunkServer) {
        ChunkServerInfo csInfo;
        if (!topo_->GetChunkServerInfo(cs, &csInfo)) {
            LOG(ERROR) << "scheduler cannot get chunkserver "
                       << cs << " which is a replica of "
                       << copySetInfo.CopySetInfoStr();
            return UNINTIALIZE_ID;
        }

        if (csInfo.IsOffline()) {
            LOG(WARNING) << "scheduler choose to remove offline chunkServer "
                         << cs << " from " << copySetInfo.CopySetInfoStr();
            return cs;
        }
    }

    // if all chunkserver are online, select according to the impact on scatter-width //NOLINT
    // first rank chunkser by the number of copyset on it
    std::map<ChunkServerIdType, std::vector<CopySetInfo>> distribute;
    std::vector<std::pair<ChunkServerIdType, std::vector<CopySetInfo>>> desc;
    for (auto csId : candidateChunkServer) {
        distribute[csId] = topo_->GetCopySetInfosInChunkServer(csId);
    }
    SchedulerHelper::SortDistribute(distribute, &desc);

    // remove the chunkserver(replica) that has the most copyset and satisfy
    // the scatter-width condition
    std::vector<std::pair<ChunkServerIdType, int>> candidates;
    for (auto it = desc.begin(); it != desc.end(); it++) {
        // calculate the impact on scatter-width of other replicas
        ChunkServerIdType source = it->first;
        ChunkServerIdType target = UNINTIALIZE_ID;
        ChunkServerIdType ignore = UNINTIALIZE_ID;
        int affected = 0;
        int minScatterWidth = GetMinScatterWidth(copySetInfo.id.first);
        if (minScatterWidth <= 0) {
            LOG(ERROR) << "minScatterWith in logical pool "
                       << copySetInfo.id.first << " is not initialized";
            return UNINTIALIZE_ID;
        }
        if (SchedulerHelper::InvovledReplicasSatisfyScatterWidthAfterMigration(
                copySetInfo, source, target, ignore, topo_,
                minScatterWidth, scatterWidthRangePerent_, &affected)) {
            return source;
        } else {
            candidates.emplace_back(
                std::pair<ChunkServerIdType, int>(source, affected));
        }
    }

    if (candidates.empty()) {
        return UNINTIALIZE_ID;
    }

    SchedulerHelper::SortScatterWitAffected(&candidates);
    return candidates[candidates.size() - 1].first;
}

int Scheduler::GetMinScatterWidth(PoolIdType lpid) {
    return topo_->GetAvgScatterWidthInLogicalPool(lpid) *
        (1 - scatterWidthRangePerent_ / 2);
}

bool Scheduler::CopysetAllPeersOnline(const CopySetInfo &copySetInfo) {
    for (auto peer : copySetInfo.peers) {
        ChunkServerInfo out;
        if (!topo_->GetChunkServerInfo(peer.id, &out)) {
            return false;
        } else if (out.IsOffline()) {
            return false;
        }
    }

    return true;
}

}  // namespace schedule
}  // namespace mds
}  // namespace curve


