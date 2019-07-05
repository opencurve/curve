/*
 * Project: curve
 * Created Date: Thu Jan 03 2019
 * Author: lixiaocui
 * Copyright (c) 2018 netease
 */

#include <glog/logging.h>
#include <map>
#include <algorithm>
#include <utility>
#include <memory>
#include "src/mds/schedule/scheduler.h"
#include "src/mds/schedule/scheduler_helper.h"

using ::curve::mds::topology::UNINTIALIZE_ID;

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
* SelectBestPlacementChunkServer 过程说明
* 目的: 对于copyset-m(1, 2, 3), 要在chunkserverList{1, 2, 3, 4, 5, 6, 7...}中
       选择一个chunkserver-n替代副本1, 即最终生成Operator(copyset-m, +n, -1)

* 步骤:
step1: 计算chunkserver-n需要满足的zone的限制生成excludeZoneList 以及
       server的限制生成excludeServerList. 即将选择的chunkserver-n所在的
       zone和server不能在两个列表中

step2: for chunkserver-n in {1, 2, 3, 4, 5, 6, 7...} [遍历chunkserverList]
            1. 判断chunkserver-n是否满足zone和server的限制，不满足continue
            2. 判断chunkserver的状态，磁盘状态是否健康，不符合条件continue
            3. 判断copyset-m(1, 2, 3), +n, -1操作执行对{1, 2, 3, n}
               的scatter-width产生的影响是否符合条件:
               - scatter-width_map的定义是 std::map<chunkserverIdType, int>
               - scatter-width即为len(scatter-width_map)
                chunkserver-a的scatter-width_map:
                key: chunkserver-a上copyset的其他副本所在的chunkserver
                value: key上拥有chunkserver-a上copyset的个数
            例如: chunkserver-a上有copyset-1{a, 1, 2}, copyset-2{a, 2, 3}
               ①对于n(记为target, copyset迁入方): scatter-width_map中{2,3}对应的value分别+1 //NOLINT
               ②对于1(记为source, copyset迁出方): scatter-width_map中{2,3}对应的value分别-1 //NOLINT
               ③对于2,3(记为other): scatter-width_map中{n}对应的value+1,{1}对应的value-1 //NOLINT
            4. 如果operator的执行对于{1, 2, 3, n}的scatter-width产生的影响均符合条件，//NOLINT
               选中该chunkserver-n作为目标节点，break
       done
       如果没有找到一个n, 使得(+n, -1)执行后{1, 2, 3, n}的scatter-width都符合条件，//NOLINT
       那就选择对{1, 2, 3, n}的scatter-width负向影响最小的chunkserver
*/
ChunkServerIdType Scheduler::SelectBestPlacementChunkServer(
    const CopySetInfo &copySetInfo, ChunkServerIdType oldPeer) {
    // 同一物理池中的所有chunkserver
    ChunkServerInfo oldPeerInfo;
    if (!topo_->GetChunkServerInfo(oldPeer, &oldPeerInfo)) {
        LOG(ERROR) << "TopoAdapter cannot get info of chunkserver " << oldPeer;
        return UNINTIALIZE_ID;
    }
    std::vector<ChunkServerInfo> chunkServers =
        topo_->GetChunkServersInPhysicalPool(oldPeerInfo.info.physicalPoolId);

    if (chunkServers.size() <= copySetInfo.peers.size()) {
        LOG(ERROR) << "physicalPool " << oldPeerInfo.info.physicalPoolId
                   << " has " << chunkServers.size() << " chunkservers, "
                   "not bigger than copysetInfo peers size: "
                   << copySetInfo.peers.size();
        return UNINTIALIZE_ID;
    }

    // zone和server的限制
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
    // chunkserver需要根据copyset的数量排序
    SchedulerHelper::SortChunkServerByCopySetNumAsc(&chunkServers, topo_);
    for (auto &cs : chunkServers) {
        // 不满足zone或者server的限制
        if (excludeZones.find(cs.info.zoneId) != excludeZones.end() ||
            excludeServers.find(cs.info.serverId) != excludeServers.end()) {
            continue;
        }
        // chunkserver online状态, 磁盘状态，容量是否符合条件
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

        // 超过concurrent的不考虑
        if (opController_->ChunkServerExceed(cs.info.id)) {
            continue;
        }

        // 计算添加该副本对其他副本上scatter-with的影响
        std::map<ChunkServerIdType, std::pair<int, int>> out;
        int source = UNINTIALIZE_ID;
        int target = cs.info.id;
        int affected = 0;
        int minScatterWidth = GetMinScatterWidth(copySetInfo.id.first);
        if (minScatterWidth <= 0) {
            LOG(ERROR) << "minScatterWith in logical pool "
                      << copySetInfo.id.first << " is not initialized";
            return UNINTIALIZE_ID;
        }
        if (SchedulerHelper::InvovledReplicasSatisfyScatterWidthAfterMigration(
                copySetInfo, source, target, oldPeer, topo_,
                minScatterWidth, scatterWidthRangePerent_, &affected)) {
            LOG(INFO) << "SelectBestPlacementChunkServer select " << target
                      << " for " << copySetInfo.CopySetInfoStr()
                      << " to replace " << oldPeerInfo.info.id
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
              << " to replace " << oldPeerInfo.info.id
              << ", target has least influence for migration";
    return candidates[candidates.size() - 1].first;
}

/**
* SelectRedundantReplicaToRemove 过程说明
* 目的: 对于copyset-m(1, 2, 3, 4), 在副本列表{1, 2, 3, 4}中选择一个chunkserver移除
       即最终生成Operator(copyset-m, -n)

* 步骤:
step1: 以下条件符合时副本不能移除
       1. copyset-m的副本数量小于标准副本数量值, 报警
       2. copyset-m的zone数量不满足标准zone数量值，报警


step2: for chunkserver-n in {1, 2, 3, 4, 5, 6, 7...} [遍历chunkserverList]
            1. 判断chunkserver-n是否满足zone和server的限制，不满足continue
            2. 判断chunkserver的状态，磁盘状态是否健康，不符合条件continue
            3. 判断copyset-m(1, 2, 3), +n, -1操作执行对{1, 2, 3, n}
               的scatter-width产生的影响是否符合条件:
               - scatter-width_map的定义是 std::map<chunkserverIdType, int>
               - scatter-width即为len(scatter-width_map)
                chunkserver-a的scatter-width_map:
                key: chunkserver-a上copyset的其他副本所在的chunkserver
                value: key上拥有chunkserver-a上copyset的个数
               ①对于n(记为target, copyset迁入方): scatter-width_map中{2,3}对应的value分别+1 //NOLINT
               ②对于1(记为source, copyset迁出方): scatter-width_map中{2,3}对应的value分别-1 //NOLINT
               ③对于2,3(记为other): scatter-width_map中{n}对应的value+1,{1}对应的value-1 //NOLINT
            4. 如果operator的执行对于{1, 2, 3, n}的scatter-width产生的影响均符合条件，//NOLINT
               选中该chunkserver-n作为目标节点，break
       done
       如果没有找到一个n, 使得(+n, -1)执行后{1, 2, 3, n}的scatter-width都符合条件，//NOLINT
       那就选择对{1, 2, 3, n}的scatter-width负向影响最小的chunkserver
*/
ChunkServerIdType Scheduler::SelectRedundantReplicaToRemove(
    const CopySetInfo &copySetInfo) {
    // 如果副本数量不大于标准副本数量，不应该进行移除，报警
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

    // 判断zone条件是否满足
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

    // 1. 不满足标准zone数量，报警
    // TODO(lixiaocui): 这种情况应该通过副本的增减进行调整
    if (zoneList.size() < standardZoneNum) {
        LOG(ERROR) << "topoAdapter find " << copySetInfo.CopySetInfoStr()
                   << " replicas distribute in "
                   << zoneList.size() << " zones, less than standard zoneNum "
                   << standardZoneNum << ", please check";
        return UNINTIALIZE_ID;
    }

    // 2. 大于等于标准zone数量
    // 2.1 等于标准zone数量
    // 为了满足zone条件的限制, 应该要从zone下包含多个ps中选取
    // 如replica的副本是 A(zone1) B(zone2) C(zone3) D(zone3) E(zone2)
    // 那么移除的副本应该从BCDE中选择
    // 2.2 大于标准zone数量
    // 无论移除哪个后都一定会满足zone条件限制。因此优先移除状态不是online的，然后考虑
    // scatter-with
    // 如replica副本是 A(zone1) B(zone2) C(zone3) D(zone4) E(zone4)
    // 可以随意从ABCDE中移除一个
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

    // 优先移除offline状态的副本
    for (auto cs : candidateChunkServer) {
        ChunkServerInfo csInfo;
        if (!topo_->GetChunkServerInfo(cs, &csInfo)) {
            LOG(ERROR) << "scheduler cannot get chunkserver "
                       << cs << " which is a replica of "
                       << copySetInfo.CopySetInfoStr();
            return UNINTIALIZE_ID;
        }

        // chunkserver不是online状态
        if (csInfo.IsOffline()) {
            LOG(ERROR) << "scheduler choose to remove offline chunkServer "
                       << cs << " from " << copySetInfo.CopySetInfoStr();
            return cs;
        }
    }

    // 所有chunkserver都是online状态，根据移除该副本对scatter-width的影响选择
    // 根据chunkserver上copyset的数量对candidateChunkserver进行排序
    std::map<ChunkServerIDType, std::vector<CopySetInfo>> distribute;
    std::vector<std::pair<ChunkServerIDType, std::vector<CopySetInfo>>> desc;
    for (auto csId : candidateChunkServer) {
        distribute[csId] = topo_->GetCopySetInfosInChunkServer(csId);
    }
    SchedulerHelper::SortDistribute(distribute, &desc);

    // 已优先移除offline状态的副本，然后移除满足scatter-width条件的copyset数量最多的
    // 用于记录移除该副本对所有chunkserver的影响
    std::vector<std::pair<ChunkServerIdType, int>> candidates;
    for (auto it = desc.begin(); it != desc.end(); it++) {
        // 计算移除该副本对其他副本上scatter-with的影响
        ChunkServerIDType source = it->first;
        ChunkServerIDType target = UNINTIALIZE_ID;
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

}  // namespace schedule
}  // namespace mds
}  // namespace curve


