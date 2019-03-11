/*
 * Project: curve
 * Created Date: Wed Nov 28 2018
 * Author: lixiaocui
 * Copyright (c) 2018 netease
 */

#include <glog/logging.h>
#include <cfloat>
#include <string>
#include <map>
#include "src/mds/schedule/topoAdapter.h"
#include "src/mds/common/mds_define.h"
#include "proto/topology.pb.h"

using ::curve::mds::heartbeat::CandidateError;
using ::curve::mds::topology::LogicalPoolType;

namespace curve {
namespace mds {
namespace schedule {
PeerInfo::PeerInfo(ChunkServerIdType id,
                   ZoneIdType zoneId,
                   ServerIdType sid,
                   const std::string &ip,
                   uint32_t port) {
    this->id = id;
    this->zoneId = zoneId;
    this->serverId = sid;
    this->ip = ip;
    this->port = port;
}

CopySetConf::CopySetConf(const CopySetKey &key, EpochType epoch,
                         const std::vector<PeerInfo> &peers,
                         ConfigChangeType type,
                         ChunkServerIdType item) {
    this->id.first = key.first;
    this->id.second = key.second;
    this->epoch = epoch;
    this->peers = peers;
    this->type = type;
    this->configChangeItem = item;
}

CopySetInfo::CopySetInfo(CopySetKey id,
                         EpochType epoch,
                         ChunkServerIdType leader,
                         const std::vector<PeerInfo> &peers,
                         const ConfigChangeInfo &info,
                         const CopysetStatistics &statistics) {
    this->id.first = id.first;
    this->id.second = id.second;
    this->epoch = epoch;
    this->leader = leader;
    this->peers = peers;
    this->configChangeInfo = info;
    this->statisticsInfo = statistics;
}

CopySetInfo::~CopySetInfo() {
    if (this->configChangeInfo.IsInitialized()) {
        this->configChangeInfo.Clear();
    }
}

CopySetInfo::CopySetInfo(const CopySetInfo &in) {
    this->id.first = in.id.first;
    this->id.second = in.id.second;
    this->epoch = in.epoch;
    this->leader = in.leader;
    this->peers = in.peers;
    this->candidatePeerInfo = in.candidatePeerInfo;
    this->configChangeInfo = in.configChangeInfo;
    this->statisticsInfo = in.statisticsInfo;
}

bool CopySetInfo::ContainPeer(ChunkServerIdType id) const {
    for (auto peerId : peers) {
        if (id == peerId.id) {
            return true;
        }
    }
    return false;
}

ChunkServerInfo::ChunkServerInfo(const PeerInfo &info,
                                 OnlineState state,
                                 uint64_t capacity,
                                 uint64_t used,
                                 uint64_t time,
                                 const ChunkServerStatisticInfo
                                 &statisticInfo) {
    this->info = info;
    this->state = state;
    this->diskCapacity = capacity;
    this->diskUsed = used;
    this->stateUpdateTime = time;
    this->statisticInfo = statisticInfo;
}

bool ChunkServerInfo::IsOffline() {
    return state == OnlineState::OFFLINE;
}

TopoAdapterImpl::TopoAdapterImpl(
    std::shared_ptr<Topology> topo,
    std::shared_ptr<TopologyServiceManager> manager) {
    this->topo_ = topo;
    this->topoServiceManager_ = manager;
}

bool TopoAdapterImpl::GetCopySetInfo(const CopySetKey &id, CopySetInfo *info) {
    ::curve::mds::topology::CopySetInfo csInfo;
    // cannot get copyset info
    if (!topo_->GetCopySet(id, &csInfo)) {
        return false;
    }

    return CopySetFromTopoToSchedule(csInfo, info);
}

std::vector<CopySetInfo> TopoAdapterImpl::GetCopySetInfos() {
    std::vector<CopySetInfo> infos;
    for (auto copySetKey : topo_->GetCopySetsInCluster()) {
        CopySetInfo copySetInfo;
        if (GetCopySetInfo(copySetKey, &copySetInfo)) {
            infos.push_back(copySetInfo);
        }
    }

    DVLOG(6) << "topoAdapter get " << infos.size() << " copySets in cluster";
    return infos;
}

bool TopoAdapterImpl::GetChunkServerInfo(ChunkServerIdType id,
                                         ChunkServerInfo *out) {
    assert(out != nullptr);

    ::curve::mds::topology::ChunkServer cs;
    if (!topo_->GetChunkServer(id, &cs)) {
        LOG(ERROR) << "can not get chunkServer:" << id << " from topology";
        return false;
    }
    return ChunkServerFromTopoToSchedule(cs, out);
}

std::vector<ChunkServerInfo> TopoAdapterImpl::GetChunkServerInfos() {
    std::vector<ChunkServerInfo> infos;
    for (auto chunkServerId : topo_->GetChunkServerInCluster()) {
        ChunkServerInfo info;
        if (GetChunkServerInfo(chunkServerId, &info)) {
            infos.push_back(info);
        }
    }

    DVLOG(6) << "topoAdapter get " << infos.size()
             << " chunkServers in cluster";
    return infos;
}

int TopoAdapterImpl::GetStandardZoneNumInLogicalPool(PoolIdType id) {
    ::curve::mds::topology::LogicalPool logicalPool;
    if (topo_->GetLogicalPool(id, &logicalPool)) {
        switch (logicalPool.GetLogicalPoolType()) {
            // TODO(lixiaocui): 暂未实现
            case LogicalPoolType::APPENDECFILE:return 0;
            case LogicalPoolType::APPENDFILE:
                return logicalPool.GetRedundanceAndPlaceMentPolicy().
                    appendFileRAP.zoneNum;
            case LogicalPoolType::PAGEFILE:
                return logicalPool.GetRedundanceAndPlaceMentPolicy().
                    pageFileRAP.zoneNum;
        }
    }
    return 0;
}

int TopoAdapterImpl::GetStandardReplicaNumInLogicalPool(PoolIdType id) {
    ::curve::mds::topology::LogicalPool logicalPool;
    if (topo_->GetLogicalPool(id, &logicalPool)) {
        switch (logicalPool.GetLogicalPoolType()) {
            // TODO(lixiaocui): 暂未实现
            case LogicalPoolType::APPENDECFILE:return 0;
            case LogicalPoolType::APPENDFILE:
                return logicalPool.GetRedundanceAndPlaceMentPolicy().
                    appendFileRAP.replicaNum;
            case LogicalPoolType::PAGEFILE:
                return logicalPool.GetRedundanceAndPlaceMentPolicy().
                    pageFileRAP.replicaNum;
        }
    }
    return 0;
}

bool TopoAdapterImpl::GetPeerInfo(ChunkServerIdType id, PeerInfo *peerInfo) {
    ::curve::mds::topology::ChunkServer cs;
    ::curve::mds::topology::Server server;

    bool canGetChunkServer, canGetServer;
    if ((canGetChunkServer = topo_->GetChunkServer(id, &cs)) &&
        (canGetServer = topo_->GetServer(cs.GetServerId(), &server))) {
        *peerInfo = PeerInfo(
            cs.GetId(), server.GetZoneId(), server.GetId(),
            cs.GetHostIp(), cs.GetPort());
    } else {
        LOG(ERROR) << "topoAdapter can not find chunkServer("
                   << id << ", res:" << canGetChunkServer
                   << ") or Server(res:" << canGetServer << ")";
        return false;
    }
    return true;
}

bool TopoAdapterImpl::CopySetFromTopoToSchedule(
    const ::curve::mds::topology::CopySetInfo &origin,
    ::curve::mds::schedule::CopySetInfo *out) {
    assert(out != nullptr);

    out->id.first = origin.GetLogicalPoolId();
    out->id.second = origin.GetId();
    out->epoch = origin.GetEpoch();
    out->leader = origin.GetLeader();

    for (auto id : origin.GetCopySetMembers()) {
        PeerInfo peerInfo;
        if (GetPeerInfo(id, &peerInfo)) {
            out->peers.emplace_back(peerInfo);
        } else {
            return false;
        }
    }

    if (origin.HasCandidate()) {
        PeerInfo peerInfo;
        if (GetPeerInfo(origin.GetCandidate(), &peerInfo)) {
            out->candidatePeerInfo = peerInfo;
        } else {
            return false;
        }
    }
    // TODO(lixiaocui): out->statisticsInfo
    return true;
}

bool TopoAdapterImpl::ChunkServerFromTopoToSchedule(
    const ::curve::mds::topology::ChunkServer &origin,
    ::curve::mds::schedule::ChunkServerInfo *out) {
    assert(out != nullptr);

    ::curve::mds::topology::Server server;
    if (topo_->GetServer(origin.GetServerId(), &server)) {
        out->info = PeerInfo{origin.GetId(), server.GetZoneId(), server.GetId(),
                             origin.GetHostIp(), origin.GetPort()};
    } else {
        LOG(ERROR) << "can not get server:" << origin.GetId()
                   << ", ip:" << origin.GetHostIp() << ", port:"
                   << origin.GetPort() << " from topology";

        return false;
    }
    out->state = origin.GetChunkServerState().GetOnlineState();
    out->diskCapacity = origin.GetChunkServerState().GetDiskCapacity();
    out->diskUsed = origin.GetChunkServerState().GetDiskUsed();
    out->stateUpdateTime = origin.GetLastStateUpdateTime();
    return true;
    // TODO(lixiaocui): out->statisticInfo
}

// TODO(chaojie-schedule): Topology需要增加创建copyset的接口
bool TopoAdapterImpl::CreateCopySetAtChunkServer(CopySetKey id,
                                                 ChunkServerIdType csID) {
    ::curve::mds::topology::CopySetInfo info(id.first, id.second);
    return topoServiceManager_->CreateCopysetAtChunkServer(info, csID);
}

ChunkServerIdType TopoAdapterImpl::SelectBestPlacementChunkServer(
    const CopySetInfo &copySetInfo, ChunkServerIdType oldPeer) {
    // chunkservers in same logical pool
    auto chunkServersId
        = topo_->GetChunkServerInLogicalPool(copySetInfo.id.first);

    DVLOG(6) << "selectBestPlacementChunkServer get " << chunkServersId.size()
             << " chunkServers in logical pool " << copySetInfo.id.first;

    if (chunkServersId.size() <= copySetInfo.peers.size()) {
        return ::curve::mds::topology::UNINTIALIZE_ID;
    }

    // zone limit and server limit
    std::map<ZoneIdType, bool> excludeZones;
    std::map<ServerIdType, bool> excludeServers;
    for (auto &peer : copySetInfo.peers) {
        if (peer.id == oldPeer) {
            continue;
        }
        excludeZones[peer.zoneId] = true;
        excludeServers[peer.serverId] = true;
    }

    int standardZoneNum = 0;
    if ((standardZoneNum =
             GetStandardZoneNumInLogicalPool(copySetInfo.id.first)) <= 0) {
        LOG(WARNING) << "topoAdapter find logicalPool " << copySetInfo.id.first
                     << " standard zone num: " << standardZoneNum
                     << " invalid";
        return ::curve::mds::topology::UNINTIALIZE_ID;
    }
    if (excludeZones.size() > standardZoneNum) {
        excludeZones.clear();
    }

    // candidates satisfy limit of logicalPool,zone,server and other condition
    std::map<ChunkServerIdType, float> candidates;
    for (auto &csId : chunkServersId) {
        ChunkServer chunkServer;
        Server server;

        // topo do not contain chunkServerInfo
        if (!topo_->GetChunkServer(csId, &chunkServer)) {
            LOG(WARNING) << "topoAdapter find can not get chunkServer " << csId
                         << " from topology" << std::endl;
            continue;
        }
        // topo do not contain serverInfo
        if (!topo_->GetServer(chunkServer.GetServerId(), &server)) {
            LOG(WARNING) << "topoAdapter find can not get Server "
                         << chunkServer.GetServerId()
                         << " from topology" << std::endl;
            continue;
        }
        // dissatisfy zone and server limit
        if (excludeZones.find(server.GetZoneId()) != excludeZones.end() ||
            excludeServers.find(server.GetId()) != excludeServers.end()) {
            continue;
        }
        // dissatisfy healthy or capacity limit
        if (!IsChunkServerHealthy(chunkServer)) {
            // TODO(lixiaocui): consider capacity
            //  || !IsChunkServerCapacitySaturated(chunkServer)) {
            LOG(WARNING) << "topoAdapter find chunkServer "
                         << chunkServer.GetId()
                         << " abnormal, diskState： "
                         << chunkServer.GetChunkServerState().GetOnlineState()
                         << ", onlineState: "
                         << chunkServer.GetChunkServerState().GetOnlineState()
                         << ", capacity： "
                         << chunkServer.GetChunkServerState().GetDiskCapacity()
                         << ", used: "
                         << chunkServer.GetChunkServerState().GetDiskUsed();
            continue;
        }

        // calculate scatter width factor in current cluster and after transfer
        std::map<ChunkServerIdType, bool> scatterMap;
        int copySetNum = GetChunkServerScatterMap(chunkServer, &scatterMap);
        float originFactor, possibleFactor;
        if (scatterMap.empty()) {
            candidates[csId] = FLT_MAX;
            continue;
        } else {
            originFactor = static_cast<float>(copySetNum)
                / static_cast<float>(scatterMap.size());
        }

        for (auto &peer : copySetInfo.peers) {
            if (peer.id == csId) {
                continue;
            }
            ChunkServer csTmp;
            if (!topo_->GetChunkServer(peer.id, &csTmp) ||
                csTmp.GetChunkServerState().GetOnlineState()
                    == OnlineState::OFFLINE) {
                continue;
            }
            scatterMap[peer.id] = true;
        }
        possibleFactor = static_cast<float>(copySetNum + 1)
            / static_cast<float>(scatterMap.size());

        candidates[csId] = originFactor - possibleFactor;
    }

    if (candidates.empty()) {
        return ::curve::mds::topology::UNINTIALIZE_ID;
    }

    // return max gap-factor
    float maxGap;
    bool first = true;
    ChunkServerIdType target;
    for (auto &candidate : candidates) {
        if (first) {
            target = candidate.first;
            maxGap = candidate.second;
            first = false;
            continue;
        }

        if (candidate.second > maxGap) {
            target = candidate.first;
            maxGap = candidate.second;
        }
    }

    return target;
}

ChunkServerIdType TopoAdapterImpl::SelectRedundantReplicaToRemove(
    const CopySetInfo &copySetInfo) {
    // 如果副本数量不大于标准副本数量，不应该进行移除，报警
    int standardReplicaNum =
        GetStandardReplicaNumInLogicalPool(copySetInfo.id.first);
    if (standardReplicaNum <= 0) {
        LOG(WARNING) << "topoAdapter get standard replicaNum "
                     << standardReplicaNum << " in logicalPool,"
                     " replicaNum must >=0, please check";
        return ::curve::mds::topology::UNINTIALIZE_ID;
    }
    if (copySetInfo.peers.size() <= standardReplicaNum) {
        LOG(WARNING) << "topoAdapter cannot select redundent replica for"
                     << " copySet(" << copySetInfo.id.first << ", "
                     << copySetInfo.id.second << ") beacuse replicaNum "
                     << copySetInfo.peers.size()
                     << " not bigger than standard num "
                     << standardReplicaNum;
        return ::curve::mds::topology::UNINTIALIZE_ID;
    }

    // 判断zone条件是否满足
    std::map<ZoneIdType, std::vector<ChunkServerIdType>> zoneList;
    int standardZoneNum = GetStandardZoneNumInLogicalPool(copySetInfo.id.first);
    if (standardZoneNum <= 0) {
        LOG(WARNING) << "topoAdapter get standard zoneNum "
                     << standardZoneNum << " in logicalPool "
                     << copySetInfo.id.first
                     << ", zoneNum must >=0, please check";
        return ::curve::mds::topology::UNINTIALIZE_ID;
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
    if (zoneList.size() < standardZoneNum) {
        LOG(ERROR) << "topoAdapter find copySet(" << copySetInfo.id.first
                   << ", " << copySetInfo.id.second << ") replicas distribute"
                   << " in " << zoneList.size() << " zones, less than standard"
                   << "zoneNum " << standardZoneNum << ", please check";
        return ::curve::mds::topology::UNINTIALIZE_ID;
    }

    // 2. 大于等于标准zone数量
    // 2.1 等于标准zone数量
    // 为了满足zone条件的限制, 应该要从zone下包含多个ps中选取
    // 如replica的副本是 A(zone1) B(zone2) C(zone3) D(zone3) E(zone2)
    // 那么移除的副本应该从BCDE中选择
    // 2.2 大于标准zone数量
    // 无论移除哪个后都一定会满足zone条件限制。因此优先移除状态不是online的，然后考虑
    // chunkserver的使用容量的大小优先移除使用量大的
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

    // 优先移除offline状态的副本，然后移除磁盘使用量较多的
    uint64_t maxUsed = -1;
    ChunkServerIdType re = -1;
    for (auto csId : candidateChunkServer) {
        ChunkServerInfo csInfo;
        if (!GetChunkServerInfo(csId, &csInfo)) {
            LOG(ERROR) << "topoAdapter cannot get chunkserver "
                       << csId << " witch is a replica of copySet("
                       << copySetInfo.id.first << ","
                       << copySetInfo.id.second << ")";
            return ::curve::mds::topology::UNINTIALIZE_ID;
        }

        if (csInfo.state != OnlineState::ONLINE) {
            LOG(ERROR) << "topoAdapter find chunkServer " << csId
                       << " offline, please check!";
            return csId;
        }

        if (maxUsed == -1 || csInfo.diskUsed > maxUsed) {
            maxUsed = csInfo.diskUsed;
            re = csId;
        }
    }
    return re;
}

bool TopoAdapterImpl::IsChunkServerHealthy(const ChunkServer &cs) {
    return cs.GetChunkServerState().GetOnlineState() == OnlineState::ONLINE &&
        cs.GetChunkServerState().GetDiskState() == DiskState::DISKNORMAL;
}

// TODO(lixiaocui): consider capacity
// bool TopoAdapterImpl::IsChunkServerCapacitySaturated(
// const ChunkServer &cs) {
//    auto csState = cs.GetChunkServerState();
//    return csState.GetDiskCapacity() - csState.GetDiskUsed()
//                                          > GetCopySetSize();
// }

int TopoAdapterImpl::GetChunkServerScatterMap(
    const ChunkServer &cs,
    std::map<ChunkServerIdType, bool> *out) {
    assert(out != nullptr);

    auto copySetsInCS = topo_->GetCopySetsInChunkServer(cs.GetId());
    for (auto key : copySetsInCS) {
        ::curve::mds::topology::CopySetInfo copySetInfo;
        if (!topo_->GetCopySet(key, &copySetInfo)) {
            LOG(WARNING) << "topoAdapter find can not get copySet ("
                         << key.first << "," << key.second << ")"
                         << " from topology" << std::endl;
            continue;
        }

        for (auto peerId : copySetInfo.GetCopySetMembers()) {
            ::curve::mds::topology::ChunkServer chunkServer;
            if (peerId == cs.GetId()) {
                continue;
            }

            if (!topo_->GetChunkServer(peerId, &chunkServer)) {
                continue;
            }

            if (chunkServer.GetChunkServerState().GetOnlineState()
                == OnlineState::OFFLINE) {
                LOG(ERROR) << "topoAdapter find chunkServer "
                           << chunkServer.GetId()
                           << " is offline, please check" << std::endl;
                continue;
            }
            (*out)[peerId] = true;
        }
    }
    return static_cast<int>(copySetsInCS.size());
}
}  // namespace schedule
}  // namespace mds
}  // namespace curve
