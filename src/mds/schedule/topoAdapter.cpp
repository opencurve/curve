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
#include "src/mds/common/topology_define.h"
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
    return infos;
}

bool TopoAdapterImpl::GetChunkServerInfo(ChunkServerIdType id,
                                         ChunkServerInfo *out) {
    assert(out != nullptr);

    ::curve::mds::topology::ChunkServer cs;
    if (!topo_->GetChunkServer(id, &cs)) {
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
    return infos;
}

int TopoAdapterImpl::GetStandardZoneNumInLogicalPool(PoolIdType id) {
    ::curve::mds::topology::LogicalPool logicalPool;
    if (topo_->GetLogicalPool(id, &logicalPool)) {
        switch (logicalPool.GetLogicalPoolType()) {
            // TODO(lixiaocui): 暂未实现
            case LogicalPoolType::APPENDECFILE:
                return 0;
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
                   << canGetChunkServer
                   << ") or Server(" << canGetServer << ")"
                   << std::endl;
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
    if (topo_->GetServer(origin.GetId(), &server)) {
        out->info = PeerInfo{origin.GetId(), server.GetZoneId(), server.GetId(),
                             origin.GetHostIp(), origin.GetPort()};
    } else {
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
                         << chunkServer.GetChunkServerState().GetDiskUsed()
                         << std::endl;
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
