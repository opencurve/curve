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
#include <memory>
#include "src/mds/schedule/topoAdapter.h"
#include "src/mds/common/mds_define.h"
#include "proto/topology.pb.h"

using ::curve::mds::heartbeat::CandidateError;
using ::curve::mds::topology::LogicalPoolType;
using ::curve::mds::topology::ChunkServerStatus;

namespace curve {
namespace mds {
namespace schedule {
PeerInfo::PeerInfo(ChunkServerIdType id,
                   ZoneIdType zoneId,
                   ServerIdType sid,
                   PhysicalPoolIDType physicalPoolId,
                   const std::string &ip,
                   uint32_t port) {
    this->id = id;
    this->zoneId = zoneId;
    this->serverId = sid;
    this->physicalPoolId = physicalPoolId;
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

bool CopySetInfo::HasCandidate() const {
    return candidatePeerInfo.id != UNINTIALIZE_ID;
}

std::string CopySetInfo::CopySetInfoStr() const {
    std::string res = "[copysetId:(" + std::to_string(id.first) + "," +
        std::to_string(id.second) + "), epoch:" + std::to_string(epoch) +
        ", leader:" + std::to_string(leader) + ", peers:(";
    for (auto peer : peers) {
        res += std::to_string(peer.id) + ",";
    }

    res += "), canidate:" + std::to_string(candidatePeerInfo.id) +
        ", has configChangeInfo:" +
        std::to_string(configChangeInfo.IsInitialized())+"]";
    return res;
}

ChunkServerInfo::ChunkServerInfo(const PeerInfo &info,
                                 OnlineState state,
                                 DiskState diskState,
                                 ChunkServerStatus status,
                                 uint32_t leaderCount,
                                 uint64_t capacity,
                                 uint64_t used,
                                 const ChunkServerStatisticInfo
                                 &statisticInfo) {
    this->info = info;
    this->state = state;
    this->status = status;
    this->diskState = diskState;
    this->leaderCount = leaderCount;
    this->diskCapacity = capacity;
    this->diskUsed = used;
    this->statisticInfo = statisticInfo;
}

bool ChunkServerInfo::IsOffline() {
    return state == OnlineState::OFFLINE;
}

bool ChunkServerInfo::IsRetired() {
    return status == ChunkServerStatus::RETIRED;
}

bool ChunkServerInfo::IsHealthy() {
    return state == OnlineState::ONLINE &&
           diskState == DiskState::DISKNORMAL &&
           status != ChunkServerStatus::RETIRED;
}

TopoAdapterImpl::TopoAdapterImpl(
    std::shared_ptr<Topology> topo,
    std::shared_ptr<TopologyServiceManager> manager,
    std::shared_ptr<TopologyStat> stat) {
    this->topo_ = topo;
    this->topoServiceManager_ = manager;
    this->topoStat_ = stat;
}

bool TopoAdapterImpl::GetCopySetInfo(const CopySetKey &id, CopySetInfo *info) {
    ::curve::mds::topology::CopySetInfo csInfo;
    // cannot get copyset info
    if (!topo_->GetCopySet(id, &csInfo)) {
        return false;
    }

    // cannot get logical pool
    ::curve::mds::topology::LogicalPool lpool;
    if (!topo_->GetLogicalPool(csInfo.GetLogicalPoolId(), &lpool)) {
        return false;
    }

    if (!CopySetFromTopoToSchedule(csInfo, info)) {
        return false;
    }

    info->logicalPoolWork = lpool.GetLogicalPoolAvaliableFlag();
    return true;
}

std::vector<CopySetInfo> TopoAdapterImpl::GetCopySetInfosInChunkServer(
    ChunkServerIdType id) {
    std::vector<CopySetKey> keys = topo_->GetCopySetsInChunkServer(id);

    std::vector<CopySetInfo> out;
    for (auto key : keys) {
        CopySetInfo info;
        if (GetCopySetInfo(key, &info)) {
            if (info.logicalPoolWork) {
                out.emplace_back(info);
            }
        }
    }
    return out;
}

std::vector<CopySetInfo> TopoAdapterImpl::GetCopySetInfos() {
    std::vector<CopySetInfo> infos;
    for (auto copySetKey : topo_->GetCopySetsInCluster()) {
        CopySetInfo copySetInfo;
        if (GetCopySetInfo(copySetKey, &copySetInfo)) {
            if (copySetInfo.logicalPoolWork) {
                infos.push_back(copySetInfo);
            }
        }
    }
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
    for (auto chunkServerId : topo_->GetChunkServerInCluster(
        [] (const ChunkServer &cs) {
            return cs.GetStatus() != ChunkServerStatus::RETIRED;
        })) {
        ChunkServerInfo info;
        if (GetChunkServerInfo(chunkServerId, &info)) {
            infos.push_back(info);
        }
    }

    return infos;
}

std::vector<ChunkServerInfo> TopoAdapterImpl::GetChunkServersInPhysicalPool(
    PhysicalPoolIDType id) {
    std::vector<ChunkServerInfo> infos;
    auto ids = topo_->GetChunkServerInPhysicalPool(id,
        [](const ChunkServer &chunkserver) {
            return chunkserver.GetStatus() != ChunkServerStatus::RETIRED;
        });
    for (auto id : ids) {
        ChunkServerInfo out;
        if (GetChunkServerInfo(id, &out)) {
            infos.emplace_back(out);
        }
    }
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

int TopoAdapterImpl::GetMinScatterWidthInLogicalPool(PoolIdType id) {
    ::curve::mds::topology::LogicalPool logicalPool;
    if (topo_->GetLogicalPool(id, &logicalPool)) {
        return logicalPool.GetScatterWidth();
    }
    LOG(WARNING) << "topoAdapter can not get logicalpool: " << id;
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
            server.GetPhysicalPoolId(), cs.GetHostIp(), cs.GetPort());
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
    return true;
}

bool TopoAdapterImpl::ChunkServerFromTopoToSchedule(
    const ::curve::mds::topology::ChunkServer &origin,
    ::curve::mds::schedule::ChunkServerInfo *out) {
    assert(out != nullptr);

    ::curve::mds::topology::Server server;
    if (topo_->GetServer(origin.GetServerId(), &server)) {
        out->info = PeerInfo{origin.GetId(), server.GetZoneId(), server.GetId(),
                             server.GetPhysicalPoolId(),
                             origin.GetHostIp(), origin.GetPort()};
    } else {
        LOG(ERROR) << "can not get server:" << origin.GetId()
                   << ", ip:" << origin.GetHostIp() << ", port:"
                   << origin.GetPort() << " from topology";

        return false;
    }

    out->state = origin.GetOnlineState();
    out->status = origin.GetStatus();
    out->diskState = origin.GetChunkServerState().GetDiskState();
    out->diskCapacity = origin.GetChunkServerState().GetDiskCapacity();
    out->diskUsed = origin.GetChunkServerState().GetDiskUsed();

    ChunkServerStat stat;
    if (topoStat_->GetChunkServerStat(origin.GetId(), &stat)) {
        out->leaderCount = stat.leaderCount;
        return true;
    }
    LOG(ERROR) << "can not get chunkserver: " << origin.GetId() << " stat";
    return false;
}

bool TopoAdapterImpl::CreateCopySetAtChunkServer(CopySetKey id,
                                                 ChunkServerIdType csID) {
    ::curve::mds::topology::CopySetInfo info(id.first, id.second);
    std::vector<::curve::mds::topology::CopySetInfo> infos;
    infos.push_back(info);
    return topoServiceManager_->CreateCopysetNodeOnChunkServer(csID, infos);
}

void TopoAdapterImpl::GetChunkServerScatterMap(
    const ChunkServerIDType &cs, std::map<ChunkServerIdType, int> *out) {
    assert(out != nullptr);

    std::vector<CopySetKey> copySetsInCS = topo_->GetCopySetsInChunkServer(cs);
    for (auto key : copySetsInCS) {
        ::curve::mds::topology::CopySetInfo copySetInfo;
        if (!topo_->GetCopySet(key, &copySetInfo)) {
            LOG(WARNING) << "topoAdapter find can not get copySet ("
                         << key.first << "," << key.second << ")"
                         << " from topology" << std::endl;
            continue;
        }

        for (ChunkServerIdType peerId : copySetInfo.GetCopySetMembers()) {
            ::curve::mds::topology::ChunkServer chunkServer;
            if (peerId == cs) {
                continue;
            }

            if (!topo_->GetChunkServer(peerId, &chunkServer)) {
                continue;
            }

            if (chunkServer.GetOnlineState() == OnlineState::OFFLINE) {
                LOG_EVERY_N(ERROR, 1000) << "topoAdapter find chunkServer "
                           << chunkServer.GetId()
                           << " is offline, please check" << std::endl;
                continue;
            }

            if (out->find(peerId) == out->end()) {
                (*out)[peerId] = 1;
            } else {
                (*out)[peerId]++;
            }
        }
    }
}
}  // namespace schedule
}  // namespace mds
}  // namespace curve
