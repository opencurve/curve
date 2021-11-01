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

#include "curvefs/src/mds/schedule/topoAdapter.h"
#include <glog/logging.h>
#include <cfloat>
#include <map>
#include <memory>
#include <string>
#include "curvefs/proto/topology.pb.h"
#include "curvefs/src/mds/common/mds_define.h"

using ::curvefs::mds::heartbeat::CandidateError;
using ::curvefs::mds::topology::PoolIdType;
using ::curvefs::mds::topology::TopoStatusCode;

namespace curvefs {
namespace mds {
namespace schedule {
PeerInfo::PeerInfo(MetaServerIdType id, ZoneIdType zoneId, ServerIdType sid,
                   const std::string &ip, uint32_t port) {
    this->id = id;
    this->zoneId = zoneId;
    this->serverId = sid;
    this->ip = ip;
    this->port = port;
}

CopySetConf::CopySetConf(const CopySetKey &key, EpochType epoch,
                         const std::vector<PeerInfo> &peers,
                         ConfigChangeType type, MetaServerIdType item,
                         MetaServerIdType oldOne) {
    this->id.first = key.first;
    this->id.second = key.second;
    this->epoch = epoch;
    this->peers = peers;
    this->type = type;
    this->configChangeItem = item;
    this->oldOne = oldOne;
}

bool CopySetInfo::ContainPeer(MetaServerIdType id) const {
    for (auto peerId : peers) {
        if (id == peerId.id) {
            return true;
        }
    }
    return false;
}

bool CopySetInfo::HasCandidate() const {
    return candidatePeerInfo.id != UNINITIALIZE_ID;
}

std::string CopySetInfo::CopySetInfoStr() const {
    std::string res = "[copysetId:(" + std::to_string(id.first) + "," +
                      std::to_string(id.second) + "), epoch:" +
                      std::to_string(epoch) + ", leader:" +
                      std::to_string(leader) + ", peers:(";
    for (auto peer : peers) {
        res += std::to_string(peer.id) + ",";
    }

    res += "), canidate:" + std::to_string(candidatePeerInfo.id) +
           ", has configChangeInfo:" +
           std::to_string(configChangeInfo.IsInitialized()) + "]";
    return res;
}

MetaServerInfo::MetaServerInfo(const PeerInfo &info, OnlineState state,
                               uint64_t capacity,
                               uint64_t used) {
    this->info = info;
    this->state = state;
    this->diskCapacity = capacity;
    this->diskUsed = used;
    this->startUpTime = 0;
}

bool MetaServerInfo::IsOnline() const { return state == OnlineState::ONLINE; }

bool MetaServerInfo::IsOffline() const { return state == OnlineState::OFFLINE; }

bool MetaServerInfo::IsUnstable() const {
    return state == OnlineState::UNSTABLE;
}

bool MetaServerInfo::IsHealthy() const { return state == OnlineState::ONLINE; }

TopoAdapterImpl::TopoAdapterImpl(std::shared_ptr<Topology> topo,
                                 std::shared_ptr<TopologyManager> manager) {
    this->topo_ = topo;
    this->topoManager_ = manager;
}

std::vector<PoolIdType> TopoAdapterImpl::Getpools() {
    return topo_->GetPoolInCluster();
}

bool TopoAdapterImpl::GetCopySetInfo(const CopySetKey &id, CopySetInfo *info) {
    ::curvefs::mds::topology::CopySetInfo csInfo;
    // cannot get copyset info
    if (!topo_->GetCopySet(id, &csInfo)) {
        return false;
    }

    // cannot get logical pool
    ::curvefs::mds::topology::Pool lpool;
    if (!topo_->GetPool(csInfo.GetPoolId(), &lpool)) {
        return false;
    }

    if (!CopySetFromTopoToSchedule(csInfo, info)) {
        return false;
    }

    info->poolWork = lpool.GetPoolAvaliableFlag();
    return true;
}

std::vector<CopySetInfo> TopoAdapterImpl::GetCopySetInfos() {
    std::vector<CopySetInfo> infos;
    for (auto copySetKey : topo_->GetCopySetsInCluster()) {
        CopySetInfo copySetInfo;
        if (GetCopySetInfo(copySetKey, &copySetInfo)) {
            if (copySetInfo.poolWork) {
                infos.push_back(copySetInfo);
            }
        }
    }
    return infos;
}

std::vector<CopySetInfo> TopoAdapterImpl::GetCopySetInfosInMetaServer(
    MetaServerIdType id) {
    std::vector<CopySetKey> keys = topo_->GetCopySetsInMetaServer(id);

    std::vector<CopySetInfo> out;
    for (auto key : keys) {
        CopySetInfo info;
        if (GetCopySetInfo(key, &info)) {
            if (info.poolWork) {
                out.emplace_back(info);
            }
        }
    }
    return out;
}

bool TopoAdapterImpl::GetMetaServerInfo(MetaServerIdType id,
                                        MetaServerInfo *out) {
    assert(out != nullptr);

    ::curvefs::mds::topology::MetaServer ms;
    if (!topo_->GetMetaServer(id, &ms)) {
        LOG(ERROR) << "can not get metaServer:" << id << " from topology";
        return false;
    }
    return MetaServerFromTopoToSchedule(ms, out);
}

std::vector<MetaServerInfo> TopoAdapterImpl::GetMetaServerInfos() {
    std::vector<MetaServerInfo> infos;
    for (auto metaServerId : topo_->GetMetaServerInCluster()) {
        MetaServerInfo info;
        if (GetMetaServerInfo(metaServerId, &info)) {
            infos.push_back(info);
        }
    }

    return infos;
}

std::vector<MetaServerInfo> TopoAdapterImpl::GetMetaServersInPool(
    PoolIdType poolId) {
    std::vector<MetaServerInfo> infos;
    auto ids = topo_->GetMetaServerInPool(poolId);
    for (auto id : ids) {
        MetaServerInfo out;
        if (GetMetaServerInfo(id, &out)) {
            infos.emplace_back(out);
        }
    }
    return infos;
}

uint16_t TopoAdapterImpl::GetStandardZoneNumInPool(PoolIdType id) {
    ::curvefs::mds::topology::Pool pool;
    if (topo_->GetPool(id, &pool)) {
        return pool.GetRedundanceAndPlaceMentPolicy().zoneNum;
    }
    return 0;
}

uint16_t TopoAdapterImpl::GetStandardReplicaNumInPool(PoolIdType id) {
    ::curvefs::mds::topology::Pool pool;
    if (topo_->GetPool(id, &pool)) {
        return pool.GetReplicaNum();
    }
    return 0;
}

bool TopoAdapterImpl::GetPeerInfo(MetaServerIdType id, PeerInfo *peerInfo) {
    ::curvefs::mds::topology::MetaServer ms;
    ::curvefs::mds::topology::Server server;

    bool canGetMetaServer, canGetServer;
    if ((canGetMetaServer = topo_->GetMetaServer(id, &ms)) &&
        (canGetServer = topo_->GetServer(ms.GetServerId(), &server))) {
        *peerInfo = PeerInfo(ms.GetId(), server.GetZoneId(), server.GetId(),
                             ms.GetInternalHostIp(), ms.GetInternalPort());
    } else {
        LOG(ERROR) << "topoAdapter can not find metaServer(" << id
                   << ", res:" << canGetMetaServer
                   << ") or Server(res:" << canGetServer << ")";
        return false;
    }
    return true;
}

bool TopoAdapterImpl::CopySetFromTopoToSchedule(
    const ::curvefs::mds::topology::CopySetInfo &origin,
    ::curvefs::mds::schedule::CopySetInfo *out) {
    assert(out != nullptr);

    out->id.first = origin.GetPoolId();
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

bool TopoAdapterImpl::MetaServerFromTopoToSchedule(
    const ::curvefs::mds::topology::MetaServer &origin,
    ::curvefs::mds::schedule::MetaServerInfo *out) {
    assert(out != nullptr);

    ::curvefs::mds::topology::Server server;
    if (topo_->GetServer(origin.GetServerId(), &server)) {
        out->info =
            PeerInfo{origin.GetId(), server.GetZoneId(), server.GetId(),
                     origin.GetInternalHostIp(), origin.GetInternalPort()};
    } else {
        LOG(ERROR) << "can not get server:" << origin.GetId()
                   << ", ip:" << origin.GetInternalHostIp()
                   << ", port:" << origin.GetInternalPort() << " from topology";

        return false;
    }

    out->startUpTime = origin.GetStartUpTime();
    out->state = origin.GetOnlineState();
    out->diskCapacity = origin.GetMetaServerSpace().GetDiskCapacity();
    out->diskUsed = origin.GetMetaServerSpace().GetDiskUsed();
    return true;
}

bool TopoAdapterImpl::CreateCopySetAtMetaServer(CopySetKey id,
                                                MetaServerIdType msId) {
    return topoManager_->CreateCopysetNodeOnMetaServer(id.first, id.second,
                                                       msId);
}

bool TopoAdapterImpl::ChooseZoneInPool(
    PoolIdType poolId, ZoneIdType *zoneId,
    const std::set<ZoneIdType> &excludeZones) {
    std::set<ZoneIdType> zones;
    int needZoneNum = 1;
    TopoStatusCode ret =
        topo_->ChooseZonesInPool(poolId, &zones, excludeZones, needZoneNum);
    if (ret != TopoStatusCode::TOPO_OK || zones.size() <= 0) {
        return false;
    }

    *zoneId = *zones.begin();
    return true;
}

bool TopoAdapterImpl::ChooseSingleMetaServerInZone(
    ZoneIdType zoneId, MetaServerIdType *metaServerId,
    const std::set<MetaServerIdType> &excludeMetaservers) {
    MetaServerIdType tmpMetaserverId;
    TopoStatusCode ret = topo_->ChooseSingleMetaServerInZone(
        zoneId, &tmpMetaserverId, excludeMetaservers);
    if (ret != TopoStatusCode::TOPO_OK) {
        return false;
    }

    *metaServerId = tmpMetaserverId;
    return true;
}
}  // namespace schedule
}  // namespace mds
}  // namespace curvefs
