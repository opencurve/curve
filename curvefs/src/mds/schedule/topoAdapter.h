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

#ifndef CURVEFS_SRC_MDS_SCHEDULE_TOPOADAPTER_H_
#define CURVEFS_SRC_MDS_SCHEDULE_TOPOADAPTER_H_
#include <cstdint>
#include <list>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>
#include "curvefs/proto/heartbeat.pb.h"
#include "curvefs/proto/topology.pb.h"
#include "curvefs/src/mds/common/mds_define.h"
#include "curvefs/src/mds/topology/topology.h"
#include "curvefs/src/mds/topology/topology_manager.h"

namespace curvefs {
namespace mds {
namespace schedule {
using ::curvefs::mds::topology::MetaServerIdType;
using ::curvefs::mds::topology::PoolIdType;
using ::curvefs::mds::topology::ServerIdType;
using ::curvefs::mds::topology::ZoneIdType;
using ::curvefs::mds::topology::EpochType;
using ::curvefs::mds::topology::CopySetKey;
using ::curvefs::mds::topology::CopySetIdType;
using ::curvefs::mds::topology::OnlineState;
using ::curvefs::mds::topology::Topology;
using ::curvefs::mds::topology::TopologyManager;
using ::curvefs::mds::topology::MetaServer;
using ::curvefs::mds::topology::Server;
using ::curvefs::mds::topology::Pool;
using ::curvefs::mds::topology::UNINITIALIZE_ID;
using ::curvefs::mds::topology::MetaServerSpace;
using ::curvefs::mds::heartbeat::ConfigChangeInfo;
using ::curve::mds::heartbeat::ConfigChangeType;

struct PeerInfo {
 public:
    PeerInfo() : id(UNINITIALIZE_ID), port(0) {}
    PeerInfo(MetaServerIdType id, ZoneIdType zoneId, ServerIdType sid,
             const std::string &ip, uint32_t port);
    MetaServerIdType id;
    ZoneIdType zoneId;
    ServerIdType serverId;
    std::string ip;
    uint32_t port;
};

struct CopySetConf {
 public:
    CopySetConf() = default;
    CopySetConf(
        const CopySetKey &key, EpochType epoch,
        const std::vector<PeerInfo> &peers, ConfigChangeType type,
        MetaServerIdType item,
        MetaServerIdType oldOne = ::curvefs::mds::topology::UNINITIALIZE_ID);

    CopySetKey id;
    EpochType epoch;
    std::vector<PeerInfo> peers;
    ConfigChangeType type;
    MetaServerIdType configChangeItem;
    MetaServerIdType oldOne = ::curvefs::mds::topology::UNINITIALIZE_ID;
};

struct CopySetInfo {
 public:
    CopySetInfo() = default;
    CopySetInfo(CopySetKey id, EpochType epoch, MetaServerIdType leader,
                const std::vector<PeerInfo> &peers,
                const ConfigChangeInfo &info) {
        this->id.first = id.first;
        this->id.second = id.second;
        this->epoch = epoch;
        this->leader = leader;
        this->peers = peers;
        this->configChangeInfo = info;
    }

    bool ContainPeer(MetaServerIdType id) const;
    bool HasCandidate() const;
    std::string CopySetInfoStr() const;

    CopySetKey id;
    EpochType epoch;
    MetaServerIdType leader;
    std::vector<PeerInfo> peers;
    PeerInfo candidatePeerInfo;
    ConfigChangeInfo configChangeInfo;
};

struct MetaServerInfo {
 public:
    MetaServerInfo() : startUpTime(0) {}
    MetaServerInfo(const PeerInfo &info, OnlineState state,
                   const MetaServerSpace &space, uint32_t copysetNum = 0,
                   uint32_t leaderNum = 0) : startUpTime(0), info(info),
                   state(state), copysetNum(copysetNum), leaderNum(leaderNum),
                   space(space) {}

    bool IsOnline() const;
    bool IsOffline() const;
    bool IsUnstable() const;
    bool IsHealthy() const;
    bool IsResourceOverload() const;
    double GetResourceUseRatioPercent() const;
    bool IsMetaserverResourceAvailable() const;

    uint64_t startUpTime;
    PeerInfo info;
    OnlineState state;
    uint32_t copysetNum;
    uint32_t leaderNum;
    mutable MetaServerSpace space;
};

/**
 * @brief TopoAdapter is the interface for providing topology info
 */
class TopoAdapter {
 public:
    virtual ~TopoAdapter() {}
    /**
     * @brief get pools
     *
     * @return pool list
     */
    virtual std::vector<PoolIdType> Getpools() = 0;

    /**
     * @brief Get_x_Info get info of specified x
     *
     * @param[in] id ID of x
     * @param[out] info of x
     *
     * @return true if succeeded, false if failed
     */
    virtual bool GetCopySetInfo(const CopySetKey &id, CopySetInfo *info) = 0;

    /**
     * @brief Get_x_Infos Get info of every available x
     *
     * @return x info list
     */
    virtual std::vector<CopySetInfo> GetCopySetInfos() = 0;

    /**
     * @brief Get_x_InfosIn_y_ Get info of x on specified y
     *
     * @param[in] id ID of y
     *
     * @return list of info of x
     */
    virtual std::vector<CopySetInfo> GetCopySetInfosInMetaServer(
        MetaServerIdType id) = 0;

    virtual std::vector<CopySetInfo> GetCopySetInfosInPool(PoolIdType id) = 0;

    /**
     * @brief GetMetaServerInfo get the specified metaserver info
     *
     * @param[in] id ID of the specified metaserver
     * @param[in] info information of the metaserver
     *
     * @return false if failed, true if succeeded
     */
    virtual bool GetMetaServerInfo(MetaServerIdType id,
                                   MetaServerInfo *info) = 0;

    /**
     * @brief GetMetaServerInfos get infos of all the metaservers
     *
     * @return metaservers info list
     */
    virtual std::vector<MetaServerInfo> GetMetaServerInfos() = 0;

    /**
     * @brief GetMetaServersInPool get all the metaservers in the
     *                                     specified pool
     *
     * @prarm[in] poolId the id of the pool
     *
     * @return the metaserver list of the pool
     */
    virtual std::vector<MetaServerInfo> GetMetaServersInPool(
        PoolIdType poolId) = 0;

    virtual std::vector<MetaServerInfo> GetMetaServersInZone(
        ZoneIdType zoneId) = 0;

    virtual std::list<ZoneIdType> GetZoneInPool(PoolIdType poolId) = 0;

    /**
     * @brief GetStandardZoneNumInPool get the standard zone num of the
     *                                        pool
     *
     * @return the zone num of the pool
     */
    virtual uint16_t GetStandardZoneNumInPool(PoolIdType id) = 0;

    /**
     * @brief GetStandardReplicaNumInPool get the standard replica
     *                                           num in pool
     *
     * @return the standard replica num
     */
    virtual uint16_t GetStandardReplicaNumInPool(PoolIdType id) = 0;

    /**
     * @brief CreateCopySetAtMetaServer Create copyset on metaserver msID.
     *                                   command add-configuration of Raft
     *                                   require Raft service on the node. thus
     *                                   before dispatching config changing
     *                                   command, metaserver should be
     *                                   informed to start Raft service of
     *                                   copyset.
     * @param[in] id Copyset key
     * @param[in] msID ID of metaserver to create copyset on
     *
     * @return false if failed, true if succeeded
     */
    virtual bool CreateCopySetAtMetaServer(CopySetKey id,
                                           MetaServerIdType msID) = 0;

    /**
     * @brief CopySetFromTopoToSchedule Transfer copyset info format from
     *                                  topology module to schedule module
     *
     * @param[in] origin Copyset info in format of Topology
     * @param[out] out Copyset info in format of Schedule
     *
     * @return false if failed, true if succeeded
     */
    virtual bool CopySetFromTopoToSchedule(
        const ::curvefs::mds::topology::CopySetInfo &origin,
        ::curvefs::mds::schedule::CopySetInfo *out) = 0;

    /**
     * @brief MetaServerFromTopoToSchedule Transfer metaserver info format from
     * //NOLINT
     *                                      topology module to schedule module
     *
     * @param[in] origin Metaserver info in format of Topology
     * @param[out] out Metaserver info in format of Schedule
     *
     * @return false if failed, true if succeeded
     */
    virtual bool MetaServerFromTopoToSchedule(
        const ::curvefs::mds::topology::MetaServer &origin,
        ::curvefs::mds::schedule::MetaServerInfo *out) = 0;

    virtual bool ChooseNewMetaServerForCopyset(
        PoolIdType poolId, const std::set<ZoneIdType> &excludeZones,
        const std::set<MetaServerIdType> &excludeMetaservers,
        MetaServerIdType *target) = 0;
};

// implementation of virtual class TopoAdapter
class TopoAdapterImpl : public TopoAdapter {
 public:
    TopoAdapterImpl() = default;
    explicit TopoAdapterImpl(std::shared_ptr<Topology> topo,
                             std::shared_ptr<TopologyManager> manager);

    std::vector<PoolIdType> Getpools() override;

    bool GetCopySetInfo(const CopySetKey &id, CopySetInfo *info) override;

    std::vector<CopySetInfo> GetCopySetInfos() override;

    std::vector<CopySetInfo> GetCopySetInfosInMetaServer(
        MetaServerIdType id) override;

    std::vector<CopySetInfo> GetCopySetInfosInPool(PoolIdType id) override;

    bool GetMetaServerInfo(MetaServerIdType id, MetaServerInfo *info) override;

    std::vector<MetaServerInfo> GetMetaServerInfos() override;

    std::vector<MetaServerInfo> GetMetaServersInPool(PoolIdType lid) override;

    std::vector<MetaServerInfo> GetMetaServersInZone(
        ZoneIdType zoneId) override;

    std::list<ZoneIdType> GetZoneInPool(PoolIdType poolId) override;

    uint16_t GetStandardZoneNumInPool(PoolIdType id) override;

    uint16_t GetStandardReplicaNumInPool(PoolIdType id) override;

    bool CreateCopySetAtMetaServer(CopySetKey id,
                                   MetaServerIdType msID) override;

    bool CopySetFromTopoToSchedule(
        const ::curvefs::mds::topology::CopySetInfo &origin,
        ::curvefs::mds::schedule::CopySetInfo *out) override;

    bool MetaServerFromTopoToSchedule(
        const ::curvefs::mds::topology::MetaServer &origin,
        ::curvefs::mds::schedule::MetaServerInfo *out) override;

    bool ChooseNewMetaServerForCopyset(
        PoolIdType poolId, const std::set<ZoneIdType> &excludeZones,
        const std::set<MetaServerIdType> &excludeMetaservers,
        MetaServerIdType *target) override;

 private:
    bool GetPeerInfo(MetaServerIdType id, PeerInfo *peerInfo);

 private:
    std::shared_ptr<Topology> topo_;
    std::shared_ptr<TopologyManager> topoManager_;
};
}  // namespace schedule
}  // namespace mds
}  // namespace curvefs

#endif  // CURVEFS_SRC_MDS_SCHEDULE_TOPOADAPTER_H_
