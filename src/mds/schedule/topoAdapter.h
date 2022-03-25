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
 * Created Date: Wed Nov 28 2018
 * Author: lixiaocui
 */

#ifndef SRC_MDS_SCHEDULE_TOPOADAPTER_H_
#define SRC_MDS_SCHEDULE_TOPOADAPTER_H_
#include <cstdint>
#include <vector>
#include <string>
#include <map>
#include <memory>
#include "src/mds/topology/topology.h"
#include "src/mds/topology/topology_service_manager.h"
#include "src/mds/topology/topology_stat.h"
#include "src/mds/common/mds_define.h"
#include "proto/topology.pb.h"
#include "proto/heartbeat.pb.h"

namespace curve {
namespace mds {
namespace schedule {
using ::curve::mds::topology::ChunkServerIdType;
using ::curve::mds::topology::PoolIdType;
using ::curve::mds::topology::ServerIdType;
using ::curve::mds::topology::ZoneIdType;
using ::curve::mds::topology::EpochType;
using ::curve::mds::topology::LastScanSecType;
using ::curve::mds::topology::CopySetKey;
using ::curve::mds::topology::CopySetIdType;
using ::curve::mds::topology::OnlineState;
using ::curve::mds::topology::Topology;
using ::curve::mds::topology::TopologyServiceManager;
using ::curve::mds::topology::TopologyStat;
using ::curve::mds::topology::ChunkServer;
using ::curve::mds::topology::Server;
using ::curve::mds::topology::LogicalPool;
using ::curve::mds::topology::DiskState;
using ::curve::mds::topology::ChunkServerStatus;
using ::curve::mds::topology::ChunkServerStat;
using ::curve::mds::topology::UNINTIALIZE_ID;
using ::curve::mds::heartbeat::ConfigChangeInfo;
using ::curve::mds::heartbeat::ConfigChangeType;
using ::curve::mds::heartbeat::CopysetStatistics;
using ::curve::mds::heartbeat::ChunkServerStatisticInfo;

struct PeerInfo {
 public:
    PeerInfo() : id(UNINTIALIZE_ID), port(0) {}
    PeerInfo(ChunkServerIdType id, ZoneIdType zoneId, ServerIdType sid,
            const std::string &ip,
            uint32_t port);
    ChunkServerIdType id;
    ZoneIdType zoneId;
    ServerIdType serverId;
    std::string ip;
    uint32_t port;

    bool operator == (const PeerInfo& other) const {
        return id == other.id &&
               zoneId == other.zoneId &&
               serverId == other.serverId &&
               ip == other.ip &&
               port == other.port;
    }
};

struct CopySetConf {
 public:
    CopySetConf() = default;
    CopySetConf(const CopySetKey &key, EpochType epoch,
        const std::vector<PeerInfo> &peers, ConfigChangeType type,
        ChunkServerIdType item,
        ChunkServerIdType oldOne = ::curve::mds::topology::UNINTIALIZE_ID);

    CopySetKey id;
    EpochType epoch;
    std::vector<PeerInfo> peers;
    ConfigChangeType type;
    ChunkServerIdType configChangeItem;
    ChunkServerIdType oldOne = ::curve::mds::topology::UNINTIALIZE_ID;
};

struct CopySetInfo {
    CopySetInfo() : logicalPoolWork(false) {}

    // Only called for test
    CopySetInfo(CopySetKey id,
                EpochType epoch,
                ChunkServerIdType leader,
                const std::vector<PeerInfo> &peers,
                const ConfigChangeInfo &info,
                const CopysetStatistics &statistics) : logicalPoolWork(false) {
        this->id.first = id.first;
        this->id.second = id.second;
        this->epoch = epoch;
        this->leader = leader;
        this->peers = peers;
        this->scaning = false;
        this->lastScanSec = 0;
        this->configChangeInfo = info;
        this->statisticsInfo = statistics;
    }

    ~CopySetInfo();

    bool ContainPeer(ChunkServerIdType id) const;
    bool HasCandidate() const;
    std::string CopySetInfoStr() const;

    CopySetKey id;
    // during the initialization, the logical pool will be available after all
    // copyset are created, and will be unavailable during the creation
    bool logicalPoolWork;
    EpochType epoch;
    ChunkServerIdType leader;
    std::vector<PeerInfo> peers;

    // whether the current copyset is in scaning
    bool scaning;

    // timestamp for last scan (seconds)
    LastScanSecType lastScanSec;

    // TODO(chaojie): add candidateInfo to Topology
    PeerInfo candidatePeerInfo;
    ConfigChangeInfo configChangeInfo;
    CopysetStatistics statisticsInfo;
};

struct ChunkServerInfo {
 public:
    ChunkServerInfo() :
        leaderCount(0), diskCapacity(0), diskUsed(0), startUpTime(0) {}
    ChunkServerInfo(const PeerInfo &info, OnlineState state,
                    DiskState diskState, ChunkServerStatus status,
                    uint32_t leaderCount, uint64_t capacity, uint64_t used,
                    const ChunkServerStatisticInfo &statisticInfo);

    bool IsOnline() const;
    bool IsOffline()const;
    bool IsUnstable()const;
    bool IsPendding()const;
    bool IsHealthy()const;

    PeerInfo info;
    uint64_t startUpTime;
    OnlineState state;
    DiskState diskState;
    ChunkServerStatus status;

    uint32_t leaderCount;
    uint64_t diskCapacity;
    uint64_t diskUsed;
    ChunkServerStatisticInfo statisticInfo;
};

/**
 * @brief TopoAdapter is the interface for providing topology info
 */
class TopoAdapter {
 public:
    /**
     * @brief get logical pools
     *
     * @return logical pool list
     */
    virtual std::vector<PoolIdType> GetLogicalpools() = 0;

    /**
     * @brief Get logical pool for specify logical pool id
     * @return logical pool
     */
    virtual bool GetLogicalPool(
        PoolIdType id,
        ::curve::mds::topology::LogicalPool* lpool) = 0;

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
    virtual std::vector<CopySetInfo> GetCopySetInfosInChunkServer(
        ChunkServerIdType id) = 0;

    virtual std::vector<CopySetInfo> GetCopySetInfosInLogicalPool(
        PoolIdType lid) = 0;

    /**
     * @brief GetChunkServerInfo get the specified chunkserver info
     *
     * @param[in] id ID of the specified chunkserver
     * @param[in] info information of the chunkserver
     *
     * @return false if failed, true if succeeded
     */
    virtual bool GetChunkServerInfo(
        ChunkServerIdType id, ChunkServerInfo *info) = 0;

    /**
     * @brief GetChunkServerInfos get infos of all the chunkservers
     *
     * @return chunkservers info list
     */
    virtual std::vector<ChunkServerInfo> GetChunkServerInfos() = 0;

    /**
     * @brief GetChunkServersInLogicalPool get all the chunkservers in the
     *                                     specified logical pool
     *
     * @prarm[in] lid the id of the logical pool
     *
     * @return the chunkserver list of the logocal pool
     */
    virtual std::vector<ChunkServerInfo> GetChunkServersInLogicalPool(
        PoolIdType lid) = 0;

    /**
     * @brief GetStandardZoneNumInLogicalPool get the standard zone num of the
     *                                        logical pool
     *
     * @return the zone num of the logical pool
     */
    virtual int GetStandardZoneNumInLogicalPool(PoolIdType id) = 0;

    /**
     * @brief GetAvgScatterWidthInLogicalPool
     *        get the average scatter-width of chunkservers in the logical pool
     *
     * @ param[in] id ID of logical pool
     *
     * @return the average scatter-width of chunkservers
     */
    virtual int GetAvgScatterWidthInLogicalPool(PoolIdType id) = 0;

    /**
     * @brief GetStandardReplicaNumInLogicalPool get the standard replica
     *                                           num in logical pool
     *
     * @return the standard replica num
     */
    virtual int GetStandardReplicaNumInLogicalPool(PoolIdType id) = 0;

    /**
     * @brief CreateCopySetAtChunkServer Create copyset on chunkserver csID.
     *                                   command add-configuration of Raft
     *                                   require Raft service on the node. thus
     *                                   before dispatching config changing
     *                                   command, chunkserver should be
     *                                   informed to start Raft service of
     *                                   copyset.
     * @param[in] id Copyset key
     * @param[in] csID ID of chunkserver to create copyset on
     *
     * @return false if failed, true if succeeded
     */
    virtual bool CreateCopySetAtChunkServer(
        CopySetKey id, ChunkServerIdType csID) = 0;

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
        const ::curve::mds::topology::CopySetInfo &origin,
        ::curve::mds::schedule::CopySetInfo *out) = 0;

    /**
     * @brief ChunkServerFromTopoToSchedule Transfer chunkserver info format from //NOLINT
     *                                      topology module to schedule module
     *
     * @param[in] origin Chunkserver info in format of Topology
     * @param[out] out Chunkserver info in format of Schedule
     *
     * @return false if failed, true if succeeded
     */
    virtual bool ChunkServerFromTopoToSchedule(
        const ::curve::mds::topology::ChunkServer &origin,
        ::curve::mds::schedule::ChunkServerInfo *out) = 0;

    /**
     * @brief GetChunkServerScatterMap Get scatter-width map of
     *                                 specified chunkserver
     *
     * @param[in] cs Chunkserver ID specified
     * @param[out] out Scatter-width map, in this map, key is the other
     *                 chunkservers in copyset on the specified chunkserver.
     *                 and value is the number of copyset that both the key and
     *                 the specified chunkserver have. here's an example below.
     *  e.g. chunkserver1: copyset1{1,2,3} copyset2{2,3,4} copyset3{4,5,6}
     *       scatter-width map:
     *       {{2, 2}, {3, 2}, {4, 2}, {5, 1}, {6, 1}}
     *       chunkserver2 contains copyset1 and copyset2
     *       chunkserver3 contains copyset1 and copyset2
     *       chunkserver4 contains copyset2 and copyset3
     *       and so on
     */
    virtual void GetChunkServerScatterMap(const ChunkServerIdType &cs,
        std::map<ChunkServerIdType, int> *out) = 0;
};

// implementation of virtual class TopoAdapter
class TopoAdapterImpl : public TopoAdapter {
 public:
    TopoAdapterImpl() = default;
    explicit TopoAdapterImpl(std::shared_ptr<Topology> topo,
                             std::shared_ptr<TopologyServiceManager> manager,
                             std::shared_ptr<TopologyStat> stat);

    std::vector<PoolIdType> GetLogicalpools() override;

    bool GetLogicalPool(
        PoolIdType id,
        ::curve::mds::topology::LogicalPool* lpool) override;

    bool GetCopySetInfo(
        const CopySetKey &id, CopySetInfo *info) override;

    std::vector<CopySetInfo> GetCopySetInfos() override;

    std::vector<CopySetInfo> GetCopySetInfosInChunkServer(
        ChunkServerIdType id) override;

    std::vector<CopySetInfo> GetCopySetInfosInLogicalPool(
        PoolIdType lid) override;

    bool GetChunkServerInfo(
        ChunkServerIdType id, ChunkServerInfo *info) override;

    std::vector<ChunkServerInfo> GetChunkServerInfos() override;

    std::vector<ChunkServerInfo> GetChunkServersInLogicalPool(
        PoolIdType lid) override;

    int GetStandardZoneNumInLogicalPool(PoolIdType id) override;

    int GetStandardReplicaNumInLogicalPool(PoolIdType id) override;

    int GetAvgScatterWidthInLogicalPool(PoolIdType id) override;

    bool CreateCopySetAtChunkServer(
        CopySetKey id, ChunkServerIdType csID) override;

    bool CopySetFromTopoToSchedule(
        const ::curve::mds::topology::CopySetInfo &origin,
        ::curve::mds::schedule::CopySetInfo *out) override;

    bool ChunkServerFromTopoToSchedule(
        const ::curve::mds::topology::ChunkServer &origin,
        ::curve::mds::schedule::ChunkServerInfo *out) override;

    void GetChunkServerScatterMap(const ChunkServerIdType &cs,
        std::map<ChunkServerIdType, int> *out) override;

 private:
    bool GetPeerInfo(ChunkServerIdType id, PeerInfo *peerInfo);

 private:
    std::shared_ptr<Topology> topo_;
    std::shared_ptr<TopologyServiceManager> topoServiceManager_;
    std::shared_ptr<TopologyStat> topoStat_;
};
}  // namespace schedule
}  // namespace mds
}  // namespace curve

#endif  // SRC_MDS_SCHEDULE_TOPOADAPTER_H_
