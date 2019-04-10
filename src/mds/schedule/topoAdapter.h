/*
 * Project: curve
 * Created Date: Wed Nov 28 2018
 * Author: lixiaocui
 * Copyright (c) 2018 netease
 */

#ifndef SRC_MDS_SCHEDULE_TOPOADAPTER_H_
#define SRC_MDS_SCHEDULE_TOPOADAPTER_H_
#include <cstdint>
#include <vector>
#include <string>
#include <map>
#include "src/mds/topology/topology.h"
#include "src/mds/topology/topology_service_manager.h"
// #include "src/mds/topology/topology_stat.h"
#include "src/mds/common/mds_define.h"
#include "proto/topology.pb.h"
#include "proto/heartbeat.pb.h"

using ::curve::mds::topology::ChunkServerIdType;
using ::curve::mds::topology::PoolIdType;
using ::curve::mds::topology::ServerIdType;
using ::curve::mds::topology::ZoneIdType;
using ::curve::mds::topology::EpochType;
using ::curve::mds::topology::CopySetKey;
using ::curve::mds::topology::CopySetIdType;
using ::curve::mds::topology::OnlineState;
using ::curve::mds::topology::Topology;
using ::curve::mds::topology::TopologyServiceManager;
// using ::curve::mds::topology::TopologyStatImpl;
using ::curve::mds::topology::ChunkServer;
using ::curve::mds::topology::Server;
using ::curve::mds::topology::LogicalPool;
using ::curve::mds::topology::DiskState;
using ::curve::mds::heartbeat::ConfigChangeInfo;
using ::curve::mds::heartbeat::ConfigChangeType;
using ::curve::mds::heartbeat::CopysetStatistics;
using ::curve::mds::heartbeat::ChunkServerStatisticInfo;

namespace curve {
namespace mds {
namespace schedule {

struct PeerInfo {
 public:
  PeerInfo() : port(0) {}
  PeerInfo(ChunkServerIdType id, ZoneIdType zoneId, ServerIdType sid,
           const std::string &ip, uint32_t port);

  ChunkServerIdType id;
  ZoneIdType zoneId;
  ServerIdType serverId;
  std::string ip;
  uint32_t port;
};

struct CopySetConf {
 public:
  CopySetConf() = default;
  CopySetConf(const CopySetKey &key, EpochType epoch,
              const std::vector<PeerInfo> &peers, ConfigChangeType type,
              ChunkServerIdType item);

  CopySetKey id;
  EpochType epoch;
  std::vector<PeerInfo> peers;
  ConfigChangeType type;
  ChunkServerIdType configChangeItem;
};

struct CopySetInfo {
 public:
  CopySetInfo() = default;
  CopySetInfo(CopySetKey id,
              EpochType epoch,
              ChunkServerIdType leader,
              const std::vector<PeerInfo> &peers,
              const ConfigChangeInfo &info,
              const CopysetStatistics &statistics);
  CopySetInfo(const CopySetInfo &in);
  ~CopySetInfo();

  bool ContainPeer(ChunkServerIdType id) const;

  CopySetKey id;
  EpochType epoch;
  ChunkServerIdType leader;
  std::vector<PeerInfo> peers;

  // TODO(chaojie): candidateInfo 增加到topology中
  PeerInfo candidatePeerInfo;
  ConfigChangeInfo configChangeInfo;
  CopysetStatistics statisticsInfo;
};

struct ChunkServerInfo {
 public:
  ChunkServerInfo() :
    leaderCount(0), diskCapacity(0), diskUsed(0), stateUpdateTime(0) {}
  ChunkServerInfo(const PeerInfo &info, OnlineState state, uint32_t leaderCount,
                  uint64_t capacity, uint64_t used, uint64_t time,
                  const ChunkServerStatisticInfo &statisticInfo);

  bool IsOffline();

  PeerInfo info;
  OnlineState state;
  uint32_t leaderCount;
  uint64_t diskCapacity;
  uint64_t diskUsed;
  uint64_t stateUpdateTime;
  ChunkServerStatisticInfo statisticInfo;
};

/**
 * @brief TopoAdapter is interface, provide information about topology
 */
class TopoAdapter {
 public:
  virtual bool GetCopySetInfo(const CopySetKey &id, CopySetInfo *info) = 0;

  virtual std::vector<CopySetInfo> GetCopySetInfos() = 0;

  virtual bool GetChunkServerInfo(
      ChunkServerIdType id, ChunkServerInfo *info) = 0;

  virtual std::vector<ChunkServerInfo> GetChunkServerInfos() = 0;

  virtual int GetStandardZoneNumInLogicalPool(PoolIdType id) = 0;

  virtual int GetStandardReplicaNumInLogicalPool(PoolIdType id) = 0;

  /**
   * @brief choose a chunkServer to replace old one
   */
  virtual ChunkServerIdType SelectBestPlacementChunkServer(
      const CopySetInfo &copySetInfo, ChunkServerIdType oldPeer) = 0;

  /**
   * @brief choose a chunkServer to remove
   * select remove replica according to the following order
   * 1. zone limit
   * 2. offline one
   * 3. max capacity
   */
  virtual ChunkServerIdType SelectRedundantReplicaToRemove(
      const CopySetInfo &copySetInfo) = 0;

  /**
   * @brief create copySet at add-peer. Raft add-configuration need to the add
   * one start raft-service first, so before send configuration mds need to tell
   * the add one to start raft service.
   */
  virtual bool CreateCopySetAtChunkServer(
      CopySetKey id, ChunkServerIdType csID) = 0;

  virtual bool CopySetFromTopoToSchedule(
      const ::curve::mds::topology::CopySetInfo &origin,
      ::curve::mds::schedule::CopySetInfo *out) = 0;

  virtual bool ChunkServerFromTopoToSchedule(
      const ::curve::mds::topology::ChunkServer &origin,
      ::curve::mds::schedule::ChunkServerInfo *out) = 0;
};

// adapter实现
class TopoAdapterImpl : public TopoAdapter {
 public:
  TopoAdapterImpl() = default;
  explicit TopoAdapterImpl(std::shared_ptr<Topology> topo,
                           std::shared_ptr<TopologyServiceManager> manager);

  bool GetCopySetInfo(
      const CopySetKey &id, CopySetInfo *info) override;

  std::vector<CopySetInfo> GetCopySetInfos() override;

  bool GetChunkServerInfo(
      ChunkServerIdType id, ChunkServerInfo *info) override;

  std::vector<ChunkServerInfo> GetChunkServerInfos() override;

  int GetStandardZoneNumInLogicalPool(PoolIdType id) override;

  int GetStandardReplicaNumInLogicalPool(PoolIdType id) override;

  ChunkServerIdType SelectBestPlacementChunkServer(
      const CopySetInfo &copySetInfo, ChunkServerIdType oldPeer) override;

  ChunkServerIdType SelectRedundantReplicaToRemove(
      const CopySetInfo &copySetInfo) override;

  bool CreateCopySetAtChunkServer(
      CopySetKey id, ChunkServerIdType csID) override;

  bool CopySetFromTopoToSchedule(
      const ::curve::mds::topology::CopySetInfo &origin,
      ::curve::mds::schedule::CopySetInfo *out) override;

  bool ChunkServerFromTopoToSchedule(
      const ::curve::mds::topology::ChunkServer &origin,
      ::curve::mds::schedule::ChunkServerInfo *out) override;

 private:
  bool IsChunkServerHealthy(const ChunkServer &cs);
  // TODO(lixiaocui): consider capacity later
  // bool IsChunkServerCapacitySaturated(const ChunkServer &cs);

  int GetChunkServerScatterMap(const ChunkServer &cs,
                               std::map<ChunkServerIdType, bool> *out);

  bool GetPeerInfo(ChunkServerIdType id, PeerInfo *peerInfo);

 private:
  std::shared_ptr<Topology> topo_;
  std::shared_ptr<TopologyServiceManager> topoServiceManager_;
  // TODO(lixiaocui): 把topologyStat加进来
  // std::shared_ptr<TopologyStatImpl> topoStat_;
};
}  // namespace schedule
}  // namespace mds
}  // namespace curve

#endif  // SRC_MDS_SCHEDULE_TOPOADAPTER_H_
