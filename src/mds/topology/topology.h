/*
 * Project: curve
 * Created Date: Fri Aug 17 2018
 * Author: xuchaojie
 * Copyright (c) 2018 netease
 */
#ifndef CURVE_SRC_MDS_TOPOLOGY_TOPOLOGY_H_
#define CURVE_SRC_MDS_TOPOLOGY_TOPOLOGY_H_

#include <unordered_map>
#include <string>
#include <list>
#include <vector>
#include <map>

#include "proto/topology.pb.h"
#include "src/mds/common/topology_define.h"
#include "src/mds/topology/topology_item.h"
#include "src/mds/topology/topology_id_generator.h"
#include "src/mds/topology/topology_token_generator.h"
#include "src/mds/topology/topology_storge.h"

// TODO(xuchaojie): 修改使用读写锁
#include <mutex>   // NOLINT
namespace curve {
namespace common {
using mutex = std::mutex;
}
}

namespace curve {
namespace mds {
namespace topology {

class Topology {
 public:
  Topology() {}
  virtual ~Topology() {}
  virtual int init() = 0;

// allocate id & token
  virtual PoolIdType AllocateLogicalPoolId() = 0;
  virtual PoolIdType AllocatePhysicalPoolId() = 0;
  virtual ZoneIdType AllocateZoneId() = 0;
  virtual ServerIdType AllocateServerId() = 0;
  virtual ChunkServerIdType AllocateChunkServerId() = 0;
  virtual CopySetIdType AllocateCopySetId(PoolIdType logicalPoolId) = 0;

  virtual std::string AllocateToken() = 0;

// add
  virtual int AddLogicalPool(const LogicalPool &data) = 0;
  virtual int AddPhysicalPool(const PhysicalPool &data) = 0;
  virtual int AddZone(const Zone &data) = 0;
  virtual int AddServer(const Server &data) = 0;
  virtual int AddChunkServer(const ChunkServer &data) = 0;
  virtual int AddCopySet(const CopySetInfo &data) = 0;
  virtual int AddCopySetList(const std::vector<CopySetInfo> &copysets) = 0;

// remove
  virtual int RemoveLogicalPool(PoolIdType id) = 0;
  virtual int RemovePhysicalPool(PoolIdType id) = 0;
  virtual int RemoveZone(ZoneIdType id) = 0;
  virtual int RemoveServer(ServerIdType id) = 0;
  virtual int RemoveChunkServer(ChunkServerIdType id) = 0;
  virtual int RemoveCopySet(CopySetKey key) = 0;

// update
  virtual int UpdateLogicalPool(const LogicalPool &data) = 0;
  virtual int UpdatePhysicalPool(const PhysicalPool &data) = 0;
  virtual int UpdateZone(const Zone &data) = 0;
  virtual int UpdateServer(const Server &data) = 0;
  // 更新内存并持久化全部数据
  virtual int UpdateChunkServer(const ChunkServer &data) = 0;
  // 更新内存，定期持久化数据
  virtual int UpdateChunkServerState(const ChunkServerState &state,
                                     ChunkServerIdType id) = 0;
  virtual int UpdateCopySet(const CopySetInfo &data) = 0;

// find
  virtual PoolIdType FindLogicalPool(
      const std::string &logicalPoolName,
      const std::string &physicalPoolName) const = 0;
  virtual PoolIdType FindPhysicalPool(
      const std::string &physicalPoolName) const = 0;
  virtual ZoneIdType FindZone(const std::string &zoneName,
                              const std::string &physicalPoolName) const = 0;
  virtual ZoneIdType FindZone(const std::string &zoneName,
                              PoolIdType physicalpoolid) const = 0;
  virtual ServerIdType FindServerByHostName(
      const std::string &hostName) const = 0;
  virtual ServerIdType FindServerByHostIp(
      const std::string &hostIp) const = 0;
  virtual ChunkServerIdType FindChunkServer(const std::string &hostIp,
                                            uint32_t port) const = 0;

// get
  virtual bool GetLogicalPool(PoolIdType poolId,
                              LogicalPool *out) const = 0;
  virtual bool GetPhysicalPool(PoolIdType poolId,
                               PhysicalPool *out) const = 0;
  virtual bool GetZone(ZoneIdType zoneId,
                       Zone *out) const = 0;
  virtual bool GetServer(ServerIdType serverId,
                         Server *out) const = 0;
  virtual bool GetChunkServer(ChunkServerIdType chunkserverId,
                              ChunkServer *out) const = 0;
  // TODO(chaojie): +const

  virtual bool GetCopySet(CopySetKey key, CopySetInfo *out) const = 0;

  virtual bool GetLogicalPool(const std::string &logicalPoolName,
                              const std::string &physicalPoolName,
                              LogicalPool *out) const = 0;

  virtual bool GetPhysicalPool(const std::string &physicalPoolName,
                               PhysicalPool *out) const = 0;

  virtual bool GetZone(const std::string &zoneName,
                       const std::string &physicalPoolName,
                       Zone *out) const = 0;

  virtual bool GetZone(const std::string &zoneName,
                       PoolIdType physicalPoolId,
                       Zone *out) const = 0;

  virtual bool GetServerByHostName(const std::string &hostName,
                                   Server *out) const = 0;

  virtual bool GetServerByHostIp(const std::string &hostIp,
                                 Server *out) const = 0;

  virtual bool GetChunkServer(const std::string &hostIp,
                              uint32_t port,
                              ChunkServer *out) const = 0;

// getlist
  virtual std::list<ChunkServerIdType> GetChunkServerInCluster() const = 0;
  virtual std::list<ServerIdType> GetServerInCluster() const = 0;
  virtual std::list<ZoneIdType> GetZoneInCluster() const = 0;
  virtual std::list<PoolIdType> GetPhysicalPoolInCluster() const = 0;
  virtual std::list<PoolIdType> GetLogicalPoolInCluster() const = 0;

  virtual std::list<ChunkServerIdType> GetChunkServerInServer(
      ServerIdType id) const = 0;
  virtual std::list<ChunkServerIdType> GetChunkServerInZone(
      ZoneIdType id) const = 0;
  virtual std::list<ChunkServerIdType> GetChunkServerInPhysicalPool(
      PoolIdType id) const = 0;

  virtual std::list<ServerIdType> GetServerInZone(ZoneIdType id) const = 0;
  virtual std::list<ServerIdType> GetServerInPhysicalPool(
      PoolIdType id) const = 0;

  virtual std::list<ZoneIdType> GetZoneInPhysicalPool(
      PoolIdType id) const = 0;
  virtual std::list<PoolIdType> GetLogicalPoolInPhysicalPool(
      PoolIdType id) const = 0;

  virtual std::list<ChunkServerIdType> GetChunkServerInLogicalPool(
      PoolIdType id) const = 0;
  virtual std::list<ServerIdType> GetServerInLogicalPool(
      PoolIdType id) const = 0;
  virtual std::list<ZoneIdType> GetZoneInLogicalPool(PoolIdType id) const = 0;

  virtual std::vector<CopySetIdType> GetCopySetsInLogicalPool(
      PoolIdType logicalPoolId) const = 0;

  // TODO(chaojie-schedule): add
  virtual std::vector<CopySetKey> GetCopySetsInCluster() const = 0;
  virtual std::vector<CopySetKey>
  GetCopySetsInChunkServer(ChunkServerIdType id) const = 0;
};

class TopologyImpl : public Topology {
 public:
  TopologyImpl(std::shared_ptr<TopologyIdGenerator> idGenerator,
               std::shared_ptr<TopologyTokenGenerator> tokenGenerator,
               std::shared_ptr<TopologyStorage> storage)
      : idGenerator_(idGenerator),
        tokenGenerator_(tokenGenerator),
        storage_(storage) {
  }

  ~TopologyImpl() {}

  //从持久化设备加载数据
  int init();

// allocate id & token

  PoolIdType AllocateLogicalPoolId();
  PoolIdType AllocatePhysicalPoolId();
  ZoneIdType AllocateZoneId();
  ServerIdType AllocateServerId();
  ChunkServerIdType AllocateChunkServerId();
  CopySetIdType AllocateCopySetId(PoolIdType logicalPoolId);

  std::string AllocateToken();

  // add
  int AddLogicalPool(const LogicalPool &data);
  int AddPhysicalPool(const PhysicalPool &data);
  int AddZone(const Zone &data);
  int AddServer(const Server &data);
  int AddChunkServer(const ChunkServer &data);
  int AddCopySet(const CopySetInfo &data);
  int AddCopySetList(const std::vector<CopySetInfo> &copysets);

// remove
  int RemoveLogicalPool(PoolIdType id);
  int RemovePhysicalPool(PoolIdType id);
  int RemoveZone(ZoneIdType id);
  int RemoveServer(ServerIdType id);
  int RemoveChunkServer(ChunkServerIdType id);
  int RemoveCopySet(CopySetKey key);

// update
  int UpdateLogicalPool(const LogicalPool &data);
  int UpdatePhysicalPool(const PhysicalPool &data);
  int UpdateZone(const Zone &data);
  int UpdateServer(const Server &data);
  // 更新内存并持久化全部数据
  int UpdateChunkServer(const ChunkServer &data);
  // 更新内存，定期持久化数据
  int UpdateChunkServerState(const ChunkServerState &state,
                             ChunkServerIdType id);
  int UpdateCopySet(const CopySetInfo &data);

// find
  PoolIdType FindLogicalPool(const std::string &logicalPoolName,
                             const std::string &physicalPoolName) const;
  PoolIdType FindPhysicalPool(const std::string &physicalPoolName) const;
  ZoneIdType FindZone(const std::string &zoneName,
                      const std::string &physicalPoolName) const;
  ZoneIdType FindZone(const std::string &zoneName,
                      PoolIdType physicalpoolid) const;
  ServerIdType FindServerByHostName(const std::string &hostName) const;
  ServerIdType FindServerByHostIp(const std::string &hostIp) const;
  ChunkServerIdType FindChunkServer(const std::string &hostIp,
                                    uint32_t port) const;

// get
  bool GetLogicalPool(PoolIdType poolId, LogicalPool *out) const;
  bool GetPhysicalPool(PoolIdType poolId, PhysicalPool *out) const;
  bool GetZone(ZoneIdType zoneId, Zone *out) const;
  bool GetServer(ServerIdType serverId, Server *out) const;
  bool GetChunkServer(ChunkServerIdType chunkserverId,
                      ChunkServer *out) const;

  // TODO(chaojie-schedule): +const
  bool GetCopySet(CopySetKey key, CopySetInfo *out) const;

  bool GetLogicalPool(const std::string &logicalPoolName,
                      const std::string &physicalPoolName,
                      LogicalPool *out) const {
      return GetLogicalPool(
          FindLogicalPool(logicalPoolName, physicalPoolName), out);
  }
  bool GetPhysicalPool(const std::string &physicalPoolName,
                       PhysicalPool *out) const {
      return GetPhysicalPool(FindPhysicalPool(physicalPoolName), out);
  }
  bool GetZone(const std::string &zoneName,
               const std::string &physicalPoolName,
               Zone *out) const {
      return GetZone(FindZone(zoneName, physicalPoolName), out);
  }
  bool GetZone(const std::string &zoneName,
               PoolIdType physicalPoolId,
               Zone *out) const {
      return GetZone(FindZone(zoneName, physicalPoolId), out);
  }
  bool GetServerByHostName(const std::string &hostName,
                           Server *out) const {
      return GetServer(FindServerByHostName(hostName), out);
  }
  bool GetServerByHostIp(const std::string &hostIp,
                         Server *out) const {
      return GetServer(FindServerByHostIp(hostIp), out);
  }
  bool GetChunkServer(const std::string &hostIp,
                      uint32_t port,
                      ChunkServer *out) const {
      return GetChunkServer(FindChunkServer(hostIp, port), out);
  }

// getlist
  std::list<ChunkServerIdType> GetChunkServerInCluster() const;
  std::list<ServerIdType> GetServerInCluster() const;
  std::list<ZoneIdType> GetZoneInCluster() const;
  std::list<PoolIdType> GetPhysicalPoolInCluster() const;
  std::list<PoolIdType> GetLogicalPoolInCluster() const;

  std::list<ChunkServerIdType> GetChunkServerInServer(ServerIdType id) const;
  std::list<ChunkServerIdType> GetChunkServerInZone(ZoneIdType id) const;
  std::list<ChunkServerIdType> GetChunkServerInPhysicalPool(
      PoolIdType id) const;

  std::list<ServerIdType> GetServerInZone(ZoneIdType id) const;
  std::list<ServerIdType> GetServerInPhysicalPool(PoolIdType id) const;

  std::list<ZoneIdType> GetZoneInPhysicalPool(PoolIdType id) const;
  std::list<PoolIdType> GetLogicalPoolInPhysicalPool(PoolIdType id) const;

  std::list<ChunkServerIdType> GetChunkServerInLogicalPool(
      PoolIdType id) const;
  std::list<ServerIdType> GetServerInLogicalPool(PoolIdType id) const;
  std::list<ZoneIdType> GetZoneInLogicalPool(PoolIdType id) const;

  std::vector<CopySetIdType> GetCopySetsInLogicalPool(
      PoolIdType logicalPoolId) const;
  std::vector<CopySetKey> GetCopySetsInCluster() const;
  std::vector<CopySetKey> GetCopySetsInChunkServer(ChunkServerIdType id) const;

 private:
  std::unordered_map<PoolIdType, LogicalPool> logicalPoolMap_;
  std::unordered_map<PoolIdType, PhysicalPool> physicalPoolMap_;
  std::unordered_map<ZoneIdType, Zone> zoneMap_;
  std::unordered_map<ServerIdType, Server> serverMap_;
  std::unordered_map<ChunkServerIdType, ChunkServer> chunkServerMap_;

  std::map<CopySetKey, CopySetInfo> copySetMap_;

  std::shared_ptr<TopologyIdGenerator> idGenerator_;
  std::shared_ptr<TopologyTokenGenerator> tokenGenerator_;
  std::shared_ptr<TopologyStorage> storage_;

  //以如下声明的顺序获取锁，防止死锁
  mutable curve::common::mutex logicalPoolMutex_;
  mutable curve::common::mutex physicalPoolMutex_;
  mutable curve::common::mutex zoneMutex_;
  mutable curve::common::mutex serverMutex_;
  mutable curve::common::mutex chunkServerMutex_;
  mutable curve::common::mutex copySetMutex_;
};
}  // namespace topology
}  // namespace mds
}  // namespace curve


#endif  // CURVE_SRC_MDS_TOPOLOGY_TOPOLOGY_H_
