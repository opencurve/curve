/*
 * Project: curve
 * Created Date: Wed Aug 22 2018
 * Author: xuchaojie
 * Copyright (c) 2018 netease
 */

#ifndef CURVE_SRC_MDS_TOPOLOGY_TOPOLOGY_STORGE_H_
#define CURVE_SRC_MDS_TOPOLOGY_TOPOLOGY_STORGE_H_

#include <list>
#include <string>
#include <unordered_map>
#include <map>

#include "src/mds/topology/topology_item.h"
#include "src/repo/repo.h"

namespace curve {
namespace mds {
namespace topology {

class TopologyStorage {
 public:
  TopologyStorage() {}
  virtual ~TopologyStorage() {}

  virtual bool init(const std::string &dbName,
                    const std::string &user,
                    const std::string &url,
                    const std::string &password) = 0;

  virtual bool LoadLogicalPool(
      std::unordered_map<PoolIdType, LogicalPool> *logicalPoolMap,
      PoolIdType *maxLogicalPoolId) = 0;
  virtual bool LoadPhysicalPool(
      std::unordered_map<PoolIdType, PhysicalPool> *physicalPoolMap,
      PoolIdType *maxPhysicalPoolId) = 0;
  virtual bool LoadZone(
      std::unordered_map<ZoneIdType, Zone> *zoneMap,
      ZoneIdType *maxZoneId) = 0;
  virtual bool LoadServer(
      std::unordered_map<ServerIdType, Server> *serverMap,
      ServerIdType *maxServerId) = 0;
  virtual bool LoadChunkServer(
      std::unordered_map<ChunkServerIdType, ChunkServer> *chunkServerMap,
      ChunkServerIdType *maxChunkServerId) = 0;
  virtual bool LoadCopySet(
      std::map<CopySetKey, CopySetInfo> *copySetMap,
      std::map<PoolIdType, CopySetIdType> *copySetIdMaxMap) = 0;

  virtual bool StorageLogicalPool(const LogicalPool &data) = 0;
  virtual bool StoragePhysicalPool(const PhysicalPool &data) = 0;
  virtual bool StorageZone(const Zone &data) = 0;
  virtual bool StorageServer(const Server &data) = 0;
  virtual bool StorageChunkServer(const ChunkServer &data) = 0;
  virtual bool StorageCopySet(const CopySetInfo &data) = 0;

  virtual bool DeleteLogicalPool(PoolIdType id) = 0;
  virtual bool DeletePhysicalPool(PoolIdType id) = 0;
  virtual bool DeleteZone(ZoneIdType id) = 0;
  virtual bool DeleteServer(ServerIdType id) = 0;
  virtual bool DeleteChunkServer(ChunkServerIdType id) = 0;
  virtual bool DeleteCopySet(CopySetKey key) = 0;

  virtual bool UpdateLogicalPool(const LogicalPool &data) = 0;
  virtual bool UpdatePhysicalPool(const PhysicalPool &data) = 0;
  virtual bool UpdateZone(const Zone &data) = 0;
  virtual bool UpdateServer(const Server &data) = 0;
  virtual bool UpdateChunkServer(const ChunkServer &data) = 0;
  virtual bool UpdateCopySet(const CopySetInfo &data) = 0;
};

class DefaultTopologyStorage : public TopologyStorage {
 public:
  explicit DefaultTopologyStorage(
      std::shared_ptr<::curve::repo::RepoInterface> repo)
      : repo_(repo) {}
  virtual ~DefaultTopologyStorage() {}

  virtual bool init(const std::string &dbName,
                    const std::string &user,
                    const std::string &url,
                    const std::string &password);

  virtual bool LoadLogicalPool(
      std::unordered_map<PoolIdType, LogicalPool> *logicalPoolMap,
      PoolIdType *maxLogicalPoolId);
  virtual bool LoadPhysicalPool(
      std::unordered_map<PoolIdType, PhysicalPool> *physicalPoolMap,
      PoolIdType *maxPhysicalPoolId);
  virtual bool LoadZone(
      std::unordered_map<ZoneIdType, Zone> *zoneMap,
      ZoneIdType *maxZoneId);
  virtual bool LoadServer(
      std::unordered_map<ServerIdType, Server> *serverMap,
      ServerIdType *maxServerId);
  virtual bool LoadChunkServer(
      std::unordered_map<ChunkServerIdType, ChunkServer> *chunkServerMap,
      ChunkServerIdType *maxChunkServerId);
  virtual bool LoadCopySet(
      std::map<CopySetKey, CopySetInfo> *copySetMap,
      std::map<PoolIdType, CopySetIdType> *copySetIdMaxMap);

  virtual bool StorageLogicalPool(const LogicalPool &data);
  virtual bool StoragePhysicalPool(const PhysicalPool &data);
  virtual bool StorageZone(const Zone &data);
  virtual bool StorageServer(const Server &data);
  virtual bool StorageChunkServer(const ChunkServer &data);
  virtual bool StorageCopySet(const CopySetInfo &data);

  virtual bool DeleteLogicalPool(PoolIdType id);
  virtual bool DeletePhysicalPool(PoolIdType id);
  virtual bool DeleteZone(ZoneIdType id);
  virtual bool DeleteServer(ServerIdType id);
  virtual bool DeleteChunkServer(ChunkServerIdType id);
  virtual bool DeleteCopySet(CopySetKey key);

  virtual bool UpdateLogicalPool(const LogicalPool &data);
  virtual bool UpdatePhysicalPool(const PhysicalPool &data);
  virtual bool UpdateZone(const Zone &data);
  virtual bool UpdateServer(const Server &data);
  virtual bool UpdateChunkServer(const ChunkServer &data);
  virtual bool UpdateCopySet(const CopySetInfo &data);

 private:
  std::shared_ptr<::curve::repo::RepoInterface> repo_;
};

}  // namespace topology
}  // namespace mds
}  // namespace curve
#endif  // CURVE_SRC_MDS_TOPOLOGY_TOPOLOGY_STORGE_H_
