/*
 * Project: curve
 * Created Date: Wed Aug 22 2018
 * Author: xuchaojie
 * Copyright (c) 2018 netease
 */

#ifndef SRC_MDS_TOPOLOGY_TOPOLOGY_STORGE_H_
#define SRC_MDS_TOPOLOGY_TOPOLOGY_STORGE_H_

#include <list>
#include <unordered_map>
#include <string>
#include <map>

#include "src/mds/topology/topology_item.h"
#include "src/mds/dao/mdsRepo.h"
#include "src/mds/topology/topology_config.h"

namespace curve {
namespace mds {
namespace topology {

class TopologyStorage {
 public:
    TopologyStorage() {}
    virtual ~TopologyStorage() {}

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
        std::shared_ptr<MdsRepo> repo)
        : repo_(repo) {}

    bool init(const TopologyOption &option);

    bool LoadLogicalPool(
        std::unordered_map<PoolIdType, LogicalPool> *logicalPoolMap,
        PoolIdType *maxLogicalPoolId) override;
    bool LoadPhysicalPool(
        std::unordered_map<PoolIdType, PhysicalPool> *physicalPoolMap,
        PoolIdType *maxPhysicalPoolId) override;
    bool LoadZone(
        std::unordered_map<ZoneIdType, Zone> *zoneMap,
        ZoneIdType *maxZoneId) override;
    bool LoadServer(
        std::unordered_map<ServerIdType, Server> *serverMap,
        ServerIdType *maxServerId) override;
    bool LoadChunkServer(
        std::unordered_map<ChunkServerIdType, ChunkServer> *chunkServerMap,
        ChunkServerIdType *maxChunkServerId) override;
    bool LoadCopySet(
        std::map<CopySetKey, CopySetInfo> *copySetMap,
        std::map<PoolIdType, CopySetIdType> *copySetIdMaxMap) override;

    bool StorageLogicalPool(const LogicalPool &data) override;
    bool StoragePhysicalPool(const PhysicalPool &data) override;
    bool StorageZone(const Zone &data) override;
    bool StorageServer(const Server &data) override;
    bool StorageChunkServer(const ChunkServer &data) override;
    bool StorageCopySet(const CopySetInfo &data) override;

    bool DeleteLogicalPool(PoolIdType id) override;
    bool DeletePhysicalPool(PoolIdType id) override;
    bool DeleteZone(ZoneIdType id) override;
    bool DeleteServer(ServerIdType id) override;
    bool DeleteChunkServer(ChunkServerIdType id) override;
    bool DeleteCopySet(CopySetKey key) override;

    bool UpdateLogicalPool(const LogicalPool &data) override;
    bool UpdatePhysicalPool(const PhysicalPool &data) override;
    bool UpdateZone(const Zone &data) override;
    bool UpdateServer(const Server &data) override;
    bool UpdateChunkServer(const ChunkServer &data) override;
    bool UpdateCopySet(const CopySetInfo &data) override;

 private:
    std::shared_ptr<MdsRepo> repo_;
};

}  // namespace topology
}  // namespace mds
}  // namespace curve
#endif  // SRC_MDS_TOPOLOGY_TOPOLOGY_STORGE_H_
