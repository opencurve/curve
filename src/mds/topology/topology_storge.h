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
 * Created Date: Wed Aug 22 2018
 * Author: xuchaojie
 */

#ifndef SRC_MDS_TOPOLOGY_TOPOLOGY_STORGE_H_
#define SRC_MDS_TOPOLOGY_TOPOLOGY_STORGE_H_

#include <list>
#include <unordered_map>
#include <string>
#include <memory>
#include <map>
#include <vector>

#include "src/mds/topology/topology_item.h"
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

    virtual bool LoadClusterInfo(std::vector<ClusterInformation> *info) = 0;
    virtual bool StorageClusterInfo(const ClusterInformation &info) = 0;
};


}  // namespace topology
}  // namespace mds
}  // namespace curve
#endif  // SRC_MDS_TOPOLOGY_TOPOLOGY_STORGE_H_
