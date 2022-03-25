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
 * Project: curve
 * Created Date: 2021-08-25
 * Author: wanghai01
 */

#ifndef CURVEFS_SRC_MDS_TOPOLOGY_TOPOLOGY_STORGE_H_
#define CURVEFS_SRC_MDS_TOPOLOGY_TOPOLOGY_STORGE_H_

#include <list>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "curvefs/src/mds/topology/topology_config.h"
#include "curvefs/src/mds/topology/topology_item.h"

namespace curvefs {
namespace mds {
namespace topology {

class TopologyStorage {
 public:
    TopologyStorage() {}
    virtual ~TopologyStorage() {}

    virtual bool LoadPool(std::unordered_map<PoolIdType, Pool> *poolMap,
                          PoolIdType *maxPoolId) = 0;
    virtual bool LoadZone(std::unordered_map<ZoneIdType, Zone> *zoneMap,
                          ZoneIdType *maxZoneId) = 0;
    virtual bool LoadServer(std::unordered_map<ServerIdType, Server> *serverMap,
                            ServerIdType *maxServerId) = 0;
    virtual bool LoadMetaServer(
        std::unordered_map<MetaServerIdType, MetaServer> *metaServerMap,
        MetaServerIdType *maxMetaServerId) = 0;
    virtual bool LoadCopySet(
        std::map<CopySetKey, CopySetInfo> *copySetMap,
        std::map<PoolIdType, CopySetIdType> *copySetIdMaxMap) = 0;
    virtual bool LoadPartition(
        std::unordered_map<PartitionIdType, Partition> *partitionMap,
        PartitionIdType *maxPartitionId) = 0;

    virtual bool StoragePool(const Pool &data) = 0;
    virtual bool StorageZone(const Zone &data) = 0;
    virtual bool StorageServer(const Server &data) = 0;
    virtual bool StorageMetaServer(const MetaServer &data) = 0;
    virtual bool StorageCopySet(const CopySetInfo &data) = 0;
    virtual bool StoragePartition(const Partition &data) = 0;

    virtual bool DeletePool(PoolIdType id) = 0;
    virtual bool DeleteZone(ZoneIdType id) = 0;
    virtual bool DeleteServer(ServerIdType id) = 0;
    virtual bool DeleteMetaServer(MetaServerIdType id) = 0;
    virtual bool DeleteCopySet(CopySetKey key) = 0;
    virtual bool DeletePartition(PartitionIdType id) = 0;

    virtual bool UpdatePool(const Pool &data) = 0;
    virtual bool UpdateZone(const Zone &data) = 0;
    virtual bool UpdateServer(const Server &data) = 0;
    virtual bool UpdateMetaServer(const MetaServer &data) = 0;
    virtual bool UpdateCopySet(const CopySetInfo &data) = 0;
    virtual bool UpdatePartition(const Partition &data) = 0;
    virtual bool UpdatePartitions(const std::vector<Partition> &datas) = 0;

    virtual bool LoadClusterInfo(std::vector<ClusterInformation> *info) = 0;
    virtual bool StorageClusterInfo(const ClusterInformation &info) = 0;
};

}  // namespace topology
}  // namespace mds
}  // namespace curvefs
#endif  // CURVEFS_SRC_MDS_TOPOLOGY_TOPOLOGY_STORGE_H_
