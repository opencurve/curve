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
 * Created Date: Wed Jun 17 2020
 * Author: xuchaojie
 */

#ifndef SRC_MDS_TOPOLOGY_TOPOLOGY_STORGE_ETCD_H_
#define SRC_MDS_TOPOLOGY_TOPOLOGY_STORGE_ETCD_H_

#include <vector>
#include <map>
#include <unordered_map>
#include <memory>

#include "src/mds/topology/topology_storge.h"
#include "src/kvstorageclient/etcd_client.h"
#include "src/mds/topology/topology_storage_codec.h"

namespace curve {
namespace mds {
namespace topology {

using ::curve::kvstorage::EtcdClientImp;
using ::curve::kvstorage::KVStorageClient;



class TopologyStorageEtcd : public TopologyStorage {
 public:
    TopologyStorageEtcd(std::shared_ptr<KVStorageClient> client,
        std::shared_ptr<TopologyStorageCodec> codec)
    : client_(client),
      codec_(codec) {}

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

    bool LoadClusterInfo(std::vector<ClusterInformation> *info) override;
    bool StorageClusterInfo(const ClusterInformation &info) override;

 private:
    // 底层存储介质
    std::shared_ptr<KVStorageClient> client_;
    // 编码模块
    std::shared_ptr<TopologyStorageCodec> codec_;
};

}  // namespace topology
}  // namespace mds
}  // namespace curve

#endif  // SRC_MDS_TOPOLOGY_TOPOLOGY_STORGE_ETCD_H_
