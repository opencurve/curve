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
 * Created Date: 2021-08-27
 * Author: wanghai01
 */

#ifndef CURVEFS_SRC_MDS_TOPOLOGY_TOPOLOGY_STORGE_ETCD_H_
#define CURVEFS_SRC_MDS_TOPOLOGY_TOPOLOGY_STORGE_ETCD_H_

#include <vector>
#include <map>
#include <unordered_map>
#include <memory>

#include "curvefs/src/mds/topology/topology_storge.h"
#include "curvefs/src/mds/topology/topology_storage_codec.h"
#include "src/kvstorageclient/etcd_client.h"

namespace curvefs {
namespace mds {
namespace topology {

using ::curve::kvstorage::EtcdClientImp;
using ::curve::kvstorage::KVStorageClient;

class TopologyStorageEtcd : public TopologyStorage {
 public:
    TopologyStorageEtcd(std::shared_ptr<KVStorageClient> client,
                        std::shared_ptr<TopologyStorageCodec> codec)
                        : client_(client), codec_(codec) {}

    bool LoadPool(std::unordered_map<PoolIdType, Pool> *poolMap,
                  PoolIdType *maxPoolId) override;
    bool LoadZone(std::unordered_map<ZoneIdType, Zone> *zoneMap,
                  ZoneIdType *maxZoneId) override;
    bool LoadServer(std::unordered_map<ServerIdType, Server> *serverMap,
                    ServerIdType *maxServerId) override;
    bool LoadMetaServer(
        std::unordered_map<MetaServerIdType, MetaServer> *metaServerMap,
        MetaServerIdType *maxMetaServerId) override;
    bool LoadCopySet(std::map<CopySetKey, CopySetInfo> *copySetMap,
        std::map<PoolIdType, CopySetIdType> *copySetIdMaxMap) override;
    bool LoadPartition(
        std::unordered_map<PartitionIdType, Partition> *partitionMap,
        PartitionIdType *maxPartitionId) override;

    bool StoragePool(const Pool &data) override;
    bool StorageZone(const Zone &data) override;
    bool StorageServer(const Server &data) override;
    bool StorageMetaServer(const MetaServer &data) override;
    bool StorageCopySet(const CopySetInfo &data) override;
    bool StoragePartition(const Partition &data) override;

    bool DeletePool(PoolIdType id) override;
    bool DeleteZone(ZoneIdType id) override;
    bool DeleteServer(ServerIdType id) override;
    bool DeleteMetaServer(MetaServerIdType id) override;
    bool DeleteCopySet(CopySetKey key) override;
    bool DeletePartition(PartitionIdType id) override;

    bool UpdatePool(const Pool &data) override;
    bool UpdateZone(const Zone &data) override;
    bool UpdateServer(const Server &data) override;
    bool UpdateMetaServer(const MetaServer &data) override;
    bool UpdateCopySet(const CopySetInfo &data) override;
    bool UpdatePartition(const Partition &data) override;
    bool UpdatePartitions(const std::vector<Partition> &datas) override;

    bool LoadClusterInfo(std::vector<ClusterInformation> *info) override;
    bool StorageClusterInfo(const ClusterInformation &info) override;

 private:
    // underlying storage media
    std::shared_ptr<KVStorageClient> client_;
    // codec module
    std::shared_ptr<TopologyStorageCodec> codec_;
};

}  // namespace topology
}  // namespace mds
}  // namespace curvefs

#endif  // CURVEFS_SRC_MDS_TOPOLOGY_TOPOLOGY_STORGE_ETCD_H_
