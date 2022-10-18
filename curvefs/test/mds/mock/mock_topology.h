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
 * Created Date: 2021-09-05
 * Author: wanghai01
 */

#ifndef CURVEFS_TEST_MDS_MOCK_MOCK_TOPOLOGY_H_
#define CURVEFS_TEST_MDS_MOCK_MOCK_TOPOLOGY_H_

#include <brpc/channel.h>
#include <brpc/controller.h>
#include <brpc/server.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <list>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "curvefs/proto/topology.pb.h"
#include "curvefs/src/mds/common/mds_define.h"
#include "curvefs/src/mds/topology/topology_id_generator.h"
#include "curvefs/src/mds/topology/topology_manager.h"
#include "curvefs/src/mds/topology/topology_service.h"
#include "src/kvstorageclient/etcd_client.h"

#include "curvefs/proto/copyset.pb.h"

using ::testing::Return;
using ::testing::_;

using ::curve::kvstorage::EtcdClientImp;
using ::curve::kvstorage::KVStorageClient;

namespace curve {
namespace kvstorage {

class MockKVStorageClient : public KVStorageClient {
 public:
    virtual ~MockKVStorageClient() {}
    MOCK_METHOD2(Put, int(const std::string &, const std::string &));
    MOCK_METHOD2(Get, int(const std::string &, std::string *));
    MOCK_METHOD3(List, int(const std::string &, const std::string &,
                           std::vector<std::string> *));
    MOCK_METHOD3(List, int(const std::string &, const std::string &,
                           std::vector<std::pair<std::string, std::string>> *));
    MOCK_METHOD1(Delete, int(const std::string &));
    MOCK_METHOD1(TxnN, int(const std::vector<Operation> &));
    MOCK_METHOD3(CompareAndSwap, int(const std::string &, const std::string &,
                                     const std::string &));
    MOCK_METHOD5(CampaignLeader, int(const std::string &, const std::string &,
                                     uint32_t, uint32_t, uint64_t *));
    MOCK_METHOD2(LeaderObserve, int(uint64_t, const std::string &));
    MOCK_METHOD2(LeaderKeyExist, bool(uint64_t, uint64_t));
    MOCK_METHOD2(LeaderResign, int(uint64_t, uint64_t));
    MOCK_METHOD1(GetCurrentRevision, int(int64_t *));
    MOCK_METHOD6(ListWithLimitAndRevision,
                 int(const std::string &, const std::string &, int64_t, int64_t,
                     std::vector<std::string> *, std::string *));
    MOCK_METHOD3(PutRewithRevision,
                 int(const std::string &, const std::string &, int64_t *));
    MOCK_METHOD2(DeleteRewithRevision, int(const std::string &, int64_t *));
};

}  // namespace kvstorage
}  // namespace curve

namespace curvefs {
namespace mds {
namespace topology {

class MockIdGenerator : public TopologyIdGenerator {
 public:
    MockIdGenerator() {}
    ~MockIdGenerator() {}

    MOCK_METHOD1(initPoolIdGenerator, void(PoolIdType idMax));
    MOCK_METHOD1(initZoneIdGenerator, void(ZoneIdType idMax));
    MOCK_METHOD1(initServerIdGenerator, void(ServerIdType idMax));
    MOCK_METHOD1(initMetaServerIdGenerator, void(MetaServerIdType idMax));
    MOCK_METHOD1(initCopySetIdGenerator,
                 void(const std::map<PoolIdType, CopySetIdType> &idMaxMap));
    MOCK_METHOD1(initPartitionIdGenerator, void(PartitionIdType idMax));
    MOCK_METHOD1(initMemcacheClusterIdGenerator,
                 void(MemcacheClusterIdType idMax));

    MOCK_METHOD0(GenPoolId, PoolIdType());
    MOCK_METHOD0(GenZoneId, ZoneIdType());
    MOCK_METHOD0(GenServerId, ServerIdType());
    MOCK_METHOD0(GenMetaServerId, MetaServerIdType());
    MOCK_METHOD1(GenCopySetId, CopySetIdType(PoolIdType poolId));
    MOCK_METHOD0(GenPartitionId, PartitionIdType());
    MOCK_METHOD0(GenMemCacheClusterId, MemcacheClusterIdType());
};

class MockTokenGenerator : public TopologyTokenGenerator {
 public:
    MockTokenGenerator() {}
    ~MockTokenGenerator() {}

    MOCK_METHOD0(GenToken, std::string());
};

class MockStorage : public TopologyStorage {
 public:
    MockStorage() {}
    ~MockStorage() {}

    MOCK_METHOD2(LoadPool, bool(std::unordered_map<PoolIdType, Pool> *poolMap,
                                PoolIdType *maxPoolId));
    MOCK_METHOD2(LoadZone, bool(std::unordered_map<ZoneIdType, Zone> *zoneMap,
                                ZoneIdType *maxZoneId));
    MOCK_METHOD2(LoadServer,
                 bool(std::unordered_map<ServerIdType, Server> *serverMap,
                      ServerIdType *maxServerId));
    MOCK_METHOD2(
        LoadMetaServer,
        bool(std::unordered_map<MetaServerIdType, MetaServer> *metaServerMap,
             MetaServerIdType *maxMetaServerId));
    MOCK_METHOD2(LoadCopySet,
                 bool(std::map<CopySetKey, CopySetInfo> *copySetMap,
                      std::map<PoolIdType, CopySetIdType> *copySetIdMaxMap));
    MOCK_METHOD2(
        LoadPartition,
        bool(std::unordered_map<PartitionIdType, Partition>* partitionMap,
             PartitionIdType* maxPartitionId));
    MOCK_METHOD2(LoadMemcacheCluster,
                 bool(std::unordered_map<MemcacheClusterIdType,
                                         MemcacheCluster>* memcacheclusterMap,
                      MemcacheClusterIdType* maxMemcacheClusterId));

    MOCK_METHOD1(StoragePool, bool(const Pool &data));
    MOCK_METHOD1(StorageZone, bool(const Zone &data));
    MOCK_METHOD1(StorageServer, bool(const Server &data));
    MOCK_METHOD1(StorageMetaServer, bool(const MetaServer &data));
    MOCK_METHOD1(StorageCopySet, bool(const CopySetInfo &data));
    MOCK_METHOD1(StoragePartition, bool(const Partition& data));
    MOCK_METHOD1(StorageMemcacheCluster,
                 bool((const MemcacheCluster& cluster)));

    MOCK_METHOD1(DeletePool, bool(PoolIdType id));
    MOCK_METHOD1(DeleteZone, bool(ZoneIdType id));
    MOCK_METHOD1(DeleteServer, bool(ServerIdType id));
    MOCK_METHOD1(DeleteMetaServer, bool(MetaServerIdType id));
    MOCK_METHOD1(DeleteCopySet, bool(CopySetKey key));
    MOCK_METHOD1(DeletePartition, bool(PartitionIdType id));

    MOCK_METHOD1(UpdatePool, bool(const Pool &data));
    MOCK_METHOD1(UpdateZone, bool(const Zone &data));
    MOCK_METHOD1(UpdateServer, bool(const Server &data));
    MOCK_METHOD1(UpdateMetaServer, bool(const MetaServer &data));
    MOCK_METHOD1(UpdateCopySet, bool(const CopySetInfo &data));
    MOCK_METHOD1(UpdatePartition, bool(const Partition &data));
    MOCK_METHOD1(UpdatePartitions, bool(const std::vector<Partition> &datas));
    MOCK_METHOD2(UpdatePartitionStatus, TopoStatusCode(
        PartitionIdType partitionId, PartitionStatus status));

    MOCK_METHOD1(LoadClusterInfo, bool(std::vector<ClusterInformation> *info));
    MOCK_METHOD1(StorageClusterInfo, bool(const ClusterInformation& info));
    MOCK_METHOD2(StorageFs2MemcacheCluster,
                 bool(FsIdType, MemcacheClusterIdType));
    MOCK_METHOD1(LoadFs2MemcacheCluster,
                 bool(std::unordered_map<FsIdType, MemcacheClusterIdType>*));
};

class MockEtcdClient : public EtcdClientImp {
 public:
    virtual ~MockEtcdClient() {}
    MOCK_METHOD2(Put, int(const std::string &, const std::string &));
    MOCK_METHOD2(Get, int(const std::string &, std::string *));
    MOCK_METHOD3(List, int(const std::string &, const std::string &,
                           std::vector<std::string> *));
    MOCK_METHOD3(List, int(const std::string &, const std::string &,
                           std::vector<std::pair<std::string, std::string>> *));
    MOCK_METHOD1(Delete, int(const std::string &));
    MOCK_METHOD1(TxnN, int(const std::vector<Operation> &));
    MOCK_METHOD3(CompareAndSwap, int(const std::string &, const std::string &,
                                     const std::string &));
    MOCK_METHOD5(CampaignLeader, int(const std::string &, const std::string &,
                                     uint32_t, uint32_t, uint64_t *));
    MOCK_METHOD2(LeaderObserve, int(uint64_t, const std::string &));
    MOCK_METHOD2(LeaderKeyExist, bool(uint64_t, uint64_t));
    MOCK_METHOD2(LeaderResign, int(uint64_t, uint64_t));
    MOCK_METHOD1(GetCurrentRevision, int(int64_t *));
    MOCK_METHOD6(ListWithLimitAndRevision,
                 int(const std::string &, const std::string &, int64_t, int64_t,
                     std::vector<std::string> *, std::string *));
    MOCK_METHOD3(PutRewithRevision,
                 int(const std::string &, const std::string &, int64_t *));
    MOCK_METHOD2(DeleteRewithRevision, int(const std::string &, int64_t *));
};

class MockTopology : public TopologyImpl {
 public:
    MockTopology(std::shared_ptr<TopologyIdGenerator> idGenerator,
                 std::shared_ptr<TopologyTokenGenerator> tokenGenerator,
                 std::shared_ptr<TopologyStorage> storage)
        : TopologyImpl(idGenerator, tokenGenerator, storage) {}
    ~MockTopology() {}

    MOCK_METHOD1(GetClusterInfo, bool(ClusterInformation *info));

    // allocate id & token
    MOCK_METHOD0(AllocatePoolId, PoolIdType());
    MOCK_METHOD0(AllocateZoneId, ZoneIdType());
    MOCK_METHOD0(AllocateServerId, ServerIdType());
    MOCK_METHOD0(AllocateMetaServerId, MetaServerIdType());
    MOCK_METHOD1(AllocateCopySetId, CopySetIdType(PoolIdType poolId));
    MOCK_METHOD0(AllocatePartitionId, PartitionIdType());
    MOCK_METHOD0(AllocateToken, std::string());

    // add
    MOCK_METHOD1(AddPool, TopoStatusCode(const Pool &data));
    MOCK_METHOD1(AddZone, TopoStatusCode(const Zone &data));
    MOCK_METHOD1(AddServer, TopoStatusCode(const Server &data));
    MOCK_METHOD1(AddMetaServer, TopoStatusCode(const MetaServer &data));
    MOCK_METHOD1(AddCopySet, TopoStatusCode(const CopySetInfo &data));
    MOCK_METHOD1(AddPartition, TopoStatusCode(const Partition &data));

    // remove
    MOCK_METHOD1(RemovePool, TopoStatusCode(PoolIdType id));
    MOCK_METHOD1(RemoveZone, TopoStatusCode(ZoneIdType id));
    MOCK_METHOD1(RemoveServer, TopoStatusCode(ServerIdType id));
    MOCK_METHOD1(RemoveMetaServer, TopoStatusCode(MetaServerIdType id));
    MOCK_METHOD1(RemoveCopySet, TopoStatusCode(CopySetKey key));
    MOCK_METHOD1(RemovePartition, TopoStatusCode(PartitionIdType id));

    // update
    MOCK_METHOD1(UpdatePool, TopoStatusCode(const Pool &data));
    MOCK_METHOD1(UpdateZone, TopoStatusCode(const Zone &data));
    MOCK_METHOD1(UpdateServer, TopoStatusCode(const Server &data));
    MOCK_METHOD2(UpdateMetaServerOnlineState,
                 TopoStatusCode(const OnlineState &onlineState,
                                MetaServerIdType id));
    MOCK_METHOD1(UpdateCopySetTopo, TopoStatusCode(const CopySetInfo &data));
    MOCK_METHOD2(SetCopySetAvalFlag, TopoStatusCode(const CopySetKey &, bool));
    MOCK_METHOD3(UpdateCopySetAllocInfo,
                 TopoStatusCode(CopySetKey key, uint32_t allocChunkNum,
                                uint64_t allocSize));
    MOCK_METHOD2(UpdatePartitionStatistic,
                 TopoStatusCode(uint32_t partitionId,
                                PartitionStatistic statistic));

    // find
    MOCK_CONST_METHOD1(FindPool, PoolIdType(const std::string &poolName));
    MOCK_CONST_METHOD2(FindZone, ZoneIdType(const std::string &zoneName,
                                            const std::string &poolName));
    MOCK_CONST_METHOD2(FindZone, ZoneIdType(const std::string &zoneName,
                                            PoolIdType poolid));
    MOCK_CONST_METHOD1(FindServerByHostName,
                       ServerIdType(const std::string &hostName));
    MOCK_CONST_METHOD2(FindServerByHostIpPort,
                       ServerIdType(const std::string &hostIp, uint32_t port));

    // get
    MOCK_CONST_METHOD2(GetPool, bool(PoolIdType poolId, Pool *out));
    MOCK_CONST_METHOD2(GetZone, bool(ZoneIdType zoneId, Zone *out));
    MOCK_CONST_METHOD2(GetServer, bool(ServerIdType serverId, Server *out));
    MOCK_CONST_METHOD2(GetMetaServer,
                       bool(MetaServerIdType metaserverId, MetaServer *out));
    MOCK_CONST_METHOD3(GetMetaServer, bool(const std::string &hostIp,
                                           uint32_t port, MetaServer *out));
    MOCK_CONST_METHOD2(GetCopySet, bool(CopySetKey key, CopySetInfo *out));
    MOCK_CONST_METHOD2(GetCopysetOfPartition,
                       bool(PartitionIdType id, CopySetInfo *out));
    MOCK_CONST_METHOD1(GetCopysetNumInMetaserver,
                       uint32_t(MetaServerIdType id));
    MOCK_CONST_METHOD1(GetLeaderNumInMetaserver, uint32_t(MetaServerIdType id));
    MOCK_CONST_METHOD1(GetAvailableCopyset, bool(CopySetInfo *out));
    MOCK_CONST_METHOD0(GetAvailableCopysetNum, int());
    MOCK_CONST_METHOD0(GetAvailableCopysetKeyList, std::list<CopySetKey>());
    MOCK_METHOD2(GetPartition,
                 bool(PartitionIdType partitionId, Partition *out));

    MOCK_CONST_METHOD2(GetPool, bool(const std::string &poolName, Pool *out));
    MOCK_CONST_METHOD3(GetZone, bool(const std::string &zoneName,
                                     const std::string &poolName, Zone *out));
    MOCK_CONST_METHOD3(GetZone, bool(const std::string &zoneName,
                                     PoolIdType poolId, Zone *out));
    MOCK_CONST_METHOD2(GetServerByHostName,
                       bool(const std::string &hostName, Server *out));
    MOCK_CONST_METHOD3(GetServerByHostIpPort, bool(const std::string &hostIp,
                                                   uint32_t port, Server *out));

    // getvector
    MOCK_CONST_METHOD1(GetMetaServerInCluster,
                       std::vector<MetaServerIdType>(MetaServerFilter filter));
    MOCK_CONST_METHOD1(GetServerInCluster,
                       std::vector<ServerIdType>(ServerFilter filter));
    MOCK_CONST_METHOD1(GetZoneInCluster,
                       std::vector<ZoneIdType>(ZoneFilter filter));
    MOCK_CONST_METHOD1(GetPoolInCluster,
                       std::vector<PoolIdType>(PoolFilter filter));

    MOCK_CONST_METHOD2(GetMetaServerInServer,
                       std::list<MetaServerIdType>(ServerIdType id,
                                                   MetaServerFilter filter));
    MOCK_CONST_METHOD2(GetMetaServerInZone,
                       std::list<MetaServerIdType>(ZoneIdType id,
                                                   MetaServerFilter filter));
    MOCK_CONST_METHOD2(GetServerInZone,
                       std::list<ServerIdType>(ZoneIdType id,
                                               ServerFilter filter));
    MOCK_CONST_METHOD2(GetZoneInPool,
                       std::list<ZoneIdType>(PoolIdType id, ZoneFilter filter));
    MOCK_CONST_METHOD2(GetCopysetOfPartition,
                       std::vector<CopySetInfo>(PartitionIdType id,
                                                CopySetFilter filter));
    MOCK_CONST_METHOD1(GetCopySetsInCluster,
                       std::vector<CopySetKey>(CopySetFilter filter));
    MOCK_CONST_METHOD2(GetCopySetsInMetaServer,
                       std::vector<CopySetKey>(MetaServerIdType id,
                                               CopySetFilter filter));
    MOCK_CONST_METHOD2(GetCopySetsInPool,
                       std::vector<CopySetIdType>(PoolIdType poolId,
                                                  CopySetFilter filter));
    MOCK_CONST_METHOD2(GetCopySetInfosInPool,
                       std::vector<CopySetInfo>(PoolIdType poolId,
                                                CopySetFilter filter));
    MOCK_CONST_METHOD4(
        ChooseNewMetaServerForCopyset,
        TopoStatusCode(PoolIdType poolId,
                       const std::set<ZoneIdType> &unavailableZones,
                       const std::set<MetaServerIdType> &unavailableMs,
                       MetaServerIdType *target));

    MOCK_CONST_METHOD1(GetPartitionInfosInCopyset,
                       std::list<Partition>(CopySetIdType copysetId));
};

class MockTopologyManager : public TopologyManager {
 public:
    MockTopologyManager(std::shared_ptr<Topology> topology,
                        std::shared_ptr<MetaserverClient> metaserverClient)
        : TopologyManager(topology, metaserverClient) {}

    ~MockTopologyManager() {}

    MOCK_METHOD2(RegistMetaServer, void(const MetaServerRegistRequest *request,
                                        MetaServerRegistResponse *response));

    MOCK_METHOD2(ListMetaServer, void(const ListMetaServerRequest *request,
                                      ListMetaServerResponse *response));

    MOCK_METHOD2(GetMetaServer, void(const GetMetaServerInfoRequest *request,
                                     GetMetaServerInfoResponse *response));

    MOCK_METHOD2(DeleteMetaServer, void(const DeleteMetaServerRequest *request,
                                        DeleteMetaServerResponse *response));

    MOCK_METHOD2(RegistServer, void(const ServerRegistRequest *request,
                                    ServerRegistResponse *response));

    MOCK_METHOD2(GetServer, void(const GetServerRequest *request,
                                 GetServerResponse *response));

    MOCK_METHOD2(DeleteServer, void(const DeleteServerRequest *request,
                                    DeleteServerResponse *response));

    MOCK_METHOD2(ListZoneServer, void(const ListZoneServerRequest *request,
                                      ListZoneServerResponse *response));

    MOCK_METHOD2(CreateZone, void(const CreateZoneRequest *request,
                                  CreateZoneResponse *response));

    MOCK_METHOD2(DeleteZone, void(const DeleteZoneRequest *request,
                                  DeleteZoneResponse *response));

    MOCK_METHOD2(GetZone, void(const GetZoneRequest *request,
                               GetZoneResponse *response));

    MOCK_METHOD2(ListPoolZone, void(const ListPoolZoneRequest *request,
                                    ListPoolZoneResponse *response));

    MOCK_METHOD2(CreatePool, void(const CreatePoolRequest *request,
                                  CreatePoolResponse *response));

    MOCK_METHOD2(DeletePool, void(const DeletePoolRequest *request,
                                  DeletePoolResponse *response));

    MOCK_METHOD2(GetPool, void(const GetPoolRequest *request,
                               GetPoolResponse *response));

    MOCK_METHOD2(ListPool, void(const ListPoolRequest *request,
                                ListPoolResponse *response));

    MOCK_METHOD2(CreatePartitions, void(const CreatePartitionRequest *request,
                                        CreatePartitionResponse *response));

    MOCK_METHOD2(DeletePartition, void(const DeletePartitionRequest *request,
                                       DeletePartitionResponse *response));

    MOCK_METHOD2(CreatePartitionsAndGetMinPartition,
                 TopoStatusCode(FsIdType fsId, PartitionInfo *partition));

    MOCK_METHOD2(CommitTx, void(const CommitTxRequest *request,
                                CommitTxResponse *response));

    MOCK_METHOD2(GetMetaServerListInCopysets,
                 void(const GetMetaServerListInCopySetsRequest *request,
                      GetMetaServerListInCopySetsResponse *response));

    MOCK_METHOD2(ListPartition, void(const ListPartitionRequest *request,
                                     ListPartitionResponse *response));

    MOCK_METHOD2(ListPartitionOfFs,
                 void(FsIdType fsId, std::list<PartitionInfo> *list));

    MOCK_METHOD2(UpdatePartitionStatus,
                 TopoStatusCode(PartitionIdType partitionId,
                                PartitionStatus status));

    MOCK_METHOD2(GetCopysetOfPartition,
                 void(const GetCopysetOfPartitionRequest *request,
                      GetCopysetOfPartitionResponse *response));

    MOCK_METHOD3(GetCopysetMembers,
                 TopoStatusCode(const PoolIdType poolId,
                                const CopySetIdType copysetId,
                                std::set<std::string> *addrs));
    MOCK_METHOD3(CreateCopysetNodeOnMetaServer,
                 bool(PoolIdType poolId, CopySetIdType copysetId,
                      MetaServerIdType metaServerId));

    MOCK_METHOD2(GetLatestPartitionsTxId,
                 void(const std::vector<PartitionTxId> &txIds,
                      std::vector<PartitionTxId> *needUpdate));
};

}  // namespace topology
}  // namespace mds
}  // namespace curvefs

#endif  // CURVEFS_TEST_MDS_MOCK_MOCK_TOPOLOGY_H_
