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
 * Created Date: Tue Sep 25 2018
 * Author: xuchaojie
 */

#ifndef TEST_MDS_TOPOLOGY_MOCK_TOPOLOGY_H_
#define TEST_MDS_TOPOLOGY_MOCK_TOPOLOGY_H_

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <brpc/controller.h>
#include <brpc/channel.h>
#include <brpc/server.h>

#include <map>
#include <string>
#include <vector>
#include <unordered_map>

#include "proto/topology.pb.h"
#include "src/mds/topology/topology_service_manager.h"
#include "src/mds/common/mds_define.h"
#include "src/mds/topology/topology_id_generator.h"
#include "src/mds/topology/topology_service.h"
#include "src/mds/topology/topology_stat.h"
#include "src/kvstorageclient/etcd_client.h"

#include "proto/copyset.pb.h"

using ::testing::Return;
using ::testing::_;

namespace curve {
namespace mds {
namespace topology {

class MockIdGenerator : public TopologyIdGenerator {
 public:
    MockIdGenerator() {}
    ~MockIdGenerator() {}
    MOCK_METHOD1(initPoolsetIdGenerator, void(PoolsetIdType
                  idMax));
    MOCK_METHOD1(initLogicalPoolIdGenerator, void(PoolIdType
                 idMax));
    MOCK_METHOD1(initPhysicalPoolIdGenerator, void(PoolIdType
                 idMax));
    MOCK_METHOD1(initZoneIdGenerator, void(ZoneIdType
                                           idMax));
    MOCK_METHOD1(initServerIdGenerator, void(ServerIdType
                 idMax));
    MOCK_METHOD1(initChunkServerIdGenerator, void(ChunkServerIdType
                 idMax));
    MOCK_METHOD1(initCopySetIdGenerator, void(
                     const std::map<PoolIdType, CopySetIdType> &idMaxMap));

    MOCK_METHOD0(GenPoolsetId, PoolsetIdType());
    MOCK_METHOD0(GenLogicalPoolId, PoolIdType());
    MOCK_METHOD0(GenPhysicalPoolId, PoolIdType());
    MOCK_METHOD0(GenZoneId, ZoneIdType());
    MOCK_METHOD0(GenServerId, ServerIdType());
    MOCK_METHOD0(GenChunkServerId, ChunkServerIdType());
    MOCK_METHOD1(GenCopySetId, CopySetIdType(PoolIdType
                 logicalPoolId));
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

    MOCK_METHOD2(LoadPoolset,
        bool(std::unordered_map<PoolsetIdType, Poolset>
            *poolsetMap, PoolsetIdType * maxPoolsetId));
    MOCK_METHOD2(LoadLogicalPool,
        bool(std::unordered_map<PoolIdType, LogicalPool>
            *logicalPoolMap, PoolIdType * maxLogicalPoolId));
    MOCK_METHOD2(LoadPhysicalPool,
                 bool(std::unordered_map<PoolIdType, PhysicalPool>
                      *physicalPoolMap, PoolIdType * maxPhysicalPoolId));
    MOCK_METHOD2(LoadZone, bool(std::unordered_map<ZoneIdType, Zone>
                                *zoneMap, ZoneIdType * maxZoneId));
    MOCK_METHOD2(LoadServer, bool(std::unordered_map<ServerIdType, Server>
                                  *serverMap, ServerIdType * maxServerId));
    MOCK_METHOD2(LoadChunkServer,
                 bool(std::unordered_map<ChunkServerIdType, ChunkServer>
                      *chunkServerMap, ChunkServerIdType * maxChunkServerId));
    MOCK_METHOD2(LoadCopySet,
        bool(std::map<CopySetKey, CopySetInfo>
        *copySetMap, std::map<PoolIdType, CopySetIdType> * copySetIdMaxMap));

    MOCK_METHOD1(StoragePoolset, bool(
                     const Poolset &data));
    MOCK_METHOD1(StorageLogicalPool, bool(
                     const LogicalPool &data));
    MOCK_METHOD1(StoragePhysicalPool, bool(
                     const PhysicalPool &data));
    MOCK_METHOD1(StorageZone, bool(
                     const Zone &data));
    MOCK_METHOD1(StorageServer, bool(
                     const Server &data));
    MOCK_METHOD1(StorageChunkServer, bool(
                     const ChunkServer &data));
    MOCK_METHOD1(StorageCopySet, bool(
                     const CopySetInfo &data));


    MOCK_METHOD1(DeletePoolset, bool(PoolsetIdType
                                         id));
    MOCK_METHOD1(DeleteLogicalPool, bool(PoolIdType
                                         id));
    MOCK_METHOD1(DeletePhysicalPool, bool(PoolIdType
                                          id));
    MOCK_METHOD1(DeleteZone, bool(ZoneIdType
                                  id));
    MOCK_METHOD1(DeleteServer, bool(ServerIdType
                                    id));
    MOCK_METHOD1(DeleteChunkServer, bool(ChunkServerIdType
                                         id));
    MOCK_METHOD1(DeleteCopySet, bool(CopySetKey
                                     key));

    MOCK_METHOD1(UpdateLogicalPool, bool(
                     const LogicalPool &data));
    MOCK_METHOD1(UpdatePhysicalPool, bool(
                     const PhysicalPool &data));
    MOCK_METHOD1(UpdateZone, bool(
                     const Zone &data));
    MOCK_METHOD1(UpdateServer, bool(
                     const Server &data));
    MOCK_METHOD1(UpdateChunkServer, bool(
                     const ChunkServer &data));
    MOCK_METHOD1(UpdateCopySet, bool(
                const CopySetInfo &data));

    MOCK_METHOD1(LoadClusterInfo,
                 bool(std::vector<ClusterInformation> *info));
    MOCK_METHOD1(StorageClusterInfo,
                 bool(const ClusterInformation &info));
};

class MockChunkServerRegistInfoBuilder : public ChunkServerRegistInfoBuilder {
 public:
     MOCK_METHOD1(BuildEpochMap, int(::google::protobuf::Map<
        ::google::protobuf::uint64, ::google::protobuf::uint64> *epochMap));
};

class MockTopologyServiceManager : public TopologyServiceManager {
 public:
    MockTopologyServiceManager(
        std::shared_ptr<Topology> topology,
        std::shared_ptr<TopologyStat> topologyStat,
        std::shared_ptr<curve::mds::copyset::CopysetManager> copysetManager) //NOLINT
        : TopologyServiceManager(topology, topologyStat, nullptr,
                                    copysetManager, nullptr) {}

    ~MockTopologyServiceManager() {}

    MOCK_METHOD2(RegistChunkServer, void(
                     const ChunkServerRegistRequest *request,
                     ChunkServerRegistResponse *response));

    MOCK_METHOD2(ListChunkServer, void(
                     const ListChunkServerRequest *request,
                     ListChunkServerResponse *response));

    MOCK_METHOD2(GetChunkServer, void(
                     const GetChunkServerInfoRequest *request,
                     GetChunkServerInfoResponse *response));

    MOCK_METHOD2(GetChunkServerInCluster, void(
                     const GetChunkServerInClusterRequest *request,
                     GetChunkServerInClusterResponse *response));

    MOCK_METHOD2(DeleteChunkServer, void(
                     const DeleteChunkServerRequest *request,
                     DeleteChunkServerResponse *response));

    MOCK_METHOD2(SetChunkServer, void(
                     const SetChunkServerStatusRequest *request,
                     SetChunkServerStatusResponse *response));

    MOCK_METHOD2(RegistServer, void(
                     const ServerRegistRequest *request,
                     ServerRegistResponse *response));

    MOCK_METHOD2(GetServer, void(
                     const GetServerRequest *request,
                     GetServerResponse *response));

    MOCK_METHOD2(DeleteServer, void(
                     const DeleteServerRequest *request,
                     DeleteServerResponse *response));

    MOCK_METHOD2(ListZoneServer, void(
                     const ListZoneServerRequest *request,
                     ListZoneServerResponse *response));

    MOCK_METHOD2(CreateZone, void(
                     const ZoneRequest *request,
                     ZoneResponse *response));

    MOCK_METHOD2(DeleteZone, void(
                     const ZoneRequest *request,
                     ZoneResponse *response));

    MOCK_METHOD2(GetZone, void(
                     const ZoneRequest *request,
                     ZoneResponse *response));

    MOCK_METHOD2(ListPoolZone, void(
                     const ListPoolZoneRequest *request,
                     ListPoolZoneResponse *response));

    MOCK_METHOD2(CreatePhysicalPool, void(
                     const PhysicalPoolRequest *request,
                     PhysicalPoolResponse *response));

    MOCK_METHOD2(DeletePhysicalPool, void(
                     const PhysicalPoolRequest *request,
                     PhysicalPoolResponse *response));

    MOCK_METHOD2(GetPhysicalPool, void(
                     const PhysicalPoolRequest *request,
                     PhysicalPoolResponse *response));

    MOCK_METHOD2(ListPhysicalPool, void(
                     const ListPhysicalPoolRequest *request,
                     ListPhysicalPoolResponse *response));

    MOCK_METHOD2(ListPhysicalPoolsInPoolset,
                 void(const ListPhysicalPoolsInPoolsetRequest* request,
                      ListPhysicalPoolResponse* response));

    MOCK_METHOD2(CreateLogicalPool, void(
                     const CreateLogicalPoolRequest *request,
                     CreateLogicalPoolResponse *response));

    MOCK_METHOD2(DeleteLogicalPool, void(
                     const DeleteLogicalPoolRequest *request,
                     DeleteLogicalPoolResponse *response));

    MOCK_METHOD2(GetLogicalPool, void(
                     const GetLogicalPoolRequest *request,
                     GetLogicalPoolResponse *response));

    MOCK_METHOD2(ListLogicalPool, void(
                     const ListLogicalPoolRequest *request,
                     ListLogicalPoolResponse *response));

    MOCK_METHOD2(SetLogicalPool, void(const SetLogicalPoolRequest *request,
                                SetLogicalPoolResponse *response));

    MOCK_METHOD2(CreatePoolset, void(
                     const PoolsetRequest *request,
                     PoolsetResponse *response));

     MOCK_METHOD2(DeletePoolset, void(
                     const PoolsetRequest *request,
                     PoolsetResponse *response));

    MOCK_METHOD2(GetPoolset,
        void(const PoolsetRequest *request,
            PoolsetResponse *response));

    MOCK_METHOD2(ListPoolset,
        void(const ListPoolsetRequest *request,
            ListPoolsetResponse *response));

    MOCK_METHOD2(GetChunkServerListInCopySets, void(
        const GetChunkServerListInCopySetsRequest *request,
        GetChunkServerListInCopySetsResponse *response));

    MOCK_METHOD2(GetCopySetsInChunkServer, void(
        const GetCopySetsInChunkServerRequest *request,
        GetCopySetsInChunkServerResponse *response));

    MOCK_METHOD2(GetCopySetsInCluster, void(
        const GetCopySetsInClusterRequest *request,
        GetCopySetsInClusterResponse *response));

    MOCK_METHOD2(GetClusterInfo,
                 void(const GetClusterInfoRequest* request,
                      GetClusterInfoResponse* response));
};

class MockTopologyServiceImpl : public TopologyService {
 public:
    MockTopologyServiceImpl() {}
    MOCK_METHOD4(RegistServer,
                 void(google::protobuf::RpcController* cntl_base,
                      const ServerRegistRequest* request,
                      ServerRegistResponse* response,
                      google::protobuf::Closure* done));

    MOCK_METHOD4(CreateZone,
                 void(google::protobuf::RpcController* cntl_base,
                      const ZoneRequest* request,
                      ZoneResponse* response,
                      google::protobuf::Closure* done));

    MOCK_METHOD4(CreatePhysicalPool,
                 void(google::protobuf::RpcController* cntl_base,
                      const PhysicalPoolRequest* request,
                      PhysicalPoolResponse* response,
                      google::protobuf::Closure* done));

    MOCK_METHOD4(CreatePoolset,
                 void(google::protobuf::RpcController* cntl_base,
                      const PoolsetRequest* request,
                      PoolsetResponse* response,
                      google::protobuf::Closure* done));
};

class MockTopologyStat : public TopologyStat {
 public:
    MockTopologyStat() {}
    MOCK_METHOD2(UpdateChunkServerStat,
                 void(ChunkServerIdType csId,
                      const ChunkServerStat &stat));

    MOCK_METHOD2(GetChunkServerStat,
                 bool(ChunkServerIdType csId,
                      ChunkServerStat *stat));

    MOCK_METHOD2(GetChunkPoolSize, bool(PoolIdType, uint64_t*));
};

}  // namespace topology
}  // namespace mds

namespace chunkserver {

class MockCopysetServiceImpl : public CopysetService {
 public:
    MOCK_METHOD4(CreateCopysetNode2,
        void(::google::protobuf::RpcController *controller,
            const ::curve::chunkserver::CopysetRequest2 *request,
            ::curve::chunkserver::CopysetResponse2 *response,
            google::protobuf::Closure *done));
};

}  // namespace chunkserver
}  // namespace curve

using ::curve::kvstorage::EtcdClientImp;
using ::curve::kvstorage::KVStorageClient;

namespace curve {
namespace kvstorage {

class MockKVStorageClient : public KVStorageClient {
 public:
    virtual ~MockKVStorageClient() {}
    MOCK_METHOD2(Put, int(const std::string&, const std::string&));
    MOCK_METHOD2(Get, int(const std::string&, std::string*));
    MOCK_METHOD3(List,
        int(const std::string&, const std::string&, std::vector<std::string>*));
    MOCK_METHOD1(Delete, int(const std::string&));
    MOCK_METHOD1(TxnN, int(const std::vector<Operation>&));
    MOCK_METHOD3(CompareAndSwap, int(const std::string&, const std::string&,
        const std::string&));
    MOCK_METHOD5(CampaignLeader, int(const std::string&, const std::string&,
        uint32_t, uint32_t, uint64_t*));
    MOCK_METHOD2(LeaderObserve, int(uint64_t, const std::string&));
    MOCK_METHOD2(LeaderKeyExist, bool(uint64_t, uint64_t));
    MOCK_METHOD2(LeaderResign, int(uint64_t, uint64_t));
    MOCK_METHOD1(GetCurrentRevision, int(int64_t *));
    MOCK_METHOD6(ListWithLimitAndRevision,
        int(const std::string&, const std::string&,
        int64_t, int64_t, std::vector<std::string>*, std::string *));
    MOCK_METHOD3(PutRewithRevision, int(const std::string &,
        const std::string &, int64_t *));
    MOCK_METHOD2(DeleteRewithRevision, int(const std::string &, int64_t *));
};

}  // namespace kvstorage
}  // namespace curve

#endif  // TEST_MDS_TOPOLOGY_MOCK_TOPOLOGY_H_
