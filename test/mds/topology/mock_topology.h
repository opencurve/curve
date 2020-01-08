/*
 * Project: curve
 * Created Date: Tue Sep 25 2018
 * Author: xuchaojie
 * Copyright (c) 2018 netease
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

class MockTopologyServiceManager : public TopologyServiceManager {
 public:
    MockTopologyServiceManager(
        std::shared_ptr<Topology> topology,
        std::shared_ptr<curve::mds::copyset::CopysetManager> copysetManager) //NOLINT
        : TopologyServiceManager(topology, copysetManager) {}

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

    MOCK_METHOD2(GetChunkServerListInCopySets, void(
        const GetChunkServerListInCopySetsRequest *request,
        GetChunkServerListInCopySetsResponse *response));

    MOCK_METHOD2(GetCopySetsInChunkServer, void(
        const GetCopySetsInChunkServerRequest *request,
        GetCopySetsInChunkServerResponse *response));

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
};

class MockRepo : public MdsRepo {
 public:
    MockRepo() {}
    ~MockRepo() {}

    MOCK_METHOD5(connectDB, int(
                     const std::string &dbName,
                     const std::string &user,
                     const std::string &url,
                     const std::string &password,
                     uint32_t poolSize));

    MOCK_METHOD0(createAllTables, int());
    MOCK_METHOD0(createDatabase, int());
    MOCK_METHOD0(useDataBase, int());
    MOCK_METHOD0(dropDataBase, int());

    MOCK_METHOD1(InsertChunkServerRepoItem,
                 int(
                     const ChunkServerRepoItem &cr));

    MOCK_METHOD1(LoadChunkServerRepoItems,
                 int(std::vector<ChunkServerRepoItem>
                     *chunkServerRepoItemList));

    MOCK_METHOD1(DeleteChunkServerRepoItem,
                 int(ChunkServerIDType
                     id));

    MOCK_METHOD1(UpdateChunkServerRepoItem,
                 int(
                     const ChunkServerRepoItem &cr));

    MOCK_METHOD2(QueryChunkServerRepoItem,
                 int(ChunkServerIDType
                     id, ChunkServerRepoItem * repo));

    MOCK_METHOD1(InsertServerRepoItem,
                 int(
                     const ServerRepoItem &sr));

    MOCK_METHOD1(LoadServerRepoItems,
                 int(std::vector<ServerRepoItem>
                     *serverList));

    MOCK_METHOD1(DeleteServerRepoItem,
                 int(ServerIDType
                     id));

    MOCK_METHOD1(UpdateServerRepoItem,
                 int(
                     const ServerRepoItem &sr));

    MOCK_METHOD2(QueryServerRepoItem,
                 int(ServerIDType
                     id, ServerRepoItem * repo));

    MOCK_METHOD1(InsertZoneRepoItem,
                 int(
                     const ZoneRepoItem &zr));

    MOCK_METHOD1(LoadZoneRepoItems,
                 int(std::vector<ZoneRepoItem>
                     *zonevector));

    MOCK_METHOD1(DeleteZoneRepoItem,
                 int(ZoneIDType
                     id));

    MOCK_METHOD1(UpdateZoneRepoItem,
                 int(
                     const ZoneRepoItem &zr));

    MOCK_METHOD2(QueryZoneRepoItem,
                 int(ZoneIDType
                     id, ZoneRepoItem * repo));

    MOCK_METHOD1(InsertPhysicalPoolRepoItem,
                 int(
                     const PhysicalPoolRepoItem &pr));

    MOCK_METHOD1(LoadPhysicalPoolRepoItems,
                 int(std::vector<PhysicalPoolRepoItem>
                     *physicalPoolvector));

    MOCK_METHOD1(DeletePhysicalPoolRepoItem,
                 int(PhysicalPoolIDType
                     id));

    MOCK_METHOD1(UpdatePhysicalPoolRepoItem,
                 int(
                     const PhysicalPoolRepoItem &pr));

    MOCK_METHOD2(QueryPhysicalPoolRepoItem,
                 int(PhysicalPoolIDType
                     id, PhysicalPoolRepoItem * repo));

    MOCK_METHOD1(InsertLogicalPoolRepoItem,
                 int(
                     const LogicalPoolRepoItem &lr));

    MOCK_METHOD1(LoadLogicalPoolRepoItems,
                 int(std::vector<LogicalPoolRepoItem>
                     *logicalPoolList));

    MOCK_METHOD1(DeleteLogicalPoolRepoItem,
                 int(LogicalPoolIDType
                     id));

    MOCK_METHOD1(UpdateLogicalPoolRepoItem,
                 int(
                     const LogicalPoolRepoItem &lr));

    MOCK_METHOD2(QueryLogicalPoolRepoItem,
                 int(LogicalPoolIDType
                     id, LogicalPoolRepoItem * repo));

    MOCK_METHOD1(InsertCopySetRepoItem,
                 int(
                     const CopySetRepoItem &cr));

    MOCK_METHOD1(LoadCopySetRepoItems,
                 int(std::vector<CopySetRepoItem>
                     *copySetList));

    MOCK_METHOD2(DeleteCopySetRepoItem,
                 int(CopySetIDType
                     id, LogicalPoolIDType
                     lid));

    MOCK_METHOD1(UpdateCopySetRepoItem,
                 int(
                     const CopySetRepoItem &cr));

    MOCK_METHOD3(QueryCopySetRepoItem,
                 int(CopySetIDType
                     id,
                     LogicalPoolIDType
                     lid,
                     CopySetRepoItem * repo));

    MOCK_METHOD1(InsertSessionRepoItem,
                 int(const SessionRepoItem &r));

    MOCK_METHOD1(LoadSessionRepoItems,
                 int(std::vector<SessionRepoItem> *sessionList));

    MOCK_METHOD1(DeleteSessionRepoItem,
                 int(const std::string &sessionID));

    MOCK_METHOD1(UpdateSessionRepoItem,
                 int(const SessionRepoItem &r));

    MOCK_METHOD2(QuerySessionRepoItem,
                 int(const std::string &sessionID, SessionRepoItem *r));

    MOCK_METHOD1(InsertClusterInfoRepoItem,
                 int(const ClusterInfoRepoItem &r));

    MOCK_METHOD1(LoadClusterInfoRepoItems,
                 int(std::vector<ClusterInfoRepoItem> *list));
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


#endif  // TEST_MDS_TOPOLOGY_MOCK_TOPOLOGY_H_
