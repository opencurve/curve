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
 * Created Date: 2021-09-07
 * Author: wanghai01
 */

#include <brpc/channel.h>
#include <brpc/controller.h>
#include <brpc/server.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <google/protobuf/util/message_differencer.h>

#include "curvefs/proto/topology.pb.h"
#include "curvefs/src/mds/common/mds_define.h"
#include "curvefs/src/mds/topology/topology_manager.h"
#include "curvefs/test/mds/mock/mock_metaserver.h"
#include "curvefs/test/mds/mock/mock_metaserver_client.h"
#include "curvefs/test/mds/mock/mock_topology.h"

namespace curvefs {
namespace mds {
namespace topology {

using ::testing::Return;
using ::testing::_;
using ::testing::AnyOf;
using ::testing::SetArgPointee;
using ::testing::Invoke;

using curvefs::mds::MetaserverClient;
using curvefs::metaserver::MockMetaserverService;
using google::protobuf::util::MessageDifferencer;

class TestTopologyManager : public ::testing::Test {
 protected:
    TestTopologyManager() {}
    virtual ~TestTopologyManager() {}
    virtual void SetUp() {
        std::string addr = "127.0.0.1:13145";
        server_ = new brpc::Server();

        idGenerator_ = std::make_shared<MockIdGenerator>();
        tokenGenerator_ = std::make_shared<MockTokenGenerator>();
        storage_ = std::make_shared<MockStorage>();
        topology_ = std::make_shared<TopologyImpl>(idGenerator_,
                                                   tokenGenerator_, storage_);
        TopologyOption topologyOption;
        topologyOption.initialCopysetNumber = 1;
        topologyOption.minAvailableCopysetNum = 1;
        topologyOption.createPartitionNumber = 3;

        MetaserverOptions metaserverOptions;
        metaserverOptions.metaserverAddr = addr;
        metaserverOptions.rpcTimeoutMs = 500;
        mockMetaserverClient_ =
            std::make_shared<MockMetaserverClient>(metaserverOptions);
        serviceManager_ =
            std::make_shared<TopologyManager>(topology_, mockMetaserverClient_);
        serviceManager_->Init(topologyOption);

        mockMetaserverService_ = new MockMetaserverService();
        ASSERT_EQ(server_->AddService(mockMetaserverService_,
                                      brpc::SERVER_DOESNT_OWN_SERVICE),
                  0);

        ASSERT_EQ(0, server_->Start(addr.c_str(), nullptr));
        listenAddr_ = server_->listen_address();
    }

    virtual void TearDown() {
        idGenerator_ = nullptr;
        tokenGenerator_ = nullptr;
        storage_ = nullptr;
        topology_ = nullptr;
        mockMetaserverClient_ = nullptr;
        serviceManager_ = nullptr;

        server_->Stop(0);
        server_->Join();
        delete server_;
        server_ = nullptr;
        delete mockMetaserverService_;
        mockMetaserverService_ = nullptr;
    }

 protected:
    void PrepareAddPool(PoolIdType id = 0x11,
                        const std::string &name = "testPool",
                        const Pool::RedundanceAndPlaceMentPolicy &rap =
                            Pool::RedundanceAndPlaceMentPolicy(),
                        uint64_t createTime = 0x888) {
        Pool pool(id, name, rap, createTime);
        EXPECT_CALL(*storage_, StoragePool(_)).WillOnce(Return(true));

        int ret = topology_->AddPool(pool);
        ASSERT_EQ(TopoStatusCode::TOPO_OK, ret);
    }

    void PrepareAddZone(ZoneIdType id = 0x21,
                        const std::string &name = "testZone",
                        PoolIdType poolId = 0x11) {
        Zone zone(id, name, poolId);
        EXPECT_CALL(*storage_, StorageZone(_)).WillOnce(Return(true));

        int ret = topology_->AddZone(zone);
        ASSERT_EQ(TopoStatusCode::TOPO_OK, ret)
            << "should have PrepareAddPool()";
    }

    void PrepareAddServer(ServerIdType id = 0x31,
                          const std::string &hostName = "testServer",
                          const std::string &internalHostIp = "testInternalIp",
                          uint32_t internalPort = 0,
                          const std::string &externalHostIp = "testExternalIp",
                          uint32_t externalPort = 0, ZoneIdType zoneId = 0x21,
                          PoolIdType poolId = 0x11) {
        Server server(id, hostName, internalHostIp, internalPort,
                      externalHostIp, externalPort, zoneId, poolId);
        EXPECT_CALL(*storage_, StorageServer(_)).WillOnce(Return(true));

        int ret = topology_->AddServer(server);
        ASSERT_EQ(TopoStatusCode::TOPO_OK, ret)
            << "should have PrepareAddZone()";
    }

    void PrepareAddMetaServer(
        MetaServerIdType id = 0x41,
        const std::string &hostname = "testMetaserver",
        const std::string &token = "testToken", ServerIdType serverId = 0x31,
        const std::string &hostIp = "testInternalIp", uint32_t port = 0,
        const std::string &externalHostIp = "testExternalIp",
        uint32_t externalPort = 0,
        OnlineState onlineState = OnlineState::ONLINE) {
        MetaServer ms(id, hostname, token, serverId, hostIp, port,
                      externalHostIp, externalPort,
                      onlineState);

        EXPECT_CALL(*storage_, StorageMetaServer(_)).WillOnce(Return(true));
        int ret = topology_->AddMetaServer(ms);
        ASSERT_EQ(TopoStatusCode::TOPO_OK, ret)
            << "should have PrepareAddServer()";
    }

    void PrepareAddCopySet(CopySetIdType copysetId, PoolIdType poolId,
                           const std::set<MetaServerIdType> &members) {
        CopySetInfo cs(poolId, copysetId);
        cs.SetCopySetMembers(members);
        EXPECT_CALL(*storage_, StorageCopySet(_)).WillOnce(Return(true));
        int ret = topology_->AddCopySet(cs);
        ASSERT_EQ(TopoStatusCode::TOPO_OK, ret)
            << "should have PrepareAddPool()";
    }

    void PrepareAddPartition(FsIdType fsId, PoolIdType poolId,
                             CopySetIdType csId, PartitionIdType pId,
                             uint64_t idStart, uint64_t idEnd,
                             uint64_t txId = 0) {
        Partition partition(fsId, poolId, csId, pId, idStart, idEnd);
        partition.SetTxId(txId);
        EXPECT_CALL(*storage_, StoragePartition(_)).WillOnce(Return(true));
        int ret = topology_->AddPartition(partition);
        ASSERT_EQ(TopoStatusCode::TOPO_OK, ret)
            << "should have PrepareAddPartition()";
    }

    void PrepareTopo() {
        FsIdType fsId = 0x01;
        PoolIdType poolId = 0x11;
        CopySetIdType copysetId = 0x51;
        PartitionIdType pId1 = 0x61;
        PartitionIdType pId2 = 0x62;
        PartitionIdType pId3 = 0x63;

        Pool::RedundanceAndPlaceMentPolicy policy;
        policy.replicaNum = 3;
        policy.copysetNum = 0;
        policy.zoneNum = 3;
        PrepareAddPool(poolId, "pool1", policy);
        PrepareAddZone(0x21, "zone1", poolId);
        PrepareAddZone(0x22, "zone2", poolId);
        PrepareAddZone(0x23, "zone3", poolId);
        PrepareAddServer(0x31, "server1", "127.0.0.1", 0, "127.0.0.1", 0, 0x21,
                         0x11);
        PrepareAddServer(0x32, "server2", "127.0.0.1", 0, "127.0.0.1", 0, 0x22,
                         0x11);
        PrepareAddServer(0x33, "server3", "127.0.0.1", 0, "127.0.0.1", 0, 0x23,
                         0x11);
        PrepareAddMetaServer(0x41, "ms1", "token1", 0x31, "127.0.0.1", 7777,
                             "ip2", 8888);
        PrepareAddMetaServer(0x42, "ms2", "token2", 0x32, "127.0.0.1", 7778,
                             "ip2", 8888);
        PrepareAddMetaServer(0x43, "ms3", "token3", 0x33, "127.0.0.1", 7779,
                             "ip2", 8888);

        std::set<MetaServerIdType> replicas;
        replicas.insert(0x41);
        replicas.insert(0x42);
        replicas.insert(0x43);
        PrepareAddCopySet(copysetId, poolId, replicas);
        PrepareAddPartition(fsId, poolId, copysetId, pId1, 1, 100, 2);
        PrepareAddPartition(fsId, poolId, copysetId, pId2, 1, 100, 2);
        PrepareAddPartition(fsId + 1, poolId, copysetId, pId3, 1, 100, 2);
    }

 protected:
    std::shared_ptr<MockIdGenerator> idGenerator_;
    std::shared_ptr<MockTokenGenerator> tokenGenerator_;
    std::shared_ptr<MockStorage> storage_;
    std::shared_ptr<Topology> topology_;
    std::shared_ptr<MockMetaserverClient> mockMetaserverClient_;
    std::shared_ptr<TopologyManager> serviceManager_;

    butil::EndPoint listenAddr_;
    brpc::Server *server_;
    MockMetaserverService *mockMetaserverService_;
};

TEST_F(TestTopologyManager, test_RegistMetaServer_SuccessWithExIp) {
    MetaServerIdType msId = 0x41;
    ServerIdType serverId = 0x31;
    std::string token = "token";

    PrepareAddPool();
    PrepareAddZone();
    PrepareAddServer(serverId, "testServer", "testInternalIp", 0, "externalIp1",
                     0);

    MetaServerRegistRequest request;
    request.set_hostname("metaserver");
    request.set_internalip("testInternalIp");
    request.set_internalport(0);
    request.set_externalip("externalIp1");
    request.set_externalport(0);

    MetaServerRegistResponse response;

    EXPECT_CALL(*tokenGenerator_, GenToken()).WillOnce(Return(token));
    EXPECT_CALL(*idGenerator_, GenMetaServerId()).WillOnce(Return(msId));

    EXPECT_CALL(*storage_, StorageMetaServer(_)).WillOnce(Return(true));
    serviceManager_->RegistMetaServer(&request, &response);

    ASSERT_EQ(TopoStatusCode::TOPO_OK, response.statuscode());
    ASSERT_TRUE(response.has_metaserverid());
    ASSERT_EQ(msId, response.metaserverid());
    ASSERT_TRUE(response.has_token());
    ASSERT_EQ(token, response.token());
    MetaServer metaserver;
    ASSERT_TRUE(topology_->GetMetaServer(msId, &metaserver));
    ASSERT_EQ("externalIp1", metaserver.GetExternalIp());


    // test regist same metaserver
    serviceManager_->RegistMetaServer(&request, &response);
    ASSERT_EQ(TopoStatusCode::TOPO_OK, response.statuscode());

    // test regist same metaserver which already has copyset
    std::set<MetaServerIdType> replicas;
    replicas.insert(msId);
    replicas.insert(msId + 1);
    replicas.insert(msId + 2);
    PrepareAddCopySet(1, 0x11, replicas);

    serviceManager_->RegistMetaServer(&request, &response);
    ASSERT_EQ(TopoStatusCode::TOPO_METASERVER_EXIST, response.statuscode());
}

TEST_F(TestTopologyManager, test_RegistMetaServer_ExIpNotMatch) {
    MetaServerIdType csId = 0x41;
    ServerIdType serverId = 0x31;
    std::string token = "token";

    PrepareAddPool();
    PrepareAddZone();
    PrepareAddServer(serverId, "testServer", "testInternalIp", 0, "externalIp1",
                     0);

    MetaServerRegistRequest request;
    request.set_hostname("metaserver");
    request.set_internalip("testInternalIp");
    request.set_internalport(0);
    request.set_externalip("externalIp2");
    request.set_externalport(0);

    MetaServerRegistResponse response;

    EXPECT_CALL(*tokenGenerator_, GenToken()).WillOnce(Return(token));
    EXPECT_CALL(*idGenerator_, GenMetaServerId()).WillOnce(Return(csId));

    serviceManager_->RegistMetaServer(&request, &response);

    ASSERT_EQ(TopoStatusCode::TOPO_INTERNAL_ERROR, response.statuscode());
}

TEST_F(TestTopologyManager, test_RegistMetaServer_ServerNotFound) {
    ServerIdType serverId = 0x31;
    std::string token = "token";

    PrepareAddPool();
    PrepareAddZone();
    PrepareAddServer(serverId, "testServer", "testInternalIp", 0, "externalIp1",
                     0);

    MetaServerRegistRequest request;
    request.set_hostname("metaserver");
    request.set_internalip("unExistIp");
    request.set_internalport(100);
    request.set_externalip("externalIp1");
    request.set_externalport(0);

    MetaServerRegistResponse response;

    serviceManager_->RegistMetaServer(&request, &response);

    ASSERT_EQ(TopoStatusCode::TOPO_SERVER_NOT_FOUND, response.statuscode());
}

TEST_F(TestTopologyManager, test_RegistMetaServer_AllocateIdFail) {
    ServerIdType serverId = 0x31;
    std::string token = "token";

    PrepareAddPool();
    PrepareAddZone();
    PrepareAddServer(serverId, "testServer", "testInternalIp", 0, "externalIp1",
                     0);

    MetaServerRegistRequest request;
    request.set_hostname("metaserver");
    request.set_internalip("testInternalIp");
    request.set_internalport(100);
    request.set_externalip("externalIp1");
    request.set_externalport(0);

    MetaServerRegistResponse response;

    EXPECT_CALL(*idGenerator_, GenMetaServerId())
        .WillOnce(Return(UNINITIALIZE_ID));

    serviceManager_->RegistMetaServer(&request, &response);

    ASSERT_EQ(TopoStatusCode::TOPO_ALLOCATE_ID_FAIL, response.statuscode());
}

TEST_F(TestTopologyManager, test_RegistMetaServer_AddMetaServerFail) {
    MetaServerIdType csId = 0x41;
    ServerIdType serverId = 0x31;
    std::string token = "token";

    PrepareAddPool();
    PrepareAddZone();
    PrepareAddServer(serverId, "testServer", "testInternalIp", 0, "externalIp1",
                     0);

    MetaServerRegistRequest request;
    request.set_hostname("metaserver");
    request.set_internalip("testInternalIp");
    request.set_internalport(100);
    request.set_externalip("externalIp1");
    request.set_externalport(0);

    MetaServerRegistResponse response;

    EXPECT_CALL(*tokenGenerator_, GenToken()).WillOnce(Return(token));
    EXPECT_CALL(*idGenerator_, GenMetaServerId()).WillOnce(Return(csId));

    EXPECT_CALL(*storage_, StorageMetaServer(_)).WillOnce(Return(false));
    serviceManager_->RegistMetaServer(&request, &response);

    ASSERT_EQ(TopoStatusCode::TOPO_STORGE_FAIL, response.statuscode());
    ASSERT_FALSE(response.has_metaserverid());
    ASSERT_FALSE(response.has_token());
}

TEST_F(TestTopologyManager, test_ListMetaServer_ByIdSuccess) {
    MetaServerIdType csId1 = 0x41;
    MetaServerIdType csId2 = 0x42;
    ServerIdType serverId = 0x31;

    PrepareAddPool();
    PrepareAddZone();
    PrepareAddServer(serverId, "server", "ip1", 0, "ip2", 0);
    PrepareAddMetaServer(csId1, "ms1", "token1", serverId, "ip1", 100, "ip2",
                         100);
    PrepareAddMetaServer(csId2, "ms2", "token2", serverId, "ip1", 200, "ip2",
                         200);

    ListMetaServerRequest request;
    request.set_serverid(serverId);

    ListMetaServerResponse response;

    serviceManager_->ListMetaServer(&request, &response);

    ASSERT_EQ(TopoStatusCode::TOPO_OK, response.statuscode());

    ASSERT_EQ(2, response.metaserverinfos_size());

    ASSERT_THAT(response.metaserverinfos(0).metaserverid(),
                AnyOf(csId1, csId2));
    ASSERT_EQ("ip1", response.metaserverinfos(0).internalip());
    ASSERT_EQ("ip2", response.metaserverinfos(0).externalip());
    ASSERT_THAT(response.metaserverinfos(0).internalport(), AnyOf(100, 200));

    ASSERT_THAT(response.metaserverinfos(1).metaserverid(),
                AnyOf(csId1, csId2));
    ASSERT_EQ("ip1", response.metaserverinfos(1).internalip());
    ASSERT_EQ("ip2", response.metaserverinfos(1).externalip());
    ASSERT_THAT(response.metaserverinfos(1).internalport(), AnyOf(100, 200));
}

TEST_F(TestTopologyManager, test_ListMetaServer_ServerNotFound) {
    MetaServerIdType csId1 = 0x41;
    MetaServerIdType csId2 = 0x42;
    ServerIdType serverId = 0x31;

    PrepareAddPool();
    PrepareAddZone();
    PrepareAddServer(serverId, "server", "ip1", 0, "ip2", 0);
    PrepareAddMetaServer(csId1, "ms1", "token1", serverId, "ip1", 100, "ip2",
                         100);
    PrepareAddMetaServer(csId2, "ms2", "token2", serverId, "ip1", 200, "ip2",
                         200);

    ListMetaServerRequest request;
    request.set_serverid(++serverId);

    ListMetaServerResponse response;

    serviceManager_->ListMetaServer(&request, &response);

    ASSERT_EQ(TopoStatusCode::TOPO_SERVER_NOT_FOUND, response.statuscode());

    ASSERT_EQ(0, response.metaserverinfos_size());
}

TEST_F(TestTopologyManager, test_GetMetaServer_ByIdSuccess) {
    MetaServerIdType csId1 = 0x41;
    ServerIdType serverId = 0x31;

    PrepareAddPool();
    PrepareAddZone();
    PrepareAddServer(serverId, "server", "ip1", 0, "ip2", 0);
    PrepareAddMetaServer(csId1, "ms1", "token1", serverId, "ip1", 100, "ip2",
                         100);

    GetMetaServerInfoRequest request;
    request.set_metaserverid(csId1);

    GetMetaServerInfoResponse response;
    serviceManager_->GetMetaServer(&request, &response);

    ASSERT_EQ(TopoStatusCode::TOPO_OK, response.statuscode());
    ASSERT_TRUE(response.has_metaserverinfo());

    ASSERT_EQ(csId1, response.metaserverinfo().metaserverid());
    ASSERT_EQ("ip1", response.metaserverinfo().internalip());
    ASSERT_EQ(100, response.metaserverinfo().internalport());
    ASSERT_EQ("ip2", response.metaserverinfo().externalip());
    ASSERT_EQ(100, response.metaserverinfo().externalport());
}

TEST_F(TestTopologyManager, test_GetMetaServer_MetaServerNotFound) {
    MetaServerIdType csId1 = 0x41;
    ServerIdType serverId = 0x31;

    PrepareAddPool();
    PrepareAddZone();
    PrepareAddServer(serverId, "server", "ip1", 0, "ip2", 0);
    PrepareAddMetaServer(csId1, "ms1", "token1", serverId, "ip1", 100, "ip2",
                         100);

    GetMetaServerInfoRequest request;
    request.set_metaserverid(++csId1);

    GetMetaServerInfoResponse response;
    serviceManager_->GetMetaServer(&request, &response);

    ASSERT_EQ(TopoStatusCode::TOPO_METASERVER_NOT_FOUND, response.statuscode());
    ASSERT_FALSE(response.has_metaserverinfo());
}

TEST_F(TestTopologyManager, test_DeleteMetaServer_success) {
    MetaServerIdType csId1 = 0x41;
    ServerIdType serverId = 0x31;

    PrepareAddPool();
    PrepareAddZone();
    PrepareAddServer(serverId, "server", "ip1", 0, "ip2", 0);
    PrepareAddMetaServer(csId1, "ms1", "token1", serverId, "ip1", 100, "ip2",
                         100);

    DeleteMetaServerRequest request;
    request.set_metaserverid(csId1);

    EXPECT_CALL(*storage_, DeleteMetaServer(_)).WillOnce(Return(true));

    DeleteMetaServerResponse response;
    serviceManager_->DeleteMetaServer(&request, &response);

    ASSERT_EQ(TopoStatusCode::TOPO_OK, response.statuscode());
}

TEST_F(TestTopologyManager, test_DeleteMetaServer_MetaServerNotFound) {
    MetaServerIdType csId1 = 0x41;
    ServerIdType serverId = 0x31;

    PrepareAddPool();
    PrepareAddZone();
    PrepareAddServer(serverId, "server", "ip1", 0, "ip2", 0);
    PrepareAddMetaServer(csId1, "ms1", "token1", serverId, "ip1", 100, "ip2",
                         100);

    DeleteMetaServerRequest request;
    request.set_metaserverid(++csId1);

    DeleteMetaServerResponse response;
    serviceManager_->DeleteMetaServer(&request, &response);

    ASSERT_EQ(TopoStatusCode::TOPO_METASERVER_NOT_FOUND, response.statuscode());
}

TEST_F(TestTopologyManager, test_RegistServer_ByZoneAndPoolNameSuccess) {
    ServerIdType id = 0x31;
    PoolIdType poolId = 0x11;
    ZoneIdType zoneId = 0x21;
    PrepareAddPool(poolId, "pool1");
    PrepareAddZone(zoneId, "zone1", poolId);

    ServerRegistRequest request;
    request.set_hostname("server1");
    request.set_internalip("ip1");
    request.set_externalip("ip2");
    request.set_zonename("zone1");
    request.set_poolname("pool1");

    ServerRegistResponse response;

    EXPECT_CALL(*idGenerator_, GenServerId()).WillOnce(Return(id));
    EXPECT_CALL(*storage_, StorageServer(_)).WillOnce(Return(true));

    serviceManager_->RegistServer(&request, &response);

    ASSERT_EQ(TopoStatusCode::TOPO_OK, response.statuscode());
    ASSERT_TRUE(response.has_serverid());
    ASSERT_EQ(id, response.serverid());
}

TEST_F(TestTopologyManager, test_RegistServer_PoolNotFound) {
    PoolIdType poolId = 0x11;
    ZoneIdType zoneId = 0x21;
    PrepareAddPool(poolId, "pool1");
    PrepareAddZone(zoneId, "test", poolId);

    ServerRegistRequest request;
    request.set_hostname("server1");
    request.set_internalip("ip1");
    request.set_externalip("ip2");
    request.set_zonename("test");
    request.set_poolname("pool2");

    ServerRegistResponse response;

    serviceManager_->RegistServer(&request, &response);

    ASSERT_EQ(TopoStatusCode::TOPO_POOL_NOT_FOUND, response.statuscode());
    ASSERT_FALSE(response.has_serverid());
}

TEST_F(TestTopologyManager, test_RegistServer_ZoneNotFound) {
    ServerIdType id = 0x31;
    PoolIdType poolId = 0x11;
    ZoneIdType zoneId = 0x21;
    PrepareAddPool(poolId, "pool1");
    PrepareAddZone(zoneId, "zone1", poolId);

    ServerRegistRequest request;
    request.set_hostname("server1");
    request.set_internalip("ip1");
    request.set_externalip("ip2");
    request.set_zonename("zone2");
    request.set_poolname("pool1");

    ServerRegistResponse response;

    serviceManager_->RegistServer(&request, &response);

    ASSERT_EQ(TopoStatusCode::TOPO_ZONE_NOT_FOUND, response.statuscode());
    ASSERT_FALSE(response.has_serverid());
}

TEST_F(TestTopologyManager, test_RegistServer_AllocateIdFail) {
    ServerIdType id = 0x31;
    PoolIdType poolId = 0x11;
    ZoneIdType zoneId = 0x21;
    PrepareAddPool(poolId, "pool");
    PrepareAddZone(zoneId, "zone", poolId);

    ServerRegistRequest request;
    request.set_hostname("server1");
    request.set_internalip("ip1");
    request.set_externalip("ip2");
    request.set_zonename("zone");
    request.set_poolname("pool");

    ServerRegistResponse response;

    EXPECT_CALL(*idGenerator_, GenServerId()).WillOnce(Return(UNINITIALIZE_ID));

    serviceManager_->RegistServer(&request, &response);

    ASSERT_EQ(TopoStatusCode::TOPO_ALLOCATE_ID_FAIL, response.statuscode());
}

TEST_F(TestTopologyManager, test_RegistServer_AddServerFail) {
    ServerIdType id = 0x31;
    PoolIdType poolId = 0x11;
    ZoneIdType zoneId = 0x21;
    PrepareAddPool(poolId, "pool");
    PrepareAddZone(zoneId, "zone", poolId);

    ServerRegistRequest request;
    request.set_hostname("server1");
    request.set_internalip("ip1");
    request.set_externalip("ip2");
    request.set_zonename("zone");
    request.set_poolname("pool");

    ServerRegistResponse response;

    EXPECT_CALL(*idGenerator_, GenServerId()).WillOnce(Return(id));
    EXPECT_CALL(*storage_, StorageServer(_)).WillOnce(Return(false));

    serviceManager_->RegistServer(&request, &response);

    ASSERT_EQ(TopoStatusCode::TOPO_STORGE_FAIL, response.statuscode());
}

TEST_F(TestTopologyManager, test_GetServer_ByIdSuccess) {
    PoolIdType poolId = 0x11;
    ZoneIdType zoneId = 0x21;
    ServerIdType serverId = 0x31;
    PrepareAddPool(poolId);
    PrepareAddZone(zoneId);
    PrepareAddServer(serverId, "hostname1", "ip1", 0, "ip2", 0, zoneId, poolId);

    GetServerRequest request;
    request.set_serverid(serverId);

    GetServerResponse response;

    serviceManager_->GetServer(&request, &response);

    ASSERT_EQ(TopoStatusCode::TOPO_OK, response.statuscode());
    ASSERT_TRUE(response.has_serverinfo());
    ASSERT_EQ(serverId, response.serverinfo().serverid());
    ASSERT_EQ("hostname1", response.serverinfo().hostname());
    ASSERT_EQ("ip1", response.serverinfo().internalip());
    ASSERT_EQ(0, response.serverinfo().internalport());
    ASSERT_EQ("ip2", response.serverinfo().externalip());
    ASSERT_EQ(0, response.serverinfo().externalport());
    ASSERT_EQ(zoneId, response.serverinfo().zoneid());
    ASSERT_EQ(poolId, response.serverinfo().poolid());
}

TEST_F(TestTopologyManager, test_GetServer_ByNameSuccess) {
    PoolIdType poolId = 0x11;
    ZoneIdType zoneId = 0x21;
    ServerIdType serverId = 0x31;
    PrepareAddPool(poolId);
    PrepareAddZone(zoneId);
    PrepareAddServer(serverId, "hostname1", "ip1", 0, "ip2", 0, zoneId, poolId);

    GetServerRequest request;
    request.set_hostname("hostname1");

    GetServerResponse response;

    serviceManager_->GetServer(&request, &response);

    ASSERT_EQ(TopoStatusCode::TOPO_OK, response.statuscode());
    ASSERT_TRUE(response.has_serverinfo());
    ASSERT_EQ(serverId, response.serverinfo().serverid());
    ASSERT_EQ("hostname1", response.serverinfo().hostname());
    ASSERT_EQ("ip1", response.serverinfo().internalip());
    ASSERT_EQ(0, response.serverinfo().internalport());
    ASSERT_EQ("ip2", response.serverinfo().externalip());
    ASSERT_EQ(0, response.serverinfo().externalport());
    ASSERT_EQ(zoneId, response.serverinfo().zoneid());
    ASSERT_EQ(poolId, response.serverinfo().poolid());
}

TEST_F(TestTopologyManager, test_GetServer_ByInternalIpSuccess) {
    PoolIdType poolId = 0x11;
    ZoneIdType zoneId = 0x21;
    ServerIdType serverId = 0x31;
    PrepareAddPool(poolId);
    PrepareAddZone(zoneId);
    PrepareAddServer(serverId, "hostname1", "ip1", 0, "ip2", 0, zoneId, poolId);

    GetServerRequest request;
    request.set_hostip("ip1");

    GetServerResponse response;

    serviceManager_->GetServer(&request, &response);

    ASSERT_EQ(TopoStatusCode::TOPO_OK, response.statuscode());
    ASSERT_TRUE(response.has_serverinfo());
    ASSERT_EQ(serverId, response.serverinfo().serverid());
    ASSERT_EQ("hostname1", response.serverinfo().hostname());
    ASSERT_EQ("ip1", response.serverinfo().internalip());
    ASSERT_EQ(0, response.serverinfo().internalport());
    ASSERT_EQ("ip2", response.serverinfo().externalip());
    ASSERT_EQ(0, response.serverinfo().externalport());
    ASSERT_EQ(zoneId, response.serverinfo().zoneid());
    ASSERT_EQ(poolId, response.serverinfo().poolid());
}

TEST_F(TestTopologyManager, test_GetServer_ByExternalIpSuccess) {
    PoolIdType poolId = 0x11;
    ZoneIdType zoneId = 0x21;
    ServerIdType serverId = 0x31;
    PrepareAddPool(poolId);
    PrepareAddZone(zoneId);
    PrepareAddServer(serverId, "hostname1", "ip1", 0, "ip2", 0, zoneId, poolId);

    GetServerRequest request;
    request.set_hostip("ip2");

    GetServerResponse response;

    serviceManager_->GetServer(&request, &response);

    ASSERT_EQ(TopoStatusCode::TOPO_OK, response.statuscode());
    ASSERT_TRUE(response.has_serverinfo());
    ASSERT_EQ(serverId, response.serverinfo().serverid());
    ASSERT_EQ("hostname1", response.serverinfo().hostname());
    ASSERT_EQ("ip1", response.serverinfo().internalip());
    ASSERT_EQ(0, response.serverinfo().internalport());
    ASSERT_EQ("ip2", response.serverinfo().externalip());
    ASSERT_EQ(0, response.serverinfo().externalport());
    ASSERT_EQ(zoneId, response.serverinfo().zoneid());
    ASSERT_EQ(poolId, response.serverinfo().poolid());
}

TEST_F(TestTopologyManager, test_GetServer_ServerNotFound) {
    PoolIdType poolId = 0x11;
    ZoneIdType zoneId = 0x21;
    ServerIdType serverId = 0x31;
    PrepareAddPool(poolId);
    PrepareAddZone(zoneId);
    PrepareAddServer(serverId, "hostname1", "ip1", 0, "ip2", 0, zoneId, poolId);

    GetServerRequest request;
    request.set_serverid(++serverId);

    GetServerResponse response;

    serviceManager_->GetServer(&request, &response);

    ASSERT_EQ(TopoStatusCode::TOPO_SERVER_NOT_FOUND, response.statuscode());
    ASSERT_FALSE(response.has_serverinfo());
}

TEST_F(TestTopologyManager, test_GetServer_ByNameServerNotFound) {
    PoolIdType poolId = 0x11;
    ZoneIdType zoneId = 0x21;
    ServerIdType serverId = 0x31;
    PrepareAddPool(poolId);
    PrepareAddZone(zoneId);
    PrepareAddServer(serverId, "hostname1", "ip1", 0, "ip2", 0, zoneId, poolId);

    GetServerRequest request;
    request.set_hostname("hostname2");

    GetServerResponse response;

    serviceManager_->GetServer(&request, &response);

    ASSERT_EQ(TopoStatusCode::TOPO_SERVER_NOT_FOUND, response.statuscode());
    ASSERT_FALSE(response.has_serverinfo());
}

TEST_F(TestTopologyManager, test_GetServer_ByIpServerNotFound) {
    PoolIdType poolId = 0x11;
    ZoneIdType zoneId = 0x21;
    ServerIdType serverId = 0x31;
    PrepareAddPool(poolId);
    PrepareAddZone(zoneId);
    PrepareAddServer(serverId, "hostname1", "ip1", 0, "ip2", 0, zoneId, poolId);

    GetServerRequest request;
    request.set_hostip("ip3");

    GetServerResponse response;

    serviceManager_->GetServer(&request, &response);

    ASSERT_EQ(TopoStatusCode::TOPO_SERVER_NOT_FOUND, response.statuscode());
    ASSERT_FALSE(response.has_serverinfo());
}

TEST_F(TestTopologyManager, test_DeleteServer_success) {
    PoolIdType poolId = 0x11;
    ZoneIdType zoneId = 0x21;
    ServerIdType serverId = 0x31;
    PrepareAddPool(poolId);
    PrepareAddZone(zoneId);
    PrepareAddServer(serverId, "hostname1", "ip1", 0, "ip2", 0, zoneId, poolId);

    DeleteServerRequest request;
    request.set_serverid(serverId);

    DeleteServerResponse response;

    EXPECT_CALL(*storage_, DeleteServer(_)).WillOnce(Return(true));

    serviceManager_->DeleteServer(&request, &response);

    ASSERT_EQ(TopoStatusCode::TOPO_OK, response.statuscode());
}

TEST_F(TestTopologyManager, test_DeleteServerHaveMetaserver_success) {
    PoolIdType poolId = 0x11;
    ZoneIdType zoneId = 0x21;
    ServerIdType serverId = 0x31;
    PrepareAddPool(poolId);
    PrepareAddZone(zoneId);
    PrepareAddServer(serverId, "hostname1", "ip1", 0, "ip2", 0, zoneId, poolId);
    PrepareAddMetaServer(0x41, "ms1", "token1", 0x31, "ip1", 0, "ip2", 8888,
        OnlineState::OFFLINE);

    DeleteServerRequest request;
    request.set_serverid(serverId);

    DeleteServerResponse response;

    EXPECT_CALL(*storage_, DeleteMetaServer(_)).WillOnce(Return(true));
    EXPECT_CALL(*storage_, DeleteServer(_)).WillOnce(Return(true));

    serviceManager_->DeleteServer(&request, &response);

    ASSERT_EQ(TopoStatusCode::TOPO_OK, response.statuscode());
}

TEST_F(TestTopologyManager, test_DeleteServerHaveMetaserver_fail) {
    PoolIdType poolId = 0x11;
    ZoneIdType zoneId = 0x21;
    ServerIdType serverId = 0x31;
    PrepareAddPool(poolId);
    PrepareAddZone(zoneId);
    PrepareAddServer(serverId, "hostname1", "ip1", 0, "ip2", 0, zoneId, poolId);
    PrepareAddMetaServer(0x41, "ms1", "token1", 0x31, "ip1", 0, "ip2", 8888);
    DeleteServerRequest request;
    request.set_serverid(serverId);

    DeleteServerResponse response;

    serviceManager_->DeleteServer(&request, &response);

    ASSERT_EQ(TopoStatusCode::TOPO_CANNOT_REMOVE_NOT_OFFLINE,
        response.statuscode());
}

TEST_F(TestTopologyManager, test_ListZoneServer_ByIdSuccess) {
    PoolIdType poolId = 0x11;
    ZoneIdType zoneId = 0x21;
    ServerIdType serverId = 0x31;
    ServerIdType serverId2 = 0x32;
    PrepareAddPool(poolId);
    PrepareAddZone(zoneId);
    PrepareAddServer(serverId, "hostname1", "ip1", 0, "ip2", 0, zoneId, poolId);
    PrepareAddServer(serverId2, "hostname1", "ip3", 0, "ip4", 0, zoneId,
                     poolId);

    ListZoneServerRequest request;
    request.set_zoneid(zoneId);

    ListZoneServerResponse response;
    serviceManager_->ListZoneServer(&request, &response);

    ASSERT_EQ(TopoStatusCode::TOPO_OK, response.statuscode());
    ASSERT_EQ(2, response.serverinfo_size());

    ASSERT_THAT(response.serverinfo(0).serverid(), AnyOf(serverId, serverId2));
    ASSERT_THAT(response.serverinfo(0).hostname(),
                AnyOf("hostname1", "hostname2"));
    ASSERT_THAT(response.serverinfo(0).internalip(), AnyOf("ip1", "ip3"));
    ASSERT_THAT(response.serverinfo(0).externalip(), AnyOf("ip2", "ip4"));
    ASSERT_EQ(zoneId, response.serverinfo(0).zoneid());
    ASSERT_EQ(poolId, response.serverinfo(0).poolid());

    ASSERT_THAT(response.serverinfo(1).serverid(), AnyOf(serverId, serverId2));
    ASSERT_THAT(response.serverinfo(1).hostname(),
                AnyOf("hostname1", "hostname2"));
    ASSERT_THAT(response.serverinfo(1).internalip(), AnyOf("ip1", "ip3"));
    ASSERT_THAT(response.serverinfo(1).externalip(), AnyOf("ip2", "ip4"));
    ASSERT_EQ(zoneId, response.serverinfo(1).zoneid());
    ASSERT_EQ(poolId, response.serverinfo(1).poolid());
}

TEST_F(TestTopologyManager, test_ListZoneServer_ByNameSuccess) {
    PoolIdType poolId = 0x11;
    ZoneIdType zoneId = 0x21;
    ServerIdType serverId = 0x31;
    ServerIdType serverId2 = 0x32;
    PrepareAddPool(poolId, "poolName1");
    PrepareAddZone(zoneId, "zone1", poolId);
    PrepareAddServer(serverId, "hostname1", "ip1", 0, "ip2", 0, zoneId, poolId);
    PrepareAddServer(serverId2, "hostname1", "ip3", 0, "ip4", 0, zoneId,
                     poolId);

    ListZoneServerRequest request;
    request.set_zonename("zone1");
    request.set_poolname("poolName1");

    ListZoneServerResponse response;
    serviceManager_->ListZoneServer(&request, &response);

    ASSERT_EQ(TopoStatusCode::TOPO_OK, response.statuscode());
    ASSERT_EQ(2, response.serverinfo_size());

    ASSERT_THAT(response.serverinfo(0).serverid(), AnyOf(serverId, serverId2));
    ASSERT_THAT(response.serverinfo(0).hostname(),
                AnyOf("hostname1", "hostname2"));
    ASSERT_THAT(response.serverinfo(0).internalip(), AnyOf("ip1", "ip3"));
    ASSERT_THAT(response.serverinfo(0).externalip(), AnyOf("ip2", "ip4"));
    ASSERT_EQ(zoneId, response.serverinfo(0).zoneid());
    ASSERT_EQ(poolId, response.serverinfo(0).poolid());

    ASSERT_THAT(response.serverinfo(1).serverid(), AnyOf(serverId, serverId2));
    ASSERT_THAT(response.serverinfo(1).hostname(),
                AnyOf("hostname1", "hostname2"));
    ASSERT_THAT(response.serverinfo(1).internalip(), AnyOf("ip1", "ip3"));
    ASSERT_THAT(response.serverinfo(1).externalip(), AnyOf("ip2", "ip4"));
    ASSERT_EQ(zoneId, response.serverinfo(1).zoneid());
    ASSERT_EQ(poolId, response.serverinfo(1).poolid());
}

TEST_F(TestTopologyManager, test_ListZoneServer_ZoneNotFound) {
    PoolIdType poolId = 0x11;
    ZoneIdType zoneId = 0x21;
    ServerIdType serverId = 0x31;
    ServerIdType serverId2 = 0x32;
    PrepareAddPool(poolId);
    PrepareAddZone(zoneId);
    PrepareAddServer(serverId, "hostname1", "ip1", 0, "ip2", 0, zoneId, poolId);
    PrepareAddServer(serverId2, "hostname1", "ip3", 0, "ip4", 0, zoneId,
                     poolId);

    ListZoneServerRequest request;
    request.set_zoneid(++zoneId);

    ListZoneServerResponse response;
    serviceManager_->ListZoneServer(&request, &response);

    ASSERT_EQ(TopoStatusCode::TOPO_ZONE_NOT_FOUND, response.statuscode());
    ASSERT_EQ(0, response.serverinfo_size());
}

TEST_F(TestTopologyManager, test_ListZoneServer_ByNameZoneNotFound) {
    PoolIdType poolId = 0x11;
    ZoneIdType zoneId = 0x21;
    ServerIdType serverId = 0x31;
    ServerIdType serverId2 = 0x32;
    PrepareAddPool(poolId, "poolName1");
    PrepareAddZone(zoneId, "zone1", poolId);
    PrepareAddServer(serverId, "hostname1", "ip1", 0, "ip2", 0, zoneId, poolId);
    PrepareAddServer(serverId2, "hostname1", "ip3", 0, "ip4", 0, zoneId,
                     poolId);

    ListZoneServerRequest request;
    request.set_zonename("zone2");
    request.set_poolname("poolName1");

    ListZoneServerResponse response;
    serviceManager_->ListZoneServer(&request, &response);

    ASSERT_EQ(TopoStatusCode::TOPO_ZONE_NOT_FOUND, response.statuscode());
    ASSERT_EQ(0, response.serverinfo_size());
}

TEST_F(TestTopologyManager, test_ListZoneServer_InvalidParam) {
    PoolIdType poolId = 0x11;
    ZoneIdType zoneId = 0x21;
    ServerIdType serverId = 0x31;
    ServerIdType serverId2 = 0x32;
    PrepareAddPool(poolId, "poolName1");
    PrepareAddZone(zoneId, "zone1", poolId);
    PrepareAddServer(serverId, "hostname1", "ip1", 0, "ip2", 0, zoneId, poolId);
    PrepareAddServer(serverId2, "hostname1", "ip3", 0, "ip4", 0, zoneId,
                     poolId);

    ListZoneServerRequest request;
    ListZoneServerResponse response;
    serviceManager_->ListZoneServer(&request, &response);

    ASSERT_EQ(TopoStatusCode::TOPO_INVALID_PARAM, response.statuscode());
    ASSERT_EQ(0, response.serverinfo_size());
}

TEST_F(TestTopologyManager, test_CreateZone_success) {
    PoolIdType poolId = 0x11;
    ZoneIdType zoneId = 0x21;
    PrepareAddPool(poolId, "poolname1");

    CreateZoneRequest request;
    request.set_zonename("zone1");
    request.set_poolname("poolname1");

    EXPECT_CALL(*idGenerator_, GenZoneId()).WillOnce(Return(zoneId));

    EXPECT_CALL(*storage_, StorageZone(_)).WillOnce(Return(true));

    CreateZoneResponse response;

    serviceManager_->CreateZone(&request, &response);
    ASSERT_EQ(TopoStatusCode::TOPO_OK, response.statuscode());
    ASSERT_TRUE(response.has_zoneinfo());

    ASSERT_EQ(zoneId, response.zoneinfo().zoneid());
    ASSERT_EQ("zone1", response.zoneinfo().zonename());
    ASSERT_EQ(poolId, response.zoneinfo().poolid());
}

TEST_F(TestTopologyManager, test_CreateZone_AllocateIdFail) {
    PoolIdType poolId = 0x11;
    ZoneIdType zoneId = 0x21;
    PrepareAddPool(poolId, "poolname1");

    CreateZoneRequest request;
    request.set_zonename("zone1");
    request.set_poolname("poolname1");

    EXPECT_CALL(*idGenerator_, GenZoneId()).WillOnce(Return(UNINITIALIZE_ID));

    CreateZoneResponse response;

    serviceManager_->CreateZone(&request, &response);
    ASSERT_EQ(TopoStatusCode::TOPO_ALLOCATE_ID_FAIL, response.statuscode());
}

TEST_F(TestTopologyManager, test_CreateZone_PoolNotFound) {
    PoolIdType poolId = 0x11;
    PrepareAddPool(poolId, "poolname1");

    CreateZoneRequest request;
    request.set_zonename("zone1");
    request.set_poolname("poolname2");

    CreateZoneResponse response;

    serviceManager_->CreateZone(&request, &response);
    ASSERT_EQ(TopoStatusCode::TOPO_POOL_NOT_FOUND, response.statuscode());
    ASSERT_FALSE(response.has_zoneinfo());
}

TEST_F(TestTopologyManager, test_CreateZone_AddZoneFail) {
    PoolIdType poolId = 0x11;
    ZoneIdType zoneId = 0x21;
    PrepareAddPool(poolId, "poolname1");

    CreateZoneRequest request;
    request.set_zonename("zone1");
    request.set_poolname("poolname1");

    EXPECT_CALL(*idGenerator_, GenZoneId()).WillOnce(Return(zoneId));

    EXPECT_CALL(*storage_, StorageZone(_)).WillOnce(Return(false));

    CreateZoneResponse response;

    serviceManager_->CreateZone(&request, &response);
    ASSERT_EQ(TopoStatusCode::TOPO_STORGE_FAIL, response.statuscode());
}

TEST_F(TestTopologyManager, test_DeleteZone_Success) {
    ZoneIdType zoneId = 0x21;
    PoolIdType poolId = 0x11;
    PrepareAddPool(poolId);
    PrepareAddZone(zoneId, "testZone", poolId);

    DeleteZoneRequest request;
    request.set_zoneid(zoneId);

    EXPECT_CALL(*storage_, DeleteZone(_)).WillOnce(Return(true));

    DeleteZoneResponse response;
    serviceManager_->DeleteZone(&request, &response);

    ASSERT_EQ(TopoStatusCode::TOPO_OK, response.statuscode());
}

TEST_F(TestTopologyManager, test_DeleteZone_ZoneNotFound) {
    ZoneIdType zoneId = 0x21;
    PoolIdType poolId = 0x11;
    PrepareAddPool(poolId);
    PrepareAddZone(zoneId, "testZone", poolId);

    DeleteZoneRequest request;
    request.set_zoneid(++zoneId);

    DeleteZoneResponse response;
    serviceManager_->DeleteZone(&request, &response);

    ASSERT_EQ(TopoStatusCode::TOPO_ZONE_NOT_FOUND, response.statuscode());
}

TEST_F(TestTopologyManager, test_GetZone_Success) {
    ZoneIdType zoneId = 0x21;
    PoolIdType poolId = 0x11;
    PrepareAddPool(poolId);
    PrepareAddZone(zoneId, "testZone", poolId);

    GetZoneRequest request;
    request.set_zoneid(zoneId);

    GetZoneResponse response;
    serviceManager_->GetZone(&request, &response);

    ASSERT_EQ(TopoStatusCode::TOPO_OK, response.statuscode());
    ASSERT_TRUE(response.has_zoneinfo());
    ASSERT_EQ(zoneId, response.zoneinfo().zoneid());
    ASSERT_EQ("testZone", response.zoneinfo().zonename());
    ASSERT_EQ(poolId, response.zoneinfo().poolid());
}

TEST_F(TestTopologyManager, test_GetZone_ZoneNotFound) {
    ZoneIdType zoneId = 0x21;
    PoolIdType poolId = 0x11;
    PrepareAddPool(poolId);
    PrepareAddZone(zoneId, "testZone", poolId);

    GetZoneRequest request;
    request.set_zoneid(++zoneId);

    GetZoneResponse response;
    serviceManager_->GetZone(&request, &response);

    ASSERT_EQ(TopoStatusCode::TOPO_ZONE_NOT_FOUND, response.statuscode());
    ASSERT_FALSE(response.has_zoneinfo());
}

TEST_F(TestTopologyManager, test_ListPoolZone_ByIdSuccess) {
    ZoneIdType zoneId = 0x21;
    ZoneIdType zoneId2 = 0x22;
    PoolIdType poolId = 0x11;
    PrepareAddPool(poolId);
    PrepareAddZone(zoneId, "testZone", poolId);

    PrepareAddZone(zoneId2, "testZone2", poolId);

    ListPoolZoneRequest request;
    request.set_poolid(poolId);

    ListPoolZoneResponse response;
    serviceManager_->ListPoolZone(&request, &response);

    ASSERT_EQ(TopoStatusCode::TOPO_OK, response.statuscode());
    ASSERT_EQ(2, response.zones_size());

    ASSERT_THAT(response.zones(0).zoneid(), AnyOf(zoneId, zoneId2));
    ASSERT_THAT(response.zones(0).zonename(), AnyOf("testZone", "testZone2"));
    ASSERT_EQ(poolId, response.zones(0).poolid());

    ASSERT_THAT(response.zones(1).zoneid(), AnyOf(zoneId, zoneId2));
    ASSERT_THAT(response.zones(1).zonename(), AnyOf("testZone", "testZone2"));
    ASSERT_EQ(poolId, response.zones(1).poolid());
}

TEST_F(TestTopologyManager, test_ListPoolZone_PoolNotFound) {
    ZoneIdType zoneId = 0x21;
    ZoneIdType zoneId2 = 0x22;
    PoolIdType poolId = 0x11;
    PrepareAddPool(poolId);
    PrepareAddZone(zoneId, "testZone", poolId);

    PrepareAddZone(zoneId2, "testZone2", poolId);

    ListPoolZoneRequest request;
    request.set_poolid(++poolId);

    ListPoolZoneResponse response;
    serviceManager_->ListPoolZone(&request, &response);

    ASSERT_EQ(TopoStatusCode::TOPO_POOL_NOT_FOUND, response.statuscode());
    ASSERT_EQ(0, response.zones_size());
}

TEST_F(TestTopologyManager, test_createPool_Success) {
    CreatePoolRequest request;
    request.set_poolname("default");
    request.set_redundanceandplacementpolicy(
        "{\"replicaNum\":3, \"copysetNum\":1, \"zoneNum\":3}");

    PoolIdType poolId = 0x12;
    EXPECT_CALL(*idGenerator_, GenPoolId()).WillOnce(Return(poolId));
    EXPECT_CALL(*storage_, StoragePool(_)).WillOnce(Return(true));

    CreatePoolResponse response;
    serviceManager_->CreatePool(&request, &response);

    ASSERT_EQ(TopoStatusCode::TOPO_OK, response.statuscode());
    ASSERT_TRUE(response.has_poolinfo());
    ASSERT_EQ(poolId, response.poolinfo().poolid());
    ASSERT_EQ(request.poolname(), response.poolinfo().poolname());
}

TEST_F(TestTopologyManager, test_createPool_Fail) {
    CreatePoolRequest request;
    CreatePoolResponse response;

    request.set_poolname("default");
    request.set_redundanceandplacementpolicy(
        "{\"replicaNum\":3, \"copysetNum\":1, \"zoneNum\":3}");

    EXPECT_CALL(*idGenerator_, GenPoolId()).WillOnce(Return(UNINITIALIZE_ID));

    serviceManager_->CreatePool(&request, &response);

    ASSERT_EQ(TopoStatusCode::TOPO_ALLOCATE_ID_FAIL, response.statuscode());
    ASSERT_TRUE(false == response.has_poolinfo());
}

TEST_F(TestTopologyManager, test_createPool_StorageFail) {
    CreatePoolRequest request;
    CreatePoolResponse response;

    request.set_poolname("default");
    request.set_redundanceandplacementpolicy(
        "{\"replicaNum\":3, \"copysetNum\":1, \"zoneNum\":3}");

    PoolIdType poolId = 0x12;
    EXPECT_CALL(*idGenerator_, GenPoolId()).WillOnce(Return(poolId));
    EXPECT_CALL(*storage_, StoragePool(_)).WillOnce(Return(false));

    serviceManager_->CreatePool(&request, &response);

    ASSERT_EQ(TopoStatusCode::TOPO_STORGE_FAIL, response.statuscode());
    ASSERT_TRUE(false == response.has_poolinfo());
}

TEST_F(TestTopologyManager, test_DeletePool_ByIdSuccess) {
    PoolIdType pid = 0x12;
    DeletePoolRequest request;
    request.set_poolid(pid);

    PrepareAddPool(pid);

    EXPECT_CALL(*storage_, DeletePool(_)).WillOnce(Return(true));

    DeletePoolResponse response;
    serviceManager_->DeletePool(&request, &response);

    ASSERT_EQ(TopoStatusCode::TOPO_OK, response.statuscode());
}

TEST_F(TestTopologyManager, test_DeletePool_PoolNotFound) {
    PoolIdType pid = 0x12;
    DeletePoolRequest request;
    request.set_poolid(pid);

    PrepareAddPool(++pid);

    DeletePoolResponse response;
    serviceManager_->DeletePool(&request, &response);

    ASSERT_EQ(TopoStatusCode::TOPO_POOL_NOT_FOUND, response.statuscode());
}

TEST_F(TestTopologyManager, test_DeletePool_StorageFail) {
    PoolIdType pid = 0x12;
    DeletePoolRequest request;
    request.set_poolid(pid);

    PrepareAddPool(pid);

    EXPECT_CALL(*storage_, DeletePool(_)).WillOnce(Return(false));

    DeletePoolResponse response;
    serviceManager_->DeletePool(&request, &response);

    ASSERT_EQ(TopoStatusCode::TOPO_STORGE_FAIL, response.statuscode());
}

TEST_F(TestTopologyManager, test_GetPool_ByIdSuccess) {
    PoolIdType pid = 0x12;
    std::string pName = "test1";

    GetPoolRequest request;
    request.set_poolid(pid);

    PrepareAddPool(pid, pName);

    GetPoolResponse response;
    serviceManager_->GetPool(&request, &response);

    ASSERT_EQ(TopoStatusCode::TOPO_OK, response.statuscode());
    ASSERT_TRUE(response.has_poolinfo());
    ASSERT_EQ(pid, response.poolinfo().poolid());
    ASSERT_EQ(pName, response.poolinfo().poolname());
}

TEST_F(TestTopologyManager, test_GetPool_PoolNotFound) {
    PoolIdType pid = 0x12;
    GetPoolRequest request;
    request.set_poolid(pid);

    PrepareAddPool(++pid);

    GetPoolResponse response;
    serviceManager_->GetPool(&request, &response);

    ASSERT_EQ(TopoStatusCode::TOPO_POOL_NOT_FOUND, response.statuscode());
    ASSERT_EQ(false, response.has_poolinfo());
}

TEST_F(TestTopologyManager, test_listPool_success) {
    ListPoolRequest request;
    ListPoolResponse response;

    PrepareAddPool(0x01, "test1");
    PrepareAddPool(0x02, "test2");

    serviceManager_->ListPool(&request, &response);

    ASSERT_EQ(TopoStatusCode::TOPO_OK, response.statuscode());
    ASSERT_EQ(2, response.poolinfos_size());

    ASSERT_THAT(response.poolinfos(0).poolid(), AnyOf(0x01, 0x02));
    ASSERT_THAT(response.poolinfos(0).poolname(), AnyOf("test1", "test2"));
    ASSERT_THAT(response.poolinfos(1).poolid(), AnyOf(0x01, 0x02));
    ASSERT_THAT(response.poolinfos(1).poolname(), AnyOf("test1", "test2"));
}

TEST_F(TestTopologyManager, test_GetMetaServerListInCopySets_success) {
    PoolIdType poolId = 0x11;
    CopySetIdType copysetId = 0x51;

    PrepareAddPool(poolId);
    PrepareAddZone(0x21, "zone1", poolId);
    PrepareAddZone(0x22, "zone2", poolId);
    PrepareAddZone(0x23, "zone3", poolId);
    PrepareAddServer(0x31, "server1", "ip1", 0, "ip2", 0, 0x21, 0x11);
    PrepareAddServer(0x32, "server2", "ip1", 0, "ip2", 0, 0x22, 0x11);
    PrepareAddServer(0x33, "server3", "ip1", 0, "ip2", 0, 0x23, 0x11);
    PrepareAddMetaServer(0x41, "ms1", "token1", 0x31, "ip1", 0, "ip2", 8888);
    PrepareAddMetaServer(0x42, "ms2", "token2", 0x32, "ip1", 0, "ip2", 8888);
    PrepareAddMetaServer(0x43, "ms3", "token3", 0x33, "ip1", 0, "ip2", 8888);

    std::set<MetaServerIdType> replicas;
    replicas.insert(0x41);
    replicas.insert(0x42);
    replicas.insert(0x43);
    PrepareAddCopySet(copysetId, poolId, replicas);

    GetMetaServerListInCopySetsRequest request;
    request.set_poolid(poolId);
    request.add_copysetid(copysetId);
    GetMetaServerListInCopySetsResponse response;
    serviceManager_->GetMetaServerListInCopysets(&request, &response);

    ASSERT_EQ(TopoStatusCode::TOPO_OK, response.statuscode());
    ASSERT_EQ(1, response.csinfo_size());
    ASSERT_EQ(copysetId, response.csinfo(0).copysetid());
    ASSERT_EQ(3, response.csinfo(0).cslocs_size());

    ASSERT_THAT(response.csinfo(0).cslocs(0).metaserverid(),
                AnyOf(0x41, 0x42, 0x43));
    ASSERT_EQ("ip1", response.csinfo(0).cslocs(0).internalip());
    ASSERT_EQ("ip2", response.csinfo(0).cslocs(0).externalip());
    ASSERT_EQ(0, response.csinfo(0).cslocs(0).internalport());
}

TEST_F(TestTopologyManager, test_GetMetaServerListInCopySets_CopysetNotFound) {
    PoolIdType poolId = 0x11;
    CopySetIdType copysetId = 0x51;

    PrepareAddPool(poolId);
    PrepareAddZone(0x21, "zone1", poolId);
    PrepareAddZone(0x22, "zone2", poolId);
    PrepareAddZone(0x23, "zone3", poolId);
    PrepareAddServer(0x31, "server1", "ip1", 0, "ip2", 0, 0x21, 0x11);
    PrepareAddServer(0x32, "server2", "ip1", 0, "ip2", 0, 0x22, 0x11);
    PrepareAddServer(0x33, "server3", "ip1", 0, "ip2", 0, 0x23, 0x11);
    PrepareAddMetaServer(0x41, "ms1", "token1", 0x31, "ip1", 0, "ip2", 8888);
    PrepareAddMetaServer(0x42, "ms2", "token2", 0x32, "ip1", 0, "ip2", 8888);
    PrepareAddMetaServer(0x43, "ms3", "token3", 0x33, "ip1", 0, "ip2", 8888);

    std::set<MetaServerIdType> replicas;
    replicas.insert(0x41);
    replicas.insert(0x42);
    replicas.insert(0x43);
    PrepareAddCopySet(copysetId, poolId, replicas);

    GetMetaServerListInCopySetsRequest request;
    request.set_poolid(poolId);
    request.add_copysetid(++copysetId);
    GetMetaServerListInCopySetsResponse response;
    serviceManager_->GetMetaServerListInCopysets(&request, &response);

    ASSERT_EQ(TopoStatusCode::TOPO_COPYSET_NOT_FOUND, response.statuscode());
}

TEST_F(TestTopologyManager, test_GetMetaServerListInCopySets_InternalError) {
    PoolIdType poolId = 0x11;
    CopySetIdType copysetId = 0x51;

    PrepareAddPool(poolId);
    PrepareAddZone(0x21, "zone1", poolId);
    PrepareAddZone(0x22, "zone2", poolId);
    PrepareAddZone(0x23, "zone3", poolId);
    PrepareAddServer(0x31, "server1", "ip1", 0, "ip2", 0, 0x21, 0x11);
    PrepareAddServer(0x32, "server2", "ip1", 0, "ip2", 0, 0x22, 0x11);
    PrepareAddServer(0x33, "server3", "ip1", 0, "ip2", 0, 0x23, 0x11);
    PrepareAddMetaServer(0x41, "ms1", "token1", 0x31, "ip1", 0, "ip2", 8888);
    PrepareAddMetaServer(0x42, "ms2", "token2", 0x32, "ip1", 0, "ip2", 8888);
    PrepareAddMetaServer(0x43, "ms3", "token3", 0x33, "ip1", 0, "ip2", 8888);

    std::set<MetaServerIdType> replicas;
    replicas.insert(0x41);
    replicas.insert(0x42);
    replicas.insert(0x44);  // here a invalid metaserver
    PrepareAddCopySet(copysetId, poolId, replicas);

    GetMetaServerListInCopySetsRequest request;
    request.set_poolid(poolId);
    request.add_copysetid(copysetId);
    GetMetaServerListInCopySetsResponse response;
    serviceManager_->GetMetaServerListInCopysets(&request, &response);

    ASSERT_EQ(TopoStatusCode::TOPO_INTERNAL_ERROR, response.statuscode());
}

TEST_F(TestTopologyManager, test_CreatePartitionWithAvailableCopyset_Sucess) {
    PoolIdType poolId = 0x11;
    CopySetIdType copysetId = 0x51;
    PartitionIdType partitionId = 0x61;

    PrepareAddPool(poolId);
    PrepareAddZone(0x21, "zone1", poolId);
    PrepareAddZone(0x22, "zone2", poolId);
    PrepareAddZone(0x23, "zone3", poolId);
    PrepareAddServer(0x31, "server1", "127.0.0.1", 0, "127.0.0.1", 0, 0x21,
                     0x11);
    PrepareAddServer(0x32, "server2", "127.0.0.1", 0, "127.0.0.1", 0, 0x22,
                     0x11);
    PrepareAddServer(0x33, "server3", "127.0.0.1", 0, "127.0.0.1", 0, 0x23,
                     0x11);
    PrepareAddMetaServer(0x41, "ms1", "token1", 0x31, "127.0.0.1", 7777, "ip2",
                         8888);
    PrepareAddMetaServer(0x42, "ms2", "token2", 0x32, "127.0.0.1", 7777, "ip2",
                         8888);
    PrepareAddMetaServer(0x43, "ms3", "token3", 0x33, "127.0.0.1", 7777, "ip2",
                         8888);

    std::set<MetaServerIdType> replicas;
    replicas.insert(0x41);
    replicas.insert(0x42);
    replicas.insert(0x43);
    PrepareAddCopySet(copysetId, poolId, replicas);

    EXPECT_CALL(*idGenerator_, GenPartitionId()).WillOnce(Return(partitionId));

    std::string leader = "127.0.0.1:7777";

    EXPECT_CALL(*storage_, StoragePartition(_))
        .WillOnce(Return(true));

    EXPECT_CALL(*mockMetaserverClient_, CreatePartition(_, _, _, _, _, _, _))
        .WillOnce(Return(FSStatusCode::OK));

    CreatePartitionRequest request;
    CreatePartitionResponse response;
    request.set_fsid(0x01);
    request.set_count(1);
    serviceManager_->CreatePartitions(&request, &response);
    ASSERT_EQ(TopoStatusCode::TOPO_OK, response.statuscode());
    ASSERT_EQ(1, response.partitioninfolist().size());

    Partition partition;
    ASSERT_TRUE(topology_->GetPartition(partitionId, &partition));
    ASSERT_EQ(copysetId, partition.GetCopySetId());

    CopySetInfo info;
    CopySetKey key(poolId, copysetId);
    ASSERT_TRUE(topology_->GetCopySet(key, &info));
    ASSERT_EQ(copysetId, info.GetId());
    ASSERT_EQ(1, info.GetPartitionNum());
}

TEST_F(TestTopologyManager,
    test_CreatePartitionsAndGetMinPartition_Sucess) {
    PoolIdType poolId = 0x11;
    CopySetIdType copysetId = 0x51;
    PartitionIdType partitionId = 0x61;
    PartitionIdType partitionId2 = 0x62;
    PartitionIdType partitionId3 = 0x63;

    PrepareAddPool(poolId);
    PrepareAddZone(0x21, "zone1", poolId);
    PrepareAddZone(0x22, "zone2", poolId);
    PrepareAddZone(0x23, "zone3", poolId);
    PrepareAddServer(0x31, "server1", "127.0.0.1", 0, "127.0.0.1", 0,
                     0x21, 0x11);
    PrepareAddServer(0x32, "server2", "127.0.0.1", 0, "127.0.0.1", 0,
                     0x22, 0x11);
    PrepareAddServer(0x33, "server3", "127.0.0.1", 0, "127.0.0.1", 0,
                     0x23, 0x11);
    PrepareAddMetaServer(0x41, "ms1", "token1", 0x31, "127.0.0.1",
                         7777, "ip2", 8888);
    PrepareAddMetaServer(0x42, "ms2", "token2", 0x32, "127.0.0.1",
                         7777, "ip2", 8888);
    PrepareAddMetaServer(0x43, "ms3", "token3", 0x33, "127.0.0.1",
                         7777, "ip2", 8888);

    std::set<MetaServerIdType> replicas;
    replicas.insert(0x41);
    replicas.insert(0x42);
    replicas.insert(0x43);
    PrepareAddCopySet(copysetId, poolId, replicas);

    EXPECT_CALL(*idGenerator_, GenPartitionId())
        .Times(3)
        .WillOnce(Return(partitionId))
        .WillOnce(Return(partitionId2))
        .WillOnce(Return(partitionId3));

    EXPECT_CALL(*storage_, StoragePartition(_))
        .Times(3)
        .WillRepeatedly(Return(true));
    EXPECT_CALL(*mockMetaserverClient_, CreatePartition(_, _, _, _, _, _, _))
        .Times(3)
        .WillRepeatedly(Return(FSStatusCode::OK));

    PartitionInfo pInfo;
    auto ret = serviceManager_->CreatePartitionsAndGetMinPartition(0x01,
                                                                &pInfo);
    ASSERT_EQ(TopoStatusCode::TOPO_OK, ret);
    ASSERT_EQ(partitionId, pInfo.partitionid());
    ASSERT_EQ(3, topology_->GetPartitionNumberOfFs(0x01));
    ASSERT_EQ(copysetId, pInfo.copysetid());
}

TEST_F(TestTopologyManager, test_CreatePartitionWithAvailableCopyset_Sucess2) {
    PoolIdType poolId = 0x11;
    CopySetIdType copysetId = 0x51;
    PartitionIdType partitionId = 0x61;

    PrepareAddPool(poolId);
    PrepareAddZone(0x21, "zone1", poolId);
    PrepareAddZone(0x22, "zone2", poolId);
    PrepareAddZone(0x23, "zone3", poolId);
    PrepareAddServer(0x31, "server1", "127.0.0.1", 0, "127.0.0.1", 0, 0x21,
                     0x11);
    PrepareAddServer(0x32, "server2", "127.0.0.1", 0, "127.0.0.1", 0, 0x22,
                     0x11);
    PrepareAddServer(0x33, "server3", "127.0.0.1", 0, "127.0.0.1", 0, 0x23,
                     0x11);
    PrepareAddMetaServer(0x41, "ms1", "token1", 0x31, "127.0.0.1", 7777, "ip2",
                         8888);
    PrepareAddMetaServer(0x42, "ms2", "token2", 0x32, "127.0.0.1", 7777, "ip2",
                         8888);
    PrepareAddMetaServer(0x43, "ms3", "token3", 0x33, "127.0.0.1", 7777, "ip2",
                         8888);

    std::set<MetaServerIdType> replicas;
    replicas.insert(0x41);
    replicas.insert(0x42);
    replicas.insert(0x43);
    PrepareAddCopySet(copysetId, poolId, replicas);

    EXPECT_CALL(*idGenerator_, GenPartitionId())
        .WillOnce(Return(partitionId))
        .WillOnce(Return(partitionId + 1));

    EXPECT_CALL(*storage_, StoragePartition(_))
        .Times(2)
        .WillRepeatedly(Return(true));
    EXPECT_CALL(*mockMetaserverClient_, CreatePartition(_, _, _, _, _, _, _))
        .Times(2)
        .WillRepeatedly(Return(FSStatusCode::OK));

    CreatePartitionRequest request;
    CreatePartitionResponse response;
    request.set_fsid(0x01);
    request.set_count(2);
    serviceManager_->CreatePartitions(&request, &response);
    ASSERT_EQ(TopoStatusCode::TOPO_OK, response.statuscode());
    ASSERT_EQ(2, response.partitioninfolist().size());

    Partition partition;
    ASSERT_TRUE(topology_->GetPartition(partitionId, &partition));
    ASSERT_EQ(copysetId, partition.GetCopySetId());

    CopySetInfo info;
    CopySetKey key(poolId, copysetId);
    ASSERT_TRUE(topology_->GetCopySet(key, &info));
    ASSERT_EQ(copysetId, info.GetId());
    ASSERT_EQ(2, info.GetPartitionNum());
}

TEST_F(TestTopologyManager,
       test_CreatePartitionWithAvailableCopyset_GetLeaderFirstFailed) {
    PoolIdType poolId = 0x11;
    CopySetIdType copysetId = 0x51;
    PartitionIdType partitionId = 0x61;

    PrepareAddPool(poolId);
    PrepareAddZone(0x21, "zone1", poolId);
    PrepareAddZone(0x22, "zone2", poolId);
    PrepareAddZone(0x23, "zone3", poolId);
    PrepareAddServer(0x31, "server1", "127.0.0.1", 0, "127.0.0.1", 0, 0x21,
                     0x11);
    PrepareAddServer(0x32, "server2", "127.0.0.1", 0, "127.0.0.1", 0, 0x22,
                     0x11);
    PrepareAddServer(0x33, "server3", "127.0.0.1", 0, "127.0.0.1", 0, 0x23,
                     0x11);
    PrepareAddMetaServer(0x41, "ms1", "token1", 0x31, "127.0.0.1", 7777, "ip2",
                         8888);
    PrepareAddMetaServer(0x42, "ms2", "token2", 0x32, "127.0.0.1", 7777, "ip2",
                         8888);
    PrepareAddMetaServer(0x43, "ms3", "token3", 0x33, "127.0.0.1", 7777, "ip2",
                         8888);

    std::set<MetaServerIdType> replicas;
    replicas.insert(0x41);
    replicas.insert(0x42);
    replicas.insert(0x43);
    PrepareAddCopySet(copysetId, poolId, replicas);

    EXPECT_CALL(*idGenerator_, GenPartitionId())
        .WillOnce(Return(partitionId));
    EXPECT_CALL(*storage_, StoragePartition(_))
        .WillOnce(Return(true));

    EXPECT_CALL(*mockMetaserverClient_, CreatePartition(_, _, _, _, _, _, _))
        .WillOnce(Return(FSStatusCode::OK));

    CreatePartitionRequest request;
    CreatePartitionResponse response;
    request.set_fsid(0x01);
    request.set_count(1);
    serviceManager_->CreatePartitions(&request, &response);
    ASSERT_EQ(TopoStatusCode::TOPO_OK, response.statuscode());
    ASSERT_EQ(1, response.partitioninfolist().size());

    Partition partition;
    ASSERT_TRUE(topology_->GetPartition(partitionId, &partition));
    ASSERT_EQ(copysetId, partition.GetCopySetId());

    CopySetInfo info;
    CopySetKey key(poolId, copysetId);
    ASSERT_TRUE(topology_->GetCopySet(key, &info));
    ASSERT_EQ(copysetId, info.GetId());
    ASSERT_EQ(1, info.GetPartitionNum());
}

TEST_F(TestTopologyManager,
       test_CreatePartitionWithAvailableCopyset_CreatePartitionFailed) {
    PoolIdType poolId = 0x11;
    CopySetIdType copysetId = 0x51;
    PartitionIdType partitionId = 0x61;

    PrepareAddPool(poolId);
    PrepareAddZone(0x21, "zone1", poolId);
    PrepareAddZone(0x22, "zone2", poolId);
    PrepareAddZone(0x23, "zone3", poolId);
    PrepareAddServer(0x31, "server1", "127.0.0.1", 0, "127.0.0.1", 0, 0x21,
                     0x11);
    PrepareAddServer(0x32, "server2", "127.0.0.1", 0, "127.0.0.1", 0, 0x22,
                     0x11);
    PrepareAddServer(0x33, "server3", "127.0.0.1", 0, "127.0.0.1", 0, 0x23,
                     0x11);
    PrepareAddMetaServer(0x41, "ms1", "token1", 0x31, "127.0.0.1", 7777, "ip2",
                         8888);
    PrepareAddMetaServer(0x42, "ms2", "token2", 0x32, "127.0.0.1", 7777, "ip2",
                         8888);
    PrepareAddMetaServer(0x43, "ms3", "token3", 0x33, "127.0.0.1", 7777, "ip2",
                         8888);

    std::set<MetaServerIdType> replicas;
    replicas.insert(0x41);
    replicas.insert(0x42);
    replicas.insert(0x43);
    PrepareAddCopySet(copysetId, poolId, replicas);

    EXPECT_CALL(*idGenerator_, GenPartitionId())
        .WillOnce(Return(partitionId));

    EXPECT_CALL(*mockMetaserverClient_, CreatePartition(_, _, _, _, _, _, _))
        .WillOnce(Return(FSStatusCode::CREATE_PARTITION_ERROR));

    CreatePartitionRequest request;
    CreatePartitionResponse response;
    request.set_fsid(0x01);
    request.set_count(1);
    serviceManager_->CreatePartitions(&request, &response);
    ASSERT_EQ(TopoStatusCode::TOPO_CREATE_PARTITION_FAIL,
              response.statuscode());
}

TEST_F(TestTopologyManager,
       test_CreatePartitionWithAvailableCopyset_AddPartitionFailed) {
    PoolIdType poolId = 0x11;
    CopySetIdType copysetId = 0x51;
    PartitionIdType partitionId = 0x61;

    PrepareAddPool(poolId);
    PrepareAddZone(0x21, "zone1", poolId);
    PrepareAddZone(0x22, "zone2", poolId);
    PrepareAddZone(0x23, "zone3", poolId);
    PrepareAddServer(0x31, "server1", "127.0.0.1", 0, "127.0.0.1", 0, 0x21,
                     0x11);
    PrepareAddServer(0x32, "server2", "127.0.0.1", 0, "127.0.0.1", 0, 0x22,
                     0x11);
    PrepareAddServer(0x33, "server3", "127.0.0.1", 0, "127.0.0.1", 0, 0x23,
                     0x11);
    PrepareAddMetaServer(0x41, "ms1", "token1", 0x31, "127.0.0.1", 7777, "ip2",
                         8888);
    PrepareAddMetaServer(0x42, "ms2", "token2", 0x32, "127.0.0.1", 7777, "ip2",
                         8888);
    PrepareAddMetaServer(0x43, "ms3", "token3", 0x33, "127.0.0.1", 7777, "ip2",
                         8888);

    std::set<MetaServerIdType> replicas;
    replicas.insert(0x41);
    replicas.insert(0x42);
    replicas.insert(0x43);
    PrepareAddCopySet(copysetId, poolId, replicas);

    EXPECT_CALL(*idGenerator_, GenPartitionId())
        .WillOnce(Return(partitionId));

    EXPECT_CALL(*mockMetaserverClient_, CreatePartition(_, _, _, _, _, _, _))
        .WillOnce(Return(FSStatusCode::OK));
    EXPECT_CALL(*storage_, StoragePartition(_)).WillOnce(Return(false));

    CreatePartitionRequest request;
    CreatePartitionResponse response;
    request.set_fsid(0x01);
    request.set_count(1);
    serviceManager_->CreatePartitions(&request, &response);
    ASSERT_EQ(TopoStatusCode::TOPO_STORGE_FAIL,
              response.statuscode());
}

TEST_F(TestTopologyManager,
       test_CreatePartitionWithOutAvailableCopyset_Success) {
    PoolIdType poolId = 0x11;
    CopySetIdType copysetId = 0x51;
    PartitionIdType partitionId = 0x61;

    Pool::RedundanceAndPlaceMentPolicy policy;
    policy.replicaNum = 3;
    policy.copysetNum = 0;
    policy.zoneNum = 3;
    PrepareAddPool(poolId, "pool1", policy);
    PrepareAddZone(0x21, "zone1", poolId);
    PrepareAddZone(0x22, "zone2", poolId);
    PrepareAddZone(0x23, "zone3", poolId);
    PrepareAddServer(0x31, "server1", "127.0.0.1", 0, "127.0.0.1", 0, 0x21,
                     0x11);
    PrepareAddServer(0x32, "server2", "127.0.0.1", 0, "127.0.0.1", 0, 0x22,
                     0x11);
    PrepareAddServer(0x33, "server3", "127.0.0.1", 0, "127.0.0.1", 0, 0x23,
                     0x11);
    PrepareAddMetaServer(0x41, "ms1", "token1", 0x31, "127.0.0.1", 7777, "ip2",
                         8888);
    PrepareAddMetaServer(0x42, "ms2", "token2", 0x32, "127.0.0.1", 7778, "ip2",
                         8888);
    PrepareAddMetaServer(0x43, "ms3", "token3", 0x33, "127.0.0.1", 7779, "ip2",
                         8888);
    topology_->UpdateMetaServerSpace(MetaServerSpace(100, 0), 0x41);
    topology_->UpdateMetaServerSpace(MetaServerSpace(100, 1), 0x42);
    topology_->UpdateMetaServerSpace(MetaServerSpace(100, 2), 0x43);

    EXPECT_CALL(*idGenerator_, GenCopySetId(_)).WillOnce(Return(copysetId));
    EXPECT_CALL(*mockMetaserverClient_, CreateCopySet(_, _, _))
        .WillOnce(Return(FSStatusCode::OK));
    EXPECT_CALL(*storage_, StorageCopySet(_)).WillOnce(Return(true));
    EXPECT_CALL(*idGenerator_, GenPartitionId())
        .WillOnce(Return(partitionId));
    EXPECT_CALL(*storage_, StoragePartition(_))
        .WillOnce(Return(true));

    EXPECT_CALL(*mockMetaserverClient_, CreatePartition(_, _, _, _, _, _, _))
        .WillOnce(Return(FSStatusCode::OK));

    CreatePartitionRequest request;
    CreatePartitionResponse response;
    request.set_fsid(0x01);
    request.set_count(1);
    serviceManager_->CreatePartitions(&request, &response);

    ASSERT_EQ(TopoStatusCode::TOPO_OK, response.statuscode());
    ASSERT_EQ(1, response.partitioninfolist().size());

    Partition partition;
    ASSERT_TRUE(topology_->GetPartition(partitionId, &partition));
    ASSERT_EQ(copysetId, partition.GetCopySetId());

    CopySetInfo info;
    CopySetKey key(poolId, copysetId);
    ASSERT_TRUE(topology_->GetCopySet(key, &info));
    ASSERT_EQ(copysetId, info.GetId());
    ASSERT_EQ(1, info.GetPartitionNum());
}

TEST_F(TestTopologyManager,
       test_CreatePartitionWithOutAvailableCopyset_HaveNoAvailableMetaserver) {
    PoolIdType poolId = 0x11;
    CopySetIdType copysetId = 0x51;
    PartitionIdType partitionId = 0x61;

    Pool::RedundanceAndPlaceMentPolicy policy;
    policy.replicaNum = 3;
    policy.copysetNum = 0;
    policy.zoneNum = 3;
    PrepareAddPool(poolId, "pool1", policy);
    PrepareAddZone(0x21, "zone1", poolId);
    PrepareAddZone(0x22, "zone2", poolId);
    PrepareAddZone(0x23, "zone3", poolId);
    PrepareAddServer(0x31, "server1", "127.0.0.1", 0, "127.0.0.1", 0, 0x21,
                     0x11);
    PrepareAddServer(0x32, "server2", "127.0.0.1", 0, "127.0.0.1", 0, 0x22,
                     0x11);
    PrepareAddServer(0x33, "server3", "127.0.0.1", 0, "127.0.0.1", 0, 0x23,
                     0x11);
    PrepareAddMetaServer(0x41, "ms1", "token1", 0x31, "127.0.0.1", 7777, "ip2",
                         8888, OnlineState::OFFLINE);
    PrepareAddMetaServer(0x42, "ms2", "token2", 0x32, "127.0.0.1", 7778, "ip2",
                         8888);
    PrepareAddMetaServer(0x43, "ms3", "token3", 0x33, "127.0.0.1", 7779, "ip2",
                         8888);
    topology_->UpdateMetaServerSpace(MetaServerSpace(100, 0), 0x41);
    topology_->UpdateMetaServerSpace(MetaServerSpace(100, 0), 0x42);
    topology_->UpdateMetaServerSpace(MetaServerSpace(100, 0), 0x43);

    CreatePartitionRequest request;
    CreatePartitionResponse response;
    request.set_fsid(0x01);
    request.set_count(1);
    serviceManager_->CreatePartitions(&request, &response);

    ASSERT_EQ(TopoStatusCode::TOPO_CREATE_COPYSET_ERROR, response.statuscode());
}

TEST_F(TestTopologyManager,
       test_CreatePartitionWithOutAvailableCopyset_MetaServerSpaceIsFull) {
    PoolIdType poolId = 0x11;
    CopySetIdType copysetId = 0x51;
    PartitionIdType partitionId = 0x61;

    Pool::RedundanceAndPlaceMentPolicy policy;
    policy.replicaNum = 3;
    policy.copysetNum = 0;
    policy.zoneNum = 3;
    PrepareAddPool(poolId, "pool1", policy);
    PrepareAddZone(0x21, "zone1", poolId);
    PrepareAddZone(0x22, "zone2", poolId);
    PrepareAddZone(0x23, "zone3", poolId);
    PrepareAddServer(0x31, "server1", "127.0.0.1", 0, "127.0.0.1", 0, 0x21,
                     0x11);
    PrepareAddServer(0x32, "server2", "127.0.0.1", 0, "127.0.0.1", 0, 0x22,
                     0x11);
    PrepareAddServer(0x33, "server3", "127.0.0.1", 0, "127.0.0.1", 0, 0x23,
                     0x11);
    PrepareAddMetaServer(0x41, "ms1", "token1", 0x31, "127.0.0.1", 7777, "ip2",
                         8888);
    PrepareAddMetaServer(0x42, "ms2", "token2", 0x32, "127.0.0.1", 7778, "ip2",
                         8888);
    PrepareAddMetaServer(0x43, "ms3", "token3", 0x33, "127.0.0.1", 7779, "ip2",
                         8888);
    topology_->UpdateMetaServerSpace(MetaServerSpace(100, 100), 0x41);
    topology_->UpdateMetaServerSpace(MetaServerSpace(100, 0), 0x42);
    topology_->UpdateMetaServerSpace(MetaServerSpace(100, 0), 0x43);

    CreatePartitionRequest request;
    CreatePartitionResponse response;
    request.set_fsid(0x01);
    request.set_count(1);
    serviceManager_->CreatePartitions(&request, &response);

    ASSERT_EQ(TopoStatusCode::TOPO_CREATE_COPYSET_ERROR, response.statuscode());
}

TEST_F(TestTopologyManager,
       test_CreatePartitionWithOutAvailableCopyset_HaveOfflineMetaserver) {
    PoolIdType poolId = 0x11;
    CopySetIdType copysetId = 0x51;
    PartitionIdType partitionId = 0x61;

    Pool::RedundanceAndPlaceMentPolicy policy;
    policy.replicaNum = 3;
    policy.copysetNum = 0;
    policy.zoneNum = 3;
    PrepareAddPool(poolId, "pool1", policy);
    PrepareAddZone(0x21, "zone1", poolId);
    PrepareAddZone(0x22, "zone2", poolId);
    PrepareAddZone(0x23, "zone3", poolId);
    PrepareAddServer(0x31, "server1", "127.0.0.1", 0, "127.0.0.1", 0, 0x21,
                     0x11);
    PrepareAddServer(0x32, "server2", "127.0.0.1", 0, "127.0.0.1", 0, 0x22,
                     0x11);
    PrepareAddServer(0x33, "server3", "127.0.0.1", 0, "127.0.0.1", 0, 0x23,
                     0x11);
    PrepareAddMetaServer(0x41, "ms1", "token1", 0x31, "127.0.0.1", 7777, "ip2",
                         8888, OnlineState::OFFLINE);
    PrepareAddMetaServer(0x42, "ms2", "token2", 0x31, "127.0.0.1", 7778, "ip2",
                         8888);
    PrepareAddMetaServer(0x43, "ms3", "token3", 0x32, "127.0.0.1", 7779, "ip2",
                         8888);
    PrepareAddMetaServer(0x44, "ms4", "token4", 0x33, "127.0.0.1", 7780, "ip2",
                         8888);
    topology_->UpdateMetaServerSpace(MetaServerSpace(100, 0), 0x41);
    topology_->UpdateMetaServerSpace(MetaServerSpace(100, 0), 0x42);
    topology_->UpdateMetaServerSpace(MetaServerSpace(100, 0), 0x43);
    topology_->UpdateMetaServerSpace(MetaServerSpace(100, 0), 0x44);

    EXPECT_CALL(*idGenerator_, GenCopySetId(_)).WillOnce(Return(copysetId));
    EXPECT_CALL(*mockMetaserverClient_, CreateCopySet(_, _, _))
        .WillOnce(Return(FSStatusCode::OK));
    EXPECT_CALL(*storage_, StorageCopySet(_)).WillOnce(Return(true));
    EXPECT_CALL(*idGenerator_, GenPartitionId())
        .WillOnce(Return(partitionId));
    EXPECT_CALL(*storage_, StoragePartition(_))
        .WillOnce(Return(true));

    EXPECT_CALL(*mockMetaserverClient_, CreatePartition(_, _, _, _, _, _, _))
        .WillOnce(Return(FSStatusCode::OK));

    CreatePartitionRequest request;
    CreatePartitionResponse response;
    request.set_fsid(0x01);
    request.set_count(1);
    serviceManager_->CreatePartitions(&request, &response);

    ASSERT_EQ(TopoStatusCode::TOPO_OK, response.statuscode());
    ASSERT_EQ(1, response.partitioninfolist().size());

    Partition partition;
    ASSERT_TRUE(topology_->GetPartition(partitionId, &partition));
    ASSERT_EQ(copysetId, partition.GetCopySetId());

    CopySetInfo info;
    CopySetKey key(poolId, copysetId);
    ASSERT_TRUE(topology_->GetCopySet(key, &info));
    ASSERT_EQ(copysetId, info.GetId());
    ASSERT_EQ(1, info.GetPartitionNum());
}

TEST_F(TestTopologyManager,
       test_CreatePartitionWithOutAvailableCopyset_HaveOfflineMetaserver1) {
    PoolIdType poolId = 0x11;
    CopySetIdType copysetId = 0x51;
    PartitionIdType partitionId = 0x61;

    Pool::RedundanceAndPlaceMentPolicy policy;
    policy.replicaNum = 3;
    policy.copysetNum = 0;
    policy.zoneNum = 3;
    PrepareAddPool(poolId, "pool1", policy);
    PrepareAddZone(0x21, "zone1", poolId);
    PrepareAddZone(0x22, "zone2", poolId);
    PrepareAddZone(0x23, "zone3", poolId);
    PrepareAddZone(0x24, "zone4", poolId);
    PrepareAddServer(0x31, "server1", "127.0.0.1", 0, "127.0.0.1", 0, 0x21,
                     0x11);
    PrepareAddServer(0x32, "server2", "127.0.0.1", 0, "127.0.0.1", 0, 0x22,
                     0x11);
    PrepareAddServer(0x33, "server3", "127.0.0.1", 0, "127.0.0.1", 0, 0x23,
                     0x11);
    PrepareAddServer(0x34, "server4", "127.0.0.1", 0, "127.0.0.1", 0, 0x24,
                     0x11);
    PrepareAddMetaServer(0x41, "ms1", "token1", 0x31, "127.0.0.1", 7777, "ip2",
                         8888, OnlineState::OFFLINE);
    PrepareAddMetaServer(0x42, "ms2", "token2", 0x32, "127.0.0.1", 7778, "ip2",
                         8888, OnlineState::OFFLINE);
    PrepareAddMetaServer(0x43, "ms3", "token3", 0x33, "127.0.0.1", 7779, "ip2",
                         8888);
    PrepareAddMetaServer(0x44, "ms4", "token4", 0x34, "127.0.0.1", 7780, "ip2",
                         8888);
    topology_->UpdateMetaServerSpace(MetaServerSpace(100, 0), 0x41);
    topology_->UpdateMetaServerSpace(MetaServerSpace(100, 0), 0x42);
    topology_->UpdateMetaServerSpace(MetaServerSpace(100, 0), 0x43);

    CreatePartitionRequest request;
    CreatePartitionResponse response;
    request.set_fsid(0x01);
    request.set_count(1);
    serviceManager_->CreatePartitions(&request, &response);

    ASSERT_EQ(TopoStatusCode::TOPO_CREATE_COPYSET_ERROR, response.statuscode());
}

TEST_F(TestTopologyManager,
       test_CreatePartitionWithOutAvailableCopyset_HaveOfflineMetaserver2) {
    PoolIdType poolId = 0x11;
    PoolIdType poolId1 = 0x12;
    CopySetIdType copysetId = 0x51;
    PartitionIdType partitionId = 0x61;
    Pool::RedundanceAndPlaceMentPolicy policy;
    policy.replicaNum = 3;
    policy.copysetNum = 0;
    policy.zoneNum = 3;
    PrepareAddPool(poolId, "pool1", policy);
    PrepareAddZone(0x21, "zone1", poolId);
    PrepareAddZone(0x22, "zone2", poolId);
    PrepareAddZone(0x23, "zone3", poolId);
    PrepareAddZone(0x24, "zone4", poolId);
    PrepareAddServer(0x31, "server1", "127.0.0.1", 0, "127.0.0.1", 0, 0x21,
                     0x11);
    PrepareAddServer(0x32, "server2", "127.0.0.1", 0, "127.0.0.1", 0, 0x22,
                     0x11);
    PrepareAddServer(0x33, "server3", "127.0.0.1", 0, "127.0.0.1", 0, 0x23,
                     0x11);
    PrepareAddServer(0x34, "server4", "127.0.0.1", 0, "127.0.0.1", 0, 0x24,
                     0x11);
    PrepareAddMetaServer(0x41, "ms1", "token1", 0x31, "127.0.0.1", 7777, "ip2",
                         8888, OnlineState::OFFLINE);
    PrepareAddMetaServer(0x42, "ms2", "token2", 0x32, "127.0.0.1", 7778, "ip2",
                         8888, OnlineState::OFFLINE);
    PrepareAddMetaServer(0x43, "ms3", "token3", 0x33, "127.0.0.1", 7779, "ip2",
                         8888);
    PrepareAddMetaServer(0x44, "ms4", "token4", 0x34, "127.0.0.1", 7780, "ip2",
                         8888);
    topology_->UpdateMetaServerSpace(MetaServerSpace(100, 0), 0x41);
    topology_->UpdateMetaServerSpace(MetaServerSpace(100, 0), 0x42);
    topology_->UpdateMetaServerSpace(MetaServerSpace(100, 0), 0x43);
    topology_->UpdateMetaServerSpace(MetaServerSpace(100, 0), 0x44);

    PrepareAddPool(poolId1, "pool2", policy);
    PrepareAddZone(0x25, "zone5", poolId1);
    PrepareAddZone(0x26, "zone6", poolId1);
    PrepareAddZone(0x27, "zone7", poolId1);
    PrepareAddServer(0x35, "server5", "127.0.0.1", 0, "127.0.0.1", 0, 0x25,
                     0x12);
    PrepareAddServer(0x36, "server6", "127.0.0.1", 0, "127.0.0.1", 0, 0x26,
                     0x12);
    PrepareAddServer(0x37, "server7", "127.0.0.1", 0, "127.0.0.1", 0, 0x27,
                     0x12);
    PrepareAddMetaServer(0x45, "ms5", "token5", 0x35, "127.0.0.1", 7777, "ip2",
                         8888);
    PrepareAddMetaServer(0x46, "ms6", "token6", 0x36, "127.0.0.1", 7778, "ip2",
                         8888);
    PrepareAddMetaServer(0x47, "ms7", "token7", 0x37, "127.0.0.1", 7779, "ip2",
                         8888);
    topology_->UpdateMetaServerSpace(MetaServerSpace(100, 0), 0x45);
    topology_->UpdateMetaServerSpace(MetaServerSpace(100, 0), 0x46);
    topology_->UpdateMetaServerSpace(MetaServerSpace(100, 0), 0x47);

    EXPECT_CALL(*idGenerator_, GenCopySetId(_)).WillOnce(Return(copysetId));
    EXPECT_CALL(*mockMetaserverClient_, CreateCopySet(_, _, _))
        .WillOnce(Return(FSStatusCode::OK));
    EXPECT_CALL(*storage_, StorageCopySet(_)).WillOnce(Return(true));
    EXPECT_CALL(*idGenerator_, GenPartitionId())
        .WillOnce(Return(partitionId));
    EXPECT_CALL(*storage_, StoragePartition(_))
        .WillOnce(Return(true));

    EXPECT_CALL(*mockMetaserverClient_, CreatePartition(_, _, _, _, _, _, _))
        .WillOnce(Return(FSStatusCode::OK));

    CreatePartitionRequest request;
    CreatePartitionResponse response;
    request.set_fsid(0x01);
    request.set_count(1);
    serviceManager_->CreatePartitions(&request, &response);

    ASSERT_EQ(TopoStatusCode::TOPO_OK, response.statuscode());
    ASSERT_EQ(1, response.partitioninfolist().size());

    Partition partition;
    ASSERT_TRUE(topology_->GetPartition(partitionId, &partition));
    ASSERT_EQ(copysetId, partition.GetCopySetId());

    CopySetInfo info;
    CopySetKey key(poolId1, copysetId);
    ASSERT_TRUE(topology_->GetCopySet(key, &info));
    ASSERT_EQ(copysetId, info.GetId());
    ASSERT_EQ(1, info.GetPartitionNum());
}

TEST_F(TestTopologyManager,
       test_CreatePartitionWithOutAvailableCopyset_MetaServerSpaceIsFull2) {
    PoolIdType poolId = 0x11;
    PoolIdType poolId1 = 0x12;
    CopySetIdType copysetId = 0x51;
    PartitionIdType partitionId = 0x61;
    Pool::RedundanceAndPlaceMentPolicy policy;
    policy.replicaNum = 3;
    policy.copysetNum = 0;
    policy.zoneNum = 3;
    PrepareAddPool(poolId, "pool1", policy);
    PrepareAddZone(0x21, "zone1", poolId);
    PrepareAddZone(0x22, "zone2", poolId);
    PrepareAddZone(0x23, "zone3", poolId);
    PrepareAddZone(0x24, "zone4", poolId);
    PrepareAddServer(0x31, "server1", "127.0.0.1", 0, "127.0.0.1", 0, 0x21,
                     0x11);
    PrepareAddServer(0x32, "server2", "127.0.0.1", 0, "127.0.0.1", 0, 0x22,
                     0x11);
    PrepareAddServer(0x33, "server3", "127.0.0.1", 0, "127.0.0.1", 0, 0x23,
                     0x11);
    PrepareAddServer(0x34, "server4", "127.0.0.1", 0, "127.0.0.1", 0, 0x24,
                     0x11);
    PrepareAddMetaServer(0x41, "ms1", "token1", 0x31, "127.0.0.1", 7777, "ip2",
                         8888);
    PrepareAddMetaServer(0x42, "ms2", "token2", 0x32, "127.0.0.1", 7778, "ip2",
                         8888);
    PrepareAddMetaServer(0x43, "ms3", "token3", 0x33, "127.0.0.1", 7779, "ip2",
                         8888);
    topology_->UpdateMetaServerSpace(MetaServerSpace(100, 0), 0x41);
    topology_->UpdateMetaServerSpace(MetaServerSpace(100, 90), 0x42);
    topology_->UpdateMetaServerSpace(MetaServerSpace(100, 100), 0x43);

    PrepareAddPool(poolId1, "pool2", policy);
    PrepareAddZone(0x25, "zone5", poolId1);
    PrepareAddZone(0x26, "zone6", poolId1);
    PrepareAddZone(0x27, "zone7", poolId1);
    PrepareAddServer(0x35, "server5", "127.0.0.1", 0, "127.0.0.1", 0, 0x25,
                     0x12);
    PrepareAddServer(0x36, "server6", "127.0.0.1", 0, "127.0.0.1", 0, 0x26,
                     0x12);
    PrepareAddServer(0x37, "server7", "127.0.0.1", 0, "127.0.0.1", 0, 0x27,
                     0x12);
    PrepareAddMetaServer(0x45, "ms5", "token5", 0x35, "127.0.0.1", 7777, "ip2",
                         8888);
    PrepareAddMetaServer(0x46, "ms6", "token6", 0x36, "127.0.0.1", 7778, "ip2",
                         8888);
    PrepareAddMetaServer(0x47, "ms7", "token7", 0x37, "127.0.0.1", 7779, "ip2",
                         8888);
    topology_->UpdateMetaServerSpace(MetaServerSpace(100, 50), 0x45);
    topology_->UpdateMetaServerSpace(MetaServerSpace(100, 50), 0x46);
    topology_->UpdateMetaServerSpace(MetaServerSpace(100, 50), 0x47);

    EXPECT_CALL(*idGenerator_, GenCopySetId(_)).WillOnce(Return(copysetId));
    EXPECT_CALL(*mockMetaserverClient_, CreateCopySet(_, _, _))
        .WillOnce(Return(FSStatusCode::OK));
    EXPECT_CALL(*storage_, StorageCopySet(_)).WillOnce(Return(true));
    EXPECT_CALL(*idGenerator_, GenPartitionId())
        .WillOnce(Return(partitionId));
    EXPECT_CALL(*storage_, StoragePartition(_))
        .WillOnce(Return(true));

    EXPECT_CALL(*mockMetaserverClient_, CreatePartition(_, _, _, _, _, _, _))
        .WillOnce(Return(FSStatusCode::OK));

    CreatePartitionRequest request;
    CreatePartitionResponse response;
    request.set_fsid(0x01);
    request.set_count(1);
    serviceManager_->CreatePartitions(&request, &response);

    ASSERT_EQ(TopoStatusCode::TOPO_OK, response.statuscode());
    ASSERT_EQ(1, response.partitioninfolist().size());

    Partition partition;
    ASSERT_TRUE(topology_->GetPartition(partitionId, &partition));
    ASSERT_EQ(copysetId, partition.GetCopySetId());

    CopySetInfo info;
    CopySetKey key(poolId1, copysetId);
    ASSERT_TRUE(topology_->GetCopySet(key, &info));
    ASSERT_EQ(copysetId, info.GetId());
    ASSERT_EQ(1, info.GetPartitionNum());
}

TEST_F(TestTopologyManager, test_CommitTx_Success) {
    PoolIdType poolId = 0x11;
    CopySetIdType copysetId = 0x51;
    PartitionIdType partitionId = 0x61;
    PartitionIdType partitionId2 = 0x62;

    Pool::RedundanceAndPlaceMentPolicy policy;
    policy.replicaNum = 3;
    policy.copysetNum = 0;
    policy.zoneNum = 3;
    PrepareAddPool(poolId, "pool1", policy);
    PrepareAddZone(0x21, "zone1", poolId);
    PrepareAddZone(0x22, "zone2", poolId);
    PrepareAddZone(0x23, "zone3", poolId);
    PrepareAddServer(0x31, "server1", "127.0.0.1", 0, "127.0.0.1", 0, 0x21,
                     0x11);
    PrepareAddServer(0x32, "server2", "127.0.0.1", 0, "127.0.0.1", 0, 0x22,
                     0x11);
    PrepareAddServer(0x33, "server3", "127.0.0.1", 0, "127.0.0.1", 0, 0x23,
                     0x11);
    PrepareAddMetaServer(0x41, "ms1", "token1", 0x31, "127.0.0.1", 7777, "ip2",
                         8888);
    PrepareAddMetaServer(0x42, "ms2", "token2", 0x32, "127.0.0.1", 7778, "ip2",
                         8888);
    PrepareAddMetaServer(0x43, "ms3", "token3", 0x33, "127.0.0.1", 7779, "ip2",
                         8888);
    PrepareAddCopySet(copysetId, poolId, {});
    PrepareAddPartition(0x01, poolId, copysetId, partitionId, 1, 100);
    PrepareAddPartition(0x01, poolId, copysetId, partitionId2, 1, 100);

    CommitTxRequest request;
    PartitionTxId *id1 = request.add_partitiontxids();
    id1->set_partitionid(partitionId);
    id1->set_txid(1);
    PartitionTxId *id2 = request.add_partitiontxids();
    id2->set_partitionid(partitionId2);
    id2->set_txid(2);

    EXPECT_CALL(*storage_, UpdatePartitions(_)).WillOnce(Return(true));

    CommitTxResponse response;
    serviceManager_->CommitTx(&request, &response);

    Partition p1, p2;
    ASSERT_TRUE(topology_->GetPartition(partitionId, &p1));
    ASSERT_TRUE(topology_->GetPartition(partitionId2, &p2));
    ASSERT_EQ(1, p1.GetTxId());
    ASSERT_EQ(2, p2.GetTxId());
}

TEST_F(TestTopologyManager, test_CommitTx_StoreFail) {
    PoolIdType poolId = 0x11;
    CopySetIdType copysetId = 0x51;
    PartitionIdType partitionId = 0x61;
    PartitionIdType partitionId2 = 0x62;

    Pool::RedundanceAndPlaceMentPolicy policy;
    policy.replicaNum = 3;
    policy.copysetNum = 0;
    policy.zoneNum = 3;
    PrepareAddPool(poolId, "pool1", policy);
    PrepareAddZone(0x21, "zone1", poolId);
    PrepareAddZone(0x22, "zone2", poolId);
    PrepareAddZone(0x23, "zone3", poolId);
    PrepareAddServer(0x31, "server1", "127.0.0.1", 0, "127.0.0.1", 0, 0x21,
                     0x11);
    PrepareAddServer(0x32, "server2", "127.0.0.1", 0, "127.0.0.1", 0, 0x22,
                     0x11);
    PrepareAddServer(0x33, "server3", "127.0.0.1", 0, "127.0.0.1", 0, 0x23,
                     0x11);
    PrepareAddMetaServer(0x41, "ms1", "token1", 0x31, "127.0.0.1", 7777, "ip2",
                         8888);
    PrepareAddMetaServer(0x42, "ms2", "token2", 0x32, "127.0.0.1", 7778, "ip2",
                         8888);
    PrepareAddMetaServer(0x43, "ms3", "token3", 0x33, "127.0.0.1", 7779, "ip2",
                         8888);
    PrepareAddCopySet(copysetId, poolId, {});
    PrepareAddPartition(0x01, poolId, copysetId, partitionId, 1, 100);
    PrepareAddPartition(0x01, poolId, copysetId, partitionId2, 1, 100);

    CommitTxRequest request;
    PartitionTxId *id1 = request.add_partitiontxids();
    id1->set_partitionid(partitionId);
    id1->set_txid(1);
    PartitionTxId *id2 = request.add_partitiontxids();
    id2->set_partitionid(partitionId2);
    id2->set_txid(2);

    EXPECT_CALL(*storage_, UpdatePartitions(_)).WillOnce(Return(false));

    CommitTxResponse response;
    serviceManager_->CommitTx(&request, &response);
    ASSERT_EQ(TopoStatusCode::TOPO_STORGE_FAIL, response.statuscode());
}

TEST_F(TestTopologyManager, test_GetMetaServerListInCopysets_Success) {
    PoolIdType poolId = 0x11;
    CopySetIdType copysetId = 0x51;

    Pool::RedundanceAndPlaceMentPolicy policy;
    policy.replicaNum = 3;
    policy.copysetNum = 0;
    policy.zoneNum = 3;
    PrepareAddPool(poolId, "pool1", policy);
    PrepareAddZone(0x21, "zone1", poolId);
    PrepareAddZone(0x22, "zone2", poolId);
    PrepareAddZone(0x23, "zone3", poolId);
    PrepareAddServer(0x31, "server1", "127.0.0.1", 0, "127.0.0.1", 0, 0x21,
                     0x11);
    PrepareAddServer(0x32, "server2", "127.0.0.1", 0, "127.0.0.1", 0, 0x22,
                     0x11);
    PrepareAddServer(0x33, "server3", "127.0.0.1", 0, "127.0.0.1", 0, 0x23,
                     0x11);
    PrepareAddMetaServer(0x41, "ms1", "token1", 0x31, "127.0.0.1", 7777, "ip2",
                         8888);
    PrepareAddMetaServer(0x42, "ms2", "token2", 0x32, "127.0.0.1", 7778, "ip2",
                         8888);
    PrepareAddMetaServer(0x43, "ms3", "token3", 0x33, "127.0.0.1", 7779, "ip2",
                         8888);

    std::set<MetaServerIdType> replicas;
    replicas.insert(0x41);
    replicas.insert(0x42);
    replicas.insert(0x43);
    PrepareAddCopySet(copysetId, poolId, replicas);

    GetMetaServerListInCopySetsRequest request;
    request.set_poolid(poolId);
    request.add_copysetid(copysetId);

    GetMetaServerListInCopySetsResponse response;
    serviceManager_->GetMetaServerListInCopysets(&request, &response);
    ASSERT_EQ(TopoStatusCode::TOPO_OK, response.statuscode());
    ASSERT_EQ(copysetId, response.csinfo(0).copysetid());

    ASSERT_THAT(response.csinfo(0).cslocs(0).metaserverid(),
                AnyOf(0x41, 0x42, 0x43));
    ASSERT_EQ(response.csinfo(0).cslocs(0).internalip(), "127.0.0.1");
    ASSERT_THAT(response.csinfo(0).cslocs(0).internalport(),
        AnyOf(7777, 7778, 7779));
}

TEST_F(TestTopologyManager, test_GetMetaServerListInCopysets_Fail) {
    PoolIdType poolId = 0x11;
    CopySetIdType copysetId = 0x51;

    Pool::RedundanceAndPlaceMentPolicy policy;
    policy.replicaNum = 3;
    policy.copysetNum = 0;
    policy.zoneNum = 3;
    PrepareAddPool(poolId, "pool1", policy);
    PrepareAddZone(0x21, "zone1", poolId);
    PrepareAddZone(0x22, "zone2", poolId);
    PrepareAddZone(0x23, "zone3", poolId);
    PrepareAddServer(0x31, "server1", "127.0.0.1", 0, "127.0.0.1", 0, 0x21,
                     0x11);
    PrepareAddServer(0x32, "server2", "127.0.0.1", 0, "127.0.0.1", 0, 0x22,
                     0x11);
    PrepareAddServer(0x33, "server3", "127.0.0.1", 0, "127.0.0.1", 0, 0x23,
                     0x11);
    PrepareAddMetaServer(0x41, "ms1", "token1", 0x31, "127.0.0.1", 7777, "ip2",
                         8888);
    PrepareAddMetaServer(0x42, "ms2", "token2", 0x32, "127.0.0.1", 7778, "ip2",
                         8888);
    PrepareAddMetaServer(0x43, "ms3", "token3", 0x33, "127.0.0.1", 7779, "ip2",
                         8888);

    std::set<MetaServerIdType> replicas;
    replicas.insert(0x41);
    replicas.insert(0x42);
    replicas.insert(0x43);
    PrepareAddCopySet(copysetId, poolId, replicas);

    GetMetaServerListInCopySetsRequest request;
    request.set_poolid(poolId);
    request.add_copysetid(copysetId + 1);

    GetMetaServerListInCopySetsResponse response;
    serviceManager_->GetMetaServerListInCopysets(&request, &response);
    ASSERT_EQ(TopoStatusCode::TOPO_COPYSET_NOT_FOUND, response.statuscode());
}

TEST_F(TestTopologyManager, test_ListPartition_Success) {
    FsIdType fsId = 0x01;
    PoolIdType poolId = 0x11;
    CopySetIdType copysetId = 0x51;
    PartitionIdType pId1 = 0x61;
    PartitionIdType pId2 = 0x62;
    PartitionIdType pId3 = 0x63;

    Pool::RedundanceAndPlaceMentPolicy policy;
    policy.replicaNum = 3;
    policy.copysetNum = 0;
    policy.zoneNum = 3;
    PrepareAddPool(poolId, "pool1", policy);
    PrepareAddZone(0x21, "zone1", poolId);
    PrepareAddZone(0x22, "zone2", poolId);
    PrepareAddZone(0x23, "zone3", poolId);
    PrepareAddServer(0x31, "server1", "127.0.0.1", 0, "127.0.0.1", 0, 0x21,
                     0x11);
    PrepareAddServer(0x32, "server2", "127.0.0.1", 0, "127.0.0.1", 0, 0x22,
                     0x11);
    PrepareAddServer(0x33, "server3", "127.0.0.1", 0, "127.0.0.1", 0, 0x23,
                     0x11);
    PrepareAddMetaServer(0x41, "ms1", "token1", 0x31, "127.0.0.1", 7777, "ip2",
                         8888);
    PrepareAddMetaServer(0x42, "ms2", "token2", 0x32, "127.0.0.1", 7778, "ip2",
                         8888);
    PrepareAddMetaServer(0x43, "ms3", "token3", 0x33, "127.0.0.1", 7779, "ip2",
                         8888);

    std::set<MetaServerIdType> replicas;
    replicas.insert(0x41);
    replicas.insert(0x42);
    replicas.insert(0x43);
    PrepareAddCopySet(copysetId, poolId, replicas);
    PrepareAddPartition(fsId, poolId, copysetId, pId1, 1, 100);
    PrepareAddPartition(fsId, poolId, copysetId, pId2, 1, 100);
    PrepareAddPartition(fsId + 1, poolId, copysetId, pId3, 1, 100);

    ListPartitionRequest request;
    request.set_fsid(fsId);

    ListPartitionResponse response;
    serviceManager_->ListPartition(&request, &response);
    ASSERT_EQ(TopoStatusCode::TOPO_OK, response.statuscode());
    ASSERT_EQ(2, response.partitioninfolist().size());
}

TEST_F(TestTopologyManager, test_GetLatestPartitionsTxId) {
    PrepareTopo();
    PartitionIdType pId1 = 0x61;

    {
        LOG(INFO) << "### case1: partition need update ###";
        PartitionTxId tmp;
        tmp.set_partitionid(pId1);
        tmp.set_txid(1);
        std::vector<PartitionTxId> partitionList({tmp});
        std::vector<PartitionTxId> out;
        serviceManager_->GetLatestPartitionsTxId(partitionList, &out);
        ASSERT_EQ(1, out.size());
        ASSERT_EQ(pId1, out[0].partitionid());
        ASSERT_EQ(2, out[0].txid());
    }
    {
        LOG(INFO) << "### case2: partition no need update ###";
        PartitionTxId tmp;
        tmp.set_partitionid(pId1);
        tmp.set_txid(2);
        std::vector<PartitionTxId> partitionList({tmp});
        std::vector<PartitionTxId> out;
        serviceManager_->GetLatestPartitionsTxId(partitionList, &out);
        ASSERT_TRUE(out.empty());
    }
}

TEST_F(TestTopologyManager, test_ListPartitionOfFs_Success) {
    FsIdType fsId = 0x01;
    PoolIdType poolId = 0x11;
    CopySetIdType copysetId = 0x51;
    PartitionIdType pId1 = 0x61;
    PartitionIdType pId2 = 0x62;
    PartitionIdType pId3 = 0x63;

    Pool::RedundanceAndPlaceMentPolicy policy;
    policy.replicaNum = 3;
    policy.copysetNum = 0;
    policy.zoneNum = 3;
    PrepareAddPool(poolId, "pool1", policy);
    PrepareAddZone(0x21, "zone1", poolId);
    PrepareAddZone(0x22, "zone2", poolId);
    PrepareAddZone(0x23, "zone3", poolId);
    PrepareAddServer(0x31, "server1", "127.0.0.1", 0, "127.0.0.1", 0, 0x21,
                     0x11);
    PrepareAddServer(0x32, "server2", "127.0.0.1", 0, "127.0.0.1", 0, 0x22,
                     0x11);
    PrepareAddServer(0x33, "server3", "127.0.0.1", 0, "127.0.0.1", 0, 0x23,
                     0x11);
    PrepareAddMetaServer(0x41, "ms1", "token1", 0x31, "127.0.0.1", 7777, "ip2",
                         8888);
    PrepareAddMetaServer(0x42, "ms2", "token2", 0x32, "127.0.0.1", 7778, "ip2",
                         8888);
    PrepareAddMetaServer(0x43, "ms3", "token3", 0x33, "127.0.0.1", 7779, "ip2",
                         8888);

    std::set<MetaServerIdType> replicas;
    replicas.insert(0x41);
    replicas.insert(0x42);
    replicas.insert(0x43);
    PrepareAddCopySet(copysetId, poolId, replicas);
    PrepareAddPartition(fsId, poolId, copysetId, pId1, 1, 100);
    PrepareAddPartition(fsId, poolId, copysetId, pId2, 1, 100);
    PrepareAddPartition(fsId + 1, poolId, copysetId, pId3, 1, 100);

    std::list<PartitionInfo> list;
    serviceManager_->ListPartitionOfFs(fsId, &list);
    ASSERT_EQ(2, list.size());
    for (auto item : list) {
        ASSERT_THAT(item.partitionid(), AnyOf(pId1, pId2));
    }
}

TEST_F(TestTopologyManager, test_ListPartitionEmpty_Success) {
    FsIdType fsId = 0x01;
    PoolIdType poolId = 0x11;
    CopySetIdType copysetId = 0x51;
    PartitionIdType pId1 = 0x61;
    PartitionIdType pId2 = 0x62;
    PartitionIdType pId3 = 0x63;

    Pool::RedundanceAndPlaceMentPolicy policy;
    policy.replicaNum = 3;
    policy.copysetNum = 0;
    policy.zoneNum = 3;
    PrepareAddPool(poolId, "pool1", policy);
    PrepareAddZone(0x21, "zone1", poolId);
    PrepareAddZone(0x22, "zone2", poolId);
    PrepareAddZone(0x23, "zone3", poolId);
    PrepareAddServer(0x31, "server1", "127.0.0.1", 0, "127.0.0.1", 0, 0x21,
                     0x11);
    PrepareAddServer(0x32, "server2", "127.0.0.1", 0, "127.0.0.1", 0, 0x22,
                     0x11);
    PrepareAddServer(0x33, "server3", "127.0.0.1", 0, "127.0.0.1", 0, 0x23,
                     0x11);
    PrepareAddMetaServer(0x41, "ms1", "token1", 0x31, "127.0.0.1", 7777, "ip2",
                         8888);
    PrepareAddMetaServer(0x42, "ms2", "token2", 0x32, "127.0.0.1", 7778, "ip2",
                         8888);
    PrepareAddMetaServer(0x43, "ms3", "token3", 0x33, "127.0.0.1", 7779, "ip2",
                         8888);

    std::set<MetaServerIdType> replicas;
    replicas.insert(0x41);
    replicas.insert(0x42);
    replicas.insert(0x43);
    PrepareAddCopySet(copysetId, poolId, replicas);
    PrepareAddPartition(fsId, poolId, copysetId, pId1, 1, 100);

    ListPartitionRequest request;
    request.set_fsid(fsId + 1);

    ListPartitionResponse response;
    serviceManager_->ListPartition(&request, &response);
    ASSERT_EQ(TopoStatusCode::TOPO_OK, response.statuscode());
    ASSERT_EQ(0, response.partitioninfolist().size());
}

TEST_F(TestTopologyManager, test_GetCopysetOfPartition_Success) {
    FsIdType fsId = 0x01;
    PoolIdType poolId = 0x11;
    CopySetIdType copysetId = 0x51;
    CopySetIdType copysetId2 = 0x52;
    PartitionIdType pId = 0x61;
    PartitionIdType pId2 = 0x62;

    Pool::RedundanceAndPlaceMentPolicy policy;
    policy.replicaNum = 3;
    policy.copysetNum = 0;
    policy.zoneNum = 3;
    PrepareAddPool(poolId, "pool1", policy);
    PrepareAddZone(0x21, "zone1", poolId);
    PrepareAddZone(0x22, "zone2", poolId);
    PrepareAddZone(0x23, "zone3", poolId);
    PrepareAddServer(0x31, "server1", "127.0.0.1", 0, "127.0.0.1", 0, 0x21,
                     0x11);
    PrepareAddServer(0x32, "server2", "127.0.0.1", 0, "127.0.0.1", 0, 0x22,
                     0x11);
    PrepareAddServer(0x33, "server3", "127.0.0.1", 0, "127.0.0.1", 0, 0x23,
                     0x11);
    PrepareAddMetaServer(0x41, "ms1", "token1", 0x31, "127.0.0.1", 7777, "ip2",
                         8888);
    PrepareAddMetaServer(0x42, "ms2", "token2", 0x32, "127.0.0.1", 7778, "ip2",
                         8888);
    PrepareAddMetaServer(0x43, "ms3", "token3", 0x33, "127.0.0.1", 7779, "ip2",
                         8888);

    std::set<MetaServerIdType> replicas;
    replicas.insert(0x41);
    replicas.insert(0x42);
    replicas.insert(0x43);
    PrepareAddCopySet(copysetId, poolId, replicas);
    PrepareAddCopySet(copysetId2, poolId, replicas);
    PrepareAddPartition(fsId, poolId, copysetId, pId, 1, 100);
    PrepareAddPartition(fsId, poolId, copysetId2, pId2, 1, 100);

    GetCopysetOfPartitionRequest request;
    request.add_partitionid(pId);
    request.add_partitionid(pId2);

    GetCopysetOfPartitionResponse response;
    serviceManager_->GetCopysetOfPartition(&request, &response);
    ASSERT_EQ(TopoStatusCode::TOPO_OK, response.statuscode());
    ASSERT_EQ(2, response.copysetmap().size());
    auto iter = response.copysetmap().begin();
    ASSERT_EQ(poolId, iter->second.poolid());
    ASSERT_THAT(iter->second.copysetid(), AnyOf(copysetId, copysetId2));
    iter++;
    ASSERT_EQ(poolId, iter->second.poolid());
    ASSERT_THAT(iter->second.copysetid(), AnyOf(copysetId, copysetId2));
}

TEST_F(TestTopologyManager, test_GetCopysetOfPartition_CopysetNotFound) {
    FsIdType fsId = 0x01;
    PoolIdType poolId = 0x11;
    CopySetIdType copysetId = 0x51;
    PartitionIdType pId = 0x61;

    Pool::RedundanceAndPlaceMentPolicy policy;
    policy.replicaNum = 3;
    policy.copysetNum = 0;
    policy.zoneNum = 3;
    PrepareAddPool(poolId, "pool1", policy);
    PrepareAddZone(0x21, "zone1", poolId);
    PrepareAddZone(0x22, "zone2", poolId);
    PrepareAddZone(0x23, "zone3", poolId);
    PrepareAddServer(0x31, "server1", "127.0.0.1", 0, "127.0.0.1", 0, 0x21,
                     0x11);
    PrepareAddServer(0x32, "server2", "127.0.0.1", 0, "127.0.0.1", 0, 0x22,
                     0x11);
    PrepareAddServer(0x33, "server3", "127.0.0.1", 0, "127.0.0.1", 0, 0x23,
                     0x11);
    PrepareAddMetaServer(0x41, "ms1", "token1", 0x31, "127.0.0.1", 7777, "ip2",
                         8888);
    PrepareAddMetaServer(0x42, "ms2", "token2", 0x32, "127.0.0.1", 7778, "ip2",
                         8888);
    PrepareAddMetaServer(0x43, "ms3", "token3", 0x33, "127.0.0.1", 7779, "ip2",
                         8888);

    std::set<MetaServerIdType> replicas;
    replicas.insert(0x41);
    replicas.insert(0x42);
    replicas.insert(0x43);
    PrepareAddCopySet(copysetId, poolId, replicas);
    PrepareAddPartition(fsId, poolId, copysetId, pId, 1, 100);

    GetCopysetOfPartitionRequest request;
    request.add_partitionid(pId + 1);

    GetCopysetOfPartitionResponse response;
    serviceManager_->GetCopysetOfPartition(&request, &response);
    ASSERT_EQ(TopoStatusCode::TOPO_COPYSET_NOT_FOUND, response.statuscode());
}

TEST_F(TestTopologyManager, test_GetCopysetMembers_Success) {
    FsIdType fsId = 0x01;
    PoolIdType poolId = 0x11;
    CopySetIdType copysetId = 0x51;

    Pool::RedundanceAndPlaceMentPolicy policy;
    policy.replicaNum = 3;
    policy.copysetNum = 0;
    policy.zoneNum = 3;
    PrepareAddPool(poolId, "pool1", policy);
    PrepareAddZone(0x21, "zone1", poolId);
    PrepareAddZone(0x22, "zone2", poolId);
    PrepareAddZone(0x23, "zone3", poolId);
    PrepareAddServer(0x31, "server1", "127.0.0.1", 0, "127.0.0.1", 0, 0x21,
                     0x11);
    PrepareAddServer(0x32, "server2", "127.0.0.1", 0, "127.0.0.1", 0, 0x22,
                     0x11);
    PrepareAddServer(0x33, "server3", "127.0.0.1", 0, "127.0.0.1", 0, 0x23,
                     0x11);
    PrepareAddMetaServer(0x41, "ms1", "token1", 0x31, "127.0.0.1", 7777, "ip2",
                         8887);
    PrepareAddMetaServer(0x42, "ms2", "token2", 0x32, "127.0.0.1", 7778, "ip2",
                         8888);
    PrepareAddMetaServer(0x43, "ms3", "token3", 0x33, "127.0.0.1", 7779, "ip2",
                         8889);

    std::set<MetaServerIdType> replicas;
    replicas.insert(0x41);
    replicas.insert(0x42);
    replicas.insert(0x43);
    PrepareAddCopySet(copysetId, poolId, replicas);
    std::set<std::string> addrs;
    auto ret = serviceManager_->GetCopysetMembers(poolId, copysetId, &addrs);
    ASSERT_EQ(TopoStatusCode::TOPO_OK, ret);
    ASSERT_EQ(3, addrs.size());
    ASSERT_THAT(*addrs.begin(), AnyOf("ip2:8887", "ip2:8888", "ip2:8889"));
}

}  // namespace topology
}  // namespace mds
}  // namespace curvefs
