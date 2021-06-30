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
 * Created Date: Thu Sep 20 2018
 * Author: xuchaojie
 */

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <brpc/controller.h>
#include <brpc/channel.h>
#include <brpc/server.h>

#include "proto/topology.pb.h"
#include "src/mds/topology/topology_service_manager.h"
#include "src/mds/common/mds_define.h"
#include "test/mds/topology/mock_topology.h"

namespace curve {
namespace mds {
namespace topology {

using ::testing::Return;
using ::testing::_;
using ::testing::AnyOf;
using ::testing::SetArgPointee;
using ::testing::Invoke;

using ::curve::chunkserver::MockCopysetServiceImpl;
using ::curve::chunkserver::CopysetResponse2;
using ::curve::chunkserver::COPYSET_OP_STATUS;
using ::curve::mds::copyset::CopysetOption;
using ::curve::common::CopysetInfo;

class TestTopologyServiceManager : public ::testing::Test {
 protected:
    TestTopologyServiceManager() {}
    virtual ~TestTopologyServiceManager() {}
    virtual void SetUp() {
        server_ = new brpc::Server();

        idGenerator_ = std::make_shared<MockIdGenerator>();
        tokenGenerator_ = std::make_shared<MockTokenGenerator>();
        storage_ = std::make_shared<MockStorage>();
        topology_ = std::make_shared<TopologyImpl>(idGenerator_,
                                               tokenGenerator_,
                                               storage_);
        TopologyOption topologyOption;
        CopysetOption copysetOption;
        copysetManager_ =
            std::make_shared<curve::mds::copyset::CopysetManager>(
                copysetOption);
        serviceManager_ = std::make_shared<TopologyServiceManager>(topology_,
             copysetManager_);
        serviceManager_->Init(topologyOption);

        mockCopySetService =
            new MockCopysetServiceImpl();
        ASSERT_EQ(server_->AddService(mockCopySetService,
                                      brpc::SERVER_DOESNT_OWN_SERVICE), 0);

        ASSERT_EQ(0, server_->Start("127.0.0.1", {8900, 8999}, nullptr));

        listenAddr_ = server_->listen_address();
    }

    virtual void TearDown() {
        idGenerator_ = nullptr;
        tokenGenerator_ = nullptr;
        storage_ = nullptr;
        topology_ = nullptr;
        copysetManager_ = nullptr;
        serviceManager_ = nullptr;

        server_->Stop(0);
        server_->Join();
        delete server_;
        server_ = nullptr;
        delete mockCopySetService;
        mockCopySetService = nullptr;
    }

 protected:
    void PrepareAddLogicalPool(PoolIdType id = 0x01,
            const std::string &name = "testLogicalPool",
            PoolIdType phyPoolId = 0x11,
            LogicalPoolType  type = PAGEFILE,
            const LogicalPool::RedundanceAndPlaceMentPolicy &rap =
                LogicalPool::RedundanceAndPlaceMentPolicy(),
            const LogicalPool::UserPolicy &policy = LogicalPool::UserPolicy(),
            uint64_t createTime = 0x888
            ) {
        LogicalPool pool(id,
                name,
                phyPoolId,
                type,
                rap,
                policy,
                createTime,
                true,
                true);

        EXPECT_CALL(*storage_, StorageLogicalPool(_))
            .WillOnce(Return(true));

        int ret = topology_->AddLogicalPool(pool);
        ASSERT_EQ(kTopoErrCodeSuccess, ret)
            << "should have PrepareAddPhysicalPool()";
    }


    void PrepareAddPhysicalPool(PoolIdType id = 0x11,
                 const std::string &name = "testPhysicalPool",
                 const std::string &desc = "descPhysicalPool") {
        PhysicalPool pool(id,
                name,
                desc);
        EXPECT_CALL(*storage_, StoragePhysicalPool(_))
            .WillOnce(Return(true));

        int ret = topology_->AddPhysicalPool(pool);
        ASSERT_EQ(kTopoErrCodeSuccess, ret);
    }

    void PrepareAddZone(ZoneIdType id = 0x21,
            const std::string &name = "testZone",
            PoolIdType physicalPoolId = 0x11,
            const std::string &desc = "descZone") {
        Zone zone(id, name, physicalPoolId, desc);
        EXPECT_CALL(*storage_, StorageZone(_))
            .WillOnce(Return(true));
        int ret = topology_->AddZone(zone);
        ASSERT_EQ(kTopoErrCodeSuccess, ret) <<
            "should have PrepareAddPhysicalPool()";
    }

    void PrepareAddServer(ServerIdType id = 0x31,
           const std::string &hostName = "testServer",
           const std::string &internalHostIp = "testInternalIp",
           const std::string &externalHostIp = "testExternalIp",
           ZoneIdType zoneId = 0x21,
           PoolIdType physicalPoolId = 0x11,
           const std::string &desc = "descServer") {
        Server server(id,
                hostName,
                internalHostIp,
                0,
                externalHostIp,
                0,
                zoneId,
                physicalPoolId,
                desc);
        EXPECT_CALL(*storage_, StorageServer(_))
            .WillOnce(Return(true));
        int ret = topology_->AddServer(server);
        ASSERT_EQ(kTopoErrCodeSuccess, ret) << "should have PrepareAddZone()";
    }

    void PrepareAddChunkServer(ChunkServerIdType id = 0x41,
                const std::string &token = "testToken",
                const std::string &diskType = "nvme",
                ServerIdType serverId = 0x31,
                const std::string &hostIp = "testInternalIp",
                const std::string &externalHostIp = "testExternalIp",
                uint32_t port = 0,
                const std::string &diskPath = "/") {
            ChunkServer cs(id,
                    token,
                    diskType,
                    serverId,
                    hostIp,
                    port,
                    diskPath,
                    READWRITE,
                    OnlineState::OFFLINE,
                    externalHostIp);
            EXPECT_CALL(*storage_, StorageChunkServer(_))
                .WillOnce(Return(true));
        int ret = topology_->AddChunkServer(cs);
        ASSERT_EQ(kTopoErrCodeSuccess, ret) << "should have PrepareAddServer()";
    }

    void PrepareAddCopySet(CopySetIdType copysetId,
        PoolIdType logicalPoolId,
        const std::set<ChunkServerIdType> &members) {
        CopySetInfo cs(logicalPoolId,
            copysetId);
        cs.SetCopySetMembers(members);
        EXPECT_CALL(*storage_, StorageCopySet(_))
            .WillOnce(Return(true));
        int ret = topology_->AddCopySet(cs);
        ASSERT_EQ(kTopoErrCodeSuccess, ret)
            << "should have PrepareAddLogicalPool()";
    }

 protected:
    std::shared_ptr<MockIdGenerator> idGenerator_;
    std::shared_ptr<MockTokenGenerator> tokenGenerator_;
    std::shared_ptr<MockStorage> storage_;
    std::shared_ptr<Topology> topology_;
    std::shared_ptr<curve::mds::copyset::CopysetManager> copysetManager_;
    std::shared_ptr<TopologyServiceManager> serviceManager_;

    butil::EndPoint listenAddr_;
    brpc::Server *server_;
    MockCopysetServiceImpl *mockCopySetService;
};


TEST_F(TestTopologyServiceManager, test_RegistChunkServer_SuccessWithExIp) {
    ChunkServerIdType csId = 0x41;
    ServerIdType serverId = 0x31;
    std::string token = "token";

    PrepareAddPhysicalPool();
    PrepareAddZone();
    PrepareAddServer(serverId, "testServer", "testInternalIp", "externalIp1");

    ChunkServerRegistRequest request;
    request.set_disktype("ssd");
    request.set_diskpath("/");
    request.set_hostip("testInternalIp");
    request.set_externalip("externalIp1");
    request.set_port(100);

    ChunkServerRegistResponse response;

    EXPECT_CALL(*tokenGenerator_, GenToken())
        .WillOnce(Return(token));
    EXPECT_CALL(*idGenerator_, GenChunkServerId())
        .WillOnce(Return(csId));

    EXPECT_CALL(*storage_, StorageChunkServer(_))
        .WillOnce(Return(true));
    serviceManager_->RegistChunkServer(&request, &response);

    ASSERT_EQ(kTopoErrCodeSuccess, response.statuscode());
    ASSERT_TRUE(response.has_chunkserverid());
    ASSERT_EQ(csId, response.chunkserverid());
    ASSERT_TRUE(response.has_token());
    ASSERT_EQ(token, response.token());
    ChunkServer chunkserver;
    ASSERT_TRUE(topology_->GetChunkServer(csId, &chunkserver));
    ASSERT_EQ("externalIp1", chunkserver.GetExternalHostIp());
}

TEST_F(TestTopologyServiceManager, test_RegistChunkServer_ExIpNotMatch) {
    ChunkServerIdType csId = 0x41;
    ServerIdType serverId = 0x31;
    std::string token = "token";

    PrepareAddPhysicalPool();
    PrepareAddZone();
    PrepareAddServer(serverId, "testServer", "testInternalIp", "externalIp1");

    ChunkServerRegistRequest request;
    request.set_disktype("ssd");
    request.set_diskpath("/");
    request.set_hostip("testInternalIp");
    request.set_externalip("externalIp2");
    request.set_port(100);

    ChunkServerRegistResponse response;

    EXPECT_CALL(*tokenGenerator_, GenToken())
        .WillOnce(Return(token));
    EXPECT_CALL(*idGenerator_, GenChunkServerId())
        .WillOnce(Return(csId));

    serviceManager_->RegistChunkServer(&request, &response);

    ASSERT_EQ(kTopoErrCodeInternalError, response.statuscode());
}

TEST_F(TestTopologyServiceManager, test_RegistChunkServer_SuccessWithoutExIp) {
    ChunkServerIdType csId = 0x41;
    ServerIdType serverId = 0x31;
    std::string token = "token";

    PrepareAddPhysicalPool();
    PrepareAddZone();
    PrepareAddServer(serverId, "testServer", "testInternalIp", "externalIp1");

    ChunkServerRegistRequest request;
    request.set_disktype("ssd");
    request.set_diskpath("/");
    request.set_hostip("testInternalIp");
    request.set_port(100);

    ChunkServerRegistResponse response;

    EXPECT_CALL(*tokenGenerator_, GenToken())
        .WillOnce(Return(token));
    EXPECT_CALL(*idGenerator_, GenChunkServerId())
        .WillOnce(Return(csId));

    EXPECT_CALL(*storage_, StorageChunkServer(_))
        .WillOnce(Return(true));
    serviceManager_->RegistChunkServer(&request, &response);

    ASSERT_EQ(kTopoErrCodeSuccess, response.statuscode());
    ASSERT_TRUE(response.has_chunkserverid());
    ASSERT_EQ(csId, response.chunkserverid());
    ASSERT_TRUE(response.has_token());
    ASSERT_EQ(token, response.token());
    ChunkServer chunkserver;
    ASSERT_TRUE(topology_->GetChunkServer(csId, &chunkserver));
    ASSERT_EQ("externalIp1", chunkserver.GetExternalHostIp());
}

TEST_F(TestTopologyServiceManager, test_RegistChunkServer_ServerNotFound) {
    ServerIdType serverId = 0x31;
    std::string token = "token";

    PrepareAddPhysicalPool();
    PrepareAddZone();
    PrepareAddServer(serverId, "testServer", "testInternalIp");

    ChunkServerRegistRequest request;
    request.set_disktype("ssd");
    request.set_diskpath("/");
    request.set_hostip("unExistIp");
    request.set_port(100);

    ChunkServerRegistResponse response;

    serviceManager_->RegistChunkServer(&request, &response);

    ASSERT_EQ(kTopoErrCodeServerNotFound, response.statuscode());
}

TEST_F(TestTopologyServiceManager, test_RegistChunkServer_AllocateIdFail) {
    ServerIdType serverId = 0x31;
    std::string token = "token";

    PrepareAddPhysicalPool();
    PrepareAddZone();
    PrepareAddServer(serverId, "testServer", "testInternalIp");

    ChunkServerRegistRequest request;
    request.set_disktype("ssd");
    request.set_diskpath("/");
    request.set_hostip("testInternalIp");
    request.set_port(100);

    ChunkServerRegistResponse response;

    EXPECT_CALL(*idGenerator_, GenChunkServerId())
        .WillOnce(Return(UNINTIALIZE_ID));

    serviceManager_->RegistChunkServer(&request, &response);

    ASSERT_EQ(kTopoErrCodeAllocateIdFail, response.statuscode());
}

TEST_F(TestTopologyServiceManager, test_RegistChunkServer_AddChunkServerFail) {
    ChunkServerIdType csId = 0x41;
    ServerIdType serverId = 0x31;
    std::string token = "token";

    PrepareAddPhysicalPool();
    PrepareAddZone();
    PrepareAddServer(serverId, "testServer", "testInternalIp");

    ChunkServerRegistRequest request;
    request.set_disktype("ssd");
    request.set_diskpath("/");
    request.set_hostip("testInternalIp");
    request.set_port(100);

    ChunkServerRegistResponse response;

    EXPECT_CALL(*tokenGenerator_, GenToken())
        .WillOnce(Return(token));
    EXPECT_CALL(*idGenerator_, GenChunkServerId())
        .WillOnce(Return(csId));

    EXPECT_CALL(*storage_, StorageChunkServer(_))
        .WillOnce(Return(false));
    serviceManager_->RegistChunkServer(&request, &response);

    ASSERT_EQ(kTopoErrCodeStorgeFail, response.statuscode());
    ASSERT_FALSE(response.has_chunkserverid());
    ASSERT_FALSE(response.has_token());
}

TEST_F(TestTopologyServiceManager, test_ListChunkServer_ByIdSuccess) {
    ChunkServerIdType csId1 = 0x41;
    ChunkServerIdType csId2 = 0x42;
    ServerIdType serverId = 0x31;

    PrepareAddPhysicalPool();
    PrepareAddZone();
    PrepareAddServer(serverId, "server", "ip1", "ip2");
    PrepareAddChunkServer(csId1, "token1", "nvme", serverId, "ip1", "ip2", 100);
    PrepareAddChunkServer(csId2, "token2", "nvme", serverId, "ip1", "ip2", 200);

    ListChunkServerRequest request;
    request.set_serverid(serverId);

    ListChunkServerResponse response;

    serviceManager_->ListChunkServer(&request, &response);

    ASSERT_EQ(kTopoErrCodeSuccess, response.statuscode());

    ASSERT_EQ(2, response.chunkserverinfos_size());

    ASSERT_THAT(response.chunkserverinfos(0).chunkserverid(),
        AnyOf(csId1, csId2));
    ASSERT_THAT(response.chunkserverinfos(0).disktype(), "nvme");
    ASSERT_EQ("ip1", response.chunkserverinfos(0).hostip());
    ASSERT_EQ("ip2", response.chunkserverinfos(0).externalip());
    ASSERT_THAT(response.chunkserverinfos(0).port(), AnyOf(100, 200));

    ASSERT_THAT(response.chunkserverinfos(1).chunkserverid(),
        AnyOf(csId1, csId2));
    ASSERT_THAT(response.chunkserverinfos(1).disktype(), "nvme");
    ASSERT_EQ("ip1", response.chunkserverinfos(1).hostip());
    ASSERT_EQ("ip2", response.chunkserverinfos(1).externalip());
    ASSERT_THAT(response.chunkserverinfos(1).port(), AnyOf(100, 200));
}

TEST_F(TestTopologyServiceManager, test_ListChunkServer_ByIpSuccess) {
    ChunkServerIdType csId1 = 0x41;
    ChunkServerIdType csId2 = 0x42;
    ServerIdType serverId = 0x31;

    PrepareAddPhysicalPool();
    PrepareAddZone();
    PrepareAddServer(serverId, "server", "ip1", "ip2");
    PrepareAddChunkServer(csId1, "token1", "nvme", serverId, "ip1", "ip2", 100);
    PrepareAddChunkServer(csId2, "token2", "nvme", serverId, "ip1", "ip2", 200);

    ListChunkServerRequest request;
    request.set_ip("ip1");

    ListChunkServerResponse response;

    serviceManager_->ListChunkServer(&request, &response);

    ASSERT_EQ(kTopoErrCodeSuccess, response.statuscode());

    ASSERT_EQ(2, response.chunkserverinfos_size());

    ASSERT_THAT(response.chunkserverinfos(0).chunkserverid(),
        AnyOf(csId1, csId2));
    ASSERT_THAT(response.chunkserverinfos(0).disktype(), "nvme");
    ASSERT_EQ("ip1", response.chunkserverinfos(0).hostip());
    ASSERT_EQ("ip2", response.chunkserverinfos(0).externalip());
    ASSERT_THAT(response.chunkserverinfos(0).port(), AnyOf(100, 200));

    ASSERT_THAT(response.chunkserverinfos(1).chunkserverid(),
        AnyOf(csId1, csId2));
    ASSERT_THAT(response.chunkserverinfos(1).disktype(), "nvme");
    ASSERT_EQ("ip1", response.chunkserverinfos(1).hostip());
    ASSERT_EQ("ip2", response.chunkserverinfos(1).externalip());
    ASSERT_THAT(response.chunkserverinfos(1).port(), AnyOf(100, 200));
}

TEST_F(TestTopologyServiceManager, test_ListChunkServer_ServerNotFound) {
    ChunkServerIdType csId1 = 0x41;
    ChunkServerIdType csId2 = 0x42;
    ServerIdType serverId = 0x31;

    PrepareAddPhysicalPool();
    PrepareAddZone();
    PrepareAddServer(serverId, "server", "ip1", "ip2");
    PrepareAddChunkServer(csId1, "token1", "nvme", serverId, "ip1", "ip2", 100);
    PrepareAddChunkServer(csId2, "token2", "nvme", serverId, "ip1", "ip2", 200);

    ListChunkServerRequest request;
    request.set_serverid(++serverId);

    ListChunkServerResponse response;

    serviceManager_->ListChunkServer(&request, &response);

    ASSERT_EQ(kTopoErrCodeServerNotFound, response.statuscode());

    ASSERT_EQ(0, response.chunkserverinfos_size());
}

TEST_F(TestTopologyServiceManager, test_ListChunkServer_IpServerNotFound) {
    ChunkServerIdType csId1 = 0x41;
    ChunkServerIdType csId2 = 0x42;
    ServerIdType serverId = 0x31;

    PrepareAddPhysicalPool();
    PrepareAddZone();
    PrepareAddServer(serverId, "server", "ip1", "ip2");
    PrepareAddChunkServer(csId1, "token1", "nvme", serverId, "ip1", "ip2", 100);
    PrepareAddChunkServer(csId2, "token2", "nvme", serverId, "ip1", "ip2", 200);

    ListChunkServerRequest request;
    request.set_ip("ip3");

    ListChunkServerResponse response;

    serviceManager_->ListChunkServer(&request, &response);

    ASSERT_EQ(kTopoErrCodeServerNotFound, response.statuscode());

    ASSERT_EQ(0, response.chunkserverinfos_size());
}

TEST_F(TestTopologyServiceManager, test_ListChunkServer_InvalidParam) {
    ListChunkServerRequest request;
    ListChunkServerResponse response;

    serviceManager_->ListChunkServer(&request, &response);

    ASSERT_EQ(kTopoErrCodeInvalidParam, response.statuscode());

    ASSERT_EQ(0, response.chunkserverinfos_size());
}

TEST_F(TestTopologyServiceManager, test_GetChunkServer_ByIdSuccess) {
    ChunkServerIdType csId1 = 0x41;
    ServerIdType serverId = 0x31;

    PrepareAddPhysicalPool();
    PrepareAddZone();
    PrepareAddServer(serverId, "server", "ip1", "ip2");
    PrepareAddChunkServer(csId1, "token1", "nvme", serverId, "ip1", "ip2", 100);

    GetChunkServerInfoRequest request;
    request.set_chunkserverid(csId1);

    GetChunkServerInfoResponse response;
    serviceManager_->GetChunkServer(&request, &response);

    ASSERT_EQ(kTopoErrCodeSuccess, response.statuscode());
    ASSERT_TRUE(response.has_chunkserverinfo());

    ASSERT_EQ(csId1, response.chunkserverinfo().chunkserverid());
    ASSERT_EQ("nvme", response.chunkserverinfo().disktype());
    ASSERT_EQ("ip1", response.chunkserverinfo().hostip());
    ASSERT_EQ("ip2", response.chunkserverinfo().externalip());
    ASSERT_EQ(100, response.chunkserverinfo().port());
}

TEST_F(TestTopologyServiceManager, test_GetChunkServer_ByIpSuccess) {
    ChunkServerIdType csId1 = 0x41;
    ServerIdType serverId = 0x31;

    PrepareAddPhysicalPool();
    PrepareAddZone();
    PrepareAddServer(serverId, "server", "ip1", "ip2");
    PrepareAddChunkServer(csId1, "token1", "nvme", serverId, "ip1", "ip2", 100);

    GetChunkServerInfoRequest request;
    request.set_hostip("ip1");
    request.set_port(100);

    GetChunkServerInfoResponse response;
    serviceManager_->GetChunkServer(&request, &response);

    ASSERT_EQ(kTopoErrCodeSuccess, response.statuscode());
    ASSERT_TRUE(response.has_chunkserverinfo());

    ASSERT_EQ(csId1, response.chunkserverinfo().chunkserverid());
    ASSERT_EQ("nvme", response.chunkserverinfo().disktype());
    ASSERT_EQ("ip1", response.chunkserverinfo().hostip());
    ASSERT_EQ("ip2", response.chunkserverinfo().externalip());
    ASSERT_EQ(100, response.chunkserverinfo().port());
}

TEST_F(TestTopologyServiceManager, test_GetChunkServer_ChunkServerNotFound) {
    ChunkServerIdType csId1 = 0x41;
    ServerIdType serverId = 0x31;

    PrepareAddPhysicalPool();
    PrepareAddZone();
    PrepareAddServer(serverId, "server", "ip1", "ip2");
    PrepareAddChunkServer(csId1, "token1", "nvme", serverId, "ip1", "ip2", 100);

    GetChunkServerInfoRequest request;
    request.set_chunkserverid(++csId1);

    GetChunkServerInfoResponse response;
    serviceManager_->GetChunkServer(&request, &response);

    ASSERT_EQ(kTopoErrCodeChunkServerNotFound, response.statuscode());
    ASSERT_FALSE(response.has_chunkserverinfo());
}

TEST_F(TestTopologyServiceManager,
    test_GetChunkServer_ByIpChunkServerNotFound) {
    ChunkServerIdType csId1 = 0x41;
    ServerIdType serverId = 0x31;

    PrepareAddPhysicalPool();
    PrepareAddZone();
    PrepareAddServer(serverId, "server", "ip1", "ip2");
    PrepareAddChunkServer(csId1, "token1", "nvme", serverId, "ip1", "ip2", 100);

    GetChunkServerInfoRequest request;
    request.set_hostip("ip3");
    request.set_port(1024);

    GetChunkServerInfoResponse response;
    serviceManager_->GetChunkServer(&request, &response);

    ASSERT_EQ(kTopoErrCodeChunkServerNotFound, response.statuscode());
    ASSERT_FALSE(response.has_chunkserverinfo());
}

TEST_F(TestTopologyServiceManager, test_GetChunkServer_InvalidParam) {
    GetChunkServerInfoRequest request;

    GetChunkServerInfoResponse response;
    serviceManager_->GetChunkServer(&request, &response);

    ASSERT_EQ(kTopoErrCodeInvalidParam, response.statuscode());
    ASSERT_FALSE(response.has_chunkserverinfo());
}

TEST_F(TestTopologyServiceManager, test_DeleteChunkServer_success) {
    ChunkServerIdType csId1 = 0x41;
    ServerIdType serverId = 0x31;

    PrepareAddPhysicalPool();
    PrepareAddZone();
    PrepareAddServer(serverId, "server", "ip1", "ip2");
    PrepareAddChunkServer(csId1, "token1", "nvme", serverId, "ip1", "ip2", 100);

    DeleteChunkServerRequest request;
    request.set_chunkserverid(csId1);

    EXPECT_CALL(*storage_, DeleteChunkServer(_))
        .WillOnce(Return(true));

    ASSERT_EQ(kTopoErrCodeSuccess,
        topology_->UpdateChunkServerRwState(
            ChunkServerStatus::RETIRED, csId1));

    DeleteChunkServerResponse response;
    serviceManager_->DeleteChunkServer(&request, &response);

    ASSERT_EQ(kTopoErrCodeSuccess, response.statuscode());
}

TEST_F(TestTopologyServiceManager, test_DeleteChunkServer_ChunkServerNotFound) {
    ChunkServerIdType csId1 = 0x41;
    ServerIdType serverId = 0x31;

    PrepareAddPhysicalPool();
    PrepareAddZone();
    PrepareAddServer(serverId, "server", "ip1", "ip2");
    PrepareAddChunkServer(csId1, "token1", "nvme", serverId, "ip1", "ip2", 100);

    DeleteChunkServerRequest request;
    request.set_chunkserverid(++csId1);

    DeleteChunkServerResponse response;
    serviceManager_->DeleteChunkServer(&request, &response);

    ASSERT_EQ(kTopoErrCodeChunkServerNotFound, response.statuscode());
}

TEST_F(TestTopologyServiceManager, test_SetChunkServer_Success) {
    ChunkServerIdType csId1 = 0x41;
    ServerIdType serverId = 0x31;

    PrepareAddPhysicalPool();
    PrepareAddZone();
    PrepareAddServer(serverId, "server", "ip1", "ip2");
    PrepareAddChunkServer(csId1, "token1", "nvme", serverId, "ip1", "ip2", 100);

    SetChunkServerStatusRequest request;
    request.set_chunkserverid(csId1);
    request.set_chunkserverstatus(RETIRED);

    SetChunkServerStatusResponse response;

    serviceManager_->SetChunkServer(&request, &response);

    ASSERT_EQ(kTopoErrCodeSuccess, response.statuscode());
}

TEST_F(TestTopologyServiceManager, test_SetChunkServer_ChunkServerNotFound) {
    ChunkServerIdType csId1 = 0x41;
    ServerIdType serverId = 0x31;

    PrepareAddPhysicalPool();
    PrepareAddZone();
    PrepareAddServer(serverId, "server", "ip1", "ip2");
    PrepareAddChunkServer(csId1, "token1", "nvme", serverId, "ip1", "ip2", 100);

    SetChunkServerStatusRequest request;
    request.set_chunkserverid(++csId1);
    request.set_chunkserverstatus(RETIRED);

    SetChunkServerStatusResponse response;
    serviceManager_->SetChunkServer(&request, &response);

    ASSERT_EQ(kTopoErrCodeChunkServerNotFound, response.statuscode());
}

TEST_F(TestTopologyServiceManager, test_RegistServer_ByZoneAndPoolIdSuccess) {
    ServerIdType id = 0x31;
    PoolIdType physicalPoolId = 0x11;
    ZoneIdType zoneId = 0x21;
    PrepareAddPhysicalPool(physicalPoolId);
    PrepareAddZone(zoneId, "test", physicalPoolId);

    ServerRegistRequest request;
    request.set_hostname("server1");
    request.set_internalip("ip1");
    request.set_externalip("ip2");
    request.set_zoneid(zoneId);
    request.set_physicalpoolid(physicalPoolId);
    request.set_desc("desc1");

    ServerRegistResponse response;

    EXPECT_CALL(*idGenerator_, GenServerId())
        .WillOnce(Return(id));
    EXPECT_CALL(*storage_, StorageServer(_))
        .WillOnce(Return(true));

    serviceManager_->RegistServer(&request, &response);

    ASSERT_EQ(kTopoErrCodeSuccess, response.statuscode());
    ASSERT_TRUE(response.has_serverid());
    ASSERT_EQ(id, response.serverid());
}

TEST_F(TestTopologyServiceManager, test_RegistServer_ByZoneAndPoolNameSuccess) {
    ServerIdType id = 0x31;
    PoolIdType physicalPoolId = 0x11;
    ZoneIdType zoneId = 0x21;
    PrepareAddPhysicalPool(physicalPoolId, "PhysicalPool1");
    PrepareAddZone(zoneId, "zone1", physicalPoolId);

    ServerRegistRequest request;
    request.set_hostname("server1");
    request.set_internalip("ip1");
    request.set_externalip("ip2");
    request.set_zonename("zone1");
    request.set_physicalpoolname("PhysicalPool1");
    request.set_desc("desc1");

    ServerRegistResponse response;

    EXPECT_CALL(*idGenerator_, GenServerId())
        .WillOnce(Return(id));
    EXPECT_CALL(*storage_, StorageServer(_))
        .WillOnce(Return(true));

    serviceManager_->RegistServer(&request, &response);

    ASSERT_EQ(kTopoErrCodeSuccess, response.statuscode());
    ASSERT_TRUE(response.has_serverid());
    ASSERT_EQ(id, response.serverid());
}

TEST_F(TestTopologyServiceManager, test_RegistServer_PhysicalPoolNotFound) {
    PoolIdType physicalPoolId = 0x11;
    ZoneIdType zoneId = 0x21;
    PrepareAddPhysicalPool(physicalPoolId);
    PrepareAddZone(zoneId, "test", physicalPoolId);

    ServerRegistRequest request;
    request.set_hostname("server1");
    request.set_internalip("ip1");
    request.set_externalip("ip2");
    request.set_zoneid(zoneId);
    request.set_physicalpoolid(++physicalPoolId);
    request.set_desc("desc1");

    ServerRegistResponse response;

    serviceManager_->RegistServer(&request, &response);

    ASSERT_EQ(kTopoErrCodePhysicalPoolNotFound, response.statuscode());
    ASSERT_FALSE(response.has_serverid());
}

TEST_F(TestTopologyServiceManager,
    test_RegistServer_ByNamePhysicalPoolNotFound) {
    ServerIdType id = 0x31;
    PoolIdType physicalPoolId = 0x11;
    ZoneIdType zoneId = 0x21;
    PrepareAddPhysicalPool(physicalPoolId, "PhysicalPool1");
    PrepareAddZone(zoneId, "zone1", physicalPoolId);

    ServerRegistRequest request;
    request.set_hostname("server1");
    request.set_internalip("ip1");
    request.set_externalip("ip2");
    request.set_zonename("zone1");
    request.set_physicalpoolname("PhysicalPool2");
    request.set_desc("desc1");

    ServerRegistResponse response;

    serviceManager_->RegistServer(&request, &response);

    ASSERT_EQ(kTopoErrCodePhysicalPoolNotFound, response.statuscode());
    ASSERT_FALSE(response.has_serverid());
}

TEST_F(TestTopologyServiceManager, test_RegistServer_ZoneNotFound) {
    PoolIdType physicalPoolId = 0x11;
    ZoneIdType zoneId = 0x21;
    PrepareAddPhysicalPool(physicalPoolId);
    PrepareAddZone(zoneId, "test", physicalPoolId);

    ServerRegistRequest request;
    request.set_hostname("server1");
    request.set_internalip("ip1");
    request.set_externalip("ip2");
    request.set_zoneid(++zoneId);
    request.set_physicalpoolid(physicalPoolId);
    request.set_desc("desc1");

    ServerRegistResponse response;

    serviceManager_->RegistServer(&request, &response);

    ASSERT_EQ(kTopoErrCodeZoneNotFound, response.statuscode());
    ASSERT_FALSE(response.has_serverid());
}

TEST_F(TestTopologyServiceManager, test_RegistServer_ByNameZoneNotFound) {
    ServerIdType id = 0x31;
    PoolIdType physicalPoolId = 0x11;
    ZoneIdType zoneId = 0x21;
    PrepareAddPhysicalPool(physicalPoolId, "PhysicalPool1");
    PrepareAddZone(zoneId, "zone1", physicalPoolId);

    ServerRegistRequest request;
    request.set_hostname("server1");
    request.set_internalip("ip1");
    request.set_externalip("ip2");
    request.set_zonename("zone2");
    request.set_physicalpoolname("PhysicalPool1");
    request.set_desc("desc1");

    ServerRegistResponse response;

    serviceManager_->RegistServer(&request, &response);

    ASSERT_EQ(kTopoErrCodeZoneNotFound, response.statuscode());
    ASSERT_FALSE(response.has_serverid());
}

TEST_F(TestTopologyServiceManager, test_RegistServer_InvalidParam) {
    PoolIdType physicalPoolId = 0x11;
    ZoneIdType zoneId = 0x21;
    PrepareAddPhysicalPool(physicalPoolId, "PhysicalPool1");
    PrepareAddZone(zoneId, "zone1", physicalPoolId);

    ServerRegistRequest request;
    ServerRegistResponse response;

    serviceManager_->RegistServer(&request, &response);

    ASSERT_EQ(kTopoErrCodeInvalidParam, response.statuscode());
    ASSERT_FALSE(response.has_serverid());
}

TEST_F(TestTopologyServiceManager,
    test_RegistServer_InvalidParamMissingZoneIdAndName) {
    PoolIdType physicalPoolId = 0x11;
    ZoneIdType zoneId = 0x21;
    PrepareAddPhysicalPool(physicalPoolId);
    PrepareAddZone(zoneId, "test", physicalPoolId);

    ServerRegistRequest request;
    request.set_hostname("server1");
    request.set_internalip("ip1");
    request.set_externalip("ip2");
    request.set_physicalpoolid(physicalPoolId);
    request.set_desc("desc1");

    ServerRegistResponse response;

    serviceManager_->RegistServer(&request, &response);

    ASSERT_EQ(kTopoErrCodeInvalidParam, response.statuscode());
    ASSERT_FALSE(response.has_serverid());
}

TEST_F(TestTopologyServiceManager,
    test_RegistServer_InvalidParamMissingPhysicalPoolIdAndName) {
    ServerIdType id = 0x31;
    PoolIdType physicalPoolId = 0x11;
    ZoneIdType zoneId = 0x21;
    PrepareAddPhysicalPool(physicalPoolId, "PhysicalPool1");
    PrepareAddZone(zoneId, "zone1", physicalPoolId);

    ServerRegistRequest request;
    request.set_hostname("server1");
    request.set_internalip("ip1");
    request.set_externalip("ip2");
    request.set_zonename("zone1");
    request.set_desc("desc1");

    ServerRegistResponse response;

    serviceManager_->RegistServer(&request, &response);

    ASSERT_EQ(kTopoErrCodeInvalidParam, response.statuscode());
    ASSERT_FALSE(response.has_serverid());
}

TEST_F(TestTopologyServiceManager, test_RegistServer_AllocateIdFail) {
    ServerIdType id = 0x31;
    PoolIdType physicalPoolId = 0x11;
    ZoneIdType zoneId = 0x21;
    PrepareAddPhysicalPool(physicalPoolId);
    PrepareAddZone(zoneId, "test", physicalPoolId);

    ServerRegistRequest request;
    request.set_hostname("server1");
    request.set_internalip("ip1");
    request.set_externalip("ip2");
    request.set_zoneid(zoneId);
    request.set_physicalpoolid(physicalPoolId);
    request.set_desc("desc1");

    ServerRegistResponse response;

    EXPECT_CALL(*idGenerator_, GenServerId())
        .WillOnce(Return(UNINTIALIZE_ID));

    serviceManager_->RegistServer(&request, &response);

    ASSERT_EQ(kTopoErrCodeAllocateIdFail, response.statuscode());
}

TEST_F(TestTopologyServiceManager, test_RegistServer_AddServerFail) {
    ServerIdType id = 0x31;
    PoolIdType physicalPoolId = 0x11;
    ZoneIdType zoneId = 0x21;
    PrepareAddPhysicalPool(physicalPoolId);
    PrepareAddZone(zoneId, "test", physicalPoolId);

    ServerRegistRequest request;
    request.set_hostname("server1");
    request.set_internalip("ip1");
    request.set_externalip("ip2");
    request.set_zoneid(zoneId);
    request.set_physicalpoolid(physicalPoolId);
    request.set_desc("desc1");

    ServerRegistResponse response;

    EXPECT_CALL(*idGenerator_, GenServerId())
        .WillOnce(Return(id));
    EXPECT_CALL(*storage_, StorageServer(_))
        .WillOnce(Return(false));

    serviceManager_->RegistServer(&request, &response);

    ASSERT_EQ(kTopoErrCodeStorgeFail, response.statuscode());
}

TEST_F(TestTopologyServiceManager, test_GetServer_ByIdSuccess) {
    PoolIdType physicalPoolId = 0x11;
    ZoneIdType zoneId = 0x21;
    ServerIdType serverId = 0x31;
    PrepareAddPhysicalPool(physicalPoolId);
    PrepareAddZone(zoneId);
    PrepareAddServer(serverId,
        "hostname1",
        "ip1",
        "ip2",
        zoneId,
        physicalPoolId,
        "desc1");

    GetServerRequest request;
    request.set_serverid(serverId);

    GetServerResponse response;

    serviceManager_->GetServer(&request, &response);

    ASSERT_EQ(kTopoErrCodeSuccess, response.statuscode());
    ASSERT_TRUE(response.has_serverinfo());
    ASSERT_EQ(serverId, response.serverinfo().serverid());
    ASSERT_EQ("hostname1", response.serverinfo().hostname());
    ASSERT_EQ("ip1", response.serverinfo().internalip());
    ASSERT_EQ("ip2", response.serverinfo().externalip());
    ASSERT_EQ(zoneId, response.serverinfo().zoneid());
    ASSERT_EQ(physicalPoolId, response.serverinfo().physicalpoolid());
    ASSERT_EQ("desc1", response.serverinfo().desc());
}

TEST_F(TestTopologyServiceManager, test_GetServer_ByNameSuccess) {
    PoolIdType physicalPoolId = 0x11;
    ZoneIdType zoneId = 0x21;
    ServerIdType serverId = 0x31;
    PrepareAddPhysicalPool(physicalPoolId);
    PrepareAddZone(zoneId);
    PrepareAddServer(serverId,
        "hostname1",
        "ip1",
        "ip2",
        zoneId,
        physicalPoolId,
        "desc1");

    GetServerRequest request;
    request.set_hostname("hostname1");

    GetServerResponse response;

    serviceManager_->GetServer(&request, &response);

    ASSERT_EQ(kTopoErrCodeSuccess, response.statuscode());
    ASSERT_TRUE(response.has_serverinfo());
    ASSERT_EQ(serverId, response.serverinfo().serverid());
    ASSERT_EQ("hostname1", response.serverinfo().hostname());
    ASSERT_EQ("ip1", response.serverinfo().internalip());
    ASSERT_EQ("ip2", response.serverinfo().externalip());
    ASSERT_EQ(zoneId, response.serverinfo().zoneid());
    ASSERT_EQ(physicalPoolId, response.serverinfo().physicalpoolid());
    ASSERT_EQ("desc1", response.serverinfo().desc());
}

TEST_F(TestTopologyServiceManager, test_GetServer_ByInternalIpSuccess) {
    PoolIdType physicalPoolId = 0x11;
    ZoneIdType zoneId = 0x21;
    ServerIdType serverId = 0x31;
    PrepareAddPhysicalPool(physicalPoolId);
    PrepareAddZone(zoneId);
    PrepareAddServer(serverId,
        "hostname1",
        "ip1",
        "ip2",
        zoneId,
        physicalPoolId,
        "desc1");

    GetServerRequest request;
    request.set_hostip("ip1");

    GetServerResponse response;

    serviceManager_->GetServer(&request, &response);

    ASSERT_EQ(kTopoErrCodeSuccess, response.statuscode());
    ASSERT_TRUE(response.has_serverinfo());
    ASSERT_EQ(serverId, response.serverinfo().serverid());
    ASSERT_EQ("hostname1", response.serverinfo().hostname());
    ASSERT_EQ("ip1", response.serverinfo().internalip());
    ASSERT_EQ("ip2", response.serverinfo().externalip());
    ASSERT_EQ(zoneId, response.serverinfo().zoneid());
    ASSERT_EQ(physicalPoolId, response.serverinfo().physicalpoolid());
    ASSERT_EQ("desc1", response.serverinfo().desc());
}

TEST_F(TestTopologyServiceManager, test_GetServer_ByExternalIpSuccess) {
    PoolIdType physicalPoolId = 0x11;
    ZoneIdType zoneId = 0x21;
    ServerIdType serverId = 0x31;
    PrepareAddPhysicalPool(physicalPoolId);
    PrepareAddZone(zoneId);
    PrepareAddServer(serverId,
        "hostname1",
        "ip1",
        "ip2",
        zoneId,
        physicalPoolId,
        "desc1");

    GetServerRequest request;
    request.set_hostip("ip2");

    GetServerResponse response;

    serviceManager_->GetServer(&request, &response);

    ASSERT_EQ(kTopoErrCodeSuccess, response.statuscode());
    ASSERT_TRUE(response.has_serverinfo());
    ASSERT_EQ(serverId, response.serverinfo().serverid());
    ASSERT_EQ("hostname1", response.serverinfo().hostname());
    ASSERT_EQ("ip1", response.serverinfo().internalip());
    ASSERT_EQ("ip2", response.serverinfo().externalip());
    ASSERT_EQ(zoneId, response.serverinfo().zoneid());
    ASSERT_EQ(physicalPoolId, response.serverinfo().physicalpoolid());
    ASSERT_EQ("desc1", response.serverinfo().desc());
}

TEST_F(TestTopologyServiceManager, test_GetServer_ServerNotFound) {
    PoolIdType physicalPoolId = 0x11;
    ZoneIdType zoneId = 0x21;
    ServerIdType serverId = 0x31;
    PrepareAddPhysicalPool(physicalPoolId);
    PrepareAddZone(zoneId);
    PrepareAddServer(serverId,
        "hostname1",
        "ip1",
        "ip2",
        zoneId,
        physicalPoolId,
        "desc1");

    GetServerRequest request;
    request.set_serverid(++serverId);

    GetServerResponse response;

    serviceManager_->GetServer(&request, &response);

    ASSERT_EQ(kTopoErrCodeServerNotFound, response.statuscode());
    ASSERT_FALSE(response.has_serverinfo());
}

TEST_F(TestTopologyServiceManager, test_GetServer_ByNameServerNotFound) {
    PoolIdType physicalPoolId = 0x11;
    ZoneIdType zoneId = 0x21;
    ServerIdType serverId = 0x31;
    PrepareAddPhysicalPool(physicalPoolId);
    PrepareAddZone(zoneId);
    PrepareAddServer(serverId,
        "hostname1",
        "ip1",
        "ip2",
        zoneId,
        physicalPoolId,
        "desc1");

    GetServerRequest request;
    request.set_hostname("hostname2");

    GetServerResponse response;

    serviceManager_->GetServer(&request, &response);

    ASSERT_EQ(kTopoErrCodeServerNotFound, response.statuscode());
    ASSERT_FALSE(response.has_serverinfo());
}

TEST_F(TestTopologyServiceManager, test_GetServer_ByIpServerNotFound) {
    PoolIdType physicalPoolId = 0x11;
    ZoneIdType zoneId = 0x21;
    ServerIdType serverId = 0x31;
    PrepareAddPhysicalPool(physicalPoolId);
    PrepareAddZone(zoneId);
    PrepareAddServer(serverId,
        "hostname1",
        "ip1",
        "ip2",
        zoneId,
        physicalPoolId,
        "desc1");

    GetServerRequest request;
    request.set_hostip("ip3");

    GetServerResponse response;

    serviceManager_->GetServer(&request, &response);

    ASSERT_EQ(kTopoErrCodeServerNotFound, response.statuscode());
    ASSERT_FALSE(response.has_serverinfo());
}

TEST_F(TestTopologyServiceManager, test_DeleteServer_success) {
    PoolIdType physicalPoolId = 0x11;
    ZoneIdType zoneId = 0x21;
    ServerIdType serverId = 0x31;
    PrepareAddPhysicalPool(physicalPoolId);
    PrepareAddZone(zoneId);
    PrepareAddServer(serverId,
        "hostname1",
        "ip1",
        "ip2",
        zoneId,
        physicalPoolId,
        "desc1");

    DeleteServerRequest request;
    request.set_serverid(serverId);

    DeleteServerResponse response;

    EXPECT_CALL(*storage_, DeleteServer(_))
        .WillOnce(Return(true));

    serviceManager_->DeleteServer(&request, &response);

    ASSERT_EQ(kTopoErrCodeSuccess, response.statuscode());
}

TEST_F(TestTopologyServiceManager, test_ListZoneServer_ByIdSuccess) {
    PoolIdType physicalPoolId = 0x11;
    ZoneIdType zoneId = 0x21;
    ServerIdType serverId = 0x31;
    ServerIdType serverId2 = 0x32;
    PrepareAddPhysicalPool(physicalPoolId);
    PrepareAddZone(zoneId);
    PrepareAddServer(serverId,
        "hostname1",
        "ip1",
        "ip2",
        zoneId,
        physicalPoolId,
        "desc1");
    PrepareAddServer(serverId2,
        "hostname2",
        "ip3",
        "ip4",
        zoneId,
        physicalPoolId,
        "desc2");

    ListZoneServerRequest request;
    request.set_zoneid(zoneId);

    ListZoneServerResponse response;
    serviceManager_->ListZoneServer(&request, &response);

    ASSERT_EQ(kTopoErrCodeSuccess, response.statuscode());
    ASSERT_EQ(2, response.serverinfo_size());

    ASSERT_THAT(response.serverinfo(0).serverid(), AnyOf(serverId, serverId2));
    ASSERT_THAT(response.serverinfo(0).hostname(),
            AnyOf("hostname1", "hostname2"));
    ASSERT_THAT(response.serverinfo(0).internalip(), AnyOf("ip1", "ip3"));
    ASSERT_THAT(response.serverinfo(0).externalip(), AnyOf("ip2", "ip4"));
    ASSERT_EQ(zoneId, response.serverinfo(0).zoneid());
    ASSERT_EQ(physicalPoolId, response.serverinfo(0).physicalpoolid());
    ASSERT_THAT(response.serverinfo(0).desc(), AnyOf("desc1", "desc2"));

    ASSERT_THAT(response.serverinfo(1).serverid(), AnyOf(serverId, serverId2));
    ASSERT_THAT(response.serverinfo(1).hostname(),
            AnyOf("hostname1", "hostname2"));
    ASSERT_THAT(response.serverinfo(1).internalip(), AnyOf("ip1", "ip3"));
    ASSERT_THAT(response.serverinfo(1).externalip(), AnyOf("ip2", "ip4"));
    ASSERT_EQ(zoneId, response.serverinfo(1).zoneid());
    ASSERT_EQ(physicalPoolId, response.serverinfo(1).physicalpoolid());
    ASSERT_THAT(response.serverinfo(1).desc(), AnyOf("desc1", "desc2"));
}

TEST_F(TestTopologyServiceManager, test_ListZoneServer_ByNameSuccess) {
    PoolIdType physicalPoolId = 0x11;
    ZoneIdType zoneId = 0x21;
    ServerIdType serverId = 0x31;
    ServerIdType serverId2 = 0x32;
    PrepareAddPhysicalPool(physicalPoolId, "poolName1");
    PrepareAddZone(zoneId, "zone1", physicalPoolId);
    PrepareAddServer(serverId,
        "hostname1",
        "ip1",
        "ip2",
        zoneId,
        physicalPoolId,
        "desc1");
    PrepareAddServer(serverId2,
        "hostname2",
        "ip3",
        "ip4",
        zoneId,
        physicalPoolId,
        "desc2");

    ListZoneServerRequest request;
    request.set_zonename("zone1");
    request.set_physicalpoolname("poolName1");

    ListZoneServerResponse response;
    serviceManager_->ListZoneServer(&request, &response);

    ASSERT_EQ(kTopoErrCodeSuccess, response.statuscode());
    ASSERT_EQ(2, response.serverinfo_size());

    ASSERT_THAT(response.serverinfo(0).serverid(), AnyOf(serverId, serverId2));
    ASSERT_THAT(response.serverinfo(0).hostname(),
            AnyOf("hostname1", "hostname2"));
    ASSERT_THAT(response.serverinfo(0).internalip(), AnyOf("ip1", "ip3"));
    ASSERT_THAT(response.serverinfo(0).externalip(), AnyOf("ip2", "ip4"));
    ASSERT_EQ(zoneId, response.serverinfo(0).zoneid());
    ASSERT_EQ(physicalPoolId, response.serverinfo(0).physicalpoolid());
    ASSERT_THAT(response.serverinfo(0).desc(), AnyOf("desc1", "desc2"));

    ASSERT_THAT(response.serverinfo(1).serverid(), AnyOf(serverId, serverId2));
    ASSERT_THAT(response.serverinfo(1).hostname(),
            AnyOf("hostname1", "hostname2"));
    ASSERT_THAT(response.serverinfo(1).internalip(), AnyOf("ip1", "ip3"));
    ASSERT_THAT(response.serverinfo(1).externalip(), AnyOf("ip2", "ip4"));
    ASSERT_EQ(zoneId, response.serverinfo(1).zoneid());
    ASSERT_EQ(physicalPoolId, response.serverinfo(1).physicalpoolid());
    ASSERT_THAT(response.serverinfo(1).desc(), AnyOf("desc1", "desc2"));
}

TEST_F(TestTopologyServiceManager, test_ListZoneServer_ZoneNotFound) {
    PoolIdType physicalPoolId = 0x11;
    ZoneIdType zoneId = 0x21;
    ServerIdType serverId = 0x31;
    ServerIdType serverId2 = 0x32;
    PrepareAddPhysicalPool(physicalPoolId);
    PrepareAddZone(zoneId);
    PrepareAddServer(serverId,
        "hostname1",
        "ip1",
        "ip2",
        zoneId,
        physicalPoolId,
        "desc1");
    PrepareAddServer(serverId2,
        "hostname2",
        "ip3",
        "ip4",
        zoneId,
        physicalPoolId,
        "desc2");

    ListZoneServerRequest request;
    request.set_zoneid(++zoneId);

    ListZoneServerResponse response;
    serviceManager_->ListZoneServer(&request, &response);

    ASSERT_EQ(kTopoErrCodeZoneNotFound, response.statuscode());
    ASSERT_EQ(0, response.serverinfo_size());
}

TEST_F(TestTopologyServiceManager, test_ListZoneServer_ByNameZoneNotFound) {
    PoolIdType physicalPoolId = 0x11;
    ZoneIdType zoneId = 0x21;
    ServerIdType serverId = 0x31;
    ServerIdType serverId2 = 0x32;
    PrepareAddPhysicalPool(physicalPoolId);
    PrepareAddZone(zoneId);
    PrepareAddServer(serverId,
        "hostname1",
        "ip1",
        "ip2",
        zoneId,
        physicalPoolId,
        "desc1");
    PrepareAddServer(serverId2,
        "hostname2",
        "ip3",
        "ip4",
        zoneId,
        physicalPoolId,
        "desc2");

    ListZoneServerRequest request;
    request.set_zonename("zone2");
    request.set_physicalpoolname("poolName1");

    ListZoneServerResponse response;
    serviceManager_->ListZoneServer(&request, &response);

    ASSERT_EQ(kTopoErrCodeZoneNotFound, response.statuscode());
    ASSERT_EQ(0, response.serverinfo_size());
}

TEST_F(TestTopologyServiceManager, test_ListZoneServer_InvalidParam) {
    PoolIdType physicalPoolId = 0x11;
    ZoneIdType zoneId = 0x21;
    ServerIdType serverId = 0x31;
    ServerIdType serverId2 = 0x32;
    PrepareAddPhysicalPool(physicalPoolId);
    PrepareAddZone(zoneId);
    PrepareAddServer(serverId,
        "hostname1",
        "ip1",
        "ip2",
        zoneId,
        physicalPoolId,
        "desc1");
    PrepareAddServer(serverId2,
        "hostname2",
        "ip3",
        "ip4",
        zoneId,
        physicalPoolId,
        "desc2");

    ListZoneServerRequest request;

    ListZoneServerResponse response;
    serviceManager_->ListZoneServer(&request, &response);

    ASSERT_EQ(kTopoErrCodeInvalidParam, response.statuscode());
    ASSERT_EQ(0, response.serverinfo_size());
}

TEST_F(TestTopologyServiceManager, test_CreateZone_success) {
    PoolIdType physicalPoolId = 0x11;
    ZoneIdType zoneId = 0x21;
    PrepareAddPhysicalPool(physicalPoolId, "poolname1");

    ZoneRequest request;
    request.set_zonename("zone1");
    request.set_physicalpoolname("poolname1");
    request.set_desc("desc1");

    EXPECT_CALL(*idGenerator_, GenZoneId())
        .WillOnce(Return(zoneId));

    EXPECT_CALL(*storage_, StorageZone(_))
        .WillOnce(Return(true));

    ZoneResponse response;

    serviceManager_->CreateZone(&request, &response);
    ASSERT_EQ(kTopoErrCodeSuccess, response.statuscode());
    ASSERT_TRUE(response.has_zoneinfo());

    ASSERT_EQ(zoneId, response.zoneinfo().zoneid());
    ASSERT_EQ("zone1", response.zoneinfo().zonename());
    ASSERT_EQ(physicalPoolId, response.zoneinfo().physicalpoolid());
    ASSERT_EQ("desc1", response.zoneinfo().desc());
}

TEST_F(TestTopologyServiceManager, test_CreateZone_AllocateIdFail) {
    PoolIdType physicalPoolId = 0x11;
    ZoneIdType zoneId = 0x21;
    PrepareAddPhysicalPool(physicalPoolId, "poolname1");

    ZoneRequest request;
    request.set_zonename("zone1");
    request.set_physicalpoolname("poolname1");
    request.set_desc("desc1");

    EXPECT_CALL(*idGenerator_, GenZoneId())
        .WillOnce(Return(UNINTIALIZE_ID));

    ZoneResponse response;

    serviceManager_->CreateZone(&request, &response);
    ASSERT_EQ(kTopoErrCodeAllocateIdFail, response.statuscode());
}

TEST_F(TestTopologyServiceManager, test_CreateZone_PhysicalPoolNotFound) {
    PoolIdType physicalPoolId = 0x11;
    PrepareAddPhysicalPool(physicalPoolId, "poolname1");

    ZoneRequest request;
    request.set_zonename("zone1");
    request.set_physicalpoolname("poolname2");
    request.set_desc("desc1");

    ZoneResponse response;

    serviceManager_->CreateZone(&request, &response);
    ASSERT_EQ(kTopoErrCodePhysicalPoolNotFound, response.statuscode());
    ASSERT_FALSE(response.has_zoneinfo());
}

TEST_F(TestTopologyServiceManager, test_CreateZone_InvalidParam) {
    PoolIdType physicalPoolId = 0x11;
    PrepareAddPhysicalPool(physicalPoolId, "poolname1");

    ZoneRequest request;
    ZoneResponse response;

    serviceManager_->CreateZone(&request, &response);
    ASSERT_EQ(kTopoErrCodeInvalidParam, response.statuscode());
    ASSERT_FALSE(response.has_zoneinfo());
}

TEST_F(TestTopologyServiceManager, test_CreateZone_AddZoneFail) {
    PoolIdType physicalPoolId = 0x11;
    ZoneIdType zoneId = 0x21;
    PrepareAddPhysicalPool(physicalPoolId, "poolname1");

    ZoneRequest request;
    request.set_zonename("zone1");
    request.set_physicalpoolname("poolname1");
    request.set_desc("desc1");

    EXPECT_CALL(*idGenerator_, GenZoneId())
        .WillOnce(Return(zoneId));

    EXPECT_CALL(*storage_, StorageZone(_))
        .WillOnce(Return(false));

    ZoneResponse response;

    serviceManager_->CreateZone(&request, &response);
    ASSERT_EQ(kTopoErrCodeStorgeFail, response.statuscode());
}

TEST_F(TestTopologyServiceManager, test_DeleteZone_Success) {
    ZoneIdType zoneId = 0x21;
    PoolIdType poolId = 0x11;
    PrepareAddPhysicalPool(poolId);
    PrepareAddZone(zoneId,
            "testZone",
            poolId);

    ZoneRequest request;
    request.set_zoneid(zoneId);

    EXPECT_CALL(*storage_, DeleteZone(_))
        .WillOnce(Return(true));

    ZoneResponse response;
    serviceManager_->DeleteZone(&request, &response);

    ASSERT_EQ(kTopoErrCodeSuccess, response.statuscode());
}

TEST_F(TestTopologyServiceManager, test_DeleteZone_ByNameSuccess) {
    ZoneIdType zoneId = 0x21;
    PoolIdType poolId = 0x11;
    PrepareAddPhysicalPool(poolId, "pool1");
    PrepareAddZone(zoneId,
            "testZone",
            poolId);

    ZoneRequest request;
    request.set_zonename("testZone");
    request.set_physicalpoolname("pool1");

    EXPECT_CALL(*storage_, DeleteZone(_))
        .WillOnce(Return(true));

    ZoneResponse response;
    serviceManager_->DeleteZone(&request, &response);

    ASSERT_EQ(kTopoErrCodeSuccess, response.statuscode());
}

TEST_F(TestTopologyServiceManager, test_DeleteZone_ZoneNotFound) {
    ZoneIdType zoneId = 0x21;
    PoolIdType poolId = 0x11;
    PrepareAddPhysicalPool(poolId);
    PrepareAddZone(zoneId,
            "testZone",
            poolId);

    ZoneRequest request;
    request.set_zoneid(++zoneId);

    ZoneResponse response;
    serviceManager_->DeleteZone(&request, &response);

    ASSERT_EQ(kTopoErrCodeZoneNotFound, response.statuscode());
}

TEST_F(TestTopologyServiceManager, test_DeleteZone_ByNameFail) {
    ZoneIdType zoneId = 0x21;
    PoolIdType poolId = 0x11;
    PrepareAddPhysicalPool(poolId, "pool1");
    PrepareAddZone(zoneId,
            "testZone",
            poolId);

    ZoneRequest request;
    request.set_zonename("testZone2");
    request.set_physicalpoolname("pool1");

    ZoneResponse response;
    serviceManager_->DeleteZone(&request, &response);

    ASSERT_EQ(kTopoErrCodeZoneNotFound, response.statuscode());
}

TEST_F(TestTopologyServiceManager, test_DeleteZone_InvalidParam) {
    ZoneIdType zoneId = 0x21;
    PoolIdType poolId = 0x11;
    PrepareAddPhysicalPool(poolId);
    PrepareAddZone(zoneId,
            "testZone",
            poolId);

    ZoneRequest request;

    ZoneResponse response;
    serviceManager_->DeleteZone(&request, &response);

    ASSERT_EQ(kTopoErrCodeInvalidParam, response.statuscode());
}

TEST_F(TestTopologyServiceManager, test_GetZone_Success) {
    ZoneIdType zoneId = 0x21;
    PoolIdType poolId = 0x11;
    PrepareAddPhysicalPool(poolId);
    PrepareAddZone(zoneId,
            "testZone",
            poolId,
            "desc1");

    ZoneRequest request;
    request.set_zoneid(zoneId);

    ZoneResponse response;
    serviceManager_->GetZone(&request, &response);

    ASSERT_EQ(kTopoErrCodeSuccess, response.statuscode());
    ASSERT_TRUE(response.has_zoneinfo());
    ASSERT_EQ(zoneId, response.zoneinfo().zoneid());
    ASSERT_EQ("testZone", response.zoneinfo().zonename());
    ASSERT_EQ(poolId, response.zoneinfo().physicalpoolid());
    ASSERT_EQ("desc1", response.zoneinfo().desc());
}

TEST_F(TestTopologyServiceManager, test_GetZone_ByNameSuccess) {
    ZoneIdType zoneId = 0x21;
    PoolIdType poolId = 0x11;
    PrepareAddPhysicalPool(poolId, "pool1");
    PrepareAddZone(zoneId,
            "testZone",
            poolId,
            "desc1");

    ZoneRequest request;
    request.set_zonename("testZone");
    request.set_physicalpoolname("pool1");

    ZoneResponse response;
    serviceManager_->GetZone(&request, &response);

    ASSERT_EQ(kTopoErrCodeSuccess, response.statuscode());
    ASSERT_TRUE(response.has_zoneinfo());
    ASSERT_EQ(zoneId, response.zoneinfo().zoneid());
    ASSERT_EQ("testZone", response.zoneinfo().zonename());
    ASSERT_EQ(poolId, response.zoneinfo().physicalpoolid());
    ASSERT_EQ("desc1", response.zoneinfo().desc());
}

TEST_F(TestTopologyServiceManager, test_GetZone_ZoneNotFound) {
    ZoneIdType zoneId = 0x21;
    PoolIdType poolId = 0x11;
    PrepareAddPhysicalPool(poolId);
    PrepareAddZone(zoneId,
            "testZone",
            poolId,
            "desc1");

    ZoneRequest request;
    request.set_zoneid(++zoneId);

    ZoneResponse response;
    serviceManager_->GetZone(&request, &response);

    ASSERT_EQ(kTopoErrCodeZoneNotFound, response.statuscode());
    ASSERT_FALSE(response.has_zoneinfo());
}

TEST_F(TestTopologyServiceManager, test_GetZone_ByNameZoneNotFound) {
    ZoneIdType zoneId = 0x21;
    PoolIdType poolId = 0x11;
    PrepareAddPhysicalPool(poolId, "pool1");
    PrepareAddZone(zoneId,
            "testZone",
            poolId);

    ZoneRequest request;
    request.set_zonename("testZone2");
    request.set_physicalpoolname("pool1");

    ZoneResponse response;
    serviceManager_->GetZone(&request, &response);

    ASSERT_EQ(kTopoErrCodeZoneNotFound, response.statuscode());
    ASSERT_FALSE(response.has_zoneinfo());
}

TEST_F(TestTopologyServiceManager, test_GetZone_InvalidParam) {
    ZoneIdType zoneId = 0x21;
    PoolIdType poolId = 0x11;
    PrepareAddPhysicalPool(poolId);
    PrepareAddZone(zoneId,
            "testZone",
            poolId,
            "desc1");

    ZoneRequest request;

    ZoneResponse response;
    serviceManager_->GetZone(&request, &response);

    ASSERT_EQ(kTopoErrCodeInvalidParam, response.statuscode());
    ASSERT_FALSE(response.has_zoneinfo());
}

TEST_F(TestTopologyServiceManager, test_ListPoolZone_ByIdSuccess) {
    ZoneIdType zoneId = 0x21;
    ZoneIdType zoneId2 = 0x22;
    PoolIdType poolId = 0x11;
    PrepareAddPhysicalPool(poolId);
    PrepareAddZone(zoneId,
            "testZone",
            poolId,
            "desc1");

    PrepareAddZone(zoneId2,
            "testZone2",
            poolId,
            "desc2");

    ListPoolZoneRequest request;
    request.set_physicalpoolid(poolId);

    ListPoolZoneResponse response;
    serviceManager_->ListPoolZone(&request, &response);

    ASSERT_EQ(kTopoErrCodeSuccess, response.statuscode());
    ASSERT_EQ(2, response.zones_size());

    ASSERT_THAT(response.zones(0).zoneid(), AnyOf(zoneId, zoneId2));
    ASSERT_THAT(response.zones(0).zonename(), AnyOf("testZone", "testZone2"));
    ASSERT_EQ(poolId, response.zones(0).physicalpoolid());
    ASSERT_THAT(response.zones(0).desc(), AnyOf("desc1", "desc2"));

    ASSERT_THAT(response.zones(1).zoneid(), AnyOf(zoneId, zoneId2));
    ASSERT_THAT(response.zones(1).zonename(), AnyOf("testZone", "testZone2"));
    ASSERT_EQ(poolId, response.zones(1).physicalpoolid());
    ASSERT_THAT(response.zones(1).desc(), AnyOf("desc1", "desc2"));
}

TEST_F(TestTopologyServiceManager, test_ListPoolZone_ByNameSuccess) {
    ZoneIdType zoneId = 0x21;
    ZoneIdType zoneId2 = 0x22;
    PoolIdType poolId = 0x11;
    PrepareAddPhysicalPool(poolId, "poolname1");
    PrepareAddZone(zoneId,
            "testZone",
            poolId,
            "desc1");

    PrepareAddZone(zoneId2,
            "testZone2",
            poolId,
            "desc2");

    ListPoolZoneRequest request;
    request.set_physicalpoolname("poolname1");

    ListPoolZoneResponse response;
    serviceManager_->ListPoolZone(&request, &response);

    ASSERT_EQ(kTopoErrCodeSuccess, response.statuscode());
    ASSERT_EQ(2, response.zones_size());

    ASSERT_THAT(response.zones(0).zoneid(), AnyOf(zoneId, zoneId2));
    ASSERT_THAT(response.zones(0).zonename(), AnyOf("testZone", "testZone2"));
    ASSERT_EQ(poolId, response.zones(0).physicalpoolid());
    ASSERT_THAT(response.zones(0).desc(), AnyOf("desc1", "desc2"));

    ASSERT_THAT(response.zones(1).zoneid(), AnyOf(zoneId, zoneId2));
    ASSERT_THAT(response.zones(1).zonename(), AnyOf("testZone", "testZone2"));
    ASSERT_EQ(poolId, response.zones(1).physicalpoolid());
    ASSERT_THAT(response.zones(1).desc(), AnyOf("desc1", "desc2"));
}

TEST_F(TestTopologyServiceManager, test_ListPoolZone_ByNameFail) {
    ZoneIdType zoneId = 0x21;
    ZoneIdType zoneId2 = 0x22;
    PoolIdType poolId = 0x11;
    PrepareAddPhysicalPool(poolId, "poolname1");
    PrepareAddZone(zoneId,
            "testZone",
            poolId,
            "desc1");

    PrepareAddZone(zoneId2,
            "testZone2",
            poolId,
            "desc2");

    ListPoolZoneRequest request;
    request.set_physicalpoolname("poolname2");

    ListPoolZoneResponse response;
    serviceManager_->ListPoolZone(&request, &response);

    ASSERT_EQ(kTopoErrCodePhysicalPoolNotFound, response.statuscode());
}

TEST_F(TestTopologyServiceManager, test_ListPoolZone_PhysicalPoolNotFound) {
    ZoneIdType zoneId = 0x21;
    ZoneIdType zoneId2 = 0x22;
    PoolIdType poolId = 0x11;
    PrepareAddPhysicalPool(poolId);
    PrepareAddZone(zoneId,
            "testZone",
            poolId,
            "desc1");

    PrepareAddZone(zoneId2,
            "testZone2",
            poolId,
            "desc2");

    ListPoolZoneRequest request;
    request.set_physicalpoolid(++poolId);

    ListPoolZoneResponse response;
    serviceManager_->ListPoolZone(&request, &response);

    ASSERT_EQ(kTopoErrCodePhysicalPoolNotFound, response.statuscode());
    ASSERT_EQ(0, response.zones_size());
}

TEST_F(TestTopologyServiceManager, test_ListPoolZone_InvalidParam) {
    ZoneIdType zoneId = 0x21;
    ZoneIdType zoneId2 = 0x22;
    PoolIdType poolId = 0x11;
    PrepareAddPhysicalPool(poolId);
    PrepareAddZone(zoneId,
            "testZone",
            poolId,
            "desc1");

    PrepareAddZone(zoneId2,
            "testZone2",
            poolId,
            "desc2");

    ListPoolZoneRequest request;

    ListPoolZoneResponse response;
    serviceManager_->ListPoolZone(&request, &response);

    ASSERT_EQ(kTopoErrCodeInvalidParam, response.statuscode());
    ASSERT_EQ(0, response.zones_size());
}

TEST_F(TestTopologyServiceManager, test_createPhysicalPool_Success) {
    PhysicalPoolRequest request;
    request.set_physicalpoolname("default");
    request.set_desc("just for test");

    PoolIdType physicalPoolId = 0x12;
    EXPECT_CALL(*idGenerator_, GenPhysicalPoolId())
        .WillOnce(Return(physicalPoolId));
    EXPECT_CALL(*storage_, StoragePhysicalPool(_))
        .WillOnce(Return(true));

    PhysicalPoolResponse response;
    serviceManager_->CreatePhysicalPool(&request, &response);

    ASSERT_EQ(kTopoErrCodeSuccess, response.statuscode());
    ASSERT_TRUE(response.has_physicalpoolinfo());
    ASSERT_EQ(physicalPoolId, response.physicalpoolinfo().physicalpoolid());
    ASSERT_EQ(request.physicalpoolname(),
        response.physicalpoolinfo().physicalpoolname());
    ASSERT_EQ(request.desc(), response.physicalpoolinfo().desc());
}

TEST_F(TestTopologyServiceManager, test_createPhysicalPool_InvalidParam) {
    PhysicalPoolRequest request;
    PhysicalPoolResponse response;
    serviceManager_->CreatePhysicalPool(&request, &response);

    ASSERT_EQ(kTopoErrCodeInvalidParam, response.statuscode());
    ASSERT_TRUE(false == response.has_physicalpoolinfo());
}

TEST_F(TestTopologyServiceManager, test_createPhysicalPool_Fail) {
    PhysicalPoolRequest request;
    PhysicalPoolResponse response;

    request.set_physicalpoolname("default");
    request.set_desc("just for test");

    EXPECT_CALL(*idGenerator_, GenPhysicalPoolId())
        .WillOnce(Return(UNINTIALIZE_ID));

    serviceManager_->CreatePhysicalPool(&request, &response);

    ASSERT_EQ(kTopoErrCodeAllocateIdFail, response.statuscode());
    ASSERT_TRUE(false == response.has_physicalpoolinfo());
}

TEST_F(TestTopologyServiceManager, test_createPhysicalPool_StorageFail) {
    PhysicalPoolRequest request;
    PhysicalPoolResponse response;

    request.set_physicalpoolname("default");
    request.set_desc("just for test");

    PoolIdType physicalPoolId = 0x12;
    EXPECT_CALL(*idGenerator_, GenPhysicalPoolId())
        .WillOnce(Return(physicalPoolId));
    EXPECT_CALL(*storage_, StoragePhysicalPool(_))
        .WillOnce(Return(false));

    serviceManager_->CreatePhysicalPool(&request, &response);

    ASSERT_EQ(kTopoErrCodeStorgeFail, response.statuscode());
    ASSERT_TRUE(false == response.has_physicalpoolinfo());
}

TEST_F(TestTopologyServiceManager, test_DeletePhysicalPool_ByIdSuccess) {
    PoolIdType pid = 0x12;
    PhysicalPoolRequest request;
    request.set_physicalpoolid(pid);

    PrepareAddPhysicalPool(pid, "default");

    EXPECT_CALL(*storage_, DeletePhysicalPool(_))
        .WillOnce(Return(true));

    PhysicalPoolResponse response;
    serviceManager_->DeletePhysicalPool(&request, &response);

    ASSERT_EQ(kTopoErrCodeSuccess, response.statuscode());
}

TEST_F(TestTopologyServiceManager,
    test_DeletePhysicalPool_PhysicalPoolNotFound) {
    PoolIdType pid = 0x12;
    PhysicalPoolRequest request;
    request.set_physicalpoolid(pid);

    PrepareAddPhysicalPool(++pid, "default");

    PhysicalPoolResponse response;
    serviceManager_->DeletePhysicalPool(&request, &response);

    ASSERT_EQ(kTopoErrCodePhysicalPoolNotFound, response.statuscode());
}

TEST_F(TestTopologyServiceManager, test_DeletePhysicalPool_StorageFail) {
    PoolIdType pid = 0x12;
    PhysicalPoolRequest request;
    request.set_physicalpoolid(pid);

    PrepareAddPhysicalPool(pid, "default");

    EXPECT_CALL(*storage_, DeletePhysicalPool(_))
        .WillOnce(Return(false));

    PhysicalPoolResponse response;
    serviceManager_->DeletePhysicalPool(&request, &response);

    ASSERT_EQ(kTopoErrCodeStorgeFail, response.statuscode());
}

TEST_F(TestTopologyServiceManager, test_DeletePhysicalPool_ByNameSuccess) {
    std::string physicalPoolName = "testpool1";
    PhysicalPoolRequest request;
    request.set_physicalpoolname(physicalPoolName);

    PrepareAddPhysicalPool(0x12, physicalPoolName);

    EXPECT_CALL(*storage_, DeletePhysicalPool(_))
        .WillOnce(Return(true));

    PhysicalPoolResponse response;
    serviceManager_->DeletePhysicalPool(&request, &response);

    ASSERT_EQ(kTopoErrCodeSuccess, response.statuscode());
}

TEST_F(TestTopologyServiceManager, test_DeletePhysicalPool_ByNameFail) {
    std::string physicalPoolName = "testpool1";
    PhysicalPoolRequest request;
    request.set_physicalpoolname("testpool2");

    PrepareAddPhysicalPool(0x12, physicalPoolName);

    PhysicalPoolResponse response;
    serviceManager_->DeletePhysicalPool(&request, &response);

    ASSERT_EQ(kTopoErrCodePhysicalPoolNotFound, response.statuscode());
}

TEST_F(TestTopologyServiceManager, test_DeletePhysicalPool_InvalidParam) {
    std::string physicalPoolName = "testpool1";
    PhysicalPoolRequest request;

    PrepareAddPhysicalPool(0x12, physicalPoolName);

    PhysicalPoolResponse response;
    serviceManager_->DeletePhysicalPool(&request, &response);

    ASSERT_EQ(kTopoErrCodeInvalidParam, response.statuscode());
}

TEST_F(TestTopologyServiceManager, test_GetPhysicalPool_ByIdSuccess) {
    PoolIdType pid = 0x12;
    std::string pName = "test1";

    PhysicalPoolRequest request;
    request.set_physicalpoolid(pid);

    PrepareAddPhysicalPool(pid, pName);

    PhysicalPoolResponse response;
    serviceManager_->GetPhysicalPool(&request, &response);

    ASSERT_EQ(kTopoErrCodeSuccess, response.statuscode());
    ASSERT_TRUE(response.has_physicalpoolinfo());
    ASSERT_EQ(pid, response.physicalpoolinfo().physicalpoolid());
    ASSERT_EQ(pName, response.physicalpoolinfo().physicalpoolname());
}


TEST_F(TestTopologyServiceManager, test_GetPhysicalPool_InvalidParam) {
    PoolIdType pid = 0x12;
    PhysicalPoolRequest request;

    PrepareAddPhysicalPool(pid, "default");

    PhysicalPoolResponse response;
    serviceManager_->GetPhysicalPool(&request, &response);

    ASSERT_EQ(kTopoErrCodeInvalidParam, response.statuscode());
    ASSERT_EQ(false, response.has_physicalpoolinfo());
}

TEST_F(TestTopologyServiceManager, test_GetPhysicalPool_PhysicalPoolNotFound) {
    PoolIdType pid = 0x12;
    PhysicalPoolRequest request;
    request.set_physicalpoolid(pid);

    PrepareAddPhysicalPool(++pid, "default");

    PhysicalPoolResponse response;
    serviceManager_->GetPhysicalPool(&request, &response);

    ASSERT_EQ(kTopoErrCodePhysicalPoolNotFound, response.statuscode());
    ASSERT_EQ(false, response.has_physicalpoolinfo());
}

TEST_F(TestTopologyServiceManager, test_GetPhysicalPool_ByNameSuccess) {
    PoolIdType pid = 0x12;
    std::string pName = "test1";

    PhysicalPoolRequest request;
    request.set_physicalpoolname(pName);

    PrepareAddPhysicalPool(pid, pName);

    PhysicalPoolResponse response;
    serviceManager_->GetPhysicalPool(&request, &response);

    ASSERT_EQ(kTopoErrCodeSuccess, response.statuscode());
    ASSERT_TRUE(response.has_physicalpoolinfo());
    ASSERT_EQ(pid, response.physicalpoolinfo().physicalpoolid());
    ASSERT_EQ(pName, response.physicalpoolinfo().physicalpoolname());
}

TEST_F(TestTopologyServiceManager, test_GetPhysicalPool_ByNameFail) {
    PoolIdType pid = 0x12;
    std::string pName = "test1";

    PhysicalPoolRequest request;
    request.set_physicalpoolname("test2");

    PrepareAddPhysicalPool(pid, pName);

    PhysicalPoolResponse response;
    serviceManager_->GetPhysicalPool(&request, &response);

    ASSERT_EQ(kTopoErrCodePhysicalPoolNotFound, response.statuscode());
}

TEST_F(TestTopologyServiceManager, test_listPhysicalPool_success) {
    ListPhysicalPoolRequest request;
    ListPhysicalPoolResponse response;

    PrepareAddPhysicalPool(0x01, "test1");
    PrepareAddPhysicalPool(0x02, "test2");

    serviceManager_->ListPhysicalPool(&request, &response);

    ASSERT_EQ(kTopoErrCodeSuccess, response.statuscode());
    ASSERT_EQ(2, response.physicalpoolinfos_size());

    ASSERT_THAT(response.physicalpoolinfos(0).physicalpoolid(),
        AnyOf(0x01, 0x02));
    ASSERT_THAT(response.physicalpoolinfos(0).physicalpoolname(),
        AnyOf("test1", "test2"));
    ASSERT_THAT(response.physicalpoolinfos(1).physicalpoolid(),
        AnyOf(0x01, 0x02));
    ASSERT_THAT(response.physicalpoolinfos(1).physicalpoolname(),
        AnyOf("test1", "test2"));
}

static void CreateCopysetNodeFunc(::google::protobuf::RpcController *controller,
                           const ::curve::chunkserver::CopysetRequest2 *request,
                           ::curve::chunkserver::CopysetResponse2 *response,
                           google::protobuf::Closure *done) {
    /* return response */
    brpc::ClosureGuard doneGuard(done);
}


TEST_F(TestTopologyServiceManager, test_CreateLogicalPool_Success) {
    PoolIdType logicalPoolId = 0x01;
    PoolIdType physicalPoolId = 0x11;
    PrepareAddPhysicalPool(physicalPoolId);
    PrepareAddZone(0x21, "zone1", physicalPoolId);
    PrepareAddZone(0x22, "zone2", physicalPoolId);
    PrepareAddZone(0x23, "zone3", physicalPoolId);
    PrepareAddServer(0x31, "server1", "127.0.0.1", "127.0.0.1", 0x21, 0x11);
    PrepareAddServer(0x32, "server2", "127.0.0.1", "127.0.0.1", 0x22, 0x11);
    PrepareAddServer(0x33, "server3", "127.0.0.1", "127.0.0.1", 0x23, 0x11);
    uint32_t port = listenAddr_.port;
    PrepareAddChunkServer(
        0x41, "token1", "nvme", 0x31, "127.0.0.1", "127.0.0.1", port);
    PrepareAddChunkServer(
        0x42, "token2", "nvme", 0x32, "127.0.0.1", "127.0.0.1", port);
    PrepareAddChunkServer(
        0x43, "token3", "nvme", 0x33, "127.0.0.1", "127.0.0.1", port);

    CreateLogicalPoolRequest request;
    request.set_logicalpoolname("logicalpoolName1");
    request.set_physicalpoolid(physicalPoolId);
    request.set_type(PAGEFILE);
    request.set_redundanceandplacementpolicy(
        "{\"replicaNum\":3, \"copysetNum\":1, \"zoneNum\":3}");
    request.set_userpolicy("{}");

    EXPECT_CALL(*idGenerator_, GenLogicalPoolId())
        .WillOnce(Return(logicalPoolId));
    EXPECT_CALL(*storage_, StorageLogicalPool(_))
        .WillOnce(Return(true));

    CopySetIdType copysetId = 0x51;
    EXPECT_CALL(*idGenerator_, GenCopySetId(_))
        .WillRepeatedly(Return(copysetId));

    EXPECT_CALL(*storage_, StorageCopySet(_))
        .WillRepeatedly(Return(true));

    CopysetResponse2 chunkserverResponse;
    chunkserverResponse.set_status(
        COPYSET_OP_STATUS::COPYSET_OP_STATUS_SUCCESS);
    EXPECT_CALL(*mockCopySetService, CreateCopysetNode2(_, _, _, _))
        .WillRepeatedly(DoAll(SetArgPointee<2>(chunkserverResponse),
            Invoke(CreateCopysetNodeFunc)));

    EXPECT_CALL(*storage_, UpdateLogicalPool(_))
        .WillOnce(Return(true));

    CreateLogicalPoolResponse response;
    serviceManager_->CreateLogicalPool(&request, &response);
    ASSERT_EQ(kTopoErrCodeSuccess, response.statuscode());
    ASSERT_TRUE(response.has_logicalpoolinfo());
    ASSERT_EQ(logicalPoolId, response.logicalpoolinfo().logicalpoolid());
    ASSERT_EQ("logicalpoolName1", response.logicalpoolinfo().logicalpoolname());
    ASSERT_EQ(physicalPoolId, response.logicalpoolinfo().physicalpoolid());
    ASSERT_EQ(PAGEFILE, response.logicalpoolinfo().type());
}

TEST_F(TestTopologyServiceManager, test_CreateLogicalPool_ByNameSuccess) {
    PoolIdType logicalPoolId = 0x01;
    PoolIdType physicalPoolId = 0x11;
    PrepareAddPhysicalPool(physicalPoolId, "pPool1");
    PrepareAddZone(0x21, "zone1", physicalPoolId);
    PrepareAddZone(0x22, "zone2", physicalPoolId);
    PrepareAddZone(0x23, "zone3", physicalPoolId);
    PrepareAddServer(0x31, "server1", "127.0.0.1", "127.0.0.1", 0x21, 0x11);
    PrepareAddServer(0x32, "server2", "127.0.0.1", "127.0.0.1", 0x22, 0x11);
    PrepareAddServer(0x33, "server3", "127.0.0.1", "127.0.0.1", 0x23, 0x11);
    uint32_t port = listenAddr_.port;
    PrepareAddChunkServer(
        0x41, "token1", "nvme", 0x31, "127.0.0.1", "127.0.0.1", port);
    PrepareAddChunkServer(
        0x42, "token2", "nvme", 0x32, "127.0.0.1", "127.0.0.1", port);
    PrepareAddChunkServer(
        0x43, "token3", "nvme", 0x33, "127.0.0.1", "127.0.0.1", port);

    CreateLogicalPoolRequest request;
    request.set_logicalpoolname("logicalpoolName1");
    request.set_physicalpoolname("pPool1");
    request.set_type(PAGEFILE);
    request.set_redundanceandplacementpolicy(
        "{\"replicaNum\":3, \"copysetNum\":1, \"zoneNum\":3}");
    request.set_userpolicy("{}");

    EXPECT_CALL(*idGenerator_, GenLogicalPoolId())
        .WillOnce(Return(logicalPoolId));
    EXPECT_CALL(*storage_, StorageLogicalPool(_))
        .WillOnce(Return(true));

    CopySetIdType copysetId = 0x51;
    EXPECT_CALL(*idGenerator_, GenCopySetId(_))
        .WillRepeatedly(Return(copysetId));

    EXPECT_CALL(*storage_, StorageCopySet(_))
        .WillRepeatedly(Return(true));

    CopysetResponse2 chunkserverResponse;
    chunkserverResponse.set_status(
        COPYSET_OP_STATUS::COPYSET_OP_STATUS_SUCCESS);
    EXPECT_CALL(*mockCopySetService, CreateCopysetNode2(_, _, _, _))
        .WillRepeatedly(DoAll(SetArgPointee<2>(chunkserverResponse),
            Invoke(CreateCopysetNodeFunc)));

    EXPECT_CALL(*storage_, UpdateLogicalPool(_))
        .WillOnce(Return(true));

    CreateLogicalPoolResponse response;
    serviceManager_->CreateLogicalPool(&request, &response);
    ASSERT_EQ(kTopoErrCodeSuccess, response.statuscode());
    ASSERT_TRUE(response.has_logicalpoolinfo());
    ASSERT_EQ(logicalPoolId, response.logicalpoolinfo().logicalpoolid());
    ASSERT_EQ("logicalpoolName1", response.logicalpoolinfo().logicalpoolname());
    ASSERT_EQ(physicalPoolId, response.logicalpoolinfo().physicalpoolid());
    ASSERT_EQ(PAGEFILE, response.logicalpoolinfo().type());
}

TEST_F(TestTopologyServiceManager,
    test_CreateLogicalPool_PhysicalPoolNotFound) {
    PoolIdType physicalPoolId = 0x11;
    PrepareAddPhysicalPool(physicalPoolId);

    CreateLogicalPoolRequest request;
    request.set_logicalpoolname("logicalpoolName1");
    request.set_physicalpoolid(++physicalPoolId);
    request.set_type(PAGEFILE);
    request.set_redundanceandplacementpolicy("{}");
    request.set_userpolicy("{}");

    CreateLogicalPoolResponse response;
    serviceManager_->CreateLogicalPool(&request, &response);
    ASSERT_EQ(kTopoErrCodePhysicalPoolNotFound, response.statuscode());
    ASSERT_FALSE(response.has_logicalpoolinfo());
}

TEST_F(TestTopologyServiceManager,
    test_CreateLogicalPool_ByNamePhysicalPoolNotFound) {
    PoolIdType physicalPoolId = 0x11;
    PrepareAddPhysicalPool(physicalPoolId, "pPool1");

    CreateLogicalPoolRequest request;
    request.set_logicalpoolname("logicalpoolName1");
    request.set_physicalpoolname("pPool2");
    request.set_type(PAGEFILE);
    request.set_redundanceandplacementpolicy("{}");
    request.set_userpolicy("{}");

    CreateLogicalPoolResponse response;
    serviceManager_->CreateLogicalPool(&request, &response);
    ASSERT_EQ(kTopoErrCodePhysicalPoolNotFound, response.statuscode());
    ASSERT_FALSE(response.has_logicalpoolinfo());
}

TEST_F(TestTopologyServiceManager,
    test_CreateLogicalPool_InvalidParam) {
    PoolIdType physicalPoolId = 0x11;
    PrepareAddPhysicalPool(physicalPoolId);

    CreateLogicalPoolRequest request;
    request.set_logicalpoolname("logicalpoolName1");
    request.set_type(PAGEFILE);
    request.set_redundanceandplacementpolicy("{}");
    request.set_userpolicy("{}");

    CreateLogicalPoolResponse response;
    serviceManager_->CreateLogicalPool(&request, &response);
    ASSERT_EQ(kTopoErrCodeInvalidParam, response.statuscode());
    ASSERT_FALSE(response.has_logicalpoolinfo());
}

TEST_F(TestTopologyServiceManager, test_DeleteLogicalPool_ByIdSuccess) {
    PoolIdType physicalPoolId = 0x11;
    PrepareAddPhysicalPool(physicalPoolId);
    PoolIdType id = 0x01;
    PrepareAddLogicalPool(id, "name", physicalPoolId);

    DeleteLogicalPoolRequest request;
    request.set_logicalpoolid(id);

    EXPECT_CALL(*storage_, DeleteLogicalPool(_))
        .WillOnce(Return(true));

    DeleteLogicalPoolResponse response;
    serviceManager_->DeleteLogicalPool(&request, &response);
    ASSERT_EQ(kTopoErrCodeSuccess, response.statuscode());
}

TEST_F(TestTopologyServiceManager, test_DeleteLogicalPool_ByNameSuccess) {
    PoolIdType physicalPoolId = 0x11;
    PrepareAddPhysicalPool(physicalPoolId, "physicalpoolname");
    PoolIdType id = 0x01;
    PrepareAddLogicalPool(id, "name", physicalPoolId);

    DeleteLogicalPoolRequest request;
    request.set_logicalpoolname("name");
    request.set_physicalpoolname("physicalpoolname");

    EXPECT_CALL(*storage_, DeleteLogicalPool(_))
        .WillOnce(Return(true));

    DeleteLogicalPoolResponse response;
    serviceManager_->DeleteLogicalPool(&request, &response);
    ASSERT_EQ(kTopoErrCodeSuccess, response.statuscode());
}

TEST_F(TestTopologyServiceManager, test_DeleteLogicalPool_LogicalPoolNotFound) {
    PoolIdType physicalPoolId = 0x11;
    PrepareAddPhysicalPool(physicalPoolId);
    PoolIdType id = 0x01;
    PrepareAddLogicalPool(id, "name", physicalPoolId);

    DeleteLogicalPoolRequest request;
    request.set_logicalpoolid(++id);

    DeleteLogicalPoolResponse response;
    serviceManager_->DeleteLogicalPool(&request, &response);
    ASSERT_EQ(kTopoErrCodeLogicalPoolNotFound, response.statuscode());
}

TEST_F(TestTopologyServiceManager, test_DeleteLogicalPool_ByNameFail) {
    PoolIdType physicalPoolId = 0x11;
    PrepareAddPhysicalPool(physicalPoolId, "physicalpoolname");
    PoolIdType id = 0x01;
    PrepareAddLogicalPool(id, "name", physicalPoolId);

    DeleteLogicalPoolRequest request;
    request.set_logicalpoolname("name2");
    request.set_physicalpoolname("physicalpoolname");

    DeleteLogicalPoolResponse response;
    serviceManager_->DeleteLogicalPool(&request, &response);
    ASSERT_EQ(kTopoErrCodeLogicalPoolNotFound, response.statuscode());
}

TEST_F(TestTopologyServiceManager, test_DeleteLogicalPool_InvalidParam) {
    PoolIdType physicalPoolId = 0x11;
    PrepareAddPhysicalPool(physicalPoolId);
    PoolIdType id = 0x01;
    PrepareAddLogicalPool(id, "name", physicalPoolId);

    DeleteLogicalPoolRequest request;
    DeleteLogicalPoolResponse response;
    serviceManager_->DeleteLogicalPool(&request, &response);
    ASSERT_EQ(kTopoErrCodeInvalidParam, response.statuscode());
}

TEST_F(TestTopologyServiceManager, test_GetLogicalPool_ByIdSuccess) {
    PoolIdType physicalPoolId = 0x11;
    PrepareAddPhysicalPool(physicalPoolId);
    PoolIdType id = 0x01;
    PrepareAddLogicalPool(id, "name", physicalPoolId, PAGEFILE);

    GetLogicalPoolRequest request;
    request.set_logicalpoolid(id);

    GetLogicalPoolResponse response;
    serviceManager_->GetLogicalPool(&request, &response);
    ASSERT_EQ(kTopoErrCodeSuccess, response.statuscode());
    ASSERT_TRUE(response.has_logicalpoolinfo());
    ASSERT_EQ(id, response.logicalpoolinfo().logicalpoolid());
    ASSERT_EQ("name", response.logicalpoolinfo().logicalpoolname());
    ASSERT_EQ(physicalPoolId, response.logicalpoolinfo().physicalpoolid());
    ASSERT_EQ(PAGEFILE, response.logicalpoolinfo().type());
}

TEST_F(TestTopologyServiceManager, test_GetLogicalPool_ByNameSuccess) {
    PoolIdType physicalPoolId = 0x11;
    PrepareAddPhysicalPool(physicalPoolId, "physicalpoolname");
    PoolIdType id = 0x01;
    PrepareAddLogicalPool(id, "name", physicalPoolId, PAGEFILE);

    GetLogicalPoolRequest request;
    request.set_logicalpoolname("name");
    request.set_physicalpoolname("physicalpoolname");

    GetLogicalPoolResponse response;
    serviceManager_->GetLogicalPool(&request, &response);
    ASSERT_EQ(kTopoErrCodeSuccess, response.statuscode());
    ASSERT_TRUE(response.has_logicalpoolinfo());
    ASSERT_EQ(id, response.logicalpoolinfo().logicalpoolid());
    ASSERT_EQ("name", response.logicalpoolinfo().logicalpoolname());
    ASSERT_EQ(physicalPoolId, response.logicalpoolinfo().physicalpoolid());
    ASSERT_EQ(PAGEFILE, response.logicalpoolinfo().type());
}

TEST_F(TestTopologyServiceManager, test_GetLogicalPool_LogicalPoolNotFound) {
    PoolIdType physicalPoolId = 0x11;
    PrepareAddPhysicalPool(physicalPoolId);
    PoolIdType id = 0x01;
    PrepareAddLogicalPool(id, "name", physicalPoolId, PAGEFILE);

    GetLogicalPoolRequest request;
    request.set_logicalpoolid(++id);

    GetLogicalPoolResponse response;
    serviceManager_->GetLogicalPool(&request, &response);
    ASSERT_EQ(kTopoErrCodeLogicalPoolNotFound, response.statuscode());
    ASSERT_FALSE(response.has_logicalpoolinfo());
}

TEST_F(TestTopologyServiceManager, test_GetLogicalPool_ByNameFail) {
    PoolIdType physicalPoolId = 0x11;
    PrepareAddPhysicalPool(physicalPoolId, "physicalpoolname");
    PoolIdType id = 0x01;
    PrepareAddLogicalPool(id, "name", physicalPoolId, PAGEFILE);

    GetLogicalPoolRequest request;
    request.set_logicalpoolname("name2");
    request.set_physicalpoolname("physicalpoolname");

    GetLogicalPoolResponse response;
    serviceManager_->GetLogicalPool(&request, &response);
    ASSERT_EQ(kTopoErrCodeLogicalPoolNotFound, response.statuscode());
}

TEST_F(TestTopologyServiceManager, test_GetLogicalPool_InvalidParam) {
    PoolIdType physicalPoolId = 0x11;
    PrepareAddPhysicalPool(physicalPoolId);
    PoolIdType id = 0x01;
    PrepareAddLogicalPool(id, "name", physicalPoolId, PAGEFILE);

    GetLogicalPoolRequest request;

    GetLogicalPoolResponse response;
    serviceManager_->GetLogicalPool(&request, &response);
    ASSERT_EQ(kTopoErrCodeInvalidParam, response.statuscode());
    ASSERT_FALSE(response.has_logicalpoolinfo());
}

TEST_F(TestTopologyServiceManager, test_ListLogicalPool_ByIdSuccess) {
    PoolIdType physicalPoolId = 0x11;
    PrepareAddPhysicalPool(physicalPoolId);
    PoolIdType id = 0x01;
    PoolIdType id2 = 0x02;
    PrepareAddLogicalPool(id, "name", physicalPoolId, PAGEFILE);
    PrepareAddLogicalPool(id2, "name2", physicalPoolId, APPENDFILE);

    ListLogicalPoolRequest request;
    request.set_physicalpoolid(physicalPoolId);

    ListLogicalPoolResponse response;
    serviceManager_->ListLogicalPool(&request, &response);

    ASSERT_EQ(kTopoErrCodeSuccess, response.statuscode());
    ASSERT_EQ(2, response.logicalpoolinfos_size());
    ASSERT_THAT(response.logicalpoolinfos(0).logicalpoolid(), AnyOf(id, id2));
    ASSERT_THAT(response.logicalpoolinfos(0).logicalpoolname(),
        AnyOf("name", "name2"));
    ASSERT_EQ(physicalPoolId, response.logicalpoolinfos(0).physicalpoolid());
    ASSERT_THAT(response.logicalpoolinfos(0).type(),
        AnyOf(PAGEFILE, APPENDFILE));

    ASSERT_THAT(response.logicalpoolinfos(1).logicalpoolid(), AnyOf(id, id2));
    ASSERT_THAT(response.logicalpoolinfos(1).logicalpoolname(),
        AnyOf("name", "name2"));
    ASSERT_EQ(physicalPoolId, response.logicalpoolinfos(1).physicalpoolid());
    ASSERT_THAT(response.logicalpoolinfos(1).type(),
        AnyOf(PAGEFILE, APPENDFILE));
}

TEST_F(TestTopologyServiceManager, test_ListLogicalPool_ByNameSuccess) {
    PoolIdType physicalPoolId = 0x11;
    PrepareAddPhysicalPool(physicalPoolId, "physicalPoolName");
    PoolIdType id = 0x01;
    PoolIdType id2 = 0x02;
    PrepareAddLogicalPool(id, "name", physicalPoolId, PAGEFILE);
    PrepareAddLogicalPool(id2, "name2", physicalPoolId, APPENDFILE);

    ListLogicalPoolRequest request;
    request.set_physicalpoolname("physicalPoolName");

    ListLogicalPoolResponse response;
    serviceManager_->ListLogicalPool(&request, &response);

    ASSERT_EQ(kTopoErrCodeSuccess, response.statuscode());
    ASSERT_EQ(2, response.logicalpoolinfos_size());
    ASSERT_THAT(response.logicalpoolinfos(0).logicalpoolid(), AnyOf(id, id2));
    ASSERT_THAT(response.logicalpoolinfos(0).logicalpoolname(),
        AnyOf("name", "name2"));
    ASSERT_EQ(physicalPoolId, response.logicalpoolinfos(0).physicalpoolid());
    ASSERT_THAT(response.logicalpoolinfos(0).type(),
        AnyOf(PAGEFILE, APPENDFILE));

    ASSERT_THAT(response.logicalpoolinfos(1).logicalpoolid(), AnyOf(id, id2));
    ASSERT_THAT(response.logicalpoolinfos(1).logicalpoolname(),
        AnyOf("name", "name2"));
    ASSERT_EQ(physicalPoolId, response.logicalpoolinfos(1).physicalpoolid());
    ASSERT_THAT(response.logicalpoolinfos(1).type(),
        AnyOf(PAGEFILE, APPENDFILE));
}

TEST_F(TestTopologyServiceManager, test_ListLogicalPool_PhysicalPoolNotFound) {
    PoolIdType physicalPoolId = 0x11;
    PrepareAddPhysicalPool(physicalPoolId);
    PoolIdType id = 0x01;
    PoolIdType id2 = 0x02;
    PrepareAddLogicalPool(id, "name", physicalPoolId, PAGEFILE);
    PrepareAddLogicalPool(id2, "name2", physicalPoolId, APPENDFILE);

    ListLogicalPoolRequest request;
    request.set_physicalpoolid(++physicalPoolId);

    ListLogicalPoolResponse response;
    serviceManager_->ListLogicalPool(&request, &response);

    ASSERT_EQ(kTopoErrCodePhysicalPoolNotFound, response.statuscode());
    ASSERT_EQ(0, response.logicalpoolinfos_size());
}

TEST_F(TestTopologyServiceManager, test_ListLogicalPool_ByNameFail) {
    PoolIdType physicalPoolId = 0x11;
    PrepareAddPhysicalPool(physicalPoolId, "physicalPoolName");
    PoolIdType id = 0x01;
    PoolIdType id2 = 0x02;
    PrepareAddLogicalPool(id, "name", physicalPoolId, PAGEFILE);
    PrepareAddLogicalPool(id2, "name2", physicalPoolId, APPENDFILE);

    ListLogicalPoolRequest request;
    request.set_physicalpoolname("physicalPoolName2");

    ListLogicalPoolResponse response;
    serviceManager_->ListLogicalPool(&request, &response);

    ASSERT_EQ(kTopoErrCodePhysicalPoolNotFound, response.statuscode());
}

TEST_F(TestTopologyServiceManager, test_ListLogicalPool_InvalidParam) {
    PoolIdType physicalPoolId = 0x11;
    PrepareAddPhysicalPool(physicalPoolId);
    PoolIdType id = 0x01;
    PoolIdType id2 = 0x02;
    PrepareAddLogicalPool(id, "name", physicalPoolId, PAGEFILE);
    PrepareAddLogicalPool(id2, "name2", physicalPoolId, APPENDFILE);

    ListLogicalPoolRequest request;
    ListLogicalPoolResponse response;
    serviceManager_->ListLogicalPool(&request, &response);

    ASSERT_EQ(kTopoErrCodeInvalidParam, response.statuscode());
    ASSERT_EQ(0, response.logicalpoolinfos_size());
}

TEST_F(TestTopologyServiceManager, test_SetLogicalPool_success) {
    PoolIdType physicalPoolId = 0x11;
    PrepareAddPhysicalPool(physicalPoolId);
    PoolIdType id = 0x01;
    PrepareAddLogicalPool(id, "name", physicalPoolId, PAGEFILE);

    SetLogicalPoolRequest request;
    request.set_logicalpoolid(id);
    request.set_status(AllocateStatus::DENY);

    EXPECT_CALL(*storage_, UpdateLogicalPool(_))
        .WillOnce(Return(true));

    SetLogicalPoolResponse response;
    serviceManager_->SetLogicalPool(&request, &response);
    ASSERT_EQ(kTopoErrCodeSuccess, response.statuscode());
}

TEST_F(TestTopologyServiceManager, test_SetLogicalPool_LogicalPoolNotFound) {
    PoolIdType physicalPoolId = 0x11;
    PrepareAddPhysicalPool(physicalPoolId);
    PoolIdType id = 0x01;
    PrepareAddLogicalPool(id, "name", physicalPoolId, PAGEFILE);

    SetLogicalPoolRequest request;
    request.set_logicalpoolid(id + 1);
    request.set_status(AllocateStatus::DENY);

    SetLogicalPoolResponse response;
    serviceManager_->SetLogicalPool(&request, &response);
    ASSERT_EQ(kTopoErrCodeLogicalPoolNotFound, response.statuscode());
}

TEST_F(TestTopologyServiceManager, TestSetLogicalPoolScanState) {
    PoolIdType ppid = 1;  // physicalPoolId
    PoolIdType lpid = 1;  // logicalPoolId
    PrepareAddPhysicalPool(ppid);
    PrepareAddLogicalPool(lpid, "name", ppid);

    SetLogicalPoolScanStateRequest request;
    SetLogicalPoolScanStateResponse response;


    // CASE 1: logical pool not found
    request.set_logicalpoolid(lpid + 1);
    request.set_scanenable(true);
    serviceManager_->SetLogicalPoolScanState(&request, &response);
    ASSERT_EQ(response.statuscode(), kTopoErrCodeLogicalPoolNotFound);

    // CASE 2: set logical pool scan state success
    EXPECT_CALL(*storage_, UpdateLogicalPool(_))
        .WillOnce(Return(true));

    request.set_logicalpoolid(lpid);
    request.set_scanenable(false);
    serviceManager_->SetLogicalPoolScanState(&request, &response);
    ASSERT_EQ(response.statuscode(), kTopoErrCodeSuccess);
}

TEST_F(TestTopologyServiceManager,
    test_GetChunkServerListInCopySets_success) {
    PoolIdType logicalPoolId = 0x01;
    PoolIdType physicalPoolId = 0x11;
    CopySetIdType copysetId = 0x51;

    PrepareAddPhysicalPool(physicalPoolId);
    PrepareAddZone(0x21, "zone1", physicalPoolId);
    PrepareAddZone(0x22, "zone2", physicalPoolId);
    PrepareAddZone(0x23, "zone3", physicalPoolId);
    PrepareAddServer(0x31, "server1", "ip1", "ip2", 0x21, 0x11);
    PrepareAddServer(0x32, "server2", "ip1", "ip2", 0x22, 0x11);
    PrepareAddServer(0x33, "server3", "ip1", "ip2", 0x23, 0x11);
    PrepareAddChunkServer(0x41, "token1", "nvme", 0x31, "ip1", "ip2", 8888);
    PrepareAddChunkServer(0x42, "token2", "nvme", 0x32, "ip1", "ip2", 8888);
    PrepareAddChunkServer(0x43, "token3", "nvme", 0x33, "ip1", "ip2", 8888);
    PrepareAddLogicalPool(logicalPoolId, "logicalPool1", physicalPoolId);
    std::set<ChunkServerIdType> replicas;
    replicas.insert(0x41);
    replicas.insert(0x42);
    replicas.insert(0x43);
    PrepareAddCopySet(copysetId, logicalPoolId, replicas);

    GetChunkServerListInCopySetsRequest request;
    request.set_logicalpoolid(logicalPoolId);
    request.add_copysetid(copysetId);
    GetChunkServerListInCopySetsResponse response;
    serviceManager_->GetChunkServerListInCopySets(&request, &response);

    ASSERT_EQ(kTopoErrCodeSuccess, response.statuscode());
    ASSERT_EQ(1, response.csinfo_size());
    ASSERT_EQ(copysetId, response.csinfo(0).copysetid());
    ASSERT_EQ(3, response.csinfo(0).cslocs_size());

    ASSERT_THAT(response.csinfo(0).cslocs(0).chunkserverid(),
        AnyOf(0x41, 0x42, 0x43));
    ASSERT_EQ("ip1", response.csinfo(0).cslocs(0).hostip());
    ASSERT_EQ("ip2", response.csinfo(0).cslocs(0).externalip());
    ASSERT_EQ(8888, response.csinfo(0).cslocs(0).port());
}

TEST_F(TestTopologyServiceManager,
    test_GetChunkServerListInCopySets_CopysetNotFound) {

    PoolIdType logicalPoolId = 0x01;
    PoolIdType physicalPoolId = 0x11;
    CopySetIdType copysetId = 0x51;

    PrepareAddPhysicalPool(physicalPoolId);
    PrepareAddZone(0x21, "zone1", physicalPoolId);
    PrepareAddZone(0x22, "zone2", physicalPoolId);
    PrepareAddZone(0x23, "zone3", physicalPoolId);
    PrepareAddServer(0x31, "server1", "ip1", "ip2", 0x21, 0x11);
    PrepareAddServer(0x32, "server2", "ip1", "ip2", 0x22, 0x11);
    PrepareAddServer(0x33, "server3", "ip1", "ip2", 0x23, 0x11);
    PrepareAddChunkServer(0x41, "token1", "nvme", 0x31, "ip1", "ip2", 8888);
    PrepareAddChunkServer(0x42, "token2", "nvme", 0x32, "ip1", "ip2", 8888);
    PrepareAddChunkServer(0x43, "token3", "nvme", 0x33, "ip1", "ip2", 8888);
    PrepareAddLogicalPool(logicalPoolId, "logicalPool1", physicalPoolId);
    std::set<ChunkServerIdType> replicas;
    replicas.insert(0x41);
    replicas.insert(0x42);
    replicas.insert(0x43);
    PrepareAddCopySet(copysetId, logicalPoolId, replicas);

    GetChunkServerListInCopySetsRequest request;
    request.set_logicalpoolid(logicalPoolId);
    request.add_copysetid(++copysetId);
    GetChunkServerListInCopySetsResponse response;
    serviceManager_->GetChunkServerListInCopySets(&request, &response);

    ASSERT_EQ(kTopoErrCodeCopySetNotFound, response.statuscode());
}

TEST_F(TestTopologyServiceManager,
    test_GetChunkServerListInCopySets_InternalError) {

    PoolIdType logicalPoolId = 0x01;
    PoolIdType physicalPoolId = 0x11;
    CopySetIdType copysetId = 0x51;

    PrepareAddPhysicalPool(physicalPoolId);
    PrepareAddZone(0x21, "zone1", physicalPoolId);
    PrepareAddZone(0x22, "zone2", physicalPoolId);
    PrepareAddZone(0x23, "zone3", physicalPoolId);
    PrepareAddServer(0x31, "server1", "ip1", "ip2", 0x21, 0x11);
    PrepareAddServer(0x32, "server2", "ip1", "ip2",  0x22, 0x11);
    PrepareAddServer(0x33, "server3", "ip1", "ip2",  0x23, 0x11);
    PrepareAddChunkServer(0x41, "token1", "nvme", 0x31, "ip1", "ip2", 8888);
    PrepareAddChunkServer(0x42, "token2", "nvme", 0x32, "ip1", "ip2", 8888);
    PrepareAddChunkServer(0x43, "token3", "nvme", 0x33, "ip1", "ip2", 8888);
    PrepareAddLogicalPool(logicalPoolId, "logicalPool1", physicalPoolId);
    std::set<ChunkServerIdType> replicas;
    replicas.insert(0x41);
    replicas.insert(0x42);
    replicas.insert(0x44);  // here a invalid chunkserver
    PrepareAddCopySet(copysetId, logicalPoolId, replicas);

    GetChunkServerListInCopySetsRequest request;
    request.set_logicalpoolid(logicalPoolId);
    request.add_copysetid(copysetId);
    GetChunkServerListInCopySetsResponse response;
    serviceManager_->GetChunkServerListInCopySets(&request, &response);

    ASSERT_EQ(kTopoErrCodeInternalError, response.statuscode());
}

TEST_F(TestTopologyServiceManager,
    test_GetCopySetsInChunkServer_ByIdSuccess) {
    PoolIdType logicalPoolId = 0x01;
    PoolIdType physicalPoolId = 0x11;

    PrepareAddPhysicalPool(physicalPoolId);
    PrepareAddZone(0x21, "zone1", physicalPoolId);
    PrepareAddZone(0x22, "zone2", physicalPoolId);
    PrepareAddZone(0x23, "zone3", physicalPoolId);
    PrepareAddServer(0x31, "server1", "10.187.0.1", "10.187.26.1", 0x21, 0x11);
    PrepareAddServer(0x32, "server2", "10.187.0.2", "10.187.26.2", 0x22, 0x11);
    PrepareAddServer(0x33, "server3", "10.187.0.3", "10.187.26.3", 0x23, 0x11);
    PrepareAddChunkServer(
        0x41, "token1", "nvme", 0x31, "10.187.0.1", "10.187.26.1", 8200);
    PrepareAddChunkServer(
        0x42, "token2", "nvme", 0x32, "10.187.0.2", "10.187.26.2", 8200);
    PrepareAddChunkServer(
        0x43, "token3", "nvme", 0x33, "10.187.0.3", "10.187.26.3", 8200);
    PrepareAddLogicalPool(logicalPoolId, "logicalPool1", physicalPoolId);

    std::set<ChunkServerIdType> replicas;
    replicas.insert(0x41);
    replicas.insert(0x42);
    replicas.insert(0x43);
    PrepareAddCopySet(0x51, logicalPoolId, replicas);

    GetCopySetsInChunkServerRequest request;
    request.set_chunkserverid(0x41);

    GetCopySetsInChunkServerResponse response;
    serviceManager_->GetCopySetsInChunkServer(&request, &response);

    ASSERT_EQ(kTopoErrCodeSuccess, response.statuscode());
    ASSERT_EQ(1, response.copysetinfos_size());
    ASSERT_EQ(0x51, response.copysetinfos(0).copysetid());
}

TEST_F(TestTopologyServiceManager,
    test_GetCopySetsInChunkServer_ByIpSuccess) {
    PoolIdType logicalPoolId = 0x01;
    PoolIdType physicalPoolId = 0x11;

    PrepareAddPhysicalPool(physicalPoolId);
    PrepareAddZone(0x21, "zone1", physicalPoolId);
    PrepareAddZone(0x22, "zone2", physicalPoolId);
    PrepareAddZone(0x23, "zone3", physicalPoolId);
    PrepareAddServer(0x31, "server1", "10.187.0.1", "10.187.26.1", 0x21, 0x11);
    PrepareAddServer(0x32, "server2", "10.187.0.2", "10.187.26.2", 0x22, 0x11);
    PrepareAddServer(0x33, "server3", "10.187.0.3", "10.187.26.3", 0x23, 0x11);
    PrepareAddChunkServer(
        0x41, "token1", "nvme", 0x31, "10.187.0.1", "10.187.26.1", 8200);
    PrepareAddChunkServer(
        0x42, "token2", "nvme", 0x32, "10.187.0.2", "10.187.26.2", 8200);
    PrepareAddChunkServer(
        0x43, "token3", "nvme", 0x33, "10.187.0.3", "10.187.26.3", 8200);
    PrepareAddLogicalPool(logicalPoolId, "logicalPool1", physicalPoolId);

    std::set<ChunkServerIdType> replicas;
    replicas.insert(0x41);
    replicas.insert(0x42);
    replicas.insert(0x43);
    PrepareAddCopySet(0x51, logicalPoolId, replicas);

    GetCopySetsInChunkServerRequest request;
    request.set_hostip("10.187.0.1");
    request.set_port(8200);

    GetCopySetsInChunkServerResponse response;
    serviceManager_->GetCopySetsInChunkServer(&request, &response);

    ASSERT_EQ(kTopoErrCodeSuccess, response.statuscode());
    ASSERT_EQ(1, response.copysetinfos_size());
    ASSERT_EQ(0x51, response.copysetinfos(0).copysetid());

    request.set_hostip("10.187.26.1");
    request.set_port(8200);
    response.Clear();
    serviceManager_->GetCopySetsInChunkServer(&request, &response);

    ASSERT_EQ(kTopoErrCodeSuccess, response.statuscode());
    ASSERT_EQ(1, response.copysetinfos_size());
    ASSERT_EQ(0x51, response.copysetinfos(0).copysetid());
}

TEST_F(TestTopologyServiceManager,
    test_GetCopySetsInChunkServer_ByIdChunkserverNotFound) {
    PoolIdType logicalPoolId = 0x01;
    PoolIdType physicalPoolId = 0x11;

    PrepareAddPhysicalPool(physicalPoolId);
    PrepareAddZone(0x21, "zone1", physicalPoolId);
    PrepareAddServer(0x31, "server1");
    PrepareAddChunkServer(0x41, "token1", "nvme", 0x31);
    PrepareAddLogicalPool(logicalPoolId, "logicalPool1", physicalPoolId);

    GetCopySetsInChunkServerRequest request;
    request.set_chunkserverid(0x42);

    GetCopySetsInChunkServerResponse response;
    serviceManager_->GetCopySetsInChunkServer(&request, &response);

    ASSERT_EQ(kTopoErrCodeChunkServerNotFound, response.statuscode());
    ASSERT_EQ(0, response.copysetinfos_size());
}

TEST_F(TestTopologyServiceManager,
    test_GetCopySetsInChunkServer_ByIpChunkserverNotFound) {
    PoolIdType logicalPoolId = 0x01;
    PoolIdType physicalPoolId = 0x11;

    PrepareAddPhysicalPool(physicalPoolId);
    PrepareAddZone(0x21, "zone1", physicalPoolId);
    PrepareAddServer(0x31, "server1", "10.187.0.1", "10.187.0.1", 0x21, 0x11);
    PrepareAddChunkServer(0x41, "token1", "nvme", 0x31, "10.187.0.1");
    PrepareAddLogicalPool(logicalPoolId, "logicalPool1", physicalPoolId);

    GetCopySetsInChunkServerRequest request;
    request.set_hostip("10.187.0.2");
    request.set_port(9999);

    GetCopySetsInChunkServerResponse response;
    serviceManager_->GetCopySetsInChunkServer(&request, &response);

    ASSERT_EQ(kTopoErrCodeChunkServerNotFound, response.statuscode());
    ASSERT_EQ(0, response.copysetinfos_size());
}

TEST_F(TestTopologyServiceManager,
    test_GetCopySetsInChunkServer_InvalidParam) {
    GetCopySetsInChunkServerRequest request;

    GetCopySetsInChunkServerResponse response;
    serviceManager_->GetCopySetsInChunkServer(&request, &response);

    ASSERT_EQ(kTopoErrCodeInvalidParam, response.statuscode());
    ASSERT_EQ(0, response.copysetinfos_size());
}

TEST_F(TestTopologyServiceManager, test_GetCopySetsInCluster) {
    PoolIdType logicalPoolId1 = 0x1;
    PoolIdType physicalPoolId1 = 0x11;
    PrepareAddPhysicalPool(physicalPoolId1);
    PrepareAddLogicalPool(logicalPoolId1, "logicalPool1", physicalPoolId1);
    PoolIdType logicalPoolId2 = 0x2;
    PoolIdType physicalPoolId2 = 0x12;
    PrepareAddPhysicalPool(physicalPoolId2);
    PrepareAddLogicalPool(logicalPoolId2, "logicalPool2", physicalPoolId2);

    std::set<ChunkServerIdType> members = {1, 2, 3};
    for (int i = 1; i <= 10; ++i) {
        PrepareAddCopySet(i, logicalPoolId1, members);
    }
    for (int i = 11; i <= 20; ++i) {
        PrepareAddCopySet(i, logicalPoolId2, members);
    }

    GetCopySetsInClusterRequest request;
    GetCopySetsInClusterResponse response;
    serviceManager_->GetCopySetsInCluster(&request, &response);

    ASSERT_EQ(kTopoErrCodeSuccess, response.statuscode());
    ASSERT_EQ(20, response.copysetinfos_size());
    for (int i = 0; i < 20; i++) {
        if (i < 10) {
            ASSERT_EQ(1, response.copysetinfos(i).logicalpoolid());
        } else {
            ASSERT_EQ(2, response.copysetinfos(i).logicalpoolid());
        }
        ASSERT_EQ(i + 1, response.copysetinfos(i).copysetid());
    }
    GetCopySetsInClusterResponse response2;
    serviceManager_->GetCopySetsInCluster(&request, &response2);

    ASSERT_EQ(kTopoErrCodeSuccess, response2.statuscode());
    ASSERT_EQ(20, response2.copysetinfos_size());
    ASSERT_EQ(1, response2.copysetinfos(0).copysetid());
}

TEST_F(TestTopologyServiceManager, test_SetCopysetsAvailFlag) {
    PoolIdType logicalPoolId1 = 0x1;
    PoolIdType physicalPoolId1 = 0x11;
    PrepareAddPhysicalPool(physicalPoolId1);
    PrepareAddLogicalPool(logicalPoolId1, "logicalPool1", physicalPoolId1);
    std::set<ChunkServerIdType> replicas;
    replicas.insert(0x41);
    replicas.insert(0x42);
    replicas.insert(0x43);
    for (int i = 1; i <= 20; i++) {
        PrepareAddCopySet(i, logicalPoolId1, replicas);
    }
    std::vector<CopySetInfo> copysets =
                topology_->GetCopySetInfosInLogicalPool(logicalPoolId1);
    for (const auto copyset : copysets) {
        ASSERT_TRUE(copyset.IsAvailable());
    }
    // success
    {
        SetCopysetsAvailFlagRequest request;
        request.set_availflag(false);
        for (int i = 1; i <= 10; ++i) {
            CopysetInfo* copyset = request.add_copysets();
            copyset->set_logicalpoolid(logicalPoolId1);
            copyset->set_copysetid(i);
        }
        SetCopysetsAvailFlagResponse response;
        EXPECT_CALL(*storage_, UpdateCopySet(_))
            .Times(10)
            .WillRepeatedly(Return(true));
        serviceManager_->SetCopysetsAvailFlag(&request, &response);
        ASSERT_EQ(kTopoErrCodeSuccess, response.statuscode());
        copysets = topology_->GetCopySetInfosInLogicalPool(logicalPoolId1);
        for (const auto copyset : copysets) {
            if (copyset.GetId() <= 10) {
                ASSERT_FALSE(copyset.IsAvailable());
            } else {
                ASSERT_TRUE(copyset.IsAvailable());
            }
        }
        request.set_availflag(true);
        EXPECT_CALL(*storage_, UpdateCopySet(_))
            .Times(10)
            .WillRepeatedly(Return(true));
        serviceManager_->SetCopysetsAvailFlag(&request, &response);
        ASSERT_EQ(kTopoErrCodeSuccess, response.statuscode());
        copysets = topology_->GetCopySetInfosInLogicalPool(logicalPoolId1);
        for (const auto copyset : copysets) {
            ASSERT_TRUE(copyset.IsAvailable());
        }
    }
    // copyset not found
    {
        SetCopysetsAvailFlagRequest request;
        SetCopysetsAvailFlagResponse response;
        request.set_availflag(false);
        CopysetInfo* copyset = request.add_copysets();
        copyset->set_logicalpoolid(logicalPoolId1);
        copyset->set_copysetid(100);
        serviceManager_->SetCopysetsAvailFlag(&request, &response);
        ASSERT_EQ(kTopoErrCodeCopySetNotFound, response.statuscode());
    }
    // storage fail!
    {
        SetCopysetsAvailFlagRequest request;
        SetCopysetsAvailFlagResponse response;
        request.set_availflag(false);
        CopysetInfo* copyset = request.add_copysets();
        copyset->set_logicalpoolid(logicalPoolId1);
        copyset->set_copysetid(10);
        EXPECT_CALL(*storage_, UpdateCopySet(_))
            .WillOnce(Return(false));
        serviceManager_->SetCopysetsAvailFlag(&request, &response);
        ASSERT_EQ(kTopoErrCodeStorgeFail, response.statuscode());
    }
}

TEST_F(TestTopologyServiceManager, test_ListUnAvailCopySets) {
    PoolIdType logicalPoolId1 = 0x1;
    PoolIdType physicalPoolId1 = 0x11;
    PrepareAddPhysicalPool(physicalPoolId1);
    PrepareAddLogicalPool(logicalPoolId1, "logicalPool1", physicalPoolId1);
    std::set<ChunkServerIdType> replicas;
    replicas.insert(0x41);
    replicas.insert(0x42);
    replicas.insert(0x43);
    for (int i = 1; i <= 20; i++) {
        PrepareAddCopySet(i, logicalPoolId1, replicas);
    }
    SetCopysetsAvailFlagRequest request;
    request.set_availflag(false);
    for (int i = 1; i <= 10; ++i) {
        CopysetInfo* copyset = request.add_copysets();
        copyset->set_logicalpoolid(logicalPoolId1);
        copyset->set_copysetid(i);
    }
    SetCopysetsAvailFlagResponse response;
    EXPECT_CALL(*storage_, UpdateCopySet(_))
        .Times(10)
        .WillRepeatedly(Return(true));
    serviceManager_->SetCopysetsAvailFlag(&request, &response);
    ASSERT_EQ(kTopoErrCodeSuccess, response.statuscode());
    ListUnAvailCopySetsRequest request2;
    ListUnAvailCopySetsResponse response2;
    serviceManager_->ListUnAvailCopySets(&request2, &response2);
    ASSERT_EQ(kTopoErrCodeSuccess, response2.statuscode());
    ASSERT_EQ(10, response2.copysets_size());
    for (int i = 1; i <= 10; ++i) {
        ASSERT_EQ(i, response2.copysets(i - 1).copysetid());
    }
}

}  // namespace topology
}  // namespace mds
}  // namespace curve













