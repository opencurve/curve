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
 * Created Date: Fri Oct 19 2018
 * Author: xuchaojie
 */


#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <brpc/controller.h>
#include <brpc/channel.h>
#include <brpc/server.h>
#include <memory>

#include "src/mds/topology/topology_service.h"
#include "src/mds/topology/topology_storge.h"
#include "src/mds/topology/topology_storge_etcd.h"
#include "src/mds/topology/topology_storage_codec.h"

#include "src/mds/common/mds_define.h"
#include "test/mds/mock/mock_etcdclient.h"
#include "test/mds/topology/mock_topology.h"
#include "proto/topology.pb.h"

namespace curve {
namespace mds {
namespace topology {

using ::testing::Return;
using ::testing::_;
using ::testing::AnyOf;
using ::testing::SetArgPointee;
using ::testing::Invoke;

using ::curve::chunkserver::MockCopysetServiceImpl;
using ::curve::chunkserver::CopysetResponse;
using ::curve::chunkserver::COPYSET_OP_STATUS;

using ::curve::mds::copyset::CopysetManager;
using ::curve::mds::copyset::CopysetOption;

class TestTopologyService : public ::testing::Test {
 protected:
    TestTopologyService() {}
    ~TestTopologyService() {}
    virtual void SetUp() {
        server_ = new brpc::Server();

        std::shared_ptr<TopologyIdGenerator> idGenerator_ =
            std::make_shared<DefaultIdGenerator>();
        std::shared_ptr<TopologyTokenGenerator> tokenGenerator_ =
            std::make_shared<DefaultTokenGenerator>();

        auto etcdClient_ = std::make_shared<MockEtcdClient>();
        auto codec = std::make_shared<TopologyStorageCodec>();
        auto storage_ =
            std::make_shared<TopologyStorageEtcd>(etcdClient_, codec);

        CopysetOption copysetOption;
        manager_ = std::make_shared<MockTopologyServiceManager>(
                       std::make_shared<TopologyImpl>(idGenerator_,
                               tokenGenerator_,
                               storage_),
                       std::make_shared<CopysetManager>(copysetOption));

        TopologyServiceImpl *topoService = new TopologyServiceImpl(manager_);
        ASSERT_EQ(0, server_->AddService(topoService,
                                         brpc::SERVER_OWNS_SERVICE));

        ASSERT_EQ(0, server_->Start("127.0.0.1", {8900, 8999}, nullptr));
        listenAddr_ = server_->listen_address();
    }

    virtual void TearDown() {
        manager_ = nullptr;

        server_->Stop(0);
        server_->Join();
        delete server_;
        server_ = nullptr;
    }

 protected:
    std::shared_ptr<MockTopologyServiceManager> manager_;
    butil::EndPoint listenAddr_;
    brpc::Server *server_;
};

TEST_F(TestTopologyService, test_RegistChunkServer_success) {
    brpc::Channel channel;
    if (channel.Init(listenAddr_, NULL) != 0) {
        FAIL() << "Fail to init channel "
               << std::endl;
    }

    TopologyService_Stub stub(&channel);

    brpc::Controller cntl;
    ChunkServerRegistRequest request;
    request.set_disktype("1");
    request.set_diskpath("2");
    request.set_hostip("3");
    request.set_port(8888);
    request.set_externalip("127.0.0.1");

    ChunkServerRegistResponse response;

    ChunkServerRegistResponse reps;
    reps.set_statuscode(kTopoErrCodeSuccess);
    EXPECT_CALL(*manager_, RegistChunkServer(_, _))
    .WillRepeatedly(SetArgPointee<1>(reps));

    stub.RegistChunkServer(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        FAIL() << cntl.ErrorText() << std::endl;
    }

    ASSERT_EQ(kTopoErrCodeSuccess, response.statuscode());
}

TEST_F(TestTopologyService, test_RegistChunkServer_fail) {
    brpc::Channel channel;
    if (channel.Init(listenAddr_, NULL) != 0) {
        FAIL() << "Fail to init channel "
               << std::endl;
    }

    TopologyService_Stub stub(&channel);

    brpc::Controller cntl;
    ChunkServerRegistRequest request;
    request.set_disktype("1");
    request.set_diskpath("2");
    request.set_hostip("3");
    request.set_port(8888);
    request.set_externalip("127.0.0.1");

    ChunkServerRegistResponse response;

    ChunkServerRegistResponse reps;
    reps.set_statuscode(kTopoErrCodeInvalidParam);
    EXPECT_CALL(*manager_, RegistChunkServer(_, _))
    .WillRepeatedly(SetArgPointee<1>(reps));

    stub.RegistChunkServer(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        FAIL() << cntl.ErrorText() << std::endl;
    }

    ASSERT_EQ(kTopoErrCodeInvalidParam, response.statuscode());
}

TEST_F(TestTopologyService, test_ListChunkServer_success) {
    brpc::Channel channel;
    if (channel.Init(listenAddr_, NULL) != 0) {
        FAIL() << "Fail to init channel "
               << std::endl;
    }

    TopologyService_Stub stub(&channel);

    brpc::Controller cntl;
    ListChunkServerRequest request;
    request.set_ip("1");

    ListChunkServerResponse response;

    ListChunkServerResponse reps;
    reps.set_statuscode(kTopoErrCodeSuccess);
    EXPECT_CALL(*manager_, ListChunkServer(_, _))
    .WillRepeatedly(SetArgPointee<1>(reps));

    stub.ListChunkServer(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        FAIL() << cntl.ErrorText() << std::endl;
    }

    ASSERT_EQ(kTopoErrCodeSuccess, response.statuscode());
}

TEST_F(TestTopologyService, test_ListChunkServer_fail) {
    brpc::Channel channel;
    if (channel.Init(listenAddr_, NULL) != 0) {
        FAIL() << "Fail to init channel "
               << std::endl;
    }

    TopologyService_Stub stub(&channel);

    brpc::Controller cntl;
    ListChunkServerRequest request;
    request.set_ip("1");

    ListChunkServerResponse response;

    ListChunkServerResponse reps;
    reps.set_statuscode(kTopoErrCodeInvalidParam);
    EXPECT_CALL(*manager_, ListChunkServer(_, _))
    .WillRepeatedly(SetArgPointee<1>(reps));

    stub.ListChunkServer(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        FAIL() << cntl.ErrorText() << std::endl;
    }

    ASSERT_EQ(kTopoErrCodeInvalidParam, response.statuscode());
}

TEST_F(TestTopologyService, test_GetChunkServer_success) {
    brpc::Channel channel;
    if (channel.Init(listenAddr_, NULL) != 0) {
        FAIL() << "Fail to init channel "
               << std::endl;
    }

    TopologyService_Stub stub(&channel);

    brpc::Controller cntl;
    GetChunkServerInfoRequest request;
    request.set_chunkserverid(1);

    GetChunkServerInfoResponse response;

    GetChunkServerInfoResponse reps;
    reps.set_statuscode(kTopoErrCodeSuccess);
    EXPECT_CALL(*manager_, GetChunkServer(_, _))
    .WillRepeatedly(SetArgPointee<1>(reps));

    stub.GetChunkServer(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        FAIL() << cntl.ErrorText() << std::endl;
    }

    ASSERT_EQ(kTopoErrCodeSuccess, response.statuscode());
}

TEST_F(TestTopologyService, test_GetChunkServer_fail) {
    brpc::Channel channel;
    if (channel.Init(listenAddr_, NULL) != 0) {
        FAIL() << "Fail to init channel "
               << std::endl;
    }

    TopologyService_Stub stub(&channel);

    brpc::Controller cntl;
    GetChunkServerInfoRequest request;
    request.set_chunkserverid(1);

    GetChunkServerInfoResponse response;

    GetChunkServerInfoResponse reps;
    reps.set_statuscode(kTopoErrCodeInvalidParam);
    EXPECT_CALL(*manager_, GetChunkServer(_, _))
    .WillRepeatedly(SetArgPointee<1>(reps));

    stub.GetChunkServer(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        FAIL() << cntl.ErrorText() << std::endl;
    }

    ASSERT_EQ(kTopoErrCodeInvalidParam, response.statuscode());
}

TEST_F(TestTopologyService, test_DeleteChunkServer_success) {
    brpc::Channel channel;
    if (channel.Init(listenAddr_, NULL) != 0) {
        FAIL() << "Fail to init channel "
               << std::endl;
    }

    TopologyService_Stub stub(&channel);

    brpc::Controller cntl;
    DeleteChunkServerRequest request;
    request.set_chunkserverid(1);

    DeleteChunkServerResponse response;

    DeleteChunkServerResponse reps;
    reps.set_statuscode(kTopoErrCodeSuccess);
    EXPECT_CALL(*manager_, DeleteChunkServer(_, _))
    .WillRepeatedly(SetArgPointee<1>(reps));

    stub.DeleteChunkServer(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        FAIL() << cntl.ErrorText() << std::endl;
    }

    ASSERT_EQ(kTopoErrCodeSuccess, response.statuscode());
}

TEST_F(TestTopologyService, test_DeleteChunkServer_fail) {
    brpc::Channel channel;
    if (channel.Init(listenAddr_, NULL) != 0) {
        FAIL() << "Fail to init channel "
               << std::endl;
    }

    TopologyService_Stub stub(&channel);

    brpc::Controller cntl;
    DeleteChunkServerRequest request;
    request.set_chunkserverid(1);

    DeleteChunkServerResponse response;

    DeleteChunkServerResponse reps;
    reps.set_statuscode(kTopoErrCodeInvalidParam);
    EXPECT_CALL(*manager_, DeleteChunkServer(_, _))
    .WillRepeatedly(SetArgPointee<1>(reps));

    stub.DeleteChunkServer(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        FAIL() << cntl.ErrorText() << std::endl;
    }

    ASSERT_EQ(kTopoErrCodeInvalidParam, response.statuscode());
}

TEST_F(TestTopologyService, test_setChunkServer_success) {
    brpc::Channel channel;
    if (channel.Init(listenAddr_, NULL) != 0) {
        FAIL() << "Fail to init channel "
               << std::endl;
    }

    TopologyService_Stub stub(&channel);

    brpc::Controller cntl;
    SetChunkServerStatusRequest request;
    request.set_chunkserverid(1);
    request.set_chunkserverstatus(READWRITE);

    SetChunkServerStatusResponse response;

    SetChunkServerStatusResponse reps;
    reps.set_statuscode(kTopoErrCodeSuccess);
    EXPECT_CALL(*manager_, SetChunkServer(_, _))
    .WillRepeatedly(SetArgPointee<1>(reps));

    stub.SetChunkServer(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        FAIL() << cntl.ErrorText() << std::endl;
    }

    ASSERT_EQ(kTopoErrCodeSuccess, response.statuscode());
}

TEST_F(TestTopologyService, test_setChunkServer_fail) {
    brpc::Channel channel;
    if (channel.Init(listenAddr_, NULL) != 0) {
        FAIL() << "Fail to init channel "
               << std::endl;
    }

    TopologyService_Stub stub(&channel);

    brpc::Controller cntl;
    SetChunkServerStatusRequest request;
    request.set_chunkserverid(1);
    request.set_chunkserverstatus(READWRITE);

    SetChunkServerStatusResponse response;

    SetChunkServerStatusResponse reps;
    reps.set_statuscode(kTopoErrCodeInvalidParam);
    EXPECT_CALL(*manager_, SetChunkServer(_, _))
    .WillRepeatedly(SetArgPointee<1>(reps));

    stub.SetChunkServer(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        FAIL() << cntl.ErrorText() << std::endl;
    }

    ASSERT_EQ(kTopoErrCodeInvalidParam, response.statuscode());
}

TEST_F(TestTopologyService, test_RegistServer_success) {
    brpc::Channel channel;
    if (channel.Init(listenAddr_, NULL) != 0) {
        FAIL() << "Fail to init channel "
               << std::endl;
    }

    TopologyService_Stub stub(&channel);

    brpc::Controller cntl;
    ServerRegistRequest request;
    request.set_hostname("1");
    request.set_internalip("2");
    request.set_externalip("3");
    request.set_desc("4");

    ServerRegistResponse response;

    ServerRegistResponse reps;
    reps.set_statuscode(kTopoErrCodeSuccess);
    EXPECT_CALL(*manager_, RegistServer(_, _))
    .WillRepeatedly(SetArgPointee<1>(reps));

    stub.RegistServer(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        FAIL() << cntl.ErrorText() << std::endl;
    }

    ASSERT_EQ(kTopoErrCodeSuccess, response.statuscode());
}

TEST_F(TestTopologyService, test_RegistServer_fail) {
    brpc::Channel channel;
    if (channel.Init(listenAddr_, NULL) != 0) {
        FAIL() << "Fail to init channel "
               << std::endl;
    }

    TopologyService_Stub stub(&channel);

    brpc::Controller cntl;
    ServerRegistRequest request;
    request.set_hostname("1");
    request.set_internalip("2");
    request.set_externalip("3");
    request.set_desc("4");

    ServerRegistResponse response;

    ServerRegistResponse reps;
    reps.set_statuscode(kTopoErrCodeInvalidParam);
    EXPECT_CALL(*manager_, RegistServer(_, _))
    .WillRepeatedly(SetArgPointee<1>(reps));

    stub.RegistServer(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        FAIL() << cntl.ErrorText() << std::endl;
    }

    ASSERT_EQ(kTopoErrCodeInvalidParam, response.statuscode());
}

TEST_F(TestTopologyService, test_GetServer_success) {
    brpc::Channel channel;
    if (channel.Init(listenAddr_, NULL) != 0) {
        FAIL() << "Fail to init channel "
               << std::endl;
    }

    TopologyService_Stub stub(&channel);

    brpc::Controller cntl;
    GetServerRequest request;
    request.set_serverid(1);

    GetServerResponse response;

    GetServerResponse reps;
    reps.set_statuscode(kTopoErrCodeSuccess);
    EXPECT_CALL(*manager_, GetServer(_, _))
    .WillRepeatedly(SetArgPointee<1>(reps));

    stub.GetServer(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        FAIL() << cntl.ErrorText() << std::endl;
    }

    ASSERT_EQ(kTopoErrCodeSuccess, response.statuscode());
}

TEST_F(TestTopologyService, test_GetServer_fail) {
    brpc::Channel channel;
    if (channel.Init(listenAddr_, NULL) != 0) {
        FAIL() << "Fail to init channel "
               << std::endl;
    }

    TopologyService_Stub stub(&channel);

    brpc::Controller cntl;
    GetServerRequest request;
    request.set_serverid(1);

    GetServerResponse response;

    GetServerResponse reps;
    reps.set_statuscode(kTopoErrCodeInvalidParam);
    EXPECT_CALL(*manager_, GetServer(_, _))
    .WillRepeatedly(SetArgPointee<1>(reps));

    stub.GetServer(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        FAIL() << cntl.ErrorText() << std::endl;
    }

    ASSERT_EQ(kTopoErrCodeInvalidParam, response.statuscode());
}

TEST_F(TestTopologyService, test_DeleteServer_success) {
    brpc::Channel channel;
    if (channel.Init(listenAddr_, NULL) != 0) {
        FAIL() << "Fail to init channel "
               << std::endl;
    }

    TopologyService_Stub stub(&channel);

    brpc::Controller cntl;
    DeleteServerRequest request;
    request.set_serverid(1);

    DeleteServerResponse response;

    DeleteServerResponse reps;
    reps.set_statuscode(kTopoErrCodeSuccess);
    EXPECT_CALL(*manager_, DeleteServer(_, _))
    .WillRepeatedly(SetArgPointee<1>(reps));

    stub.DeleteServer(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        FAIL() << cntl.ErrorText() << std::endl;
    }

    ASSERT_EQ(kTopoErrCodeSuccess, response.statuscode());
}

TEST_F(TestTopologyService, test_DeleteServer_fail) {
    brpc::Channel channel;
    if (channel.Init(listenAddr_, NULL) != 0) {
        FAIL() << "Fail to init channel "
               << std::endl;
    }

    TopologyService_Stub stub(&channel);

    brpc::Controller cntl;
    DeleteServerRequest request;
    request.set_serverid(1);

    DeleteServerResponse response;

    DeleteServerResponse reps;
    reps.set_statuscode(kTopoErrCodeInvalidParam);
    EXPECT_CALL(*manager_, DeleteServer(_, _))
    .WillRepeatedly(SetArgPointee<1>(reps));

    stub.DeleteServer(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        FAIL() << cntl.ErrorText() << std::endl;
    }

    ASSERT_EQ(kTopoErrCodeInvalidParam, response.statuscode());
}

TEST_F(TestTopologyService, test_ListZoneServer_success) {
    brpc::Channel channel;
    if (channel.Init(listenAddr_, NULL) != 0) {
        FAIL() << "Fail to init channel "
               << std::endl;
    }

    TopologyService_Stub stub(&channel);

    brpc::Controller cntl;
    ListZoneServerRequest request;
    request.set_zoneid(1);

    ListZoneServerResponse response;

    ListZoneServerResponse reps;
    reps.set_statuscode(kTopoErrCodeSuccess);
    EXPECT_CALL(*manager_, ListZoneServer(_, _))
    .WillRepeatedly(SetArgPointee<1>(reps));

    stub.ListZoneServer(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        FAIL() << cntl.ErrorText() << std::endl;
    }

    ASSERT_EQ(kTopoErrCodeSuccess, response.statuscode());
}

TEST_F(TestTopologyService, test_ListZoneServer_fail) {
    brpc::Channel channel;
    if (channel.Init(listenAddr_, NULL) != 0) {
        FAIL() << "Fail to init channel "
               << std::endl;
    }

    TopologyService_Stub stub(&channel);

    brpc::Controller cntl;
    ListZoneServerRequest request;
    request.set_zoneid(1);

    ListZoneServerResponse response;

    ListZoneServerResponse reps;
    reps.set_statuscode(kTopoErrCodeInvalidParam);
    EXPECT_CALL(*manager_, ListZoneServer(_, _))
    .WillRepeatedly(SetArgPointee<1>(reps));

    stub.ListZoneServer(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        FAIL() << cntl.ErrorText() << std::endl;
    }

    ASSERT_EQ(kTopoErrCodeInvalidParam, response.statuscode());
}

TEST_F(TestTopologyService, test_CreateZone_success) {
    brpc::Channel channel;
    if (channel.Init(listenAddr_, NULL) != 0) {
        FAIL() << "Fail to init channel "
               << std::endl;
    }

    TopologyService_Stub stub(&channel);

    brpc::Controller cntl;
    ZoneRequest request;
    request.set_zoneid(1);

    ZoneResponse response;

    ZoneResponse reps;
    reps.set_statuscode(kTopoErrCodeSuccess);
    EXPECT_CALL(*manager_, CreateZone(_, _))
    .WillRepeatedly(SetArgPointee<1>(reps));

    stub.CreateZone(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        FAIL() << cntl.ErrorText() << std::endl;
    }

    ASSERT_EQ(kTopoErrCodeSuccess, response.statuscode());
}

TEST_F(TestTopologyService, test_CreateZone_fail) {
    brpc::Channel channel;
    if (channel.Init(listenAddr_, NULL) != 0) {
        FAIL() << "Fail to init channel "
               << std::endl;
    }

    TopologyService_Stub stub(&channel);

    brpc::Controller cntl;
    ZoneRequest request;
    request.set_zoneid(1);

    ZoneResponse response;

    ZoneResponse reps;
    reps.set_statuscode(kTopoErrCodeInvalidParam);
    EXPECT_CALL(*manager_, CreateZone(_, _))
    .WillRepeatedly(SetArgPointee<1>(reps));

    stub.CreateZone(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        FAIL() << cntl.ErrorText() << std::endl;
    }

    ASSERT_EQ(kTopoErrCodeInvalidParam, response.statuscode());
}

TEST_F(TestTopologyService, test_DeleteZone_success) {
    brpc::Channel channel;
    if (channel.Init(listenAddr_, NULL) != 0) {
        FAIL() << "Fail to init channel "
               << std::endl;
    }

    TopologyService_Stub stub(&channel);

    brpc::Controller cntl;
    ZoneRequest request;
    request.set_zoneid(1);

    ZoneResponse response;

    ZoneResponse reps;
    reps.set_statuscode(kTopoErrCodeSuccess);
    EXPECT_CALL(*manager_, DeleteZone(_, _))
    .WillRepeatedly(SetArgPointee<1>(reps));

    stub.DeleteZone(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        FAIL() << cntl.ErrorText() << std::endl;
    }

    ASSERT_EQ(kTopoErrCodeSuccess, response.statuscode());
}

TEST_F(TestTopologyService, test_DeleteZone_fail) {
    brpc::Channel channel;
    if (channel.Init(listenAddr_, NULL) != 0) {
        FAIL() << "Fail to init channel "
               << std::endl;
    }

    TopologyService_Stub stub(&channel);

    brpc::Controller cntl;
    ZoneRequest request;
    request.set_zoneid(1);

    ZoneResponse response;

    ZoneResponse reps;
    reps.set_statuscode(kTopoErrCodeInvalidParam);
    EXPECT_CALL(*manager_, DeleteZone(_, _))
    .WillRepeatedly(SetArgPointee<1>(reps));

    stub.DeleteZone(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        FAIL() << cntl.ErrorText() << std::endl;
    }

    ASSERT_EQ(kTopoErrCodeInvalidParam, response.statuscode());
}

TEST_F(TestTopologyService, test_GetZone_success) {
    brpc::Channel channel;
    if (channel.Init(listenAddr_, NULL) != 0) {
        FAIL() << "Fail to init channel "
               << std::endl;
    }

    TopologyService_Stub stub(&channel);

    brpc::Controller cntl;
    ZoneRequest request;
    request.set_zoneid(1);

    ZoneResponse response;

    ZoneResponse reps;
    reps.set_statuscode(kTopoErrCodeSuccess);
    EXPECT_CALL(*manager_, GetZone(_, _))
    .WillRepeatedly(SetArgPointee<1>(reps));

    stub.GetZone(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        FAIL() << cntl.ErrorText() << std::endl;
    }

    ASSERT_EQ(kTopoErrCodeSuccess, response.statuscode());
}

TEST_F(TestTopologyService, test_GetZone_fail) {
    brpc::Channel channel;
    if (channel.Init(listenAddr_, NULL) != 0) {
        FAIL() << "Fail to init channel "
               << std::endl;
    }

    TopologyService_Stub stub(&channel);

    brpc::Controller cntl;
    ZoneRequest request;
    request.set_zoneid(1);

    ZoneResponse response;

    ZoneResponse reps;
    reps.set_statuscode(kTopoErrCodeInvalidParam);
    EXPECT_CALL(*manager_, GetZone(_, _))
    .WillRepeatedly(SetArgPointee<1>(reps));

    stub.GetZone(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        FAIL() << cntl.ErrorText() << std::endl;
    }

    ASSERT_EQ(kTopoErrCodeInvalidParam, response.statuscode());
}

TEST_F(TestTopologyService, test_ListPoolZone_success) {
    brpc::Channel channel;
    if (channel.Init(listenAddr_, NULL) != 0) {
        FAIL() << "Fail to init channel "
               << std::endl;
    }

    TopologyService_Stub stub(&channel);

    brpc::Controller cntl;
    ListPoolZoneRequest request;
    request.set_physicalpoolid(1);

    ListPoolZoneResponse response;

    ListPoolZoneResponse reps;
    reps.set_statuscode(kTopoErrCodeSuccess);
    EXPECT_CALL(*manager_, ListPoolZone(_, _))
    .WillRepeatedly(SetArgPointee<1>(reps));

    stub.ListPoolZone(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        FAIL() << cntl.ErrorText() << std::endl;
    }

    ASSERT_EQ(kTopoErrCodeSuccess, response.statuscode());
}

TEST_F(TestTopologyService, test_ListPoolZone_fail) {
    brpc::Channel channel;
    if (channel.Init(listenAddr_, NULL) != 0) {
        FAIL() << "Fail to init channel "
               << std::endl;
    }

    TopologyService_Stub stub(&channel);

    brpc::Controller cntl;
    ListPoolZoneRequest request;
    request.set_physicalpoolid(1);

    ListPoolZoneResponse response;

    ListPoolZoneResponse reps;
    reps.set_statuscode(kTopoErrCodeInvalidParam);
    EXPECT_CALL(*manager_, ListPoolZone(_, _))
    .WillRepeatedly(SetArgPointee<1>(reps));

    stub.ListPoolZone(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        FAIL() << cntl.ErrorText() << std::endl;
    }

    ASSERT_EQ(kTopoErrCodeInvalidParam, response.statuscode());
}

TEST_F(TestTopologyService, test_CreatePhysicalPool_success) {
    brpc::Channel channel;
    if (channel.Init(listenAddr_, NULL) != 0) {
        FAIL() << "Fail to init channel "
               << std::endl;
    }

    TopologyService_Stub stub(&channel);

    brpc::Controller cntl;
    PhysicalPoolRequest request;
    request.set_physicalpoolid(1);

    PhysicalPoolResponse response;

    PhysicalPoolResponse reps;
    reps.set_statuscode(kTopoErrCodeSuccess);
    EXPECT_CALL(*manager_, CreatePhysicalPool(_, _))
    .WillRepeatedly(SetArgPointee<1>(reps));

    stub.CreatePhysicalPool(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        FAIL() << cntl.ErrorText() << std::endl;
    }

    ASSERT_EQ(kTopoErrCodeSuccess, response.statuscode());
}

TEST_F(TestTopologyService, test_CreatePhysicalPool_fail) {
    brpc::Channel channel;
    if (channel.Init(listenAddr_, NULL) != 0) {
        FAIL() << "Fail to init channel "
               << std::endl;
    }

    TopologyService_Stub stub(&channel);

    brpc::Controller cntl;
    PhysicalPoolRequest request;
    request.set_physicalpoolid(1);

    PhysicalPoolResponse response;

    PhysicalPoolResponse reps;
    reps.set_statuscode(kTopoErrCodeInvalidParam);
    EXPECT_CALL(*manager_, CreatePhysicalPool(_, _))
    .WillRepeatedly(SetArgPointee<1>(reps));

    stub.CreatePhysicalPool(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        FAIL() << cntl.ErrorText() << std::endl;
    }

    ASSERT_EQ(kTopoErrCodeInvalidParam, response.statuscode());
}

TEST_F(TestTopologyService, test_DeletePhysicalPool_success) {
    brpc::Channel channel;
    if (channel.Init(listenAddr_, NULL) != 0) {
        FAIL() << "Fail to init channel "
               << std::endl;
    }

    TopologyService_Stub stub(&channel);

    brpc::Controller cntl;
    PhysicalPoolRequest request;
    request.set_physicalpoolid(1);

    PhysicalPoolResponse response;

    PhysicalPoolResponse reps;
    reps.set_statuscode(kTopoErrCodeSuccess);
    EXPECT_CALL(*manager_, DeletePhysicalPool(_, _))
    .WillRepeatedly(SetArgPointee<1>(reps));

    stub.DeletePhysicalPool(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        FAIL() << cntl.ErrorText() << std::endl;
    }

    ASSERT_EQ(kTopoErrCodeSuccess, response.statuscode());
}

TEST_F(TestTopologyService, test_DeletePhysicalPool_fail) {
    brpc::Channel channel;
    if (channel.Init(listenAddr_, NULL) != 0) {
        FAIL() << "Fail to init channel "
               << std::endl;
    }

    TopologyService_Stub stub(&channel);

    brpc::Controller cntl;
    PhysicalPoolRequest request;
    request.set_physicalpoolid(1);

    PhysicalPoolResponse response;

    PhysicalPoolResponse reps;
    reps.set_statuscode(kTopoErrCodeInvalidParam);
    EXPECT_CALL(*manager_, DeletePhysicalPool(_, _))
    .WillRepeatedly(SetArgPointee<1>(reps));

    stub.DeletePhysicalPool(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        FAIL() << cntl.ErrorText() << std::endl;
    }

    ASSERT_EQ(kTopoErrCodeInvalidParam, response.statuscode());
}

TEST_F(TestTopologyService, test_GetPhysicalPool_success) {
    brpc::Channel channel;
    if (channel.Init(listenAddr_, NULL) != 0) {
        FAIL() << "Fail to init channel "
               << std::endl;
    }

    TopologyService_Stub stub(&channel);

    brpc::Controller cntl;
    PhysicalPoolRequest request;
    request.set_physicalpoolid(1);

    PhysicalPoolResponse response;

    PhysicalPoolResponse reps;
    reps.set_statuscode(kTopoErrCodeSuccess);
    EXPECT_CALL(*manager_, GetPhysicalPool(_, _))
    .WillRepeatedly(SetArgPointee<1>(reps));

    stub.GetPhysicalPool(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        FAIL() << cntl.ErrorText() << std::endl;
    }

    ASSERT_EQ(kTopoErrCodeSuccess, response.statuscode());
}

TEST_F(TestTopologyService, test_GetPhysicalPool_fail) {
    brpc::Channel channel;
    if (channel.Init(listenAddr_, NULL) != 0) {
        FAIL() << "Fail to init channel "
               << std::endl;
    }

    TopologyService_Stub stub(&channel);

    brpc::Controller cntl;
    PhysicalPoolRequest request;
    request.set_physicalpoolid(1);

    PhysicalPoolResponse response;

    PhysicalPoolResponse reps;
    reps.set_statuscode(kTopoErrCodeInvalidParam);
    EXPECT_CALL(*manager_, GetPhysicalPool(_, _))
    .WillRepeatedly(SetArgPointee<1>(reps));

    stub.GetPhysicalPool(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        FAIL() << cntl.ErrorText() << std::endl;
    }

    ASSERT_EQ(kTopoErrCodeInvalidParam, response.statuscode());
}

TEST_F(TestTopologyService, test_ListPhysicalPool_success) {
    brpc::Channel channel;
    if (channel.Init(listenAddr_, NULL) != 0) {
        FAIL() << "Fail to init channel "
               << std::endl;
    }

    TopologyService_Stub stub(&channel);

    brpc::Controller cntl;
    ListPhysicalPoolRequest request;

    ListPhysicalPoolResponse response;

    ListPhysicalPoolResponse reps;
    reps.set_statuscode(kTopoErrCodeSuccess);
    EXPECT_CALL(*manager_, ListPhysicalPool(_, _))
    .WillRepeatedly(SetArgPointee<1>(reps));

    stub.ListPhysicalPool(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        FAIL() << cntl.ErrorText() << std::endl;
    }

    ASSERT_EQ(kTopoErrCodeSuccess, response.statuscode());
}

TEST_F(TestTopologyService, test_ListPhysicalPool_fail) {
    brpc::Channel channel;
    if (channel.Init(listenAddr_, NULL) != 0) {
        FAIL() << "Fail to init channel "
               << std::endl;
    }

    TopologyService_Stub stub(&channel);

    brpc::Controller cntl;
    ListPhysicalPoolRequest request;

    ListPhysicalPoolResponse response;

    ListPhysicalPoolResponse reps;
    reps.set_statuscode(kTopoErrCodeInvalidParam);
    EXPECT_CALL(*manager_, ListPhysicalPool(_, _))
    .WillRepeatedly(SetArgPointee<1>(reps));

    stub.ListPhysicalPool(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        FAIL() << cntl.ErrorText() << std::endl;
    }

    ASSERT_EQ(kTopoErrCodeInvalidParam, response.statuscode());
}

TEST_F(TestTopologyService, test_CreateLogicalPool_success) {
    brpc::Channel channel;
    if (channel.Init(listenAddr_, NULL) != 0) {
        FAIL() << "Fail to init channel "
               << std::endl;
    }

    TopologyService_Stub stub(&channel);

    brpc::Controller cntl;
    CreateLogicalPoolRequest request;
    request.set_logicalpoolname("2");
    request.set_physicalpoolid(3);
    request.set_type(PAGEFILE);
    request.set_redundanceandplacementpolicy("");
    request.set_userpolicy("");

    CreateLogicalPoolResponse response;

    CreateLogicalPoolResponse reps;
    reps.set_statuscode(kTopoErrCodeSuccess);
    EXPECT_CALL(*manager_, CreateLogicalPool(_, _))
    .WillRepeatedly(SetArgPointee<1>(reps));

    stub.CreateLogicalPool(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        FAIL() << cntl.ErrorText() << std::endl;
    }

    ASSERT_EQ(kTopoErrCodeSuccess, response.statuscode());
}

TEST_F(TestTopologyService, test_CreateLogicalPool_fail) {
    brpc::Channel channel;
    if (channel.Init(listenAddr_, NULL) != 0) {
        FAIL() << "Fail to init channel "
               << std::endl;
    }

    TopologyService_Stub stub(&channel);

    brpc::Controller cntl;
    CreateLogicalPoolRequest request;
    request.set_logicalpoolname("2");
    request.set_physicalpoolid(3);
    request.set_type(PAGEFILE);
    request.set_redundanceandplacementpolicy("");
    request.set_userpolicy("");

    CreateLogicalPoolResponse response;

    CreateLogicalPoolResponse reps;
    reps.set_statuscode(kTopoErrCodeInvalidParam);
    EXPECT_CALL(*manager_, CreateLogicalPool(_, _))
    .WillRepeatedly(SetArgPointee<1>(reps));

    stub.CreateLogicalPool(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        FAIL() << cntl.ErrorText() << std::endl;
    }

    ASSERT_EQ(kTopoErrCodeInvalidParam, response.statuscode());
}

TEST_F(TestTopologyService, test_DeleteLogicalPool_success) {
    brpc::Channel channel;
    if (channel.Init(listenAddr_, NULL) != 0) {
        FAIL() << "Fail to init channel "
               << std::endl;
    }

    TopologyService_Stub stub(&channel);

    brpc::Controller cntl;
    DeleteLogicalPoolRequest request;
    request.set_logicalpoolid(3);

    DeleteLogicalPoolResponse response;

    DeleteLogicalPoolResponse reps;
    reps.set_statuscode(kTopoErrCodeSuccess);
    EXPECT_CALL(*manager_, DeleteLogicalPool(_, _))
    .WillRepeatedly(SetArgPointee<1>(reps));

    stub.DeleteLogicalPool(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        FAIL() << cntl.ErrorText() << std::endl;
    }

    ASSERT_EQ(kTopoErrCodeSuccess, response.statuscode());
}

TEST_F(TestTopologyService, test_DeleteLogicalPool_fail) {
    brpc::Channel channel;
    if (channel.Init(listenAddr_, NULL) != 0) {
        FAIL() << "Fail to init channel "
               << std::endl;
    }

    TopologyService_Stub stub(&channel);

    brpc::Controller cntl;
    DeleteLogicalPoolRequest request;
    request.set_logicalpoolid(3);

    DeleteLogicalPoolResponse response;

    DeleteLogicalPoolResponse reps;
    reps.set_statuscode(kTopoErrCodeInvalidParam);
    EXPECT_CALL(*manager_, DeleteLogicalPool(_, _))
    .WillRepeatedly(SetArgPointee<1>(reps));

    stub.DeleteLogicalPool(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        FAIL() << cntl.ErrorText() << std::endl;
    }

    ASSERT_EQ(kTopoErrCodeInvalidParam, response.statuscode());
}

TEST_F(TestTopologyService, test_GetLogicalPool_success) {
    brpc::Channel channel;
    if (channel.Init(listenAddr_, NULL) != 0) {
        FAIL() << "Fail to init channel "
               << std::endl;
    }

    TopologyService_Stub stub(&channel);

    brpc::Controller cntl;
    GetLogicalPoolRequest request;
    request.set_logicalpoolid(3);

    GetLogicalPoolResponse response;

    GetLogicalPoolResponse reps;
    reps.set_statuscode(kTopoErrCodeSuccess);
    EXPECT_CALL(*manager_, GetLogicalPool(_, _))
    .WillRepeatedly(SetArgPointee<1>(reps));

    stub.GetLogicalPool(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        FAIL() << cntl.ErrorText() << std::endl;
    }

    ASSERT_EQ(kTopoErrCodeSuccess, response.statuscode());
}

TEST_F(TestTopologyService, test_GetLogicalPool_fail) {
    brpc::Channel channel;
    if (channel.Init(listenAddr_, NULL) != 0) {
        FAIL() << "Fail to init channel "
               << std::endl;
    }

    TopologyService_Stub stub(&channel);

    brpc::Controller cntl;
    GetLogicalPoolRequest request;
    request.set_logicalpoolid(3);

    GetLogicalPoolResponse response;

    GetLogicalPoolResponse reps;
    reps.set_statuscode(kTopoErrCodeInvalidParam);
    EXPECT_CALL(*manager_, GetLogicalPool(_, _))
    .WillRepeatedly(SetArgPointee<1>(reps));

    stub.GetLogicalPool(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        FAIL() << cntl.ErrorText() << std::endl;
    }

    ASSERT_EQ(kTopoErrCodeInvalidParam, response.statuscode());
}

TEST_F(TestTopologyService, test_ListLogicalPool_success) {
    brpc::Channel channel;
    if (channel.Init(listenAddr_, NULL) != 0) {
        FAIL() << "Fail to init channel "
               << std::endl;
    }

    TopologyService_Stub stub(&channel);

    brpc::Controller cntl;
    ListLogicalPoolRequest request;
    request.set_physicalpoolid(3);

    ListLogicalPoolResponse response;

    ListLogicalPoolResponse reps;
    reps.set_statuscode(kTopoErrCodeSuccess);
    EXPECT_CALL(*manager_, ListLogicalPool(_, _))
    .WillRepeatedly(SetArgPointee<1>(reps));

    stub.ListLogicalPool(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        FAIL() << cntl.ErrorText() << std::endl;
    }

    ASSERT_EQ(kTopoErrCodeSuccess, response.statuscode());
}

TEST_F(TestTopologyService, test_ListLogicalPool_fail) {
    brpc::Channel channel;
    if (channel.Init(listenAddr_, NULL) != 0) {
        FAIL() << "Fail to init channel "
               << std::endl;
    }

    TopologyService_Stub stub(&channel);

    brpc::Controller cntl;
    ListLogicalPoolRequest request;
    request.set_physicalpoolid(3);

    ListLogicalPoolResponse response;

    ListLogicalPoolResponse reps;
    reps.set_statuscode(kTopoErrCodeInvalidParam);
    EXPECT_CALL(*manager_, ListLogicalPool(_, _))
    .WillRepeatedly(SetArgPointee<1>(reps));

    stub.ListLogicalPool(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        FAIL() << cntl.ErrorText() << std::endl;
    }

    ASSERT_EQ(kTopoErrCodeInvalidParam, response.statuscode());
}

TEST_F(TestTopologyService, test_GetChunkServerListInCopySets_success) {
    brpc::Channel channel;
    if (channel.Init(listenAddr_, NULL) != 0) {
        FAIL() << "Fail to init channel "
               << std::endl;
    }

    TopologyService_Stub stub(&channel);

    brpc::Controller cntl;
    GetChunkServerListInCopySetsRequest request;
    request.set_logicalpoolid(3);

    GetChunkServerListInCopySetsResponse response;

    GetChunkServerListInCopySetsResponse reps;
    reps.set_statuscode(kTopoErrCodeSuccess);
    EXPECT_CALL(*manager_, GetChunkServerListInCopySets(_, _))
    .WillRepeatedly(SetArgPointee<1>(reps));

    stub.GetChunkServerListInCopySets(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        FAIL() << cntl.ErrorText() << std::endl;
    }

    ASSERT_EQ(kTopoErrCodeSuccess, response.statuscode());
}

TEST_F(TestTopologyService, test_GetChunkServerListInCopySets_fail) {
    brpc::Channel channel;
    if (channel.Init(listenAddr_, NULL) != 0) {
        FAIL() << "Fail to init channel "
               << std::endl;
    }

    TopologyService_Stub stub(&channel);

    brpc::Controller cntl;
    GetChunkServerListInCopySetsRequest request;
    request.set_logicalpoolid(3);

    GetChunkServerListInCopySetsResponse response;

    GetChunkServerListInCopySetsResponse reps;
    reps.set_statuscode(kTopoErrCodeInvalidParam);
    EXPECT_CALL(*manager_, GetChunkServerListInCopySets(_, _))
    .WillRepeatedly(SetArgPointee<1>(reps));

    stub.GetChunkServerListInCopySets(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        FAIL() << cntl.ErrorText() << std::endl;
    }

    ASSERT_EQ(kTopoErrCodeInvalidParam, response.statuscode());
}

TEST_F(TestTopologyService, test_GetCopySetsInChunkServer_success) {
    brpc::Channel channel;
    if (channel.Init(listenAddr_, NULL) != 0) {
        FAIL() << "Fail to init channel "
               << std::endl;
    }

    TopologyService_Stub stub(&channel);

    brpc::Controller cntl;
    GetCopySetsInChunkServerRequest request;
    request.set_chunkserverid(1);

    GetCopySetsInChunkServerResponse response;

    GetCopySetsInChunkServerResponse reps;
    reps.set_statuscode(kTopoErrCodeSuccess);
    EXPECT_CALL(*manager_, GetCopySetsInChunkServer(_, _))
    .WillRepeatedly(SetArgPointee<1>(reps));

    stub.GetCopySetsInChunkServer(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        FAIL() << cntl.ErrorText() << std::endl;
    }

    ASSERT_EQ(kTopoErrCodeSuccess, response.statuscode());
}

TEST_F(TestTopologyService, test_GetCopySetsInChunkServer_fail) {
    brpc::Channel channel;
    if (channel.Init(listenAddr_, NULL) != 0) {
        FAIL() << "Fail to init channel "
               << std::endl;
    }

    TopologyService_Stub stub(&channel);

    brpc::Controller cntl;
    GetCopySetsInChunkServerRequest request;
    request.set_chunkserverid(1);

    GetCopySetsInChunkServerResponse response;

    GetCopySetsInChunkServerResponse reps;
    reps.set_statuscode(kTopoErrCodeInvalidParam);
    EXPECT_CALL(*manager_, GetCopySetsInChunkServer(_, _))
    .WillRepeatedly(SetArgPointee<1>(reps));

    stub.GetCopySetsInChunkServer(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        FAIL() << cntl.ErrorText() << std::endl;
    }

    ASSERT_EQ(kTopoErrCodeInvalidParam, response.statuscode());
}

TEST_F(TestTopologyService, test_GetCopySetsInCluster_success) {
    brpc::Channel channel;
    if (channel.Init(listenAddr_, NULL) != 0) {
        FAIL() << "Fail to init channel "
               << std::endl;
    }

    TopologyService_Stub stub(&channel);

    brpc::Controller cntl;
    GetCopySetsInClusterRequest request;

    GetCopySetsInClusterResponse response;

    GetCopySetsInClusterResponse reps;
    reps.set_statuscode(kTopoErrCodeSuccess);
    EXPECT_CALL(*manager_, GetCopySetsInCluster(_, _))
    .WillRepeatedly(SetArgPointee<1>(reps));

    stub.GetCopySetsInCluster(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        FAIL() << cntl.ErrorText() << std::endl;
    }

    ASSERT_EQ(kTopoErrCodeSuccess, response.statuscode());
}

TEST_F(TestTopologyService, test_GetCopySetsInCluster_fail) {
    brpc::Channel channel;
    if (channel.Init(listenAddr_, NULL) != 0) {
        FAIL() << "Fail to init channel "
               << std::endl;
    }

    TopologyService_Stub stub(&channel);

    brpc::Controller cntl;
    GetCopySetsInClusterRequest request;

    GetCopySetsInClusterResponse response;

    GetCopySetsInClusterResponse reps;
    reps.set_statuscode(kTopoErrCodeInvalidParam);
    EXPECT_CALL(*manager_, GetCopySetsInCluster(_, _))
    .WillRepeatedly(SetArgPointee<1>(reps));

    stub.GetCopySetsInCluster(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        FAIL() << cntl.ErrorText() << std::endl;
    }

    ASSERT_EQ(kTopoErrCodeInvalidParam, response.statuscode());
}

}  // namespace topology
}  // namespace mds
}  // namespace curve


