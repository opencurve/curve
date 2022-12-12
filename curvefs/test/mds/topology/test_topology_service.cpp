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


#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <brpc/controller.h>
#include <brpc/channel.h>
#include <brpc/server.h>
#include <memory>

#include "curvefs/src/mds/topology/topology_item.h"
#include "curvefs/test/mds/mock/mock_topology.h"
#include "curvefs/src/mds/topology/topology_service.h"
#include "curvefs/src/mds/topology/topology_storge.h"
#include "curvefs/src/mds/topology/topology_storge_etcd.h"
#include "curvefs/src/mds/topology/topology_storage_codec.h"
#include "curvefs/test/mds/mock/mock_metaserver.h"

#include "curvefs/src/mds/common/mds_define.h"
#include "curvefs/proto/topology.pb.h"

namespace curvefs {
namespace mds {
namespace topology {

using ::testing::Return;
using ::testing::_;
using ::testing::AnyOf;
using ::testing::SetArgPointee;
using ::testing::Invoke;

class TestTopologyService : public ::testing::Test {
 protected:
    TestTopologyService() {}
    ~TestTopologyService() {}
    virtual void SetUp() {
        std::string addr = "127.0.0.1:8999";
        server_ = new brpc::Server();

        std::shared_ptr<TopologyIdGenerator> idGenerator_ =
            std::make_shared<DefaultIdGenerator>();
        std::shared_ptr<TopologyTokenGenerator> tokenGenerator_ =
            std::make_shared<DefaultTokenGenerator>();

        auto etcdClient_ = std::make_shared<MockEtcdClient>();
        auto codec = std::make_shared<TopologyStorageCodec>();
        auto storage_ =
            std::make_shared<TopologyStorageEtcd>(etcdClient_, codec);

        MetaserverOptions metaserverOptions;
        metaserverOptions.metaserverAddr = addr;
        metaserverOptions.rpcTimeoutMs = 500;
        manager_ = std::make_shared<MockTopologyManager>(
                       std::make_shared<TopologyImpl>(idGenerator_,
                            tokenGenerator_, storage_),
                       std::make_shared<MetaserverClient>(metaserverOptions));

        topoService_ = new TopologyServiceImpl(manager_);
        ASSERT_EQ(0, server_->AddService(topoService_,
                                         brpc::SERVER_OWNS_SERVICE));
        ASSERT_EQ(0, server_->Start(addr.c_str(), nullptr));
        listenAddr_ = server_->listen_address();
        if (channel_.Init(listenAddr_, NULL) != 0) {
            FAIL() << "Fail to init channel_ "
                   << std::endl;
        }
    }

    virtual void TearDown() {
        manager_ = nullptr;
        server_->Stop(0);
        server_->Join();
        delete topoService_;
        server_ = nullptr;
    }

 protected:
    TopologyServiceImpl* topoService_;
    std::shared_ptr<MockTopologyManager> manager_;
    butil::EndPoint listenAddr_;
    brpc::Server *server_;
    brpc::Channel channel_;
};

TEST_F(TestTopologyService, test_RegistMetaServer_success) {
    TopologyService_Stub stub(&channel_);

    brpc::Controller cntl;
    MetaServerRegistRequest request;
    request.set_hostname("metaserver");
    request.set_internalip("127.0.0.1");
    request.set_internalport(8888);
    request.set_externalip("127.0.0.1");
    request.set_externalport(9999);

    MetaServerRegistResponse response;

    MetaServerRegistResponse reps;
    reps.set_statuscode(TopoStatusCode::TOPO_OK);
    EXPECT_CALL(*manager_, RegistMetaServer(_, _))
    .WillRepeatedly(SetArgPointee<1>(reps));

    stub.RegistMetaServer(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        FAIL() << cntl.ErrorText() << std::endl;
    }

    ASSERT_EQ(TopoStatusCode::TOPO_OK, response.statuscode());
}

TEST_F(TestTopologyService, test_RegistMetaServer_fail) {
    TopologyService_Stub stub(&channel_);

    brpc::Controller cntl;
    MetaServerRegistRequest request;
    request.set_hostname("metaserver");
    request.set_internalip("127.0.0.1");
    request.set_internalport(8888);
    request.set_externalip("127.0.0.1");
    request.set_externalport(9999);

    MetaServerRegistResponse response;

    MetaServerRegistResponse reps;
    reps.set_statuscode(TopoStatusCode::TOPO_INVALID_PARAM);
    EXPECT_CALL(*manager_, RegistMetaServer(_, _))
    .WillRepeatedly(SetArgPointee<1>(reps));

    stub.RegistMetaServer(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        FAIL() << cntl.ErrorText() << std::endl;
    }

    ASSERT_EQ(TopoStatusCode::TOPO_INVALID_PARAM, response.statuscode());
}

TEST_F(TestTopologyService, test_ListMetaServer_success) {
    TopologyService_Stub stub(&channel_);

    brpc::Controller cntl;
    ListMetaServerRequest request;
    request.set_serverid(1);

    ListMetaServerResponse response;

    ListMetaServerResponse reps;
    reps.set_statuscode(TopoStatusCode::TOPO_OK);
    EXPECT_CALL(*manager_, ListMetaServer(_, _))
    .WillRepeatedly(SetArgPointee<1>(reps));

    stub.ListMetaServer(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        FAIL() << cntl.ErrorText() << std::endl;
    }

    ASSERT_EQ(TopoStatusCode::TOPO_OK, response.statuscode());
}

TEST_F(TestTopologyService, test_ListMetaServer_fail) {
    TopologyService_Stub stub(&channel_);

    brpc::Controller cntl;
    ListMetaServerRequest request;
    request.set_serverid(1);

    ListMetaServerResponse response;

    ListMetaServerResponse reps;
    reps.set_statuscode(TopoStatusCode::TOPO_INVALID_PARAM);
    EXPECT_CALL(*manager_, ListMetaServer(_, _))
    .WillRepeatedly(SetArgPointee<1>(reps));

    stub.ListMetaServer(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        FAIL() << cntl.ErrorText() << std::endl;
    }

    ASSERT_EQ(TopoStatusCode::TOPO_INVALID_PARAM, response.statuscode());
}

TEST_F(TestTopologyService, test_GetMetaServer_success) {
    TopologyService_Stub stub(&channel_);

    brpc::Controller cntl;
    GetMetaServerInfoRequest request;
    request.set_metaserverid(1);

    GetMetaServerInfoResponse response;

    GetMetaServerInfoResponse reps;
    reps.set_statuscode(TopoStatusCode::TOPO_OK);
    EXPECT_CALL(*manager_, GetMetaServer(_, _))
    .WillRepeatedly(SetArgPointee<1>(reps));

    stub.GetMetaServer(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        FAIL() << cntl.ErrorText() << std::endl;
    }

    ASSERT_EQ(TopoStatusCode::TOPO_OK, response.statuscode());
}

TEST_F(TestTopologyService, test_GetMetaServer_fail) {
    TopologyService_Stub stub(&channel_);

    brpc::Controller cntl;
    GetMetaServerInfoRequest request;
    request.set_metaserverid(1);

    GetMetaServerInfoResponse response;

    GetMetaServerInfoResponse reps;
    reps.set_statuscode(TopoStatusCode::TOPO_INVALID_PARAM);
    EXPECT_CALL(*manager_, GetMetaServer(_, _))
    .WillRepeatedly(SetArgPointee<1>(reps));

    stub.GetMetaServer(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        FAIL() << cntl.ErrorText() << std::endl;
    }

    ASSERT_EQ(TopoStatusCode::TOPO_INVALID_PARAM, response.statuscode());
}

TEST_F(TestTopologyService, test_DeleteMetaServer_success) {
    TopologyService_Stub stub(&channel_);

    brpc::Controller cntl;
    DeleteMetaServerRequest request;
    request.set_metaserverid(1);

    DeleteMetaServerResponse response;

    DeleteMetaServerResponse reps;
    reps.set_statuscode(TopoStatusCode::TOPO_OK);
    EXPECT_CALL(*manager_, DeleteMetaServer(_, _))
    .WillRepeatedly(SetArgPointee<1>(reps));

    stub.DeleteMetaServer(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        FAIL() << cntl.ErrorText() << std::endl;
    }

    ASSERT_EQ(TopoStatusCode::TOPO_OK, response.statuscode());
}

TEST_F(TestTopologyService, test_DeleteMetaServer_fail) {
    TopologyService_Stub stub(&channel_);

    brpc::Controller cntl;
    DeleteMetaServerRequest request;
    request.set_metaserverid(1);

    DeleteMetaServerResponse response;

    DeleteMetaServerResponse reps;
    reps.set_statuscode(TopoStatusCode::TOPO_INVALID_PARAM);
    EXPECT_CALL(*manager_, DeleteMetaServer(_, _))
    .WillRepeatedly(SetArgPointee<1>(reps));

    stub.DeleteMetaServer(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        FAIL() << cntl.ErrorText() << std::endl;
    }

    ASSERT_EQ(TopoStatusCode::TOPO_INVALID_PARAM, response.statuscode());
}

TEST_F(TestTopologyService, test_RegistServer_success) {
    TopologyService_Stub stub(&channel_);

    brpc::Controller cntl;
    ServerRegistRequest request;
    request.set_hostname("1");
    request.set_internalip("2");
    request.set_internalport(2);
    request.set_externalip("3");
    request.set_externalport(3);
    request.set_zonename("zone");
    request.set_poolname("pool");

    ServerRegistResponse response;

    ServerRegistResponse reps;
    reps.set_statuscode(TopoStatusCode::TOPO_OK);
    EXPECT_CALL(*manager_, RegistServer(_, _))
    .WillRepeatedly(SetArgPointee<1>(reps));

    stub.RegistServer(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        FAIL() << cntl.ErrorText() << std::endl;
    }

    ASSERT_EQ(TopoStatusCode::TOPO_OK, response.statuscode());
}

TEST_F(TestTopologyService, test_RegistServer_fail) {
    TopologyService_Stub stub(&channel_);

    brpc::Controller cntl;
    ServerRegistRequest request;
    request.set_hostname("1");
    request.set_internalip("2");
    request.set_internalport(2);
    request.set_externalip("3");
    request.set_externalport(3);
    request.set_zonename("zone");
    request.set_poolname("pool");

    ServerRegistResponse response;

    ServerRegistResponse reps;
    reps.set_statuscode(TopoStatusCode::TOPO_INVALID_PARAM);
    EXPECT_CALL(*manager_, RegistServer(_, _))
    .WillRepeatedly(SetArgPointee<1>(reps));

    stub.RegistServer(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        FAIL() << cntl.ErrorText() << std::endl;
    }

    ASSERT_EQ(TopoStatusCode::TOPO_INVALID_PARAM, response.statuscode());
}

TEST_F(TestTopologyService, test_GetServer_success) {
    TopologyService_Stub stub(&channel_);

    brpc::Controller cntl;
    GetServerRequest request;
    request.set_serverid(1);

    GetServerResponse response;

    GetServerResponse reps;
    reps.set_statuscode(TopoStatusCode::TOPO_OK);
    EXPECT_CALL(*manager_, GetServer(_, _))
    .WillRepeatedly(SetArgPointee<1>(reps));

    stub.GetServer(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        FAIL() << cntl.ErrorText() << std::endl;
    }

    ASSERT_EQ(TopoStatusCode::TOPO_OK, response.statuscode());
}

TEST_F(TestTopologyService, test_GetServer_fail) {
    TopologyService_Stub stub(&channel_);

    brpc::Controller cntl;
    GetServerRequest request;
    request.set_serverid(1);

    GetServerResponse response;

    GetServerResponse reps;
    reps.set_statuscode(TopoStatusCode::TOPO_INVALID_PARAM);
    EXPECT_CALL(*manager_, GetServer(_, _))
    .WillRepeatedly(SetArgPointee<1>(reps));

    stub.GetServer(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        FAIL() << cntl.ErrorText() << std::endl;
    }

    ASSERT_EQ(TopoStatusCode::TOPO_INVALID_PARAM, response.statuscode());
}

TEST_F(TestTopologyService, test_DeleteServer_success) {
    TopologyService_Stub stub(&channel_);

    brpc::Controller cntl;
    DeleteServerRequest request;
    request.set_serverid(1);

    DeleteServerResponse response;

    DeleteServerResponse reps;
    reps.set_statuscode(TopoStatusCode::TOPO_OK);
    EXPECT_CALL(*manager_, DeleteServer(_, _))
    .WillRepeatedly(SetArgPointee<1>(reps));

    stub.DeleteServer(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        FAIL() << cntl.ErrorText() << std::endl;
    }

    ASSERT_EQ(TopoStatusCode::TOPO_OK, response.statuscode());
}

TEST_F(TestTopologyService, test_DeleteServer_fail) {
    TopologyService_Stub stub(&channel_);

    brpc::Controller cntl;
    DeleteServerRequest request;
    request.set_serverid(1);

    DeleteServerResponse response;

    DeleteServerResponse reps;
    reps.set_statuscode(TopoStatusCode::TOPO_INVALID_PARAM);
    EXPECT_CALL(*manager_, DeleteServer(_, _))
    .WillRepeatedly(SetArgPointee<1>(reps));

    stub.DeleteServer(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        FAIL() << cntl.ErrorText() << std::endl;
    }

    ASSERT_EQ(TopoStatusCode::TOPO_INVALID_PARAM, response.statuscode());
}

TEST_F(TestTopologyService, test_ListZoneServer_success) {
    TopologyService_Stub stub(&channel_);

    brpc::Controller cntl;
    ListZoneServerRequest request;
    request.set_zoneid(1);

    ListZoneServerResponse response;

    ListZoneServerResponse reps;
    reps.set_statuscode(TopoStatusCode::TOPO_OK);
    EXPECT_CALL(*manager_, ListZoneServer(_, _))
    .WillRepeatedly(SetArgPointee<1>(reps));

    stub.ListZoneServer(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        FAIL() << cntl.ErrorText() << std::endl;
    }

    ASSERT_EQ(TopoStatusCode::TOPO_OK, response.statuscode());
}

TEST_F(TestTopologyService, test_ListZoneServer_fail) {
    TopologyService_Stub stub(&channel_);

    brpc::Controller cntl;
    ListZoneServerRequest request;
    request.set_zoneid(1);

    ListZoneServerResponse response;

    ListZoneServerResponse reps;
    reps.set_statuscode(TopoStatusCode::TOPO_INVALID_PARAM);
    EXPECT_CALL(*manager_, ListZoneServer(_, _))
    .WillRepeatedly(SetArgPointee<1>(reps));

    stub.ListZoneServer(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        FAIL() << cntl.ErrorText() << std::endl;
    }

    ASSERT_EQ(TopoStatusCode::TOPO_INVALID_PARAM, response.statuscode());
}

TEST_F(TestTopologyService, test_CreateZone_success) {
    TopologyService_Stub stub(&channel_);

    brpc::Controller cntl;
    CreateZoneRequest request;
    request.set_zonename("zone");
    request.set_poolname("pool");

    CreateZoneResponse response;

    CreateZoneResponse reps;
    reps.set_statuscode(TopoStatusCode::TOPO_OK);
    EXPECT_CALL(*manager_, CreateZone(_, _))
    .WillRepeatedly(SetArgPointee<1>(reps));

    stub.CreateZone(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        FAIL() << cntl.ErrorText() << std::endl;
    }

    ASSERT_EQ(TopoStatusCode::TOPO_OK, response.statuscode());
}

TEST_F(TestTopologyService, test_CreateZone_fail) {
    TopologyService_Stub stub(&channel_);

    brpc::Controller cntl;
    CreateZoneRequest request;
    request.set_zonename("zone");
    request.set_poolname("pool");

    CreateZoneResponse response;

    CreateZoneResponse reps;
    reps.set_statuscode(TopoStatusCode::TOPO_INVALID_PARAM);
    EXPECT_CALL(*manager_, CreateZone(_, _))
    .WillRepeatedly(SetArgPointee<1>(reps));

    stub.CreateZone(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        FAIL() << cntl.ErrorText() << std::endl;
    }

    ASSERT_EQ(TopoStatusCode::TOPO_INVALID_PARAM, response.statuscode());
}

TEST_F(TestTopologyService, test_DeleteZone_success) {
    TopologyService_Stub stub(&channel_);

    brpc::Controller cntl;
    DeleteZoneRequest request;
    request.set_zoneid(1);

    DeleteZoneResponse response;

    DeleteZoneResponse reps;
    reps.set_statuscode(TopoStatusCode::TOPO_OK);
    EXPECT_CALL(*manager_, DeleteZone(_, _))
    .WillRepeatedly(SetArgPointee<1>(reps));

    stub.DeleteZone(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        FAIL() << cntl.ErrorText() << std::endl;
    }

    ASSERT_EQ(TopoStatusCode::TOPO_OK, response.statuscode());
}

TEST_F(TestTopologyService, test_DeleteZone_fail) {
    TopologyService_Stub stub(&channel_);

    brpc::Controller cntl;
    DeleteZoneRequest request;
    request.set_zoneid(1);

    DeleteZoneResponse response;

    DeleteZoneResponse reps;
    reps.set_statuscode(TopoStatusCode::TOPO_INVALID_PARAM);
    EXPECT_CALL(*manager_, DeleteZone(_, _))
    .WillRepeatedly(SetArgPointee<1>(reps));

    stub.DeleteZone(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        FAIL() << cntl.ErrorText() << std::endl;
    }

    ASSERT_EQ(TopoStatusCode::TOPO_INVALID_PARAM, response.statuscode());
}

TEST_F(TestTopologyService, test_GetZone_success) {
    TopologyService_Stub stub(&channel_);

    brpc::Controller cntl;
    GetZoneRequest request;
    request.set_zoneid(1);

    GetZoneResponse response;

    GetZoneResponse reps;
    reps.set_statuscode(TopoStatusCode::TOPO_OK);
    EXPECT_CALL(*manager_, GetZone(_, _))
    .WillRepeatedly(SetArgPointee<1>(reps));

    stub.GetZone(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        FAIL() << cntl.ErrorText() << std::endl;
    }

    ASSERT_EQ(TopoStatusCode::TOPO_OK, response.statuscode());
}

TEST_F(TestTopologyService, test_GetZone_fail) {
    TopologyService_Stub stub(&channel_);

    brpc::Controller cntl;
    GetZoneRequest request;
    request.set_zoneid(1);

    GetZoneResponse response;

    GetZoneResponse reps;
    reps.set_statuscode(TopoStatusCode::TOPO_INVALID_PARAM);
    EXPECT_CALL(*manager_, GetZone(_, _))
    .WillRepeatedly(SetArgPointee<1>(reps));

    stub.GetZone(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        FAIL() << cntl.ErrorText() << std::endl;
    }

    ASSERT_EQ(TopoStatusCode::TOPO_INVALID_PARAM, response.statuscode());
}

TEST_F(TestTopologyService, test_ListPoolZone_success) {
    TopologyService_Stub stub(&channel_);

    brpc::Controller cntl;
    ListPoolZoneRequest request;
    request.set_poolid(1);

    ListPoolZoneResponse response;

    ListPoolZoneResponse reps;
    reps.set_statuscode(TopoStatusCode::TOPO_OK);
    EXPECT_CALL(*manager_, ListPoolZone(_, _))
    .WillRepeatedly(SetArgPointee<1>(reps));

    stub.ListPoolZone(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        FAIL() << cntl.ErrorText() << std::endl;
    }

    ASSERT_EQ(TopoStatusCode::TOPO_OK, response.statuscode());
}

TEST_F(TestTopologyService, test_ListPoolZone_fail) {
    TopologyService_Stub stub(&channel_);

    brpc::Controller cntl;
    ListPoolZoneRequest request;
    request.set_poolid(1);

    ListPoolZoneResponse response;

    ListPoolZoneResponse reps;
    reps.set_statuscode(TopoStatusCode::TOPO_INVALID_PARAM);
    EXPECT_CALL(*manager_, ListPoolZone(_, _))
    .WillRepeatedly(SetArgPointee<1>(reps));

    stub.ListPoolZone(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        FAIL() << cntl.ErrorText() << std::endl;
    }

    ASSERT_EQ(TopoStatusCode::TOPO_INVALID_PARAM, response.statuscode());
}

TEST_F(TestTopologyService, test_CreatePool_success) {
    TopologyService_Stub stub(&channel_);

    brpc::Controller cntl;
    CreatePoolRequest request;
    request.set_poolname("pool");
    request.set_redundanceandplacementpolicy("");

    CreatePoolResponse response;

    CreatePoolResponse reps;
    reps.set_statuscode(TopoStatusCode::TOPO_OK);
    EXPECT_CALL(*manager_, CreatePool(_, _))
    .WillRepeatedly(SetArgPointee<1>(reps));

    stub.CreatePool(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        FAIL() << cntl.ErrorText() << std::endl;
    }

    ASSERT_EQ(TopoStatusCode::TOPO_OK, response.statuscode());
}

TEST_F(TestTopologyService, test_CreatePool_fail) {
    TopologyService_Stub stub(&channel_);

    brpc::Controller cntl;
    CreatePoolRequest request;
    request.set_poolname("pool");
    request.set_redundanceandplacementpolicy("");

    CreatePoolResponse response;

    CreatePoolResponse reps;
    reps.set_statuscode(TopoStatusCode::TOPO_INVALID_PARAM);
    EXPECT_CALL(*manager_, CreatePool(_, _))
    .WillRepeatedly(SetArgPointee<1>(reps));

    stub.CreatePool(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        FAIL() << cntl.ErrorText() << std::endl;
    }

    ASSERT_EQ(TopoStatusCode::TOPO_INVALID_PARAM, response.statuscode());
}

TEST_F(TestTopologyService, test_DeletePool_success) {
    TopologyService_Stub stub(&channel_);

    brpc::Controller cntl;
    DeletePoolRequest request;
    request.set_poolid(3);

    DeletePoolResponse response;

    DeletePoolResponse reps;
    reps.set_statuscode(TopoStatusCode::TOPO_OK);
    EXPECT_CALL(*manager_, DeletePool(_, _))
    .WillRepeatedly(SetArgPointee<1>(reps));

    stub.DeletePool(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        FAIL() << cntl.ErrorText() << std::endl;
    }

    ASSERT_EQ(TopoStatusCode::TOPO_OK, response.statuscode());
}

TEST_F(TestTopologyService, test_DeletePool_fail) {
    TopologyService_Stub stub(&channel_);

    brpc::Controller cntl;
    DeletePoolRequest request;
    request.set_poolid(3);

    DeletePoolResponse response;

    DeletePoolResponse reps;
    reps.set_statuscode(TopoStatusCode::TOPO_INVALID_PARAM);
    EXPECT_CALL(*manager_, DeletePool(_, _))
    .WillRepeatedly(SetArgPointee<1>(reps));

    stub.DeletePool(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        FAIL() << cntl.ErrorText() << std::endl;
    }

    ASSERT_EQ(TopoStatusCode::TOPO_INVALID_PARAM, response.statuscode());
}

TEST_F(TestTopologyService, test_GetPool_success) {
    TopologyService_Stub stub(&channel_);

    brpc::Controller cntl;
    GetPoolRequest request;
    request.set_poolid(3);

    GetPoolResponse response;

    GetPoolResponse reps;
    reps.set_statuscode(TopoStatusCode::TOPO_OK);
    EXPECT_CALL(*manager_, GetPool(_, _))
    .WillRepeatedly(SetArgPointee<1>(reps));

    stub.GetPool(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        FAIL() << cntl.ErrorText() << std::endl;
    }

    ASSERT_EQ(TopoStatusCode::TOPO_OK, response.statuscode());
}

TEST_F(TestTopologyService, test_GetPool_fail) {
    brpc::Channel channel_;
    if (channel_.Init(listenAddr_, NULL) != 0) {
        FAIL() << "Fail to init channel_ "
               << std::endl;
    }

    TopologyService_Stub stub(&channel_);

    brpc::Controller cntl;
    GetPoolRequest request;
    request.set_poolid(3);

    GetPoolResponse response;

    GetPoolResponse reps;
    reps.set_statuscode(TopoStatusCode::TOPO_INVALID_PARAM);
    EXPECT_CALL(*manager_, GetPool(_, _))
    .WillRepeatedly(SetArgPointee<1>(reps));

    stub.GetPool(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        FAIL() << cntl.ErrorText() << std::endl;
    }

    ASSERT_EQ(TopoStatusCode::TOPO_INVALID_PARAM, response.statuscode());
}

TEST_F(TestTopologyService, test_ListPool_success) {
    TopologyService_Stub stub(&channel_);

    brpc::Controller cntl;
    ListPoolRequest request;
    ListPoolResponse response;

    ListPoolResponse reps;
    reps.set_statuscode(TopoStatusCode::TOPO_OK);
    EXPECT_CALL(*manager_, ListPool(_, _))
    .WillRepeatedly(SetArgPointee<1>(reps));

    stub.ListPool(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        FAIL() << cntl.ErrorText() << std::endl;
    }

    ASSERT_EQ(TopoStatusCode::TOPO_OK, response.statuscode());
}

TEST_F(TestTopologyService, test_ListPool_fail) {
    TopologyService_Stub stub(&channel_);

    brpc::Controller cntl;
    ListPoolRequest request;
    ListPoolResponse response;

    ListPoolResponse reps;
    reps.set_statuscode(TopoStatusCode::TOPO_INVALID_PARAM);
    EXPECT_CALL(*manager_, ListPool(_, _))
    .WillRepeatedly(SetArgPointee<1>(reps));

    stub.ListPool(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        FAIL() << cntl.ErrorText() << std::endl;
    }

    ASSERT_EQ(TopoStatusCode::TOPO_INVALID_PARAM, response.statuscode());
}

TEST_F(TestTopologyService, test_GetMetaServerListInCopySets_success) {
    TopologyService_Stub stub(&channel_);

    brpc::Controller cntl;
    GetMetaServerListInCopySetsRequest request;
    request.set_poolid(1);
    request.add_copysetid(1);

    GetMetaServerListInCopySetsResponse response;

    GetMetaServerListInCopySetsResponse reps;
    reps.set_statuscode(TopoStatusCode::TOPO_OK);
    EXPECT_CALL(*manager_, GetMetaServerListInCopysets(_, _))
    .WillRepeatedly(SetArgPointee<1>(reps));

    stub.GetMetaServerListInCopysets(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        FAIL() << cntl.ErrorText() << std::endl;
    }

    ASSERT_EQ(TopoStatusCode::TOPO_OK, response.statuscode());
}

TEST_F(TestTopologyService, test_GetMetaServerListInCopySets_fail) {
    TopologyService_Stub stub(&channel_);

    brpc::Controller cntl;
    GetMetaServerListInCopySetsRequest request;
    request.set_poolid(1);
    request.add_copysetid(1);

    GetMetaServerListInCopySetsResponse response;

    GetMetaServerListInCopySetsResponse reps;
    reps.set_statuscode(TopoStatusCode::TOPO_INVALID_PARAM);
    EXPECT_CALL(*manager_, GetMetaServerListInCopysets(_, _))
    .WillRepeatedly(SetArgPointee<1>(reps));

    stub.GetMetaServerListInCopysets(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        FAIL() << cntl.ErrorText() << std::endl;
    }

    ASSERT_EQ(TopoStatusCode::TOPO_INVALID_PARAM, response.statuscode());
}

TEST_F(TestTopologyService, test_CreatePartition_success) {
    TopologyService_Stub stub(&channel_);

    brpc::Controller cntl;
    CreatePartitionRequest request;
    request.set_fsid(1);
    request.set_count(1);

    CreatePartitionResponse response;

    CreatePartitionResponse reps;
    reps.set_statuscode(TopoStatusCode::TOPO_OK);
    EXPECT_CALL(*manager_, CreatePartitions(_, _))
    .WillRepeatedly(SetArgPointee<1>(reps));

    stub.CreatePartition(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        FAIL() << cntl.ErrorText() << std::endl;
    }

    ASSERT_EQ(TopoStatusCode::TOPO_OK, response.statuscode());
}

TEST_F(TestTopologyService, test_CreatePartition_fail) {
    TopologyService_Stub stub(&channel_);

    brpc::Controller cntl;
    CreatePartitionRequest request;
    request.set_fsid(1);
    request.set_count(1);

    CreatePartitionResponse response;

    CreatePartitionResponse reps;
    reps.set_statuscode(TopoStatusCode::TOPO_CREATE_PARTITION_FAIL);
    EXPECT_CALL(*manager_, CreatePartitions(_, _))
    .WillRepeatedly(SetArgPointee<1>(reps));

    stub.CreatePartition(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        FAIL() << cntl.ErrorText() << std::endl;
    }

    ASSERT_EQ(TopoStatusCode::TOPO_CREATE_PARTITION_FAIL,
              response.statuscode());
}

TEST_F(TestTopologyService, test_DeletePartition_success) {
    TopologyService_Stub stub(&channel_);

    brpc::Controller cntl;
    DeletePartitionRequest request;
    request.set_partitionid(1);

    DeletePartitionResponse response;

    DeletePartitionResponse reps;
    reps.set_statuscode(TopoStatusCode::TOPO_OK);
    EXPECT_CALL(*manager_, DeletePartition(_, _))
    .WillOnce(SetArgPointee<1>(reps));

    stub.DeletePartition(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        FAIL() << cntl.ErrorText() << std::endl;
    }

    ASSERT_EQ(TopoStatusCode::TOPO_OK, response.statuscode());
}

TEST_F(TestTopologyService, test_DeletePartition_fail) {
    TopologyService_Stub stub(&channel_);

    brpc::Controller cntl;
    DeletePartitionRequest request;
    request.set_partitionid(1);

    DeletePartitionResponse response;

    DeletePartitionResponse reps;
    reps.set_statuscode(
        TopoStatusCode::TOPO_DELETE_PARTITION_ON_METASERVER_FAIL);
    EXPECT_CALL(*manager_, DeletePartition(_, _))
    .WillOnce(SetArgPointee<1>(reps));

    stub.DeletePartition(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        FAIL() << cntl.ErrorText() << std::endl;
    }

    ASSERT_EQ(TopoStatusCode::TOPO_DELETE_PARTITION_ON_METASERVER_FAIL,
              response.statuscode());
}

TEST_F(TestTopologyService, test_CommitTx_success) {
    TopologyService_Stub stub(&channel_);

    brpc::Controller cntl;
    CommitTxRequest request;
    PartitionTxId *ptxId = request.add_partitiontxids();
    ptxId->set_partitionid(1);
    ptxId->set_txid(1);

    CommitTxResponse response;

    CommitTxResponse reps;
    reps.set_statuscode(TopoStatusCode::TOPO_OK);
    EXPECT_CALL(*manager_, CommitTx(_, _))
    .WillRepeatedly(SetArgPointee<1>(reps));

    stub.CommitTx(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        FAIL() << cntl.ErrorText() << std::endl;
    }

    ASSERT_EQ(TopoStatusCode::TOPO_OK, response.statuscode());
}

TEST_F(TestTopologyService, test_CommitTx_fail) {
    TopologyService_Stub stub(&channel_);

    brpc::Controller cntl;
    CommitTxRequest request;
    PartitionTxId *ptxId = request.add_partitiontxids();
    ptxId->set_partitionid(1);
    ptxId->set_txid(1);

    CommitTxResponse response;

    CommitTxResponse reps;
    reps.set_statuscode(TopoStatusCode::TOPO_PARTITION_NOT_FOUND);
    EXPECT_CALL(*manager_, CommitTx(_, _))
    .WillRepeatedly(SetArgPointee<1>(reps));

    stub.CommitTx(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        FAIL() << cntl.ErrorText() << std::endl;
    }

    ASSERT_EQ(TopoStatusCode::TOPO_PARTITION_NOT_FOUND, response.statuscode());
}

TEST_F(TestTopologyService, test_ListPartition_success) {
    TopologyService_Stub stub(&channel_);

    brpc::Controller cntl;
    ListPartitionRequest request;
    request.set_fsid(1);

    ListPartitionResponse response;

    ListPartitionResponse reps;
    reps.set_statuscode(TopoStatusCode::TOPO_OK);
    EXPECT_CALL(*manager_, ListPartition(_, _))
    .WillRepeatedly(SetArgPointee<1>(reps));

    stub.ListPartition(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        FAIL() << cntl.ErrorText() << std::endl;
    }

    ASSERT_EQ(TopoStatusCode::TOPO_OK, response.statuscode());
}

TEST_F(TestTopologyService, test_GetCopysetOfPartition_success) {
    TopologyService_Stub stub(&channel_);

    brpc::Controller cntl;
    GetCopysetOfPartitionRequest request;
    request.add_partitionid(1);

    GetCopysetOfPartitionResponse response;

    GetCopysetOfPartitionResponse reps;
    reps.set_statuscode(TopoStatusCode::TOPO_OK);
    EXPECT_CALL(*manager_, GetCopysetOfPartition(_, _))
    .WillRepeatedly(SetArgPointee<1>(reps));

    stub.GetCopysetOfPartition(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        FAIL() << cntl.ErrorText() << std::endl;
    }

    ASSERT_EQ(TopoStatusCode::TOPO_OK, response.statuscode());
}

TEST_F(TestTopologyService, test_GetCopysetOfPartition_fail) {
    TopologyService_Stub stub(&channel_);

    brpc::Controller cntl;
    GetCopysetOfPartitionRequest request;
    request.add_partitionid(1);

    GetCopysetOfPartitionResponse response;

    GetCopysetOfPartitionResponse reps;
    reps.set_statuscode(TopoStatusCode::TOPO_COPYSET_NOT_FOUND);
    EXPECT_CALL(*manager_, GetCopysetOfPartition(_, _))
    .WillRepeatedly(SetArgPointee<1>(reps));

    stub.GetCopysetOfPartition(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        FAIL() << cntl.ErrorText() << std::endl;
    }

    ASSERT_EQ(TopoStatusCode::TOPO_COPYSET_NOT_FOUND, response.statuscode());
}

TEST_F(TestTopologyService, test_RegistMemcache_success) {
    TopologyService_Stub stub(&channel_);
    brpc::Controller cntl;
    RegistMemcacheClusterRequest request;
    MemcacheServerInfo server;
    server.set_ip("127.0.0.1");
    server.set_port(1);
    *request.add_servers() = server;
    server.set_port(2);
    *request.add_servers() = server;

    RegistMemcacheClusterResponse response;

    RegistMemcacheClusterResponse reps;
    reps.set_statuscode(TopoStatusCode::TOPO_OK);
    EXPECT_CALL(*manager_, RegistMemcacheCluster(_, _))
        .WillRepeatedly(SetArgPointee<1>(reps));

    stub.RegistMemcacheCluster(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        FAIL() << cntl.ErrorText() << std::endl;
    }

    ASSERT_EQ(TopoStatusCode::TOPO_OK, response.statuscode());
}

TEST_F(TestTopologyService, test_RegistMemcache_fail) {
    TopologyService_Stub stub(&channel_);
    brpc::Controller cntl;
    RegistMemcacheClusterRequest request;
    MemcacheServerInfo server;
    server.set_ip("127.0.0.1");
    server.set_port(1);
    *request.add_servers() = server;
    server.set_port(2);
    *request.add_servers() = server;

    RegistMemcacheClusterResponse response;

    RegistMemcacheClusterResponse reps;
    reps.set_statuscode(TopoStatusCode::TOPO_IP_PORT_DUPLICATED);
    EXPECT_CALL(*manager_, RegistMemcacheCluster(_, _))
        .WillRepeatedly(SetArgPointee<1>(reps));

    stub.RegistMemcacheCluster(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        FAIL() << cntl.ErrorText() << std::endl;
    }

    ASSERT_EQ(TopoStatusCode::TOPO_IP_PORT_DUPLICATED, response.statuscode());
}

TEST_F(TestTopologyService, test_ListMemcache_success) {
    TopologyService_Stub stub(&channel_);
    brpc::Controller cntl;
    ListMemcacheClusterRequest request;
    ListMemcacheClusterResponse response;

    ListMemcacheClusterResponse reps;
    reps.set_statuscode(TopoStatusCode::TOPO_OK);
    MemcacheClusterInfo cluster;
    cluster.set_clusterid(1);
    MemcacheServerInfo server;
    server.set_ip("127.0.0.1");
    server.set_port(1);
    *cluster.add_servers() = server;
    *reps.add_memcacheclusters() = cluster;
    EXPECT_CALL(*manager_, ListMemcacheCluster(_))
        .WillRepeatedly(SetArgPointee<0>(reps));

    stub.ListMemcacheCluster(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        FAIL() << cntl.ErrorText() << std::endl;
    }

    ASSERT_EQ(TopoStatusCode::TOPO_OK, response.statuscode());

    auto memcacheclusters1 = response.memcacheclusters();
    auto memcacheclusters2 = reps.memcacheclusters();
    ASSERT_EQ(memcacheclusters1.size(), memcacheclusters2.size());
    for (auto const& cluster1 : memcacheclusters1) {
        ASSERT_NE(
            std::find_if(memcacheclusters2.cbegin(), memcacheclusters2.cend(),
                         [=](const MemcacheClusterInfo& cluster2) {
                             return static_cast<MemcacheCluster>(cluster1) ==
                                    static_cast<MemcacheCluster>(cluster2);
                         }),
            memcacheclusters2.cend());
    }
}

TEST_F(TestTopologyService, test_ListMemcache_fail) {
    TopologyService_Stub stub(&channel_);
    brpc::Controller cntl;
    ListMemcacheClusterRequest request;
    ListMemcacheClusterResponse response;

    ListMemcacheClusterResponse reps;
    reps.set_statuscode(TopoStatusCode::TOPO_MEMCACHECLUSTER_NOT_FOUND);
    EXPECT_CALL(*manager_, ListMemcacheCluster(_))
        .WillRepeatedly(SetArgPointee<0>(reps));

    stub.ListMemcacheCluster(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        FAIL() << cntl.ErrorText() << std::endl;
    }

    ASSERT_EQ(TopoStatusCode::TOPO_MEMCACHECLUSTER_NOT_FOUND,
              response.statuscode());
}

}  // namespace topology
}  // namespace mds
}  // namespace curvefs
