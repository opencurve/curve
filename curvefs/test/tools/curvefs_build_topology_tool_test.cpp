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
 * @Project: curve
 * @Date: 2021-10-09
 * @Author: chengyi01
 */

#include "curvefs/src/tools/create/curvefs_create_topology_tool.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "curvefs/test/tools/mock_topology_service.h"

using ::testing::_;
using ::testing::Invoke;
using ::testing::SetArgPointee;

namespace curvefs {
namespace mds {
namespace topology {

class BuildTopologyToolTest : public ::testing::Test {
 protected:
    void SetUp() override {
        ASSERT_EQ(tool_.Init(), 0);
        ASSERT_EQ(tool_.TryAnotherMdsAddress(), 0);
        ASSERT_EQ(0, server_.AddService(&mockTopologyService_,
                                        brpc::SERVER_DOESNT_OWN_SERVICE));
        ASSERT_EQ(0, server_.Start(addr_.c_str(), nullptr));
    }

    void TearDown() override {
        server_.Stop(0);
        server_.Join();
    }

 protected:
    CurvefsBuildTopologyTool tool_;

    MockTopologyService mockTopologyService_;
    std::string addr_ = "127.0.0.1:16800";
    brpc::Server server_;
};

template <typename RpcRequestType, typename RpcResponseType,
          bool RpcFailed = false>
void RpcService(google::protobuf::RpcController* cntl_base,
                const RpcRequestType* request, RpcResponseType* response,
                google::protobuf::Closure* done) {
    if (RpcFailed) {
        brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);
        cntl->SetFailed(112, "Not connected to");
    }
    done->Run();
}

// test build a new cluster
TEST_F(BuildTopologyToolTest, test_BuildEmptyCluster) {
    // make list response
    ListPoolResponse listPoolResponse;
    listPoolResponse.set_statuscode(TopoStatusCode::TOPO_OK);
    ListPoolZoneResponse listPoolZoneResponse;
    listPoolZoneResponse.set_statuscode(TopoStatusCode::TOPO_OK);
    ListZoneServerResponse listZoneServerResponse;
    listZoneServerResponse.set_statuscode(TopoStatusCode::TOPO_OK);
    ListMetaServerResponse listMetaServerResponse;
    listMetaServerResponse.set_statuscode(TopoStatusCode::TOPO_OK);

    // make create respons
    CreatePoolResponse createPoolResponse;
    createPoolResponse.set_statuscode(TopoStatusCode::TOPO_OK);
    CreateZoneResponse createZoneResponse;
    createZoneResponse.set_statuscode(TopoStatusCode::TOPO_OK);
    ServerRegistResponse serverRegistResponse;
    serverRegistResponse.set_statuscode(TopoStatusCode::TOPO_OK);

    // make expected call
    EXPECT_CALL(mockTopologyService_, ListPool(_, _, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(listPoolResponse),
                        Invoke(RpcService<ListPoolRequest, ListPoolResponse>)));
    EXPECT_CALL(mockTopologyService_, CreatePool(_, _, _, _))
        .WillOnce(
            DoAll(SetArgPointee<2>(createPoolResponse),
                  Invoke(RpcService<CreatePoolRequest, CreatePoolResponse>)));
    EXPECT_CALL(mockTopologyService_, CreateZone(_, _, _, _))
        .Times(3)
        .WillRepeatedly(
            DoAll(SetArgPointee<2>(createZoneResponse),
                  Invoke(RpcService<CreateZoneRequest, CreateZoneResponse>)));
    EXPECT_CALL(mockTopologyService_, RegistServer(_, _, _, _))
        .Times(3)
        .WillRepeatedly(DoAll(
            SetArgPointee<2>(serverRegistResponse),
            Invoke(RpcService<ServerRegistRequest, ServerRegistResponse>)));

    ASSERT_EQ(0, tool_.InitTopoData());
    ASSERT_EQ(0, tool_.HandleBuildCluster());
    ASSERT_EQ(1, tool_.GetPoolDatas().size());
    ASSERT_EQ(3, tool_.GetZoneDatas().size());
    ASSERT_EQ(3, tool_.GetServerDatas().size());
}

// test add pool/zone/server/metaserver in cluster
TEST_F(BuildTopologyToolTest, test_BuildCluster) {
    // make list response
    // a pool with a zone with a server and metaserver has already in cluster
    ListPoolResponse listPoolResponse;
    listPoolResponse.set_statuscode(TopoStatusCode::TOPO_OK);
    PoolInfo* poolInfo = listPoolResponse.add_poolinfos();
    poolInfo->set_poolid(1);
    poolInfo->set_poolname("pool1");
    poolInfo->set_createtime(0);
    poolInfo->set_redundanceandplacementpolicy("");

    ListPoolZoneResponse listPoolZoneResponse;
    listPoolZoneResponse.set_statuscode(TopoStatusCode::TOPO_OK);
    ZoneInfo* zoneInfo = listPoolZoneResponse.add_zones();
    zoneInfo->set_zoneid(1);
    zoneInfo->set_zonename("zone1");
    zoneInfo->set_poolid(1);
    zoneInfo->set_poolname("pool1");

    ListZoneServerResponse listZoneServerResponse;
    listZoneServerResponse.set_statuscode(TopoStatusCode::TOPO_OK);
    ServerInfo* serverInfo = listZoneServerResponse.add_serverinfo();
    serverInfo->set_serverid(1);
    serverInfo->set_hostname("server1");
    serverInfo->set_internalip("127.0.0.1");
    serverInfo->set_internalport(0);
    serverInfo->set_externalip("127.0.0.1");
    serverInfo->set_externalport(0);
    serverInfo->set_zoneid(1);
    serverInfo->set_zonename("zone1");
    serverInfo->set_poolid(1);
    serverInfo->set_poolname("pool1");

    // make create respons
    CreatePoolResponse createPoolResponse;
    createPoolResponse.set_statuscode(TopoStatusCode::TOPO_OK);
    CreateZoneResponse createZoneResponse;
    createZoneResponse.set_statuscode(TopoStatusCode::TOPO_OK);
    ServerRegistResponse serverRegistResponse;
    serverRegistResponse.set_statuscode(TopoStatusCode::TOPO_OK);

    // make expected call
    EXPECT_CALL(mockTopologyService_, ListPool(_, _, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(listPoolResponse),
                        Invoke(RpcService<ListPoolRequest, ListPoolResponse>)));
    EXPECT_CALL(mockTopologyService_, ListPoolZone(_, _, _, _))
        .WillOnce(DoAll(
            SetArgPointee<2>(listPoolZoneResponse),
            Invoke(RpcService<ListPoolZoneRequest, ListPoolZoneResponse>)));
    EXPECT_CALL(mockTopologyService_, ListZoneServer(_, _, _, _))
        .WillOnce(DoAll(
            SetArgPointee<2>(listZoneServerResponse),
            Invoke(RpcService<ListZoneServerRequest, ListZoneServerResponse>)));

    EXPECT_CALL(mockTopologyService_, CreateZone(_, _, _, _))
        .Times(2)
        .WillRepeatedly(
            DoAll(SetArgPointee<2>(createZoneResponse),
                  Invoke(RpcService<CreateZoneRequest, CreateZoneResponse>)));
    EXPECT_CALL(mockTopologyService_, RegistServer(_, _, _, _))
        .Times(2)
        .WillRepeatedly(DoAll(
            SetArgPointee<2>(serverRegistResponse),
            Invoke(RpcService<ServerRegistRequest, ServerRegistResponse>)));

    ASSERT_EQ(0, tool_.InitTopoData());
    ASSERT_EQ(0, tool_.HandleBuildCluster());
    ASSERT_EQ(0, tool_.GetPoolDatas().size());
    ASSERT_EQ(2, tool_.GetZoneDatas().size());
    ASSERT_EQ(2, tool_.GetServerDatas().size());
}

TEST_F(BuildTopologyToolTest, test_ListPoolFailed) {
    // make list response
    ListPoolResponse listPoolResponse;
    listPoolResponse.set_statuscode(TopoStatusCode::TOPO_INTERNAL_ERROR);

    // make expected call
    EXPECT_CALL(mockTopologyService_, ListPool(_, _, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(listPoolResponse),
                        Invoke(RpcService<ListPoolRequest, ListPoolResponse>)));

    ASSERT_EQ(1, tool_.HandleBuildCluster());
}

TEST_F(BuildTopologyToolTest, test_ListPoolZoneFailed) {
    // make list response
    ListPoolResponse listPoolResponse;
    listPoolResponse.set_statuscode(TopoStatusCode::TOPO_OK);
    PoolInfo* poolInfo = listPoolResponse.add_poolinfos();
    poolInfo->set_poolid(1);
    poolInfo->set_poolname("pool1");
    poolInfo->set_createtime(0);
    poolInfo->set_redundanceandplacementpolicy("");

    ListPoolZoneResponse listPoolZoneResponse;
    listPoolZoneResponse.set_statuscode(TopoStatusCode::TOPO_INTERNAL_ERROR);

    // make expected call
    EXPECT_CALL(mockTopologyService_, ListPool(_, _, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(listPoolResponse),
                        Invoke(RpcService<ListPoolRequest, ListPoolResponse>)));
    EXPECT_CALL(mockTopologyService_, ListPoolZone(_, _, _, _))
        .WillOnce(DoAll(
            SetArgPointee<2>(listPoolZoneResponse),
            Invoke(RpcService<ListPoolZoneRequest, ListPoolZoneResponse>)));
    ASSERT_EQ(1, tool_.HandleBuildCluster());
}

TEST_F(BuildTopologyToolTest, test_ListZoneServerFailed) {
    // make list response
    ListPoolResponse listPoolResponse;
    listPoolResponse.set_statuscode(TopoStatusCode::TOPO_OK);
    PoolInfo* poolInfo = listPoolResponse.add_poolinfos();
    poolInfo->set_poolid(1);
    poolInfo->set_poolname("pool1");
    poolInfo->set_createtime(0);
    poolInfo->set_redundanceandplacementpolicy("");

    ListPoolZoneResponse listPoolZoneResponse;
    listPoolZoneResponse.set_statuscode(TopoStatusCode::TOPO_OK);
    ZoneInfo* zoneInfo = listPoolZoneResponse.add_zones();
    zoneInfo->set_zoneid(1);
    zoneInfo->set_zonename("zone1");
    zoneInfo->set_poolid(1);
    zoneInfo->set_poolname("pool1");

    ListZoneServerResponse listZoneServerResponse;
    listZoneServerResponse.set_statuscode(TopoStatusCode::TOPO_INTERNAL_ERROR);

    // make expected call
    EXPECT_CALL(mockTopologyService_, ListPool(_, _, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(listPoolResponse),
                        Invoke(RpcService<ListPoolRequest, ListPoolResponse>)));
    EXPECT_CALL(mockTopologyService_, ListPoolZone(_, _, _, _))
        .WillOnce(DoAll(
            SetArgPointee<2>(listPoolZoneResponse),
            Invoke(RpcService<ListPoolZoneRequest, ListPoolZoneResponse>)));
    EXPECT_CALL(mockTopologyService_, ListZoneServer(_, _, _, _))
        .WillOnce(DoAll(
            SetArgPointee<2>(listZoneServerResponse),
            Invoke(RpcService<ListZoneServerRequest, ListZoneServerResponse>)));
    ASSERT_EQ(1, tool_.HandleBuildCluster());
}

TEST_F(BuildTopologyToolTest, test_CreatePoolFailed) {
    // make list response
    ListPoolResponse listPoolResponse;
    listPoolResponse.set_statuscode(TopoStatusCode::TOPO_OK);
    ListPoolZoneResponse listPoolZoneResponse;
    listPoolZoneResponse.set_statuscode(TopoStatusCode::TOPO_OK);
    ListZoneServerResponse listZoneServerResponse;
    listZoneServerResponse.set_statuscode(TopoStatusCode::TOPO_OK);

    // make create respons
    CreatePoolResponse createPoolResponse;
    createPoolResponse.set_statuscode(TopoStatusCode::TOPO_INTERNAL_ERROR);

    // make expected call
    EXPECT_CALL(mockTopologyService_, ListPool(_, _, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(listPoolResponse),
                        Invoke(RpcService<ListPoolRequest, ListPoolResponse>)));
    EXPECT_CALL(mockTopologyService_, CreatePool(_, _, _, _))
        .WillOnce(
            DoAll(SetArgPointee<2>(createPoolResponse),
                  Invoke(RpcService<CreatePoolRequest, CreatePoolResponse>)));
    ASSERT_EQ(0, tool_.InitTopoData());
    ASSERT_EQ(1, tool_.HandleBuildCluster());
}

TEST_F(BuildTopologyToolTest, test_CreateZoneFailed) {
    // make list response
    ListPoolResponse listPoolResponse;
    listPoolResponse.set_statuscode(TopoStatusCode::TOPO_OK);
    ListPoolZoneResponse listPoolZoneResponse;
    listPoolZoneResponse.set_statuscode(TopoStatusCode::TOPO_OK);
    ListZoneServerResponse listZoneServerResponse;
    listZoneServerResponse.set_statuscode(TopoStatusCode::TOPO_OK);
    ListMetaServerResponse listMetaServerResponse;
    listMetaServerResponse.set_statuscode(TopoStatusCode::TOPO_OK);

    // make create respons
    CreatePoolResponse createPoolResponse;
    createPoolResponse.set_statuscode(TopoStatusCode::TOPO_OK);
    CreateZoneResponse createZoneResponse;
    createZoneResponse.set_statuscode(TopoStatusCode::TOPO_INTERNAL_ERROR);

    // make expected call
    EXPECT_CALL(mockTopologyService_, ListPool(_, _, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(listPoolResponse),
                        Invoke(RpcService<ListPoolRequest, ListPoolResponse>)));
    EXPECT_CALL(mockTopologyService_, CreatePool(_, _, _, _))
        .WillOnce(
            DoAll(SetArgPointee<2>(createPoolResponse),
                  Invoke(RpcService<CreatePoolRequest, CreatePoolResponse>)));
    EXPECT_CALL(mockTopologyService_, CreateZone(_, _, _, _))
        .WillOnce(
            DoAll(SetArgPointee<2>(createZoneResponse),
                  Invoke(RpcService<CreateZoneRequest, CreateZoneResponse>)));
    ASSERT_EQ(0, tool_.InitTopoData());
    ASSERT_EQ(1, tool_.HandleBuildCluster());
}

TEST_F(BuildTopologyToolTest, test_ServerRegisterFailed) {
    // make list response
    ListPoolResponse listPoolResponse;
    listPoolResponse.set_statuscode(TopoStatusCode::TOPO_OK);
    ListPoolZoneResponse listPoolZoneResponse;
    listPoolZoneResponse.set_statuscode(TopoStatusCode::TOPO_OK);
    ListZoneServerResponse listZoneServerResponse;
    listZoneServerResponse.set_statuscode(TopoStatusCode::TOPO_OK);
    ListMetaServerResponse listMetaServerResponse;
    listMetaServerResponse.set_statuscode(TopoStatusCode::TOPO_OK);

    // make create respons
    CreatePoolResponse createPoolResponse;
    createPoolResponse.set_statuscode(TopoStatusCode::TOPO_OK);
    CreateZoneResponse createZoneResponse;
    createZoneResponse.set_statuscode(TopoStatusCode::TOPO_OK);
    ServerRegistResponse serverRegistResponse;
    serverRegistResponse.set_statuscode(TopoStatusCode::TOPO_INTERNAL_ERROR);

    // make expected call
    EXPECT_CALL(mockTopologyService_, ListPool(_, _, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(listPoolResponse),
                        Invoke(RpcService<ListPoolRequest, ListPoolResponse>)));
    EXPECT_CALL(mockTopologyService_, CreatePool(_, _, _, _))
        .WillOnce(
            DoAll(SetArgPointee<2>(createPoolResponse),
                  Invoke(RpcService<CreatePoolRequest, CreatePoolResponse>)));
    EXPECT_CALL(mockTopologyService_, CreateZone(_, _, _, _))
        .Times(3)
        .WillRepeatedly(
            DoAll(SetArgPointee<2>(createZoneResponse),
                  Invoke(RpcService<CreateZoneRequest, CreateZoneResponse>)));
    EXPECT_CALL(mockTopologyService_, RegistServer(_, _, _, _))
        .WillOnce(DoAll(
            SetArgPointee<2>(serverRegistResponse),
            Invoke(RpcService<ServerRegistRequest, ServerRegistResponse>)));
    ASSERT_EQ(0, tool_.InitTopoData());
    ASSERT_EQ(1, tool_.HandleBuildCluster());
}

}  // namespace topology
}  // namespace mds
}  // namespace curvefs
