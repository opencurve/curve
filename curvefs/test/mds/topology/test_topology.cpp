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

#include <gtest/gtest.h>

#include "curvefs/test/mds/mock/mock_topology.h"
#include "curvefs/src/mds/topology/topology.h"
#include "curvefs/src/mds/topology/topology_item.h"
#include "src/common/configuration.h"

namespace curvefs {
namespace mds {
namespace topology {

using ::testing::Return;
using ::testing::_;
using ::testing::AnyOf;
using ::testing::Contains;
using ::testing::SetArgPointee;
using ::curve::common::Configuration;

class TestTopology : public ::testing::Test {
 protected:
    TestTopology() {}
    ~TestTopology() {}

    virtual void SetUp() {
        idGenerator_ = std::make_shared<MockIdGenerator>();
        tokenGenerator_ = std::make_shared<MockTokenGenerator>();
        storage_ = std::make_shared<MockStorage>();
        topology_ = std::make_shared<TopologyImpl>(idGenerator_,
                                                   tokenGenerator_,
                                                   storage_);
    }

    virtual void TearDown() {
        idGenerator_ = nullptr;
        tokenGenerator_ = nullptr;
        storage_ = nullptr;
        topology_ = nullptr;
    }

 protected:
    void PrepareAddPool(PoolIdType id = 0x11,
            const std::string &name = "testPool",
            const Pool::RedundanceAndPlaceMentPolicy &rap =
                Pool::RedundanceAndPlaceMentPolicy(),
            uint64_t createTime = 0x888
            ) {
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
           uint32_t externalPort = 0,
           ZoneIdType zoneId = 0x21,
           PoolIdType poolId = 0x11) {
        Server server(id, hostName, internalHostIp, internalPort,
                      externalHostIp, externalPort, zoneId, poolId);
        EXPECT_CALL(*storage_, StorageServer(_)).WillOnce(Return(true));

        int ret = topology_->AddServer(server);
        ASSERT_EQ(TopoStatusCode::TOPO_OK, ret)
            << "should have PrepareAddZone()";
    }

    void PrepareAddMetaServer(MetaServerIdType id = 0x41,
                const std::string &hostname = "testMetaserver",
                const std::string &token = "testToken",
                ServerIdType serverId = 0x31,
                const std::string &hostIp = "testInternalIp",
                uint32_t port = 0,
                const std::string &externalHostIp = "testExternalIp",
                uint32_t externalPort = 0,
                OnlineState onlineState = OnlineState::OFFLINE) {
            MetaServer ms(id, hostname, token, serverId, hostIp, port,
                          externalHostIp, externalPort,
                          onlineState);

            EXPECT_CALL(*storage_, StorageMetaServer(_))
                .WillOnce(Return(true));
        int ret = topology_->AddMetaServer(ms);
        ASSERT_EQ(TopoStatusCode::TOPO_OK, ret)
            << "should have PrepareAddServer()";
    }

    void PrepareAddCopySet(CopySetIdType copysetId,
        PoolIdType poolId,
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
                            uint64_t idStart, uint64_t idEnd) {
        Partition partition(fsId, poolId, csId, pId, idStart, idEnd);
        EXPECT_CALL(*storage_, StoragePartition(_)).WillOnce(Return(true));
        int ret = topology_->AddPartition(partition);
        ASSERT_EQ(TopoStatusCode::TOPO_OK, ret)
            << "should have PrepareAddPartition()";
    }

 protected:
    std::shared_ptr<MockIdGenerator> idGenerator_;
    std::shared_ptr<MockTokenGenerator> tokenGenerator_;
    std::shared_ptr<MockStorage> storage_;
    std::shared_ptr<TopologyImpl> topology_;
    std::shared_ptr<Configuration> conf_;
};

TEST_F(TestTopology, test_init_success) {
    std::vector<ClusterInformation> infos;
    EXPECT_CALL(*storage_, LoadClusterInfo(_))
        .WillOnce(DoAll(SetArgPointee<0>(infos), Return(true)));

    EXPECT_CALL(*storage_, StorageClusterInfo(_)).WillOnce(Return(true));

    std::unordered_map<PoolIdType, Pool> poolMap_;
    std::unordered_map<ZoneIdType, Zone> zoneMap_;
    std::unordered_map<ServerIdType, Server> serverMap_;
    std::unordered_map<MetaServerIdType, MetaServer> metaServerMap_;
    std::map<CopySetKey, CopySetInfo> copySetMap_;
    std::unordered_map<PartitionIdType, Partition> partitionMap_;

    poolMap_[0x11] = Pool(0x11, "pool",
        Pool::RedundanceAndPlaceMentPolicy(), 0);
    zoneMap_[0x21] = Zone(0x21, "zone1", 0x11);
    serverMap_[0x31] = Server(0x31, "server", "127.0.0.1", 8080,
                                "127.0.0.1", 8080, 0x21, 0x11);
    metaServerMap_[0x41] = MetaServer(0x41, "metaserver", "token",
        0x31, "127.0.0.1", 8200, "127.0.0.1", 8080,
        OnlineState::OFFLINE);
    copySetMap_[std::pair<PoolIdType, CopySetIdType>(0x01, 0x51)] =
        CopySetInfo(0x01, 0x51);
    partitionMap_[0x61] = Partition(0x01, 0x11, 0x51, 0x61, 0, 100);

    EXPECT_CALL(*storage_, LoadPool(_, _)).WillOnce(
        DoAll(SetArgPointee<0>(poolMap_), Return(true)));
    EXPECT_CALL(*storage_, LoadZone(_, _)).WillOnce(
        DoAll(SetArgPointee<0>(zoneMap_), Return(true)));
    EXPECT_CALL(*storage_, LoadServer(_, _)).WillOnce(
        DoAll(SetArgPointee<0>(serverMap_), Return(true)));
    EXPECT_CALL(*storage_, LoadMetaServer(_, _)).WillOnce(
        DoAll(SetArgPointee<0>(metaServerMap_), Return(true)));
    EXPECT_CALL(*storage_, LoadCopySet(_, _)).WillOnce(
        DoAll(SetArgPointee<0>(copySetMap_), Return(true)));
    EXPECT_CALL(*storage_, LoadPartition(_, _)).WillOnce(
        DoAll(SetArgPointee<0>(partitionMap_), Return(true)));

    EXPECT_CALL(*idGenerator_, initPoolIdGenerator(_));
    EXPECT_CALL(*idGenerator_, initZoneIdGenerator(_));
    EXPECT_CALL(*idGenerator_, initServerIdGenerator(_));
    EXPECT_CALL(*idGenerator_, initMetaServerIdGenerator(_));
    EXPECT_CALL(*idGenerator_, initCopySetIdGenerator(_));
    EXPECT_CALL(*idGenerator_, initPartitionIdGenerator(_));

    TopologyOption option;
    int ret = topology_->Init(option);
    ASSERT_EQ(TopoStatusCode::TOPO_OK, ret);
}

TEST_F(TestTopology, test_init_loadClusterFail) {
    std::vector<ClusterInformation> infos;
    EXPECT_CALL(*storage_, LoadClusterInfo(_))
        .WillOnce(DoAll(SetArgPointee<0>(infos),
                Return(false)));

    TopologyOption option;
    int ret = topology_->Init(option);
    ASSERT_EQ(TopoStatusCode::TOPO_STORGE_FAIL, ret);
}

TEST_F(TestTopology, test_init_StorageClusterInfoFail) {
    std::vector<ClusterInformation> infos;
    EXPECT_CALL(*storage_, LoadClusterInfo(_))
        .WillOnce(DoAll(SetArgPointee<0>(infos),
                Return(true)));

    EXPECT_CALL(*storage_, StorageClusterInfo(_))
        .WillOnce(Return(false));

    TopologyOption option;
    int ret = topology_->Init(option);
    ASSERT_EQ(TopoStatusCode::TOPO_STORGE_FAIL, ret);
}

TEST_F(TestTopology, test_init_loadPoolFail) {
    std::vector<ClusterInformation> infos;
    ClusterInformation info("uuid1");
    infos.push_back(info);
    EXPECT_CALL(*storage_, LoadClusterInfo(_))
        .WillOnce(DoAll(SetArgPointee<0>(infos),
                Return(true)));

    EXPECT_CALL(*storage_, LoadPool(_, _))
        .WillOnce(Return(false));

    TopologyOption option;
    int ret = topology_->Init(option);
    ASSERT_EQ(TopoStatusCode::TOPO_STORGE_FAIL, ret);
}

TEST_F(TestTopology, test_init_LoadZoneFail) {
    std::vector<ClusterInformation> infos;
    ClusterInformation info("uuid1");
    infos.push_back(info);
    EXPECT_CALL(*storage_, LoadClusterInfo(_))
        .WillOnce(DoAll(SetArgPointee<0>(infos),
                Return(true)));

    EXPECT_CALL(*storage_, LoadPool(_, _))
        .WillOnce(Return(true));
    EXPECT_CALL(*storage_, LoadZone(_, _))
        .WillOnce(Return(false));
    EXPECT_CALL(*idGenerator_, initPoolIdGenerator(_));

    TopologyOption option;
    int ret = topology_->Init(option);
    ASSERT_EQ(TopoStatusCode::TOPO_STORGE_FAIL, ret);
}

TEST_F(TestTopology, test_init_LoadServerFail) {
    std::vector<ClusterInformation> infos;
    ClusterInformation info("uuid1");
    infos.push_back(info);
    EXPECT_CALL(*storage_, LoadClusterInfo(_))
        .WillOnce(DoAll(SetArgPointee<0>(infos),
                Return(true)));

    EXPECT_CALL(*storage_, LoadPool(_, _))
        .WillOnce(Return(true));
    EXPECT_CALL(*storage_, LoadZone(_, _))
        .WillOnce(Return(true));
    EXPECT_CALL(*storage_, LoadServer(_, _))
        .WillOnce(Return(false));

    EXPECT_CALL(*idGenerator_, initPoolIdGenerator(_));
    EXPECT_CALL(*idGenerator_, initZoneIdGenerator(_));

    TopologyOption option;
    int ret = topology_->Init(option);
    ASSERT_EQ(TopoStatusCode::TOPO_STORGE_FAIL, ret);
}

TEST_F(TestTopology, test_init_LoadMetaServerFail) {
    std::vector<ClusterInformation> infos;
    ClusterInformation info("uuid1");
    infos.push_back(info);
    EXPECT_CALL(*storage_, LoadClusterInfo(_))
        .WillOnce(DoAll(SetArgPointee<0>(infos),
                Return(true)));

    EXPECT_CALL(*storage_, LoadPool(_, _))
        .WillOnce(Return(true));
    EXPECT_CALL(*storage_, LoadZone(_, _))
        .WillOnce(Return(true));
    EXPECT_CALL(*storage_, LoadServer(_, _))
        .WillOnce(Return(true));
    EXPECT_CALL(*storage_, LoadMetaServer(_, _))
        .WillOnce(Return(false));

    EXPECT_CALL(*idGenerator_, initPoolIdGenerator(_));
    EXPECT_CALL(*idGenerator_, initZoneIdGenerator(_));
    EXPECT_CALL(*idGenerator_, initServerIdGenerator(_));

    TopologyOption option;
    int ret = topology_->Init(option);
    ASSERT_EQ(TopoStatusCode::TOPO_STORGE_FAIL, ret);
}

TEST_F(TestTopology, test_init_LoadCopysetFail) {
    std::vector<ClusterInformation> infos;
    ClusterInformation info("uuid1");
    infos.push_back(info);
    EXPECT_CALL(*storage_, LoadClusterInfo(_))
        .WillOnce(DoAll(SetArgPointee<0>(infos),
                Return(true)));

    EXPECT_CALL(*storage_, LoadPool(_, _))
        .WillOnce(Return(true));
    EXPECT_CALL(*storage_, LoadZone(_, _))
        .WillOnce(Return(true));
    EXPECT_CALL(*storage_, LoadServer(_, _))
        .WillOnce(Return(true));
    EXPECT_CALL(*storage_, LoadMetaServer(_, _))
        .WillOnce(Return(true));
    EXPECT_CALL(*storage_, LoadCopySet(_, _))
        .WillOnce(Return(false));

    EXPECT_CALL(*idGenerator_, initPoolIdGenerator(_));
    EXPECT_CALL(*idGenerator_, initZoneIdGenerator(_));
    EXPECT_CALL(*idGenerator_, initServerIdGenerator(_));
    EXPECT_CALL(*idGenerator_, initMetaServerIdGenerator(_));

    TopologyOption option;
    int ret = topology_->Init(option);
    ASSERT_EQ(TopoStatusCode::TOPO_STORGE_FAIL, ret);
}

TEST_F(TestTopology, test_init_LoadPartitionFail) {
    std::vector<ClusterInformation> infos;
    ClusterInformation info("uuid1");
    infos.push_back(info);
    EXPECT_CALL(*storage_, LoadClusterInfo(_))
        .WillOnce(DoAll(SetArgPointee<0>(infos),
                Return(true)));

    EXPECT_CALL(*storage_, LoadPool(_, _))
        .WillOnce(Return(true));
    EXPECT_CALL(*storage_, LoadZone(_, _))
        .WillOnce(Return(true));
    EXPECT_CALL(*storage_, LoadServer(_, _))
        .WillOnce(Return(true));
    EXPECT_CALL(*storage_, LoadMetaServer(_, _))
        .WillOnce(Return(true));
    EXPECT_CALL(*storage_, LoadCopySet(_, _))
        .WillOnce(Return(true));
    EXPECT_CALL(*storage_, LoadPartition(_, _))
        .WillOnce(Return(false));

    EXPECT_CALL(*idGenerator_, initPoolIdGenerator(_));
    EXPECT_CALL(*idGenerator_, initZoneIdGenerator(_));
    EXPECT_CALL(*idGenerator_, initServerIdGenerator(_));
    EXPECT_CALL(*idGenerator_, initMetaServerIdGenerator(_));
    EXPECT_CALL(*idGenerator_, initCopySetIdGenerator(_));

    TopologyOption option;
    int ret = topology_->Init(option);
    ASSERT_EQ(TopoStatusCode::TOPO_STORGE_FAIL, ret);
}

TEST_F(TestTopology, test_AddPool_success) {
    Pool pool(0x01,
            "test1",
            Pool::RedundanceAndPlaceMentPolicy(),
            0);

    EXPECT_CALL(*storage_, StoragePool(_))
        .WillOnce(Return(true));

    int ret = topology_->AddPool(pool);

    ASSERT_EQ(TopoStatusCode::TOPO_OK, ret);
}

TEST_F(TestTopology, test_AddPool_IdDuplicated) {
    PoolIdType id = 0x01;
    PrepareAddPool(id, "test1");

    Pool pool(id,
            "test2",
            Pool::RedundanceAndPlaceMentPolicy(),
            0);

    int ret = topology_->AddPool(pool);

    ASSERT_EQ(TopoStatusCode::TOPO_ID_DUPLICATED, ret);
}

TEST_F(TestTopology, test_AddPool_StorageFail) {
    Pool pool(0x01, "test1", Pool::RedundanceAndPlaceMentPolicy(), 0);

    EXPECT_CALL(*storage_, StoragePool(_))
        .WillOnce(Return(false));

    int ret = topology_->AddPool(pool);

    ASSERT_EQ(TopoStatusCode::TOPO_STORGE_FAIL, ret);
}

TEST_F(TestTopology, test_AddZone_success) {
    PoolIdType poolId = 0x11;
    ZoneIdType zoneId = 0x21;
    PrepareAddPool(poolId);

    Zone zone(zoneId, "testZone", poolId);

    EXPECT_CALL(*storage_, StorageZone(_))
        .WillOnce(Return(true));

    int ret = topology_->AddZone(zone);

    ASSERT_EQ(TopoStatusCode::TOPO_OK, ret);
    Pool pool;
    topology_->GetPool(poolId, &pool);

    std::list<ZoneIdType> zonelist = pool.GetZoneList();

    auto it = std::find(zonelist.begin(), zonelist.end(), zoneId);
    ASSERT_TRUE(it != zonelist.end());
}

TEST_F(TestTopology, test_AddZone_IdDuplicated) {
    PoolIdType poolId = 0x11;
    ZoneIdType zoneId = 0x21;
    PrepareAddPool(poolId);
    PrepareAddZone(zoneId, "test", poolId);
    Zone zone(zoneId, "testZone", poolId);

    int ret = topology_->AddZone(zone);

    ASSERT_EQ(TopoStatusCode::TOPO_ID_DUPLICATED, ret);
}

TEST_F(TestTopology, test_AddZone_StorageFail) {
    PoolIdType poolId = 0x11;
    PrepareAddPool(poolId);

    Zone zone(0x21, "testZone", poolId);

    EXPECT_CALL(*storage_, StorageZone(_))
        .WillOnce(Return(false));

    int ret = topology_->AddZone(zone);

    ASSERT_EQ(TopoStatusCode::TOPO_STORGE_FAIL, ret);
}

TEST_F(TestTopology, test_AddZone_PoolNotFound) {
    PoolIdType poolId = 0x11;
    ZoneIdType zoneId = 0x21;

    Zone zone(zoneId, "testZone", poolId);
    int ret = topology_->AddZone(zone);

    ASSERT_EQ(TopoStatusCode::TOPO_POOL_NOT_FOUND, ret);
}

TEST_F(TestTopology, test_AddServer_success) {
    ServerIdType id = 0x31;
    PoolIdType poolId = 0x11;
    ZoneIdType zoneId = 0x21;
    PrepareAddPool(poolId);
    PrepareAddZone(zoneId, "test", poolId);

    EXPECT_CALL(*storage_, StorageServer(_))
        .WillOnce(Return(true));

    Server server(id,
           "server1",
           "ip1",
           0,
           "ip2",
           0,
           zoneId,
           poolId);

    int ret = topology_->AddServer(server);
    ASSERT_EQ(TopoStatusCode::TOPO_OK, ret);

    Zone zone;
    topology_->GetZone(zoneId, &zone);
    std::list<ServerIdType> serverlist = zone.GetServerList();
    auto it = std::find(serverlist.begin(), serverlist.end(), id);
    ASSERT_TRUE(it != serverlist.end());
}

TEST_F(TestTopology, test_AddServer_IdDuplicated) {
    ServerIdType id = 0x31;
    PoolIdType poolId = 0x11;
    ZoneIdType zoneId = 0x21;
    PrepareAddPool(poolId);
    PrepareAddZone(zoneId, "test", poolId);
    PrepareAddServer(id);

    Server server(id,
           "server1",
           "ip1",
           0,
           "ip2",
           0,
           zoneId,
           poolId);

    int ret = topology_->AddServer(server);
    ASSERT_EQ(TopoStatusCode::TOPO_ID_DUPLICATED, ret);
}

TEST_F(TestTopology, test_AddServer_StorageFail) {
    ServerIdType id = 0x31;
    PoolIdType poolId = 0x11;
    ZoneIdType zoneId = 0x21;
    PrepareAddPool(poolId);
    PrepareAddZone(zoneId, "test", poolId);

    EXPECT_CALL(*storage_, StorageServer(_))
        .WillOnce(Return(false));

    Server server(id,
           "server1",
           "ip1",
           0,
           "ip2",
           0,
           zoneId,
           poolId);

    int ret = topology_->AddServer(server);
    ASSERT_EQ(TopoStatusCode::TOPO_STORGE_FAIL, ret);
}

TEST_F(TestTopology, test_AddServer_ZoneNotFound) {
    ServerIdType id = 0x31;
    PoolIdType poolId = 0x11;
    ZoneIdType zoneId = 0x21;
    Server server(id,
           "server1",
           "ip1",
           0,
           "ip2",
           0,
           zoneId,
           poolId);

    int ret = topology_->AddServer(server);
    ASSERT_EQ(TopoStatusCode::TOPO_ZONE_NOT_FOUND, ret);
}

TEST_F(TestTopology, test_AddMetaServers_success) {
    MetaServerIdType csId = 0x41;
    ServerIdType serverId = 0x31;

    PrepareAddPool();
    PrepareAddZone();
    PrepareAddServer(serverId);

    MetaServer cs(csId,
            "metaserver",
            "token",
            serverId,
            "ip1",
            100,
            "ip2",
            100);

    EXPECT_CALL(*storage_, StorageMetaServer(_))
        .WillOnce(Return(true));

    int ret = topology_->AddMetaServer(cs);

    ASSERT_EQ(TopoStatusCode::TOPO_OK, ret);

    Server server;
    ASSERT_TRUE(topology_->GetServer(serverId, &server));
    std::list<MetaServerIdType> csList = server.GetMetaServerList();

    auto it = std::find(csList.begin(), csList.end(), csId);
    ASSERT_TRUE(it != csList.end());
}

TEST_F(TestTopology, test_AddMetaServer_IdDuplicated) {
    MetaServerIdType csId = 0x41;
    ServerIdType serverId = 0x31;

    PrepareAddPool();
    PrepareAddZone();
    PrepareAddServer(serverId);
    PrepareAddMetaServer(csId,
            "metaserver1",
            "token2",
            serverId);

    MetaServer cs(csId,
            "metaserver2",
            "token",
            serverId,
            "ip1",
            100,
            "ip2",
            200);

    int ret = topology_->AddMetaServer(cs);
    ASSERT_EQ(TopoStatusCode::TOPO_ID_DUPLICATED, ret);
}

TEST_F(TestTopology, test_AddMetaServer_StorageFail) {
    MetaServerIdType csId = 0x41;
    ServerIdType serverId = 0x31;

    PrepareAddPool();
    PrepareAddZone();
    PrepareAddServer(serverId);

    MetaServer cs(csId,
            "metaserver",
            "token",
            serverId,
            "ip1",
            100,
            "ip2",
            100);

    EXPECT_CALL(*storage_, StorageMetaServer(_))
        .WillOnce(Return(false));

    int ret = topology_->AddMetaServer(cs);
    ASSERT_EQ(TopoStatusCode::TOPO_STORGE_FAIL, ret);
}

TEST_F(TestTopology, test_AddMetaServer_ServerNotFound) {
    MetaServerIdType csId = 0x41;
    ServerIdType serverId = 0x31;

    MetaServer cs(csId,
            "metaserver",
            "token",
            serverId,
            "ip1",
            100,
            "ip2",
            100);

    int ret = topology_->AddMetaServer(cs);
    ASSERT_EQ(TopoStatusCode::TOPO_SERVER_NOT_FOUND, ret);
}

TEST_F(TestTopology, test_AddPartition_success) {
    PrepareAddPool();  // poolid=0x11
    PrepareAddZone();  // zoneid=0x21
    PrepareAddServer();  // serverid=0x31
    PrepareAddCopySet(0x41, 0x11, {});

    Partition partition(0x01, 0x11, 0x41, 0x51, 1, 100);

    EXPECT_CALL(*storage_, StoragePartition(_))
        .WillOnce(Return(true));

    int ret = topology_->AddPartition(partition);

    ASSERT_EQ(TopoStatusCode::TOPO_OK, ret);

    CopySetInfo copyset;
    CopySetKey key(0x11, 0x41);
    ASSERT_TRUE(topology_->GetCopySet(key, &copyset));
    ASSERT_EQ(1, copyset.GetPartitionNum());
}

TEST_F(TestTopology, test_AddPartition_IdDuplicated) {
    PrepareAddPool();  // poolid=0x11
    PrepareAddZone();  // zoneid=0x21
    PrepareAddServer();  // serverid=0x31
    PrepareAddCopySet(0x41, 0x11, {});
    PrepareAddPartition(0x01, 0x11, 0x41, 0x51, 1, 100);

    Partition partition(0x01, 0x11, 0x41, 0x51, 101, 200);

    int ret = topology_->AddPartition(partition);
    ASSERT_EQ(TopoStatusCode::TOPO_ID_DUPLICATED, ret);
}

TEST_F(TestTopology, test_AddPartition_StorageFail) {
    PrepareAddPool();  // poolid=0x11
    PrepareAddZone();  // zoneid=0x21
    PrepareAddServer();  // serverid=0x31
    PrepareAddCopySet(0x41, 0x11, {});

    Partition partition(0x01, 0x11, 0x41, 0x51, 1, 100);

    EXPECT_CALL(*storage_, StoragePartition(_))
        .WillOnce(Return(false));

    int ret = topology_->AddPartition(partition);
    ASSERT_EQ(TopoStatusCode::TOPO_STORGE_FAIL, ret);
}

TEST_F(TestTopology, test_AddPartition_CopysetNotFound) {
    PrepareAddPool();  // poolid=0x11
    PrepareAddZone();  // zoneid=0x21
    PrepareAddServer();  // serverid=0x31

    Partition partition(0x01, 0x11, 0x41, 0x51, 1, 100);

    int ret = topology_->AddPartition(partition);
    ASSERT_EQ(TopoStatusCode::TOPO_COPYSET_NOT_FOUND, ret);
}

TEST_F(TestTopology, test_RemovePool_success) {
    PoolIdType id = 0x01;
    PrepareAddPool(id, "name");

    EXPECT_CALL(*storage_, DeletePool(_))
        .WillOnce(Return(true));

    int ret = topology_->RemovePool(id);

    ASSERT_EQ(TopoStatusCode::TOPO_OK, ret);
}

TEST_F(TestTopology, test_RemovePool_PoolNotFound) {
    PoolIdType id = 0x01;

    int ret = topology_->RemovePool(id);

    ASSERT_EQ(TopoStatusCode::TOPO_POOL_NOT_FOUND, ret);
}

TEST_F(TestTopology, test_RemovePool_StorageFail) {
    PoolIdType id = 0x01;
    PrepareAddPool(id, "name");

    EXPECT_CALL(*storage_, DeletePool(_))
        .WillOnce(Return(false));

    int ret = topology_->RemovePool(id);

    ASSERT_EQ(TopoStatusCode::TOPO_STORGE_FAIL, ret);
}

TEST_F(TestTopology, test_RemoveZone_success) {
    ZoneIdType zoneId = 0x21;
    PoolIdType poolId = 0x11;
    PrepareAddPool(poolId);
    PrepareAddZone(zoneId,
            "testZone",
            poolId);

    EXPECT_CALL(*storage_, DeleteZone(_))
        .WillOnce(Return(true));

    int ret = topology_->RemoveZone(zoneId);
    ASSERT_EQ(TopoStatusCode::TOPO_OK, ret);

    Pool pool;
    topology_->GetPool(poolId, &pool);
    std::list<ZoneIdType> zoneList = pool.GetZoneList();
    auto it = std::find(zoneList.begin(), zoneList.end(), zoneId);
    ASSERT_TRUE(it == zoneList.end());
}

TEST_F(TestTopology, test_RemoveZone_ZoneNotFound) {
    ZoneIdType zoneId = 0x21;

    int ret = topology_->RemoveZone(zoneId);
    ASSERT_EQ(TopoStatusCode::TOPO_ZONE_NOT_FOUND, ret);
}

TEST_F(TestTopology, test_RemoveZone_StorageFail) {
    ZoneIdType zoneId = 0x21;
    PrepareAddPool();
    PrepareAddZone(zoneId);

    EXPECT_CALL(*storage_, DeleteZone(_))
        .WillOnce(Return(false));

    int ret = topology_->RemoveZone(zoneId);
    ASSERT_EQ(TopoStatusCode::TOPO_STORGE_FAIL, ret);
}

TEST_F(TestTopology, test_RemoveServer_success) {
    ServerIdType serverId = 0x31;
    ZoneIdType zoneId = 0x21;
    PrepareAddPool();
    PrepareAddZone(zoneId);
    PrepareAddServer(serverId,
            "testSever",
            "ip1",
            0,
            "ip2",
            0,
            zoneId);

    EXPECT_CALL(*storage_, DeleteServer(_))
        .WillOnce(Return(true));

    int ret = topology_->RemoveServer(serverId);
    ASSERT_EQ(TopoStatusCode::TOPO_OK, ret);

    Zone zone;
    topology_->GetZone(zoneId, &zone);
    std::list<ServerIdType> serverList = zone.GetServerList();
    auto it = std::find(serverList.begin(), serverList.end(), serverId);

    ASSERT_TRUE(it == serverList.end());
}

TEST_F(TestTopology, test_RemoveSever_ServerNotFound) {
    ServerIdType serverId = 0x31;

    int ret = topology_->RemoveServer(serverId);
    ASSERT_EQ(TopoStatusCode::TOPO_SERVER_NOT_FOUND, ret);
}

TEST_F(TestTopology, test_RemoveServer_StorageFail) {
    ServerIdType serverId = 0x31;
    ZoneIdType zoneId = 0x21;
    PrepareAddPool();
    PrepareAddZone(zoneId);
    PrepareAddServer(serverId,
            "testSever",
            "ip1",
            0,
            "ip2",
            0,
            zoneId);

    EXPECT_CALL(*storage_, DeleteServer(_))
        .WillOnce(Return(false));

    int ret = topology_->RemoveServer(serverId);
    ASSERT_EQ(TopoStatusCode::TOPO_STORGE_FAIL, ret);
}

TEST_F(TestTopology, test_RemoveMetaServer_success) {
    MetaServerIdType csId = 0x41;
    ServerIdType serverId = 0x31;

    PrepareAddPool();
    PrepareAddZone();
    PrepareAddServer(serverId);
    PrepareAddMetaServer(csId,
            "metaserver",
            "token",
            serverId);

    EXPECT_CALL(*storage_, DeleteMetaServer(_))
        .WillOnce(Return(true));

    int ret = topology_->RemoveMetaServer(csId);
    ASSERT_EQ(TopoStatusCode::TOPO_OK, ret);

    Server server;
    topology_->GetServer(serverId, &server);
    std::list<MetaServerIdType> csList = server.GetMetaServerList();
    auto it = std::find(csList.begin(), csList.end(), serverId);
    ASSERT_TRUE(it == csList.end());
}

TEST_F(TestTopology, test_RemoveMetaServer_MetaSeverNotFound) {
    MetaServerIdType csId = 0x41;

    int ret = topology_->RemoveMetaServer(csId);
    ASSERT_EQ(TopoStatusCode::TOPO_METASERVER_NOT_FOUND, ret);
}

TEST_F(TestTopology, test_RemoveMetaServer_StorageFail) {
    MetaServerIdType csId = 0x41;
    ServerIdType serverId = 0x31;

    PrepareAddPool();
    PrepareAddZone();
    PrepareAddServer(serverId);
    PrepareAddMetaServer(csId,
            "metaserver",
            "token",
            serverId);

    EXPECT_CALL(*storage_, DeleteMetaServer(_))
        .WillOnce(Return(false));

    int ret = topology_->RemoveMetaServer(csId);
    ASSERT_EQ(TopoStatusCode::TOPO_STORGE_FAIL, ret);
}

TEST_F(TestTopology, test_RemovePartition_success) {
    PoolIdType poolId = 0x11;
    CopySetIdType csId = 0x41;
    PartitionIdType pId = 0x51;

    PrepareAddPool(poolId);
    PrepareAddZone();
    PrepareAddServer();
    PrepareAddMetaServer();
    PrepareAddCopySet(csId, poolId, {});
    PrepareAddPartition(0x01, poolId, csId, pId, 1, 100);


    EXPECT_CALL(*storage_, DeletePartition(_))
        .WillOnce(Return(true));

    int ret = topology_->RemovePartition(pId);
    ASSERT_EQ(TopoStatusCode::TOPO_OK, ret);

    CopySetInfo cs;
    CopySetKey key(poolId, csId);
    topology_->GetCopySet(key, &cs);
    ASSERT_EQ(0, cs.GetPartitionNum());
}

TEST_F(TestTopology, test_RemovePartition_PartitionNotFound) {
    PrepareAddPool();
    PartitionIdType pId = 0x51;

    int ret = topology_->RemovePartition(pId);
    ASSERT_EQ(TopoStatusCode::TOPO_PARTITION_NOT_FOUND, ret);
}

TEST_F(TestTopology, test_RemovePartition_StorageFail) {
PoolIdType poolId = 0x11;
    CopySetIdType csId = 0x41;
    PartitionIdType pId = 0x51;

    PrepareAddPool(poolId);
    PrepareAddZone();
    PrepareAddServer();
    PrepareAddMetaServer();
    PrepareAddCopySet(csId, poolId, {});
    PrepareAddPartition(0x01, poolId, csId, pId, 1, 100);

    EXPECT_CALL(*storage_, DeleteMetaServer(_))
        .WillOnce(Return(false));

    int ret = topology_->RemoveMetaServer(csId);
    ASSERT_EQ(TopoStatusCode::TOPO_STORGE_FAIL, ret);
}

TEST_F(TestTopology, UpdatePool_success) {
    PoolIdType poolId = 0x01;
    PrepareAddPool(poolId,
            "name1",
            Pool::RedundanceAndPlaceMentPolicy(),
            0);

    Pool pool(poolId,
            "name1",
            Pool::RedundanceAndPlaceMentPolicy(),
            1);

    EXPECT_CALL(*storage_, UpdatePool(_))
        .WillOnce(Return(true));

    int ret = topology_->UpdatePool(pool);

    ASSERT_EQ(TopoStatusCode::TOPO_OK, ret);

    Pool pool2;
    topology_->GetPool(poolId, &pool2);
    ASSERT_EQ(1, pool2.GetCreateTime());
}

TEST_F(TestTopology, UpdatePool_PoolNotFound) {
    PoolIdType poolId = 0x11;
    Pool pool(poolId,
            "name1",
            Pool::RedundanceAndPlaceMentPolicy(),
            0);

    int ret = topology_->UpdatePool(pool);

    ASSERT_EQ(TopoStatusCode::TOPO_POOL_NOT_FOUND, ret);
}

TEST_F(TestTopology, UpdatePool_StorageFail) {
    PoolIdType poolId = 0x11;
    PrepareAddPool(poolId,
            "name1",
            Pool::RedundanceAndPlaceMentPolicy(),
            0);

    Pool pool(poolId,
            "name1",
            Pool::RedundanceAndPlaceMentPolicy(),
            0);

    EXPECT_CALL(*storage_, UpdatePool(_))
        .WillOnce(Return(false));

    int ret = topology_->UpdatePool(pool);

    ASSERT_EQ(TopoStatusCode::TOPO_STORGE_FAIL, ret);
}

TEST_F(TestTopology, UpdateZone_success) {
    ZoneIdType zoneId = 0x21;
    PoolIdType poolId = 0x11;
    PrepareAddPool(poolId);
    PrepareAddZone(zoneId,
            "name1",
            poolId);

    Zone newZone(zoneId,
            "name1",
            poolId);

    EXPECT_CALL(*storage_, UpdateZone(_))
        .WillOnce(Return(true));
    int ret = topology_->UpdateZone(newZone);
    ASSERT_EQ(TopoStatusCode::TOPO_OK, ret);
}

TEST_F(TestTopology, UpdateZone_ZoneNotFound) {
    ZoneIdType zoneId = 0x21;
    PoolIdType poolId = 0x11;

    Zone newZone(zoneId,
            "name1",
            poolId);

    int ret = topology_->UpdateZone(newZone);
    ASSERT_EQ(TopoStatusCode::TOPO_ZONE_NOT_FOUND, ret);
}

TEST_F(TestTopology, UpdateZone_StorageFail) {
    ZoneIdType zoneId = 0x21;
    PoolIdType poolId = 0x11;
    PrepareAddPool(poolId);
    PrepareAddZone(zoneId,
            "name1",
            poolId);

    Zone newZone(zoneId,
            "name1",
            poolId);

    EXPECT_CALL(*storage_, UpdateZone(_))
        .WillOnce(Return(false));
    int ret = topology_->UpdateZone(newZone);
    ASSERT_EQ(TopoStatusCode::TOPO_STORGE_FAIL, ret);
}

TEST_F(TestTopology, UpdateServer_success) {
    PoolIdType poolId = 0x11;
    ZoneIdType zoneId = 0x21;
    ServerIdType serverId = 0x31;
    PrepareAddPool(poolId);
    PrepareAddZone(zoneId);
    PrepareAddServer(serverId,
            "name1",
            "ip1",
            0,
            "ip2",
            0,
            zoneId,
            poolId);

    Server newServer(serverId,
            "name1",
            "ip1",
            0,
            "ip2",
            0,
            zoneId,
            poolId);

    EXPECT_CALL(*storage_, UpdateServer(_))
        .WillOnce(Return(true));

    int ret = topology_->UpdateServer(newServer);
    ASSERT_EQ(TopoStatusCode::TOPO_OK, ret);
}

TEST_F(TestTopology, UpdateServer_ServerNotFound) {
    PoolIdType poolId = 0x11;
    ZoneIdType zoneId = 0x21;
    ServerIdType serverId = 0x31;

    Server newServer(serverId,
            "name1",
            "ip1",
            0,
            "ip2",
            0,
            zoneId,
            poolId);

    int ret = topology_->UpdateServer(newServer);
    ASSERT_EQ(TopoStatusCode::TOPO_SERVER_NOT_FOUND, ret);
}

TEST_F(TestTopology, UpdateServer_StorageFail) {
    PoolIdType poolId = 0x11;
    ZoneIdType zoneId = 0x21;
    ServerIdType serverId = 0x31;
    PrepareAddPool(poolId);
    PrepareAddZone(zoneId);
    PrepareAddServer(serverId,
            "name1",
            "ip1",
            0,
            "ip2",
            0,
            zoneId,
            poolId);

    Server newServer(serverId,
            "name1",
            "ip1",
            0,
            "ip2",
            0,
            zoneId,
            poolId);

    EXPECT_CALL(*storage_, UpdateServer(_))
        .WillOnce(Return(false));

    int ret = topology_->UpdateServer(newServer);
    ASSERT_EQ(TopoStatusCode::TOPO_STORGE_FAIL, ret);
}

TEST_F(TestTopology, UpdateMetaServerOnlineState_success) {
    PoolIdType poolId = 0x11;
    ZoneIdType zoneId = 0x21;
    ServerIdType serverId = 0x31;
    MetaServerIdType csId = 0x41;
    PrepareAddPool(poolId);
    PrepareAddZone(zoneId);
    PrepareAddServer(serverId);
    PrepareAddMetaServer(csId,
            "metaserver",
            "token",
            serverId,
            "ip1",
            100,
            "ip2",
            100);

    int ret = topology_->UpdateMetaServerOnlineState(OnlineState::ONLINE, csId);
    ASSERT_EQ(TopoStatusCode::TOPO_OK, ret);
}

TEST_F(TestTopology, UpdateMetaServerTopo_MetaServerNotFound) {
    MetaServerIdType csId = 0x41;

    int ret = topology_->UpdateMetaServerOnlineState(OnlineState::ONLINE, csId);
    ASSERT_EQ(TopoStatusCode::TOPO_METASERVER_NOT_FOUND, ret);
}

TEST_F(TestTopology, UpdatePartition_success) {
    PoolIdType poolId = 0x11;
    ZoneIdType zoneId = 0x21;
    ServerIdType serverId = 0x31;
    MetaServerIdType msId = 0x41;
    CopySetIdType csId = 0x51;
    PartitionIdType pId = 0x61;
    PrepareAddPool(poolId);
    PrepareAddZone(zoneId);
    PrepareAddServer(serverId);
    PrepareAddMetaServer(msId,
            "metaserver",
            "token",
            serverId);
    PrepareAddCopySet(csId, poolId, {});
    PrepareAddPartition(0x01, poolId, csId, pId, 1, 100);

    Partition partition(0x01, poolId, csId, pId, 101, 200);

    EXPECT_CALL(*storage_, UpdatePartition(_))
        .WillOnce(Return(true));
    int ret = topology_->UpdatePartition(partition);
    ASSERT_EQ(TopoStatusCode::TOPO_OK, ret);
}

TEST_F(TestTopology, UpdatePartitionTxIds_success) {
    PoolIdType poolId = 0x11;
    ZoneIdType zoneId = 0x21;
    ServerIdType serverId = 0x31;
    MetaServerIdType msId = 0x41;
    CopySetIdType csId = 0x51;
    PartitionIdType pId = 0x61;
    PrepareAddPool(poolId);
    PrepareAddZone(zoneId);
    PrepareAddServer(serverId);
    PrepareAddMetaServer(msId,
            "metaserver",
            "token",
            serverId);
    PrepareAddCopySet(csId, poolId, {});
    PrepareAddPartition(0x01, poolId, csId, pId, 1, 100);

    std::vector<PartitionTxId> data;
    PartitionTxId ptx;
    ptx.set_partitionid(pId);
    ptx.set_txid(1);
    data.emplace_back(ptx);

    EXPECT_CALL(*storage_, UpdatePartitions(_))
        .WillOnce(Return(true));
    int ret = topology_->UpdatePartitionTxIds(data);
    ASSERT_EQ(TopoStatusCode::TOPO_OK, ret);
}

TEST_F(TestTopology, FindPool_success) {
    PoolIdType poolId = 0x01;
    std::string poolName = "pool1";
    PrepareAddPool(poolId, poolName);
    PoolIdType ret = topology_->FindPool(poolName);
    ASSERT_EQ(poolId, ret);
}

TEST_F(TestTopology, FindPool_PoolNotFound) {
    std::string poolName = "pool1";
    PoolIdType ret = topology_->FindPool(poolName);

    ASSERT_EQ(static_cast<PoolIdType>(UNINITIALIZE_ID),
                                      ret);
}

TEST_F(TestTopology, FindZone_success) {
    PoolIdType poolId = 0x11;
    std::string poolName = "poolName";
    ZoneIdType zoneId = 0x21;
    std::string zoneName = "zoneName";
    PrepareAddPool(poolId, poolName);
    PrepareAddZone(zoneId, zoneName);
    ZoneIdType ret = topology_->FindZone(zoneName, poolName);
    ASSERT_EQ(zoneId, ret);
}

TEST_F(TestTopology, FindZone_ZoneNotFound) {
    std::string poolName = "poolName";
    std::string zoneName = "zoneName";
    ZoneIdType ret = topology_->FindZone(zoneName, poolName);
    ASSERT_EQ(static_cast<ZoneIdType>(UNINITIALIZE_ID),
                                      ret);
}

TEST_F(TestTopology, FindZone_success2) {
    PoolIdType poolId = 0x11;
    std::string poolName = "poolName";
    ZoneIdType zoneId = 0x21;
    std::string zoneName = "zoneName";
    PrepareAddPool(poolId, poolName);
    PrepareAddZone(zoneId, zoneName);
    ZoneIdType ret = topology_->FindZone(zoneName, poolId);
    ASSERT_EQ(zoneId, ret);
}

TEST_F(TestTopology, FindZone_ZoneNotFound2) {
    PoolIdType poolId = 0x11;
    std::string poolName = "poolName";
    std::string zoneName = "zoneName";
    ZoneIdType ret = topology_->FindZone(zoneName, poolId);
    ASSERT_EQ(static_cast<ZoneIdType>(UNINITIALIZE_ID),
                                      ret);
}

TEST_F(TestTopology, FindServerByHostName_success) {
    ServerIdType serverId = 0x31;
    std::string hostName = "host1";
    PrepareAddPool();
    PrepareAddZone();
    PrepareAddServer(serverId, hostName);

    ServerIdType ret = topology_->FindServerByHostName(hostName);
    ASSERT_EQ(serverId, ret);
}

TEST_F(TestTopology, FindServerByHostName_ServerNotFound) {
    std::string hostName = "host1";
    ServerIdType ret = topology_->FindServerByHostName(hostName);
    ASSERT_EQ(static_cast<ServerIdType>(UNINITIALIZE_ID),
                                        ret);
}

TEST_F(TestTopology, FindServerByHostIpPort_success) {
    ServerIdType serverId = 0x31;
    std::string hostName = "host1";
    std::string internalHostIp = "ip1";
    std::string externalHostIp = "ip2";
    PrepareAddPool();
    PrepareAddZone();
    PrepareAddServer(serverId,
            hostName,
            internalHostIp,
            0,
            externalHostIp,
            0);

    ServerIdType ret = topology_->FindServerByHostIpPort(internalHostIp, 0);
    ASSERT_EQ(serverId, ret);

    ServerIdType ret2 = topology_->FindServerByHostIpPort(externalHostIp, 0);
    ASSERT_EQ(serverId, ret2);
}

TEST_F(TestTopology, FindSeverByHostIp_ServerNotFound) {
    ServerIdType serverId = 0x31;
    std::string hostName = "host1";
    std::string internalHostIp = "ip1";
    std::string externalHostIp = "ip2";
    PrepareAddPool();
    PrepareAddZone();
    PrepareAddServer(serverId,
            hostName,
            internalHostIp,
            0,
            externalHostIp,
            0);

    ServerIdType ret = topology_->FindServerByHostIpPort("ip3", 0);
    ASSERT_EQ(static_cast<ServerIdType>(UNINITIALIZE_ID),
                                        ret);
}

TEST_F(TestTopology, GetPool_success) {
    PoolIdType poolId = 0x11;
    PrepareAddPool(poolId);
    Pool pool;
    bool ret = topology_->GetPool(poolId, &pool);
    ASSERT_EQ(true, ret);
}

TEST_F(TestTopology, GetPool_PoolNotFound) {
    PoolIdType poolId = 0x01;
    Pool pool;
    bool ret = topology_->GetPool(poolId, &pool);
    ASSERT_EQ(false, ret);
}

TEST_F(TestTopology, GetZone_success) {
    PoolIdType poolId = 0x11;
    ZoneIdType zoneId = 0x21;
    PrepareAddPool(poolId);
    PrepareAddZone(zoneId);
    Zone zone;
    bool ret = topology_->GetZone(zoneId, &zone);
    ASSERT_EQ(true, ret);
}

TEST_F(TestTopology, GetZone_ZoneNotFound) {
    ZoneIdType zoneId = 0x21;
    Zone zone;
    bool ret = topology_->GetZone(zoneId, &zone);
    ASSERT_EQ(false, ret);
}

TEST_F(TestTopology, GetServer_success) {
    PoolIdType poolId = 0x11;
    ZoneIdType zoneId = 0x21;
    ServerIdType serverId = 0x31;
    PrepareAddPool(poolId);
    PrepareAddZone(zoneId);
    PrepareAddServer(serverId);
    Server server;
    bool ret = topology_->GetServer(serverId, &server);
    ASSERT_EQ(true, ret);
}


TEST_F(TestTopology, GetServer_GetServerNotFound) {
    PoolIdType poolId = 0x11;
    ZoneIdType zoneId = 0x21;
    ServerIdType serverId = 0x31;
    PrepareAddPool(poolId);
    PrepareAddZone(zoneId);
    PrepareAddServer(serverId);
    Server server;
    bool ret = topology_->GetServer(serverId + 1, &server);
    ASSERT_EQ(false, ret);
}

TEST_F(TestTopology, GetMetaServer_success) {
    PoolIdType poolId = 0x11;
    ZoneIdType zoneId = 0x21;
    ServerIdType serverId = 0x31;
    MetaServerIdType csId = 0x41;
    PrepareAddPool(poolId);
    PrepareAddZone(zoneId);
    PrepareAddServer(serverId);
    PrepareAddMetaServer(csId);
    MetaServer metaserver;
    bool ret = topology_->GetMetaServer(csId, &metaserver);
    ASSERT_EQ(true, ret);
}

TEST_F(TestTopology, GetMetaServer_MetaServerNotFound) {
    PoolIdType poolId = 0x11;
    ZoneIdType zoneId = 0x21;
    ServerIdType serverId = 0x31;
    MetaServerIdType csId = 0x41;
    PrepareAddPool(poolId);
    PrepareAddZone(zoneId);
    PrepareAddServer(serverId);
    PrepareAddMetaServer(csId);
    MetaServer metaserver;
    bool ret = topology_->GetMetaServer(csId + 1, &metaserver);
    ASSERT_EQ(false, ret);
}

TEST_F(TestTopology, GetMetaServerInCluster_success) {
    PoolIdType poolId = 0x11;
    ZoneIdType zoneId = 0x21;
    ServerIdType serverId = 0x31;
    MetaServerIdType csId = 0x41;
    MetaServerIdType csId2 = 0x42;

    PrepareAddPool(poolId);
    PrepareAddZone(zoneId);
    PrepareAddServer(serverId);
    PrepareAddMetaServer(csId);
    PrepareAddMetaServer(csId2);

    auto csList = topology_->GetMetaServerInCluster();
    ASSERT_THAT(csList, Contains(csId));
    ASSERT_THAT(csList, Contains(csId2));
}

TEST_F(TestTopology, GetServerInCluster_success) {
    PoolIdType poolId = 0x11;
    ZoneIdType zoneId = 0x21;
    ServerIdType serverId = 0x31;
    ServerIdType serverId2 = 0x32;

    PrepareAddPool(poolId);
    PrepareAddZone(zoneId);
    PrepareAddServer(serverId);
    PrepareAddServer(serverId2);

    auto serverList = topology_->GetServerInCluster();
    ASSERT_THAT(serverList, Contains(serverId));
    ASSERT_THAT(serverList, Contains(serverId2));
}

TEST_F(TestTopology, GetZoneInCluster_success) {
    PoolIdType poolId = 0x11;
    ZoneIdType zoneId = 0x21;
    ZoneIdType zoneId2 = 0x22;

    PrepareAddPool(poolId);
    PrepareAddZone(zoneId);
    PrepareAddZone(zoneId2);

    auto zoneList = topology_->GetZoneInCluster();
    ASSERT_THAT(zoneList, Contains(zoneId));
    ASSERT_THAT(zoneList, Contains(zoneId2));
}

TEST_F(TestTopology, GetPoolInCluster_success) {
    PoolIdType poolId = 0x11;
    PoolIdType poolId2 = 0x02;

    PrepareAddPool(poolId, "name");
    PrepareAddPool(poolId2, "name2");

    auto poolList = topology_->GetPoolInCluster();
    ASSERT_THAT(poolList, Contains(poolId));
    ASSERT_THAT(poolList, Contains(poolId2));
}

TEST_F(TestTopology, GetMetaServerInServer_success) {
    PoolIdType poolId = 0x11;
    ZoneIdType zoneId = 0x21;
    ServerIdType serverId = 0x31;
    MetaServerIdType csId = 0x41;
    MetaServerIdType csId2 = 0x42;

    PrepareAddPool(poolId);
    PrepareAddZone(zoneId);
    PrepareAddServer(serverId);
    PrepareAddMetaServer(csId);
    PrepareAddMetaServer(csId2);

    std::list<MetaServerIdType> csList =
        topology_->GetMetaServerInServer(serverId);
    ASSERT_THAT(csList, Contains(csId));
    ASSERT_THAT(csList, Contains(csId2));
}

TEST_F(TestTopology, GetMetaServerInServer_empty) {
    ServerIdType serverId = 0x31;
    std::list<MetaServerIdType> csList =
        topology_->GetMetaServerInServer(serverId);
    ASSERT_EQ(0, csList.size());
}

TEST_F(TestTopology, GetMetaServerInZone_success) {
    PoolIdType poolId = 0x11;
    ZoneIdType zoneId = 0x21;
    ServerIdType serverId = 0x31;
    MetaServerIdType csId = 0x41;
    MetaServerIdType csId2 = 0x42;

    PrepareAddPool(poolId);
    PrepareAddZone(zoneId);
    PrepareAddServer(serverId);
    PrepareAddMetaServer(csId);
    PrepareAddMetaServer(csId2);

    std::list<MetaServerIdType> csList =
        topology_->GetMetaServerInZone(zoneId);
    ASSERT_THAT(csList, Contains(csId));
    ASSERT_THAT(csList, Contains(csId2));
}

TEST_F(TestTopology, GetServerInZone_success) {
    PoolIdType poolId = 0x11;
    ZoneIdType zoneId = 0x21;
    ServerIdType serverId = 0x31;
    ServerIdType serverId2 = 0x32;

    PrepareAddPool(poolId);
    PrepareAddZone(zoneId);
    PrepareAddServer(serverId);
    PrepareAddServer(serverId2);

    std::list<ServerIdType> serverList = topology_->GetServerInZone(zoneId);
    ASSERT_THAT(serverList, Contains(serverId));
    ASSERT_THAT(serverList, Contains(serverId2));
}

TEST_F(TestTopology, GetServerInZone_empty) {
    ZoneIdType zoneId = 0x21;
    std::list<ServerIdType> serverList = topology_->GetServerInZone(zoneId);
    ASSERT_EQ(0, serverList.size());
}

TEST_F(TestTopology, GetZoneInPool_success) {
    PoolIdType poolId = 0x11;
    ZoneIdType zoneId = 0x21;
    ZoneIdType zoneId2 = 0x22;

    PrepareAddPool(poolId);
    PrepareAddZone(zoneId);
    PrepareAddZone(zoneId2);

    std::list<ZoneIdType> zoneList =
        topology_->GetZoneInPool(poolId);
    ASSERT_THAT(zoneList, Contains(zoneId));
    ASSERT_THAT(zoneList, Contains(zoneId2));
}

TEST_F(TestTopology, GetZoneInPool_empty) {
    PoolIdType poolId = 0x11;
    std::list<ZoneIdType> zoneList =
        topology_->GetZoneInPool(poolId);
    ASSERT_EQ(0, zoneList.size());
}

TEST_F(TestTopology, GetPartition_success) {
    PoolIdType poolId = 0x11;
    ZoneIdType zoneId = 0x21;
    ServerIdType serverId = 0x31;
    MetaServerIdType msId = 0x41;
    CopySetIdType csId = 0x51;
    PartitionIdType pId = 0x61;

    PrepareAddPool(poolId);
    PrepareAddZone(zoneId);
    PrepareAddServer(serverId);
    PrepareAddMetaServer(msId);
    PrepareAddCopySet(csId, poolId, {});
    PrepareAddPartition(0x01, poolId, csId, pId, 1, 100);

    Partition data;
    topology_->GetPartition(pId, &data);
    ASSERT_EQ(pId, data.GetPartitionId());
}

TEST_F(TestTopology, GetPartitionOfFs_success) {
    PoolIdType poolId = 0x11;
    ZoneIdType zoneId = 0x21;
    ServerIdType serverId = 0x31;
    MetaServerIdType msId = 0x41;
    CopySetIdType csId = 0x51;
    PartitionIdType pId = 0x61;

    PrepareAddPool(poolId);
    PrepareAddZone(zoneId);
    PrepareAddServer(serverId);
    PrepareAddMetaServer(msId);
    PrepareAddCopySet(csId, poolId, {});
    PrepareAddPartition(0x01, poolId, csId, pId, 1, 100);
    PrepareAddPartition(0x01, poolId, csId, pId+1, 1, 100);
    PrepareAddPartition(0x02, poolId, csId, pId+2, 1, 100);

    Partition data;
    topology_->GetPartition(pId, &data);
    ASSERT_EQ(pId, data.GetPartitionId());

    std::list<Partition> partitionList =
        topology_->GetPartitionOfFs(0x01);
    ASSERT_EQ(2, partitionList.size());
    for (auto item : partitionList) {
        ASSERT_THAT(item.GetPartitionId(), AnyOf(pId, pId + 1));
    }

    std::list<Partition> partitionList1 =
        topology_->GetPartitionInfosInCopyset(csId);
    ASSERT_EQ(3, partitionList1.size());
    for (auto item : partitionList1) {
        ASSERT_THAT(item.GetPartitionId(), AnyOf(pId, pId + 1, pId + 2));
    }
}

TEST_F(TestTopology, GetAvailableCopyset_success) {
    PoolIdType poolId = 0x11;
    ZoneIdType zoneId = 0x21;
    ServerIdType serverId = 0x31;
    MetaServerIdType msId = 0x41;
    CopySetIdType csId = 0x51;
    PartitionIdType pId = 0x61;

    PrepareAddPool(poolId);
    PrepareAddZone(zoneId);
    PrepareAddServer(serverId);
    PrepareAddMetaServer(msId);
    PrepareAddCopySet(csId, poolId, {});

    CopySetInfo data;
    CopySetKey key(poolId, csId);
    bool ret = topology_->GetAvailableCopyset(&data);
    ASSERT_EQ(true, ret);

    data.SetPartitionNum(256);
    ASSERT_EQ(TopoStatusCode::TOPO_OK, topology_->UpdateCopySetTopo(data));
    ret = topology_->GetAvailableCopyset(&data);
    ASSERT_EQ(false, ret);
}


TEST_F(TestTopology, AddCopySet_success) {
    PoolIdType poolId = 0x11;
    CopySetIdType copysetId = 0x51;

    PrepareAddPool(poolId);
    PrepareAddZone(0x21, "zone1", poolId);
    PrepareAddZone(0x22, "zone2", poolId);
    PrepareAddZone(0x23, "zone3", poolId);
    PrepareAddServer(
        0x31, "server1", "127.0.0.1", 0, "127.0.0.1", 0, 0x21, 0x11);
    PrepareAddServer(
        0x32, "server2", "127.0.0.1", 0, "127.0.0.1", 0, 0x22, 0x11);
    PrepareAddServer(
        0x33, "server3", "127.0.0.1", 0, "127.0.0.1", 0, 0x23, 0x11);
    PrepareAddMetaServer(0x41, "metaserver1", "token1", 0x31,
                        "127.0.0.1", 8200, "127.0.0.1", 8200);
    PrepareAddMetaServer(0x42, "metaserver2", "token2", 0x32,
                        "127.0.0.1", 8200, "127.0.0.1", 8200);
    PrepareAddMetaServer(0x43, "metaserver3", "token3", 0x33,
                        "127.0.0.1", 8200, "127.0.0.1", 8200);

    std::set<MetaServerIdType> replicas;
    replicas.insert(0x41);
    replicas.insert(0x42);
    replicas.insert(0x43);

    CopySetInfo csInfo(poolId, copysetId);
    csInfo.SetCopySetMembers(replicas);

    EXPECT_CALL(*storage_, StorageCopySet(_))
        .WillOnce(Return(true));
    int ret = topology_->AddCopySet(csInfo);
    ASSERT_EQ(TopoStatusCode::TOPO_OK, ret);
}

TEST_F(TestTopology, AddCopySet_IdDuplicated) {
    PoolIdType poolId = 0x11;
    CopySetIdType copysetId = 0x51;

    PrepareAddPool(poolId);
    PrepareAddZone(0x21, "zone1", poolId);
    PrepareAddZone(0x22, "zone2", poolId);
    PrepareAddZone(0x23, "zone3", poolId);
    PrepareAddServer(
        0x31, "server1", "127.0.0.1", 0, "127.0.0.1", 0, 0x21, 0x11);
    PrepareAddServer(
        0x32, "server2", "127.0.0.1", 0, "127.0.0.1", 0, 0x22, 0x11);
    PrepareAddServer(
        0x33, "server3", "127.0.0.1", 0, "127.0.0.1", 0, 0x23, 0x11);
    PrepareAddMetaServer(0x41, "metaserver1", "token1", 0x31,
                        "127.0.0.1", 8200, "127.0.0.1", 8200);
    PrepareAddMetaServer(0x42, "metaserver2", "token2", 0x32,
                        "127.0.0.1", 8200, "127.0.0.1", 8200);
    PrepareAddMetaServer(0x43, "metaserver3", "token3", 0x33,
                        "127.0.0.1", 8200, "127.0.0.1", 8200);

    std::set<MetaServerIdType> replicas;
    replicas.insert(0x41);
    replicas.insert(0x42);
    replicas.insert(0x43);
    PrepareAddCopySet(copysetId, poolId, replicas);

    CopySetInfo csInfo(poolId, copysetId);
    csInfo.SetCopySetMembers(replicas);

    int ret = topology_->AddCopySet(csInfo);
    ASSERT_EQ(TopoStatusCode::TOPO_ID_DUPLICATED, ret);
}

TEST_F(TestTopology, AddCopySet_PoolNotFound) {
    PoolIdType poolId = 0x11;
    CopySetIdType copysetId = 0x51;

    PrepareAddPool(poolId);
    PrepareAddZone(0x21, "zone1", poolId);
    PrepareAddZone(0x22, "zone2", poolId);
    PrepareAddZone(0x23, "zone3", poolId);
    PrepareAddServer(
        0x31, "server1", "127.0.0.1", 0, "127.0.0.1", 0, 0x21, 0x11);
    PrepareAddServer(
        0x32, "server2", "127.0.0.1", 0, "127.0.0.1", 0, 0x22, 0x11);
    PrepareAddServer(
        0x33, "server3", "127.0.0.1", 0, "127.0.0.1", 0, 0x23, 0x11);
    PrepareAddMetaServer(0x41, "metaserver1", "token1", 0x31,
                        "127.0.0.1", 8200, "127.0.0.1", 8200);
    PrepareAddMetaServer(0x42, "metaserver2", "token2", 0x32,
                        "127.0.0.1", 8200, "127.0.0.1", 8200);
    PrepareAddMetaServer(0x43, "metaserver3", "token3", 0x33,
                        "127.0.0.1", 8200, "127.0.0.1", 8200);

    std::set<MetaServerIdType> replicas;
    replicas.insert(0x41);
    replicas.insert(0x42);
    replicas.insert(0x43);

    CopySetInfo csInfo(++poolId, copysetId);
    csInfo.SetCopySetMembers(replicas);

    int ret = topology_->AddCopySet(csInfo);
    ASSERT_EQ(TopoStatusCode::TOPO_POOL_NOT_FOUND, ret);
}

TEST_F(TestTopology, AddCopySet_StorageFail) {
    PoolIdType poolId = 0x11;
    CopySetIdType copysetId = 0x51;

    PrepareAddPool(poolId);
    PrepareAddZone(0x21, "zone1", poolId);
    PrepareAddZone(0x22, "zone2", poolId);
    PrepareAddZone(0x23, "zone3", poolId);
    PrepareAddServer(
        0x31, "server1", "127.0.0.1", 0, "127.0.0.1", 0, 0x21, 0x11);
    PrepareAddServer(
        0x32, "server2", "127.0.0.1", 0, "127.0.0.1", 0, 0x22, 0x11);
    PrepareAddServer(
        0x33, "server3", "127.0.0.1", 0, "127.0.0.1", 0, 0x23, 0x11);
    PrepareAddMetaServer(0x41, "metaserver1", "token1", 0x31,
                        "127.0.0.1", 8200, "127.0.0.1", 8200);
    PrepareAddMetaServer(0x42, "metaserver2", "token2", 0x32,
                        "127.0.0.1", 8200, "127.0.0.1", 8200);
    PrepareAddMetaServer(0x43, "metaserver3", "token3", 0x33,
                        "127.0.0.1", 8200, "127.0.0.1", 8200);

    std::set<MetaServerIdType> replicas;
    replicas.insert(0x41);
    replicas.insert(0x42);
    replicas.insert(0x43);

    CopySetInfo csInfo(poolId, copysetId);
    csInfo.SetCopySetMembers(replicas);

    EXPECT_CALL(*storage_, StorageCopySet(_))
        .WillOnce(Return(false));
    int ret = topology_->AddCopySet(csInfo);
    ASSERT_EQ(TopoStatusCode::TOPO_STORGE_FAIL, ret);
}

TEST_F(TestTopology, CopySetCreating) {
    PoolIdType poolId = 0x11;
    CopySetIdType copysetId = 0x51;

    ASSERT_EQ(TopoStatusCode::TOPO_OK,
        topology_->AddCopySetCreating(CopySetKey(poolId, copysetId)));
    ASSERT_EQ(TopoStatusCode::TOPO_ID_DUPLICATED,
        topology_->AddCopySetCreating(CopySetKey(poolId, copysetId)));

    ASSERT_TRUE(topology_->IsCopysetCreating(CopySetKey(poolId, copysetId)));

    topology_->RemoveCopySetCreating(CopySetKey(poolId, copysetId));
    ASSERT_FALSE(topology_->IsCopysetCreating(CopySetKey(poolId, copysetId)));
}

TEST_F(TestTopology, RemoveCopySet_success) {
    PoolIdType poolId = 0x11;
    CopySetIdType copysetId = 0x51;

    PrepareAddPool(poolId);
    PrepareAddZone(0x21, "zone1", poolId);
    PrepareAddZone(0x22, "zone2", poolId);
    PrepareAddZone(0x23, "zone3", poolId);
    PrepareAddServer(
        0x31, "server1", "127.0.0.1", 0, "127.0.0.1", 0, 0x21, 0x11);
    PrepareAddServer(
        0x32, "server2", "127.0.0.1", 0, "127.0.0.1", 0, 0x22, 0x11);
    PrepareAddServer(
        0x33, "server3", "127.0.0.1", 0, "127.0.0.1", 0, 0x23, 0x11);
    PrepareAddMetaServer(0x41, "metaserver1", "token1", 0x31,
                        "127.0.0.1", 8200, "127.0.0.1", 8200);
    PrepareAddMetaServer(0x42, "metaserver2", "token2", 0x32,
                        "127.0.0.1", 8200, "127.0.0.1", 8200);
    PrepareAddMetaServer(0x43, "metaserver3", "token3", 0x33,
                        "127.0.0.1", 8200, "127.0.0.1", 8200);

    std::set<MetaServerIdType> replicas;
    replicas.insert(0x41);
    replicas.insert(0x42);
    replicas.insert(0x43);
    PrepareAddCopySet(copysetId, poolId, replicas);

    EXPECT_CALL(*storage_, DeleteCopySet(_))
        .WillOnce(Return(true));

    int ret = topology_->RemoveCopySet(
        std::pair<PoolIdType, CopySetIdType>(poolId, copysetId));

    ASSERT_EQ(TopoStatusCode::TOPO_OK, ret);
}

TEST_F(TestTopology, RemoveCopySet_storageFail) {
    PoolIdType poolId = 0x11;
    CopySetIdType copysetId = 0x51;

    PrepareAddPool(poolId);
    PrepareAddZone(0x21, "zone1", poolId);
    PrepareAddZone(0x22, "zone2", poolId);
    PrepareAddZone(0x23, "zone3", poolId);
    PrepareAddServer(
        0x31, "server1", "127.0.0.1" , 0, "127.0.0.1" , 0, 0x21, 0x11);
    PrepareAddServer(
        0x32, "server2", "127.0.0.1" , 0, "127.0.0.1" , 0, 0x22, 0x11);
    PrepareAddServer(
        0x33, "server3", "127.0.0.1" , 0, "127.0.0.1" , 0, 0x23, 0x11);
    PrepareAddMetaServer(0x41, "metaserver1", "token1", 0x31,
                        "127.0.0.1", 8200, "127.0.0.1", 8200);
    PrepareAddMetaServer(0x42, "metaserver2", "token2", 0x32,
                        "127.0.0.1", 8200, "127.0.0.1", 8200);
    PrepareAddMetaServer(0x43, "metaserver3", "token3", 0x33,
                        "127.0.0.1", 8200, "127.0.0.1", 8200);

    std::set<MetaServerIdType> replicas;
    replicas.insert(0x41);
    replicas.insert(0x42);
    replicas.insert(0x43);
    PrepareAddCopySet(copysetId, poolId, replicas);

    EXPECT_CALL(*storage_, DeleteCopySet(_))
        .WillOnce(Return(false));

    int ret = topology_->RemoveCopySet(
        std::pair<PoolIdType, CopySetIdType>(poolId, copysetId));

    ASSERT_EQ(TopoStatusCode::TOPO_STORGE_FAIL, ret);
}

TEST_F(TestTopology, RemoveCopySet_CopySetNotFound) {
    PoolIdType poolId = 0x11;
    CopySetIdType copysetId = 0x51;

    PrepareAddPool(poolId);
    PrepareAddZone(0x21, "zone1", poolId);
    PrepareAddZone(0x22, "zone2", poolId);
    PrepareAddZone(0x23, "zone3", poolId);
    PrepareAddServer(
        0x31, "server1", "127.0.0.1" , 0, "127.0.0.1" , 0, 0x21, 0x11);
    PrepareAddServer(
        0x32, "server2", "127.0.0.1" , 0, "127.0.0.1" , 0, 0x22, 0x11);
    PrepareAddServer(
        0x33, "server3", "127.0.0.1" , 0, "127.0.0.1" , 0, 0x23, 0x11);
    PrepareAddMetaServer(0x41, "metaserver1", "token1", 0x31,
                        "127.0.0.1", 8200, "127.0.0.1", 8200);
    PrepareAddMetaServer(0x42, "metaserver2", "token2", 0x32,
                        "127.0.0.1", 8200, "127.0.0.1", 8200);
    PrepareAddMetaServer(0x43, "metaserver3", "token3", 0x33,
                        "127.0.0.1", 8200, "127.0.0.1", 8200);

    std::set<MetaServerIdType> replicas;
    replicas.insert(0x41);
    replicas.insert(0x42);
    replicas.insert(0x43);
    PrepareAddCopySet(copysetId, poolId, replicas);

    int ret = topology_->RemoveCopySet(
        std::pair<PoolIdType, CopySetIdType>(poolId, ++copysetId));

    ASSERT_EQ(TopoStatusCode::TOPO_COPYSET_NOT_FOUND, ret);
}

TEST_F(TestTopology, UpdateCopySetTopo_success) {
    PoolIdType poolId = 0x11;
    CopySetIdType copysetId = 0x51;

    PrepareAddPool(poolId);
    PrepareAddZone(0x21, "zone1", poolId);
    PrepareAddZone(0x22, "zone2", poolId);
    PrepareAddZone(0x23, "zone3", poolId);
    PrepareAddServer(
        0x31, "server1", "127.0.0.1" , 0, "127.0.0.1" , 0, 0x21, 0x11);
    PrepareAddServer(
        0x32, "server2", "127.0.0.1" , 0, "127.0.0.1" , 0, 0x22, 0x11);
    PrepareAddServer(
        0x33, "server3", "127.0.0.1" , 0, "127.0.0.1" , 0, 0x23, 0x11);
    PrepareAddMetaServer(0x41, "metaserver1", "token1", 0x31,
                        "127.0.0.1", 8200, "127.0.0.1", 8200);
    PrepareAddMetaServer(0x42, "metaserver2", "token2", 0x32,
                        "127.0.0.1", 8200, "127.0.0.1", 8200);
    PrepareAddMetaServer(0x43, "metaserver3", "token3", 0x33,
                        "127.0.0.1", 8200, "127.0.0.1", 8200);
    PrepareAddMetaServer(0x44, "metaserver4", "token4", 0x33,
                        "127.0.0.1", 8200, "127.0.0.1", 8200);

    std::set<MetaServerIdType> replicas;
    replicas.insert(0x41);
    replicas.insert(0x42);
    replicas.insert(0x43);
    PrepareAddCopySet(copysetId, poolId, replicas);

    std::set<MetaServerIdType> replicas2;
    replicas2.insert(0x41);
    replicas2.insert(0x42);
    replicas2.insert(0x44);

    CopySetInfo csInfo(poolId, copysetId);
    csInfo.SetCopySetMembers(replicas2);
    csInfo.SetCandidate(0x45);

    int ret = topology_->UpdateCopySetTopo(csInfo);

    ASSERT_EQ(TopoStatusCode::TOPO_OK, ret);

    EXPECT_CALL(*storage_, UpdateCopySet(_))
        .WillOnce(Return(true));
    topology_->Run();

    sleep(3);
    topology_->Stop();
}

TEST_F(TestTopology, UpdateCopySetTopo_CopySetNotFound) {
    PoolIdType poolId = 0x11;
    CopySetIdType copysetId = 0x51;

    PrepareAddPool(poolId);
    PrepareAddZone(0x21, "zone1", poolId);
    PrepareAddZone(0x22, "zone2", poolId);
    PrepareAddZone(0x23, "zone3", poolId);
    PrepareAddServer(
        0x31, "server1", "127.0.0.1" , 0, "127.0.0.1" , 0, 0x21, 0x11);
    PrepareAddServer(
        0x32, "server2", "127.0.0.1" , 0, "127.0.0.1" , 0, 0x22, 0x11);
    PrepareAddServer(
        0x33, "server3", "127.0.0.1" , 0, "127.0.0.1" , 0, 0x23, 0x11);
    PrepareAddMetaServer(0x41, "metaserver1", "token1", 0x31,
                        "127.0.0.1", 8200, "127.0.0.1", 8200);
    PrepareAddMetaServer(0x42, "metaserver2", "token2", 0x32,
                        "127.0.0.1", 8200, "127.0.0.1", 8200);
    PrepareAddMetaServer(0x43, "metaserver3", "token3", 0x33,
                        "127.0.0.1", 8200, "127.0.0.1", 8200);
    PrepareAddMetaServer(0x44, "metaserver4", "token4", 0x33,
                        "127.0.0.1", 8200, "127.0.0.1", 8200);

    std::set<MetaServerIdType> replicas;
    replicas.insert(0x41);
    replicas.insert(0x42);
    replicas.insert(0x43);
    PrepareAddCopySet(copysetId, poolId, replicas);

    std::set<MetaServerIdType> replicas2;
    replicas.insert(0x41);
    replicas.insert(0x42);
    replicas.insert(0x44);

    CopySetInfo csInfo(poolId, ++copysetId);
    csInfo.SetCopySetMembers(replicas2);

    int ret = topology_->UpdateCopySetTopo(csInfo);

    ASSERT_EQ(TopoStatusCode::TOPO_COPYSET_NOT_FOUND, ret);
}

TEST_F(TestTopology, GetCopySet_success) {
    PoolIdType poolId = 0x11;
    CopySetIdType copysetId = 0x51;

    PrepareAddPool(poolId);
    PrepareAddZone(0x21, "zone1", poolId);
    PrepareAddZone(0x22, "zone2", poolId);
    PrepareAddZone(0x23, "zone3", poolId);
    PrepareAddServer(
        0x31, "server1", "127.0.0.1" , 0, "127.0.0.1" , 0, 0x21, 0x11);
    PrepareAddServer(
        0x32, "server2", "127.0.0.1" , 0, "127.0.0.1" , 0, 0x22, 0x11);
    PrepareAddServer(
        0x33, "server3", "127.0.0.1" , 0, "127.0.0.1" , 0, 0x23, 0x11);
    PrepareAddMetaServer(0x41, "metaserver1", "token1", 0x31,
                        "127.0.0.1", 8200, "127.0.0.1", 8200);
    PrepareAddMetaServer(0x42, "metaserver2", "token2", 0x32,
                        "127.0.0.1", 8200, "127.0.0.1", 8200);
    PrepareAddMetaServer(0x43, "metaserver3", "token3", 0x33,
                        "127.0.0.1", 8200, "127.0.0.1", 8200);

    std::set<MetaServerIdType> replicas;
    replicas.insert(0x41);
    replicas.insert(0x42);
    replicas.insert(0x43);
    PrepareAddCopySet(copysetId, poolId, replicas);

    CopySetInfo copysetInfo;
    int ret = topology_->GetCopySet(
        std::pair<PoolIdType, CopySetIdType>(poolId, copysetId),
        &copysetInfo);

    ASSERT_EQ(true, ret);
}

TEST_F(TestTopology, GetCopySet_CopysetNotFound) {
    PoolIdType poolId = 0x11;
    CopySetIdType copysetId = 0x51;

    PrepareAddPool(poolId);
    PrepareAddZone(0x21, "zone1", poolId);
    PrepareAddZone(0x22, "zone2", poolId);
    PrepareAddZone(0x23, "zone3", poolId);
    PrepareAddServer(
        0x31, "server1", "127.0.0.1" , 0, "127.0.0.1" , 0, 0x21, 0x11);
    PrepareAddServer(
        0x32, "server2", "127.0.0.1" , 0, "127.0.0.1" , 0, 0x22, 0x11);
    PrepareAddServer(
        0x33, "server3", "127.0.0.1" , 0, "127.0.0.1" , 0, 0x23, 0x11);
    PrepareAddMetaServer(0x41, "metaserver1", "token1", 0x31,
                        "127.0.0.1", 8200, "127.0.0.1", 8200);
    PrepareAddMetaServer(0x42, "metaserver2", "token2", 0x32,
                        "127.0.0.1", 8200, "127.0.0.1", 8200);
    PrepareAddMetaServer(0x43, "metaserver3", "token3", 0x33,
                        "127.0.0.1", 8200, "127.0.0.1", 8200);

    std::set<MetaServerIdType> replicas;
    replicas.insert(0x41);
    replicas.insert(0x42);
    replicas.insert(0x43);
    PrepareAddCopySet(copysetId, poolId, replicas);

    CopySetInfo copysetInfo;
    int ret = topology_->GetCopySet(
        std::pair<PoolIdType, CopySetIdType>(poolId, ++copysetId),
        &copysetInfo);

    ASSERT_EQ(false, ret);
}

}  // namespace topology
}  // namespace mds
}  // namespace curvefs
