/*
 * Project: curve
 * Created Date: Tue Sep 25 2018
 * Author: xuchaojie
 * Copyright (c) 2018 netease
 */

#include <gtest/gtest.h>

#include "test/mds/topology/mock_topology.h"
#include "src/mds/topology/topology.h"
#include "src/mds/topology/topology_item.h"
#include "src/common/configuration.h"

namespace curve {
namespace mds {
namespace topology {

using ::testing::Return;
using ::testing::_;
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
        ASSERT_EQ(kTopoErrCodeSuccess, ret)
            << "should have PrepareAddPhysicalPool()";
    }

    void PrepareAddServer(ServerIdType id = 0x31,
           const std::string &hostName = "testServer",
           const std::string &internalHostIp = "testInternalIp",
           uint32_t internalPort = 0,
           const std::string &externalHostIp = "testExternalIp",
           uint32_t externalPort = 0,
           ZoneIdType zoneId = 0x21,
           PoolIdType physicalPoolId = 0x11,
           const std::string &desc = "descServer") {
        Server server(id,
                hostName,
                internalHostIp,
                internalPort,
                externalHostIp,
                externalPort,
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
                uint32_t port = 0,
                const std::string &diskPath = "/") {
            ChunkServer cs(id,
                    token,
                    diskType,
                    serverId,
                    hostIp,
                    port,
                    diskPath);
            EXPECT_CALL(*storage_, StorageChunkServer(_))
                .WillOnce(Return(true));
        int ret = topology_->AddChunkServer(cs);
        ASSERT_EQ(kTopoErrCodeSuccess, ret)
            << "should have PrepareAddServer()";
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
    std::shared_ptr<TopologyImpl> topology_;
    std::shared_ptr<Configuration> conf_;
};

TEST_F(TestTopology, test_init_success) {
    std::unordered_map<PoolIdType, LogicalPool> logicalPoolMap_;
    std::unordered_map<PoolIdType, PhysicalPool> physicalPoolMap_;
    std::unordered_map<ZoneIdType, Zone> zoneMap_;
    std::unordered_map<ServerIdType, Server> serverMap_;
    std::unordered_map<ChunkServerIdType, ChunkServer> chunkServerMap_;
    std::map<CopySetKey, CopySetInfo> copySetMap_;

    logicalPoolMap_[0x01] = LogicalPool();
    physicalPoolMap_[0x11] = PhysicalPool();
    zoneMap_[0x21] = Zone();
    serverMap_[0x31] = Server();
    chunkServerMap_[0x41] = ChunkServer();
    copySetMap_[std::pair<PoolIdType, CopySetIdType>(0x01, 0x51)] =
        CopySetInfo(0x01, 0x51);

    EXPECT_CALL(*storage_, LoadLogicalPool(_, _))
        .WillOnce(DoAll(SetArgPointee<0>(logicalPoolMap_),
                    Return(true)));
    EXPECT_CALL(*storage_, LoadPhysicalPool(_, _))
        .WillOnce(DoAll(SetArgPointee<0>(physicalPoolMap_),
                    Return(true)));
    EXPECT_CALL(*storage_, LoadZone(_, _))
        .WillOnce(DoAll(SetArgPointee<0>(zoneMap_),
                    Return(true)));
    EXPECT_CALL(*storage_, LoadServer(_, _))
        .WillOnce(DoAll(SetArgPointee<0>(serverMap_),
                    Return(true)));
    EXPECT_CALL(*storage_, LoadChunkServer(_, _))
        .WillOnce(DoAll(SetArgPointee<0>(chunkServerMap_),
                    Return(true)));
    EXPECT_CALL(*storage_, LoadCopySet(_, _))
        .WillOnce(DoAll(SetArgPointee<0>(copySetMap_),
                    Return(true)));

    EXPECT_CALL(*idGenerator_, initLogicalPoolIdGenerator(_));
    EXPECT_CALL(*idGenerator_, initPhysicalPoolIdGenerator(_));
    EXPECT_CALL(*idGenerator_, initZoneIdGenerator(_));
    EXPECT_CALL(*idGenerator_, initServerIdGenerator(_));
    EXPECT_CALL(*idGenerator_, initChunkServerIdGenerator(_));
    EXPECT_CALL(*idGenerator_, initCopySetIdGenerator(_));

    EXPECT_CALL(*storage_, DeleteLogicalPool(_))
        .WillOnce(Return(true));
    EXPECT_CALL(*storage_, DeleteCopySet(_))
        .WillOnce(Return(true));

    TopologyOption option;
    int ret = topology_->init(option);
    ASSERT_EQ(kTopoErrCodeSuccess, ret);
}

TEST_F(TestTopology, test_init_loadLogicalPoolFail) {
    EXPECT_CALL(*storage_, LoadLogicalPool(_, _))
        .WillOnce(Return(false));

    TopologyOption option;
    int ret = topology_->init(option);
    ASSERT_EQ(kTopoErrCodeStorgeFail, ret);
}

TEST_F(TestTopology, test_init_LoadPhysicalPoolFail) {
    EXPECT_CALL(*storage_, LoadLogicalPool(_, _))
        .WillOnce(Return(true));
    EXPECT_CALL(*storage_, LoadPhysicalPool(_, _))
        .WillOnce(Return(false));

    EXPECT_CALL(*idGenerator_, initLogicalPoolIdGenerator(_));

    TopologyOption option;
    int ret = topology_->init(option);
    ASSERT_EQ(kTopoErrCodeStorgeFail, ret);
}

TEST_F(TestTopology, test_init_LoadZoneFail) {
    EXPECT_CALL(*storage_, LoadLogicalPool(_, _))
        .WillOnce(Return(true));
    EXPECT_CALL(*storage_, LoadPhysicalPool(_, _))
        .WillOnce(Return(true));
    EXPECT_CALL(*storage_, LoadZone(_, _))
        .WillOnce(Return(false));

    EXPECT_CALL(*idGenerator_, initLogicalPoolIdGenerator(_));
    EXPECT_CALL(*idGenerator_, initPhysicalPoolIdGenerator(_));

    TopologyOption option;
    int ret = topology_->init(option);
    ASSERT_EQ(kTopoErrCodeStorgeFail, ret);
}

TEST_F(TestTopology, test_init_LoadServerFail) {
    EXPECT_CALL(*storage_, LoadLogicalPool(_, _))
        .WillOnce(Return(true));
    EXPECT_CALL(*storage_, LoadPhysicalPool(_, _))
        .WillOnce(Return(true));
    EXPECT_CALL(*storage_, LoadZone(_, _))
        .WillOnce(Return(true));
    EXPECT_CALL(*storage_, LoadServer(_, _))
        .WillOnce(Return(false));

    EXPECT_CALL(*idGenerator_, initLogicalPoolIdGenerator(_));
    EXPECT_CALL(*idGenerator_, initPhysicalPoolIdGenerator(_));
    EXPECT_CALL(*idGenerator_, initZoneIdGenerator(_));

    TopologyOption option;
    int ret = topology_->init(option);
    ASSERT_EQ(kTopoErrCodeStorgeFail, ret);
}

TEST_F(TestTopology, test_init_LoadChunkServerFail) {
    EXPECT_CALL(*storage_, LoadLogicalPool(_, _))
        .WillOnce(Return(true));
    EXPECT_CALL(*storage_, LoadPhysicalPool(_, _))
        .WillOnce(Return(true));
    EXPECT_CALL(*storage_, LoadZone(_, _))
        .WillOnce(Return(true));
    EXPECT_CALL(*storage_, LoadServer(_, _))
        .WillOnce(Return(true));
    EXPECT_CALL(*storage_, LoadChunkServer(_, _))
        .WillOnce(Return(false));

    EXPECT_CALL(*idGenerator_, initLogicalPoolIdGenerator(_));
    EXPECT_CALL(*idGenerator_, initPhysicalPoolIdGenerator(_));
    EXPECT_CALL(*idGenerator_, initZoneIdGenerator(_));
    EXPECT_CALL(*idGenerator_, initServerIdGenerator(_));

    TopologyOption option;
    int ret = topology_->init(option);
    ASSERT_EQ(kTopoErrCodeStorgeFail, ret);
}

TEST_F(TestTopology, test_init_LoadCopysetFail) {
    EXPECT_CALL(*storage_, LoadLogicalPool(_, _))
        .WillOnce(Return(true));
    EXPECT_CALL(*storage_, LoadPhysicalPool(_, _))
        .WillOnce(Return(true));
    EXPECT_CALL(*storage_, LoadZone(_, _))
        .WillOnce(Return(true));
    EXPECT_CALL(*storage_, LoadServer(_, _))
        .WillOnce(Return(true));
    EXPECT_CALL(*storage_, LoadChunkServer(_, _))
        .WillOnce(Return(true));
    EXPECT_CALL(*storage_, LoadCopySet(_, _))
        .WillOnce(Return(false));

    EXPECT_CALL(*idGenerator_, initLogicalPoolIdGenerator(_));
    EXPECT_CALL(*idGenerator_, initPhysicalPoolIdGenerator(_));
    EXPECT_CALL(*idGenerator_, initZoneIdGenerator(_));
    EXPECT_CALL(*idGenerator_, initServerIdGenerator(_));
    EXPECT_CALL(*idGenerator_, initChunkServerIdGenerator(_));

    TopologyOption option;
    int ret = topology_->init(option);
    ASSERT_EQ(kTopoErrCodeStorgeFail, ret);
}

TEST_F(TestTopology, test_AddLogicalPool_success) {
    PoolIdType physicalPoolId = 0x11;
    PrepareAddPhysicalPool(physicalPoolId);
    LogicalPool pool(0x01,
            "test1",
            physicalPoolId,
            PAGEFILE,
            LogicalPool::RedundanceAndPlaceMentPolicy(),
            LogicalPool::UserPolicy(),
            0,
            true);

    EXPECT_CALL(*storage_, StorageLogicalPool(_))
        .WillOnce(Return(true));

    int ret = topology_->AddLogicalPool(pool);

    ASSERT_EQ(kTopoErrCodeSuccess, ret);
}

TEST_F(TestTopology, test_AddLogicalPool_IdDuplicated) {
    PoolIdType physicalPoolId = 0x11;
    PrepareAddPhysicalPool(physicalPoolId);
    PoolIdType id = 0x01;
    PrepareAddLogicalPool(id, "test1", physicalPoolId);

    LogicalPool pool(id,
            "test2",
            physicalPoolId,
            PAGEFILE,
            LogicalPool::RedundanceAndPlaceMentPolicy(),
            LogicalPool::UserPolicy(),
            0,
            true);

    int ret = topology_->AddLogicalPool(pool);

    ASSERT_EQ(kTopoErrCodeIdDuplicated, ret);
}

TEST_F(TestTopology, test_AddLogicalPool_StorageFail) {
    PoolIdType physicalPoolId = 0x11;
    PrepareAddPhysicalPool(physicalPoolId);
    LogicalPool pool(0x01,
            "test1",
            physicalPoolId,
            PAGEFILE,
            LogicalPool::RedundanceAndPlaceMentPolicy(),
            LogicalPool::UserPolicy(),
            0,
            true);

    EXPECT_CALL(*storage_, StorageLogicalPool(_))
        .WillOnce(Return(false));

    int ret = topology_->AddLogicalPool(pool);

    ASSERT_EQ(kTopoErrCodeStorgeFail, ret);
}

TEST_F(TestTopology, test_AddLogicalPool_PhysicalPoolNotFound) {
    PoolIdType physicalPoolId = 0x11;
    PrepareAddPhysicalPool(physicalPoolId);
    LogicalPool pool(0x01,
            "test1",
            ++physicalPoolId,
            PAGEFILE,
            LogicalPool::RedundanceAndPlaceMentPolicy(),
            LogicalPool::UserPolicy(),
            0,
            true);


    int ret = topology_->AddLogicalPool(pool);

    ASSERT_EQ(kTopoErrCodePhysicalPoolNotFound, ret);
}

TEST_F(TestTopology, test_AddPhysicalPool_success) {
    PhysicalPool pool(0x11,
            "test1",
            "desc");
    EXPECT_CALL(*storage_, StoragePhysicalPool(_))
        .WillOnce(Return(true));

    int ret = topology_->AddPhysicalPool(pool);
    ASSERT_EQ(kTopoErrCodeSuccess, ret);
}


TEST_F(TestTopology, test_AddPhysicalPool_IdDuplicated) {
    PoolIdType id = 0x11;

    PhysicalPool pool(id,
            "test1",
            "desc");
    PrepareAddPhysicalPool(id);
    int ret = topology_->AddPhysicalPool(pool);
    ASSERT_EQ(kTopoErrCodeIdDuplicated, ret);
}

TEST_F(TestTopology, test_AddPhysicalPool_StorageFail) {
    PhysicalPool pool(0x11,
            "test1",
            "desc");
    EXPECT_CALL(*storage_, StoragePhysicalPool(_))
        .WillOnce(Return(false));

    int ret = topology_->AddPhysicalPool(pool);
    ASSERT_EQ(kTopoErrCodeStorgeFail, ret);
}

TEST_F(TestTopology, test_AddZone_success) {
    PoolIdType physicalPoolId = 0x11;
    ZoneIdType zoneId = 0x21;
    PrepareAddPhysicalPool(physicalPoolId);

    Zone zone(zoneId,
            "testZone",
            physicalPoolId,
            "desc");

    EXPECT_CALL(*storage_, StorageZone(_))
        .WillOnce(Return(true));

    int ret = topology_->AddZone(zone);

    ASSERT_EQ(kTopoErrCodeSuccess, ret);
    PhysicalPool pool;
    topology_->GetPhysicalPool(physicalPoolId, &pool);

    std::list<ZoneIdType> zonelist = pool.GetZoneList();

    auto it = std::find(zonelist.begin(), zonelist.end(), zoneId);
    ASSERT_TRUE(it != zonelist.end());
}

TEST_F(TestTopology, test_AddZone_IdDuplicated) {
    PoolIdType physicalPoolId = 0x11;
    ZoneIdType zoneId = 0x21;
    PrepareAddPhysicalPool(physicalPoolId);
    PrepareAddZone(zoneId, "test", physicalPoolId);
    Zone zone(zoneId,
            "testZone",
            physicalPoolId,
            "desc");

    int ret = topology_->AddZone(zone);

    ASSERT_EQ(kTopoErrCodeIdDuplicated, ret);
}

TEST_F(TestTopology, test_AddZone_StorageFail) {
    PoolIdType physicalPoolId = 0x11;
    PrepareAddPhysicalPool(physicalPoolId);

    Zone zone(0x21,
            "testZone",
            physicalPoolId,
            "desc");

    EXPECT_CALL(*storage_, StorageZone(_))
        .WillOnce(Return(false));

    int ret = topology_->AddZone(zone);

    ASSERT_EQ(kTopoErrCodeStorgeFail, ret);
}

TEST_F(TestTopology, test_AddZone_PhysicalPoolNotFound) {
    PoolIdType physicalPoolId = 0x11;
    ZoneIdType zoneId = 0x21;

    Zone zone(zoneId,
            "testZone",
            physicalPoolId,
            "desc");


    int ret = topology_->AddZone(zone);

    ASSERT_EQ(kTopoErrCodePhysicalPoolNotFound, ret);
}

TEST_F(TestTopology, test_AddServer_success) {
    ServerIdType id = 0x31;
    PoolIdType physicalPoolId = 0x11;
    ZoneIdType zoneId = 0x21;
    PrepareAddPhysicalPool(physicalPoolId);
    PrepareAddZone(zoneId, "test", physicalPoolId);

    EXPECT_CALL(*storage_, StorageServer(_))
        .WillOnce(Return(true));

    Server server(id,
           "server1",
           "ip1",
           0,
           "ip2",
           0,
           zoneId,
           physicalPoolId,
           "desc");

    int ret = topology_->AddServer(server);
    ASSERT_EQ(kTopoErrCodeSuccess, ret);

    Zone zone;

    topology_->GetZone(zoneId, &zone);
    std::list<ServerIdType> serverlist = zone.GetServerList();
    auto it = std::find(serverlist.begin(), serverlist.end(), id);
    ASSERT_TRUE(it != serverlist.end());
}

TEST_F(TestTopology, test_AddServer_IdDuplicated) {
    ServerIdType id = 0x31;
    PoolIdType physicalPoolId = 0x11;
    ZoneIdType zoneId = 0x21;
    PrepareAddPhysicalPool(physicalPoolId);
    PrepareAddZone(zoneId, "test", physicalPoolId);
    PrepareAddServer(id);

    Server server(id,
           "server1",
           "ip1",
           0,
           "ip2",
           0,
           zoneId,
           physicalPoolId,
           "desc");

    int ret = topology_->AddServer(server);

    ASSERT_EQ(kTopoErrCodeIdDuplicated, ret);
}

TEST_F(TestTopology, test_AddServer_StorageFail) {
    ServerIdType id = 0x31;
    PoolIdType physicalPoolId = 0x11;
    ZoneIdType zoneId = 0x21;
    PrepareAddPhysicalPool(physicalPoolId);
    PrepareAddZone(zoneId, "test", physicalPoolId);

    EXPECT_CALL(*storage_, StorageServer(_))
        .WillOnce(Return(false));

    Server server(id,
           "server1",
           "ip1",
           0,
           "ip2",
           0,
           zoneId,
           physicalPoolId,
           "desc");

    int ret = topology_->AddServer(server);

    ASSERT_EQ(kTopoErrCodeStorgeFail, ret);
}


TEST_F(TestTopology, test_AddServer_ZoneNotFound) {
    ServerIdType id = 0x31;
    PoolIdType physicalPoolId = 0x11;
    ZoneIdType zoneId = 0x21;
    Server server(id,
           "server1",
           "ip1",
           0,
           "ip2",
           0,
           zoneId,
           physicalPoolId,
           "desc");

    int ret = topology_->AddServer(server);

    ASSERT_EQ(kTopoErrCodeZoneNotFound, ret);
}


TEST_F(TestTopology, test_AddChunkServers_success) {
    ChunkServerIdType csId = 0x41;
    ServerIdType serverId = 0x31;

    PrepareAddPhysicalPool();
    PrepareAddZone();
    PrepareAddServer(serverId);

    ChunkServer cs(csId,
            "token",
            "ssd",
            serverId,
            "ip1",
            100,
            "/");

    EXPECT_CALL(*storage_, StorageChunkServer(_))
        .WillOnce(Return(true));

    int ret = topology_->AddChunkServer(cs);

    ASSERT_EQ(kTopoErrCodeSuccess, ret);

    Server server;
    topology_->GetServer(serverId, &server);
    std::list<ChunkServerIdType> csList = server.GetChunkServerList();

    auto it = std::find(csList.begin(), csList.end(), csId);
    ASSERT_TRUE(it != csList.end());
}

TEST_F(TestTopology, test_AddChunkServer_IdDuplicated) {
    ChunkServerIdType csId = 0x41;
    ServerIdType serverId = 0x31;

    PrepareAddPhysicalPool();
    PrepareAddZone();
    PrepareAddServer(serverId);
    PrepareAddChunkServer(csId,
            "token2",
            "ssd",
            serverId);

    ChunkServer cs(csId,
            "token",
            "ssd",
            serverId,
            "ip1",
            100,
            "/");

    int ret = topology_->AddChunkServer(cs);

    ASSERT_EQ(kTopoErrCodeIdDuplicated, ret);
}

TEST_F(TestTopology, test_AddChunkServer_StorageFail) {
    ChunkServerIdType csId = 0x41;
    ServerIdType serverId = 0x31;

    PrepareAddPhysicalPool();
    PrepareAddZone();
    PrepareAddServer(serverId);

    ChunkServer cs(csId,
            "token",
            "ssd",
            serverId,
            "ip1",
            100,
            "/");

    EXPECT_CALL(*storage_, StorageChunkServer(_))
        .WillOnce(Return(false));

    int ret = topology_->AddChunkServer(cs);

    ASSERT_EQ(kTopoErrCodeStorgeFail, ret);
}

TEST_F(TestTopology, test_AddChunkServer_ServerNotFound) {
    ChunkServerIdType csId = 0x41;
    ServerIdType serverId = 0x31;

    ChunkServer cs(csId,
            "token",
            "ssd",
            serverId,
            "ip1",
            100,
            "/");

    int ret = topology_->AddChunkServer(cs);

    ASSERT_EQ(kTopoErrCodeServerNotFound, ret);
}

TEST_F(TestTopology, test_RemoveLogicalPool_success) {
    PoolIdType physicalPoolId = 0x11;
    PrepareAddPhysicalPool(physicalPoolId);
    PoolIdType id = 0x01;
    PrepareAddLogicalPool(id, "name", physicalPoolId);

    EXPECT_CALL(*storage_, DeleteLogicalPool(_))
        .WillOnce(Return(true));

    int ret = topology_->RemoveLogicalPool(id);

    ASSERT_EQ(kTopoErrCodeSuccess, ret);
}

TEST_F(TestTopology, test_RemoveLogicalPool_LogicalPoolNotFound) {
    PoolIdType id = 0x01;

    int ret = topology_->RemoveLogicalPool(id);

    ASSERT_EQ(kTopoErrCodeLogicalPoolNotFound, ret);
}

TEST_F(TestTopology, test_RemoveLogicalPool_StorageFail) {
    PoolIdType physicalPoolId = 0x11;
    PrepareAddPhysicalPool(physicalPoolId);
    PoolIdType id = 0x01;
    PrepareAddLogicalPool(id, "name", physicalPoolId);

    EXPECT_CALL(*storage_, DeleteLogicalPool(_))
        .WillOnce(Return(false));

    int ret = topology_->RemoveLogicalPool(id);

    ASSERT_EQ(kTopoErrCodeStorgeFail, ret);
}

TEST_F(TestTopology, test_RemovePhysicalPool_success) {
    PoolIdType poolId = 0x11;
    PrepareAddPhysicalPool(poolId);

    EXPECT_CALL(*storage_, DeletePhysicalPool(_))
        .WillOnce(Return(true));

    int ret = topology_->RemovePhysicalPool(poolId);

    ASSERT_EQ(kTopoErrCodeSuccess, ret);
}

TEST_F(TestTopology, test_RemovePhysicalPool_PhysicalPoolNotFound) {
    PoolIdType poolId = 0x11;

    int ret = topology_->RemovePhysicalPool(poolId);

    ASSERT_EQ(kTopoErrCodePhysicalPoolNotFound, ret);
}

TEST_F(TestTopology, test_RemovePhysicalPool_StorageFail) {
    PoolIdType poolId = 0x11;
    PrepareAddPhysicalPool(poolId);

    EXPECT_CALL(*storage_, DeletePhysicalPool(_))
        .WillOnce(Return(false));

    int ret = topology_->RemovePhysicalPool(poolId);

    ASSERT_EQ(kTopoErrCodeStorgeFail, ret);
}

TEST_F(TestTopology, test_RemoveZone_success) {
    ZoneIdType zoneId = 0x21;
    PoolIdType poolId = 0x11;
    PrepareAddPhysicalPool(poolId);
    PrepareAddZone(zoneId,
            "testZone",
            poolId);

    EXPECT_CALL(*storage_, DeleteZone(_))
        .WillOnce(Return(true));

    int ret = topology_->RemoveZone(zoneId);
    ASSERT_EQ(kTopoErrCodeSuccess, ret);

    PhysicalPool pool;
    topology_->GetPhysicalPool(poolId, &pool);
    std::list<ZoneIdType> zoneList = pool.GetZoneList();
    auto it = std::find(zoneList.begin(), zoneList.end(), zoneId);
    ASSERT_TRUE(it == zoneList.end());
}

TEST_F(TestTopology, test_RemoveZone_ZoneNotFound) {
    ZoneIdType zoneId = 0x21;

    int ret = topology_->RemoveZone(zoneId);
    ASSERT_EQ(kTopoErrCodeZoneNotFound, ret);
}

TEST_F(TestTopology, test_RemoveZone_StorageFail) {
    ZoneIdType zoneId = 0x21;
    PrepareAddPhysicalPool();
    PrepareAddZone(zoneId);

    EXPECT_CALL(*storage_, DeleteZone(_))
        .WillOnce(Return(false));

    int ret = topology_->RemoveZone(zoneId);
    ASSERT_EQ(kTopoErrCodeStorgeFail, ret);
}

TEST_F(TestTopology, test_RemoveServer_success) {
    ServerIdType serverId = 0x31;
    ZoneIdType zoneId = 0x21;
    PrepareAddPhysicalPool();
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
    ASSERT_EQ(kTopoErrCodeSuccess, ret);

    Zone zone;
    topology_->GetZone(zoneId, &zone);
    std::list<ServerIdType> serverList = zone.GetServerList();
    auto it = std::find(serverList.begin(), serverList.end(), serverId);

    ASSERT_TRUE(it == serverList.end());
}

TEST_F(TestTopology, test_RemoveSever_ServerNotFound) {
    ServerIdType serverId = 0x31;

    int ret = topology_->RemoveServer(serverId);
    ASSERT_EQ(kTopoErrCodeServerNotFound, ret);
}

TEST_F(TestTopology, test_RemoveServer_StorageFail) {
    ServerIdType serverId = 0x31;
    ZoneIdType zoneId = 0x21;
    PrepareAddPhysicalPool();
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
    ASSERT_EQ(kTopoErrCodeStorgeFail, ret);
}

TEST_F(TestTopology, test_RemoveChunkServer_success) {
    ChunkServerIdType csId = 0x41;
    ServerIdType serverId = 0x31;

    PrepareAddPhysicalPool();
    PrepareAddZone();
    PrepareAddServer(serverId);
    PrepareAddChunkServer(csId,
            "token",
            "ssd",
            serverId);


    EXPECT_CALL(*storage_, DeleteChunkServer(_))
        .WillOnce(Return(true));

    int ret = topology_->RemoveChunkServer(csId);
    ASSERT_EQ(kTopoErrCodeSuccess, ret);

    Server server;
    topology_->GetServer(serverId, &server);
    std::list<ChunkServerIdType> csList = server.GetChunkServerList();
    auto it = std::find(csList.begin(), csList.end(), serverId);
    ASSERT_TRUE(it == csList.end());
}


TEST_F(TestTopology, test_RemoveChunkServer_ChunkSeverNotFound) {
    ChunkServerIdType csId = 0x41;

    int ret = topology_->RemoveChunkServer(csId);
    ASSERT_EQ(kTopoErrCodeChunkServerNotFound, ret);
}

TEST_F(TestTopology, test_RemoveChunkServer_StorageFail) {
    ChunkServerIdType csId = 0x41;
    ServerIdType serverId = 0x31;

    PrepareAddPhysicalPool();
    PrepareAddZone();
    PrepareAddServer(serverId);
    PrepareAddChunkServer(csId,
            "token",
            "ssd",
            serverId);


    EXPECT_CALL(*storage_, DeleteChunkServer(_))
        .WillOnce(Return(false));

    int ret = topology_->RemoveChunkServer(csId);
    ASSERT_EQ(kTopoErrCodeStorgeFail, ret);
}

TEST_F(TestTopology, UpdateLogicalPool_success) {
    PoolIdType logicalPoolId = 0x01;
    PoolIdType physicalPoolId = 0x11;
    PrepareAddPhysicalPool(physicalPoolId);
    PrepareAddLogicalPool(logicalPoolId,
            "name1",
            physicalPoolId,
            PAGEFILE,
            LogicalPool::RedundanceAndPlaceMentPolicy(),
            LogicalPool::UserPolicy(),
            0);

    LogicalPool pool(logicalPoolId,
            "name1",
            physicalPoolId,
            APPENDFILE,
            LogicalPool::RedundanceAndPlaceMentPolicy(),
            LogicalPool::UserPolicy(),
            0,
            true);

    EXPECT_CALL(*storage_, UpdateLogicalPool(_))
        .WillOnce(Return(true));

    int ret = topology_->UpdateLogicalPool(pool);

    ASSERT_EQ(kTopoErrCodeSuccess, ret);

    LogicalPool pool2;
    topology_->GetLogicalPool(logicalPoolId, &pool2);
    ASSERT_EQ(APPENDFILE, pool2.GetLogicalPoolType());
}

TEST_F(TestTopology, UpdateLogicalPool_LogicalPoolNotFound) {
    PoolIdType logicalPoolId = 0x01;
    PoolIdType physicalPoolId = 0x11;
    LogicalPool pool(logicalPoolId,
            "name1",
            physicalPoolId,
            APPENDFILE,
            LogicalPool::RedundanceAndPlaceMentPolicy(),
            LogicalPool::UserPolicy(),
            0,
            true);

    int ret = topology_->UpdateLogicalPool(pool);

    ASSERT_EQ(kTopoErrCodeLogicalPoolNotFound, ret);
}


TEST_F(TestTopology, UpdateLogicalPool_StorageFail) {
    PoolIdType logicalPoolId = 0x01;
    PoolIdType physicalPoolId = 0x11;
    PrepareAddPhysicalPool(physicalPoolId);
    PrepareAddLogicalPool(logicalPoolId,
            "name1",
            physicalPoolId,
            PAGEFILE,
            LogicalPool::RedundanceAndPlaceMentPolicy(),
            LogicalPool::UserPolicy(),
            0);

    LogicalPool pool(logicalPoolId,
            "name1",
            physicalPoolId,
            APPENDFILE,
            LogicalPool::RedundanceAndPlaceMentPolicy(),
            LogicalPool::UserPolicy(),
            0,
            true);

    EXPECT_CALL(*storage_, UpdateLogicalPool(_))
        .WillOnce(Return(false));

    int ret = topology_->UpdateLogicalPool(pool);

    ASSERT_EQ(kTopoErrCodeStorgeFail, ret);
}


TEST_F(TestTopology, UpdatePhysicalPool_success) {
    PoolIdType physicalPoolId = 0x11;
    PrepareAddPhysicalPool(physicalPoolId,
            "name1",
            "desc1");

    PhysicalPool newPool(physicalPoolId,
            "name1",
            "desc2");

    EXPECT_CALL(*storage_, UpdatePhysicalPool(_))
        .WillOnce(Return(true));

    int ret = topology_->UpdatePhysicalPool(newPool);
    ASSERT_EQ(kTopoErrCodeSuccess, ret);
    PhysicalPool pool2;

    topology_->GetPhysicalPool(physicalPoolId, &pool2);
    ASSERT_STREQ("desc2", pool2.GetDesc().c_str());
}

TEST_F(TestTopology, UpdatePhysicalPool_PhysicalPoolNotFound) {
    PoolIdType physicalPoolId = 0x11;

    PhysicalPool newPool(physicalPoolId,
            "name1",
            "desc2");

    int ret = topology_->UpdatePhysicalPool(newPool);
    ASSERT_EQ(kTopoErrCodePhysicalPoolNotFound, ret);
}


TEST_F(TestTopology, UpdatePhysicalPool_StorageFail) {
    PoolIdType physicalPoolId = 0x11;
    PrepareAddPhysicalPool(physicalPoolId,
            "name1",
            "desc1");

    PhysicalPool newPool(physicalPoolId,
            "name1",
            "desc2");

    EXPECT_CALL(*storage_, UpdatePhysicalPool(_))
        .WillOnce(Return(false));

    int ret = topology_->UpdatePhysicalPool(newPool);
    ASSERT_EQ(kTopoErrCodeStorgeFail, ret);
}



TEST_F(TestTopology, UpdateZone_success) {
    ZoneIdType zoneId = 0x21;
    PoolIdType physicalPoolId = 0x11;
    PrepareAddPhysicalPool(physicalPoolId);
    PrepareAddZone(zoneId,
            "name1",
            physicalPoolId,
            "desc1");

    Zone newZone(zoneId,
            "name1",
            physicalPoolId,
            "desc2");

    EXPECT_CALL(*storage_, UpdateZone(_))
        .WillOnce(Return(true));
    int ret = topology_->UpdateZone(newZone);
    ASSERT_EQ(kTopoErrCodeSuccess, ret);
}


TEST_F(TestTopology, UpdateZone_ZoneNotFound) {
    ZoneIdType zoneId = 0x21;
    PoolIdType physicalPoolId = 0x11;

    Zone newZone(zoneId,
            "name1",
            physicalPoolId,
            "desc2");

    int ret = topology_->UpdateZone(newZone);
    ASSERT_EQ(kTopoErrCodeZoneNotFound, ret);
}

TEST_F(TestTopology, UpdateZone_StorageFail) {
    ZoneIdType zoneId = 0x21;
    PoolIdType physicalPoolId = 0x11;
    PrepareAddPhysicalPool(physicalPoolId);
    PrepareAddZone(zoneId,
            "name1",
            physicalPoolId,
            "desc1");

    Zone newZone(zoneId,
            "name1",
            physicalPoolId,
            "desc2");

    EXPECT_CALL(*storage_, UpdateZone(_))
        .WillOnce(Return(false));
    int ret = topology_->UpdateZone(newZone);
    ASSERT_EQ(kTopoErrCodeStorgeFail, ret);
}

TEST_F(TestTopology, UpdateServer_success) {
    PoolIdType physicalPoolId = 0x11;
    ZoneIdType zoneId = 0x21;
    ServerIdType serverId = 0x31;
    PrepareAddPhysicalPool(physicalPoolId);
    PrepareAddZone(zoneId);
    PrepareAddServer(serverId,
            "name1",
            "ip1",
            0,
            "ip2",
            0,
            zoneId,
            physicalPoolId,
            "desc1");

    Server newServer(serverId,
            "name1",
            "ip1",
            0,
            "ip2",
            0,
            zoneId,
            physicalPoolId,
            "desc2");

    EXPECT_CALL(*storage_, UpdateServer(_))
        .WillOnce(Return(true));

    int ret = topology_->UpdateServer(newServer);
    ASSERT_EQ(kTopoErrCodeSuccess, ret);
}

TEST_F(TestTopology, UpdateServer_ServerNotFound) {
    PoolIdType physicalPoolId = 0x11;
    ZoneIdType zoneId = 0x21;
    ServerIdType serverId = 0x31;

    Server newServer(serverId,
            "name1",
            "ip1",
            0,
            "ip2",
            0,
            zoneId,
            physicalPoolId,
            "desc2");

    int ret = topology_->UpdateServer(newServer);
    ASSERT_EQ(kTopoErrCodeServerNotFound, ret);
}

TEST_F(TestTopology, UpdateServer_StorageFail) {
    PoolIdType physicalPoolId = 0x11;
    ZoneIdType zoneId = 0x21;
    ServerIdType serverId = 0x31;
    PrepareAddPhysicalPool(physicalPoolId);
    PrepareAddZone(zoneId);
    PrepareAddServer(serverId,
            "name1",
            "ip1",
            0,
            "ip2",
            0,
            zoneId,
            physicalPoolId,
            "desc1");

    Server newServer(serverId,
            "name1",
            "ip1",
            0,
            "ip2",
            0,
            zoneId,
            physicalPoolId,
            "desc2");

    EXPECT_CALL(*storage_, UpdateServer(_))
        .WillOnce(Return(false));

    int ret = topology_->UpdateServer(newServer);
    ASSERT_EQ(kTopoErrCodeStorgeFail, ret);
}


TEST_F(TestTopology, UpdateChunkServerTopo_success) {
    PoolIdType physicalPoolId = 0x11;
    ZoneIdType zoneId = 0x21;
    ServerIdType serverId = 0x31;
    ChunkServerIdType csId = 0x41;
    PrepareAddPhysicalPool(physicalPoolId);
    PrepareAddZone(zoneId);
    PrepareAddServer(serverId);
    PrepareAddChunkServer(csId,
            "token",
            "ssd",
            serverId,
            "ip1",
            100,
            "/");

    ChunkServer newCs(csId,
            "token",
            "ssd",
            serverId,
            "ip1",
            100,
            "/abc");

    EXPECT_CALL(*storage_, UpdateChunkServer(_))
        .WillOnce(Return(true));
    int ret = topology_->UpdateChunkServerTopo(newCs);
    ASSERT_EQ(kTopoErrCodeSuccess, ret);
}

TEST_F(TestTopology, UpdateChunkServerTopo_UpdateServerSuccess) {
    PoolIdType physicalPoolId = 0x11;
    ZoneIdType zoneId = 0x21;
    ServerIdType serverId = 0x31;
    ServerIdType serverId2 = 0x32;
    ChunkServerIdType csId = 0x41;
    PrepareAddPhysicalPool(physicalPoolId);
    PrepareAddZone(zoneId);
    PrepareAddServer(serverId, "server1",
        "ip1", 0, "ip2", 0, zoneId, physicalPoolId);
    PrepareAddServer(serverId2, "server2",
        "ip3", 0, "ip4", 0, zoneId, physicalPoolId);
    PrepareAddChunkServer(csId,
            "token",
            "ssd",
            serverId,
            "ip1",
            100,
            "/");

    ChunkServer newCs(csId,
            "token",
            "ssd",
            serverId2,
            "ip3",
            100,
            "/abc");

    EXPECT_CALL(*storage_, UpdateChunkServer(_))
        .WillOnce(Return(true));
    int ret = topology_->UpdateChunkServerTopo(newCs);
    ASSERT_EQ(kTopoErrCodeSuccess, ret);
}

TEST_F(TestTopology, UpdateChunkServerTopo_ChunkServerNotFound) {
    ServerIdType serverId = 0x31;
    ChunkServerIdType csId = 0x41;

    ChunkServer newCs(csId,
            "token",
            "ssd",
            serverId,
            "ip1",
            100,
            "/abc");

    int ret = topology_->UpdateChunkServerTopo(newCs);
    ASSERT_EQ(kTopoErrCodeChunkServerNotFound, ret);
}

TEST_F(TestTopology, UpdateChunkServerTopo_StorageFail) {
    PoolIdType physicalPoolId = 0x11;
    ZoneIdType zoneId = 0x21;
    ServerIdType serverId = 0x31;
    ChunkServerIdType csId = 0x41;
    PrepareAddPhysicalPool(physicalPoolId);
    PrepareAddZone(zoneId);
    PrepareAddServer(serverId);
    PrepareAddChunkServer(csId,
            "token",
            "ssd",
            serverId,
            "ip1",
            100,
            "/");

    ChunkServer newCs(csId,
            "token",
            "ssd",
            serverId,
            "ip1",
            100,
            "/abc");

    EXPECT_CALL(*storage_, UpdateChunkServer(_))
        .WillOnce(Return(false));
    int ret = topology_->UpdateChunkServerTopo(newCs);
    ASSERT_EQ(kTopoErrCodeStorgeFail, ret);
}


TEST_F(TestTopology, UpdateChunkServerDiskStatus_success) {
    PoolIdType physicalPoolId = 0x11;
    ZoneIdType zoneId = 0x21;
    ServerIdType serverId = 0x31;
    ChunkServerIdType csId = 0x41;
    PrepareAddPhysicalPool(physicalPoolId);
    PrepareAddZone(zoneId);
    PrepareAddServer(serverId);
    PrepareAddChunkServer(csId,
            "token",
            "ssd",
            serverId,
            "/");

    ChunkServerState csState;
    csState.SetDiskState(DISKERROR);
    csState.SetDiskCapacity(100);

    int ret = topology_->UpdateChunkServerDiskStatus(csState,  csId);
    ASSERT_EQ(kTopoErrCodeSuccess, ret);

    // 只刷一次
    EXPECT_CALL(*storage_, UpdateChunkServer(_))
        .WillOnce(Return(true));
    topology_->Run();
    // sleep 等待刷数据库
    sleep(5);
    topology_->Stop();
}

TEST_F(TestTopology, UpdateChunkServerDiskStatus_ChunkServerNotFound) {
    ChunkServerIdType csId = 0x41;

    ChunkServerState csState;
    csState.SetDiskState(DISKERROR);
    csState.SetDiskCapacity(100);

    int ret = topology_->UpdateChunkServerDiskStatus(csState,  csId);
    ASSERT_EQ(kTopoErrCodeChunkServerNotFound, ret);
}

TEST_F(TestTopology, FindLogicalPool_success) {
    PoolIdType logicalPoolId = 0x01;
    std::string logicalPoolName = "logicalPool1";
    PoolIdType physicalPoolId = 0x11;
    std::string physicalPoolName = "PhysiclPool1";
    PrepareAddPhysicalPool(physicalPoolId, physicalPoolName);
    PrepareAddLogicalPool(logicalPoolId, logicalPoolName, physicalPoolId);
    PoolIdType ret = topology_->FindLogicalPool(logicalPoolName,
        physicalPoolName);
    ASSERT_EQ(logicalPoolId, ret);
}

TEST_F(TestTopology, FindLogicalPool_LogicalPoolNotFound) {
    std::string logicalPoolName = "logicalPool1";
    std::string physicalPoolName = "PhysiclPool1";
    PoolIdType ret = topology_->FindLogicalPool(logicalPoolName,
                                                physicalPoolName);

    ASSERT_EQ(static_cast<PoolIdType>(UNINTIALIZE_ID),
                                      ret);
}

TEST_F(TestTopology, FindPhysicalPool_success) {
    PoolIdType physicalPoolId = 0x11;
    std::string physicalPoolName = "physicalPoolName";
    PrepareAddPhysicalPool(physicalPoolId, physicalPoolName);
    PoolIdType ret = topology_->FindPhysicalPool(physicalPoolName);
    ASSERT_EQ(physicalPoolId, ret);
}

TEST_F(TestTopology, FindPhysicalPool_PhysicalPoolNotFound) {
    std::string physicalPoolName = "physicalPoolName";
    PoolIdType ret = topology_->FindPhysicalPool(physicalPoolName);
    ASSERT_EQ(static_cast<PoolIdType>(UNINTIALIZE_ID),
                                      ret);
}


TEST_F(TestTopology, FindZone_success) {
    PoolIdType physicalPoolId = 0x11;
    std::string physicalPoolName = "physicalPoolName";
    ZoneIdType zoneId = 0x21;
    std::string zoneName = "zoneName";
    PrepareAddPhysicalPool(physicalPoolId, physicalPoolName);
    PrepareAddZone(zoneId, zoneName);
    ZoneIdType ret = topology_->FindZone(zoneName, physicalPoolName);
    ASSERT_EQ(zoneId, ret);
}

TEST_F(TestTopology, FindZone_ZoneNotFound) {
    std::string physicalPoolName = "physicalPoolName";
    std::string zoneName = "zoneName";
    ZoneIdType ret = topology_->FindZone(zoneName, physicalPoolName);
    ASSERT_EQ(static_cast<ZoneIdType>(UNINTIALIZE_ID),
                                      ret);
}

TEST_F(TestTopology, FindZone_success2) {
    PoolIdType physicalPoolId = 0x11;
    std::string physicalPoolName = "physicalPoolName";
    ZoneIdType zoneId = 0x21;
    std::string zoneName = "zoneName";
    PrepareAddPhysicalPool(physicalPoolId, physicalPoolName);
    PrepareAddZone(zoneId, zoneName);
    ZoneIdType ret = topology_->FindZone(zoneName, physicalPoolId);
    ASSERT_EQ(zoneId, ret);
}

TEST_F(TestTopology, FindZone_ZoneNotFound2) {
    PoolIdType physicalPoolId = 0x11;
    std::string physicalPoolName = "physicalPoolName";
    std::string zoneName = "zoneName";
    ZoneIdType ret = topology_->FindZone(zoneName, physicalPoolId);
    ASSERT_EQ(static_cast<ZoneIdType>(UNINTIALIZE_ID),
                                      ret);
}

TEST_F(TestTopology, FindServerByHostName_success) {
    ServerIdType serverId = 0x31;
    std::string hostName = "host1";
    PrepareAddPhysicalPool();
    PrepareAddZone();
    PrepareAddServer(serverId,
            hostName);

    ServerIdType ret = topology_->FindServerByHostName(hostName);
    ASSERT_EQ(serverId, ret);
}

TEST_F(TestTopology, FindServerByHostName_ServerNotFound) {
    std::string hostName = "host1";
    ServerIdType ret = topology_->FindServerByHostName(hostName);
    ASSERT_EQ(static_cast<ServerIdType>(UNINTIALIZE_ID),
                                        ret);
}

TEST_F(TestTopology, FindServerByHostIpPort_success) {
    ServerIdType serverId = 0x31;
    std::string hostName = "host1";
    std::string internalHostIp = "ip1";
    std::string externalHostIp = "ip2";
    PrepareAddPhysicalPool();
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
    PrepareAddPhysicalPool();
    PrepareAddZone();
    PrepareAddServer(serverId,
            hostName,
            internalHostIp,
            0,
            externalHostIp,
            0);

    ServerIdType ret = topology_->FindServerByHostIpPort("ip3", 0);
    ASSERT_EQ(static_cast<ServerIdType>(UNINTIALIZE_ID),
                                        ret);
}

TEST_F(TestTopology, FindChunkServerNotRetired_success) {
    ServerIdType serverId = 0x31;
    std::string hostName = "host1";
    std::string internalHostIp = "ip1";
    std::string externalHostIp = "ip2";
    ChunkServerIdType csId = 0x41;
    uint32_t port = 1024;

    PrepareAddPhysicalPool();
    PrepareAddZone();
    PrepareAddServer(serverId,
            hostName,
            internalHostIp,
            0,
            externalHostIp,
            0);
    PrepareAddChunkServer(csId,
            "token",
            "ssd",
            serverId,
            "/",
            port);

    ChunkServerIdType ret = topology_->FindChunkServerNotRetired(
            internalHostIp, port);
    ASSERT_EQ(csId, ret);
}

TEST_F(TestTopology, FindChunkServerNotRetired_ChunkServerNotFound) {
    ServerIdType serverId = 0x31;
    std::string hostName = "host1";
    std::string internalHostIp = "ip1";
    std::string externalHostIp = "ip2";
    ChunkServerIdType csId = 0x41;
    uint32_t port = 1024;

    PrepareAddPhysicalPool();
    PrepareAddZone();
    PrepareAddServer(serverId,
            hostName,
            internalHostIp,
            0,
            externalHostIp,
            0);
    PrepareAddChunkServer(csId,
            "token",
            "ssd",
            serverId,
            "/",
            port);

    ChunkServerIdType ret = topology_->FindChunkServerNotRetired("ip3", port);
    ASSERT_EQ(static_cast<ChunkServerIdType>(
              UNINTIALIZE_ID), ret);
}

TEST_F(TestTopology, GetLogicalPool_success) {
    PoolIdType physicalPoolId = 0x11;
    PrepareAddPhysicalPool(physicalPoolId);
    PoolIdType logicalPoolId = 0x01;
    PrepareAddLogicalPool(logicalPoolId, "name", physicalPoolId);
    LogicalPool pool;
    bool ret = topology_->GetLogicalPool(logicalPoolId, &pool);
    ASSERT_EQ(true, ret);
}

TEST_F(TestTopology, GetLogicalPool_LogicalPoolNotFound) {
    PoolIdType logicalPoolId = 0x01;
    LogicalPool pool;
    bool ret = topology_->GetLogicalPool(logicalPoolId, &pool);
    ASSERT_EQ(false, ret);
}

TEST_F(TestTopology, GetPhysicalPool_success) {
    PoolIdType physicalPoolId = 0x11;
    PrepareAddPhysicalPool(physicalPoolId);
    PhysicalPool pool;
    bool ret = topology_->GetPhysicalPool(physicalPoolId, &pool);
    ASSERT_EQ(true, ret);
}

TEST_F(TestTopology, GetPhysicalPool_PhysicalPoolNotFound) {
    PoolIdType physicalPoolId = 0x11;
    PhysicalPool pool;
    bool ret = topology_->GetPhysicalPool(physicalPoolId, &pool);
    ASSERT_EQ(false, ret);
}

TEST_F(TestTopology, GetZone_success) {
    PoolIdType physicalPoolId = 0x11;
    ZoneIdType zoneId = 0x21;
    PrepareAddPhysicalPool(physicalPoolId);
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
    PoolIdType physicalPoolId = 0x11;
    ZoneIdType zoneId = 0x21;
    ServerIdType serverId = 0x31;
    PrepareAddPhysicalPool(physicalPoolId);
    PrepareAddZone(zoneId);
    PrepareAddServer(serverId);
    Server server;
    bool ret = topology_->GetServer(serverId, &server);
    ASSERT_EQ(true, ret);
}


TEST_F(TestTopology, GetServer_GetServerNotFound) {
    PoolIdType physicalPoolId = 0x11;
    ZoneIdType zoneId = 0x21;
    ServerIdType serverId = 0x31;
    PrepareAddPhysicalPool(physicalPoolId);
    PrepareAddZone(zoneId);
    PrepareAddServer(serverId);
    Server server;
    bool ret = topology_->GetServer(serverId + 1, &server);
    ASSERT_EQ(false, ret);
}

TEST_F(TestTopology, GetChunkServer_success) {
    PoolIdType physicalPoolId = 0x11;
    ZoneIdType zoneId = 0x21;
    ServerIdType serverId = 0x31;
    ChunkServerIdType csId = 0x41;
    PrepareAddPhysicalPool(physicalPoolId);
    PrepareAddZone(zoneId);
    PrepareAddServer(serverId);
    PrepareAddChunkServer(csId);
    ChunkServer chunkserver;
    bool ret = topology_->GetChunkServer(csId, &chunkserver);
    ASSERT_EQ(true, ret);
}

TEST_F(TestTopology, GetChunkServer_ChunkServerNotFound) {
    PoolIdType physicalPoolId = 0x11;
    ZoneIdType zoneId = 0x21;
    ServerIdType serverId = 0x31;
    ChunkServerIdType csId = 0x41;
    PrepareAddPhysicalPool(physicalPoolId);
    PrepareAddZone(zoneId);
    PrepareAddServer(serverId);
    PrepareAddChunkServer(csId);
    ChunkServer chunkserver;
    bool ret = topology_->GetChunkServer(csId + 1, &chunkserver);
    ASSERT_EQ(false, ret);
}


TEST_F(TestTopology, GetChunkServerInCluster_success) {
    PoolIdType physicalPoolId = 0x11;
    ZoneIdType zoneId = 0x21;
    ServerIdType serverId = 0x31;
    ChunkServerIdType csId = 0x41;
    ChunkServerIdType csId2 = 0x42;

    PrepareAddPhysicalPool(physicalPoolId);
    PrepareAddZone(zoneId);
    PrepareAddServer(serverId);
    PrepareAddChunkServer(csId);
    PrepareAddChunkServer(csId2);

    auto csList = topology_->GetChunkServerInCluster();
    ASSERT_THAT(csList, Contains(csId));
    ASSERT_THAT(csList, Contains(csId2));
}

TEST_F(TestTopology, GetServerInCluster_success) {
    PoolIdType physicalPoolId = 0x11;
    ZoneIdType zoneId = 0x21;
    ServerIdType serverId = 0x31;
    ServerIdType serverId2 = 0x32;

    PrepareAddPhysicalPool(physicalPoolId);
    PrepareAddZone(zoneId);
    PrepareAddServer(serverId);
    PrepareAddServer(serverId2);

    auto serverList = topology_->GetServerInCluster();
    ASSERT_THAT(serverList, Contains(serverId));
    ASSERT_THAT(serverList, Contains(serverId2));
}

TEST_F(TestTopology, GetZoneInCluster_success) {
    PoolIdType physicalPoolId = 0x11;
    ZoneIdType zoneId = 0x21;
    ZoneIdType zoneId2 = 0x22;

    PrepareAddPhysicalPool(physicalPoolId);
    PrepareAddZone(zoneId);
    PrepareAddZone(zoneId2);

    auto zoneList = topology_->GetZoneInCluster();
    ASSERT_THAT(zoneList, Contains(zoneId));
    ASSERT_THAT(zoneList, Contains(zoneId2));
}

TEST_F(TestTopology, GetPhysicalPoolInCluster_success) {
    PoolIdType physicalPoolId = 0x11;
    PoolIdType physicalPoolId2 = 0x12;

    PrepareAddPhysicalPool(physicalPoolId);
    PrepareAddPhysicalPool(physicalPoolId2);

    auto poolList = topology_->GetPhysicalPoolInCluster();
    ASSERT_THAT(poolList, Contains(physicalPoolId));
    ASSERT_THAT(poolList, Contains(physicalPoolId2));
}

TEST_F(TestTopology, GetLogicalPoolInCluster_success) {
    PoolIdType physicalPoolId = 0x11;
    PoolIdType logicalPoolId = 0x01;
    PoolIdType logicalPoolId2 = 0x02;

    PrepareAddPhysicalPool(physicalPoolId);
    PrepareAddLogicalPool(logicalPoolId, "name", physicalPoolId);
    PrepareAddLogicalPool(logicalPoolId2, "name2", physicalPoolId);

    auto poolList = topology_->GetLogicalPoolInCluster();
    ASSERT_THAT(poolList, Contains(logicalPoolId));
    ASSERT_THAT(poolList, Contains(logicalPoolId2));
}

TEST_F(TestTopology, GetChunkServerInServer_success) {
    PoolIdType physicalPoolId = 0x11;
    ZoneIdType zoneId = 0x21;
    ServerIdType serverId = 0x31;
    ChunkServerIdType csId = 0x41;
    ChunkServerIdType csId2 = 0x42;

    PrepareAddPhysicalPool(physicalPoolId);
    PrepareAddZone(zoneId);
    PrepareAddServer(serverId);
    PrepareAddChunkServer(csId);
    PrepareAddChunkServer(csId2);

    std::list<ChunkServerIdType> csList =
        topology_->GetChunkServerInServer(serverId);
    ASSERT_THAT(csList, Contains(csId));
    ASSERT_THAT(csList, Contains(csId2));
}

TEST_F(TestTopology, GetChunkServerInServer_empty) {
    ServerIdType serverId = 0x31;
    std::list<ChunkServerIdType> csList =
        topology_->GetChunkServerInServer(serverId);
    ASSERT_EQ(0, csList.size());
}

TEST_F(TestTopology, GetChunkServerInZone_success) {
    PoolIdType physicalPoolId = 0x11;
    ZoneIdType zoneId = 0x21;
    ServerIdType serverId = 0x31;
    ChunkServerIdType csId = 0x41;
    ChunkServerIdType csId2 = 0x42;

    PrepareAddPhysicalPool(physicalPoolId);
    PrepareAddZone(zoneId);
    PrepareAddServer(serverId);
    PrepareAddChunkServer(csId);
    PrepareAddChunkServer(csId2);

    std::list<ChunkServerIdType> csList =
        topology_->GetChunkServerInZone(zoneId);
    ASSERT_THAT(csList, Contains(csId));
    ASSERT_THAT(csList, Contains(csId2));
}

TEST_F(TestTopology, GetChunkServerInPhysicalPool_success) {
    PoolIdType physicalPoolId = 0x11;
    ZoneIdType zoneId = 0x21;
    ServerIdType serverId = 0x31;
    ChunkServerIdType csId = 0x41;
    ChunkServerIdType csId2 = 0x42;

    PrepareAddPhysicalPool(physicalPoolId);
    PrepareAddZone(zoneId);
    PrepareAddServer(serverId);
    PrepareAddChunkServer(csId);
    PrepareAddChunkServer(csId2);

    std::list<ChunkServerIdType> csList =
        topology_->GetChunkServerInPhysicalPool(physicalPoolId);
    ASSERT_THAT(csList, Contains(csId));
    ASSERT_THAT(csList, Contains(csId2));
}

TEST_F(TestTopology, GetServerInZone_success) {
    PoolIdType physicalPoolId = 0x11;
    ZoneIdType zoneId = 0x21;
    ServerIdType serverId = 0x31;
    ServerIdType serverId2 = 0x32;

    PrepareAddPhysicalPool(physicalPoolId);
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

TEST_F(TestTopology, GetServerInPhysicalPool_success) {
    PoolIdType physicalPoolId = 0x11;
    ZoneIdType zoneId = 0x21;
    ServerIdType serverId = 0x31;
    ServerIdType serverId2 = 0x32;

    PrepareAddPhysicalPool(physicalPoolId);
    PrepareAddZone(zoneId);
    PrepareAddServer(serverId);
    PrepareAddServer(serverId2);

    std::list<ServerIdType> serverList =
        topology_->GetServerInPhysicalPool(physicalPoolId);
    ASSERT_THAT(serverList, Contains(serverId));
    ASSERT_THAT(serverList, Contains(serverId2));
}

TEST_F(TestTopology, GetZoneInPhysicalPool_success) {
    PoolIdType physicalPoolId = 0x11;
    ZoneIdType zoneId = 0x21;
    ZoneIdType zoneId2 = 0x22;

    PrepareAddPhysicalPool(physicalPoolId);
    PrepareAddZone(zoneId);
    PrepareAddZone(zoneId2);

    std::list<ZoneIdType> zoneList =
        topology_->GetZoneInPhysicalPool(physicalPoolId);
    ASSERT_THAT(zoneList, Contains(zoneId));
    ASSERT_THAT(zoneList, Contains(zoneId2));
}

TEST_F(TestTopology, GetZoneInPhysicalPool_empty) {
    PoolIdType physicalPoolId = 0x11;
    std::list<ZoneIdType> zoneList =
        topology_->GetZoneInPhysicalPool(physicalPoolId);
    ASSERT_EQ(0, zoneList.size());
}

TEST_F(TestTopology, GetLogicalPoolInPhysicalPool_success) {
    PoolIdType physicalPoolId = 0x01;
    PoolIdType logicalPoolId = 0x01;
    PoolIdType logicalPoolId2 = 0x02;

    PrepareAddPhysicalPool(physicalPoolId);
    PrepareAddLogicalPool(logicalPoolId, "logicalPool1", physicalPoolId);
    PrepareAddLogicalPool(logicalPoolId2, "logicalPool2", physicalPoolId);

    std::list<PoolIdType> poolList =
        topology_->GetLogicalPoolInPhysicalPool(physicalPoolId);
    ASSERT_THAT(poolList, Contains(logicalPoolId));
    ASSERT_THAT(poolList, Contains(logicalPoolId2));
}

TEST_F(TestTopology, GetChunkServerInLogicalPool_success) {
    PoolIdType physicalPoolId = 0x11;
    ZoneIdType zoneId = 0x21;
    ServerIdType serverId = 0x31;
    ChunkServerIdType csId = 0x41;
    ChunkServerIdType csId2 = 0x42;
    PoolIdType logicalPoolId = 0x01;

    PrepareAddPhysicalPool(physicalPoolId);
    PrepareAddZone(zoneId, "name", physicalPoolId);
    PrepareAddServer(
        serverId, "name2", "ip1", 0, "ip2", 0, zoneId, physicalPoolId);
    PrepareAddChunkServer(csId, "token", "ssd", serverId);
    PrepareAddChunkServer(csId2, "token", "ssd", serverId);
    PrepareAddLogicalPool(logicalPoolId, "logicalPool1", physicalPoolId);

    std::list<ChunkServerIdType> csList =
        topology_->GetChunkServerInLogicalPool(logicalPoolId);
    ASSERT_THAT(csList, Contains(csId));
    ASSERT_THAT(csList, Contains(csId2));
}

TEST_F(TestTopology, GetChunkServerInLogicalPool_empty) {
    PoolIdType logicalPoolId = 0x01;
    std::list<ChunkServerIdType> csList =
        topology_->GetChunkServerInLogicalPool(logicalPoolId);
    ASSERT_EQ(0, csList.size());
}

TEST_F(TestTopology, GetServerInLogicalPool_success) {
    PoolIdType physicalPoolId = 0x11;
    ZoneIdType zoneId = 0x21;
    ServerIdType serverId = 0x31;
    ServerIdType serverId2 = 0x32;
    PoolIdType logicalPoolId = 0x01;

    PrepareAddPhysicalPool(physicalPoolId);
    PrepareAddZone(zoneId);
    PrepareAddServer(serverId);
    PrepareAddServer(serverId2);
    PrepareAddLogicalPool(logicalPoolId, "logicalPool1", physicalPoolId);

    std::list<ServerIdType> serverList =
        topology_->GetServerInLogicalPool(logicalPoolId);
    ASSERT_THAT(serverList, Contains(serverId));
    ASSERT_THAT(serverList, Contains(serverId2));
}

TEST_F(TestTopology, GetServerInLogicalPool_empty) {
    PoolIdType logicalPoolId = 0x01;
    std::list<ServerIdType> serverList =
        topology_->GetServerInLogicalPool(logicalPoolId);
    ASSERT_EQ(0, serverList.size());
}

TEST_F(TestTopology, GetZoneInLogicalPool_success) {
    PoolIdType physicalPoolId = 0x11;
    ZoneIdType zoneId = 0x21;
    ZoneIdType zoneId2 = 0x22;
    PoolIdType logicalPoolId = 0x01;

    PrepareAddPhysicalPool(physicalPoolId);
    PrepareAddZone(zoneId);
    PrepareAddZone(zoneId2);
    PrepareAddLogicalPool(logicalPoolId, "logicalPool1", physicalPoolId);

    std::list<ServerIdType> zoneList =
        topology_->GetZoneInLogicalPool(logicalPoolId);
    ASSERT_THAT(zoneList, Contains(zoneId));
    ASSERT_THAT(zoneList, Contains(zoneId2));
}

TEST_F(TestTopology, GetZoneInLogicalPool_empty) {
    PoolIdType logicalPoolId = 0x01;
    std::list<ServerIdType> zoneList =
        topology_->GetZoneInLogicalPool(logicalPoolId);
    ASSERT_EQ(0, zoneList.size());
}

TEST_F(TestTopology, AddCopySet_success) {
    PoolIdType logicalPoolId = 0x01;
    PoolIdType physicalPoolId = 0x11;
    CopySetIdType copysetId = 0x51;

    PrepareAddPhysicalPool(physicalPoolId);
    PrepareAddZone(0x21, "zone1", physicalPoolId);
    PrepareAddZone(0x22, "zone2", physicalPoolId);
    PrepareAddZone(0x23, "zone3", physicalPoolId);
    PrepareAddServer(
        0x31, "server1", "127.0.0.1", 0, "127.0.0.1", 0, 0x21, 0x11);
    PrepareAddServer(
        0x32, "server2", "127.0.0.1", 0, "127.0.0.1", 0, 0x22, 0x11);
    PrepareAddServer(
        0x33, "server3", "127.0.0.1", 0, "127.0.0.1", 0, 0x23, 0x11);
    PrepareAddChunkServer(0x41, "token1", "nvme", 0x31, "127.0.0.1", 8200);
    PrepareAddChunkServer(0x42, "token2", "nvme", 0x32, "127.0.0.1", 8200);
    PrepareAddChunkServer(0x43, "token3", "nvme", 0x33, "127.0.0.1", 8200);
    PrepareAddLogicalPool(logicalPoolId, "logicalPool1", physicalPoolId);
    std::set<ChunkServerIdType> replicas;
    replicas.insert(0x41);
    replicas.insert(0x42);
    replicas.insert(0x43);

    CopySetInfo csInfo(logicalPoolId, copysetId);
    csInfo.SetCopySetMembers(replicas);

    EXPECT_CALL(*storage_, StorageCopySet(_))
        .WillOnce(Return(true));
    int ret = topology_->AddCopySet(csInfo);
    ASSERT_EQ(kTopoErrCodeSuccess, ret);
}

TEST_F(TestTopology, AddCopySet_IdDuplicated) {
    PoolIdType logicalPoolId = 0x01;
    PoolIdType physicalPoolId = 0x11;
    CopySetIdType copysetId = 0x51;

    PrepareAddPhysicalPool(physicalPoolId);
    PrepareAddZone(0x21, "zone1", physicalPoolId);
    PrepareAddZone(0x22, "zone2", physicalPoolId);
    PrepareAddZone(0x23, "zone3", physicalPoolId);
    PrepareAddServer(
        0x31, "server1", "127.0.0.1", 0, "127.0.0.1", 0, 0x21, 0x11);
    PrepareAddServer(
        0x32, "server2", "127.0.0.1", 0, "127.0.0.1", 0, 0x22, 0x11);
    PrepareAddServer(
        0x33, "server3", "127.0.0.1", 0, "127.0.0.1", 0, 0x23, 0x11);
    PrepareAddChunkServer(0x41, "token1", "nvme", 0x31, "127.0.0.1", 8200);
    PrepareAddChunkServer(0x42, "token2", "nvme", 0x32, "127.0.0.1", 8200);
    PrepareAddChunkServer(0x43, "token3", "nvme", 0x33, "127.0.0.1", 8200);
    PrepareAddLogicalPool(logicalPoolId, "logicalPool1", physicalPoolId);
    std::set<ChunkServerIdType> replicas;
    replicas.insert(0x41);
    replicas.insert(0x42);
    replicas.insert(0x43);
    PrepareAddCopySet(copysetId, logicalPoolId, replicas);

    CopySetInfo csInfo(logicalPoolId, copysetId);
    csInfo.SetCopySetMembers(replicas);

    int ret = topology_->AddCopySet(csInfo);
    ASSERT_EQ(kTopoErrCodeIdDuplicated, ret);
}

TEST_F(TestTopology, AddCopySet_LogicalPoolNotFound) {
    PoolIdType logicalPoolId = 0x01;
    PoolIdType physicalPoolId = 0x11;
    CopySetIdType copysetId = 0x51;

    PrepareAddPhysicalPool(physicalPoolId);
    PrepareAddZone(0x21, "zone1", physicalPoolId);
    PrepareAddZone(0x22, "zone2", physicalPoolId);
    PrepareAddZone(0x23, "zone3", physicalPoolId);
    PrepareAddServer(
        0x31, "server1", "127.0.0.1", 0, "127.0.0.1", 0, 0x21, 0x11);
    PrepareAddServer(
        0x32, "server2", "127.0.0.1", 0, "127.0.0.1", 0, 0x22, 0x11);
    PrepareAddServer(
        0x33, "server3", "127.0.0.1", 0, "127.0.0.1", 0, 0x23, 0x11);
    PrepareAddChunkServer(0x41, "token1", "nvme", 0x31, "127.0.0.1", 8200);
    PrepareAddChunkServer(0x42, "token2", "nvme", 0x32, "127.0.0.1", 8200);
    PrepareAddChunkServer(0x43, "token3", "nvme", 0x33, "127.0.0.1", 8200);
    PrepareAddLogicalPool(logicalPoolId, "logicalPool1", physicalPoolId);
    std::set<ChunkServerIdType> replicas;
    replicas.insert(0x41);
    replicas.insert(0x42);
    replicas.insert(0x43);

    CopySetInfo csInfo(++logicalPoolId, copysetId);
    csInfo.SetCopySetMembers(replicas);

    int ret = topology_->AddCopySet(csInfo);
    ASSERT_EQ(kTopoErrCodeLogicalPoolNotFound, ret);
}

TEST_F(TestTopology, AddCopySet_StorageFail) {
    PoolIdType logicalPoolId = 0x01;
    PoolIdType physicalPoolId = 0x11;
    CopySetIdType copysetId = 0x51;

    PrepareAddPhysicalPool(physicalPoolId);
    PrepareAddZone(0x21, "zone1", physicalPoolId);
    PrepareAddZone(0x22, "zone2", physicalPoolId);
    PrepareAddZone(0x23, "zone3", physicalPoolId);
    PrepareAddServer(
        0x31, "server1", "127.0.0.1", 0, "127.0.0.1", 0, 0x21, 0x11);
    PrepareAddServer(
        0x32, "server2", "127.0.0.1", 0, "127.0.0.1", 0, 0x22, 0x11);
    PrepareAddServer(
        0x33, "server3", "127.0.0.1", 0, "127.0.0.1", 0, 0x23, 0x11);
    PrepareAddChunkServer(0x41, "token1", "nvme", 0x31, "127.0.0.1", 8200);
    PrepareAddChunkServer(0x42, "token2", "nvme", 0x32, "127.0.0.1", 8200);
    PrepareAddChunkServer(0x43, "token3", "nvme", 0x33, "127.0.0.1", 8200);
    PrepareAddLogicalPool(logicalPoolId, "logicalPool1", physicalPoolId);
    std::set<ChunkServerIdType> replicas;
    replicas.insert(0x41);
    replicas.insert(0x42);
    replicas.insert(0x43);

    CopySetInfo csInfo(logicalPoolId, copysetId);
    csInfo.SetCopySetMembers(replicas);

    EXPECT_CALL(*storage_, StorageCopySet(_))
        .WillOnce(Return(false));
    int ret = topology_->AddCopySet(csInfo);
    ASSERT_EQ(kTopoErrCodeStorgeFail, ret);
}

TEST_F(TestTopology, RemoveCopySet_success) {
    PoolIdType logicalPoolId = 0x01;
    PoolIdType physicalPoolId = 0x11;
    CopySetIdType copysetId = 0x51;

    PrepareAddPhysicalPool(physicalPoolId);
    PrepareAddZone(0x21, "zone1", physicalPoolId);
    PrepareAddZone(0x22, "zone2", physicalPoolId);
    PrepareAddZone(0x23, "zone3", physicalPoolId);
    PrepareAddServer(
        0x31, "server1", "127.0.0.1", 0, "127.0.0.1", 0, 0x21, 0x11);
    PrepareAddServer(
        0x32, "server2", "127.0.0.1", 0, "127.0.0.1", 0, 0x22, 0x11);
    PrepareAddServer(
        0x33, "server3", "127.0.0.1", 0, "127.0.0.1", 0, 0x23, 0x11);
    PrepareAddChunkServer(0x41, "token1", "nvme", 0x31, "127.0.0.1", 8200);
    PrepareAddChunkServer(0x42, "token2", "nvme", 0x32, "127.0.0.1", 8200);
    PrepareAddChunkServer(0x43, "token3", "nvme", 0x33, "127.0.0.1", 8200);
    PrepareAddLogicalPool(logicalPoolId, "logicalPool1", physicalPoolId);
    std::set<ChunkServerIdType> replicas;
    replicas.insert(0x41);
    replicas.insert(0x42);
    replicas.insert(0x43);
    PrepareAddCopySet(copysetId, logicalPoolId, replicas);

    EXPECT_CALL(*storage_, DeleteCopySet(_))
        .WillOnce(Return(true));

    int ret = topology_->RemoveCopySet(
        std::pair<PoolIdType, CopySetIdType>(logicalPoolId, copysetId));

    ASSERT_EQ(kTopoErrCodeSuccess, ret);
}

TEST_F(TestTopology, RemoveCopySet_storageFail) {
    PoolIdType logicalPoolId = 0x01;
    PoolIdType physicalPoolId = 0x11;
    CopySetIdType copysetId = 0x51;

    PrepareAddPhysicalPool(physicalPoolId);
    PrepareAddZone(0x21, "zone1", physicalPoolId);
    PrepareAddZone(0x22, "zone2", physicalPoolId);
    PrepareAddZone(0x23, "zone3", physicalPoolId);
    PrepareAddServer(
        0x31, "server1", "127.0.0.1" , 0, "127.0.0.1" , 0, 0x21, 0x11);
    PrepareAddServer(
        0x32, "server2", "127.0.0.1" , 0, "127.0.0.1" , 0, 0x22, 0x11);
    PrepareAddServer(
        0x33, "server3", "127.0.0.1" , 0, "127.0.0.1" , 0, 0x23, 0x11);
    PrepareAddChunkServer(0x41, "token1", "nvme", 0x31, "127.0.0.1", 8200);
    PrepareAddChunkServer(0x42, "token2", "nvme", 0x32, "127.0.0.1", 8200);
    PrepareAddChunkServer(0x43, "token3", "nvme", 0x33, "127.0.0.1", 8200);
    PrepareAddLogicalPool(logicalPoolId, "logicalPool1", physicalPoolId);
    std::set<ChunkServerIdType> replicas;
    replicas.insert(0x41);
    replicas.insert(0x42);
    replicas.insert(0x43);
    PrepareAddCopySet(copysetId, logicalPoolId, replicas);

    EXPECT_CALL(*storage_, DeleteCopySet(_))
        .WillOnce(Return(false));

    int ret = topology_->RemoveCopySet(
        std::pair<PoolIdType, CopySetIdType>(logicalPoolId, copysetId));

    ASSERT_EQ(kTopoErrCodeStorgeFail, ret);
}

TEST_F(TestTopology, RemoveCopySet_CopySetNotFound) {
    PoolIdType logicalPoolId = 0x01;
    PoolIdType physicalPoolId = 0x11;
    CopySetIdType copysetId = 0x51;

    PrepareAddPhysicalPool(physicalPoolId);
    PrepareAddZone(0x21, "zone1", physicalPoolId);
    PrepareAddZone(0x22, "zone2", physicalPoolId);
    PrepareAddZone(0x23, "zone3", physicalPoolId);
    PrepareAddServer(
        0x31, "server1", "127.0.0.1" , 0, "127.0.0.1" , 0, 0x21, 0x11);
    PrepareAddServer(
        0x32, "server2", "127.0.0.1" , 0, "127.0.0.1" , 0, 0x22, 0x11);
    PrepareAddServer(
        0x33, "server3", "127.0.0.1" , 0, "127.0.0.1" , 0, 0x23, 0x11);
    PrepareAddChunkServer(0x41, "token1", "nvme", 0x31, "127.0.0.1", 8200);
    PrepareAddChunkServer(0x42, "token2", "nvme", 0x32, "127.0.0.1", 8200);
    PrepareAddChunkServer(0x43, "token3", "nvme", 0x33, "127.0.0.1", 8200);
    PrepareAddLogicalPool(logicalPoolId, "logicalPool1", physicalPoolId);
    std::set<ChunkServerIdType> replicas;
    replicas.insert(0x41);
    replicas.insert(0x42);
    replicas.insert(0x43);
    PrepareAddCopySet(copysetId, logicalPoolId, replicas);

    int ret = topology_->RemoveCopySet(
        std::pair<PoolIdType, CopySetIdType>(logicalPoolId, ++copysetId));

    ASSERT_EQ(kTopoErrCodeCopySetNotFound, ret);
}

TEST_F(TestTopology, UpdateCopySetTopo_success) {
    PoolIdType logicalPoolId = 0x01;
    PoolIdType physicalPoolId = 0x11;
    CopySetIdType copysetId = 0x51;

    PrepareAddPhysicalPool(physicalPoolId);
    PrepareAddZone(0x21, "zone1", physicalPoolId);
    PrepareAddZone(0x22, "zone2", physicalPoolId);
    PrepareAddZone(0x23, "zone3", physicalPoolId);
    PrepareAddServer(
        0x31, "server1", "127.0.0.1" , 0, "127.0.0.1" , 0, 0x21, 0x11);
    PrepareAddServer(
        0x32, "server2", "127.0.0.1" , 0, "127.0.0.1" , 0, 0x22, 0x11);
    PrepareAddServer(
        0x33, "server3", "127.0.0.1" , 0, "127.0.0.1" , 0, 0x23, 0x11);
    PrepareAddChunkServer(0x41, "token1", "nvme", 0x31, "127.0.0.1", 8200);
    PrepareAddChunkServer(0x42, "token2", "nvme", 0x32, "127.0.0.1", 8200);
    PrepareAddChunkServer(0x43, "token3", "nvme", 0x33, "127.0.0.1", 8200);
    PrepareAddChunkServer(0x44, "token3", "nvme", 0x33, "127.0.0.1", 8200);
    PrepareAddLogicalPool(logicalPoolId, "logicalPool1", physicalPoolId);
    std::set<ChunkServerIdType> replicas;
    replicas.insert(0x41);
    replicas.insert(0x42);
    replicas.insert(0x43);
    PrepareAddCopySet(copysetId, logicalPoolId, replicas);

    std::set<ChunkServerIdType> replicas2;
    replicas.insert(0x41);
    replicas.insert(0x42);
    replicas.insert(0x44);

    CopySetInfo csInfo(logicalPoolId, copysetId);
    csInfo.SetCopySetMembers(replicas2);

    int ret = topology_->UpdateCopySetTopo(csInfo);

    ASSERT_EQ(kTopoErrCodeSuccess, ret);

    // 只刷一次
    EXPECT_CALL(*storage_, UpdateCopySet(_))
        .WillOnce(Return(true));
    topology_->Run();
    // sleep 等待刷数据库
    sleep(5);
    topology_->Stop();
}

TEST_F(TestTopology, UpdateCopySetTopo_CopySetNotFound) {
    PoolIdType logicalPoolId = 0x01;
    PoolIdType physicalPoolId = 0x11;
    CopySetIdType copysetId = 0x51;

    PrepareAddPhysicalPool(physicalPoolId);
    PrepareAddZone(0x21, "zone1", physicalPoolId);
    PrepareAddZone(0x22, "zone2", physicalPoolId);
    PrepareAddZone(0x23, "zone3", physicalPoolId);
    PrepareAddServer(
        0x31, "server1", "127.0.0.1" , 0, "127.0.0.1" , 0, 0x21, 0x11);
    PrepareAddServer(
        0x32, "server2", "127.0.0.1" , 0, "127.0.0.1" , 0, 0x22, 0x11);
    PrepareAddServer(
        0x33, "server3", "127.0.0.1" , 0, "127.0.0.1" , 0, 0x23, 0x11);
    PrepareAddChunkServer(0x41, "token1", "nvme", 0x31, "127.0.0.1", 8200);
    PrepareAddChunkServer(0x42, "token2", "nvme", 0x32, "127.0.0.1", 8200);
    PrepareAddChunkServer(0x43, "token3", "nvme", 0x33, "127.0.0.1", 8200);
    PrepareAddChunkServer(0x44, "token3", "nvme", 0x33, "127.0.0.1", 8200);
    PrepareAddLogicalPool(logicalPoolId, "logicalPool1", physicalPoolId);
    std::set<ChunkServerIdType> replicas;
    replicas.insert(0x41);
    replicas.insert(0x42);
    replicas.insert(0x43);
    PrepareAddCopySet(copysetId, logicalPoolId, replicas);

    std::set<ChunkServerIdType> replicas2;
    replicas.insert(0x41);
    replicas.insert(0x42);
    replicas.insert(0x44);

    CopySetInfo csInfo(logicalPoolId, ++copysetId);
    csInfo.SetCopySetMembers(replicas2);

    int ret = topology_->UpdateCopySetTopo(csInfo);

    ASSERT_EQ(kTopoErrCodeCopySetNotFound, ret);
}

TEST_F(TestTopology, GetCopySet_success) {
    PoolIdType logicalPoolId = 0x01;
    PoolIdType physicalPoolId = 0x11;
    CopySetIdType copysetId = 0x51;

    PrepareAddPhysicalPool(physicalPoolId);
    PrepareAddZone(0x21, "zone1", physicalPoolId);
    PrepareAddZone(0x22, "zone2", physicalPoolId);
    PrepareAddZone(0x23, "zone3", physicalPoolId);
    PrepareAddServer(
        0x31, "server1", "127.0.0.1" , 0, "127.0.0.1" , 0, 0x21, 0x11);
    PrepareAddServer(
        0x32, "server2", "127.0.0.1" , 0, "127.0.0.1" , 0, 0x22, 0x11);
    PrepareAddServer(
        0x33, "server3", "127.0.0.1" , 0, "127.0.0.1" , 0, 0x23, 0x11);
    PrepareAddChunkServer(0x41, "token1", "nvme", 0x31, "127.0.0.1", 8200);
    PrepareAddChunkServer(0x42, "token2", "nvme", 0x32, "127.0.0.1", 8200);
    PrepareAddChunkServer(0x43, "token3", "nvme", 0x33, "127.0.0.1", 8200);
    PrepareAddLogicalPool(logicalPoolId, "logicalPool1", physicalPoolId);
    std::set<ChunkServerIdType> replicas;
    replicas.insert(0x41);
    replicas.insert(0x42);
    replicas.insert(0x43);
    PrepareAddCopySet(copysetId, logicalPoolId, replicas);

    CopySetInfo copysetInfo;
    int ret = topology_->GetCopySet(
        std::pair<PoolIdType, CopySetIdType>(logicalPoolId, copysetId),
        &copysetInfo);

    ASSERT_EQ(true, ret);
}

TEST_F(TestTopology, GetCopySet_CopysetNotFound) {
    PoolIdType logicalPoolId = 0x01;
    PoolIdType physicalPoolId = 0x11;
    CopySetIdType copysetId = 0x51;

    PrepareAddPhysicalPool(physicalPoolId);
    PrepareAddZone(0x21, "zone1", physicalPoolId);
    PrepareAddZone(0x22, "zone2", physicalPoolId);
    PrepareAddZone(0x23, "zone3", physicalPoolId);
    PrepareAddServer(
        0x31, "server1", "127.0.0.1" , 0, "127.0.0.1" , 0, 0x21, 0x11);
    PrepareAddServer(
        0x32, "server2", "127.0.0.1" , 0, "127.0.0.1" , 0, 0x22, 0x11);
    PrepareAddServer(
        0x33, "server3", "127.0.0.1" , 0, "127.0.0.1" , 0, 0x23, 0x11);
    PrepareAddChunkServer(0x41, "token1", "nvme", 0x31, "127.0.0.1", 8200);
    PrepareAddChunkServer(0x42, "token2", "nvme", 0x32, "127.0.0.1", 8200);
    PrepareAddChunkServer(0x43, "token3", "nvme", 0x33, "127.0.0.1", 8200);
    PrepareAddLogicalPool(logicalPoolId, "logicalPool1", physicalPoolId);
    std::set<ChunkServerIdType> replicas;
    replicas.insert(0x41);
    replicas.insert(0x42);
    replicas.insert(0x43);
    PrepareAddCopySet(copysetId, logicalPoolId, replicas);

    CopySetInfo copysetInfo;
    int ret = topology_->GetCopySet(
        std::pair<PoolIdType, CopySetIdType>(logicalPoolId, ++copysetId),
        &copysetInfo);

    ASSERT_EQ(false, ret);
}

TEST_F(TestTopology, GetCopySetsInLogicalPool_success) {
    PoolIdType logicalPoolId = 0x01;
    PoolIdType physicalPoolId = 0x11;
    CopySetIdType copysetId = 0x51;

    PrepareAddPhysicalPool(physicalPoolId);
    PrepareAddZone(0x21, "zone1", physicalPoolId);
    PrepareAddZone(0x22, "zone2", physicalPoolId);
    PrepareAddZone(0x23, "zone3", physicalPoolId);
    PrepareAddServer(
        0x31, "server1", "127.0.0.1" , 0, "127.0.0.1" , 0, 0x21, 0x11);
    PrepareAddServer(
        0x32, "server2", "127.0.0.1" , 0, "127.0.0.1" , 0, 0x22, 0x11);
    PrepareAddServer(
        0x33, "server3", "127.0.0.1" , 0, "127.0.0.1" , 0, 0x23, 0x11);
    PrepareAddChunkServer(0x41, "token1", "nvme", 0x31, "127.0.0.1", 8200);
    PrepareAddChunkServer(0x42, "token2", "nvme", 0x32, "127.0.0.1", 8200);
    PrepareAddChunkServer(0x43, "token3", "nvme", 0x33, "127.0.0.1", 8200);
    PrepareAddLogicalPool(logicalPoolId, "logicalPool1", physicalPoolId);
    std::set<ChunkServerIdType> replicas;
    replicas.insert(0x41);
    replicas.insert(0x42);
    replicas.insert(0x43);
    PrepareAddCopySet(copysetId, logicalPoolId, replicas);

    std::vector<CopySetIdType> csList =
    topology_->GetCopySetsInLogicalPool(logicalPoolId);
    ASSERT_EQ(1, csList.size());
}

TEST_F(TestTopology, GetCopySetsInCluster_success) {
    PoolIdType logicalPoolId = 0x01;
    PoolIdType physicalPoolId = 0x11;
    CopySetIdType copysetId = 0x51;

    PrepareAddPhysicalPool(physicalPoolId);
    PrepareAddZone(0x21, "zone1", physicalPoolId);
    PrepareAddZone(0x22, "zone2", physicalPoolId);
    PrepareAddZone(0x23, "zone3", physicalPoolId);
    PrepareAddServer(
        0x31, "server1", "127.0.0.1" , 0, "127.0.0.1" , 0, 0x21, 0x11);
    PrepareAddServer(
        0x32, "server2", "127.0.0.1" , 0, "127.0.0.1" , 0, 0x22, 0x11);
    PrepareAddServer(
        0x33, "server3", "127.0.0.1" , 0, "127.0.0.1" , 0, 0x23, 0x11);
    PrepareAddChunkServer(0x41, "token1", "nvme", 0x31, "127.0.0.1", 8200);
    PrepareAddChunkServer(0x42, "token2", "nvme", 0x32, "127.0.0.1", 8200);
    PrepareAddChunkServer(0x43, "token3", "nvme", 0x33, "127.0.0.1", 8200);
    PrepareAddLogicalPool(logicalPoolId, "logicalPool1", physicalPoolId);
    std::set<ChunkServerIdType> replicas;
    replicas.insert(0x41);
    replicas.insert(0x42);
    replicas.insert(0x43);
    PrepareAddCopySet(copysetId, logicalPoolId, replicas);

    std::vector<CopySetKey> csList =
    topology_->GetCopySetsInCluster();
    ASSERT_EQ(1, csList.size());
}

TEST_F(TestTopology, GetCopySetsInChunkServer_success) {
    PoolIdType logicalPoolId = 0x01;
    PoolIdType physicalPoolId = 0x11;
    CopySetIdType copysetId = 0x51;

    PrepareAddPhysicalPool(physicalPoolId);
    PrepareAddZone(0x21, "zone1", physicalPoolId);
    PrepareAddZone(0x22, "zone2", physicalPoolId);
    PrepareAddZone(0x23, "zone3", physicalPoolId);
    PrepareAddServer(
        0x31, "server1", "127.0.0.1" , 0, "127.0.0.1" , 0, 0x21, 0x11);
    PrepareAddServer(
        0x32, "server2", "127.0.0.1" , 0, "127.0.0.1" , 0, 0x22, 0x11);
    PrepareAddServer(
        0x33, "server3", "127.0.0.1" , 0, "127.0.0.1" , 0, 0x23, 0x11);
    PrepareAddChunkServer(0x41, "token1", "nvme", 0x31, "127.0.0.1", 8200);
    PrepareAddChunkServer(0x42, "token2", "nvme", 0x32, "127.0.0.1", 8200);
    PrepareAddChunkServer(0x43, "token3", "nvme", 0x33, "127.0.0.1", 8200);
    PrepareAddLogicalPool(logicalPoolId, "logicalPool1", physicalPoolId);
    std::set<ChunkServerIdType> replicas;
    replicas.insert(0x41);
    replicas.insert(0x42);
    replicas.insert(0x43);
    PrepareAddCopySet(copysetId, logicalPoolId, replicas);

    std::vector<CopySetKey> csList =
    topology_->GetCopySetsInChunkServer(0x41);
    ASSERT_EQ(1, csList.size());
}





}  // namespace topology
}  // namespace mds
}  // namespace curve














