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

#include <gtest/gtest.h>

#include "test/mds/topology/mock_topology.h"
#include "src/mds/topology/topology.h"
#include "src/mds/topology/topology_item.h"
#include "src/common/configuration.h"
#include "src/common/namespace_define.h"

namespace curve {
namespace mds {
namespace topology {

using ::testing::Return;
using ::testing::_;
using ::testing::Contains;
using ::testing::SetArgPointee;
using ::testing::SaveArg;
using ::testing::DoAll;
using ::curve::common::Configuration;
using ::curve::common::kDefaultPoolsetId;
using ::curve::common::kDefaultPoolsetName;

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

        const std::unordered_map<PoolsetIdType, Poolset> poolsetMap{
            {kDefaultPoolsetId,
             {kDefaultPoolsetId, kDefaultPoolsetName, "", ""}}
        };

        ON_CALL(*storage_, LoadPoolset(_, _))
            .WillByDefault(DoAll(
                SetArgPointee<0>(poolsetMap),
                SetArgPointee<1>(static_cast<PoolsetIdType>(kDefaultPoolsetId)),
                Return(true)));
    }

    virtual void TearDown() {
        idGenerator_ = nullptr;
        tokenGenerator_ = nullptr;
        storage_ = nullptr;
        topology_ = nullptr;
    }

 protected:
    void PrepareAddPoolset(PoolsetIdType id = 0x61,
                           const std::string& name = "ssdPoolset1",
                           const std::string& type = "SSD",
                           const std::string& desc = "descPoolset") {
        Poolset poolset(id, name, type, desc);
        EXPECT_CALL(*storage_, StoragePoolset(_))
            .WillOnce(Return(true));

        int ret = topology_->AddPoolset(poolset);
        ASSERT_EQ(kTopoErrCodeSuccess, ret);
    }

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
                 PoolsetIdType pid = 0x61,
                 const std::string &desc = "descPhysicalPool",
                 uint64_t diskCapacity = 0) {
        PhysicalPool pool(id,
                name,
                pid,
                desc);
        pool.SetDiskCapacity(diskCapacity);
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
                const std::string &diskPath = "/",
                uint64_t diskUsed = 512,
                uint64_t diskCapacity = 1024) {
            ChunkServer cs(id,
                    token,
                    diskType,
                    serverId,
                    hostIp,
                    port,
                    diskPath);
            ChunkServerState state;
            state.SetDiskCapacity(diskCapacity);
            state.SetDiskUsed(diskUsed);
            cs.SetChunkServerState(state);
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
    std::shared_ptr<TopologyImpl> topology_;
    std::shared_ptr<Configuration> conf_;
};

TEST_F(TestTopology, test_init_success) {
    std::vector<ClusterInformation> infos;
    EXPECT_CALL(*storage_, LoadClusterInfo(_))
        .WillOnce(DoAll(SetArgPointee<0>(infos),
                Return(true)));

    EXPECT_CALL(*storage_, StorageClusterInfo(_))
        .WillOnce(Return(true));

    const std::unordered_map<PoolsetIdType, Poolset> poolsetMap{
        {kDefaultPoolsetId,
         {kDefaultPoolsetId, kDefaultPoolsetName, "", ""}}
    };
    std::unordered_map<PoolIdType, LogicalPool> logicalPoolMap_;
    std::unordered_map<PoolIdType, PhysicalPool> physicalPoolMap_;
    std::unordered_map<ZoneIdType, Zone> zoneMap_;
    std::unordered_map<ServerIdType, Server> serverMap_;
    std::unordered_map<ChunkServerIdType, ChunkServer> chunkServerMap_;
    std::map<CopySetKey, CopySetInfo> copySetMap_;

    logicalPoolMap_[0x01] = LogicalPool(0x01, "lpool1", 0x11, PAGEFILE,
        LogicalPool::RedundanceAndPlaceMentPolicy(),
        LogicalPool::UserPolicy(),
        0, false, true);
    physicalPoolMap_[0x11] = PhysicalPool(0x11, "pPool1", 0X61, "des1");
    zoneMap_[0x21] = Zone(0x21, "zone1", 0x11, "desc1");
    serverMap_[0x31] = Server(0x31, "server1", "127.0.0.1", 8200,
        "127.0.0.1", 8200, 0x21, 0x11, "desc1");
    chunkServerMap_[0x41] = ChunkServer(0x41, "token", "ssd",
        0x31, "127.0.0.1", 8200, "/");
    copySetMap_[std::pair<PoolIdType, CopySetIdType>(0x01, 0x51)] =
        CopySetInfo(0x01, 0x51);

    EXPECT_CALL(*storage_, LoadPoolset(_, _))
        .WillOnce(DoAll(SetArgPointee<0>(poolsetMap),
                    Return(true)));
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

    EXPECT_CALL(*idGenerator_, initPoolsetIdGenerator(_));
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
    int ret = topology_->Init(option);
    ASSERT_EQ(kTopoErrCodeSuccess, ret);
}

TEST_F(TestTopology, test_init_loadClusterFail) {
    std::vector<ClusterInformation> infos;
    EXPECT_CALL(*storage_, LoadClusterInfo(_))
        .WillOnce(DoAll(SetArgPointee<0>(infos),
                Return(false)));

    TopologyOption option;
    int ret = topology_->Init(option);
    ASSERT_EQ(kTopoErrCodeStorgeFail, ret);
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
    ASSERT_EQ(kTopoErrCodeStorgeFail, ret);
}

TEST_F(TestTopology, test_init_loadLogicalPoolFail) {
    std::vector<ClusterInformation> infos;
    ClusterInformation info("uuid1");
    infos.push_back(info);
    EXPECT_CALL(*storage_, LoadClusterInfo(_))
        .WillOnce(DoAll(SetArgPointee<0>(infos),
                Return(true)));

    EXPECT_CALL(*storage_, LoadLogicalPool(_, _))
        .WillOnce(Return(false));

    TopologyOption option;
    int ret = topology_->Init(option);
    ASSERT_EQ(kTopoErrCodeStorgeFail, ret);
}

TEST_F(TestTopology, test_init_LoadPhysicalPoolFail) {
    std::vector<ClusterInformation> infos;
    ClusterInformation info("uuid1");
    infos.push_back(info);
    EXPECT_CALL(*storage_, LoadClusterInfo(_))
        .WillOnce(DoAll(SetArgPointee<0>(infos),
                Return(true)));

    EXPECT_CALL(*storage_, LoadLogicalPool(_, _))
        .WillOnce(Return(true));
    EXPECT_CALL(*storage_, LoadPhysicalPool(_, _))
        .WillOnce(Return(false));

    EXPECT_CALL(*idGenerator_, initLogicalPoolIdGenerator(_));

    TopologyOption option;
    int ret = topology_->Init(option);
    ASSERT_EQ(kTopoErrCodeStorgeFail, ret);
}

TEST_F(TestTopology, test_init_LoadZoneFail) {
    std::vector<ClusterInformation> infos;
    ClusterInformation info("uuid1");
    infos.push_back(info);
    EXPECT_CALL(*storage_, LoadClusterInfo(_))
        .WillOnce(DoAll(SetArgPointee<0>(infos),
                Return(true)));

    EXPECT_CALL(*storage_, LoadLogicalPool(_, _))
        .WillOnce(Return(true));
    EXPECT_CALL(*storage_, LoadPhysicalPool(_, _))
        .WillOnce(Return(true));
    EXPECT_CALL(*storage_, LoadZone(_, _))
        .WillOnce(Return(false));

    EXPECT_CALL(*idGenerator_, initLogicalPoolIdGenerator(_));
    EXPECT_CALL(*idGenerator_, initPhysicalPoolIdGenerator(_));

    TopologyOption option;
    int ret = topology_->Init(option);
    ASSERT_EQ(kTopoErrCodeStorgeFail, ret);
}

TEST_F(TestTopology, test_init_LoadServerFail) {
    std::vector<ClusterInformation> infos;
    ClusterInformation info("uuid1");
    infos.push_back(info);
    EXPECT_CALL(*storage_, LoadClusterInfo(_))
        .WillOnce(DoAll(SetArgPointee<0>(infos),
                Return(true)));
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
    int ret = topology_->Init(option);
    ASSERT_EQ(kTopoErrCodeStorgeFail, ret);
}

TEST_F(TestTopology, test_init_LoadChunkServerFail) {
    std::vector<ClusterInformation> infos;
    ClusterInformation info("uuid1");
    infos.push_back(info);
    EXPECT_CALL(*storage_, LoadClusterInfo(_))
        .WillOnce(DoAll(SetArgPointee<0>(infos),
                Return(true)));

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
    int ret = topology_->Init(option);
    ASSERT_EQ(kTopoErrCodeStorgeFail, ret);
}

TEST_F(TestTopology, test_init_LoadCopysetFail) {
    std::vector<ClusterInformation> infos;
    ClusterInformation info("uuid1");
    infos.push_back(info);
    EXPECT_CALL(*storage_, LoadClusterInfo(_))
        .WillOnce(DoAll(SetArgPointee<0>(infos),
                Return(true)));

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
    int ret = topology_->Init(option);
    ASSERT_EQ(kTopoErrCodeStorgeFail, ret);
}

TEST_F(TestTopology, test_AddLogicalPool_success) {
    PrepareAddPoolset();
    PoolIdType physicalPoolId = 0x11;
    PrepareAddPhysicalPool(physicalPoolId);
    LogicalPool pool(0x01,
            "test1",
            physicalPoolId,
            PAGEFILE,
            LogicalPool::RedundanceAndPlaceMentPolicy(),
            LogicalPool::UserPolicy(),
            0,
            true,
            true);

    EXPECT_CALL(*storage_, StorageLogicalPool(_))
        .WillOnce(Return(true));

    int ret = topology_->AddLogicalPool(pool);

    ASSERT_EQ(kTopoErrCodeSuccess, ret);
}

TEST_F(TestTopology, test_AddLogicalPool_IdDuplicated) {
    PrepareAddPoolset();
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
            true,
            true);

    int ret = topology_->AddLogicalPool(pool);

    ASSERT_EQ(kTopoErrCodeIdDuplicated, ret);
}

TEST_F(TestTopology, test_AddLogicalPool_StorageFail) {
    PrepareAddPoolset();
    PoolIdType physicalPoolId = 0x11;
    PrepareAddPhysicalPool(physicalPoolId);
    LogicalPool pool(0x01,
            "test1",
            physicalPoolId,
            PAGEFILE,
            LogicalPool::RedundanceAndPlaceMentPolicy(),
            LogicalPool::UserPolicy(),
            0,
            true,
            true);

    EXPECT_CALL(*storage_, StorageLogicalPool(_))
        .WillOnce(Return(false));

    int ret = topology_->AddLogicalPool(pool);

    ASSERT_EQ(kTopoErrCodeStorgeFail, ret);
}

TEST_F(TestTopology, test_AddLogicalPool_PhysicalPoolNotFound) {
    PrepareAddPoolset();
    PoolIdType physicalPoolId = 0x11;
    PrepareAddPhysicalPool(physicalPoolId);
    LogicalPool pool(0x01,
            "test1",
            ++physicalPoolId,
            PAGEFILE,
            LogicalPool::RedundanceAndPlaceMentPolicy(),
            LogicalPool::UserPolicy(),
            0,
            true,
            true);


    int ret = topology_->AddLogicalPool(pool);

    ASSERT_EQ(kTopoErrCodePhysicalPoolNotFound, ret);
}

TEST_F(TestTopology, test_AddPhysicalPool_success) {
    PrepareAddPoolset();
    PhysicalPool pool(0x11,
            "test1",
            0X61,
            "desc");
    EXPECT_CALL(*storage_, StoragePhysicalPool(_))
        .WillOnce(Return(true));

    int ret = topology_->AddPhysicalPool(pool);
    ASSERT_EQ(kTopoErrCodeSuccess, ret);
}


TEST_F(TestTopology, test_AddPhysicalPool_IdDuplicated) {
    PrepareAddPoolset();
    PoolIdType id = 0x11;
    PoolsetIdType pid = 0x61;
    PhysicalPool pool(id,
            "test1",
            pid,
            "desc");
    PrepareAddPhysicalPool(id);
    int ret = topology_->AddPhysicalPool(pool);
    ASSERT_EQ(kTopoErrCodeIdDuplicated, ret);
}

TEST_F(TestTopology, test_AddPhysicalPool_StorageFail) {
    PrepareAddPoolset();
    PhysicalPool pool(0x11,
            "test1",
            0X61,
            "desc");
    EXPECT_CALL(*storage_, StoragePhysicalPool(_))
        .WillOnce(Return(false));

    int ret = topology_->AddPhysicalPool(pool);
    ASSERT_EQ(kTopoErrCodeStorgeFail, ret);
}

TEST_F(TestTopology, test_AddZone_success) {
    PrepareAddPoolset();
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
    PrepareAddPoolset();
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
    PrepareAddPoolset();
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
    PrepareAddPoolset();
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
    PrepareAddPoolset();
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
    PrepareAddPoolset();
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
    PrepareAddPoolset();
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
    PrepareAddPoolset();
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
    PrepareAddPoolset();
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
    ChunkServerState state;
    state.SetDiskCapacity(1024);
    state.SetDiskUsed(512);
    cs.SetChunkServerState(state);

    EXPECT_CALL(*storage_, StorageChunkServer(_))
        .WillOnce(Return(true));

    int ret = topology_->AddChunkServer(cs);

    ASSERT_EQ(kTopoErrCodeSuccess, ret);

    Server server;
    ASSERT_TRUE(topology_->GetServer(serverId, &server));
    std::list<ChunkServerIdType> csList = server.GetChunkServerList();

    auto it = std::find(csList.begin(), csList.end(), csId);
    ASSERT_TRUE(it != csList.end());

    PhysicalPool pool;
    ASSERT_TRUE(topology_->GetPhysicalPool(0x11, &pool));
    ASSERT_EQ(1024, pool.GetDiskCapacity());
}

TEST_F(TestTopology, test_AddChunkServer_IdDuplicated) {
    PrepareAddPoolset();
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
    PrepareAddPoolset();
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
    PrepareAddPoolset();
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
    PrepareAddPoolset();
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
    PrepareAddPoolset();
    PoolIdType id = 0x01;

    int ret = topology_->RemoveLogicalPool(id);

    ASSERT_EQ(kTopoErrCodeLogicalPoolNotFound, ret);
}

TEST_F(TestTopology, test_RemoveLogicalPool_StorageFail) {
    PrepareAddPoolset();
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
    PrepareAddPoolset();
    PoolIdType poolId = 0x11;
    PrepareAddPhysicalPool(poolId);

    EXPECT_CALL(*storage_, DeletePhysicalPool(_))
        .WillOnce(Return(true));

    int ret = topology_->RemovePhysicalPool(poolId);

    ASSERT_EQ(kTopoErrCodeSuccess, ret);
}

TEST_F(TestTopology, test_RemovePhysicalPool_PhysicalPoolNotFound) {
    PrepareAddPoolset();
    PoolIdType poolId = 0x11;

    int ret = topology_->RemovePhysicalPool(poolId);

    ASSERT_EQ(kTopoErrCodePhysicalPoolNotFound, ret);
}

TEST_F(TestTopology, test_RemovePhysicalPool_StorageFail) {
    PrepareAddPoolset();
    PoolIdType poolId = 0x11;
    PrepareAddPhysicalPool(poolId);

    EXPECT_CALL(*storage_, DeletePhysicalPool(_))
        .WillOnce(Return(false));

    int ret = topology_->RemovePhysicalPool(poolId);

    ASSERT_EQ(kTopoErrCodeStorgeFail, ret);
}

TEST_F(TestTopology, test_RemoveZone_success) {
    PrepareAddPoolset();
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
    PrepareAddPoolset();
    ZoneIdType zoneId = 0x21;
    PrepareAddPhysicalPool();
    PrepareAddZone(zoneId);

    EXPECT_CALL(*storage_, DeleteZone(_))
        .WillOnce(Return(false));

    int ret = topology_->RemoveZone(zoneId);
    ASSERT_EQ(kTopoErrCodeStorgeFail, ret);
}

TEST_F(TestTopology, test_RemoveServer_success) {
    PrepareAddPoolset();
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
    PrepareAddPoolset();
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
    PrepareAddPoolset();
    PrepareAddPhysicalPool();
    PrepareAddZone();
    PrepareAddServer(serverId);
    PrepareAddChunkServer(csId,
            "token",
            "ssd",
            serverId);

    int ret = topology_->UpdateChunkServerRwState(
        ChunkServerStatus::RETIRED, csId);

    ASSERT_EQ(kTopoErrCodeSuccess, ret);

    EXPECT_CALL(*storage_, DeleteChunkServer(_))
        .WillOnce(Return(true));

    ret = topology_->RemoveChunkServer(csId);
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
    PrepareAddPoolset();
    PrepareAddPhysicalPool();
    PrepareAddZone();
    PrepareAddServer(serverId);
    PrepareAddChunkServer(csId,
            "token",
            "ssd",
            serverId);

    int ret = topology_->UpdateChunkServerRwState(
        ChunkServerStatus::RETIRED, csId);

    ASSERT_EQ(kTopoErrCodeSuccess, ret);


    EXPECT_CALL(*storage_, DeleteChunkServer(_))
        .WillOnce(Return(false));

    ret = topology_->RemoveChunkServer(csId);
    ASSERT_EQ(kTopoErrCodeStorgeFail, ret);
}

TEST_F(TestTopology, UpdateLogicalPool_success) {
    PrepareAddPoolset();
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
            true,
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
            true,
            true);

    int ret = topology_->UpdateLogicalPool(pool);

    ASSERT_EQ(kTopoErrCodeLogicalPoolNotFound, ret);
}

TEST_F(TestTopology, UpdateLogicalPool_StorageFail) {
    PrepareAddPoolset();
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
            true,
            true);

    EXPECT_CALL(*storage_, UpdateLogicalPool(_))
        .WillOnce(Return(false));

    int ret = topology_->UpdateLogicalPool(pool);

    ASSERT_EQ(kTopoErrCodeStorgeFail, ret);
}

TEST_F(TestTopology, UpdateLogicalPoolAllocateStatus_success) {
    PrepareAddPoolset();
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

    LogicalPool pool2;
    topology_->GetLogicalPool(logicalPoolId, &pool2);
    ASSERT_EQ(AllocateStatus::ALLOW, pool2.GetStatus());

    // update to deny
    EXPECT_CALL(*storage_, UpdateLogicalPool(_))
        .WillOnce(Return(true));

    int ret = topology_->UpdateLogicalPoolAllocateStatus(
        AllocateStatus::DENY, logicalPoolId);

    ASSERT_EQ(kTopoErrCodeSuccess, ret);

    LogicalPool pool3;
    topology_->GetLogicalPool(logicalPoolId, &pool3);
    ASSERT_EQ(AllocateStatus::DENY, pool3.GetStatus());

    // update to allow
    EXPECT_CALL(*storage_, UpdateLogicalPool(_))
        .WillOnce(Return(true));

    ret = topology_->UpdateLogicalPoolAllocateStatus(
        AllocateStatus::ALLOW, logicalPoolId);

    ASSERT_EQ(kTopoErrCodeSuccess, ret);

    LogicalPool pool4;
    topology_->GetLogicalPool(logicalPoolId, &pool4);
    ASSERT_EQ(AllocateStatus::ALLOW, pool4.GetStatus());
}

TEST_F(TestTopology, UpdateLogicalPoolAllocateStatus_LogicalPoolNotFound) {
    PoolIdType logicalPoolId = 0x01;
    PoolIdType physicalPoolId = 0x11;
    LogicalPool pool(logicalPoolId,
            "name1",
            physicalPoolId,
            APPENDFILE,
            LogicalPool::RedundanceAndPlaceMentPolicy(),
            LogicalPool::UserPolicy(),
            0,
            true,
            true);

    int ret = topology_->UpdateLogicalPoolAllocateStatus(
        AllocateStatus::ALLOW, logicalPoolId);

    ASSERT_EQ(kTopoErrCodeLogicalPoolNotFound, ret);
}

TEST_F(TestTopology, UpdateLogicalPoolAllocateStatus_StorageFail) {
    PrepareAddPoolset();
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

    EXPECT_CALL(*storage_, UpdateLogicalPool(_))
        .WillOnce(Return(false));

    int ret = topology_->UpdateLogicalPoolAllocateStatus(
        AllocateStatus::ALLOW, logicalPoolId);

    ASSERT_EQ(kTopoErrCodeStorgeFail, ret);
}

TEST_F(TestTopology, TestUpdateLogicalPoolScanState) {
    PrepareAddPoolset();
    PoolIdType lpid = 1;  // logicalPoolId
    PoolIdType ppid = 1;  // physicalPoolId
    PrepareAddPhysicalPool(ppid);
    PrepareAddLogicalPool(lpid, "name", ppid);

    auto set_state = [&](PoolIdType lpid, bool scanEnable) {
        EXPECT_CALL(*storage_, UpdateLogicalPool(_))
            .WillOnce(Return(true));
        auto retCode = topology_->UpdateLogicalPoolScanState(lpid, scanEnable);
        ASSERT_EQ(retCode, kTopoErrCodeSuccess);
    };

    auto check_state = [&](PoolIdType lpid, bool scanEnable) {
        LogicalPool lpool;
        ASSERT_EQ(topology_->GetLogicalPool(lpid, &lpool), true);
        ASSERT_EQ(lpool.ScanEnable(), scanEnable);
    };

    // CASE 1: default scan state is enable
    check_state(lpid, true);

    // CASE 2: set scan state to disable
    set_state(lpid, false);
    check_state(lpid, false);

    // CASE 3: set scan state to enable
    set_state(lpid, true);
    check_state(lpid, true);

    // CASE 4: logical pool not found -> set scan state fail
    EXPECT_CALL(*storage_, UpdateLogicalPool(_))
        .Times(0);
    auto retCode = topology_->UpdateLogicalPoolScanState(lpid + 1, true);
    ASSERT_EQ(retCode, kTopoErrCodeLogicalPoolNotFound);

    // CASE 5: update storage fail -> set scan state fail
    EXPECT_CALL(*storage_, UpdateLogicalPool(_))
        .WillOnce(Return(false));
    retCode = topology_->UpdateLogicalPoolScanState(lpid, true);
    ASSERT_EQ(retCode, kTopoErrCodeStorgeFail);
}

TEST_F(TestTopology, UpdatePhysicalPool_success) {
    PrepareAddPoolset();
    PoolIdType physicalPoolId = 0x11;
    PoolsetIdType poolsetId = 0x61;
    PrepareAddPhysicalPool(physicalPoolId,
            "name1",
             poolsetId,
            "desc1");

    PhysicalPool newPool(physicalPoolId,
            "name1",
            poolsetId,
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
    PoolIdType pid = 0x61;
    PhysicalPool newPool(physicalPoolId,
            "name1",
            pid,
            "desc2");

    int ret = topology_->UpdatePhysicalPool(newPool);
    ASSERT_EQ(kTopoErrCodePhysicalPoolNotFound, ret);
}


TEST_F(TestTopology, UpdatePhysicalPool_StorageFail) {
    PrepareAddPoolset();
    PoolIdType physicalPoolId = 0x11;
    PoolsetIdType poolsetId = 0x61;
    PrepareAddPhysicalPool(physicalPoolId,
            "name1",
            poolsetId,
            "desc1");

    PhysicalPool newPool(physicalPoolId,
            "name1",
            poolsetId,
            "desc2");

    EXPECT_CALL(*storage_, UpdatePhysicalPool(_))
        .WillOnce(Return(false));

    int ret = topology_->UpdatePhysicalPool(newPool);
    ASSERT_EQ(kTopoErrCodeStorgeFail, ret);
}



TEST_F(TestTopology, UpdateZone_success) {
    PrepareAddPoolset();
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
    PrepareAddPoolset();
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
    PrepareAddPoolset();
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
    PrepareAddPoolset();
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
    PrepareAddPoolset();
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
    PrepareAddPoolset();
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
    PrepareAddPoolset();
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
    PrepareAddPoolset();
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

    PhysicalPool pool;
    ASSERT_TRUE(topology_->GetPhysicalPool(0x11, &pool));
    ASSERT_EQ(1024, pool.GetDiskCapacity());

    ChunkServerState csState;
    csState.SetDiskState(DISKERROR);
    csState.SetDiskCapacity(100);

    int ret = topology_->UpdateChunkServerDiskStatus(csState,  csId);
    ASSERT_EQ(kTopoErrCodeSuccess, ret);

    ASSERT_TRUE(topology_->GetPhysicalPool(0x11, &pool));
    ASSERT_EQ(100, pool.GetDiskCapacity());

    // Only brush once
    EXPECT_CALL(*storage_, UpdateChunkServer(_))
        .WillOnce(Return(true));
    topology_->Run();
    // Sleep waiting to flush the database
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

TEST_F(TestTopology, UpdateChunkServerRwStateToStorage_success) {
    PrepareAddPoolset();
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

    ChunkServerStatus rwState;
    rwState = ChunkServerStatus::PENDDING;
    int ret = topology_->UpdateChunkServerRwState(rwState,  csId);
    ASSERT_EQ(kTopoErrCodeSuccess, ret);

    // Only brush once
    EXPECT_CALL(*storage_, UpdateChunkServer(_))
        .WillOnce(Return(true));
    topology_->Run();
    // Sleep waiting to flush the database
    sleep(5);
    topology_->Stop();
}

TEST_F(TestTopology, UpdateChunkServerRwStateTestPhysicalPoolCapacity_success) {
    PrepareAddPoolset();
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

    PhysicalPool pool;
    ASSERT_TRUE(topology_->GetPhysicalPool(0x11, &pool));
    ASSERT_EQ(1024, pool.GetDiskCapacity());

    // READWRITE -> RETIRED
    ASSERT_EQ(kTopoErrCodeSuccess,
        topology_->UpdateChunkServerRwState(
            ChunkServerStatus::RETIRED,  csId));

    ASSERT_TRUE(topology_->GetPhysicalPool(0x11, &pool));
    ASSERT_EQ(0, pool.GetDiskCapacity());

    // RETIRED -> PENDDING
    ASSERT_EQ(kTopoErrCodeSuccess,
        topology_->UpdateChunkServerRwState(
            ChunkServerStatus::PENDDING,  csId));

    ASSERT_TRUE(topology_->GetPhysicalPool(0x11, &pool));
    ASSERT_EQ(1024, pool.GetDiskCapacity());

    // PENDDING -> RETIRED
    ASSERT_EQ(kTopoErrCodeSuccess,
        topology_->UpdateChunkServerRwState(
            ChunkServerStatus::RETIRED,  csId));

    ASSERT_TRUE(topology_->GetPhysicalPool(0x11, &pool));
    ASSERT_EQ(0, pool.GetDiskCapacity());

    // RETIRED -> READWRITE
    ASSERT_EQ(kTopoErrCodeSuccess,
        topology_->UpdateChunkServerRwState(
            ChunkServerStatus::READWRITE,  csId));

    ASSERT_TRUE(topology_->GetPhysicalPool(0x11, &pool));
    ASSERT_EQ(1024, pool.GetDiskCapacity());

    // READWRITE -> PENDDING
    ASSERT_EQ(kTopoErrCodeSuccess,
        topology_->UpdateChunkServerRwState(
            ChunkServerStatus::PENDDING,  csId));

    ASSERT_TRUE(topology_->GetPhysicalPool(0x11, &pool));
    ASSERT_EQ(1024, pool.GetDiskCapacity());

    // PENDDING -> READWRITE
    ASSERT_EQ(kTopoErrCodeSuccess,
        topology_->UpdateChunkServerRwState(
            ChunkServerStatus::READWRITE,  csId));

    ASSERT_TRUE(topology_->GetPhysicalPool(0x11, &pool));
    ASSERT_EQ(1024, pool.GetDiskCapacity());
}

TEST_F(TestTopology, UpdateChunkServerRwState_ChunkServerNotFound) {
    ChunkServerIdType csId = 0x41;

    ChunkServerStatus rwState;
    rwState = ChunkServerStatus::PENDDING;
    int ret = topology_->UpdateChunkServerRwState(rwState,  csId);
    ASSERT_EQ(kTopoErrCodeChunkServerNotFound, ret);
}

TEST_F(TestTopology, UpdateChunkServerStartUpTime_success) {
    PrepareAddPoolset();
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
    uint64_t time = 0x1234567812345678;
    int ret = topology_->UpdateChunkServerStartUpTime(time,  csId);
    ASSERT_EQ(kTopoErrCodeSuccess, ret);

    ChunkServer cs;
    topology_->GetChunkServer(csId, &cs);
    ASSERT_EQ(time, cs.GetStartUpTime());
}

TEST_F(TestTopology, UpdateChunkServerStartUpTime_ChunkServerNotFound) {
    ChunkServerIdType csId = 0x41;
    int ret = topology_->UpdateChunkServerStartUpTime(1000,  csId);
    ASSERT_EQ(kTopoErrCodeChunkServerNotFound, ret);
}

TEST_F(TestTopology, FindLogicalPool_success) {
    PrepareAddPoolset();
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
    PrepareAddPoolset();
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
    PrepareAddPoolset();
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
    PrepareAddPoolset();
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
    PrepareAddPoolset();
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
    PrepareAddPoolset();
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
    PrepareAddPoolset();
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
    PrepareAddPoolset();
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
    PrepareAddPoolset();
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
    PrepareAddPoolset();
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
    PrepareAddPoolset();
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
    PrepareAddPoolset();
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
    PrepareAddPoolset();
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
    PrepareAddPoolset();
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
    PrepareAddPoolset();
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
    PrepareAddPoolset();
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

    PrepareAddPoolset();
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

    PrepareAddPoolset();
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

    PrepareAddPoolset();
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

    PrepareAddPoolset();
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

    PrepareAddPoolset();
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

    PrepareAddPoolset();
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

    PrepareAddPoolset();
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

    PrepareAddPoolset();
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

    PrepareAddPoolset();
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

    PrepareAddPoolset();
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

    PrepareAddPoolset();
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

    PrepareAddPoolset();
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

    PrepareAddPoolset();
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

    PrepareAddPoolset();
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

    PrepareAddPoolset();
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

    PrepareAddPoolset();
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

    PrepareAddPoolset();
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

    PrepareAddPoolset();
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

    PrepareAddPoolset();
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

    PrepareAddPoolset();
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

    PrepareAddPoolset();
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

    PrepareAddPoolset();
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

    PrepareAddPoolset();
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

    // Only brush once
    EXPECT_CALL(*storage_, UpdateCopySet(_))
        .WillOnce(Return(true));
    topology_->Run();
    // Sleep waiting to flush the database
    sleep(5);
    topology_->Stop();
}

TEST_F(TestTopology, UpdateCopySetTopo_CopySetNotFound) {
    PoolIdType logicalPoolId = 0x01;
    PoolIdType physicalPoolId = 0x11;
    CopySetIdType copysetId = 0x51;

    PrepareAddPoolset();
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

    PrepareAddPoolset();
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

    PrepareAddPoolset();
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

    PrepareAddPoolset();
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

    PrepareAddPoolset();
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
    PrepareAddPoolset();
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

TEST_F(TestTopology, test_create_default_poolset) {
    EXPECT_CALL(*storage_, LoadClusterInfo(_))
        .WillOnce(Return(true));
    EXPECT_CALL(*storage_, StorageClusterInfo(_))
        .WillOnce(Return(true));

    EXPECT_CALL(*storage_, LoadPoolset(_, _))
        .WillOnce(Return(true));

    Poolset poolset;
    EXPECT_CALL(*storage_, StoragePoolset(_))
        .WillOnce(
            DoAll(SaveArg<0>(&poolset), Return(true)));

    std::unordered_map<PoolIdType, PhysicalPool> physicalPoolMap{
        {1, {1, "pool1", UNINTIALIZE_ID, ""}},
        {2, {2, "pool2", UNINTIALIZE_ID, ""}},
    };
    EXPECT_CALL(*storage_, LoadPhysicalPool(_, _))
        .WillOnce(DoAll(SetArgPointee<0>(physicalPoolMap),
                        SetArgPointee<1>(2),
                        Return(true)));

    EXPECT_CALL(*storage_, LoadLogicalPool(_, _))
        .WillOnce(Return(true));
    EXPECT_CALL(*storage_, LoadZone(_, _))
        .WillOnce(Return(true));
    EXPECT_CALL(*storage_, LoadServer(_, _))
        .WillOnce(Return(true));
    EXPECT_CALL(*storage_, LoadChunkServer(_, _))
        .WillOnce(Return(true));
    EXPECT_CALL(*storage_, LoadCopySet(_, _))
        .WillOnce(Return(true));

    int rc = topology_->Init({});
    ASSERT_EQ(kTopoErrCodeSuccess, rc);

    ASSERT_EQ(curve::common::kDefaultPoolsetId, poolset.GetId());
    ASSERT_EQ(curve::common::kDefaultPoolsetName, poolset.GetName());

    auto poolsets = topology_->GetPoolsetInCluster();
    auto physicals = topology_->GetPhysicalPoolInCluster();
    EXPECT_EQ(1, poolsets.size());
    EXPECT_EQ(2, physicals.size());
    EXPECT_EQ(2, topology_->GetPhysicalPoolInPoolset(poolsets[0]).size());
}

}  // namespace topology
}  // namespace mds
}  // namespace curve
