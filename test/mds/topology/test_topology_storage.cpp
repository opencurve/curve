/*
 * Project: curve
 * Created Date: Thu Oct 18 2018
 * Author: xuchaojie
 * Copyright (c) 2018 netease
 */

#include <gtest/gtest.h>

#include <memory>

#include "test/mds/topology/mock_topology.h"
#include "src/mds/topology/topology_storge.h"
#include "src/mds/dao/mdsRepo.h"
#include "json/json.h"

namespace curve {
namespace mds {
namespace topology {

using ::curve::mds::topology::MockRepo;

using ::curve::repo::OperationOK;
using ::curve::repo::SqlException;
using ::testing::SetArgPointee;

class TestTopologyStorage : public ::testing::Test {
 protected:
    TestTopologyStorage() {}
    ~TestTopologyStorage() {}


    virtual void SetUp() {
        repo_ = std::make_shared<MockRepo>();
        storage_ = std::make_shared<DefaultTopologyStorage>(repo_);
    }

    virtual void TearDown() {
        repo_ = nullptr;
    }

 protected:
    std::shared_ptr<MockRepo> repo_;
    std::shared_ptr<DefaultTopologyStorage> storage_;
};

TEST_F(TestTopologyStorage, test_LoadLogicalPool_success) {
    std::unordered_map<PoolIdType, LogicalPool> logicalPoolMap;
    PoolIdType maxLogicalPoolId = 0;

    std::vector<LogicalPoolRepoItem> logicalPoolRepos;
    LogicalPoolRepoItem data1(0x01,
        "lPool1",
        0x11,
        PAGEFILE,
        100,
        LogicalPool::ALLOCATABLE,
        "{\"replicaNum\":3, \"copysetNum\":3, \"zoneNum\":3}",
        "",
        true);
    logicalPoolRepos.push_back(data1);

    EXPECT_CALL(*repo_, LoadLogicalPoolRepoItems(_))
        .WillOnce(DoAll(SetArgPointee<0>(logicalPoolRepos),
                    Return(OperationOK)));

    int ret = storage_->LoadLogicalPool(&logicalPoolMap, &maxLogicalPoolId);

    ASSERT_TRUE(ret);
    ASSERT_EQ(1, logicalPoolMap.size());
    ASSERT_EQ(0x01, logicalPoolMap[0x01].GetId());
    ASSERT_STREQ("lPool1", logicalPoolMap[0x01].GetName().c_str());
    ASSERT_EQ(0x11, logicalPoolMap[0x01].GetPhysicalPoolId());
    ASSERT_EQ(PAGEFILE, logicalPoolMap[0x01].GetLogicalPoolType());
    ASSERT_EQ(100, logicalPoolMap[0x01].GetCreateTime());
    ASSERT_EQ(LogicalPool::ALLOCATABLE, logicalPoolMap[0x01].GetStatus());
    ASSERT_STREQ(
    "{\n\t\"copysetNum\" : 3,\n\t\"replicaNum\" : 3,\n\t\"zoneNum\" : 3\n}\n",
        logicalPoolMap[0x01].GetRedundanceAndPlaceMentPolicyJsonStr().c_str());
    ASSERT_EQ(0x01, maxLogicalPoolId);
}

TEST_F(TestTopologyStorage, test_LoadLogicalPool_IdDuplicated) {
    std::unordered_map<PoolIdType, LogicalPool> logicalPoolMap;
    PoolIdType maxLogicalPoolId = 0;

    std::vector<LogicalPoolRepoItem> logicalPoolRepos;
    LogicalPoolRepoItem data1(0x01,
        "lPool1",
        0x11,
        PAGEFILE,
        100,
        LogicalPool::ALLOCATABLE,
        "{\"replicaNum\":3, \"copysetNum\":3, \"zoneNum\":3}",
        "",
        true);
    logicalPoolRepos.push_back(data1);
    logicalPoolRepos.push_back(data1);

    EXPECT_CALL(*repo_, LoadLogicalPoolRepoItems(_))
        .WillOnce(DoAll(SetArgPointee<0>(logicalPoolRepos),
                    Return(OperationOK)));

    int ret = storage_->LoadLogicalPool(&logicalPoolMap, &maxLogicalPoolId);

    ASSERT_FALSE(ret);
}

TEST_F(TestTopologyStorage, test_LoadLogicalPool_fail) {
    std::unordered_map<PoolIdType, LogicalPool> logicalPoolMap;
    PoolIdType maxLogicalPoolId = 0;

    EXPECT_CALL(*repo_, LoadLogicalPoolRepoItems(_))
        .WillOnce(Return(SqlException));

    int ret = storage_->LoadLogicalPool(&logicalPoolMap, &maxLogicalPoolId);

    ASSERT_FALSE(ret);
}

TEST_F(TestTopologyStorage, test_LoadLogicalPool_ParseRapJsonFail) {
    std::unordered_map<PoolIdType, LogicalPool> logicalPoolMap;
    PoolIdType maxLogicalPoolId = 0;

    std::vector<LogicalPoolRepoItem> logicalPoolRepos;
    LogicalPoolRepoItem data1(0x01,
        "lPool1",
        0x11,
        0,
        0,
        0,
        "{\"replicaNum\":3, \"copysetNum\":3}",
        "",
        true);
    logicalPoolRepos.push_back(data1);

    EXPECT_CALL(*repo_, LoadLogicalPoolRepoItems(_))
        .WillOnce(DoAll(SetArgPointee<0>(logicalPoolRepos),
                    Return(OperationOK)));

    int ret = storage_->LoadLogicalPool(&logicalPoolMap, &maxLogicalPoolId);

    ASSERT_FALSE(ret);
}


TEST_F(TestTopologyStorage, test_LoadPhysicalPool_success) {
    std::unordered_map<PoolIdType, PhysicalPool> physicalPoolMap;
    PoolIdType maxPhysicalPoolId = 0;
    std::vector<PhysicalPoolRepoItem> physicalPoolRepos;
    PhysicalPoolRepoItem data(0x11,
        "pPool1",
        "desc");
    physicalPoolRepos.push_back(data);
    EXPECT_CALL(*repo_, LoadPhysicalPoolRepoItems(_))
        .WillOnce(DoAll(SetArgPointee<0>(physicalPoolRepos),
                    Return(OperationOK)));

    int ret = storage_->LoadPhysicalPool(&physicalPoolMap, &maxPhysicalPoolId);

    ASSERT_TRUE(ret);
    ASSERT_EQ(1, physicalPoolMap.size());
    ASSERT_EQ(0x11, physicalPoolMap[0x11].GetId());
    ASSERT_STREQ("pPool1", physicalPoolMap[0x11].GetName().c_str());
    ASSERT_STREQ("desc", physicalPoolMap[0x11].GetDesc().c_str());
    ASSERT_EQ(0x11, maxPhysicalPoolId);
}

TEST_F(TestTopologyStorage, test_LoadPhysicalPool_IdDuplicated) {
    std::unordered_map<PoolIdType, PhysicalPool> physicalPoolMap;
    PoolIdType maxPhysicalPoolId = 0;
    std::vector<PhysicalPoolRepoItem> physicalPoolRepos;
    PhysicalPoolRepoItem data(0x11,
        "pPool1",
        "desc");
    physicalPoolRepos.push_back(data);
    physicalPoolRepos.push_back(data);
    EXPECT_CALL(*repo_, LoadPhysicalPoolRepoItems(_))
        .WillOnce(DoAll(SetArgPointee<0>(physicalPoolRepos),
                    Return(OperationOK)));

    int ret = storage_->LoadPhysicalPool(&physicalPoolMap, &maxPhysicalPoolId);

    ASSERT_FALSE(ret);
}

TEST_F(TestTopologyStorage, test_LoadPhysicalPool_fail) {
    std::unordered_map<PoolIdType, PhysicalPool> physicalPoolMap;
    PoolIdType maxPhysicalPoolId = 0;

    EXPECT_CALL(*repo_, LoadPhysicalPoolRepoItems(_))
        .WillOnce(Return(SqlException));

    int ret = storage_->LoadPhysicalPool(&physicalPoolMap, &maxPhysicalPoolId);

    ASSERT_FALSE(ret);
}

TEST_F(TestTopologyStorage, test_LoadZone_success) {
    std::unordered_map<ZoneIdType, Zone> zoneMap;
    ZoneIdType maxZoneId = 0;
    std::vector<ZoneRepoItem> zoneRepos;
    ZoneRepoItem data(0x21,
        "zone1",
        0x11,
        "desc");
    zoneRepos.push_back(data);
    EXPECT_CALL(*repo_, LoadZoneRepoItems(_))
        .WillOnce(DoAll(SetArgPointee<0>(zoneRepos),
                Return(OperationOK)));

    int ret = storage_->LoadZone(&zoneMap, &maxZoneId);

    ASSERT_TRUE(ret);
    ASSERT_EQ(1, zoneMap.size());
    ASSERT_EQ(0x21, zoneMap[0x21].GetId());
    ASSERT_STREQ("zone1", zoneMap[0x21].GetName().c_str());
    ASSERT_EQ(0x11, zoneMap[0x21].GetPhysicalPoolId());
    ASSERT_STREQ("desc", zoneMap[0x21].GetDesc().c_str());
    ASSERT_EQ(0x21, maxZoneId);
}

TEST_F(TestTopologyStorage, test_LoadZone_IdDuplicated) {
    std::unordered_map<ZoneIdType, Zone> zoneMap;
    ZoneIdType maxZoneId = 0;
    std::vector<ZoneRepoItem> zoneRepos;
    ZoneRepoItem data(0x21,
        "zone1",
        0x11,
        "desc");
    zoneRepos.push_back(data);
    zoneRepos.push_back(data);
    EXPECT_CALL(*repo_, LoadZoneRepoItems(_))
        .WillOnce(DoAll(SetArgPointee<0>(zoneRepos),
                Return(OperationOK)));

    int ret = storage_->LoadZone(&zoneMap, &maxZoneId);

    ASSERT_FALSE(ret);
}

TEST_F(TestTopologyStorage, test_LoadZone_fail) {
    std::unordered_map<ZoneIdType, Zone> zoneMap;
    ZoneIdType maxZoneId = 0;

    EXPECT_CALL(*repo_, LoadZoneRepoItems(_))
        .WillOnce(Return(SqlException));

    int ret = storage_->LoadZone(&zoneMap, &maxZoneId);

    ASSERT_FALSE(ret);
}

TEST_F(TestTopologyStorage, test_LoadServer_success) {
    std::unordered_map<ServerIdType, Server> serverMap;
    ServerIdType maxServerId;
    std::vector<ServerRepoItem> serverRepos;
    ServerRepoItem data(0x31,
        "server1",
        "ip1",
        0,
        "ip2",
        0,
        0x21,
        0x11,
        "desc");
    serverRepos.push_back(data);
    EXPECT_CALL(*repo_, LoadServerRepoItems(_))
        .WillOnce(DoAll(SetArgPointee<0>(serverRepos),
                    Return(OperationOK)));

    int ret = storage_->LoadServer(&serverMap, &maxServerId);

    ASSERT_TRUE(ret);
    ASSERT_EQ(1, serverMap.size());
    ASSERT_EQ(0x31, serverMap[0x31].GetId());
    ASSERT_STREQ("server1", serverMap[0x31].GetHostName().c_str());
    ASSERT_STREQ("ip1", serverMap[0x31].GetInternalHostIp().c_str());
    ASSERT_STREQ("ip2", serverMap[0x31].GetExternalHostIp().c_str());
    ASSERT_EQ(0x21, serverMap[0x31].GetZoneId());
    ASSERT_EQ(0x11, serverMap[0x31].GetPhysicalPoolId());
    ASSERT_STREQ("desc", serverMap[0x31].GetDesc().c_str());
    ASSERT_EQ(0x31, maxServerId);
}

TEST_F(TestTopologyStorage, test_LoadServer_IdDuplicated) {
    std::unordered_map<ServerIdType, Server> serverMap;
    ServerIdType maxServerId;
    std::vector<ServerRepoItem> serverRepos;
    ServerRepoItem data(0x31,
        "server1",
        "ip1",
        0,
        "ip2",
        0,
        0x21,
        0x11,
        "desc");
    serverRepos.push_back(data);
    serverRepos.push_back(data);
    EXPECT_CALL(*repo_, LoadServerRepoItems(_))
        .WillOnce(DoAll(SetArgPointee<0>(serverRepos),
                    Return(OperationOK)));

    int ret = storage_->LoadServer(&serverMap, &maxServerId);

    ASSERT_FALSE(ret);
}

TEST_F(TestTopologyStorage, test_LoadServer_fail) {
    std::unordered_map<ServerIdType, Server> serverMap;
    ServerIdType maxServerId;

    EXPECT_CALL(*repo_, LoadServerRepoItems(_))
        .WillOnce(Return(SqlException));

    int ret = storage_->LoadServer(&serverMap, &maxServerId);

    ASSERT_FALSE(ret);
}

TEST_F(TestTopologyStorage, test_LoadChunkServer_success) {
    std::unordered_map<ChunkServerIdType, ChunkServer> chunkServerMap;
    ChunkServerIdType maxChunkServerId;
    std::vector<ChunkServerRepoItem> chunkServerRepos;
    ChunkServerRepoItem data(0x41,
        "token",
        "ssd",
        "ip1",
        1024,
        0x31,
        READWRITE,
        DISKNORMAL,
        ONLINE,
        "/",
        100,
        99);
    chunkServerRepos.push_back(data);
    EXPECT_CALL(*repo_, LoadChunkServerRepoItems(_))
        .WillOnce(DoAll(SetArgPointee<0>(chunkServerRepos),
                      Return(OperationOK)));

    int ret = storage_->LoadChunkServer(&chunkServerMap, &maxChunkServerId);
    ASSERT_TRUE(ret);
    ASSERT_EQ(1, chunkServerMap.size());
    ASSERT_EQ(0x41, chunkServerMap[0x41].GetId());
    ASSERT_STREQ("token", chunkServerMap[0x41].GetToken().c_str());
    ASSERT_STREQ("ssd", chunkServerMap[0x41].GetDiskType().c_str());
    ASSERT_EQ(0x31, chunkServerMap[0x41].GetServerId());
    ASSERT_STREQ("ip1", chunkServerMap[0x41].GetHostIp().c_str());
    ASSERT_EQ(1024, chunkServerMap[0x41].GetPort());
    ASSERT_STREQ("/", chunkServerMap[0x41].GetMountPoint().c_str());
    ASSERT_EQ(READWRITE, chunkServerMap[0x41].GetStatus());
    ASSERT_EQ(DISKNORMAL,
        chunkServerMap[0x41].GetChunkServerState().GetDiskState());
    ASSERT_EQ(ONLINE,
        chunkServerMap[0x41].GetOnlineState());
    ASSERT_EQ(100,
        chunkServerMap[0x41].GetChunkServerState().GetDiskCapacity());
    ASSERT_EQ(99,
        chunkServerMap[0x41].GetChunkServerState().GetDiskUsed());
    ASSERT_EQ(0x41, maxChunkServerId);
}

TEST_F(TestTopologyStorage, test_LoadChunkServer_IdDuplicated) {
    std::unordered_map<ChunkServerIdType, ChunkServer> chunkServerMap;
    ChunkServerIdType maxChunkServerId;
    std::vector<ChunkServerRepoItem> chunkServerRepos;
    ChunkServerRepoItem data(0x41,
        "token",
        "ssd",
        "ip1",
        1024,
        0x31,
        READWRITE,
        DISKNORMAL,
        ONLINE,
        "/",
        100,
        99);
    chunkServerRepos.push_back(data);
    chunkServerRepos.push_back(data);
    EXPECT_CALL(*repo_, LoadChunkServerRepoItems(_))
        .WillOnce(DoAll(SetArgPointee<0>(chunkServerRepos),
                      Return(OperationOK)));

    int ret = storage_->LoadChunkServer(&chunkServerMap, &maxChunkServerId);
    ASSERT_FALSE(ret);
}

TEST_F(TestTopologyStorage, test_LoadChunkServer_Fail) {
    std::unordered_map<ChunkServerIdType, ChunkServer> chunkServerMap;
    ChunkServerIdType maxChunkServerId;

    EXPECT_CALL(*repo_, LoadChunkServerRepoItems(_))
        .WillOnce(Return(SqlException));

    int ret = storage_->LoadChunkServer(&chunkServerMap, &maxChunkServerId);
    ASSERT_FALSE(ret);
}

TEST_F(TestTopologyStorage, test_LoadCopySet_success) {
    std::map<CopySetKey, CopySetInfo> copySetMap;
    std::map<PoolIdType, CopySetIdType> copySetIdMaxMap;
    std::vector<CopySetRepoItem> copySetRepos;
    CopySetRepoItem data(0x51,
        0x01,
        0,
        "[41, 42, 43]");
    copySetRepos.push_back(data);
    EXPECT_CALL(*repo_, LoadCopySetRepoItems(_))
        .WillOnce(DoAll(SetArgPointee<0>(copySetRepos),
                      Return(OperationOK)));

    int ret = storage_->LoadCopySet(&copySetMap, &copySetIdMaxMap);
    ASSERT_TRUE(ret);
    ASSERT_EQ(1, copySetMap.size());

    std::pair<PoolIdType, CopySetIdType> key(0x01, 0x51);
    ASSERT_EQ(0x51,
        copySetMap[key].GetId());
    ASSERT_EQ(0x01,
        copySetMap[key]
            .GetLogicalPoolId());
    ASSERT_STREQ("[\n\t41,\n\t42,\n\t43\n]\n",
        copySetMap[key].GetCopySetMembersStr().c_str());
    ASSERT_EQ(1, copySetIdMaxMap.size());
    ASSERT_EQ(0x51, copySetIdMaxMap[0x01]);
}

TEST_F(TestTopologyStorage, test_LoadCopySet_IdDuplicated) {
    std::map<CopySetKey, CopySetInfo> copySetMap;
    std::map<PoolIdType, CopySetIdType> copySetIdMaxMap;
    std::vector<CopySetRepoItem> copySetRepos;
    CopySetRepoItem data(0x51,
        0x01,
        0,
        "[41, 42, 43]");
    copySetRepos.push_back(data);
    copySetRepos.push_back(data);
    EXPECT_CALL(*repo_, LoadCopySetRepoItems(_))
        .WillOnce(DoAll(SetArgPointee<0>(copySetRepos),
                      Return(OperationOK)));

    int ret = storage_->LoadCopySet(&copySetMap, &copySetIdMaxMap);
    ASSERT_FALSE(ret);
}

TEST_F(TestTopologyStorage, test_LoadCopySet_fail) {
    std::map<CopySetKey, CopySetInfo> copySetMap;
    std::map<PoolIdType, CopySetIdType> copySetIdMaxMap;

    EXPECT_CALL(*repo_, LoadCopySetRepoItems(_))
        .WillOnce(Return(SqlException));

    int ret = storage_->LoadCopySet(&copySetMap, &copySetIdMaxMap);
    ASSERT_FALSE(ret);
}

TEST_F(TestTopologyStorage, test_LoadCopySet_parseJsonFail) {
    std::map<CopySetKey, CopySetInfo> copySetMap;
    std::map<PoolIdType, CopySetIdType> copySetIdMaxMap;
    std::vector<CopySetRepoItem> copySetRepos;
    CopySetRepoItem data(0x51,
        0x01,
        0,
        "[41, 42, ab]");
    copySetRepos.push_back(data);
    EXPECT_CALL(*repo_, LoadCopySetRepoItems(_))
        .WillOnce(DoAll(SetArgPointee<0>(copySetRepos),
                      Return(OperationOK)));

    int ret = storage_->LoadCopySet(&copySetMap, &copySetIdMaxMap);
    ASSERT_FALSE(ret);
}

TEST_F(TestTopologyStorage, test_StorageLogicalPool_success) {
    LogicalPool data;
    EXPECT_CALL(*repo_, InsertLogicalPoolRepoItem(_))
        .WillOnce(Return(OperationOK));
    int ret = storage_->StorageLogicalPool(data);

    ASSERT_TRUE(ret);
}

TEST_F(TestTopologyStorage, test_StorageLogicalPool_fail) {
    LogicalPool data;
    EXPECT_CALL(*repo_, InsertLogicalPoolRepoItem(_))
        .WillOnce(Return(SqlException));
    int ret = storage_->StorageLogicalPool(data);

    ASSERT_FALSE(ret);
}

TEST_F(TestTopologyStorage, test_StoragePhysicalPool_success) {
    PhysicalPool data;
    EXPECT_CALL(*repo_, InsertPhysicalPoolRepoItem(_))
        .WillOnce(Return(OperationOK));
    int ret = storage_->StoragePhysicalPool(data);
    ASSERT_TRUE(ret);
}

TEST_F(TestTopologyStorage, test_StoragePhysicalPool_fail) {
    PhysicalPool data;
    EXPECT_CALL(*repo_, InsertPhysicalPoolRepoItem(_))
        .WillOnce(Return(SqlException));
    int ret = storage_->StoragePhysicalPool(data);
    ASSERT_FALSE(ret);
}

TEST_F(TestTopologyStorage, test_StorageZone_success) {
    Zone data;
    EXPECT_CALL(*repo_, InsertZoneRepoItem(_))
        .WillOnce(Return(OperationOK));
    int ret = storage_->StorageZone(data);
    ASSERT_TRUE(ret);
}

TEST_F(TestTopologyStorage, test_StorageZone_fail) {
    Zone data;
    EXPECT_CALL(*repo_, InsertZoneRepoItem(_))
        .WillOnce(Return(SqlException));
    int ret = storage_->StorageZone(data);
    ASSERT_FALSE(ret);
}

TEST_F(TestTopologyStorage, test_StorageServer_success) {
    Server data;
    EXPECT_CALL(*repo_, InsertServerRepoItem(_))
        .WillOnce(Return(OperationOK));
    int ret = storage_->StorageServer(data);
    ASSERT_TRUE(ret);
}

TEST_F(TestTopologyStorage, test_StorageServer_fail) {
    Server data;
    EXPECT_CALL(*repo_, InsertServerRepoItem(_))
        .WillOnce(Return(SqlException));
    int ret = storage_->StorageServer(data);
    ASSERT_FALSE(ret);
}

TEST_F(TestTopologyStorage, test_StorageChunkServer_success) {
    ChunkServer data;
    EXPECT_CALL(*repo_, InsertChunkServerRepoItem(_))
        .WillOnce(Return(OperationOK));
    int ret = storage_->StorageChunkServer(data);
    ASSERT_TRUE(ret);
}

TEST_F(TestTopologyStorage, test_StorageChunkServer_fail) {
    ChunkServer data;
    EXPECT_CALL(*repo_, InsertChunkServerRepoItem(_))
        .WillOnce(Return(SqlException));
    int ret = storage_->StorageChunkServer(data);
    ASSERT_FALSE(ret);
}

TEST_F(TestTopologyStorage, test_StorageCopySet_success) {
    CopySetInfo data;
    EXPECT_CALL(*repo_, InsertCopySetRepoItem(_))
        .WillOnce(Return(OperationOK));
    int ret = storage_->StorageCopySet(data);
    ASSERT_TRUE(ret);
}

TEST_F(TestTopologyStorage, test_StorageCopySet_fail) {
    CopySetInfo data;
    EXPECT_CALL(*repo_, InsertCopySetRepoItem(_))
        .WillOnce(Return(SqlException));
    int ret = storage_->StorageCopySet(data);
    ASSERT_FALSE(ret);
}

TEST_F(TestTopologyStorage, test_DeleteLogicalPool_success) {
    PoolIdType id = 0x01;
    EXPECT_CALL(*repo_, DeleteLogicalPoolRepoItem(_))
        .WillOnce(Return(OperationOK));
    int ret = storage_->DeleteLogicalPool(id);
    ASSERT_TRUE(ret);
}

TEST_F(TestTopologyStorage, test_DeleteLogicalPool_fail) {
    PoolIdType id = 0x01;
    EXPECT_CALL(*repo_, DeleteLogicalPoolRepoItem(_))
        .WillOnce(Return(SqlException));
    int ret = storage_->DeleteLogicalPool(id);
    ASSERT_FALSE(ret);
}

TEST_F(TestTopologyStorage, test_DeletePhysicalPool_success) {
    PoolIdType id = 0x01;
    EXPECT_CALL(*repo_, DeletePhysicalPoolRepoItem(_))
        .WillOnce(Return(OperationOK));
    int ret = storage_->DeletePhysicalPool(id);
    ASSERT_TRUE(ret);
}

TEST_F(TestTopologyStorage, test_DeletePhysicalPool_fail) {
    PoolIdType id = 0x01;
    EXPECT_CALL(*repo_, DeletePhysicalPoolRepoItem(_))
        .WillOnce(Return(SqlException));
    int ret = storage_->DeletePhysicalPool(id);
    ASSERT_FALSE(ret);
}

TEST_F(TestTopologyStorage, test_DeleteZone_success) {
    ZoneIdType id = 0x01;
    EXPECT_CALL(*repo_, DeleteZoneRepoItem(_))
        .WillOnce(Return(OperationOK));
    int ret = storage_->DeleteZone(id);
    ASSERT_TRUE(ret);
}

TEST_F(TestTopologyStorage, test_DeleteZone_fail) {
    ZoneIdType id = 0x01;
    EXPECT_CALL(*repo_, DeleteZoneRepoItem(_))
        .WillOnce(Return(SqlException));
    int ret = storage_->DeleteZone(id);
    ASSERT_FALSE(ret);
}

TEST_F(TestTopologyStorage, test_DeleteServer_success) {
    ServerIdType id = 0x01;
    EXPECT_CALL(*repo_, DeleteServerRepoItem(_))
        .WillOnce(Return(OperationOK));
    int ret = storage_->DeleteServer(id);
    ASSERT_TRUE(ret);
}

TEST_F(TestTopologyStorage, test_DeleteServer_fail) {
    ServerIdType id = 0x01;
    EXPECT_CALL(*repo_, DeleteServerRepoItem(_))
        .WillOnce(Return(SqlException));
    int ret = storage_->DeleteServer(id);
    ASSERT_FALSE(ret);
}

TEST_F(TestTopologyStorage, test_DeleteChunkServer_success) {
    ChunkServerIdType id = 0x01;
    EXPECT_CALL(*repo_, DeleteChunkServerRepoItem(_))
        .WillOnce(Return(OperationOK));
    int ret = storage_->DeleteChunkServer(id);
    ASSERT_TRUE(ret);
}

TEST_F(TestTopologyStorage, test_DeleteChunkServer_fail) {
    ChunkServerIdType id = 0x01;
    EXPECT_CALL(*repo_, DeleteChunkServerRepoItem(_))
        .WillOnce(Return(SqlException));
    int ret = storage_->DeleteChunkServer(id);
    ASSERT_FALSE(ret);
}

TEST_F(TestTopologyStorage, test_DeleteCopySet_success) {
    CopySetKey key;
    EXPECT_CALL(*repo_, DeleteCopySetRepoItem(_, _))
        .WillOnce(Return(OperationOK));
    int ret = storage_->DeleteCopySet(key);
    ASSERT_TRUE(ret);
}

TEST_F(TestTopologyStorage, test_DeleteCopySet_fail) {
    CopySetKey key;
    EXPECT_CALL(*repo_, DeleteCopySetRepoItem(_, _))
        .WillOnce(Return(SqlException));
    int ret = storage_->DeleteCopySet(key);
    ASSERT_FALSE(ret);
}

TEST_F(TestTopologyStorage, test_UpdateLogicalPool_success) {
    LogicalPool data;
    EXPECT_CALL(*repo_, UpdateLogicalPoolRepoItem(_))
        .WillOnce(Return(OperationOK));
    int ret = storage_->UpdateLogicalPool(data);

    ASSERT_TRUE(ret);
}

TEST_F(TestTopologyStorage, test_UpdateLogicalPool_fail) {
    LogicalPool data;
    EXPECT_CALL(*repo_, UpdateLogicalPoolRepoItem(_))
        .WillOnce(Return(SqlException));
    int ret = storage_->UpdateLogicalPool(data);

    ASSERT_FALSE(ret);
}

TEST_F(TestTopologyStorage, test_UpdatePhysicalPool_success) {
    PhysicalPool data;
    EXPECT_CALL(*repo_, UpdatePhysicalPoolRepoItem(_))
        .WillOnce(Return(OperationOK));
    int ret = storage_->UpdatePhysicalPool(data);
    ASSERT_TRUE(ret);
}

TEST_F(TestTopologyStorage, test_UpdatePhysicalPool_fail) {
    PhysicalPool data;
    EXPECT_CALL(*repo_, UpdatePhysicalPoolRepoItem(_))
        .WillOnce(Return(SqlException));
    int ret = storage_->UpdatePhysicalPool(data);
    ASSERT_FALSE(ret);
}

TEST_F(TestTopologyStorage, test_UpdateZone_success) {
    Zone data;
    EXPECT_CALL(*repo_, UpdateZoneRepoItem(_))
        .WillOnce(Return(OperationOK));
    int ret = storage_->UpdateZone(data);
    ASSERT_TRUE(ret);
}

TEST_F(TestTopologyStorage, test_UpdateZone_fail) {
    Zone data;
    EXPECT_CALL(*repo_, UpdateZoneRepoItem(_))
        .WillOnce(Return(SqlException));
    int ret = storage_->UpdateZone(data);
    ASSERT_FALSE(ret);
}

TEST_F(TestTopologyStorage, test_UpdateServer_success) {
    Server data;
    EXPECT_CALL(*repo_, UpdateServerRepoItem(_))
        .WillOnce(Return(OperationOK));
    int ret = storage_->UpdateServer(data);
    ASSERT_TRUE(ret);
}

TEST_F(TestTopologyStorage, test_UpdateServer_fail) {
    Server data;
    EXPECT_CALL(*repo_, UpdateServerRepoItem(_))
        .WillOnce(Return(SqlException));
    int ret = storage_->UpdateServer(data);
    ASSERT_FALSE(ret);
}

TEST_F(TestTopologyStorage, test_UpdateChunkServer_success) {
    ChunkServer data;
    EXPECT_CALL(*repo_, UpdateChunkServerRepoItem(_))
        .WillOnce(Return(OperationOK));
    int ret = storage_->UpdateChunkServer(data);
    ASSERT_TRUE(ret);
}

TEST_F(TestTopologyStorage, test_UpdateChunkServer_fail) {
    ChunkServer data;
    EXPECT_CALL(*repo_, UpdateChunkServerRepoItem(_))
        .WillOnce(Return(SqlException));
    int ret = storage_->UpdateChunkServer(data);
    ASSERT_FALSE(ret);
}

TEST_F(TestTopologyStorage, test_UpdateCopySet_success) {
    CopySetInfo data;
    EXPECT_CALL(*repo_, UpdateCopySetRepoItem(_))
        .WillOnce(Return(OperationOK));
    int ret = storage_->UpdateCopySet(data);
    ASSERT_TRUE(ret);
}

TEST_F(TestTopologyStorage, test_UpdateCopySet_fail) {
    CopySetInfo data;
    EXPECT_CALL(*repo_, UpdateCopySetRepoItem(_))
        .WillOnce(Return(SqlException));
    int ret = storage_->UpdateCopySet(data);
    ASSERT_FALSE(ret);
}

}  // namespace topology
}  // namespace mds
}  // namespace curve
