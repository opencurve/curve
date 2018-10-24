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
#include "src/mds/repo/repo.h"
#include "src/mds/repo/repoItem.h"
#include "src/mds/repo/dataBase.h"
#include "json/json.h"

namespace curve {
namespace mds {
namespace topology {


using ::curve::repo::MockRepo;
using ::curve::repo::OperationOK;
using ::curve::repo::SqlException;

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

TEST_F(TestTopologyStorage, test_init_success) {
    std::string dbName = "dbName";
    std::string user = "user";
    std::string url = "url";
    std::string password = "password";

    EXPECT_CALL(*repo_, connectDB(_, _, _, _))
        .WillOnce(Return(OperationOK));

    EXPECT_CALL(*repo_, createDatabase())
        .WillOnce(Return(OperationOK));

    EXPECT_CALL(*repo_, useDataBase())
        .WillOnce(Return(OperationOK));

    EXPECT_CALL(*repo_, createAllTables())
        .WillOnce(Return(OperationOK));

    int ret = storage_->init(dbName,
        user,
        url,
        password);

    ASSERT_TRUE(ret);
}

TEST_F(TestTopologyStorage, test_LoadLogicalPool_success) {
    std::unordered_map<PoolIdType, LogicalPool> logicalPoolMap;
    PoolIdType maxLogicalPoolId = 0;

    EXPECT_CALL(*repo_, LoadLogicalPoolRepos(_))
        .WillOnce(Return(OperationOK));

    int ret = storage_->LoadLogicalPool(&logicalPoolMap, &maxLogicalPoolId);

    ASSERT_TRUE(ret);
}

TEST_F(TestTopologyStorage, test_LoadLogicalPool_fail) {
    std::unordered_map<PoolIdType, LogicalPool> logicalPoolMap;
    PoolIdType maxLogicalPoolId = 0;

    EXPECT_CALL(*repo_, LoadLogicalPoolRepos(_))
        .WillOnce(Return(SqlException));

    int ret = storage_->LoadLogicalPool(&logicalPoolMap, &maxLogicalPoolId);

    ASSERT_FALSE(ret);
}

TEST_F(TestTopologyStorage, test_LoadPhysicalPool_success) {
    std::unordered_map<PoolIdType, PhysicalPool> physicalPoolMap;
    PoolIdType maxPhysicalPoolId = 0;

    EXPECT_CALL(*repo_, LoadPhysicalPoolRepos(_))
        .WillOnce(Return(OperationOK));

    int ret = storage_->LoadPhysicalPool(&physicalPoolMap, &maxPhysicalPoolId);

    ASSERT_TRUE(ret);
}

TEST_F(TestTopologyStorage, test_LoadPhysicalPool_fail) {
    std::unordered_map<PoolIdType, PhysicalPool> physicalPoolMap;
    PoolIdType maxPhysicalPoolId = 0;

    EXPECT_CALL(*repo_, LoadPhysicalPoolRepos(_))
        .WillOnce(Return(SqlException));

    int ret = storage_->LoadPhysicalPool(&physicalPoolMap, &maxPhysicalPoolId);

    ASSERT_FALSE(ret);
}

TEST_F(TestTopologyStorage, test_LoadZone_success) {
    std::unordered_map<ZoneIdType, Zone> zoneMap;
    ZoneIdType maxZoneId = 0;

    EXPECT_CALL(*repo_, LoadZoneRepos(_))
        .WillOnce(Return(OperationOK));

    int ret = storage_->LoadZone(&zoneMap, &maxZoneId);

    ASSERT_TRUE(ret);
}

TEST_F(TestTopologyStorage, test_LoadZone_fail) {
    std::unordered_map<ZoneIdType, Zone> zoneMap;
    ZoneIdType maxZoneId = 0;

    EXPECT_CALL(*repo_, LoadZoneRepos(_))
        .WillOnce(Return(SqlException));

    int ret = storage_->LoadZone(&zoneMap, &maxZoneId);

    ASSERT_FALSE(ret);
}

TEST_F(TestTopologyStorage, test_LoadServer_success) {
    std::unordered_map<ServerIdType, Server> serverMap;
    ServerIdType maxServerId;

    EXPECT_CALL(*repo_, LoadServerRepos(_))
        .WillOnce(Return(OperationOK));

    int ret = storage_->LoadServer(&serverMap, &maxServerId);

    ASSERT_TRUE(ret);
}

TEST_F(TestTopologyStorage, test_LoadServer_fail) {
    std::unordered_map<ServerIdType, Server> serverMap;
    ServerIdType maxServerId;

    EXPECT_CALL(*repo_, LoadServerRepos(_))
        .WillOnce(Return(SqlException));

    int ret = storage_->LoadServer(&serverMap, &maxServerId);

    ASSERT_FALSE(ret);
}

TEST_F(TestTopologyStorage, test_LoadChunkServer_success) {
    std::unordered_map<ChunkServerIdType, ChunkServer> chunkServerMap;
    ChunkServerIdType maxChunkServerId;

    EXPECT_CALL(*repo_, LoadChunkServerRepos(_))
        .WillOnce(Return(OperationOK));

    int ret = storage_->LoadChunkServer(&chunkServerMap, &maxChunkServerId);
    ASSERT_TRUE(ret);
}

TEST_F(TestTopologyStorage, test_LoadChunkServer_Fail) {
    std::unordered_map<ChunkServerIdType, ChunkServer> chunkServerMap;
    ChunkServerIdType maxChunkServerId;

    EXPECT_CALL(*repo_, LoadChunkServerRepos(_))
        .WillOnce(Return(SqlException));

    int ret = storage_->LoadChunkServer(&chunkServerMap, &maxChunkServerId);
    ASSERT_FALSE(ret);
}

TEST_F(TestTopologyStorage, test_LoadCopySet_success) {
    std::map<CopySetKey, CopySetInfo> copySetMap;
    std::map<PoolIdType, CopySetIdType> copySetIdMaxMap;

    EXPECT_CALL(*repo_, LoadCopySetRepos(_))
        .WillOnce(Return(OperationOK));

    int ret = storage_->LoadCopySet(&copySetMap, &copySetIdMaxMap);
    ASSERT_TRUE(ret);
}

TEST_F(TestTopologyStorage, test_LoadCopySet_fail) {
    std::map<CopySetKey, CopySetInfo> copySetMap;
    std::map<PoolIdType, CopySetIdType> copySetIdMaxMap;

    EXPECT_CALL(*repo_, LoadCopySetRepos(_))
        .WillOnce(Return(SqlException));

    int ret = storage_->LoadCopySet(&copySetMap, &copySetIdMaxMap);
    ASSERT_FALSE(ret);
}

TEST_F(TestTopologyStorage, test_StorageLogicalPool_success) {
    LogicalPool data;
    EXPECT_CALL(*repo_, InsertLogicalPoolRepo(_))
        .WillOnce(Return(OperationOK));
    int ret = storage_->StorageLogicalPool(data);

    ASSERT_TRUE(ret);
}

TEST_F(TestTopologyStorage, test_StorageLogicalPool_fail) {
    LogicalPool data;
    EXPECT_CALL(*repo_, InsertLogicalPoolRepo(_))
        .WillOnce(Return(SqlException));
    int ret = storage_->StorageLogicalPool(data);

    ASSERT_FALSE(ret);
}

TEST_F(TestTopologyStorage, test_StoragePhysicalPool_success) {
    PhysicalPool data;
    EXPECT_CALL(*repo_, InsertPhysicalPoolRepo(_))
        .WillOnce(Return(OperationOK));
    int ret = storage_->StoragePhysicalPool(data);
    ASSERT_TRUE(ret);
}

TEST_F(TestTopologyStorage, test_StoragePhysicalPool_fail) {
    PhysicalPool data;
    EXPECT_CALL(*repo_, InsertPhysicalPoolRepo(_))
        .WillOnce(Return(SqlException));
    int ret = storage_->StoragePhysicalPool(data);
    ASSERT_FALSE(ret);
}

TEST_F(TestTopologyStorage, test_StorageZone_success) {
    Zone data;
    EXPECT_CALL(*repo_, InsertZoneRepo(_))
        .WillOnce(Return(OperationOK));
    int ret = storage_->StorageZone(data);
    ASSERT_TRUE(ret);
}

TEST_F(TestTopologyStorage, test_StorageZone_fail) {
    Zone data;
    EXPECT_CALL(*repo_, InsertZoneRepo(_))
        .WillOnce(Return(SqlException));
    int ret = storage_->StorageZone(data);
    ASSERT_FALSE(ret);
}

TEST_F(TestTopologyStorage, test_StorageServer_success) {
    Server data;
    EXPECT_CALL(*repo_, InsertServerRepo(_))
        .WillOnce(Return(OperationOK));
    int ret = storage_->StorageServer(data);
    ASSERT_TRUE(ret);
}

TEST_F(TestTopologyStorage, test_StorageServer_fail) {
    Server data;
    EXPECT_CALL(*repo_, InsertServerRepo(_))
        .WillOnce(Return(SqlException));
    int ret = storage_->StorageServer(data);
    ASSERT_FALSE(ret);
}

TEST_F(TestTopologyStorage, test_StorageChunkServer_success) {
    ChunkServer data;
    EXPECT_CALL(*repo_, InsertChunkServerRepo(_))
        .WillOnce(Return(OperationOK));
    int ret = storage_->StorageChunkServer(data);
    ASSERT_TRUE(ret);
}

TEST_F(TestTopologyStorage, test_StorageChunkServer_fail) {
    ChunkServer data;
    EXPECT_CALL(*repo_, InsertChunkServerRepo(_))
        .WillOnce(Return(SqlException));
    int ret = storage_->StorageChunkServer(data);
    ASSERT_FALSE(ret);
}

TEST_F(TestTopologyStorage, test_StorageCopySet_success) {
    CopySetInfo data;
    EXPECT_CALL(*repo_, InsertCopySetRepo(_))
        .WillOnce(Return(OperationOK));
    int ret = storage_->StorageCopySet(data);
    ASSERT_TRUE(ret);
}

TEST_F(TestTopologyStorage, test_StorageCopySet_fail) {
    CopySetInfo data;
    EXPECT_CALL(*repo_, InsertCopySetRepo(_))
        .WillOnce(Return(SqlException));
    int ret = storage_->StorageCopySet(data);
    ASSERT_FALSE(ret);
}

TEST_F(TestTopologyStorage, test_DeleteLogicalPool_success) {
    PoolIdType id = 0x01;
    EXPECT_CALL(*repo_, DeleteLogicalPoolRepo(_))
        .WillOnce(Return(OperationOK));
    int ret = storage_->DeleteLogicalPool(id);
    ASSERT_TRUE(ret);
}

TEST_F(TestTopologyStorage, test_DeleteLogicalPool_fail) {
    PoolIdType id = 0x01;
    EXPECT_CALL(*repo_, DeleteLogicalPoolRepo(_))
        .WillOnce(Return(SqlException));
    int ret = storage_->DeleteLogicalPool(id);
    ASSERT_FALSE(ret);
}

TEST_F(TestTopologyStorage, test_DeletePhysicalPool_success) {
    PoolIdType id = 0x01;
    EXPECT_CALL(*repo_, DeletePhysicalPoolRepo(_))
        .WillOnce(Return(OperationOK));
    int ret = storage_->DeletePhysicalPool(id);
    ASSERT_TRUE(ret);
}

TEST_F(TestTopologyStorage, test_DeletePhysicalPool_fail) {
    PoolIdType id = 0x01;
    EXPECT_CALL(*repo_, DeletePhysicalPoolRepo(_))
        .WillOnce(Return(SqlException));
    int ret = storage_->DeletePhysicalPool(id);
    ASSERT_FALSE(ret);
}

TEST_F(TestTopologyStorage, test_DeleteZone_success) {
    ZoneIdType id = 0x01;
    EXPECT_CALL(*repo_, DeleteZoneRepo(_))
        .WillOnce(Return(OperationOK));
    int ret = storage_->DeleteZone(id);
    ASSERT_TRUE(ret);
}

TEST_F(TestTopologyStorage, test_DeleteZone_fail) {
    ZoneIdType id = 0x01;
    EXPECT_CALL(*repo_, DeleteZoneRepo(_))
        .WillOnce(Return(SqlException));
    int ret = storage_->DeleteZone(id);
    ASSERT_FALSE(ret);
}

TEST_F(TestTopologyStorage, test_DeleteServer_success) {
    ServerIdType id = 0x01;
    EXPECT_CALL(*repo_, DeleteServerRepo(_))
        .WillOnce(Return(OperationOK));
    int ret = storage_->DeleteServer(id);
    ASSERT_TRUE(ret);
}

TEST_F(TestTopologyStorage, test_DeleteServer_fail) {
    ServerIdType id = 0x01;
    EXPECT_CALL(*repo_, DeleteServerRepo(_))
        .WillOnce(Return(SqlException));
    int ret = storage_->DeleteServer(id);
    ASSERT_FALSE(ret);
}

TEST_F(TestTopologyStorage, test_DeleteChunkServer_success) {
    ChunkServerIdType id = 0x01;
    EXPECT_CALL(*repo_, DeleteChunkServerRepo(_))
        .WillOnce(Return(OperationOK));
    int ret = storage_->DeleteChunkServer(id);
    ASSERT_TRUE(ret);
}

TEST_F(TestTopologyStorage, test_DeleteChunkServer_fail) {
    ChunkServerIdType id = 0x01;
    EXPECT_CALL(*repo_, DeleteChunkServerRepo(_))
        .WillOnce(Return(SqlException));
    int ret = storage_->DeleteChunkServer(id);
    ASSERT_FALSE(ret);
}

TEST_F(TestTopologyStorage, test_DeleteCopySet_success) {
    CopySetKey key;
    EXPECT_CALL(*repo_, DeleteCopySetRepo(_, _))
        .WillOnce(Return(OperationOK));
    int ret = storage_->DeleteCopySet(key);
    ASSERT_TRUE(ret);
}

TEST_F(TestTopologyStorage, test_DeleteCopySet_fail) {
    CopySetKey key;
    EXPECT_CALL(*repo_, DeleteCopySetRepo(_, _))
        .WillOnce(Return(SqlException));
    int ret = storage_->DeleteCopySet(key);
    ASSERT_FALSE(ret);
}

TEST_F(TestTopologyStorage, test_UpdateLogicalPool_success) {
    LogicalPool data;
    EXPECT_CALL(*repo_, UpdateLogicalPoolRepo(_))
        .WillOnce(Return(OperationOK));
    int ret = storage_->UpdateLogicalPool(data);

    ASSERT_TRUE(ret);
}

TEST_F(TestTopologyStorage, test_UpdateLogicalPool_fail) {
    LogicalPool data;
    EXPECT_CALL(*repo_, UpdateLogicalPoolRepo(_))
        .WillOnce(Return(SqlException));
    int ret = storage_->UpdateLogicalPool(data);

    ASSERT_FALSE(ret);
}

TEST_F(TestTopologyStorage, test_UpdatePhysicalPool_success) {
    PhysicalPool data;
    EXPECT_CALL(*repo_, UpdatePhysicalPoolRepo(_))
        .WillOnce(Return(OperationOK));
    int ret = storage_->UpdatePhysicalPool(data);
    ASSERT_TRUE(ret);
}

TEST_F(TestTopologyStorage, test_UpdatePhysicalPool_fail) {
    PhysicalPool data;
    EXPECT_CALL(*repo_, UpdatePhysicalPoolRepo(_))
        .WillOnce(Return(SqlException));
    int ret = storage_->UpdatePhysicalPool(data);
    ASSERT_FALSE(ret);
}

TEST_F(TestTopologyStorage, test_UpdateZone_success) {
    Zone data;
    EXPECT_CALL(*repo_, UpdateZoneRepo(_))
        .WillOnce(Return(OperationOK));
    int ret = storage_->UpdateZone(data);
    ASSERT_TRUE(ret);
}

TEST_F(TestTopologyStorage, test_UpdateZone_fail) {
    Zone data;
    EXPECT_CALL(*repo_, UpdateZoneRepo(_))
        .WillOnce(Return(SqlException));
    int ret = storage_->UpdateZone(data);
    ASSERT_FALSE(ret);
}

TEST_F(TestTopologyStorage, test_UpdateServer_success) {
    Server data;
    EXPECT_CALL(*repo_, UpdateServerRepo(_))
        .WillOnce(Return(OperationOK));
    int ret = storage_->UpdateServer(data);
    ASSERT_TRUE(ret);
}

TEST_F(TestTopologyStorage, test_UpdateServer_fail) {
    Server data;
    EXPECT_CALL(*repo_, UpdateServerRepo(_))
        .WillOnce(Return(SqlException));
    int ret = storage_->UpdateServer(data);
    ASSERT_FALSE(ret);
}

TEST_F(TestTopologyStorage, test_UpdateChunkServer_success) {
    ChunkServer data;
    EXPECT_CALL(*repo_, UpdateChunkServerRepo(_))
        .WillOnce(Return(OperationOK));
    int ret = storage_->UpdateChunkServer(data);
    ASSERT_TRUE(ret);
}

TEST_F(TestTopologyStorage, test_UpdateChunkServer_fail) {
    ChunkServer data;
    EXPECT_CALL(*repo_, UpdateChunkServerRepo(_))
        .WillOnce(Return(SqlException));
    int ret = storage_->UpdateChunkServer(data);
    ASSERT_FALSE(ret);
}

TEST_F(TestTopologyStorage, test_UpdateCopySet_success) {
    CopySetInfo data;
    EXPECT_CALL(*repo_, UpdateCopySetRepo(_))
        .WillOnce(Return(OperationOK));
    int ret = storage_->UpdateCopySet(data);
    ASSERT_TRUE(ret);
}

TEST_F(TestTopologyStorage, test_UpdateCopySet_fail) {
    CopySetInfo data;
    EXPECT_CALL(*repo_, UpdateCopySetRepo(_))
        .WillOnce(Return(SqlException));
    int ret = storage_->UpdateCopySet(data);
    ASSERT_FALSE(ret);
}

}  // namespace topology
}  // namespace mds
}  // namespace curve
