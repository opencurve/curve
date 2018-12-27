/*
 * Project: curve
 * Created Date: Tue Sep 18 2018
 * Author: lixiaocui
 * Copyright (c) 2018 netease
 */


#include <gtest/gtest.h>
#include <json/json.h>
#include "src/mds/repo/repo.h"

namespace curve {
namespace repo {
const uint8_t RW = 0;
const uint8_t Healthy = 0;
const uint8_t Unhealthy = 1;
const uint8_t Online = 0;
const uint8_t StoreType = 0;

class RepoTest : public ::testing::Test {
 public:
  void SetUp() override {
      repo = new Repo();
      ASSERT_EQ(OperationOK,
                repo->connectDB("curve_mds_repo_test",
                                "root",
                                "localhost",
                                "qwer"));
      ASSERT_EQ(OperationOK, repo->dropDataBase());
      ASSERT_EQ(OperationOK, repo->createDatabase());
      ASSERT_EQ(OperationOK, repo->useDataBase());
      ASSERT_EQ(OperationOK, repo->createAllTables());
  }

  void TearDown() override {
      repo->dropDataBase();
      delete (repo);
  }

  Repo *repo;
};

TEST_F(RepoTest, testChunkserverCUDA) {
    // insert chunk server, id=1
    // success
    ChunkServerRepo r1(1,
                       "hello",
                       "ssd",
                       "127.0.0.1",
                       9000,
                       1,
                       RW,
                       Healthy,
                       Online,
                       "/mnt/1",
                       20 << 3,
                       10 << 3);
    ASSERT_EQ(OperationOK, repo->InsertChunkServerRepo(r1));

    // insert duplicate chunk server id
    // err
    ASSERT_EQ(SqlException, repo->InsertChunkServerRepo(r1));

    // insert chunk server, id=2
    ChunkServerRepo r2(r1);
    r2.chunkServerID = 2;
    r2.port = 9001;
    r2.mountPoint = "/mnt/2";
    ASSERT_EQ(OperationOK, repo->InsertChunkServerRepo(r2));

    // query chunk server id=1
    ChunkServerRepo queryRes;
    ASSERT_EQ(OperationOK,
              repo->QueryChunkServerRepo(r1.chunkServerID, &queryRes));
    ASSERT_TRUE(queryRes == r1);

    // query chunk server id=3, empty
    queryRes.chunkServerID = 0;
    ASSERT_EQ(OperationOK, repo->QueryChunkServerRepo(3, &queryRes));
    ASSERT_EQ(0, queryRes.chunkServerID);

    // query all chunk servers
    std::vector<ChunkServerRepo> chunkServers;
    ASSERT_EQ(OperationOK, repo->LoadChunkServerRepos(&chunkServers));
    ASSERT_EQ(2, chunkServers.size());
    ASSERT_TRUE(r1 == chunkServers[0]);
    ASSERT_TRUE(r2 == chunkServers[1]);

    // update used
    r1.used = 15 << 3;
    ASSERT_EQ(OperationOK, repo->UpdateChunkServerRepo(r1));
    queryRes.chunkServerID = 0;
    ASSERT_EQ(OperationOK,
              repo->QueryChunkServerRepo(r1.chunkServerID, &queryRes));
    ASSERT_EQ(r1.used, queryRes.used);

    // delete chunk server id=1
    ASSERT_EQ(OperationOK, repo->DeleteChunkServerRepo(r1.chunkServerID));
    queryRes.chunkServerID = 0;
    ASSERT_EQ(OperationOK,
              repo->QueryChunkServerRepo(r1.chunkServerID, &queryRes));
    ASSERT_EQ(0, queryRes.chunkServerID);

    // close statement, query get sqlException
    repo->getDataBase()->statement_->close();
    ASSERT_EQ(SqlException, repo->QueryChunkServerRepo(2, &queryRes));
    ASSERT_EQ(SqlException, repo->LoadChunkServerRepos(&chunkServers));
    ASSERT_EQ(SqlException, repo->createAllTables());
}

TEST_F(RepoTest, testServerCUDA) {
    // insert server id=1
    ServerRepo s1(1,
                  "curve-nos1.dg.163.org",
                  "10.172.168.1",
                  "192.168.2.1",
                  1,
                  1,
                  "first server");
    ASSERT_EQ(OperationOK, repo->InsertServerRepo(s1));

    // insert duplicate server id
    ASSERT_EQ(SqlException, repo->InsertServerRepo(s1));

    // insert server id=2
    ServerRepo s2(2,
                  "curve-nos2.dg.163.org",
                  "10.172.168.2",
                  "192.168.2.2",
                  1,
                  2,
                  "second server");
    ASSERT_EQ(OperationOK, repo->InsertServerRepo(s2));

    // query server id=2
    ServerRepo queryRes(0);
    ASSERT_EQ(OperationOK, repo->QueryServerRepo(s1.serverID, &queryRes));
    ASSERT_TRUE(queryRes == s1);

    // query server id=3, empty
    queryRes.serverID = 0;
    ASSERT_EQ(OperationOK, repo->QueryServerRepo(3, &queryRes));
    ASSERT_EQ(0, queryRes.serverID);

    // query all servers
    std::vector<ServerRepo> servers;
    ASSERT_EQ(OperationOK, repo->LoadServerRepos(&servers));
    ASSERT_EQ(2, servers.size());
    ASSERT_TRUE(s1 == servers[0]);
    ASSERT_TRUE(s2 == servers[1]);

    // update hostname
    s1.hostName = "curve-nos0.dg.163.org";
    ASSERT_EQ(OperationOK, repo->UpdateServerRepo(s1));
    queryRes.serverID = 0;
    ASSERT_EQ(OperationOK, repo->QueryServerRepo(s1.serverID, &queryRes));
    ASSERT_EQ(s1.hostName, queryRes.hostName);

    // delete server id=1
    ASSERT_EQ(OperationOK, repo->DeleteServerRepo(s1.serverID));
    queryRes.serverID = 0;
    ASSERT_EQ(OperationOK, repo->QueryServerRepo(s1.serverID, &queryRes));
    ASSERT_EQ(0, queryRes.serverID);

    // close statement, query get sqlException
    repo->getDataBase()->statement_->close();
    ASSERT_EQ(SqlException, repo->QueryServerRepo(2, &queryRes));
    ASSERT_EQ(SqlException, repo->LoadServerRepos(&servers));
}

TEST_F(RepoTest, testZoneCUDA) {
    // insert zone id=1
    ZoneRepo r1(1, "test", 1, "first zone");
    ASSERT_EQ(OperationOK, repo->InsertZoneRepo(r1));

    // insert duplicate chunk server id
    ASSERT_EQ(SqlException, repo->InsertZoneRepo(r1));

    // insert zone id=2
    ZoneRepo r2(2, "test", 1, "second zone");
    ASSERT_EQ(OperationOK, repo->InsertZoneRepo(r2));

    // query zone id=2
    ZoneRepo queryRes;
    ASSERT_EQ(OperationOK, repo->QueryZoneRepo(r1.zoneID, &queryRes));
    ASSERT_TRUE(queryRes == r1);

    // query zone id=3
    queryRes.zoneID = 0;
    ASSERT_EQ(OperationOK, repo->QueryZoneRepo(3, &queryRes));
    ASSERT_EQ(0, queryRes.zoneID);

    // query all zones
    std::vector<ZoneRepo> zones;
    ASSERT_EQ(OperationOK, repo->LoadZoneRepos(&zones));
    ASSERT_EQ(2, zones.size());
    ASSERT_TRUE(r1 == zones[0]);
    ASSERT_TRUE(r2 == zones[1]);

    // update desc
    r1.desc = "init first zone";
    ASSERT_EQ(OperationOK, repo->UpdateZoneRepo(r1));
    queryRes.zoneID = 0;
    ASSERT_EQ(OperationOK, repo->QueryZoneRepo(r1.zoneID, &queryRes));
    ASSERT_EQ(r1.desc, queryRes.desc);

    // delete zone id=1
    ASSERT_EQ(OperationOK, repo->DeleteZoneRepo(r1.zoneID));
    queryRes.zoneID = 0;
    ASSERT_EQ(OperationOK, repo->QueryZoneRepo(r1.zoneID, &queryRes));
    ASSERT_EQ(0, queryRes.zoneID);

    // close statement, query get sqlException
    repo->getDataBase()->statement_->close();
    ASSERT_EQ(SqlException, repo->QueryZoneRepo(2, &queryRes));
    ASSERT_EQ(SqlException, repo->LoadZoneRepos(&zones));
}

TEST_F(RepoTest, testLogicalPoolCUDA) {
    // insert logical pool, id=1
    std::time_t result;
    std::localtime(&result);
    Json::Value rpPolicy;
    rpPolicy["rule"] = "EC";
    Json::Value userPolicy;
    userPolicy["replica"] = 3;
    LogicalPoolRepo r1(1,
                       "logical pool 1",
                       1,
                       StoreType,
                       result,
                       Healthy,
                       rpPolicy.toStyledString(),
                       userPolicy.toStyledString());

    ASSERT_EQ(OperationOK, repo->InsertLogicalPoolRepo(r1));

    // insert duplicate logical pool
    ASSERT_EQ(SqlException, repo->InsertLogicalPoolRepo(r1));

    // insert logical pool, id=2
    LogicalPoolRepo r2(r1);
    r2.logicalPoolID = 2;
    ASSERT_EQ(OperationOK, repo->InsertLogicalPoolRepo(r2));

    // query logical pool, id=1
    LogicalPoolRepo queryRes;
    ASSERT_EQ(OperationOK,
              repo->QueryLogicalPoolRepo(r1.logicalPoolID, &queryRes));

    // query all logical pools
    std::vector<LogicalPoolRepo> logicalPools;
    ASSERT_EQ(OperationOK, repo->LoadLogicalPoolRepos(&logicalPools));
    ASSERT_EQ(2, logicalPools.size());
    ASSERT_TRUE(r1 == logicalPools[0]);
    ASSERT_TRUE(r2 == logicalPools[1]);

    // update status
    r1.status = Unhealthy;
    ASSERT_EQ(OperationOK, repo->UpdateLogicalPoolRepo(r1));
    queryRes.logicalPoolID = 0;
    ASSERT_EQ(OperationOK,
              repo->QueryLogicalPoolRepo(r1.logicalPoolID, &queryRes));
    ASSERT_EQ(r1.status, queryRes.status);

    // delete logical pool id=1
    ASSERT_EQ(OperationOK, repo->DeleteLogicalPoolRepo(r1.logicalPoolID));
    queryRes.logicalPoolID = 0;
    ASSERT_EQ(OperationOK,
              repo->QueryLogicalPoolRepo(r1.logicalPoolID, &queryRes));
    ASSERT_EQ(0, queryRes.logicalPoolID);

    // close conn, query get sqlException
    repo->getDataBase()->statement_->close();
    ASSERT_EQ(SqlException, repo->QueryLogicalPoolRepo(2, &queryRes));
    ASSERT_EQ(SqlException, repo->LoadLogicalPoolRepos(&logicalPools));
}

TEST_F(RepoTest, testPhysicalPoolCUDA) {
    // insert physical pool, id=1
    PhysicalPoolRepo r1(1, "physical pool 1", "first physical pool");
    ASSERT_EQ(OperationOK, repo->InsertPhysicalPoolRepo(r1));

    // insert duplicate physical pool id
    ASSERT_EQ(SqlException, repo->InsertPhysicalPoolRepo(r1));

    // insert physical pool, id=2
    PhysicalPoolRepo r2(2, "physical pool 2", "second physical pool");
    ASSERT_EQ(OperationOK, repo->InsertPhysicalPoolRepo(r2));

    // query physical pool id=1
    PhysicalPoolRepo queryRes;
    ASSERT_EQ(OperationOK,
              repo->QueryPhysicalPoolRepo(r1.physicalPoolID, &queryRes));
    ASSERT_TRUE(queryRes == r1);

    // query physical pool id=3, empty
    queryRes.physicalPoolID = 0;
    ASSERT_EQ(OperationOK, repo->QueryPhysicalPoolRepo(3, &queryRes));
    ASSERT_EQ(0, queryRes.physicalPoolID);

    // query all physical pools
    std::vector<PhysicalPoolRepo> physicalPools;
    ASSERT_EQ(OperationOK, repo->LoadPhysicalPoolRepos(&physicalPools));
    ASSERT_EQ(2, physicalPools.size());
    ASSERT_TRUE(r1 == physicalPools[0]);
    ASSERT_TRUE(r2 == physicalPools[1]);

    // update desc
    r1.desc = "init first physical pool";
    ASSERT_EQ(OperationOK, repo->UpdatePhysicalPoolRepo(r1));
    queryRes.physicalPoolID = 0;
    ASSERT_EQ(OperationOK,
              repo->QueryPhysicalPoolRepo(r1.physicalPoolID, &queryRes));
    ASSERT_EQ(r1.desc, queryRes.desc);

    // delete physical pool id=1
    ASSERT_EQ(OperationOK, repo->DeletePhysicalPoolRepo(r1.physicalPoolID));
    queryRes.physicalPoolID = 0;
    ASSERT_EQ(OperationOK,
              repo->QueryPhysicalPoolRepo(r1.physicalPoolID, &queryRes));
    ASSERT_EQ(0, queryRes.physicalPoolID);

    // close conn, query get sqlException
    repo->getDataBase()->statement_->close();
    ASSERT_EQ(SqlException, repo->QueryPhysicalPoolRepo(2, &queryRes));
    ASSERT_EQ(SqlException, repo->LoadPhysicalPoolRepos(&physicalPools));
}

TEST_F(RepoTest, testCopySetCUDA) {
    // insert copySet, id=1,logicalPoolID=1
    CopySetRepo r1(1, 1, "1-2-3");
    ASSERT_EQ(OperationOK, repo->InsertCopySetRepo(r1));

    // insert duplicate copySet id=1 in same logical pool
    ASSERT_EQ(SqlException, repo->InsertCopySetRepo(r1));

    // insert copySet id=1 in logical pool 2
    CopySetRepo r2(1, 2, "1-2-3");
    ASSERT_EQ(OperationOK, repo->InsertCopySetRepo(r2));

    // query copySet id=1, lid=1
    CopySetRepo queryRes;
    ASSERT_EQ(OperationOK,
              repo->QueryCopySetRepo(r1.copySetID,
                                     r1.logicalPoolID,
                                     &queryRes));
    ASSERT_TRUE(queryRes == r1);

    // query all copySets
    std::vector<CopySetRepo> copySets;
    ASSERT_EQ(OperationOK, repo->LoadCopySetRepos(&copySets));
    ASSERT_EQ(2, copySets.size());
    ASSERT_TRUE(r1 == copySets[0]);
    ASSERT_TRUE(r2 == copySets[1]);

    // update chunkserver list
    r1.chunkServerIDList = "2-3-4";
    ASSERT_EQ(OperationOK, repo->UpdateCopySetRepo(r1));
    queryRes.copySetID = 0;
    queryRes.logicalPoolID = 0;
    ASSERT_EQ(OperationOK,
              repo->QueryCopySetRepo(r1.copySetID,
                                     r1.logicalPoolID,
                                     &queryRes));
    ASSERT_TRUE(r1 == queryRes);

    // delete copyset id=1,lid=1
    ASSERT_EQ(OperationOK,
              repo->DeleteCopySetRepo(r1.copySetID, r1.logicalPoolID));
    queryRes.copySetID = 0;
    queryRes.logicalPoolID = 0;
    ASSERT_EQ(OperationOK,
              repo->QueryCopySetRepo(r1.copySetID,
                                     r1.logicalPoolID,
                                     &queryRes));
    ASSERT_EQ(0, queryRes.copySetID);
    ASSERT_EQ(0, queryRes.logicalPoolID);

    // close conn, query get sqlException
    repo->getDataBase()->statement_->close();
    ASSERT_EQ(SqlException, repo->QueryCopySetRepo(1, 2, &queryRes));
    ASSERT_EQ(SqlException, repo->LoadCopySetRepos(&copySets));
}
}  // namespace repo
}  // namespace curve

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);

    return RUN_ALL_TESTS();
}
