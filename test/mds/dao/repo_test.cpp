/*
 * Project: curve
 * Created Date: Tue Sep 18 2018
 * Author: lixiaocui
 * Copyright (c) 2018 netease
 */


#include <gtest/gtest.h>
#include <json/json.h>
#include "src/mds/dao/mdsRepo.h"

namespace curve {
namespace mds {
const uint8_t RW = 0;
const uint8_t Healthy = 0;
const uint8_t Unhealthy = 1;
const uint8_t Online = 0;
const uint8_t StoreType = 0;

class RepoItemTest : public ::testing::Test {
 public:
  void SetUp() override {
      repo = new MdsRepo();
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

  MdsRepo *repo;
};

TEST_F(RepoItemTest, testChunkserverCUDA) {
    // insert chunk server, id=1
    // success
    ChunkServerRepoItem r1(1,
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
    ASSERT_EQ(OperationOK, repo->InsertChunkServerRepoItem(r1));

    // insert duplicate chunk server id
    // err
    ASSERT_EQ(SqlException, repo->InsertChunkServerRepoItem(r1));

    // insert chunk server, id=2
    ChunkServerRepoItem r2(r1);
    r2.chunkServerID = 2;
    r2.port = 9001;
    r2.mountPoint = "/mnt/2";
    ASSERT_EQ(OperationOK, repo->InsertChunkServerRepoItem(r2));

    // query chunk server id=1
    ChunkServerRepoItem queryRes;
    ASSERT_EQ(OperationOK,
              repo->QueryChunkServerRepoItem(r1.chunkServerID, &queryRes));
    ASSERT_TRUE(queryRes == r1);

    // query chunk server id=3, empty
    queryRes.chunkServerID = 0;
    ASSERT_EQ(OperationOK, repo->QueryChunkServerRepoItem(3, &queryRes));
    ASSERT_EQ(0, queryRes.chunkServerID);

    // query all chunk servers
    std::vector<ChunkServerRepoItem> chunkServers;
    ASSERT_EQ(OperationOK, repo->LoadChunkServerRepoItems(&chunkServers));
    ASSERT_EQ(2, chunkServers.size());
    ASSERT_TRUE(r1 == chunkServers[0]);
    ASSERT_TRUE(r2 == chunkServers[1]);

    // update used
    r1.used = 15 << 3;
    ASSERT_EQ(OperationOK, repo->UpdateChunkServerRepoItem(r1));
    queryRes.chunkServerID = 0;
    ASSERT_EQ(OperationOK,
              repo->QueryChunkServerRepoItem(r1.chunkServerID, &queryRes));
    ASSERT_EQ(r1.used, queryRes.used);

    // delete chunk server id=1
    ASSERT_EQ(OperationOK, repo->DeleteChunkServerRepoItem(r1.chunkServerID));
    queryRes.chunkServerID = 0;
    ASSERT_EQ(OperationOK,
              repo->QueryChunkServerRepoItem(r1.chunkServerID, &queryRes));
    ASSERT_EQ(0, queryRes.chunkServerID);

    // close statement, query get sqlException
    repo->getDataBase()->statement_->close();
    ASSERT_EQ(SqlException, repo->QueryChunkServerRepoItem(2, &queryRes));
    ASSERT_EQ(SqlException, repo->LoadChunkServerRepoItems(&chunkServers));
    ASSERT_EQ(SqlException, repo->createAllTables());
}

TEST_F(RepoItemTest, testServerCUDA) {
    // insert server id=1
    ServerRepoItem s1(1,
                  "curve-nos1.dg.163.org",
                  "10.172.168.1",
                  0,
                  "192.168.2.1",
                  0,
                  1,
                  1,
                  "first server");
    ASSERT_EQ(OperationOK, repo->InsertServerRepoItem(s1));

    // insert duplicate server id
    ASSERT_EQ(SqlException, repo->InsertServerRepoItem(s1));

    // insert server id=2
    ServerRepoItem s2(2,
                  "curve-nos2.dg.163.org",
                  "10.172.168.2",
                  0,
                  "192.168.2.2",
                  0,
                  1,
                  2,
                  "second server");
    ASSERT_EQ(OperationOK, repo->InsertServerRepoItem(s2));

    // query server id=2
    ServerRepoItem queryRes(0);
    ASSERT_EQ(OperationOK, repo->QueryServerRepoItem(s1.serverID, &queryRes));
    ASSERT_TRUE(queryRes == s1);

    // query server id=3, empty
    queryRes.serverID = 0;
    ASSERT_EQ(OperationOK, repo->QueryServerRepoItem(3, &queryRes));
    ASSERT_EQ(0, queryRes.serverID);

    // query all servers
    std::vector<ServerRepoItem> servers;
    ASSERT_EQ(OperationOK, repo->LoadServerRepoItems(&servers));
    ASSERT_EQ(2, servers.size());
    ASSERT_TRUE(s1 == servers[0]);
    ASSERT_TRUE(s2 == servers[1]);

    // update hostname
    s1.hostName = "curve-nos0.dg.163.org";
    ASSERT_EQ(OperationOK, repo->UpdateServerRepoItem(s1));
    queryRes.serverID = 0;
    ASSERT_EQ(OperationOK, repo->QueryServerRepoItem(s1.serverID, &queryRes));
    ASSERT_EQ(s1.hostName, queryRes.hostName);

    // delete server id=1
    ASSERT_EQ(OperationOK, repo->DeleteServerRepoItem(s1.serverID));
    queryRes.serverID = 0;
    ASSERT_EQ(OperationOK, repo->QueryServerRepoItem(s1.serverID, &queryRes));
    ASSERT_EQ(0, queryRes.serverID);

    // close statement, query get sqlException
    repo->getDataBase()->statement_->close();
    ASSERT_EQ(SqlException, repo->QueryServerRepoItem(2, &queryRes));
    ASSERT_EQ(SqlException, repo->LoadServerRepoItems(&servers));
}

TEST_F(RepoItemTest, testZoneCUDA) {
    // insert zone id=1
    ZoneRepoItem r1(1, "test", 1, "first zone");
    ASSERT_EQ(OperationOK, repo->InsertZoneRepoItem(r1));

    // insert duplicate chunk server id
    ASSERT_EQ(SqlException, repo->InsertZoneRepoItem(r1));

    // insert zone id=2
    ZoneRepoItem r2(2, "test", 1, "second zone");
    ASSERT_EQ(OperationOK, repo->InsertZoneRepoItem(r2));

    // query zone id=2
    ZoneRepoItem queryRes;
    ASSERT_EQ(OperationOK, repo->QueryZoneRepoItem(r1.zoneID, &queryRes));
    ASSERT_TRUE(queryRes == r1);

    // query zone id=3
    queryRes.zoneID = 0;
    ASSERT_EQ(OperationOK, repo->QueryZoneRepoItem(3, &queryRes));
    ASSERT_EQ(0, queryRes.zoneID);

    // query all zones
    std::vector<ZoneRepoItem> zones;
    ASSERT_EQ(OperationOK, repo->LoadZoneRepoItems(&zones));
    ASSERT_EQ(2, zones.size());
    ASSERT_TRUE(r1 == zones[0]);
    ASSERT_TRUE(r2 == zones[1]);

    // update desc
    r1.desc = "init first zone";
    ASSERT_EQ(OperationOK, repo->UpdateZoneRepoItem(r1));
    queryRes.zoneID = 0;
    ASSERT_EQ(OperationOK, repo->QueryZoneRepoItem(r1.zoneID, &queryRes));
    ASSERT_EQ(r1.desc, queryRes.desc);

    // delete zone id=1
    ASSERT_EQ(OperationOK, repo->DeleteZoneRepoItem(r1.zoneID));
    queryRes.zoneID = 0;
    ASSERT_EQ(OperationOK, repo->QueryZoneRepoItem(r1.zoneID, &queryRes));
    ASSERT_EQ(0, queryRes.zoneID);

    // close statement, query get sqlException
    repo->getDataBase()->statement_->close();
    ASSERT_EQ(SqlException, repo->QueryZoneRepoItem(2, &queryRes));
    ASSERT_EQ(SqlException, repo->LoadZoneRepoItems(&zones));
}

TEST_F(RepoItemTest, testLogicalPoolCUDA) {
    // insert logical pool, id=1
    std::time_t result;
    std::localtime(&result);
    Json::Value rpPolicy;
    rpPolicy["rule"] = "EC";
    Json::Value userPolicy;
    userPolicy["replica"] = 3;
    LogicalPoolRepoItem r1(1,
                       "logical pool 1",
                       1,
                       StoreType,
                       result,
                       Healthy,
                       rpPolicy.toStyledString(),
                       userPolicy.toStyledString(),
                       true);

    ASSERT_EQ(OperationOK, repo->InsertLogicalPoolRepoItem(r1));

    // insert duplicate logical pool
    ASSERT_EQ(SqlException, repo->InsertLogicalPoolRepoItem(r1));

    // insert logical pool, id=2
    LogicalPoolRepoItem r2(r1);
    r2.logicalPoolID = 2;
    ASSERT_EQ(OperationOK, repo->InsertLogicalPoolRepoItem(r2));

    // query logical pool, id=1
    LogicalPoolRepoItem queryRes;
    ASSERT_EQ(OperationOK,
              repo->QueryLogicalPoolRepoItem(r1.logicalPoolID, &queryRes));

    // query all logical pools
    std::vector<LogicalPoolRepoItem> logicalPools;
    ASSERT_EQ(OperationOK, repo->LoadLogicalPoolRepoItems(&logicalPools));
    ASSERT_EQ(2, logicalPools.size());
    ASSERT_TRUE(r1 == logicalPools[0]);
    ASSERT_TRUE(r2 == logicalPools[1]);

    // update status
    r1.status = Unhealthy;
    ASSERT_EQ(OperationOK, repo->UpdateLogicalPoolRepoItem(r1));
    queryRes.logicalPoolID = 0;
    ASSERT_EQ(OperationOK,
              repo->QueryLogicalPoolRepoItem(r1.logicalPoolID, &queryRes));
    ASSERT_EQ(r1.status, queryRes.status);

    // delete logical pool id=1
    ASSERT_EQ(OperationOK, repo->DeleteLogicalPoolRepoItem(r1.logicalPoolID));
    queryRes.logicalPoolID = 0;
    ASSERT_EQ(OperationOK,
              repo->QueryLogicalPoolRepoItem(r1.logicalPoolID, &queryRes));
    ASSERT_EQ(0, queryRes.logicalPoolID);

    // close conn, query get sqlException
    repo->getDataBase()->statement_->close();
    ASSERT_EQ(SqlException, repo->QueryLogicalPoolRepoItem(2, &queryRes));
    ASSERT_EQ(SqlException, repo->LoadLogicalPoolRepoItems(&logicalPools));
}

TEST_F(RepoItemTest, testPhysicalPoolCUDA) {
    // insert physical pool, id=1
    PhysicalPoolRepoItem r1(1, "physical pool 1", "first physical pool");
    ASSERT_EQ(OperationOK, repo->InsertPhysicalPoolRepoItem(r1));

    // insert duplicate physical pool id
    ASSERT_EQ(SqlException, repo->InsertPhysicalPoolRepoItem(r1));

    // insert physical pool, id=2
    PhysicalPoolRepoItem r2(2, "physical pool 2", "second physical pool");
    ASSERT_EQ(OperationOK, repo->InsertPhysicalPoolRepoItem(r2));

    // query physical pool id=1
    PhysicalPoolRepoItem queryRes;
    ASSERT_EQ(OperationOK,
              repo->QueryPhysicalPoolRepoItem(r1.physicalPoolID, &queryRes));
    ASSERT_TRUE(queryRes == r1);

    // query physical pool id=3, empty
    queryRes.physicalPoolID = 0;
    ASSERT_EQ(OperationOK, repo->QueryPhysicalPoolRepoItem(3, &queryRes));
    ASSERT_EQ(0, queryRes.physicalPoolID);

    // query all physical pools
    std::vector<PhysicalPoolRepoItem> physicalPools;
    ASSERT_EQ(OperationOK, repo->LoadPhysicalPoolRepoItems(&physicalPools));
    ASSERT_EQ(2, physicalPools.size());
    ASSERT_TRUE(r1 == physicalPools[0]);
    ASSERT_TRUE(r2 == physicalPools[1]);

    // update desc
    r1.desc = "init first physical pool";
    ASSERT_EQ(OperationOK, repo->UpdatePhysicalPoolRepoItem(r1));
    queryRes.physicalPoolID = 0;
    ASSERT_EQ(OperationOK,
              repo->QueryPhysicalPoolRepoItem(r1.physicalPoolID, &queryRes));
    ASSERT_EQ(r1.desc, queryRes.desc);

    // delete physical pool id=1
    ASSERT_EQ(OperationOK, repo->DeletePhysicalPoolRepoItem(r1.physicalPoolID));
    queryRes.physicalPoolID = 0;
    ASSERT_EQ(OperationOK,
              repo->QueryPhysicalPoolRepoItem(r1.physicalPoolID, &queryRes));
    ASSERT_EQ(0, queryRes.physicalPoolID);

    // close conn, query get sqlException
    repo->getDataBase()->statement_->close();
    ASSERT_EQ(SqlException, repo->QueryPhysicalPoolRepoItem(2, &queryRes));
    ASSERT_EQ(SqlException, repo->LoadPhysicalPoolRepoItems(&physicalPools));
}

TEST_F(RepoItemTest, testCopySetCUDA) {
    // insert copySet, id=1,logicalPoolID=1
    CopySetRepoItem r1(1, 1, 1, "1-2-3");
    ASSERT_EQ(OperationOK, repo->InsertCopySetRepoItem(r1));

    // insert duplicate copySet id=1 in same logical pool
    ASSERT_EQ(SqlException, repo->InsertCopySetRepoItem(r1));

    // insert copySet id=1 in logical pool 2
    CopySetRepoItem r2(1, 2, 2, "1-2-3");
    ASSERT_EQ(OperationOK, repo->InsertCopySetRepoItem(r2));

    // query copySet id=1, lid=1
    CopySetRepoItem queryRes;
    ASSERT_EQ(OperationOK,
              repo->QueryCopySetRepoItem(r1.copySetID,
                                     r1.logicalPoolID,
                                     &queryRes));
    ASSERT_TRUE(queryRes == r1);
    ASSERT_EQ(r1.epoch, queryRes.epoch);
    ASSERT_EQ(r1.chunkServerIDList, queryRes.chunkServerIDList);

    // query all copySets
    std::vector<CopySetRepoItem> copySets;
    ASSERT_EQ(OperationOK, repo->LoadCopySetRepoItems(&copySets));
    ASSERT_EQ(2, copySets.size());
    ASSERT_TRUE(r1 == copySets[0]);
    ASSERT_EQ(r1.epoch, copySets[0].epoch);
    ASSERT_EQ(r1.chunkServerIDList, copySets[0].chunkServerIDList);
    ASSERT_TRUE(r2 == copySets[1]);
    ASSERT_EQ(r2.epoch, copySets[1].epoch);
    ASSERT_EQ(r2.chunkServerIDList, copySets[1].chunkServerIDList);

    // update chunkserver list
    r1.chunkServerIDList = "2-3-4";
    ASSERT_EQ(OperationOK, repo->UpdateCopySetRepoItem(r1));
    queryRes.copySetID = 0;
    queryRes.logicalPoolID = 0;
    ASSERT_EQ(OperationOK,
              repo->QueryCopySetRepoItem(r1.copySetID,
                                     r1.logicalPoolID,
                                     &queryRes));
    ASSERT_TRUE(r1 == queryRes);
    ASSERT_EQ(r1.chunkServerIDList, queryRes.chunkServerIDList);
    ASSERT_EQ(r1.epoch, queryRes.epoch);

    // delete copyset id=1,lid=1
    ASSERT_EQ(OperationOK,
              repo->DeleteCopySetRepoItem(r1.copySetID, r1.logicalPoolID));
    queryRes.copySetID = 0;
    queryRes.logicalPoolID = 0;
    ASSERT_EQ(OperationOK,
              repo->QueryCopySetRepoItem(r1.copySetID,
                                     r1.logicalPoolID,
                                     &queryRes));
    ASSERT_EQ(0, queryRes.copySetID);
    ASSERT_EQ(0, queryRes.logicalPoolID);

    // close conn, query get sqlException
    repo->getDataBase()->statement_->close();
    ASSERT_EQ(SqlException, repo->QueryCopySetRepoItem(1, 2, &queryRes));
    ASSERT_EQ(SqlException, repo->LoadCopySetRepoItems(&copySets));
}

TEST_F(RepoItemTest, testSessionCUDA) {
    // insert session sessionid1
    uint64_t leaseTime = 5000000;
    uint64_t createTime = 123456789;
    SessionRepoItem r1("/file1", "sessionid1", leaseTime,
                    0, createTime, "127.0.0.1");
    ASSERT_EQ(OperationOK, repo->InsertSessionRepoItem(r1));

    // sessionid1不唯一，插入失败
    ASSERT_EQ(SqlException, repo->InsertSessionRepoItem(r1));

    // insert session sessionid2
    SessionRepoItem r2("/file2", "sessionid2", leaseTime,
                    0, createTime, "127.0.0.1");
    ASSERT_EQ(OperationOK, repo->InsertSessionRepoItem(r2));

    // query session
    SessionRepoItem queryRes;
    ASSERT_EQ(OperationOK,
              repo->QuerySessionRepoItem(r1.sessionID,
                                     &queryRes));
    ASSERT_TRUE(queryRes == r1);
    ASSERT_EQ(r1.sessionID, queryRes.sessionID);
    ASSERT_EQ(r1.fileName, queryRes.fileName);
    ASSERT_EQ(r1.leaseTime, queryRes.leaseTime);
    ASSERT_EQ(r1.sessionStatus, queryRes.sessionStatus);
    ASSERT_EQ(r1.createTime, queryRes.createTime);
    ASSERT_EQ(r1.clientIP, queryRes.clientIP);

    // query all session
    std::vector<SessionRepoItem> sessionList;
    ASSERT_EQ(OperationOK, repo->LoadSessionRepoItems(&sessionList));
    ASSERT_EQ(2, sessionList.size());
    ASSERT_TRUE(r1 == sessionList[0]);
    ASSERT_EQ(r1.sessionID, sessionList[0].sessionID);
    ASSERT_EQ(r1.fileName, sessionList[0].fileName);
    ASSERT_EQ(r1.sessionStatus, sessionList[0].sessionStatus);
    ASSERT_TRUE(r2 == sessionList[1]);
    ASSERT_EQ(r2.sessionID, sessionList[1].sessionID);
    ASSERT_EQ(r2.fileName, sessionList[1].fileName);
    ASSERT_EQ(r2.sessionStatus, sessionList[1].sessionStatus);

    // update session
    r1.sessionStatus = 1;
    ASSERT_EQ(OperationOK, repo->UpdateSessionRepoItem(r1));
    SessionRepoItem queryRes1;
    ASSERT_EQ(OperationOK,
              repo->QuerySessionRepoItem("sessionid1",
                                     &queryRes1));
    ASSERT_TRUE(r1 == queryRes1);
    ASSERT_EQ(r1.sessionID, queryRes1.sessionID);
    ASSERT_EQ(1, queryRes1.sessionStatus);

    // delete session
    ASSERT_EQ(OperationOK,
              repo->DeleteSessionRepoItem(r1.sessionID));
    SessionRepoItem queryRes2;
    queryRes2.sessionID = "sessionIDtest";
    ASSERT_EQ(OperationOK,
              repo->QuerySessionRepoItem(r1.sessionID,
                                     &queryRes2));
    ASSERT_EQ("sessionIDtest", queryRes2.sessionID);

    // close conn, query get sqlException
    repo->getDataBase()->statement_->close();
    ASSERT_EQ(SqlException,
        repo->QuerySessionRepoItem(r1.sessionID, &queryRes));
    ASSERT_EQ(SqlException, repo->LoadSessionRepoItems(&sessionList));
}

TEST_F(RepoItemTest, testCheckConn) {
    repo->getDataBase()->conn_->close();
    CopySetRepoItem r1(1, 1, 1, "1-2-3");
    ASSERT_EQ(ConnLost, repo->InsertCopySetRepoItem(r1));
}
}  // namespace mds
}  // namespace curve

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);

    return RUN_ALL_TESTS();
}
