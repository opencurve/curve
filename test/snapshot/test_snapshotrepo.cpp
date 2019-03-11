/*
 * Project: curve
 * Created Date: Tue Sep 18 2018
 * Author: lixiaocui
 * Copyright (c) 2018 netease
 */


#include <gtest/gtest.h>
#include <json/json.h>
#include "src/snapshot/dao/snapshotRepo.h"

namespace curve {
namespace snapshotserver {
const uint8_t RW = 0;
const uint8_t Healthy = 0;
const uint8_t Unhealthy = 1;
const uint8_t Online = 0;
const uint8_t StoreType = 0;

using ::curve::repo::OperationOK;
using ::curve::repo::SqlException;
using ::curve::repo::RuntimeExecption;
using ::curve::repo::ConnLost;
using ::curve::repo::InternalError;

class RepoTest : public ::testing::Test {
 public:
  void SetUp() override {
      repo = new SnapshotRepo();
      ASSERT_EQ(OperationOK,
                repo->connectDB("curve_snapshot_repo_test",
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

  SnapshotRepo *repo;
};

TEST_F(RepoTest, testSnapshotRepoCUDA) {
    SnapshotRepoItem sr1("uuid1",
                       "curve",
                       "test",
                       "mysnap",
                       1,
                       1024,
                       10240,
                       102400,
                       9999,
                       1);
    ASSERT_EQ(OperationOK, repo->InsertSnapshotRepoItem(sr1));

    // query id=uuid-test
    SnapshotRepoItem queryRes;
    ASSERT_EQ(OperationOK,
              repo->QuerySnapshotRepoItem(sr1.uuid, &queryRes));
    ASSERT_TRUE(queryRes == sr1);

    // query all
    std::vector<SnapshotRepoItem> list;
    ASSERT_EQ(OperationOK, repo->LoadSnapshotRepoItems(&list));
    ASSERT_EQ(1, list.size());
    ASSERT_TRUE(sr1 == list[0]);

    // update used
    sr1.status = 0;
    ASSERT_EQ(OperationOK, repo->UpdateSnapshotRepoItem(sr1));
    queryRes.uuid = "uuid-test";
    ASSERT_EQ(OperationOK,
              repo->QuerySnapshotRepoItem(sr1.uuid, &queryRes));
    ASSERT_EQ(sr1.status, queryRes.status);

    // delete id=uuid-test
    ASSERT_EQ(OperationOK, repo->DeleteSnapshotRepoItem(sr1.uuid));
    ASSERT_EQ(OperationOK,
              repo->QuerySnapshotRepoItem(sr1.uuid, &queryRes));

    // close statement, query get sqlException
    repo->getDataBase()->statement_->close();
    ASSERT_EQ(SqlException, repo->QuerySnapshotRepoItem("test", &queryRes));
    ASSERT_EQ(SqlException, repo->LoadSnapshotRepoItems(&list));
    ASSERT_EQ(SqlException, repo->createAllTables());
}

}  // namespace snapshotserver
}  // namespace curve

