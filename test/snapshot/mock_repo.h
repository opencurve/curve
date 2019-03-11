/*************************************************************************
> File Name: mock_repo.h
> Author:
> Created Time: Thu 27 Dec 2018 09:56:04 PM CST
> Copyright (c) 2018 netease
 ************************************************************************/
#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include "src/snapshot/dao/snapshotRepo.h"
#include <string>  //NOLINT
#include <vector>  //NOLINT
#ifndef _MOCK_REPO_H
#define _MOCK_REO_H

using ::testing::Return;
using ::testing::_;
namespace curve {
namespace snapshotserver {
class MockRepo : public SnapshotRepo {
 public:
  MockRepo() {}
  ~MockRepo() {}

  MOCK_METHOD4(connectDB, int(
      const std::string &dbName,
      const std::string &user,
      const std::string &url,
      const std::string &password));

  MOCK_METHOD0(createAllTables, int());
  MOCK_METHOD0(createDatabase, int());
  MOCK_METHOD0(useDataBase, int());
  MOCK_METHOD0(dropDataBase, int());

  MOCK_METHOD1(InsertSnapshotRepoItem,
               int(const SnapshotRepoItem &sr));

  MOCK_METHOD1(LoadSnapshotRepoItems,
               int(std::vector<SnapshotRepoItem>
                   *SnapshotRepoList));

  MOCK_METHOD1(DeleteSnapshotRepoItem,
               int(const std::string
                   uuid));

  MOCK_METHOD1(UpdateSnapshotRepoItem,
               int(
                   const SnapshotRepoItem &sr));

  MOCK_METHOD2(QuerySnapshotRepoItem,
               int(const std::string
                   uuid, SnapshotRepoItem * repo));
};
}  // namespace snapshotserver
}  // namespace curve
#endif
