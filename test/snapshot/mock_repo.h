/*************************************************************************
> File Name: mock_repo.h
> Author:
> Created Time: Thu 27 Dec 2018 09:56:04 PM CST
> Copyright (c) 2018 netease
 ************************************************************************/
#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include "src/snapshot/repo/repo.h"
#include <string>  //NOLINT
#include <vector>  //NOLINT
#ifndef _MOCK_REPO_H
#define _MOCK_REO_H

using ::testing::Return;
using ::testing::_;
namespace curve {
namespace snapshotserver {
class MockRepo : public RepoInterface {
 public:
  MockRepo() {}
  ~MockRepo() {}

  MOCK_METHOD4(connectDB, int(
      const std::string &dbName,
      const std::string &user,
      const std::string &url,
      const std::string &password));

  MOCK_METHOD0(createAllTables, int());
  MOCK_METHOD0(createDataBase, int());
  MOCK_METHOD0(useDataBase, int());
  MOCK_METHOD0(dropDataBase, int());

  MOCK_METHOD1(InsertSnapshotRepo,
               int(
                   const SnapshotRepo &sr));

  MOCK_METHOD1(LoadSnapshotRepos,
               int(std::vector<SnapshotRepo>
                   *SnapshotRepoList));

  MOCK_METHOD1(DeleteSnapshotRepo,
               int(const std::string
                   uuid));

  MOCK_METHOD1(UpdateSnapshotRepo,
               int(
                   const SnapshotRepo &sr));

  MOCK_METHOD2(QuerySnapshotRepo,
               int(const std::string
                   uuid, SnapshotRepo * repo));
};
}  // namespace snapshotserver
}  // namespace curve
#endif
