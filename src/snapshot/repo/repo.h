/*
 * Project: curve
 * Created Date: Wed Sep 04 2018
 * Author: lixiaocui
 * Copyright (c) 2018 netease
 */


#ifndef REPO_REPO_H_
#define REPO_REPO_H_

#include <vector>
#include <string>

#include "src/snapshot/repo/repoItem.h"
#include "src/snapshot/repo/dataBase.h"
#include "src/snapshot/repo/sqlStatement.h"
namespace curve {
namespace snapshotserver {

class RepoInterface {
 public:
  RepoInterface() {}
  virtual ~RepoInterface() {}
  // constructor: open db
  // destructtor: close db
  virtual int connectDB(const std::string &dbName,
                        const std::string &user,
                        const std::string &url,
                        const std::string &password) = 0;

  virtual int createAllTables() = 0;

  virtual int createDataBase() = 0;

  virtual int useDataBase() = 0;

  virtual int dropDataBase() = 0;
  // snapshot repo operation
  virtual int InsertSnapshotRepo(const SnapshotRepo &sr) = 0;
  virtual int DeleteSnapshotRepo(const std::string uuid) = 0;
  virtual int UpdateSnapshotRepo(const SnapshotRepo &sr) = 0;
  virtual int QuerySnapshotRepo(const std::string uuid, SnapshotRepo *sr) = 0;
  virtual int LoadSnapshotRepos(std::vector<SnapshotRepo> *snapshotlist) = 0;
};

class Repo : public RepoInterface {
 public:
  // constructor: open db
  // destructtor: close db
  Repo() = default;

  virtual ~Repo();

  int connectDB(const std::string &dbName, const std::string &user,
                const std::string &url, const std::string &password) override;

  int createAllTables() override;

  int createDataBase() override;

  int useDataBase() override;

  int dropDataBase() override;

  DataBase *getDataBase();

  int InsertSnapshotRepo(const SnapshotRepo &sr) override;
  int DeleteSnapshotRepo(const std::string uuid) override;
  int UpdateSnapshotRepo(const SnapshotRepo &sr) override;
  int QuerySnapshotRepo(const std::string uuid, SnapshotRepo *sr) override;
  int LoadSnapshotRepos(std::vector<SnapshotRepo> *snapshotlist) override;

 private:
  DataBase *db_;
  std::string dbName_;
};
}  // namespace snapshotserver
}  // namespace curve

#endif  // REPO_REPO_H_
