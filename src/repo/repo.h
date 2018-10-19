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

#include "src/repo/repoItem.h"
#include "src/repo/dataBase.h"
#include "src/repo/sqlStatement.h"
namespace curve {
namespace repo {

class RepoInterface {
 public:
  // constructor: open db
  // destructtor: close db
  virtual int connectDB(const std::string &dbName, const std::string &user,
                const std::string &url, const std::string &password) = 0;

  virtual int createAllTables() = 0;

  virtual int createDatabase() = 0;

  virtual int useDataBase() = 0;

  virtual int dropDataBase() = 0;

  // chunkServerRepo operation
  virtual int InsertChunkServerRepo(const ChunkServerRepo &cr) = 0;

  virtual int LoadChunkServerRepos(std::vector<ChunkServerRepo> *chunkServerRepoList) = 0;

  virtual int DeleteChunkServerRepo(ChunkServerIDType id) = 0;

  virtual int UpdateChunkServerRepo(const ChunkServerRepo &cr) = 0;

  virtual int QueryChunkServerRepo(ChunkServerIDType id, ChunkServerRepo *repo) = 0;

  // server operation
  virtual int InsertServerRepo(const ServerRepo &sr) = 0;

  virtual int LoadServerRepos(std::vector<ServerRepo> *serverList) = 0;

  virtual int DeleteServerRepo(ServerIDType id) = 0;

  virtual int UpdateServerRepo(const ServerRepo &sr) = 0;

  virtual int QueryServerRepo(ServerIDType id, ServerRepo *repo) = 0;

  // zone operation
  virtual int InsertZoneRepo(const ZoneRepo &zr) = 0;

  virtual int LoadZoneRepos(std::vector<ZoneRepo> *zonevector) = 0;

  virtual int DeleteZoneRepo(ZoneIDType id) = 0;

  virtual int UpdateZoneRepo(const ZoneRepo &zr) = 0;

  virtual int QueryZoneRepo(ZoneIDType id, ZoneRepo *repo) = 0;

  // physical pool operation
  virtual int InsertPhysicalPoolRepo(const PhysicalPoolRepo &pr) = 0;

  virtual int LoadPhysicalPoolRepos(std::vector<PhysicalPoolRepo> *physicalPoolvector) = 0;

  virtual int DeletePhysicalPoolRepo(PhysicalPoolIDType id) = 0;

  virtual int UpdatePhysicalPoolRepo(const PhysicalPoolRepo &pr) = 0;

  virtual int QueryPhysicalPoolRepo(PhysicalPoolIDType id, PhysicalPoolRepo *repo) = 0;

  // logical pool operation
  virtual int InsertLogicalPoolRepo(const LogicalPoolRepo &lr) = 0;

  virtual int LoadLogicalPoolRepos(std::vector<LogicalPoolRepo> *logicalPoolList) = 0;

  virtual int DeleteLogicalPoolRepo(LogicalPoolIDType id) = 0;

  virtual int UpdateLogicalPoolRepo(const LogicalPoolRepo &lr) = 0;

  virtual int QueryLogicalPoolRepo(LogicalPoolIDType id, LogicalPoolRepo *repo) = 0;

  // copyset operation
  virtual int InsertCopySetRepo(const CopySetRepo &cr) = 0;

  virtual int LoadCopySetRepos(std::vector<CopySetRepo> *copySetList) = 0;

  virtual int DeleteCopySetRepo(CopySetIDType id, LogicalPoolIDType lid) = 0;

  virtual int UpdateCopySetRepo(const CopySetRepo &cr) = 0;

  virtual int QueryCopySetRepo(CopySetIDType id,
                       LogicalPoolIDType lid,
                       CopySetRepo *repo) = 0;
};

class Repo : public RepoInterface {
 public:
  // constructor: open db
  // destructtor: close db
  int connectDB(const std::string &dbName, const std::string &user,
                const std::string &url, const std::string &password) override;

  int createAllTables() override;

  int createDatabase() override;

  int useDataBase() override;

  int dropDataBase() override;

  // chunkServerRepo operation
  int InsertChunkServerRepo(const ChunkServerRepo &cr) override;

  int LoadChunkServerRepos(std::vector<ChunkServerRepo> *chunkServerRepoList) override;

  int DeleteChunkServerRepo(ChunkServerIDType id) override;

  int UpdateChunkServerRepo(const ChunkServerRepo &cr) override;

  int QueryChunkServerRepo(ChunkServerIDType id, ChunkServerRepo *repo) override;

  // server operation
  int InsertServerRepo(const ServerRepo &sr) override;

  int LoadServerRepos(std::vector<ServerRepo> *serverList) override;

  int DeleteServerRepo(ServerIDType id) override;

  int UpdateServerRepo(const ServerRepo &sr) override;

  int QueryServerRepo(ServerIDType id, ServerRepo *repo) override;

  // zone operation
  int InsertZoneRepo(const ZoneRepo &zr) override;

  int LoadZoneRepos(std::vector<ZoneRepo> *zonevector) override;

  int DeleteZoneRepo(ZoneIDType id) override;

  int UpdateZoneRepo(const ZoneRepo &zr) override;

  int QueryZoneRepo(ZoneIDType id, ZoneRepo *repo) override;

  // physical pool operation
  int InsertPhysicalPoolRepo(const PhysicalPoolRepo &pr) override;

  int LoadPhysicalPoolRepos(std::vector<PhysicalPoolRepo> *physicalPoolvector) override;

  int DeletePhysicalPoolRepo(PhysicalPoolIDType id) override;

  int UpdatePhysicalPoolRepo(const PhysicalPoolRepo &pr) override;

  int QueryPhysicalPoolRepo(PhysicalPoolIDType id, PhysicalPoolRepo *repo) override;

  // logical pool operation
  int InsertLogicalPoolRepo(const LogicalPoolRepo &lr) override;

  int LoadLogicalPoolRepos(std::vector<LogicalPoolRepo> *logicalPoolList) override;

  int DeleteLogicalPoolRepo(LogicalPoolIDType id) override;

  int UpdateLogicalPoolRepo(const LogicalPoolRepo &lr) override;

  int QueryLogicalPoolRepo(LogicalPoolIDType id, LogicalPoolRepo *repo) override;

  // copyset operation
  int InsertCopySetRepo(const CopySetRepo &cr) override;

  int LoadCopySetRepos(std::vector<CopySetRepo> *copySetList) override;

  int DeleteCopySetRepo(CopySetIDType id, LogicalPoolIDType lid) override;

  int UpdateCopySetRepo(const CopySetRepo &cr) override;

  int QueryCopySetRepo(CopySetIDType id,
                       LogicalPoolIDType lid,
                       CopySetRepo *repo) override;

 private:
  DataBase *db_;
  std::string dbName_;
};
}  // namespace repo
}  // namespace curve

#endif  // REPO_REPO_H_
