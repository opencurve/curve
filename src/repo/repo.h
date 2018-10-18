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
class Repo {
 public:
  // constructor: open db
  // destructtor: close db
  int connectDB(const std::string &dbName, const std::string &user,
                const std::string &url, const std::string &password);

  int createAllTables();

  int createDatabase();

  int useDataBase();

  int dropDataBase();

  // chunkServerRepo operation
  int InsertChunkServerRepo(const ChunkServerRepo &cr);

  int LoadChunkServerRepos(std::vector<ChunkServerRepo> *chunkServerRepoList);

  int DeleteChunkServerRepo(ChunkServerIDType id);

  int UpdateChunkServerRepo(const ChunkServerRepo &cr);

  int QueryChunkServerRepo(ChunkServerIDType id, ChunkServerRepo *repo);

  // server operation
  int InsertServerRepo(const ServerRepo &sr);

  int LoadServerRepos(std::vector<ServerRepo> *serverList);

  int DeleteServerRepo(ServerIDType id);

  int UpdateServerRepo(const ServerRepo &sr);

  int QueryServerRepo(ServerIDType id, ServerRepo *repo);

  // zone operation
  int InsertZoneRepo(const ZoneRepo &zr);

  int LoadZoneRepos(std::vector<ZoneRepo> *zonevector);

  int DeleteZoneRepo(ZoneIDType id);

  int UpdateZoneRepo(const ZoneRepo &zr);

  int QueryZoneRepo(ZoneIDType id, ZoneRepo *repo);

  // physical pool operation
  int InsertPhysicalPoolRepo(const PhysicalPoolRepo &pr);

  int LoadPhysicalPoolRepos(std::vector<PhysicalPoolRepo> *physicalPoolvector);

  int DeletePhysicalPoolRepo(PhysicalPoolIDType id);

  int UpdatePhysicalPoolRepo(const PhysicalPoolRepo &pr);

  int QueryPhysicalPoolRepo(PhysicalPoolIDType id, PhysicalPoolRepo *repo);

  // logical pool operation
  int InsertLogicalPoolRepo(const LogicalPoolRepo &lr);

  int LoadLogicalPoolRepos(std::vector<LogicalPoolRepo> *logicalPoolList);

  int DeleteLogicalPoolRepo(LogicalPoolIDType id);

  int UpdateLogicalPoolRepo(const LogicalPoolRepo &lr);

  int QueryLogicalPoolRepo(LogicalPoolIDType id, LogicalPoolRepo *repo);

  // copyset operation
  int InsertCopySetRepo(const CopySetRepo &cr);

  int LoadCopySetRepos(std::vector<CopySetRepo> *copySetList);

  int DeleteCopySetRepo(CopySetIDType id, LogicalPoolIDType lid);

  int UpdateCopySetRepo(const CopySetRepo &cr);

  int QueryCopySetRepo(CopySetIDType id,
                       LogicalPoolIDType lid,
                       CopySetRepo *repo);

 private:
  DataBase *db_;
  std::string dbName_;
};
}  // namespace repo
}  // namespace curve

#endif  // REPO_REPO_H_
