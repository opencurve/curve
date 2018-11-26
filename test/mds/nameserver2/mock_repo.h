/*
 * Project: curve
 * Created Date: Tue Sep 25 2018
 * Author: xuchaojie
 * Copyright (c) 2018 netease
 */

#ifndef TEST_MDS_NAMESERVER2_MOCK_REPO_H_
#define TEST_MDS_NAMESERVER2_MOCK_REPO_H_

#include <gmock/gmock.h>
#include <vector>
#include <string>
#include "src/mds/repo/repo.h"


using ::testing::Return;
using ::testing::_;

namespace curve {
namespace repo {
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
  MOCK_METHOD0(createDatabase, int());
  MOCK_METHOD0(useDataBase, int());
  MOCK_METHOD0(dropDataBase, int());

  MOCK_METHOD1(InsertChunkServerRepo,
               int(
                   const ChunkServerRepo &cr));

  MOCK_METHOD1(LoadChunkServerRepos,
               int(std::vector<ChunkServerRepo>
                   *chunkServerRepoList));

  MOCK_METHOD1(DeleteChunkServerRepo,
               int(ChunkServerIDType
                   id));

  MOCK_METHOD1(UpdateChunkServerRepo,
               int(
                   const ChunkServerRepo &cr));

  MOCK_METHOD2(QueryChunkServerRepo,
               int(ChunkServerIDType
                   id, ChunkServerRepo * repo));

  MOCK_METHOD1(InsertServerRepo,
               int(
                   const ServerRepo &sr));

  MOCK_METHOD1(LoadServerRepos,
               int(std::vector<ServerRepo>
                   *serverList));

  MOCK_METHOD1(DeleteServerRepo,
               int(ServerIDType
                   id));

  MOCK_METHOD1(UpdateServerRepo,
               int(
                   const ServerRepo &sr));

  MOCK_METHOD2(QueryServerRepo,
               int(ServerIDType
                   id, ServerRepo * repo));

  MOCK_METHOD1(InsertZoneRepo,
               int(
                   const ZoneRepo &zr));

  MOCK_METHOD1(LoadZoneRepos,
               int(std::vector<ZoneRepo>
                   *zonevector));

  MOCK_METHOD1(DeleteZoneRepo,
               int(ZoneIDType
                   id));

  MOCK_METHOD1(UpdateZoneRepo,
               int(
                   const ZoneRepo &zr));

  MOCK_METHOD2(QueryZoneRepo,
               int(ZoneIDType
                   id, ZoneRepo * repo));

  MOCK_METHOD1(InsertPhysicalPoolRepo,
               int(
                   const PhysicalPoolRepo &pr));

  MOCK_METHOD1(LoadPhysicalPoolRepos,
               int(std::vector<PhysicalPoolRepo>
                   *physicalPoolvector));

  MOCK_METHOD1(DeletePhysicalPoolRepo,
               int(PhysicalPoolIDType
                   id));

  MOCK_METHOD1(UpdatePhysicalPoolRepo,
               int(
                   const PhysicalPoolRepo &pr));

  MOCK_METHOD2(QueryPhysicalPoolRepo,
               int(PhysicalPoolIDType
                   id, PhysicalPoolRepo * repo));

  MOCK_METHOD1(InsertLogicalPoolRepo,
               int(
                   const LogicalPoolRepo &lr));

  MOCK_METHOD1(LoadLogicalPoolRepos,
               int(std::vector<LogicalPoolRepo>
                   *logicalPoolList));

  MOCK_METHOD1(DeleteLogicalPoolRepo,
               int(LogicalPoolIDType
                   id));

  MOCK_METHOD1(UpdateLogicalPoolRepo,
               int(
                   const LogicalPoolRepo &lr));

  MOCK_METHOD2(QueryLogicalPoolRepo,
               int(LogicalPoolIDType
                   id, LogicalPoolRepo * repo));

  MOCK_METHOD1(InsertCopySetRepo,
               int(
                   const CopySetRepo &cr));

  MOCK_METHOD1(LoadCopySetRepos,
               int(std::vector<CopySetRepo>
                   *copySetList));

  MOCK_METHOD2(DeleteCopySetRepo,
               int(CopySetIDType
                   id, LogicalPoolIDType
                   lid));

  MOCK_METHOD1(UpdateCopySetRepo,
               int(
                   const CopySetRepo &cr));

  MOCK_METHOD3(QueryCopySetRepo,
               int(CopySetIDType
                   id,
                       LogicalPoolIDType
                   lid,
                       CopySetRepo * repo));

  MOCK_METHOD1(InsertSessionRepo,
               int(const SessionRepo &r));

  MOCK_METHOD1(LoadSessionRepo,
               int(std::vector<SessionRepo> *sessionList));

  MOCK_METHOD1(DeleteSessionRepo,
               int(const std::string &sessionID));

  MOCK_METHOD1(UpdateSessionRepo,
               int(const SessionRepo &r));

  MOCK_METHOD2(QuerySessionRepo,
                int(const std::string &sessionID, SessionRepo *r));

  MOCK_METHOD1(SetAutoCommit, int(
      const bool &autoCommit));
  MOCK_METHOD0(Commit, int());
  MOCK_METHOD0(RollBack, int());
};
}  // namespace repo
}  // namespace curve

#endif  // TEST_MDS_NAMESERVER2_MOCK_REPO_H_
