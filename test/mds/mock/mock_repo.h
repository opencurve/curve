/*
 *  Copyright (c) 2020 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * Project: curve
 * Created Date: Tue Sep 25 2018
 * Author: xuchaojie
 */

#ifndef TEST_MDS_MOCK_MOCK_REPO_H_
#define TEST_MDS_MOCK_MOCK_REPO_H_

#include <gmock/gmock.h>
#include <vector>
#include <string>
#include "src/mds/dao/mdsRepo.h"


using ::testing::Return;
using ::testing::_;

namespace curve {
namespace mds {
class MockRepo : public MdsRepo {
 public:
  MockRepo() {}
  ~MockRepo() {}

  MOCK_METHOD5(connectDB, int(
      const std::string &dbName,
      const std::string &user,
      const std::string &url,
      const std::string &password,
      uint32_t poolsize));

  MOCK_METHOD0(createAllTables, int());
  MOCK_METHOD0(createDatabase, int());
  MOCK_METHOD0(useDataBase, int());
  MOCK_METHOD0(dropDataBase, int());

  MOCK_METHOD1(InsertChunkServerRepoItem,
               int(
                   const ChunkServerRepoItem &cr));

  MOCK_METHOD1(LoadChunkServerRepoItems,
               int(std::vector<ChunkServerRepoItem>
                   *chunkServerRepoItemList));

  MOCK_METHOD1(DeleteChunkServerRepoItem,
               int(ChunkServerIDType
                   id));

  MOCK_METHOD1(UpdateChunkServerRepoItem,
               int(
                   const ChunkServerRepoItem &cr));

  MOCK_METHOD2(QueryChunkServerRepoItem,
               int(ChunkServerIDType
                   id, ChunkServerRepoItem * repo));

  MOCK_METHOD1(InsertServerRepoItem,
               int(
                   const ServerRepoItem &sr));

  MOCK_METHOD1(LoadServerRepoItems,
               int(std::vector<ServerRepoItem>
                   *serverList));

  MOCK_METHOD1(DeleteServerRepoItem,
               int(ServerIDType
                   id));

  MOCK_METHOD1(UpdateServerRepoItem,
               int(
                   const ServerRepoItem &sr));

  MOCK_METHOD2(QueryServerRepoItem,
               int(ServerIDType
                   id, ServerRepoItem * repo));

  MOCK_METHOD1(InsertZoneRepoItem,
               int(
                   const ZoneRepoItem &zr));

  MOCK_METHOD1(LoadZoneRepoItems,
               int(std::vector<ZoneRepoItem>
                   *zonevector));

  MOCK_METHOD1(DeleteZoneRepoItem,
               int(ZoneIDType
                   id));

  MOCK_METHOD1(UpdateZoneRepoItem,
               int(
                   const ZoneRepoItem &zr));

  MOCK_METHOD2(QueryZoneRepoItem,
               int(ZoneIDType
                   id, ZoneRepoItem * repo));

  MOCK_METHOD1(InsertPhysicalPoolRepoItem,
               int(
                   const PhysicalPoolRepoItem &pr));

  MOCK_METHOD1(LoadPhysicalPoolRepoItems,
               int(std::vector<PhysicalPoolRepoItem>
                   *physicalPoolvector));

  MOCK_METHOD1(DeletePhysicalPoolRepoItem,
               int(PhysicalPoolIDType
                   id));

  MOCK_METHOD1(UpdatePhysicalPoolRepoItem,
               int(
                   const PhysicalPoolRepoItem &pr));

  MOCK_METHOD2(QueryPhysicalPoolRepoItem,
               int(PhysicalPoolIDType
                   id, PhysicalPoolRepoItem * repo));

  MOCK_METHOD1(InsertLogicalPoolRepoItem,
               int(
                   const LogicalPoolRepoItem &lr));

  MOCK_METHOD1(LoadLogicalPoolRepoItems,
               int(std::vector<LogicalPoolRepoItem>
                   *logicalPoolList));

  MOCK_METHOD1(DeleteLogicalPoolRepoItem,
               int(LogicalPoolIDType
                   id));

  MOCK_METHOD1(UpdateLogicalPoolRepoItem,
               int(
                   const LogicalPoolRepoItem &lr));

  MOCK_METHOD2(QueryLogicalPoolRepoItem,
               int(LogicalPoolIDType
                   id, LogicalPoolRepoItem * repo));

  MOCK_METHOD1(InsertCopySetRepoItem,
               int(
                   const CopySetRepoItem &cr));

  MOCK_METHOD1(LoadCopySetRepoItems,
               int(std::vector<CopySetRepoItem>
                   *copySetList));

  MOCK_METHOD2(DeleteCopySetRepoItem,
               int(CopySetIDType
                   id, LogicalPoolIDType
                   lid));

  MOCK_METHOD1(UpdateCopySetRepoItem,
               int(
                   const CopySetRepoItem &cr));

  MOCK_METHOD3(QueryCopySetRepoItem,
               int(CopySetIDType
                   id,
                       LogicalPoolIDType
                   lid,
                       CopySetRepoItem * repo));

  MOCK_METHOD1(InsertSessionRepoItem,
               int(const SessionRepoItem &r));

  MOCK_METHOD1(LoadSessionRepoItems,
               int(std::vector<SessionRepoItem> *sessionList));

  MOCK_METHOD1(DeleteSessionRepoItem,
               int(const std::string &sessionID));

  MOCK_METHOD1(UpdateSessionRepoItem,
               int(const SessionRepoItem &r));

  MOCK_METHOD2(QuerySessionRepoItem,
                int(const std::string &sessionID, SessionRepoItem *r));

  MOCK_METHOD1(InsertClientInfoRepoItem,
               int(const ClientInfoRepoItem &r));

  MOCK_METHOD1(LoadClientInfoRepoItems,
               int(std::vector<ClientInfoRepoItem> *clientList));

  MOCK_METHOD2(DeleteClientInfoRepoItem,
               int(const std::string &clientIp, uint32_t clientPort));

  MOCK_METHOD3(QueryClientInfoRepoItem,
               int(const std::string &clientIp, uint32_t clientPort,
               ClientInfoRepoItem *r));
};
}  // namespace mds
}  // namespace curve

#endif  // TEST_MDS_MOCK_MOCK_REPO_H_
