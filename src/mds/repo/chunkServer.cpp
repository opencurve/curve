/*
 * Project: curve
 * Created Date: Fri Sep 07 2018
 * Author: lixiaocui
 * Copyright (c) 2018 netease
 */
#include <list>

#include "src/mds/repo/repo.h"

namespace curve {
namespace repo {
// chunkServerRepo operation
int Repo::InsertChunkServerRepo(const ChunkServerRepo &cr) {
  return db_->ExecUpdate(makeSql.makeInsert(cr));
}

int Repo::LoadChunkServerRepos(
    std::vector<curve::repo::ChunkServerRepo> *chunkServerRepoList) {
  assert(chunkServerRepoList != nullptr);

  sql::ResultSet *res;
  int resCode = db_->QueryRows(makeSql.makeQueryRows(ChunkServerRepo{}), &res);
  if (resCode != OperationOK) {
    return resCode;
  }

  while (res->next()) {
    chunkServerRepoList->push_back(
        ChunkServerRepo(res->getUInt("chunkServerID"),
                        res->getString("token"),
                        res->getString("diskType"),
                        res->getString("internalHostIP"),
                        res->getUInt("port"),
                        res->getUInt("serverID"),
                        static_cast<uint8_t>(res->getUInt("rwstatus")),
                        static_cast<uint8_t>(res->getUInt("diskState")),
                        static_cast<uint8_t>(res->getUInt("onlineState")),
                        res->getString("mountPoint"),
                        res->getInt64("capacity"),
                        res->getInt64("used")));
  }
  delete (res);
  return resCode;
}

int Repo::DeleteChunkServerRepo(ChunkServerIDType id) {
  return db_->ExecUpdate(makeSql.makeDelete(ChunkServerRepo(id)));
}

int Repo::UpdateChunkServerRepo(const curve::repo::ChunkServerRepo &cr) {
  return db_->ExecUpdate(makeSql.makeUpdate(cr));
}

int Repo::QueryChunkServerRepo(curve::repo::ChunkServerIDType id,
                               ChunkServerRepo *repo) {
  assert(repo != nullptr);

  sql::ResultSet *res;
  int resCode = db_->QueryRows(makeSql.makeQueryRow(ChunkServerRepo{id}), &res);
  if (resCode != OperationOK) {
    return resCode;
  }

  while (res->next()) {
    repo->chunkServerID = res->getUInt("chunkServerID");
    repo->token = res->getString("token");
    repo->diskType = res->getString("diskType");
    repo->internalHostIP = res->getString("internalHostIP");
    repo->port = res->getUInt("port");
    repo->serverID = res->getUInt("serverID");
    repo->rwstatus = static_cast<uint8_t>(res->getUInt("rwstatus"));
    repo->diskState = static_cast<uint8_t>(res->getUInt("diskState"));
    repo->onlineState = static_cast<uint8_t>(res->getUInt("onlineState"));
    repo->mountPoint = res->getString("mountPoint");
    repo->capacity = res->getInt64("capacity");
    repo->used = res->getInt64("used");
  }

  delete (res);
  return resCode;
}
}  // namespace repo
}  // namespace curve
