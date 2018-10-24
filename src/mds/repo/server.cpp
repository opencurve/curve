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
// server operation
int Repo::InsertServerRepo(const ServerRepo &sr) {
  return db_->ExecUpdate(makeSql.makeInsert(sr));
}

int Repo::LoadServerRepos(std::vector<ServerRepo> *serverList) {
  assert(serverList != nullptr);

  sql::ResultSet *res;
  int resCode = db_->QueryRows(makeSql.makeQueryRows(ServerRepo{}), &res);
  if (OperationOK != resCode) {
    return resCode;
  }

  while (res->next()) {
    serverList->push_back(ServerRepo(
        res->getUInt("serverID"),
        res->getString("hostName"),
        res->getString("internalHostIP"),
        res->getString("externalHostIP"),
        static_cast<uint16_t>(res->getUInt("zoneID")),
        static_cast<uint16_t>(res->getUInt("poolID")),
        res->getString("desc")));
  }
  delete (res);
  return resCode;
}

int Repo::DeleteServerRepo(ServerIDType id) {
  return db_->ExecUpdate(makeSql.makeDelete(ServerRepo(id)));
}

int Repo::UpdateServerRepo(const curve::repo::ServerRepo &sr) {
  return db_->ExecUpdate(makeSql.makeUpdate(sr));
}

int Repo::QueryServerRepo(curve::repo::ServerIDType id,
                          curve::repo::ServerRepo *repo) {
  assert(repo != nullptr);

  sql::ResultSet *res;
  int resCode = db_->QueryRows(makeSql.makeQueryRow(ServerRepo(id)), &res);
  if (OperationOK != resCode) {
    return resCode;
  }

  while (res->next()) {
    repo->serverID = res->getUInt("serverID");
    repo->hostName = res->getString("hostName");
    repo->internalHostIP = res->getString("internalHostIP");
    repo->externalHostIP = res->getString("externalHostIP");
    repo->zoneID = static_cast<uint16_t>(res->getInt("zoneID"));
    repo->poolID = static_cast<uint16_t>(res->getInt("poolID"));
    repo->desc = res->getString("desc");
  }

  delete (res);
  return resCode;
}
}  // namespace repo
}  // namespace curve
