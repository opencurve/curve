/*
 * Project: curve
 * Created Date: Fri Sep 07 2018
 * Author: lixiaocui
 * Copyright (c) 2018 netease
 */

#include "src/mds/repo/repo.h"

namespace curve {
namespace repo {
// physical pool operation
int Repo::InsertPhysicalPoolRepo(const PhysicalPoolRepo &pr) {
  return db_->ExecUpdate(makeSql.makeInsert(pr));
}

int Repo::LoadPhysicalPoolRepos(
    std::vector<curve::repo::PhysicalPoolRepo> *physicalPoollist) {
  assert(physicalPoollist != nullptr);

  sql::ResultSet *res;
  int resCode = db_->QueryRows(makeSql.makeQueryRows(PhysicalPoolRepo{}), &res);
  if (resCode != OperationOK) {
    return resCode;
  }
  while (res->next()) {
    physicalPoollist->push_back(PhysicalPoolRepo(
        static_cast<uint16_t>(res->getUInt("physicalPoolID")),
        res->getString("physicalPoolName"),
        res->getString("desc")));
  }
  delete (res);
  return resCode;
}

int Repo::DeletePhysicalPoolRepo(PhysicalPoolIDType id) {
  return db_->ExecUpdate(makeSql.makeDelete(PhysicalPoolRepo(id)));
}

int Repo::UpdatePhysicalPoolRepo(const curve::repo::PhysicalPoolRepo &pr) {
  return db_->ExecUpdate(makeSql.makeUpdate(pr));
}

int Repo::QueryPhysicalPoolRepo(curve::repo::PhysicalPoolIDType id,
                                curve::repo::PhysicalPoolRepo *repo) {
  assert(repo != nullptr);

  sql::ResultSet *res;
  int resCode =
      db_->QueryRows(makeSql.makeQueryRow(PhysicalPoolRepo(id)), &res);
  if (resCode != OperationOK) {
    return resCode;
  }
  while (res->next()) {
    repo->physicalPoolID =
        static_cast<uint16_t>(res->getUInt("physicalPoolID"));
    repo->physicalPoolName = res->getString("physicalPoolName");
    repo->desc = res->getString("desc");
  }

  delete (res);
  return resCode;
}
}  // namespace repo
}  // namespace curve
