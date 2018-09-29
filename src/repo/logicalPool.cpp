/*
 * Project: curve
 * Created Date: Fri Sep 07 2018
 * Author: lixiaocui
 * Copyright (c) 2018 netease
 */

#include <string>

#include "src/repo/repo.h"

namespace curve {
namespace repo {
// logical pool operation
int Repo::InsertLogicalPoolRepo(const LogicalPoolRepo &lr) {
  return db_->ExecUpdate(makeSql.makeInsert(lr));
}

int Repo::LoadLogicalPoolRepos(std::vector<LogicalPoolRepo> *logicalPoolList) {
  assert(logicalPoolList != nullptr);

  sql::ResultSet *res;
  int resCode = db_->QueryRows(makeSql.makeQueryRows(LogicalPoolRepo{}), &res);
  if (resCode != QueryOK) {
    return resCode;
  }
  while (res->next()) {
    logicalPoolList->push_back(LogicalPoolRepo(
        static_cast<uint16_t>(res->getInt("logicalPoolID")),
        res->getString("logicalPoolName"),
        static_cast<uint16_t>(res->getInt("physicalPoolID")),
        static_cast<uint8_t>(res->getInt("type")),
        res->getInt64("createTime"),
        static_cast<uint8_t>(res->getInt("status")),
        res->getString("redundanceAndPlacementPolicy"),
        res->getString("userPolicy")));
  }

  delete (res);
  return resCode;
}

int Repo::DeleteLogicalPoolRepo(LogicalPoolIDType id) {
  return db_->ExecUpdate(makeSql.makeDelete(LogicalPoolRepo(id)));
}

int Repo::UpdateLogicalPoolRepo(const curve::repo::LogicalPoolRepo &lr) {
  return db_->ExecUpdate(makeSql.makeUpdate(lr));
}

int Repo::QueryLogicalPoolRepo(curve::repo::LogicalPoolIDType id,
                               curve::repo::LogicalPoolRepo *repo) {
  assert(repo != nullptr);

  sql::ResultSet *res;
  int resCode = db_->QueryRows(makeSql.makeQueryRow(LogicalPoolRepo(id)), &res);
  if (resCode != QueryOK) {
    return resCode;
  }
  while (res->next()) {
    repo->logicalPoolID = static_cast<uint16_t>(res->getUInt("logicalPoolID"));
    repo->logicalPoolName = res->getString("logicalPoolName");
    repo->physicalPoolID =
        static_cast<uint16_t>(res->getUInt("physicalPoolID"));
    repo->type = static_cast<uint8_t>(res->getUInt("type"));
    repo->createTime = res->getInt64("createTime");
    repo->status = static_cast<uint8_t>(res->getUInt("status"));
    repo->redundanceAndPlacementPolicy =
        res->getString("redundanceAndPlacementPolicy");
    repo->userPolicy = res->getString("userPolicy");
  }

  delete (res);
  return resCode;
}
}  // namespace repo
}  // namespace curve
