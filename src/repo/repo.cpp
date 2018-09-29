/*
 * Project: curve
 * Created Date: Wed Sep 04 2018
 * Author: lixiaocui
 * Copyright (c) 2018 netease
 */

#include "src/repo/repo.h"

namespace curve {
namespace repo {
// copyset operation
int Repo::connectDB(const std::string &dbName, const std::string &user,
                    const std::string &url, const std::string &password) {
  dbName_ = dbName;
  db_ = new DataBase(user, url, password);
  return db_->connectDB();
}

int Repo::createDatabase() {
  char createSql[SqlBufferLen];
  snprintf(createSql, SqlBufferLen, CreateDataBase, dbName_.c_str());
  return db_->ExecUpdate(createSql);
}

int Repo::useDataBase() {
  char useSql[SqlBufferLen];
  snprintf(useSql, SqlBufferLen, UseDataBase, dbName_.c_str());
  return db_->ExecUpdate(useSql);
}

int Repo::dropDataBase() {
  char dropSql[SqlBufferLen];
  snprintf(dropSql, SqlBufferLen, DropDataBase, dbName_.c_str());
  return db_->ExecUpdate(dropSql);
}

int Repo::createAllTables() {
  auto iterator = CurveTables.begin();
  while (iterator != CurveTables.end()) {
    int resCode = db_->ExecUpdate(iterator->second);
    if (ExecUpdateOK != resCode) {
      return resCode;
    }
    iterator++;
  }
  return ExecUpdateOK;
}
}  // namespace repo
}  // namespace curve

