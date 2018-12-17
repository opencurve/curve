/*
 * Project: curve
 * Created Date: Wed Sep 04 2018
 * Author: lixiaocui
 * Copyright (c) 2018 netease
 */

#include "src/snapshot/repo/repo.h"

namespace curve {
namespace snapshotserver {

Repo::~Repo() {
    delete db_;
}

int Repo::connectDB(const std::string &dbName, const std::string &user,
                    const std::string &url, const std::string &password) {
    dbName_ = dbName;
    db_ = new DataBase(user, url, password);
    return db_->connectDB();
}

int Repo::createDataBase() {
    const size_t kLen = CreateDataBaseLen + dbName_.size() + 1;
    char createSql[kLen];
    snprintf(createSql,
             kLen,
             CreateDataBase,
             dbName_.c_str());
    return db_->ExecUpdate(createSql);
}

int Repo::useDataBase() {
    const size_t kLen = UseDataBaseLen + dbName_.size() + 1;
    char useSql[kLen];
    snprintf(useSql, kLen, UseDataBase, dbName_.c_str());
    return db_->ExecUpdate(useSql);
}

int Repo::dropDataBase() {
    const size_t kLen = DropDataBaseLen + dbName_.size() + 1;
    char dropSql[kLen];
    snprintf(dropSql, kLen, DropDataBase, dbName_.c_str());
    return db_->ExecUpdate(dropSql);
}

int Repo::createAllTables() {
    auto iterator = CurveTables.begin();
    while (iterator != CurveTables.end()) {
        int resCode = db_->ExecUpdate(iterator->second);
        if (OperationOK != resCode) {
            return resCode;
        }
        iterator++;
    }
    return OperationOK;
}

DataBase *Repo::getDataBase() {
    return db_;
}

}  // namespace snapshotserver
}  // namespace curve

