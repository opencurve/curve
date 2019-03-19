/*************************************************************************
> File Name: snapshotRepo.cpp
> Author:
> Created Time: Mon Dec 17 17:17:31 2018
> Copyright (c) 2018 netease
 ************************************************************************/

#include <list>

#include "src/snapshotcloneserver/dao/snapshotRepo.h"

namespace curve {
namespace snapshotcloneserver {

int SnapshotRepo::connectDB(const std::string &dbName, const std::string &user,
                    const std::string &url, const std::string &password) {
    dbName_ = dbName;
    db_ = std::make_shared<DataBase>(user, url, password);
    return db_->connectDB();
}

int SnapshotRepo::createDatabase() {
    const size_t kLen = CreateDataBaseLen + dbName_.size() + 1;
    char createSql[kLen];
    snprintf(createSql,
             kLen,
             CreateDataBase,
             dbName_.c_str());
    return db_->ExecUpdate(createSql);
}

int SnapshotRepo::useDataBase() {
    const size_t kLen = UseDataBaseLen + dbName_.size() + 1;
    char useSql[kLen];
    snprintf(useSql, kLen, UseDataBase, dbName_.c_str());
    return db_->ExecUpdate(useSql);
}

int SnapshotRepo::dropDataBase() {
    const size_t kLen = DropDataBaseLen + dbName_.size() + 1;
    char dropSql[kLen];
    snprintf(dropSql, kLen, DropDataBase, dbName_.c_str());
    return db_->ExecUpdate(dropSql);
}

int SnapshotRepo::createAllTables() {
    auto iterator = CurveSnapshotTables.begin();
    while (iterator != CurveSnapshotTables.end()) {
        int resCode = db_->ExecUpdate(iterator->second);
        if (OperationOK != resCode) {
            return resCode;
        }
        iterator++;
    }
    return OperationOK;
}

std::shared_ptr<DataBase> SnapshotRepo::getDataBase() {
    return db_;
}

int SnapshotRepo::InsertSnapshotRepoItem(const SnapshotRepoItem &sr) {
  return db_->ExecUpdate(makeSql.makeInsert(sr));
}

int SnapshotRepo::LoadSnapshotRepoItems(
    std::vector<SnapshotRepoItem> *snapList) {
  assert(snapList != nullptr);

  sql::ResultSet *res;
  int resCode = db_->QueryRows(makeSql.makeQueryRows(SnapshotRepoItem{}), &res);
  if (resCode != OperationOK) {
    return resCode;
  }

  while (res->next()) {
    snapList->push_back(
        SnapshotRepoItem(res->getString("uuid"),
                    res->getString("user"),
                    res->getString("filename"),
                    res->getString("snapdesc"),
                    res->getInt64("seqnum"),
                    res->getInt("chunksize"),
                    res->getInt("segmentsize"),
                    res->getInt64("filelength"),
                    res->getInt64("time"),
                    static_cast<uint8_t>(res->getUInt("status"))));
  }
  delete (res);
  return resCode;
}

int SnapshotRepo::DeleteSnapshotRepoItem(const std::string uuid) {
  return db_->ExecUpdate(makeSql.makeDelete(SnapshotRepoItem(uuid)));
}

int SnapshotRepo::UpdateSnapshotRepoItem(const SnapshotRepoItem &sr) {
  return db_->ExecUpdate(makeSql.makeUpdate(sr));
}

int SnapshotRepo::QuerySnapshotRepoItem(const std::string uuid,
    SnapshotRepoItem *repo) {

  assert(repo != nullptr);

  sql::ResultSet *res;
  int resCode = db_->QueryRows(makeSql.makeQueryRow(SnapshotRepoItem{uuid}), &res); //NOLINT
  if (resCode != OperationOK) {
    return resCode;
  }

  while (res->next()) {
    repo->uuid = res->getString("uuid");
    repo->user = res->getString("user");
    repo->fileName = res->getString("filename");
    repo->desc = res->getString("snapdesc");
    repo->seqNum = res->getInt64("seqnum");
    repo->chunkSize = res->getInt("chunksize");
    repo->segmentSize = res->getInt("segmentsize");
    repo->fileLength = res->getInt64("filelength");
    repo->time = res->getInt64("time");
    repo->status = static_cast<uint8_t>(res->getUInt("status"));
  }

  delete (res);
  return resCode;
}

SnapshotRepoItem::SnapshotRepoItem(const std::string &uuid) {
  this->uuid = uuid;
}

SnapshotRepoItem::SnapshotRepoItem(const std::string &uuid,
                            const std::string &user,
                            const std::string &filename,
                            const std::string &desc,
                            uint64_t seq,
                            uint32_t csize,
                            uint64_t segsize,
                            uint64_t flen,
                            uint64_t time,
                            int status) {
  this->uuid = uuid;
  this->user = user;
  this->fileName = filename;
  this->desc = desc;
  this->seqNum = seq;
  this->chunkSize = csize;
  this->segmentSize = segsize;
  this->fileLength = flen;
  this->time = time;
  this->status = status;
}


bool SnapshotRepoItem::operator==(const SnapshotRepoItem &r) {
    return uuid == r.uuid &&
      user == r.user &&
      fileName == r.fileName &&
      desc == r.desc &&
      seqNum == r.seqNum &&
      chunkSize == r.chunkSize &&
      segmentSize == r.segmentSize &&
      fileLength == r.fileLength &&
      time == r.time &&
      status == r.status;
}


void SnapshotRepoItem::getKV(std::map<std::string, std::string> (*kv)) const {
  ((*kv))["uuid"] = curve::repo::convertToSqlValue(uuid);
  ((*kv))["user"] = curve::repo::convertToSqlValue(user);
  ((*kv))["filename"] = curve::repo::convertToSqlValue(fileName);
  ((*kv))["seqnum"] = std::to_string(seqNum);
  ((*kv))["chunksize"] = std::to_string(chunkSize);
  ((*kv))["segmentsize"] = std::to_string(segmentSize);
  ((*kv))["filelength"] = std::to_string(fileLength);
  ((*kv))["time"] = std::to_string(time);
  ((*kv))["status"] = std::to_string(status);
  ((*kv))["snapdesc"] = curve::repo::convertToSqlValue(desc);
}

void SnapshotRepoItem::getPrimaryKV(std::map<std::string,
                                            std::string> *primary) const {
  (*primary)["uuid"] = curve::repo::convertToSqlValue(uuid);
}

std::string SnapshotRepoItem::getTable() const {
    return SnapshotTable;
}

}  // namespace snapshotcloneserver
}  // namespace curve

