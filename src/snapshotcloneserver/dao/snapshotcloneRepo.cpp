/*************************************************************************
> File Name: snapshotcloneRepo.cpp
> Author:
> Created Time: Mon Dec 17 17:17:31 2018
> Copyright (c) 2018 netease
 ************************************************************************/

#include <list>

#include "src/snapshotcloneserver/dao/snapshotcloneRepo.h"

namespace curve {
namespace snapshotcloneserver {

int SnapshotCloneRepo::connectDB(const std::string &dbName,
                                 const std::string &user,
                                 const std::string &url,
                                 const std::string &password,
                                 uint32_t poolSize) {
    dbName_ = dbName;
    db_ = std::make_shared<DataBase>(user, url, password, dbName, poolSize);
    return db_->InitDB();
}

int SnapshotCloneRepo::createDatabase() {
    const size_t kLen = CreateDataBaseLen + dbName_.size() + 1;
    char createSql[kLen];
    snprintf(createSql,
             kLen,
             CreateDataBase,
             dbName_.c_str());
    return db_->Execute(createSql);
}

int SnapshotCloneRepo::useDataBase() {
    const size_t kLen = UseDataBaseLen + dbName_.size() + 1;
    char useSql[kLen];
    snprintf(useSql, kLen, UseDataBase, dbName_.c_str());
    return db_->Execute(useSql);
}

int SnapshotCloneRepo::dropDataBase() {
    const size_t kLen = DropDataBaseLen + dbName_.size() + 1;
    char dropSql[kLen];
    snprintf(dropSql, kLen, DropDataBase, dbName_.c_str());
    return db_->Execute(dropSql);
}

int SnapshotCloneRepo::createAllTables() {
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

std::shared_ptr<DataBase> SnapshotCloneRepo::getDataBase() {
    return db_;
}

void SnapshotCloneRepo::setDataBase(std::shared_ptr<DataBase> db) {
    db_ = db;
}

int SnapshotCloneRepo::InsertCloneRepoItem(const CloneRepoItem &cr) {
  return db_->ExecUpdate(makeSql.makeInsert(cr));
}

int SnapshotCloneRepo::LoadCloneRepoItems(
    std::vector<CloneRepoItem> *cloneList) {
    assert(cloneList != nullptr);

    sql::ResultSet *res;
    int resCode = db_->QueryRows(makeSql.makeQueryRows(CloneRepoItem{}), &res);
    if (resCode != OperationOK) {
      return resCode;
    }

  while (res->next()) {
    cloneList->push_back(
        CloneRepoItem(res->getString("taskID"),
                      res->getString("user"),
                      res->getUInt("tasktype"),
                      res->getString("src"),
                      res->getString("dest"),
                      res->getInt64("originID"),
                      res->getInt64("destID"),
                      res->getInt64("time"),
                      res->getUInt("filetype"),
                      res->getBoolean("isLazy"),
                      res->getUInt("nextstep"),
                      res->getUInt("status")));
  }
  delete (res);
  return resCode;
}

int SnapshotCloneRepo::DeleteCloneRepoItem(const std::string &taskID) {
  return db_->ExecUpdate(makeSql.makeDelete(CloneRepoItem(taskID)));
}

int SnapshotCloneRepo::UpdateCloneRepoItem(const CloneRepoItem &cr) {
  return db_->ExecUpdate(makeSql.makeUpdate(cr));
}

int SnapshotCloneRepo::QueryCloneRepoItem(const std::string &taskID,
    CloneRepoItem *repo) {

    assert(repo != nullptr);

    sql::ResultSet *res;
    int resCode = db_->QueryRows(makeSql.makeQueryRow(CloneRepoItem{taskID}), &res); //NOLINT
    if (resCode != OperationOK) {
      return resCode;
    }

  while (res->next()) {
    repo->taskID = res->getString("taskID");
    repo->user = res->getString("user");
    repo->tasktype = res->getUInt("tasktype");
    repo->src = res->getString("src");
    repo->dest = res->getString("dest");
    repo->originID = res->getInt64("originID");
    repo->destID = res->getInt64("destID");
    repo->time = res->getInt64("time");
    repo->filetype = res->getUInt("filetype");
    repo->isLazy = res->getBoolean("isLazy");
    repo->nextstep = res->getUInt("nextstep"),
    repo->status = res->getUInt("status");
  }

  delete (res);
  return resCode;
}


int SnapshotCloneRepo::InsertSnapshotRepoItem(const SnapshotRepoItem &sr) {
  return db_->ExecUpdate(makeSql.makeInsert(sr));
}

int SnapshotCloneRepo::LoadSnapshotRepoItems(
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

int SnapshotCloneRepo::DeleteSnapshotRepoItem(const std::string &uuid) {
  return db_->ExecUpdate(makeSql.makeDelete(SnapshotRepoItem(uuid)));
}

int SnapshotCloneRepo::UpdateSnapshotRepoItem(const SnapshotRepoItem &sr) {
  return db_->ExecUpdate(makeSql.makeUpdate(sr));
}

int SnapshotCloneRepo::QuerySnapshotRepoItem(const std::string &uuid,
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

CloneRepoItem::CloneRepoItem(const std::string &taskID) {
  this->taskID = taskID;
}

CloneRepoItem::CloneRepoItem(const std::string &taskID,
                            const std::string &user,
                            uint8_t tasktype,
                            const std::string &src,
                            const std::string &dest,
                            uint64_t originID,
                            uint64_t destID,
                            uint64_t time,
                            uint8_t filetype,
                            bool isLazy,
                            uint8_t nextstep,
                            uint8_t status) {
  this->taskID = taskID;
  this->user = user;
  this->tasktype = tasktype;
  this->src = src;
  this->dest = dest;
  this->originID = originID;
  this->destID = destID;
  this->time = time;
  this->filetype = filetype;
  this->isLazy = isLazy;
  this->nextstep = nextstep;
  this->status = status;
}


bool CloneRepoItem::operator==(const CloneRepoItem &r) {
    return taskID == r.taskID &&
           user == r.user &&
           tasktype == r.tasktype &&
           src == r.src &&
           dest == r.dest &&
           originID == r.originID &&
           destID == r.destID &&
           time == r.time &&
           filetype == r.filetype &&
           isLazy == r.isLazy &&
           nextstep == r.nextstep &&
           status == r.status;
}


void CloneRepoItem::getKV(std::map<std::string, std::string> (*kv)) const {
  ((*kv))["taskID"] = curve::repo::convertToSqlValue(taskID);
  ((*kv))["user"] = curve::repo::convertToSqlValue(user);
  ((*kv))["tasktype"] = std::to_string(tasktype);
  ((*kv))["src"] = curve::repo::convertToSqlValue(src);
  ((*kv))["dest"] = curve::repo::convertToSqlValue(dest);
  ((*kv))["originID"] = std::to_string(originID);
  ((*kv))["destID"] = std::to_string(destID);
  ((*kv))["time"] = std::to_string(time);
  ((*kv))["filetype"] = std::to_string(filetype);
  ((*kv))["isLazy"] = std::to_string(isLazy);
  ((*kv))["nextstep"] = std::to_string(nextstep);
  ((*kv))["status"] = std::to_string(status);
}

void CloneRepoItem::getPrimaryKV(std::map<std::string,
                                            std::string> *primary) const {
  (*primary)["taskID"] = curve::repo::convertToSqlValue(taskID);
}

std::string CloneRepoItem::getTable() const {
    return CloneTable;
}


}  // namespace snapshotcloneserver
}  // namespace curve
