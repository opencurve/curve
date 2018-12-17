/*
 * Project: curve
 * Created Date: Mon Sep 10 2018
 * Author: lixiaocui
 * Copyright (c) 2018 netease
 */

#include <iostream>
#include "src/snapshot/repo/repoItem.h"

namespace curve {
namespace snapshotserver {
std::string convertToSqlValue(const std::string &value) {
  return "'" + value + "'";
}

std::string MakeSql::makeCondtion(const curve::snapshotserver::RepoItem &t) {
  std::map<std::string, std::string> primaryKV;
  t.getPrimaryKV(&primaryKV);
  if (primaryKV.empty()) {
    return "";
  }

  auto iter = primaryKV.begin();
  auto endIter = --primaryKV.end();
  std::string condition;
  while (iter != primaryKV.end()) {
    if (iter == endIter) {
      condition += iter->first + "=" + iter->second;
    } else {
      condition += iter->first + "=" + iter->second + " and ";
    }
    iter++;
  }

  return condition;
}

std::string MakeSql::makeInsert(const RepoItem &t) {
  std::map<std::string, std::string> kvMap;
  t.getKV(&kvMap);
  if (kvMap.empty()) {
    return "";
  }

  auto iter = kvMap.begin();
  auto endIter = --kvMap.end();
  std::string rows = "(";
  std::string values = "(";
  while (iter != kvMap.end()) {
    if (iter == endIter) {
      rows += iter->first + ")";
      values += iter->second + ")";
    } else {
      rows += iter->first + ",";
      values += iter->second + ",";
    }
    iter++;
  }

  const size_t kLen =
      InsertLen + t.getTable().size() + rows.size() + values.size() + 1;
  char res[kLen];
  snprintf(res, kLen, Insert,
           t.getTable().c_str(),
           rows.c_str(),
           values.c_str());
  return std::string(res);
}

std::string MakeSql::makeQueryRows(const RepoItem &t) {
  const size_t kLen = QueryAllLen + t.getTable().size() + 1;
  char res[kLen];
  snprintf(res, kLen, QueryAll, t.getTable().c_str());
  return std::string(res);
}

std::string MakeSql::makeQueryRow(const RepoItem &t) {
  std::string condition = makeCondtion(t);
  if (condition.empty()) {
    return "";
  }

  const size_t kLen = QueryLen + t.getTable().size() + condition.size() + 1;
  char res[kLen];
  snprintf(res, kLen, Query, t.getTable().c_str(), condition.c_str());
  return std::string(res);
}

std::string MakeSql::makeDelete(const RepoItem &t) {
  std::string condition = makeCondtion(t);
  if (condition.empty()) {
    return "";
  }

  const size_t kLen = DeleteLen + t.getTable().size() + condition.size() + 1;
  char res[kLen];
  snprintf(res, kLen, Delete, t.getTable().c_str(), condition.c_str());
  return std::string(res);
}

std::string MakeSql::makeUpdate(const RepoItem &t) {
  std::map<std::string, std::string> kvMap;
  t.getKV(&kvMap);
  if (kvMap.empty()) {
    return "";
  }

  auto iter = kvMap.begin();
  auto endIter = --kvMap.end();
  std::string newValues;
  while (iter != kvMap.end()) {
    if (iter == --kvMap.end()) {
      newValues += iter->first + "=" + iter->second;
    } else {
      newValues += iter->first + "=" + iter->second + ",";
    }
    iter++;
  }

  std::string condition = makeCondtion(t);
  if (condition.empty()) {
    return "";
  }

  const size_t kLen =
      UpdateLen + t.getTable().size() + newValues.size() + condition.size() + 1;
  char res[kLen];
  snprintf(res, kLen, Update,
           t.getTable().c_str(),
           newValues.c_str(),
           condition.c_str());
  return std::string(res);
}

// Snapshot
SnapshotRepo::SnapshotRepo(const std::string &uuid) {
  this->uuid = uuid;
}

SnapshotRepo::SnapshotRepo(const std::string &uuid,
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


bool SnapshotRepo::operator==(const curve::snapshotserver::SnapshotRepo &r) {
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


void SnapshotRepo::getKV(std::map<std::string, std::string> (*kv)) const {
  ((*kv))["uuid"] = convertToSqlValue(uuid);
  ((*kv))["user"] = convertToSqlValue(user);
  ((*kv))["filename"] = convertToSqlValue(fileName);
  ((*kv))["seqnum"] = std::to_string(seqNum);
  ((*kv))["chunksize"] = std::to_string(chunkSize);
  ((*kv))["segmentsize"] = std::to_string(segmentSize);
  ((*kv))["filelength"] = std::to_string(fileLength);
  ((*kv))["time"] = std::to_string(time);
  ((*kv))["status"] = std::to_string(status);
  ((*kv))["snapdesc"] = convertToSqlValue(desc);
}

void SnapshotRepo::getPrimaryKV(std::map<std::string,
                                            std::string> *primary) const {
  (*primary)["uuid"] = convertToSqlValue(uuid);
}

std::string SnapshotRepo::getTable() const {
    return SnapshotTable;
}


}  // namespace snapshotserver
}  // namespace curve

