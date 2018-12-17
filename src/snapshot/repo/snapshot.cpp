/*************************************************************************
> File Name: snapshot.cpp
> Author:
> Created Time: Mon Dec 17 17:17:31 2018
> Copyright (c) 2018 netease
 ************************************************************************/

#include <list>

#include "src/snapshot/repo/repo.h"

namespace curve {
namespace snapshotserver {

int Repo::InsertSnapshotRepo(const SnapshotRepo &sr) {
  return db_->ExecUpdate(makeSql.makeInsert(sr));
}

int Repo::LoadSnapshotRepos(
    std::vector<curve::snapshotserver::SnapshotRepo> *snapList) {
  assert(snapList != nullptr);

  sql::ResultSet *res;
  int resCode = db_->QueryRows(makeSql.makeQueryRows(SnapshotRepo{}), &res);
  if (resCode != OperationOK) {
    return resCode;
  }

  while (res->next()) {
    snapList->push_back(
        SnapshotRepo(res->getString("uuid"),
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

int Repo::DeleteSnapshotRepo(const std::string uuid) {
  return db_->ExecUpdate(makeSql.makeDelete(SnapshotRepo(uuid)));
}

int Repo::UpdateSnapshotRepo(const curve::snapshotserver::SnapshotRepo &sr) {
  return db_->ExecUpdate(makeSql.makeUpdate(sr));
}

int Repo::QuerySnapshotRepo(const std::string uuid, SnapshotRepo *repo) {
  assert(repo != nullptr);

  sql::ResultSet *res;
  int resCode = db_->QueryRows(makeSql.makeQueryRow(SnapshotRepo{uuid}), &res);
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
}  // namespace snapshotserver
}  // namespace curve

