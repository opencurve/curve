/*************************************************************************
> File Name: snapshot_meta_store.cpp
> Author:
> Created Time: Mon Dec 17 13:47:19 2018
> Copyright (c) 2018 netease
 ************************************************************************/
#include "src/snapshot/snapshot_meta_store.h"
#include <gflags/gflags.h>
#include <memory>
#include <glog/logging.h> //NOLINT

using ::curve::snapshotserver::SnapshotRepoItem;

DEFINE_string(db_name, "curve_snapshot", "database name");
DEFINE_string(user, "root", "user name");
DEFINE_string(passwd, "qwer", "password");
DEFINE_string(url, "localhost", "db url");
namespace curve {
namespace snapshotserver {

int DBSnapshotMetaStore::Init() {
    std::string dbname = FLAGS_db_name;
    std::string user = FLAGS_user;
    std::string passwd = FLAGS_passwd;
    std::string url = FLAGS_url;
    if (repo_ -> connectDB(dbname, user, url, passwd) < 0) {
        return -1;
    }
    if (repo_ -> createDatabase() < 0) {
        return -1;
    }
    if (repo_ -> useDataBase() < 0) {
        return -1;
    }
    if (repo_ -> createAllTables() < 0) {
        return -1;
    }
    if (LoadSnapshotInfos() < 0) {
        return -1;
    }
    return 0;
}

int DBSnapshotMetaStore::LoadSnapshotInfos() {
    // load snapshot from db to map
    std::vector<SnapshotRepoItem> snapRepoV;
    if (repo_ -> LoadSnapshotRepoItems(&snapRepoV) < 0) {
        LOG(ERROR) << "Load snapshot repo failed";
        return -1;
    }
    for (auto &v : snapRepoV) {
        std::string uuid = v.uuid;
        SnapshotInfo info(v.uuid,
                          v.user,
                          v.fileName,
                          v.desc,
                          v.seqNum,
                          v.chunkSize,
                          v.segmentSize,
                          v.fileLength,
                          v.time,
                          static_cast<Status>(v.status));
        snapInfos_.emplace(uuid, info);
    }
    return 0;
}

int DBSnapshotMetaStore::AddSnapshot(const SnapshotInfo &info) {
    // first add to db
    SnapshotRepoItem sr(info.GetUuid(),
                   info.GetUser(),
                   info.GetFileName(),
                   info.GetSnapshotName(),
                   info.GetSeqNum(),
                   info.GetChunkSize(),
                   info.GetSegmentSize(),
                   info.GetFileLength(),
                   info.GetCreateTime(),
                   static_cast<int>(info.GetStatus()));
    if (repo_ -> InsertSnapshotRepoItem(sr) < 0) {
        LOG(ERROR) << "Add snapshot failed";
        return -1;
    }
    // add to map
    std::lock_guard<std::mutex> guard(snapInfos_mutex);
    this->snapInfos_.emplace(info.GetUuid(), info);
    return 0;
}

int DBSnapshotMetaStore::DeleteSnapshot(const UUID uuid) {
    // first delete from  db
    if (repo_ -> DeleteSnapshotRepoItem(uuid) < 0) {
        LOG(ERROR) << "Delete snapshot failed";
        return -1;
    }
    // delete from map
    std::lock_guard<std::mutex> guard(snapInfos_mutex);
    auto search = this->snapInfos_.find(uuid);
    if (search != this->snapInfos_.end()) {
        this->snapInfos_.erase(search);
    }
    return 0;
}

int DBSnapshotMetaStore::UpdateSnapshot(const SnapshotInfo &info) {
    // first update db
    SnapshotRepoItem sr(info.GetUuid(),
                   info.GetUser(),
                   info.GetFileName(),
                   info.GetSnapshotName(),
                   info.GetSeqNum(),
                   info.GetChunkSize(),
                   info.GetSegmentSize(),
                   info.GetFileLength(),
                   info.GetCreateTime(),
                   static_cast<int>(info.GetStatus()));
    if (repo_ -> UpdateSnapshotRepoItem(sr) < 0) {
        LOG(ERROR) << "Update snapshot failed";
        return -1;
    }
    // update map (delete and insert)
    std::lock_guard<std::mutex> guard(snapInfos_mutex);
    auto search = this->snapInfos_.find(info.GetUuid());
    if (search != this->snapInfos_.end()) {
        this->snapInfos_.erase(search);
    }
    this->snapInfos_.emplace(info.GetUuid(), info);
    return 0;
}
int DBSnapshotMetaStore::GetSnapshotInfo(UUID uuid,
                                    SnapshotInfo *info) {
    // search from map
    std::lock_guard<std::mutex> guard(snapInfos_mutex);
    auto search = this->snapInfos_.find(uuid);
    if (search != this->snapInfos_.end()) {
        *info = search->second;
        return 0;
    }
    LOG(ERROR) << "GetSnapshotList failed,"
               << "uuid:" << uuid;
        return -1;
}

int DBSnapshotMetaStore::GetSnapshotList(const std::string &name,
                                        std::vector<SnapshotInfo> *v) {
    // return from map
    std::lock_guard<std::mutex> guard(snapInfos_mutex);
    for (auto it = this->snapInfos_.begin();
             it != this->snapInfos_.end();
             it++) {
        if (name.compare(it->second.GetFileName()) == 0) {
            v->push_back(it->second);
        }
    }
    if (v->size() != 0) {
        return 0;
    }
    LOG(ERROR) << "GetSnapshotList failed,"
               << "filename:" << name;
    return -1;
}
int DBSnapshotMetaStore::GetSnapshotList(std::vector<SnapshotInfo> *list) {
    // return from map
    std::lock_guard<std::mutex> guard(snapInfos_mutex);
    for (auto it = this->snapInfos_.begin();
            it != this->snapInfos_.end();
            it++) {
       list->push_back(it->second);
    }
    return 0;
}
}  // namespace snapshotserver
}  // namespace curve

