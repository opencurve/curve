/*************************************************************************
> File Name: snapshot_meta_store.cpp
> Author:
> Created Time: Mon Dec 17 13:47:19 2018
> Copyright (c) 2018 netease
 ************************************************************************/
#include "src/snapshotcloneserver/common/snapshotclone_meta_store.h"
#include <gflags/gflags.h>
#include <memory>
#include <glog/logging.h> //NOLINT

using ::curve::snapshotcloneserver::SnapshotRepoItem;
using ::curve::snapshotcloneserver::CloneRepoItem;

namespace curve {
namespace snapshotcloneserver {

int DBSnapshotCloneMetaStore::Init(
        const SnapshotCloneMetaStoreOptions &options) {
    std::string dbname = options.dbName;
    std::string user = options.dbUser;
    std::string passwd = options.dbPassword;
    std::string url = options.dbAddr;
    uint32_t poolsize = options.dbPoolSize;
    auto ret = 0;
    ret = repo_ -> connectDB(dbname, user, url, passwd, poolsize);
    if (ret < 0) {
        LOG(ERROR) << "ConnectDB failed";
        return -1;
    }
    ret = repo_->createDatabase();
    if (ret < 0) {
        LOG(ERROR) << "create Database failed";
        return -1;
    }
    ret = repo_->useDataBase();
    if (ret < 0) {
        LOG(ERROR) << "use DataBase failed";
        return -1;
    }
    ret = repo_->createAllTables();
    if (ret < 0) {
        LOG(ERROR) << "create table failed";
        return -1;
    }
    ret = LoadSnapshotInfos();
    if (ret < 0) {
        return -1;
    }
    ret = LoadCloneInfos();
    if (ret < 0) {
        return -1;
    }
    return 0;
}

int DBSnapshotCloneMetaStore::LoadCloneInfos() {
    // load clone info from db to map
    std::vector<CloneRepoItem> cloneRepoV;
    if (repo_ -> LoadCloneRepoItems(&cloneRepoV) < 0) {
        LOG(ERROR) << "Load clone repo failed";
        return -1;
    }
    for (auto &v : cloneRepoV) {
          std::string taskID = v.taskID;
          CloneInfo info(v.taskID,
                         v.user,
                         CloneTaskType(v.tasktype),
                         v.src,
                         v.dest,
                         v.originID,
                         v.destID,
                         v.time,
                         CloneFileType(v.filetype),
                         v.isLazy,
                         CloneStep(v.nextstep),
                         CloneStatus(v.status));
        cloneInfos_.emplace(taskID, info);
    }
    return 0;
}

int DBSnapshotCloneMetaStore::AddCloneInfo(const CloneInfo &info) {
    // first add to db
    CloneRepoItem cr(info.GetTaskId(),
                     info.GetUser(),
                     static_cast<uint8_t>(info.GetTaskType()),
                     info.GetSrc(),
                     info.GetDest(),
                     info.GetOriginId(),
                     info.GetDestId(),
                     info.GetTime(),
                     static_cast<uint8_t>(info.GetFileType()),
                     info.GetIsLazy(),
                     static_cast<uint8_t>(info.GetNextStep()),
                     static_cast<uint8_t>(info.GetStatus()));
    auto ret = 0;
    ret = repo_ -> InsertCloneRepoItem(cr);
    if (ret < 0) {
        LOG(ERROR) << "Add cloneinfo failed";
        return -1;
    }
    // add to map
    curve::common::WriteLockGuard guard(cloneInfos_lock_);
    cloneInfos_.emplace(info.GetTaskId(), info);
    return 0;
}

int DBSnapshotCloneMetaStore::DeleteCloneInfo(const std::string &taskID) {
    // first delete from  db
    auto ret = 0;
    ret = repo_->DeleteCloneRepoItem(taskID);
    if (ret < 0) {
        LOG(ERROR) << "Delete cloneinfo failed";
        return -1;
    }
    // delete from map
    curve::common::WriteLockGuard guard(cloneInfos_lock_);
    auto search = cloneInfos_.find(taskID);
    if (search != cloneInfos_.end()) {
        cloneInfos_.erase(search);
    }
    return 0;
}

int DBSnapshotCloneMetaStore::UpdateCloneInfo(const CloneInfo &info) {
    // first update db
     CloneRepoItem cr(info.GetTaskId(),
                      info.GetUser(),
                      static_cast<uint8_t>(info.GetTaskType()),
                      info.GetSrc(),
                      info.GetDest(),
                      info.GetOriginId(),
                      info.GetDestId(),
                      info.GetTime(),
                      static_cast<uint8_t>(info.GetFileType()),
                      info.GetIsLazy(),
                      static_cast<uint8_t>(info.GetNextStep()),
                      static_cast<uint8_t>(info.GetStatus()));
    auto ret = 0;
    ret = repo_->UpdateCloneRepoItem(cr);
    if (ret < 0) {
        LOG(ERROR) << "Update cloneinfo failed";
        return -1;
    }
    // update map (delete and insert)
    curve::common::WriteLockGuard guard(cloneInfos_lock_);
    auto search = cloneInfos_.find(info.GetTaskId());
    if (search != cloneInfos_.end()) {
        search->second = info;
    } else {
        cloneInfos_.emplace(info.GetTaskId(), info);
    }
    return 0;
}
int DBSnapshotCloneMetaStore::GetCloneInfo(const std::string &taskID,
                                    CloneInfo *info) {
    // search from map
    curve::common::ReadLockGuard guard(cloneInfos_lock_);
    auto search = cloneInfos_.find(taskID);
    if (search != cloneInfos_.end()) {
        *info = search->second;
        return 0;
    }
    return -1;
}

int DBSnapshotCloneMetaStore::GetCloneInfoList(std::vector<CloneInfo> *v) {
    // return from map
    curve::common::ReadLockGuard guard(cloneInfos_lock_);
    for (auto it = cloneInfos_.begin();
             it != cloneInfos_.end();
             it++) {
            v->push_back(it->second);
    }
    if (v->size() != 0) {
        return 0;
    }
    return -1;
}

int DBSnapshotCloneMetaStore::LoadSnapshotInfos() {
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

int DBSnapshotCloneMetaStore::AddSnapshot(const SnapshotInfo &info) {
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
    snapInfos_.emplace(info.GetUuid(), info);
    return 0;
}

int DBSnapshotCloneMetaStore::DeleteSnapshot(const UUID &uuid) {
    // first delete from  db
    if (repo_ -> DeleteSnapshotRepoItem(uuid) < 0) {
        LOG(ERROR) << "Delete snapshot failed";
        return -1;
    }
    // delete from map
    std::lock_guard<std::mutex> guard(snapInfos_mutex);
    auto search = snapInfos_.find(uuid);
    if (search != snapInfos_.end()) {
        snapInfos_.erase(search);
    }
    return 0;
}

int DBSnapshotCloneMetaStore::UpdateSnapshot(const SnapshotInfo &info) {
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
    auto search = snapInfos_.find(info.GetUuid());
    if (search != snapInfos_.end()) {
        search->second = info;
    } else {
        snapInfos_.emplace(info.GetUuid(), info);
    }
    return 0;
}
int DBSnapshotCloneMetaStore::GetSnapshotInfo(const UUID &uuid,
                                    SnapshotInfo *info) {
    // search from map
    std::lock_guard<std::mutex> guard(snapInfos_mutex);
    auto search = snapInfos_.find(uuid);
    if (search != snapInfos_.end()) {
        *info = search->second;
        return 0;
    }
    return -1;
}

int DBSnapshotCloneMetaStore::GetSnapshotList(const std::string &name,
                                        std::vector<SnapshotInfo> *v) {
    // return from map
    std::lock_guard<std::mutex> guard(snapInfos_mutex);
    for (auto it = snapInfos_.begin();
             it != snapInfos_.end();
             it++) {
        if (name.compare(it->second.GetFileName()) == 0) {
            v->push_back(it->second);
        }
    }
    if (v->size() != 0) {
        return 0;
    }
    return -1;
}
int DBSnapshotCloneMetaStore::GetSnapshotList(std::vector<SnapshotInfo> *list) {
    // return from map
    std::lock_guard<std::mutex> guard(snapInfos_mutex);
    for (auto it = snapInfos_.begin();
              it != snapInfos_.end();
              it++) {
       list->push_back(it->second);
    }
    return 0;
}

uint32_t DBSnapshotCloneMetaStore::GetSnapshotCount() {
    return snapInfos_.size();
}

}  // namespace snapshotcloneserver
}  // namespace curve
