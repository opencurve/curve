/*
 * Project: curve
 * Created Date: Fri Sep 07 2018
 * Author: lixiaocui
 * Copyright (c) 2018 netease
 */


#ifndef REPO_REPOITEM_H_
#define REPO_REPOITEM_H_

#include <string>
#include <map>
#include <list>
#include <vector>

#include "sqlStatement.h"

namespace curve {
namespace snapshotserver {

struct RepoItem {
 public:
    RepoItem() {}
    virtual ~RepoItem() {}
    RepoItem(const RepoItem&) = default;
    RepoItem(RepoItem&&) = default;
    RepoItem& operator=(const RepoItem&) = default;
    RepoItem& operator=(RepoItem&&) = default;

    virtual void getKV(std::map<std::string, std::string> *kv) const = 0;

    virtual void getPrimaryKV(
      std::map<std::string,
               std::string> *primary) const = 0;

    virtual std::string getTable() const = 0;
};

struct SnapshotRepo : public RepoItem {
 public:
  std::string uuid;
  std::string user;
  std::string fileName;
  std::string desc;
  uint64_t seqNum;
  uint32_t chunkSize;
  uint64_t segmentSize;
  uint64_t fileLength;
  uint64_t time;
  int status;
  SnapshotRepo() = default;
  explicit SnapshotRepo(const std::string &uuid);
  SnapshotRepo(const std::string &uuid,
                const std::string &user,
                const std::string &filename,
                const std::string &desc,
                uint64_t seq,
                uint32_t csize,
                uint64_t segsize,
                uint64_t flen,
                uint64_t time,
                int status);

  bool operator==(const SnapshotRepo &r);

  void getKV(std::map<std::string, std::string> *kv) const override;

  void getPrimaryKV(std::map<std::string, std::string> *primary) const override;

  std::string getTable() const override;
};

static class MakeSql {
 public:
  std::string makeInsert(const RepoItem &t);

  std::string makeQueryRows(const RepoItem &t);

  std::string makeQueryRow(const RepoItem &t);

  std::string makeDelete(const RepoItem &t);

  std::string makeUpdate(const RepoItem &t);

 private:
  std::string makeCondtion(const RepoItem &t);
} makeSql;
}  // namespace snapshotserver
}  // namespace curve

#endif  // REPO_REPOITEM_H_
