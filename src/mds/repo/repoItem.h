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

#include "src/mds/repo/sqlStatement.h"

namespace curve {
namespace repo {
typedef uint32_t ChunkServerIDType;
typedef uint32_t ServerIDType;
typedef uint32_t ZoneIDType;
typedef uint16_t LogicalPoolIDType;
typedef uint16_t PhysicalPoolIDType;
typedef uint32_t CopySetIDType;

struct RepoItem {
 public:
  virtual void getKV(std::map<std::string, std::string> *kv) const = 0;

  virtual void getPrimaryKV(
      std::map<std::string,
               std::string> *primary) const = 0;

  virtual std::string getTable() const = 0;
};

struct ChunkServerRepo : public RepoItem {
 public:
  uint32_t chunkServerID;
  std::string token;
  std::string diskType;
  std::string internalHostIP;
  uint32_t port;
  uint32_t serverID;
  uint8_t rwstatus;
  uint8_t diskState;
  uint8_t onlineState;
  std::string mountPoint;
  int64_t capacity;
  int64_t used;

 public:
  ChunkServerRepo() = default;

  explicit ChunkServerRepo(uint32_t id);

  ChunkServerRepo(uint32_t id,
                  const std::string &token,
                  const std::string &diskType,
                  const std::string &ip,
                  uint32_t port,
                  uint32_t serverID,
                  uint8_t status,
                  uint8_t diskState,
                  uint8_t onlineState,
                  const std::string &mountPoint,
                  int64_t diskCapacity,
                  int64_t diskUsed);

  bool operator==(const ChunkServerRepo &r);

  void getKV(std::map<std::string, std::string> *kv) const override;

  void getPrimaryKV(std::map<std::string, std::string> *primary) const override;

  std::string getTable() const override;
};

struct ServerRepo : public RepoItem {
 public:
  uint32_t serverID;
  std::string hostName;
  std::string internalHostIP;
  uint32_t internalPort;
  std::string externalHostIP;
  uint32_t externalPort;
  uint16_t zoneID;
  uint16_t poolID;
  std::string desc;

 public:
  ServerRepo() = default;

  explicit ServerRepo(uint32_t id);

  ServerRepo(uint32_t id,
             const std::string &host,
             const std::string &inIp,
             uint32_t inPort,
             const std::string &exIp,
             uint32_t exPort,
             uint16_t zoneID,
             uint16_t poolID,
             const std::string &desc);

  bool operator==(const ServerRepo &s);

  void getKV(std::map<std::string, std::string> *kv) const override;

  void getPrimaryKV(std::map<std::string, std::string> *primary) const override;

  std::string getTable() const override;
};

struct ZoneRepo : public RepoItem {
 public:
  uint32_t zoneID;
  std::string zoneName;
  uint16_t poolID;
  std::string desc;

 public:
  ZoneRepo() = default;

  explicit ZoneRepo(uint32_t id);

  ZoneRepo(uint32_t zoneID,
           const std::string &name,
           uint16_t poolID,
           const std::string &desc);

  bool operator==(const ZoneRepo &r);

  void getKV(std::map<std::string, std::string> *kv) const override;

  void getPrimaryKV(std::map<std::string, std::string> *primary) const override;

  std::string getTable() const override;
};

struct LogicalPoolRepo : public RepoItem {
 public:
  uint16_t logicalPoolID;
  std::string logicalPoolName;
  uint16_t physicalPoolID;
  uint8_t type;
  int64_t createTime;
  uint8_t status;
  std::string redundanceAndPlacementPolicy;
  std::string userPolicy;
  bool availFlag;

 public:
  LogicalPoolRepo() = default;

  explicit LogicalPoolRepo(uint16_t id);

  LogicalPoolRepo(uint16_t logicalID,
                  const std::string &logicalName,
                  uint16_t physicalID,
                  uint8_t type,
                  int64_t createTime,
                  uint8_t status,
                  const std::string &reduancePolicy,
                  const std::string &userPolicy,
                  bool availFlag);

  bool operator==(const LogicalPoolRepo &r);

  void getKV(std::map<std::string, std::string> *kv) const override;

  void getPrimaryKV(std::map<std::string, std::string> *primary) const override;

  std::string getTable() const override;
};

struct PhysicalPoolRepo : public RepoItem {
  uint16_t physicalPoolID;
  std::string physicalPoolName;
  std::string desc;

 public:
  PhysicalPoolRepo() = default;

  explicit PhysicalPoolRepo(uint16_t id);

  PhysicalPoolRepo(uint16_t id,
                   const std::string &name,
                   const std::string &desc);

  bool operator==(const PhysicalPoolRepo &r);

 public:
  void getKV(std::map<std::string, std::string> *kv) const override;

  void getPrimaryKV(std::map<std::string, std::string> *primary) const override;

  std::string getTable() const override;
};

struct CopySetRepo : public RepoItem {
 public:
  uint32_t copySetID;
  uint16_t logicalPoolID;
  uint64_t epoch;
  std::string chunkServerIDList;

 public:
  CopySetRepo() = default;

  CopySetRepo(uint32_t id,
              uint16_t poolID,
              uint64_t epoch = 0);

  CopySetRepo(uint32_t id,
              uint16_t poolID,
              uint64_t epoch,
              const std::string &chunkServerList);

  bool operator==(const CopySetRepo &r);

  void getKV(std::map<std::string, std::string> *kv) const override;

  void getPrimaryKV(std::map<std::string, std::string> *primary) const override;

  std::string getTable() const override;
};

struct SessionRepo : public RepoItem {
 public:
  uint32_t entryID;
  std::string sessionID;
  std::string token;
  std::string fileName;
  uint32_t leaseTime;
  uint16_t sessionStatus;
  uint64_t createTime;
  uint64_t updateTime;
  std::string clientIP;

 public:
  SessionRepo() = default;

  SessionRepo(std::string fileName, std::string sessionID, std::string token,
              uint32_t leaseTime, uint16_t sessionStatus, uint64_t createTime,
              std::string clientIP);

  explicit SessionRepo(std::string sessionID);

  bool operator==(const SessionRepo &r);

  void getKV(std::map<std::string, std::string> *kv) const override;

  void getPrimaryKV(std::map<std::string, std::string> *primary) const override;

  std::string getTable() const override;

  uint16_t GetSessionStatus();

  void SetSessionStatus(uint16_t sessionStatus);
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
}  // namespace repo
}  // namespace curve

#endif  // REPO_REPOITEM_H_
