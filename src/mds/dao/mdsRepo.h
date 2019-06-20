/*************************************************************************
> File Name: mdsRepo.h
> Author: lixiaocui
> Created Time: Mon Dec 17 17:17:31 2018
> Copyright (c) 2018 netease
 ************************************************************************/

#ifndef SRC_MDS_DAO_MDSREPO_H_
#define SRC_MDS_DAO_MDSREPO_H_

#include <list>
#include <map>
#include <string>
#include <vector>
#include <memory>

#include "src/repo/repo.h"

using namespace ::curve::repo; //NOLINT
namespace curve {
namespace mds {

typedef uint32_t ChunkServerIDType;
typedef uint32_t ServerIDType;
typedef uint32_t ZoneIDType;
typedef uint16_t LogicalPoolIDType;
typedef uint16_t PhysicalPoolIDType;
typedef uint32_t CopySetIDType;

struct ChunkServerRepoItem : public RepoItem {
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
  ChunkServerRepoItem() = default;

  explicit ChunkServerRepoItem(uint32_t id);

  ChunkServerRepoItem(uint32_t id,
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

  bool operator==(const ChunkServerRepoItem &r);

  void getKV(std::map<std::string, std::string> *kv) const override;

  void getPrimaryKV(std::map<std::string, std::string> *primary) const override;

  std::string getTable() const override;
};

struct ServerRepoItem : public RepoItem {
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
  ServerRepoItem() = default;

  explicit ServerRepoItem(uint32_t id);

  ServerRepoItem(uint32_t id,
                const std::string &host,
                const std::string &inIp,
                uint32_t inPort,
                const std::string &exIp,
                uint32_t exPort,
                uint16_t zoneID,
                uint16_t poolID,
                const std::string &desc);

  bool operator==(const ServerRepoItem &s);

  void getKV(std::map<std::string, std::string> *kv) const override;

  void getPrimaryKV(std::map<std::string, std::string> *primary) const override;

  std::string getTable() const override;
};

struct ZoneRepoItem : public RepoItem {
 public:
  uint32_t zoneID;
  std::string zoneName;
  uint16_t poolID;
  std::string desc;

 public:
  ZoneRepoItem() = default;

  explicit ZoneRepoItem(uint32_t id);

  ZoneRepoItem(uint32_t zoneID,
           const std::string &name,
           uint16_t poolID,
           const std::string &desc);

  bool operator==(const ZoneRepoItem &r);

  void getKV(std::map<std::string, std::string> *kv) const override;

  void getPrimaryKV(std::map<std::string, std::string> *primary) const override;

  std::string getTable() const override;
};

struct LogicalPoolRepoItem : public RepoItem {
 public:
  uint16_t logicalPoolID;
  std::string logicalPoolName;
  uint16_t physicalPoolID;
  uint8_t type;
  uint32_t initialScatterWidth;
  int64_t createTime;
  uint8_t status;
  std::string redundanceAndPlacementPolicy;
  std::string userPolicy;
  bool availFlag;;

 public:
  LogicalPoolRepoItem() = default;

  explicit LogicalPoolRepoItem(uint16_t id);

  LogicalPoolRepoItem(uint16_t logicalID,
                  const std::string &logicalName,
                  uint16_t physicalID,
                  uint8_t type,
                  uint32_t initialScatterWidth,
                  int64_t createTime,
                  uint8_t status,
                  const std::string &reduancePolicy,
                  const std::string &userPolicy,
                  bool availFlag);

  bool operator==(const LogicalPoolRepoItem &r);

  void getKV(std::map<std::string, std::string> *kv) const override;

  void getPrimaryKV(std::map<std::string, std::string> *primary) const override;

  std::string getTable() const override;
};

struct PhysicalPoolRepoItem : public RepoItem {
  uint16_t physicalPoolID;
  std::string physicalPoolName;
  std::string desc;

 public:
  PhysicalPoolRepoItem() = default;

  explicit PhysicalPoolRepoItem(uint16_t id);

  PhysicalPoolRepoItem(uint16_t id,
                   const std::string &name,
                   const std::string &desc);

  bool operator==(const PhysicalPoolRepoItem &r);

 public:
  void getKV(std::map<std::string, std::string> *kv) const override;

  void getPrimaryKV(std::map<std::string, std::string> *primary) const override;

  std::string getTable() const override;
};

struct CopySetRepoItem : public RepoItem {
 public:
  uint32_t copySetID;
  uint16_t logicalPoolID;
  uint64_t epoch;
  uint32_t allocChunkNum;
  uint64_t allocSize;
  std::string chunkServerIDList;

 public:
  CopySetRepoItem() = default;

  CopySetRepoItem(uint32_t id, uint16_t logicalPoolID);

  CopySetRepoItem(uint32_t id, uint16_t poolID,
                  const std::string &chunkServerList);

  CopySetRepoItem(uint32_t id,
              uint16_t poolID,
              uint64_t epoch,
              const std::string &chunkServerList);

  bool operator==(const CopySetRepoItem &r);

  void getKV(std::map<std::string, std::string> *kv) const override;

  void getPrimaryKV(std::map<std::string, std::string> *primary) const override;

  std::string getTable() const override;
};

struct SessionRepoItem : public RepoItem {
 public:
  uint32_t entryID;
  std::string sessionID;
  std::string fileName;
  uint32_t leaseTime;
  uint16_t sessionStatus;
  uint64_t createTime;
  uint64_t updateTime;
  std::string clientIP;

 public:
  SessionRepoItem() = default;

  SessionRepoItem(std::string fileName, std::string sessionID,
              uint32_t leaseTime,
              uint16_t sessionStatus, uint64_t createTime,
              std::string clientIP);

  explicit SessionRepoItem(std::string sessionID);

  bool operator==(const SessionRepoItem &r);

  void getKV(std::map<std::string, std::string> *kv) const override;

  void getPrimaryKV(std::map<std::string, std::string> *primary) const override;

  std::string getTable() const override;

  uint16_t GetSessionStatus();

  void SetSessionStatus(uint16_t sessionStatus);
};

class MdsRepo : public RepoInterface {
 public:
  // constructor: open db
  // destructtor: close db
  MdsRepo() = default;

  ~MdsRepo() {}

  int connectDB(const std::string &dbName, const std::string &user,
                const std::string &url, const std::string &password,
                uint32_t poolSize) override;

  int createAllTables() override;

  int createDatabase() override;

  int useDataBase() override;

  int dropDataBase() override;

  std::shared_ptr<curve::repo::DataBase> getDataBase();

  // chunkServerRepo operation
  virtual int InsertChunkServerRepoItem(const ChunkServerRepoItem &cr);

  virtual int LoadChunkServerRepoItems(
      std::vector<ChunkServerRepoItem> *chunkServerRepoList);

  virtual int DeleteChunkServerRepoItem(ChunkServerIDType id);

  virtual int UpdateChunkServerRepoItem(const ChunkServerRepoItem &cr);

  virtual int QueryChunkServerRepoItem(ChunkServerIDType id,
                           ChunkServerRepoItem *repo);

  // server operation
  virtual int InsertServerRepoItem(const ServerRepoItem &sr);

  virtual int LoadServerRepoItems(std::vector<ServerRepoItem> *serverList);

  virtual int DeleteServerRepoItem(ServerIDType id);

  virtual int UpdateServerRepoItem(const ServerRepoItem &sr);

  virtual int QueryServerRepoItem(ServerIDType id, ServerRepoItem *repo);

  // zone operation
  virtual int InsertZoneRepoItem(const ZoneRepoItem &zr);

  virtual int LoadZoneRepoItems(std::vector<ZoneRepoItem> *zonevector);

  virtual int DeleteZoneRepoItem(ZoneIDType id);

  virtual int UpdateZoneRepoItem(const ZoneRepoItem &zr);

  virtual int QueryZoneRepoItem(ZoneIDType id, ZoneRepoItem *repo);

  // physical pool operation
  virtual int InsertPhysicalPoolRepoItem(const PhysicalPoolRepoItem &pr);

  virtual int LoadPhysicalPoolRepoItems(
      std::vector<PhysicalPoolRepoItem> *physicalPoolvector);

  virtual int DeletePhysicalPoolRepoItem(PhysicalPoolIDType id);

  virtual int UpdatePhysicalPoolRepoItem(const PhysicalPoolRepoItem &pr);

  virtual int QueryPhysicalPoolRepoItem(PhysicalPoolIDType id,
                            PhysicalPoolRepoItem *repo);

  // logical pool operation
  virtual int InsertLogicalPoolRepoItem(const LogicalPoolRepoItem &lr);

  virtual int LoadLogicalPoolRepoItems(
      std::vector<LogicalPoolRepoItem> *logicalPoolList);

  virtual int DeleteLogicalPoolRepoItem(LogicalPoolIDType id);

  virtual int UpdateLogicalPoolRepoItem(const LogicalPoolRepoItem &lr);

  virtual int QueryLogicalPoolRepoItem(LogicalPoolIDType id,
                           LogicalPoolRepoItem *repo);

  // copyset operation
  virtual int InsertCopySetRepoItem(const CopySetRepoItem &cr);

  virtual int LoadCopySetRepoItems(std::vector<CopySetRepoItem> *copySetList);

  virtual int DeleteCopySetRepoItem(CopySetIDType id, LogicalPoolIDType lid);

  virtual int UpdateCopySetRepoItem(const CopySetRepoItem &cr);

  virtual int QueryCopySetRepoItem(CopySetIDType id,
                       LogicalPoolIDType lid,
                       CopySetRepoItem *repo);

  // session operation
  virtual int InsertSessionRepoItem(const SessionRepoItem &r);

  virtual int LoadSessionRepoItems(std::vector<SessionRepoItem> *sessionList);

  virtual int DeleteSessionRepoItem(const std::string &sessionID);

  virtual int UpdateSessionRepoItem(const SessionRepoItem &r);

  virtual int QuerySessionRepoItem(
      const std::string &sessionID, SessionRepoItem *r);

 private:
  std::shared_ptr<curve::repo::DataBase> db_;
  std::string dbName_;
};
}  // namespace mds
}  // namespace curve
#endif  // SRC_MDS_DAO_MDSREPO_H_

