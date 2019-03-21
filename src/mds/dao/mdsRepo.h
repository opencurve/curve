/*************************************************************************
> File Name: mdsRepo.h
> Author:
> Created Time: Mon Dec 17 17:17:31 2018
> Copyright (c) 2018 netease
 ************************************************************************/

#include <list>
#include <map>
#include <string>
#include <vector>

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
  std::string externalHostIP;
  uint16_t zoneID;
  uint16_t poolID;
  std::string desc;

 public:
  ServerRepoItem() = default;

  explicit ServerRepoItem(uint32_t id);

  ServerRepoItem(uint32_t id,
             const std::string &host,
             const std::string &inIp,
             const std::string &exIp,
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
  int64_t createTime;
  uint8_t status;
  std::string redundanceAndPlacementPolicy;
  std::string userPolicy;

 public:
  LogicalPoolRepoItem() = default;

  explicit LogicalPoolRepoItem(uint16_t id);

  LogicalPoolRepoItem(uint16_t logicalID,
                  const std::string &logicalName,
                  uint16_t physicalID,
                  uint8_t type,
                  int64_t createTime,
                  uint8_t status,
                  const std::string &reduancePolicy,
                  const std::string &userPolicy);

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
  std::string token;
  std::string fileName;
  uint32_t leaseTime;
  uint16_t sessionStatus;
  uint64_t createTime;
  uint64_t updateTime;
  std::string clientIP;

 public:
  SessionRepoItem() = default;

  SessionRepoItem(std::string fileName, std::string sessionID,
              std::string token, uint32_t leaseTime,
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
                const std::string &url, const std::string &password) override;

  int createAllTables() override;

  int createDatabase() override;

  int useDataBase() override;

  int dropDataBase() override;

  std::shared_ptr<curve::repo::DataBase> getDataBase();

  // chunkServerRepo operation
  int InsertChunkServerRepoItem(const ChunkServerRepoItem &cr);

  int LoadChunkServerRepoItems(
      std::vector<ChunkServerRepoItem> *chunkServerRepoList);

  int DeleteChunkServerRepoItem(ChunkServerIDType id);

  int UpdateChunkServerRepoItem(const ChunkServerRepoItem &cr);

  int QueryChunkServerRepoItem(ChunkServerIDType id,
                           ChunkServerRepoItem *repo);

  // server operation
  int InsertServerRepoItem(const ServerRepoItem &sr);

  int LoadServerRepoItems(std::vector<ServerRepoItem> *serverList);

  int DeleteServerRepoItem(ServerIDType id);

  int UpdateServerRepoItem(const ServerRepoItem &sr);

  int QueryServerRepoItem(ServerIDType id, ServerRepoItem *repo);

  // zone operation
  int InsertZoneRepoItem(const ZoneRepoItem &zr);

  int LoadZoneRepoItems(std::vector<ZoneRepoItem> *zonevector);

  int DeleteZoneRepoItem(ZoneIDType id);

  int UpdateZoneRepoItem(const ZoneRepoItem &zr);

  int QueryZoneRepoItem(ZoneIDType id, ZoneRepoItem *repo);

  // physical pool operation
  int InsertPhysicalPoolRepoItem(const PhysicalPoolRepoItem &pr);

  int LoadPhysicalPoolRepoItems(
      std::vector<PhysicalPoolRepoItem> *physicalPoolvector);

  int DeletePhysicalPoolRepoItem(PhysicalPoolIDType id);

  int UpdatePhysicalPoolRepoItem(const PhysicalPoolRepoItem &pr);

  int QueryPhysicalPoolRepoItem(PhysicalPoolIDType id,
                            PhysicalPoolRepoItem *repo);

  // logical pool operation
  int InsertLogicalPoolRepoItem(const LogicalPoolRepoItem &lr);

  int LoadLogicalPoolRepoItems(
      std::vector<LogicalPoolRepoItem> *logicalPoolList);

  int DeleteLogicalPoolRepoItem(LogicalPoolIDType id);

  int UpdateLogicalPoolRepoItem(const LogicalPoolRepoItem &lr);

  int QueryLogicalPoolRepoItem(LogicalPoolIDType id,
                           LogicalPoolRepoItem *repo);

  // copyset operation
  int InsertCopySetRepoItem(const CopySetRepoItem &cr);

  int LoadCopySetRepoItems(std::vector<CopySetRepoItem> *copySetList);

  int DeleteCopySetRepoItem(CopySetIDType id, LogicalPoolIDType lid);

  int UpdateCopySetRepoItem(const CopySetRepoItem &cr);

  int QueryCopySetRepoItem(CopySetIDType id,
                       LogicalPoolIDType lid,
                       CopySetRepoItem *repo);

  // session operation
  int InsertSessionRepoItem(const SessionRepoItem &r);

  int LoadSessionRepoItems(std::vector<SessionRepoItem> *sessionList);

  int DeleteSessionRepoItem(const std::string &sessionID);

  int UpdateSessionRepoItem(const SessionRepoItem &r);

  int QuerySessionRepoItem(const std::string &sessionID, SessionRepoItem *r);

 private:
  std::shared_ptr<curve::repo::DataBase> db_;
  std::string dbName_;
};


}  // namespace mds
}  // namespace curve

