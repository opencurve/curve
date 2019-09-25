/*************************************************************************
> File Name: mdsRepo.cpp
> Author:lixiaocui
> Created Time: Mon Dec 17 17:17:31 2018
> Copyright (c) 2018 netease
 ************************************************************************/

#include <memory>
#include <list>

#include "src/mds/dao/mdsRepo.h"

namespace curve {
namespace mds {


int MdsRepo::connectDB(const std::string &dbName,
                       const std::string &user,
                       const std::string &url,
                       const std::string &password,
                       uint32_t poolSize) {
    dbName_ = dbName;
    db_ = std::make_shared<DataBase>(user, url, password, dbName, poolSize);
    return db_->InitDB();
}

int MdsRepo::createDatabase() {
    const size_t kLen = CreateDataBaseLen + dbName_.size() + 1;
    char createSql[kLen];
    snprintf(createSql,
             kLen,
             CreateDataBase,
             dbName_.c_str());
    return db_->Execute(createSql);
}

int MdsRepo::useDataBase() {
    const size_t kLen = UseDataBaseLen + dbName_.size() + 1;
    char useSql[kLen];
    snprintf(useSql, kLen, UseDataBase, dbName_.c_str());
    return db_->Execute(useSql);
}

int MdsRepo::dropDataBase() {
    const size_t kLen = DropDataBaseLen + dbName_.size() + 1;
    char dropSql[kLen];
    snprintf(dropSql, kLen, DropDataBase, dbName_.c_str());
    return db_->Execute(dropSql);
}

int MdsRepo::createAllTables() {
    auto iterator = CurveMDSTables.begin();
    while (iterator != CurveMDSTables.end()) {
        int resCode = db_->ExecUpdate(iterator->second);
        if (OperationOK != resCode) {
            return resCode;
        }
        iterator++;
    }
    return OperationOK;
}

std::shared_ptr<DataBase> MdsRepo::getDataBase() {
    return db_;
}

// ClusterInfoRepoItem
void ClusterInfoRepoItem::getKV(std::map<std::string, std::string> *kv) const {
    (*kv)["clusterId"] = convertToSqlValue(clusterId);
}

void ClusterInfoRepoItem::getPrimaryKV(
    std::map<std::string, std::string> *primary) const {
    (*primary)["clusterId"] = convertToSqlValue(clusterId);
}

std::string ClusterInfoRepoItem::getTable() const {
    return ClusterInfoTable;
}

// ChunkServer
ChunkServerRepoItem::ChunkServerRepoItem(uint32_t id) {
    this->chunkServerID = id;
}

ChunkServerRepoItem::ChunkServerRepoItem(uint32_t id,
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
                                 int64_t diskUsed) {
    this->chunkServerID = id;
    this->token = token;
    this->diskType = diskType;
    this->internalHostIP = ip;
    this->port = port;
    this->serverID = serverID;
    this->rwstatus = status;
    this->diskState = diskState;
    this->onlineState = onlineState;
    this->mountPoint = mountPoint;
    this->capacity = diskCapacity;
    this->used = diskUsed;
}

bool ChunkServerRepoItem::operator==(const ChunkServerRepoItem &r) {
    return chunkServerID == r.chunkServerID &&
        serverID == r.serverID &&
        token == r.token &&
        diskType == r.diskType;
}

void ChunkServerRepoItem::getKV(std::map<std::string,
                              std::string> (*kv)) const {
    ((*kv))["chunkServerID"] = std::to_string(chunkServerID);
    ((*kv))["token"] = convertToSqlValue(token);
    ((*kv))["diskType"] = convertToSqlValue(diskType);
    ((*kv))["internalHostIP"] = convertToSqlValue(internalHostIP);
    ((*kv))["port"] = std::to_string(port);
    ((*kv))["serverID"] = std::to_string(serverID);
    ((*kv))["rwstatus"] = std::to_string(rwstatus);
    ((*kv))["onlineState"] = std::to_string(onlineState);
    ((*kv))["diskState"] = std::to_string(diskState);
    ((*kv))["mountPoint"] = convertToSqlValue(mountPoint);
    ((*kv))["capacity"] = std::to_string(capacity);
    ((*kv))["used"] = std::to_string(used);
}

void ChunkServerRepoItem::getPrimaryKV(std::map<std::string,
                                            std::string> *primary) const {
    (*primary)["chunkServerID"] = std::to_string(chunkServerID);
}

std::string ChunkServerRepoItem::getTable() const {
    return ChunkServerTable;
}

// Server
ServerRepoItem::ServerRepoItem(uint32_t id) {
    this->serverID = id;
}

ServerRepoItem::ServerRepoItem(uint32_t id,
                       const std::string &host,
                       const std::string &inIp,
                       uint32_t inPort,
                       const std::string &exIp,
                       uint32_t exPort,
                       uint16_t zoneID,
                       uint16_t poolID,
                       const std::string &desc) {
    this->serverID = id;
    this->hostName = host;
    this->internalHostIP = inIp;
    this->internalPort = inPort;
    this->externalHostIP = exIp;
    this->externalPort = exPort;
    this->zoneID = zoneID;
    this->poolID = poolID;
    this->desc = desc;
}

bool ServerRepoItem::operator==(const ServerRepoItem &s) {
    return serverID == s.serverID &&
        hostName == s.hostName &&
        internalHostIP == s.internalHostIP &&
        externalHostIP == s.externalHostIP &&
        zoneID == s.zoneID &&
        poolID == s.poolID;
}

void ServerRepoItem::getKV(std::map<std::string, std::string> (*kv)) const {
    ((*kv))["serverID"] = std::to_string(serverID);
    ((*kv))["hostName"] = convertToSqlValue(hostName);
    ((*kv))["internalHostIP"] = convertToSqlValue(internalHostIP);
    ((*kv))["internalPort"] = std::to_string(internalPort);
    ((*kv))["externalHostIP"] = convertToSqlValue(externalHostIP);
    ((*kv))["externalPort"] = std::to_string(externalPort);
    ((*kv))["zoneID"] = std::to_string(zoneID);
    ((*kv))["poolID"] = std::to_string(poolID);
    ((*kv))["`desc`"] = convertToSqlValue(desc);
}

void ServerRepoItem::getPrimaryKV(std::map<std::string,
                                       std::string> *primary) const {
    (*primary)["serverID"] = std::to_string(serverID);
}

std::string ServerRepoItem::getTable() const {
    return ServerTable;
}

// Zone
ZoneRepoItem::ZoneRepoItem(uint32_t id) {
    this->zoneID = id;
}

ZoneRepoItem::ZoneRepoItem(uint32_t zoneID,
                   const std::string &name,
                   uint16_t poolID,
                   const std::string &desc) {
    this->zoneID = zoneID;
    this->zoneName = name;
    this->poolID = poolID;
    this->desc = desc;
}

bool ZoneRepoItem::operator==(const ZoneRepoItem &r) {
    return zoneID == r.zoneID &&
        zoneName == r.zoneName &&
        poolID == r.poolID;
}

void ZoneRepoItem::getKV(std::map<std::string, std::string> (*kv)) const {
    ((*kv))["zoneID"] = std::to_string(zoneID);
    ((*kv))["zoneName"] = convertToSqlValue(zoneName);
    ((*kv))["poolID"] = std::to_string(poolID);
    ((*kv))["`desc`"] = convertToSqlValue(desc);
}

void ZoneRepoItem::getPrimaryKV(std::map<std::string,
                                std::string> *primary) const {
    (*primary)["zoneID"] = std::to_string(zoneID);
}

std::string ZoneRepoItem::getTable() const {
    return ZoneTable;
}

// LogicalPool
LogicalPoolRepoItem::LogicalPoolRepoItem(uint16_t id) {
    this->logicalPoolID = id;
}

LogicalPoolRepoItem::LogicalPoolRepoItem(uint16_t logicalID,
                                 const std::string &logicalName,
                                 uint16_t physicalID,
                                 uint8_t type,
                                 uint32_t initialScatterWidth,
                                 int64_t createTime,
                                 uint8_t status,
                                 const std::string &reduancePolicy,
                                 const std::string &userPolicy,
                                 bool availFlag) {
    this->logicalPoolID = logicalID;
    this->logicalPoolName = logicalName;
    this->physicalPoolID = physicalID;
    this->type = type;
    this->initialScatterWidth = initialScatterWidth;
    this->createTime = createTime;
    this->status = status;
    this->redundanceAndPlacementPolicy = reduancePolicy;
    this->userPolicy = userPolicy;
    this->availFlag = availFlag;
}

// TODO(lixiaocui): 复制操作符的这种重载是不合适的
bool LogicalPoolRepoItem::operator==(const LogicalPoolRepoItem &r) {
    return logicalPoolID == r.logicalPoolID &&
        logicalPoolName == r.logicalPoolName &&
        physicalPoolID == r.physicalPoolID &&
        type == r.type &&
        createTime == r.createTime;
}

void LogicalPoolRepoItem::getKV(
    std::map<std::string, std::string> (*kv)) const {
    (*kv)["logicalPoolID"] = std::to_string(logicalPoolID);
    (*kv)["logicalPoolName"] = convertToSqlValue(logicalPoolName);
    (*kv)["physicalPoolID"] = std::to_string(physicalPoolID);
    (*kv)["type"] = std::to_string(type);
    (*kv)["initialScatterWidth"] = std::to_string(initialScatterWidth);
    (*kv)["createTime"] = std::to_string(createTime);
    (*kv)["status"] = std::to_string(status);
    (*kv)["redundanceAndPlacementPolicy"] =
        convertToSqlValue(redundanceAndPlacementPolicy);
    (*kv)["userPolicy"] = convertToSqlValue(userPolicy);
    (*kv)["availFlag"] = std::to_string(availFlag);
}

void LogicalPoolRepoItem::getPrimaryKV(std::map<std::string,
                                            std::string> *primay) const {
    (*primay)["logicalPoolID"] = std::to_string(logicalPoolID);
}

std::string LogicalPoolRepoItem::getTable() const {
    return LogicalPoolTable;
}

// PhysicalPool
PhysicalPoolRepoItem::PhysicalPoolRepoItem(uint16_t id) {
    this->physicalPoolID = id;
}

PhysicalPoolRepoItem::PhysicalPoolRepoItem(uint16_t id,
                                   const std::string &name,
                                   const std::string &desc) {
    this->physicalPoolID = id;
    this->physicalPoolName = name;
    this->desc = desc;
}

bool PhysicalPoolRepoItem::operator==(const PhysicalPoolRepoItem &r) {
    return physicalPoolID == r.physicalPoolID &&
        physicalPoolName == r.physicalPoolName;
}

void PhysicalPoolRepoItem::getKV(std::map<std::string, std::string> *kv) const {
    (*kv)["physicalPoolID"] = std::to_string(physicalPoolID);
    (*kv)["physicalPoolName"] = convertToSqlValue(physicalPoolName);
    (*kv)["`desc`"] = convertToSqlValue(desc);
}

void PhysicalPoolRepoItem::getPrimaryKV(std::map<std::string,
                                             std::string> *primary) const {
    (*primary)["physicalPoolID"] = std::to_string(physicalPoolID);
}

std::string PhysicalPoolRepoItem::getTable() const {
    return PhysicalPoolTable;
}

// CopySet
CopySetRepoItem::CopySetRepoItem(uint32_t id, uint16_t logicalPoolID) {
    this->copySetID = id;
    this->logicalPoolID = logicalPoolID;
    this->epoch = 0;
}

CopySetRepoItem::CopySetRepoItem(uint32_t id,
                         uint16_t poolID,
                         const std::string &chunkServerList) {
    this->copySetID = id;
    this->logicalPoolID = poolID;
    this->epoch = 0;
    this->chunkServerIDList = chunkServerList;
}

CopySetRepoItem::CopySetRepoItem(uint32_t id,
                         uint16_t poolID,
                         uint64_t epoch,
                         const std::string &chunkServerList) {
    this->copySetID = id;
    this->logicalPoolID = poolID;
    this->epoch = epoch;
    this->chunkServerIDList = chunkServerList;
}

bool CopySetRepoItem::operator==(const CopySetRepoItem &r) {
    return copySetID == r.copySetID &&
        logicalPoolID == r.logicalPoolID;
}

void CopySetRepoItem::getKV(std::map<std::string, std::string> *kv) const {
    (*kv)["copySetID"] = std::to_string(copySetID);
    (*kv)["logicalPoolID"] = std::to_string(logicalPoolID);
    (*kv)["epoch"] = std::to_string(epoch);
    (*kv)["chunkServerIDList"] = convertToSqlValue(chunkServerIDList);
}

void CopySetRepoItem::getPrimaryKV(std::map<std::string,
                                        std::string> *primary) const {
    (*primary)["copySetID"] = std::to_string(copySetID);
    (*primary)["logicalPoolID"] = std::to_string(logicalPoolID);
}

std::string CopySetRepoItem::getTable() const {
    return CopySetTable;
}

// session
SessionRepoItem::SessionRepoItem(std::string fileName, std::string sessionID,
                         uint32_t leaseTime,
                         uint16_t sessionStatus, uint64_t createTime,
                         std::string clientIP) {
  this->fileName = fileName;
  this->sessionID = sessionID;
  this->leaseTime = leaseTime;
  this->sessionStatus = sessionStatus;
  this->createTime = createTime;
  this->clientIP = clientIP;
}

SessionRepoItem::SessionRepoItem(std::string sessionID) {
  this->sessionID = sessionID;
}

bool SessionRepoItem::operator==(const SessionRepoItem &r) {
  return sessionID == r.sessionID && fileName == r.fileName;
}


void SessionRepoItem::getKV(std::map<std::string, std::string> *kv) const {
  (*kv)["fileName"] = convertToSqlValue(fileName);
  (*kv)["sessionID"] = convertToSqlValue(sessionID);
  (*kv)["leaseTime"] = std::to_string(leaseTime);
  (*kv)["sessionStatus"] = std::to_string(sessionStatus);
  (*kv)["createTime"] = std::to_string(createTime);
  (*kv)["clientIP"] = convertToSqlValue(clientIP);
}

void SessionRepoItem::getPrimaryKV(std::map<std::string,
                               std::string> *primary) const {
  (*primary)["sessionID"] = convertToSqlValue(sessionID);
  return;
}

std::string SessionRepoItem::getTable() const {
  return SessionTable;
}

uint16_t SessionRepoItem::GetSessionStatus() {
  return sessionStatus;
}

void SessionRepoItem::SetSessionStatus(uint16_t status) {
  sessionStatus = status;
}

// client info
ClientInfoRepoItem::ClientInfoRepoItem(const std::string &clientIp,
                                      uint32_t clientPort) {
  this->clientIp = clientIp;
  this->clientPort = clientPort;
}

bool ClientInfoRepoItem::operator==(const ClientInfoRepoItem &r) {
  return clientIp == r.clientIp && clientPort == r.clientPort;
}


void ClientInfoRepoItem::getKV(std::map<std::string, std::string> *kv) const {
  (*kv)["clientIp"] = convertToSqlValue(clientIp);
  (*kv)["clientPort"] = std::to_string(clientPort);
}

void ClientInfoRepoItem::getPrimaryKV(std::map<std::string,
                               std::string> *primary) const {
  (*primary)["clientIp"] = convertToSqlValue(clientIp);
  (*primary)["clientPort"] = std::to_string(clientPort);
  return;
}

std::string ClientInfoRepoItem::getTable() const {
  return ClientInfoTable;
}

std::string ClientInfoRepoItem::GetClientIp() {
  return clientIp;
}

uint32_t ClientInfoRepoItem::GetClientPort() {
  return clientPort;
}

int MdsRepo::InsertChunkServerRepoItem(const ChunkServerRepoItem &cr) {
  return db_->ExecUpdate(makeSql.makeInsert(cr));
}

int MdsRepo::LoadChunkServerRepoItems(
    std::vector<curve::mds::ChunkServerRepoItem> *chunkServerRepoList) {
  assert(chunkServerRepoList != nullptr);

  sql::ResultSet *res;
  int resCode =
      db_->QueryRows(makeSql.makeQueryRows(ChunkServerRepoItem{}), &res);
  if (resCode != OperationOK) {
    return resCode;
  }

  while (res->next()) {
    chunkServerRepoList->push_back(
        ChunkServerRepoItem(res->getUInt("chunkServerID"),
                        res->getString("token"),
                        res->getString("diskType"),
                        res->getString("internalHostIP"),
                        res->getUInt("port"),
                        res->getUInt("serverID"),
                        static_cast<uint8_t>(res->getUInt("rwstatus")),
                        static_cast<uint8_t>(res->getUInt("diskState")),
                        static_cast<uint8_t>(res->getUInt("onlineState")),
                        res->getString("mountPoint"),
                        res->getInt64("capacity"),
                        res->getInt64("used")));
  }
  delete (res);
  return resCode;
}

int MdsRepo::DeleteChunkServerRepoItem(ChunkServerIDType id) {
  return db_->ExecUpdate(makeSql.makeDelete(ChunkServerRepoItem(id)));
}

int MdsRepo::UpdateChunkServerRepoItem(const ChunkServerRepoItem &cr) {
  return db_->ExecUpdate(makeSql.makeUpdate(cr));
}

int MdsRepo::QueryChunkServerRepoItem(ChunkServerIDType id,
                               ChunkServerRepoItem *repo) {
  assert(repo != nullptr);

  sql::ResultSet *res;
  int resCode =
      db_->QueryRows(makeSql.makeQueryRow(ChunkServerRepoItem{id}), &res);
  if (resCode != OperationOK) {
    return resCode;
  }

  while (res->next()) {
    repo->chunkServerID = res->getUInt("chunkServerID");
    repo->token = res->getString("token");
    repo->diskType = res->getString("diskType");
    repo->internalHostIP = res->getString("internalHostIP");
    repo->port = res->getUInt("port");
    repo->serverID = res->getUInt("serverID");
    repo->rwstatus = static_cast<uint8_t>(res->getUInt("rwstatus"));
    repo->diskState = static_cast<uint8_t>(res->getUInt("diskState"));
    repo->onlineState = static_cast<uint8_t>(res->getUInt("onlineState"));
    repo->mountPoint = res->getString("mountPoint");
    repo->capacity = res->getInt64("capacity");
    repo->used = res->getInt64("used");
  }

  delete (res);
  return resCode;
}

int MdsRepo::InsertServerRepoItem(const ServerRepoItem &sr) {
  return db_->ExecUpdate(makeSql.makeInsert(sr));
}

int MdsRepo::LoadServerRepoItems(std::vector<ServerRepoItem> *serverList) {
  assert(serverList != nullptr);

  sql::ResultSet *res;
  int resCode = db_->QueryRows(makeSql.makeQueryRows(ServerRepoItem{}), &res);
  if (OperationOK != resCode) {
    return resCode;
  }

  while (res->next()) {
    serverList->push_back(ServerRepoItem(
        res->getUInt("serverID"),
        res->getString("hostName"),
        res->getString("internalHostIP"),
        res->getUInt("internalPort"),
        res->getString("externalHostIP"),
        res->getUInt("externalPort"),
        static_cast<uint16_t>(res->getUInt("zoneID")),
        static_cast<uint16_t>(res->getUInt("poolID")),
        res->getString("desc")));
  }
  delete (res);
  return resCode;
}

int MdsRepo::DeleteServerRepoItem(ServerIDType id) {
  return db_->ExecUpdate(makeSql.makeDelete(ServerRepoItem(id)));
}

int MdsRepo::UpdateServerRepoItem(const ServerRepoItem &sr) {
  return db_->ExecUpdate(makeSql.makeUpdate(sr));
}

int MdsRepo::QueryServerRepoItem(ServerIDType id,
                          ServerRepoItem *repo) {
  assert(repo != nullptr);

  sql::ResultSet *res;
  int resCode = db_->QueryRows(makeSql.makeQueryRow(ServerRepoItem(id)), &res);
  if (OperationOK != resCode) {
    return resCode;
  }

  while (res->next()) {
    repo->serverID = res->getUInt("serverID");
    repo->hostName = res->getString("hostName");
    repo->internalHostIP = res->getString("internalHostIP");
    repo->externalHostIP = res->getString("externalHostIP");
    repo->zoneID = static_cast<uint16_t>(res->getInt("zoneID"));
    repo->poolID = static_cast<uint16_t>(res->getInt("poolID"));
    repo->desc = res->getString("desc");
  }

  delete (res);
  return resCode;
}

int MdsRepo::InsertCopySetRepoItem(const CopySetRepoItem &cr) {
    return db_->ExecUpdate(makeSql.makeInsert(cr));
}

int MdsRepo::LoadCopySetRepoItems(std::vector<CopySetRepoItem> *copySetList) {
    assert(copySetList != nullptr);

    sql::ResultSet *res;
    int resCode =
        db_->QueryRows(makeSql.makeQueryRows(CopySetRepoItem{}), &res);
    if (resCode != OperationOK) {
        return resCode;
    }

    while (res->next()) {
        copySetList->push_back(
            CopySetRepoItem(res->getUInt("copySetID"),
                        static_cast<uint16_t>(res->getUInt("logicalPoolID")),
                        static_cast<uint64_t >(res->getUInt64("epoch")),
                        res->getString("chunkServerIDList")));
    }
    delete (res);
    return resCode;
}

int MdsRepo::DeleteCopySetRepoItem(CopySetIDType id, LogicalPoolIDType lid) {
    return db_->ExecUpdate(makeSql.makeDelete(CopySetRepoItem(id, lid)));
}

int MdsRepo::UpdateCopySetRepoItem(const CopySetRepoItem &cr) {
    return db_->ExecUpdate(makeSql.makeUpdate(cr));
}

int MdsRepo::QueryCopySetRepoItem(CopySetIDType id,
                           LogicalPoolIDType lid,
                           CopySetRepoItem *repo) {
    assert(repo != nullptr);

    sql::ResultSet *res;
    int status =
        db_->QueryRows(makeSql.makeQueryRow(CopySetRepoItem(id, lid)), &res);
    if (status != OperationOK) {
        return status;
    }
    while (res->next()) {
        repo->copySetID = res->getUInt("copySetID");
        repo->logicalPoolID =
            static_cast<uint16_t>(res->getUInt("logicalPoolID"));
        repo->epoch = static_cast<uint64_t >(res->getUInt64("epoch"));
        repo->chunkServerIDList = res->getString("chunkServerIDList");
    }

    delete (res);
    return status;
}

int MdsRepo::InsertLogicalPoolRepoItem(const LogicalPoolRepoItem &lr) {
  return db_->ExecUpdate(makeSql.makeInsert(lr));
}

int MdsRepo::LoadLogicalPoolRepoItems(
    std::vector<LogicalPoolRepoItem> *logicalPoolList) {
  assert(logicalPoolList != nullptr);

  sql::ResultSet *res;
  int resCode = db_->QueryRows(makeSql.makeQueryRows(
      LogicalPoolRepoItem{}), &res);
  if (resCode != OperationOK) {
    return resCode;
  }
  while (res->next()) {
    logicalPoolList->push_back(LogicalPoolRepoItem(
        static_cast<uint16_t>(res->getInt("logicalPoolID")),
        res->getString("logicalPoolName"),
        static_cast<uint16_t>(res->getInt("physicalPoolID")),
        static_cast<uint8_t>(res->getInt("type")),
        res->getUInt("initialScatterWidth"),
        res->getInt64("createTime"),
        static_cast<uint8_t>(res->getInt("status")),
        res->getString("redundanceAndPlacementPolicy"),
        res->getString("userPolicy"),
        res->getBoolean("availFlag")));
  }

  delete (res);
  return resCode;
}

int MdsRepo::DeleteLogicalPoolRepoItem(LogicalPoolIDType id) {
  return db_->ExecUpdate(makeSql.makeDelete(LogicalPoolRepoItem(id)));
}

int MdsRepo::UpdateLogicalPoolRepoItem(const LogicalPoolRepoItem &lr) {
  return db_->ExecUpdate(makeSql.makeUpdate(lr));
}

int MdsRepo::QueryLogicalPoolRepoItem(LogicalPoolIDType id,
                               LogicalPoolRepoItem *repo) {
  assert(repo != nullptr);

  sql::ResultSet *res;
  int resCode =
      db_->QueryRows(makeSql.makeQueryRow(LogicalPoolRepoItem(id)), &res);
  if (resCode != OperationOK) {
    return resCode;
  }
  while (res->next()) {
    repo->logicalPoolID = static_cast<uint16_t>(res->getUInt("logicalPoolID"));
    repo->logicalPoolName = res->getString("logicalPoolName");
    repo->physicalPoolID =
        static_cast<uint16_t>(res->getUInt("physicalPoolID"));
    repo->type = static_cast<uint8_t>(res->getUInt("type"));
    repo->initialScatterWidth = res->getUInt("initialScatterWidth");
    repo->createTime = res->getInt64("createTime");
    repo->status = static_cast<uint8_t>(res->getUInt("status"));
    repo->redundanceAndPlacementPolicy =
        res->getString("redundanceAndPlacementPolicy");
    repo->userPolicy = res->getString("userPolicy");
  }

  delete (res);
  return resCode;
}

int MdsRepo::InsertPhysicalPoolRepoItem(const PhysicalPoolRepoItem &pr) {
  return db_->ExecUpdate(makeSql.makeInsert(pr));
}

int MdsRepo::LoadPhysicalPoolRepoItems(
    std::vector<PhysicalPoolRepoItem> *physicalPoollist) {
  assert(physicalPoollist != nullptr);

  sql::ResultSet *res;
  int resCode =
      db_->QueryRows(makeSql.makeQueryRows(PhysicalPoolRepoItem{}), &res);
  if (resCode != OperationOK) {
    return resCode;
  }
  while (res->next()) {
    physicalPoollist->push_back(PhysicalPoolRepoItem(
        static_cast<uint16_t>(res->getUInt("physicalPoolID")),
        res->getString("physicalPoolName"),
        res->getString("desc")));
  }
  delete (res);
  return resCode;
}

int MdsRepo::DeletePhysicalPoolRepoItem(PhysicalPoolIDType id) {
  return db_->ExecUpdate(makeSql.makeDelete(PhysicalPoolRepoItem(id)));
}

int MdsRepo::UpdatePhysicalPoolRepoItem(const PhysicalPoolRepoItem &pr) {
  return db_->ExecUpdate(makeSql.makeUpdate(pr));
}

int MdsRepo::QueryPhysicalPoolRepoItem(PhysicalPoolIDType id,
                                PhysicalPoolRepoItem *repo) {
  assert(repo != nullptr);

  sql::ResultSet *res;
  int resCode =
      db_->QueryRows(makeSql.makeQueryRow(PhysicalPoolRepoItem(id)), &res);
  if (resCode != OperationOK) {
    return resCode;
  }
  while (res->next()) {
    repo->physicalPoolID =
        static_cast<uint16_t>(res->getUInt("physicalPoolID"));
    repo->physicalPoolName = res->getString("physicalPoolName");
    repo->desc = res->getString("desc");
  }

  delete (res);
  return resCode;
}

int MdsRepo::InsertZoneRepoItem(const ZoneRepoItem &zr) {
    return db_->ExecUpdate(makeSql.makeInsert(zr));
}

int MdsRepo::LoadZoneRepoItems(std::vector<ZoneRepoItem> *zonelist) {
    assert(zonelist != nullptr);

    sql::ResultSet *res;
    int resCode = db_->QueryRows(makeSql.makeQueryRows(ZoneRepoItem{}), &res);
    if (OperationOK != resCode) {
        return resCode;
    }
    while (res->next()) {
        zonelist->push_back(*new ZoneRepoItem(
            static_cast<uint32_t>(res->getInt("zoneID")),
            res->getString("zoneName"),
            static_cast<uint16_t>(res->getInt("poolID")),
            res->getString("desc")));
    }

    delete (res);
    return resCode;
}

int MdsRepo::DeleteZoneRepoItem(ZoneIDType id) {
    return db_->ExecUpdate(makeSql.makeDelete(ZoneRepoItem(id)));
}

int MdsRepo::UpdateZoneRepoItem(const ZoneRepoItem &zr) {
    return db_->ExecUpdate(makeSql.makeUpdate(zr));
}

int MdsRepo::QueryZoneRepoItem(ZoneIDType id,
                        ZoneRepoItem *repo) {
    assert(repo != nullptr);

    sql::ResultSet *res;
    int resCode = db_->QueryRows(makeSql.makeQueryRow(ZoneRepoItem(id)), &res);
    if (OperationOK != resCode) {
        return resCode;
    }

    while (res->next()) {
        repo->zoneID = res->getUInt("zoneID");
        repo->zoneName = res->getString("zoneName");
        repo->poolID = static_cast<uint16_t>(res->getUInt("poolID"));
        repo->desc = res->getString("desc");
    }

    delete (res);
    return resCode;
}

int MdsRepo::InsertSessionRepoItem(const SessionRepoItem &r) {
    return db_->ExecUpdate(makeSql.makeInsert(r));
}

int MdsRepo::LoadSessionRepoItems(std::vector<SessionRepoItem> *sessionList) {
    assert(sessionList != nullptr);

    sql::ResultSet *res;
    int resCode =
        db_->QueryRows(makeSql.makeQueryRows(SessionRepoItem{}), &res);
    if (OperationOK != resCode) {
        return resCode;
    }

    while (res->next()) {
        sessionList->push_back(
            SessionRepoItem(res->getString("fileName"),
                        res->getString("sessionID"),
                        res->getUInt("leaseTime"),
                        static_cast<uint8_t>(res->getUInt("sessionStatus")),
                        res->getUInt64("createTime"),
                        res->getString("clientIP")));
    }

    delete (res);
    return OperationOK;
}

int MdsRepo::DeleteSessionRepoItem(const std::string &sessionID) {
    return db_->ExecUpdate(makeSql.makeDelete(SessionRepoItem(sessionID)));
}

int MdsRepo::UpdateSessionRepoItem(const SessionRepoItem &repo) {
    return db_->ExecUpdate(makeSql.makeUpdate(repo));
}

int MdsRepo::QuerySessionRepoItem(const std::string &sessionID,
                                  SessionRepoItem *repo) {
    assert(repo != nullptr);

    sql::ResultSet *res;
    int resCode =
        db_->QueryRows(makeSql.makeQueryRow(SessionRepoItem(sessionID)),
                                 &res);
    if (OperationOK != resCode) {
        return resCode;
    }

    if (res->next()) {
        repo->fileName = res->getString("fileName");
        repo->sessionID = res->getString("sessionID");
        repo->leaseTime = res->getUInt("leaseTime");
        repo->sessionStatus =
                        static_cast<uint8_t>(res->getUInt("sessionStatus"));
        repo->createTime = res->getUInt64("createTime");
        repo->clientIP = res->getString("clientIP");
    }

    delete (res);
    return OperationOK;
}

int MdsRepo::InsertClientInfoRepoItem(const ClientInfoRepoItem &r) {
    return db_->ExecUpdate(makeSql.makeInsert(r));
}

int MdsRepo::LoadClientInfoRepoItems(
                        std::vector<ClientInfoRepoItem> *clientList) {
    sql::ResultSet *res;
    int resCode =
        db_->QueryRows(makeSql.makeQueryRows(ClientInfoRepoItem{}), &res);
    if (OperationOK != resCode) {
        return resCode;
    }

    while (res->next()) {
        clientList->push_back(
            ClientInfoRepoItem(res->getString("clientIp"),
                        res->getUInt("clientPort")));
    }

    delete (res);
    return OperationOK;
}

int MdsRepo::DeleteClientInfoRepoItem(const std::string &clientIp,
                                      uint32_t clientPort) {
    return db_->ExecUpdate(makeSql.makeDelete(
                                ClientInfoRepoItem(clientIp, clientPort)));
}

int MdsRepo::QueryClientInfoRepoItem(const std::string &clientIp,
                                      uint32_t clientPort,
                                      ClientInfoRepoItem *repo) {
    assert(repo != nullptr);

    sql::ResultSet *res;
    int resCode = db_->QueryRows(makeSql.makeQueryRow(
                            ClientInfoRepoItem(clientIp, clientPort)), &res);
    if (OperationOK != resCode) {
        return resCode;
    }

    if (res->next()) {
        repo->clientIp = res->getString("clientIp");
        repo->clientPort = res->getUInt("clientPort");
    }

    delete (res);
    return OperationOK;
}

int MdsRepo::InsertClusterInfoRepoItem(const ClusterInfoRepoItem &r) {
    return db_->ExecUpdate(makeSql.makeInsert(r));
}

int MdsRepo::LoadClusterInfoRepoItems(
    std::vector<ClusterInfoRepoItem> *list) {
    sql::ResultSet *res;
    int resCode =
        db_->QueryRows(makeSql.makeQueryRows(ClusterInfoRepoItem{}), &res);
    if (OperationOK != resCode) {
        return resCode;
    }

    while (res->next()) {
        list->push_back(
            ClusterInfoRepoItem(res->getString("clusterId")));
    }

    delete (res);
    return OperationOK;
}


}  // namespace mds
}  // namespace curve

