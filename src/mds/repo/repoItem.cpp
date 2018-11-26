/*
 * Project: curve
 * Created Date: Mon Sep 10 2018
 * Author: lixiaocui
 * Copyright (c) 2018 netease
 */

#include <iostream>
#include "src/mds/repo/repoItem.h"

namespace curve {
namespace repo {
std::string convertToSqlValue(const std::string value) {
  return "'" + value + "'";
}

std::string MakeSql::makeCondtion(const curve::repo::RepoItem &t) {
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

// ChunkServer
ChunkServerRepo::ChunkServerRepo(uint32_t id) {
  this->chunkServerID = id;
}

ChunkServerRepo::ChunkServerRepo(uint32_t id,
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

bool ChunkServerRepo::operator==(const curve::repo::ChunkServerRepo &r) {
  return chunkServerID == r.chunkServerID &&
      serverID == r.serverID &&
      token == r.token &&
      diskType == r.diskType;
}

void ChunkServerRepo::getKV(std::map<std::string, std::string> (*kv)) const {
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

void ChunkServerRepo::getPrimaryKV(std::map<std::string,
                                            std::string> *primary) const {
  (*primary)["chunkServerID"] = std::to_string(chunkServerID);
}

std::string ChunkServerRepo::getTable() const {
  return ChunkServerTable;
}

// Server
ServerRepo::ServerRepo(uint32_t id) {
  this->serverID = id;
}

ServerRepo::ServerRepo(uint32_t id,
                       const std::string &host,
                       const std::string &inIp,
                       const std::string &exIp,
                       uint16_t zoneID,
                       uint16_t poolID,
                       const std::string &desc) {
  this->serverID = id;
  this->hostName = host;
  this->internalHostIP = inIp;
  this->externalHostIP = exIp;
  this->zoneID = zoneID;
  this->poolID = poolID;
  this->desc = desc;
}

bool ServerRepo::operator==(const curve::repo::ServerRepo &s) {
  return serverID == s.serverID &&
      hostName == s.hostName &&
      internalHostIP == s.internalHostIP &&
      externalHostIP == s.externalHostIP &&
      zoneID == s.zoneID &&
      poolID == s.poolID;
}

void ServerRepo::getKV(std::map<std::string, std::string> (*kv)) const {
  ((*kv))["serverID"] = std::to_string(serverID);
  ((*kv))["hostName"] = convertToSqlValue(hostName);
  ((*kv))["internalHostIP"] = convertToSqlValue(internalHostIP);
  ((*kv))["externalHostIP"] = convertToSqlValue(externalHostIP);
  ((*kv))["zoneID"] = std::to_string(zoneID);
  ((*kv))["poolID"] = std::to_string(poolID);
  ((*kv))["`desc`"] = convertToSqlValue(desc);
}

void ServerRepo::getPrimaryKV(std::map<std::string,
                                       std::string> *primary) const {
  (*primary)["serverID"] = std::to_string(serverID);
}

std::string ServerRepo::getTable() const {
  return ServerTable;
}

// Zone
ZoneRepo::ZoneRepo(uint32_t id) {
  this->zoneID = id;
}

ZoneRepo::ZoneRepo(uint32_t zoneID,
                   const std::string &name,
                   uint16_t poolID,
                   const std::string &desc) {
  this->zoneID = zoneID;
  this->zoneName = name;
  this->poolID = poolID;
  this->desc = desc;
}

bool ZoneRepo::operator==(const curve::repo::ZoneRepo &r) {
  return zoneID == r.zoneID &&
      zoneName == r.zoneName &&
      poolID == r.poolID;
}

void ZoneRepo::getKV(std::map<std::string, std::string> (*kv)) const {
  ((*kv))["zoneID"] = std::to_string(zoneID);
  ((*kv))["zoneName"] = convertToSqlValue(zoneName);
  ((*kv))["poolID"] = std::to_string(poolID);
  ((*kv))["`desc`"] = convertToSqlValue(desc);
}

void ZoneRepo::getPrimaryKV(std::map<std::string, std::string> *primary) const {
  (*primary)["zoneID"] = std::to_string(zoneID);
}

std::string ZoneRepo::getTable() const {
  return ZoneTable;
}

// LogicalPool
LogicalPoolRepo::LogicalPoolRepo(uint16_t id) {
  this->logicalPoolID = id;
}

LogicalPoolRepo::LogicalPoolRepo(uint16_t logicalID,
                                 const std::string &logicalName,
                                 uint16_t physicalID,
                                 uint8_t type,
                                 int64_t createTime,
                                 uint8_t status,
                                 const std::string &reduancePolicy,
                                 const std::string &userPolicy) {
  this->logicalPoolID = logicalID;
  this->logicalPoolName = logicalName;
  this->physicalPoolID = physicalID;
  this->type = type;
  this->createTime = createTime;
  this->status = status;
  this->redundanceAndPlacementPolicy = reduancePolicy;
  this->userPolicy = userPolicy;
}

bool LogicalPoolRepo::operator==(const curve::repo::LogicalPoolRepo &r) {
  return logicalPoolID == r.logicalPoolID &&
      logicalPoolName == r.logicalPoolName &&
      physicalPoolID == r.physicalPoolID &&
      type == r.type &&
      createTime == r.createTime;
}

void LogicalPoolRepo::getKV(std::map<std::string, std::string> (*kv)) const {
  (*kv)["logicalPoolID"] = std::to_string(logicalPoolID);
  (*kv)["logicalPoolName"] = convertToSqlValue(logicalPoolName);
  (*kv)["physicalPoolID"] = std::to_string(physicalPoolID);
  (*kv)["type"] = std::to_string(type);
  (*kv)["createTime"] = std::to_string(createTime);
  (*kv)["status"] = std::to_string(status);
  (*kv)["redundanceAndPlacementPolicy"] =
      convertToSqlValue(redundanceAndPlacementPolicy);
  (*kv)["userPolicy"] = convertToSqlValue(userPolicy);
}

void LogicalPoolRepo::getPrimaryKV(std::map<std::string,
                                            std::string> *primay) const {
  (*primay)["logicalPoolID"] = std::to_string(logicalPoolID);
}

std::string LogicalPoolRepo::getTable() const {
  return LogicalPoolTable;
}

// PhysicalPool
PhysicalPoolRepo::PhysicalPoolRepo(uint16_t id) {
  this->physicalPoolID = id;
}

PhysicalPoolRepo::PhysicalPoolRepo(uint16_t id,
                                   const std::string &name,
                                   const std::string &desc) {
  this->physicalPoolID = id;
  this->physicalPoolName = name;
  this->desc = desc;
}

bool PhysicalPoolRepo::operator==(const curve::repo::PhysicalPoolRepo &r) {
  return physicalPoolID == r.physicalPoolID &&
      physicalPoolName == r.physicalPoolName;
}

void PhysicalPoolRepo::getKV(std::map<std::string, std::string> *kv) const {
  (*kv)["physicalPoolID"] = std::to_string(physicalPoolID);
  (*kv)["physicalPoolName"] = convertToSqlValue(physicalPoolName);
  (*kv)["`desc`"] = convertToSqlValue(desc);
}

void PhysicalPoolRepo::getPrimaryKV(std::map<std::string,
                                             std::string> *primary) const {
  (*primary)["physicalPoolID"] = std::to_string(physicalPoolID);
}

std::string PhysicalPoolRepo::getTable() const {
  return PhysicalPoolTable;
}

// CopySet
CopySetRepo::CopySetRepo(uint32_t id, uint16_t logicalPoolID) {
  this->copySetID = id;
  this->logicalPoolID = logicalPoolID;
}

CopySetRepo::CopySetRepo(uint32_t id,
                         uint16_t poolID,
                         const std::string &chunkServerList) {
  this->copySetID = id;
  this->logicalPoolID = poolID;
  this->chunkServerIDList = chunkServerList;
}

bool CopySetRepo::operator==(const curve::repo::CopySetRepo &r) {
  return copySetID == r.copySetID &&
      logicalPoolID == r.logicalPoolID;
}

void CopySetRepo::getKV(std::map<std::string, std::string> *kv) const {
  (*kv)["copySetID"] = std::to_string(copySetID);
  (*kv)["logicalPoolID"] = std::to_string(logicalPoolID);
  (*kv)["chunkServerIDList"] = convertToSqlValue(chunkServerIDList);
}

void CopySetRepo::getPrimaryKV(std::map<std::string,
                                        std::string> *primary) const {
  (*primary)["copySetID"] = std::to_string(copySetID);
  (*primary)["logicalPoolID"] = std::to_string(logicalPoolID);
}

std::string CopySetRepo::getTable() const {
  return CopySetTable;
}

// session
SessionRepo::SessionRepo(std::string fileName, std::string sessionID,
                         std::string token, uint32_t leaseTime,
                         uint16_t sessionStatus, uint64_t createTime,
                         std::string clientIP) {
  this->fileName = fileName;
  this->sessionID = sessionID;
  this->token = token;
  this->leaseTime = leaseTime;
  this->sessionStatus = sessionStatus;
  this->createTime = createTime;
  this->clientIP = clientIP;

  return;
}

SessionRepo::SessionRepo(std::string sessionID) {
  this->sessionID = sessionID;
}

bool SessionRepo::operator==(const SessionRepo &r) {
  return sessionID == r.sessionID &&
         token == r.token &&
         fileName == r.fileName;
}


void SessionRepo::getKV(std::map<std::string, std::string> *kv) const {
  (*kv)["fileName"] = convertToSqlValue(fileName);
  (*kv)["sessionID"] = convertToSqlValue(sessionID);
  (*kv)["token"] = convertToSqlValue(token);
  (*kv)["leaseTime"] = std::to_string(leaseTime);
  (*kv)["sessionStatus"] = std::to_string(sessionStatus);
  (*kv)["createTime"] = std::to_string(createTime);
  (*kv)["clientIP"] = convertToSqlValue(clientIP);
  return;
}

void SessionRepo::getPrimaryKV(std::map<std::string,
                               std::string> *primary) const {
  (*primary)["sessionID"] = convertToSqlValue(sessionID);
  return;
}

std::string SessionRepo::getTable() const {
  return SessionTable;
}

uint16_t SessionRepo::GetSessionStatus() {
  return sessionStatus;
}

void SessionRepo::SetSessionStatus(uint16_t status) {
  sessionStatus = status;
}
}  // namespace repo
}  // namespace curve
