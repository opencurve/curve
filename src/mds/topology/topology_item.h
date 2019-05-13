/*
 * Project: curve
 * Created Date: Mon Aug 27 2018
 * Author: xuchaojie
 * Copyright (c) 2018 netease
 */

#ifndef SRC_MDS_TOPOLOGY_TOPOLOGY_ITEM_H_
#define SRC_MDS_TOPOLOGY_TOPOLOGY_ITEM_H_

#include <list>
#include <string>
#include <set>
#include <utility>

#include "src/mds/topology/topology_id_generator.h"
#include "proto/topology.pb.h"

namespace curve {
namespace mds {
namespace topology {

class LogicalPool {
 public:
  enum LogicalPoolStatus {
    ALLOCATABLE = 0,
    UNALLOCATABLE = 1,
  };

  union RedundanceAndPlaceMentPolicy {
    struct PRAP {
      uint16_t replicaNum;
      uint32_t copysetNum;
      uint16_t zoneNum;
    } pageFileRAP;

    struct ARAP {
      uint16_t replicaNum;
      uint32_t copysetNum;
      uint16_t zoneNum;
    } appendFileRAP;

    struct ECRAP {
      uint16_t dSegmentNum;
      uint32_t cSegmentNum;
      uint16_t zoneNum;
    } appendECFileRAP;
  };

  // TODO(xuchaojie): 用户控制相关逻辑可暂行先不实现，后续再修改
  struct UserPolicy {
    UserPolicy()
        : strictAllow(false) {}

    bool strictAllow;
    std::list<UserIdType> allowUserList;
    std::list<UserIdType> allowGroupList;
    std::list<UserIdType> denyUserList;
    std::list<UserIdType> denyGroupList;
  };

 public:
  static bool TransRedundanceAndPlaceMentPolicyFromJsonStr(
      const std::string &jsonStr,
      LogicalPoolType type,
      RedundanceAndPlaceMentPolicy *rap);
  static bool TransUserPolicyFromJsonStr(
      const std::string &jsonStr, LogicalPoolType type, UserPolicy *policy);

 public:
  LogicalPool()
      : id_(UNINTIALIZE_ID),
        name_(""),
        physicalPoolId_(UNINTIALIZE_ID),
        type_(PAGEFILE),
        createTime_(0),
        status_(UNALLOCATABLE),
        avaliable_(false) {}
  LogicalPool(PoolIdType id,
              const std::string &name,
              PoolIdType phyPoolId,
              LogicalPoolType type,
              const RedundanceAndPlaceMentPolicy &rap,
              const UserPolicy &policy,
              uint64_t createTime,
              bool avaliable)
      : id_(id),
        name_(name),
        physicalPoolId_(phyPoolId),
        type_(type),
        rap_(rap),
        policy_(policy),
        createTime_(createTime),
        status_(UNALLOCATABLE),
        avaliable_(avaliable) {}

  PoolIdType GetId() const {
      return id_;
  }

  std::string GetName() const {
      return name_;
  }

  PoolIdType GetPhysicalPoolId() const {
      return physicalPoolId_;
  }

  LogicalPoolType GetLogicalPoolType() const {
      return type_;
  }

  bool SetRedundanceAndPlaceMentPolicyByJson(const std::string &jsonStr);

  RedundanceAndPlaceMentPolicy GetRedundanceAndPlaceMentPolicy() const {
      return rap_;
  }

  std::string GetRedundanceAndPlaceMentPolicyJsonStr() const;

  uint64_t GetCreateTime() const {
      return createTime_;
  }

  void SetUserPolicy(const UserPolicy &policy) {
      policy_ = policy;
  }

  bool SetUserPolicyByJson(const std::string &jsonStr);

  UserPolicy GetUserPolicy() const {
      return policy_;
  }

  std::string GetUserPolicyJsonStr() const;

  void SetStatus(LogicalPoolStatus status) {
      status_ = status;
  }

  LogicalPoolStatus GetStatus() const {
      return status_;
  }

  void SetLogicalPoolAvaliableFlag(bool avaliable) {
      avaliable_ = avaliable;
  }

  bool GetLogicalPoolAvaliableFlag() const {
      return avaliable_;
  }

 private:
  PoolIdType id_;

  std::string name_;
  PoolIdType physicalPoolId_;
  LogicalPoolType type_;
  RedundanceAndPlaceMentPolicy rap_;
  UserPolicy policy_;

  uint64_t createTime_;
  LogicalPoolStatus status_;
  bool avaliable_;
};

class PhysicalPool {
 public:
  PhysicalPool()
      : id_(UNINTIALIZE_ID),
        name_(""),
        desc_("") {}
  PhysicalPool(PoolIdType id,
               const std::string &name,
               const std::string &desc)
      : id_(id),
        name_(name),
        desc_(desc) {}

  PoolIdType GetId() const {
      return id_;
  }
  std::string GetName() const {
      return name_;
  }

  void SetDesc(const std::string &desc) {
      desc_ = desc;
  }
  std::string GetDesc() const {
      return desc_;
  }

  void AddZone(ZoneIdType id) {
      zoneList_.push_back(id);
  }
  void RemoveZone(ZoneIdType id) {
      zoneList_.remove(id);
  }
  std::list<ZoneIdType> GetZoneList() const {
      return zoneList_;
  }

 private:
  PoolIdType id_;
  std::string name_;
  std::string desc_;

  std::list<ZoneIdType> zoneList_;
};

class Zone {
 public:
  Zone()
      : id_(UNINTIALIZE_ID),
        name_(""),
        physicalPoolId_(UNINTIALIZE_ID),
        desc_("") {}
  Zone(PoolIdType id,
       const std::string &name,
       PoolIdType physicalPoolId,
       const std::string &desc)
      : id_(id),
        name_(name),
        physicalPoolId_(physicalPoolId),
        desc_(desc) {}

  ZoneIdType GetId() const {
      return id_;
  }
  std::string GetName() const {
      return name_;
  }
  PoolIdType GetPhysicalPoolId() const {
      return physicalPoolId_;
  }

  void SetDesc(const std::string &desc) {
      desc_ = desc;
  }
  std::string GetDesc() const {
      return desc_;
  }

  void AddServer(ServerIdType id) {
      serverList_.push_back(id);
  }
  void RemoveServer(ServerIdType id) {
      serverList_.remove(id);
  }
  std::list<ServerIdType> GetServerList() const {
      return serverList_;
  }

 private:
  ZoneIdType id_;
  std::string name_;
  PoolIdType physicalPoolId_;
  std::string desc_;

  std::list<ServerIdType> serverList_;
};

class Server {
 public:
  Server()
      : id_(UNINTIALIZE_ID),
        hostName_(""),
        internalHostIp_(""),
        internalPort_(0),
        externalHostIp_(""),
        externalPort_(0),
        zoneId_(UNINTIALIZE_ID),
        physicalPoolId_(UNINTIALIZE_ID),
        desc_("") {}
  Server(ServerIdType id,
         const std::string &hostName,
         const std::string &internalHostIp,
         uint32_t internalPort,
         const std::string &externalHostIp,
         uint32_t externalPort,
         ZoneIdType zoneId,
         PoolIdType physicalPoolId,
         const std::string &desc)
      : id_(id),
        hostName_(hostName),
        internalHostIp_(internalHostIp),
        internalPort_(internalPort),
        externalHostIp_(externalHostIp),
        externalPort_(externalPort),
        zoneId_(zoneId),
        physicalPoolId_(physicalPoolId),
        desc_(desc) {}

  ServerIdType GetId() const {
      return id_;
  }
  std::string GetHostName() const {
      return hostName_;
  }
  std::string GetInternalHostIp() const {
      return internalHostIp_;
  }
  uint32_t GetInternalPort() const {
      return internalPort_;
  }
  std::string GetExternalHostIp() const {
      return externalHostIp_;
  }
  uint32_t GetExternalPort() const {
      return externalPort_;
  }
  ZoneIdType GetZoneId() const {
      return zoneId_;
  }
  PoolIdType GetPhysicalPoolId() const {
      return physicalPoolId_;
  }

  void SetDesc(const std::string &desc) {
      desc_ = desc;
  }

  std::string GetDesc() const {
      return desc_;
  }

  void AddChunkServer(ChunkServerIdType id) {
      chunkserverList_.push_back(id);
  }
  void RemoveChunkServer(ChunkServerIdType id) {
      chunkserverList_.remove(id);
  }
  std::list<ChunkServerIdType> GetChunkServerList() const {
      return chunkserverList_;
  }

 private:
  ServerIdType id_;
  std::string hostName_;
  std::string internalHostIp_;
  uint32_t internalPort_;
  std::string externalHostIp_;
  uint32_t externalPort_;
  ZoneIdType zoneId_;
  PoolIdType physicalPoolId_;
  std::string desc_;

  std::list<ChunkServerIdType> chunkserverList_;
};

class ChunkServerState {
 public:
  ChunkServerState()
      : diskState_(DISKNORMAL),
        onlineState_(OFFLINE),
        diskCapacity_(0),
        diskUsed_(0) {}

  void SetDiskState(DiskState state) {
      diskState_ = state;
  }
  DiskState GetDiskState() const {
      return diskState_;
  }
  void SetOnlineState(OnlineState state) {
      onlineState_ = state;
  }
  OnlineState GetOnlineState() const {
      return onlineState_;
  }

  void SetDiskCapacity(uint64_t capacity) {
      diskCapacity_ = capacity;
  }
  uint64_t GetDiskCapacity() const {
      return diskCapacity_;
  }
  void SetDiskUsed(uint64_t diskUsed) {
      diskUsed_ = diskUsed;
  }
  uint64_t GetDiskUsed() const {
      return diskUsed_;
  }

 private:
  DiskState diskState_;  // 磁盘状态，DistError、DiskNormal；
  OnlineState onlineState_;  // 0:online、1: offline

  uint64_t diskCapacity_;
  uint64_t diskUsed_;
};

class ChunkServer {
 public:
  ChunkServer()
      : id_(UNINTIALIZE_ID),
        token_(""),
        diskType_(""),
        serverId_(UNINTIALIZE_ID),
        internalHostIp_(""),
        port_(0),
        mountPoint_(""),
        status_(READWRITE),
        lastStateUpdateTime_(0) {}

  ChunkServer(ChunkServerIdType id,
              const std::string &token,
              const std::string &diskType,
              ServerIdType serverId,
              const std::string &hostIp,
              uint32_t port,
              const std::string &diskPath,
              ChunkServerStatus status = READWRITE)
      : id_(id),
        token_(token),
        diskType_(diskType),
        serverId_(serverId),
        internalHostIp_(hostIp),
        port_(port),
        mountPoint_(diskPath),
        status_(status),
        lastStateUpdateTime_(0) {}

  ChunkServerIdType GetId() const {
      return id_;
  }
  std::string GetToken() const {
      return token_;
  }

  void SetToken(std::string token) {
      token_ = token;
  }

  std::string GetDiskType() const {
      return diskType_;
  }
  void SetServerId(ServerIdType id) {
      serverId_ = id;
  }
  ServerIdType GetServerId() const {
      return serverId_;
  }
  void SetHostIp(const std::string &ip) {
      internalHostIp_ = ip;
  }
  std::string GetHostIp() const {
      return internalHostIp_;
  }
  void SetPort(uint32_t port) {
      port_ = port;
  }
  uint32_t GetPort() const {
      return port_;
  }
  void SetMountPoint(const std::string &mountPoint) {
      mountPoint_ = mountPoint;
  }
  std::string GetMountPoint() const {
      return mountPoint_;
  }

  void SetStatus(ChunkServerStatus status) {
      status_ = status;
  }
  ChunkServerStatus GetStatus() const {
      return status_;
  }

  void SetChunkServerState(const ChunkServerState &state) {
      state_ = state;
  }
  ChunkServerState GetChunkServerState() const {
      return state_;
  }

  void SetLastStateUpdateTime(uint64_t time) {
      lastStateUpdateTime_ = time;
  }
  uint64_t GetLastStateUpdateTime() const {
      return lastStateUpdateTime_;
  }

 private:
  ChunkServerIdType id_;
  std::string token_;
  std::string diskType_;  // "nvme_ssd"
  ServerIdType serverId_;
  std::string internalHostIp_;
  uint32_t port_;
  std::string mountPoint_;  // mnt/ssd1

  ChunkServerStatus status_;

  ChunkServerState state_;
  uint64_t lastStateUpdateTime_;
};

typedef std::pair<PoolIdType, CopySetIdType> CopySetKey;

class CopySetInfo {
 public:
  CopySetInfo() :
      logicalPoolId_(UNINTIALIZE_ID),
      copySetId_(UNINTIALIZE_ID) {}
  CopySetInfo(PoolIdType logicalPoolId,
              CopySetIdType id)
      : logicalPoolId_(logicalPoolId),
        copySetId_(id),
        epoch_(0),
        hasCandidate_(false),
        candidate_(UNINTIALIZE_ID) {}

  PoolIdType GetLogicalPoolId() const {
      return logicalPoolId_;
  }

  CopySetIdType GetId() const {
      return copySetId_;
  }

  void SetEpoch(EpochType epoch) {
      epoch_ = epoch;
  }

  EpochType GetEpoch() const {
      return epoch_;
  }

  ChunkServerIdType GetLeader() const {
      return leader_;
  }

  void SetLeader(ChunkServerIdType leader) {
      leader_ = leader;
  }

  CopySetKey GetCopySetKey() const {
      return CopySetKey(logicalPoolId_, copySetId_);
  }

  std::set<ChunkServerIdType> GetCopySetMembers() const {
      return peers_;
  }

  std::string GetCopySetMembersStr() const;

  void SetCopySetMembers(const std::set<ChunkServerIdType> &peers) {
      peers_ = peers;
  }

  bool HasMember(ChunkServerIdType peer) const {
      return peers_.count(peer) > 0;
  }

  bool SetCopySetMembersByJson(const std::string &jsonStr);

  bool HasCandidate() const {
      return hasCandidate_;
  }

  void SetCandidate(ChunkServerIdType csId) {
      hasCandidate_ = true;
      candidate_ = csId;
  }

  ChunkServerIdType GetCandidate() const {
      if (hasCandidate_) {
          return candidate_;
      } else {
          return UNINTIALIZE_ID;
      }
  }

  void ClearCandidate() {
      hasCandidate_ = false;
  }

 private:
  PoolIdType logicalPoolId_;
  CopySetIdType copySetId_;
  ChunkServerIdType leader_;
  EpochType epoch_;
  std::set<ChunkServerIdType> peers_;
  bool hasCandidate_;
  ChunkServerIdType candidate_;
};

/**
 * @brief 生成peerId
 *
 * @param ip hostIp
 * @param port 端口号
 * @param idx index
 *
 * @return peerId
 */
inline std::string BuildPeerId(
    const std::string &ip,
    uint32_t port,
    uint32_t idx = 0) {
    return ip + ":" + std::to_string(port) + ":" + std::to_string(idx);
}

/**
 * @brief 拆分peerId
 *
 * @param peerId peerId
 * @param[out] ip hostIp
 * @param[out] port 端口号
 * @param[out] idx index
 *
 * @retval true success
 * @retval false fail
 */
bool SplitPeerId(
    const std::string &peerId,
    std::string *ip,
    uint32_t *port,
    uint32_t *idx = nullptr);

}  // namespace topology
}  // namespace mds
}  // namespace curve

#endif  // SRC_MDS_TOPOLOGY_TOPOLOGY_ITEM_H_
