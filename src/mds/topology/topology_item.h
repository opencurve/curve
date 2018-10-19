/*
 * Project: curve
 * Created Date: Mon Aug 27 2018
 * Author: xuchaojie
 * Copyright (c) 2018 netease
 */

#ifndef CURVE_SRC_MDS_TOPOLOGY_TOPOLOGY_ITEM_H_
#define CURVE_SRC_MDS_TOPOLOGY_TOPOLOGY_ITEM_H_


#include <list>
#include <string>
#include <set>

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
    LogicalPool()
        : id_(TopologyIdGenerator::UNINTIALIZE_ID),
          name_(""),
          physicalPoolId_(TopologyIdGenerator::UNINTIALIZE_ID),
          type_(PAGEFILE),
          createTime_(0),
          status_(UNALLOCATABLE) {}
    LogicalPool(PoolIdType id,
            const std::string &name,
            PoolIdType phyPoolId,
            LogicalPoolType  type,
            const RedundanceAndPlaceMentPolicy &rap,
            const UserPolicy &policy,
            uint64_t createTime)
        : id_(id),
          name_(name),
          physicalPoolId_(phyPoolId),
          type_(type),
          rap_(rap),
          policy_(policy),
          createTime_(createTime),
          status_(UNALLOCATABLE) {}

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

    RedundanceAndPlaceMentPolicy GetRedundanceAndPlaceMentPolicy() const {
        return rap_;
    }

    uint64_t GetCreateTime() const {
        return createTime_;
    }

    void SetUserPolicy(const UserPolicy &policy) {
        policy_ = policy;
    }

    UserPolicy GetUserPolicy() const {
        return policy_;
    }

    void SetStatus(LogicalPoolStatus status) {
        status_ = status;
    }

    LogicalPoolStatus GetStatus() const {
        return status_;
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
};

class PhysicalPool {
 public:
    PhysicalPool()
        : id_(TopologyIdGenerator::UNINTIALIZE_ID),
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
        : id_(TopologyIdGenerator::UNINTIALIZE_ID),
          name_(""),
          physicalPoolId_(TopologyIdGenerator::UNINTIALIZE_ID),
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
        : id_(TopologyIdGenerator::UNINTIALIZE_ID),
          hostName_(""),
          internalHostIp_(""),
          externalHostIp_(""),
          zoneId_(TopologyIdGenerator::UNINTIALIZE_ID),
          physicalPoolId_(TopologyIdGenerator::UNINTIALIZE_ID),
          desc_("") {}
    Server(ServerIdType id,
           const std::string &hostName,
           const std::string &internalHostIp,
           const std::string &externalHostIp,
           ZoneIdType zoneId,
           PoolIdType physicalPoolId,
           const std::string &desc)
        : id_(id),
          hostName_(hostName),
          internalHostIp_(internalHostIp),
          externalHostIp_(externalHostIp),
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
    std::string GetExternalHostIp() const {
        return externalHostIp_;
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
    std::string externalHostIp_;
    ZoneIdType zoneId_;
    PoolIdType physicalPoolId_;
    std::string desc_;

    std::list<ChunkServerIdType> chunkserverList_;
};

class ChunkServerState {
 public:
    ChunkServerState()
    : diskState_(DISKNORMAL),
      onlineState_(ONLINE),
      diskCapacity_(0),
      diskUsed_(0) {}
    ~ChunkServerState() {}

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
        : id_(TopologyIdGenerator::UNINTIALIZE_ID),
          token_(""),
          diskType_(""),
          serverId_(TopologyIdGenerator::UNINTIALIZE_ID),
          internalHostIp_(""),
          port_(0),
          mountPoint_(""),
          status_(READWRITE),
          lastStateUpdateTime_(0) {}

    ChunkServer(ChunkServerIdType id,
                const std::string token,
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
        logicalPoolId_(TopologyIdGenerator::UNINTIALIZE_ID),
        copySetId_(TopologyIdGenerator::UNINTIALIZE_ID) {}
    CopySetInfo(PoolIdType logicalPoolId,
        CopySetIdType id)
        : logicalPoolId_(logicalPoolId),
        copySetId_(id) {}
    ~CopySetInfo() {}

    PoolIdType GetLogicalPoolId() const {
        return logicalPoolId_;
    }

    CopySetIdType GetId() const {
        return copySetId_;
    }

    std::set<ChunkServerIdType> GetCopySetMembers() const {
        return csIdList_;
    }

    void SetCopySetMembers(const std::set<ChunkServerIdType> &idList) {
        csIdList_ = idList;
    }

 private:
    PoolIdType logicalPoolId_;
    CopySetIdType copySetId_;
    std::set<ChunkServerIdType> csIdList_;
};

}  // namespace topology
}  // namespace mds
}  // namespace curve

#endif  // CURVE_SRC_MDS_TOPOLOGY_TOPOLOGY_ITEM_H_
