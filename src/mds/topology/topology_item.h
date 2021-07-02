/*
 *  Copyright (c) 2020 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * Project: curve
 * Created Date: Mon Aug 27 2018
 * Author: xuchaojie
 */

#ifndef SRC_MDS_TOPOLOGY_TOPOLOGY_ITEM_H_
#define SRC_MDS_TOPOLOGY_TOPOLOGY_ITEM_H_

#include <list>
#include <string>
#include <set>
#include <utility>

#include "src/mds/topology/topology_id_generator.h"
#include "proto/topology.pb.h"
#include "src/common/concurrent/concurrent.h"

namespace curve {
namespace mds {
namespace topology {

/**
 * @brief cluster information, so far we only use clusterId
 */
struct ClusterInformation {
    // the only and unique Id of a cluster
    std::string clusterId;

    ClusterInformation() = default;
    explicit ClusterInformation(const std::string &clusterId)
        : clusterId(clusterId) {}

    bool SerializeToString(std::string *value) const;

    bool ParseFromString(const std::string &value);
};

class LogicalPool {
 public:
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

    // TODO(xuchaojie): User control related logic not implemented

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
          initialScatterWidth_(0),
          createTime_(0),
          status_(AllocateStatus::ALLOW),
          avaliable_(false),
          scanEnable_(true) {}
    LogicalPool(PoolIdType id,
                const std::string &name,
                PoolIdType phyPoolId,
                LogicalPoolType type,
                const RedundanceAndPlaceMentPolicy &rap,
                const UserPolicy &policy,
                uint64_t createTime,
                bool avaliable,
                bool scanEnable)
        : id_(id),
          name_(name),
          physicalPoolId_(phyPoolId),
          type_(type),
          rap_(rap),
          policy_(policy),
          initialScatterWidth_(0),
          createTime_(createTime),
          status_(AllocateStatus::ALLOW),
          avaliable_(avaliable),
          scanEnable_(scanEnable) {}

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


    void SetRedundanceAndPlaceMentPolicy(
        const RedundanceAndPlaceMentPolicy &rap) {
        rap_ = rap;
    }

    bool SetRedundanceAndPlaceMentPolicyByJson(const std::string &jsonStr);

    RedundanceAndPlaceMentPolicy GetRedundanceAndPlaceMentPolicy() const {
        return rap_;
    }

    uint16_t GetReplicaNum() const;

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

    void SetScatterWidth(uint32_t scatterWidth) {
        initialScatterWidth_ = scatterWidth;
    }

    uint32_t GetScatterWidth() const {
        return initialScatterWidth_;
    }

    void SetStatus(AllocateStatus status) {
        status_ = status;
    }

    AllocateStatus GetStatus() const {
        return status_;
    }

    void SetLogicalPoolAvaliableFlag(bool avaliable) {
        avaliable_ = avaliable;
    }

    bool GetLogicalPoolAvaliableFlag() const {
        return avaliable_;
    }

    bool SerializeToString(std::string *value) const;

    bool ParseFromString(const std::string &value);

    void SetScanEnable(bool scanEnable) {
        scanEnable_ = scanEnable;
    }

    bool ScanEnable() const {
        return scanEnable_;
    }

 private:
    PoolIdType id_;

    std::string name_;
    PoolIdType physicalPoolId_;
    LogicalPoolType type_;
    RedundanceAndPlaceMentPolicy rap_;
    UserPolicy policy_;
    /**
     * @brief initial average scatterwidth for scheduler
     */
    uint32_t initialScatterWidth_;

    uint64_t createTime_;
    AllocateStatus status_;
    bool avaliable_;
    bool scanEnable_;
};

class PhysicalPool {
 public:
    PhysicalPool()
        : id_(UNINTIALIZE_ID),
          name_(""),
          desc_(""),
          diskCapacity_(0) {}
    PhysicalPool(PoolIdType id,
                 const std::string &name,
                 const std::string &desc)
        : id_(id),
          name_(name),
          desc_(desc),
          diskCapacity_(0) {}

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

    void SetDiskCapacity(uint64_t diskCapacity) {
        diskCapacity_ = diskCapacity;
    }
    uint64_t GetDiskCapacity() const {
        return diskCapacity_;
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

    bool SerializeToString(std::string *value) const;

    bool ParseFromString(const std::string &value);

 private:
    PoolIdType id_;
    std::string name_;
    std::string desc_;

    // logical total capacity
    uint64_t diskCapacity_;

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

    bool SerializeToString(std::string *value) const;

    bool ParseFromString(const std::string &value);

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

    bool SerializeToString(std::string *value) const;

    bool ParseFromString(const std::string &value);

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
          diskCapacity_(0),
          diskUsed_(0) {}

    void SetDiskState(DiskState state) {
        diskState_ = state;
    }
    DiskState GetDiskState() const {
        return diskState_;
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
    DiskState diskState_;  // disk state，DistError、DiskNormal;

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
          externalHostIp_(""),
          port_(0),
          mountPoint_(""),
          startUpTime_(0),
          status_(READWRITE),
          onlineState_(OFFLINE),
          dirty_(false) {}

    ChunkServer(ChunkServerIdType id,
                const std::string &token,
                const std::string &diskType,
                ServerIdType serverId,
                const std::string &hostIp,
                uint32_t port,
                const std::string &diskPath,
                ChunkServerStatus status = READWRITE,
                OnlineState onlineState = OnlineState::OFFLINE,
                const std::string &externalHostIp = "")
        : id_(id),
          token_(token),
          diskType_(diskType),
          serverId_(serverId),
          internalHostIp_(hostIp),
          externalHostIp_(externalHostIp),
          port_(port),
          mountPoint_(diskPath),
          startUpTime_(0),
          status_(status),
          onlineState_(onlineState),
          dirty_(false) {}

    ChunkServer(const ChunkServer& v) :
        id_(v.id_),
        token_(v.token_),
        diskType_(v.diskType_),
        serverId_(v.serverId_),
        internalHostIp_(v.internalHostIp_),
        port_(v.port_),
        mountPoint_(v.mountPoint_),
        startUpTime_(v.startUpTime_),
        status_(v.status_),
        onlineState_(v.onlineState_),
        state_(v.state_),
        dirty_(v.dirty_) {}

    ChunkServer& operator= (const ChunkServer& v) {
        if (&v == this) {
            return *this;
        }
        id_ = v.id_;
        token_ = v.token_;
        diskType_ = v.diskType_;
        serverId_ = v.serverId_;
        internalHostIp_ = v.internalHostIp_;
        externalHostIp_ = v.externalHostIp_;
        port_ = v.port_;
        mountPoint_ = v.mountPoint_;
        startUpTime_ = v.startUpTime_;
        status_ = v.status_;
        onlineState_ = v.onlineState_;
        state_ = v.state_;
        dirty_ = v.dirty_;
        return *this;
    }

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
    void SetExternalHostIp(const std::string &ip) {
        externalHostIp_ = ip;
    }
    std::string GetExternalHostIp() const {
        return externalHostIp_;
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

    void SetStartUpTime(uint64_t time) {
        startUpTime_ = time;
    }

    uint64_t GetStartUpTime() const {
        return startUpTime_;
    }

    void SetStatus(ChunkServerStatus status) {
        status_ = status;
    }
    ChunkServerStatus GetStatus() const {
        return status_;
    }

    void SetOnlineState(OnlineState state) {
        onlineState_ = state;
    }
    OnlineState GetOnlineState() const {
        return onlineState_;
    }

    void SetChunkServerState(const ChunkServerState &state) {
        state_ = state;
    }
    ChunkServerState GetChunkServerState() const {
        return state_;
    }

    bool GetDirtyFlag() const {
        return dirty_;
    }

    void SetDirtyFlag(bool dirty) {
        dirty_ = dirty;
    }

    ::curve::common::RWLock& GetRWLockRef() const {
        return mutex_;
    }

    bool SerializeToString(std::string *value) const;

    bool ParseFromString(const std::string &value);

 private:
    ChunkServerIdType id_;
    std::string token_;
    std::string diskType_;  // "nvme_ssd"
    ServerIdType serverId_;
    std::string internalHostIp_;
    std::string externalHostIp_;
    uint32_t port_;
    std::string mountPoint_;  // mnt/ssd1

    /**
     * @brief chunkserver start up time
     */
    uint64_t startUpTime_;

    ChunkServerStatus status_;
    OnlineState onlineState_;  // 0:online、1: offline

    ChunkServerState state_;

    /**
     * @brief to mark whether data is dirty, for writing to storage regularly
     */
    bool dirty_;
    /**
     * @brief chunkserver read/write lock, for protecting
     *        concurrent read/write on the chunksever
     */
    mutable ::curve::common::RWLock mutex_;
};

typedef std::pair<PoolIdType, CopySetIdType> CopySetKey;

struct CopysetIdInfo {
    PoolIdType logicalPoolId;
    CopySetIdType copySetId;
};

class CopySetInfo {
 public:
    CopySetInfo() :
        logicalPoolId_(UNINTIALIZE_ID),
        copySetId_(UNINTIALIZE_ID),
        leader_(UNINTIALIZE_ID),
        epoch_(0),
        hasCandidate_(false),
        candidate_(UNINTIALIZE_ID),
        scaning_(false),
        hasScaning_(false),
        lastScanSec_(0),
        hasLastScanSec_(false),
        lastScanConsistent_(true),
        dirty_(false),
        available_(true) {}

    CopySetInfo(PoolIdType logicalPoolId,
                CopySetIdType id) :
        logicalPoolId_(logicalPoolId),
        copySetId_(id),
        leader_(UNINTIALIZE_ID),
        epoch_(0),
        hasCandidate_(false),
        candidate_(UNINTIALIZE_ID),
        scaning_(false),
        hasScaning_(false),
        lastScanSec_(0),
        hasLastScanSec_(false),
        lastScanConsistent_(true),
        dirty_(false),
        available_(true) {}

    CopySetInfo(const CopySetInfo &v) :
        logicalPoolId_(v.logicalPoolId_),
        copySetId_(v.copySetId_),
        leader_(v.leader_),
        epoch_(v.epoch_),
        peers_(v.peers_),
        hasCandidate_(v.hasCandidate_),
        candidate_(v.candidate_),
        scaning_(v.scaning_),
        hasScaning_(v.hasScaning_),
        lastScanSec_(v.lastScanSec_),
        hasLastScanSec_(v.hasLastScanSec_),
        lastScanConsistent_(v.lastScanConsistent_),
        dirty_(v.dirty_),
        available_(v.available_) {}

    CopySetInfo& operator= (const CopySetInfo &v) {
        if (&v == this) {
            return *this;
        }
        logicalPoolId_ = v.logicalPoolId_;
        copySetId_ = v.copySetId_;
        leader_ = v.leader_;
        epoch_ = v.epoch_;
        peers_ = v.peers_;
        hasCandidate_ = v.hasCandidate_;
        candidate_ = v.candidate_;
        scaning_ = v.scaning_;
        hasScaning_ = v.hasScaning_;
        lastScanSec_ = v.lastScanSec_;
        hasLastScanSec_ = v.hasLastScanSec_;
        lastScanConsistent_ = v.lastScanConsistent_;
        dirty_ = v.dirty_;
        available_ = v.available_;
        return *this;
    }

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

    void SetScaning(bool scaning) {
        scaning_ = scaning;
        hasScaning_ = true;
    }

    bool GetScaning() const {
        return scaning_;
    }

    bool IsLatestScaning(bool scaning) const {
        return hasScaning_ && scaning_ != scaning;
    }

    void SetLastScanSec(LastScanSecType lastScanSec) {
        lastScanSec_ = lastScanSec;
        hasLastScanSec_ = true;
    }

    bool IsLatestLastScanSec(LastScanSecType lastScanSec) const {
        return hasLastScanSec_ && lastScanSec_ > lastScanSec;
    }

    LastScanSecType GetLastScanSec() const {
        return lastScanSec_;
    }

    bool SetLastScanConsistent(bool lastScanConsistent) {
        lastScanConsistent_ = lastScanConsistent;
    }

    bool GetLastScanConsistent() const {
        return lastScanConsistent_;
    }

    void ClearCandidate() {
        hasCandidate_ = false;
    }

    bool GetDirtyFlag() const {
        return dirty_;
    }

    void SetDirtyFlag(bool dirty) {
        dirty_ = dirty;
    }

    bool IsAvailable() const {
        return available_;
    }

    void SetAvailableFlag(bool aval) {
        available_ = aval;
    }

    ::curve::common::RWLock& GetRWLockRef() const {
        return mutex_;
    }

    bool SerializeToString(std::string *value) const;

    bool ParseFromString(const std::string &value);

 private:
    PoolIdType logicalPoolId_;
    CopySetIdType copySetId_;
    ChunkServerIdType leader_;
    EpochType epoch_;
    std::set<ChunkServerIdType> peers_;
    bool hasCandidate_;
    ChunkServerIdType candidate_;

    // whether the current copyset is on scaning
    bool scaning_;

    // whether the scaning_ has been set
    bool hasScaning_;

    // timestamp for last success scan (seconds)
    LastScanSecType lastScanSec_;

    // whether the lastScanSec_ has been set
    bool hasLastScanSec_;

    // whether the data of copies is consistent for last scan
    bool lastScanConsistent_;

    /**
     * @brief to mark whether data is dirty, for writing to storage regularly
     */
    bool dirty_;

    /**
     * @brief To mark whether the copyset is available. If not available,
     *        will stop allocating chunks into this copyset.
     */
    bool available_;

    /**
     * @brief chunkserver read/write lock, for protecting concurrent
     *        read/write on the chunksever
     */
    mutable ::curve::common::RWLock mutex_;
};

/**
 * @brief generate peerId
 *
 * @param ip hostIp
 * @param port port number
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
 * @brief split peer ID
 *
 * @param peerId peer ID
 * @param[out] ip hostIp
 * @param[out] import port
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
