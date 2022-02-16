/*
 *  Copyright (c) 2021 NetEase Inc.
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
 * Created Date: 2021-08-24
 * Author: wanghai01
 */

#ifndef CURVEFS_SRC_MDS_TOPOLOGY_TOPOLOGY_ITEM_H_
#define CURVEFS_SRC_MDS_TOPOLOGY_TOPOLOGY_ITEM_H_

#include <list>
#include <set>
#include <string>
#include <utility>

#include "curvefs/proto/common.pb.h"
#include "curvefs/proto/topology.pb.h"
#include "curvefs/src/mds/topology/topology_id_generator.h"
#include "src/common/concurrent/concurrent.h"

using curvefs::common::PartitionStatus;

namespace curvefs {
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

class Pool {
 public:
    struct RedundanceAndPlaceMentPolicy {
        uint16_t replicaNum;
        uint32_t copysetNum;
        uint16_t zoneNum;
    };

 public:
    static bool TransRedundanceAndPlaceMentPolicyFromJsonStr(
        const std::string &jsonStr, RedundanceAndPlaceMentPolicy *rap);

 public:
    Pool()
        : id_(UNINITIALIZE_ID), name_(""), createTime_(0), diskCapacity_(0) {}
    Pool(PoolIdType id, const std::string &name,
         const RedundanceAndPlaceMentPolicy &rap, uint64_t createTime)
        : id_(id),
          name_(name),
          rap_(rap),
          createTime_(createTime),
          diskCapacity_(0) {}

    PoolIdType GetId() const { return id_; }

    std::string GetName() const { return name_; }

    void SetRedundanceAndPlaceMentPolicy(
        const RedundanceAndPlaceMentPolicy &rap) {
        rap_ = rap;
    }

    bool SetRedundanceAndPlaceMentPolicyByJson(const std::string &jsonStr);

    RedundanceAndPlaceMentPolicy GetRedundanceAndPlaceMentPolicy() const {
        return rap_;
    }

    std::string GetRedundanceAndPlaceMentPolicyJsonStr() const;

    uint16_t GetReplicaNum() const { return rap_.replicaNum; }

    uint64_t GetCreateTime() const { return createTime_; }

    void SetDiskThreshold(uint64_t diskCapacity) {
        diskCapacity_ = diskCapacity;
    }
    uint64_t GetDiskThreshold() const { return diskCapacity_; }

    void AddZone(ZoneIdType id) { zoneList_.push_back(id); }

    void RemoveZone(ZoneIdType id) { zoneList_.remove(id); }

    std::list<ZoneIdType> GetZoneList() const { return zoneList_; }

    bool SerializeToString(std::string *value) const;

    bool ParseFromString(const std::string &value);

 private:
    PoolIdType id_;
    std::string name_;
    RedundanceAndPlaceMentPolicy rap_;
    uint64_t createTime_;
    uint64_t diskCapacity_;

    std::list<ZoneIdType> zoneList_;
};

class Zone {
 public:
    Zone() : id_(UNINITIALIZE_ID), name_(""), poolId_(UNINITIALIZE_ID) {}
    Zone(PoolIdType id, const std::string &name, PoolIdType poolId)
        : id_(id), name_(name), poolId_(poolId) {}

    ZoneIdType GetId() const { return id_; }

    std::string GetName() const { return name_; }

    PoolIdType GetPoolId() const { return poolId_; }

    void AddServer(ServerIdType id) { serverList_.push_back(id); }

    void RemoveServer(ServerIdType id) { serverList_.remove(id); }

    std::list<ServerIdType> GetServerList() const { return serverList_; }

    bool SerializeToString(std::string *value) const;

    bool ParseFromString(const std::string &value);

 private:
    ZoneIdType id_;
    std::string name_;
    PoolIdType poolId_;

    std::list<ServerIdType> serverList_;
};

class Server {
 public:
    Server()
        : id_(UNINITIALIZE_ID),
          hostName_(""),
          internalIp_(""),
          internalPort_(0),
          externalIp_(""),
          externalPort_(0),
          zoneId_(UNINITIALIZE_ID),
          poolId_(UNINITIALIZE_ID) {}
    Server(ServerIdType id, const std::string &hostName,
           const std::string &internalIp, uint32_t internalPort,
           const std::string &externalIp, uint32_t externalPort,
           ZoneIdType zoneId, PoolIdType poolId)
        : id_(id),
          hostName_(hostName),
          internalIp_(internalIp),
          internalPort_(internalPort),
          externalIp_(externalIp),
          externalPort_(externalPort),
          zoneId_(zoneId),
          poolId_(poolId) {}

    ServerIdType GetId() const { return id_; }

    std::string GetHostName() const { return hostName_; }

    std::string GetInternalIp() const { return internalIp_; }

    uint32_t GetInternalPort() const { return internalPort_; }

    std::string GetExternalIp() const { return externalIp_; }

    uint32_t GetExternalPort() const { return externalPort_; }

    ZoneIdType GetZoneId() const { return zoneId_; }

    PoolIdType GetPoolId() const { return poolId_; }

    void AddMetaServer(MetaServerIdType id) { metaserverList_.push_back(id); }

    void RemoveMetaServer(MetaServerIdType id) { metaserverList_.remove(id); }

    std::list<MetaServerIdType> GetMetaServerList() const {
        return metaserverList_;
    }

    bool SerializeToString(std::string *value) const;

    bool ParseFromString(const std::string &value);

 private:
    ServerIdType id_;
    std::string hostName_;
    std::string internalIp_;
    uint32_t internalPort_;
    std::string externalIp_;
    uint32_t externalPort_;
    ZoneIdType zoneId_;
    PoolIdType poolId_;

    std::list<MetaServerIdType> metaserverList_;
};

class MetaServerSpace {
 public:
    // for test only
    MetaServerSpace(uint64_t diskThreshold = 0, uint64_t diskUsed = 0,
                    uint64_t diskCopysetMinRequire = 0,
                    uint64_t memoryThreshold = 0, uint64_t memoryUsed = 0,
                    uint64_t memoryCopySetMinRequire = 0) {
        memoryThresholdByte_ = memoryThreshold;
        memoryCopySetMinRequireByte_ = memoryCopySetMinRequire;
        memoryUsedByte_ = memoryUsed;
        diskThresholdByte_ = diskThreshold;
        diskCopysetMinRequireByte_ = diskCopysetMinRequire;
        diskUsedByte_ = diskUsed;
    }

    explicit MetaServerSpace(heartbeat::MetaSeverSpaceStatus status) {
        SetSpaceStatus(status);
    }

    void SetDiskThreshold(uint64_t threshold) {
        diskThresholdByte_ = threshold;
    }
    uint64_t GetDiskThreshold() const { return diskThresholdByte_; }
    void SetDiskUsed(uint64_t diskUsed) { diskUsedByte_ = diskUsed; }
    uint64_t GetDiskUsed() const { return diskUsedByte_; }
    void SetDiskMinRequire(uint64_t require) {
        diskCopysetMinRequireByte_ = require;
    }
    uint64_t GetDiskMinRequire() const { return diskCopysetMinRequireByte_; }

    void SetMemoryThreshold(uint64_t threshold) {
        memoryThresholdByte_ = threshold;
    }
    uint64_t GetMemoryThreshold() const { return memoryThresholdByte_; }
    void SetMemoryUsed(uint64_t memoryUsed) { memoryUsedByte_ = memoryUsed; }
    uint64_t GetMemoryUsed() const { return memoryUsedByte_; }
    void SetMemoryMinRequire(uint64_t require) {
        memoryCopySetMinRequireByte_ = require;
    }
    uint64_t GetMemoryMinRequire() const {
        return memoryCopySetMinRequireByte_;
    }

    void SetSpaceStatus(heartbeat::MetaSeverSpaceStatus status) {
        diskThresholdByte_ = status.diskthresholdbyte();
        diskCopysetMinRequireByte_ = status.diskcopysetminrequirebyte();
        diskUsedByte_ = status.diskusedbyte();
        memoryThresholdByte_ = status.memorythresholdbyte();
        memoryCopySetMinRequireByte_ = status.memorycopysetminrequirebyte();
        memoryUsedByte_ = status.memoryusedbyte();
    }

    double GetResourceUseRatioPercent() {
        if (memoryCopySetMinRequireByte_ == 0) {
            if (diskThresholdByte_ != 0) {
                return 100.0 * diskUsedByte_ / diskThresholdByte_;
            } else {
                return 0;
            }
        } else {
            if (memoryThresholdByte_ != 0) {
                return 100.0 * memoryUsedByte_ / memoryThresholdByte_;
            } else {
                return 0;
            }
        }
    }

    bool IsMetaserverResourceAvailable() {
        if (diskThresholdByte_ < (diskCopysetMinRequireByte_ + diskUsedByte_)) {
            return false;
        }

        if (memoryCopySetMinRequireByte_ != 0 &&
            (memoryThresholdByte_ <
             (memoryCopySetMinRequireByte_ + memoryUsedByte_))) {
            return false;
        }

        return true;
    }

    // if memoryCopySetMinRequireByte_ equals 0, not consider the memory usage
    bool IsResourceOverload() {
        if (diskThresholdByte_ < diskUsedByte_) {
            return true;
        }

        if (memoryCopySetMinRequireByte_ != 0 &&
            memoryThresholdByte_ < memoryUsedByte_) {
            return true;
        }

        return false;
    }

 private:
    uint64_t memoryThresholdByte_;
    uint64_t memoryCopySetMinRequireByte_;
    uint64_t memoryUsedByte_;

    uint64_t diskThresholdByte_;
    uint64_t diskCopysetMinRequireByte_;
    uint64_t diskUsedByte_;
};

class MetaServer {
 public:
    MetaServer()
        : id_(UNINITIALIZE_ID),
          hostName_(""),
          token_(""),
          serverId_(UNINITIALIZE_ID),
          internalIp_(""),
          internalPort_(0),
          externalIp_(""),
          externalPort_(0),
          startUpTime_(0),
          onlineState_(OFFLINE),
          dirty_(false) {}

    MetaServer(MetaServerIdType id, const std::string &hostName,
               const std::string &token, ServerIdType serverId,
               const std::string &internalIp, uint32_t internalPort,
               const std::string &externalIp, uint32_t externalPort,
               OnlineState onlineState = OnlineState::OFFLINE)
        : id_(id),
          hostName_(hostName),
          token_(token),
          serverId_(serverId),
          internalIp_(internalIp),
          internalPort_(internalPort),
          externalIp_(externalIp),
          externalPort_(externalPort),
          startUpTime_(0),
          onlineState_(onlineState),
          dirty_(false) {}

    MetaServer(const MetaServer &v)
        : id_(v.id_),
          hostName_(v.hostName_),
          token_(v.token_),
          serverId_(v.serverId_),
          internalIp_(v.internalIp_),
          internalPort_(v.internalPort_),
          externalIp_(v.externalIp_),
          externalPort_(v.externalPort_),
          startUpTime_(v.startUpTime_),
          onlineState_(v.onlineState_),
          space_(v.space_),
          dirty_(v.dirty_) {}

    MetaServer &operator=(const MetaServer &v) {
        if (&v == this) {
            return *this;
        }
        id_ = v.id_;
        hostName_ = v.hostName_;
        token_ = v.token_;
        serverId_ = v.serverId_;
        internalIp_ = v.internalIp_;
        internalPort_ = v.internalPort_;
        externalIp_ = v.externalIp_;
        externalPort_ = v.externalPort_;
        startUpTime_ = v.startUpTime_;
        onlineState_ = v.onlineState_;
        space_ = v.space_;
        dirty_ = v.dirty_;
        return *this;
    }

    MetaServerIdType GetId() const { return id_; }

    std::string GetHostName() const { return hostName_; }

    std::string GetToken() const { return token_; }

    void SetToken(std::string token) { token_ = token; }

    void SetServerId(ServerIdType id) { serverId_ = id; }

    ServerIdType GetServerId() const { return serverId_; }

    std::string GetInternalIp() const { return internalIp_; }

    uint32_t GetInternalPort() const { return internalPort_; }

    void SetInternalIp(std::string internalIp) { internalIp_ = internalIp; }

    void SetInternalPort(uint32_t internalPort) {
        internalPort_ = internalPort;
    }

    std::string GetExternalIp() const { return externalIp_; }

    uint32_t GetExternalPort() const { return externalPort_; }

    void SetStartUpTime(uint64_t time) { startUpTime_ = time; }

    uint64_t GetStartUpTime() const { return startUpTime_; }

    void SetOnlineState(OnlineState state) { onlineState_ = state; }

    OnlineState GetOnlineState() const { return onlineState_; }

    void SetMetaServerSpace(const MetaServerSpace &space) { space_ = space; }

    MetaServerSpace GetMetaServerSpace() const { return space_; }

    bool GetDirtyFlag() const { return dirty_; }

    void SetDirtyFlag(bool dirty) { dirty_ = dirty; }

    ::curve::common::RWLock &GetRWLockRef() const { return mutex_; }

    bool SerializeToString(std::string *value) const;

    bool ParseFromString(const std::string &value);

 private:
    MetaServerIdType id_;
    std::string hostName_;
    std::string token_;
    ServerIdType serverId_;
    std::string internalIp_;
    uint32_t internalPort_;
    std::string externalIp_;
    uint32_t externalPort_;
    uint64_t startUpTime_;
    OnlineState onlineState_;  // 0:online、1: offline
    MetaServerSpace space_;
    bool dirty_;
    mutable ::curve::common::RWLock mutex_;
};

typedef std::pair<PoolIdType, CopySetIdType> CopySetKey;

struct CopysetIdInfo {
    PoolIdType poolId;
    CopySetIdType copySetId;
};

class CopySetInfo {
 public:
    CopySetInfo()
        : poolId_(UNINITIALIZE_ID),
          copySetId_(UNINITIALIZE_ID),
          leader_(UNINITIALIZE_ID),
          epoch_(0),
          partitionNum_(0),
          hasCandidate_(false),
          candidate_(UNINITIALIZE_ID),
          dirty_(false),
          available_(true) {}

    CopySetInfo(PoolIdType poolId, CopySetIdType id)
        : poolId_(poolId),
          copySetId_(id),
          leader_(UNINITIALIZE_ID),
          epoch_(0),
          partitionNum_(0),
          hasCandidate_(false),
          candidate_(UNINITIALIZE_ID),
          dirty_(false),
          available_(true) {}

    CopySetInfo(const CopySetInfo &v)
        : poolId_(v.poolId_),
          copySetId_(v.copySetId_),
          leader_(v.leader_),
          epoch_(v.epoch_),
          peers_(v.peers_),
          partitionNum_(v.partitionNum_),
          hasCandidate_(v.hasCandidate_),
          candidate_(v.candidate_),
          dirty_(v.dirty_),
          available_(v.available_) {}

    CopySetInfo &operator=(const CopySetInfo &v) {
        if (&v == this) {
            return *this;
        }
        poolId_ = v.poolId_;
        copySetId_ = v.copySetId_;
        leader_ = v.leader_;
        epoch_ = v.epoch_;
        peers_ = v.peers_;
        partitionNum_ = v.partitionNum_;
        hasCandidate_ = v.hasCandidate_;
        candidate_ = v.candidate_;
        dirty_ = v.dirty_;
        available_ = v.available_;
        return *this;
    }

    void SetPoolId(PoolIdType poolId) { poolId_ = poolId; }

    PoolIdType GetPoolId() const { return poolId_; }

    void SetCopySetId(CopySetIdType copySetId) { copySetId_ = copySetId; }

    CopySetIdType GetId() const { return copySetId_; }

    void SetEpoch(EpochType epoch) { epoch_ = epoch; }

    EpochType GetEpoch() const { return epoch_; }

    MetaServerIdType GetLeader() const { return leader_; }

    void SetLeader(MetaServerIdType leader) { leader_ = leader; }

    CopySetKey GetCopySetKey() const { return CopySetKey(poolId_, copySetId_); }

    std::set<MetaServerIdType> GetCopySetMembers() const { return peers_; }

    std::string GetCopySetMembersStr() const;

    void SetCopySetMembers(const std::set<MetaServerIdType> &peers) {
        peers_ = peers;
    }

    bool HasMember(MetaServerIdType peer) const {
        return peers_.count(peer) > 0;
    }

    bool SetCopySetMembersByJson(const std::string &jsonStr);

    uint64_t GetPartitionNum() const { return partitionNum_; }

    void SetPartitionNum(u_int64_t number) { partitionNum_ = number; }

    void AddPartitionNum() { partitionNum_ += 1; }

    void ReducePartitionNum() { partitionNum_ -= 1; }

    bool HasCandidate() const { return hasCandidate_; }

    void SetCandidate(MetaServerIdType id) {
        hasCandidate_ = true;
        candidate_ = id;
    }

    MetaServerIdType GetCandidate() const {
        if (hasCandidate_) {
            return candidate_;
        } else {
            return UNINITIALIZE_ID;
        }
    }

    void ClearCandidate() { hasCandidate_ = false; }

    bool GetDirtyFlag() const { return dirty_; }

    void SetDirtyFlag(bool dirty) { dirty_ = dirty; }

    bool IsAvailable() const { return available_; }

    void SetAvailableFlag(bool aval) { available_ = aval; }

    ::curve::common::RWLock &GetRWLockRef() const { return mutex_; }

    bool SerializeToString(std::string *value) const;

    bool ParseFromString(const std::string &value);

    void AddPartitionId(const PartitionIdType &id) { partitionIds_.insert(id); }

    const std::set<PartitionIdType> &GetPartitionIds() const {
        return partitionIds_;
    }

 private:
    PoolIdType poolId_;
    CopySetIdType copySetId_;
    MetaServerIdType leader_;
    EpochType epoch_;
    std::set<MetaServerIdType> peers_;
    // TODO(chengyi01): replace it whith partitionIds.size()
    uint64_t partitionNum_;
    std::set<PartitionIdType> partitionIds_;
    bool hasCandidate_;
    MetaServerIdType candidate_;

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
     * @brief metaserver read/write lock, for protecting concurrent
     *        read/write on the copyset
     */
    mutable ::curve::common::RWLock mutex_;
};

struct PartitionStatistic {
    common::PartitionStatus status;
    uint64_t inodeNum;
    uint64_t dentryNum;
};

class Partition {
 public:
    Partition()
        : fsId_(UNINITIALIZE_ID),
          poolId_(UNINITIALIZE_ID),
          copySetId_(UNINITIALIZE_ID),
          partitionId_(UNINITIALIZE_ID),
          idStart_(0),
          idEnd_(0),
          txId_(0),
          status_(PartitionStatus::READWRITE),
          inodeNum_(UNINITIALIZE_COUNT),
          dentryNum_(UNINITIALIZE_COUNT) {}

    Partition(FsIdType fsId, PoolIdType poolId, CopySetIdType copySetId,
              PartitionIdType partitionId, uint64_t idStart, uint64_t idEnd)
        : fsId_(fsId),
          poolId_(poolId),
          copySetId_(copySetId),
          partitionId_(partitionId),
          idStart_(idStart),
          idEnd_(idEnd),
          txId_(0),
          status_(PartitionStatus::READWRITE),
          inodeNum_(UNINITIALIZE_COUNT),
          dentryNum_(UNINITIALIZE_COUNT) {}

    Partition(const Partition &v)
        : fsId_(v.fsId_),
          poolId_(v.poolId_),
          copySetId_(v.copySetId_),
          partitionId_(v.partitionId_),
          idStart_(v.idStart_),
          idEnd_(v.idEnd_),
          txId_(v.txId_),
          status_(v.status_),
          inodeNum_(v.inodeNum_),
          dentryNum_(v.dentryNum_) {}

    Partition &operator=(const Partition &v) {
        if (&v == this) {
            return *this;
        }
        fsId_ = v.fsId_;
        poolId_ = v.poolId_;
        copySetId_ = v.copySetId_;
        partitionId_ = v.partitionId_;
        idStart_ = v.idStart_;
        idEnd_ = v.idEnd_;
        txId_ = v.txId_;
        status_ = v.status_;
        inodeNum_ = v.inodeNum_;
        dentryNum_ = v.dentryNum_;
        return *this;
    }

    explicit Partition(const common::PartitionInfo &v) {
        fsId_ = v.fsid();
        poolId_ = v.poolid();
        copySetId_ = v.copysetid();
        partitionId_ = v.partitionid();
        idStart_ = v.start();
        idEnd_ = v.end();
        txId_ = v.txid();
        status_ = v.status();
        inodeNum_ = v.has_inodenum() ? v.inodenum() : UNINITIALIZE_COUNT;
        dentryNum_ = v.has_dentrynum() ? v.dentrynum() : UNINITIALIZE_COUNT;
    }

    explicit operator common::PartitionInfo() const {
        common::PartitionInfo partition;
        partition.set_fsid(fsId_);
        partition.set_poolid(poolId_);
        partition.set_copysetid(copySetId_);
        partition.set_start(idStart_);
        partition.set_end(idEnd_);
        partition.set_txid(txId_);
        partition.set_status(status_);
        partition.set_inodenum(inodeNum_);
        partition.set_dentrynum(dentryNum_);
        return partition;
    }

    FsIdType GetFsId() const { return fsId_; }

    void SetFsId(FsIdType fsId) { fsId_ = fsId; }

    PoolIdType GetPoolId() const { return poolId_; }

    void SetPoolId(PoolIdType poolId) { poolId_ = poolId; }

    CopySetIdType GetCopySetId() const { return copySetId_; }

    void SetCopySetId(CopySetIdType copySetId) { copySetId_ = copySetId; }

    PartitionIdType GetPartitionId() const { return partitionId_; }

    void SetPartitionId(PartitionIdType partitionId) {
        partitionId_ = partitionId;
    }

    uint64_t GetIdStart() const { return idStart_; }

    void SetIdStart(uint64_t idStart) { idStart_ = idStart; }

    uint64_t GetIdEnd() const { return idEnd_; }

    void SetIdEnd(uint64_t idEnd) { idEnd_ = idEnd; }

    uint64_t GetTxId() const { return txId_; }

    void SetTxId(uint64_t txId) { txId_ = txId; }

    PartitionStatus GetStatus() const { return status_; }

    void SetStatus(PartitionStatus status) { status_ = status; }

    uint64_t GetInodeNum() const { return inodeNum_; }

    void SetInodeNum(uint64_t inodeNum) { inodeNum_ = inodeNum; }

    uint64_t GetDentryNum() const { return dentryNum_; }

    void SetDentryNum(uint64_t dentryNum) { dentryNum_ = dentryNum; }

    ::curve::common::RWLock &GetRWLockRef() const { return mutex_; }

    bool SerializeToString(std::string *value) const;

    bool ParseFromString(const std::string &value);

    common::PartitionInfo ToPartitionInfo();

 private:
    FsIdType fsId_;
    PoolIdType poolId_;
    CopySetIdType copySetId_;
    PartitionIdType partitionId_;
    uint64_t idStart_;
    uint64_t idEnd_;
    uint64_t txId_;
    common::PartitionStatus status_;
    uint64_t inodeNum_;
    uint64_t dentryNum_;
    mutable ::curve::common::RWLock mutex_;
};

}  // namespace topology
}  // namespace mds
}  // namespace curvefs

#endif  // CURVEFS_SRC_MDS_TOPOLOGY_TOPOLOGY_ITEM_H_
