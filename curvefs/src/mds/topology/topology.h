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
#ifndef CURVEFS_SRC_MDS_TOPOLOGY_TOPOLOGY_H_
#define CURVEFS_SRC_MDS_TOPOLOGY_TOPOLOGY_H_

#include <list>
#include <map>
#include <memory>
#include <random>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include "curvefs/proto/mds.pb.h"
#include "curvefs/proto/topology.pb.h"
#include "curvefs/src/mds/topology/topology_id_generator.h"
#include "curvefs/src/mds/topology/topology_storge.h"
#include "curvefs/src/mds/topology/topology_token_generator.h"
#include "src/common/concurrent/concurrent.h"
#include "src/common/concurrent/rw_lock.h"
#include "src/common/interruptible_sleeper.h"

namespace curvefs {
namespace mds {
namespace topology {

using RWLock = ::curve::common::BthreadRWLock;
using ::curve::common::ReadLockGuard;
using ::curve::common::WriteLockGuard;
using ::curve::common::InterruptibleSleeper;

using PartitionFilter = std::function<bool(const Partition &)>;
using CopySetFilter = std::function<bool(const CopySetInfo &)>;
using MetaServerFilter = std::function<bool(const MetaServer &)>;
using ServerFilter = std::function<bool(const Server &)>;
using ZoneFilter = std::function<bool(const Zone &)>;
using PoolFilter = std::function<bool(const Pool &)>;

class Topology {
 public:
    Topology() {}
    virtual ~Topology() {}

    virtual bool GetClusterInfo(ClusterInformation *info) = 0;

    virtual PoolIdType AllocatePoolId() = 0;
    virtual ZoneIdType AllocateZoneId() = 0;
    virtual ServerIdType AllocateServerId() = 0;
    virtual MetaServerIdType AllocateMetaServerId() = 0;
    virtual CopySetIdType AllocateCopySetId(PoolIdType poolId) = 0;
    virtual PartitionIdType AllocatePartitionId() = 0;

    virtual std::string AllocateToken() = 0;

    virtual TopoStatusCode AddPool(const Pool &data) = 0;
    virtual TopoStatusCode AddZone(const Zone &data) = 0;
    virtual TopoStatusCode AddServer(const Server &data) = 0;
    virtual TopoStatusCode AddMetaServer(const MetaServer &data) = 0;
    virtual TopoStatusCode AddCopySet(const CopySetInfo &data) = 0;
    virtual TopoStatusCode AddPartition(const Partition &data) = 0;

    virtual TopoStatusCode RemovePool(PoolIdType id) = 0;
    virtual TopoStatusCode RemoveZone(ZoneIdType id) = 0;
    virtual TopoStatusCode RemoveServer(ServerIdType id) = 0;
    virtual TopoStatusCode RemoveMetaServer(MetaServerIdType id) = 0;
    virtual TopoStatusCode RemoveCopySet(CopySetKey key) = 0;
    virtual TopoStatusCode RemovePartition(PartitionIdType id) = 0;

    virtual TopoStatusCode UpdatePool(const Pool &data) = 0;
    virtual TopoStatusCode UpdateZone(const Zone &data) = 0;
    virtual TopoStatusCode UpdateServer(const Server &data) = 0;
    virtual TopoStatusCode UpdateMetaServerOnlineState(
        const OnlineState &onlineState, MetaServerIdType id) = 0;
    virtual TopoStatusCode UpdateMetaServerSpace(const MetaServerSpace &space,
                                                 MetaServerIdType id) = 0;
    virtual TopoStatusCode UpdateMetaServerStartUpTime(uint64_t time,
                                                       MetaServerIdType id) = 0;
    virtual TopoStatusCode UpdateCopySetTopo(const CopySetInfo &data) = 0;
    virtual TopoStatusCode UpdatePartition(const Partition &data) = 0;
    virtual TopoStatusCode UpdatePartitionStatistic(
        uint32_t partitionId, PartitionStatistic statistic) = 0;
    virtual TopoStatusCode UpdatePartitionTxIds(
        std::vector<PartitionTxId> txIds) = 0;

    virtual TopoStatusCode SetCopySetAvalFlag(const CopySetKey &key,
                                              bool aval) = 0;

    virtual PoolIdType FindPool(const std::string &poolName) const = 0;
    virtual ZoneIdType FindZone(const std::string &zoneName,
                                const std::string &poolName) const = 0;
    virtual ZoneIdType FindZone(const std::string &zoneName,
                                PoolIdType poolid) const = 0;
    virtual ServerIdType FindServerByHostName(
        const std::string &hostName) const = 0;
    virtual ServerIdType FindServerByHostIpPort(const std::string &hostIp,
                                                uint32_t port) const = 0;
    virtual bool GetPool(PoolIdType poolId, Pool *out) const = 0;
    virtual bool GetZone(ZoneIdType zoneId, Zone *out) const = 0;
    virtual bool GetServer(ServerIdType serverId, Server *out) const = 0;
    virtual bool GetMetaServer(MetaServerIdType metaserverId,
                               MetaServer *out) const = 0;
    virtual bool GetMetaServer(const std::string &hostIp, uint32_t port,
                               MetaServer *out) const = 0;
    virtual bool GetCopySet(CopySetKey key, CopySetInfo *out) const = 0;
    virtual bool GetCopysetOfPartition(PartitionIdType id,
                                       CopySetInfo *out) const = 0;
    virtual bool GetAvailableCopyset(CopySetInfo *out) const = 0;
    virtual bool GetPartition(PartitionIdType partitionId, Partition *out) = 0;

    virtual bool GetPool(const std::string &poolName, Pool *out) const = 0;
    virtual bool GetZone(const std::string &zoneName,
                         const std::string &poolName, Zone *out) const = 0;
    virtual bool GetZone(const std::string &zoneName, PoolIdType poolId,
                         Zone *out) const = 0;
    virtual bool GetServerByHostName(const std::string &hostName,
                                     Server *out) const = 0;
    virtual bool GetServerByHostIpPort(const std::string &hostIp, uint32_t port,
                                       Server *out) const = 0;

    virtual std::vector<MetaServerIdType> GetMetaServerInCluster(
        MetaServerFilter filter = [](const MetaServer &) {
            return true;
        }) const = 0;
    virtual std::vector<ServerIdType> GetServerInCluster(
        ServerFilter filter = [](const Server &) { return true; }) const = 0;
    virtual std::vector<ZoneIdType> GetZoneInCluster(
        ZoneFilter filter = [](const Zone &) { return true; }) const = 0;
    virtual std::vector<PoolIdType> GetPoolInCluster(
        PoolFilter filter = [](const Pool &) { return true; }) const = 0;

    // get metaserver list
    virtual std::list<MetaServerIdType> GetMetaServerInServer(
        ServerIdType id, MetaServerFilter filter = [](const MetaServer &) {
            return true;
        }) const = 0;
    virtual std::list<MetaServerIdType> GetMetaServerInZone(
        ZoneIdType id, MetaServerFilter filter = [](const MetaServer &) {
            return true;
        }) const = 0;
    // get server list
    virtual std::list<ServerIdType> GetServerInZone(
        ZoneIdType id,
        ServerFilter filter = [](const Server &) { return true; }) const = 0;

    // get zone list
    virtual std::list<ZoneIdType> GetZoneInPool(
        PoolIdType id,
        ZoneFilter filter = [](const Zone &) { return true; }) const = 0;

    // get copyset list
    virtual std::vector<CopySetIdType> GetCopySetsInPool(
        PoolIdType poolId, CopySetFilter filter = [](const CopySetInfo &) {
            return true;
        }) const = 0;

    virtual std::vector<CopySetInfo> GetCopySetInfosInPool(
        PoolIdType poolId, CopySetFilter filter = [](const CopySetInfo &) {
            return true;
        }) const = 0;

    // get partition list
    virtual std::list<Partition> GetPartitionOfFs(FsIdType id,
                                                  PartitionFilter filter =
                                                      [](const Partition &) {
                                                          return true;
                                                      }) const = 0;
    virtual std::list<Partition> GetPartitionInfosInPool(
        PoolIdType poolId, PartitionFilter filter = [](const Partition &) {
            return true;
        }) const = 0;

    // choose randomly
    virtual TopoStatusCode ChooseSinglePoolRandom(PoolIdType *out,
        const std::set<PoolIdType> &unavailablePools) const = 0;

    virtual TopoStatusCode ChooseZonesInPool(PoolIdType poolId,
                                  std::set<ZoneIdType> *zones,
                                  const std::set<ZoneIdType> &unavailableZones,
                                  int count) const = 0;
    virtual TopoStatusCode ChooseSingleMetaServerInZone(ZoneIdType zoneId,
                        MetaServerIdType *metaServerId) const = 0;
    virtual uint32_t GetPartitionNumberOfFs(FsIdType fsId) = 0;

    virtual std::vector<CopySetInfo> ListCopysetInfo() const = 0;
};

class TopologyImpl : public Topology {
 public:
    TopologyImpl(std::shared_ptr<TopologyIdGenerator> idGenerator,
                 std::shared_ptr<TopologyTokenGenerator> tokenGenerator,
                 std::shared_ptr<TopologyStorage> storage)
        : idGenerator_(idGenerator),
          tokenGenerator_(tokenGenerator),
          storage_(storage),
          isStop_(true) {}

    ~TopologyImpl() { Stop(); }

    TopoStatusCode Init(const TopologyOption &option);

    int Run();
    int Stop();

    bool GetClusterInfo(ClusterInformation *info) override;

    PoolIdType AllocatePoolId() override;
    ZoneIdType AllocateZoneId() override;
    ServerIdType AllocateServerId() override;
    MetaServerIdType AllocateMetaServerId() override;
    CopySetIdType AllocateCopySetId(PoolIdType poolId) override;
    PartitionIdType AllocatePartitionId() override;

    std::string AllocateToken() override;

    TopoStatusCode AddPool(const Pool &data) override;
    TopoStatusCode AddZone(const Zone &data) override;
    TopoStatusCode AddServer(const Server &data) override;
    TopoStatusCode AddMetaServer(const MetaServer &data) override;
    TopoStatusCode AddCopySet(const CopySetInfo &data) override;
    TopoStatusCode AddPartition(const Partition &data) override;

    TopoStatusCode RemovePool(PoolIdType id) override;
    TopoStatusCode RemoveZone(ZoneIdType id) override;
    TopoStatusCode RemoveServer(ServerIdType id) override;
    TopoStatusCode RemoveMetaServer(MetaServerIdType id) override;
    TopoStatusCode RemoveCopySet(CopySetKey key) override;
    TopoStatusCode RemovePartition(PartitionIdType id) override;

    TopoStatusCode UpdatePool(const Pool &data) override;
    TopoStatusCode UpdateZone(const Zone &data) override;
    TopoStatusCode UpdateServer(const Server &data) override;
    TopoStatusCode UpdateMetaServerOnlineState(const OnlineState &onlineState,
                                               MetaServerIdType id) override;
    TopoStatusCode UpdateMetaServerSpace(const MetaServerSpace &space,
                                         MetaServerIdType id) override;
    TopoStatusCode UpdateMetaServerStartUpTime(uint64_t time,
                                               MetaServerIdType id) override;
    TopoStatusCode UpdateCopySetTopo(const CopySetInfo &data) override;
    TopoStatusCode SetCopySetAvalFlag(const CopySetKey &key,
                                      bool aval) override;
    TopoStatusCode UpdatePartition(const Partition &data) override;
    TopoStatusCode UpdatePartitionStatistic(
        uint32_t partitionId, PartitionStatistic statistic) override;
    TopoStatusCode UpdatePartitionTxIds(
        std::vector<PartitionTxId> txIds) override;

    PoolIdType FindPool(const std::string &poolName) const override;
    ZoneIdType FindZone(const std::string &zoneName,
                        const std::string &poolName) const override;
    ZoneIdType FindZone(const std::string &zoneName,
                        PoolIdType poolid) const override;
    ServerIdType FindServerByHostName(
        const std::string &hostName) const override;
    ServerIdType FindServerByHostIpPort(const std::string &hostIp,
                                        uint32_t port) const override;
    bool GetPool(PoolIdType poolId, Pool *out) const override;
    bool GetZone(ZoneIdType zoneId, Zone *out) const override;
    bool GetServer(ServerIdType serverId, Server *out) const override;
    bool GetMetaServer(MetaServerIdType metaserverId,
                       MetaServer *out) const override;
    bool GetMetaServer(const std::string &hostIp, uint32_t port,
                       MetaServer *out) const override;
    bool GetCopySet(CopySetKey key, CopySetInfo *out) const override;
    bool GetCopysetOfPartition(PartitionIdType id,
                               CopySetInfo *out) const override;
    bool GetAvailableCopyset(CopySetInfo *out) const override;
    bool GetPartition(PartitionIdType partitionId, Partition *out) override;

    bool GetPool(const std::string &poolName, Pool *out) const override {
        return GetPool(FindPool(poolName), out);
    }
    bool GetZone(const std::string &zoneName, const std::string &poolName,
                 Zone *out) const override {
        return GetZone(FindZone(zoneName, poolName), out);
    }
    bool GetZone(const std::string &zoneName, PoolIdType poolId,
                 Zone *out) const override {
        return GetZone(FindZone(zoneName, poolId), out);
    }
    bool GetServerByHostName(const std::string &hostName,
                             Server *out) const override {
        return GetServer(FindServerByHostName(hostName), out);
    }
    bool GetServerByHostIpPort(const std::string &hostIp, uint32_t port,
                               Server *out) const override {
        return GetServer(FindServerByHostIpPort(hostIp, port), out);
    }

    std::vector<MetaServerIdType> GetMetaServerInCluster(
        MetaServerFilter filter = [](const MetaServer &) {
            return true;
        }) const override;

    std::vector<ServerIdType> GetServerInCluster(ServerFilter filter =
                                                     [](const Server &) {
                                                         return true;
                                                     }) const override;

    std::vector<ZoneIdType> GetZoneInCluster(
        ZoneFilter filter = [](const Zone &) { return true; }) const override;

    std::vector<PoolIdType> GetPoolInCluster(
        PoolFilter filter = [](const Pool &) { return true; }) const override;

    // get metasever list
    std::list<MetaServerIdType> GetMetaServerInServer(
        ServerIdType id, MetaServerFilter filter = [](const MetaServer &) {
            return true;
        }) const override;
    std::list<MetaServerIdType> GetMetaServerInZone(ZoneIdType id,
                                                    MetaServerFilter filter =
                                                        [](const MetaServer &) {
                                                            return true;
                                                        }) const override;

    // get server list
    std::list<ServerIdType> GetServerInZone(ZoneIdType id,
                                            ServerFilter filter =
                                                [](const Server &) {
                                                    return true;
                                                }) const override;

    // get zone list
    std::list<ZoneIdType> GetZoneInPool(PoolIdType id,
                                        ZoneFilter filter = [](const Zone &) {
                                            return true;
                                        }) const override;

    // get copyset list
    std::vector<CopySetIdType> GetCopySetsInPool(PoolIdType poolId,
                                                 CopySetFilter filter =
                                                     [](const CopySetInfo &) {
                                                         return true;
                                                     }) const override;

    std::vector<CopySetInfo> GetCopySetInfosInPool(PoolIdType poolId,
                                                   CopySetFilter filter =
                                                       [](const CopySetInfo &) {
                                                           return true;
                                                       }) const override;

    // get partition list
    std::list<Partition> GetPartitionOfFs(FsIdType id,
                                          PartitionFilter filter =
                                              [](const Partition &) {
                                                  return true;
                                              }) const override;

    std::list<Partition> GetPartitionInfosInPool(PoolIdType poolId,
                                                 PartitionFilter filter =
                                                     [](const Partition &) {
                                                         return true;
                                                     }) const override;

    // choose random
    TopoStatusCode ChooseSinglePoolRandom(PoolIdType *out,
        const std::set<PoolIdType> &unavailablePools) const override;
    TopoStatusCode ChooseZonesInPool(PoolIdType poolId,
        std::set<ZoneIdType> *zones,
        const std::set<ZoneIdType> &unavailableZones,
        int count) const override;
    TopoStatusCode ChooseSingleMetaServerInZone(ZoneIdType zoneId,
                            MetaServerIdType *metaServerId) const override;
    uint32_t GetPartitionNumberOfFs(FsIdType fsId);

    TopoStatusCode GetPoolIdByMetaserverId(MetaServerIdType id,
                                           PoolIdType *poolIdOut);

    TopoStatusCode GetPoolIdByServerId(ServerIdType id, PoolIdType *poolIdOut);

    std::vector<CopySetInfo> ListCopysetInfo() const override;

 private:
    TopoStatusCode LoadClusterInfo();

    void BackEndFunc();

    void FlushCopySetToStorage();

    void FlushMetaServerToStorage();

    int GetOneRandomNumber(int start, int end) const;

 private:
    std::unordered_map<PoolIdType, Pool> poolMap_;
    std::unordered_map<ZoneIdType, Zone> zoneMap_;
    std::unordered_map<ServerIdType, Server> serverMap_;
    std::unordered_map<MetaServerIdType, MetaServer> metaServerMap_;
    std::map<CopySetKey, CopySetInfo> copySetMap_;
    std::unordered_map<PartitionIdType, Partition> partitionMap_;

    // cluster info
    ClusterInformation clusterInfo_;

    std::shared_ptr<TopologyIdGenerator> idGenerator_;
    std::shared_ptr<TopologyTokenGenerator> tokenGenerator_;
    std::shared_ptr<TopologyStorage> storage_;

    // fetch lock in the order below to avoid deadlock
    mutable RWLock poolMutex_;
    mutable RWLock zoneMutex_;
    mutable RWLock serverMutex_;
    mutable RWLock metaServerMutex_;
    mutable RWLock copySetMutex_;
    mutable RWLock partitionMutex_;

    TopologyOption option_;
    curve::common::Thread backEndThread_;
    curve::common::Atomic<bool> isStop_;
    InterruptibleSleeper sleeper_;
};

}  // namespace topology
}  // namespace mds
}  // namespace curvefs

#endif  // CURVEFS_SRC_MDS_TOPOLOGY_TOPOLOGY_H_
