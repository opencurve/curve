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
 * Created Date: 2021-08-25
 * Author: wanghai01
 */

#include "curvefs/src/mds/topology/topology.h"
#include <glog/logging.h>
#include <chrono>  // NOLINT
#include <utility>
#include "src/common/timeutility.h"
#include "src/common/uuid.h"

namespace curvefs {
namespace mds {
namespace topology {

using ::curve::common::UUIDGenerator;

PoolIdType TopologyImpl::AllocatePoolId() { return idGenerator_->GenPoolId(); }

ZoneIdType TopologyImpl::AllocateZoneId() { return idGenerator_->GenZoneId(); }

ServerIdType TopologyImpl::AllocateServerId() {
    return idGenerator_->GenServerId();
}

MetaServerIdType TopologyImpl::AllocateMetaServerId() {
    return idGenerator_->GenMetaServerId();
}

CopySetIdType TopologyImpl::AllocateCopySetId(PoolIdType poolId) {
    return idGenerator_->GenCopySetId(poolId);
}

PartitionIdType TopologyImpl::AllocatePartitionId() {
    return idGenerator_->GenPartitionId();
}

std::string TopologyImpl::AllocateToken() {
    return tokenGenerator_->GenToken();
}

TopoStatusCode TopologyImpl::AddPool(const Pool &data) {
    WriteLockGuard wlockPool(poolMutex_);
    if (poolMap_.find(data.GetId()) == poolMap_.end()) {
        if (!storage_->StoragePool(data)) {
            return TopoStatusCode::TOPO_STORGE_FAIL;
        }
        poolMap_[data.GetId()] = data;
        return TopoStatusCode::TOPO_OK;
    } else {
        return TopoStatusCode::TOPO_ID_DUPLICATED;
    }
}

TopoStatusCode TopologyImpl::AddZone(const Zone &data) {
    ReadLockGuard rlockPool(poolMutex_);
    WriteLockGuard wlockZone(zoneMutex_);
    auto it = poolMap_.find(data.GetPoolId());
    if (it != poolMap_.end()) {
        if (zoneMap_.find(data.GetId()) == zoneMap_.end()) {
            if (!storage_->StorageZone(data)) {
                return TopoStatusCode::TOPO_STORGE_FAIL;
            }
            it->second.AddZone(data.GetId());
            zoneMap_[data.GetId()] = data;
            return TopoStatusCode::TOPO_OK;
        } else {
            return TopoStatusCode::TOPO_ID_DUPLICATED;
        }
    } else {
        return TopoStatusCode::TOPO_POOL_NOT_FOUND;
    }
}

TopoStatusCode TopologyImpl::AddServer(const Server &data) {
    ReadLockGuard rlockZone(zoneMutex_);
    WriteLockGuard wlockServer(serverMutex_);
    auto it = zoneMap_.find(data.GetZoneId());
    if (it != zoneMap_.end()) {
        if (serverMap_.find(data.GetId()) == serverMap_.end()) {
            if (!storage_->StorageServer(data)) {
                return TopoStatusCode::TOPO_STORGE_FAIL;
            }
            it->second.AddServer(data.GetId());
            serverMap_[data.GetId()] = data;
            return TopoStatusCode::TOPO_OK;
        } else {
            return TopoStatusCode::TOPO_ID_DUPLICATED;
        }
    } else {
        return TopoStatusCode::TOPO_ZONE_NOT_FOUND;
    }
}

TopoStatusCode TopologyImpl::AddMetaServer(const MetaServer &data) {
    // find the pool that the meatserver belongs to
    PoolIdType poolId = UNINTIALIZE_ID;
    TopoStatusCode ret = GetPoolIdByServerId(data.GetServerId(), &poolId);
    if (ret != TopoStatusCode::TOPO_OK) {
        return ret;
    }

    // fetch lock on pool, server and chunkserver
    WriteLockGuard wlockPool(poolMutex_);
    uint64_t metaserverCapacity = 0;
    {
        ReadLockGuard rlockServer(serverMutex_);
        WriteLockGuard wlockMetaServer(metaServerMutex_);
        auto it = serverMap_.find(data.GetServerId());
        if (it != serverMap_.end()) {
            if (metaServerMap_.find(data.GetId()) == metaServerMap_.end()) {
                if (!storage_->StorageMetaServer(data)) {
                    return TopoStatusCode::TOPO_STORGE_FAIL;
                }
                it->second.AddMetaServer(data.GetId());
                metaServerMap_[data.GetId()] = data;
                metaserverCapacity =
                    data.GetMetaServerSpace().GetDiskCapacity();
            } else {
                return TopoStatusCode::TOPO_ID_DUPLICATED;
            }
        } else {
            return TopoStatusCode::TOPO_SERVER_NOT_FOUND;
        }
    }

    // update pool
    auto it = poolMap_.find(poolId);
    if (it != poolMap_.end()) {
        uint64_t totalCapacity = it->second.GetDiskCapacity();
        totalCapacity += metaserverCapacity;
        it->second.SetDiskCapacity(totalCapacity);
    } else {
        return TopoStatusCode::TOPO_POOL_NOT_FOUND;
    }

    return TopoStatusCode::TOPO_OK;
}

TopoStatusCode TopologyImpl::RemovePool(PoolIdType id) {
    WriteLockGuard wlockPool(poolMutex_);
    auto it = poolMap_.find(id);
    if (it != poolMap_.end()) {
        if (it->second.GetZoneList().size() != 0) {
            return TopoStatusCode::TOPO_CANNOT_REMOVE_WHEN_NOT_EMPTY;
        }
        // TODO(wanghai): remove copysets and partition of this pool
        if (!storage_->DeletePool(id)) {
            return TopoStatusCode::TOPO_STORGE_FAIL;
        }
        poolMap_.erase(it);
        return TopoStatusCode::TOPO_OK;
    } else {
        return TopoStatusCode::TOPO_POOL_NOT_FOUND;
    }
}

TopoStatusCode TopologyImpl::RemoveZone(ZoneIdType id) {
    WriteLockGuard wlockPool(poolMutex_);
    WriteLockGuard wlockZone(zoneMutex_);
    auto it = zoneMap_.find(id);
    if (it != zoneMap_.end()) {
        if (it->second.GetServerList().size() != 0) {
            return TopoStatusCode::TOPO_CANNOT_REMOVE_WHEN_NOT_EMPTY;
        }
        if (!storage_->DeleteZone(id)) {
            return TopoStatusCode::TOPO_STORGE_FAIL;
        }
        auto ix = poolMap_.find(it->second.GetPoolId());
        if (ix != poolMap_.end()) {
            ix->second.RemoveZone(id);
        }
        zoneMap_.erase(it);
        return TopoStatusCode::TOPO_OK;
    } else {
        return TopoStatusCode::TOPO_ZONE_NOT_FOUND;
    }
}

TopoStatusCode TopologyImpl::RemoveServer(ServerIdType id) {
    WriteLockGuard wlockZone(zoneMutex_);
    WriteLockGuard wlockServer(serverMutex_);
    auto it = serverMap_.find(id);
    if (it != serverMap_.end()) {
        if (it->second.GetMetaServerList().size() != 0) {
            return TopoStatusCode::TOPO_CANNOT_REMOVE_WHEN_NOT_EMPTY;
        }
        if (!storage_->DeleteServer(id)) {
            return TopoStatusCode::TOPO_STORGE_FAIL;
        }
        auto ix = zoneMap_.find(it->second.GetZoneId());
        if (ix != zoneMap_.end()) {
            ix->second.RemoveServer(id);
        }
        serverMap_.erase(it);
        return TopoStatusCode::TOPO_OK;
    } else {
        return TopoStatusCode::TOPO_SERVER_NOT_FOUND;
    }
}

TopoStatusCode TopologyImpl::RemoveMetaServer(MetaServerIdType id) {
    WriteLockGuard wlockServer(serverMutex_);
    WriteLockGuard wlockMetaServer(metaServerMutex_);
    auto it = metaServerMap_.find(id);
    if (it != metaServerMap_.end()) {
        if (!storage_->DeleteMetaServer(id)) {
            return TopoStatusCode::TOPO_STORGE_FAIL;
        }
        auto ix = serverMap_.find(it->second.GetServerId());
        if (ix != serverMap_.end()) {
            ix->second.RemoveMetaServer(id);
        }
        metaServerMap_.erase(it);
        return TopoStatusCode::TOPO_OK;
    } else {
        return TopoStatusCode::TOPO_METASERVER_NOT_FOUND;
    }
}

TopoStatusCode TopologyImpl::UpdatePool(const Pool &data) {
    WriteLockGuard wlockPool(poolMutex_);
    auto it = poolMap_.find(data.GetId());
    if (it != poolMap_.end()) {
        if (!storage_->UpdatePool(data)) {
            return TopoStatusCode::TOPO_STORGE_FAIL;
        }
        it->second = data;
        return TopoStatusCode::TOPO_OK;
    } else {
        return TopoStatusCode::TOPO_POOL_NOT_FOUND;
    }
}

TopoStatusCode TopologyImpl::UpdateZone(const Zone &data) {
    WriteLockGuard wlockZone(zoneMutex_);
    auto it = zoneMap_.find(data.GetId());
    if (it != zoneMap_.end()) {
        if (!storage_->UpdateZone(data)) {
            return TopoStatusCode::TOPO_STORGE_FAIL;
        }
        it->second = data;
        return TopoStatusCode::TOPO_OK;
    } else {
        return TopoStatusCode::TOPO_ZONE_NOT_FOUND;
    }
}

TopoStatusCode TopologyImpl::UpdateServer(const Server &data) {
    WriteLockGuard wlockServer(serverMutex_);
    auto it = serverMap_.find(data.GetId());
    if (it != serverMap_.end()) {
        if (!storage_->UpdateServer(data)) {
            return TopoStatusCode::TOPO_STORGE_FAIL;
        }
        it->second = data;
        return TopoStatusCode::TOPO_OK;
    } else {
        return TopoStatusCode::TOPO_SERVER_NOT_FOUND;
    }
}

TopoStatusCode TopologyImpl::UpdateMetaServerOnlineState(
    const OnlineState &onlineState, MetaServerIdType id) {
    ReadLockGuard rlockMetaServerMap(metaServerMutex_);
    auto it = metaServerMap_.find(id);
    if (it != metaServerMap_.end()) {
        if (onlineState != it->second.GetOnlineState()) {
            WriteLockGuard wlockMetaServer(it->second.GetRWLockRef());
            it->second.SetOnlineState(onlineState);
        }
        return TopoStatusCode::TOPO_OK;
    } else {
        return TopoStatusCode::TOPO_METASERVER_NOT_FOUND;
    }
}

TopoStatusCode TopologyImpl::GetPoolIdByMetaserverId(MetaServerIdType id,
                                                     PoolIdType *poolIdOut) {
    *poolIdOut = UNINTIALIZE_ID;
    MetaServer metaserver;
    if (!GetMetaServer(id, &metaserver)) {
        LOG(ERROR) << "TopologyImpl::GetPoolIdByMetaserverId "
                   << "Fail On GetMetaServer, "
                   << "metaserverId = " << id;
        return TopoStatusCode::TOPO_METASERVER_NOT_FOUND;
    }
    return GetPoolIdByServerId(metaserver.GetServerId(), poolIdOut);
}

TopoStatusCode TopologyImpl::GetPoolIdByServerId(ServerIdType id,
                                                 PoolIdType *poolIdOut) {
    *poolIdOut = UNINTIALIZE_ID;
    Server server;
    if (!GetServer(id, &server)) {
        LOG(ERROR) << "TopologyImpl::GetPoolIdByServerId "
                   << "Fail On GetServer, "
                   << "serverId = " << id;
        return TopoStatusCode::TOPO_SERVER_NOT_FOUND;
    }

    *poolIdOut = server.GetPoolId();
    return TopoStatusCode::TOPO_OK;
}

TopoStatusCode TopologyImpl::UpdateMetaServerSpace(const MetaServerSpace &space,
                                                   MetaServerIdType id) {
    // find pool it belongs to
    PoolIdType belongPoolId = UNINTIALIZE_ID;
    TopoStatusCode ret = GetPoolIdByMetaserverId(id, &belongPoolId);
    if (ret != TopoStatusCode::TOPO_OK) {
        return ret;
    }

    // fetch write lock of the pool and read lock of chunkserver map
    WriteLockGuard wlocklPool(poolMutex_);
    int64_t diffCapacity = 0;
    {
        ReadLockGuard rlockMetaServerMap(metaServerMutex_);
        auto it = metaServerMap_.find(id);
        if (it != metaServerMap_.end()) {
            WriteLockGuard wlockChunkServer(it->second.GetRWLockRef());
            diffCapacity = space.GetDiskCapacity() -
                           it->second.GetMetaServerSpace().GetDiskCapacity();
            int64_t diffUsed = space.GetDiskUsed() -
                               it->second.GetMetaServerSpace().GetDiskUsed();
            int64_t diffMemory =
                space.GetMemoryUsed() -
                it->second.GetMetaServerSpace().GetMemoryUsed();
            it->second.SetMetaServerSpace(space);
            if (diffCapacity != 0 || diffUsed != 0 || diffMemory != 0) {
                DVLOG(6) << "update metaserver, diffCapacity = " << diffCapacity
                         << ", diffUsed = " << diffUsed
                         << ", diffMemory = " << diffMemory;
                it->second.SetDirtyFlag(true);
            } else {
                return TopoStatusCode::TOPO_OK;
            }

        } else {
            return TopoStatusCode::TOPO_METASERVER_NOT_FOUND;
        }
    }

    if (diffCapacity != 0) {
        // update pool
        auto it = poolMap_.find(belongPoolId);
        if (it != poolMap_.end()) {
            uint64_t totalCapacity = it->second.GetDiskCapacity();
            totalCapacity += diffCapacity;
            DVLOG(6) << "update pool to " << totalCapacity;
            it->second.SetDiskCapacity(totalCapacity);
        } else {
            return TopoStatusCode::TOPO_POOL_NOT_FOUND;
        }
    }

    return TopoStatusCode::TOPO_OK;
}

TopoStatusCode TopologyImpl::UpdateMetaServerStartUpTime(uint64_t time,
                                                         MetaServerIdType id) {
    ReadLockGuard rlockMetaServerMap(metaServerMutex_);
    auto it = metaServerMap_.find(id);
    if (it != metaServerMap_.end()) {
        WriteLockGuard wlockMetaServer(it->second.GetRWLockRef());
        it->second.SetStartUpTime(time);
        return TopoStatusCode::TOPO_OK;
    } else {
        return TopoStatusCode::TOPO_METASERVER_NOT_FOUND;
    }
}

PoolIdType TopologyImpl::FindPool(const std::string &poolName) const {
    ReadLockGuard rlockPool(poolMutex_);
    for (auto it = poolMap_.begin(); it != poolMap_.end(); it++) {
        if (it->second.GetName() == poolName) {
            return it->first;
        }
    }
    return static_cast<PoolIdType>(UNINTIALIZE_ID);
}

ZoneIdType TopologyImpl::FindZone(const std::string &zoneName,
                                  const std::string &poolName) const {
    PoolIdType poolId = FindPool(poolName);
    return FindZone(zoneName, poolId);
}

ZoneIdType TopologyImpl::FindZone(const std::string &zoneName,
                                  PoolIdType poolId) const {
    ReadLockGuard rlockZone(zoneMutex_);
    for (auto it = zoneMap_.begin(); it != zoneMap_.end(); it++) {
        if ((it->second.GetPoolId() == poolId) &&
            (it->second.GetName() == zoneName)) {
            return it->first;
        }
    }
    return static_cast<ZoneIdType>(UNINTIALIZE_ID);
}

ServerIdType TopologyImpl::FindServerByHostName(
    const std::string &hostName) const {
    ReadLockGuard rlockServer(serverMutex_);
    for (auto it = serverMap_.begin(); it != serverMap_.end(); it++) {
        if (it->second.GetHostName() == hostName) {
            return it->first;
        }
    }
    return static_cast<ServerIdType>(UNINTIALIZE_ID);
}

ServerIdType TopologyImpl::FindServerByHostIpPort(const std::string &hostIp,
                                                  uint32_t port) const {
    ReadLockGuard rlockServer(serverMutex_);
    for (auto it = serverMap_.begin(); it != serverMap_.end(); it++) {
        if (it->second.GetInternalHostIp() == hostIp) {
            if (0 == it->second.GetInternalPort()) {
                return it->first;
            } else if (port == it->second.GetInternalPort()) {
                return it->first;
            }
        } else if (it->second.GetExternalHostIp() == hostIp) {
            if (0 == it->second.GetExternalPort()) {
                return it->first;
            } else if (port == it->second.GetExternalPort()) {
                return it->first;
            }
        }
    }
    return static_cast<ServerIdType>(UNINTIALIZE_ID);
}

bool TopologyImpl::GetPool(PoolIdType poolId, Pool *out) const {
    ReadLockGuard rlockPool(poolMutex_);
    auto it = poolMap_.find(poolId);
    if (it != poolMap_.end()) {
        *out = it->second;
        return true;
    }
    return false;
}

bool TopologyImpl::GetZone(ZoneIdType zoneId, Zone *out) const {
    ReadLockGuard rlockZone(zoneMutex_);
    auto it = zoneMap_.find(zoneId);
    if (it != zoneMap_.end()) {
        *out = it->second;
        return true;
    }
    return false;
}

bool TopologyImpl::GetServer(ServerIdType serverId, Server *out) const {
    ReadLockGuard rlockServer(serverMutex_);
    auto it = serverMap_.find(serverId);
    if (it != serverMap_.end()) {
        *out = it->second;
        return true;
    }
    return false;
}

bool TopologyImpl::GetMetaServer(MetaServerIdType metaserverId,
                                 MetaServer *out) const {
    ReadLockGuard rlockMetaServerMap(metaServerMutex_);
    auto it = metaServerMap_.find(metaserverId);
    if (it != metaServerMap_.end()) {
        ReadLockGuard rlockMetaServer(it->second.GetRWLockRef());
        *out = it->second;
        return true;
    }
    return false;
}

bool TopologyImpl::GetMetaServer(const std::string &hostIp, uint32_t port,
                                 MetaServer *out) const {
    ReadLockGuard rlockMetaServerMap(metaServerMutex_);
    for (auto it = metaServerMap_.begin(); it != metaServerMap_.end(); it++) {
        ReadLockGuard rlockMetaServer(it->second.GetRWLockRef());
        if (it->second.GetInternalHostIp() == hostIp &&
            it->second.GetInternalPort() == port) {
            *out = it->second;
            return true;
        }
    }
    return false;
}

TopoStatusCode TopologyImpl::AddPartition(const Partition &data) {
    ReadLockGuard rlockPool(poolMutex_);
    WriteLockGuard wlockCopyset(copySetMutex_);
    WriteLockGuard wlockPartition(partitionMutex_);
    PartitionIdType id = data.GetPartitionId();

    if (poolMap_.find(data.GetPoolId()) != poolMap_.end()) {
        CopySetKey key(data.GetPoolId(), data.GetCopySetId());
        auto it = copySetMap_.find(key);
        if (it != copySetMap_.end()) {
            if (partitionMap_.find(id) == partitionMap_.end()) {
                if (!storage_->StoragePartition(data)) {
                    return TopoStatusCode::TOPO_STORGE_FAIL;
                }
                partitionMap_[id] = data;
                it->second.AddPartitionNum();
                it->second.SetDirtyFlag(true);
                return TopoStatusCode::TOPO_OK;
            } else {
                return TopoStatusCode::TOPO_ID_DUPLICATED;
            }
        } else {
            return TopoStatusCode::TOPO_COPYSET_NOT_FOUND;
        }
    } else {
        return TopoStatusCode::TOPO_POOL_NOT_FOUND;
    }
}

TopoStatusCode TopologyImpl::RemovePartition(PartitionIdType id) {
    WriteLockGuard wlockCopySet(copySetMutex_);
    WriteLockGuard wlockPartition(partitionMutex_);
    auto it = partitionMap_.find(id);
    if (it != partitionMap_.end()) {
        if (!storage_->DeletePartition(id)) {
            return TopoStatusCode::TOPO_STORGE_FAIL;
        }
        CopySetKey key(it->second.GetPoolId(), it->second.GetCopySetId());
        auto ix = copySetMap_.find(key);
        if (ix != copySetMap_.end()) {
            ix->second.ReducePartitionNum();
        }
        partitionMap_.erase(it);
        return TopoStatusCode::TOPO_OK;
    } else {
        return TopoStatusCode::TOPO_PARTITION_NOT_FOUND;
    }
}

TopoStatusCode TopologyImpl::UpdatePartition(const Partition &data) {
    WriteLockGuard wlockPartition(partitionMutex_);
    auto it = partitionMap_.find(data.GetPartitionId());
    if (it != partitionMap_.end()) {
        if (!storage_->UpdatePartition(data)) {
            return TopoStatusCode::TOPO_STORGE_FAIL;
        }
        it->second = data;
        return TopoStatusCode::TOPO_OK;
    } else {
        return TopoStatusCode::TOPO_PARTITION_NOT_FOUND;
    }
}

TopoStatusCode TopologyImpl::UpdatePartitionStatistic(
    uint32_t partitionId, PartitionStatistic statistic) {
    WriteLockGuard wlockPartition(partitionMutex_);
    auto it = partitionMap_.find(partitionId);
    if (it != partitionMap_.end()) {
        Partition temp = it->second;
        temp.SetStatus(statistic.status);
        temp.SetInodeNum(statistic.inodeNum);
        temp.SetDentryNum(statistic.dentryNum);
        if (!storage_->UpdatePartition(temp)) {
            return TopoStatusCode::TOPO_STORGE_FAIL;
        }
        it->second = std::move(temp);
        return TopoStatusCode::TOPO_OK;
    } else {
        return TopoStatusCode::TOPO_PARTITION_NOT_FOUND;
    }
}

bool TopologyImpl::GetPartition(PartitionIdType partitionId, Partition *out) {
    ReadLockGuard rlockPartition(partitionMutex_);
    auto it = partitionMap_.find(partitionId);
    if (it != partitionMap_.end()) {
        *out = it->second;
        return true;
    }
    return false;
}

bool TopologyImpl::GetCopysetOfPartition(PartitionIdType id,
                                         CopySetInfo *out) const {
    ReadLockGuard rlockPartition(partitionMutex_);
    auto it = partitionMap_.find(id);
    if (it != partitionMap_.end()) {
        PoolIdType poolId = it->second.GetPoolId();
        CopySetIdType csId = it->second.GetCopySetId();
        CopySetKey key(poolId, csId);
        ReadLockGuard rlockCopyset(copySetMutex_);
        auto iter = copySetMap_.find(key);
        if (iter != copySetMap_.end()) {
            *out = iter->second;
            return true;
        }
    }
    return false;
}

bool TopologyImpl::GetAvailableCopyset(CopySetInfo *out) const {
    ReadLockGuard rlockCopySet(copySetMutex_);
    uint64_t maxPartitionNum = option_.partitionNumberInCopyset;
    std::set<CopySetKey> copysets;
    for (auto const &it : copySetMap_) {
        if (it.second.GetPartitionNum() < maxPartitionNum) {
            copysets.emplace(it.first);
        }
    }
    if (copysets.empty()) {
        return false;
    }

    int randomValue = GetOneRandomNumber(0, copysets.size() - 1);
    auto iter = copysets.begin();
    std::advance(iter, randomValue);
    auto it = copySetMap_.find(*iter);
    if (it != copySetMap_.end()) {
        *out = it->second;
        return true;
    }
    return false;
}

std::list<Partition> TopologyImpl::GetPartitionOfFs(
    FsIdType id, PartitionFilter filter) const {
    std::list<Partition> ret;
    ReadLockGuard rlockPartitionMap(partitionMutex_);
    for (auto it = partitionMap_.begin(); it != partitionMap_.end(); it++) {
        if (filter(it->second) && it->second.GetFsId() == id) {
            ret.push_back(it->second);
        }
    }
    return ret;
}

// getList
std::vector<MetaServerIdType> TopologyImpl::GetMetaServerInCluster(
    MetaServerFilter filter) const {
    std::vector<MetaServerIdType> ret;
    ReadLockGuard rlockMetaServerMap(metaServerMutex_);
    for (auto it = metaServerMap_.begin(); it != metaServerMap_.end(); it++) {
        ReadLockGuard rlockMetaServer(it->second.GetRWLockRef());
        if (filter(it->second)) {
            ret.push_back(it->first);
        }
    }
    return ret;
}

std::vector<ServerIdType> TopologyImpl::GetServerInCluster(
    ServerFilter filter) const {
    std::vector<ServerIdType> ret;
    ReadLockGuard rlockServer(serverMutex_);
    for (auto it = serverMap_.begin(); it != serverMap_.end(); it++) {
        if (filter(it->second)) {
            ret.push_back(it->first);
        }
    }
    return ret;
}

std::vector<ZoneIdType> TopologyImpl::GetZoneInCluster(
    ZoneFilter filter) const {
    std::vector<ZoneIdType> ret;
    ReadLockGuard rlockZone(zoneMutex_);
    for (auto it = zoneMap_.begin(); it != zoneMap_.end(); it++) {
        if (filter(it->second)) {
            ret.push_back(it->first);
        }
    }
    return ret;
}

std::vector<PoolIdType> TopologyImpl::GetPoolInCluster(
    PoolFilter filter) const {
    std::vector<PoolIdType> ret;
    ReadLockGuard rlockPool(poolMutex_);
    for (auto it = poolMap_.begin(); it != poolMap_.end(); it++) {
        if (filter(it->second)) {
            ret.push_back(it->first);
        }
    }
    return ret;
}

std::list<MetaServerIdType> TopologyImpl::GetMetaServerInServer(
    ServerIdType id, MetaServerFilter filter) const {
    std::list<MetaServerIdType> ret;
    ReadLockGuard rlockMetaServerMap(metaServerMutex_);
    for (auto it = metaServerMap_.begin(); it != metaServerMap_.end(); it++) {
        ReadLockGuard rlockMetaServer(it->second.GetRWLockRef());
        if (filter(it->second) && it->second.GetServerId() == id) {
            ret.push_back(it->first);
        }
    }
    return ret;
}

std::list<MetaServerIdType> TopologyImpl::GetMetaServerInZone(
    ZoneIdType id, MetaServerFilter filter) const {
    std::list<MetaServerIdType> ret;
    std::list<ServerIdType> serverList = GetServerInZone(id);
    for (ServerIdType s : serverList) {
        std::list<MetaServerIdType> temp = GetMetaServerInServer(s, filter);
        ret.splice(ret.begin(), temp);
    }
    return ret;
}

std::list<ServerIdType> TopologyImpl::GetServerInZone(
    ZoneIdType id, ServerFilter filter) const {
    std::list<ServerIdType> ret;
    ReadLockGuard rlockServer(serverMutex_);
    for (auto it = serverMap_.begin(); it != serverMap_.end(); it++) {
        if (filter(it->second) && it->second.GetZoneId() == id) {
            ret.push_back(it->first);
        }
    }
    return ret;
}

std::list<ZoneIdType> TopologyImpl::GetZoneInPool(PoolIdType id,
                                                  ZoneFilter filter) const {
    std::list<ZoneIdType> ret;
    ReadLockGuard rlockZone(zoneMutex_);
    for (auto it = zoneMap_.begin(); it != zoneMap_.end(); it++) {
        if (filter(it->second) && it->second.GetPoolId() == id) {
            ret.push_back(it->first);
        }
    }
    return ret;
}

TopoStatusCode TopologyImpl::Init(const TopologyOption &option) {
    option_ = option;
    TopoStatusCode ret = LoadClusterInfo();
    if (ret != TopoStatusCode::TOPO_OK) {
        LOG(ERROR) << "[TopologyImpl::init], LoadClusterInfo fail.";
        return ret;
    }

    PoolIdType maxPoolId;
    if (!storage_->LoadPool(&poolMap_, &maxPoolId)) {
        LOG(ERROR) << "[TopologyImpl::init], LoadPool fail.";
        return TopoStatusCode::TOPO_STORGE_FAIL;
    }
    idGenerator_->initPoolIdGenerator(maxPoolId);
    LOG(INFO) << "[TopologyImpl::init], LoadPool success, "
              << "pool num = " << poolMap_.size();

    ZoneIdType maxZoneId;
    if (!storage_->LoadZone(&zoneMap_, &maxZoneId)) {
        LOG(ERROR) << "[TopologyImpl::init], LoadZone fail.";
        return TopoStatusCode::TOPO_STORGE_FAIL;
    }
    idGenerator_->initZoneIdGenerator(maxZoneId);
    LOG(INFO) << "[TopologyImpl::init], LoadZone success, "
              << "zone num = " << zoneMap_.size();

    ServerIdType maxServerId;
    if (!storage_->LoadServer(&serverMap_, &maxServerId)) {
        LOG(ERROR) << "[TopologyImpl::init], LoadServer fail.";
        return TopoStatusCode::TOPO_STORGE_FAIL;
    }
    idGenerator_->initServerIdGenerator(maxServerId);
    LOG(INFO) << "[TopologyImpl::init], LoadServer success, "
              << "server num = " << serverMap_.size();

    MetaServerIdType maxMetaServerId;
    if (!storage_->LoadMetaServer(&metaServerMap_, &maxMetaServerId)) {
        LOG(ERROR) << "[TopologyImpl::init], LoadMetaServer fail.";
        return TopoStatusCode::TOPO_STORGE_FAIL;
    }
    idGenerator_->initMetaServerIdGenerator(maxMetaServerId);
    LOG(INFO) << "[TopologyImpl::init], LoadMetaServer success, "
              << "metaserver num = " << metaServerMap_.size();

    // update pool capacity
    for (auto pair : metaServerMap_) {
        PoolIdType poolId = UNINTIALIZE_ID;
        TopoStatusCode ret =
            GetPoolIdByMetaserverId(pair.second.GetId(), &poolId);
        if (ret != TopoStatusCode::TOPO_OK) {
            return ret;
        }

        auto it = poolMap_.find(poolId);
        if (it != poolMap_.end()) {
            uint64_t totalCapacity =
                it->second.GetDiskCapacity() +
                pair.second.GetMetaServerSpace().GetDiskCapacity();
            it->second.SetDiskCapacity(totalCapacity);
        } else {
            LOG(ERROR) << "TopologyImpl::Init Fail On Get Pool, "
                       << "poolId = " << poolId;
            return TopoStatusCode::TOPO_POOL_NOT_FOUND;
        }
    }
    LOG(INFO) << "Calc Pool capacity success.";

    std::map<PoolIdType, CopySetIdType> copySetIdMaxMap;
    if (!storage_->LoadCopySet(&copySetMap_, &copySetIdMaxMap)) {
        LOG(ERROR) << "[TopologyImpl::init], LoadCopySet fail.";
        return TopoStatusCode::TOPO_STORGE_FAIL;
    }
    idGenerator_->initCopySetIdGenerator(copySetIdMaxMap);
    LOG(INFO) << "[TopologyImpl::init], LoadCopySet success, "
              << "copyset num = " << copySetMap_.size();

    PartitionIdType maxPartitionId;
    if (!storage_->LoadPartition(&partitionMap_, &maxPartitionId)) {
        LOG(ERROR) << "[TopologyImpl::init], LoadPartition fail.";
        return TopoStatusCode::TOPO_STORGE_FAIL;
    }
    idGenerator_->initPartitionIdGenerator(maxPartitionId);
    LOG(INFO) << "[TopologyImpl::init], LoadPartition success, "
              << "partition num = " << partitionMap_.size();

    for (auto it : zoneMap_) {
        PoolIdType poolid = it.second.GetPoolId();
        poolMap_[poolid].AddZone(it.first);
    }

    for (auto it : serverMap_) {
        ZoneIdType zid = it.second.GetZoneId();
        zoneMap_[zid].AddServer(it.first);
    }

    for (auto it : metaServerMap_) {
        ServerIdType sId = it.second.GetServerId();
        serverMap_[sId].AddMetaServer(it.first);
    }

    return TopoStatusCode::TOPO_OK;
}

TopoStatusCode TopologyImpl::AddCopySet(const CopySetInfo &data) {
    ReadLockGuard rlockPool(poolMutex_);
    WriteLockGuard wlockCopySetMap(copySetMutex_);
    auto it = poolMap_.find(data.GetPoolId());
    if (it != poolMap_.end()) {
        CopySetKey key(data.GetPoolId(), data.GetId());
        if (copySetMap_.find(key) == copySetMap_.end()) {
            if (!storage_->StorageCopySet(data)) {
                return TopoStatusCode::TOPO_STORGE_FAIL;
            }
            copySetMap_[key] = data;
            return TopoStatusCode::TOPO_OK;
        } else {
            return TopoStatusCode::TOPO_ID_DUPLICATED;
        }
    } else {
        return TopoStatusCode::TOPO_POOL_NOT_FOUND;
    }
}

TopoStatusCode TopologyImpl::RemoveCopySet(CopySetKey key) {
    WriteLockGuard wlockCopySetMap(copySetMutex_);
    auto it = copySetMap_.find(key);
    if (it != copySetMap_.end()) {
        if (!storage_->DeleteCopySet(key)) {
            return TopoStatusCode::TOPO_STORGE_FAIL;
        }
        copySetMap_.erase(key);
        return TopoStatusCode::TOPO_OK;
    } else {
        return TopoStatusCode::TOPO_COPYSET_NOT_FOUND;
    }
}

TopoStatusCode TopologyImpl::UpdateCopySetTopo(const CopySetInfo &data) {
    ReadLockGuard rlockCopySetMap(copySetMutex_);
    CopySetKey key(data.GetPoolId(), data.GetId());
    auto it = copySetMap_.find(key);
    if (it != copySetMap_.end()) {
        WriteLockGuard wlockCopySet(it->second.GetRWLockRef());
        it->second.SetLeader(data.GetLeader());
        it->second.SetEpoch(data.GetEpoch());
        it->second.SetPartitionNum(data.GetPartitionNum());
        it->second.SetCopySetMembers(data.GetCopySetMembers());
        it->second.SetDirtyFlag(true);
        return TopoStatusCode::TOPO_OK;
    } else {
        LOG(WARNING) << "UpdateCopySetTopo can not find copyset, "
                     << "poolId = " << data.GetPoolId()
                     << ", copysetId = " << data.GetId();
        return TopoStatusCode::TOPO_COPYSET_NOT_FOUND;
    }
}

TopoStatusCode TopologyImpl::SetCopySetAvalFlag(const CopySetKey &key,
                                                bool aval) {
    ReadLockGuard rlockCopySetMap(copySetMutex_);
    auto it = copySetMap_.find(key);
    if (it != copySetMap_.end()) {
        WriteLockGuard wlockCopySet(it->second.GetRWLockRef());
        auto copysetInfo = it->second;
        copysetInfo.SetAvailableFlag(aval);
        bool ret = storage_->UpdateCopySet(copysetInfo);
        if (!ret) {
            LOG(ERROR) << "UpdateCopySet met storage error";
            return TopoStatusCode::TOPO_STORGE_FAIL;
        }
        it->second.SetAvailableFlag(aval);
        return TopoStatusCode::TOPO_OK;
    } else {
        LOG(WARNING) << "SetCopySetAvalFlag can not find copyset, "
                     << "logicalPoolId = " << key.first
                     << ", copysetId = " << key.second;
        return TopoStatusCode::TOPO_COPYSET_NOT_FOUND;
    }
}

bool TopologyImpl::GetCopySet(CopySetKey key, CopySetInfo *out) const {
    ReadLockGuard rlockCopySetMap(copySetMutex_);
    auto it = copySetMap_.find(key);
    if (it != copySetMap_.end()) {
        ReadLockGuard rlockCopySet(it->second.GetRWLockRef());
        *out = it->second;
        return true;
    } else {
        return false;
    }
}

int TopologyImpl::Run() {
    if (isStop_.exchange(false)) {
        backEndThread_ =
            curve::common::Thread(&TopologyImpl::BackEndFunc, this);
    }
    return 0;
}

int TopologyImpl::Stop() {
    if (!isStop_.exchange(true)) {
        LOG(INFO) << "stop TopologyImpl...";
        sleeper_.interrupt();
        backEndThread_.join();
        LOG(INFO) << "stop TopologyImpl ok.";
    }
    return 0;
}

void TopologyImpl::BackEndFunc() {
    while (sleeper_.wait_for(
        std::chrono::seconds(option_.topologyUpdateToRepoSec))) {
        FlushCopySetToStorage();
        FlushMetaServerToStorage();
    }
}

void TopologyImpl::FlushCopySetToStorage() {
    std::vector<PoolIdType> pools = GetPoolInCluster();
    for (const auto poolId : pools) {
        ReadLockGuard rlockCopySetMap(copySetMutex_);
        for (auto &c : copySetMap_) {
            WriteLockGuard wlockCopySet(c.second.GetRWLockRef());
            if (c.second.GetDirtyFlag() && c.second.GetPoolId() == poolId) {
                c.second.SetDirtyFlag(false);
                if (!storage_->UpdateCopySet(c.second)) {
                    LOG(WARNING) << "update copyset(" << c.second.GetPoolId()
                                 << "," << c.second.GetId() << ") to repo fail";
                }
            }
        }
    }
}

void TopologyImpl::FlushMetaServerToStorage() {
    std::vector<MetaServer> toUpdate;
    {
        ReadLockGuard rlockMetaServerMap(metaServerMutex_);
        for (auto &c : metaServerMap_) {
            // update DirtyFlag only, thus only read lock is needed
            ReadLockGuard rlockMetaServer(c.second.GetRWLockRef());
            if (c.second.GetDirtyFlag()) {
                c.second.SetDirtyFlag(false);
                toUpdate.push_back(c.second);
            }
        }
    }
    for (const auto &v : toUpdate) {
        if (!storage_->UpdateMetaServer(v)) {
            LOG(WARNING) << "update metaserver to repo fail"
                         << ", metaserverid = " << v.GetId();
        }
    }
}

TopoStatusCode TopologyImpl::LoadClusterInfo() {
    std::vector<ClusterInformation> infos;
    if (!storage_->LoadClusterInfo(&infos)) {
        return TopoStatusCode::TOPO_STORGE_FAIL;
    }
    if (infos.empty()) {
        std::string uuid = UUIDGenerator().GenerateUUID();
        ClusterInformation info(uuid);
        if (!storage_->StorageClusterInfo(info)) {
            return TopoStatusCode::TOPO_STORGE_FAIL;
        }
        clusterInfo_ = info;
    } else {
        clusterInfo_ = infos[0];
    }
    return TopoStatusCode::TOPO_OK;
}

bool TopologyImpl::GetClusterInfo(ClusterInformation *info) {
    *info = clusterInfo_;
    return true;
}

// update partition tx, and ensure atomicity
TopoStatusCode TopologyImpl::UpdatePartitionTxIds(
    std::vector<PartitionTxId> txIds) {
    std::vector<Partition> partitions;
    WriteLockGuard wlockPartition(partitionMutex_);
    for (auto item : txIds) {
        auto it = partitionMap_.find(item.partitionid());
        if (it != partitionMap_.end()) {
            ReadLockGuard rlockPartition(it->second.GetRWLockRef());
            Partition tmp = it->second;
            tmp.SetTxId(item.txid());
            partitions.emplace_back(tmp);
        } else {
            LOG(ERROR) << "UpdatePartition failed, partition not found."
                       << " partition id = " << item.partitionid();
            return TopoStatusCode::TOPO_PARTITION_NOT_FOUND;
        }
    }
    if (storage_->UpdatePartitions(partitions)) {
        // update memory
        for (auto item : partitions) {
            partitionMap_[item.GetPartitionId()] = item;
        }
        return TopoStatusCode::TOPO_OK;
    }
    LOG(ERROR) << "UpdatepPartition failed, storage failure.";
    return TopoStatusCode::TOPO_STORGE_FAIL;
}

// choose random
int TopologyImpl::GetOneRandomNumber(int start, int end) const {
    static std::random_device rd;
    static std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(start, end);
    return dis(gen);
}

TopoStatusCode TopologyImpl::ChooseSinglePoolRandom(PoolIdType *out) const {
    ReadLockGuard rlockPool(poolMutex_);
    if (poolMap_.empty()) {
        LOG(ERROR) << "Choose single pool failed, poolmap is empty.";
        return TopoStatusCode::TOPO_POOL_NOT_FOUND;
    }
    int randomValue = GetOneRandomNumber(0, poolMap_.size() - 1);
    auto iter = poolMap_.begin();
    std::advance(iter, randomValue);
    *out = iter->second.GetId();
    return TopoStatusCode::TOPO_OK;
}

TopoStatusCode TopologyImpl::ChooseZonesInPool(PoolIdType poolId,
                                               std::set<ZoneIdType> *zones,
                                               int count) const {
    std::list<ZoneIdType> zoneList = GetZoneInPool(poolId);
    if (zoneList.size() < count) {
        LOG(ERROR) << "Choose zone in pool failed,"
                   << "the zone.size = " << zoneList.size()
                   << ", the need count = " << count;
        return TopoStatusCode::TOPO_ZONE_NOT_FOUND;
    }

    while (zones->size() < count) {
        auto iter = zoneList.begin();
        int randomValue = GetOneRandomNumber(0, zoneList.size() - 1);
        std::advance(iter, randomValue);
        zones->emplace(*iter);
    }
    return TopoStatusCode::TOPO_OK;
}

TopoStatusCode TopologyImpl::ChooseSingleMetaServerInZone(
    ZoneIdType zoneId, MetaServerIdType *metaServerId) const {
    std::list<MetaServerIdType> metaServerList = GetMetaServerInZone(zoneId);
    if (metaServerList.empty()) {
        LOG(ERROR) << "Choose metaserver in zone failed,"
                   << " the metaserver is empty."
                   << "zoneId = " << zoneId;
        return TopoStatusCode::TOPO_METASERVER_NOT_FOUND;
    }
    auto iter = metaServerList.begin();
    int randomValue = GetOneRandomNumber(0, metaServerList.size() - 1);
    std::advance(iter, randomValue);
    *metaServerId = *iter;
    return TopoStatusCode::TOPO_OK;
}

uint32_t TopologyImpl::GetPartitionNumberOfFs(FsIdType fsId) {
    ReadLockGuard rlockPartition(partitionMutex_);
    uint32_t pNumber = 0;
    for (const auto &it : partitionMap_) {
        if (it.second.GetFsId() == fsId) {
            pNumber++;
        }
    }
    return pNumber;
}

}  // namespace topology
}  // namespace mds
}  // namespace curvefs
