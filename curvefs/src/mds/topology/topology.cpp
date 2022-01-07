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

TopoStatusCode TopologyImpl::AddPool(const Pool& data) {
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
    PoolIdType poolId = UNINITIALIZE_ID;
    TopoStatusCode ret = GetPoolIdByServerId(data.GetServerId(), &poolId);
    if (ret != TopoStatusCode::TOPO_OK) {
        return ret;
    }

    // fetch lock on pool, server and metaserver
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
        uint64_t metaserverCapacity =
            it->second.GetMetaServerSpace().GetDiskCapacity();
        if (!storage_->DeleteMetaServer(id)) {
            return TopoStatusCode::TOPO_STORGE_FAIL;
        }
        auto ix = serverMap_.find(it->second.GetServerId());
        if (ix != serverMap_.end()) {
            ix->second.RemoveMetaServer(id);
        }
        metaServerMap_.erase(it);

        // update pool
        WriteLockGuard wlockPool(poolMutex_);
        PoolIdType poolId = ix->second.GetPoolId();
        auto it = poolMap_.find(poolId);
        if (it != poolMap_.end()) {
            it->second.SetDiskCapacity(it->second.GetDiskCapacity() -
                metaserverCapacity);
        } else {
            return TopoStatusCode::TOPO_POOL_NOT_FOUND;
        }
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
    *poolIdOut = UNINITIALIZE_ID;
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
    *poolIdOut = UNINITIALIZE_ID;
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
    PoolIdType belongPoolId = UNINITIALIZE_ID;
    TopoStatusCode ret = GetPoolIdByMetaserverId(id, &belongPoolId);
    if (ret != TopoStatusCode::TOPO_OK) {
        return ret;
    }

    // fetch write lock of the pool and read lock of metaserver map
    WriteLockGuard wlocklPool(poolMutex_);
    int64_t diffCapacity = 0;
    {
        ReadLockGuard rlockMetaServerMap(metaServerMutex_);
        auto it = metaServerMap_.find(id);
        if (it != metaServerMap_.end()) {
            WriteLockGuard wlockMetaServer(it->second.GetRWLockRef());
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
    return static_cast<PoolIdType>(UNINITIALIZE_ID);
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
    return static_cast<ZoneIdType>(UNINITIALIZE_ID);
}

ServerIdType TopologyImpl::FindServerByHostName(
    const std::string &hostName) const {
    ReadLockGuard rlockServer(serverMutex_);
    for (auto it = serverMap_.begin(); it != serverMap_.end(); it++) {
        if (it->second.GetHostName() == hostName) {
            return it->first;
        }
    }
    return static_cast<ServerIdType>(UNINITIALIZE_ID);
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
    return static_cast<ServerIdType>(UNINITIALIZE_ID);
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
                it->second.AddPartitionId(id);
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


TopoStatusCode TopologyImpl::UpdatePartitionStatus(PartitionIdType partitionId,
                                                   PartitionStatus status) {
    WriteLockGuard wlockPartition(partitionMutex_);
    auto it = partitionMap_.find(partitionId);
    if (it != partitionMap_.end()) {
        Partition temp = it->second;
        temp.SetStatus(status);
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
    CopySetKey key;
    uint64_t minPartitionNum = maxPartitionNum;

    for (auto const &it : copySetMap_) {
        uint64_t partitionNum = it.second.GetPartitionNum();
        if (partitionNum < maxPartitionNum) {
            if (partitionNum < minPartitionNum) {
                minPartitionNum = partitionNum;
                key = it.first;
            }
        }
    }

    if (minPartitionNum == maxPartitionNum) {
        return false;
    }

    auto it = copySetMap_.find(key);
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

std::list<Partition> TopologyImpl::GetPartitionInfosInPool(
    PoolIdType poolId, PartitionFilter filter) const {
    std::list<Partition> ret;
    ReadLockGuard rlockPartitionMap(partitionMutex_);
    for (auto it = partitionMap_.begin(); it != partitionMap_.end(); it++) {
        if (filter(it->second) && it->second.GetPoolId() == poolId) {
            ret.push_back(it->second);
        }
    }
    return ret;
}

std::list<Partition> TopologyImpl::GetPartitionInfosInCopyset(
    CopySetIdType copysetId) const {
    std::list<Partition> ret;
    ReadLockGuard rlockPartitionMap(partitionMutex_);
    for (auto it = partitionMap_.begin(); it != partitionMap_.end(); it++) {
        if (it->second.GetCopySetId() == copysetId) {
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

std::list<MetaServerIdType> TopologyImpl::GetMetaServerInPool(
    PoolIdType id, MetaServerFilter filter) const {
    std::list<MetaServerIdType> ret;
    std::list<ZoneIdType> zoneList = GetZoneInPool(id);
    for (ZoneIdType z : zoneList) {
        std::list<MetaServerIdType> temp = GetMetaServerInZone(z, filter);
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

std::vector<CopySetIdType> TopologyImpl::GetCopySetsInPool(
    PoolIdType poolId, CopySetFilter filter) const {
    std::vector<CopySetIdType> ret;
    ReadLockGuard rlockCopySet(copySetMutex_);
    for (const auto &it : copySetMap_) {
        if (filter(it.second) && it.first.first == poolId) {
            ret.push_back(it.first.second);
        }
    }
    return ret;
}

std::vector<CopySetKey> TopologyImpl::GetCopySetsInCluster(
    CopySetFilter filter) const {
    std::vector<CopySetKey> ret;
    ReadLockGuard rlockCopySet(copySetMutex_);
    for (const auto &it : copySetMap_) {
        if (filter(it.second)) {
            ret.push_back(it.first);
        }
    }
    return ret;
}

std::vector<CopySetInfo> TopologyImpl::GetCopySetInfosInPool(
    PoolIdType poolId, CopySetFilter filter) const {
    std::vector<CopySetInfo> ret;
    ReadLockGuard rlockCopySet(copySetMutex_);
    for (const auto &it : copySetMap_) {
        if (filter(it.second) && it.first.first == poolId) {
            ret.push_back(it.second);
        }
    }
    return ret;
}

std::vector<CopySetKey> TopologyImpl::GetCopySetsInMetaServer(
    MetaServerIdType id, CopySetFilter filter) const {
    std::vector<CopySetKey> ret;
    ReadLockGuard rlockCopySet(copySetMutex_);
    for (const auto &it : copySetMap_) {
        if (filter(it.second) && it.second.GetCopySetMembers().count(id) > 0) {
            ret.push_back(it.first);
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
        PoolIdType poolId = UNINITIALIZE_ID;
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

    for (const auto &it : zoneMap_) {
        PoolIdType poolid = it.second.GetPoolId();
        poolMap_[poolid].AddZone(it.first);
    }

    for (const auto &it : serverMap_) {
        ZoneIdType zid = it.second.GetZoneId();
        zoneMap_[zid].AddServer(it.first);
    }

    for (const auto &it : metaServerMap_) {
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
                     << "poolId = " << key.first
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

TopoStatusCode TopologyImpl::ChooseRecoveredMetaServer(
    PoolIdType poolId,
    const std::set<ZoneIdType> &unavailableZones,
    const std::set<MetaServerIdType> &unavailableMs,
    MetaServerIdType *target) {
    auto metaservers = GetMetaServerInPool(poolId);
    *target = UNINITIALIZE_ID;
    double usedCapacityPercent = 1;

    for (const auto &it : metaservers) {
        auto iter = unavailableMs.find(it);
        if (iter != unavailableMs.end()) {
            continue;
        }

        MetaServer metaserver;
        if (GetMetaServer(it, &metaserver)) {
            Server server;
            if (GetServer(metaserver.GetServerId(), &server)) {
                auto iter = unavailableZones.find(server.GetZoneId());
                if (iter == unavailableZones.end()) {
                    auto used = static_cast<double>(
                        metaserver.GetMetaServerSpace().GetDiskUsed()) /
                        metaserver.GetMetaServerSpace().GetDiskCapacity();
                    if (used < usedCapacityPercent) {
                        *target = it;
                        usedCapacityPercent = used;
                    }
                }
            } else {
                LOG(ERROR) << "get server failed,"
                           << " the server id = " << metaserver.GetServerId();
                return TopoStatusCode::TOPO_SERVER_NOT_FOUND;
            }
        } else {
            LOG(ERROR) << "get metaserver failed,"
                       << " the metaserver id = " << it;
            return TopoStatusCode::TOPO_METASERVER_NOT_FOUND;
        }
    }

    if (UNINITIALIZE_ID == *target) {
        return TopoStatusCode::TOPO_METASERVER_NOT_FOUND;
    }
    return TopoStatusCode::TOPO_OK;
}

TopoStatusCode TopologyImpl::ChooseAvailableMetaServers(
    std::set<MetaServerIdType> *metaServers,
    PoolIdType *poolId) {
    ReadLockGuard rlockMetaserver(metaServerMutex_);
    std::vector<const MetaServer*> vec;
    for (const auto &it : metaServerMap_) {
        vec.emplace_back(&(it.second));
    }
    // sort by left disk capacity
    std::sort(vec.begin(), vec.end(), [](const MetaServer *a,
        const MetaServer *b) {
        return static_cast<double>(a->GetMetaServerSpace().GetDiskUsed()) /
            a->GetMetaServerSpace().GetDiskCapacity() <
            static_cast<double>(b->GetMetaServerSpace().GetDiskUsed()) /
            b->GetMetaServerSpace().GetDiskCapacity();});

    std::map<PoolIdType, std::map<ZoneIdType, MetaServer>> candidateMap;
    std::map<PoolIdType, uint16_t> replicaMap;
    for (const auto &it : vec) {
        uint64_t leftSize = it->GetMetaServerSpace().GetDiskCapacity() -
            it->GetMetaServerSpace().GetDiskUsed();
        if (it->GetOnlineState() != OnlineState::ONLINE || leftSize <= 0) {
            continue;
        }

        Server server;
        ServerIdType serverId = it->GetServerId();
        if (GetServer(serverId, &server)) {
            PoolIdType poolId = server.GetPoolId();
            ZoneIdType zoneId = server.GetZoneId();
            auto iter = candidateMap.find(poolId);
            if (iter == candidateMap.end()) {
                std::map<ZoneIdType, MetaServer> tmpMap;
                tmpMap.emplace(zoneId, *it);
                candidateMap.emplace(poolId, tmpMap);
            } else {
                bool isDup = false;
                for (const auto &it : iter->second) {
                    if (zoneId == it.first) {
                        isDup = true;
                        break;
                    }
                }
                if (!isDup) {
                    iter->second.emplace(zoneId, *it);
                }
            }
        } else {
            LOG(ERROR) << "get server failed when choose metaservers,"
                       << " the serverId = " << serverId;
            return TopoStatusCode::TOPO_SERVER_NOT_FOUND;
        }

        for (const auto &it : candidateMap) {
            uint16_t replicaNum = 0;
            auto iter = replicaMap.find(it.first);
            if (iter == replicaMap.end()) {
                Pool pool;
                if (GetPool(it.first, &pool)) {
                    replicaNum = pool.GetReplicaNum();
                    replicaMap.emplace(it.first, replicaNum);
                } else {
                    LOG(ERROR) << "get pool failed when choose metaservers,"
                               << " the poolId = " << it.first;
                    return TopoStatusCode::TOPO_POOL_NOT_FOUND;
                }
            } else {
                replicaNum = iter->second;
            }
            // find the available metaserver group
            if (it.second.size() == replicaNum) {
                for (const auto &item : it.second) {
                    metaServers->emplace(item.second.GetId());
                }
                *poolId = it.first;
                return TopoStatusCode::TOPO_OK;
            }
        }
    }
    return TopoStatusCode::TOPO_METASERVER_NOT_FOUND;
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
std::vector<CopySetInfo> TopologyImpl::ListCopysetInfo() const {
    std::vector<CopySetInfo> ret;
    for (auto const& i : copySetMap_) {
        ret.emplace_back(i.second);
    }
    return ret;
}

void TopologyImpl::GetMetaServersSpace(
    ::google::protobuf::RepeatedPtrField<curvefs::mds::topology::MetadataUsage>*
        spaces) {
    ReadLockGuard rlockMetaServerMap(metaServerMutex_);
    for (auto const& i : metaServerMap_) {
        ReadLockGuard rlockMetaServer(i.second.GetRWLockRef());
        auto metaServerUsage = new curvefs::mds::topology::MetadataUsage();
        metaServerUsage->set_metaserveraddr(
            i.second.GetInternalHostIp() + ":" +
            std::to_string(i.second.GetInternalPort()));
        auto const& space = i.second.GetMetaServerSpace();
        metaServerUsage->set_total(space.GetDiskCapacity());
        metaServerUsage->set_used(space.GetDiskUsed());
        spaces->AddAllocated(metaServerUsage);
    }
}

std::string TopologyImpl::GetHostNameAndPortById(MetaServerIdType msId) {
    // get target metaserver
    MetaServer ms;
    if (!GetMetaServer(msId, &ms)) {
        LOG(INFO) << "get metaserver " << msId << " err";
        return "";
    }

    // get the server of the target metaserver
    Server server;
    if (!GetServer(ms.GetServerId(), &server)) {
        LOG(INFO) << "get server " << ms.GetServerId() << " err";
        return "";
    }

    // get hostName of the metaserver
    return server.GetHostName() + ":" + std::to_string(ms.GetInternalPort());
}
}  // namespace topology
}  // namespace mds
}  // namespace curvefs
