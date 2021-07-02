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
 * Created Date: Fri Aug 17 2018
 * Author: xuchaojie
 */
#include "src/mds/topology/topology.h"
#include <glog/logging.h>
#include "src/common/timeutility.h"
#include "src/common/uuid.h"

#include <chrono>  //NOLINT

using ::curve::common::UUIDGenerator;

namespace curve {
namespace mds {
namespace topology {

PoolIdType TopologyImpl::AllocateLogicalPoolId() {
    return idGenerator_->GenLogicalPoolId();
}

PoolIdType TopologyImpl::AllocatePhysicalPoolId() {
    return idGenerator_->GenPhysicalPoolId();
}

ZoneIdType TopologyImpl::AllocateZoneId() {
    return idGenerator_->GenZoneId();
}

ServerIdType TopologyImpl::AllocateServerId() {
    return idGenerator_->GenServerId();
}

ChunkServerIdType TopologyImpl::AllocateChunkServerId() {
    return idGenerator_->GenChunkServerId();
}

std::string TopologyImpl::AllocateToken() {
    return tokenGenerator_->GenToken();
}

int TopologyImpl::AddLogicalPool(const LogicalPool &data) {
    ReadLockGuard rlockPhysicalPool(physicalPoolMutex_);
    WriteLockGuard wlockLogicalPool(logicalPoolMutex_);
    auto it = physicalPoolMap_.find(data.GetPhysicalPoolId());
    if (it != physicalPoolMap_.end()) {
        if (logicalPoolMap_.find(data.GetId()) == logicalPoolMap_.end()) {
            if (!storage_->StorageLogicalPool(data)) {
                return kTopoErrCodeStorgeFail;
            }
            logicalPoolMap_[data.GetId()] = data;
            return kTopoErrCodeSuccess;
        } else {
            return kTopoErrCodeIdDuplicated;
        }
    } else {
        return kTopoErrCodePhysicalPoolNotFound;
    }
}

int TopologyImpl::AddPhysicalPool(const PhysicalPool &data) {
    WriteLockGuard wlockPhysicalPool(physicalPoolMutex_);
    if (physicalPoolMap_.find(data.GetId()) == physicalPoolMap_.end()) {
        if (!storage_->StoragePhysicalPool(data)) {
            return kTopoErrCodeStorgeFail;
        }
        physicalPoolMap_[data.GetId()] = data;
        return kTopoErrCodeSuccess;
    } else {
        return kTopoErrCodeIdDuplicated;
    }
}

int TopologyImpl::AddZone(const Zone &data) {
    ReadLockGuard rlockPhysicalPool(physicalPoolMutex_);
    WriteLockGuard wlockZone(zoneMutex_);
    auto it = physicalPoolMap_.find(data.GetPhysicalPoolId());
    if (it != physicalPoolMap_.end()) {
        if (zoneMap_.find(data.GetId()) == zoneMap_.end()) {
            if (!storage_->StorageZone(data)) {
                return kTopoErrCodeStorgeFail;
            }
            it->second.AddZone(data.GetId());
            zoneMap_[data.GetId()] = data;
            return kTopoErrCodeSuccess;
        } else {
            return kTopoErrCodeIdDuplicated;
        }
    } else {
        return kTopoErrCodePhysicalPoolNotFound;
    }
}

int TopologyImpl::AddServer(const Server &data) {
    ReadLockGuard rlockZone(zoneMutex_);
    WriteLockGuard wlockServer(serverMutex_);
    auto it = zoneMap_.find(data.GetZoneId());
    if (it != zoneMap_.end()) {
        if (serverMap_.find(data.GetId()) == serverMap_.end()) {
            if (!storage_->StorageServer(data)) {
                return kTopoErrCodeStorgeFail;
            }
            it->second.AddServer(data.GetId());
            serverMap_[data.GetId()] = data;
            return kTopoErrCodeSuccess;
        } else {
            return kTopoErrCodeIdDuplicated;
        }
    } else {
        return kTopoErrCodeZoneNotFound;
    }
}

int TopologyImpl::AddChunkServer(const ChunkServer &data) {
    // find the physical pool that the chunkserver belongs to
    PoolIdType belongPhysicalPoolId = UNINTIALIZE_ID;
    int ret = GetBelongPhysicalPoolIdByServerId(
        data.GetServerId(), &belongPhysicalPoolId);
    if (ret != kTopoErrCodeSuccess) {
        return ret;
    }
    // fetch lock on physical pool, server and chunkserver
    WriteLockGuard wlockPhysicalPool(physicalPoolMutex_);
    uint64_t csCapacity = 0;
    {
        ReadLockGuard rlockServer(serverMutex_);
        WriteLockGuard wlockChunkServer(chunkServerMutex_);
        auto it = serverMap_.find(data.GetServerId());
        if (it != serverMap_.end()) {
            if (chunkServerMap_.find(data.GetId()) == chunkServerMap_.end()) {
                if (!storage_->StorageChunkServer(data)) {
                    return kTopoErrCodeStorgeFail;
                }
                it->second.AddChunkServer(data.GetId());
                chunkServerMap_[data.GetId()] = data;
                csCapacity = data.GetChunkServerState().GetDiskCapacity();
            } else {
                return kTopoErrCodeIdDuplicated;
            }
        } else {
            return kTopoErrCodeServerNotFound;
        }
    }
    // update physical pool
    auto it = physicalPoolMap_.find(belongPhysicalPoolId);
    if (it != physicalPoolMap_.end()) {
        uint64_t totalCapacity = it->second.GetDiskCapacity();
        totalCapacity += csCapacity;
        it->second.SetDiskCapacity(totalCapacity);
    } else {
        return kTopoErrCodePhysicalPoolNotFound;
    }
    return kTopoErrCodeSuccess;
}

int TopologyImpl::RemoveLogicalPool(PoolIdType id) {
    WriteLockGuard wlockLogicalPool(logicalPoolMutex_);
    auto it = logicalPoolMap_.find(id);
    if (it != logicalPoolMap_.end()) {
        if (!storage_->DeleteLogicalPool(id)) {
            return kTopoErrCodeStorgeFail;
        }
        logicalPoolMap_.erase(it);
        return kTopoErrCodeSuccess;
    } else {
        return kTopoErrCodeLogicalPoolNotFound;
    }
}

int TopologyImpl::RemovePhysicalPool(PoolIdType id) {
    WriteLockGuard wlockPhysicalPool(physicalPoolMutex_);
    auto it = physicalPoolMap_.find(id);
    if (it != physicalPoolMap_.end()) {
        if (it->second.GetZoneList().size() != 0) {
            return kTopoErrCodeCannotRemoveWhenNotEmpty;
        }
        if (!storage_->DeletePhysicalPool(id)) {
            return kTopoErrCodeStorgeFail;
        }
        physicalPoolMap_.erase(it);
        return kTopoErrCodeSuccess;
    } else {
        return kTopoErrCodePhysicalPoolNotFound;
    }
}

int TopologyImpl::RemoveZone(ZoneIdType id) {
    WriteLockGuard wlockPhysicalPool(physicalPoolMutex_);
    WriteLockGuard wlockZone(zoneMutex_);
    auto it = zoneMap_.find(id);
    if (it != zoneMap_.end()) {
        if (it->second.GetServerList().size() != 0) {
            return kTopoErrCodeCannotRemoveWhenNotEmpty;
        }
        if (!storage_->DeleteZone(id)) {
            return kTopoErrCodeStorgeFail;
        }
        auto ix = physicalPoolMap_.find(it->second.GetPhysicalPoolId());
        if (ix != physicalPoolMap_.end()) {
            ix->second.RemoveZone(id);
        }
        zoneMap_.erase(it);
        return kTopoErrCodeSuccess;
    } else {
        return kTopoErrCodeZoneNotFound;
    }
}

int TopologyImpl::RemoveServer(ServerIdType id) {
    WriteLockGuard wlockZone(zoneMutex_);
    WriteLockGuard wlockServer(serverMutex_);
    auto it = serverMap_.find(id);
    if (it != serverMap_.end()) {
        if (it->second.GetChunkServerList().size() != 0) {
            return kTopoErrCodeCannotRemoveWhenNotEmpty;
        }
        if (!storage_->DeleteServer(id)) {
            return kTopoErrCodeStorgeFail;
        }
        auto ix = zoneMap_.find(it->second.GetZoneId());
        if (ix != zoneMap_.end()) {
            ix->second.RemoveServer(id);
        }
        serverMap_.erase(it);
        return kTopoErrCodeSuccess;
    } else {
        return kTopoErrCodeServerNotFound;
    }
}

int TopologyImpl::RemoveChunkServer(ChunkServerIdType id) {
    WriteLockGuard wlockServer(serverMutex_);
    WriteLockGuard wlockChunkServer(chunkServerMutex_);
    auto it = chunkServerMap_.find(id);
    if (it != chunkServerMap_.end()) {
        if (it->second.GetStatus() != ChunkServerStatus::RETIRED) {
            return kTopoErrCodeCannotRemoveNotRetired;
        }
        if (!storage_->DeleteChunkServer(id)) {
            return kTopoErrCodeStorgeFail;
        }
        auto ix = serverMap_.find(it->second.GetServerId());
        if (ix != serverMap_.end()) {
            ix->second.RemoveChunkServer(id);
        }
        chunkServerMap_.erase(it);
        return kTopoErrCodeSuccess;
    } else {
        return kTopoErrCodeChunkServerNotFound;
    }
}

int TopologyImpl::UpdateLogicalPool(const LogicalPool &data) {
    WriteLockGuard wlockLogicalPool(logicalPoolMutex_);
    auto it = logicalPoolMap_.find(data.GetId());
    if (it != logicalPoolMap_.end()) {
        if (!storage_->UpdateLogicalPool(data)) {
            return kTopoErrCodeStorgeFail;
        }
        it->second = data;
        return kTopoErrCodeSuccess;
    } else {
        return kTopoErrCodeLogicalPoolNotFound;
    }
}

int TopologyImpl::UpdateLogicalPoolAllocateStatus(const AllocateStatus &status,
                                    PoolIdType id) {
    WriteLockGuard wlockLogicalPool(logicalPoolMutex_);
    auto it = logicalPoolMap_.find(id);
    if (it != logicalPoolMap_.end()) {
        LogicalPool temp = it->second;
        temp.SetStatus(status);
        if (!storage_->UpdateLogicalPool(temp)) {
            return kTopoErrCodeStorgeFail;
        }
        it->second.SetStatus(status);
        return kTopoErrCodeSuccess;
    } else {
        return kTopoErrCodeLogicalPoolNotFound;
    }
}

int TopologyImpl::UpdateLogicalPoolScanState(PoolIdType lpid, bool scanEnable) {
    WriteLockGuard wlockLogicalPool(logicalPoolMutex_);

    auto iter = logicalPoolMap_.find(lpid);
    if (iter == logicalPoolMap_.end()) {
        return kTopoErrCodeLogicalPoolNotFound;
    }

    LogicalPool lpool = iter->second;
    lpool.SetScanEnable(scanEnable);
    if (!storage_->UpdateLogicalPool(lpool)) {
        return kTopoErrCodeStorgeFail;
    }

    iter->second.SetScanEnable(scanEnable);
    return kTopoErrCodeSuccess;
}

int TopologyImpl::UpdatePhysicalPool(const PhysicalPool &data) {
    WriteLockGuard wlockPhysicalPool(physicalPoolMutex_);
    auto it = physicalPoolMap_.find(data.GetId());
    if (it != physicalPoolMap_.end()) {
        if (!storage_->UpdatePhysicalPool(data)) {
            return kTopoErrCodeStorgeFail;
        }
        it->second = data;
        return kTopoErrCodeSuccess;
    } else {
        return kTopoErrCodePhysicalPoolNotFound;
    }
}

int TopologyImpl::UpdateZone(const Zone &data) {
    WriteLockGuard wlockZone(zoneMutex_);
    auto it = zoneMap_.find(data.GetId());
    if (it != zoneMap_.end()) {
        if (!storage_->UpdateZone(data)) {
            return kTopoErrCodeStorgeFail;
        }
        it->second = data;
        return kTopoErrCodeSuccess;
    } else {
        return kTopoErrCodeZoneNotFound;
    }
}

int TopologyImpl::UpdateServer(const Server &data) {
    WriteLockGuard wlockServer(serverMutex_);
    auto it = serverMap_.find(data.GetId());
    if (it != serverMap_.end()) {
        if (!storage_->UpdateServer(data)) {
            return kTopoErrCodeStorgeFail;
        }
        it->second = data;
        return kTopoErrCodeSuccess;
    } else {
        return kTopoErrCodeServerNotFound;
    }
}

int TopologyImpl::UpdateChunkServerTopo(const ChunkServer &data) {
    ReadLockGuard rlockChunkServerMap(chunkServerMutex_);
    auto it = chunkServerMap_.find(data.GetId());
    if (it != chunkServerMap_.end()) {
        WriteLockGuard wlockChunkServer(it->second.GetRWLockRef());
        ChunkServer temp = it->second;
        temp.SetServerId(data.GetServerId());
        temp.SetHostIp(data.GetHostIp());
        temp.SetPort(data.GetPort());
        temp.SetMountPoint(data.GetMountPoint());
        if (!storage_->UpdateChunkServer(temp)) {
            return kTopoErrCodeStorgeFail;
        }
        it->second = temp;
        it->second.SetDirtyFlag(false);
        return kTopoErrCodeSuccess;
    } else {
        return kTopoErrCodeChunkServerNotFound;
    }
}

int TopologyImpl::UpdateChunkServerRwState(const ChunkServerStatus &rwState,
                              ChunkServerIdType id) {
    // find physical pool that it belongs to
    PoolIdType belongPhysicalPoolId = UNINTIALIZE_ID;
    int ret = GetBelongPhysicalPoolId(id, &belongPhysicalPoolId);
    if (ret != kTopoErrCodeSuccess) {
        return ret;
    }
    // fetch write lock on physical pool and read lock on chunkserver map
    WriteLockGuard wlockPhysicalPool(physicalPoolMutex_);
    ChunkServerStatus lastRwState;
    uint64_t csCapacity = 0;
    {
        ReadLockGuard rlockChunkServerMap(chunkServerMutex_);
        // update chunkserver
        auto it = chunkServerMap_.find(id);
        if (it == chunkServerMap_.end()) {
            return kTopoErrCodeChunkServerNotFound;
        } else {
            WriteLockGuard wlockChunkServer(it->second.GetRWLockRef());
            lastRwState = it->second.GetStatus();
            csCapacity = it->second.GetChunkServerState().GetDiskCapacity();
            it->second.SetStatus(rwState);
            it->second.SetDirtyFlag(true);
        }
    }
    // update physical pool
    switch (lastRwState) {
        case ChunkServerStatus::READWRITE:
        case ChunkServerStatus::PENDDING:
            // a disk is going to retire, remove corresponding capacity
            // from physical pool
            if (ChunkServerStatus::RETIRED == rwState) {
                auto it = physicalPoolMap_.find(belongPhysicalPoolId);
                if (it != physicalPoolMap_.end()) {
                    uint64_t totalCapacity = it->second.GetDiskCapacity();
                    totalCapacity = (totalCapacity > csCapacity) ?
                        (totalCapacity - csCapacity) : 0;
                    it->second.SetDiskCapacity(totalCapacity);
                } else {
                    return kTopoErrCodePhysicalPoolNotFound;
                }
            }
            break;
        case ChunkServerStatus::RETIRED:
            // disk recover from retired status, add corresponding capacity
            if (ChunkServerStatus::READWRITE == rwState ||
                ChunkServerStatus::PENDDING == rwState) {
                auto it = physicalPoolMap_.find(belongPhysicalPoolId);
                if (it != physicalPoolMap_.end()) {
                    uint64_t totalCapacity = it->second.GetDiskCapacity();
                    totalCapacity += csCapacity;
                    it->second.SetDiskCapacity(totalCapacity);
                } else {
                    return kTopoErrCodePhysicalPoolNotFound;
                }
            }
            break;
        default:
            return kTopoErrCodeInternalError;
    }
    return kTopoErrCodeSuccess;
}

int TopologyImpl::GetBelongPhysicalPoolIdByServerId(ServerIdType serverId,
    PoolIdType *physicalPoolIdOut) {
    *physicalPoolIdOut = UNINTIALIZE_ID;
    Server server;
    if (!GetServer(serverId, &server)) {
        LOG(ERROR) << "TopologyImpl::GetBelongPhysicalPoolId "
                   << "Fail On GetServer, "
                   << "serverId = " << serverId;
        return kTopoErrCodeServerNotFound;
    }
    Zone zone;
    if (!GetZone(server.GetZoneId(), &zone)) {
        LOG(ERROR) << "TopologyImpl::GetBelongPhysicalPoolId "
                   << "Fail On GetZone, "
                   << "zoneId = " << server.GetZoneId();
        return kTopoErrCodeZoneNotFound;
    }
    *physicalPoolIdOut = zone.GetPhysicalPoolId();
    return kTopoErrCodeSuccess;
}

int TopologyImpl::GetBelongPhysicalPoolId(ChunkServerIdType csId,
    PoolIdType *physicalPoolIdOut) {
    *physicalPoolIdOut = UNINTIALIZE_ID;
    ChunkServer cs;
    if (!GetChunkServer(csId, &cs)) {
        LOG(ERROR) << "TopologyImpl::GetBelongPhysicalPoolId "
                   << "Fail On GetChunkServer, "
                   << "chunkserverId = " << csId;
        return kTopoErrCodeChunkServerNotFound;
    }
    return GetBelongPhysicalPoolIdByServerId(
        cs.GetServerId(), physicalPoolIdOut);
}

int TopologyImpl::UpdateChunkServerOnlineState(const OnlineState &onlineState,
                                    ChunkServerIdType id) {
    ReadLockGuard rlockChunkServerMap(chunkServerMutex_);
    auto it = chunkServerMap_.find(id);
    if (it != chunkServerMap_.end()) {
        WriteLockGuard wlockChunkServer(it->second.GetRWLockRef());
        it->second.SetOnlineState(onlineState);
        it->second.SetDirtyFlag(true);
        return kTopoErrCodeSuccess;
    } else {
        return kTopoErrCodeChunkServerNotFound;
    }
}

int TopologyImpl::UpdateChunkServerDiskStatus(const ChunkServerState &state,
                                   ChunkServerIdType id) {
    // find physical pool it belongs to
    PoolIdType belongPhysicalPoolId = UNINTIALIZE_ID;
    int ret = GetBelongPhysicalPoolId(id, &belongPhysicalPoolId);
    if (ret != kTopoErrCodeSuccess) {
        return ret;
    }

    // fetch write lock of the physical pool and read lock of chunkserver map
    WriteLockGuard wlockPhysicalPool(physicalPoolMutex_);
    int64_t diff = 0;
    {
        ReadLockGuard rlockChunkServerMap(chunkServerMutex_);
        auto it = chunkServerMap_.find(id);
        if (it != chunkServerMap_.end()) {
            WriteLockGuard wlockChunkServer(it->second.GetRWLockRef());
            diff = state.GetDiskCapacity() -
                it->second.GetChunkServerState().GetDiskCapacity();
            // only data in RAM is updated, data will be synchronized to
            // database by background process regularly
            it->second.SetChunkServerState(state);
            it->second.SetDirtyFlag(true);
        } else {
            return kTopoErrCodeChunkServerNotFound;
        }
    }
    // update physical pool
    auto it = physicalPoolMap_.find(belongPhysicalPoolId);
    if (it != physicalPoolMap_.end()) {
        uint64_t totalCapacity = it->second.GetDiskCapacity();
        totalCapacity += diff;
        it->second.SetDiskCapacity(totalCapacity);
    } else {
        return kTopoErrCodePhysicalPoolNotFound;
    }
    return kTopoErrCodeSuccess;
}

int TopologyImpl::UpdateChunkServerStartUpTime(uint64_t time,
                     ChunkServerIdType id) {
    ReadLockGuard rlockChunkServerMap(chunkServerMutex_);
    auto it = chunkServerMap_.find(id);
    if (it != chunkServerMap_.end()) {
        WriteLockGuard wlockChunkServer(it->second.GetRWLockRef());
        it->second.SetStartUpTime(time);
        return kTopoErrCodeSuccess;
    } else {
        return kTopoErrCodeChunkServerNotFound;
    }
}

PoolIdType TopologyImpl::FindLogicalPool(
    const std::string &logicalPoolName,
    const std::string &physicalPoolName) const {
    PoolIdType physicalPoolId = FindPhysicalPool(physicalPoolName);
    ReadLockGuard rlockLogicalPool(logicalPoolMutex_);
    for (auto it = logicalPoolMap_.begin();
         it != logicalPoolMap_.end();
         it++) {
        if ((it->second.GetPhysicalPoolId() == physicalPoolId) &&
            (it->second.GetName() == logicalPoolName)) {
            return it->first;
        }
    }
    return static_cast<PoolIdType>(UNINTIALIZE_ID);
}

PoolIdType TopologyImpl::FindPhysicalPool(
    const std::string &physicalPoolName) const {
    ReadLockGuard rlockPhysicalPool(physicalPoolMutex_);
    for (auto it = physicalPoolMap_.begin();
         it != physicalPoolMap_.end();
         it++) {
        if (it->second.GetName() == physicalPoolName) {
            return it->first;
        }
    }
    return static_cast<PoolIdType>(UNINTIALIZE_ID);
}

ZoneIdType TopologyImpl::FindZone(const std::string &zoneName,
                                  const std::string &physicalPoolName) const {
    PoolIdType physicalPoolId = FindPhysicalPool(physicalPoolName);
    return FindZone(zoneName, physicalPoolId);
}

ZoneIdType TopologyImpl::FindZone(const std::string &zoneName,
                                  PoolIdType physicalPoolId) const {
    ReadLockGuard rlockZone(zoneMutex_);
    for (auto it = zoneMap_.begin(); it != zoneMap_.end(); it++) {
        if ((it->second.GetPhysicalPoolId() == physicalPoolId) &&
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

ServerIdType TopologyImpl::FindServerByHostIpPort(
    const std::string &hostIp,
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

ChunkServerIdType TopologyImpl::FindChunkServerNotRetired(
    const std::string &hostIp,
    uint32_t port) const {
    ServerIdType serverId = FindServerByHostIpPort(hostIp, port);
    ReadLockGuard rlockChunkServerMap(chunkServerMutex_);
    for (auto it = chunkServerMap_.begin();
         it != chunkServerMap_.end();
         it++) {
        ReadLockGuard rlockChunkServer(it->second.GetRWLockRef());
        if ((it->second.GetStatus() != ChunkServerStatus::RETIRED) &&
            (it->second.GetServerId() == serverId) &&
            (it->second.GetPort() == port)) {
            return it->first;
        }
    }
    return static_cast<ChunkServerIdType>(UNINTIALIZE_ID);
}

bool TopologyImpl::GetLogicalPool(PoolIdType poolId, LogicalPool *out) const {
    ReadLockGuard rlockLogicalPool(logicalPoolMutex_);
    auto it = logicalPoolMap_.find(poolId);
    if (it != logicalPoolMap_.end()) {
        *out = it->second;
        return true;
    }
    return false;
}

bool TopologyImpl::GetPhysicalPool(PoolIdType poolId, PhysicalPool *out) const {
    ReadLockGuard rlockPhysicalPool(physicalPoolMutex_);
    auto it = physicalPoolMap_.find(poolId);
    if (it != physicalPoolMap_.end()) {
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

bool TopologyImpl::GetChunkServer(ChunkServerIdType chunkserverId,
                                  ChunkServer *out) const {
    ReadLockGuard rlockChunkServerMap(chunkServerMutex_);
    auto it = chunkServerMap_.find(chunkserverId);
    if (it != chunkServerMap_.end()) {
        ReadLockGuard rlockChunkServer(it->second.GetRWLockRef());
        *out = it->second;
        return true;
    }
    return false;
}


////////////////////////////////////////////////////////////////////////////////
// getList

std::vector<ChunkServerIdType> TopologyImpl::GetChunkServerInCluster(
    ChunkServerFilter filter) const {
    std::vector<ChunkServerIdType> ret;
    ReadLockGuard rlockChunkServerMap(chunkServerMutex_);
    for (auto it = chunkServerMap_.begin();
         it != chunkServerMap_.end();
         it++) {
        ReadLockGuard rlockChunkServer(it->second.GetRWLockRef());
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

std::vector<PoolIdType> TopologyImpl::GetPhysicalPoolInCluster(
    PhysicalPoolFilter filter) const {
    std::vector<PoolIdType> ret;
    ReadLockGuard rlockPhysicalPool(physicalPoolMutex_);
    for (auto it = physicalPoolMap_.begin();
         it != physicalPoolMap_.end();
         it++) {
        if (filter(it->second)) {
            ret.push_back(it->first);
        }
    }
    return ret;
}

std::vector<PoolIdType> TopologyImpl::GetLogicalPoolInCluster(
    LogicalPoolFilter filter) const {
    std::vector<PoolIdType> ret;
    ReadLockGuard rlockLogicalPool(logicalPoolMutex_);
    for (auto it = logicalPoolMap_.begin();
         it != logicalPoolMap_.end();
         it++) {
        if (filter(it->second)) {
            ret.push_back(it->first);
        }
    }
    return ret;
}

std::list<ChunkServerIdType> TopologyImpl::GetChunkServerInServer(
    ServerIdType id,
    ChunkServerFilter filter) const {
    std::list<ChunkServerIdType> ret;
    ReadLockGuard rlockChunkServerMap(chunkServerMutex_);
    for (auto it = chunkServerMap_.begin();
         it != chunkServerMap_.end();
         it++) {
        ReadLockGuard rlockChunkServer(it->second.GetRWLockRef());
        if (filter(it->second) && it->second.GetServerId() == id) {
            ret.push_back(it->first);
        }
    }
    return ret;
}

std::list<ChunkServerIdType> TopologyImpl::GetChunkServerInZone(
    ZoneIdType id,
    ChunkServerFilter filter) const {
    std::list<ChunkServerIdType> ret;
    std::list<ServerIdType> serverList = GetServerInZone(id);
    for (ServerIdType s : serverList) {
        std::list<ChunkServerIdType> temp = GetChunkServerInServer(s, filter);
        ret.splice(ret.begin(), temp);
    }
    return ret;
}

std::list<ChunkServerIdType> TopologyImpl::GetChunkServerInPhysicalPool(
    PoolIdType id,
    ChunkServerFilter filter) const {
    std::list<ChunkServerIdType> ret;
    std::list<ServerIdType> serverList = GetServerInPhysicalPool(id);
    for (ServerIdType s : serverList) {
        std::list<ChunkServerIdType> temp = GetChunkServerInServer(s, filter);
        ret.splice(ret.begin(), temp);
    }
    return ret;
}

std::list<ServerIdType> TopologyImpl::GetServerInZone(ZoneIdType id,
    ServerFilter filter) const {
    std::list<ServerIdType> ret;
    ReadLockGuard rlockServer(serverMutex_);
    for (auto it = serverMap_.begin(); it != serverMap_.end(); it++) {
        if (filter(it->second) && it->second.GetZoneId() == id) {
            ret.push_back(it->first);
        }
    }
    return ret;
}

std::list<ServerIdType> TopologyImpl::GetServerInPhysicalPool(
    PoolIdType id,
    ServerFilter filter) const {
    std::list<ServerIdType> ret;
    std::list<ZoneIdType> zoneList = GetZoneInPhysicalPool(id);
    for (ZoneIdType z : zoneList) {
        std::list<ServerIdType> temp = GetServerInZone(z, filter);
        ret.splice(ret.begin(), temp);
    }
    return ret;
}

std::list<ZoneIdType> TopologyImpl::GetZoneInPhysicalPool(PoolIdType id,
    ZoneFilter filter) const {
    std::list<ZoneIdType> ret;
    ReadLockGuard rlockZone(zoneMutex_);
    for (auto it = zoneMap_.begin(); it != zoneMap_.end(); it++) {
        if (filter(it->second) && it->second.GetPhysicalPoolId() == id) {
            ret.push_back(it->first);
        }
    }
    return ret;
}

std::list<PoolIdType> TopologyImpl::GetLogicalPoolInPhysicalPool(
    PoolIdType id,
    LogicalPoolFilter filter) const {
    std::list<PoolIdType> ret;
    ReadLockGuard rlockLogicalPool(logicalPoolMutex_);
    for (auto it = logicalPoolMap_.begin(); it != logicalPoolMap_.end(); it++) {
        if (filter(it->second) && it->second.GetPhysicalPoolId() == id) {
            ret.push_back(it->first);
        }
    }
    return ret;
}

std::list<ChunkServerIdType> TopologyImpl::GetChunkServerInLogicalPool(
    PoolIdType id,
    ChunkServerFilter filter) const {
    LogicalPool lPool;
    if (GetLogicalPool(id, &lPool)) {
        return GetChunkServerInPhysicalPool(lPool.GetPhysicalPoolId(), filter);
    }
    return std::list<ChunkServerIdType>();
}

std::list<ServerIdType> TopologyImpl::GetServerInLogicalPool(
    PoolIdType id,
    ServerFilter filter) const {
    LogicalPool lPool;
    if (GetLogicalPool(id, &lPool)) {
        return GetServerInPhysicalPool(lPool.GetPhysicalPoolId(), filter);
    }
    return std::list<ServerIdType>();
}

std::list<ZoneIdType> TopologyImpl::GetZoneInLogicalPool(PoolIdType id,
    ZoneFilter filter) const {
    LogicalPool lPool;
    if (GetLogicalPool(id, &lPool)) {
        return GetZoneInPhysicalPool(lPool.GetPhysicalPoolId(), filter);
    }
    return std::list<ZoneIdType>();
}

int TopologyImpl::Init(const TopologyOption &option) {
    option_ = option;

    int ret = LoadClusterInfo();
    if (ret != kTopoErrCodeSuccess) {
        LOG(ERROR) << "[TopologyImpl::init], LoadClusterInfo fail.";
        return ret;
    }

    WriteLockGuard wlockLogicalPool(logicalPoolMutex_);
    WriteLockGuard wlockPhysicalPool(physicalPoolMutex_);
    WriteLockGuard wlockZone(zoneMutex_);
    WriteLockGuard wlockServer(serverMutex_);
    WriteLockGuard wlockChunkServer(chunkServerMutex_);
    WriteLockGuard wlockCopySet(copySetMutex_);

    PoolIdType maxLogicalPoolId;
    if (!storage_->LoadLogicalPool(&logicalPoolMap_, &maxLogicalPoolId)) {
        LOG(ERROR) << "[TopologyImpl::init], LoadLogicalPool fail.";
        return kTopoErrCodeStorgeFail;
    }
    idGenerator_->initLogicalPoolIdGenerator(maxLogicalPoolId);
    LOG(INFO) << "[TopologyImpl::init], LoadLogicalPool success, "
              << "logicalPool num = " << logicalPoolMap_.size();

    PoolIdType maxPhysicalPoolId;
    if (!storage_->LoadPhysicalPool(&physicalPoolMap_, &maxPhysicalPoolId)) {
        LOG(ERROR) << "[TopologyImpl::init], LoadPhysicalPool fail.";
        return kTopoErrCodeStorgeFail;
    }
    idGenerator_->initPhysicalPoolIdGenerator(maxPhysicalPoolId);
    LOG(INFO) << "[TopologyImpl::init], LoadPhysicalPool success, "
              << "physicalPool num = " << physicalPoolMap_.size();

    ZoneIdType maxZoneId;
    if (!storage_->LoadZone(&zoneMap_, &maxZoneId)) {
        LOG(ERROR) << "[TopologyImpl::init], LoadZone fail.";
        return kTopoErrCodeStorgeFail;
    }
    idGenerator_->initZoneIdGenerator(maxZoneId);
    LOG(INFO) << "[TopologyImpl::init], LoadZone success, "
               << "zone num = " << zoneMap_.size();

    ServerIdType maxServerId;
    if (!storage_->LoadServer(&serverMap_, &maxServerId)) {
        LOG(ERROR) << "[TopologyImpl::init], LoadServer fail.";
        return kTopoErrCodeStorgeFail;
    }
    idGenerator_->initServerIdGenerator(maxServerId);
    LOG(INFO) << "[TopologyImpl::init], LoadServer success, "
              << "server num = " << serverMap_.size();

    ChunkServerIdType maxChunkServerId;
    if (!storage_->LoadChunkServer(&chunkServerMap_, &maxChunkServerId)) {
        LOG(ERROR) << "[TopologyImpl::init], LoadChunkServer fail.";
        return kTopoErrCodeStorgeFail;
    }
    SetChunkServerExternalIp();
    idGenerator_->initChunkServerIdGenerator(maxChunkServerId);
    LOG(INFO) << "[TopologyImpl::init], LoadChunkServer success, "
              << "chunkserver num = " << chunkServerMap_.size();

    // update physical pool volume
    for (auto pair : chunkServerMap_) {
        ServerIdType serverId = pair.second.GetServerId();
        auto itServer = serverMap_.find(serverId);
        if (itServer == serverMap_.end()) {
            LOG(ERROR) << "TopologyImpl::Init "
                       << "Fail On Get Server, "
                       << "serverId = " << serverId;
            return kTopoErrCodeServerNotFound;
        }
        ZoneIdType zoneId = itServer->second.GetZoneId();
        auto itZone = zoneMap_.find(zoneId);
        if (itZone == zoneMap_.end()) {
            LOG(ERROR) << "TopologyImpl::Init "
                       << "Fail On Get zone, "
                       << "zoneId = " << zoneId;
            return kTopoErrCodeZoneNotFound;
        }
        PoolIdType physicalPoolId = itZone->second.GetPhysicalPoolId();
        auto it = physicalPoolMap_.find(physicalPoolId);
        if (it != physicalPoolMap_.end()) {
            uint64_t totalCapacity = it->second.GetDiskCapacity();
            totalCapacity +=
                pair.second.GetChunkServerState().GetDiskCapacity();
            it->second.SetDiskCapacity(totalCapacity);
        } else {
            LOG(ERROR) << "TopologyImpl::Init "
                       << "Fail On Get physicalPool, "
                       << "physicalPoolId = " << physicalPoolId;
            return kTopoErrCodePhysicalPoolNotFound;
        }
    }
    LOG(INFO) << "Calc physicalPool capacity success.";

    std::map<PoolIdType, CopySetIdType> copySetIdMaxMap;
    if (!storage_->LoadCopySet(&copySetMap_, &copySetIdMaxMap)) {
        LOG(ERROR) << "[TopologyImpl::init], LoadCopySet fail.";
        return kTopoErrCodeStorgeFail;
    }
    idGenerator_->initCopySetIdGenerator(copySetIdMaxMap);
    LOG(INFO) << "[TopologyImpl::init], LoadCopySet success, "
              << "copyset num = " << copySetMap_.size();

    for (auto it : zoneMap_) {
        PoolIdType poolid = it.second.GetPhysicalPoolId();
        physicalPoolMap_[poolid].AddZone(it.first);
    }

    for (auto it : serverMap_) {
        ZoneIdType zid = it.second.GetZoneId();
        zoneMap_[zid].AddServer(it.first);
    }

    for (auto it : chunkServerMap_) {
        ServerIdType sId = it.second.GetServerId();
        serverMap_[sId].AddChunkServer(it.first);
    }

    // remove invalid copyset and logicalPool
    ret = CleanInvalidLogicalPoolAndCopyset();

    if (kTopoErrCodeSuccess != ret) {
        LOG(ERROR) << "CleanInvalidLogicalPoolAndCopyset error, ret = " << ret;
        return ret;
    }
    LOG(INFO) << "Clean Invalid LogicalPool and copyset success.";

    return kTopoErrCodeSuccess;
}

void TopologyImpl::SetChunkServerExternalIp() {
    for (auto& it : chunkServerMap_) {
        Server server = serverMap_[it.second.GetServerId()];
        it.second.SetExternalHostIp(server.GetExternalHostIp());
    }
}

int TopologyImpl::CleanInvalidLogicalPoolAndCopyset() {
    for (auto ix = logicalPoolMap_.begin(); ix != logicalPoolMap_.end();) {
        if (false == ix->second.GetLogicalPoolAvaliableFlag()) {
            for (auto it = copySetMap_.begin(); it != copySetMap_.end();) {
                if (it->second.GetLogicalPoolId() == ix->first) {
                    if (!storage_->DeleteCopySet(it->first)) {
                        return kTopoErrCodeStorgeFail;
                    }
                    it = copySetMap_.erase(it);
                } else {
                    it++;
                }
            }
            if (!storage_->DeleteLogicalPool(ix->first)) {
                return kTopoErrCodeStorgeFail;
            }
            ix = logicalPoolMap_.erase(ix);
        } else {
            ix++;
        }
    }
    return kTopoErrCodeSuccess;
}


CopySetIdType TopologyImpl::AllocateCopySetId(PoolIdType logicalPoolId) {
    return idGenerator_->GenCopySetId(logicalPoolId);
}

int TopologyImpl::AddCopySet(const CopySetInfo &data) {
    ReadLockGuard rlockLogicalPool(logicalPoolMutex_);
    WriteLockGuard wlockCopySetMap(copySetMutex_);
    auto it = logicalPoolMap_.find(data.GetLogicalPoolId());
    if (it != logicalPoolMap_.end()) {
        CopySetKey key(data.GetLogicalPoolId(), data.GetId());
        if (copySetMap_.find(key) == copySetMap_.end()) {
            if (!storage_->StorageCopySet(data)) {
                return kTopoErrCodeStorgeFail;
            }
            copySetMap_[key] = data;
            return kTopoErrCodeSuccess;
        } else {
            return kTopoErrCodeIdDuplicated;
        }
    } else {
        return kTopoErrCodeLogicalPoolNotFound;
    }
}

int TopologyImpl::RemoveCopySet(CopySetKey key) {
    WriteLockGuard wlockCopySetMap(copySetMutex_);
    auto it = copySetMap_.find(key);
    if (it != copySetMap_.end()) {
        if (!storage_->DeleteCopySet(key)) {
            return kTopoErrCodeStorgeFail;
        }
        copySetMap_.erase(key);
        return kTopoErrCodeSuccess;
    } else {
        return kTopoErrCodeCopySetNotFound;
    }
}

int TopologyImpl::UpdateCopySetTopo(const CopySetInfo &data) {
    ReadLockGuard rlockCopySetMap(copySetMutex_);
    CopySetKey key(data.GetLogicalPoolId(), data.GetId());
    auto it = copySetMap_.find(key);
    if (it != copySetMap_.end()) {
        WriteLockGuard wlockCopySet(it->second.GetRWLockRef());
        it->second.SetLeader(data.GetLeader());
        it->second.SetEpoch(data.GetEpoch());
        it->second.SetCopySetMembers(data.GetCopySetMembers());
        if (data.HasCandidate()) {
            it->second.SetCandidate(data.GetCandidate());
        } else {
            it->second.ClearCandidate();
        }

        if (data.IsLatestScaning(it->second.GetScaning())) {
            it->second.SetScaning(data.GetScaning());
        }

        if (data.IsLatestLastScanSec(it->second.GetLastScanSec())) {
            it->second.SetLastScanSec(data.GetLastScanSec());
            it->second.SetLastScanConsistent(data.GetLastScanConsistent());
        }

        it->second.SetDirtyFlag(true);
        return kTopoErrCodeSuccess;
    } else {
        LOG(WARNING) << "UpdateCopySetTopo can not find copyset, "
                     << "logicalPoolId = " << data.GetLogicalPoolId()
                     << ", copysetId = " << data.GetId();
        return kTopoErrCodeCopySetNotFound;
    }
}

int TopologyImpl::SetCopySetAvalFlag(const CopySetKey &key, bool aval) {
    ReadLockGuard rlockCopySetMap(copySetMutex_);
    auto it = copySetMap_.find(key);
    if (it != copySetMap_.end()) {
        WriteLockGuard wlockCopySet(it->second.GetRWLockRef());
        auto copysetInfo = it->second;
        copysetInfo.SetAvailableFlag(aval);
        bool ret = storage_->UpdateCopySet(copysetInfo);
        if (!ret) {
            LOG(ERROR) << "UpdateCopySet met storage error";
            return kTopoErrCodeStorgeFail;
        }
        it->second.SetAvailableFlag(aval);
        return kTopoErrCodeSuccess;
    } else {
        LOG(WARNING) << "SetCopySetAvalFlag can not find copyset, "
                     << "logicalPoolId = " << key.first
                     << ", copysetId = " << key.second;
        return kTopoErrCodeCopySetNotFound;
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

std::vector<CopySetIdType> TopologyImpl::GetCopySetsInLogicalPool(
    PoolIdType logicalPoolId,
    CopySetFilter filter) const {
    std::vector<CopySetIdType> ret;
    ReadLockGuard rlockCopySet(copySetMutex_);
    for (auto it : copySetMap_) {
        if (filter(it.second) && it.first.first == logicalPoolId) {
            ret.push_back(it.first.second);
        }
    }
    return ret;
}

std::vector<CopySetInfo> TopologyImpl::GetCopySetInfosInLogicalPool(
    PoolIdType logicalPoolId,
    CopySetFilter filter) const {
    std::vector<CopySetInfo> ret;
    ReadLockGuard rlockCopySet(copySetMutex_);
    for (auto it : copySetMap_) {
        if (filter(it.second) && it.first.first == logicalPoolId) {
            ret.push_back(it.second);
        }
    }
    return ret;
}

std::vector<CopySetKey> TopologyImpl::GetCopySetsInCluster(
    CopySetFilter filter) const {
    std::vector<CopySetKey> ret;
    ReadLockGuard rlockCopySet(copySetMutex_);
    for (auto it : copySetMap_) {
        if (filter(it.second)) {
            ret.push_back(it.first);
        }
    }
    return ret;
}

std::vector<CopySetKey> TopologyImpl::GetCopySetsInChunkServer(
    ChunkServerIdType id,
    CopySetFilter filter) const {
    std::vector<CopySetKey> ret;
    ReadLockGuard rlockCopySet(copySetMutex_);
    for (auto it : copySetMap_) {
        if (filter(it.second) && it.second.GetCopySetMembers().count(id) > 0) {
            ret.push_back(it.first);
        }
    }
    return ret;
}

int TopologyImpl::Run() {
    if (isStop_.exchange(false)) {
        backEndThread_ = curve::common::Thread(
            &TopologyImpl::BackEndFunc, this);
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
         std::chrono::seconds(option_.TopologyUpdateToRepoSec))) {
        FlushCopySetToStorage();
        FlushChunkServerToStorage();
    }
}

void TopologyImpl::FlushCopySetToStorage() {
    std::vector<PoolIdType> pools = GetLogicalPoolInCluster();
    for (const auto poolId : pools) {
        ReadLockGuard rlockCopySetMap(copySetMutex_);
        for (auto &c : copySetMap_) {
            WriteLockGuard wlockCopySet(c.second.GetRWLockRef());
            if (c.second.GetDirtyFlag() &&
                        c.second.GetLogicalPoolId() == poolId) {
                c.second.SetDirtyFlag(false);
                if (!storage_->UpdateCopySet(c.second)) {
                    LOG(WARNING) << "update copyset("
                                 << c.second.GetLogicalPoolId()
                                 << "," << c.second.GetId() << ") to repo fail";
                }
            }
        }
    }
}

void TopologyImpl::FlushChunkServerToStorage() {
    std::vector<ChunkServer> toUpdate;
    {
        ReadLockGuard rlockChunkServerMap(chunkServerMutex_);
        for (auto &c : chunkServerMap_) {
            // update DirtyFlag only, thus only read lock is needed
            ReadLockGuard rlockChunkServer(c.second.GetRWLockRef());
            if (c.second.GetDirtyFlag()) {
                c.second.SetDirtyFlag(false);
                toUpdate.push_back(c.second);
            }
        }
    }
    for (auto &v : toUpdate) {
        if (!storage_->UpdateChunkServer(v)) {
            LOG(WARNING) << "update chunkserver to repo fail"
                         << ", chunkserverid = " << v.GetId();
        }
    }
}

int TopologyImpl::LoadClusterInfo() {
    std::vector<ClusterInformation> infos;
    if (!storage_->LoadClusterInfo(&infos)) {
        return kTopoErrCodeStorgeFail;
    }
    if (infos.empty()) {
        std::string uuid = UUIDGenerator().GenerateUUID();
        ClusterInformation info(uuid);
        if (!storage_->StorageClusterInfo(info)) {
            return kTopoErrCodeStorgeFail;
        }
        clusterInfo = info;
    } else {
        clusterInfo = infos[0];
    }
    return kTopoErrCodeSuccess;
}

bool TopologyImpl::GetClusterInfo(ClusterInformation *info) {
    *info = clusterInfo;
    return true;
}

}  // namespace topology
}  // namespace mds
}  // namespace curve

