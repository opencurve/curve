/*
 * Project:
 * Created Date: Mon Sep 03 2018
 * Author: xuchaojie
 * Copyright (c) 2018 netease
 */

#include "src/mds/topology/topology_storge.h"

#include <glog/logging.h>

#include <unordered_map>
#include <vector>
#include <map>
#include <set>
#include <string>
#include <utility>

#include "json/json.h"

namespace curve {
namespace mds {
namespace topology {

using ::curve::repo::OperationOK;

bool DefaultTopologyStorage::init(const TopologyOption &option) {
    return true;
}

bool DefaultTopologyStorage::LoadLogicalPool(
    std::unordered_map<PoolIdType,
                       LogicalPool> *logicalPoolMap,
    PoolIdType *maxLogicalPoolId) {
    std::vector<LogicalPoolRepoItem> logicalPoolRepos;
    if (repo_->LoadLogicalPoolRepoItems(&logicalPoolRepos) != OperationOK) {
        LOG(ERROR) << "[DefaultTopologyStorage::LoadLogicalPool]: "
                   << "LoadLogicalPoolRepos fail.";
        return false;
    }
    logicalPoolMap->clear();
    *maxLogicalPoolId = 0;
    for (LogicalPoolRepoItem &rp : logicalPoolRepos) {
        LogicalPool::RedundanceAndPlaceMentPolicy rap;
        LogicalPool::UserPolicy policy;

        if (!LogicalPool::TransRedundanceAndPlaceMentPolicyFromJsonStr(
            rp.redundanceAndPlacementPolicy,
            static_cast<LogicalPoolType>(rp.type),
            &rap)) {
            LOG(ERROR) << "[DefaultTopologyStorage::LoadLogicalPool]: "
                       << "parse redundanceAndPlacementPolicy string fail.";
            return false;
        }

        if (!LogicalPool::TransUserPolicyFromJsonStr(
            rp.userPolicy,
            static_cast<LogicalPoolType>(rp.type),
            &policy)) {
            LOG(ERROR) << "[DefaultTopologyStorage::LoadLogicalPool]: "
                       << "parse userPolicy string fail.";
            return false;
        }

        LogicalPool pool(rp.logicalPoolID,
                         rp.logicalPoolName,
                         rp.physicalPoolID,
                         static_cast<LogicalPoolType>(rp.type),
                         rap,
                         policy,
                         rp.createTime,
                         rp.availFlag);
        pool.SetStatus(static_cast<LogicalPool::LogicalPoolStatus>(rp.status));
        pool.SetScatterWidth(rp.initialScatterWidth);
        auto ret = logicalPoolMap->emplace(
            std::move(rp.logicalPoolID),
            std::move(pool));
        if (!ret.second) {
            LOG(ERROR) << "[DefaultTopologyStorage::LoadLogicalPool]: "
                       << "id duplicated, id ="
                       << rp.logicalPoolID;
            return false;
        }
        if (rp.logicalPoolID > *maxLogicalPoolId) {
            *maxLogicalPoolId = rp.logicalPoolID;
        }
    }
    return true;
}

bool DefaultTopologyStorage::LoadPhysicalPool(
    std::unordered_map<PoolIdType, PhysicalPool> *physicalPoolMap,
    PoolIdType *maxPhysicalPoolId) {
    std::vector<PhysicalPoolRepoItem> physicalPoolRepos;
    if (repo_->LoadPhysicalPoolRepoItems(&physicalPoolRepos) != OperationOK) {
        LOG(ERROR) << "[DefaultTopologyStorage::LoadPhysicalPool]: "
                   << "LoadPhysicalPoolRepos fail.";
        return false;
    }
    physicalPoolMap->clear();
    *maxPhysicalPoolId = 0;
    for (PhysicalPoolRepoItem &rp : physicalPoolRepos) {
        PhysicalPool pool(rp.physicalPoolID,
                          rp.physicalPoolName,
                          rp.desc);
        auto ret = physicalPoolMap->emplace(
            std::move(rp.physicalPoolID),
            std::move(pool));
        if (!ret.second) {
            LOG(ERROR) << "[DefaultTopologyStorage::LoadPhysicalPool]: "
                       << "id duplicated, id ="
                       << rp.physicalPoolID;
            return false;
        }
        if (rp.physicalPoolID > *maxPhysicalPoolId) {
            *maxPhysicalPoolId = rp.physicalPoolID;
        }
    }
    return true;
}

bool DefaultTopologyStorage::LoadZone(
    std::unordered_map<ZoneIdType, Zone> *zoneMap,
    ZoneIdType *maxZoneId) {
    std::vector<ZoneRepoItem> zoneRepos;
    if (repo_->LoadZoneRepoItems(&zoneRepos) != OperationOK) {
        LOG(ERROR) << "[DefaultTopologyStorage::LoadZone]: "
                   << "LoadZoneRepos fail.";
        return false;
    }
    zoneMap->clear();
    *maxZoneId = 0;
    for (ZoneRepoItem &rp : zoneRepos) {
        Zone zone(rp.zoneID,
                  rp.zoneName,
                  rp.poolID,
                  rp.desc);
        auto ret = zoneMap->emplace(
            std::move(rp.zoneID),
            std::move(zone));
        if (!ret.second) {
            LOG(ERROR) << "[DefaultTopologyStorage::LoadZone]: "
                       << "zoneId duplicated, zoneId = "
                       << rp.zoneID;
            return false;
        }
        if (rp.zoneID > *maxZoneId) {
            *maxZoneId = rp.zoneID;
        }
    }
    return true;
}

bool DefaultTopologyStorage::LoadServer(
    std::unordered_map<ServerIdType, Server> *serverMap,
    ServerIdType *maxServerId) {
    std::vector<ServerRepoItem> serverRepos;
    if (repo_->LoadServerRepoItems(&serverRepos) != OperationOK) {
        LOG(ERROR) << "[DefaultTopologyStorage::LoadServer]: "
                   << "LoadServerRepos fail.";
        return false;
    }
    serverMap->clear();
    *maxServerId = 0;
    for (ServerRepoItem &rp : serverRepos) {
        Server server(rp.serverID,
                      rp.hostName,
                      rp.internalHostIP,
                      rp.internalPort,
                      rp.externalHostIP,
                      rp.externalPort,
                      rp.zoneID,
                      rp.poolID,
                      rp.desc);
        auto ret = serverMap->emplace(
            std::move(rp.serverID),
            std::move(server));
        if (!ret.second) {
            LOG(ERROR) << "[DefaultTopologyStorage::LoadServer]: "
                       << "serverId duplicated, serverId = "
                       << rp.serverID;
            return false;
        }
        if (rp.serverID > *maxServerId) {
            *maxServerId = rp.serverID;
        }
    }
    return true;
}

bool DefaultTopologyStorage::LoadChunkServer(
    std::unordered_map<ChunkServerIdType, ChunkServer> *chunkServerMap,
    ChunkServerIdType *maxChunkServerId) {
    std::vector<ChunkServerRepoItem> chunkServerRepos;
    if (repo_->LoadChunkServerRepoItems(&chunkServerRepos) != OperationOK) {
        LOG(ERROR) << "[DefaultTopologyStorage::LoadChunkServer]: "
                   << "LoadChunkServerRepos fail.";
        return false;
    }
    chunkServerMap->clear();
    *maxChunkServerId = 0;
    for (ChunkServerRepoItem &rp : chunkServerRepos) {
        ChunkServer cs(rp.chunkServerID,
                       rp.token,
                       rp.diskType,
                       rp.serverID,
                       rp.internalHostIP,
                       rp.port,
                       rp.mountPoint,
                       static_cast<ChunkServerStatus>(rp.rwstatus),
                       OnlineState::UNSTABLE);
        ChunkServerState csState;
        csState.SetDiskState(static_cast<DiskState>(rp.diskState));
        csState.SetDiskCapacity(rp.capacity);
        csState.SetDiskUsed(rp.used);
        cs.SetChunkServerState(csState);
        auto ret = chunkServerMap->emplace(
            std::move(rp.chunkServerID),
            std::move(cs));
        if (!ret.second) {
            LOG(ERROR) << "[DefaultTopologyStorage::LoadChunkServer]: "
                       << "chunkServerID duplicated, id = "
                       << rp.chunkServerID;
            return false;
        }
        if (rp.chunkServerID > *maxChunkServerId) {
            *maxChunkServerId = rp.chunkServerID;
        }
    }
    return true;
}

bool DefaultTopologyStorage::LoadCopySet(
    std::map<CopySetKey, CopySetInfo> *copySetMap,
    std::map<PoolIdType, CopySetIdType> *copySetIdMaxMap) {
    std::vector<CopySetRepoItem> copySetRepos;
    if (repo_->LoadCopySetRepoItems(&copySetRepos) != OperationOK) {
        LOG(ERROR) << "[DefaultTopologyStorage::LoadCopySet]: "
                   << "LoadCopySetRepos fail.";
        return false;
    }
    copySetMap->clear();
    copySetIdMaxMap->clear();
    for (CopySetRepoItem &rp : copySetRepos) {
        CopySetInfo copyset(rp.logicalPoolID, rp.copySetID);
        copyset.SetEpoch(rp.epoch);
        if (!copyset.SetCopySetMembersByJson(rp.chunkServerIDList)) {
            LOG(ERROR) << "[DefaultTopologyStorage::LoadCopySet]: "
                       << "parse json string fail.";
            return false;
        }
        std::pair<PoolIdType, CopySetIdType> key(rp.logicalPoolID,
                                                 rp.copySetID);
        auto ret = copySetMap->emplace(std::move(key),
                                       std::move(copyset));
        if (!ret.second) {
            LOG(ERROR) << "[DefaultTopologyStorage::LoadCopySet]: "
                       << "copySet Id duplicated, logicalPoolId = "
                       << rp.logicalPoolID
                       << " , copySetId = "
                       << rp.copySetID;
            return false;
        }
        if ((*copySetIdMaxMap)[rp.logicalPoolID] < rp.copySetID) {
            (*copySetIdMaxMap)[rp.logicalPoolID] = rp.copySetID;
        }
    }
    return true;
}

bool DefaultTopologyStorage::StorageLogicalPool(const LogicalPool &data) {
    std::string rapStr =
        data.GetRedundanceAndPlaceMentPolicyJsonStr();
    std::string policyStr =
        data.GetUserPolicyJsonStr();
    LogicalPoolRepoItem rp(data.GetId(),
                       data.GetName(),
                       data.GetPhysicalPoolId(),
                       data.GetLogicalPoolType(),
                       data.GetScatterWidth(),
                       data.GetCreateTime(),
                       data.GetStatus(),
                       rapStr,
                       policyStr,
                       data.GetLogicalPoolAvaliableFlag());
    if (repo_->InsertLogicalPoolRepoItem(rp) != OperationOK) {
        LOG(ERROR) << "[DefaultTopologyStorage::StorageLogicalPool]: "
                   << "InsertLogicalPoolRepo fail.";
        return false;
    }
    return true;
}

bool DefaultTopologyStorage::StoragePhysicalPool(const PhysicalPool &data) {
    PhysicalPoolRepoItem rp(data.GetId(),
                        data.GetName(),
                        data.GetDesc());
    if (repo_->InsertPhysicalPoolRepoItem(rp) != OperationOK) {
        LOG(ERROR) << "[DefaultTopologyStorage::StoragePhysicalPool]: "
                   << "InsertPhysicalPoolRepo fail.";
        return false;
    }
    return true;
}

bool DefaultTopologyStorage::StorageZone(const Zone &data) {
    ZoneRepoItem rp(data.GetId(),
                data.GetName(),
                data.GetPhysicalPoolId(),
                data.GetDesc());
    if (repo_->InsertZoneRepoItem(rp) != OperationOK) {
        LOG(ERROR) << "[DefaultTopologyStorage::StorageZone]: "
                   << "InsertZoneRepo fail.";
        return false;
    }
    return true;
}

bool DefaultTopologyStorage::StorageServer(const Server &data) {
    ServerRepoItem rp(data.GetId(),
                  data.GetHostName(),
                  data.GetInternalHostIp(),
                  data.GetInternalPort(),
                  data.GetExternalHostIp(),
                  data.GetExternalPort(),
                  data.GetZoneId(),
                  data.GetPhysicalPoolId(),
                  data.GetDesc());
    if (repo_->InsertServerRepoItem(rp) != OperationOK) {
        LOG(ERROR) << "[DefaultTopologyStorage::StorageServer]: "
                   << "InsertServerRepo fail.";
        return false;
    }
    return true;
}

bool DefaultTopologyStorage::StorageChunkServer(const ChunkServer &data) {
    ChunkServerState csState = data.GetChunkServerState();
    ChunkServerRepoItem rp(data.GetId(),
                       data.GetToken(),
                       data.GetDiskType(),
                       data.GetHostIp(),
                       data.GetPort(),
                       data.GetServerId(),
                       data.GetStatus(),
                       csState.GetDiskState(),
                       data.GetOnlineState(),
                       data.GetMountPoint(),
                       csState.GetDiskCapacity(),
                       csState.GetDiskUsed());

    if (repo_->InsertChunkServerRepoItem(rp) != OperationOK) {
        LOG(ERROR) << "[DefaultTopologyStorage::StorageChunkServer]: "
                   << "StorageChunkServerRepo fail.";
        return false;
    }
    return true;
}

bool DefaultTopologyStorage::StorageCopySet(const CopySetInfo &data) {
    std::string chunkServerListStr = data.GetCopySetMembersStr();
    CopySetRepoItem rp(data.GetId(),
                   data.GetLogicalPoolId(),
                   data.GetEpoch(),
                   chunkServerListStr);
    if (repo_->InsertCopySetRepoItem(rp) != OperationOK) {
        LOG(ERROR) << "[DefaultTopologyStorage::StorageCopySet]: "
                   << "InsertCopySetRepo fail.";
        return false;
    }
    return true;
}

bool DefaultTopologyStorage::DeleteLogicalPool(PoolIdType id) {
    if (repo_->DeleteLogicalPoolRepoItem(id) != OperationOK) {
        LOG(ERROR) << "[DefaultTopologyStorage::DeleteLogicalPool]: "
                   << "DeleteLogicalPoolRepo fail.";
        return false;
    }
    return true;
}

bool DefaultTopologyStorage::DeletePhysicalPool(PoolIdType id) {
    if (repo_->DeletePhysicalPoolRepoItem(id) != OperationOK) {
        LOG(ERROR) << "[DefaultTopologyStorage::DeletePhysicalPool]: "
                   << "DeletePhysicalPoolRepo fail.";
        return false;
    }
    return true;
}

bool DefaultTopologyStorage::DeleteZone(ZoneIdType id) {
    if (repo_->DeleteZoneRepoItem(id) != OperationOK) {
        LOG(ERROR) << "[DefaultTopologyStorage::DeleteZone]: "
                   << "DeleteZoneRepo fail.";
        return false;
    }
    return true;
}

bool DefaultTopologyStorage::DeleteServer(ServerIdType id) {
    if (repo_->DeleteServerRepoItem(id) != OperationOK) {
        LOG(ERROR) << "[DefaultTopologyStorage::DeleteServer]: "
                   << "DeleteServerRepo fail.";
        return false;
    }
    return true;
}

bool DefaultTopologyStorage::DeleteChunkServer(ChunkServerIdType id) {
    if (repo_->DeleteChunkServerRepoItem(id) != OperationOK) {
        LOG(ERROR) << "[DefaultTopologyStorage::DeleteChunkServer]: "
                   << "DeleteChunkServerRepo fail.";
        return false;
    }
    return true;
}

bool DefaultTopologyStorage::DeleteCopySet(CopySetKey key) {
    if (repo_->DeleteCopySetRepoItem(key.second, key.first) != OperationOK) {
        LOG(ERROR) << "[DefaultTopologyStorage::DeleteCopySet]: "
                   << "DeleteCopySetRepo fail.";
        return false;
    }
    return true;
}

bool DefaultTopologyStorage::UpdateLogicalPool(const LogicalPool &data) {
    std::string rapStr =
        data.GetRedundanceAndPlaceMentPolicyJsonStr();
    std::string policyStr =
        data.GetUserPolicyJsonStr();
    LogicalPoolRepoItem rp(data.GetId(),
                       data.GetName(),
                       data.GetPhysicalPoolId(),
                       data.GetLogicalPoolType(),
                       data.GetScatterWidth(),
                       data.GetCreateTime(),
                       data.GetStatus(),
                       rapStr,
                       policyStr,
                       data.GetLogicalPoolAvaliableFlag());
    if (repo_->UpdateLogicalPoolRepoItem(rp) != OperationOK) {
        LOG(ERROR) << "[DefaultTopologyStorage::UpdateLogicalPool]: "
                   << "UpdateLogicalPoolRepo fail.";
        return false;
    }
    return true;
}

bool DefaultTopologyStorage::UpdatePhysicalPool(const PhysicalPool &data) {
    PhysicalPoolRepoItem rp(data.GetId(),
                        data.GetName(),
                        data.GetDesc());
    if (repo_->UpdatePhysicalPoolRepoItem(rp) != OperationOK) {
        LOG(ERROR) << "[DefaultTopologyStorage::UpdatePhysicalPool]: "
                   << "UpdatePhysicalPoolRepo fail.";
        return false;
    }
    return true;
}

bool DefaultTopologyStorage::UpdateZone(const Zone &data) {
    ZoneRepoItem rp(data.GetId(),
                data.GetName(),
                data.GetPhysicalPoolId(),
                data.GetDesc());
    if (repo_->UpdateZoneRepoItem(rp) != OperationOK) {
        LOG(ERROR) << "[DefaultTopologyStorage::UpdateZone]: "
                   << "UpdateZoneRepo fail.";
        return false;
    }
    return true;
}

bool DefaultTopologyStorage::UpdateServer(const Server &data) {
    ServerRepoItem rp(data.GetId(),
                  data.GetHostName(),
                  data.GetInternalHostIp(),
                  data.GetInternalPort(),
                  data.GetExternalHostIp(),
                  data.GetExternalPort(),
                  data.GetZoneId(),
                  data.GetPhysicalPoolId(),
                  data.GetDesc());
    if (repo_->UpdateServerRepoItem(rp) != OperationOK) {
        LOG(ERROR) << "[DefaultTopologyStorage::UpdateServer]: "
                   << "UpdateServerRepo fail.";
        return false;
    }
    return true;
}

bool DefaultTopologyStorage::UpdateChunkServer(const ChunkServer &data) {
    ChunkServerState csState = data.GetChunkServerState();
    ChunkServerRepoItem rp(data.GetId(),
                       data.GetToken(),
                       data.GetDiskType(),
                       data.GetHostIp(),
                       data.GetPort(),
                       data.GetServerId(),
                       data.GetStatus(),
                       csState.GetDiskState(),
                       data.GetOnlineState(),
                       data.GetMountPoint(),
                       csState.GetDiskCapacity(),
                       csState.GetDiskUsed());

    if (repo_->UpdateChunkServerRepoItem(rp) != OperationOK) {
        LOG(ERROR) << "[DefaultTopologyStorage::UpdateChunkServer]: "
                   << "UpdateChunkServerRepo fail.";
        return false;
    }
    return true;
}

bool DefaultTopologyStorage::UpdateCopySet(const CopySetInfo &data) {
    std::string chunkServerListStr = data.GetCopySetMembersStr();
    CopySetRepoItem rp(data.GetId(),
                   data.GetLogicalPoolId(),
                   data.GetEpoch(),
                   chunkServerListStr);
    if (repo_->UpdateCopySetRepoItem(rp) != OperationOK) {
        LOG(ERROR) << "[DefaultTopologyStorage::UpdateCopySet]: "
                   << "UpdateCopySetRepo fail.";
        return false;
    }
    return true;
}

bool DefaultTopologyStorage::LoadClusterInfo(
    std::vector<ClusterInformation> *info) {
    std::vector<ClusterInfoRepoItem> repos;
    if (repo_->LoadClusterInfoRepoItems(&repos) != OperationOK) {
        LOG(ERROR) << "[DefaultTopologyStorage::LoadClusterInfo]: "
                   << "LoadClusterInfoRepoItems fail.";
        return false;
    }
    info->clear();

    for (auto &rp : repos) {
        ClusterInformation data(rp.clusterId);
        info->push_back(data);
    }
    return true;
}

bool DefaultTopologyStorage::StorageClusterInfo(
    const ClusterInformation &info) {
    ClusterInfoRepoItem rp(info.clusterId);
    if (repo_->InsertClusterInfoRepoItem(rp) != OperationOK) {
        LOG(ERROR) << "[DefaultTopologyStorage::StorageClusterInfo]:"
                   << "InsertClusterInfoRepoItem fail.";
        return false;
    }
    return true;
}

}  // namespace topology
}  // namespace mds
}  // namespace curve
