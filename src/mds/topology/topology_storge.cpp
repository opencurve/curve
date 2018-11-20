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

#include "src/mds/repo/repo.h"
#include "src/mds/repo/repoItem.h"
#include "src/mds/repo/dataBase.h"
#include "json/json.h"

namespace curve {
namespace mds {
namespace topology {

using ::curve::repo::OperationOK;
using ::curve::repo::LogicalPoolRepo;
using ::curve::repo::PhysicalPoolRepo;
using ::curve::repo::LogicalPoolRepo;
using ::curve::repo::ZoneRepo;
using ::curve::repo::ServerRepo;
using ::curve::repo::ChunkServerRepo;
using ::curve::repo::CopySetRepo;

bool DefaultTopologyStorage::init(const std::string &dbName,
                                  const std::string &user,
                                  const std::string &url,
                                  const std::string &password) {
    if (repo_->connectDB(dbName, user, url, password) != OperationOK) {
        LOG(ERROR) << "[DefaultTopologyStorage::init]: connectDB fail.";
        return false;
    } else if (repo_->createDatabase() != OperationOK) {
        LOG(ERROR) << "[DefaultTopologyStorage::init]: createDatabase fail.";
        return false;
    } else if (repo_->useDataBase() != OperationOK) {
        LOG(ERROR) << "[DefaultTopologyStorage::init]: useDataBase fail.";
        return false;
    } else if (repo_->createAllTables() != OperationOK) {
        LOG(ERROR) << "[DefaultTopologyStorage::init]: createAllTables fail.";
        return false;
    }
    return true;
}

bool DefaultTopologyStorage::LoadLogicalPool(
    std::unordered_map<PoolIdType,
                       LogicalPool> *logicalPoolMap,
    PoolIdType *maxLogicalPoolId) {
    std::vector<LogicalPoolRepo> logicalPoolRepos;
    if (repo_->LoadLogicalPoolRepos(&logicalPoolRepos) != OperationOK) {
        LOG(ERROR) << "[DefaultTopologyStorage::LoadLogicalPool]: "
                   << "LoadLogicalPoolRepos fail.";
        return false;
    }
    logicalPoolMap->clear();
    *maxLogicalPoolId = 0;
    for (LogicalPoolRepo &rp : logicalPoolRepos) {
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
                         rp.createTime);
        pool.SetStatus(static_cast<LogicalPool::LogicalPoolStatus>(rp.status));
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
    std::vector<PhysicalPoolRepo> physicalPoolRepos;
    if (repo_->LoadPhysicalPoolRepos(&physicalPoolRepos) != OperationOK) {
        LOG(ERROR) << "[DefaultTopologyStorage::LoadPhysicalPool]: "
                   << "LoadPhysicalPoolRepos fail.";
        return false;
    }
    physicalPoolMap->clear();
    *maxPhysicalPoolId = 0;
    for (PhysicalPoolRepo &rp : physicalPoolRepos) {
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
    std::vector<ZoneRepo> zoneRepos;
    if (repo_->LoadZoneRepos(&zoneRepos) != OperationOK) {
        LOG(ERROR) << "[DefaultTopologyStorage::LoadZone]: "
                   << "LoadZoneRepos fail.";
        return false;
    }
    zoneMap->clear();
    *maxZoneId = 0;
    for (ZoneRepo &rp : zoneRepos) {
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
    std::vector<ServerRepo> serverRepos;
    if (repo_->LoadServerRepos(&serverRepos) != OperationOK) {
        LOG(ERROR) << "[DefaultTopologyStorage::LoadServer]: "
                   << "LoadServerRepos fail.";
        return false;
    }
    serverMap->clear();
    *maxServerId = 0;
    for (ServerRepo &rp : serverRepos) {
        Server server(rp.serverID,
                      rp.hostName,
                      rp.internalHostIP,
                      rp.externalHostIP,
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
    std::vector<ChunkServerRepo> chunkServerRepos;
    if (repo_->LoadChunkServerRepos(&chunkServerRepos) != OperationOK) {
        LOG(ERROR) << "[DefaultTopologyStorage::LoadChunkServer]: "
                   << "LoadChunkServerRepos fail.";
        return false;
    }
    chunkServerMap->clear();
    *maxChunkServerId = 0;
    for (ChunkServerRepo &rp : chunkServerRepos) {
        ChunkServer cs(rp.chunkServerID,
                       rp.token,
                       rp.diskType,
                       rp.serverID,
                       rp.internalHostIP,
                       rp.port,
                       rp.mountPoint,
                       static_cast<ChunkServerStatus>(rp.rwstatus));
        ChunkServerState csState;
        csState.SetDiskState(static_cast<DiskState>(rp.diskState));
        csState.SetOnlineState(OnlineState::ONLINE);
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
    std::vector<CopySetRepo> copySetRepos;
    if (repo_->LoadCopySetRepos(&copySetRepos) != OperationOK) {
        LOG(ERROR) << "[DefaultTopologyStorage::LoadCopySet]: "
                   << "LoadCopySetRepos fail.";
        return false;
    }
    copySetMap->clear();
    copySetIdMaxMap->clear();
    for (CopySetRepo &rp : copySetRepos) {
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
    LogicalPoolRepo rp(data.GetId(),
                       data.GetName(),
                       data.GetPhysicalPoolId(),
                       data.GetLogicalPoolType(),
                       data.GetCreateTime(),
                       data.GetStatus(),
                       rapStr,
                       policyStr);
    if (repo_->InsertLogicalPoolRepo(rp) != OperationOK) {
        LOG(ERROR) << "[DefaultTopologyStorage::StorageLogicalPool]: "
                   << "InsertLogicalPoolRepo fail.";
        return false;
    }
    return true;
}

bool DefaultTopologyStorage::StoragePhysicalPool(const PhysicalPool &data) {
    PhysicalPoolRepo rp(data.GetId(),
                        data.GetName(),
                        data.GetDesc());
    if (repo_->InsertPhysicalPoolRepo(rp) != OperationOK) {
        LOG(ERROR) << "[DefaultTopologyStorage::StoragePhysicalPool]: "
                   << "InsertPhysicalPoolRepo fail.";
        return false;
    }
    return true;
}

bool DefaultTopologyStorage::StorageZone(const Zone &data) {
    ZoneRepo rp(data.GetId(),
                data.GetName(),
                data.GetPhysicalPoolId(),
                data.GetDesc());
    if (repo_->InsertZoneRepo(rp) != OperationOK) {
        LOG(ERROR) << "[DefaultTopologyStorage::StorageZone]: "
                   << "InsertZoneRepo fail.";
        return false;
    }
    return true;
}

bool DefaultTopologyStorage::StorageServer(const Server &data) {
    ServerRepo rp(data.GetId(),
                  data.GetHostName(),
                  data.GetInternalHostIp(),
                  data.GetExternalHostIp(),
                  data.GetZoneId(),
                  data.GetPhysicalPoolId(),
                  data.GetDesc());
    if (repo_->InsertServerRepo(rp) != OperationOK) {
        LOG(ERROR) << "[DefaultTopologyStorage::StorageServer]: "
                   << "InsertServerRepo fail.";
        return false;
    }
    return true;
}

bool DefaultTopologyStorage::StorageChunkServer(const ChunkServer &data) {
    ChunkServerState csState = data.GetChunkServerState();
    ChunkServerRepo rp(data.GetId(),
                       data.GetToken(),
                       data.GetDiskType(),
                       data.GetHostIp(),
                       data.GetPort(),
                       data.GetServerId(),
                       data.GetStatus(),
                       csState.GetDiskState(),
                       csState.GetOnlineState(),
                       data.GetMountPoint(),
                       csState.GetDiskCapacity(),
                       csState.GetDiskUsed());

    if (repo_->InsertChunkServerRepo(rp) != OperationOK) {
        LOG(ERROR) << "[DefaultTopologyStorage::StorageChunkServer]: "
                   << "StorageChunkServerRepo fail.";
        return false;
    }
    return true;
}

bool DefaultTopologyStorage::StorageCopySet(const CopySetInfo &data) {
    std::string chunkServerListStr = data.GetCopySetMembersStr();
    CopySetRepo rp(data.GetId(),
                   data.GetLogicalPoolId(),
                   data.GetEpoch(),
                   chunkServerListStr);
    if (repo_->InsertCopySetRepo(rp) != OperationOK) {
        LOG(ERROR) << "[DefaultTopologyStorage::StorageCopySet]: "
                   << "InsertCopySetRepo fail.";
        return false;
    }
    return true;
}

bool DefaultTopologyStorage::DeleteLogicalPool(PoolIdType id) {
    if (repo_->DeleteLogicalPoolRepo(id) != OperationOK) {
        LOG(ERROR) << "[DefaultTopologyStorage::DeleteLogicalPool]: "
                   << "DeleteLogicalPoolRepo fail.";
        return false;
    }
    return true;
}

bool DefaultTopologyStorage::DeletePhysicalPool(PoolIdType id) {
    if (repo_->DeletePhysicalPoolRepo(id) != OperationOK) {
        LOG(ERROR) << "[DefaultTopologyStorage::DeletePhysicalPool]: "
                   << "DeletePhysicalPoolRepo fail.";
        return false;
    }
    return true;
}

bool DefaultTopologyStorage::DeleteZone(ZoneIdType id) {
    if (repo_->DeleteZoneRepo(id) != OperationOK) {
        LOG(ERROR) << "[DefaultTopologyStorage::DeleteZone]: "
                   << "DeleteZoneRepo fail.";
        return false;
    }
    return true;
}

bool DefaultTopologyStorage::DeleteServer(ServerIdType id) {
    if (repo_->DeleteServerRepo(id) != OperationOK) {
        LOG(ERROR) << "[DefaultTopologyStorage::DeleteServer]: "
                   << "DeleteServerRepo fail.";
        return false;
    }
    return true;
}

bool DefaultTopologyStorage::DeleteChunkServer(ChunkServerIdType id) {
    if (repo_->DeleteChunkServerRepo(id) != OperationOK) {
        LOG(ERROR) << "[DefaultTopologyStorage::DeleteChunkServer]: "
                   << "DeleteChunkServerRepo fail.";
        return false;
    }
    return true;
}

bool DefaultTopologyStorage::DeleteCopySet(CopySetKey key) {
    if (repo_->DeleteCopySetRepo(key.second, key.first) != OperationOK) {
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
    LogicalPoolRepo rp(data.GetId(),
                       data.GetName(),
                       data.GetPhysicalPoolId(),
                       data.GetLogicalPoolType(),
                       data.GetCreateTime(),
                       data.GetStatus(),
                       rapStr,
                       policyStr);
    if (repo_->UpdateLogicalPoolRepo(rp) != OperationOK) {
        LOG(ERROR) << "[DefaultTopologyStorage::UpdateLogicalPool]: "
                   << "UpdateLogicalPoolRepo fail.";
        return false;
    }
    return true;
}

bool DefaultTopologyStorage::UpdatePhysicalPool(const PhysicalPool &data) {
    PhysicalPoolRepo rp(data.GetId(),
                        data.GetName(),
                        data.GetDesc());
    if (repo_->UpdatePhysicalPoolRepo(rp) != OperationOK) {
        LOG(ERROR) << "[DefaultTopologyStorage::UpdatePhysicalPool]: "
                   << "UpdatePhysicalPoolRepo fail.";
        return false;
    }
    return true;
}

bool DefaultTopologyStorage::UpdateZone(const Zone &data) {
    ZoneRepo rp(data.GetId(),
                data.GetName(),
                data.GetPhysicalPoolId(),
                data.GetDesc());
    if (repo_->UpdateZoneRepo(rp) != OperationOK) {
        LOG(ERROR) << "[DefaultTopologyStorage::UpdateZone]: "
                   << "UpdateZoneRepo fail.";
        return false;
    }
    return true;
}

bool DefaultTopologyStorage::UpdateServer(const Server &data) {
    ServerRepo rp(data.GetId(),
                  data.GetHostName(),
                  data.GetInternalHostIp(),
                  data.GetExternalHostIp(),
                  data.GetZoneId(),
                  data.GetPhysicalPoolId(),
                  data.GetDesc());
    if (repo_->UpdateServerRepo(rp) != OperationOK) {
        LOG(ERROR) << "[DefaultTopologyStorage::UpdateServer]: "
                   << "UpdateServerRepo fail.";
        return false;
    }
    return true;
}

bool DefaultTopologyStorage::UpdateChunkServer(const ChunkServer &data) {
    ChunkServerState csState = data.GetChunkServerState();
    ChunkServerRepo rp(data.GetId(),
                       data.GetToken(),
                       data.GetDiskType(),
                       data.GetHostIp(),
                       data.GetPort(),
                       data.GetServerId(),
                       data.GetStatus(),
                       csState.GetDiskState(),
                       csState.GetOnlineState(),
                       data.GetMountPoint(),
                       csState.GetDiskCapacity(),
                       csState.GetDiskUsed());

    if (repo_->UpdateChunkServerRepo(rp) != OperationOK) {
        LOG(ERROR) << "[DefaultTopologyStorage::UpdateChunkServer]: "
                   << "UpdateChunkServerRepo fail.";
        return false;
    }
    return true;
}

bool DefaultTopologyStorage::UpdateCopySet(const CopySetInfo &data) {
    std::string chunkServerListStr = data.GetCopySetMembersStr();
    CopySetRepo rp(data.GetId(),
                   data.GetLogicalPoolId(),
                   data.GetEpoch(),
                   chunkServerListStr);
    if (repo_->UpdateCopySetRepo(rp) != OperationOK) {
        LOG(ERROR) << "[DefaultTopologyStorage::UpdateCopySet]: "
                   << "UpdateCopySetRepo fail.";
        return false;
    }
    return true;
}
}  // namespace topology
}  // namespace mds
}  // namespace curve
