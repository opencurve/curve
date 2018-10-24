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

        Json::Reader reader;
        Json::Value rapJson;
        if (!reader.parse(rp.redundanceAndPlacementPolicy, rapJson)) {
            LOG(ERROR) << "[DefaultTopologyStorage::LoadLogicalPool]: "
                       << "parse redundanceAndPlacementPolicy json fail.";
            return false;
        }

        switch (rp.type) {
            case LogicalPoolType::PAGEFILE: {
                if (!rapJson["replicaNum"].isNull()) {
                    rap.pageFileRAP.replicaNum =
                        rapJson["replicaNum"].asInt();
                } else {
                    return false;
                }
                if (!rapJson["copysetNum"].isNull()) {
                    rap.pageFileRAP.copysetNum =
                        rapJson["copysetNum"].asInt();
                } else {
                    return false;
                }
                if (!rapJson["zoneNum"].isNull()) {
                    rap.pageFileRAP.zoneNum = rapJson["zoneNum"].asInt();
                } else {
                    return false;
                }
                break;
            }
            case LogicalPoolType::APPENDFILE: {
                // TODO(xuchaojie): it is not done.
                LOG(ERROR) << "[DefaultTopologyStorage::LoadLogicalPool]: "
                           << "logicalpool type error, type = "
                           << rp.type;
                return false;
                break;
            }
            case LogicalPoolType::APPENDECFILE: {
                // TODO(xuchaojie): it is not done.
                LOG(ERROR) << "[DefaultTopologyStorage::LoadLogicalPool]: "
                           << "logicalpool type error, type = "
                           << rp.type;
                return false;
                break;
            }
            default: {
                LOG(ERROR) << "[DefaultTopologyStorage::LoadLogicalPool]: "
                           << "logicalpool type error, type = "
                           << rp.type;
                return false;
                break;
            }
        }

        // TODO(xuchaojie): parse JSON String to fill policy objects
        LogicalPool pool(rp.logicalPoolID,
                         rp.logicalPoolName,
                         rp.physicalPoolID,
                         static_cast<LogicalPoolType>(rp.type),
                         rap,
                         policy,
                         rp.createTime);
        logicalPoolMap->emplace(std::make_pair(rp.logicalPoolID, pool));

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
        physicalPoolMap->emplace(std::make_pair(rp.physicalPoolID, pool));
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
        zoneMap->emplace(std::make_pair(rp.zoneID, zone));
        if (rp.zoneID > *maxZoneId) {
            *maxZoneId = rp.zoneID;
        }
    }
    return true;
}

bool DefaultTopologyStorage::LoadServer(
    std::unordered_map<ServerIdType, Server> *serverMap,
    ServerIdType *maxServerId) {
    std::vector<ServerRepo> ServerRepos;
    if (repo_->LoadServerRepos(&ServerRepos) != OperationOK) {
        LOG(ERROR) << "[DefaultTopologyStorage::LoadServer]: "
                   << "LoadServerRepos fail.";
        return false;
    }
    serverMap->clear();
    *maxServerId = 0;
    for (ServerRepo &rp : ServerRepos) {
        Server server(rp.serverID,
                      rp.hostName,
                      rp.internalHostIP,
                      rp.externalHostIP,
                      rp.zoneID,
                      rp.poolID,
                      rp.desc);
        serverMap->emplace(std::make_pair(rp.serverID, server));
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
        csState.SetOnlineState(static_cast<OnlineState>(rp.onlineState));
        csState.SetDiskCapacity(rp.capacity);
        csState.SetDiskUsed(rp.used);
        cs.SetChunkServerState(csState);
        chunkServerMap->emplace(std::make_pair(rp.chunkServerID, cs));
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
        std::set<ChunkServerIdType> idList;
        Json::Reader reader;
        Json::Value copysetMemJson;
        if (!reader.parse(rp.chunkServerIDList, copysetMemJson)) {
            LOG(ERROR) << "[DefaultTopologyStorage::LoadCopySet]: "
                       << "parse json string fail.";
            return false;
        }
        for (int i = 0; i < copysetMemJson.size(); i++) {
            idList.insert(copysetMemJson[i].asInt());
        }
        copyset.SetCopySetMembers(idList);
        if ((*copySetIdMaxMap)[rp.logicalPoolID] < rp.copySetID) {
            (*copySetIdMaxMap)[rp.logicalPoolID] = rp.copySetID;
        }
    }
    return true;
}

bool DefaultTopologyStorage::StorageLogicalPool(const LogicalPool &data) {
    LogicalPool::RedundanceAndPlaceMentPolicy rap =
        data.GetRedundanceAndPlaceMentPolicy();
    LogicalPool::UserPolicy policy = data.GetUserPolicy();
    Json::Value rapJson;
    switch (data.GetLogicalPoolType()) {
        case LogicalPoolType::PAGEFILE : {
            rapJson["replicaNum"] = rap.pageFileRAP.replicaNum;
            rapJson["copysetNum"] = rap.pageFileRAP.copysetNum;
            rapJson["zoneNum"] = rap.pageFileRAP.zoneNum;
            break;
        }
        case LogicalPoolType::APPENDFILE : {
            // TODO(xuchaojie): fix it
            LOG(ERROR) << "[DefaultTopologyStorage::StorageLogicalPool]: "
                       << "logicalpool type error.";
            return false;
            break;
        }
        case LogicalPoolType::APPENDECFILE : {
            // TODO(xuchaojie): fix it
            LOG(ERROR) << "[DefaultTopologyStorage::StorageLogicalPool]: "
                       << "logicalpool type error.";
            return false;
            break;
        }
        default:
            LOG(ERROR) << "[DefaultTopologyStorage::StorageLogicalPool]: "
                       << "logicalpool type error.";
            return false;
            break;
    }
    std::string rapStr = rapJson.toStyledString();
    // TODO(xuchaojie) Parse policy to JSON string
    std::string policyStr = rapStr;
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
    Json::Value copysetMemJson;
    for (ChunkServerIdType id : data.GetCopySetMembers()) {
        copysetMemJson.append(id);
    }
    std::string chunkServerListStr = copysetMemJson.toStyledString();

    CopySetRepo rp(data.GetId(),
                   data.GetLogicalPoolId(),
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
    LogicalPool::RedundanceAndPlaceMentPolicy rap =
        data.GetRedundanceAndPlaceMentPolicy();
    LogicalPool::UserPolicy policy = data.GetUserPolicy();
    Json::Value rapJson;
    switch (data.GetLogicalPoolType()) {
        case LogicalPoolType::PAGEFILE : {
            rapJson["replicaNum"] = rap.pageFileRAP.replicaNum;
            rapJson["copysetNum"] = rap.pageFileRAP.copysetNum;
            rapJson["zoneNum"] = rap.pageFileRAP.zoneNum;
            break;
        }
        case LogicalPoolType::APPENDFILE : {
            LOG(ERROR) << "[DefaultTopologyStorage::UpdateLogicalPool]: "
                       << "logicalpool type error.";
            // TODO(xuchaojie): fix it
            return false;
            break;
        }
        case LogicalPoolType::APPENDECFILE : {
            LOG(ERROR) << "[DefaultTopologyStorage::UpdateLogicalPool]: "
                       << "logicalpool type error.";
            // TODO(xuchaojie): fix it
            return false;
            break;
        }
        default:
            LOG(ERROR) << "[DefaultTopologyStorage::UpdateLogicalPool]: "
                       << "logicalpool type error.";
            return false;
            break;
    }
    std::string rapStr = rapJson.toStyledString();

    // TODO(xuchaojie) Parse policy to JSON string
    LogicalPoolRepo rp(data.GetId(),
                       data.GetName(),
                       data.GetPhysicalPoolId(),
                       data.GetLogicalPoolType(),
                       data.GetCreateTime(),
                       data.GetStatus(),
                       rapStr,
                       "");
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
    Json::Value copysetMemJson;
    for (ChunkServerIdType id : data.GetCopySetMembers()) {
        copysetMemJson.append(id);
    }
    std::string chunkServerListStr = copysetMemJson.toStyledString();

    CopySetRepo rp(data.GetId(),
                   data.GetLogicalPoolId(),
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
