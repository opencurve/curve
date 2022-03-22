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
 * Created Date: 2021-08-26
 * Author: wanghai01
 */

#include "curvefs/src/mds/topology/topology_manager.h"

#include <sys/time.h>
#include <sys/types.h>

#include <chrono>  //NOLINT
#include <list>
#include <set>
#include <string>
#include <thread>  //NOLINT
#include <utility>
#include <vector>

#include "brpc/channel.h"
#include "brpc/controller.h"
#include "brpc/server.h"
#include "src/common/concurrent/concurrent.h"
#include "src/common/concurrent/name_lock.h"

namespace curvefs {
namespace mds {
namespace topology {

void TopologyManager::Init(const TopologyOption &option) { option_ = option; }

void TopologyManager::RegistMetaServer(const MetaServerRegistRequest *request,
                                       MetaServerRegistResponse *response) {
    std::string hostIp = request->internalip();
    uint32_t port = request->internalport();
    NameLockGuard lock(registMsMutex, hostIp + ":" + std::to_string(port));

    // here we get metaserver already registered in the cluster that have
    // the same ip and port as what we're trying to register and are running
    // normally
    std::vector<MetaServerIdType> list = topology_->GetMetaServerInCluster(
        [&hostIp, &port](const MetaServer &ms) {
            return (ms.GetInternalIp() == hostIp) &&
                   (ms.GetInternalPort() == port);
        });
    if (1 == list.size()) {
        // report duplicated register (already a metaserver with same ip and
        // port in the cluster) to promise the idempotence of the interface.
        // If metaserver has copyset, return TOPO_METASERVER_EXIST;
        // else return OK
        auto copysetList = topology_->GetCopySetsInMetaServer(list[0]);
        if (copysetList.empty()) {
            MetaServer ms;
            topology_->GetMetaServer(list[0], &ms);
            response->set_statuscode(TopoStatusCode::TOPO_OK);
            response->set_metaserverid(ms.GetId());
            response->set_token(ms.GetToken());
            LOG(WARNING) << "Received duplicated registMetaServer message, "
                         << "metaserver is empty, hostip = "
                         << hostIp << ", port = " << port;
        } else {
            response->set_statuscode(TopoStatusCode::TOPO_METASERVER_EXIST);
            LOG(ERROR) << "Received duplicated registMetaServer message, "
                       << "metaserver is not empty, hostip = "
                       << hostIp << ", port = " << port;
        }

        return;
    } else if (list.size() > 1) {
        // more than one metaserver with same ip:port found, internal error
        response->set_statuscode(TopoStatusCode::TOPO_INTERNAL_ERROR);
        LOG(ERROR) << "Topology has counter an internal error: "
                      "Found metaServer data ipPort duplicated.";
        return;
    }

    ServerIdType serverId = topology_->FindServerByHostIpPort(
        request->internalip(), request->internalport());
    if (serverId == static_cast<ServerIdType>(UNINITIALIZE_ID)) {
        response->set_statuscode(TopoStatusCode::TOPO_SERVER_NOT_FOUND);
        return;
    }

    MetaServerIdType metaServerId = topology_->AllocateMetaServerId();
    if (metaServerId == static_cast<MetaServerIdType>(UNINITIALIZE_ID)) {
        response->set_statuscode(TopoStatusCode::TOPO_ALLOCATE_ID_FAIL);
        return;
    }

    std::string token = topology_->AllocateToken();
    Server server;
    bool foundServer = topology_->GetServer(serverId, &server);
    if (!foundServer) {
        LOG(ERROR) << "Get server " << serverId << " from topology fail";
        response->set_statuscode(TopoStatusCode::TOPO_SERVER_NOT_FOUND);
        return;
    }
    if (request->has_externalip()) {
        if (request->externalip() != server.GetExternalIp()) {
            LOG(ERROR) << "External ip of metaserver not match server's"
                       << ", server external ip: " << server.GetExternalIp()
                       << ", request external ip: " << request->externalip();
            response->set_statuscode(TopoStatusCode::TOPO_INTERNAL_ERROR);
            return;
        }
    }

    MetaServer metaserver(metaServerId, request->hostname(), token, serverId,
                          request->internalip(), request->internalport(),
                          request->externalip(), request->externalport(),
                          ONLINE);

    TopoStatusCode errcode = topology_->AddMetaServer(metaserver);
    if (errcode == TopoStatusCode::TOPO_OK) {
        response->set_statuscode(TopoStatusCode::TOPO_OK);
        response->set_metaserverid(metaserver.GetId());
        response->set_token(metaserver.GetToken());
    } else {
        response->set_statuscode(errcode);
    }
}

void TopologyManager::ListMetaServer(const ListMetaServerRequest *request,
                                     ListMetaServerResponse *response) {
    Server server;
    if (!topology_->GetServer(request->serverid(), &server)) {
        response->set_statuscode(TopoStatusCode::TOPO_SERVER_NOT_FOUND);
        return;
    }

    std::list<MetaServerIdType> metaserverList = server.GetMetaServerList();
    response->set_statuscode(TopoStatusCode::TOPO_OK);

    for (MetaServerIdType id : metaserverList) {
        MetaServer ms;
        if (topology_->GetMetaServer(id, &ms)) {
            MetaServerInfo *msInfo = response->add_metaserverinfos();
            msInfo->set_metaserverid(ms.GetId());
            msInfo->set_hostname(ms.GetHostName());
            msInfo->set_internalip(ms.GetInternalIp());
            msInfo->set_internalport(ms.GetInternalPort());
            msInfo->set_externalip(ms.GetExternalIp());
            msInfo->set_externalport(ms.GetExternalPort());
            msInfo->set_onlinestate(ms.GetOnlineState());
        } else {
            LOG(ERROR) << "Topology has counter an internal error: "
                       << "[func:] ListMetaServer, "
                       << "[msg:] metaserver not found, id = " << id;
            response->set_statuscode(TopoStatusCode::TOPO_INTERNAL_ERROR);
            return;
        }
    }
}

void TopologyManager::GetMetaServer(const GetMetaServerInfoRequest *request,
                                    GetMetaServerInfoResponse *response) {
    MetaServer ms;
    if (request->has_metaserverid()) {
        if (!topology_->GetMetaServer(request->metaserverid(), &ms)) {
            response->set_statuscode(TopoStatusCode::TOPO_METASERVER_NOT_FOUND);
            return;
        }
    } else if (request->has_hostip() && request->has_port()) {
        if (!topology_->GetMetaServer(request->hostip(), request->port(),
                                      &ms)) {
            response->set_statuscode(TopoStatusCode::TOPO_METASERVER_NOT_FOUND);
            return;
        }
    } else {
        response->set_statuscode(TopoStatusCode::TOPO_INVALID_PARAM);
        return;
    }
    response->set_statuscode(TopoStatusCode::TOPO_OK);
    MetaServerInfo *msInfo = response->mutable_metaserverinfo();
    msInfo->set_metaserverid(ms.GetId());
    msInfo->set_hostname(ms.GetHostName());
    msInfo->set_internalip(ms.GetInternalIp());
    msInfo->set_internalport(ms.GetInternalPort());
    msInfo->set_externalip(ms.GetExternalIp());
    msInfo->set_externalport(ms.GetExternalPort());
    msInfo->set_onlinestate(ms.GetOnlineState());
}

void TopologyManager::DeleteMetaServer(const DeleteMetaServerRequest *request,
                                       DeleteMetaServerResponse *response) {
    TopoStatusCode errcode =
        topology_->RemoveMetaServer(request->metaserverid());
    response->set_statuscode(errcode);
}

void TopologyManager::RegistServer(const ServerRegistRequest *request,
                                   ServerRegistResponse *response) {
    Pool pPool;
    if (!topology_->GetPool(request->poolname(), &pPool)) {
        response->set_statuscode(TopoStatusCode::TOPO_POOL_NOT_FOUND);
        return;
    }

    Zone zone;
    if (!topology_->GetZone(request->zonename(), pPool.GetId(), &zone)) {
        response->set_statuscode(TopoStatusCode::TOPO_ZONE_NOT_FOUND);
        return;
    }

    uint32_t internalPort = 0;
    if (request->has_internalport()) {
        internalPort = request->internalport();
    }
    uint32_t externalPort = 0;
    if (request->has_externalport()) {
        externalPort = request->externalport();
    }

    // check whether there's any duplicated ip&port
    if (topology_->FindServerByHostIpPort(request->internalip(),
                                          internalPort) !=
        static_cast<ServerIdType>(UNINITIALIZE_ID)) {
        response->set_statuscode(TopoStatusCode::TOPO_IP_PORT_DUPLICATED);
        return;
    } else if (topology_->FindServerByHostIpPort(request->externalip(),
                                                 externalPort) !=
               static_cast<ServerIdType>(UNINITIALIZE_ID)) {
        response->set_statuscode(TopoStatusCode::TOPO_IP_PORT_DUPLICATED);
        return;
    }

    ServerIdType serverId = topology_->AllocateServerId();
    if (serverId == static_cast<ServerIdType>(UNINITIALIZE_ID)) {
        response->set_statuscode(TopoStatusCode::TOPO_ALLOCATE_ID_FAIL);
        return;
    }

    Server server(serverId, request->hostname(), request->internalip(),
                  internalPort, request->externalip(), externalPort,
                  zone.GetId(), pPool.GetId());

    TopoStatusCode errcode = topology_->AddServer(server);
    if (TopoStatusCode::TOPO_OK == errcode) {
        response->set_statuscode(TopoStatusCode::TOPO_OK);
        response->set_serverid(serverId);
    } else {
        response->set_statuscode(errcode);
    }
}

void TopologyManager::GetServer(const GetServerRequest *request,
                                GetServerResponse *response) {
    Server sv;
    if (request->has_serverid()) {
        if (!topology_->GetServer(request->serverid(), &sv)) {
            response->set_statuscode(TopoStatusCode::TOPO_SERVER_NOT_FOUND);
            return;
        }
    } else if (request->has_hostname()) {
        if (!topology_->GetServerByHostName(request->hostname(), &sv)) {
            response->set_statuscode(TopoStatusCode::TOPO_SERVER_NOT_FOUND);
            return;
        }
    } else if (request->has_hostip()) {
        uint32_t port = 0;
        if (request->has_port()) {
            port = request->port();
        }
        if (!topology_->GetServerByHostIpPort(request->hostip(), port, &sv)) {
            response->set_statuscode(TopoStatusCode::TOPO_SERVER_NOT_FOUND);
            return;
        }
    }
    Zone zone;
    if (!topology_->GetZone(sv.GetZoneId(), &zone)) {
        LOG(ERROR) << "Topology has counter an internal error: "
                   << " Server belong Zone not found, ServerId = " << sv.GetId()
                   << " ZoneId = " << sv.GetZoneId();
        response->set_statuscode(TopoStatusCode::TOPO_INTERNAL_ERROR);
        return;
    }
    Pool pPool;
    if (!topology_->GetPool(zone.GetPoolId(), &pPool)) {
        LOG(ERROR) << "Topology has counter an internal error: "
                   << " Zone belong Pool not found, zoneId = " << zone.GetId()
                   << " poolId = " << zone.GetPoolId();
        response->set_statuscode(TopoStatusCode::TOPO_INTERNAL_ERROR);
        return;
    }
    ServerInfo *info = new ServerInfo();
    info->set_serverid(sv.GetId());
    info->set_hostname(sv.GetHostName());
    info->set_internalip(sv.GetInternalIp());
    info->set_internalport(sv.GetInternalPort());
    info->set_externalip(sv.GetExternalIp());
    info->set_externalport(sv.GetExternalPort());
    info->set_zoneid(sv.GetZoneId());
    info->set_zonename(zone.GetName());
    info->set_poolid(sv.GetPoolId());
    info->set_poolname(pPool.GetName());
    response->set_allocated_serverinfo(info);
}

void TopologyManager::DeleteServer(const DeleteServerRequest *request,
                                   DeleteServerResponse *response) {
    TopoStatusCode errcode = TopoStatusCode::TOPO_OK;
    Server server;
    if (!topology_->GetServer(request->serverid(), &server)) {
        response->set_statuscode(TopoStatusCode::TOPO_SERVER_NOT_FOUND);
        return;
    }
    for (auto &msId : server.GetMetaServerList()) {
        MetaServer ms;
        if (!topology_->GetMetaServer(msId, &ms)) {
            LOG(ERROR) << "Topology has counter an internal error: "
                       << ", metaServer in server not found"
                       << ", metaserverId = " << msId
                       << ", serverId = " << request->serverid();
            response->set_statuscode(TopoStatusCode::TOPO_INTERNAL_ERROR);
            return;
        } else if (OnlineState::OFFLINE != ms.GetOnlineState()) {
            LOG(ERROR) << "Can not delete server which have "
                       << "metaserver not offline.";
            response->set_statuscode(
                TopoStatusCode::TOPO_CANNOT_REMOVE_NOT_OFFLINE);
            return;
        } else {
            errcode = topology_->RemoveMetaServer(msId);
            if (errcode != TopoStatusCode::TOPO_OK) {
                response->set_statuscode(errcode);
                return;
            }
        }
    }
    errcode = topology_->RemoveServer(request->serverid());
    response->set_statuscode(errcode);
}

void TopologyManager::ListZoneServer(const ListZoneServerRequest *request,
                                     ListZoneServerResponse *response) {
    Zone zone;
    if (request->has_zoneid()) {
        if (!topology_->GetZone(request->zoneid(), &zone)) {
            response->set_statuscode(TopoStatusCode::TOPO_ZONE_NOT_FOUND);
            return;
        }
    } else if (request->has_zonename() && request->has_poolname()) {
        if (!topology_->GetZone(request->zonename(), request->poolname(),
                                &zone)) {
            response->set_statuscode(TopoStatusCode::TOPO_ZONE_NOT_FOUND);
            return;
        }
    } else {
        response->set_statuscode(TopoStatusCode::TOPO_INVALID_PARAM);
        return;
    }
    response->set_statuscode(TopoStatusCode::TOPO_OK);
    std::list<ServerIdType> serverIdList = zone.GetServerList();
    for (ServerIdType id : serverIdList) {
        Server sv;
        if (topology_->GetServer(id, &sv)) {
            Zone zone;
            if (!topology_->GetZone(sv.GetZoneId(), &zone)) {
                LOG(ERROR) << "Topology has counter an internal error: "
                           << " Server belong Zone not found, ServerId = "
                           << sv.GetId() << " ZoneId = " << sv.GetZoneId();
                response->set_statuscode(TopoStatusCode::TOPO_INTERNAL_ERROR);
                return;
            }
            Pool pPool;
            if (!topology_->GetPool(zone.GetPoolId(), &pPool)) {
                LOG(ERROR) << "Topology has counter an internal error: "
                           << " Zone belong Pool not found, zoneId = "
                           << zone.GetId() << " poolId = " << zone.GetPoolId();
                response->set_statuscode(TopoStatusCode::TOPO_INTERNAL_ERROR);
                return;
            }
            ServerInfo *info = response->add_serverinfo();
            info->set_serverid(sv.GetId());
            info->set_hostname(sv.GetHostName());
            info->set_internalip(sv.GetInternalIp());
            info->set_internalport(sv.GetInternalPort());
            info->set_externalip(sv.GetExternalIp());
            info->set_externalport(sv.GetExternalPort());
            info->set_zoneid(sv.GetZoneId());
            info->set_zonename(zone.GetName());
            info->set_poolid(sv.GetPoolId());
            info->set_poolname(pPool.GetName());
        } else {
            LOG(ERROR) << "Topology has counter an internal error: "
                       << "[func:] ListZoneServer, "
                       << "[msg:] server not found, id = " << id;
            response->set_statuscode(TopoStatusCode::TOPO_INTERNAL_ERROR);
            return;
        }
    }
}

void TopologyManager::CreateZone(const CreateZoneRequest *request,
                                 CreateZoneResponse *response) {
    Pool pPool;
    if (!topology_->GetPool(request->poolname(), &pPool)) {
        response->set_statuscode(TopoStatusCode::TOPO_POOL_NOT_FOUND);
        return;
    }
    if (topology_->FindZone(request->zonename(), pPool.GetId()) !=
        static_cast<PoolIdType>(UNINITIALIZE_ID)) {
        response->set_statuscode(TopoStatusCode::TOPO_NAME_DUPLICATED);
        return;
    }

    ZoneIdType zid = topology_->AllocateZoneId();
    if (zid == static_cast<ZoneIdType>(UNINITIALIZE_ID)) {
        response->set_statuscode(TopoStatusCode::TOPO_ALLOCATE_ID_FAIL);
        return;
    }
    Zone zone(zid, request->zonename(), pPool.GetId());
    TopoStatusCode errcode = topology_->AddZone(zone);
    if (TopoStatusCode::TOPO_OK == errcode) {
        response->set_statuscode(errcode);
        ZoneInfo *info = new ZoneInfo();
        info->set_zoneid(zid);
        info->set_zonename(request->zonename());
        info->set_poolid(pPool.GetId());
        info->set_poolname(pPool.GetName());
        response->set_allocated_zoneinfo(info);
    } else {
        response->set_statuscode(errcode);
    }
}

void TopologyManager::DeleteZone(const DeleteZoneRequest *request,
                                 DeleteZoneResponse *response) {
    Zone zone;
    if (!topology_->GetZone(request->zoneid(), &zone)) {
        response->set_statuscode(TopoStatusCode::TOPO_ZONE_NOT_FOUND);
        return;
    }
    TopoStatusCode errcode = topology_->RemoveZone(zone.GetId());
    response->set_statuscode(errcode);
}

void TopologyManager::GetZone(const GetZoneRequest *request,
                              GetZoneResponse *response) {
    Zone zone;
    if (!topology_->GetZone(request->zoneid(), &zone)) {
        response->set_statuscode(TopoStatusCode::TOPO_ZONE_NOT_FOUND);
        return;
    }
    Pool pPool;
    if (!topology_->GetPool(zone.GetPoolId(), &pPool)) {
        response->set_statuscode(TopoStatusCode::TOPO_INTERNAL_ERROR);
        return;
    }
    response->set_statuscode(TopoStatusCode::TOPO_OK);
    ZoneInfo *info = new ZoneInfo();
    info->set_zoneid(zone.GetId());
    info->set_zonename(zone.GetName());
    info->set_poolid((zone.GetPoolId()));
    info->set_poolname(pPool.GetName());
    response->set_allocated_zoneinfo(info);
}

void TopologyManager::ListPoolZone(const ListPoolZoneRequest *request,
                                   ListPoolZoneResponse *response) {
    Pool pPool;
    if (!topology_->GetPool(request->poolid(), &pPool)) {
        response->set_statuscode(TopoStatusCode::TOPO_POOL_NOT_FOUND);
        return;
    }
    std::list<ZoneIdType> zidList = pPool.GetZoneList();
    response->set_statuscode(TopoStatusCode::TOPO_OK);
    for (ZoneIdType id : zidList) {
        Zone zone;
        if (topology_->GetZone(id, &zone)) {
            ZoneInfo *info = response->add_zones();
            info->set_zoneid(zone.GetId());
            info->set_zonename(zone.GetName());
            info->set_poolid(pPool.GetId());
            info->set_poolname(pPool.GetName());
        } else {
            LOG(ERROR) << "Topology has counter an internal error: "
                       << "[func:] ListPoolZone, "
                       << "[msg:] Zone not found, id = " << id;
            response->set_statuscode(TopoStatusCode::TOPO_INTERNAL_ERROR);
            return;
        }
    }
}

void TopologyManager::CreatePool(const CreatePoolRequest *request,
                                 CreatePoolResponse *response) {
    if (topology_->FindPool(request->poolname()) !=
        static_cast<PoolIdType>(UNINITIALIZE_ID)) {
        response->set_statuscode(TopoStatusCode::TOPO_NAME_DUPLICATED);
        return;
    }

    PoolIdType pid = topology_->AllocatePoolId();
    if (pid == static_cast<PoolIdType>(UNINITIALIZE_ID)) {
        response->set_statuscode(TopoStatusCode::TOPO_ALLOCATE_ID_FAIL);
        return;
    }

    Pool::RedundanceAndPlaceMentPolicy rap;
    if (!Pool::TransRedundanceAndPlaceMentPolicyFromJsonStr(
            request->redundanceandplacementpolicy(), &rap)) {
        LOG(ERROR) << "[TopologyManager::CreatePool]:"
                   << "parse redundanceandplacementpolicy fail.";
        response->set_statuscode(TopoStatusCode::TOPO_INVALID_PARAM);
        return;
    }

    uint64_t time = TimeUtility::GetTimeofDaySec();

    Pool pool(pid, request->poolname(), rap, time);

    TopoStatusCode errcode = topology_->AddPool(pool);
    if (TopoStatusCode::TOPO_OK == errcode) {
        response->set_statuscode(errcode);
        PoolInfo *info = new PoolInfo();
        info->set_poolid(pid);
        info->set_poolname(request->poolname());
        info->set_createtime(time);
        info->set_redundanceandplacementpolicy(
            pool.GetRedundanceAndPlaceMentPolicyJsonStr());
        response->set_allocated_poolinfo(info);
    } else {
        response->set_statuscode(errcode);
    }
}

void TopologyManager::DeletePool(const DeletePoolRequest *request,
                                 DeletePoolResponse *response) {
    Pool pool;
    if (!topology_->GetPool(request->poolid(), &pool)) {
        response->set_statuscode(TopoStatusCode::TOPO_POOL_NOT_FOUND);
        return;
    }

    TopoStatusCode errcode = topology_->RemovePool(pool.GetId());
    response->set_statuscode(errcode);
}

void TopologyManager::GetPool(const GetPoolRequest *request,
                              GetPoolResponse *response) {
    Pool pool;
    if (!topology_->GetPool(request->poolid(), &pool)) {
        response->set_statuscode(TopoStatusCode::TOPO_POOL_NOT_FOUND);
        return;
    }

    response->set_statuscode(TopoStatusCode::TOPO_OK);
    PoolInfo *info = new PoolInfo();
    info->set_poolid(pool.GetId());
    info->set_poolname(pool.GetName());
    info->set_createtime(pool.GetCreateTime());
    info->set_redundanceandplacementpolicy(
        pool.GetRedundanceAndPlaceMentPolicyJsonStr());
    response->set_allocated_poolinfo(info);
}

void TopologyManager::ListPool(const ListPoolRequest *request,
                               ListPoolResponse *response) {
    response->set_statuscode(TopoStatusCode::TOPO_OK);
    auto poolList = topology_->GetPoolInCluster();
    for (PoolIdType id : poolList) {
        Pool pool;
        if (topology_->GetPool(id, &pool)) {
            PoolInfo *info = response->add_poolinfos();
            info->set_poolid(pool.GetId());
            info->set_poolname(pool.GetName());
            info->set_createtime(pool.GetCreateTime());
            info->set_redundanceandplacementpolicy(
                pool.GetRedundanceAndPlaceMentPolicyJsonStr());
        } else {
            LOG(ERROR) << "Topology has counter an internal error: "
                       << "[func:] ListPool, "
                       << "[msg:] Pool not found, id = " << id;
            response->set_statuscode(TopoStatusCode::TOPO_INTERNAL_ERROR);
            return;
        }
    }
}

TopoStatusCode TopologyManager::CreatePartitionsAndGetMinPartition(
    FsIdType fsId, PartitionInfo *partition) {
    CreatePartitionRequest request;
    CreatePartitionResponse response;
    request.set_fsid(fsId);
    request.set_count(option_.createPartitionNumber);
    CreatePartitions(&request, &response);
    if (TopoStatusCode::TOPO_OK != response.statuscode() ||
        response.partitioninfolist_size() != request.count()) {
        return TopoStatusCode::TOPO_CREATE_PARTITION_FAIL;
    }
    // return the min one
    PartitionIdType minId = 0;
    if (response.partitioninfolist_size() > 0) {
        minId = response.partitioninfolist(0).partitionid();
        *partition = response.partitioninfolist(0);
        for (int i = 1; i < response.partitioninfolist_size(); i++) {
            if (response.partitioninfolist(i).partitionid() < minId) {
                minId = response.partitioninfolist(i).partitionid();
                *partition = response.partitioninfolist(i);
            }
        }
    } else {
        LOG(WARNING) << "CreatePartition but empty response, "
                     << "request = " << request.ShortDebugString()
                     << "response = " << response.ShortDebugString();
        return TopoStatusCode::TOPO_CREATE_PARTITION_FAIL;
    }
    return TopoStatusCode::TOPO_OK;
}

void TopologyManager::CreatePartitions(const CreatePartitionRequest *request,
                                       CreatePartitionResponse *response) {
    FsIdType fsId = request->fsid();
    uint32_t count = request->count();
    auto partitionInfoList = response->mutable_partitioninfolist();
    response->set_statuscode(TopoStatusCode::TOPO_OK);

    while (partitionInfoList->size() < count) {
        if (topology_->GetAvailableCopysetNum()
                            < option_.minAvailableCopysetNum) {
            if (CreateEnoughCopyset() != TopoStatusCode::TOPO_OK) {
                LOG(ERROR) << "Create copyset failed when create partition.";
                response->set_statuscode(
                    TopoStatusCode::TOPO_CREATE_COPYSET_ERROR);
                return;
            }
        }

        CopySetInfo copyset;
        if (!topology_->GetAvailableCopyset(&copyset)) {
            LOG(ERROR) << "Get available copyset fail when create partition.";
            response->set_statuscode(
                TopoStatusCode::TOPO_GET_AVAILABLE_COPYSET_ERROR);
            return;
        }

        // get copyset members
        std::set<MetaServerIdType> copysetMembers = copyset.GetCopySetMembers();
        std::set<std::string> copysetMemberAddr;
        for (auto item : copysetMembers) {
            MetaServer metaserver;
            if (topology_->GetMetaServer(item, &metaserver)) {
                std::string addr = metaserver.GetInternalIp() + ":" +
                                   std::to_string(metaserver.GetInternalPort());
                copysetMemberAddr.emplace(addr);
            } else {
                LOG(WARNING) << "Get metaserver info failed.";
            }
        }

        // calculate partition number of this fs
        uint32_t pNumber = topology_->GetPartitionNumberOfFs(fsId);
        uint64_t idStart = pNumber * option_.idNumberInPartition;
        uint64_t idEnd = (pNumber + 1) * option_.idNumberInPartition - 1;
        PartitionIdType partitionId = topology_->AllocatePartitionId();
        LOG(INFO) << "CreatePartiton partitionId = " << partitionId;
        if (partitionId == static_cast<ServerIdType>(UNINITIALIZE_ID)) {
            response->set_statuscode(TopoStatusCode::TOPO_ALLOCATE_ID_FAIL);
            return;
        }

        PoolIdType poolId = copyset.GetPoolId();
        CopySetIdType copysetId = copyset.GetId();
        FSStatusCode retcode = metaserverClient_->CreatePartition(
            fsId, poolId, copysetId, partitionId, idStart, idEnd,
            copysetMemberAddr);
        Partition partition(fsId, poolId, copysetId, partitionId, idStart,
                            idEnd);
        if (FSStatusCode::OK == retcode) {
            TopoStatusCode ret = topology_->AddPartition(partition);
            if (TopoStatusCode::TOPO_OK == ret) {
                PartitionInfo *info = partitionInfoList->Add();
                info->set_fsid(fsId);
                info->set_poolid(poolId);
                info->set_copysetid(copysetId);
                info->set_partitionid(partitionId);
                info->set_start(idStart);
                info->set_end(idEnd);
                info->set_status(PartitionStatus::READONLY);
            } else {
                // TODO(wanghai): delete partition on metaserver
                LOG(ERROR) << "Add partition failed after create partition."
                           << " error code = " << ret;
                response->set_statuscode(ret);
                return;
            }
        } else {
            LOG(ERROR) << "CreatePartition failed, "
                       << "fsId = " << fsId << ", poolId = " << poolId
                       << ", copysetId = " << copysetId
                       << ", partitionId = " << partitionId;
            response->set_statuscode(
                TopoStatusCode::TOPO_CREATE_PARTITION_FAIL);
            return;
        }
    }
}

bool TopologyManager::CreateCopysetNodeOnMetaServer(
    PoolIdType poolId, CopySetIdType copysetId, MetaServerIdType metaServerId) {
    MetaServer metaserver;
    std::string addr;
    if (topology_->GetMetaServer(metaServerId, &metaserver)) {
        addr = metaserver.GetInternalIp() + ":" +
               std::to_string(metaserver.GetInternalPort());
    } else {
        LOG(ERROR) << "Get metaserver info failed.";
        return false;
    }

    FSStatusCode retcode = metaserverClient_->CreateCopySetOnOneMetaserver(
        poolId, copysetId, addr);
    if (FSStatusCode::OK != retcode) {
        LOG(ERROR) << "CreateCopysetNodeOnMetaServer fail, poolId = " << poolId
                   << ", copysetId = " << copysetId
                   << ", metaServerId = " << metaServerId << ", addr = " << addr
                   << ", ret = " << FSStatusCode_Name(retcode);
        return false;
    }
    return true;
}

void TopologyManager::ClearCopysetCreating(PoolIdType poolId,
                                           CopySetIdType copysetId) {
    topology_->RemoveCopySetCreating(CopySetKey(poolId, copysetId));
}

TopoStatusCode TopologyManager::CreateEnoughCopyset() {
    int avaibleCopysetNum = topology_->GetAvailableCopysetNum();
    int copysetNumTotal = topology_->GetCopySetsInCluster().size();

    // if the cluster has no copyset, it create batch copysets random, the
    // create num equals option_.initialCopysetNumber;
    // else create copyset by metaserver resource usage, the create num equals
    // option_.minAvailableCopysetNum - avaibleCopysetNum
    TopoStatusCode ret = TopoStatusCode::TOPO_OK;
    if (copysetNumTotal == 0) {
        ret = InitialCreateCopyset();
    } else {
        int createNum = option_.minAvailableCopysetNum - avaibleCopysetNum;
        if (createNum <= 0) {
            return TopoStatusCode::TOPO_OK;
        }
        ret = CreateCopysetByResourceUsage(createNum);
    }

    return ret;
}

TopoStatusCode TopologyManager::InitialCreateCopyset() {
    std::list<CopysetCreateInfo> copysetList;
    TopoStatusCode ret =
        topology_->GenInitialCopysetAddrBatch(option_.initialCopysetNumber,
                                              &copysetList);
    if (ret != TopoStatusCode::TOPO_OK) {
        LOG(ERROR) << "initial create copyset, generate copyset addr fail";
        return ret;
    }

    for (auto copyset : copysetList) {
        // alloce copyset id
        auto copysetId = topology_->AllocateCopySetId(copyset.poolId);
        if (copysetId == static_cast<ServerIdType>(UNINITIALIZE_ID)) {
            return TopoStatusCode::TOPO_ALLOCATE_ID_FAIL;
        }

        copyset.copysetId = copysetId;
        ret = CreateCopyset(copyset);
        if (ret != TopoStatusCode::TOPO_OK) {
            LOG(ERROR) << "initial create copyset, create copyset fail";
            return ret;
        }
    }

    return TopoStatusCode::TOPO_OK;
}

TopoStatusCode TopologyManager::CreateCopysetByResourceUsage(int createNum) {
    for (int i = 0; i < createNum; i++) {
        // select metaserver for copyset
        std::set<MetaServerIdType> metaServerIds;
        PoolIdType poolId;
        TopoStatusCode ret =
            topology_->GenCopysetAddrByResourceUsage(&metaServerIds, &poolId);
        if (ret != TopoStatusCode::TOPO_OK) {
            LOG(ERROR) << "Generate copyset addr fail";
            return ret;
        }

        // alloce copyset id
        CopySetIdType copysetId = topology_->AllocateCopySetId(poolId);
        if (copysetId == static_cast<ServerIdType>(UNINITIALIZE_ID)) {
            return TopoStatusCode::TOPO_ALLOCATE_ID_FAIL;
        }

        CopysetCreateInfo copyset(poolId, copysetId, metaServerIds);
        ret = CreateCopyset(copyset);
        if (ret != TopoStatusCode::TOPO_OK) {
            LOG(ERROR) << "Create copyset fail";
            return ret;
        }
    }

    return TopoStatusCode::TOPO_OK;
}

TopoStatusCode TopologyManager::CreateCopyset(
    const CopysetCreateInfo &copyset) {
    LOG(INFO) << "Create new copyset: " << copyset.ToString();
    // translate metaserver id to metaserver addr
    std::set<std::string> metaServerAddrs;
    for (const auto &it : copyset.metaServerIds) {
        MetaServer metaServer;
        if (topology_->GetMetaServer(it, &metaServer)) {
            metaServerAddrs.emplace(
                metaServer.GetInternalIp() + ":" +
                std::to_string(metaServer.GetInternalPort()));
        } else {
            LOG(ERROR) << "get metaserver failed, metaserverId = " << it;
            return TopoStatusCode::TOPO_METASERVER_NOT_FOUND;
        }
    }

    if (TopoStatusCode::TOPO_OK !=
        topology_->AddCopySetCreating(
            CopySetKey(copyset.poolId, copyset.copysetId))) {
        LOG(WARNING) << "the copyset key = (" << copyset.poolId << ", "
                     << copyset.copysetId << ") is already creating.";
    }

    // create copyset on metaserver
    FSStatusCode retcode = metaserverClient_->CreateCopySet(
        copyset.poolId, copyset.copysetId, metaServerAddrs);
    if (FSStatusCode::OK != retcode) {
        ClearCopysetCreating(copyset.poolId, copyset.copysetId);
        return TopoStatusCode::TOPO_CREATE_COPYSET_ON_METASERVER_FAIL;
    }

    // add copyset record to topogy
    CopySetInfo copysetInfo(copyset.poolId, copyset.copysetId);
    copysetInfo.SetCopySetMembers(copyset.metaServerIds);
    auto ret = topology_->AddCopySet(copysetInfo);
    if (TopoStatusCode::TOPO_OK != ret) {
        LOG(ERROR) << "Add copyset failed after create copyset."
                   << " poolId = " << copyset.poolId
                   << ", copysetId = " << copyset.copysetId
                   << ", error msg = " << TopoStatusCode_Name(ret);
        ClearCopysetCreating(copyset.poolId, copyset.copysetId);
        return ret;
    }

    return TopoStatusCode::TOPO_OK;
}

void TopologyManager::CommitTx(const CommitTxRequest *request,
                               CommitTxResponse *response) {
    if (request->partitiontxids_size() == 0) {
        response->set_statuscode(TopoStatusCode::TOPO_OK);
        return;
    }

    std::vector<PartitionTxId> partitionTxIds;
    for (int i = 0; i < request->partitiontxids_size(); i++) {
        partitionTxIds.emplace_back(request->partitiontxids(i));
    }
    response->set_statuscode(topology_->UpdatePartitionTxIds(partitionTxIds));
}

void TopologyManager::GetMetaServerListInCopysets(
    const GetMetaServerListInCopySetsRequest *request,
    GetMetaServerListInCopySetsResponse *response) {
    PoolIdType poolId = request->poolid();
    auto csIds = request->copysetid();
    response->set_statuscode(TopoStatusCode::TOPO_OK);
    for (auto id : csIds) {
        CopySetKey key(poolId, id);
        CopySetInfo info;
        if (topology_->GetCopySet(key, &info)) {
            CopySetServerInfo *serverInfo = response->add_csinfo();
            serverInfo->set_copysetid(id);
            for (auto metaserverId : info.GetCopySetMembers()) {
                MetaServer metaserver;
                if (topology_->GetMetaServer(metaserverId, &metaserver)) {
                    MetaServerLocation *location = serverInfo->add_cslocs();
                    location->set_metaserverid(metaserver.GetId());
                    location->set_internalip(metaserver.GetInternalIp());
                    location->set_internalport(metaserver.GetInternalPort());
                    location->set_externalip(metaserver.GetExternalIp());
                    location->set_externalport(metaserver.GetExternalPort());
                } else {
                    LOG(INFO) << "GetMetaserver failed"
                              << " when GetMetaServerListInCopysets.";
                    response->set_statuscode(
                        TopoStatusCode::TOPO_INTERNAL_ERROR);
                    return;
                }
            }
        } else {
            LOG(ERROR) << "GetCopyset failed when GetMetaServerListInCopysets.";
            response->set_statuscode(TopoStatusCode::TOPO_COPYSET_NOT_FOUND);
            return;
        }
    }
}

void TopologyManager::ListPartition(const ListPartitionRequest *request,
                                    ListPartitionResponse *response) {
    FsIdType fsId = request->fsid();
    auto partitionInfoList = response->mutable_partitioninfolist();
    response->set_statuscode(TopoStatusCode::TOPO_OK);
    std::list<Partition> partitions = topology_->GetPartitionOfFs(fsId);

    for (auto partition : partitions) {
        PartitionInfo *info = partitionInfoList->Add();
        info->set_fsid(partition.GetFsId());
        info->set_poolid(partition.GetPoolId());
        info->set_copysetid(partition.GetCopySetId());
        info->set_partitionid(partition.GetPartitionId());
        info->set_start(partition.GetIdStart());
        info->set_end(partition.GetIdEnd());
        info->set_txid(partition.GetTxId());
        info->set_status(partition.GetStatus());
        if (partition.GetInodeNum() != UNINITIALIZE_COUNT) {
            info->set_inodenum(partition.GetInodeNum());
        }

        if (partition.GetDentryNum() != UNINITIALIZE_COUNT) {
            info->set_dentrynum(partition.GetDentryNum());
        }
    }
}

void TopologyManager::ListPartitionOfFs(FsIdType fsId,
                                        std::list<PartitionInfo>* list) {
    for (auto &partition : topology_->GetPartitionOfFs(fsId)) {
        list->emplace_back(partition.ToPartitionInfo());
    }

    return;
}

void TopologyManager::GetCopysetOfPartition(
    const GetCopysetOfPartitionRequest *request,
    GetCopysetOfPartitionResponse *response) {
    for (int i = 0; i < request->partitionid_size(); i++) {
        PartitionIdType pId = request->partitionid(i);
        CopySetInfo copyset;
        if (topology_->GetCopysetOfPartition(pId, &copyset)) {
            Copyset cs;
            cs.set_poolid(copyset.GetPoolId());
            cs.set_copysetid(copyset.GetId());
            // get coptset members
            for (auto msId : copyset.GetCopySetMembers()) {
                MetaServer ms;
                if (topology_->GetMetaServer(msId, &ms)) {
                    common::Peer *peer = cs.add_peers();
                    peer->set_id(ms.GetId());
                    peer->set_address(BuildPeerIdWithIpPort(
                        ms.GetInternalIp(), ms.GetInternalPort()));
                } else {
                    LOG(ERROR) << "GetMetaServer failed, id = " << msId;
                    response->set_statuscode(
                        TopoStatusCode::TOPO_METASERVER_NOT_FOUND);
                    response->clear_copysetmap();
                    return;
                }
            }
            (*response->mutable_copysetmap())[pId] = cs;
        } else {
            LOG(ERROR) << "GetCopysetOfPartition failed. partitionId = " << pId;
            response->set_statuscode(TopoStatusCode::TOPO_COPYSET_NOT_FOUND);
            response->clear_copysetmap();
            return;
        }
    }
    response->set_statuscode(TopoStatusCode::TOPO_OK);
}

TopoStatusCode TopologyManager::GetCopysetMembers(
    const PoolIdType poolId, const CopySetIdType copysetId,
    std::set<std::string> *addrs) {
    CopySetKey key(poolId, copysetId);
    CopySetInfo info;
    if (topology_->GetCopySet(key, &info)) {
        for (auto metaserverId : info.GetCopySetMembers()) {
            MetaServer server;
            if (topology_->GetMetaServer(metaserverId, &server)) {
                std::string addr = server.GetExternalIp() + ":" +
                                   std::to_string(server.GetExternalPort());
                addrs->emplace(addr);
            } else {
                LOG(ERROR) << "GetMetaserver failed,"
                           << " metaserverId =" << metaserverId;
                return TopoStatusCode::TOPO_METASERVER_NOT_FOUND;
            }
        }
    } else {
        LOG(ERROR) << "Get copyset failed."
                   << " poolId = " << poolId << "copysetId = " << copysetId;
        return TopoStatusCode::TOPO_COPYSET_NOT_FOUND;
    }
    return TopoStatusCode::TOPO_OK;
}

void TopologyManager::GetCopysetInfo(const uint32_t& poolId,
                                     const uint32_t& copysetId,
                                     CopysetValue* copysetValue) {
    // default is ok, when find error set to error code
    copysetValue->set_statuscode(TopoStatusCode::TOPO_OK);
    CopySetKey key(poolId, copysetId);
    CopySetInfo info;
    if (topology_->GetCopySet(key, &info)) {
        auto valueCopysetInfo = new curvefs::mds::heartbeat::CopySetInfo();
        valueCopysetInfo->set_poolid(info.GetPoolId());
        valueCopysetInfo->set_copysetid(info.GetId());
        // set peers
        for (auto const& msId : info.GetCopySetMembers()) {
            MetaServer ms;
            if (topology_->GetMetaServer(msId, &ms)) {
                common::Peer* peer = valueCopysetInfo->add_peers();
                peer->set_id(ms.GetId());
                peer->set_address(BuildPeerIdWithIpPort(ms.GetInternalIp(),
                                                        ms.GetInternalPort()));
            } else {
                LOG(ERROR) << "perrs: poolId=" << poolId
                           << " copysetid=" << copysetId
                           << " has metaServer error, metaserverId = " << msId;
                copysetValue->set_statuscode(
                    TopoStatusCode::TOPO_METASERVER_NOT_FOUND);
            }
        }
        valueCopysetInfo->set_epoch(info.GetEpoch());

        // set leader peer
        auto msId = info.GetLeader();
        MetaServer ms;
        auto peer = new common::Peer();
        if (topology_->GetMetaServer(msId, &ms)) {
            peer->set_id(ms.GetId());
            peer->set_address(BuildPeerIdWithIpPort(ms.GetInternalIp(),
                                                    ms.GetInternalPort()));
        } else {
            LOG(WARNING) << "leaderpeer: poolId=" << poolId
                         << " copysetid=" << copysetId
                         << " has metaServer error, metaserverId = " << msId;
            copysetValue->set_statuscode(
                TopoStatusCode::TOPO_METASERVER_NOT_FOUND);
        }
        valueCopysetInfo->set_allocated_leaderpeer(peer);

        // set partitioninfolist
        for (auto const& i : info.GetPartitionIds()) {
            Partition tmp;
            if (!topology_->GetPartition(i, &tmp)) {
                LOG(WARNING) << "poolId=" << poolId
                             << " copysetid=" << copysetId
                             << " has pattition error, partitionId=" << i;
                copysetValue->set_statuscode(
                    TopoStatusCode::TOPO_PARTITION_NOT_FOUND);
            } else {
                auto partition = valueCopysetInfo->add_partitioninfolist();
                partition->set_fsid(tmp.GetFsId());
                partition->set_poolid(tmp.GetPoolId());
                partition->set_copysetid(tmp.GetCopySetId());
                partition->set_start(tmp.GetIdStart());
                partition->set_end(tmp.GetIdEnd());
                partition->set_txid(tmp.GetTxId());
                partition->set_status(tmp.GetStatus());
                partition->set_inodenum(tmp.GetInodeNum());
                partition->set_dentrynum(tmp.GetDentryNum());
            }
        }

        copysetValue->set_allocated_copysetinfo(valueCopysetInfo);
    } else {
        LOG(ERROR) << "Get copyset failed."
                   << " poolId=" << poolId << " copysetId=" << copysetId;
        copysetValue->set_statuscode(TopoStatusCode::TOPO_COPYSET_NOT_FOUND);
    }
}

void TopologyManager::GetCopysetsInfo(const GetCopysetsInfoRequest* request,
                                      GetCopysetsInfoResponse* response) {
    for (auto const& i : request->copysetkeys()) {
        GetCopysetInfo(i.poolid(), i.copysetid(),
                       response->add_copysetvalues());
    }
}

void TopologyManager::ListCopysetsInfo(ListCopysetInfoResponse *response) {
    auto cpysetInfoVec = topology_->ListCopysetInfo();
    for (auto const &i : cpysetInfoVec) {
        auto copysetValue = response->add_copysetvalues();
        // default is ok, when find error set to error code
        copysetValue->set_statuscode(TopoStatusCode::TOPO_OK);
        auto valueCopysetInfo = new curvefs::mds::heartbeat::CopySetInfo();
        valueCopysetInfo->set_poolid(i.GetPoolId());
        valueCopysetInfo->set_copysetid(i.GetId());
        // set peers
        for (auto const& msId : i.GetCopySetMembers()) {
            MetaServer ms;
            if (topology_->GetMetaServer(msId, &ms)) {
                common::Peer* peer = valueCopysetInfo->add_peers();
                peer->set_id(ms.GetId());
                peer->set_address(BuildPeerIdWithIpPort(ms.GetInternalIp(),
                                                        ms.GetInternalPort()));
            } else {
                LOG(ERROR) << "perrs: poolId=" << i.GetPoolId()
                           << " copysetid=" << i.GetId()
                           << " has metaServer error, metaserverId = " << msId;
                copysetValue->set_statuscode(
                    TopoStatusCode::TOPO_METASERVER_NOT_FOUND);
            }
        }
        valueCopysetInfo->set_epoch(i.GetEpoch());

        // set leader peer
        auto msId = i.GetLeader();
        MetaServer ms;
        auto peer = new common::Peer();
        if (topology_->GetMetaServer(msId, &ms)) {
            peer->set_id(ms.GetId());
            peer->set_address(BuildPeerIdWithIpPort(ms.GetInternalIp(),
                                                    ms.GetInternalPort()));
        } else {
            LOG(WARNING) << "leaderpeer: poolId=" << i.GetPoolId()
                         << " copysetid=" << i.GetId()
                         << " has metaServer error, metaserverId = " << msId;
            copysetValue->set_statuscode(
                TopoStatusCode::TOPO_METASERVER_NOT_FOUND);
        }
        valueCopysetInfo->set_allocated_leaderpeer(peer);

        // set partitioninfolist
        for (auto const& j : i.GetPartitionIds()) {
            Partition tmp;
            if (!topology_->GetPartition(j, &tmp)) {
                LOG(WARNING) << "poolId=" << i.GetPoolId()
                             << " copysetid=" << i.GetId()
                             << " has pattition error, partitionId=" << j;
                copysetValue->set_statuscode(
                    TopoStatusCode::TOPO_PARTITION_NOT_FOUND);
            } else {
                *valueCopysetInfo->add_partitioninfolist() =
                    std::move(common::PartitionInfo(tmp));
            }
        }

        copysetValue->set_allocated_copysetinfo(valueCopysetInfo);
    }
}

void TopologyManager::GetMetaServersSpace(
    ::google::protobuf::RepeatedPtrField<curvefs::mds::topology::MetadataUsage>
        *spaces) {
    topology_->GetMetaServersSpace(spaces);
}

void TopologyManager::GetTopology(ListTopologyResponse *response) {
    // cluster info
    ClusterInformation info;
    if (topology_->GetClusterInfo(&info)) {
        response->set_clusterid(info.clusterId);
    } else {
        response->set_clusterid("unknown");
    }

    ListPool(nullptr, response->mutable_pools());
    ListZone(response->mutable_zones());
    ListServer(response->mutable_servers());
    ListMetaserverOfCluster(response->mutable_metaservers());
}

void TopologyManager::ListZone(ListZoneResponse* response) {
    response->set_statuscode(TopoStatusCode::TOPO_OK);
    auto zoneIdVec = topology_->GetZoneInCluster();
    for (auto const& zoneId : zoneIdVec) {
        Zone zone;
        if (topology_->GetZone(zoneId, &zone)) {
            auto zoneInfo = response->add_zoneinfos();
            zoneInfo->set_zoneid(zone.GetId());
            zoneInfo->set_zonename(zone.GetName());
            zoneInfo->set_poolid(zone.GetPoolId());
        } else {
            LOG(ERROR) << "Topology has counter an internal error: "
                       << "[func:] ListZone, "
                       << "[msg:] Zone not found, id = " << zoneId;
            response->set_statuscode(TopoStatusCode::TOPO_INTERNAL_ERROR);
        }
    }
}

void TopologyManager::ListServer(ListServerResponse* response) {
    response->set_statuscode(TopoStatusCode::TOPO_OK);
    auto serverIdVec = topology_->GetServerInCluster();
    for (auto const& serverId : serverIdVec) {
        Server server;
        if (topology_->GetServer(serverId, &server)) {
            auto serverInfo = response->add_serverinfos();
            serverInfo->set_serverid(server.GetId());
            serverInfo->set_hostname(server.GetHostName());
            serverInfo->set_internalip(server.GetInternalIp());
            serverInfo->set_internalport(server.GetInternalPort());
            serverInfo->set_externalip(server.GetExternalIp());
            serverInfo->set_externalport(server.GetExternalPort());
            serverInfo->set_zoneid(server.GetZoneId());
            serverInfo->set_poolid(server.GetPoolId());
        } else {
            LOG(ERROR) << "Topology has counter an internal error: "
                       << "[func:] ListServer, "
                       << "[msg:] Server not found, id = " << serverId;
            response->set_statuscode(TopoStatusCode::TOPO_INTERNAL_ERROR);
        }
    }
}

void TopologyManager::ListMetaserverOfCluster(
    ListMetaServerResponse* response) {
    response->set_statuscode(TopoStatusCode::TOPO_OK);
    auto metaserverIdList = topology_->GetMetaServerInCluster();
    for (auto const& id : metaserverIdList) {
        MetaServer ms;
        if (topology_->GetMetaServer(id, &ms)) {
            MetaServerInfo* msInfo = response->add_metaserverinfos();
            msInfo->set_metaserverid(ms.GetId());
            msInfo->set_hostname(ms.GetHostName());
            msInfo->set_internalip(ms.GetInternalIp());
            msInfo->set_internalport(ms.GetInternalPort());
            msInfo->set_externalip(ms.GetExternalIp());
            msInfo->set_externalport(ms.GetExternalPort());
            msInfo->set_onlinestate(ms.GetOnlineState());
            msInfo->set_serverid(ms.GetServerId());
        } else {
            LOG(ERROR) << "Topology has counter an internal error: "
                       << "[func:] ListMetaServerOfCluster, "
                       << "[msg:] metaserver not found, id = " << id;
            response->set_statuscode(TopoStatusCode::TOPO_INTERNAL_ERROR);
            return;
        }
    }
}

TopoStatusCode TopologyManager::UpdatePartitionStatus(
    PartitionIdType partitionId, PartitionStatus status) {
    return topology_->UpdatePartitionStatus(partitionId, status);
}

}  // namespace topology
}  // namespace mds
}  // namespace curvefs
