/*
 * Project: curve
 * Created Date: Mon Aug 27 2018
 * Author: xuchaojie
 * Copyright (c) 2018 netease
 */

#include "src/mds/topology/topology_service_manager.h"

#include <sys/time.h>
#include <sys/types.h>

#include <set>
#include <string>
#include <list>
#include <vector>
#include <chrono>  //NOLINT
#include <thread>  //NOLINT

#include "brpc/channel.h"
#include "brpc/controller.h"
#include "brpc/server.h"
#include "proto/copyset.pb.h"

#include "src/common/concurrent/concurrent.h"
#include "src/common/concurrent/name_lock.h"

namespace curve {
namespace mds {
namespace topology {

using ::curve::chunkserver::CopysetService_Stub;
using ::curve::chunkserver::CopysetRequest2;
using ::curve::chunkserver::CopysetResponse2;
using ::curve::chunkserver::COPYSET_OP_STATUS;

using ::curve::mds::copyset::ClusterInfo;
using ::curve::mds::copyset::CopysetPermutationPolicy;
using ::curve::mds::copyset::Copyset;
using ::curve::mds::copyset::CopysetConstrait;

void TopologyServiceManager::RegistChunkServer(
    const ChunkServerRegistRequest *request,
    ChunkServerRegistResponse *response) {
    std::string hostIp = request->hostip();
    uint32_t port = request->port();
    ::curve::common::NameLockGuard lock(registCsMutex,
        hostIp + ":" + std::to_string(port));

    // 需要为Retired或offline情况才能换盘，否则视为ipPort重复的chunkserver
    std::vector<ChunkServerIdType> list =
        topology_->GetChunkServerInCluster(
            [&hostIp, &port](const ChunkServer &cs){
                return (cs.GetStatus() != ChunkServerStatus::RETIRED) &&
                       (cs.GetOnlineState() != OnlineState::OFFLINE) &&
                       (cs.GetHostIp() == hostIp) &&
                       (cs.GetPort() == port);
            });
    if (1 == list.size()) {
        // 处理重复的注册报文，保证接口的幂等性
        ChunkServer cs;
        topology_->GetChunkServer(list[0], &cs);
        response->set_statuscode(kTopoErrCodeSuccess);
        response->set_chunkserverid(cs.GetId());
        response->set_token(cs.GetToken());
        LOG(WARNING) << "Received duplicated registChunkServer message, "
                      << "hostip = " << hostIp
                      << ", port = " << port;
        return;
    } else if (list.size() > 1) {
        response->set_statuscode(kTopoErrCodeInternalError);
        LOG(ERROR) << "Topology has counter an internal error: "
            "Found chunkServer data ipPort duplicated.";
        return;
    }

    ServerIdType serverId =
        topology_->FindServerByHostIpPort(request->hostip(), request->port());
    if (serverId ==
        static_cast<ServerIdType>(UNINTIALIZE_ID)) {
        response->set_statuscode(kTopoErrCodeServerNotFound);
        return;
    }

    ChunkServerIdType chunkServerId = topology_->AllocateChunkServerId();
    if (chunkServerId ==
        static_cast<ChunkServerIdType>(
            UNINTIALIZE_ID)) {
        response->set_statuscode(kTopoErrCodeAllocateIdFail);
        return;
    }

    std::string token = topology_->AllocateToken();

    ChunkServer chunkserver(
        chunkServerId,
        token,
        request->disktype(),
        serverId,
        request->hostip(),
        request->port(),
        request->diskpath());
    chunkserver.SetOnlineState(ONLINE);

    int errcode = topology_->AddChunkServer(chunkserver);
    if (errcode == kTopoErrCodeSuccess) {
        response->set_statuscode(kTopoErrCodeSuccess);
        response->set_chunkserverid(chunkserver.GetId());
        response->set_token(chunkserver.GetToken());
    } else {
        response->set_statuscode(errcode);
    }
}

void TopologyServiceManager::ListChunkServer(
    const ListChunkServerRequest *request,
    ListChunkServerResponse *response) {
    Server server;
    if (request->has_ip()) {
        uint32_t port = 0;
        if (request->has_port()) {
            port = request->port();
        }
        if (!topology_->GetServerByHostIpPort(request->ip(), port, &server)) {
            response->set_statuscode(kTopoErrCodeServerNotFound);
            return;
        }
    } else if (request->has_serverid()) {
        if (!topology_->GetServer(request->serverid(), &server)) {
            response->set_statuscode(kTopoErrCodeServerNotFound);
            return;
        }
    } else {
        response->set_statuscode(kTopoErrCodeInvalidParam);
        return;
    }

    std::list<ChunkServerIdType> chunkserverList = server.GetChunkServerList();

    response->set_statuscode(kTopoErrCodeSuccess);
    for (ChunkServerIdType id : chunkserverList) {
        ChunkServer cs;
        if (topology_->GetChunkServer(id, &cs)) {
            ChunkServerInfo *csInfo = response->add_chunkserverinfos();
            csInfo->set_chunkserverid(cs.GetId());
            csInfo->set_disktype(cs.GetDiskType());
            csInfo->set_hostip(server.GetInternalHostIp());
            csInfo->set_port(cs.GetPort());
            csInfo->set_status(cs.GetStatus());
            csInfo->set_onlinestate(cs.GetOnlineState());

            ChunkServerState st = cs.GetChunkServerState();
            csInfo->set_diskstatus(st.GetDiskState());
            csInfo->set_mountpoint(cs.GetMountPoint());
            csInfo->set_diskcapacity(st.GetDiskCapacity());
            csInfo->set_diskused(st.GetDiskUsed());
        } else {
            LOG(ERROR) << "Topology has counter an internal error: "
                       << "[func:] ListChunkServer, "
                       << "[msg:] chunkserver not found, id = "
                       << id;
            response->set_statuscode(kTopoErrCodeInternalError);
            return;
        }
    }
}

void TopologyServiceManager::GetChunkServer(
    const GetChunkServerInfoRequest *request,
    GetChunkServerInfoResponse *response) {
    ChunkServer cs;
    if (request->has_chunkserverid()) {
        if (!topology_->GetChunkServer(request->chunkserverid(), &cs)) {
            response->set_statuscode(kTopoErrCodeChunkServerNotFound);
            return;
        }
    } else if (request->has_hostip() && request->has_port()) {
        if (!topology_->GetChunkServerNotRetired(request->hostip(),
                                       request->port(), &cs)) {
            response->set_statuscode(kTopoErrCodeChunkServerNotFound);
            return;
        }
    } else {
        response->set_statuscode(kTopoErrCodeInvalidParam);
        return;
    }

    response->set_statuscode(kTopoErrCodeSuccess);
    ChunkServerInfo *csInfo = new ChunkServerInfo();

    csInfo->set_chunkserverid(cs.GetId());
    csInfo->set_disktype(cs.GetDiskType());
    csInfo->set_hostip(cs.GetHostIp());
    csInfo->set_port(cs.GetPort());
    csInfo->set_status(cs.GetStatus());
    csInfo->set_onlinestate(cs.GetOnlineState());

    ChunkServerState st = cs.GetChunkServerState();
    csInfo->set_diskstatus(st.GetDiskState());
    csInfo->set_mountpoint(cs.GetMountPoint());
    csInfo->set_diskcapacity(st.GetDiskCapacity());
    csInfo->set_diskused(st.GetDiskUsed());
    response->set_allocated_chunkserverinfo(csInfo);
}

void TopologyServiceManager::DeleteChunkServer(
    const DeleteChunkServerRequest *request,
    DeleteChunkServerResponse *response) {
    int errcode = topology_->RemoveChunkServer(request->chunkserverid());
    response->set_statuscode(errcode);
}

void TopologyServiceManager::SetChunkServer(
    const SetChunkServerStatusRequest *request,
    SetChunkServerStatusResponse *response) {
    ChunkServer chunkserver;
    bool find = topology_->GetChunkServer(request->chunkserverid(),
                                          &chunkserver);

    if (find != true) {
        response->set_statuscode(kTopoErrCodeChunkServerNotFound);
        return;
    } else {
        int errcode = topology_->UpdateChunkServerRwState(
            request->chunkserverstatus(), request->chunkserverid());
        response->set_statuscode(errcode);
    }
}

void TopologyServiceManager::RegistServer(const ServerRegistRequest *request,
                                          ServerRegistResponse *response) {
    if ((!request->has_hostname()) &&
        (!request->has_internalip()) &&
        (!request->has_externalip()) &&
        (!request->has_desc())) {
        response->set_statuscode(kTopoErrCodeInvalidParam);
        return;
    }

    PhysicalPool pPool;
    if (request->has_physicalpoolid()) {
        if (!topology_->GetPhysicalPool(request->physicalpoolid(), &pPool)) {
            response->set_statuscode(kTopoErrCodePhysicalPoolNotFound);
            return;
        }
    } else if (request->has_physicalpoolname()) {
        if (!topology_->GetPhysicalPool(request->physicalpoolname(), &pPool)) {
            response->set_statuscode(kTopoErrCodePhysicalPoolNotFound);
            return;
        }
    } else {
        response->set_statuscode(kTopoErrCodeInvalidParam);
        return;
    }
    Zone zone;
    if (request->has_zoneid()) {
        if (!topology_->GetZone(request->zoneid(), &zone)) {
            response->set_statuscode(kTopoErrCodeZoneNotFound);
            return;
        }
    } else if (request->has_zonename()) {
        if (!topology_->GetZone(request->zonename(), pPool.GetId(), &zone)) {
            response->set_statuscode(kTopoErrCodeZoneNotFound);
            return;
        }
    } else {
        response->set_statuscode(kTopoErrCodeInvalidParam);
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

    // 检查ip Port是否重复
    if (topology_->FindServerByHostIpPort(
        request->internalip(), internalPort) !=
        static_cast<ServerIdType>(UNINTIALIZE_ID)) {
        response->set_statuscode(kTopoErrCodeIpPortDuplicated);
        return;
    } else if (topology_->FindServerByHostIpPort(
        request->externalip(), externalPort) !=
        static_cast<ServerIdType>(UNINTIALIZE_ID)) {
        response->set_statuscode(kTopoErrCodeIpPortDuplicated);
        return;
    }

    ServerIdType serverId = topology_->AllocateServerId();
    if (serverId ==
        static_cast<ServerIdType>(UNINTIALIZE_ID)) {
        response->set_statuscode(kTopoErrCodeAllocateIdFail);
        return;
    }

    Server server(serverId,
                  request->hostname(),
                  request->internalip(),
                  internalPort,
                  request->externalip(),
                  externalPort,
                  zone.GetId(),
                  pPool.GetId(),
                  request->desc());

    int errcode = topology_->AddServer(server);
    if (kTopoErrCodeSuccess == errcode) {
        response->set_statuscode(kTopoErrCodeSuccess);
        response->set_serverid(serverId);
    } else {
        response->set_statuscode(errcode);
    }
}

void TopologyServiceManager::GetServer(const GetServerRequest *request,
                                       GetServerResponse *response) {
    Server sv;
    if (request->has_serverid()) {
        if (!topology_->GetServer(request->serverid(), &sv)) {
            response->set_statuscode(kTopoErrCodeServerNotFound);
            return;
        }
    } else if (request->has_hostname()) {
        if (!topology_->GetServerByHostName(request->hostname(), &sv)) {
            response->set_statuscode(kTopoErrCodeServerNotFound);
            return;
        }
    } else if (request->has_hostip()) {
        uint32_t port = 0;
        if (request->has_port()) {
            port = request->port();
        }
        if (!topology_->GetServerByHostIpPort(request->hostip(), port, &sv)) {
            response->set_statuscode(kTopoErrCodeServerNotFound);
            return;
        }
    }
    Zone zone;
    if (!topology_->GetZone(sv.GetZoneId(), &zone)) {
        LOG(ERROR) << "Topology has counter an internal error: "
                   << " Server belong Zone not found, ServerId = "
                   << sv.GetId()
                   << " ZoneId = "
                   << sv.GetZoneId();
        response->set_statuscode(kTopoErrCodeInternalError);
        return;
    }
    PhysicalPool pPool;
    if (!topology_->GetPhysicalPool(zone.GetPhysicalPoolId(), &pPool)) {
        LOG(ERROR) << "Topology has counter an internal error: "
                   << " Zone belong PhysicalPool not found, zoneId = "
                   << zone.GetId()
                   << " physicalPoolId = "
                   << zone.GetPhysicalPoolId();
        response->set_statuscode(kTopoErrCodeInternalError);
        return;
    }
    ServerInfo *info = new ServerInfo();
    info->set_serverid(sv.GetId());
    info->set_hostname(sv.GetHostName());
    info->set_internalip(sv.GetInternalHostIp());
    info->set_internalport(sv.GetInternalPort());
    info->set_externalip(sv.GetExternalHostIp());
    info->set_externalport(sv.GetExternalPort());
    info->set_zoneid(sv.GetZoneId());
    info->set_zonename(zone.GetName());
    info->set_physicalpoolid(sv.GetPhysicalPoolId());
    info->set_physicalpoolname(pPool.GetName());
    info->set_desc(sv.GetDesc());
    response->set_allocated_serverinfo(info);
}

void TopologyServiceManager::DeleteServer(const DeleteServerRequest *request,
                                          DeleteServerResponse *response) {
    int errcode = kTopoErrCodeSuccess;
    Server server;
    if (!topology_->GetServer(request->serverid(), &server)) {
        response->set_statuscode(kTopoErrCodeServerNotFound);
        return;
    }
    for (auto &csId : server.GetChunkServerList()) {
        ChunkServer cs;
        if (!topology_->GetChunkServer(csId, &cs)) {
            LOG(ERROR) << "Topology has counter an internal error: "
                       << ", chunkServer in server not found"
                       << ", chunkserverId = " << csId
                       << ", serverId = " << request->serverid();
            response->set_statuscode(kTopoErrCodeInternalError);
            return;
        } else {
            if (cs.GetStatus() != ChunkServerStatus::RETIRED) {
                LOG(ERROR) << "Cannot Remove Server Which"
                           << "Has ChunkServer Not Retired";
                response->set_statuscode(kTopoErrCodeCannotRemoveWhenNotEmpty);
                return;
            } else  {
                errcode = topology_->RemoveChunkServer(csId);
                if (errcode != kTopoErrCodeSuccess) {
                    response->set_statuscode(errcode);
                    return;
                }
            }
        }
    }
    errcode = topology_->RemoveServer(request->serverid());
    response->set_statuscode(errcode);
}

void TopologyServiceManager::ListZoneServer(
    const ListZoneServerRequest *request,
    ListZoneServerResponse *response) {
    Zone zone;
    if (request->has_zoneid()) {
        if (!topology_->GetZone(request->zoneid(), &zone)) {
            response->set_statuscode(kTopoErrCodeZoneNotFound);
            return;
        }
    } else if (request->has_zonename() &&
        request->has_physicalpoolname()) {
        if (!topology_->GetZone(request->zonename(),
                                request->physicalpoolname(),
                                &zone)) {
            response->set_statuscode(kTopoErrCodeZoneNotFound);
            return;
        }
    } else {
        response->set_statuscode(kTopoErrCodeInvalidParam);
        return;
    }
    response->set_statuscode(kTopoErrCodeSuccess);
    std::list<ServerIdType> serverIdList = zone.GetServerList();
    for (ServerIdType id : serverIdList) {
        Server sv;
        if (topology_->GetServer(id, &sv)) {
            Zone zone;
            if (!topology_->GetZone(sv.GetZoneId(), &zone)) {
                LOG(ERROR) << "Topology has counter an internal error: "
                           << " Server belong Zone not found, ServerId = "
                           << sv.GetId()
                           << " ZoneId = "
                           << sv.GetZoneId();
                response->set_statuscode(kTopoErrCodeInternalError);
                return;
            }
            PhysicalPool pPool;
            if (!topology_->GetPhysicalPool(zone.GetPhysicalPoolId(), &pPool)) {
                LOG(ERROR) << "Topology has counter an internal error: "
                           << " Zone belong PhysicalPool not found, zoneId = "
                           << zone.GetId()
                           << " physicalPoolId = "
                           << zone.GetPhysicalPoolId();
                response->set_statuscode(kTopoErrCodeInternalError);
                return;
            }
            ServerInfo *info = response->add_serverinfo();
            info->set_serverid(sv.GetId());
            info->set_hostname(sv.GetHostName());
            info->set_internalip(sv.GetInternalHostIp());
            info->set_internalport(sv.GetInternalPort());
            info->set_externalip(sv.GetExternalHostIp());
            info->set_externalport(sv.GetExternalPort());
            info->set_zoneid(sv.GetZoneId());
            info->set_zonename(zone.GetName());
            info->set_physicalpoolid(sv.GetPhysicalPoolId());
            info->set_physicalpoolname(pPool.GetName());
            info->set_desc(sv.GetDesc());
        } else {
            LOG(ERROR) << "Topology has counter an internal error: "
                       << "[func:] ListZoneServer, "
                       << "[msg:] server not found, id = "
                       << id;
            response->set_statuscode(kTopoErrCodeInternalError);
            return;
        }
    }
}

void TopologyServiceManager::CreateZone(const ZoneRequest *request,
                                        ZoneResponse *response) {
    if ((request->has_zonename()) &&
        (request->has_physicalpoolname()) &&
        (request->has_desc())) {
        PhysicalPool pPool;
        if (!topology_->GetPhysicalPool(request->physicalpoolname(), &pPool)) {
            response->set_statuscode(kTopoErrCodePhysicalPoolNotFound);
            return;
        }
        ZoneIdType zid = topology_->AllocateZoneId();
        if (zid ==
            static_cast<ZoneIdType>(UNINTIALIZE_ID)) {
            response->set_statuscode(kTopoErrCodeAllocateIdFail);
            return;
        }
        Zone zone(zid,
            request->zonename(),
            pPool.GetId(),
            request->desc());
        int errcode = topology_->AddZone(zone);
        if (kTopoErrCodeSuccess == errcode) {
            response->set_statuscode(errcode);
            ZoneInfo *info = new ZoneInfo();
            info->set_zoneid(zid);
            info->set_zonename(request->zonename());
            info->set_physicalpoolid(pPool.GetId());
            info->set_physicalpoolname(pPool.GetName());
            info->set_desc(request->desc());
            response->set_allocated_zoneinfo(info);
        } else {
            response->set_statuscode(errcode);
        }
    } else {
        response->set_statuscode(kTopoErrCodeInvalidParam);
    }
}

void TopologyServiceManager::DeleteZone(const ZoneRequest *request,
                                        ZoneResponse *response) {
    Zone zone;
    if (request->has_zoneid()) {
        if (!topology_->GetZone(request->zoneid(), &zone)) {
            response->set_statuscode(kTopoErrCodeZoneNotFound);
            return;
        }
    } else if ((request->has_zonename()) && (request->has_physicalpoolname())) {
        if (!topology_->GetZone(request->zonename(),
                                request->physicalpoolname(), &zone)) {
            response->set_statuscode(kTopoErrCodeZoneNotFound);
            return;
        }
    } else {
        response->set_statuscode(kTopoErrCodeInvalidParam);
        return;
    }
    int errcode = topology_->RemoveZone(zone.GetId());
    response->set_statuscode(errcode);
}

void TopologyServiceManager::GetZone(const ZoneRequest *request,
                                     ZoneResponse *response) {
    Zone zone;
    if (request->has_zoneid()) {
        if (!topology_->GetZone(request->zoneid(), &zone)) {
            response->set_statuscode(kTopoErrCodeZoneNotFound);
            return;
        }
    } else if ((request->has_zonename()) && (request->has_physicalpoolname())) {
        if (!topology_->GetZone(request->zonename(),
                                request->physicalpoolname(), &zone)) {
            response->set_statuscode(kTopoErrCodeZoneNotFound);
            return;
        }
    } else {
        response->set_statuscode(kTopoErrCodeInvalidParam);
        return;
    }
    PhysicalPool pPool;
    if (!topology_->GetPhysicalPool(zone.GetPhysicalPoolId(), &pPool)) {
        response->set_statuscode(kTopoErrCodeInternalError);
        return;
    }
    response->set_statuscode(kTopoErrCodeSuccess);
    ZoneInfo *info = new ZoneInfo();
    info->set_zoneid(zone.GetId());
    info->set_zonename(zone.GetName());
    info->set_physicalpoolid((zone.GetPhysicalPoolId()));
    info->set_physicalpoolname(pPool.GetName());
    info->set_desc(zone.GetDesc());
    response->set_allocated_zoneinfo(info);
}

void TopologyServiceManager::ListPoolZone(const ListPoolZoneRequest* request,
    ListPoolZoneResponse* response) {
    PhysicalPool pPool;
    if (request->has_physicalpoolid()) {
        if (!topology_->GetPhysicalPool(request->physicalpoolid(), &pPool)) {
            response->set_statuscode(kTopoErrCodePhysicalPoolNotFound);
            return;
        }
    } else if (request->has_physicalpoolname()) {
        if (!topology_->GetPhysicalPool(request->physicalpoolname(), &pPool)) {
            response->set_statuscode(kTopoErrCodePhysicalPoolNotFound);
            return;
        }
    } else {
        response->set_statuscode(kTopoErrCodeInvalidParam);
        return;
    }
    std::list<ZoneIdType> zidList = pPool.GetZoneList();
    response->set_statuscode(kTopoErrCodeSuccess);
    for (ZoneIdType id : zidList) {
        Zone zone;
        if (topology_->GetZone(id, &zone)) {
            ZoneInfo *info = response->add_zones();
            info->set_zoneid(zone.GetId());
            info->set_zonename(zone.GetName());
            info->set_physicalpoolid(pPool.GetId());
            info->set_physicalpoolname(pPool.GetName());
            info->set_desc(zone.GetDesc());
        } else {
            LOG(ERROR) << "Topology has counter an internal error: "
                       << "[func:] ListPoolZone, "
                       << "[msg:] Zone not found, id = "
                       << id;
            response->set_statuscode(kTopoErrCodeInternalError);
            return;
        }
    }
}

void TopologyServiceManager::CreatePhysicalPool(
    const PhysicalPoolRequest *request,
    PhysicalPoolResponse *response) {
    if ((request->has_physicalpoolname()) &&
        (request->has_desc())) {
        PoolIdType pid = topology_->AllocatePhysicalPoolId();
        if (pid ==
            static_cast<PoolIdType>(UNINTIALIZE_ID)) {
            response->set_statuscode(kTopoErrCodeAllocateIdFail);
            return;
        }
        PhysicalPool pool(pid,
                          request->physicalpoolname(),
                          request->desc());

        int errcode = topology_->AddPhysicalPool(pool);
        if (kTopoErrCodeSuccess == errcode) {
            response->set_statuscode(errcode);
            PhysicalPoolInfo *info = new PhysicalPoolInfo();
            info->set_physicalpoolid(pid);
            info->set_physicalpoolname(request->physicalpoolname());
            info->set_desc(request->desc());
            response->set_allocated_physicalpoolinfo(info);
        } else {
            response->set_statuscode(errcode);
        }
    } else {
        response->set_statuscode(kTopoErrCodeInvalidParam);
    }
}

void TopologyServiceManager::DeletePhysicalPool(
    const PhysicalPoolRequest *request,
    PhysicalPoolResponse *response) {
    PhysicalPool pool;
    if (request->has_physicalpoolid()) {
        if (!topology_->GetPhysicalPool(request->physicalpoolid(), &pool)) {
            response->set_statuscode(kTopoErrCodePhysicalPoolNotFound);
            return;
        }
    } else if (request->has_physicalpoolname()) {
        if (!topology_->GetPhysicalPool(request->physicalpoolname(), &pool)) {
            response->set_statuscode(kTopoErrCodePhysicalPoolNotFound);
            return;
        }
    } else {
        response->set_statuscode(kTopoErrCodeInvalidParam);
        return;
    }

    int errcode = topology_->RemovePhysicalPool(pool.GetId());
    response->set_statuscode(errcode);
}

void TopologyServiceManager::GetPhysicalPool(const PhysicalPoolRequest *request,
                                             PhysicalPoolResponse *response) {
    PhysicalPool pool;
    if (request->has_physicalpoolid()) {
        if (!topology_->GetPhysicalPool(request->physicalpoolid(), &pool)) {
            response->set_statuscode(kTopoErrCodePhysicalPoolNotFound);
            return;
        }
    } else if (request->has_physicalpoolname()) {
        if (!topology_->GetPhysicalPool(request->physicalpoolname(), &pool)) {
            response->set_statuscode(kTopoErrCodePhysicalPoolNotFound);
            return;
        }
    } else {
        response->set_statuscode(kTopoErrCodeInvalidParam);
        return;
    }

    response->set_statuscode(kTopoErrCodeSuccess);
    PhysicalPoolInfo *info = new PhysicalPoolInfo();
    info->set_physicalpoolid(pool.GetId());
    info->set_physicalpoolname(pool.GetName());
    info->set_desc(pool.GetDesc());
    response->set_allocated_physicalpoolinfo(info);
}

void TopologyServiceManager::ListPhysicalPool(
    const ListPhysicalPoolRequest *request,
    ListPhysicalPoolResponse *response) {
    response->set_statuscode(kTopoErrCodeSuccess);
    auto poolList = topology_->GetPhysicalPoolInCluster();
    for (PoolIdType id : poolList) {
        PhysicalPool pool;
        if (topology_->GetPhysicalPool(id, &pool)) {
            PhysicalPoolInfo *info = response->add_physicalpoolinfos();
            info->set_physicalpoolid(pool.GetId());
            info->set_physicalpoolname(pool.GetName());
            info->set_desc(pool.GetDesc());
        } else {
            LOG(ERROR) << "Topology has counter an internal error: "
                       << "[func:] ListPhysicalPool, "
                       << "[msg:] PhysicalPool not found, id = "
                       << id;
            response->set_statuscode(kTopoErrCodeInternalError);
            return;
        }
    }
}

int TopologyServiceManager::CreateCopysetForLogicalPool(
    const LogicalPool &lPool,
    uint32_t *scatterWidth,
    std::vector<CopySetInfo> *copysetInfos) {
    switch (lPool.GetLogicalPoolType()) {
        case LogicalPoolType::PAGEFILE: {
            int errcode = GenCopysetForPageFilePool(lPool,
                scatterWidth,
                copysetInfos);
            if (kTopoErrCodeSuccess != errcode) {
                LOG(ERROR) << "CreateCopysetForLogicalPool fail in : "
                           << "GenCopysetForPageFilePool.";
                return errcode;
            }
            errcode = CreateCopysetNodeOnChunkServer(*copysetInfos);
            if (kTopoErrCodeSuccess != errcode) {
                LOG(ERROR) << "CreateCopysetForLogicalPool fail in : "
                           << "CreateCopysetNodeOnChunkServer.";
                return errcode;
            }
            break;
        }
        case LogicalPoolType::APPENDFILE: {
            // TODO(xuchaojie): it is not done.
            LOG(ERROR) << "CreateCopysetForLogicalPool invalid logicalPoolType:"
                       << lPool.GetLogicalPoolType();
            return kTopoErrCodeInvalidParam;
        }
        case LogicalPoolType::APPENDECFILE: {
            // TODO(xuchaojie): it is not done.
            LOG(ERROR) << "CreateCopysetForLogicalPool invalid logicalPoolType:"
                       << lPool.GetLogicalPoolType();
            return kTopoErrCodeInvalidParam;
        }
        default: {
            LOG(ERROR) << "CreateCopysetForLogicalPool invalid logicalPoolType:"
                       << lPool.GetLogicalPoolType();
            return kTopoErrCodeInvalidParam;
        }
    }
    return kTopoErrCodeSuccess;
}

int TopologyServiceManager::GenCopysetForPageFilePool(
    const LogicalPool &lPool,
    uint32_t *scatterWidth,
    std::vector<CopySetInfo> *copysetInfos) {
    ClusterInfo cluster;
    std::list<ChunkServerIdType> csList =
        topology_->GetChunkServerInLogicalPool(lPool.GetId(),
            [] (const ChunkServer &cs) {
                return cs.GetStatus() != ChunkServerStatus::RETIRED;});

    for (ChunkServerIdType id : csList) {
        ChunkServer cs;
        if (topology_->GetChunkServer(id, &cs)) {
            Server belongServer;
            if (topology_->GetServer(cs.GetServerId(), &belongServer)) {
                curve::mds::copyset::ChunkServerInfo csInfo;
                csInfo.id = id;
                csInfo.location.zoneId = belongServer.GetZoneId();
                csInfo.location.logicalPoolId = lPool.GetId();
                cluster.AddChunkServerInfo(csInfo);
            } else {
                LOG(ERROR) << "Topology has counter an internal error: "
                           << "[func:] CreateLogicalPool, "
                           << "[msg:] ChunkServer not found, id = "
                           << id
                           << ", logicalPoolid = "
                           << lPool.GetId();
                return kTopoErrCodeInternalError;
            }
        }
    }

    std::vector<Copyset> copysets;
    LogicalPool::RedundanceAndPlaceMentPolicy rap =
        lPool.GetRedundanceAndPlaceMentPolicy();
    PoolIdType logicalPoolId = lPool.GetId();

    CopysetConstrait constrait;
    constrait.zoneNum = CopysetConstrait::NUM_ANY;
    constrait.zoneChoseNum = rap.pageFileRAP.zoneNum;
    constrait.replicaNum = rap.pageFileRAP.replicaNum;
    if (copysetManager_->Init(constrait)) {
        if (!copysetManager_->GenCopyset(cluster,
            rap.pageFileRAP.copysetNum,
            scatterWidth,
            &copysets)) {
            LOG(ERROR) << "GenCopysetForPageFilePool failed"
                       << ", Cluster size = "
                       << cluster.GetClusterSize()
                       << ", copysetNum = "
                       << rap.pageFileRAP.copysetNum
                       << ", scatterWidth = "
                       << *scatterWidth
                       << ", logicalPoolid = "
                       << lPool.GetId();
            return kTopoErrCodeGenCopysetErr;
        }
    } else {
        LOG(ERROR) << "GenCopysetForPageFilePool invalid param :"
                   << " zoneNum = "
                   << rap.pageFileRAP.zoneNum
                   << " replicaNum = "
                   << rap.pageFileRAP.replicaNum
                   << ", logicalPoolid = "
                   << lPool.GetId();
        return kTopoErrCodeInvalidParam;
    }

    for (const Copyset &cs : copysets) {
        CopySetIdType copysetId =
            topology_->AllocateCopySetId(logicalPoolId);
        if (copysetId ==
            static_cast<PoolIdType>(UNINTIALIZE_ID)) {
            return kTopoErrCodeAllocateIdFail;
        }
        CopySetInfo copysetInfo(logicalPoolId, copysetId);
        copysetInfo.SetCopySetMembers(cs.replicas);
        int errcode = topology_->AddCopySet(copysetInfo);
        if (kTopoErrCodeSuccess != errcode) {
            return errcode;
        }
        copysetInfos->push_back(copysetInfo);
    }
    return kTopoErrCodeSuccess;
}

int TopologyServiceManager::CreateCopysetNodeOnChunkServer(
    const std::vector<CopySetInfo> &copysetInfos) {
    std::set<ChunkServerIdType> chunkserverToSend;
    for (auto &cs : copysetInfos) {
        for (auto &csId : cs.GetCopySetMembers()) {
            chunkserverToSend.insert(csId);
        }
    }
    for (auto &csId : chunkserverToSend) {
        std::vector<CopySetInfo> infos;
        for (auto &cs : copysetInfos) {
            if (cs.GetCopySetMembers().count(csId) != 0) {
                infos.push_back(cs);
            }
        }
        if (!CreateCopysetNodeOnChunkServer(csId, infos)) {
            return kTopoErrCodeCreateCopysetNodeOnChunkServerFail;
        }
    }
    return kTopoErrCodeSuccess;
}

bool TopologyServiceManager::CreateCopysetNodeOnChunkServer(
    ChunkServerIdType id,
    const std::vector<CopySetInfo> &copysetInfos) {
    ChunkServer chunkServer;
    if (true != topology_->GetChunkServer(id, &chunkServer)) {
        return false;
    }

    std::string ip = chunkServer.GetHostIp();
    int port = chunkServer.GetPort();

    brpc::Channel channel;
    if (channel.Init(ip.c_str(), port, NULL) != 0) {
        LOG(ERROR) << "Fail to init channel to ip: "
                   << ip
                   << " port "
                   << port;
        return false;
    }
    CopysetService_Stub stub(&channel);

    // 调用chunkserver接口创建copyset
    brpc::Controller cntl;

    CopysetRequest2 copysetRequest;
    for (auto &cs : copysetInfos) {
        ::curve::chunkserver::Copyset *copyset =
            copysetRequest.add_copysets();
        copyset->set_logicpoolid(cs.GetLogicalPoolId());
        copyset->set_copysetid(cs.GetId());

        for (ChunkServerIdType id : cs.GetCopySetMembers()) {
                ChunkServer chunkserverInfo;
                if (true != topology_->GetChunkServer(id, &chunkserverInfo)) {
                    return false;
                }

                std::string address =
                    BuildPeerId(chunkserverInfo.GetHostIp(),
                    chunkserverInfo.GetPort());

                ::curve::common::Peer *peer = copyset->add_peers();
                peer->set_id(id);
                peer->set_address(address);
        }
    }

    CopysetResponse2 copysetResponse;

    uint32_t retry = 0;

    do {
        cntl.Reset();
        cntl.set_timeout_ms(option_.CreateCopysetRpcTimeoutMs);
        stub.CreateCopysetNode2(&cntl,
            &copysetRequest,
            &copysetResponse,
            nullptr);
        LOG(INFO) << "Send CopysetRequest[log_id=" << cntl.log_id()
                  << "] from " << cntl.local_side()
                  << " to " << cntl.remote_side()
                  << ". [CopysetRequest] : "
                  << " copysetRequest.copysets_size() = "
                  << copysetRequest.copysets_size();
        if (cntl.Failed()) {
            LOG(WARNING) << "Send CopysetRequest failed, "
                       << "cntl.errorText = "
                       << cntl.ErrorText()
                       << ", retry = "
                       << retry;
            std::this_thread::sleep_for(
                std::chrono::milliseconds(
                    option_.CreateCopysetRpcRetrySleepTimeMs));
        }
        retry++;
    }while(cntl.Failed() &&
        retry < option_.CreateCopysetRpcRetryTimes);

    if (cntl.Failed()) {
        LOG(ERROR) << "Send CopysetRequest failed, retry times exceed, "
                   << "cntl.errorText = "
                   << cntl.ErrorText() << std::endl;
        return false;
    } else {
        if ((copysetResponse.status() !=
                COPYSET_OP_STATUS::COPYSET_OP_STATUS_SUCCESS) &&
           (copysetResponse.status() !=
                COPYSET_OP_STATUS::COPYSET_OP_STATUS_EXIST)) {
            LOG(ERROR) << "Received CopysetResponse[log_id="
                      << cntl.log_id()
                      << "] from " << cntl.remote_side()
                      << " to " << cntl.local_side()
                      << ". [CopysetResponse] "
                      << copysetResponse.DebugString();
            return false;
        } else {
            LOG(INFO) << "Received CopysetResponse[log_id="
                      << cntl.log_id()
                      << "] from " << cntl.remote_side()
                      << " to " << cntl.local_side()
                      << ". [CopysetResponse] "
                      << copysetResponse.DebugString();
        }
    }
    return true;
}

int TopologyServiceManager::RemoveErrLogicalPoolAndCopyset(
    const LogicalPool &pool,
    const std::vector<CopySetInfo> *copysetInfos) {
    int errcode = kTopoErrCodeSuccess;
    for (const CopySetInfo& cs : *copysetInfos) {
        errcode = topology_->RemoveCopySet(cs.GetCopySetKey());
        if (kTopoErrCodeSuccess !=
            errcode) {
            LOG(ERROR) << "RemoveCopySet Fail."
                       << " logicalpoolid = " << pool.GetId()
                       << ", copysetId = " << cs.GetId();
            return errcode;
        }
    }
    errcode = topology_->RemoveLogicalPool(pool.GetId());
    if (kTopoErrCodeSuccess !=  errcode) {
        LOG(ERROR) << "RemoveLogicalPool Fail."
                   << " logicalpoolid = " << pool.GetId();
    }
    return errcode;
}

void TopologyServiceManager::CreateLogicalPool(
    const CreateLogicalPoolRequest *request,
    CreateLogicalPoolResponse *response) {
    PhysicalPool pPool;
    if (request->has_physicalpoolid()) {
        if (!topology_->GetPhysicalPool(request->physicalpoolid(), &pPool)) {
            response->set_statuscode(kTopoErrCodePhysicalPoolNotFound);
            return;
        }
    } else if (request->has_physicalpoolname()) {
        if (!topology_->GetPhysicalPool(request->physicalpoolname(), &pPool)) {
            response->set_statuscode(kTopoErrCodePhysicalPoolNotFound);
            return;
        }
    } else {
        response->set_statuscode(kTopoErrCodeInvalidParam);
        return;
    }

    PoolIdType lPoolId = topology_->AllocateLogicalPoolId();
    if (lPoolId ==
        static_cast<PoolIdType>(UNINTIALIZE_ID)) {
        response->set_statuscode(kTopoErrCodeAllocateIdFail);
        return;
    }

    LogicalPool::RedundanceAndPlaceMentPolicy rap;
    if (!LogicalPool::TransRedundanceAndPlaceMentPolicyFromJsonStr(
        request->redundanceandplacementpolicy(),
        request->type(),
        &rap)) {
        LOG(ERROR) << "[TopologyServiceManager::CreateLogicalPool]:"
                   << "parse redundanceandplacementpolicy fail.";
        response->set_statuscode(kTopoErrCodeInvalidParam);
        return;
    }

    LogicalPool::UserPolicy userPolicy;
    if (!LogicalPool::TransUserPolicyFromJsonStr(
        request->userpolicy(),
        request->type(),
        &userPolicy)) {
        LOG(ERROR) << "[TopologyServiceManager::CreateLogicalPool]:"
                   << "parse userpolicy fail.";
        response->set_statuscode(kTopoErrCodeInvalidParam);
        return;
    }

    uint32_t scatterWidth = 0u;
    if (request->has_scatterwidth()) {
        scatterWidth = request->scatterwidth();
    }

    timeval now;
    gettimeofday(&now, NULL);
    uint64_t cTime = now.tv_sec;
    LogicalPool lPool(lPoolId,
    request->logicalpoolname(),
    pPool.GetId(),
    request->type(),
    rap,
    userPolicy,
    cTime,
    false);

    int errcode = topology_->AddLogicalPool(lPool);
    if (kTopoErrCodeSuccess == errcode) {
        std::vector<CopySetInfo> copysetInfos;
        errcode =
            CreateCopysetForLogicalPool(lPool, &scatterWidth, &copysetInfos);
        if (kTopoErrCodeSuccess != errcode) {
            if (kTopoErrCodeSuccess ==
                    RemoveErrLogicalPoolAndCopyset(lPool,
                        &copysetInfos)) {
                response->set_statuscode(errcode);
            } else {
                LOG(ERROR) << "Topology has counter an internal error: "
                           << "recover from CreateCopysetForLogicalPool fail, "
                           << "remove logicalpool and copyset Fail."
                           << " logicalpoolid = " << lPool.GetId();
                response->set_statuscode(kTopoErrCodeInternalError);
            }
        } else {
            lPool.SetLogicalPoolAvaliableFlag(true);
            lPool.SetScatterWidth(scatterWidth);
            // 更新copysetnum
            switch (lPool.GetLogicalPoolType()) {
                case LogicalPoolType::PAGEFILE: {
                    rap.pageFileRAP.copysetNum =  copysetInfos.size();
                    lPool.SetRedundanceAndPlaceMentPolicy(rap);
                    break;
                }
                default: {
                    LOG(ERROR) << "invalid logicalPoolType:"
                               << lPool.GetLogicalPoolType();
                    if (kTopoErrCodeSuccess ==
                            RemoveErrLogicalPoolAndCopyset(lPool,
                                &copysetInfos)) {
                        response->set_statuscode(kTopoErrCodeInvalidParam);
                    } else {
                        LOG(ERROR) << "Topology has counter an internal error: "
                                   << "recover from UpdateLogicalPool fail, "
                                   << "remove logicalpool and copyset Fail."
                                   << " logicalpoolid = "
                                   << lPool.GetId();
                        response->set_statuscode(kTopoErrCodeInternalError);
                    }
                    return;
                }
            }

            errcode = topology_->UpdateLogicalPool(lPool);
            if (kTopoErrCodeSuccess == errcode) {
                response->set_statuscode(kTopoErrCodeSuccess);
                LogicalPoolInfo *info = new LogicalPoolInfo();
                info->set_logicalpoolid(lPoolId);
                info->set_logicalpoolname(request->logicalpoolname());
                info->set_physicalpoolid(pPool.GetId());
                info->set_type(request->type());
                info->set_createtime(cTime);
                info->set_redundanceandplacementpolicy(
                    lPool.GetRedundanceAndPlaceMentPolicyJsonStr());
                info->set_userpolicy(request->userpolicy());
                info->set_allocatestatus(AllocateStatus::ALLOW);
                response->set_allocated_logicalpoolinfo(info);
            } else {
                if (kTopoErrCodeSuccess ==
                        RemoveErrLogicalPoolAndCopyset(lPool,
                            &copysetInfos)) {
                    response->set_statuscode(errcode);
                } else {
                    LOG(ERROR) << "Topology has counter an internal error: "
                               << "recover from UpdateLogicalPool fail, "
                               << "remove logicalpool and copyset Fail."
                               << " logicalpoolid = "
                               << lPool.GetId();
                    response->set_statuscode(kTopoErrCodeInternalError);
                }
            }
        }
    } else {
        response->set_statuscode(errcode);
    }
}

void TopologyServiceManager::DeleteLogicalPool(
    const DeleteLogicalPoolRequest* request,
    DeleteLogicalPoolResponse* response) {
    PoolIdType pid = UNINTIALIZE_ID;
    if (request->has_logicalpoolid()) {
        pid = request->logicalpoolid();
    } else if (request->has_logicalpoolname() &&
        request->has_physicalpoolname()) {
        pid = topology_->FindLogicalPool(request->logicalpoolname(),
                                         request->physicalpoolname());
        if (pid == static_cast<PoolIdType>(
            UNINTIALIZE_ID)) {
            response->set_statuscode(kTopoErrCodeLogicalPoolNotFound);
            return;
        }
    } else {
        response->set_statuscode(kTopoErrCodeInvalidParam);
        return;
    }

    int errcode = topology_->RemoveLogicalPool(pid);
    if (kTopoErrCodeSuccess == errcode) {
        response->set_statuscode(errcode);
    } else {
        response->set_statuscode(errcode);
    }
}

void TopologyServiceManager::GetLogicalPool(
    const GetLogicalPoolRequest *request,
    GetLogicalPoolResponse *response) {
    LogicalPool lPool;
    if (request->has_logicalpoolid()) {
        if (!topology_->GetLogicalPool(request->logicalpoolid(), &lPool)) {
            response->set_statuscode(kTopoErrCodeLogicalPoolNotFound);
            return;
        }
    } else if ((request->has_logicalpoolname()) &&
        (request->has_physicalpoolname())) {
        if (!topology_->GetLogicalPool(request->logicalpoolname(),
                                       request->physicalpoolname(),
                                       &lPool)) {
            response->set_statuscode(kTopoErrCodeLogicalPoolNotFound);
            return;
        }
    } else {
        response->set_statuscode(kTopoErrCodeInvalidParam);
        return;
    }
    response->set_statuscode(kTopoErrCodeSuccess);
    LogicalPoolInfo *info = new LogicalPoolInfo();
    info->set_logicalpoolid(lPool.GetId());
    info->set_logicalpoolname(lPool.GetName());
    info->set_physicalpoolid(lPool.GetPhysicalPoolId());
    info->set_type(lPool.GetLogicalPoolType());
    info->set_createtime(lPool.GetCreateTime());

    std::string rapStr = lPool.GetRedundanceAndPlaceMentPolicyJsonStr();
    std::string policyStr = lPool.GetUserPolicyJsonStr();

    info->set_redundanceandplacementpolicy(rapStr);
    info->set_userpolicy(policyStr);
    info->set_allocatestatus(AllocateStatus::ALLOW);
    response->set_allocated_logicalpoolinfo(info);
}

void TopologyServiceManager::ListLogicalPool(
    const ListLogicalPoolRequest *request,
    ListLogicalPoolResponse *response) {
    PhysicalPool pPool;
    if (request->has_physicalpoolid()) {
        if (!topology_->GetPhysicalPool(request->physicalpoolid(), &pPool)) {
            response->set_statuscode(kTopoErrCodePhysicalPoolNotFound);
            return;
        }
    } else if (request->has_physicalpoolname()) {
        if (!topology_->GetPhysicalPool(request->physicalpoolname(), &pPool)) {
            response->set_statuscode(kTopoErrCodePhysicalPoolNotFound);
            return;
        }
    } else {
        response->set_statuscode(kTopoErrCodeInvalidParam);
        return;
    }

    std::list<PoolIdType> lPoolIdList =
        topology_->GetLogicalPoolInPhysicalPool(pPool.GetId());
    response->set_statuscode(kTopoErrCodeSuccess);
    for (PoolIdType id : lPoolIdList) {
        LogicalPool lPool;
        if (topology_->GetLogicalPool(id, &lPool)) {
            LogicalPoolInfo *info = response->add_logicalpoolinfos();
            info->set_logicalpoolid(lPool.GetId());
            info->set_logicalpoolname(lPool.GetName());
            info->set_physicalpoolid(lPool.GetPhysicalPoolId());
            info->set_type(lPool.GetLogicalPoolType());
            info->set_createtime(lPool.GetCreateTime());

            std::string rapStr = lPool.GetRedundanceAndPlaceMentPolicyJsonStr();
            std::string policyStr = lPool.GetUserPolicyJsonStr();

            info->set_redundanceandplacementpolicy(rapStr);
            info->set_userpolicy(policyStr);
            info->set_allocatestatus(AllocateStatus::ALLOW);
        } else {
            LOG(ERROR) << "Topology has counter an internal error: "
                       << "[func:] ListLogicalPool, "
                       << "[msg:] LogicalPool not found, id = "
                       << id;
            response->set_statuscode(kTopoErrCodeInternalError);
            return;
        }
    }
}

void TopologyServiceManager::GetChunkServerListInCopySets(
    const GetChunkServerListInCopySetsRequest *request,
    GetChunkServerListInCopySetsResponse *response) {
    int copysetNum = request->copysetid_size();
    for (int i = 0; i < copysetNum; i++) {
        CopySetKey key(request->logicalpoolid(), request->copysetid(i));
        CopySetInfo csInfo;
        if (topology_->GetCopySet(key, &csInfo)) {
            CopySetServerInfo *cssInfo = response->add_csinfo();
            cssInfo->set_copysetid(csInfo.GetId());
            for (ChunkServerIdType csId : csInfo.GetCopySetMembers()) {
                ChunkServer cs;
                if (topology_->GetChunkServer(csId, &cs)) {
                    ChunkServerLocation *csLoc = cssInfo->add_cslocs();
                    csLoc->set_chunkserverid(csId);
                    csLoc->set_hostip(cs.GetHostIp());
                    csLoc->set_port(cs.GetPort());
                } else {
                    LOG(ERROR) << "Topology has counter an internal error: "
                               << "[func:] GetChunkServerListInCopySets, "
                               << "[msg:] ChunkServer not found, id = "
                               << csId;
                    response->set_statuscode(kTopoErrCodeInternalError);
                    return;
                }
            }
            response->set_statuscode(kTopoErrCodeSuccess);
        } else {
            response->set_statuscode(kTopoErrCodeCopySetNotFound);
            return;
        }
    }
    response->set_statuscode(kTopoErrCodeSuccess);
    return;
}

void TopologyServiceManager::GetCopySetsInChunkServer(
                      const GetCopySetsInChunkServerRequest* request,
                      GetCopySetsInChunkServerResponse* response) {
    ChunkServer cs;
    if (request->has_chunkserverid()) {
        if (!topology_->GetChunkServer(request->chunkserverid(), &cs)) {
            response->set_statuscode(kTopoErrCodeChunkServerNotFound);
            return;
        }
    } else if (request->has_hostip() && request->has_port()) {
        if (!topology_->GetChunkServerNotRetired(request->hostip(),
                                       request->port(), &cs)) {
            response->set_statuscode(kTopoErrCodeChunkServerNotFound);
            return;
        }
    } else {
        response->set_statuscode(kTopoErrCodeInvalidParam);
        return;
    }

    response->set_statuscode(kTopoErrCodeSuccess);
    std::vector<CopySetKey> copysets =
                    topology_->GetCopySetsInChunkServer(cs.GetId());
    for (const CopySetKey& copyset : copysets) {
        CopysetInfo *info = response->add_copysetinfos();
        info->set_logicalpoolid(copyset.first);
        info->set_copysetid(copyset.second);
    }
}

}  // namespace topology
}  // namespace mds
}  // namespace curve
