/*
 * Project: curve
 * Created Date: Mon Aug 27 2018
 * Author: xuchaojie
 * Copyright (c) 2018 netease
 */

#include "src/mds/topology/topology_service_manager.h"

#include <sys/time.h>
#include <sys/types.h>

#include <string>
#include <list>
#include <vector>

#include "brpc/channel.h"
#include "brpc/controller.h"
#include "brpc/server.h"
#include "proto/copyset.pb.h"

namespace curve {
namespace mds {
namespace topology {

using ::curve::chunkserver::CopysetService_Stub;
using ::curve::chunkserver::CopysetRequest;
using ::curve::chunkserver::CopysetResponse;
using ::curve::chunkserver::COPYSET_OP_STATUS;

using ::curve::mds::copyset::ClusterInfo;
using ::curve::mds::copyset::CopysetPermutationPolicy;
using curve::mds::copyset::Copyset;

void TopologyServiceManager::RegistChunkServer(
    const ChunkServerRegistRequest* request,
    ChunkServerRegistResponse* response) {
    ServerIdType serverId  =
        topology_->FindServerByHostIp(request->hostip());
    if (serverId ==
        static_cast<ServerIdType>(TopologyIdGenerator::UNINTIALIZE_ID)) {
        response->set_statuscode(kTopoErrCodeServerNotFound);
        return;
    }

    ChunkServerIdType chunkServerId = topology_->AllocateChunkServerId();
    if (chunkServerId ==
        static_cast<ChunkServerIdType>(
            TopologyIdGenerator::UNINTIALIZE_ID)) {
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
    ChunkServerState state;
    state.SetOnlineState(OFFLINE);
    chunkserver.SetChunkServerState(state);

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
    const ListChunkServerRequest* request,
    ListChunkServerResponse* response) {
    Server server;
    if (request->has_ip()) {
        if (!topology_->GetServerByHostIp(request->ip(), &server)) {
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

    std::list<ChunkServerIdType>  chunkserverList = server.GetChunkServerList();

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

            ChunkServerState st = cs.GetChunkServerState();
            csInfo->set_diskstatus(st.GetDiskState());
            csInfo->set_onlinestate(st.GetOnlineState());
            csInfo->set_mountpoint(cs.GetMountPoint());
            csInfo->set_diskcapacity(st.GetDiskCapacity());
            csInfo->set_diskused(st.GetDiskUsed());
        } else {
            LOG(ERROR) << "TopologyServiceManager has encounter"
                       << "a internalError."
                       << "[func:] ListChunkServer, "
                       << "[msg:] chunkserver not found, id = "
                       << id;
            response->set_statuscode(kTopoErrCodeInternalError);
            return;
        }
    }
}

void TopologyServiceManager::GetChunkServer(
    const GetChunkServerInfoRequest* request,
    GetChunkServerInfoResponse* response) {
    ChunkServer cs;
    if (request->has_chunkserverid()) {
        if (!topology_->GetChunkServer(request->chunkserverid(), &cs)) {
            response->set_statuscode(kTopoErrCodeChunkServerNotFound);
            return;
        }
    } else if (request->has_hostip() && request->has_port()) {
        if (!topology_->GetChunkServer(request->hostip(),
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

    ChunkServerState st = cs.GetChunkServerState();
    csInfo->set_diskstatus(st.GetDiskState());
    csInfo->set_onlinestate(st.GetOnlineState());
    csInfo->set_mountpoint(cs.GetMountPoint());
    csInfo->set_diskcapacity(st.GetDiskCapacity());
    csInfo->set_diskused(st.GetDiskUsed());
    response->set_allocated_chunkserverinfo(csInfo);
}

void TopologyServiceManager::DeleteChunkServer(
    const DeleteChunkServerRequest* request,
    DeleteChunkServerResponse* response) {
    int errcode = topology_->RemoveChunkServer(request->chunkserverid());
    response->set_statuscode(errcode);
}

void TopologyServiceManager::SetChunkServer(
    const SetChunkServerStatusRequest* request,
    SetChunkServerStatusResponse* response) {
    ChunkServer chunkserver;
    bool find = topology_->GetChunkServer(request->chunkserverid(),
                                          &chunkserver);

    if (find != true) {
        response->set_statuscode(kTopoErrCodeChunkServerNotFound);
        return;
    } else {
        chunkserver.SetStatus(request->chunkserverstatus());
        int errcode = topology_->UpdateChunkServer(chunkserver);
        response->set_statuscode(errcode);
    }
}

void TopologyServiceManager::RegistServer(const ServerRegistRequest* request,
                  ServerRegistResponse* response) {
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

    ServerIdType serverId = topology_->AllocateServerId();
    if (serverId ==
        static_cast<ServerIdType>(TopologyIdGenerator::UNINTIALIZE_ID)) {
        response->set_statuscode(kTopoErrCodeAllocateIdFail);
        return;
    }

    Server server(serverId,
        request->hostname(),
        request->internalip(),
        request->externalip(),
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

void TopologyServiceManager::GetServer(const GetServerRequest* request,
                  GetServerResponse* response) {
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
        if (!topology_->GetServerByHostIp(request->hostip(), &sv)) {
            response->set_statuscode(kTopoErrCodeServerNotFound);
            return;
        }
    }
    ServerInfo *info = new ServerInfo();
    info->set_serverid(sv.GetId());
    info->set_hostname(sv.GetHostName());
    info->set_internalip(sv.GetInternalHostIp());
    info->set_externalip(sv.GetExternalHostIp());
    info->set_zoneid(sv.GetZoneId());
    info->set_physicalpoolid(sv.GetPhysicalPoolId());
    info->set_desc(sv.GetDesc());
    response->set_allocated_serverinfo(info);
}

void TopologyServiceManager::DeleteServer(const DeleteServerRequest* request,
                  DeleteServerResponse* response) {
    int errcode = topology_->RemoveServer(request->serverid());
    response->set_statuscode(errcode);
}

void TopologyServiceManager::ListZoneServer(
    const ListZoneServerRequest* request,
    ListZoneServerResponse* response) {
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
            ServerInfo *info = response->add_serverinfo();
            info->set_serverid(sv.GetId());
            info->set_hostname(sv.GetHostName());
            info->set_internalip(sv.GetInternalHostIp());
            info->set_externalip(sv.GetExternalHostIp());
            info->set_zoneid(sv.GetZoneId());
            info->set_physicalpoolid(sv.GetPhysicalPoolId());
            info->set_desc(sv.GetDesc());
        } else {
            LOG(ERROR) << "TopologyServiceManager has encounter"
                       << "a internalError."
                       << "[func:] ListZoneServer, "
                       << "[msg:] server not found, id = "
                       << id;
            response->set_statuscode(kTopoErrCodeInternalError);
            return;
        }
    }
}

void TopologyServiceManager::CreateZone(const ZoneRequest* request,
                  ZoneResponse* response) {
    if ((request->has_zonename()) &&
        (request->has_physicalpoolname()) &&
        (request->has_desc())) {
        PoolIdType pid = topology_->FindPhysicalPool(
                         request->physicalpoolname());
        if (pid ==
            static_cast<PoolIdType>(TopologyIdGenerator::UNINTIALIZE_ID)) {
            response->set_statuscode(kTopoErrCodePhysicalPoolNotFound);
            return;
        }
        ZoneIdType zid = topology_->AllocateZoneId();
        if (zid ==
            static_cast<ZoneIdType>(TopologyIdGenerator::UNINTIALIZE_ID)) {
            response->set_statuscode(kTopoErrCodeAllocateIdFail);
            return;
        }
        Zone zone(zid,
            request->zonename(),
            pid,
            request->desc());
        int errcode = topology_->AddZone(zone);
        if (kTopoErrCodeSuccess == errcode) {
            response->set_statuscode(errcode);
            ZoneInfo *info = new ZoneInfo();
            info->set_zoneid(zid);
            info->set_zonename(request->zonename());
            info->set_physicalpoolid(pid);
            info->set_desc(request->desc());
            response->set_allocated_zoneinfo(info);
        } else {
            response->set_statuscode(errcode);
        }
    } else {
        response->set_statuscode(kTopoErrCodeInvalidParam);
    }
}

void TopologyServiceManager::DeleteZone(const ZoneRequest* request,
                  ZoneResponse* response) {
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

void TopologyServiceManager::GetZone(const ZoneRequest* request,
                  ZoneResponse* response) {
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
    response->set_statuscode(kTopoErrCodeSuccess);
    ZoneInfo *info = new ZoneInfo();
    info->set_zoneid(zone.GetId());
    info->set_zonename(zone.GetName());
    info->set_physicalpoolid((zone.GetPhysicalPoolId()));
    info->set_desc(zone.GetDesc());
    response->set_allocated_zoneinfo(info);
}

void TopologyServiceManager::ListPoolZone(const ListPoolZoneRequest* request,
                  ListPoolZoneResponse* response) {
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
    std::list<ZoneIdType> zidList = pool.GetZoneList();
    response->set_statuscode(kTopoErrCodeSuccess);
    for (ZoneIdType id : zidList) {
        Zone zone;
        if (topology_->GetZone(id, &zone)) {
            ZoneInfo *info = response->add_zones();
            info->set_zoneid(zone.GetId());
            info->set_zonename(zone.GetName());
            info->set_physicalpoolid((zone.GetPhysicalPoolId()));
            info->set_desc(zone.GetDesc());
        } else {
            LOG(ERROR) << "TopologyServiceManager has encounter"
                       << "a internalError."
                       << "[func:] ListPoolZone, "
                       << "[msg:] Zone not found, id = "
                       << id;
            response->set_statuscode(kTopoErrCodeInternalError);
            return;
        }
    }
}

void TopologyServiceManager::CreatePhysicalPool(
    const PhysicalPoolRequest* request,
    PhysicalPoolResponse* response) {
    if ((request->has_physicalpoolname()) &&
        (request->has_desc())) {
        PoolIdType pid = topology_->AllocatePhysicalPoolId();
        if (pid ==
            static_cast<PoolIdType>(TopologyIdGenerator::UNINTIALIZE_ID)) {
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
    const PhysicalPoolRequest* request,
    PhysicalPoolResponse* response) {
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

void TopologyServiceManager::GetPhysicalPool(const PhysicalPoolRequest* request,
                  PhysicalPoolResponse* response) {
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
    const ListPhysicalPoolRequest* request,
    ListPhysicalPoolResponse* response) {
    response->set_statuscode(kTopoErrCodeSuccess);
    std::list<PoolIdType> poolList = topology_->GetPhysicalPoolInCluster();
    for (PoolIdType id : poolList) {
        PhysicalPool pool;
        if (topology_->GetPhysicalPool(id, &pool)) {
            PhysicalPoolInfo *info = response->add_physicalpoolinfos();
            info->set_physicalpoolid(pool.GetId());
            info->set_physicalpoolname(pool.GetName());
            info->set_desc(pool.GetDesc());
        } else {
            LOG(ERROR) << "TopologyServiceManager has encounter"
                       << "a internalError."
                       << "[func:] ListPhysicalPool, "
                       << "[msg:] PhysicalPool not found, id = "
                       << id;
            response->set_statuscode(kTopoErrCodeInternalError);
            return;
        }
    }
}

int TopologyServiceManager::CreateCopysetForLogicalPool(
    const LogicalPool &lPool) {
    switch (lPool.GetLogicalPoolType()) {
        case LogicalPoolType::PAGEFILE: {
            std::vector<CopySetInfo> copysetInfos;
            int errcode = GenCopysetForPageFilePool(lPool,
                &copysetInfos);
            if (kTopoErrCodeSuccess != errcode) {
                LOG(ERROR) << "CreateCopysetForLogicalPool fail in : "
                           << "GenCopysetForPageFilePool.";
                return errcode;
            }
            errcode = CreateCopysetOnChunkServer(copysetInfos);
            if (kTopoErrCodeSuccess != errcode) {
                LOG(ERROR) << "CreateCopysetForLogicalPool fail in : "
                           << "CreateCopysetOnChunkServer.";
                return errcode;
            }
            break;
        }
        case LogicalPoolType::APPENDFILE: {
            // TODO(xuchaojie): it is not done.
            LOG(ERROR) << "CreateCopysetForLogicalPool invalid logicalPoolType:"
                       << lPool.GetLogicalPoolType();
            return kTopoErrCodeInvalidParam;
            break;
        }
        case LogicalPoolType::APPENDECFILE: {
            // TODO(xuchaojie): it is not done.
            LOG(ERROR) << "CreateCopysetForLogicalPool invalid logicalPoolType:"
                       << lPool.GetLogicalPoolType();
            return kTopoErrCodeInvalidParam;
            break;
        }
        default: {
            LOG(ERROR) << "CreateCopysetForLogicalPool invalid logicalPoolType:"
                       << lPool.GetLogicalPoolType();
            return kTopoErrCodeInvalidParam;
            break;
        }
    }
    return kTopoErrCodeSuccess;
}


int TopologyServiceManager::GenCopysetForPageFilePool(
    const LogicalPool &lPool,
    std::vector<CopySetInfo> *copysetInfos) {
    ClusterInfo cluster;
    std::list<ChunkServerIdType> csList =
        topology_->GetChunkServerInLogicalPool(lPool.GetId());

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
                LOG(ERROR) << "TopologyServiceManager has encounter"
                           << "a internalError."
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
    std::shared_ptr<curve::mds::copyset::CopysetPolicy> policy =
        copysetManager_->GetCopysetPolicy(
            CopysetPermutationPolicy::NUM_ANY,
            rap.pageFileRAP.zoneNum,
            rap.pageFileRAP.replicaNum);
    if (policy != nullptr) {
        if (policy->GenCopyset(cluster,
            rap.pageFileRAP.copysetNum,
            &copysets) != true) {
            LOG(ERROR) << "GenCopysetForPageFilePool error :"
                       << " Cluster size = "
                       << cluster.GetClusterSize()
                       << " copysetNum = "
                       << rap.pageFileRAP.copysetNum
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
            static_cast<PoolIdType>(TopologyIdGenerator::UNINTIALIZE_ID)) {
            return kTopoErrCodeAllocateIdFail;
        }
        CopySetInfo copysetInfo(logicalPoolId, copysetId);
        copysetInfo.SetCopySetMembers(cs.replicas);
        copysetInfos->push_back(copysetInfo);
    }
    // TODO(xuchaojie): 优化以删除事物
    int errcode = topology_->AddCopySetList(*copysetInfos);
    if (kTopoErrCodeSuccess != errcode) {
        return errcode;
    }
    return kTopoErrCodeSuccess;
}


int TopologyServiceManager::CreateCopysetOnChunkServer(
    const std::vector<CopySetInfo> &copysetInfos) {
    for (const CopySetInfo &cs : copysetInfos) {
        for (ChunkServerIdType csId : cs.GetCopySetMembers()) {
            ChunkServer chunkServer;
            topology_->GetChunkServer(csId, &chunkServer);

            std::string ip = chunkServer.GetHostIp();
            int port = chunkServer.GetPort();

            brpc::Channel channel;
            if (channel.Init(ip.c_str(), port, NULL) != 0) {
                LOG(ERROR) << "Fail to init channel to ip: "
                           << ip
                           << " port "
                           << port
                           << std::endl;
                return kTopoErrCodeGenCopysetErr;
            }
            CopysetService_Stub stub(&channel);

            // 调用chunkserver接口创建copyset
            brpc::Controller cntl;
            // TODO(xuchaojie): 添加配置模块，使用配置参数
            cntl.set_timeout_ms(1000);

            CopysetRequest chunkServerRequest;
            chunkServerRequest.set_logicpoolid(cs.GetLogicalPoolId());
            chunkServerRequest.set_copysetid(cs.GetId());

            for (ChunkServerIdType id : cs.GetCopySetMembers()) {
                    ChunkServer chunkserverInfo;
                    topology_->GetChunkServer(id, &chunkserverInfo);
                    std::string ipStr = chunkserverInfo.GetHostIp();
                    std::string portStr =
                        std::to_string(chunkserverInfo.GetPort());
                    chunkServerRequest.add_peerid(ipStr + ":" + portStr);
            }

            CopysetResponse chunkSeverResponse;

            LOG(INFO) << "Send CopysetRequest[log_id=" << cntl.log_id()
                      << "] from " << cntl.local_side()
                      << " to " << cntl.remote_side()
                      << ". [CopysetRequest] "
                      << chunkServerRequest.DebugString();

            stub.CreateCopysetNode(&cntl,
                &chunkServerRequest,
                &chunkSeverResponse,
                nullptr);

            // TODO(xuchaojie): 添加配置模块，使用配置参数
            const int retryTime = 3;
            int retry = 0;
            while (cntl.Failed() && retry < retryTime) {
                LOG(ERROR) << "Received CopysetResponse error, "
                           << "cntl.errorText = "
                           << cntl.ErrorText()
                           << ", retry, time = "
                           << retry;
                stub.CreateCopysetNode(&cntl,
                    &chunkServerRequest,
                    &chunkSeverResponse,
                    nullptr);
                retry++;
            }

            if (cntl.Failed()) {
                LOG(ERROR) << "Received CopysetResponse error, retry fail,"
                           << "cntl.errorText = "
                           << cntl.ErrorText() << std::endl;
                return kTopoErrCodeGenCopysetErr;
            } else {
                if ((chunkSeverResponse.status() !=
                        COPYSET_OP_STATUS::COPYSET_OP_STATUS_SUCCESS) &&
                   (chunkSeverResponse.status() !=
                        COPYSET_OP_STATUS::COPYSET_OP_STATUS_EXIST)) {
                    LOG(ERROR) << "Received CopysetResponse[log_id="
                              << cntl.log_id()
                              << "] from " << cntl.remote_side()
                              << " to " << cntl.local_side()
                              << ". [CopysetResponse] "
                              << chunkSeverResponse.DebugString();
                    return kTopoErrCodeGenCopysetErr;
                } else {
                    LOG(INFO) << "Received CopysetResponse[log_id="
                              << cntl.log_id()
                              << "] from " << cntl.remote_side()
                              << " to " << cntl.local_side()
                              << ". [CopysetResponse] "
                              << chunkSeverResponse.DebugString();
                }
            }
        }
    }
    return kTopoErrCodeSuccess;
}

void TopologyServiceManager::CreateLogicalPool(
    const CreateLogicalPoolRequest* request,
    CreateLogicalPoolResponse* response) {
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
        static_cast<PoolIdType>(TopologyIdGenerator::UNINTIALIZE_ID)) {
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

    timeval now;
    gettimeofday(&now, NULL);
    uint64_t cTime = now.tv_sec;
    LogicalPool lPool(lPoolId,
        request->logicalpoolname(),
        pPool.GetId(),
        request->type(),
        rap,
        userPolicy,
        cTime);

    int errcode = topology_->AddLogicalPool(lPool);
    if (kTopoErrCodeSuccess == errcode) {
        int errcode2 = CreateCopysetForLogicalPool(lPool);
        if (kTopoErrCodeSuccess != errcode2) {
            if (topology_->RemoveLogicalPool(lPool.GetId())) {
                response->set_statuscode(errcode2);
            } else {
                LOG(ERROR) << "[CreateLogicalPool] has counter a internalError:"
                           << "recover from AddLogicalPool, "
                           << "remove logicalpool Fail. logicalpoolid = "
                           << lPool.GetId();
                response->set_statuscode(kTopoErrCodeInternalError);
            }
        } else {
            response->set_statuscode(errcode);
            LogicalPoolInfo *info = new LogicalPoolInfo();
            info->set_logicalpoolid(lPoolId);
            info->set_logicalpoolname(request->logicalpoolname());
            info->set_physicalpoolid(pPool.GetId());
            info->set_type(request->type());
            info->set_createtime(cTime);
            info->set_redundanceandplacementpolicy(
                request->redundanceandplacementpolicy());
            info->set_userpolicy(request->userpolicy());
            info->set_allocatestatus(AllocateStatus::ALLOW);
            response->set_allocated_logicalpoolinfo(info);
        }
    } else {
        response->set_statuscode(errcode);
    }
}

void TopologyServiceManager::DeleteLogicalPool(
    const DeleteLogicalPoolRequest* request,
    DeleteLogicalPoolResponse* response) {
    PoolIdType pid = TopologyIdGenerator::UNINTIALIZE_ID;
    if (request->has_logicalpoolid()) {
        pid = request->logicalpoolid();
    } else if (request->has_logicalpoolname() &&
        request->has_physicalpoolname()) {
        pid = topology_->FindLogicalPool(request->logicalpoolname(),
            request->physicalpoolname());
        if (pid == static_cast<PoolIdType>(
            TopologyIdGenerator::UNINTIALIZE_ID)) {
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
    const GetLogicalPoolRequest* request,
    GetLogicalPoolResponse* response) {
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
    const ListLogicalPoolRequest* request,
    ListLogicalPoolResponse* response) {
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
            LOG(ERROR) << "TopologyServiceManager has encounter"
                       << "a internalError."
                       << "[func:] ListLogicalPool, "
                       << "[msg:] LogicalPool not found, id = "
                       << id;
            response->set_statuscode(kTopoErrCodeInternalError);
            return;
        }
    }
}

void TopologyServiceManager::GetChunkServerListInCopySets(
    const GetChunkServerListInCopySetsRequest* request,
    GetChunkServerListInCopySetsResponse* response) {
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
                    LOG(ERROR) << "TopologyServiceManager has encounter"
                               << "a internalError."
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
}

}  // namespace topology
}  // namespace mds
}  // namespace curve
