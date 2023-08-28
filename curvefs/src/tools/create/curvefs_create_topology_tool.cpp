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
 * Created Date: 2021-09-03
 * Author: wanghai01
 */

// TODO(chengyi): add out put when build sucess

#include "curvefs/src/tools/create/curvefs_create_topology_tool.h"

DECLARE_string(mds_addr);
DECLARE_string(cluster_map);
DECLARE_string(confPath);
DECLARE_string(op);

using ::curve::common::SplitString;

namespace curvefs {
namespace mds {
namespace topology {

void UpdateFlagsFromConf(curve::common::Configuration* conf) {
    LOG_IF(FATAL, !conf->LoadConfig());
    google::CommandLineFlagInfo info;
    if (GetCommandLineFlagInfo("mds_addr", &info) && info.is_default) {
        conf->GetStringValue("mdsAddr", &FLAGS_mds_addr);
        LOG(INFO) << "conf: " << FLAGS_mds_addr;
    }

    if (GetCommandLineFlagInfo("rpcTimeoutMs", &info) && info.is_default) {
        conf->GetUInt32Value("rpcTimeoutMs", &FLAGS_rpcTimeoutMs);
    }

    if (GetCommandLineFlagInfo("cluster_map", &info) && info.is_default) {
        conf->GetStringValue("topoFilePath", &FLAGS_cluster_map);
    }
}

int CurvefsBuildTopologyTool::Init() {
    std::string confPath = FLAGS_confPath.c_str();
    curve::common::Configuration conf;
    conf.SetConfigPath(confPath);
    UpdateFlagsFromConf(&conf);
    SplitString(FLAGS_mds_addr, ",", &mdsAddressStr_);
    if (mdsAddressStr_.size() <= 0) {
        LOG(ERROR) << "no avaliable mds address.";
        return kRetCodeCommonErr;
    }

    for (auto addr : mdsAddressStr_) {
        butil::EndPoint endpt;
        if (butil::str2endpoint(addr.c_str(), &endpt) < 0) {
            LOG(ERROR) << "Invalid sub mds ip:port provided: " << addr;
            return kRetCodeCommonErr;
        }
    }
    mdsAddressIndex_ = -1;
    return 0;
}

int CurvefsBuildTopologyTool::TryAnotherMdsAddress() {
    if (mdsAddressStr_.size() == 0) {
        LOG(ERROR) << "no avaliable mds address.";
        return kRetCodeCommonErr;
    }
    mdsAddressIndex_ = (mdsAddressIndex_ + 1) % mdsAddressStr_.size();
    std::string mdsAddress = mdsAddressStr_[mdsAddressIndex_];
    LOG(INFO) << "try mds address(" << mdsAddressIndex_ << "): " << mdsAddress;
    int ret = channel_.Init(mdsAddress.c_str(), NULL);
    if (ret != 0) {
        LOG(ERROR) << "Fail to init channel to mdsAddress: " << mdsAddress;
    }
    return ret;
}

int CurvefsBuildTopologyTool::DealFailedRet(int ret, std::string operation) {
    if (kRetCodeRedirectMds == ret) {
        LOG(WARNING) << operation
                     << " fail on mds: " << mdsAddressStr_[mdsAddressIndex_];
    } else {
        LOG(ERROR) << operation << " fail.";
    }
    return ret;
}

int CurvefsBuildTopologyTool::InitTopoData() {
    int ret = ReadClusterMap();
    if (ret != 0) {
        return DealFailedRet(ret, "read cluster map");
    }

    ret = InitPoolData();
    if (ret != 0) {
        return DealFailedRet(ret, "init pool data");
    }

    ret = InitServerZoneData();
    if (ret != 0) {
        return DealFailedRet(ret, "init server data");
    }

    return ret;
}

int CurvefsBuildTopologyTool::HandleBuildCluster() {
    int ret = ScanCluster();
    if (ret != 0) {
        return DealFailedRet(ret, "scan cluster");
    }

    ret = RemoveServersNotInNewTopo();
    if (ret != 0) {
        return DealFailedRet(ret, "remove server");
    }

    ret = RemoveZonesNotInNewTopo();
    if (ret != 0) {
        return DealFailedRet(ret, "remove zone");
    }

    ret = RemovePoolsNotInNewTopo();
    if (ret != 0) {
        return DealFailedRet(ret, "remove pool");
    }

    ret = CreatePool();
    if (ret != 0) {
        return DealFailedRet(ret, "create pool");
    }

    ret = CreateZone();
    if (ret != 0) {
        return DealFailedRet(ret, "create zone");
    }

    ret = CreateServer();
    if (ret != 0) {
        return DealFailedRet(ret, "create server");
    }

    return ret;
}

int CurvefsBuildTopologyTool::ReadClusterMap() {
    std::ifstream fin;
    fin.open(FLAGS_cluster_map.c_str(), std::ios::in);
    if (fin.is_open()) {
        Json::CharReaderBuilder reader;
        JSONCPP_STRING errs;
        bool ok = Json::parseFromStream(reader, fin, &clusterMap_, &errs);
        fin.close();
        if (!ok) {
            LOG(ERROR) << "Parse cluster map file " << FLAGS_cluster_map
                       << " fail: " << errs;
            return -1;
        }
    } else {
        LOG(ERROR) << "open cluster map file : " << FLAGS_cluster_map
                   << " fail.";
        return -1;
    }
    return 0;
}

int CurvefsBuildTopologyTool::InitPoolData() {
    if (clusterMap_[kPools].isNull()) {
        LOG(ERROR) << "No pools in cluster map";
        return -1;
    }
    for (const auto& pool : clusterMap_[kPools]) {
        Pool poolData;
        if (!pool[kName].isString()) {
            LOG(ERROR) << "pool name must be string";
            return -1;
        }
        poolData.name = pool[kName].asString();
        if (!pool[kReplicasNum].isUInt()) {
            LOG(ERROR) << "pool replicasnum must be uint";
            return -1;
        }
        poolData.replicasNum = pool[kReplicasNum].asUInt();
        if (!pool[kCopysetNum].isUInt64()) {
            LOG(ERROR) << "pool copysetnum must be uint64";
            return -1;
        }
        poolData.copysetNum = pool[kCopysetNum].asUInt64();
        if (!pool[kZoneNum].isUInt64()) {
            LOG(ERROR) << "pool zonenum must be uint64";
            return -1;
        }
        poolData.zoneNum = pool[kZoneNum].asUInt();

        poolDatas.emplace_back(poolData);
    }
    return 0;
}

int CurvefsBuildTopologyTool::InitServerZoneData() {
    if (clusterMap_[kServers].isNull()) {
        LOG(ERROR) << "No servers in cluster map";
        return -1;
    }
    for (const auto& server : clusterMap_[kServers]) {
        Server serverData;
        Zone zoneData;
        if (!server[kName].isString()) {
            LOG(ERROR) << "server name should be string";
        } else {
            serverData.name = server[kName].asString();
        }
        if (!server[kInternalIp].isString()) {
            LOG(ERROR) << "server internal ip must be string";
            return -1;
        }
        serverData.internalIp = server[kInternalIp].asString();
        if (!server[kInternalPort].isUInt()) {
            LOG(ERROR) << "server internal port must be uint";
            return -1;
        }
        serverData.internalPort = server[kInternalPort].asUInt();
        if (!server[kExternalIp].isString()) {
            LOG(ERROR) << "server internal port must be string";
            return -1;
        }
        serverData.externalIp = server[kExternalIp].asString();
        if (!server[kExternalPort].isUInt()) {
            LOG(ERROR) << "server internal port must be string";
            return -1;
        }
        serverData.externalPort = server[kExternalPort].asUInt();
        if (!server[kZone].isString()) {
            LOG(ERROR) << "server zone must be string";
            return -1;
        }
        serverData.zoneName = server[kZone].asString();
        zoneData.name = server[kZone].asString();
        if (!server[kPool].isString()) {
            LOG(ERROR) << "server pool must be string";
            return -1;
        }
        serverData.poolName = server[kPool].asString();
        zoneData.poolName = server[kPool].asString();

        serverDatas.emplace_back(serverData);

        if (std::find_if(zoneDatas.begin(), zoneDatas.end(),
                         [serverData](Zone& data) {
                             return (data.poolName == serverData.poolName) &&
                                    (data.name == serverData.zoneName);
                         }) == zoneDatas.end()) {
            zoneDatas.emplace_back(zoneData);
        }
    }
    return 0;
}

int CurvefsBuildTopologyTool::ScanCluster() {
    // get pools and compare
    // De-duplication
    std::list<PoolInfo> poolInfos;
    int ret = ListPool(&poolInfos);
    if (ret != 0) {
        return ret;
    }

    for (auto it = poolInfos.begin(); it != poolInfos.end(); it++) {
        auto ix = std::find_if(
            poolDatas.begin(), poolDatas.end(),
            [it](const Pool& data) { return data.name == it->poolname(); });
        if (ix != poolDatas.end()) {
            poolDatas.erase(ix);
        } else {
            poolToDel.emplace_back(it->poolid());
        }
    }

    // get zone and compare
    // De-duplication
    std::list<ZoneInfo> zoneInfos;
    for (auto pool : poolInfos) {
        ret = GetZonesInPool(pool.poolid(), &zoneInfos);
        if (ret != 0) {
            return ret;
        }
    }

    for (auto it = zoneInfos.begin(); it != zoneInfos.end(); it++) {
        auto ix = std::find_if(
            zoneDatas.begin(), zoneDatas.end(), [it](const Zone& data) {
                return (data.poolName == it->poolname()) &&
                       (data.name == it->zonename());
            });
        if (ix != zoneDatas.end()) {
            zoneDatas.erase(ix);
        } else {
            zoneToDel.emplace_back(it->zoneid());
        }
    }

    // get server and compare
    // De-duplication
    std::list<ServerInfo> serverInfos;
    for (auto zone : zoneInfos) {
        ret = GetServersInZone(zone.zoneid(), &serverInfos);
        if (ret != 0) {
            return ret;
        }
    }

    for (auto it = serverInfos.begin(); it != serverInfos.end(); it++) {
        auto ix = std::find_if(serverDatas.begin(), serverDatas.end(),
                               [it](const Server& data) {
                                   return (data.name == it->hostname()) &&
                                          (data.zoneName == it->zonename()) &&
                                          (data.poolName == it->poolname());
                               });
        if (ix != serverDatas.end()) {
            serverDatas.erase(ix);
        } else {
            serverToDel.emplace_back(it->serverid());
        }
    }

    return 0;
}

int CurvefsBuildTopologyTool::ListPool(std::list<PoolInfo>* poolInfos) {
    TopologyService_Stub stub(&channel_);
    ListPoolRequest request;
    ListPoolResponse response;
    brpc::Controller cntl;
    cntl.set_timeout_ms(FLAGS_rpcTimeoutMs);

    LOG(INFO) << "ListPool send request: " << request.DebugString();
    stub.ListPool(&cntl, &request, &response, nullptr);
    if (cntl.Failed()) {
        return kRetCodeRedirectMds;
    }

    if (response.statuscode() != TopoStatusCode::TOPO_OK) {
        LOG(ERROR) << "ListPool Rpc response fail. "
                   << "Message is :" << response.DebugString();
        return response.statuscode();
    } else {
        LOG(INFO) << "Received ListPool rpc response success, "
                  << response.DebugString();
    }

    for (int i = 0; i < response.poolinfos_size(); i++) {
        poolInfos->emplace_back(response.poolinfos(i));
    }
    return 0;
}

int CurvefsBuildTopologyTool::GetZonesInPool(PoolIdType poolid,
                                             std::list<ZoneInfo>* zoneInfos) {
    TopologyService_Stub stub(&channel_);
    ListPoolZoneRequest request;
    ListPoolZoneResponse response;
    request.set_poolid(poolid);

    brpc::Controller cntl;
    cntl.set_timeout_ms(FLAGS_rpcTimeoutMs);

    LOG(INFO) << "ListZoneInPool, send request: " << request.DebugString();

    stub.ListPoolZone(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        return kRetCodeRedirectMds;
    }
    if (response.statuscode() != TopoStatusCode::TOPO_OK) {
        LOG(ERROR) << "ListPoolZone rpc response fail. "
                   << "Message is :" << response.DebugString()
                   << " , physicalpoolid = " << poolid;
        return response.statuscode();
    } else {
        LOG(INFO) << "Received ListPoolZone rpc response success, "
                  << response.DebugString();
    }

    for (int i = 0; i < response.zones_size(); i++) {
        zoneInfos->emplace_back(response.zones(i));
    }
    return 0;
}

int CurvefsBuildTopologyTool::GetServersInZone(
    ZoneIdType zoneid, std::list<ServerInfo>* serverInfos) {
    TopologyService_Stub stub(&channel_);
    ListZoneServerRequest request;
    ListZoneServerResponse response;
    request.set_zoneid(zoneid);
    brpc::Controller cntl;
    cntl.set_timeout_ms(FLAGS_rpcTimeoutMs);

    LOG(INFO) << "ListZoneServer, send request: " << request.DebugString();

    stub.ListZoneServer(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        return kRetCodeRedirectMds;
    }
    if (response.statuscode() != TopoStatusCode::TOPO_OK) {
        LOG(ERROR) << "ListZoneServer rpc response fail. "
                   << "Message is :" << response.DebugString()
                   << " , zoneid = " << zoneid;
        return response.statuscode();
    } else {
        LOG(INFO) << "ListZoneServer rpc response success, "
                  << response.DebugString();
    }

    for (int i = 0; i < response.serverinfo_size(); i++) {
        serverInfos->emplace_back(response.serverinfo(i));
    }
    return 0;
}

int CurvefsBuildTopologyTool::RemovePoolsNotInNewTopo() {
    TopologyService_Stub stub(&channel_);
    for (auto it : poolToDel) {
        DeletePoolRequest request;
        DeletePoolResponse response;
        request.set_poolid(it);

        brpc::Controller cntl;
        cntl.set_timeout_ms(FLAGS_rpcTimeoutMs);

        LOG(INFO) << "ClearPool, send request: " << request.DebugString();

        stub.DeletePool(&cntl, &request, &response, nullptr);

        if (cntl.ErrorCode() == EHOSTDOWN ||
            cntl.ErrorCode() == brpc::ELOGOFF) {
            return kRetCodeRedirectMds;
        } else if (cntl.Failed()) {
            LOG(ERROR) << "ClearPool errcorde = " << response.statuscode()
                       << ", error content:" << cntl.ErrorText()
                       << " , poolId = " << it;
            return kRetCodeCommonErr;
        }

        if (response.statuscode() != TopoStatusCode::TOPO_OK) {
            LOG(ERROR) << "ClearPool rpc response fail. "
                       << "Message is :" << response.DebugString()
                       << " , poolId =" << it;
            return response.statuscode();
        } else {
            LOG(INFO) << "Received ClearPool response success, "
                      << response.DebugString();
        }
    }
    return 0;
}

int CurvefsBuildTopologyTool::RemoveZonesNotInNewTopo() {
    TopologyService_Stub stub(&channel_);
    for (auto it : zoneToDel) {
        DeleteZoneRequest request;
        DeleteZoneResponse response;
        request.set_zoneid(it);

        brpc::Controller cntl;
        cntl.set_timeout_ms(FLAGS_rpcTimeoutMs);

        LOG(INFO) << "ClearZone, send request: " << request.DebugString();

        stub.DeleteZone(&cntl, &request, &response, nullptr);

        if (cntl.ErrorCode() == EHOSTDOWN ||
            cntl.ErrorCode() == brpc::ELOGOFF) {
            return kRetCodeRedirectMds;
        } else if (cntl.Failed()) {
            LOG(ERROR) << "ClearZone, errcorde = " << response.statuscode()
                       << ", error content:" << cntl.ErrorText()
                       << " , zoneId = " << it;
            return kRetCodeCommonErr;
        }
        if (response.statuscode() != TopoStatusCode::TOPO_OK) {
            LOG(ERROR) << "ClearZone Rpc response fail. "
                       << "Message is :" << response.DebugString()
                       << " , zoneId = " << it;
            return response.statuscode();
        } else {
            LOG(INFO) << "Received ClearZone Rpc success, "
                      << response.DebugString();
        }
    }
    return 0;
}

int CurvefsBuildTopologyTool::RemoveServersNotInNewTopo() {
    TopologyService_Stub stub(&channel_);
    for (auto it : serverToDel) {
        DeleteServerRequest request;
        DeleteServerResponse response;
        request.set_serverid(it);

        brpc::Controller cntl;
        cntl.set_timeout_ms(FLAGS_rpcTimeoutMs);

        LOG(INFO) << "ClearServer, send request: " << request.DebugString();

        stub.DeleteServer(&cntl, &request, &response, nullptr);

        if (cntl.ErrorCode() == EHOSTDOWN ||
            cntl.ErrorCode() == brpc::ELOGOFF) {
            return kRetCodeRedirectMds;
        } else if (cntl.Failed()) {
            LOG(ERROR) << "ClearServer, errcorde = " << response.statuscode()
                       << ", error content : " << cntl.ErrorText()
                       << " , serverId = " << it;
            return kRetCodeCommonErr;
        }
        if (response.statuscode() != TopoStatusCode::TOPO_OK) {
            LOG(ERROR) << "ClearServer Rpc response fail. "
                       << "Message is :" << response.DebugString()
                       << " , serverId = " << it;
            return response.statuscode();
        } else {
            LOG(INFO) << "Received ClearServer Rpc success, "
                      << response.DebugString();
        }
    }
    return 0;
}

int CurvefsBuildTopologyTool::CreatePool() {
    TopologyService_Stub stub(&channel_);
    for (auto it : poolDatas) {
        CreatePoolRequest request;
        CreatePoolResponse response;
        request.set_poolname(it.name);
        std::string replicaNumStr = std::to_string(it.replicasNum);
        std::string copysetNumStr = std::to_string(it.copysetNum);
        std::string zoneNumStr = std::to_string(it.zoneNum);
        std::string rapString = "{\"replicaNum\":" + replicaNumStr +
                                ", \"copysetNum\":" + copysetNumStr +
                                ", \"zoneNum\":" + zoneNumStr + "}";
        request.set_redundanceandplacementpolicy(rapString);

        brpc::Controller cntl;
        cntl.set_timeout_ms(FLAGS_rpcTimeoutMs);

        LOG(INFO) << "CreatePool, send request: " << request.DebugString();

        stub.CreatePool(&cntl, &request, &response, nullptr);

        if (cntl.ErrorCode() == EHOSTDOWN ||
            cntl.ErrorCode() == brpc::ELOGOFF) {
            return kRetCodeRedirectMds;
        } else if (cntl.Failed()) {
            LOG(ERROR) << "CreatePool errcorde = " << response.statuscode()
                       << ", error content:" << cntl.ErrorText()
                       << " , poolName = " << it.name;
            return kRetCodeCommonErr;
        }

        if (response.statuscode() != TopoStatusCode::TOPO_OK) {
            LOG(ERROR) << "CreatePool rpc response fail. "
                       << "Message is :" << response.DebugString()
                       << " , poolName =" << it.name;
            return response.statuscode();
        } else {
            LOG(INFO) << "Received CreatePool response success, "
                      << response.DebugString();
        }
    }
    return 0;
}

int CurvefsBuildTopologyTool::CreateZone() {
    TopologyService_Stub stub(&channel_);
    for (auto it : zoneDatas) {
        CreateZoneRequest request;
        CreateZoneResponse response;
        request.set_zonename(it.name);
        request.set_poolname(it.poolName);

        brpc::Controller cntl;
        cntl.set_timeout_ms(FLAGS_rpcTimeoutMs);

        LOG(INFO) << "CreateZone, send request: " << request.DebugString();

        stub.CreateZone(&cntl, &request, &response, nullptr);

        if (cntl.ErrorCode() == EHOSTDOWN ||
            cntl.ErrorCode() == brpc::ELOGOFF) {
            return kRetCodeRedirectMds;
        } else if (cntl.Failed()) {
            LOG(ERROR) << "CreateZone, errcorde = " << response.statuscode()
                       << ", error content:" << cntl.ErrorText()
                       << " , zoneName = " << it.name;
            return kRetCodeCommonErr;
        }
        if (response.statuscode() != TopoStatusCode::TOPO_OK) {
            LOG(ERROR) << "CreateZone Rpc response fail. "
                       << "Message is :" << response.DebugString()
                       << " , zoneName = " << it.name;
            return response.statuscode();
        } else {
            LOG(INFO) << "Received CreateZone Rpc success, "
                      << response.DebugString();
        }
    }
    return 0;
}

int CurvefsBuildTopologyTool::CreateServer() {
    TopologyService_Stub stub(&channel_);
    for (auto it : serverDatas) {
        ServerRegistRequest request;
        ServerRegistResponse response;
        if (!it.name.empty()) {
            request.set_hostname(it.name);
        }
        request.set_internalip(it.internalIp);
        request.set_internalport(it.internalPort);
        request.set_externalip(it.externalIp);
        request.set_externalport(it.externalPort);
        request.set_zonename(it.zoneName);
        request.set_poolname(it.poolName);

        brpc::Controller cntl;
        cntl.set_timeout_ms(FLAGS_rpcTimeoutMs);

        LOG(INFO) << "CreateServer, send request: " << request.DebugString();

        stub.RegistServer(&cntl, &request, &response, nullptr);

        if (cntl.ErrorCode() == EHOSTDOWN ||
            cntl.ErrorCode() == brpc::ELOGOFF) {
            return kRetCodeRedirectMds;
        } else if (cntl.Failed()) {
            LOG(ERROR) << "RegistServer, errcorde = " << response.statuscode()
                       << ", error content : " << cntl.ErrorText()
                       << " , serverName = " << it.name;
            return kRetCodeCommonErr;
        }
        if (response.statuscode() == TopoStatusCode::TOPO_OK) {
            LOG(INFO) << "Received RegistServer Rpc response success, "
                      << response.DebugString();
        } else if (response.statuscode() ==
                   TopoStatusCode::TOPO_IP_PORT_DUPLICATED) {
            LOG(INFO) << "Server already exist";
        } else {
            LOG(ERROR) << "RegistServer Rpc response fail. "
                       << "Message is :" << response.DebugString()
                       << " , serverName = " << it.name;
            return response.statuscode();
        }
    }
    return 0;
}

int CurvefsBuildTopologyTool::RunCommand() {
    int ret = 0;
    int maxTry = GetMaxTry();
    int retry = 0;
    for (; retry < maxTry; retry++) {
        ret = TryAnotherMdsAddress();
        if (ret < 0) {
            return kRetCodeCommonErr;
        }

        ret = HandleBuildCluster();
        if (ret != kRetCodeRedirectMds) {
            break;
        }
    }
    if (retry >= maxTry) {
        LOG(ERROR) << "rpc retry times exceed.";
        return kRetCodeCommonErr;
    }
    if (ret != 0) {
        LOG(ERROR) << "exec fail, ret = " << ret;
    } else {
        LOG(INFO) << "exec success, ret = " << ret;
    }
    return ret;
}

}  // namespace topology
}  // namespace mds
}  // namespace curvefs
