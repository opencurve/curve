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

#include "curvefs/src/tools/curvefs_topology_tool.h"

DEFINE_string(mds_addr, "127.0.0.1:6700", "mds ip and port, separated by \",\"");  // NOLINT
DEFINE_string(cluster_map, "topo_example.json", "cluster topology map.");
DEFINE_uint32(rpcTimeOutMs, 5000u, "rpc time out");
DEFINE_string(confPath, "curvefs/conf/tools.conf", "config file path of tools");

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

    if (GetCommandLineFlagInfo("rpcTimeOutMs", &info) && info.is_default) {
        conf->GetUInt32Value("rpcTimeoutMs", &FLAGS_rpcTimeOutMs);
    }

    if (GetCommandLineFlagInfo("cluster_map", &info) && info.is_default) {
        conf->GetStringValue("topoFilePath", &FLAGS_cluster_map);
    }
}

int CurvefsTopologyTool::Init() {
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

int CurvefsTopologyTool::TryAnotherMdsAddress() {
    if (mdsAddressStr_.size() == 0) {
        LOG(ERROR) << "no avaliable mds address.";
        return kRetCodeCommonErr;
    }
    mdsAddressIndex_ = (mdsAddressIndex_ + 1) % mdsAddressStr_.size();
    std::string mdsAddress = mdsAddressStr_[mdsAddressIndex_];
    LOG(INFO) << "try mds address(" << mdsAddressIndex_
              << "): " << mdsAddress;
    int ret = channel_.Init(mdsAddress.c_str(), NULL);
    if (ret != 0) {
        LOG(ERROR) << "Fail to init channel to mdsAddress: "
                   << mdsAddress;
    }
    return ret;
}

int CurvefsTopologyTool::DealFailedRet(int ret, std::string operation) {
    if (kRetCodeRedirectMds == ret) {
        LOG(WARNING) << operation << " fail on mds: "
                   << mdsAddressStr_[mdsAddressIndex_];
    } else {
        LOG(ERROR) << operation << " fail.";
    }
    return ret;
}

int CurvefsTopologyTool::InitTopoData() {
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

int CurvefsTopologyTool::HandleBuildCluster() {
    int ret = ScanCluster();
    if (ret != 0) {
        return DealFailedRet(ret, "scan cluster");
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
    if (ret !=0) {
        return DealFailedRet(ret, "create server");
    }

    return ret;
}

int CurvefsTopologyTool::ReadClusterMap() {
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
        LOG(ERROR) << "open cluster map file : "
                   << FLAGS_cluster_map << " fail.";
        return -1;
    }
    return 0;
}

int CurvefsTopologyTool::InitPoolData() {
    if (clusterMap_[kPools].isNull()) {
        LOG(ERROR) << "No pools in cluster map";
        return -1;
    }
    for (const auto pool : clusterMap_[kPools]) {
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

int CurvefsTopologyTool::InitServerZoneData() {
    if (clusterMap_[kServers].isNull()) {
        LOG(ERROR) << "No servers in cluster map";
        return -1;
    }
    for (const auto server : clusterMap_[kServers]) {
        Server serverData;
        Zone zoneData;
        if (!server[kName].isString()) {
            LOG(ERROR) << "server name must be string";
            return -1;
        }
        serverData.name = server[kName].asString();
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
        zoneDatas.emplace_back(zoneData);
    }
    return 0;
}

int CurvefsTopologyTool::ScanCluster() {
    // get pools and compare
    // De-duplication
    std::list<PoolInfo> poolInfos;
    int ret = ListPool(&poolInfos);
    if (ret != 0) {
        return ret;
    }

    for (auto it = poolInfos.begin(); it != poolInfos.end(); it++) {
            auto ix = std::find_if(poolDatas.begin(), poolDatas.end(),
                [it] (Pool& data) {
                    return data.name == it->poolname();
                });
            if (ix != poolDatas.end()) {
                poolDatas.erase(ix);
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
        auto ix = std::find_if(zoneDatas.begin(), zoneDatas.end(),
            [it] (Zone &data) {
                return (data.poolName == it->poolname()) &&
                       (data.name == it->zonename());
            });
        if (ix != zoneDatas.end()) {
            zoneDatas.erase(ix);
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
            [it] (Server &data) {
                    return (data.name == it->hostname()) &&
                    (data.zoneName == it->zonename()) &&
                    (data.poolName == it->poolname());
            });
        if (ix != serverDatas.end()) {
            serverDatas.erase(ix);
        }
    }

    return 0;
}

int CurvefsTopologyTool::ListPool(std::list<PoolInfo> *poolInfos) {
    TopologyService_Stub stub(&channel_);
    ListPoolRequest request;
    ListPoolResponse response;
    brpc::Controller cntl;
    cntl.set_timeout_ms(FLAGS_rpcTimeOutMs);
    cntl.set_log_id(1);

    LOG(INFO) << "ListPool send request: " << request.DebugString();
    stub.ListPool(&cntl, &request, &response, nullptr);
    if (cntl.Failed()) {
        return kRetCodeRedirectMds;
    }

    if (response.statuscode() != TopoStatusCode::TOPO_OK) {
        LOG(ERROR) << "ListPool Rpc response fail. "
                   << "Message is :"
                   << response.DebugString();
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

int CurvefsTopologyTool::GetZonesInPool(PoolIdType poolid,
    std::list<ZoneInfo> *zoneInfos) {
    TopologyService_Stub stub(&channel_);
    ListPoolZoneRequest request;
    ListPoolZoneResponse response;
    request.set_poolid(poolid);

    brpc::Controller cntl;
    cntl.set_timeout_ms(FLAGS_rpcTimeOutMs);
    cntl.set_log_id(1);

    LOG(INFO) << "ListZoneInPool, send request: "
              << request.DebugString();

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

int CurvefsTopologyTool::GetServersInZone(ZoneIdType zoneid,
    std::list<ServerInfo> *serverInfos) {
    TopologyService_Stub stub(&channel_);
    ListZoneServerRequest request;
    ListZoneServerResponse response;
    request.set_zoneid(zoneid);
    brpc::Controller cntl;
    cntl.set_timeout_ms(FLAGS_rpcTimeOutMs);
    cntl.set_log_id(1);

    LOG(INFO) << "ListZoneServer, send request: "
              << request.DebugString();

    stub.ListZoneServer(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        return kRetCodeRedirectMds;
    }
    if (response.statuscode() != TopoStatusCode::TOPO_OK) {
        LOG(ERROR) << "ListZoneServer rpc response fail. "
                   << "Message is :"
                   << response.DebugString()
                   << " , zoneid = "
                   << zoneid;
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

int CurvefsTopologyTool::CreatePool() {
    TopologyService_Stub stub(&channel_);
    for (auto it : poolDatas) {
        CreatePoolRequest request;
        CreatePoolResponse response;
        request.set_poolname(it.name);
        std::string replicaNumStr = std::to_string(it.replicasNum);
        std::string copysetNumStr = std::to_string(it.copysetNum);
        std::string zoneNumStr = std::to_string(it.zoneNum);
        std::string rapString = "{\"replicaNum\":" + replicaNumStr
                             + ", \"copysetNum\":" + copysetNumStr
                             + ", \"zoneNum\":" + zoneNumStr
                             + "}";
        request.set_redundanceandplacementpolicy(rapString);

        brpc::Controller cntl;
        cntl.set_timeout_ms(FLAGS_rpcTimeOutMs);
        cntl.set_log_id(1);

        LOG(INFO) << "CreatePool, send request: "
                  << request.DebugString();

        stub.CreatePool(&cntl, &request, &response, nullptr);

        if (cntl.ErrorCode() == EHOSTDOWN ||
            cntl.ErrorCode() == brpc::ELOGOFF) {
            return kRetCodeRedirectMds;
        } else if (cntl.Failed()) {
            LOG(ERROR) << "CreatePool errcorde = "
                       << response.statuscode()
                       << ", error content:"
                       << cntl.ErrorText()
                       << " , poolName = "
                       << it.name;
            return kRetCodeCommonErr;
        }

        if (response.statuscode() != TopoStatusCode::TOPO_OK) {
            LOG(ERROR) << "CreatePool rpc response fail. "
                       << "Message is :"
                       << response.DebugString()
                       << " , poolName ="
                       << it.name;
            return response.statuscode();
        } else {
            LOG(INFO) << "Received CreatePool response success, "
                      << response.DebugString();
        }
    }
    return 0;
}

int CurvefsTopologyTool::CreateZone() {
    TopologyService_Stub stub(&channel_);
    for (auto it : zoneDatas) {
        CreateZoneRequest request;
        CreateZoneResponse response;
        request.set_zonename(it.name);
        request.set_poolname(it.poolName);

        brpc::Controller cntl;
        cntl.set_timeout_ms(FLAGS_rpcTimeOutMs);
        cntl.set_log_id(1);

        LOG(INFO) << "CreateZone, send request: "
                  << request.DebugString();

        stub.CreateZone(&cntl, &request, &response, nullptr);

        if (cntl.ErrorCode() == EHOSTDOWN ||
            cntl.ErrorCode() == brpc::ELOGOFF) {
            return kRetCodeRedirectMds;
        } else if (cntl.Failed()) {
            LOG(ERROR) << "CreateZone, errcorde = "
                       << response.statuscode()
                       << ", error content:"
                       << cntl.ErrorText()
                       << " , zoneName = "
                       << it.name;
            return kRetCodeCommonErr;
        }
        if (response.statuscode() != 0) {
            LOG(ERROR) << "CreateZone Rpc response fail. "
                       << "Message is :"
                       << response.DebugString()
                       << " , zoneName = "
                       << it.name;
            return response.statuscode();
        } else {
            LOG(INFO) << "Received CreateZone Rpc success, "
                      << response.DebugString();
        }
    }
    return 0;
}

int CurvefsTopologyTool::CreateServer() {
    TopologyService_Stub stub(&channel_);
    for (auto it : serverDatas) {
        ServerRegistRequest request;
        ServerRegistResponse response;
        request.set_hostname(it.name);
        request.set_internalip(it.internalIp);
        request.set_internalport(it.internalPort);
        request.set_externalip(it.externalIp);
        request.set_externalport(it.externalPort);
        request.set_zonename(it.zoneName);
        request.set_poolname(it.poolName);

        brpc::Controller cntl;
        cntl.set_timeout_ms(FLAGS_rpcTimeOutMs);
        cntl.set_log_id(1);

        LOG(INFO) << "CreateServer, send request: "
                  << request.DebugString();

        stub.RegistServer(&cntl, &request, &response, nullptr);

        if (cntl.ErrorCode() == EHOSTDOWN ||
            cntl.ErrorCode() == brpc::ELOGOFF) {
            return kRetCodeRedirectMds;
        } else if (cntl.Failed()) {
            LOG(ERROR) << "RegistServer, errcorde = "
                       << response.statuscode()
                       << ", error content : "
                       << cntl.ErrorText()
                       << " , serverName = "
                       << it.name;
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
                       << "Message is :"
                       << response.DebugString()
                       << " , serverName = "
                       << it.name;
            return response.statuscode();
        }
    }
    return 0;
}

}  // namespace topology
}  // namespace mds
}  // namespace curvefs
