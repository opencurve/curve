/*
 * Project: curve
 * Created Date: Fri Oct 19 2018
 * Author: xuchaojie
 * Copyright (c) 2018 netease
 */

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <brpc/server.h>
#include <brpc/channel.h>

#include <fstream>
#include <map>
#include <set>
#include <list>

#include "proto/topology.pb.h"
#include "src/mds/common/mds_define.h"

DEFINE_string(mds_ip, "127.0.0.1", "mds ip");
DEFINE_int32(mds_port, 8000, "mds port");

DEFINE_string(op,
    "",
    "operation: create_logicalpool, create_physicalpool");

DEFINE_string(name, "defaultLogicalPool", "logical pool name.");
DEFINE_string(physicalpool_name, "pool1", "physicalPool name.");
DEFINE_int32(logicalpool_type,
    ::curve::mds::topology::PAGEFILE,
    "logical pool type.");

DEFINE_int32(replica_num, 3, "replica num.");
DEFINE_int32(copyset_num, 0, "copyset num.");
DEFINE_int32(zone_num, 3, "zone num.");
DEFINE_int32(scatterWidth, 0, "scatter width.");

DEFINE_string(cluster_map, "./topo.txt", "cluster topology map.");

namespace curve {
namespace mds {
namespace topology {

struct ServerData {
    std::string serverName;
    std::string internalIp;
    uint32_t internalPort;
    std::string externalIp;
    uint32_t externalPort;
    std::string zoneName;
    std::string physicalPoolName;
};

struct ZoneData {
    std::string zoneName;
    std::string physicalPoolName;
    PoolIdType physicalPoolId;
};

struct PhysicalPoolData {
    std::string physicalPoolName;
};

class CurvefsTools {
 public:
    CurvefsTools() {}
    ~CurvefsTools() {}

    int Init();

    int HandleCreateLogicalPool();
    int HandleBuildCluster();

    static const std::string clusterMapSeprator;

 private:
    void SplitString(const std::string& s,
        std::vector<std::string> *v,
        const std::string& c);

    int ReadClusterMap();
    int ScanCluster();
    int CreatePhysicalPool();
    int CreateZone();
    int CreateServer();

    int ClearPhysicalPool();
    int ClearZone();
    int ClearServer();

    int ListPhysicalPool(
        std::list<PhysicalPoolInfo> *physicalPoolInfos);

    int AddListPoolZone(PoolIdType poolid,
        std::list<ZoneInfo> *zoneInfos);

    int AddListZoneServer(ZoneIdType zoneid,
        std::list<ServerInfo> *serverInfos);

 private:
    std::list<ServerData> cluster;
    std::list<PhysicalPoolData> physicalPoolToAdd;
    std::list<ZoneData> zoneToAdd;
    std::list<ServerData> serverToAdd;

    std::list<PoolIdType> physicalPoolToDel;
    std::list<ZoneIdType> zoneToDel;
    std::list<ServerIdType> serverToDel;

    brpc::Channel channel_;
};

const std::string CurvefsTools::clusterMapSeprator = " ";  // NOLINT

int CurvefsTools::Init() {
    int ret = channel_.Init(FLAGS_mds_ip.c_str(), FLAGS_mds_port, NULL);
    if (ret != 0) {
        LOG(FATAL) << "Fail to init channel to ip: "
                   << FLAGS_mds_ip
                   << " port "
                   << FLAGS_mds_port
                   << std::endl;
    }
    return ret;
}

int CurvefsTools::HandleCreateLogicalPool() {
    TopologyService_Stub stub(&channel_);

    brpc::Controller cntl;
    cntl.set_timeout_ms(10000);
    cntl.set_log_id(1);

    CreateLogicalPoolRequest request;
    request.set_logicalpoolname(FLAGS_name);
    request.set_physicalpoolname(FLAGS_physicalpool_name);
    request.set_type(static_cast<LogicalPoolType>(FLAGS_logicalpool_type));

    std::string replicaNumStr = std::to_string(FLAGS_replica_num);
    std::string copysetNumStr = std::to_string(FLAGS_copyset_num);
    std::string zoneNumStr = std::to_string(FLAGS_zone_num);

    std::string rapString = "{\"replicaNum\":" + replicaNumStr
                         + ", \"copysetNum\":" + copysetNumStr
                         + ", \"zoneNum\":" + zoneNumStr
                         + "}";

    request.set_redundanceandplacementpolicy(rapString);
    request.set_userpolicy("{\"aaa\":1}");
    request.set_scatterwidth(FLAGS_scatterWidth);

    CreateLogicalPoolResponse response;
    stub.CreateLogicalPool(&cntl, &request, &response, nullptr);

     if (cntl.Failed()) {
        LOG(ERROR) << "CreateLogicalPool, errcorde = "
                    << response.statuscode()
                    << ", error content:"
                    << cntl.ErrorText();
        return -1;
    }
    if (response.statuscode() != 0) {
        LOG(ERROR) << "Rpc response fail. "
                   << "Message is :"
                   << response.DebugString();
        return response.statuscode();
    }

    return 0;
}

int CurvefsTools::HandleBuildCluster() {
    int ret = ReadClusterMap();
    if (ret < 0) {
        LOG(ERROR) << "read cluster map fail";
        return ret;
    }
    ret = ScanCluster();
    if (ret < 0) {
        LOG(ERROR) << "scan cluster fail";
        return ret;
    }
    ret = ClearServer();
    if (ret < 0) {
        LOG(ERROR) << "clear server fail.";
        return ret;
    }
    ret = ClearZone();
    if (ret < 0) {
        LOG(ERROR) << "clear zone fail.";
        return ret;
    }
    ret = ClearPhysicalPool();
    if (ret < 0) {
        LOG(ERROR) << "clear physicalpool fail.";
        return ret;
    }
    ret = CreatePhysicalPool();
    if (ret < 0) {
        LOG(ERROR) << "create physicalpool fail.";
        return ret;
    }
    ret = CreateZone();
    if (ret < 0) {
        LOG(ERROR) << "create zone fail.";
        return ret;
    }
    ret = CreateServer();
    if (ret < 0) {
        LOG(ERROR) << "create server fail.";
        return ret;
    }
    return ret;
}

int CurvefsTools::ReadClusterMap() {
    std::string clusterMap = FLAGS_cluster_map;

    std::ifstream fin;
    fin.open(clusterMap, std::ios::in);

    if (fin.is_open()) {
        int lineNo = 0;
        for (std::string line; std::getline(fin, line); ) {
            lineNo++;
            int colNo = 0;
            if (line.empty()) {
                continue;
            }
            std::vector<std::string> strList;
            SplitString(line, &strList, CurvefsTools::clusterMapSeprator);

            std::vector<std::string>::size_type index = 0;
            while (index < strList.size() && strList[index].empty()) {
                index++;
                colNo++;
            }

            if (index >= strList.size()) {
                continue;   // blank line
            }

            ServerData info;
            // serverName
            if (index < strList.size() && !strList[index].empty()) {
                info.serverName = strList[index];
                index++;
                colNo += strList[index].size();
            } else {
                LOG(ERROR) << "parse cluster map error in line, context: \""
                           << line
                           << "\", line No: "
                           << lineNo
                           << ", colume No: "
                           << colNo;
                return -1;
            }

            while (index < strList.size() && strList[index].empty()) {
                index++;
                colNo++;
            }
            // internalIp & port
            if (index < strList.size() && !strList[index].empty()) {
                std::string ipPort = strList[index];
                std::vector<std::string> ipPortList;
                SplitString(ipPort, &ipPortList, ":");
                if (1 == ipPortList.size()) {
                    info.internalIp = ipPortList[0];
                    info.internalPort = 0;
                } else if (2 == ipPortList.size()) {
                    info.internalIp = ipPortList[0];
                    info.internalPort = std::stoul(ipPortList[1]);
                } else {
                    LOG(ERROR) << "parse cluster map error in line, context: \""
                               << line
                               << "\", line No: "
                               << lineNo
                               << ", ipPort string error: "
                               << ipPort;
                }
                index++;
                colNo += strList[index].size();
            } else {
                LOG(ERROR) << "parse cluster map error in line, context: \""
                           << line
                           << "\", line No: "
                           << lineNo
                           << ", colume No: "
                           << colNo;
                return -1;
            }

            while (index < strList.size() && strList[index].empty()) {
                index++;
                colNo++;
            }
            // externalIp & port
            if (index < strList.size() && !strList[index].empty()) {
                std::string ipPort = strList[index];
                std::vector<std::string> ipPortList;
                SplitString(ipPort, &ipPortList, ":");
                if (1 == ipPortList.size()) {
                    info.externalIp = ipPortList[0];
                    info.externalPort = 0;
                } else if (2 == ipPortList.size()) {
                    info.externalIp = ipPortList[0];
                    info.externalPort = std::stoul(ipPortList[1]);
                } else {
                    LOG(ERROR) << "parse cluster map error in line, context: \""
                               << line
                               << "\", line No: "
                               << lineNo
                               << ", ipPort string error: "
                               << ipPort;
                }
                index++;
                colNo += strList[index].size();
            } else {
                LOG(ERROR) << "parse cluster map error in line, context: \""
                           << line
                           << "\", line No: "
                           << lineNo
                           << ", colume No: "
                           << colNo;
                return -1;
            }

            while (index < strList.size() && strList[index].empty()) {
                index++;
                colNo++;
            }
            // zoneName
            if (index < strList.size() && !strList[index].empty()) {
                info.zoneName = strList[index];
                index++;
                colNo += strList[index].size();
            } else {
                LOG(ERROR) << "parse cluster map error in line, context: \""
                           << line
                           << "\", line No: "
                           << lineNo
                           << ", colume No: "
                           << colNo;
                return -1;
            }

            while (index < strList.size() && strList[index].empty()) {
                index++;
                colNo++;
            }
            // physicalPoolName
            if (index < strList.size() && !strList[index].empty()) {
                info.physicalPoolName = strList[index];
                index++;
                colNo += strList[index].size();
            } else {
                LOG(ERROR) << "parse cluster map error in line, context: \""
                           << line
                           << "\", line No: "
                           << lineNo
                           << ", colume No: "
                           << colNo;
                return -1;
            }

            cluster.push_back(info);
        }
    } else {
        LOG(ERROR) << "open cluster map file : "
                   << clusterMap
                   << " fail.";
        return -1;
    }
    return 0;
}

int CurvefsTools::ListPhysicalPool(
    std::list<PhysicalPoolInfo> *physicalPoolInfos) {
    TopologyService_Stub stub(&channel_);
    brpc::Controller listPhysicalPoolCntl;
    listPhysicalPoolCntl.set_timeout_ms(1000);
    listPhysicalPoolCntl.set_log_id(1);
    ListPhysicalPoolRequest listPhysicalPoolRequest;
    ListPhysicalPoolResponse listPhysicalPoolResponse;
    stub.ListPhysicalPool(&listPhysicalPoolCntl,
        &listPhysicalPoolRequest,
        &listPhysicalPoolResponse,
        nullptr);

    if (listPhysicalPoolCntl.Failed()) {
        LOG(ERROR) << "ListPhysicalPool Rpc fail, errcorde = "
                    << listPhysicalPoolResponse.statuscode()
                    << ", error content:"
                    << listPhysicalPoolCntl.ErrorText();
        return -1;
    }
    if (listPhysicalPoolResponse.statuscode() != kTopoErrCodeSuccess) {
        LOG(ERROR) << "ListPhysicalPool Rpc response fail. "
                   << "Message is :"
                   << listPhysicalPoolResponse.DebugString();
        return listPhysicalPoolResponse.statuscode();
    }

    for (int i = 0;
            i < listPhysicalPoolResponse.physicalpoolinfos_size();
            i++) {
        physicalPoolInfos->push_back(
            listPhysicalPoolResponse.physicalpoolinfos(i));
    }
    return 0;
}

int CurvefsTools::AddListPoolZone(PoolIdType poolid,
    std::list<ZoneInfo> *zoneInfos) {
    TopologyService_Stub stub(&channel_);
    ListPoolZoneRequest request;
    ListPoolZoneResponse response;
    brpc::Controller cntl;
    cntl.set_timeout_ms(10000);
    cntl.set_log_id(1);
    request.set_physicalpoolid(poolid);
    stub.ListPoolZone(&cntl, &request, &response, nullptr);
    if (cntl.Failed()) {
        LOG(ERROR) << "ListPoolZone Rpc fail, errcorde = "
                   << response.statuscode()
                   << ", error content:"
                   << cntl.ErrorText()
                   << ", physicalpoolid = "
                   << poolid;
        return -1;
    }
    if (response.statuscode() != kTopoErrCodeSuccess) {
        LOG(ERROR) << "ListPoolZone Rpc response fail. "
                   << "Message is :"
                   << response.DebugString()
                   << " , physicalpoolid = "
                   << poolid;
        return response.statuscode();
    }
    for (int i = 0; i < response.zones_size(); i++) {
        zoneInfos->push_back(response.zones(i));
    }
    return 0;
}

int CurvefsTools::AddListZoneServer(ZoneIdType zoneid,
    std::list<ServerInfo> *serverInfos) {
    TopologyService_Stub stub(&channel_);
    ListZoneServerRequest request;
    ListZoneServerResponse response;
    brpc::Controller cntl;
    cntl.set_timeout_ms(10000);
    cntl.set_log_id(1);
    request.set_zoneid(zoneid);
    stub.ListZoneServer(&cntl, &request, &response, nullptr);
    if (cntl.Failed()) {
        LOG(ERROR) << "ListZoneServer Rpc fail, errcorde = "
                   << response.statuscode()
                   << ", error content:"
                   << cntl.ErrorText()
                   << ", zoneid = "
                   << zoneid;
        return -1;
    }
    if (response.statuscode() != kTopoErrCodeSuccess) {
        LOG(ERROR) << "ListZoneServer Rpc response fail. "
                   << "Message is :"
                   << response.DebugString()
                   << " , zoneid = "
                   << zoneid;
        return response.statuscode();
    }
    for (int i = 0; i < response.serverinfo_size(); i++) {
        serverInfos->push_back(response.serverinfo(i));
    }
    return 0;
}

int CurvefsTools::ScanCluster() {
    // get all phsicalpool and compare
    // 去重
    for (auto server : cluster) {
        if (std::find_if(physicalPoolToAdd.begin(),
            physicalPoolToAdd.end(),
            [server](curve::mds::topology::PhysicalPoolData& data) {
            return data.physicalPoolName ==
            server.physicalPoolName;
        }) != physicalPoolToAdd.end()) {
            continue;
        }
        PhysicalPoolData poolData;
        poolData.physicalPoolName = server.physicalPoolName;
        physicalPoolToAdd.push_back(poolData);
    }

    std::list<PhysicalPoolInfo> physicalPoolInfos;
    int ret = ListPhysicalPool(&physicalPoolInfos);
    if (ret < 0) {
        return ret;
    }

    for (auto it = physicalPoolInfos.begin();
            it != physicalPoolInfos.end();) {
            auto ix = std::find_if(physicalPoolToAdd.begin(),
                physicalPoolToAdd.end(),
                [it] (curve::mds::topology::PhysicalPoolData& data) {
                    return data.physicalPoolName == it->physicalpoolname();
                });
            if (ix != physicalPoolToAdd.end()) {
                physicalPoolToAdd.erase(ix);
                it++;
            } else {
                physicalPoolToDel.push_back(it->physicalpoolid());
                it = physicalPoolInfos.erase(it);
            }
    }

    // get zone and compare
    // 去重
    for (auto server : cluster) {
        if (std::find_if(zoneToAdd.begin(),
            zoneToAdd.end(),
            [server](curve::mds::topology::ZoneData& data) {
            return (data.physicalPoolName ==
                server.physicalPoolName) &&
                   (data.zoneName ==
                server.zoneName);
        }) != zoneToAdd.end()) {
            continue;
        }
        ZoneData zoneData;
        zoneData.physicalPoolName = server.physicalPoolName;
        zoneData.zoneName = server.zoneName;
        zoneToAdd.push_back(zoneData);
    }

    std::list<ZoneInfo> zoneInfos;
    for (auto poolid : physicalPoolToDel) {
        ret = AddListPoolZone(poolid, &zoneInfos);
        if (ret < 0) {
            return ret;
        }
    }

    for (auto zinfo : zoneInfos) {
        zoneToDel.push_back(zinfo.zoneid());
    }

    zoneInfos.clear();
    for (auto it = physicalPoolInfos.begin();
            it != physicalPoolInfos.end();
            it++) {
        PoolIdType poolid = it->physicalpoolid();
        ret = AddListPoolZone(poolid, &zoneInfos);
        if (ret < 0) {
            return ret;
        }
    }

    for (auto it = zoneInfos.begin();
            it != zoneInfos.end();) {
        auto ix = std::find_if(zoneToAdd.begin(),
            zoneToAdd.end(),
            [it] (ZoneData &data) {
                return (data.physicalPoolName ==
                    it->physicalpoolname()) &&
                       (data.zoneName ==
                    it->zonename());
            });
        if (ix != zoneToAdd.end()) {
            zoneToAdd.erase(ix);
            it++;
        } else {
            zoneToDel.push_back(it->zoneid());
            it = zoneInfos.erase(it);
        }
    }

    // get server and compare
    // 去重
    for (auto server : cluster) {
        if (std::find_if(serverToAdd.begin(),
            serverToAdd.end(),
            [server](curve::mds::topology::ServerData& data) {
            return data.serverName ==
                 server.serverName;
            }) != serverToAdd.end()) {
            LOG(WARNING) << "WARING! Duplicated Server Name: "
                        << server.serverName
                        << " , ignored.";
            continue;
        }
        serverToAdd.push_back(server);
    }

    std::list<ServerInfo> serverInfos;
    for (auto zoneid : zoneToDel) {
        ret = AddListZoneServer(zoneid, &serverInfos);
        if (ret < 0) {
            return ret;
        }
    }

    for (auto sinfo : serverInfos) {
        serverToDel.push_back(sinfo.serverid());
    }

    serverInfos.clear();
    for (auto it = zoneInfos.begin();
            it != zoneInfos.end();
            it++) {
        ZoneIdType zoneid = it->zoneid();
        ret = AddListZoneServer(zoneid, &serverInfos);
        if (ret < 0) {
            return ret;
        }
    }

    for (auto it = serverInfos.begin();
            it != serverInfos.end();
            it++) {
        auto ix = std::find_if(serverToAdd.begin(),
            serverToAdd.end(),
            [it] (ServerData &data) {
                    return (data.serverName == it->hostname()) &&
                    (data.zoneName == it->zonename()) &&
                    (data.physicalPoolName == it->physicalpoolname());
            });
        if (ix != serverToAdd.end()) {
            serverToAdd.erase(ix);
        } else {
            serverToDel.push_back(it->serverid());
        }
    }

    return 0;
}

int CurvefsTools::CreatePhysicalPool() {
    TopologyService_Stub stub(&channel_);
    for (auto it : physicalPoolToAdd) {
        brpc::Controller cntl;
        cntl.set_timeout_ms(1000);
        cntl.set_log_id(1);

        PhysicalPoolRequest request;
        request.set_physicalpoolname(it.physicalPoolName);
        request.set_desc("");

        PhysicalPoolResponse response;
        stub.CreatePhysicalPool(&cntl, &request, &response, nullptr);

        LOG(INFO) << "CreatePhysicalPool, send request: "
                  << request.DebugString();

        if (cntl.Failed()) {
            LOG(ERROR) << "CreatePhysicalPool, errcorde = "
                       << response.statuscode()
                       << ", error content:"
                       << cntl.ErrorText()
                       << " , physicalPoolName ="
                       << it.physicalPoolName;
            return -1;
        }
        if (response.statuscode() != kTopoErrCodeSuccess) {
            LOG(ERROR) << "CreatePhysicalPool Rpc response fail. "
                       << "Message is :"
                       << response.DebugString()
                       << " , physicalPoolName ="
                       << it.physicalPoolName;
            return response.statuscode();
        }
    }
    return 0;
}

int CurvefsTools::CreateZone() {
    TopologyService_Stub stub(&channel_);
    for (auto it : zoneToAdd) {
        brpc::Controller cntl;
        cntl.set_timeout_ms(1000);
        cntl.set_log_id(1);

        ZoneRequest request;
        request.set_zonename(it.zoneName);
        request.set_physicalpoolname(it.physicalPoolName);
        request.set_desc("");

        ZoneResponse response;
        stub.CreateZone(&cntl, &request, &response, nullptr);

        LOG(INFO) << "CreateZone, send request: "
                  << request.DebugString();

        if (cntl.Failed()) {
            LOG(ERROR) << "CreateZone, errcorde = "
                       << response.statuscode()
                       << ", error content:"
                       << cntl.ErrorText()
                       << " , zoneName = "
                       << it.zoneName;
            return -1;
        }
        if (response.statuscode() != 0) {
            LOG(ERROR) << "CreateZone Rpc response fail. "
                       << "Message is :"
                       << response.DebugString()
                       << " , zoneName = "
                       << it.zoneName;
            return response.statuscode();
        }
    }
    return 0;
}

int CurvefsTools::CreateServer() {
    TopologyService_Stub stub(&channel_);
    for (auto it : serverToAdd) {
        brpc::Controller cntl;
        cntl.set_timeout_ms(1000);
        cntl.set_log_id(1);

        ServerRegistRequest request;
        request.set_hostname(it.serverName);
        request.set_internalip(it.internalIp);
        request.set_internalport(it.internalPort);
        request.set_externalip(it.externalIp);
        request.set_externalport(it.externalPort);
        request.set_zonename(it.zoneName);
        request.set_physicalpoolname(it.physicalPoolName);
        request.set_desc("");

        ServerRegistResponse response;
        stub.RegistServer(&cntl, &request, &response, nullptr);

        LOG(INFO) << "CreateServer, send request: "
                  << request.DebugString();

        if (cntl.Failed()) {
            LOG(ERROR) << "RegistServer, errcorde = "
                       << response.statuscode()
                       << ", error content : "
                       << cntl.ErrorText()
                       << " , serverName = "
                       << it.serverName;
            return -1;
        }
        if (response.statuscode() != 0) {
            LOG(ERROR) << "RegistServer Rpc response fail. "
                       << "Message is :"
                       << response.DebugString()
                       << " , serverName = "
                       << it.serverName;
            return response.statuscode();
        }
    }
    return 0;
}

int CurvefsTools::ClearPhysicalPool() {
    TopologyService_Stub stub(&channel_);
    for (auto it : physicalPoolToDel) {
        brpc::Controller cntl;
        cntl.set_timeout_ms(1000);
        cntl.set_log_id(1);

        PhysicalPoolRequest request;
        request.set_physicalpoolid(it);

        PhysicalPoolResponse response;
        stub.DeletePhysicalPool(&cntl, &request, &response, nullptr);

        LOG(INFO) << "DeletePhysicalPool, send request: "
                  << request.DebugString();

        if (cntl.Failed()) {
            LOG(ERROR) << "DeletePhysicalPool, errcorde = "
                       << response.statuscode()
                       << ", error content:"
                       << cntl.ErrorText()
                       << " , physicalPoolId = "
                       << it;
            return -1;
        }
        if (response.statuscode() != kTopoErrCodeSuccess) {
            LOG(ERROR) << "DeletePhysicalPool Rpc response fail. "
                       << "Message is :"
                       << response.DebugString()
                       << " , physicalPoolId = "
                       << it;
            return response.statuscode();
        }
    }
    return 0;
}

int CurvefsTools::ClearZone() {
    TopologyService_Stub stub(&channel_);
    for (auto it : zoneToDel) {
        brpc::Controller cntl;
        cntl.set_timeout_ms(1000);
        cntl.set_log_id(1);

        ZoneRequest request;
        request.set_zoneid(it);

        ZoneResponse response;
        stub.DeleteZone(&cntl, &request, &response, nullptr);

        LOG(INFO) << "DeleteZone, send request: "
                  << request.DebugString();

        if (cntl.Failed()) {
            LOG(ERROR) << "DeleteZone, errcorde = "
                       << response.statuscode()
                       << ", error content:"
                       << cntl.ErrorText()
                       << " , zoneId = "
                       << it;
            return -1;
        }
        if (response.statuscode() != kTopoErrCodeSuccess) {
            LOG(ERROR) << "DeleteZone Rpc response fail. "
                       << "Message is :"
                       << response.DebugString()
                       << " , zoneId = "
                       << it;
            return response.statuscode();
        }
    }
    return 0;
}

int CurvefsTools::ClearServer() {
    TopologyService_Stub stub(&channel_);
    for (auto it : serverToDel) {
        brpc::Controller cntl;
        cntl.set_timeout_ms(1000);
        cntl.set_log_id(1);

        DeleteServerRequest request;
        request.set_serverid(it);

        DeleteServerResponse response;
        stub.DeleteServer(&cntl, &request, &response, nullptr);

        LOG(INFO) << "DeleteServer, send request: "
                  << request.DebugString();

        if (cntl.Failed()) {
            LOG(ERROR) << "DeleteServer, errcorde = "
                       << response.statuscode()
                       << ", error content:"
                       << cntl.ErrorText()
                       << " , serverId = "
                       << it;
            return -1;
        }
        if (response.statuscode() != kTopoErrCodeSuccess) {
            LOG(ERROR) << "DeleteServer Rpc response fail. "
                       << "Message is :"
                       << response.DebugString()
                       << " , serverId = "
                       << it;
            return response.statuscode();
        }
    }
    return 0;
}

void CurvefsTools::SplitString(const std::string& s,
    std::vector<std::string> *v,
    const std::string& c) {
  std::string::size_type pos1, pos2;
  pos2 = s.find(c);
  pos1 = 0;
  while (std::string::npos != pos2) {
    v->push_back(s.substr(pos1, pos2-pos1));

    pos1 = pos2 + c.size();
    pos2 = s.find(c, pos1);
  }
  if (pos1 != s.length())
    v->push_back(s.substr(pos1));
}

}  // namespace topology
}  // namespace mds
}  // namespace curve



int main(int argc, char **argv) {
    google::InitGoogleLogging(argv[0]);
    google::ParseCommandLineFlags(&argc, &argv, false);

    int ret = 0;
    curve::mds::topology::CurvefsTools tools;
    if (tools.Init() < 0) {
        LOG(ERROR) << "curvefsTool init error.";
        return -1;
    }

    std::string operation = FLAGS_op;
    if (operation == "create_logicalpool") {
        ret = tools.HandleCreateLogicalPool();
    } else if (operation == "create_physicalpool") {
        ret = tools.HandleBuildCluster();
    } else {
        LOG(ERROR) << "undefined op.";
        ret = -1;
    }

    return ret;
}


