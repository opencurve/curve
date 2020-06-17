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
 * Created Date: Fri Oct 19 2018
 * Author: xuchaojie
 */

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <brpc/server.h>
#include <brpc/channel.h>
#include <butil/endpoint.h>

#include <fstream>
#include <map>
#include <set>
#include <list>

#include "proto/topology.pb.h"
#include "src/mds/common/mds_define.h"
#include "src/common/string_util.h"

DEFINE_string(mds_addr, "127.0.0.1:6666",
    "mds ip and port list, separated by \",\"");
// 兼容原有接口
DEFINE_string(mds_ip, "127.0.0.1", "mds ip");
DEFINE_int32(mds_port, 6666, "mds port");

DEFINE_string(op,
    "",
    "operation: create_logicalpool, create_physicalpool, set_chunkserver");

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

DEFINE_int32(chunkserver_id, -1, "chunkserver id for set chunkserver status.");
DEFINE_string(chunkserver_status, "readwrite",
    "chunkserver status: readwrite, pendding, retired.");

DEFINE_uint32(rpcTimeOutMs, 5000u, "rpc time out");

const int kRetCodeCommonErr = -1;
const int kRetCodeRedirectMds = -2;

using ::curve::common::SplitString;

namespace curve {
namespace mds {
namespace topology {

struct CurveServerData {
    std::string serverName;
    std::string internalIp;
    uint32_t internalPort;
    std::string externalIp;
    uint32_t externalPort;
    std::string zoneName;
    std::string physicalPoolName;
};

struct CurveZoneData {
    std::string zoneName;
    std::string physicalPoolName;
    PoolIdType physicalPoolId;
};

struct CurvePhysicalPoolData {
    std::string physicalPoolName;
};

class CurvefsTools {
 public:
    CurvefsTools() {}
    ~CurvefsTools() {}

    int Init();

    int HandleCreateLogicalPool();
    int HandleBuildCluster();
    int SetChunkServer();

    int GetMaxTry() {
        return mdsAddressStr_.size();
    }

    int TryAnotherMdsAddress();

    static const std::string clusterMapSeprator;

 private:
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
    std::list<CurveServerData> cluster;
    std::list<CurvePhysicalPoolData> physicalPoolToAdd;
    std::list<CurveZoneData> zoneToAdd;
    std::list<CurveServerData> serverToAdd;

    std::list<PoolIdType> physicalPoolToDel;
    std::list<ZoneIdType> zoneToDel;
    std::list<ServerIdType> serverToDel;

    std::vector<std::string> mdsAddressStr_;
    int mdsAddressIndex_;
    brpc::Channel channel_;
};

const std::string CurvefsTools::clusterMapSeprator = " ";  // NOLINT

int CurvefsTools::Init() {
    // 兼容原有接口
    google::CommandLineFlagInfo info1, info2;
    if ((GetCommandLineFlagInfo("mds_ip", &info1) && !info1.is_default) ||
        (GetCommandLineFlagInfo("mds_port", &info2) && !info2.is_default)) {
        mdsAddressStr_.push_back(
            FLAGS_mds_ip + ':' + std::to_string(FLAGS_mds_port));
         mdsAddressIndex_ = -1;
         return 0;
    }

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

int CurvefsTools::TryAnotherMdsAddress() {
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

int CurvefsTools::HandleCreateLogicalPool() {
    TopologyService_Stub stub(&channel_);

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

    brpc::Controller cntl;
    cntl.set_max_retry(0);
    cntl.set_timeout_ms(-1);
    cntl.set_log_id(1);

    LOG(INFO) << "CreateLogicalPool, second request: "
              << request.DebugString();

    stub.CreateLogicalPool(&cntl, &request, &response, nullptr);

    if (cntl.ErrorCode() == EHOSTDOWN ||
        cntl.ErrorCode() == brpc::ELOGOFF) {
        return kRetCodeRedirectMds;

    } else if (cntl.Failed()) {
        LOG(ERROR) << "CreateLogicalPool, errcorde = "
                    << response.statuscode()
                    << ", error content:"
                    << cntl.ErrorText();
        return kRetCodeCommonErr;
    }
    if (response.statuscode() != 0) {
        LOG(ERROR) << "CreateLogicalPool Rpc response fail. "
                   << "Message is :"
                   << response.DebugString();
        return response.statuscode();
    } else {
        LOG(INFO) << "Received CreateLogicalPool Rpc response success, "
                  << response.DebugString();
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
            SplitString(line, CurvefsTools::clusterMapSeprator, &strList);

            std::vector<std::string>::size_type index = 0;
            while (index < strList.size() && strList[index].empty()) {
                index++;
                colNo++;
            }

            if (index >= strList.size()) {
                continue;   // blank line
            }

            CurveServerData info;
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
                return kRetCodeCommonErr;
            }

            while (index < strList.size() && strList[index].empty()) {
                index++;
                colNo++;
            }
            // internalIp & port
            if (index < strList.size() && !strList[index].empty()) {
                std::string ipPort = strList[index];
                std::vector<std::string> ipPortList;
                SplitString(ipPort, ":", &ipPortList);
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
                butil::ip_t ip;
                if (butil::str2ip(info.internalIp.c_str(), &ip) != 0) {
                    LOG(ERROR) << "parse cluster map error in line, context: \""
                               << line
                               << "\", line No: "
                               << lineNo
                               << ", invalid ip adress, ipPort str: "
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
                return kRetCodeCommonErr;
            }

            while (index < strList.size() && strList[index].empty()) {
                index++;
                colNo++;
            }
            // externalIp & port
            if (index < strList.size() && !strList[index].empty()) {
                std::string ipPort = strList[index];
                std::vector<std::string> ipPortList;
                SplitString(ipPort, ":", &ipPortList);
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
                butil::ip_t ip;
                if (butil::str2ip(info.externalIp.c_str(), &ip) != 0) {
                    LOG(ERROR) << "parse cluster map error in line, context: \""
                               << line
                               << "\", line No: "
                               << lineNo
                               << ", invalid ip adress, ipPort str: "
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
                return kRetCodeCommonErr;
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
                return kRetCodeCommonErr;
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
                return kRetCodeCommonErr;
            }

            cluster.push_back(info);
        }
    } else {
        LOG(ERROR) << "open cluster map file : "
                   << clusterMap
                   << " fail.";
        return kRetCodeCommonErr;
    }
    return 0;
}

int CurvefsTools::ListPhysicalPool(
    std::list<PhysicalPoolInfo> *physicalPoolInfos) {
    TopologyService_Stub stub(&channel_);
    ListPhysicalPoolRequest request;
    ListPhysicalPoolResponse response;
    brpc::Controller cntl;
    cntl.set_timeout_ms(FLAGS_rpcTimeOutMs);
    cntl.set_log_id(1);

    LOG(INFO) << "ListPhysicalPool send request: "
              << request.DebugString();

    stub.ListPhysicalPool(&cntl,
        &request,
        &response,
        nullptr);

    if (cntl.ErrorCode() == EHOSTDOWN ||
        cntl.ErrorCode() == brpc::ELOGOFF) {
        return kRetCodeRedirectMds;
    } else if (cntl.Failed()) {
        LOG(ERROR) << "ListPhysicalPool Rpc fail, errcorde = "
                    << response.statuscode()
                    << ", error content:"
                    << cntl.ErrorText();
        return kRetCodeCommonErr;
    }
    if (response.statuscode() != kTopoErrCodeSuccess) {
        LOG(ERROR) << "ListPhysicalPool Rpc response fail. "
                   << "Message is :"
                   << response.DebugString();
        return response.statuscode();
    } else {
        LOG(INFO) << "Received ListPhysicalPool Rpc response success, "
                  << response.DebugString();
    }

    for (int i = 0;
            i < response.physicalpoolinfos_size();
            i++) {
        physicalPoolInfos->push_back(
            response.physicalpoolinfos(i));
    }
    return 0;
}

int CurvefsTools::AddListPoolZone(PoolIdType poolid,
    std::list<ZoneInfo> *zoneInfos) {
    TopologyService_Stub stub(&channel_);
    ListPoolZoneRequest request;
    ListPoolZoneResponse response;
    request.set_physicalpoolid(poolid);

    brpc::Controller cntl;
    cntl.set_timeout_ms(FLAGS_rpcTimeOutMs);
    cntl.set_log_id(1);

    LOG(INFO) << "ListPoolZone, send request: "
              << request.DebugString();

    stub.ListPoolZone(&cntl, &request, &response, nullptr);

    if (cntl.ErrorCode() == EHOSTDOWN ||
        cntl.ErrorCode() == brpc::ELOGOFF) {
        return kRetCodeRedirectMds;
    } else if (cntl.Failed()) {
        LOG(ERROR) << "ListPoolZone Rpc fail, errcorde = "
                   << response.statuscode()
                   << ", error content:"
                   << cntl.ErrorText()
                   << ", physicalpoolid = "
                   << poolid;
        return kRetCodeCommonErr;
    }
    if (response.statuscode() != kTopoErrCodeSuccess) {
        LOG(ERROR) << "ListPoolZone Rpc response fail. "
                   << "Message is :"
                   << response.DebugString()
                   << " , physicalpoolid = "
                   << poolid;
        return response.statuscode();
    } else {
        LOG(INFO) << "Received ListPoolZone Rpc response success, "
                  << response.DebugString();
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
    request.set_zoneid(zoneid);
    brpc::Controller cntl;
    cntl.set_timeout_ms(FLAGS_rpcTimeOutMs);
    cntl.set_log_id(1);

    LOG(INFO) << "ListZoneServer, send request: "
              << request.DebugString();

    stub.ListZoneServer(&cntl, &request, &response, nullptr);

    if (cntl.ErrorCode() == EHOSTDOWN ||
        cntl.ErrorCode() == brpc::ELOGOFF) {
        return kRetCodeRedirectMds;
    } else if (cntl.Failed()) {
        LOG(ERROR) << "ListZoneServer Rpc fail, errcorde = "
                   << response.statuscode()
                   << ", error content:"
                   << cntl.ErrorText()
                   << ", zoneid = "
                   << zoneid;
        return kRetCodeCommonErr;
    }
    if (response.statuscode() != kTopoErrCodeSuccess) {
        LOG(ERROR) << "ListZoneServer Rpc response fail. "
                   << "Message is :"
                   << response.DebugString()
                   << " , zoneid = "
                   << zoneid;
        return response.statuscode();
    } else {
        LOG(ERROR) << "ListZoneServer Rpc response success, "
                   << response.DebugString();
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
            [server](CurvePhysicalPoolData& data) {
            return data.physicalPoolName ==
            server.physicalPoolName;
        }) != physicalPoolToAdd.end()) {
            continue;
        }
        CurvePhysicalPoolData poolData;
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
                [it] (CurvePhysicalPoolData& data) {
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
            [server](CurveZoneData& data) {
            return (data.physicalPoolName ==
                server.physicalPoolName) &&
                   (data.zoneName ==
                server.zoneName);
        }) != zoneToAdd.end()) {
            continue;
        }
        CurveZoneData CurveZoneData;
        CurveZoneData.physicalPoolName = server.physicalPoolName;
        CurveZoneData.zoneName = server.zoneName;
        zoneToAdd.push_back(CurveZoneData);
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
            [it] (CurveZoneData &data) {
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
            [server](CurveServerData& data) {
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
            [it] (CurveServerData &data) {
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
        PhysicalPoolRequest request;
        request.set_physicalpoolname(it.physicalPoolName);
        request.set_desc("");

        PhysicalPoolResponse response;

        brpc::Controller cntl;
        cntl.set_timeout_ms(FLAGS_rpcTimeOutMs);
        cntl.set_log_id(1);

        LOG(INFO) << "CreatePhysicalPool, send request: "
                  << request.DebugString();

        stub.CreatePhysicalPool(&cntl, &request, &response, nullptr);

        if (cntl.ErrorCode() == EHOSTDOWN ||
            cntl.ErrorCode() == brpc::ELOGOFF) {
            return kRetCodeRedirectMds;
        } else if (cntl.Failed()) {
            LOG(ERROR) << "CreatePhysicalPool, errcorde = "
                       << response.statuscode()
                       << ", error content:"
                       << cntl.ErrorText()
                       << " , physicalPoolName ="
                       << it.physicalPoolName;
            return kRetCodeCommonErr;
        }
        if (response.statuscode() != kTopoErrCodeSuccess) {
            LOG(ERROR) << "CreatePhysicalPool Rpc response fail. "
                       << "Message is :"
                       << response.DebugString()
                       << " , physicalPoolName ="
                       << it.physicalPoolName;
            return response.statuscode();
        } else {
            LOG(INFO) << "Received CreatePhysicalPool response success, "
                      << response.DebugString();
        }
    }
    return 0;
}

int CurvefsTools::CreateZone() {
    TopologyService_Stub stub(&channel_);
    for (auto it : zoneToAdd) {
        ZoneRequest request;
        request.set_zonename(it.zoneName);
        request.set_physicalpoolname(it.physicalPoolName);
        request.set_desc("");

        ZoneResponse response;

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
                       << it.zoneName;
            return kRetCodeCommonErr;
        }
        if (response.statuscode() != 0) {
            LOG(ERROR) << "CreateZone Rpc response fail. "
                       << "Message is :"
                       << response.DebugString()
                       << " , zoneName = "
                       << it.zoneName;
            return response.statuscode();
        } else {
            LOG(INFO) << "Received CreateZone Rpc success, "
                      << response.DebugString();
        }
    }
    return 0;
}

int CurvefsTools::CreateServer() {
    TopologyService_Stub stub(&channel_);
    for (auto it : serverToAdd) {
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
                       << it.serverName;
            return kRetCodeCommonErr;
        }
        if (response.statuscode() != 0) {
            LOG(ERROR) << "RegistServer Rpc response fail. "
                       << "Message is :"
                       << response.DebugString()
                       << " , serverName = "
                       << it.serverName;
            return response.statuscode();
        } else {
            LOG(INFO) << "Received RegistServer Rpc response success, "
                      << response.DebugString();
        }
    }
    return 0;
}

int CurvefsTools::ClearPhysicalPool() {
    TopologyService_Stub stub(&channel_);
    for (auto it : physicalPoolToDel) {
        PhysicalPoolRequest request;
        request.set_physicalpoolid(it);

        PhysicalPoolResponse response;

        brpc::Controller cntl;
        cntl.set_timeout_ms(FLAGS_rpcTimeOutMs);
        cntl.set_log_id(1);

        LOG(INFO) << "DeletePhysicalPool, send request: "
                  << request.DebugString();

        stub.DeletePhysicalPool(&cntl, &request, &response, nullptr);

        if (cntl.ErrorCode() == EHOSTDOWN ||
            cntl.ErrorCode() == brpc::ELOGOFF) {
            return kRetCodeRedirectMds;
        } else if (cntl.Failed()) {
            LOG(ERROR) << "DeletePhysicalPool, errcorde = "
                       << response.statuscode()
                       << ", error content:"
                       << cntl.ErrorText()
                       << " , physicalPoolId = "
                       << it;
            return kRetCodeCommonErr;
        }
        if (response.statuscode() != kTopoErrCodeSuccess) {
            LOG(ERROR) << "DeletePhysicalPool Rpc response fail. "
                       << "Message is :"
                       << response.DebugString()
                       << " , physicalPoolId = "
                       << it;
            return response.statuscode();
        } else {
            LOG(INFO) << "Received DeletePhysicalPool Rpc response success, "
                      << response.statuscode();
        }
    }
    return 0;
}

int CurvefsTools::ClearZone() {
    TopologyService_Stub stub(&channel_);
    for (auto it : zoneToDel) {
        ZoneRequest request;
        request.set_zoneid(it);

        ZoneResponse response;

        brpc::Controller cntl;
        cntl.set_timeout_ms(FLAGS_rpcTimeOutMs);
        cntl.set_log_id(1);

        LOG(INFO) << "DeleteZone, send request: "
                  << request.DebugString();

        stub.DeleteZone(&cntl, &request, &response, nullptr);

        if (cntl.ErrorCode() == EHOSTDOWN ||
            cntl.ErrorCode() == brpc::ELOGOFF) {
            return kRetCodeRedirectMds;
        } else if (cntl.Failed()) {
            LOG(ERROR) << "DeleteZone, errcorde = "
                       << response.statuscode()
                       << ", error content:"
                       << cntl.ErrorText()
                       << " , zoneId = "
                       << it;
            return kRetCodeCommonErr;
        } else {
            break;
        }
        if (response.statuscode() != kTopoErrCodeSuccess) {
            LOG(ERROR) << "DeleteZone Rpc response fail. "
                       << "Message is :"
                       << response.DebugString()
                       << " , zoneId = "
                       << it;
            return response.statuscode();
        } else {
            LOG(INFO) << "Received DeleteZone Rpc success, "
                      << response.DebugString();
        }
    }
    return 0;
}

int CurvefsTools::ClearServer() {
    TopologyService_Stub stub(&channel_);
    for (auto it : serverToDel) {
        DeleteServerRequest request;
        request.set_serverid(it);

        DeleteServerResponse response;

        brpc::Controller cntl;
        cntl.set_timeout_ms(FLAGS_rpcTimeOutMs);
        cntl.set_log_id(1);

        LOG(INFO) << "DeleteServer, send request: "
                  << request.DebugString();

        stub.DeleteServer(&cntl, &request, &response, nullptr);

        if (cntl.ErrorCode() == EHOSTDOWN ||
            cntl.ErrorCode() == brpc::ELOGOFF) {
            return kRetCodeRedirectMds;
        } else if (cntl.Failed()) {
            LOG(ERROR) << "DeleteServer, errcorde = "
                       << response.statuscode()
                       << ", error content:"
                       << cntl.ErrorText()
                       << " , serverId = "
                       << it;
            return kRetCodeCommonErr;
        }
        if (response.statuscode() != kTopoErrCodeSuccess) {
            LOG(ERROR) << "DeleteServer Rpc response fail. "
                       << "Message is :"
                       << response.DebugString()
                       << " , serverId = "
                       << it;
            return response.statuscode();
        } else {
            LOG(INFO) << "Received DeleteServer Rpc response success, "
                      << response.DebugString();
        }
    }
    return 0;
}

int CurvefsTools::SetChunkServer() {
    SetChunkServerStatusRequest request;
    request.set_chunkserverid(FLAGS_chunkserver_id);
    if (FLAGS_chunkserver_status == "retired") {
        request.set_chunkserverstatus(ChunkServerStatus::RETIRED);
    } else if (FLAGS_chunkserver_status == "pendding") {
        request.set_chunkserverstatus(ChunkServerStatus::PENDDING);
    } else if (FLAGS_chunkserver_status == "readwrite") {
        request.set_chunkserverstatus(ChunkServerStatus::READWRITE);
    } else {
        LOG(ERROR) << "SetChunkServer param error, unknown chunkserver status";
        return kRetCodeCommonErr;
    }

    SetChunkServerStatusResponse response;
    TopologyService_Stub stub(&channel_);
    brpc::Controller cntl;
    cntl.set_timeout_ms(FLAGS_rpcTimeOutMs);
    cntl.set_log_id(1);

    LOG(INFO) << "SetChunkServerStatusRequest, send request: "
              << request.DebugString();

    stub.SetChunkServer(&cntl, &request, &response, nullptr);

    if (cntl.ErrorCode() == EHOSTDOWN ||
        cntl.ErrorCode() == brpc::ELOGOFF) {
        return kRetCodeRedirectMds;
    } else if (cntl.Failed()) {
        LOG(ERROR) << "SetChunkServerStatusRequest, errcorde = "
                   << response.statuscode()
                   << ", error content:"
                   << cntl.ErrorText();
        return kRetCodeCommonErr;
    }
    if (response.statuscode() != kTopoErrCodeSuccess) {
        LOG(ERROR) << "SetChunkServerStatusRequest Rpc response fail. "
                   << "Message is :"
                   << response.DebugString();
        return response.statuscode();
    } else {
        LOG(INFO) << "Received SetChunkServerStatusRequest Rpc "
                  << "response success, "
                  << response.DebugString();
    }
    return 0;
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
        return kRetCodeCommonErr;
    }

    int maxTry = tools.GetMaxTry();
    int retry = 0;
    for (; retry < maxTry; retry++) {
        ret = tools.TryAnotherMdsAddress();
        if (ret < 0) {
            return kRetCodeCommonErr;
        }

        std::string operation = FLAGS_op;
        if (operation == "create_logicalpool") {
            ret = tools.HandleCreateLogicalPool();
        } else if (operation == "create_physicalpool") {
            ret = tools.HandleBuildCluster();
        } else if (operation == "set_chunkserver") {
            ret = tools.SetChunkServer();
        } else {
            LOG(ERROR) << "undefined op.";
            ret = kRetCodeCommonErr;
            break;
        }
        if (ret != kRetCodeRedirectMds) {
            break;
        }
    }

    if (retry >= maxTry) {
        LOG(ERROR) << "rpc retry times exceed.";
        return kRetCodeCommonErr;
    }
    if (ret < 0) {
        LOG(ERROR) << "exec fail, ret = " << ret;
    } else {
        LOG(INFO) << "exec success, ret = " << ret;
    }

    return ret;
}


