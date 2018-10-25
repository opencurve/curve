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

#include "proto/topology.pb.h"

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
DEFINE_int32(copyset_num, 100, "copyset num.");
DEFINE_int32(zone_num, 3, "zone num.");

DEFINE_string(cluster_map, "./topo.txt", "cluster topology map.");

namespace curve {
namespace mds {
namespace topology {

struct ServerData {
    std::string serverName;
    std::string internalIp;
    std::string externalIp;
    std::string zoneName;
    std::string physicalPoolName;
};

struct ZoneData {
    std::string zoneName;
    std::string physicalPoolName;
};

class CurvefsTools {
 public:
    CurvefsTools() {}
    ~CurvefsTools() {}

    bool HandleCreateLogicalPool();
    bool HandleBuildCluster();

    static const std::string clusterMapSeprator;
 private:
    void SplitString(const std::string& s,
        std::vector<std::string> *v,
        const std::string& c);

    bool ReadClusterMap();
    bool CreatePhysicalPool();
    bool CreateZone();
    bool CreateServer();

    std::vector<ServerData> cluster;
};

const std::string CurvefsTools::clusterMapSeprator = " ";  // NOLINT

bool CurvefsTools::HandleCreateLogicalPool() {
    bool ret = true;

    brpc::Channel channel;
    if (channel.Init(FLAGS_mds_ip.c_str(), FLAGS_mds_port, NULL) != 0) {
        LOG(FATAL) << "Fail to init channel to ip: "
                   << FLAGS_mds_ip
                   << " port "
                   << FLAGS_mds_port
                   << std::endl;
    }

    TopologyService_Stub stub(&channel);

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

    CreateLogicalPoolResponse response;
    stub.CreateLogicalPool(&cntl, &request, &response, nullptr);

     if (cntl.Failed()) {
        LOG(ERROR) << "CreateLogicalPool, errcorde = "
                    << response.statuscode()
                    << ", error content:"
                    << cntl.ErrorText();
        return false;
    }
    if (response.statuscode() != 0) {
        LOG(ERROR) << "Rpc response fail. "
                   << "Message is :"
                   << response.DebugString();
        return false;
    }

    return ret;
}

bool CurvefsTools::HandleBuildCluster() {
    if (ReadClusterMap()) {
        if (CreatePhysicalPool()) {
            if (CreateZone()) {
                if (CreateServer()) {
                    return true;
                } else {
                    LOG(ERROR) << "create server fail.";
                    return false;
                }
            } else {
                LOG(ERROR) << "create zone fail.";
                return false;
            }
        } else {
            LOG(ERROR) << "create physicalpool fail.";
            return false;
        }
    } else {
        LOG(ERROR) << "read cluster map fail";
        return false;
    }
    return true;
}

bool CurvefsTools::ReadClusterMap() {
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

            int index = 0;
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
                return false;
            }

            while (index < strList.size() && strList[index].empty()) {
                index++;
                colNo++;
            }
            // internalIp
            if (index < strList.size() && !strList[index].empty()) {
                info.internalIp = strList[index];
                index++;
                colNo += strList[index].size();
            } else {
                LOG(ERROR) << "parse cluster map error in line, context: \""
                           << line
                           << "\", line No: "
                           << lineNo
                           << ", colume No: "
                           << colNo;
                return false;
            }

            while (index < strList.size() && strList[index].empty()) {
                index++;
                colNo++;
            }
            // externalIp
            if (index < strList.size() && !strList[index].empty()) {
                info.externalIp = strList[index];
                index++;
                colNo += strList[index].size();
            } else {
                LOG(ERROR) << "parse cluster map error in line, context: \""
                           << line
                           << "\", line No: "
                           << lineNo
                           << ", colume No: "
                           << colNo;
                return false;
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
                return false;
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
                return false;
            }

            cluster.push_back(info);
        }
    } else {
        LOG(ERROR) << "open cluster map file : "
                   << clusterMap
                   << " fail.";
        return false;
    }
    return true;
}

bool CurvefsTools::CreatePhysicalPool() {
    std::set<std::string> physicalPoolSet;
    for (auto server : cluster) {
        physicalPoolSet.insert(server.physicalPoolName);
    }

    for (auto it : physicalPoolSet) {
        brpc::Channel channel;
        if (channel.Init(FLAGS_mds_ip.c_str(), FLAGS_mds_port, NULL) != 0) {
        LOG(FATAL) << "Fail to init channel to ip: "
                   << FLAGS_mds_ip
                   << " port "
                   << FLAGS_mds_port
                   << std::endl;
        }

        TopologyService_Stub stub(&channel);

        brpc::Controller cntl;
        cntl.set_timeout_ms(1000);
        cntl.set_log_id(1);

        PhysicalPoolRequest request;
        request.set_physicalpoolname(it);
        request.set_desc("");

        PhysicalPoolResponse response;
        stub.CreatePhysicalPool(&cntl, &request, &response, nullptr);

         if (cntl.Failed()) {
            LOG(ERROR) << "CreatePhysicalPool, errcorde = "
                        << response.statuscode()
                        << ", error content:"
                        << cntl.ErrorText();

            return false;
        }
        if (response.statuscode() != 0) {
            LOG(ERROR) << "Rpc response fail. "
                       << "Message is :"
                       << response.DebugString();

            return false;
        }
    }
    return true;
}

bool CurvefsTools::CreateZone() {
    std::map<std::string, ZoneData> zoneMap;
    for (auto server : cluster) {
        ZoneData zone;
        zone.zoneName = server.zoneName;
        zone.physicalPoolName = server.physicalPoolName;
        zoneMap[zone.zoneName] = zone;
    }

    for (auto it : zoneMap) {
        brpc::Channel channel;
        if (channel.Init(FLAGS_mds_ip.c_str(), FLAGS_mds_port, NULL) != 0) {
        LOG(FATAL) << "Fail to init channel to ip: "
                   << FLAGS_mds_ip
                   << " port "
                   << FLAGS_mds_port
                   << std::endl;
        }

        TopologyService_Stub stub(&channel);

        brpc::Controller cntl;
        cntl.set_timeout_ms(1000);
        cntl.set_log_id(1);

        ZoneRequest request;
        request.set_zonename(it.first);
        request.set_physicalpoolname(it.second.physicalPoolName);
        request.set_desc("");

        ZoneResponse response;
        stub.CreateZone(&cntl, &request, &response, nullptr);

         if (cntl.Failed()) {
            LOG(ERROR) << "CreateZone, errcorde = "
                        << response.statuscode()
                        << ", error content:"
                        << cntl.ErrorText();
            return false;
        }
        if (response.statuscode() != 0) {
            LOG(ERROR) << "Rpc response fail. "
                       << "Message is :"
                       << response.DebugString();
            return false;
        }
    }
    return true;
}

bool CurvefsTools::CreateServer() {
    for (auto it : cluster) {
        brpc::Channel channel;
        if (channel.Init(FLAGS_mds_ip.c_str(), FLAGS_mds_port, NULL) != 0) {
        LOG(FATAL) << "Fail to init channel to ip: "
                   << FLAGS_mds_ip
                   << " port "
                   << FLAGS_mds_port
                   << std::endl;
        }

        TopologyService_Stub stub(&channel);

        brpc::Controller cntl;
        cntl.set_timeout_ms(1000);
        cntl.set_log_id(1);

        ServerRegistRequest request;
        request.set_hostname(it.serverName);
        request.set_internalip(it.internalIp);
        request.set_externalip(it.externalIp);
        request.set_zonename(it.zoneName);
        request.set_physicalpoolname(it.physicalPoolName);
        request.set_desc("");

        ServerRegistResponse response;
        stub.RegistServer(&cntl, &request, &response, nullptr);

         if (cntl.Failed()) {
            LOG(ERROR) << "RegistServer, errcorde = "
                        << response.statuscode()
                        << ", error content:"
                        << cntl.ErrorText();
            return false;
        }
        if (response.statuscode() != 0) {
            LOG(ERROR) << "Rpc response fail. "
                       << "Message is :"
                       << response.DebugString();
            return false;
        }
    }
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

    curve::mds::topology::CurvefsTools tools;

    std::string operation = FLAGS_op;
    if (operation == "create_logicalpool") {
        tools.HandleCreateLogicalPool();
    } else if (operation == "create_physicalpool") {
        tools.HandleBuildCluster();
    } else {
        LOG(ERROR) << "undefined op.";
    }

    return 0;
}


