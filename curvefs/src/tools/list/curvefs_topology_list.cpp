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
 * Created Date: 2021-12-20
 * Author: chengyi01
 */

#include "curvefs/src/tools/list/curvefs_topology_list.h"

#include <json/json.h>

#include <fstream>
#include <memory>

#include "src/common/string_util.h"

DECLARE_string(mdsAddr);
DECLARE_string(jsonPath);
DECLARE_string(jsonType);

namespace curvefs {
namespace tools {
namespace list {

void TopologyListTool::PrintHelp() {
    CurvefsToolRpc::PrintHelp();
    std::cout << " [-mdsAddr=" << FLAGS_mdsAddr
              << "] [-jsonType=" << FLAGS_jsonType
              << " -jsonPath=" << FLAGS_jsonPath << "]" << std::endl;
}

void TopologyListTool::AddUpdateFlags() {
    AddUpdateFlagsFunc(curvefs::tools::SetMdsAddr);
}

int TopologyListTool::Init() {
    if (CurvefsToolRpc::Init() != 0) {
        return -1;
    }

    curve::common::SplitString(FLAGS_mdsAddr, ",", &hostsAddr_);

    service_stub_func_ =
        std::bind(&curvefs::mds::topology::TopologyService_Stub::ListTopology,
                  service_stub_.get(), std::placeholders::_1,
                  std::placeholders::_2, std::placeholders::_3, nullptr);

    curvefs::mds::topology::ListTopologyRequest request;
    AddRequest(request);

    return 0;
}

bool TopologyListTool::AfterSendRequestToHost(const std::string& host) {
    bool ret = false;
    if (controller_->Failed()) {
        errorOutput_ << "get fsinfo from mds: " << host
                     << " failed, errorcode= " << controller_->ErrorCode()
                     << ", error text " << controller_->ErrorText() << "\n";
    } else {
        ret = true;
        // clusterId
        clusterId_ = response_->clusterid();
        clusterId2CLusterInfo_.insert(std::pair<std::string, ClusterInfo>(
            clusterId_,
            ClusterInfo(clusterId_, std::vector<mds::topology::PoolIdType>())));
        // pool
        if (!GetPoolInfoFromResponse()) {
            ret = false;
        }
        // zone
        if (!GetZoneInfoFromResponse()) {
            ret = false;
        }
        // server
        if (!GetServerInfoFromResponse()) {
            ret = false;
        }
        // metaserver
        if (!GetMetaserverInfoFromResponse()) {
            ret = false;
        }

        // show
        if (show_) {
            // cluster
            std::cout << "[cluster]\n"
                      << mds::topology::kClusterId << ": " << clusterId_
                      << std::endl;

            // pool
            std::cout << "[pool]" << std::endl;
            for (auto const& i : poolId2PoolInfo_) {
                ShowPoolInfo(i.second);
            }
            // zone
            std::cout << "[zone]" << std::endl;
            for (auto const& i : zoneId2ZoneInfo_) {
                ShowZoneInfo(i.second);
            }
            // server
            std::cout << "[server]" << std::endl;
            for (auto const& i : serverId2ServerInfo_) {
                ShowServerInfo(i.second);
            }
            // metaserver
            std::cout << "[metaserver]" << std::endl;
            for (auto const& i : metaserverId2MetaserverInfo_) {
                ShowMetaserverInfo(i.second);
            }
        }

        google::CommandLineFlagInfo info;
        if (!CheckJsonPathDefault(&info) && ret) {
            OutputFile();
        }
    }
    return ret;
}

PoolPolicy::PoolPolicy(const std::string& jsonStr) {
    Json::CharReaderBuilder reader;
    std::stringstream ss(jsonStr);
    Json::Value json;
    bool parseCode = Json::parseFromStream(reader, ss, &json, nullptr);
    if (parseCode && !json["replicaNum"].isNull() &&
        !json["copysetNum"].isNull() && !json["zoneNum"].isNull()) {
        replicaNum = json["replicaNum"].asUInt();
        copysetNum = json["copysetNum"].asUInt();
        zoneNum = json["zoneNum"].asUInt();
    } else {
        error = true;
    }
}

std::ostream& operator<<(std::ostream& os, const PoolPolicy& policy) {
    if (policy.error) {
        os << "policy has error!";
    } else {
        os << mds::topology::kCopysetNum << ":" << policy.copysetNum << " "
           << mds::topology::kReplicasNum << ":" << policy.replicaNum << " "
           << mds::topology::kZoneNum << ":" << policy.zoneNum;
    }
    return os;
}

bool TopologyListTool::GetPoolInfoFromResponse() {
    bool ret = true;
    auto const& pools = response_->pools();
    if (pools.statuscode() != curvefs::mds::topology::TOPO_OK) {
        std::cerr << "the pools of cluster has error, error code is "
                  << pools.statuscode() << ", code name is "
                  << curvefs::mds::topology::TopoStatusCode_Name(
                         pools.statuscode())
                  << std::endl;
        ret = false;
    } else {
        for (auto const& i : pools.poolinfos()) {
            poolId2PoolInfo_.insert(
                std::pair<mds::topology::PoolIdType, PoolInfoType>(
                    i.poolid(),
                    PoolInfoType(i, std::vector<mds::topology::ZoneIdType>())));
            clusterId2CLusterInfo_[clusterId_].second.emplace_back(i.poolid());
        }
    }
    return ret;
}

bool TopologyListTool::GetZoneInfoFromResponse() {
    bool ret = true;
    auto const& zones = response_->zones();
    if (zones.statuscode() != curvefs::mds::topology::TOPO_OK) {
        std::cerr << "the zones of cluster has error, error code is "
                  << zones.statuscode() << ", code name is "
                  << curvefs::mds::topology::TopoStatusCode_Name(
                         zones.statuscode())
                  << std::endl;
        ret = false;
    } else {
        for (auto const& i : zones.zoneinfos()) {
            zoneId2ZoneInfo_.insert(
                std::pair<mds::topology::ZoneIdType, ZoneInfoType>(
                    i.zoneid(),
                    ZoneInfoType(i,
                                 std::vector<mds::topology::ServerIdType>())));
            if (poolId2PoolInfo_.find(i.poolid()) != poolId2PoolInfo_.end()) {
                poolId2PoolInfo_[i.poolid()].second.emplace_back(i.zoneid());
            } else {
                errorOutput_ << "zone:" << i.zoneid()
                             << " has error: poolId:" << i.poolid()
                             << " can't be found." << std::endl;
                ret = false;
            }
        }
    }
    return ret;
}

bool TopologyListTool::GetServerInfoFromResponse() {
    bool ret = true;
    auto const& servers = response_->servers();
    if (servers.statuscode() != curvefs::mds::topology::TOPO_OK) {
        std::cerr << "the servers of cluster has error, error code is "
                  << servers.statuscode() << ", code name is "
                  << curvefs::mds::topology::TopoStatusCode_Name(
                         servers.statuscode())
                  << std::endl;
        ret = false;
    } else {
        for (auto const& i : servers.serverinfos()) {
            serverId2ServerInfo_.insert(
                std::pair<mds::topology::ServerIdType, ServerInfoType>(
                    i.serverid(),
                    ServerInfoType(
                        i, std::vector<mds::topology::MetaServerIdType>())));

            if (zoneId2ZoneInfo_.find(i.zoneid()) != zoneId2ZoneInfo_.end()) {
                zoneId2ZoneInfo_[i.zoneid()].second.emplace_back(i.serverid());
            } else {
                errorOutput_ << "server:" << i.serverid()
                             << " has error: zoneId:" << i.zoneid()
                             << " can't be found." << std::endl;
                ret = false;
            }
        }
    }
    return ret;
}

bool TopologyListTool::GetMetaserverInfoFromResponse() {
    bool ret = true;
    auto const& metaservers = response_->metaservers();
    if (metaservers.statuscode() != curvefs::mds::topology::TOPO_OK) {
        std::cerr << "the metaservers of cluster has error, error code is "
                  << metaservers.statuscode() << ", code name is "
                  << curvefs::mds::topology::TopoStatusCode_Name(
                         metaservers.statuscode())
                  << std::endl;
        ret = false;
    } else {
        for (auto const& i : metaservers.metaserverinfos()) {
            metaserverId2MetaserverInfo_.insert(
                std::pair<mds::topology::MetaServerIdType, MetaserverInfoType>(
                    i.metaserverid(), MetaserverInfoType(i)));
            if (serverId2ServerInfo_.find(i.serverid()) !=
                serverId2ServerInfo_.end()) {
                serverId2ServerInfo_[i.serverid()].second.emplace_back(
                    i.metaserverid());
            } else {
                errorOutput_ << "metaserver:" << i.metaserverid()
                             << " has error: serverId:" << i.serverid()
                             << " can't be found." << std::endl;
                ret = false;
            }
        }
    }
    return ret;
}

void TopologyListTool::ShowPoolInfo(const PoolInfoType& pool) const {
    std::cout << PoolInfo2Str(pool.first);
    auto const& zoneIdVec = pool.second;
    std::cout << ", zoneList:{ ";
    for (auto const& i : zoneIdVec) {
        std::cout << i << " ";
    }
    std::cout << "}" << std::endl;
}

void TopologyListTool::ShowZoneInfo(const ZoneInfoType& zone) const {
    std::cout << ZoneInfo2Str(zone.first);
    auto const& serverIdVec = zone.second;
    std::cout << "serverList:{ ";
    for (auto const& i : serverIdVec) {
        std::cout << i << " ";
    }
    std::cout << "}" << std::endl;
}

void TopologyListTool::ShowServerInfo(const ServerInfoType& server) const {
    std::cout << ServerInfo2Str(server.first);
    auto const& metaserverIdVec = server.second;
    std::cout << "metaserverList:{ ";
    for (auto const& i : metaserverIdVec) {
        std::cout << i << " ";
    }
    std::cout << "}" << std::endl;
}

void TopologyListTool::ShowMetaserverInfo(
    const MetaserverInfoType& metaserver) const {
    std::cout << MetaserverInfo2Str(metaserver) << std::endl;
}

bool TopologyListTool::OutputFile() {
    Json::Value value;
    topology::TopologyTreeJson treeJson(*this);
    if (!treeJson.BuildJsonValue(&value, FLAGS_jsonType)) {
        std::cerr << "build json file failed!" << std::endl;
        return false;
    }

    std::ofstream jsonFile;
    jsonFile.open(FLAGS_jsonPath.c_str(), std::ios::out);
    if (!jsonFile) {
        std::cerr << "open json file failed!" << std::endl;
        return false;
    }
    Json::StreamWriterBuilder clusterMap;
    std::unique_ptr<Json::StreamWriter> writer(clusterMap.newStreamWriter());
    writer->write(value, &jsonFile);
    jsonFile.close();
    return true;
}

}  // namespace list
}  // namespace tools
}  // namespace curvefs
