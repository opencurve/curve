/*
 *  Copyright (c) 2022 NetEase Inc.
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
 * Created Date: 2022-01-14
 * Author: chengyi01
 */

#include "curvefs/src/tools/list/curvefs_topology_tree_json.h"

#include "curvefs/src/tools/list/curvefs_topology_list.h"

namespace curvefs {
namespace tools {
namespace topology {

TopologyTreeJson::TopologyTreeJson(const list::TopologyListTool& topologyTool) {
    clusterId_ = topologyTool.clusterId_;
    clusterId2CLusterInfo_ = topologyTool.clusterId2CLusterInfo_;
    poolId2PoolInfo_ = topologyTool.poolId2PoolInfo_;
    zoneId2ZoneInfo_ = topologyTool.zoneId2ZoneInfo_;
    serverId2ServerInfo_ = topologyTool.serverId2ServerInfo_;
    metaserverId2MetaserverInfo_ = topologyTool.metaserverId2MetaserverInfo_;
}  // namespace topology

bool TopologyTreeJson::BuildClusterMapPools(Json::Value* pools) {
    bool ret = true;
    for (auto const& i : poolId2PoolInfo_) {
        Json::Value value;
        value[mds::topology::kName] = i.second.first.poolname();

        auto policy =
            list::PoolPolicy(i.second.first.redundanceandplacementpolicy());
        if (policy.error) {
            std::cerr << "build pool error!" << std::endl;
            ret = false;
            break;
        }
        value[mds::topology::kReplicasNum] = policy.replicaNum;
        value[mds::topology::kCopysetNum] = policy.copysetNum;
        value[mds::topology::kZoneNum] = policy.zoneNum;
        pools->append(value);
    }
    return ret;
}

bool TopologyTreeJson::BuildClusterMapServers(Json::Value* servers) {
    bool ret = true;
    for (auto const& i : serverId2ServerInfo_) {
        Json::Value value;
        auto server = i.second.first;
        value[mds::topology::kName] = server.hostname();
        value[mds::topology::kInternalIp] = server.internalip();
        value[mds::topology::kInternalPort] = server.internalport();
        value[mds::topology::kExternalIp] = server.externalip();
        value[mds::topology::kExternalPort] = server.externalport();
        auto zone = zoneId2ZoneInfo_[server.zoneid()].first;
        value[mds::topology::kZone] = zone.zonename();
        auto pool = poolId2PoolInfo_[zone.poolid()].first;
        value[mds::topology::kPool] = pool.poolname();
        servers->append(value);
    }
    return ret;
}

bool TopologyTreeJson::BuildJsonValue(Json::Value* value,
                                      const std::string& jsonType) {
    if (jsonType == kJsonTypeBuild) {
        // build json
        Json::Value pools;
        Json::Value servers;
        if (!(BuildClusterMapPools(&pools) &&
              BuildClusterMapServers(&servers))) {
            return false;
        }
        (*value)[mds::topology::kServers] = servers;
        (*value)[mds::topology::kPools] = pools;
    } else if (jsonType == kJsonTypeTree) {
        if (!GetClusterTree(value, clusterId_)) {
            return false;
        }
    } else {
        std::cerr << "-jsonType should be " << kJsonTypeBuild << " or "
                  << kJsonTypeTree << "!" << std::endl;
        return false;
    }

    return true;
}
bool TopologyTreeJson::GetClusterTree(Json::Value* cluster,
                                      const std::string& clusterId) {
    bool ret = true;
    auto clusterInfo = clusterId2CLusterInfo_[clusterId];
    (*cluster)[mds::topology::kClusterId] = clusterInfo.first;
    for (auto const& poolId : clusterInfo.second) {
        Json::Value pool;
        ret = GetPoolTree(&pool, poolId);
        if (!ret) {
            break;
        }
        (*cluster)[mds::topology::kPoollist].append(pool);
    }
    return ret;
}

bool TopologyTreeJson::GetPoolTree(Json::Value* pool, uint64_t poolId) {
    bool ret = true;
    auto poolInfo = poolId2PoolInfo_[poolId];
    (*pool)[mds::topology::kPoolId] = poolInfo.first.poolid();
    (*pool)[mds::topology::kPoolName] = poolInfo.first.poolname();
    (*pool)[mds::topology::kCreateTime] = poolInfo.first.createtime();
    Json::CharReaderBuilder reader;
    std::stringstream ss(poolInfo.first.redundanceandplacementpolicy());
    Json::Value policyValue;
    std::string err;
    if (!Json::parseFromStream(reader, ss, &policyValue, &err)) {
        std::cerr << "parse policy failed! error is " << err << std::endl;
        ret = false;
    }
    (*pool)[mds::topology::kPolicy] = policyValue;

    for (auto const& zoneId : poolInfo.second) {
        Json::Value zone;
        ret = GetZoneTree(&zone, zoneId);
        if (!ret) {
            break;
        }
        (*pool)[mds::topology::kZonelist].append(zone);
    }

    return ret;
}

bool TopologyTreeJson::GetZoneTree(Json::Value* zone, uint64_t zoneId) {
    bool ret = true;
    auto zoneInfo = zoneId2ZoneInfo_[zoneId];
    (*zone)[mds::topology::kZoneId] = zoneInfo.first.zoneid();
    (*zone)[mds::topology::kZoneName] = zoneInfo.first.zonename();
    (*zone)[mds::topology::kPoolId] = zoneInfo.first.poolid();

    for (auto const& serverId : zoneInfo.second) {
        Json::Value server;
        ret = GetServerTree(&server, serverId);
        if (!ret) {
            break;
        }
        (*zone)[mds::topology::kServerlist].append(server);
    }

    return ret;
}

bool TopologyTreeJson::GetServerTree(Json::Value* server, uint64_t serverId) {
    bool ret = true;
    auto serverinfo = serverId2ServerInfo_[serverId];
    (*server)[mds::topology::kServerId] = serverinfo.first.serverid();
    (*server)[mds::topology::kHostName] = serverinfo.first.hostname();
    (*server)[mds::topology::kInternalIp] = serverinfo.first.internalip();
    (*server)[mds::topology::kInternalPort] = serverinfo.first.internalport();
    (*server)[mds::topology::kExternalIp] = serverinfo.first.externalip();
    (*server)[mds::topology::kExternalPort] = serverinfo.first.externalport();
    (*server)[mds::topology::kZoneId] = serverinfo.first.zoneid();
    (*server)[mds::topology::kPoolId] = serverinfo.first.poolid();

    for (auto const& metaserverId : serverinfo.second) {
        Json::Value metaserver;
        ret = GetMetaserverTree(&metaserver, metaserverId);
        if (!ret) {
            break;
        }
        (*server)[mds::topology::kMetaserverList].append(metaserver);
    }

    return ret;
}

bool TopologyTreeJson::GetMetaserverTree(Json::Value* metaserver,
                                         uint64_t metaserverId) {
    bool ret = true;
    auto metaserverinfo = metaserverId2MetaserverInfo_[metaserverId];
    (*metaserver)[mds::topology::kMetaserverId] = metaserverinfo.metaserverid();
    (*metaserver)[mds::topology::kHostName] = metaserverinfo.hostname();
    (*metaserver)[mds::topology::kHostIp] = metaserverinfo.internalip();
    (*metaserver)[mds::topology::kPort] = metaserverinfo.internalport();
    (*metaserver)[mds::topology::kExternalIp] = metaserverinfo.externalip();
    (*metaserver)[mds::topology::kExternalPort] = metaserverinfo.externalport();
    (*metaserver)[mds::topology::kOnlineState] = metaserverinfo.onlinestate();
    (*metaserver)[mds::topology::kServerId] = metaserverinfo.serverid();

    return ret;
}

}  // namespace topology
}  // namespace tools
}  // namespace curvefs
