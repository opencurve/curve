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

#ifndef CURVEFS_SRC_TOOLS_CURVEFS_TOPOLOGY_TOOL_H_
#define CURVEFS_SRC_TOOLS_CURVEFS_TOPOLOGY_TOOL_H_

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <brpc/server.h>
#include <brpc/channel.h>
#include <butil/endpoint.h>
#include <json/json.h>

#include <fstream>
#include <string>
#include <vector>
#include <map>
#include <set>
#include <list>

#include "curvefs/proto/topology.pb.h"
#include "curvefs/src/mds/common/mds_define.h"
#include "src/common/string_util.h"
#include "src/common/configuration.h"

const int kRetCodeCommonErr = -1;
const int kRetCodeRedirectMds = -2;

const char kPools[] = "pools";
const char kPool[] = "pool";
const char kServers[] = "servers";
const char kMetaServer[] = "metaservers";
const char kName[] = "name";
const char kInternalIp[] = "internalip";
const char kInternalPort[] = "internalport";
const char kExternalIp[] = "externalip";
const char kExternalPort[] = "externalport";
const char kZone[] = "zone";
const char kReplicasNum[] = "replicasnum";
const char kCopysetNum[] = "copysetnum";
const char kZoneNum[] = "zonenum";

namespace curvefs {
namespace mds {
namespace topology {

struct MetaServer {
    std::string name;
    std::string internalIp;
    uint32_t internalPort;
    std::string externalIp;
    uint32_t externalPort;
};

struct Server {
    std::string name;
    std::string internalIp;
    uint32_t internalPort;
    std::string externalIp;
    uint32_t externalPort;
    std::string zoneName;
    std::string poolName;
};

struct Pool {
    std::string name;
    uint32_t replicasNum;
    uint64_t copysetNum;
    uint32_t zoneNum;
};

struct Zone {
    std::string name;
    std::string poolName;
};


class CurvefsTopologyTool {
 public:
    CurvefsTopologyTool() {}
    ~CurvefsTopologyTool() {}

    int Init();

    int HandleBuildCluster();

    int GetMaxTry() {
        return mdsAddressStr_.size();
    }

    int TryAnotherMdsAddress();

 public:
    // for unit test
    const std::list<MetaServer>& GetMetaServerDatas() {
        return metaserverDatas;
    }

    const std::list<Server>& GetServerDatas() {
        return serverDatas;
    }

    const std::list<Zone>& GetZoneDatas() {
        return zoneDatas;
    }

    const std::list<Pool>& GetPoolDatas() {
        return poolDatas;
    }

 private:
    int ReadClusterMap();
    int InitMetaServerData();
    int InitServerZoneData();
    int InitPoolData();
    int ScanCluster();
    int ScanPool();
    int CreatePool();
    int CreateZone();
    int CreateServer();
    int CreateMetaServer();

    int DealFailedRet(int ret, std::string operation);

    int ListPool(std::list<PoolInfo> *poolInfos);

    int GetZonesInPool(PoolIdType poolid, std::list<ZoneInfo> *zoneInfos);

    int GetServersInZone(ZoneIdType zoneid, std::list<ServerInfo> *serverInfos);

    int GetMetaServersInServer(ServerIdType serverId,
        std::list<MetaServerInfo> *metaserverInfos);

 private:
    std::list<MetaServer> metaserverDatas;
    std::list<Server> serverDatas;
    std::list<Zone> zoneDatas;
    std::list<Pool> poolDatas;

    std::vector<std::string> mdsAddressStr_;
    int mdsAddressIndex_;
    brpc::Channel channel_;
    Json::Value clusterMap_;
};

}  // namespace topology
}  // namespace mds
}  // namespace curvefs

#endif  // CURVEFS_SRC_TOOLS_CURVEFS_TOPOLOGY_TOOL_H_
