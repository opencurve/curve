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
 * Created Date: April 29 2021
 * Author: wanghai01
 */

#ifndef TOOLS_CURVEFSTOOL_H_
#define TOOLS_CURVEFSTOOL_H_

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

#include "proto/topology.pb.h"
#include "src/mds/common/mds_define.h"
#include "src/common/string_util.h"
#include "src/common/configuration.h"


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

struct CurveLogicalPoolData {
    std::string name;
    std::string physicalPoolName;
    curve::mds::topology::LogicalPoolType type;
    AllocateStatus status;
    uint32_t replicasNum;
    uint64_t copysetNum;
    uint32_t zoneNum;
    uint32_t scatterwidth;
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
    int SetLogicalPool();

    int GetMaxTry() {
        return mdsAddressStr_.size();
    }

    int TryAnotherMdsAddress();

    static const std::string clusterMapSeprator;

 private:
    int ReadClusterMap();
    int InitServerData();
    int InitLogicalPoolData();
    int ScanCluster();
    int ScanLogicalPool();
    int CreatePhysicalPool();
    int CreateZone();
    int CreateServer();

    int ClearPhysicalPool();
    int ClearZone();
    int ClearServer();

    int DealFailedRet(int ret, std::string operation);

    int ListPhysicalPool(
        std::list<PhysicalPoolInfo> *physicalPoolInfos);

    int ListLogicalPool(const std::string& phyPoolName,
        std::list<LogicalPoolInfo> *logicalPoolInfos);

    int AddListPoolZone(PoolIdType poolid,
        std::list<ZoneInfo> *zoneInfos);

    int AddListZoneServer(ZoneIdType zoneid,
        std::list<ServerInfo> *serverInfos);

 private:
    std::list<CurveServerData> serverDatas;
    std::list<CurveLogicalPoolData> lgPoolDatas;
    std::list<CurvePhysicalPoolData> physicalPoolToAdd;
    std::list<CurveZoneData> zoneToAdd;
    std::list<CurveServerData> serverToAdd;

    std::list<PoolIdType> physicalPoolToDel;
    std::list<ZoneIdType> zoneToDel;
    std::list<ServerIdType> serverToDel;

    std::vector<std::string> mdsAddressStr_;
    int mdsAddressIndex_;
    brpc::Channel channel_;
    Json::Value clusterMap_;
};

}  // namespace topology
}  // namespace mds
}  // namespace curve

#endif  // TOOLS_CURVEFSTOOL_H_
