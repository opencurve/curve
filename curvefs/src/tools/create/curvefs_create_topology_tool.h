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
 * Author: chengyi01
 */

#ifndef CURVEFS_SRC_TOOLS_CREATE_CURVEFS_CREATE_TOPOLOGY_TOOL_H_
#define CURVEFS_SRC_TOOLS_CREATE_CURVEFS_CREATE_TOPOLOGY_TOOL_H_

#include <brpc/channel.h>
#include <brpc/server.h>
#include <butil/endpoint.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <json/json.h>

#include <fstream>
#include <list>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "curvefs/proto/topology.pb.h"
#include "curvefs/src/mds/common/mds_define.h"
#include "curvefs/src/tools/curvefs_tool.h"
#include "curvefs/src/tools/curvefs_tool_abstract_creator.h"
#include "curvefs/src/tools/curvefs_tool_define.h"
#include "src/common/configuration.h"
#include "src/common/string_util.h"

namespace curvefs {
namespace mds {
namespace topology {
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

class CurvefsBuildTopologyTool : public curvefs::tools::CurvefsTool {
 public:
    CurvefsBuildTopologyTool()
        : curvefs::tools::CurvefsTool(
              std::string(curvefs::tools::kCreateTopologyCmd),
              std::string(curvefs::tools::kProgrameName)) {}
    ~CurvefsBuildTopologyTool() {}

    int Init();

    int InitTopoData();

    int HandleBuildCluster();

    int GetMaxTry() {
        return mdsAddressStr_.size();
    }

    int TryAnotherMdsAddress();

    void PrintHelp() override {
        std::cout << "Example :" << std::endl;
        std::cout << programe_ << " " << command_ << " "
                  << curvefs::tools::kConfPathHelp << std::endl;
    }

    int RunCommand();

    int Run() override {
        if (Init() < 0) {
            LOG(ERROR) << "CurvefsBuildTopologyTool init error.";
            return kRetCodeCommonErr;
        }
        int ret = InitTopoData();
        if (ret < 0) {
            LOG(ERROR) << "Init topo json data error.";
            return ret;
        }
        ret = RunCommand();
        return ret;
    }

 public:
    // for unit test
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
    int InitServerZoneData();
    int InitPoolData();
    int ScanCluster();
    int ScanPool();
    int RemovePoolsNotInNewTopo();
    int RemoveZonesNotInNewTopo();
    int RemoveServersNotInNewTopo();
    int CreatePool();
    int CreateZone();
    int CreateServer();

    int DealFailedRet(int ret, const std::string& operation);

    int ListPool(std::list<PoolInfo>* poolInfos);

    int GetZonesInPool(PoolIdType poolid, std::list<ZoneInfo>* zoneInfos);

    int GetServersInZone(ZoneIdType zoneid, std::list<ServerInfo>* serverInfos);

 private:
    std::list<Server> serverDatas;
    std::list<Zone> zoneDatas;
    std::list<Pool> poolDatas;

    std::list<ServerIdType> serverToDel;
    std::list<ZoneIdType> zoneToDel;
    std::list<PoolIdType> poolToDel;

    std::vector<std::string> mdsAddressStr_;
    int mdsAddressIndex_;
    brpc::Channel channel_;
    Json::Value clusterMap_;
};

}  // namespace topology
}  // namespace mds
}  // namespace curvefs

#endif  // CURVEFS_SRC_TOOLS_CREATE_CURVEFS_CREATE_TOPOLOGY_TOOL_H_
