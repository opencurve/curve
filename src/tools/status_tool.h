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
 * File Created: 2019-12-03
 * Author: charisu
 */

#ifndef SRC_TOOLS_STATUS_TOOL_H_
#define SRC_TOOLS_STATUS_TOOL_H_

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <brpc/channel.h>
#include <iostream>
#include <iomanip>
#include <vector>
#include <string>
#include <memory>
#include <map>
#include <unordered_map>
#include "proto/topology.pb.h"
#include "src/common/timeutility.h"
#include "src/mds/common/mds_define.h"
#include "src/tools/mds_client.h"
#include "src/tools/chunkserver_client.h"
#include "src/tools/namespace_tool_core.h"
#include "src/tools/copyset_check_core.h"
#include "src/tools/etcd_client.h"
#include "src/tools/version_tool.h"
#include "src/tools/curve_tool.h"
#include "src/tools/curve_tool_define.h"
#include "src/tools/metric_client.h"
#include "src/tools/metric_name.h"
#include "src/tools/snapshot_clone_client.h"
#include "src/common/uri_parser.h"

using curve::mds::topology::ChunkServerInfo;
using curve::mds::topology::ChunkServerStatus;
using curve::mds::topology::DiskState;
using curve::mds::topology::LogicalPoolInfo;
using curve::mds::topology::OnlineState;
using curve::mds::topology::PhysicalPoolInfo;
using curve::mds::topology::PoolIdType;

namespace curve {
namespace tool {
struct LogicalpoolSpaceInfo {
    std::string poolName = "";
    uint64_t totalChunkSize = 0;
    uint64_t usedChunkSize = 0;
    //The overall file size that can be accommodated
    uint64_t totalCapacity = 0;
    //Allocation size
    uint64_t allocatedSize = 0;
};

struct SpaceInfo {
    uint64_t totalChunkSize = 0;
    uint64_t usedChunkSize = 0;
    //The overall file size that can be accommodated
    uint64_t totalCapacity = 0;
    //Allocation size
    uint64_t allocatedSize = 0;
    //Allocation size of recycleBin
    uint64_t recycleAllocSize = 0;
    //File size present in the system
    uint64_t currentFileSize = 0;
    std::unordered_map<uint32_t, LogicalpoolSpaceInfo> lpoolspaceinfo;
};

enum class ServiceName {
    // mds
    kMds,
    kEtcd,
    kSnapshotCloneServer,
};

std::string ToString(ServiceName name);

class StatusTool : public CurveTool {
 public:
    StatusTool(std::shared_ptr<MDSClient> mdsClient,
               std::shared_ptr<EtcdClient> etcdClient,
               std::shared_ptr<CopysetCheckCore> copysetCheckCore,
               std::shared_ptr<VersionTool> versionTool,
               std::shared_ptr<MetricClient> metricClient,
               std::shared_ptr<SnapshotCloneClient> snapshotClient)
        : mdsClient_(mdsClient), copysetCheckCore_(copysetCheckCore),
          etcdClient_(etcdClient), metricClient_(metricClient),
          snapshotClient_(snapshotClient), versionTool_(versionTool),
          mdsInited_(false), etcdInited_(false), noSnapshotServer_(false) {}
    ~StatusTool() = default;

    /**
     * @brief Print help information
     * @param cmd: Command executed
     * @return None
     */
    void PrintHelp(const std::string &command) override;

    /**
     * @brief Execute command
     * @param cmd: Command executed
     * @return returns 0 for success, -1 for failure
     */
    int RunCommand(const std::string &command) override;

    /**
     * @brief returns whether the command is supported
     * @param command: The command executed
     * @return true/false
     */
    static bool SupportCommand(const std::string &command);

    /**
     * @brief to determine whether the cluster is healthy
     */
    bool IsClusterHeatlhy();

 private:
    int Init(const std::string &command);
    int SpaceCmd();
    int StatusCmd();
    int ChunkServerListCmd();
    int ServerListCmd();
    int LogicalPoolListCmd();
    int ChunkServerStatusCmd();
    int GetPoolsInCluster(std::vector<PhysicalPoolInfo> *phyPools,
                          std::vector<LogicalPoolInfo> *lgPools);
    int GetSpaceInfo(SpaceInfo *spaceInfo);
    int PrintClusterStatus();
    int PrintMdsStatus();
    int PrintEtcdStatus();
    int PrintChunkserverStatus(bool checkLeftSize = true);
    int PrintClientStatus();
    int ClientListCmd();
    int ScanStatusCmd();
    void PrintCsLeftSizeStatistics(
        const std::string &name,
        const std::map<PoolIdType, std::vector<uint64_t>> &poolLeftSize);
    int PrintSnapshotCloneStatus();

    /**
     * @brief to determine if the command needs to interact with ETCD
     * @param command: The command executed
     * @return needs to return true, otherwise it will return false
     */
    bool CommandNeedEtcd(const std::string &command);


    /**
     * @brief to determine if the command requires mds
     * @param command: The command executed
     * @return needs to return true, otherwise it will return false
     */
    bool CommandNeedMds(const std::string &command);

    /**
     * @brief: Determine if the command requires a snapshot clone server
     * @param command: The command executed
     * @return needs to return true, otherwise it will return false
     */
    bool CommandNeedSnapshotClone(const std::string &command);

    /**
     * @brief Print online status
     * @param name: The name corresponding to the online status
     * @param onlineStatus Map of online status
     */
    void PrintOnlineStatus(const std::string &name,
                           const std::map<std::string, bool> &onlineStatus);

    /**
     * @brief Get and print mds version information
     */
    int GetAndPrintMdsVersion();

    /**
     * @brief Check if the service is healthy
     * @param name Service Name
     */
    bool CheckServiceHealthy(const ServiceName &name);

 private:
    //Client sending RPC to mds
    std::shared_ptr<MDSClient> mdsClient_;
    //Copyset checking tool, used to check the health status of clusters and chunkservers
    std::shared_ptr<CopysetCheckCore> copysetCheckCore_;
    //ETCD client, used to call the ETCD API to obtain status
    std::shared_ptr<EtcdClient> etcdClient_;
    //Used to obtain metric
    std::shared_ptr<MetricClient> metricClient_;
    //Used to obtain the status of snapshot clones
    std::shared_ptr<SnapshotCloneClient> snapshotClient_;
    //Version client, used to obtain version information
    std::shared_ptr<VersionTool> versionTool_;
    //Has the mds been initialized
    bool mdsInited_;
    //Has ETCD been initialized
    bool etcdInited_;
    // Is there a snapshot service or not
    bool noSnapshotServer_;
};
}  // namespace tool
}  // namespace curve

#endif  // SRC_TOOLS_STATUS_TOOL_H_
