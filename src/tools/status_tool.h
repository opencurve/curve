/*
 * Project: curve
 * File Created: 2019-12-03
 * Author: charisu
 * Copyright (c)￼ 2018 netease
 */

#ifndef SRC_TOOLS_STATUS_TOOL_H_
#define SRC_TOOLS_STATUS_TOOL_H_

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <brpc/channel.h>
#include <iostream>
#include <vector>
#include <string>
#include <memory>
#include <map>
#include "proto/topology.pb.h"
#include "src/common/timeutility.h"
#include "src/mds/common/mds_define.h"
#include "src/tools/mds_client.h"
#include "src/tools/chunkserver_client.h"
#include "src/tools/namespace_tool_core.h"
#include "src/tools/copyset_check_core.h"
#include "src/tools/etcd_client.h"

using curve::mds::topology::ChunkServerStatus;
using curve::mds::topology::DiskState;
using curve::mds::topology::OnlineState;
using curve::mds::topology::PhysicalPoolInfo;
using curve::mds::topology::LogicalPoolInfo;
using curve::mds::topology::PoolIdType;
using curve::mds::topology::ChunkServerInfo;

namespace curve {
namespace tool {
struct SpaceInfo{
    uint64_t total = 0;
    uint64_t logicalUsed = 0;
    uint64_t physicalUsed = 0;
    uint64_t canBeRecycled = 0;
};

class StatusTool {
 public:
    StatusTool(std::shared_ptr<MDSClient> mdsClient,
               std::shared_ptr<EtcdClient> etcdClient,
               std::shared_ptr<NameSpaceToolCore> nameSpaceTool,
               std::shared_ptr<CopysetCheckCore> copysetCheck) :
                  mdsClient_(mdsClient), etcdClient_(etcdClient),
                  nameSpaceTool_(nameSpaceTool), copysetCheck_(copysetCheck) {}
    ~StatusTool() = default;

    /**
     *  @brief 打印help信息
     *  @param cmd：执行的命令
     *  @return 无
     */
    void PrintHelp(const std::string &cmd);

    /**
     *  @brief 执行命令
     *  @param cmd：执行的命令
     *  @return 成功返回0，失败返回-1
     */
    int RunCommand(const std::string &cmd);

 private:
    int SpaceCmd();
    int StatusCmd();
    int ChunkServerListCmd();
    int ChunkServerStatusCmd();
    int GetPoolsInCluster(std::vector<PhysicalPoolInfo>* phyPools,
                          std::vector<LogicalPoolInfo>* lgPools);
    int GetSpaceInfo(SpaceInfo* spaceInfo);
    int PrintClusterStatus();
    int PrintMdsStatus();
    int PrintEtcdStatus();
    int PrintChunkserverStatus(bool checkLeftSize = true);

    // 向mds发送RPC的client
    std::shared_ptr<MDSClient> mdsClient_;
    // NameSpace工具，用于获取recycleBin的大小
    std::shared_ptr<NameSpaceToolCore> nameSpaceTool_;
    // Copyset检查工具，用于检查集群和chunkserver的健康状态
    std::shared_ptr<CopysetCheckCore> copysetCheck_;
    // etcd client，用于调etcd API获取状态
    std::shared_ptr<EtcdClient> etcdClient_;
};
}  // namespace tool
}  // namespace curve

#endif  // SRC_TOOLS_STATUS_TOOL_H_
