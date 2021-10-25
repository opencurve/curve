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
 * Created Date: 18-8-27
 * Author: wudemiao
 */

#ifndef SRC_TOOLS_CURVE_CLI_H_
#define SRC_TOOLS_CURVE_CLI_H_

#include <gflags/gflags.h>
#include <butil/string_splitter.h>
#include <braft/cli.h>
#include <braft/configuration.h>

#include <map>
#include <string>
#include <iostream>
#include <memory>

#include "include/chunkserver/chunkserver_common.h"
#include "src/chunkserver/copyset_node.h"
#include "src/chunkserver/cli2.h"
#include "src/tools/curve_tool.h"
#include "src/tools/curve_tool_define.h"
#include "src/tools/mds_client.h"
#include "proto/copyset.pb.h"

namespace curve {
namespace tool {

using ::curve::chunkserver::LogicPoolID;
using ::curve::chunkserver::CopysetID;
using ::curve::chunkserver::CopysetRequest;
using ::curve::chunkserver::CopysetResponse;
using ::curve::chunkserver::CopysetService_Stub;
using ::curve::chunkserver::COPYSET_OP_STATUS::COPYSET_OP_STATUS_SUCCESS;
using ::curve::chunkserver::COPYSET_OP_STATUS::COPYSET_OP_STATUS_FAILURE_UNKNOWN;  // NOLINT

class CurveCli : public CurveTool {
 public:
    explicit CurveCli(std::shared_ptr<MDSClient> mdsClient) :
                                       mdsClient_(mdsClient) {}

    /**
     *  @brief 初始化mds client
     *  @return 成功返回0，失败返回-1
     */
    int Init();

    /**
     *  @brief 打印help信息
     *  @param 无
     *  @return 无
     */
    void PrintHelp(const std::string &cmd) override;

    /**
     *  @brief 执行命令
     *  @param cmd：执行的命令
     *  @return 成功返回0，失败返回-1
     */
    int RunCommand(const std::string &cmd) override;

    /**
     *  @brief 返回是否支持该命令
     *  @param command：执行的命令
     *  @return true / false
     */
    static bool SupportCommand(const std::string& command);

 private:
    /**
     * @brief Delete broken copyset
     * @param[in] peerId chunkserver peer (ip, port)
     * @param[in] poolId logical pool id
     * @param[in] copysetId copyset id
     * @return butil::Status (code, err_msg)
     */
    butil::Status DeleteBrokenCopyset(braft::PeerId peerId,
                                      const LogicPoolID& poolId,
                                      const CopysetID& copysetId);

    /**
     *  @brief 删除peer
     *  @param 无
     *  @return 成功返回0，失败返回-1
     */
    int RemovePeer();

    /**
     *  @brief 转移leader
     *  @param 无
     *  @return 成功返回0，失败返回-1
     */
    int TransferLeader();

    /**
     *  @brief 触发打快照
     *  @param 无
     *  @return 成功返回0，失败返回-1
     */
    int DoSnapshot();

    /**
     *  @brief 触发打快照
     *  @param lgPoolId 逻辑池id
     *  @param copysetId 复制组id
     *  @param peer 复制组成员
     *  @return 成功返回0，失败返回-1
     */
    int DoSnapshot(uint32_t lgPoolId, uint32_t copysetId,
                   const curve::common::Peer& peer);

    /**
     *  @brief 给集群中全部copyset node触发打快照
     *  @param 无
     *  @return 成功返回0，失败返回-1
     */
    int DoSnapshotAll();

    /**
     *  @brief 重置配置组成员，目前只支持reset成一个成员
     *  @param 无
     *  @return 成功返回0，失败返回-1
     */
    int ResetPeer();

    std::shared_ptr<MDSClient> mdsClient_;
};
}  // namespace tool
}  // namespace curve

#endif  // SRC_TOOLS_CURVE_CLI_H_
