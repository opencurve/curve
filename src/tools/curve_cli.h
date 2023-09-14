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
     * @brief Initialize mds client
     * @return returns 0 for success, -1 for failure
     */
    int Init();

    /**
     * @brief Print help information
     * @param None
     * @return None
     */
    void PrintHelp(const std::string &cmd) override;

    /**
     * @brief Execute command
     * @param cmd: Command executed
     * @return returns 0 for success, -1 for failure
     */
    int RunCommand(const std::string &cmd) override;

    /**
     * @brief returns whether the command is supported
     * @param command: The command executed
     * @return true/false
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
     * @brief delete peer
     * @param None
     * @return returns 0 for success, -1 for failure
     */
    int RemovePeer();

    /**
     * @brief transfer leader
     * @param None
     * @return returns 0 for success, -1 for failure
     */
    int TransferLeader();

    /**
     * @brief trigger to take a snapshot
     * @param None
     * @return returns 0 for success, -1 for failure
     */
    int DoSnapshot();

    /**
     * @brief trigger to take a snapshot
     * @param lgPoolId Logical Pool ID
     * @param copysetId Copy Group ID
     * @param peer replication group members
     * @return returns 0 for success, -1 for failure
     */
    int DoSnapshot(uint32_t lgPoolId, uint32_t copysetId,
                   const curve::common::Peer& peer);

    /**
     * @brief Trigger a snapshot of all copyset nodes in the cluster
     * @param None
     * @return returns 0 for success, -1 for failure
     */
    int DoSnapshotAll();

    /**
     * @brief Reset configuration group members, currently only supports resetting to one member
     * @param None
     * @return returns 0 for success, -1 for failure
     */
    int ResetPeer();

    std::shared_ptr<MDSClient> mdsClient_;
};
}  // namespace tool
}  // namespace curve

#endif  // SRC_TOOLS_CURVE_CLI_H_
