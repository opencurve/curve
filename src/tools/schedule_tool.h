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
 * File Created: 20200108
 * Author: lixiaocui
 */

#ifndef SRC_TOOLS_SCHEDULE_TOOL_H_
#define SRC_TOOLS_SCHEDULE_TOOL_H_

#include <memory>
#include <string>

#include "src/tools/curve_tool.h"
#include "src/tools/mds_client.h"

namespace curve {
namespace tool {

using curve::mds::topology::PoolIdType;

class ScheduleTool : public CurveTool {
 public:
    explicit ScheduleTool(std::shared_ptr<MDSClient> mdsClient)
        : mdsClient_(mdsClient) {}

    /**
     * @brief returns whether the command is supported
     * @param command: The command executed
     * @return true/false
     */
    static bool SupportCommand(const std::string& command);

    /**
     * @brief Print help information
     * @param cmd: Command executed
     * @return None
     */
    void PrintHelp(const std::string& command) override;

    /**
     * @brief Execute command
     * @param cmd: Command executed
     * @return returns 0 for success, -1 for failure
     */
    int RunCommand(const std::string& command) override;

 private:
    /**
     * @brief PrintRapidLeaderSchedule Print help information for
     * rapid-leader-schdule
     */
    void PrintRapidLeaderScheduleHelp();

    void PrintSetScanStateHelp();

    /**
     * @brief DoRapidLeaderSchedule sends rpc to mds for fast transfer leader
     */
    int DoRapidLeaderSchedule();

    int DoSetScanState();

    int ScheduleOne(PoolIdType lpoolId);

    int ScheduleAll();

 private:
    std::shared_ptr<MDSClient> mdsClient_;
};
}  // namespace tool
}  // namespace curve

#endif  // SRC_TOOLS_SCHEDULE_TOOL_H_
