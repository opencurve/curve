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
 * Created Date: 2021-10-28
 * Author: chengyi01
 */
#ifndef CURVEFS_SRC_TOOLS_STATUS_CURVEFS_MDS_STATUS_H_
#define CURVEFS_SRC_TOOLS_STATUS_CURVEFS_MDS_STATUS_H_

#include <map>
#include <string>
#include <vector>

#include "curvefs/src/tools/curvefs_tool_define.h"
#include "curvefs/src/tools/status/curvefs_status_base_tool.h"
#include "src/common/string_util.h"

namespace curvefs {
namespace tools {
namespace status {

class MdsStatusTool : public StatusBaseTool {
 public:
    explicit MdsStatusTool(const std::string& cmd = kMdsStatusCmd,
                           const std::string& hostType = kHostTypeMds)
        : StatusBaseTool(cmd, hostType) {}
    void PrintHelp() override;
    int Init() override;

 protected:
    /**
     * @brief
     *
     * @details
     * Since the main port of non-main mds does not work,
     * all dummy ports of mds provide services
     * so use dummy port when querying mds status
     */
    std::map<std::string, std::string> dummy2MainAddr_;

    void AfterGetMetric(const std::string& hostAddr, const std::string& subUri,
                        const std::string& value,
                        const MetricStatusCode& statusCode) override;
    void InitHostsAddr() override;
    void AddUpdateFlags() override;
};

}  // namespace status
}  // namespace tools
}  // namespace curvefs

#endif  // CURVEFS_SRC_TOOLS_STATUS_CURVEFS_MDS_STATUS_H_
