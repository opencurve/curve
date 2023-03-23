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
 * Created Date: 2021-10-26
 * Author: chengyi01
 */
#ifndef CURVEFS_SRC_TOOLS_STATUS_CURVEFS_STATUS_BASE_TOOL_H_
#define CURVEFS_SRC_TOOLS_STATUS_CURVEFS_STATUS_BASE_TOOL_H_

#include <set>
#include <string>
#include <vector>

#include "curvefs/src/tools/curvefs_tool.h"
#include "curvefs/src/tools/curvefs_tool_define.h"
#include "curvefs/src/tools/curvefs_tool_metric.h"

namespace curvefs {
namespace tools {
namespace status {

/**
 * @brief
 *
 * @tparam hostType will defined in curvefs/src/tools/curvefs_tool_define.h
 * @details
 */
class StatusBaseTool : public CurvefsToolMetric {
 public:
    explicit StatusBaseTool(
        const std::string& cmd, const std::string& hostType,
        const std::string& hostLeaderValue = kHostLeaderValue,
        const std::string& hostFollowerValue = kHostFollowerValue)
        : CurvefsToolMetric(cmd),
          hostType_(hostType),
          hostLeaderValue_(hostLeaderValue),
          hostStandbyValue_(hostFollowerValue) {}
    virtual ~StatusBaseTool() {}

 protected:
    void AfterGetMetric(const std::string& hostAddr, const std::string& subUri,
                        const std::string& value,
                        const MetricStatusCode& statusCode);
    int Init() override;
    virtual int ProcessMetrics();

 protected:
    std::vector<std::string> hostsAddr_;
    std::set<std::string> standbyHost_;
    std::set<std::string> errorHosts_;  // not leader, not standby
    std::set<std::string> offlineHosts_;
    std::set<std::string> leaderHosts_;
    std::set<std::string> onlineHosts_;
    std::string version_;
    std::string hostType_;
    std::string versionSubUri_ = "";
    std::string statusSubUri_ = "";
    std::string versionKey_;
    std::string statusKey_;
    std::string hostLeaderValue_;
    std::string hostStandbyValue_;
};

}  // namespace status
}  // namespace tools
}  // namespace curvefs

#endif  // CURVEFS_SRC_TOOLS_STATUS_CURVEFS_STATUS_BASE_TOOL_H_
