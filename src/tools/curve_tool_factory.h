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
 * Created Date: 2019-12-27
 * Author: charisu
 */

#ifndef SRC_TOOLS_CURVE_TOOL_FACTORY_H_
#define SRC_TOOLS_CURVE_TOOL_FACTORY_H_

#include <memory>
#include <set>
#include <string>

#include "src/tools/consistency_check.h"
#include "src/tools/copyset_check.h"
#include "src/tools/copyset_tool.h"
#include "src/tools/curve_cli.h"
#include "src/tools/curve_tool.h"
#include "src/tools/namespace_tool.h"
#include "src/tools/schedule_tool.h"
#include "src/tools/status_tool.h"

namespace curve {
namespace tool {

class CurveToolFactory {
 public:
    /**
     * @brief Retrieve the CurveTool object based on the input command
     * @param command The name of the command to be executed
     * @return CurveTool instance
     */
    static std::shared_ptr<CurveTool> GenerateCurveTool(
        const std::string& command);

 private:
    /**
     * @brief Get StatusTool instance
     */
    static std::shared_ptr<StatusTool> GenerateStatusTool();

    /**
     * @brief Get NameSpaceTool instance
     */
    static std::shared_ptr<NameSpaceTool> GenerateNameSpaceTool();

    /**
     * @brief Get ConsistencyCheck instance
     */
    static std::shared_ptr<ConsistencyCheck> GenerateConsistencyCheck();

    /**
     * @brief Get CurveCli instance
     */
    static std::shared_ptr<CurveCli> GenerateCurveCli();

    /**
     * @brief Get CopysetCheck instance
     */
    static std::shared_ptr<CopysetCheck> GenerateCopysetCheck();

    /**
     * @brief to obtain a ScheduleTool instance
     */
    static std::shared_ptr<ScheduleTool> GenerateScheduleTool();

    static std::shared_ptr<CopysetTool> GenerateCopysetTool();
};
}  // namespace tool
}  // namespace curve

#endif  // SRC_TOOLS_CURVE_TOOL_FACTORY_H_
