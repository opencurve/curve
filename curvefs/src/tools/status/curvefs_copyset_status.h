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
 * Created Date: 2021-11-23
 * Author: chengyi01
 */
#ifndef CURVEFS_SRC_TOOLS_STATUS_CURVEFS_COPYSET_STATUS_H_
#define CURVEFS_SRC_TOOLS_STATUS_CURVEFS_COPYSET_STATUS_H_

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "curvefs/src/tools/copyset/curvefs_copyset_base_tool.h"
#include "curvefs/src/tools/curvefs_tool.h"
#include "curvefs/src/tools/curvefs_tool_define.h"
#include "curvefs/src/tools/list/curvefs_copysetinfo_list.h"

namespace curvefs {
namespace tools {
namespace status {

class CopysetStatusTool : public CurvefsTool {
 public:
    explicit CopysetStatusTool(const std::string& command = kCopysetStatusCmd)
        : CurvefsTool(command) {}
    void PrintHelp() override;

    int RunCommand() override;
    int Init() override;

 protected:
    std::shared_ptr<list::CopysetInfoListTool> copyInfoListTool_;
};
}  // namespace status
}  // namespace tools
}  // namespace curvefs
#endif  // CURVEFS_SRC_TOOLS_STATUS_CURVEFS_COPYSET_STATUS_H_
