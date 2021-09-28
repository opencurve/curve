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
 * Created Date: 2021-09-14
 * Author: chengyi01
 */

#ifndef CURVEFS_SRC_TOOLS_VERSION_CURVEFS_VERSION_TOOL_H_
#define CURVEFS_SRC_TOOLS_VERSION_CURVEFS_VERSION_TOOL_H_

#include <gflags/gflags.h>

#include <iostream>
#include <memory>
#include <string>

#include "curvefs/src/tools/curvefs_tool.h"
#include "curvefs/src/tools/curvefs_tool_abstract_creator.h"
#include "curvefs/src/tools/curvefs_tool_define.h"

namespace curvefs {
namespace tools {
namespace version {

class VersionTool : public CurvefsTool {
 public:
    VersionTool()
        : CurvefsTool(std::string(kVersionCmd), std::string(kProgrameName)) {}
    VersionTool(const std::string& command, const std::string& programe)
        : CurvefsTool(command, programe) {}
    void PrintHelp() override;

 private:
    int RunCommand() override;
    int Init() override;
};

}  // namespace version
}  // namespace tools
}  // namespace curvefs

#endif  // CURVEFS_SRC_TOOLS_VERSION_CURVEFS_VERSION_TOOL_H_
