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

#include "curvefs/src/tools/version/curvefs_version_tool.h"

namespace curvefs {
namespace tools {
namespace version {

int VersionTool::Init() {
    return 0;
}

void VersionTool::PrintHelp() {
    CurvefsTool::PrintHelp();
    std::cout << std::endl;
}

int VersionTool::RunCommand() {
    std::cout <<
#ifdef CURVEVERSION
#define STR(val) #val
#define XSTR(val) STR(val)
        std::string(XSTR(CURVEVERSION));
#else
        std::string("unknown");
#endif
    std::cout << std::endl;
    return 0;
}

}  // namespace version
}  // namespace tools
}  // namespace curvefs
