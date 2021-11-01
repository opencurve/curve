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
 * Created Date: 2021-09-13
 * Author: chengyi
 */

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <iostream>
#include <memory>

#include "curvefs/src/tools/curvefs_tool.h"
#include "curvefs/src/tools/curvefs_tool_define.h"
#include "curvefs/src/tools/curvefs_tool_factory.h"

DECLARE_string(mdsAddr);
DECLARE_bool(example);
DECLARE_string(confPath);

namespace brpc {
DECLARE_int32(health_check_interval);
}

int main(int argc, char** argv) {
    google::SetUsageMessage(curvefs::tools::kHelpStr);
    google::ParseCommandLineFlags(&argc, &argv, true);
    google::InitGoogleLogging(argv[0]);

    if (argc < 2) {
        std::cout << curvefs::tools::kHelpStr << std::endl;
        return -1;
    }

    std::string command = argv[1];

    // Turn off the health check,
    // otherwise it does not make sense to try again when Not Connect to
    brpc::FLAGS_health_check_interval = -1;
    curvefs::tools::CurvefsToolFactory curveToolFactory;
    std::shared_ptr<curvefs::tools::CurvefsTool> curveTool =
        curveToolFactory.GenerateCurvefsTool(command);

    if (curveTool == nullptr) {
        std::cout << curvefs::tools::kHelpStr << std::endl;
        return -1;
    }
    if (FLAGS_example) {
        curveTool->PrintHelp();
        return 0;
    }

    return curveTool->Run();
}
