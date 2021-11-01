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
#include "curvefs/src/tools/status/curvefs_mds_status.h"

DECLARE_string(mdsAddr);

namespace curvefs {
namespace tools {
namespace status {

void MdsStatusTool::PrintHelp() {
    StatusBaseTool::PrintHelp();
    std::cout << " [-mdsAddr=" << FLAGS_mdsAddr << "]";
    std::cout << std::endl;
}

int MdsStatusTool::Init() {
    versionSubUri_ = kVersionUri;
    StatusSubUri_ = kMdsStatusUri;
    statusKey_ = kMdsStatusKey;
    int ret = StatusBaseTool::Init();
    return ret;
}

void MdsStatusTool::InitHostAddr() {
    curve::common::SplitString(FLAGS_mdsAddr, ",", &hostAddr_);
}

void MdsStatusTool::AddUpdateFlags() {
    AddUpdateFlagsFunc(curvefs::tools::SetMdsAddr);
    StatusBaseTool::AddUpdateFlags();
}

}  // namespace status
}  // namespace tools
}  // namespace curvefs
