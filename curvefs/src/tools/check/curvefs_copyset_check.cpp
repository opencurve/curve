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
 * Created Date: 2021-10-31
 * Author: chengyi01
 */

#include "curvefs/src/tools/check/curvefs_copyset_check.h"

#include "curvefs/src/tools/copyset/curvefs_copyset_base_tool.h"

DECLARE_string(mdsAddr);
DECLARE_string(poolId);
DECLARE_string(copysetId);
DECLARE_bool(detail);

namespace curvefs {
namespace tools {
namespace check {

void CopysetCheckTool::PrintHelp() {
    CurvefsTool::PrintHelp();
    std::cout << " -copysetId=" << FLAGS_copysetId
              << " -poolId=" << FLAGS_poolId << " [-mdsAddr=" << FLAGS_mdsAddr
              << "]";
    std::cout << std::endl;
}

int CopysetCheckTool::Init() {
    FLAGS_detail = true;  // try to get copyset status
    queryCopysetTool_ = std::make_shared<query::CopysetQueryTool>("", false);
    return 0;
}

int CopysetCheckTool::RunCommand() {
    queryCopysetTool_->Run();
    auto key2Info = queryCopysetTool_->GetKey2Info();
    auto key2Status = queryCopysetTool_->GetKey2Status();
    auto copysetKey = queryCopysetTool_->GetKey_();
    for (auto const& key : copysetKey) {
        std::cout << "copyset[" << key << "]: is ";
        auto result =
            copyset::checkCopysetHelthy(key2Info[key], key2Status[key]);
        if (result != copyset::CheckResult::kHealthy) {
            std::cerr << "unhealthy or not exist!" << std::endl;
        } else {
            std::cout << "healthy." << std::endl;
        }
    }
    return 0;
}

}  // namespace check
}  // namespace tools
}  // namespace curvefs
