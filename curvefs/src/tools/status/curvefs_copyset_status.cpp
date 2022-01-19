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

#include "curvefs/src/tools/status/curvefs_copyset_status.h"

DECLARE_string(mdsAddr);

namespace curvefs {
namespace tools {
namespace status {

int CopysetStatusTool::Init() {
    copyInfoListTool_ = std::make_shared<list::CopysetInfoListTool>("", false);
    copyInfoListTool_->Init();
    return 0;
}

void CopysetStatusTool::PrintHelp() {
    CurvefsTool::PrintHelp();
    std::cout << " [-mdsAddr=" << FLAGS_mdsAddr << "]";
    std::cout << std::endl;
}

int CopysetStatusTool::RunCommand() {
    copyInfoListTool_->RunCommand();
    auto response = copyInfoListTool_->GetResponse();
    std::map<uint64_t,
             std::vector<curvefs::metaserver::copyset::CopysetStatusResponse>>
        key2Status;
    copyset::CopysetInfo2CopysetStatus(*response.get(), &key2Status);

    std::map<uint64_t, std::vector<curvefs::mds::topology::CopysetValue>>
        key2Info;

    copyset::Response2CopysetInfo(*response.get(), &key2Info);

    bool isHealth = true;
    for (auto const& i : key2Info) {
        if (copyset::checkCopysetHelthy(i.second, key2Status[i.first]) !=
            copyset::CheckResult::kHealthy) {
            isHealth = false;
            break;
        }
    }
    int ret = 0;
    if (show_) {
        if (isHealth) {
            std::cout << "all copyset is healthy." << std::endl;
        } else {
            std::cout << "copysets is unhealthy." << std::endl;
            ret = -1;
        }
        for (auto const& i : key2Info) {
            std::cout << "copyset[" << i.first << "]:\n-info:\n";
            for (auto const& j : i.second) {
                std::cout << j.ShortDebugString() << std::endl;
            }
            std::cout << "-status:\n";
            for (auto const& j : key2Status[i.first]) {
                std::cout << j.ShortDebugString() << std::endl;
            }
        }
    }

    return ret;
}

}  // namespace status
}  // namespace tools
}  // namespace curvefs
