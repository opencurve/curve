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
 * Created Date: 2021-10-22
 * Author: chengyi01
 */

#include "curvefs/src/tools/usage/curvefs_metadata_usage_tool.h"

DECLARE_string(mdsAddr);

namespace curvefs {
namespace tools {
namespace usage {

void MatedataUsageTool::PrintHelp() {
    CurvefsToolRpc::PrintHelp();
    std::cout << " [-mdsAddr=" << FLAGS_mdsAddr << "]";
    std::cout << std::endl;
}

void MatedataUsageTool::AddUpdateFlags() {
    AddUpdateFlagsFunc(curvefs::tools::SetMdsAddr);
}

bool MatedataUsageTool::AfterSendRequestToHost(const std::string& host) {
    bool ret = true;
    if (controller_->Failed()) {
        errorOutput_ << "get metadata usage from mds: " << host
                     << " failed, errorcode= " << controller_->ErrorCode()
                     << ", error text: " << controller_->ErrorText()
                     << std::endl;
        ret = false;
    } else {
        uint64_t total = 0;
        uint64_t used = 0;
        for (auto const& i : response_->metadatausages()) {
            auto totalTmp = i.total();
            total += totalTmp;
            auto usedTmp = i.used();
            used += usedTmp;
            std::cout << "metaserver[" << i.metaserveraddr()
                      << "] usage: total: " << ToReadableByte(totalTmp)
                      << " used: " << ToReadableByte(usedTmp)
                      << " left: " << ToReadableByte(totalTmp - usedTmp)
                      << std::endl;
        }
        std::cout << "all cluster usage: total: " << ToReadableByte(total)
                  << " used: " << ToReadableByte(used)
                  << " left: " << ToReadableByte(total - used) << std::endl;
    }

    return ret;
}

int MatedataUsageTool::Init() {
    if (CurvefsToolRpc::Init() != 0) {
        return -1;
    }
    curve::common::SplitString(FLAGS_mdsAddr, ",", &hostsAddr_);
    service_stub_func_ = std::bind(
        &curvefs::mds::topology::TopologyService_Stub::StatMetadataUsage,
        service_stub_.get(), std::placeholders::_1, std::placeholders::_2,
        std::placeholders::_3, nullptr);

    curvefs::mds::topology::StatMetadataUsageRequest request;
    AddRequest(request);
    return 0;
}

}  // namespace usage
}  // namespace tools
}  // namespace curvefs
