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

#include "curvefs/src/tools/space/curvefs_metadata_usage_tool.h"

DECLARE_string(mdsAddr);

namespace curvefs {
namespace tools {
namespace space {

void MatedataUsageTool::PrintHelp() {
    CurvefsToolRpc::PrintHelp();
    std::cout << " -mdsAddr=" << FLAGS_mdsAddr;
    std::cout << std::endl;
}

void MatedataUsageTool::AddUpdateFlagsFuncs() {
    AddUpdateFlagsFunc(curvefs::tools::SetMdsAddr);
}

bool MatedataUsageTool::AfterSendRequestToService(const std::string& host) {
    bool ret = true;
    if (controller_->Failed() ||
        response_->statuscode() != curvefs::mds::FSStatusCode::OK) {
        // connect error or mds internal error
        std::cerr << "get metadata usage from mds: " << host
                  << " failed, errorcode= " << controller_->ErrorCode()
                  << ", error text: " << controller_->ErrorText() << std::endl;
        ret = false;
    } else {
        std::cout << "total: " << ByteToStringByMagnitude(response_->total())
                  << std::endl
                  << "used: " << ByteToStringByMagnitude(response_->used())
                  << std::endl
                  << "left: " << ByteToStringByMagnitude(response_->left())
                  << std::endl;
    }

    return ret;
}

int MatedataUsageTool::Init() {
    CurvefsToolRpc::Init();

    curve::common::SplitString(FLAGS_mdsAddr, ",", &hostsAddressStr_);

    service_stub_func_ =
        std::bind(&curvefs::mds::MdsService_Stub::StatMetadataUsage,
                  service_stub_.get(), std::placeholders::_1,
                  std::placeholders::_2, std::placeholders::_3, nullptr);
    return 0;
}
}  // namespace space
}  // namespace tools
}  // namespace curvefs
