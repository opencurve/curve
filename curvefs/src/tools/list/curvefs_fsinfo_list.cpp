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

#include "curvefs/src/tools/list/curvefs_fsinfo_list.h"

DECLARE_string(mdsAddr);
DECLARE_uint32(rpcTimeoutMs);

namespace curvefs {
namespace tools {
namespace list {

void FsInfoListTool::PrintHelp() {
    CurvefsToolRpc::PrintHelp();
    std::cout << " [-mdsAddr=" << FLAGS_mdsAddr << "]"
              << " [-rpcTimeoutMs=" << FLAGS_rpcTimeoutMs << "]";
    std::cout << std::endl;
}

void FsInfoListTool::AddUpdateFlags() {
    AddUpdateFlagsFunc(curvefs::tools::SetMdsAddr);
    AddUpdateFlagsFunc(curvefs::tools::SetRpcTimeoutMs);
}

int FsInfoListTool::Init() {
    if (CurvefsToolRpc::Init() != 0) {
        return -1;
    }

    curve::common::SplitString(FLAGS_mdsAddr, ",", &hostsAddr_);

    service_stub_func_ =
        std::bind(&curvefs::mds::MdsService_Stub::ListClusterFsInfo,
                  service_stub_.get(), std::placeholders::_1,
                  std::placeholders::_2, std::placeholders::_3, nullptr);

    curvefs::mds::ListClusterFsInfoRequest request;
    AddRequest(request);

    controller_->set_timeout_ms(FLAGS_rpcTimeoutMs);

    return 0;
}

bool FsInfoListTool::AfterSendRequestToHost(const std::string& host) {
    bool ret = false;
    if (controller_->Failed()) {
        std::cerr << "get fsinfo from mds: " << host
                  << " failed, errorcode= " << controller_->ErrorCode()
                  << ", error text " << controller_->ErrorText() << "\n";
    } else if (show_) {
        for (auto const& i : response_->fsinfo()) {
            std::cout << i.DebugString() << std::endl;
        }

        ret = true;
    }
    return ret;
}

}  // namespace list
}  // namespace tools
}  // namespace curvefs
