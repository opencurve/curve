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

#include "curvefs/src/tools/list/curvefs_copysetinfo_list.h"

DECLARE_string(mdsAddr);

namespace curvefs {
namespace tools {
namespace list {

void CopysetInfoListTool::PrintHelp() {
    CurvefsToolRpc::PrintHelp();
    std::cout << " [-mds=" << FLAGS_mdsAddr << "]";
    std::cout << std::endl;
}

int CopysetInfoListTool::Init() {
    if (CurvefsToolRpc::Init() != 0) {
        return -1;
    }
    curve::common::SplitString(FLAGS_mdsAddr, ",", &hostsAddr_);
    curvefs::mds::topology::ListCopysetInfoRequest request;
    AddRequest(request);

    service_stub_func_ = std::bind(
        &curvefs::mds::topology::TopologyService_Stub::ListCopysetInfo,
        service_stub_.get(), std::placeholders::_1, std::placeholders::_2,
        std::placeholders::_3, nullptr);
    return 0;
}

void CopysetInfoListTool::AddUpdateFlags() { AddUpdateFlagsFunc(SetMdsAddr); }

bool CopysetInfoListTool::AfterSendRequestToHost(const std::string &host) {
    (void)host;
    bool ret = true;
    if (controller_->Failed()) {
        errorOutput_ << "get all copysetInfo from [ " << FLAGS_mdsAddr
                     << "] fail, errorcode= " << controller_->ErrorCode()
                     << ", error text " << controller_->ErrorText()
                     << std::endl;
        ret = false;
    } else if (show_) {
        for (auto const &i : response_->copysetvalues()) {
            std::cout << "copyset["
                      << copyset::GetCopysetKey(i.copysetinfo().poolid(),
                                                i.copysetinfo().copysetid())
                      << "]:" << std::endl
                      << i.DebugString() << std::endl;
        }
        std::cout << std::endl;
    }
    return ret;
}

}  // namespace list
}  // namespace tools
}  // namespace curvefs
