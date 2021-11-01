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

#include "curvefs/src/tools/list/curvefs_copysetid_list.h"

DECLARE_string(metaserverAddr);

namespace curvefs {
namespace tools {
namespace list {

void FsCopysetIdListTool::PrintHelp() {
    CurvefsToolRpc::PrintHelp();
    std::cout << " [-metaserver=" << FLAGS_metaserverAddr;
    std::cout << std::endl;
}

int FsCopysetIdListTool::Init() {
    if (CurvefsToolRpc::Init() != 0) {
        return -1;
    }
    curve::common::SplitString(FLAGS_metaserverAddr, ",", &hostsAddr_);
    curvefs::metaserver::GetAllCopysetsIdRequest request;
    AddRequest(request);

    service_stub_func_ =
        std::bind(&curvefs::metaserver::MetaServerService_Stub::GetAllCopysetId,
                  service_stub_.get(), std::placeholders::_1,
                  std::placeholders::_2, std::placeholders::_3, nullptr);
    return 0;
}

void FsCopysetIdListTool::AddUpdateFlags() {
    AddUpdateFlagsFunc(SetMetaserverAddr);
}

bool FsCopysetIdListTool::AfterSendRequestToHost(const std::string& host) {
    bool ret = true;
    if (controller_->Failed()) {
        std::cerr << "get all copysetId from [ " << FLAGS_metaserverAddr
                  << "] fail, errorcode= " << controller_->ErrorCode()
                  << ", error text " << controller_->ErrorText() << std::endl;
        ret = false;
    } else if (response_->statuscode() !=
               curvefs::metaserver::MetaStatusCode::OK) {
        std::cerr << "get copysetId from [ " << FLAGS_metaserverAddr
                  << " fail, error code is " << response_->statuscode()
                  << std::endl;
        ret = false;
    } else if (show_) {
        std::cout << "copysetsId: [ ";
        for (auto const& i : response_->copysetsid()) {
            std::cout << i << " ";
        }
        std::cout << "]." << std::endl;
    }
    return ret;
}

}  // namespace list
}  // namespace tools
}  // namespace curvefs
