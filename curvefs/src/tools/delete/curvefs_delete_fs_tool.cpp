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
 * Created Date: 2021-10-16
 * Author: chengyi01
 */

#include "curvefs/src/tools/delete/curvefs_delete_fs_tool.h"

DECLARE_string(fsname);
DECLARE_string(mdsAddr);
DECLARE_bool(confirm);

namespace curvefs {
namespace tools {
namespace delete_ {

void DeleteFsTool::PrintHelp() {
    CurvefsToolRpc::PrintHelp();
    std::cout << " -fsname=" << FLAGS_fsname << " -confirm=" << FLAGS_confirm
              << " [-mdsAddr=" << FLAGS_mdsAddr << "]";
    std::cout << std::endl;
}

int DeleteFsTool::Init() {
    int ret = CurvefsToolRpc::Init();

    curvefs::mds::DeleteFsRequest request;
    request.set_fsname(FLAGS_fsname);
    AddRequest(request);
    service_stub_func_ =
        std::bind(&curvefs::mds::MdsService_Stub::DeleteFs, service_stub_.get(),
                  std::placeholders::_1, std::placeholders::_2,
                  std::placeholders::_3, nullptr);
    return ret;
}

void DeleteFsTool::InitHostsAddr() {
    curve::common::SplitString(FLAGS_mdsAddr, ",", &hostsAddr_);
}

void DeleteFsTool::AddUpdateFlags() {
    AddUpdateFlagsFunc(curvefs::tools::SetMdsAddr);
}

int DeleteFsTool::RunCommand() {
    if (!FLAGS_confirm) {
        std::cerr << "please input \"-confirm\" or \"-confirm=true\""
                  << std::endl;
        return -1;
    }

    // confirm input
    for (size_t i = 0; i < checkTimes_; i++) {
        std::cout << i + 1 << ". do you really want to delete fs ("
                  << FLAGS_fsname << ") :[Ny]";
        char confirm = 'N';
        std::cin >> confirm;
        // clean up redundant output
        std::cin.clear();
        std::cin.ignore(INT_MAX, '\n');
        if (!(confirm == 'Y' || confirm == 'y')) {
            // input is not 'Y' or 'y'
            return -1;
        }
    }

    return CurvefsToolRpc::RunCommand();
}

bool DeleteFsTool::AfterSendRequestToHost(const std::string& host) {
    bool ret = false;
    if (controller_->Failed()) {
        std::cerr << "send delete fs request to mds: " << host
                  << " failed. errorcode= " << controller_->ErrorCode()
                  << ", error text " << controller_->ErrorText() << "\n";
    } else if (response_->statuscode() != curvefs::mds::FSStatusCode::OK) {
        std::cerr << "delete fs from mds: " << host
                  << " failed. errorcode= " << response_->statuscode()
                  << std::endl;
    } else {
        std::cout << "delete fs " << FLAGS_fsname << " success." << std::endl;
        ret = true;
    }
    return ret;
}

}  // namespace delete_
}  // namespace tools
}  // namespace curvefs
