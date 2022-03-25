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

DECLARE_string(fsName);
DECLARE_string(mdsAddr);
DECLARE_bool(noconfirm);

namespace curvefs {
namespace tools {
namespace delete_ {

void DeleteFsTool::PrintHelp() {
    CurvefsToolRpc::PrintHelp();
    std::cout << " -fsName=" << FLAGS_fsName
              << " [-noconfirm=" << FLAGS_noconfirm
              << "] [-mdsAddr=" << FLAGS_mdsAddr << "]";
    std::cout << std::endl;
}

int DeleteFsTool::Init() {
    int ret = CurvefsToolRpc::Init();

    curvefs::mds::DeleteFsRequest request;
    request.set_fsname(FLAGS_fsName);
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
    if (!FLAGS_noconfirm) {
        // confirm input
        std::cout << "This command will delete fs (" << FLAGS_fsName
                  << ") and is not recoverable!!!\n"
                  << "Do you really want to delete this fs? [Yes, delete!]: ";
        std::string confirm = "no";
        std::getline(std::cin, confirm);
        if (confirm != "Yes, delete!") {
            // input is not 'Y' or 'y'
            std::cout << "delete canceled!" << std::endl;
            return -1;
        }
    }
    return CurvefsToolRpc::RunCommand();
}

bool DeleteFsTool::AfterSendRequestToHost(const std::string& host) {
    bool ret = false;
    if (controller_->Failed()) {
        errorOutput_ << "send delete fs request to mds: " << host
                     << " failed. errorcode= " << controller_->ErrorCode()
                     << ", error text " << controller_->ErrorText() << "\n";
    } else if (response_->statuscode() ==
               curvefs::mds::FSStatusCode::NOT_FOUND) {
        std::cerr << "delete fs failed, fs not found!" << std::endl;
    } else if (response_->statuscode() == curvefs::mds::FSStatusCode::OK) {
        std::cout << "delete fs (" << FLAGS_fsName << ") success." << std::endl;
        ret = true;
    } else {
        std::cerr << "delete fs from mds: " << host
                  << " failed. errorcode= " << response_->statuscode()
                  << ", errorname: "
                  << mds::FSStatusCode_Name(response_->statuscode())
                  << std::endl;
    }
    return ret;
}

bool DeleteFsTool::CheckRequiredFlagDefault() {
    google::CommandLineFlagInfo info;
    if (CheckFsNameDefault(&info)) {
        std::cerr << "no -fsName=***, please use -example!" << std::endl;
        return true;
    }
    return false;
}

}  // namespace delete_
}  // namespace tools
}  // namespace curvefs
