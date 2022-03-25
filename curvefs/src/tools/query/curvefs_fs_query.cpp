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
 * Created Date: 2021-11-18
 * Author: chengyi01
 */

#include "curvefs/src/tools/query/curvefs_fs_query.h"

DECLARE_string(mdsAddr);
DECLARE_string(fsName);
DECLARE_string(fsId);

namespace curvefs {
namespace tools {
namespace query {

void FsQueryTool::PrintHelp() {
    CurvefsToolRpc::PrintHelp();
    std::cout << " -fsName=" << FLAGS_fsName << "|-fsId=" << FLAGS_fsId
              << " [-mdsAddr=" << FLAGS_mdsAddr << "]";
    std::cout << std::endl;
}

int FsQueryTool::Init() {
    if (CurvefsToolRpc::Init() != 0) {
        return -1;
    }

    curve::common::SplitString(FLAGS_mdsAddr, ",", &hostsAddr_);

    // If fsName and fsId exist at the same time, only fsId work
    google::CommandLineFlagInfo info;
    std::vector<std::string> fsIdVec;
    std::vector<std::string> fsNameVec;
    if (!CheckFsNameDefault(&info) && CheckFsIdDefault(&info)) {
        // only use fsName in this case
        curve::common::SplitString(FLAGS_fsName, ",", &fsNameVec);
    } else {
        curve::common::SplitString(FLAGS_fsId, ",", &fsIdVec);
    }

    // fsId
    for (auto const& i : fsIdVec) {
        curvefs::mds::GetFsInfoRequest request;
        request.set_fsid(std::stoul(i));
        AddRequest(request);
        requestValueVec_.push_back(i);
    }

    // fsName
    for (auto const& i : fsNameVec) {
        curvefs::mds::GetFsInfoRequest request;
        request.set_fsname(i);
        AddRequest(request);
        requestValueVec_.push_back(i);
    }

    service_stub_func_ =
        std::bind(&curvefs::mds::MdsService_Stub::GetFsInfo,
                  service_stub_.get(), std::placeholders::_1,
                  std::placeholders::_2, std::placeholders::_3, nullptr);

    return 0;
}

void FsQueryTool::AddUpdateFlags() {
    AddUpdateFlagsFunc(curvefs::tools::SetMdsAddr);
}

bool FsQueryTool::AfterSendRequestToHost(const std::string& host) {
    if (controller_->Failed()) {
        errorOutput_ << "send query fs [ ";
        for (auto const& i : requestValueVec_) {
            errorOutput_ << i << " ";
        }
        errorOutput_ << "] to mds: " << host
                     << " failed, errorcode= " << controller_->ErrorCode()
                     << ", error text " << controller_->ErrorText() << "\n";
        return false;
    } else if (show_) {
        if (response_->statuscode() == mds::FSStatusCode::OK) {
            std::cout << response_->DebugString() << std::endl;
        } else if (response_->statuscode() == mds::FSStatusCode::NOT_FOUND) {
            std::cerr << "fs not found!" << std::endl;
            return false;
        } else {
            std::cerr << response_->DebugString() << std::endl;
            return false;
        }
    }
    return true;
}

bool FsQueryTool::CheckRequiredFlagDefault() {
    google::CommandLineFlagInfo info;
    if (CheckFsNameDefault(&info) && CheckFsIdDefault(&info)) {
        std::cerr << "no -fsName=***|-fsId=***, please use --example check!"
                  << std::endl;
        return true;
    } else if (!CheckFsNameDefault(&info) && !CheckFsIdDefault(&info)) {
        std::cerr << "please use one of -fsName=***|-fsId=*** !" << std::endl;
        return true;
    }
    return false;
}

}  // namespace query
}  // namespace tools
}  // namespace curvefs
