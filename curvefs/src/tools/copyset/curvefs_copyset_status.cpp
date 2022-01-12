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

#include "curvefs/src/tools/copyset/curvefs_copyset_status.h"

DECLARE_string(metaserverAddr);
DECLARE_string(poolId);
DECLARE_string(copysetId);

namespace curvefs {
namespace tools {
namespace copyset {

void GetCopysetStatusTool::PrintHelp() {
    CurvefsToolRpc::PrintHelp();
    std::cout << " -copysetId=" << FLAGS_copysetId
              << " -poolId=" << FLAGS_poolId
              << " [-metaserverAddr=" << FLAGS_metaserverAddr << "]";
    std::cout << std::endl;
}

int GetCopysetStatusTool::Init() {
    if (CurvefsToolRpc::Init() != 0) {
        return -1;
    }

    curve::common::SplitString(FLAGS_metaserverAddr, ",", &hostsAddr_);

    std::vector<std::string> copysetsId;
    curve::common::SplitString(FLAGS_copysetId, ",", &copysetsId);
    std::vector<std::string> poolsId;
    curve::common::SplitString(FLAGS_poolId, ",", &poolsId);
    if (copysetsId.size() != poolsId.size()) {
        std::cerr << "copysets not match pools." << std::endl;
        return -1;
    }
    curvefs::metaserver::copyset::CopysetsStatusRequest request;
    for (size_t i = 0; i < poolsId.size(); ++i) {
        auto copysetKey = request.add_copysets();
        auto poolId = std::stoul(poolsId[i]);
        auto copysetId = std::stoul(copysetsId[i]);
        copysetKey->set_poolid(poolId);
        copysetKey->set_copysetid(copysetId);
        key_.push_back((poolId << 32) | copysetId);
    }
    AddRequest(request);

    service_stub_func_ = std::bind(
        &curvefs::metaserver::copyset::CopysetService_Stub::GetCopysetsStatus,
        service_stub_.get(), std::placeholders::_1, std::placeholders::_2,
        std::placeholders::_3, nullptr);
    return 0;
}

int GetCopysetStatusTool::RunCommand() {
    return CurvefsToolRpc::RunCommand();
}

bool GetCopysetStatusTool::AfterSendRequestToHost(const std::string& host) {
    bool ret = true;
    if (controller_->Failed()) {
        errorOutput_ << "get copyset status from metaserver: " << host
                     << " failed, errorcode= " << controller_->ErrorCode()
                     << ", error text " << controller_->ErrorText() << "\n";
        ret = false;
    } else {
        auto copysetsStatus = response_->status();
        for (size_t i = 0; i < key_.size(); i++) {
            key2CopysetStatus_[key_[i]].push_back(
                copysetsStatus[i].copysetstatus());
        }
    }
    if (show_) {
        for (auto const& i : key2CopysetStatus_) {
            std::cout << "copyset[" << i.first << "]:" << std::endl;
            for (auto const& j : i.second) {
                std::cout << j.DebugString() << std::endl;
            }
        }
    }
    return ret;
}

void GetCopysetStatusTool::AddUpdateFlags() {
    AddUpdateFlagsFunc(curvefs::tools::SetMetaserverAddr);
}

bool GetCopysetStatusTool::CheckRequiredFlagDefault() {
    google::CommandLineFlagInfo info;
    if (CheckPoolIdDefault(&info) && CheckCopysetIdDefault(&info)) {
        std::cerr << "no -poolId=*,* -copysetId=*,* , please use -example!"
                  << std::endl;
        return true;
    }
    return false;
}

}  // namespace copyset
}  // namespace tools
}  // namespace curvefs
