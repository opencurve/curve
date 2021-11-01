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

#include "curvefs/src/tools/status/curvefs_copyset_status.h"

DECLARE_string(metaserverAddr);

namespace curvefs {
namespace tools {
namespace status {

int CopysetStatusTool::Init() {
    if (CurvefsToolRpc::Init() != 0) {
        return -1;
    }

    curve::common::SplitString(FLAGS_metaserverAddr, ",", &hostsAddr_);

    service_stub_func_ = std::bind(
        &curvefs::metaserver::copyset::CopysetService_Stub::GetCopysetsStatus,
        service_stub_.get(), std::placeholders::_1, std::placeholders::_2,
        std::placeholders::_3, nullptr);
    curvefs::metaserver::copyset::CopysetsStatusRequest request;
    AddRequest(request);
    return 0;
}

int CopysetStatusTool::RunCommand() {
    return CurvefsToolRpc::RunCommand();
}

bool CopysetStatusTool::AfterSendRequestToHost(const std::string& host) {
    bool ret = true;
    if (controller_->Failed()) {
        std::cerr << "get copyset status from metaserver: " << host
                  << " failed, errorcode= " << controller_->ErrorCode()
                  << ", error text " << controller_->ErrorText() << "\n";
        ret = false;
    } else {
        if (show_) {
            for (auto const& i : response_->status()) {
                std::cout << MetadataserverCopysetCopysetStatusResponse2Str(i)
                          << std::endl;
            }
        }
        for (auto const& i : response_->status()) {
            auto status = i.status();
            if (status == metaserver::copyset::COPYSET_OP_STATUS::
                              COPYSET_OP_STATUS_COPYSET_NOTEXIST ||
                status == metaserver::copyset::COPYSET_OP_STATUS::
                              COPYSET_OP_STATUS_PARSE_PEER_ERROR ||
                status == metaserver::copyset::COPYSET_OP_STATUS::
                              COPYSET_OP_STATUS_PEER_MISMATCH ||
                status == metaserver::copyset::COPYSET_OP_STATUS::
                              COPYSET_OP_STATUS_FAILURE_UNKNOWN) {
                // As long as there is one unhealthy and report unhealthy
                ret = false;
                break;
            }
        }
    }
    return ret;
}

void CopysetStatusTool::AddUpdateFlags() {
    AddUpdateFlagsFunc(curvefs::tools::SetMetaserverAddr);
}

}  // namespace status
}  // namespace tools
}  // namespace curvefs
