/*
 *  Copyright (c) 2022 NetEase Inc.
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
 * Created Date: 2022-09-07
 * Author: wanghai01
 */

#include "curvefs/src/tools/offline/curvefs_offline_metaserver_tool.h"

DECLARE_string(metaserverId);
DECLARE_string(mdsAddr);
DECLARE_bool(noconfirm);

namespace curvefs {
namespace tools {
namespace offline {

void OfflineMetaserverTool::PrintHelp() {
    CurvefsToolRpc::PrintHelp();
    std::cout << " -metaserverId=" << FLAGS_metaserverId
              << " [-noconfirm=" << FLAGS_noconfirm
              << "] [-mdsAddr=" << FLAGS_mdsAddr << "]";
    std::cout << std::endl;
}

int OfflineMetaserverTool::Init() {
    int ret = CurvefsToolRpc::Init();

    curvefs::mds::topology::OfflineMetaServerRequest request;
    request.set_metaserverid(std::stoul(FLAGS_metaserverId));
    AddRequest(request);
    service_stub_func_ =
        std::bind(
            &curvefs::mds::topology::TopologyService_Stub::OfflineMetaServer,
            service_stub_.get(),
            std::placeholders::_1, std::placeholders::_2,
            std::placeholders::_3, nullptr);
    return ret;
}

void OfflineMetaserverTool::InitHostsAddr() {
    curve::common::SplitString(FLAGS_mdsAddr, ",", &hostsAddr_);
}

void OfflineMetaserverTool::AddUpdateFlags() {
    AddUpdateFlagsFunc(curvefs::tools::SetMdsAddr);
}

int OfflineMetaserverTool::RunCommand() {
    if (!FLAGS_noconfirm) {
        // confirm input
        std::cout << "This command will offline metaserver ("
                  << FLAGS_metaserverId << ")!!!\n"
                  << "offline this metaserver? [Yes, offline!]: ";
        std::string confirm = "no";
        std::getline(std::cin, confirm);
        if (confirm != "Yes, offline!") {
            // input is not 'Y' or 'y'
            std::cout << "offline canceled!" << std::endl;
            return -1;
        }
    }
    return CurvefsToolRpc::RunCommand();
}

bool OfflineMetaserverTool::AfterSendRequestToHost(const std::string& host) {
    bool ret = false;
    if (controller_->Failed()) {
        errorOutput_ << "send offline metaserver request to mds: " << host
                     << " failed. errorcode= " << controller_->ErrorCode()
                     << ", error text " << controller_->ErrorText() << "\n";
    } else if (response_->statuscode() ==
               curvefs::mds::topology::TopoStatusCode::TOPO_OK) {
        std::cout << "offline metaserver ("
                  << FLAGS_metaserverId << ") success." << std::endl;
        ret = true;
    } else {
        std::cerr << "offline metaserver from mds: " << host
                  << " failed. errorcode= " << response_->statuscode()
                  << ", errorname: "
                  << mds::topology::TopoStatusCode_Name(response_->statuscode())
                  << std::endl;
    }
    return ret;
}

bool OfflineMetaserverTool::CheckRequiredFlagDefault() {
    google::CommandLineFlagInfo info;
    if (CheckMetaserverIdDefault(&info)) {
        std::cerr << "no -metaserverId=***, please use -example!" << std::endl;
        return true;
    }
    return false;
}

}  // namespace offline
}  // namespace tools
}  // namespace curvefs
