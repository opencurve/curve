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
 * Created Date: 2021-10-19
 * Author: chengyi01
 */
#include "curvefs/src/tools/query/curvefs_metaserver_query.h"

DECLARE_string(metaserverId);
DECLARE_string(metaserverAddr);
DECLARE_string(mdsAddr);

namespace curvefs {
namespace tools {
namespace query {

void MetaserverQueryTool::PrintHelp() {
    CurvefsToolRpc::PrintHelp();
    std::cout << " -metaserverId=" << FLAGS_metaserverId
              << "(matter)|-metaserverAddr=" << FLAGS_metaserverAddr
              << " [-mdsAddr=" << FLAGS_mdsAddr << "]";
    std::cout << std::endl;
}

int MetaserverQueryTool::Init() {
    if (CurvefsToolRpc::Init() != 0) {
        return -1;
    }

    curve::common::SplitString(FLAGS_mdsAddr, ",", &hostsAddr_);

    std::vector<std::string> metaserverIdVec;
    std::vector<std::string> metaserverAddrVec;
    google::CommandLineFlagInfo info;
    if (CheckMetaserverIdDefault(&info) && !CheckMetaserverAddrDefault(&info)) {
        // only use mateserverAddr in this case
        curve::common::SplitString(FLAGS_metaserverAddr, ",",
                                   &metaserverAddrVec);
    } else {
        curve::common::SplitString(FLAGS_metaserverId, ",", &metaserverIdVec);
    }

    for (auto const& i : metaserverAddrVec) {
        std::string ip;
        uint32_t port;
        if (curvefs::mds::topology::SplitAddrToIpPort(i, &ip, &port)) {
            curvefs::mds::topology::GetMetaServerInfoRequest request;
            request.set_hostip(ip);
            request.set_port(port);
            AddRequest(request);
        } else {
            std::cerr << "metaserverAddr:" << i
                      << " is invalid, please check it." << std::endl;
        }
    }

    for (auto const& i : metaserverIdVec) {
        curvefs::mds::topology::GetMetaServerInfoRequest request;
        request.set_metaserverid(std::stoul(i));
        AddRequest(request);
    }

    service_stub_func_ =
        std::bind(&curvefs::mds::topology::TopologyService_Stub::GetMetaServer,
                  service_stub_.get(), std::placeholders::_1,
                  std::placeholders::_2, std::placeholders::_3, nullptr);
    return 0;
}

void MetaserverQueryTool::AddUpdateFlags() {
    AddUpdateFlagsFunc(curvefs::tools::SetMdsAddr);
}

bool MetaserverQueryTool::AfterSendRequestToHost(const std::string& host) {
    if (controller_->Failed()) {
        std::cerr << "send query metaserver \n"
                  << requestQueue_.front().DebugString() << "\nto mds: " << host
                  << " failed, errorcode= " << controller_->ErrorCode()
                  << ", error text " << controller_->ErrorText() << "\n";
        return false;
    } else if (show_) {
        std::cout << response_->DebugString() << std::endl;
    }

    return true;
}

}  // namespace query
}  // namespace tools
}  // namespace curvefs
