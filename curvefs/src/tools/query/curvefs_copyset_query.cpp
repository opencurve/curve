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
 * Created Date: 2021-10-30
 * Author: chengyi01
 */

#include "curvefs/src/tools/query/curvefs_copyset_query.h"

DECLARE_string(copysetId);
DECLARE_string(mdsAddr);
DECLARE_bool(detail);

namespace curvefs {
namespace tools {
namespace query {

void CopysetQueryTool::PrintHelp() {
    CurvefsToolRpc::PrintHelp();
    std::cout << " -copysetId=" << FLAGS_copysetId
              << " [-mdsAddr=" << FLAGS_mdsAddr << "]";
    std::cout << std::endl;
}

void CopysetQueryTool::AddUpdateFlags() {
    AddUpdateFlagsFunc(curvefs::tools::SetMdsAddr);
}

int CopysetQueryTool::Init() {
    int ret = CurvefsToolRpc::Init();

    curve::common::SplitString(FLAGS_mdsAddr, ",", &hostsAddr_);
    std::vector<std::string> copysetsId;
    curve::common::SplitString(FLAGS_copysetId, ",", &copysetsId);
    for (const auto& i : copysetsId) {
        requestQueue_.front().add_copysetid(std::stoul(i));
    }

    service_stub_func_ =
        std::bind(&curvefs::mds::topology::TopologyService_Stub::GetCopysetInfo,
                  service_stub_.get(), std::placeholders::_1,
                  std::placeholders::_2, std::placeholders::_3, nullptr);
    return ret;
}

bool CopysetQueryTool::AfterSendRequestToHost(const std::string& host) {
    bool ret = true;
    if (controller_->Failed()) {
        std::cerr << "query copyset [ " << FLAGS_copysetId
                  << " ] from mds: " << host
                  << " failed, errorcode= " << controller_->ErrorCode()
                  << ", error text " << controller_->ErrorText() << "\n";
        ret = false;
    } else if (response_->statuscode() !=
               curvefs::mds::topology::TopoStatusCode::TOPO_OK) {
        std::cerr << "query copyset [ " << FLAGS_copysetId
                  << " ] from mds: " << host << " fail, error code is "
                  << response_->statuscode() << "\n";
        ret = false;
    } else {
        if (FLAGS_detail == false) {
            if (show_) {
                for (auto const& i : response_->copysetinfo()) {
                    std::cout << HeartbeatCopysetInfo2Str(i) << std::endl;
                }
            }
        } else {
            // check copyset status
            curvefs::metaserver::copyset::GetCopysetsStatusRequest request;
            for (auto const& i : response_->copysetinfo()) {
                curvefs::metaserver::copyset::CopysetStatusRequest tmp;
                tmp.set_poolid(i.poolid());
                tmp.set_copysetid(i.copysetid());
                tmp.set_allocated_peer(
                    new curvefs::common::Peer(i.leaderpeer()));
                *request.add_copysetsinfo() = std::move(tmp);
            }
            status::CopysetStatusTool copysetStatustool(false);
            copysetStatustool.Init(request);
            ret = copysetStatustool.RunCommand();
            if (show_) {
                for (auto const& i :
                     copysetStatustool.GetResponse()->copysetsstatus()) {
                    std::cout << i.DebugString() << std::endl;
                }
            }
        }
    }
    return ret;
}

}  // namespace query
}  // namespace tools
}  // namespace curvefs
