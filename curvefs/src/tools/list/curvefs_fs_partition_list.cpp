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

#include "curvefs/src/tools/list/curvefs_fs_partition_list.h"

DECLARE_string(fsId);
DECLARE_string(mdsAddr);

namespace curvefs {
namespace tools {
namespace list {

void FsPartitionListTool::PrintHelp() {
    CurvefsToolRpc::PrintHelp();
    std::cout << " -fsId=" << FLAGS_fsId << " [-mdsAddr=" << FLAGS_mdsAddr
              << "]";
    std::cout << std::endl;
}

void FsPartitionListTool::AddUpdateFlags() {
    AddUpdateFlagsFunc(curvefs::tools::SetMdsAddr);
}

int FsPartitionListTool::Init() {
    if (CurvefsToolRpc::Init() != 0) {
        return -1;
    }
    curve::common::SplitString(FLAGS_mdsAddr, ",", &hostsAddr_);
    std::vector<std::string> fsIds;
    curve::common::SplitString(FLAGS_fsId, ",", &fsIds);
    curvefs::mds::topology::ListPartitionsRequest request;
    for (auto const& i : fsIds) {
        request.add_fsid(std::stoul(i));
    }
    AddRequest(request);

    service_stub_func_ =
        std::bind(&curvefs::mds::topology::TopologyService_Stub::ListPartitions,
                  service_stub_.get(), std::placeholders::_1,
                  std::placeholders::_2, std::placeholders::_3, nullptr);
    return 0;
}

bool FsPartitionListTool::AfterSendRequestToHost(const std::string& host) {
    bool ret = true;
    if (controller_->Failed()) {
        std::cerr << "get fs [ " << FLAGS_fsId
                  << " ] partition from mds: " << host
                  << " failed, errorcode= " << controller_->ErrorCode()
                  << ", error text " << controller_->ErrorText() << std::endl;
        ret = false;
    } else if (response_->statuscode() !=
               curvefs::mds::topology::TopoStatusCode::TOPO_OK) {
        std::cerr << "get fs [ " << FLAGS_fsId
                  << " ] partition from mds: " << host
                  << " fail, error code is " << response_->statuscode()
                  << std::endl;
        ret = false;
    } else if (show_) {
        for (auto const& i : requestQueue_.front().fsid()) {
            auto fsid2part = response_->fsid2partitionlist();
            auto iPosition = fsid2part.find(i);
            if (iPosition != fsid2part.end()) {
                std::cout << "fsId: " << i << " partitionlist: [ ";
                for (auto const& j : fsid2part[i].partitioninfolist()) {
                    std::cout << j.DebugString() << " ";
                }
                std::cout << "]" << std::endl;
            } else {
                std::cerr << "fsId: " << i << "not found partition."
                          << std::endl;
            }
        }
    }
    return ret;
}

}  // namespace list
}  // namespace tools
}  // namespace curvefs
