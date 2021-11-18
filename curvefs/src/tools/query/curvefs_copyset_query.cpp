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
DECLARE_string(poolId);
DECLARE_string(mdsAddr);
DECLARE_bool(detail);

// used for CopysetStatusTool
DECLARE_string(metaserverAddr);

namespace curvefs {
namespace tools {
namespace query {

void CopysetQueryTool::PrintHelp() {
    CurvefsToolRpc::PrintHelp();
    std::cout << " -copysetsId=" << FLAGS_copysetId
              << " -poolsId=" << FLAGS_poolId << " [-mdsAddr=" << FLAGS_mdsAddr
              << "]"
              << " [-detail=" << FLAGS_detail << "]";
    std::cout << std::endl;
}

void CopysetQueryTool::AddUpdateFlags() {
    AddUpdateFlagsFunc(curvefs::tools::SetMdsAddr);
}

int CopysetQueryTool::Init() {
    if (CurvefsToolRpc::Init() != 0) {
        return -1;
    }

    curve::common::SplitString(FLAGS_mdsAddr, ",", &hostsAddr_);
    std::vector<std::string> copysetsId;
    curve::common::SplitString(FLAGS_copysetId, ",", &copysetsId);
    std::vector<std::string> poolsId;
    curve::common::SplitString(FLAGS_poolId, ",", &poolsId);
    if (copysetsId.size() != poolsId.size() || poolsId.empty()) {
        std::cerr << "copysets not match pools." << std::endl;
        return -1;
    }
    curvefs::mds::topology::GetCopysetsInfoRequest request;
    for (unsigned i = 0; i < poolsId.size(); ++i) {
        curvefs::mds::topology::GetCopysetInfoRequest copyset;
        copyset.set_poolid(std::stoul(poolsId[i]));
        copyset.set_copysetid(std::stoul(copysetsId[i]));
        *request.add_copysets() = copyset;
    }
    AddRequest(request);

    service_stub_func_ = std::bind(
        &curvefs::mds::topology::TopologyService_Stub::GetCopysetsInfo,
        service_stub_.get(), std::placeholders::_1, std::placeholders::_2,
        std::placeholders::_3, nullptr);
    return 0;
}

bool CopysetQueryTool::AfterSendRequestToHost(const std::string& host) {
    bool ret = true;
    if (controller_->Failed()) {
        std::cerr << "query copysets [ " << FLAGS_copysetId
                  << " ] from mds: " << host
                  << " failed, errorcode= " << controller_->ErrorCode()
                  << ", error text " << controller_->ErrorText() << "\n";
        ret = false;
    } else {
        // copysetId and copysetstatus get from metaserver
        if (FLAGS_detail) {
            using StatusRequestType =
                curvefs::metaserver::copyset::CopysetsStatusRequest;
            // metaserver ip and send to metaserver request
            std::map<std::string, std::queue<StatusRequestType>> ip2requests;
            for (auto const& i : response_->copysetsinfo()) {
                using tmpType =
                    curvefs::metaserver::copyset::CopysetStatusRequest;

                if (i.statuscode() !=
                    curvefs::mds::topology::TopoStatusCode::TOPO_OK) {
                    // some error in copyset
                    std::cout << "query copyset [ " << i.DebugString()
                              << " ] from mds error." << std::endl;
                    continue;
                }
                avaliableCopysetId.insert(i.copysetinfo().copysetid());

                tmpType tmp;
                tmp.set_copysetid(i.copysetinfo().copysetid());
                tmp.set_poolid(i.copysetinfo().poolid());
                for (auto const& j : i.copysetinfo().peers()) {
                    tmp.set_allocated_peer(new curvefs::common::Peer(j));
                    std::string addr;
                    if (!curvefs::mds::topology::SplitPeerId(j.address(),
                                                             &addr)) {
                        std::cerr << "copyset[" << tmp.copysetid()
                                  << "] has error peerid: " << j.address()
                                  << std::endl;
                        break;
                    }
                    auto& queueRequest = ip2requests[addr];
                    if (queueRequest.empty()) {
                        queueRequest.push(curvefs::metaserver::copyset::
                                              CopysetsStatusRequest());
                    }
                    *queueRequest.front().add_copysets() = tmp;
                }
            }
            for (auto const& i : ip2requests) {
                // set host
                FLAGS_metaserverAddr = i.first;
                status::CopysetStatusTool copysetStatustool("", false);
                copysetStatustool.Init();
                auto copysets = i.second.front().copysets();
                copysetStatustool.SetRequestQueue(i.second);
                ret = copysetStatustool.RunCommand();
                auto copysetsStatus = copysetStatustool.GetResponse()->status();
                for (int m = 0, n = 0;
                     m < copysets.size() && n < copysetsStatus.size();
                     ++m, ++n) {
                    id2Status_[copysets[m].copysetid()].push_back(
                        copysetsStatus[n]);
                }
            }
            if (show_) {
                for (auto const& i : avaliableCopysetId) {
                    std::cout << "copyset: " << i << " status:[ ";
                    for (auto const& j : id2Status_[i]) {
                        std::cout << j.DebugString() << " ";
                    }
                    std::cout << "]." << std::endl;
                }
            }
        } else if (show_) {
            for (auto const& i : response_->copysetsinfo()) {
                std::cout << i.DebugString() << std::endl;
            }
        }
    }
    return ret;
}

std::map<uint32_t, std::vector<StatusType>> CopysetQueryTool::GetId2Status() {
    return id2Status_;
}

}  // namespace query
}  // namespace tools
}  // namespace curvefs
