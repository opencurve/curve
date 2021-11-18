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

DECLARE_string(copysetsId);
DECLARE_string(poolsId);
DECLARE_string(mdsAddr);
DECLARE_bool(detail);

// used for CopysetStatusTool
DECLARE_string(metaserverAddr);

namespace curvefs {
namespace tools {
namespace query {

void CopysetQueryTool::PrintHelp() {
    CurvefsToolRpc::PrintHelp();
    std::cout << " -copysetsId=" << FLAGS_copysetsId
              << " -poolsId=" << FLAGS_poolsId << " [-mdsAddr=" << FLAGS_mdsAddr
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
    curve::common::SplitString(FLAGS_copysetsId, ",", &copysetsId);
    std::vector<std::string> poolsId;
    curve::common::SplitString(FLAGS_poolsId, ",", &poolsId);
    if (copysetsId.size() != poolsId.size() || poolsId.empty()) {
        std::cerr << "copysets not match pools." << std::endl;
        return -1;
    }
    curvefs::mds::topology::GetCopysetsInfoRequest request;
    for (unsigned i = 0; i < poolsId.size(); ++i) {
        auto copysetKey = request.add_copysetkeys();
        uint64_t poolId = std::stoul(poolsId[i]);
        uint64_t copysetId = std::stoul(copysetsId[i]);
        uint64_t key = (poolId << 32) | copysetId;
        if (std::find(copysetKeys_.begin(), copysetKeys_.end(), key) !=
            copysetKeys_.end()) {  // repeat key. ignore it
            continue;
        }
        copysetKeys_.push_back(key);
        copysetKey->set_poolid(poolId);
        copysetKey->set_copysetid(copysetId);
    }
    AddRequest(request);

    service_stub_func_ = std::bind(
        &curvefs::mds::topology::TopologyService_Stub::GetCopysetsInfo,
        service_stub_.get(), std::placeholders::_1, std::placeholders::_2,
        std::placeholders::_3, nullptr);
    return 0;
}

bool CopysetQueryTool::AfterSendRequestToHost(const std::string& host) {
    if (controller_->Failed()) {
        std::cerr << "send query copysets [ " << FLAGS_copysetsId
                  << " ] request to mds: " << host
                  << " failed, errorcode= " << controller_->ErrorCode()
                  << ", error text " << controller_->ErrorText() << "\n";
        return false;
    } else {
        auto copysetsValue = response_->copysetvalues();
        if (static_cast<size_t>(copysetsValue.size()) != copysetKeys_.size()) {
            std::cerr << "wrong number of coysetsinfo. number of key is "
                      << copysetKeys_.size() << " ,number of copysets is "
                      << copysetsValue.size() << std::endl;
            return false;
        }

        for (size_t i = 0; i < copysetKeys_.size(); i++) {
            key2Infos_[copysetKeys_[i]].push_back(copysetsValue[i]);
        }

        if (show_) {
            for (auto const& i : copysetsValue) {
                std::cout << "copyset: " << i.DebugString() << std::endl;
            }
        }

        // TODO(chengyi01): detail
    }
    return true;
}

}  // namespace query
}  // namespace tools
}  // namespace curvefs
